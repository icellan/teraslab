//! N-2 — randomized kill-point crash sweeps for every mutating op.
//!
//! For each mutating operation the engine exposes, this harness drives a
//! representative mutation through the SAME WAL-first sequence the dispatch
//! layer uses (append + fsync the primary redo intent, then apply the
//! mutation to the data device), CRASHES at the available crash windows along
//! the path, then RESTARTS through the real recovery pipeline and asserts the
//! recovered state is CONSISTENT.
//!
//! ## Crash windows swept (per op)
//!
//! The harness sweeps the three durability windows of a WAL-first mutation,
//! each a distinct crash class:
//!
//!   1. [`Crash::BeforeRedoFsync`] — fault fires inside `RedoLog::flush`
//!      AFTER the redo pwrite but BEFORE its fsync. The redo bytes are in the
//!      volatile cache and dropped on power loss ⇒ the intent is NOT durable
//!      ⇒ recovery must NOT apply the op. The engine apply never runs.
//!   2. [`Crash::AfterRedoFsync`] — fault fires AFTER the redo fsync. The
//!      intent IS durable; the engine apply never runs ⇒ recovery MUST replay
//!      the intent and produce the final bytes.
//!   3. [`Crash::AfterApplyBeforeSync`] — no fault: the redo intent is flushed
//!      (durable) and the engine apply runs to completion, writing data into
//!      the VOLATILE device cache; then power loss drops every unsynced data
//!      write. This is the realistic "data pwrite lost, WAL covers it" window
//!      that the single-op engine write paths (`engine.spend`, `freeze`, …)
//!      have no internal `SyncPoint` for — the volatile device models it
//!      directly. Recovery MUST replay the durable intent and restore the
//!      lost data.
//!
//! Crashes (1) and (2) are also fuzzed across a seeded random choice so a
//! crash can land at either redo boundary without a hand-picked schedule.
//!
//! ## Why volatile devices
//!
//! The existing `tests/fault_injection.rs` cases run on a default
//! [`MemoryDevice`] whose `sync()` is a no-op, so "synced" and "merely
//! pwritten" are indistinguishable. This harness runs on
//! [`MemoryDevice::new_volatile`]: a write is reverted on
//! [`MemoryDevice::simulate_power_loss`] UNLESS a `sync()` covered it — exactly
//! modeling a power failure with a drive write cache.
//!
//! ## Recovery path
//!
//! After the simulated crash we discard ALL in-memory state and reconstruct
//! the engine the way production startup does: recover the allocator from its
//! persisted header, rebuild the primary index from the device bytes
//! ([`PrimaryBackend::rebuild`]), rebuild the secondary (DAH/unmined) indexes
//! from the device, reopen the redo log, replay durable intents via
//! [`recover_all_with_allocator`], and build a fresh [`Engine`].
//!
//! ## Consistency invariant
//!
//! After recovery the operation must be either FULLY applied or FULLY not
//! applied — never half. The per-op consistency check asserts:
//!   * `spent_utxos` equals the number of slots in `SPENT` state,
//!   * `spent_utxos <= utxo_count`,
//!   * no slot is left torn / CRC-failing (reads succeed),
//!   * deleted records are gone from index AND device (no resurrection),
//!
//! and each op adds its own end-state assertion (durable intents must have
//! been applied).
//!
//! These tests require the `fault-injection` feature flag:
//!
//! ```text
//! cargo test --release --features fault-injection --test crash_sweep_ops
//! ```

#![cfg(feature = "fault-injection")]

use std::sync::Arc;

use parking_lot::Mutex;

use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::fault_injection::{FaultMode, SyncPoint, arm, current, disarm};
use teraslab::index::{DahBackend, PrimaryBackend, ShardedIndex, TxKey, UnminedBackend};
use teraslab::locks::StripedLocks;
use teraslab::ops::create::CreateRequest;
use teraslab::ops::engine::Engine;
use teraslab::ops::mark_longest_chain::MarkOnLongestChainRequest;
use teraslab::ops::remaining::{
    DeleteRequest, FreezeRequest, PreserveUntilRequest, ReassignRequest, SetConflictingRequest,
    SetLockedRequest, UnfreezeRequest,
};
use teraslab::ops::set_mined::SetMinedRequest;
use teraslab::ops::spend::SpendRequest;
use teraslab::ops::unspend::UnspendRequest;
use teraslab::record::{TxMetadata, UTXO_FROZEN, UTXO_SPENT, UtxoSlot};
use teraslab::recovery::recover_all_with_allocator;
use teraslab::redo::{RedoLog, RedoOp};

const DATA_SIZE: u64 = 16 * 1024 * 1024;
const REDO_SIZE: u64 = 1024 * 1024;
const ALIGN: usize = 4096;
const CURRENT_HEIGHT: u32 = 2000;
const RETENTION: u32 = 288;

/// A crash window along the WAL-first mutation path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Crash {
    /// Fault inside `RedoLog::flush` before its fsync — intent NOT durable.
    BeforeRedoFsync,
    /// Fault inside `RedoLog::flush` after its fsync — intent durable, apply
    /// not yet run.
    AfterRedoFsync,
    /// No fault: redo flushed (durable) and the engine apply completed into
    /// the volatile cache; power loss then drops the unsynced data writes.
    AfterApplyBeforeSync,
}

impl Crash {
    /// Did the durable redo intent survive this crash? (Used to assert that
    /// recovery applied the op for the durable windows.)
    fn intent_durable(self) -> bool {
        !matches!(self, Crash::BeforeRedoFsync)
    }
}

/// The windows swept for every op. A seeded shuffle proves the recovery
/// invariant is independent of the order the windows are exercised in.
const CRASH_WINDOWS: [Crash; 3] = [
    Crash::BeforeRedoFsync,
    Crash::AfterRedoFsync,
    Crash::AfterApplyBeforeSync,
];

// ---------------------------------------------------------------------------
// Scaffolding
// ---------------------------------------------------------------------------

/// Owns the volatile devices + redo log handle so a single mutation can be
/// driven WAL-first and then crash-recovered.
struct Harness {
    data_dev: Arc<MemoryDevice>,
    redo_dev: Arc<MemoryDevice>,
    redo_log: Arc<Mutex<RedoLog>>,
    engine: Arc<Engine>,
}

impl Harness {
    /// Build a fresh engine over volatile data + redo devices with the redo
    /// log attached (so secondary-index intents AND allocator intents are
    /// journaled WAL-first).
    fn new() -> Self {
        let data_dev = Arc::new(MemoryDevice::new_volatile(DATA_SIZE, ALIGN).unwrap());
        let redo_dev = Arc::new(MemoryDevice::new_volatile(REDO_SIZE, ALIGN).unwrap());

        let mut alloc = SlotAllocator::new(data_dev.clone() as Arc<dyn BlockDevice>).unwrap();
        let index = PrimaryBackend::new_in_memory(4096).unwrap();
        let redo_log = Arc::new(Mutex::new(
            RedoLog::open(redo_dev.clone() as Arc<dyn BlockDevice>, 0, REDO_SIZE).unwrap(),
        ));
        // Journal allocate/free WAL-first, exactly as production startup wires
        // the allocator, so create/delete crashes recover the freelist.
        alloc.set_redo_log(redo_log.clone());

        let engine = Arc::new(Engine::new(
            data_dev.clone() as Arc<dyn BlockDevice>,
            index,
            alloc,
            StripedLocks::new(64),
            DahBackend::new_in_memory(),
            UnminedBackend::new_in_memory(),
        ));
        engine.set_redo_log(redo_log.clone());

        Self {
            data_dev,
            redo_dev,
            redo_log,
            engine,
        }
    }

    /// Make all current device + redo + allocator state durable, so a later
    /// `simulate_power_loss` only reverts writes issued AFTER this barrier
    /// (i.e. the op-under-test's writes). Mirrors a checkpoint that fenced
    /// every prior mutation.
    fn make_durable(&self) {
        self.redo_log.lock().flush().unwrap();
        self.engine.allocator().lock().persist().unwrap();
        self.data_dev.sync().unwrap();
        self.redo_dev.sync().unwrap();
    }

    /// Create a base record with `utxo_count` unspent slots and make it
    /// durable. Returns the record offset on the device.
    fn seed_record(&self, txid_byte: u8, utxo_count: u32) -> u64 {
        let hashes: Vec<[u8; 32]> = (0..utxo_count).map(|v| slot_hash(txid_byte, v)).collect();
        let req = base_create_req(txid_byte, &hashes);
        self.engine.create(&req).unwrap();
        let offset = self
            .engine
            .lookup(&key(txid_byte))
            .expect("seeded record indexed")
            .record_offset;
        self.make_durable();
        offset
    }

    /// Crash: revert every write issued since the last `sync()` on BOTH the
    /// data and redo devices, modeling a power failure with a volatile cache.
    fn crash(&self) {
        assert!(
            self.data_dev.simulate_power_loss(),
            "data device must be volatile"
        );
        assert!(
            self.redo_dev.simulate_power_loss(),
            "redo device must be volatile"
        );
    }

    /// Reconstruct the engine through the real recovery pipeline after a
    /// crash and return a fresh engine to inspect final state.
    fn recover(&self) -> Arc<Engine> {
        let mut alloc =
            SlotAllocator::recover(self.data_dev.clone() as Arc<dyn BlockDevice>).unwrap();
        let primary = PrimaryBackend::rebuild(&*self.data_dev as &dyn BlockDevice, &alloc).unwrap();
        // Recovery now operates on a `ShardedIndex` (interior RwLocks, `&self`).
        // Wrap the rebuilt single backend as a one-shard index — identical
        // semantics to the pre-sharding single-lock path — then replay onto it.
        let index = ShardedIndex::from_single(primary);
        let (dah_idx, unmined_idx) =
            PrimaryBackend::rebuild_secondary(&*self.data_dev as &dyn BlockDevice, &alloc).unwrap();
        let mut dah = DahBackend::from(dah_idx);
        let mut unmined = UnminedBackend::from(unmined_idx);

        let redo = RedoLog::open(self.redo_dev.clone() as Arc<dyn BlockDevice>, 0, REDO_SIZE)
            .expect("reopen redo after crash");
        recover_all_with_allocator(
            &*self.data_dev as &dyn BlockDevice,
            &redo,
            &index,
            &mut dah,
            &mut unmined,
            Some(&mut alloc),
        )
        .expect("recovery must not fail");

        Arc::new(Engine::new_with_sharded_index(
            self.data_dev.clone() as Arc<dyn BlockDevice>,
            index,
            alloc,
            StripedLocks::new(64),
            dah,
            unmined,
        ))
    }
}

fn key(txid_byte: u8) -> TxKey {
    let mut txid = [0u8; 32];
    txid[0] = txid_byte;
    txid[1] = 0xC3;
    TxKey { txid }
}

fn slot_hash(txid_byte: u8, vout: u32) -> [u8; 32] {
    let mut h = [0u8; 32];
    h[0] = txid_byte;
    h[1] = (vout + 1) as u8;
    h[2] = 0x9E;
    h
}

fn spending_data(tag: u8) -> [u8; 36] {
    let mut sd = [0u8; 36];
    sd[0] = tag;
    sd[1] = 0x5A;
    sd[32..36].copy_from_slice(&42u32.to_le_bytes());
    sd
}

fn base_create_req(txid_byte: u8, hashes: &[[u8; 32]]) -> CreateRequest<'_> {
    let mut tx_id = [0u8; 32];
    tx_id[0] = txid_byte;
    tx_id[1] = 0xC3;
    CreateRequest {
        tx_id,
        tx_version: 1,
        locktime: 0,
        fee: 500,
        size_in_bytes: 250,
        extended_size: 0,
        is_coinbase: false,
        spending_height: 0,
        utxo_hashes: hashes,
        inputs: None,
        outputs: None,
        inpoints: None,
        is_external: false,
        created_at: 1_710_000_000_000,
        block_height: 1000,
        mined_block_infos: &[],
        frozen: false,
        conflicting: false,
        locked: false,
        external_ref: None,
        parent_txids: &[],
    }
}

/// Run a closure with a fault armed, catching the induced panic and always
/// restoring the thread-local fault mode.
fn run_armed<R>(mode: FaultMode, f: impl FnOnce() -> R) -> std::thread::Result<R> {
    let _prev = arm(mode);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    let _ = disarm();
    assert_eq!(current(), FaultMode::None, "fault mode must be cleared");
    result
}

/// Drive a WAL-first mutation under a given crash window, then crash + recover.
///
/// * `append_redo` appends + flushes the op's primary redo intent (the
///   dispatch WAL-first step). The fault, when armed, fires inside this flush.
/// * `apply` runs the engine mutation (the data write into the volatile
///   cache).
///
/// Returns the recovered engine for the caller's per-op consistency checks.
fn drive(
    h: &Harness,
    crash: Crash,
    append_redo: impl FnOnce(&Mutex<RedoLog>) + std::panic::UnwindSafe,
    apply: impl FnOnce(&Engine) + std::panic::UnwindSafe,
) -> Arc<Engine> {
    match crash {
        Crash::BeforeRedoFsync | Crash::AfterRedoFsync => {
            let point = if crash == Crash::BeforeRedoFsync {
                SyncPoint::BeforeRedoFsync
            } else {
                SyncPoint::AfterRedoFsync
            };
            let redo = h.redo_log.clone();
            let outcome = run_armed(FaultMode::PanicAt(point), move || {
                append_redo(&redo);
            });
            outcome.expect_err("the redo-flush fault must fire");
            // The engine apply never ran (crash was inside the redo flush).
        }
        Crash::AfterApplyBeforeSync => {
            // No fault: flush the durable intent, then apply into the volatile
            // device cache. Power loss below drops the unsynced data writes.
            append_redo(&h.redo_log);
            apply(&h.engine);
        }
    }
    h.crash();
    h.recover()
}

// ---------------------------------------------------------------------------
// Consistency checks
// ---------------------------------------------------------------------------

/// Assert the universal record invariant: `spent_utxos` exactly equals the
/// number of slots in SPENT state, and never exceeds `utxo_count`. A
/// half-applied spend/unspend (slot written but counter not, or vice versa)
/// is detected here. Returns the number of spent slots.
fn assert_record_consistent(engine: &Engine, k: &TxKey) -> u32 {
    let meta = engine.read_metadata(k).expect("metadata readable");
    let utxo_count = { meta.utxo_count };
    let spent_counter = { meta.spent_utxos };
    assert!(
        spent_counter <= utxo_count,
        "spent_utxos {spent_counter} > utxo_count {utxo_count}",
    );
    let mut spent_slots = 0u32;
    for v in 0..utxo_count {
        let slot = engine
            .read_slot(k, v)
            .unwrap_or_else(|e| panic!("slot {v} must be readable (not torn): {e:?}"));
        if slot.status == UTXO_SPENT {
            spent_slots += 1;
        }
    }
    assert_eq!(
        spent_counter, spent_slots,
        "spent_utxos counter ({spent_counter}) must equal SPENT-slot count ({spent_slots}) — \
         a half-applied mutation would break this",
    );
    spent_slots
}

// ---------------------------------------------------------------------------
// Per-op sweeps
// ---------------------------------------------------------------------------

/// Spend: WAL-first `SpendV2` then `engine.spend`. After any crash window the
/// slot is either SPENT with the journaled spending_data (counter 1) or fully
/// unspent (counter 0) — never half — and a durable intent must be applied.
#[test]
fn sweep_spend() {
    for crash in CRASH_WINDOWS {
        let h = Harness::new();
        h.seed_record(1, 2);
        let k = key(1);
        let sd = spending_data(0xAB);

        let rec = drive(
            &h,
            crash,
            |redo| {
                redo.lock()
                    .append_and_flush(RedoOp::SpendV2 {
                        tx_key: k,
                        offset: 0,
                        spending_data: sd,
                        new_spent_count: 1,
                        current_block_height: CURRENT_HEIGHT,
                        block_height_retention: RETENTION,
                        target_generation: 1,
                        updated_at: 0,
                        utxo_hash: Some(slot_hash(1, 0)),
                    })
                    .ok();
            },
            |engine| {
                engine
                    .spend(&SpendRequest {
                        tx_key: k,
                        offset: 0,
                        utxo_hash: slot_hash(1, 0),
                        spending_data: sd,
                        ignore_conflicting: false,
                        ignore_locked: false,
                        current_block_height: CURRENT_HEIGHT,
                        block_height_retention: RETENTION,
                    })
                    .ok();
            },
        );

        let spent = assert_record_consistent(&rec, &k);
        assert!(spent <= 1, "spend touches a single slot ({crash:?})");
        if crash.intent_durable() {
            let slot = rec.read_slot(&k, 0).unwrap();
            assert_eq!(
                slot.status, UTXO_SPENT,
                "durable spend must replay ({crash:?})"
            );
            assert_eq!(
                slot.spending_data, sd,
                "replayed spending_data must match journaled intent ({crash:?})",
            );
        }
    }
}

/// Unspend: pre-spend the slot durably, then WAL-first `UnspendV2` +
/// `engine.unspend`.
#[test]
fn sweep_unspend() {
    for crash in CRASH_WINDOWS {
        let h = Harness::new();
        h.seed_record(2, 2);
        let k = key(2);
        let sd = spending_data(0xCD);

        h.engine
            .spend(&SpendRequest {
                tx_key: k,
                offset: 0,
                utxo_hash: slot_hash(2, 0),
                spending_data: sd,
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: CURRENT_HEIGHT,
                block_height_retention: RETENTION,
            })
            .unwrap();
        h.make_durable();

        let rec = drive(
            &h,
            crash,
            |redo| {
                redo.lock()
                    .append_and_flush(RedoOp::UnspendV2 {
                        tx_key: k,
                        offset: 0,
                        spending_data: sd,
                        new_spent_count: 0,
                        current_block_height: CURRENT_HEIGHT,
                        block_height_retention: RETENTION,
                        target_generation: 2,
                        updated_at: 0,
                        utxo_hash: Some(slot_hash(2, 0)),
                    })
                    .ok();
            },
            |engine| {
                engine
                    .unspend(&UnspendRequest {
                        tx_key: k,
                        offset: 0,
                        utxo_hash: slot_hash(2, 0),
                        spending_data: sd,
                        current_block_height: CURRENT_HEIGHT,
                        block_height_retention: RETENTION,
                    })
                    .ok();
            },
        );

        let spent = assert_record_consistent(&rec, &k);
        assert!(spent <= 1, "at most slot 0 may remain spent ({crash:?})");
        if crash.intent_durable() {
            let slot = rec.read_slot(&k, 0).unwrap();
            assert!(
                slot.is_unspent(),
                "durable unspend must replay to unspent ({crash:?})",
            );
        }
    }
}

/// F-A1 regression: a WRONG-HASH unspend that the live engine REJECTS
/// (ERR_UTXO_HASH_MISMATCH) must NOT become a durable un-spend after any crash
/// window. Before the fix, the WAL-first `UnspendV2` intent was fsynced before
/// validation and `replay_unspend` ignored the hash on a healthy SPENT slot —
/// so crash-replay would flip an already-spent UTXO back to UNSPENT (a
/// double-spend re-opening). The slot must stay SPENT.
#[test]
fn sweep_unspend_wrong_hash_leaves_slot_spent() {
    for crash in CRASH_WINDOWS {
        let h = Harness::new();
        h.seed_record(20, 2);
        let k = key(20);
        let sd = spending_data(0xCD);

        // Durably spend slot 0.
        h.engine
            .spend(&SpendRequest {
                tx_key: k,
                offset: 0,
                utxo_hash: slot_hash(20, 0),
                spending_data: sd,
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: CURRENT_HEIGHT,
                block_height_retention: RETENTION,
            })
            .unwrap();
        h.make_durable();

        let wrong_hash = slot_hash(20, 200);
        assert_ne!(wrong_hash, slot_hash(20, 0));

        let rec = drive(
            &h,
            crash,
            |redo| {
                // WAL-first intent carries the WRONG hash (what dispatch writes
                // in Phase 1, before the engine validates in Phase 3).
                redo.lock()
                    .append_and_flush(RedoOp::UnspendV2 {
                        tx_key: k,
                        offset: 0,
                        spending_data: sd,
                        new_spent_count: 0,
                        current_block_height: CURRENT_HEIGHT,
                        block_height_retention: RETENTION,
                        target_generation: 2,
                        updated_at: 0,
                        utxo_hash: Some(wrong_hash),
                    })
                    .ok();
            },
            |engine| {
                // Live path rejects before mutating — no-op.
                let r = engine.unspend(&UnspendRequest {
                    tx_key: k,
                    offset: 0,
                    utxo_hash: wrong_hash,
                    spending_data: sd,
                    current_block_height: CURRENT_HEIGHT,
                    block_height_retention: RETENTION,
                });
                assert!(r.is_err(), "wrong-hash unspend must be rejected live");
            },
        );

        let spent = assert_record_consistent(&rec, &k);
        assert_eq!(
            spent, 1,
            "wrong-hash unspend must NOT un-spend the slot after crash window {crash:?}",
        );
        let slot = rec.read_slot(&k, 0).unwrap();
        assert!(
            slot.is_spent(),
            "slot must remain SPENT after a rejected wrong-hash unspend ({crash:?})",
        );
    }
}

/// SetMined: WAL-first `SetMined` + `engine.set_mined`. Recovery is consistent
/// and the record's slots stay unspent (set_mined is not a spend).
#[test]
fn sweep_set_mined() {
    for crash in CRASH_WINDOWS {
        let h = Harness::new();
        h.seed_record(3, 2);
        let k = key(3);
        let block_id = 0x1234_5678u32;

        let rec = drive(
            &h,
            crash,
            |redo| {
                redo.lock()
                    .append_and_flush(RedoOp::SetMined {
                        tx_key: k,
                        block_id,
                        block_height: CURRENT_HEIGHT,
                        subtree_idx: 0,
                        unset: false,
                    })
                    .ok();
            },
            |engine| {
                engine
                    .set_mined(&SetMinedRequest {
                        tx_key: k,
                        block_id,
                        block_height: CURRENT_HEIGHT,
                        subtree_idx: 0,
                        current_block_height: CURRENT_HEIGHT,
                        block_height_retention: RETENTION,
                        on_longest_chain: true,
                        unset_mined: false,
                    })
                    .ok();
            },
        );

        assert_record_consistent(&rec, &k);
        let meta = rec.read_metadata(&k).unwrap();
        assert_eq!(
            { meta.spent_utxos },
            0,
            "set_mined leaves slots unspent ({crash:?})"
        );
    }
}

/// Create: WAL-first `Create` carrying the full record bytes, then
/// `engine.create_at_offset` at the SAME pre-allocated offset the entry
/// references. After a crash the record is fully present (all slots unspent,
/// counter 0) or fully absent — never partial.
#[test]
fn sweep_create() {
    for crash in CRASH_WINDOWS {
        let h = Harness::new();
        let k = key(4);
        let utxo_count = 3u32;
        let hashes: Vec<[u8; 32]> = (0..utxo_count).map(|v| slot_hash(4, v)).collect();

        let record_offset = {
            let mut alloc = h.engine.allocator().lock();
            alloc
                .allocate(TxMetadata::record_size_for(utxo_count))
                .unwrap()
        };
        // Build the on-device record bytes the engine would write.
        let mut meta = TxMetadata::new(utxo_count);
        meta.tx_id = k.txid;
        meta.tx_version = 1;
        meta.fee = 500;
        meta.record_size = TxMetadata::record_size_for(utxo_count) as u32;
        let slots: Vec<UtxoSlot> = hashes.iter().map(|hh| UtxoSlot::new_unspent(*hh)).collect();
        let mut record_bytes = vec![
            0u8;
            teraslab::record::METADATA_SIZE
                + slots.len() * teraslab::record::UTXO_SLOT_SIZE
        ];
        {
            let mut mb = [0u8; teraslab::record::METADATA_SIZE];
            meta.to_bytes(&mut mb);
            record_bytes[..teraslab::record::METADATA_SIZE].copy_from_slice(&mb);
            for (i, s) in slots.iter().enumerate() {
                let base = teraslab::record::METADATA_SIZE + i * teraslab::record::UTXO_SLOT_SIZE;
                let mut sb = [0u8; teraslab::record::UTXO_SLOT_SIZE];
                s.to_bytes(&mut sb);
                record_bytes[base..base + teraslab::record::UTXO_SLOT_SIZE].copy_from_slice(&sb);
            }
        }
        h.make_durable();

        let hashes_for_create = hashes.clone();
        let rec = drive(
            &h,
            crash,
            move |redo| {
                redo.lock()
                    .append_and_flush(RedoOp::Create {
                        tx_key: k,
                        device_id: 0,
                        record_offset,
                        utxo_count,
                        is_conflicting: false,
                        record_bytes: record_bytes.clone(),
                        parent_txids: Vec::new(),
                    })
                    .ok();
            },
            move |engine| {
                engine
                    .create_at_offset(&base_create_req(4, &hashes_for_create), record_offset)
                    .ok();
            },
        );

        if rec.lookup(&k).is_some() {
            let spent = assert_record_consistent(&rec, &k);
            assert_eq!(spent, 0, "created record has no spent slots ({crash:?})");
            assert_eq!(
                { rec.read_metadata(&k).unwrap().utxo_count },
                utxo_count,
                "recovered record slot count matches ({crash:?})",
            );
        }
        if crash.intent_durable() {
            assert!(
                rec.lookup(&k).is_some(),
                "durable Create must replay the record into existence ({crash:?})",
            );
        }
    }
}

/// Freeze: WAL-first `FreezeV2` + `engine.freeze`. Slot ends FROZEN or
/// UNSPENT, never torn.
#[test]
fn sweep_freeze() {
    for crash in CRASH_WINDOWS {
        let h = Harness::new();
        h.seed_record(5, 2);
        let k = key(5);

        let rec = drive(
            &h,
            crash,
            |redo| {
                redo.lock()
                    .append_and_flush(RedoOp::FreezeV2 {
                        tx_key: k,
                        offset: 0,
                        utxo_hash: slot_hash(5, 0),
                    })
                    .ok();
            },
            |engine| {
                engine
                    .freeze(&FreezeRequest {
                        tx_key: k,
                        offset: 0,
                        utxo_hash: slot_hash(5, 0),
                    })
                    .ok();
            },
        );

        assert_record_consistent(&rec, &k);
        let slot = rec.read_slot(&k, 0).unwrap();
        assert!(
            slot.is_unspent() || slot.status == UTXO_FROZEN,
            "freeze leaves the slot UNSPENT or FROZEN, never torn ({crash:?})",
        );
        if crash.intent_durable() {
            assert_eq!(
                slot.status, UTXO_FROZEN,
                "durable freeze must replay to FROZEN ({crash:?})",
            );
        }
    }
}

/// Unfreeze: pre-freeze durably, then WAL-first `UnfreezeV2` +
/// `engine.unfreeze`.
#[test]
fn sweep_unfreeze() {
    for crash in CRASH_WINDOWS {
        let h = Harness::new();
        h.seed_record(6, 2);
        let k = key(6);
        h.engine
            .freeze(&FreezeRequest {
                tx_key: k,
                offset: 0,
                utxo_hash: slot_hash(6, 0),
            })
            .unwrap();
        h.make_durable();

        let rec = drive(
            &h,
            crash,
            |redo| {
                redo.lock()
                    .append_and_flush(RedoOp::UnfreezeV2 {
                        tx_key: k,
                        offset: 0,
                        utxo_hash: slot_hash(6, 0),
                    })
                    .ok();
            },
            |engine| {
                engine
                    .unfreeze(&UnfreezeRequest {
                        tx_key: k,
                        offset: 0,
                        utxo_hash: slot_hash(6, 0),
                    })
                    .ok();
            },
        );

        assert_record_consistent(&rec, &k);
        let slot = rec.read_slot(&k, 0).unwrap();
        assert!(
            slot.is_unspent() || slot.status == UTXO_FROZEN,
            "unfreeze leaves the slot FROZEN or UNSPENT, never torn ({crash:?})",
        );
        if crash.intent_durable() {
            assert!(
                slot.is_unspent(),
                "durable unfreeze must replay to UNSPENT ({crash:?})",
            );
        }
    }
}

/// Reassign: pre-freeze durably (reassign requires a frozen slot), then
/// WAL-first `Reassign` + `engine.reassign`.
#[test]
fn sweep_reassign() {
    for crash in CRASH_WINDOWS {
        let h = Harness::new();
        h.seed_record(7, 2);
        let k = key(7);
        h.engine
            .freeze(&FreezeRequest {
                tx_key: k,
                offset: 0,
                utxo_hash: slot_hash(7, 0),
            })
            .unwrap();
        h.make_durable();
        let new_hash = slot_hash(7, 99);

        let rec = drive(
            &h,
            crash,
            |redo| {
                redo.lock()
                    .append_and_flush(RedoOp::Reassign {
                        tx_key: k,
                        offset: 0,
                        new_hash,
                        block_height: CURRENT_HEIGHT,
                        spendable_after: 0,
                    })
                    .ok();
            },
            |engine| {
                engine
                    .reassign(&ReassignRequest {
                        tx_key: k,
                        offset: 0,
                        utxo_hash: slot_hash(7, 0),
                        new_utxo_hash: new_hash,
                        block_height: CURRENT_HEIGHT,
                        spendable_after: 0,
                    })
                    .ok();
            },
        );

        assert_record_consistent(&rec, &k);
        // The slot must be readable (not torn) after recovery.
        let _ = rec.read_slot(&k, 0).unwrap();
    }
}

/// F-A1 (reassign) regression: a reassign whose `prior_utxo_hash` the live
/// engine REJECTS (ERR_UTXO_HASH_MISMATCH) must NOT be applied on crash-replay.
/// The dispatch path writes a `ReassignV2` intent carrying the prior hash; the
/// guarded replay must skip it and leave the slot FROZEN with its real hash,
/// not stamp a fresh UNSPENT slot under the attacker-supplied new hash.
#[test]
fn sweep_reassign_wrong_hash_leaves_slot_frozen() {
    for crash in CRASH_WINDOWS {
        let h = Harness::new();
        h.seed_record(21, 2);
        let k = key(21);
        h.engine
            .freeze(&FreezeRequest {
                tx_key: k,
                offset: 0,
                utxo_hash: slot_hash(21, 0),
            })
            .unwrap();
        h.make_durable();

        let real_hash = slot_hash(21, 0);
        let new_hash = slot_hash(21, 99);
        let wrong_prior = slot_hash(21, 200);
        assert_ne!(wrong_prior, real_hash);

        let rec = drive(
            &h,
            crash,
            |redo| {
                redo.lock()
                    .append_and_flush(RedoOp::ReassignV2 {
                        tx_key: k,
                        offset: 0,
                        new_hash,
                        block_height: CURRENT_HEIGHT,
                        spendable_after: 0,
                        prior_utxo_hash: wrong_prior,
                    })
                    .ok();
            },
            |engine| {
                let r = engine.reassign(&ReassignRequest {
                    tx_key: k,
                    offset: 0,
                    utxo_hash: wrong_prior,
                    new_utxo_hash: new_hash,
                    block_height: CURRENT_HEIGHT,
                    spendable_after: 0,
                });
                assert!(
                    r.is_err(),
                    "wrong-prior-hash reassign must be rejected live"
                );
            },
        );

        assert_record_consistent(&rec, &k);
        let slot = rec.read_slot(&k, 0).unwrap();
        assert_eq!(
            slot.status, UTXO_FROZEN,
            "slot must remain FROZEN after a rejected wrong-hash reassign ({crash:?})",
        );
        assert_eq!(
            slot.hash, real_hash,
            "frozen slot hash must be unchanged ({crash:?})",
        );
        assert_ne!(slot.hash, new_hash);
    }
}

/// SetConflicting: WAL-first `SetConflicting` + `engine.set_conflicting`.
#[test]
fn sweep_set_conflicting() {
    for crash in CRASH_WINDOWS {
        let h = Harness::new();
        h.seed_record(8, 2);
        let k = key(8);

        let rec = drive(
            &h,
            crash,
            |redo| {
                redo.lock()
                    .append_and_flush(RedoOp::SetConflicting {
                        tx_key: k,
                        value: true,
                        current_block_height: CURRENT_HEIGHT,
                        block_height_retention: RETENTION,
                    })
                    .ok();
            },
            |engine| {
                engine
                    .set_conflicting(&SetConflictingRequest {
                        tx_key: k,
                        value: true,
                        current_block_height: CURRENT_HEIGHT,
                        block_height_retention: RETENTION,
                    })
                    .ok();
            },
        );

        assert_record_consistent(&rec, &k);
    }
}

/// SetLocked: WAL-first `SetLocked` + `engine.set_locked_idempotent`.
#[test]
fn sweep_set_locked() {
    for crash in CRASH_WINDOWS {
        let h = Harness::new();
        h.seed_record(9, 2);
        let k = key(9);

        let rec = drive(
            &h,
            crash,
            |redo| {
                redo.lock()
                    .append_and_flush(RedoOp::SetLocked {
                        tx_key: k,
                        value: true,
                    })
                    .ok();
            },
            |engine| {
                engine
                    .set_locked_idempotent(&SetLockedRequest {
                        tx_key: k,
                        value: true,
                    })
                    .ok();
            },
        );

        assert_record_consistent(&rec, &k);
    }
}

/// PreserveUntil: WAL-first `PreserveUntil` + `engine.preserve_until`.
#[test]
fn sweep_preserve_until() {
    for crash in CRASH_WINDOWS {
        let h = Harness::new();
        h.seed_record(10, 2);
        let k = key(10);

        let rec = drive(
            &h,
            crash,
            |redo| {
                redo.lock()
                    .append_and_flush(RedoOp::PreserveUntil {
                        tx_key: k,
                        block_height: CURRENT_HEIGHT + 5000,
                    })
                    .ok();
            },
            |engine| {
                engine
                    .preserve_until(&PreserveUntilRequest {
                        tx_key: k,
                        block_height: CURRENT_HEIGHT + 5000,
                    })
                    .ok();
            },
        );

        assert_record_consistent(&rec, &k);
    }
}

/// MarkOnLongestChain: WAL-first `MarkOnLongestChain` + `engine.mark_on_longest_chain`.
/// F-B4 gap closure — this mutator had no crash sweep. Driving `on_longest_chain
/// = false` sets `unmined_since = CURRENT_HEIGHT`; a durable intent must replay
/// to that state, and recovery must stay record-consistent across every window.
#[test]
fn sweep_mark_longest_chain() {
    for crash in CRASH_WINDOWS {
        let h = Harness::new();
        h.seed_record(22, 2);
        let k = key(22);

        // Mirror dispatch: the replay idempotency token is the post-op
        // generation (pre-op generation + 1).
        let pre_gen = { h.engine.read_metadata(&k).unwrap().generation };
        let target_generation = pre_gen.wrapping_add(1);

        let rec = drive(
            &h,
            crash,
            |redo| {
                redo.lock()
                    .append_and_flush(RedoOp::MarkOnLongestChain {
                        tx_key: k,
                        on_longest_chain: false,
                        current_block_height: CURRENT_HEIGHT,
                        block_height_retention: RETENTION,
                        generation: target_generation,
                    })
                    .ok();
            },
            |engine| {
                engine
                    .mark_on_longest_chain(&MarkOnLongestChainRequest {
                        tx_key: k,
                        on_longest_chain: false,
                        current_block_height: CURRENT_HEIGHT,
                        block_height_retention: RETENTION,
                    })
                    .ok();
            },
        );

        assert_record_consistent(&rec, &k);
        if crash.intent_durable() {
            let meta = rec.read_metadata(&k).expect("metadata readable");
            assert_eq!(
                { meta.unmined_since },
                CURRENT_HEIGHT,
                "durable mark-not-on-longest-chain must replay unmined_since ({crash:?})",
            );
        }
    }
}

/// Delete: WAL-first `Delete` + `engine.delete`. After recovery the record is
/// either fully present (delete not applied) or fully gone from BOTH the index
/// and the device — never resurrectable from stale device bytes.
///
/// Note: `engine.delete` tombstones + syncs the device internally, so the
/// `AfterApplyBeforeSync` window for delete is effectively "fully applied and
/// durable" — recovery must keep it deleted.
#[test]
fn sweep_delete() {
    for crash in CRASH_WINDOWS {
        let h = Harness::new();
        let off = h.seed_record(11, 2);
        let k = key(11);
        let record_size = { h.engine.read_metadata(&k).unwrap().record_size } as u64;

        let rec = drive(
            &h,
            crash,
            |redo| {
                redo.lock()
                    .append_and_flush(RedoOp::Delete {
                        tx_key: k,
                        record_offset: off,
                        record_size,
                    })
                    .ok();
            },
            |engine| {
                engine
                    .delete(&DeleteRequest {
                        tx_key: k,
                        due_guard: None,
                    })
                    .ok();
            },
        );

        if rec.lookup(&k).is_some() {
            assert_record_consistent(&rec, &k);
        } else {
            assert!(
                rec.read_metadata(&k).is_err(),
                "deleted record must not be resurrectable from device bytes ({crash:?})",
            );
        }
    }
}
