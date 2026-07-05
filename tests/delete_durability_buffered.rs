//! Buffered-mode delete durability: the FreeRegion-durable / index-lost window.
//!
//! Under the DEFAULT buffered redo mode (`set_buffered_durability(true)`), a
//! `delete` frees the record's slot via `SlotAllocator::free`, which
//! UNCONDITIONALLY appends **and fsyncs** a `FreeRegion` redo record — while the
//! tombstone header write and the primary-index removal stay in the volatile
//! write-back cache and are NOT journaled (no production path emits
//! `RedoOp::Delete`; `handle_delete_batch` treats deletes as local prune GC).
//!
//! So a power loss AFTER the FreeRegion fsync but BEFORE the next checkpoint
//! reverts the unsynced tombstone (leaving the record's header intact on the
//! data device) while the redo device keeps the durable FreeRegion. On recovery
//! the device scan re-indexes the still-intact record at offset X, and redo
//! replay pushes X onto the freelist — leaving the record LIVE in the index AND
//! its offset FREE for reuse. A later `create` then allocates X and silently
//! overwrites the acked, durable record.
//!
//! These tests reproduce that exact window through the PRODUCTION delete + the
//! real recovery pipeline (device-scan rebuild + redo replay), with NO
//! `RedoOp::Delete` journaled — because production journals none. The invariant
//! they assert is: after recovery it is never the case that a record is live in
//! the index AND its offset is on the freelist.

use std::sync::Arc;

use parking_lot::Mutex;

use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahBackend, PrimaryBackend, ShardedIndex, TxKey, UnminedBackend};
use teraslab::locks::StripedLocks;
use teraslab::ops::create::CreateRequest;
use teraslab::ops::engine::Engine;
use teraslab::ops::remaining::DeleteRequest;
use teraslab::recovery::recover_all_with_allocator;
use teraslab::redo::RedoLog;

const DATA_SIZE: u64 = 16 * 1024 * 1024;
const REDO_SIZE: u64 = 1024 * 1024;
const ALIGN: usize = 4096;

/// Owns the volatile devices + redo log so a delete can be driven through the
/// production buffered path and then crash-recovered.
struct Harness {
    data_dev: Arc<MemoryDevice>,
    redo_dev: Arc<MemoryDevice>,
    redo_log: Arc<Mutex<RedoLog>>,
    engine: Arc<Engine>,
}

impl Harness {
    /// Build a fresh engine over volatile data + redo devices with the redo log
    /// attached to BOTH the engine and the allocator (exactly as production
    /// startup wires them), and buffered durability enabled — the default,
    /// hot-path mode this bug lives in.
    fn new() -> Self {
        let data_dev = Arc::new(MemoryDevice::new_volatile(DATA_SIZE, ALIGN).unwrap());
        let redo_dev = Arc::new(MemoryDevice::new_volatile(REDO_SIZE, ALIGN).unwrap());

        let mut alloc = SlotAllocator::new(data_dev.clone() as Arc<dyn BlockDevice>).unwrap();
        let index = PrimaryBackend::new_in_memory(4096).unwrap();
        let redo_log = Arc::new(Mutex::new(
            RedoLog::open(redo_dev.clone() as Arc<dyn BlockDevice>, 0, REDO_SIZE).unwrap(),
        ));
        // The allocator fsyncs a FreeRegion on every free() through THIS log.
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
        // The default production mode, and the one the bug lives in.
        engine.set_buffered_durability(true);

        Self {
            data_dev,
            redo_dev,
            redo_log,
            engine,
        }
    }

    /// Make all current device + redo + allocator state durable, so a later
    /// `crash` only reverts writes issued AFTER this barrier. Mirrors a
    /// checkpoint that fenced every prior mutation.
    fn make_durable(&self) {
        self.redo_log.lock().flush().unwrap();
        self.engine.allocator().lock().persist().unwrap();
        self.data_dev.sync().unwrap();
        self.redo_dev.sync().unwrap();
    }

    /// Create a record with `utxo_count` unspent slots and make it durable.
    /// Returns `(record offset, the slot hashes as stored on device)`.
    fn seed_record(&self, txid_byte: u8, utxo_count: u32) -> (u64, Vec<[u8; 32]>) {
        let hashes: Vec<[u8; 32]> = (0..utxo_count).map(|v| slot_hash(txid_byte, v)).collect();
        let req = base_create_req(txid_byte, &hashes);
        self.engine.create(&req).unwrap();
        let k = key(txid_byte);
        let offset = self
            .engine
            .lookup(&k)
            .expect("seeded record indexed")
            .record_offset;
        // Capture the ON-DEVICE slot hashes so post-recovery comparison does not
        // assume how create derives the stored slot from the input hash.
        let stored: Vec<[u8; 32]> = (0..utxo_count)
            .map(|v| {
                self.engine
                    .read_slot(&k, v)
                    .expect("seed slot readable")
                    .hash
            })
            .collect();
        self.make_durable();
        (offset, stored)
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

    /// Reconstruct the engine through the real recovery pipeline (device-scan
    /// rebuild → redo replay → allocator reconciliation) and return a fresh
    /// engine to inspect final state.
    fn recover(&self) -> Arc<Engine> {
        let mut alloc: teraslab::allocator::BoxedAllocator = Box::new(
            SlotAllocator::recover(self.data_dev.clone() as Arc<dyn BlockDevice>).unwrap(),
        );
        let primary = PrimaryBackend::rebuild(&*self.data_dev as &dyn BlockDevice, &alloc).unwrap();
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

/// Assert the core invariant on a recovered engine: NOT (record `k` live in the
/// index AND its offset on the freelist). Either the record is fully gone, or it
/// is live and its offset is NOT allocatable (absent from the freelist). Returns
/// `Some(offset)` if the record is live, `None` if fully gone.
fn assert_no_live_and_free(engine: &Engine, k: &TxKey) -> Option<u64> {
    match engine.lookup(k) {
        Some(entry) => {
            let on_freelist = engine
                .allocator()
                .lock()
                .free_region_containing(entry.record_offset)
                .is_some();
            assert!(
                !on_freelist,
                "INVARIANT VIOLATED: record is live in the index at offset {} \
                 AND that offset is on the freelist — a later create can overwrite \
                 acked durable data",
                entry.record_offset,
            );
            Some(entry.record_offset)
        }
        None => None,
    }
}

/// Test 1 — the data-loss window itself.
///
/// Create A (buffered, acked, checkpointed), delete A (fsyncs FreeRegion, buffers
/// the tombstone + index removal), crash BEFORE the next checkpoint, recover.
/// The invariant must hold: A is never both live in the index and on the
/// freelist. Index-wins reconciliation restores the consistent "delete never
/// happened" tail-loss state — A alive, its offset reserved.
#[test]
fn buffered_delete_freeregion_durable_index_lost_stays_consistent() {
    let h = Harness::new();
    let (off, stored) = h.seed_record(11, 2);
    let k = key(11);

    // Production buffered delete: fsyncs FreeRegion via allocator.free, but the
    // tombstone header + index removal stay in the volatile cache and NO
    // RedoOp::Delete is journaled (production path emits none).
    h.engine
        .delete(&DeleteRequest {
            tx_key: k,
            due_guard: None,
        })
        .expect("delete must succeed");

    // Power loss BEFORE any post-delete checkpoint.
    h.crash();
    let rec = h.recover();

    // The record's header survived (tombstone unsynced), so the device-scan
    // rebuild re-indexes it. Reconciliation must have pulled its offset back off
    // the freelist — otherwise a create could overwrite it.
    let recovered_off =
        assert_no_live_and_free(&rec, &k).expect("A's intact header must be re-indexed on rebuild");
    assert_eq!(
        recovered_off, off,
        "A must be re-indexed at its original offset",
    );

    // And A's data must be intact and readable.
    let meta = rec.read_metadata(&k).expect("A metadata readable");
    assert_eq!({ meta.utxo_count }, 2, "A must keep its two slots");
    for v in 0..2u32 {
        let slot = rec.read_slot(&k, v).expect("A slot readable");
        assert_eq!(
            slot.hash, stored[v as usize],
            "A slot {v} data must be intact",
        );
    }
}

/// Test 2 — no silent overwrite of the resurrected record.
///
/// Following recovery from the window in test 1 (A live again), create a NEW
/// record B. B must NOT land on A's offset while A is still indexed, and A must
/// read back intact afterwards.
#[test]
fn create_after_recovery_does_not_overwrite_resurrected_record() {
    let h = Harness::new();
    let (off_a, stored_a) = h.seed_record(11, 2);
    let k_a = key(11);

    h.engine
        .delete(&DeleteRequest {
            tx_key: k_a,
            due_guard: None,
        })
        .expect("delete must succeed");
    h.crash();
    let rec = h.recover();

    // A is live again after reconciliation.
    let recovered_a = assert_no_live_and_free(&rec, &k_a).expect("A must be live after recovery");
    assert_eq!(recovered_a, off_a);

    // Allocate/create B. With A's offset reserved, B must land elsewhere.
    let hashes_b: Vec<[u8; 32]> = (0..3).map(|v| slot_hash(22, v)).collect();
    rec.create(&base_create_req(22, &hashes_b))
        .expect("create B must succeed");
    let k_b = key(22);
    let off_b = rec.lookup(&k_b).expect("B must be indexed").record_offset;
    assert_ne!(
        off_b, off_a,
        "B must NOT be placed on A's still-indexed offset (silent overwrite)",
    );

    // A must still read back intact — not overwritten by B.
    let meta_a = rec.read_metadata(&k_a).expect("A metadata still readable");
    assert_eq!({ meta_a.utxo_count }, 2, "A must keep its two slots");
    for v in 0..2u32 {
        let slot = rec
            .read_slot(&k_a, v)
            .expect("A slot readable after B create");
        assert_eq!(
            slot.hash, stored_a[v as usize],
            "A slot {v} must be intact after B is created",
        );
    }
    // And B's own data is correct (its create really happened, not a no-op).
    let meta_b = rec.read_metadata(&k_b).expect("B metadata readable");
    assert_eq!({ meta_b.utxo_count }, 3, "B must have its three slots");
}

/// Test 3 — positive control / regression guard.
///
/// A delete that DID checkpoint before the crash stays deleted after recovery:
/// its tombstone is durable (so the device scan does not re-index it) and its
/// FreeRegion is durable (offset free). Reconciliation must NOT resurrect it.
#[test]
fn checkpointed_delete_stays_deleted_after_recovery() {
    let h = Harness::new();
    let (off, _stored) = h.seed_record(11, 2);
    let k = key(11);

    h.engine
        .delete(&DeleteRequest {
            tx_key: k,
            due_guard: None,
        })
        .expect("delete must succeed");

    // Checkpoint: make the tombstone + FreeRegion + freelist all durable BEFORE
    // the crash, so the delete is fully committed.
    h.make_durable();
    h.crash();
    let rec = h.recover();

    // The record is gone: not re-indexed (tombstone durable), and its offset is
    // legitimately free (the delete really happened).
    assert!(
        rec.lookup(&k).is_none(),
        "a checkpointed delete must stay deleted — not resurrected by reconciliation",
    );
    assert!(
        rec.read_metadata(&k).is_err(),
        "deleted record must not be resurrectable from device bytes",
    );
    // The freed offset stays reclaimable (no false reservation of dead space).
    assert!(
        rec.allocator().lock().free_region_containing(off).is_some(),
        "a genuinely deleted record's offset must remain on the freelist for reuse",
    );
}
