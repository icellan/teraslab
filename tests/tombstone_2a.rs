//! Reverse-heal Phase 2a — generation-aware deletion tombstones + retention.
//!
//! These tests drive the PRODUCTION delete path (`Engine::delete` /
//! `Engine::prune_delete`) and the real buffered-delete crash-recovery pipeline
//! (device-scan rebuild → redo replay → allocator reconciliation) to prove the
//! Phase 2a deliverables:
//!
//!   * a delete records a tombstone carrying the record's FROZEN generation N;
//!   * the tombstone is durable across a checkpoint + reload (boot replay);
//!   * **Invariant TS-1** — a tombstone for `k` exists on this node ⟺ this
//!     node's delete of `k` is durable: a delete lost to a pre-checkpoint crash
//!     leaves the record LIVE and writes NO tombstone;
//!   * checkpoint-time GC expires a tombstone past its retention horizon;
//!   * the whole subsystem is a zero-cost no-op when no tombstone log is
//!     attached (flag off);
//!   * the KO-3 gate: a `NotDue`/preserved sweep-delete writes no tombstone;
//!   * the O(1) `tombstone_at_or_ahead` query is generation-correct.

use std::sync::Arc;

use parking_lot::Mutex;

use teraslab::allocator::SlotAllocator;
use teraslab::cluster::migration::AtomicShardBitmap;
use teraslab::cluster::shards::ShardTable;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahBackend, PrimaryBackend, ShardedIndex, TxKey};
use teraslab::locks::StripedLocks;
use teraslab::ops::create::CreateRequest;
use teraslab::ops::engine::Engine;
use teraslab::ops::error::SpendError;
use teraslab::ops::remaining::DeleteRequest;
use teraslab::ops::spend::SpendRequest;
use teraslab::ops::tombstone::{TombstoneCause, TombstoneLog};
use teraslab::recovery::recover_all_with_allocator;
use teraslab::redo::RedoLog;

const DATA_SIZE: u64 = 16 * 1024 * 1024;
const REDO_SIZE: u64 = 1024 * 1024;
const ALIGN: usize = 4096;
const TEST_RETENTION: u32 = 10;

/// Owns the volatile devices + redo log + a real file-backed tombstone log so a
/// delete can be driven through the production buffered path, checkpointed
/// (`make_durable`), crash-recovered, and inspected for its tombstone state.
struct Harness {
    data_dev: Arc<MemoryDevice>,
    redo_dev: Arc<MemoryDevice>,
    redo_log: Arc<Mutex<RedoLog>>,
    engine: Arc<Engine>,
    tombstone_path: std::path::PathBuf,
    _dir: tempfile::TempDir,
    tombstones: bool,
    retention: u32,
}

impl Harness {
    fn build(tombstones: bool, retention: u32) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let tombstone_path = dir.path().join("teraslab.tombstones");

        let data_dev = Arc::new(MemoryDevice::new_volatile(DATA_SIZE, ALIGN).unwrap());
        let redo_dev = Arc::new(MemoryDevice::new_volatile(REDO_SIZE, ALIGN).unwrap());

        let mut alloc = SlotAllocator::new(data_dev.clone() as Arc<dyn BlockDevice>).unwrap();
        let index = PrimaryBackend::new_in_memory(4096).unwrap();
        let redo_log = Arc::new(Mutex::new(
            RedoLog::open(redo_dev.clone() as Arc<dyn BlockDevice>, 0, REDO_SIZE).unwrap(),
        ));
        alloc.set_redo_log(redo_log.clone());

        let engine = Arc::new(Engine::new(
            data_dev.clone() as Arc<dyn BlockDevice>,
            index,
            alloc,
            StripedLocks::new(64),
            DahBackend::new_in_memory(),
        ));
        engine.set_redo_log(redo_log.clone());
        engine.set_buffered_durability(true);

        if tombstones {
            let log = TombstoneLog::new(
                tombstone_path.clone(),
                engine.index_seed(),
                engine.index_shard_count(),
                retention,
            );
            engine.set_tombstone_log(log);
        }

        Self {
            data_dev,
            redo_dev,
            redo_log,
            engine,
            tombstone_path,
            _dir: dir,
            tombstones,
            retention,
        }
    }

    fn new_with_tombstones() -> Self {
        Self::build(true, TEST_RETENTION)
    }

    fn new_no_tombstones() -> Self {
        Self::build(false, TEST_RETENTION)
    }

    /// Make all current device + redo + allocator state durable AND persist the
    /// tombstone log — modeling a checkpoint that fenced every prior mutation
    /// and flushed the tombstone set.
    fn make_durable(&self) {
        self.redo_log.lock().flush().unwrap();
        self.engine.allocator().lock().persist().unwrap();
        self.data_dev.sync().unwrap();
        self.redo_dev.sync().unwrap();
        if self.tombstones {
            self.engine.persist_tombstones().unwrap();
        }
    }

    fn seed_record(&self, txid_byte: u8, utxo_count: u32) -> TxKey {
        let hashes: Vec<[u8; 32]> = (0..utxo_count).map(|v| slot_hash(txid_byte, v)).collect();
        let req = base_create_req(txid_byte, &hashes);
        self.engine.create(&req).unwrap();
        self.make_durable();
        key(txid_byte)
    }

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

    /// Reconstruct the engine through the real recovery pipeline and re-attach a
    /// freshly LOADED tombstone log (boot replay), then run the boot
    /// reconcile-against-live + floor-GC, exactly as production startup does.
    fn recover(&self) -> Arc<Engine> {
        let mut alloc: teraslab::allocator::BoxedAllocator = Box::new(
            SlotAllocator::recover(self.data_dev.clone() as Arc<dyn BlockDevice>).unwrap(),
        );
        let primary = PrimaryBackend::rebuild(&*self.data_dev as &dyn BlockDevice, &alloc).unwrap();
        let index = ShardedIndex::from_single(primary);
        let dah_idx =
            PrimaryBackend::rebuild_secondary(&*self.data_dev as &dyn BlockDevice, &alloc).unwrap();
        let mut dah = DahBackend::from(dah_idx);

        let redo = RedoLog::open(self.redo_dev.clone() as Arc<dyn BlockDevice>, 0, REDO_SIZE)
            .expect("reopen redo after crash");
        recover_all_with_allocator(
            &*self.data_dev as &dyn BlockDevice,
            &redo,
            &index,
            &mut dah,
            Some(&mut alloc),
        )
        .expect("recovery must not fail");

        let engine = Arc::new(Engine::new_with_sharded_index(
            self.data_dev.clone() as Arc<dyn BlockDevice>,
            index,
            alloc,
            StripedLocks::new(64),
            dah,
        ));
        engine.set_buffered_durability(true);

        if self.tombstones {
            let log = TombstoneLog::load(
                self.tombstone_path.clone(),
                engine.index_seed(),
                engine.index_shard_count(),
                self.retention,
            )
            .expect("tombstone log must reload");
            engine.set_tombstone_log(log);
            // Boot recovery order (design §A): reconcile against the recovered
            // live index (drop any dangling tombstone over a resurrected record),
            // then floor-GC by the restored last-durable height.
            engine.reconcile_tombstones_against_live_index();
            engine.gc_tombstones();
        }
        engine
    }

    fn spend_output(&self, k: &TxKey, vout: u32, height: u32) {
        self.engine
            .spend(&SpendRequest {
                tx_key: *k,
                offset: vout,
                utxo_hash: slot_hash(k.txid[0], vout),
                spending_data: [0xAB; 36],
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: height,
                block_height_retention: 288,
            })
            .expect("spend must succeed");
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

/// The O(1) heal-apply query is generation-correct: at-or-ahead is true for any
/// generation <= the frozen deletion generation, false above it, and false for
/// an unknown key.
#[test]
fn tombstone_at_or_ahead_query() {
    let h = Harness::new_with_tombstones();
    let k = h.seed_record(11, 3);
    // Two spends bump the record's generation to 2 (each real mutation +1).
    h.spend_output(&k, 0, 1000);
    h.spend_output(&k, 1, 1000);
    let generation = { h.engine.read_metadata(&k).unwrap().generation };
    assert_eq!(generation, 2, "two spends must bump generation to 2");

    h.engine
        .delete(&DeleteRequest {
            tx_key: k,
            due_guard: None,
        })
        .expect("delete must succeed");

    assert!(
        h.engine.tombstone_at_or_ahead(&k, 0),
        "deletion at gen 2 is at-or-ahead of gen 0"
    );
    assert!(
        h.engine.tombstone_at_or_ahead(&k, 2),
        "deletion at gen 2 is at-or-ahead of gen 2 (equal)"
    );
    assert!(
        !h.engine.tombstone_at_or_ahead(&k, 3),
        "deletion at gen 2 is NOT at-or-ahead of gen 3 (source strictly newer)"
    );
    assert!(
        !h.engine.tombstone_at_or_ahead(&key(99), 0),
        "an unknown key has no tombstone"
    );
}

/// A delete records a tombstone carrying the record's frozen generation N (the
/// value the record held at its last real mutation, discarded pre-2a).
#[test]
fn delete_records_tombstone_at_generation_when_enabled() {
    let h = Harness::new_with_tombstones();
    let k = h.seed_record(11, 2);
    h.spend_output(&k, 0, 1000);
    let frozen_gen = { h.engine.read_metadata(&k).unwrap().generation };
    assert_eq!(frozen_gen, 1);

    h.engine
        .delete(&DeleteRequest {
            tx_key: k,
            due_guard: None,
        })
        .expect("delete must succeed");

    let (recorded_gen, _height) = h
        .engine
        .tombstone_lookup(&k)
        .expect("a tombstone must exist for the deleted record");
    assert_eq!(
        recorded_gen, frozen_gen,
        "the tombstone must carry the record's FROZEN generation N"
    );
    assert!(h.engine.tombstone_at_or_ahead(&k, frozen_gen));
}

/// Flag off (no tombstone log attached): the whole subsystem is a zero-cost
/// no-op — no log, no index, no behavior change to the delete.
#[test]
fn tombstone_disabled_is_zero_cost_noop() {
    let h = Harness::new_no_tombstones();
    let k = h.seed_record(11, 2);

    assert!(
        !h.engine.tombstones_enabled(),
        "no log attached ⇒ tombstones disabled"
    );

    h.engine
        .delete(&DeleteRequest {
            tx_key: k,
            due_guard: None,
        })
        .expect("delete must succeed");

    // No index, no query hit, no file on disk.
    assert!(!h.engine.tombstone_at_or_ahead(&k, 0));
    assert!(h.engine.tombstone_lookup(&k).is_none());
    assert!(
        !h.tombstone_path.exists(),
        "a disabled node must not create a tombstone file"
    );
    // Delete behavior unchanged: the record is gone.
    assert!(h.engine.lookup(&k).is_none());
}

/// KO-3 gate (design §G/E3): a sweep-delete that re-validates as `NotDue`
/// (preserved / not DAH-eligible under the stripe lock) destroys nothing and
/// therefore writes NO tombstone. A tombstone rides only an actually-executed
/// `delete_inner` past the KO-3 recheck.
#[test]
fn tombstone_not_written_for_preserved_or_notdue_delete() {
    let h = Harness::new_with_tombstones();
    // Fresh unmined record is not DAH-eligible, so a guarded sweep-delete
    // re-validates as NotDue.
    let k = h.seed_record(11, 2);

    let err = h
        .engine
        .prune_delete(&DeleteRequest {
            tx_key: k,
            due_guard: Some(500),
        })
        .expect_err("a not-due sweep-delete must be refused");
    assert!(
        matches!(err, SpendError::NotDue),
        "expected NotDue, got {err:?}"
    );

    assert!(
        h.engine.lookup(&k).is_some(),
        "the record must still be live after a NotDue delete"
    );
    assert!(
        !h.engine.tombstone_at_or_ahead(&k, 0),
        "no tombstone may be written for a NotDue/preserved delete"
    );
    assert!(h.engine.tombstone_lookup(&k).is_none());
}

/// **Invariant TS-1.** A delete lost to a pre-checkpoint crash leaves the record
/// LIVE (resurrected by the buffered-delete reconcile) and writes NO tombstone.
/// This empirically validates that the tombstone append rides the SAME barrier
/// as the delete's index-unregister + FreeRegion: a crash before checkpoint
/// reverts BOTH, so there is never a dangling tombstone over a live record.
#[test]
fn tombstone_absent_when_delete_reverted_by_buffered_reconcile() {
    let h = Harness::new_with_tombstones();
    let k = h.seed_record(11, 2);

    // Production buffered delete: fsyncs FreeRegion, buffers the tombstone header
    // + index removal, records the tombstone in RAM only (NOT persisted — no
    // checkpoint runs before the crash).
    h.engine
        .delete(&DeleteRequest {
            tx_key: k,
            due_guard: None,
        })
        .expect("delete must succeed");

    // Crash BEFORE any post-delete checkpoint.
    h.crash();
    let rec = h.recover();

    // The record's intact header is re-indexed and reconciliation pulls its
    // offset back off the freelist: the record is LIVE again.
    assert!(
        rec.lookup(&k).is_some(),
        "TS-1: a delete reverted by the buffered reconcile must leave the record LIVE"
    );
    // And there must be NO tombstone — the un-checkpointed append reverted too.
    assert!(
        !rec.tombstone_at_or_ahead(&k, u32::MAX),
        "TS-1 VIOLATED: a dangling tombstone survived over a resurrected live record"
    );
    assert!(rec.tombstone_lookup(&k).is_none());
}

/// A checkpointed delete's tombstone survives a crash + reload: boot replay of
/// the tombstone log rebuilds the in-RAM index.
#[test]
fn tombstone_survives_crash_and_reload() {
    let h = Harness::new_with_tombstones();
    let k = h.seed_record(11, 2);
    let frozen_gen = { h.engine.read_metadata(&k).unwrap().generation };

    h.engine
        .delete(&DeleteRequest {
            tx_key: k,
            due_guard: None,
        })
        .expect("delete must succeed");
    // Checkpoint: makes the tombstone (+ zeroed header + FreeRegion) durable.
    h.make_durable();
    h.crash();
    let rec = h.recover();

    // The record is genuinely gone (durable delete), and the tombstone survived
    // the reload.
    assert!(
        rec.lookup(&k).is_none(),
        "a checkpointed delete must stay deleted"
    );
    let (reloaded_gen, _h) = rec
        .tombstone_lookup(&k)
        .expect("the tombstone must survive the crash + reload (boot replay)");
    assert_eq!(
        reloaded_gen, frozen_gen,
        "reloaded tombstone carries the frozen generation"
    );
    assert!(rec.tombstone_at_or_ahead(&k, frozen_gen));
}

/// P2-2 (Invariant TS-1 for re-created keys, E5 re-org mitigation). Re-creating
/// a deleted txid must clear the stale tombstone from the prior delete IMMEDIATELY
/// (online, not just at boot reconcile), and the durable log must not carry it
/// either — otherwise a Phase 3/4 online heal would drop the resurrected live
/// record. Drives the real create/delete/create path + a durable reload.
#[test]
fn create_after_delete_clears_tombstone() {
    let h = Harness::new_with_tombstones();
    let k = h.seed_record(11, 2);

    // Delete k: a tombstone is recorded (in RAM + buffered append).
    h.engine
        .delete(&DeleteRequest {
            tx_key: k,
            due_guard: None,
        })
        .expect("delete must succeed");
    assert!(
        h.engine.tombstone_lookup(&k).is_some(),
        "the delete must record a tombstone",
    );
    assert!(h.engine.tombstone_at_or_ahead(&k, u32::MAX));

    // Re-create the SAME txid: the key is LIVE again, so its stale tombstone must
    // be dropped from the in-RAM index at once (ONLINE — before any boot reconcile).
    let hashes: Vec<[u8; 32]> = (0..2u32).map(|v| slot_hash(11, v)).collect();
    h.engine
        .create(&base_create_req(11, &hashes))
        .expect("re-create of the deleted txid must succeed");
    assert!(
        h.engine.lookup(&k).is_some(),
        "the re-created record must be live",
    );
    assert!(
        !h.engine.tombstone_at_or_ahead(&k, u32::MAX),
        "re-create must clear the stale tombstone (else an online heal drops the live record)",
    );
    assert!(
        h.engine.tombstone_lookup(&k).is_none(),
        "no tombstone may remain for the re-created key",
    );

    // The durable log must not carry it either: persist, then reload WITHOUT the
    // boot reconcile (which would otherwise mask a stale durable tombstone).
    h.make_durable();
    let reloaded = TombstoneLog::load(
        h.tombstone_path.clone(),
        h.engine.index_seed(),
        h.engine.index_shard_count(),
        TEST_RETENTION,
    )
    .unwrap();
    assert!(
        reloaded.lookup(&k).is_none(),
        "the compacted durable log must not re-append the cleared tombstone",
    );
}

/// Checkpoint-time GC drops a tombstone once
/// `deletion_height + RETENTION_BLOCKS <= last_durable_height`.
#[test]
fn tombstone_gc_expires_past_retention_horizon() {
    let h = Harness::new_with_tombstones();
    // Observe a base height so the client delete stamps deletion_height = H.
    let base_h = 100u32;
    h.engine.observe_block_height(base_h);
    let k = h.seed_record(11, 2);

    h.engine
        .delete(&DeleteRequest {
            tx_key: k,
            due_guard: None,
        })
        .expect("delete must succeed");
    let (_gen, height) = h
        .engine
        .tombstone_lookup(&k)
        .expect("tombstone present at delete");
    assert_eq!(
        height, base_h,
        "deletion_height must be the observed height"
    );

    // Persist just below the horizon: still retained.
    h.engine.observe_block_height(base_h + TEST_RETENTION - 1);
    h.engine.persist_tombstones().unwrap();
    assert!(
        h.engine.tombstone_at_or_ahead(&k, 0),
        "tombstone still within retention horizon must be retained"
    );

    // Advance past the horizon and persist again: GC drops it (log + in-RAM).
    h.engine.observe_block_height(base_h + TEST_RETENTION);
    h.engine.persist_tombstones().unwrap();
    assert!(
        !h.engine.tombstone_at_or_ahead(&k, 0),
        "tombstone past retention horizon must be GC'd"
    );
    assert!(h.engine.tombstone_lookup(&k).is_none());

    // The durable log is compacted too: a fresh reload sees no tombstone.
    let reloaded = TombstoneLog::load(
        h.tombstone_path.clone(),
        h.engine.index_seed(),
        h.engine.index_shard_count(),
        TEST_RETENTION,
    )
    .unwrap();
    assert!(
        !reloaded.at_or_ahead(&k, 0),
        "GC must compact the durable log, not just the in-RAM index"
    );
    // Sanity: the cause enum is exercised end-to-end.
    assert_eq!(TombstoneCause::ClientDelete as u8, 1);
}

// ---------------------------------------------------------------------------
// Reverse-heal Phase 2d — retention / GC-vs-heal race hardening.
//
// A reverse-heal spanning a checkpoint must NOT let the checkpoint GC a
// tombstone the heal still needs to gate an incoming record (which would open a
// resurrection window mid-heal). The engine's checkpoint retention GC consults a
// shared inbound-fence bitmap (`Engine::set_tombstone_gc_guard`): while a heal
// (or forward migration) references a key's CLUSTER shard, that key's tombstone
// is RETAINED past its retention horizon so RULE-DS still blocks a resurrecting
// image; once the heal completes and the fence clears, the next checkpoint GC's
// it normally. These tests drive the REAL delete + retention-GC pipeline with a
// guard bitmap standing in for the cluster's inbound fence.
// ---------------------------------------------------------------------------

/// A tombstone that WOULD be GC'd by retention
/// (`deletion_height + RETENTION_BLOCKS <= last_durable_height`) is RETAINED
/// while a heal references its shard, and is GC'd normally once the heal
/// completes. Pre-fix (guard not consulted) it is GC'd mid-heal — the
/// resurrection window this phase closes.
#[test]
fn tombstone_retained_across_inflight_heal() {
    let h = Harness::new_with_tombstones();
    // The cluster inbound-fence bitmap stand-in; a set bit == "shard S has an
    // in-flight reverse-heal", exactly the handle the boot path shares via
    // `RunningCluster::inbound_fence_bitmap()`.
    let guard = Arc::new(AtomicShardBitmap::new());
    h.engine.set_tombstone_gc_guard(guard.clone());

    // Delete `k` at height H, stamping a tombstone at deletion_height = H.
    let base_h = 100u32;
    h.engine.observe_block_height(base_h);
    let k = h.seed_record(11, 2);
    h.engine
        .delete(&DeleteRequest {
            tx_key: k,
            due_guard: None,
        })
        .expect("delete must succeed");
    let (frozen_gen, height) = h
        .engine
        .tombstone_lookup(&k)
        .expect("tombstone present at delete");
    assert_eq!(
        height, base_h,
        "deletion_height must be the observed height"
    );

    // A reverse-heal begins for `k`'s CLUSTER shard: raise the inbound fence.
    let shard = ShardTable::shard_for_key(&k);
    guard.set(shard);

    // Advance strictly PAST the retention horizon and checkpoint. Without the
    // guard this GC's the tombstone (the pre-fix resurrection window); WITH the
    // guard the healing shard's tombstone is RETAINED.
    h.engine.observe_block_height(base_h + TEST_RETENTION);
    h.engine.persist_tombstones().unwrap();

    assert!(
        h.engine.tombstone_lookup(&k).is_some(),
        "a past-retention tombstone must be RETAINED while its shard is being \
         reverse-healed (else the heal reopens a resurrection window)",
    );
    // RULE-DS still fires on the retained tombstone: an incoming heal image at
    // any generation <= the frozen N is dropped, never resurrected.
    assert!(
        h.engine.tombstone_blocks_heal_apply(&k, frozen_gen),
        "RULE-DS must still block a resurrecting heal image while the tombstone \
         is retained mid-heal",
    );
    assert!(h.engine.tombstone_at_or_ahead(&k, frozen_gen));

    // The heal completes → the inbound fence clears → the next checkpoint GC's
    // the now-unprotected, past-retention tombstone normally (no permanent leak).
    guard.clear(shard);
    h.engine.persist_tombstones().unwrap();
    assert!(
        h.engine.tombstone_lookup(&k).is_none(),
        "once the heal completes the deferred tombstone is GC'd on the next \
         checkpoint — the guard defers, it never leaks",
    );

    // The durable log is compacted too: a fresh reload sees no tombstone.
    let reloaded = TombstoneLog::load(
        h.tombstone_path.clone(),
        h.engine.index_seed(),
        h.engine.index_shard_count(),
        TEST_RETENTION,
    )
    .unwrap();
    assert!(!reloaded.at_or_ahead(&k, frozen_gen));
}

/// The guard NEVER leaks tombstones when no heal is in flight: with a guard
/// attached but no shard fenced, a past-retention tombstone is GC'd exactly as
/// it is without the guard — normal retention GC is unchanged.
#[test]
fn tombstone_gc_unaffected_when_no_heal_inflight() {
    let h = Harness::new_with_tombstones();
    let guard = Arc::new(AtomicShardBitmap::new());
    h.engine.set_tombstone_gc_guard(guard.clone());

    let base_h = 100u32;
    h.engine.observe_block_height(base_h);
    let k = h.seed_record(12, 2);
    h.engine
        .delete(&DeleteRequest {
            tx_key: k,
            due_guard: None,
        })
        .expect("delete must succeed");
    assert!(h.engine.tombstone_lookup(&k).is_some());

    // No shard fenced (no heal in flight). Advance past retention and checkpoint:
    // the tombstone is GC'd normally — the guard adds no retention when empty.
    h.engine.observe_block_height(base_h + TEST_RETENTION);
    h.engine.persist_tombstones().unwrap();
    assert!(
        h.engine.tombstone_lookup(&k).is_none(),
        "with no heal in flight the guard must not retain a past-retention \
         tombstone — normal GC is unchanged",
    );

    // A heal on a DIFFERENT shard must not protect this key either (per-shard
    // scoping). `observe_block_height` is a running max, so use a HIGHER base for
    // k2 (the first half already advanced the height). Delete k2, fence a shard
    // guaranteed NOT to be k2's (flip the low bit), and confirm k2's tombstone
    // still GC's past retention.
    let base_h2 = base_h + TEST_RETENTION + 100;
    h.engine.observe_block_height(base_h2);
    let k2 = h.seed_record(13, 2);
    h.engine
        .delete(&DeleteRequest {
            tx_key: k2,
            due_guard: None,
        })
        .expect("delete must succeed");
    let unrelated_shard = ShardTable::shard_for_key(&k2) ^ 1; // != k2's shard
    guard.set(unrelated_shard);
    h.engine.observe_block_height(base_h2 + TEST_RETENTION);
    h.engine.persist_tombstones().unwrap();
    assert!(
        h.engine.tombstone_lookup(&k2).is_none(),
        "a heal on an unrelated shard must not retain this key's tombstone",
    );
}
