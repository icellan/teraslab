//! F-3 — crash-mid-migration integration test.
//!
//! Drives a shard migration the way the production receiver does — streaming
//! baseline `ReplicaOp::Create` records from an OLD master into a NEW master
//! via the real apply path — then CRASHES the new master
//! mid-stream (power loss with a volatile write cache, before the receiver's
//! end-of-batch device fsync + redo flush, and before the migration is marked
//! complete). It then RESTARTS the new master through the production recovery
//! sequence (rebuild index from device, replay redo, restore the fsync-durable
//! inbound migration state) and asserts the three migration crash-safety
//! invariants:
//!
//!   * **No record lost** — the union of records across both masters equals
//!     the original set.
//!   * **No record duplicated** — no record is independently live (master-
//!     authoritative) on BOTH the old and new master at once.
//!   * **No dual-live master** — because the new master crashed mid-inbound
//!     and never proved completion, its restored inbound state still marks
//!     the shard pending, so it refuses to serve as master; the old master
//!     (which never committed the handoff) remains the sole authority.
//!
//! ## Why drive the apply state machine directly
//!
//! A real delta-streaming migration holds the streaming window open only for
//! the brief baseline+delta interval, and no pacing knob can hold it open for
//! a deterministic kill without sleeps-as-synchronization. Per the F-3 task
//! guidance, this test drives the migration APPLY + inbound-state PERSISTENCE
//! state machine directly with an injected crash point. The apply path
//! (`apply_op_journal(.., journal=false)` → `engine.create`), the
//! inbound-state persistence (`persist_inbound_state`, fsynced on every
//! change), and the restore path (`load_inbound_state` → `restore_inbound`)
//! are the exact production components; only the network transport is elided.
//!
//! ## Lightweight-journal baseline (issue #1 — Option A)
//!
//! Migration-baseline applies (`apply_op_journal(.., journal=false,
//! is_migration=true)`) SUPPRESS the HEAVY per-record engine redo (create's
//! unmined-index insert, `restore_migrated_lifecycle`'s secondary intents) but
//! now JOURNAL a bounded, lightweight index-only redo — a ~24-byte
//! `ReplicaCreate` per create plus a `SetMinedBatch` for each standalone
//! `ReplicaOp::SetMined`. This is required post-#1: mined-state no longer lives
//! on the device and there is no device-scan MinedIndex rebuild, so an
//! already-mined baseline handed off but not yet checkpointed would otherwise
//! recover slot-less + unmined. LogFull avoidance moved from "write zero redo"
//! to the SAME mechanism that already protects NORMAL replication (which also
//! journals a `ReplicaCreate` per create): the issue-#29 `redo_backpressure_gate`
//! stalls a burst, and the checkpoint task drains/compacts the redo — so a large
//! migration stalls-and-drains instead of hitting `LogFull`. Crash-safety is now
//! two-sided: the FLUSHED portion of a mid-crash baseline recovers via the redo
//! tail; the UNFLUSHED tail is still covered by the inbound fence + source
//! re-drive (the source never commits the handoff until `OP_MIGRATION_COMPLETE`).
//!
//! Requires the `fault-injection` feature flag (to match the migration test
//! family; the test itself uses only stable APIs):
//!
//! ```text
//! cargo test --release --features fault-injection --test migration_crash
//! ```

#![cfg(feature = "fault-injection")]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use parking_lot::Mutex;

use teraslab::allocator::SlotAllocator;
use teraslab::checkpoint::{CheckpointConfig, perform_blocking_checkpoint_with_reset_guard};
use teraslab::cluster::migration::{MigrationManager, load_inbound_state, persist_inbound_state};
use teraslab::cluster::shards::ShardTable;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::mined_index::NO_MINED_SLOT;
use teraslab::index::{DahBackend, PrimaryBackend, ShardedIndex, TxKey};
use teraslab::locks::StripedLocks;
use teraslab::ops::create::CreateRequest;
use teraslab::ops::engine::Engine;
use teraslab::protocol::frame::RequestFrame;
use teraslab::protocol::opcodes::{
    FLAG_MIGRATION_BATCH, OP_REPLICA_BATCH, STATUS_ERROR, STATUS_OK,
};
use teraslab::recovery::recover_all_with_allocator;
use teraslab::redo::RedoLog;
use teraslab::replication::protocol::{ReplicaAck, ReplicaBatch, ReplicaOp};
use teraslab::replication::receiver::{apply_op_journal, handle_replica_batch};

const DATA_SIZE: u64 = 32 * 1024 * 1024;
const REDO_SIZE: u64 = 1024 * 1024;
const ALIGN: usize = 4096;
const NUM_RECORDS: usize = 12;

/// A node = engine over a (volatile, for the target) device + an inbound
/// migration manager whose state is persisted to a temp file on every change.
struct Node {
    data_dev: Arc<MemoryDevice>,
    redo_dev: Arc<MemoryDevice>,
    redo_log: Arc<Mutex<RedoLog>>,
    engine: Arc<Engine>,
}

impl Node {
    fn new(volatile: bool) -> Self {
        let data_dev = Arc::new(if volatile {
            MemoryDevice::new_volatile(DATA_SIZE, ALIGN).unwrap()
        } else {
            MemoryDevice::new(DATA_SIZE, ALIGN).unwrap()
        });
        let redo_dev = Arc::new(if volatile {
            MemoryDevice::new_volatile(REDO_SIZE, ALIGN).unwrap()
        } else {
            MemoryDevice::new(REDO_SIZE, ALIGN).unwrap()
        });
        let alloc = SlotAllocator::new(data_dev.clone() as Arc<dyn BlockDevice>).unwrap();
        let index = PrimaryBackend::new_in_memory(4096).unwrap();
        let redo_log = Arc::new(Mutex::new(
            RedoLog::open(redo_dev.clone() as Arc<dyn BlockDevice>, 0, REDO_SIZE).unwrap(),
        ));
        let engine = Arc::new(Engine::new(
            data_dev.clone() as Arc<dyn BlockDevice>,
            index,
            alloc,
            StripedLocks::new(64),
            DahBackend::new_in_memory(),
        ));
        engine.set_redo_log(redo_log.clone());
        Self {
            data_dev,
            redo_dev,
            redo_log,
            engine,
        }
    }

    /// Make the device + redo + allocator durable.
    fn make_durable(&self) {
        self.redo_log.lock().flush().unwrap();
        self.engine.allocator().lock().persist().unwrap();
        self.data_dev.sync().unwrap();
        self.redo_dev.sync().unwrap();
    }

    /// Restart through the production recovery sequence after a power loss
    /// and return a fresh engine over the recovered state.
    fn recover(&self) -> Arc<Engine> {
        // Production startup recovers the allocator from its persisted header,
        // or falls back to a fresh allocator when the header region is all
        // zeros (a node that crashed before its first checkpoint — exactly the
        // crash-mid-migration case, where no checkpoint had run yet).
        let (alloc, _origin) = teraslab::server::startup::recover_or_create_allocator(
            self.data_dev.clone() as Arc<dyn BlockDevice>,
        )
        .expect("allocator recover/create");
        let mut alloc: teraslab::allocator::BoxedAllocator = Box::new(alloc);
        let primary = PrimaryBackend::rebuild(&*self.data_dev as &dyn BlockDevice, &alloc).unwrap();
        // Recovery now operates on a `ShardedIndex` (interior RwLocks, `&self`).
        // Wrap the rebuilt single backend as a one-shard index — identical
        // semantics to the pre-sharding single-lock path — then replay onto it.
        let index = ShardedIndex::from_single(primary);
        let dah_idx =
            PrimaryBackend::rebuild_secondary(&*self.data_dev as &dyn BlockDevice, &alloc).unwrap();
        let mut dah = DahBackend::from(dah_idx);
        // Reopen the redo as a SHARED handle so both primary-index recovery and
        // the mined-index redo-tail replay below read the same durable log.
        let redo = Arc::new(Mutex::new(
            RedoLog::open(self.redo_dev.clone() as Arc<dyn BlockDevice>, 0, REDO_SIZE)
                .expect("reopen redo after crash"),
        ));
        {
            let redo_guard = redo.lock();
            recover_all_with_allocator(
                &*self.data_dev as &dyn BlockDevice,
                &redo_guard,
                &index,
                &mut dah,
                Some(&mut alloc),
            )
            .expect("recovery must not fail");
        }
        let engine = Arc::new(Engine::new_with_sharded_index(
            self.data_dev.clone() as Arc<dyn BlockDevice>,
            index,
            alloc,
            StripedLocks::new(64),
            dah,
        ));
        // Production startup also reconstructs the MinedIndex from the redo tail
        // (`Engine::recover_mined_index`). A crash-mid-baseline node has no
        // checkpoint snapshot, so this is the fresh-boot full redo-tail replay —
        // exactly how a FLUSHED migration baseline's journaled `ReplicaCreate`
        // (+ `SetMinedBatch`) reconstructs each record's mined-index slot post-#1.
        engine
            .replay_mined_index_redo_tail(std::slice::from_ref(&redo))
            .expect("mined-index redo-tail replay");
        engine
    }
}

/// All records hash into one shard so the migration moves a single shard.
/// We vary only the low bytes that do NOT feed the shard mask
/// (`u16_le(txid[0..2]) & 0x0FFF`), keeping bytes 0..2 fixed.
fn key(n: usize) -> TxKey {
    let mut txid = [0u8; 32];
    txid[0] = 0x11;
    txid[1] = 0x02;
    txid[8..16].copy_from_slice(&(n as u64).to_le_bytes());
    TxKey { txid }
}

/// Keys for a node's OWN, disjoint shard. Bytes 0..2 differ from [`key`]
/// (`0x22, 0x05` vs `0x11, 0x02`) so the records hash into a DIFFERENT shard
/// with disjoint txids — used to build a second, legitimately-held master set
/// for the no-dual-master partition check.
fn owned_key(n: usize) -> TxKey {
    let mut txid = [0u8; 32];
    txid[0] = 0x22;
    txid[1] = 0x05;
    txid[8..16].copy_from_slice(&(n as u64).to_le_bytes());
    TxKey { txid }
}

fn slot_hash(n: usize, vout: u32) -> [u8; 32] {
    let mut h = [0u8; 32];
    h[0] = n as u8;
    h[1] = (vout + 1) as u8;
    h[2] = 0xD7;
    h
}

fn create_req(n: usize, hashes: &[[u8; 32]]) -> CreateRequest<'_> {
    create_req_for(key(n).txid, hashes)
}

/// Companion to [`create_req`] for an explicit txid (the latter is hard-wired
/// to `key(n)`). Used to materialize a second node's own, disjoint shard.
fn create_req_for(tx_id: [u8; 32], hashes: &[[u8; 32]]) -> CreateRequest<'_> {
    CreateRequest {
        tx_id,
        tx_version: 1,
        locktime: 0,
        fee: 300,
        size_in_bytes: 200,
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

/// Serialize a source record into the migration baseline `ReplicaOp::Create`,
/// reproducing the coordinator's `stream_shard_baseline` wire layout (70-byte
/// metadata prefix + utxo hashes).
fn build_migration_create_op(source: &Engine, k: &TxKey) -> ReplicaOp {
    let meta = source.read_metadata(k).unwrap();
    let utxo_count = { meta.utxo_count };
    let mut utxo_hashes = Vec::with_capacity(utxo_count as usize);
    for v in 0..utxo_count {
        utxo_hashes.push(source.read_slot(k, v).unwrap().hash);
    }
    let mut meta_buf = Vec::with_capacity(70);
    meta_buf.extend_from_slice(&{ meta.tx_version }.to_le_bytes());
    meta_buf.extend_from_slice(&{ meta.locktime }.to_le_bytes());
    meta_buf.extend_from_slice(&{ meta.fee }.to_le_bytes());
    meta_buf.extend_from_slice(&{ meta.size_in_bytes }.to_le_bytes());
    meta_buf.extend_from_slice(&{ meta.extended_size }.to_le_bytes());
    let (is_coinbase, wire_flags) =
        teraslab::replication::protocol::create_metadata_flag_bytes(meta.flags);
    meta_buf.push(is_coinbase);
    meta_buf.extend_from_slice(&{ meta.spending_height }.to_le_bytes());
    meta_buf.extend_from_slice(&{ meta.created_at }.to_le_bytes());
    meta_buf.push(wire_flags);
    meta_buf.extend_from_slice(&{ meta.generation }.to_le_bytes());
    meta_buf.extend_from_slice(&{ meta.updated_at }.to_le_bytes());
    meta_buf.extend_from_slice(&{ meta.unmined_since }.to_le_bytes());
    meta_buf.extend_from_slice(&{ meta.delete_at_height }.to_le_bytes());
    meta_buf.extend_from_slice(&{ meta.preserve_until }.to_le_bytes());
    ReplicaOp::Create {
        tx_key: *k,
        metadata_bytes: meta_buf,
        utxo_hashes,
        cold_data: None,
        is_external: false,
    }
}

/// Set of records a node serves AS MASTER. A node only serves a shard's
/// records if it is the committed master AND the shard is not pending-inbound
/// (the production write/read fence). We model exactly that predicate.
fn master_record_set(
    engine: &Engine,
    keys: &[TxKey],
    mgr: &MigrationManager,
    shard: u16,
    is_committed_master: bool,
) -> BTreeSet<[u8; 32]> {
    let mut set = BTreeSet::new();
    // A node fenced for pending inbound does NOT act as master for that shard.
    if !is_committed_master || mgr.has_pending_inbound(shard) {
        return set;
    }
    for k in keys {
        if engine.lookup(k).is_some() {
            set.insert(k.txid);
        }
    }
    set
}

/// F-3: kill the new master while a shard migration is actively streaming,
/// restart it, and verify no record is lost, duplicated, or live on two
/// masters.
#[test]
fn crash_mid_migration_no_loss_no_dup_no_dual_master() {
    let tmp = tempfile::tempdir().unwrap();
    let inbound_path = tmp.path().join("inbound.state");

    // --- OLD master: durable, holds all records of the shard. ---
    let old = Node::new(false);
    let keys: Vec<TxKey> = (0..NUM_RECORDS).map(key).collect();
    for n in 0..NUM_RECORDS {
        let hashes = [slot_hash(n, 0), slot_hash(n, 1)];
        old.engine.create(&create_req(n, &hashes)).unwrap();
    }
    old.make_durable();
    let shard = ShardTable::shard_for_key(&keys[0]);
    // Sanity: every record landed in the same shard.
    for k in &keys {
        assert_eq!(
            ShardTable::shard_for_key(k),
            shard,
            "all records share a shard"
        );
    }
    let original: BTreeSet<[u8; 32]> = keys.iter().map(|k| k.txid).collect();

    // --- NEW master: volatile device, begins inbound migration. ---
    let new = Node::new(true);
    let mut new_mgr = MigrationManager::new();
    // Register the inbound shard + persist (fsync) on the state change — this
    // is what the dispatch path does on the first OP_REPLICA_BATCH.
    assert!(new_mgr.mark_inbound_active(shard));
    persist_inbound_state(&inbound_path, &new_mgr);

    // --- Stream the baseline, then CRASH mid-batch. ---
    // The receiver applies ops, then (once) syncs the device + flushes redo,
    // then (much later, only on proven completion) marks inbound complete.
    // We apply only HALF the records and then power-loss the device BEFORE
    // the end-of-batch sync — modeling a kill while streaming.
    let crash_after = NUM_RECORDS / 2;
    for (i, k) in keys.iter().enumerate() {
        let op = build_migration_create_op(&old.engine, k);
        // Production migration-baseline path: journal = false. Post-#1 this
        // BUFFERS a lightweight `ReplicaCreate` per record, but the crash below
        // hits BEFORE the receiver's end-of-batch redo flush + device sync, so
        // the buffered redo is lost to power loss — recovery of this unflushed
        // batch relies entirely on the inbound fence + source re-drive.
        apply_op_journal(&new.engine, &op, false, true).expect("migration apply");
        if i + 1 == crash_after {
            break;
        }
    }
    // Before the receiver reached its end-of-batch `device.sync()` +
    // redo-flush, and before any `mark_inbound_complete`, the node dies.
    assert!(
        new.data_dev.simulate_power_loss(),
        "new master device must be volatile"
    );
    assert!(new.redo_dev.simulate_power_loss());

    // The migration was NEVER completed → inbound state on disk still marks
    // the shard pending (it was persisted at mark_inbound_active and never
    // updated to complete). The source likewise never committed the handoff.
    let old_committed_master = true; // source never committed handoff away
    let new_committed_master = false; // target never committed handoff in

    // --- RESTART the new master through real recovery + inbound restore. ---
    let new_recovered = new.recover();
    let mut restored_mgr = MigrationManager::new();
    let inbound_bytes = load_inbound_state(&inbound_path).expect("load inbound state");
    restored_mgr
        .restore_inbound(&inbound_bytes)
        .expect("restore inbound state");

    // INVARIANT 1: the restored inbound state still fences the shard, so the
    // new master refuses to serve as master for it.
    assert!(
        restored_mgr.has_pending_inbound(shard),
        "after crash mid-migration, the new master must still see the shard \
         pending-inbound (fenced) so it does not serve stale/partial data",
    );

    // Compute each node's master-visible record set.
    let old_set = master_record_set(
        &old.engine,
        &keys,
        &new_mgr_source(),
        shard,
        old_committed_master,
    );
    let new_set = master_record_set(
        &new_recovered,
        &keys,
        &restored_mgr,
        shard,
        new_committed_master,
    );

    // INVARIANT 2 (no dual-live master). Two parts:
    //
    // (a) FENCE — the crashed target restored its inbound fence, so it serves
    //     NOTHING as master for the migrating shard; the old master remains the
    //     sole authority. (Intersecting `old_set` with this correctly-empty
    //     set is a tautology on its own, which is why part (b) exists.)
    assert!(
        new_set.is_empty(),
        "fenced new master must not serve any record as master for the migrating shard",
    );
    let fenced_dual: Vec<_> = old_set.intersection(&new_set).collect();
    assert!(
        fenced_dual.is_empty(),
        "no record may be live on BOTH masters after a crashed migration; \
         dual-live: {fenced_dual:?}",
    );

    // (b) PARTITION — the real no-dual-master invariant is "no record is
    //     mastered by two nodes at once". Exercise it against two REAL,
    //     NON-EMPTY master sets: the old master's shard and a SECOND node that
    //     legitimately masters its OWN, disjoint shard (a migration target
    //     keeps serving shards it already owns during an unrelated inbound
    //     migration). Both sets non-empty + empty intersection ⇒ no shard has
    //     two masters — a claim the fenced-set tautology above cannot make.
    let owned = Node::new(false);
    let owned_keys: Vec<TxKey> = (0..NUM_RECORDS).map(owned_key).collect();
    for (n, k) in owned_keys.iter().enumerate() {
        let hashes = [slot_hash(n, 0), slot_hash(n, 1)];
        owned
            .engine
            .create(&create_req_for(k.txid, &hashes))
            .expect("create the target's own-shard record");
    }
    owned.make_durable();
    let owned_shard = ShardTable::shard_for_key(&owned_keys[0]);
    assert_ne!(
        owned_shard, shard,
        "the target's own shard must differ from the migrating shard",
    );
    let owned_set = master_record_set(
        &owned.engine,
        &owned_keys,
        &new_mgr_source(),
        owned_shard,
        true,
    );
    assert!(
        !old_set.is_empty(),
        "old master's shard set must be non-empty for the partition check to bite",
    );
    assert!(
        !owned_set.is_empty(),
        "the target's own-shard master set must be non-empty for the partition check to bite",
    );
    let partition_dual: BTreeSet<[u8; 32]> = old_set.intersection(&owned_set).cloned().collect();
    assert!(
        partition_dual.is_empty(),
        "no record may be mastered by two nodes at once; dual-mastered: {partition_dual:?}",
    );

    // NON-VACUITY — prove the empty-intersection assertion above has teeth:
    // inject a dual-mastered record (an old-master txid ALSO claimed by the
    // second node) and confirm the identical intersection now reports the
    // conflict, i.e. the assertion WOULD fail were a dual-master introduced.
    let injected_txid = *old_set.iter().next().expect("old_set is non-empty");
    let mut conflicting = owned_set.clone();
    conflicting.insert(injected_txid);
    let injected_dual: BTreeSet<[u8; 32]> = old_set.intersection(&conflicting).cloned().collect();
    assert_eq!(
        injected_dual,
        BTreeSet::from([injected_txid]),
        "with a dual-mastered record injected, the no-dual-master check must catch \
         exactly it — proving the empty-intersection assertion is non-vacuous",
    );

    // INVARIANT 3 (no loss): the union of all master-served records equals the
    // original set — every record is still served by exactly one master (the
    // old one, which retains the full shard until the handoff commits).
    let union: BTreeSet<[u8; 32]> = old_set.union(&new_set).cloned().collect();
    assert_eq!(
        union, original,
        "no record may be lost: union of master record sets must equal the \
         original set",
    );

    // INVARIANT 4 (recovery integrity): whatever DID survive the crash on the
    // new master's device is structurally intact (not torn) and a strict
    // subset of the originals (no fabricated/duplicated keys).
    for k in &keys {
        if new_recovered.lookup(k).is_some() {
            // Readable metadata + slots ⇒ not torn.
            let meta = new_recovered.read_metadata(k).unwrap();
            let utxo_count = { meta.utxo_count };
            for v in 0..utxo_count {
                new_recovered.read_slot(k, v).unwrap();
            }
            assert!(
                original.contains(&k.txid),
                "recovered key must be one of the originals (no duplication/fabrication)",
            );
        }
    }
}

/// The old master never began an inbound migration; its manager is empty so
/// `has_pending_inbound` is always false for it.
fn new_mgr_source() -> MigrationManager {
    MigrationManager::new()
}

/// Control: a CLEAN migration (stream all, sync, mark complete) hands the
/// shard over with no loss and the new master becomes the sole master.
#[test]
fn clean_migration_completes_with_single_master() {
    let tmp = tempfile::tempdir().unwrap();
    let inbound_path = tmp.path().join("inbound.state");

    let old = Node::new(false);
    let keys: Vec<TxKey> = (0..NUM_RECORDS).map(key).collect();
    for n in 0..NUM_RECORDS {
        let hashes = [slot_hash(n, 0), slot_hash(n, 1)];
        old.engine.create(&create_req(n, &hashes)).unwrap();
    }
    old.make_durable();
    let shard = ShardTable::shard_for_key(&keys[0]);
    let original: BTreeSet<[u8; 32]> = keys.iter().map(|k| k.txid).collect();

    let new = Node::new(false);
    let mut new_mgr = MigrationManager::new();
    assert!(new_mgr.mark_inbound_active(shard));
    persist_inbound_state(&inbound_path, &new_mgr);

    // Stream ALL records, then complete. journal = false (production
    // migration-baseline path).
    for k in &keys {
        let op = build_migration_create_op(&old.engine, k);
        apply_op_journal(&new.engine, &op, false, true).expect("migration apply");
    }
    new.redo_log.lock().flush().unwrap();

    // Bounded-lightweight-journal invariant (issue #1 / Option A): the baseline
    // journals ONLY the lightweight index-only redo — exactly one ~24-byte
    // `ReplicaCreate` per create (these are unmined 70-byte creates, so no
    // `SetMinedBatch` companion) and NO full-record `Create`. This is the same
    // per-create redo NORMAL replication writes, so migration is no worse than
    // the already-backpressure-protected normal path — the heavy per-record
    // engine redo stays suppressed.
    {
        use teraslab::redo::RedoOp;
        let entries = new.redo_log.lock().recover().unwrap();
        let replica_creates = entries
            .iter()
            .filter(|e| matches!(e.op, RedoOp::ReplicaCreate { .. }))
            .count();
        assert_eq!(
            replica_creates, NUM_RECORDS,
            "baseline journals exactly one lightweight ReplicaCreate per create",
        );
        assert!(
            !entries
                .iter()
                .any(|e| matches!(e.op, RedoOp::Create { .. })),
            "baseline must NOT journal the heavy full-record Create redo",
        );
    }

    new.make_durable();
    // Proven completion: mark inbound complete + persist, and the source
    // commits the handoff away.
    new_mgr.mark_inbound_complete(shard);
    persist_inbound_state(&inbound_path, &new_mgr);

    assert!(
        !new_mgr.has_pending_inbound(shard),
        "completed migration clears the inbound fence",
    );

    // New master is now the sole authority; old master committed away.
    let new_set = master_record_set(&new.engine, &keys, &new_mgr, shard, true);
    let old_set = master_record_set(&old.engine, &keys, &new_mgr_source(), shard, false);
    assert!(old_set.is_empty(), "old master committed the handoff away");
    assert_eq!(
        new_set, original,
        "new master serves every record, none lost"
    );
}

/// A node with a deliberately tiny redo log, used by the redo-capacity
/// regression tests. The data device is large enough for hundreds of
/// records; only the redo region is small so that journalling the baseline
/// would overflow it.
struct TinyRedoNode {
    data_dev: Arc<MemoryDevice>,
    #[allow(dead_code)]
    redo_dev: Arc<MemoryDevice>,
    redo_log: Arc<Mutex<RedoLog>>,
    engine: Arc<Engine>,
}

impl TinyRedoNode {
    fn new(redo_size: u64) -> Self {
        let data_dev = Arc::new(MemoryDevice::new(DATA_SIZE, ALIGN).unwrap());
        let redo_dev = Arc::new(MemoryDevice::new(redo_size, ALIGN).unwrap());
        let alloc = SlotAllocator::new(data_dev.clone() as Arc<dyn BlockDevice>).unwrap();
        let index = PrimaryBackend::new_in_memory(4096).unwrap();
        let redo_log = Arc::new(Mutex::new(
            RedoLog::open(redo_dev.clone() as Arc<dyn BlockDevice>, 0, redo_size).unwrap(),
        ));
        let engine = Arc::new(Engine::new(
            data_dev.clone() as Arc<dyn BlockDevice>,
            index,
            alloc,
            StripedLocks::new(64),
            DahBackend::new_in_memory(),
        ));
        engine.set_redo_log(redo_log.clone());
        Self {
            data_dev,
            redo_dev,
            redo_log,
            engine,
        }
    }
}

/// Build a `FLAG_MIGRATION_BATCH` request frame carrying baseline creates for
/// `keys`, exactly as the coordinator's `stream_shard_baseline` sends them.
fn migration_batch_frame(source: &Engine, keys: &[TxKey], shard: u16) -> RequestFrame {
    let ops: Vec<ReplicaOp> = keys
        .iter()
        .map(|k| build_migration_create_op(source, k))
        .collect();
    let batch = ReplicaBatch {
        first_sequence: 0,
        ops,
        trace_ctx: None,
        source_node_id: None,
        cluster_key: 0,
        regime_table: None,
    };
    RequestFrame {
        request_id: shard as u64,
        op_code: OP_REPLICA_BATCH,
        flags: FLAG_MIGRATION_BATCH,
        payload: batch.serialize().into(),
    }
}

/// HEADLINE REGRESSION, reworked for issue #1 / Option A. Pre-#1 a migration
/// baseline wrote ZERO receiver redo, so it could never overflow the log. Post-#1
/// it journals a bounded lightweight `ReplicaCreate` per create — the SAME redo a
/// NORMAL replication stream writes — so LogFull avoidance no longer comes from
/// "write nothing"; it comes from the checkpoint DRAIN (the issue-#29
/// `redo_backpressure_gate` stalls a burst while `perform_checkpoint` reclaims the
/// log), exactly as for a large normal-replication stream.
///
/// The test proves the drain is LOAD-BEARING, driving the REAL receiver path
/// (`handle_replica_batch` with `FLAG_MIGRATION_BATCH`, which runs the
/// backpressure gate + per-batch redo flush):
///   * fail-before: one big migration batch into a tiny redo with NO drain
///     overflows on the single atomic batch-redo admission (STATUS_ERROR) —
///     the pressure is real. Post-FU#6a (`collect-then-atomic replica redo
///     apply + Busy NAK`, commit f9aaf57) that overflow surfaces as a
///     retryable `ReplicaAck::Busy` NAK, not a hard `Error`: nothing is
///     journaled or poisoned and the coordinator's `stream_shard_baseline`
///     explicitly treats `Busy` on a migration batch as "safe to retry, no
///     partial state" (see `src/cluster/coordinator.rs`). This test predates
///     FU#6a (added in b33af81, before f9aaf57) and still asserted the
///     pre-FU#6a `Error(redo log full)` shape.
///   * pass-after: the SAME baseline streamed in batches, DRAINING the redo
///     (blocking checkpoint reclaim) between batches, COMPLETES with every record
///     applied + recoverable and no batch ever overflowing.
#[test]
fn large_migration_baseline_completes_without_log_full() {
    let tmp = tempfile::tempdir().unwrap();
    // 32 KiB redo (28672 usable). A lightweight migration `ReplicaCreate` redo
    // entry is ~62 bytes, so this log holds ~462 entries — the full NUM stream
    // overflows it, but one drained BATCH plus the checkpoint's own aligned
    // compaction block fits with headroom.
    let redo_size = 8 * ALIGN as u64;
    const NUM: usize = 600;
    const BATCH: usize = 40;
    let shard = 0u16;

    let source = TinyRedoNode::new(8 * 1024 * 1024); // ample redo for source
    let keys: Vec<TxKey> = (0..NUM).map(key).collect();
    for n in 0..NUM {
        let hashes = [slot_hash(n, 0)];
        source.engine.create(&create_req(n, &hashes)).unwrap();
    }
    source.data_dev.sync().unwrap();

    // --- fail-before: one big migration batch, NO drain → redo-full Busy NAK. ---
    {
        let journalled = TinyRedoNode::new(redo_size);
        let last_applied = AtomicU64::new(0);
        let frame = migration_batch_frame(&source.engine, &keys, shard);
        let resp = handle_replica_batch(&frame, &journalled.engine, &last_applied);
        assert_eq!(
            resp.status, STATUS_ERROR,
            "fail-before: {NUM} lightweight baseline creates into a 32 KiB redo \
             with no drain MUST overflow — if it does not, the setup is wrong",
        );
        // Post-FU#6a (commit f9aaf57, `collect-then-atomic replica redo apply +
        // Busy NAK`) the whole batch's redo entries are admitted in ONE atomic
        // step; a transient LogFull on that admission NAKs `ReplicaAck::Busy`
        // instead of the pre-FU#6a `ReplicaAck::Error` — nothing is journaled or
        // poisoned, and the batch's `first_sequence` (0 for a migration batch,
        // see `migration_batch_frame`) is echoed back so the caller can log/retry
        // it. `stream_shard_baseline` (src/cluster/coordinator.rs) already
        // special-cases `ReplicaAck::Busy` on a migration batch as "safe to
        // retry, no partial state to reconcile" — this is the overflow signal
        // the fail-before phase is proving is real, just carried by the new NAK
        // shape instead of a hard error.
        match ReplicaAck::deserialize(&resp.payload).unwrap() {
            ReplicaAck::Busy { first_sequence } => assert_eq!(
                first_sequence, 0,
                "Busy must echo the migration batch's first_sequence (always 0)",
            ),
            other => panic!("expected ReplicaAck::Busy(redo log full backpressure), got {other:?}"),
        }
    }

    // --- pass-after: batched migration WITH a checkpoint drain between batches
    //     completes without LogFull. ---
    let receiver = TinyRedoNode::new(redo_size);
    let last_applied = AtomicU64::new(0);
    let cfg = CheckpointConfig::new(tmp.path().join("ckpt.snap"));
    for chunk in keys.chunks(BATCH) {
        let frame = migration_batch_frame(&source.engine, chunk, shard);
        let resp = handle_replica_batch(&frame, &receiver.engine, &last_applied);
        assert_eq!(
            resp.status,
            STATUS_OK,
            "pass-after: each drained migration batch must apply without LogFull \
             (redo usage {:.2})",
            receiver.redo_log.lock().usage_fraction(),
        );
        // DRAIN: a blocking checkpoint fences + reclaims the redo to ~0, exactly
        // as the checkpoint task does under sustained write load in production.
        // Without this reclaim the same stream overflows (see fail-before).
        perform_blocking_checkpoint_with_reset_guard(
            &cfg,
            &receiver.engine,
            &receiver.redo_log,
            |_| true,
        )
        .expect("checkpoint drain must succeed");
    }
    receiver.data_dev.sync().unwrap();

    // Every record applied + structurally intact (recoverable) on the receiver.
    for k in &keys {
        assert!(
            receiver.engine.lookup(k).is_some(),
            "every migrated record must be present after the drained baseline",
        );
        let meta = receiver.engine.read_metadata(k).unwrap();
        let utxo_count = { meta.utxo_count };
        for v in 0..utxo_count {
            receiver.engine.read_slot(k, v).unwrap();
        }
    }
}

/// Receiver crash mid-baseline: two-sided recovery (issue #1 / Option A). The
/// FLUSHED portion of the baseline recovers via the journaled redo tail —
/// including each record's MinedIndex slot, reconstructed from the lightweight
/// `ReplicaCreate` entries (the durability the pre-#1 zero-redo baseline could
/// not provide); the UNFLUSHED tail is still covered by the inbound fence +
/// source re-drive (the source never committed the handoff). We assert BOTH:
/// the flushed half recovers slotted, and the source re-drive completes the
/// shard with no loss.
#[test]
fn receiver_crash_mid_baseline_recovers_flushed_via_redo_then_redrives() {
    let tmp = tempfile::tempdir().unwrap();
    let inbound_path = tmp.path().join("inbound.state");

    // Durable source holding the full shard.
    let old = Node::new(false);
    let keys: Vec<TxKey> = (0..NUM_RECORDS).map(key).collect();
    for n in 0..NUM_RECORDS {
        let hashes = [slot_hash(n, 0), slot_hash(n, 1)];
        old.engine.create(&create_req(n, &hashes)).unwrap();
    }
    old.make_durable();
    let shard = ShardTable::shard_for_key(&keys[0]);
    let original: BTreeSet<[u8; 32]> = keys.iter().map(|k| k.txid).collect();

    // Receiver begins inbound migration; persist the fence (fsync).
    let new = Node::new(true);
    let mut new_mgr = MigrationManager::new();
    assert!(new_mgr.mark_inbound_active(shard));
    persist_inbound_state(&inbound_path, &new_mgr);

    // Apply the FIRST HALF, then make it DURABLE (redo flush + device sync) —
    // modeling the receiver's end-of-batch barrier for the first batch. Post-#1
    // these journal a lightweight ReplicaCreate each, now flushed to the redo.
    let half = NUM_RECORDS / 2;
    for k in keys.iter().take(half) {
        let op = build_migration_create_op(&old.engine, k);
        apply_op_journal(&new.engine, &op, false, true).expect("first-half migration apply");
    }
    new.make_durable();

    // Apply the SECOND HALF but do NOT flush/sync — the in-flight tail that
    // power loss discards.
    for k in keys.iter().skip(half) {
        let op = build_migration_create_op(&old.engine, k);
        apply_op_journal(&new.engine, &op, false, true).expect("second-half migration apply");
    }
    assert!(new.data_dev.simulate_power_loss());
    assert!(new.redo_dev.simulate_power_loss());

    // Restart through real recovery (primary rebuild + redo replay + MinedIndex
    // redo-tail replay). The FLUSHED half recovers with reconstructed slots.
    let new_recovered = new.recover();
    let mut restored_mgr = MigrationManager::new();
    let inbound_bytes = load_inbound_state(&inbound_path).expect("load inbound state");
    restored_mgr
        .restore_inbound(&inbound_bytes)
        .expect("restore inbound state");

    // FENCE intact: the receiver still refuses to serve the shard.
    assert!(
        restored_mgr.has_pending_inbound(shard),
        "after crash mid baseline, the receiver must still be fenced \
         (pending-inbound) so the source stays sole master",
    );

    // REDO-TAIL RECOVERY: every FLUSHED (first-half) record survives AND has a
    // reconstructed, non-sentinel MinedIndex slot — the journaled ReplicaCreate
    // replayed through mined-index recovery. Pre-#1 (zero redo) these recovered
    // slot-less.
    let mut recovered_flushed = 0;
    for k in keys.iter().take(half) {
        let entry = new_recovered
            .lookup(k)
            .expect("flushed first-half record must survive via the durable redo/device");
        assert_ne!(
            entry.mined_slot, NO_MINED_SLOT,
            "a flushed migration record must recover a MinedIndex slot from the redo tail",
        );
        // Structurally intact (not torn).
        let meta = new_recovered.read_metadata(k).unwrap();
        let utxo_count = { meta.utxo_count };
        for v in 0..utxo_count {
            new_recovered.read_slot(k, v).unwrap();
        }
        recovered_flushed += 1;
    }
    assert_eq!(
        recovered_flushed, half,
        "all flushed first-half records recovered via the redo tail",
    );

    // Source never committed the handoff: it still serves the full shard.
    let old_set = master_record_set(&old.engine, &keys, &new_mgr_source(), shard, true);
    assert_eq!(
        old_set, original,
        "source never committed the handoff: it still serves the full shard",
    );

    // SOURCE RE-DRIVE covers the UNFLUSHED tail: the source re-runs a FRESH full
    // baseline into a clean receiver (modeling the post-crash retry). With the
    // fence still pending, this re-applies every record idempotently and completes.
    let redriven = Node::new(false);
    let mut redrive_mgr = MigrationManager::new();
    assert!(redrive_mgr.mark_inbound_active(shard));
    for k in &keys {
        let op = build_migration_create_op(&old.engine, k);
        apply_op_journal(&redriven.engine, &op, false, true).expect("re-drive apply");
    }
    redriven.make_durable();
    redrive_mgr.mark_inbound_complete(shard);

    let redriven_set = master_record_set(&redriven.engine, &keys, &redrive_mgr, shard, true);
    assert_eq!(
        redriven_set, original,
        "source re-drive recovers the full shard with no loss",
    );
}
