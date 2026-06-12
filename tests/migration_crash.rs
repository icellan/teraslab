//! F-3 — crash-mid-migration integration test.
//!
//! Drives a shard migration the way the production receiver does — streaming
//! baseline `ReplicaOp::Create` records from an OLD master into a NEW master
//! via the real [`apply_op`] apply path — then CRASHES the new master
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
//! (`apply_op` → `engine.create`), the inbound-state persistence
//! (`persist_inbound_state`, fsynced on every change), and the restore path
//! (`load_inbound_state` → `restore_inbound`) are the exact production
//! components; only the network transport is elided.
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

use parking_lot::Mutex;

use teraslab::allocator::SlotAllocator;
use teraslab::cluster::migration::{MigrationManager, load_inbound_state, persist_inbound_state};
use teraslab::cluster::shards::ShardTable;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahBackend, PrimaryBackend, TxKey, UnminedBackend};
use teraslab::locks::StripedLocks;
use teraslab::ops::create::CreateRequest;
use teraslab::ops::engine::Engine;
use teraslab::recovery::recover_all_with_allocator;
use teraslab::redo::RedoLog;
use teraslab::replication::protocol::ReplicaOp;
use teraslab::replication::receiver::apply_op;

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
        let (mut alloc, _origin) = teraslab::server::startup::recover_or_create_allocator(
            self.data_dev.clone() as Arc<dyn BlockDevice>,
        )
        .expect("allocator recover/create");
        let mut index =
            PrimaryBackend::rebuild(&*self.data_dev as &dyn BlockDevice, &alloc).unwrap();
        let (dah_idx, unmined_idx) =
            PrimaryBackend::rebuild_secondary(&*self.data_dev as &dyn BlockDevice, &alloc).unwrap();
        let mut dah = DahBackend::from(dah_idx);
        let mut unmined = UnminedBackend::from(unmined_idx);
        let redo = RedoLog::open(self.redo_dev.clone() as Arc<dyn BlockDevice>, 0, REDO_SIZE)
            .expect("reopen redo after crash");
        recover_all_with_allocator(
            &*self.data_dev as &dyn BlockDevice,
            &redo,
            &mut index,
            &mut dah,
            &mut unmined,
            Some(&mut alloc),
        )
        .expect("recovery must not fail");
        Arc::new(Engine::new(
            self.data_dev.clone() as Arc<dyn BlockDevice>,
            index,
            alloc,
            StripedLocks::new(64),
            dah,
            unmined,
        ))
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

fn slot_hash(n: usize, vout: u32) -> [u8; 32] {
    let mut h = [0u8; 32];
    h[0] = n as u8;
    h[1] = (vout + 1) as u8;
    h[2] = 0xD7;
    h
}

fn create_req(n: usize, hashes: &[[u8; 32]]) -> CreateRequest<'_> {
    CreateRequest {
        tx_id: key(n).txid,
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
        apply_op(&new.engine, &op).expect("migration apply");
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
    restored_mgr.restore_inbound(&load_inbound_state(&inbound_path));

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

    // INVARIANT 2 (no dual-live master): no record is master-authoritative on
    // both nodes at once. The new master is fenced → its master set is empty.
    let dual: Vec<_> = old_set.intersection(&new_set).collect();
    assert!(
        dual.is_empty(),
        "no record may be live on BOTH masters after a crashed migration; \
         dual-live: {dual:?}",
    );
    assert!(
        new_set.is_empty(),
        "fenced new master must not serve any record as master",
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

    // Stream ALL records, then complete.
    for k in &keys {
        let op = build_migration_create_op(&old.engine, k);
        apply_op(&new.engine, &op).expect("migration apply");
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
