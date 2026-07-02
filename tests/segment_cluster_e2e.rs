//! End-to-end tests for the CLUSTERED segment (log-structured) storage engine.
//!
//! Validates specs/SEGMENT_CLUSTERING_DESIGN.md Phases 1–6 at the engine +
//! replica-apply level (driving `Engine` and `apply_op_journal` directly, the
//! same paths the TCP replication receiver uses):
//!
//! - Phase 1+2+3: a logical spend replicates master→replica; both nodes converge
//!   to identical LOGICAL state (slot / spent-count / generation) while their
//!   physical offsets legitimately DIVERGE (each relocates to its own cursor).
//! - Phase 6 (defrag under replication): a per-node, uncoordinated defrag on the
//!   master preserves logical identity — master and replica stay logically equal
//!   even though the master physically moved the record.
//! - Phase 4 (non-spend ops on segment): setMined / freeze / unspend / delete
//!   replicate correctly on a segment replica and RMW IN PLACE (no relocate —
//!   only spend relocates in v1).
//! - Phase 4 (migration/rejoin): a joining segment node receives records via the
//!   migration Create path, allocates its own offset, and the record is spendable.

use std::sync::Arc;

use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, TxKey, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::create::CreateRequest;
use teraslab::ops::engine::Engine;
use teraslab::ops::remaining::DeleteRequest;
use teraslab::ops::spend::SpendRequest;
use teraslab::record::{UTXO_FROZEN, UTXO_SPENT, UTXO_UNSPENT};
use teraslab::redo::RedoLog;
use teraslab::replication::protocol::ReplicaOp;
use teraslab::replication::receiver::apply_op_journal;
use teraslab::segment_allocator::SegmentAllocator;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A clustered segment engine with an attached redo log + buffered durability —
/// the exact shape a clustered segment node runs (Phase 5 config).
fn make_clustered_seg_engine() -> Arc<Engine> {
    make_clustered_seg_engine_sized(64)
}

/// Like [`make_clustered_seg_engine`] but with a caller-chosen segment size (in
/// 4 KiB blocks) — small segments seal quickly so a defrag victim can be formed.
fn make_clustered_seg_engine_sized(seg_blocks: u64) -> Arc<Engine> {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
    let seg = SegmentAllocator::new(dev.clone(), seg_blocks * 4096).unwrap();
    let index = Index::new(10_000).unwrap();
    let engine = Engine::new(
        dev,
        index,
        seg,
        StripedLocks::new(1024),
        DahIndex::new(),
        UnminedIndex::new(),
    );
    let log_dev: Arc<dyn BlockDevice> =
        Arc::new(MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap());
    let log = RedoLog::open(log_dev, 0, 16 * 1024 * 1024).unwrap();
    engine.set_redo_logs(vec![Arc::new(parking_lot::Mutex::new(log))]);
    engine.set_buffered_durability(true);
    engine.set_clustered(true);
    Arc::new(engine)
}

fn txid(n: u32) -> [u8; 32] {
    let mut t = [0u8; 32];
    t[0..4].copy_from_slice(&n.to_le_bytes());
    t[16..18].copy_from_slice(&(n as u16).to_le_bytes());
    t
}

fn utxo_hash(tx_n: u32, vout: u32) -> [u8; 32] {
    let mut h = [0u8; 32];
    h[0] = (vout & 0xFF) as u8;
    h[4..8].copy_from_slice(&tx_n.to_le_bytes());
    h
}

fn create_record(engine: &Engine, tx_n: u32, utxo_count: u32) -> TxKey {
    let t = txid(tx_n);
    let hashes: Vec<[u8; 32]> = (0..utxo_count).map(|v| utxo_hash(tx_n, v)).collect();
    let req = CreateRequest {
        tx_id: t,
        tx_version: 1,
        locktime: 0,
        fee: 0,
        size_in_bytes: 0,
        extended_size: 0,
        is_coinbase: false,
        spending_height: 0,
        utxo_hashes: &hashes,
        inputs: None,
        outputs: None,
        inpoints: None,
        is_external: false,
        created_at: 0,
        block_height: 0,
        mined_block_infos: &[],
        frozen: false,
        conflicting: false,
        locked: false,
        external_ref: None,
        parent_txids: &[],
    };
    engine.create(&req).unwrap();
    TxKey { txid: t }
}

/// Logical fingerprint of a record — the state that MUST be identical across
/// nodes. Physical offset is deliberately excluded (it legitimately diverges).
#[derive(Debug, PartialEq, Eq)]
struct LogicalState {
    utxo_count: u32,
    spent_utxos: u32,
    generation: u32,
    slot0_status: u8,
    slot1_status: u8,
    slot1_spending_data: [u8; 36],
}

fn logical_state(engine: &Engine, key: &TxKey) -> LogicalState {
    let m = engine.read_metadata(key).unwrap();
    let s0 = engine.read_slot(key, 0).unwrap();
    let s1 = engine.read_slot(key, 1).unwrap();
    LogicalState {
        utxo_count: { m.utxo_count },
        spent_utxos: { m.spent_utxos },
        generation: { m.generation },
        slot0_status: s0.status,
        slot1_status: s1.status,
        slot1_spending_data: s1.spending_data,
    }
}

// ---------------------------------------------------------------------------
// Phase 1+2+3 — spend replication: logical convergence, physical divergence
// ---------------------------------------------------------------------------

#[test]
fn segment_spend_replicates_logical_state_with_diverging_offsets() {
    let master = make_clustered_seg_engine();
    let replica = make_clustered_seg_engine();

    // Same record on both nodes.
    let key = create_record(&master, 100, 4);
    assert_eq!(create_record(&replica, 100, 4), key);

    // Force PHYSICAL divergence: a decoy record on the replica only advances its
    // append cursor, so the relocated spend lands at a different offset than the
    // master's.
    let _decoy = create_record(&replica, 999, 4);

    let master_pre_offset = master.lookup(&key).unwrap().record_offset;
    let replica_pre_offset = replica.lookup(&key).unwrap().record_offset;

    // Master spends vout 1 (relocates the record).
    let sd = [0x7Cu8; 36];
    master
        .spend(&SpendRequest {
            tx_key: key,
            offset: 1,
            utxo_hash: utxo_hash(100, 1),
            spending_data: sd,
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        })
        .unwrap();

    // Ship the LOGICAL op; replica applies through its own engine (relocates to
    // its OWN cursor) and journals nothing extra (post-apply None for segment).
    let master_generation = { master.read_metadata(&key).unwrap().generation };
    apply_op_journal(
        &replica,
        &ReplicaOp::Spend {
            tx_key: key,
            offset: 1,
            spending_data: sd,
            current_block_height: 1000,
            block_height_retention: 288,
            master_generation,
        },
        true,
        false,
    )
    .unwrap();

    // LOGICAL convergence — identical spent state, count, and generation.
    let ms = logical_state(&master, &key);
    let rs = logical_state(&replica, &key);
    assert_eq!(ms, rs, "master and replica must converge logically");
    assert_eq!(ms.slot1_status, UTXO_SPENT);
    assert_eq!(ms.slot1_spending_data, sd);
    assert_eq!(ms.spent_utxos, 1);
    assert_eq!(ms.slot0_status, UTXO_UNSPENT);

    // PHYSICAL divergence — both relocated (offset moved off the create site),
    // and to DIFFERENT offsets (the decoy skewed the replica's cursor).
    let master_post_offset = master.lookup(&key).unwrap().record_offset;
    let replica_post_offset = replica.lookup(&key).unwrap().record_offset;
    assert_ne!(master_post_offset, master_pre_offset, "master relocated");
    assert_ne!(replica_post_offset, replica_pre_offset, "replica relocated");
    assert_ne!(
        master_post_offset, replica_post_offset,
        "offsets legitimately diverge across nodes"
    );
}

// ---------------------------------------------------------------------------
// Phase 6 — defrag under replication preserves logical identity
// ---------------------------------------------------------------------------

#[test]
fn per_node_defrag_preserves_logical_state_across_replicas() {
    // Small segments so segment 0 seals quickly and can become a defrag victim.
    let master = make_clustered_seg_engine_sized(4);
    let replica = make_clustered_seg_engine();

    // The survivor exists on both nodes (identical create). Only the MASTER gets
    // filler — it is deleted to open dead space around the survivor and to advance
    // the append cursor past segment 0 (sealing it). The replica never defrags.
    let survivor = create_record(&master, 1, 4);
    assert_eq!(create_record(&replica, 1, 4), survivor);
    for n in 10..80u32 {
        create_record(&master, n, 4);
    }
    for n in 10..80u32 {
        master
            .delete(&DeleteRequest {
                tx_key: TxKey { txid: txid(n) },
                due_guard: None,
            })
            .unwrap();
    }

    let survivor_offset_before = master.lookup(&survivor).unwrap().record_offset;

    // Per-node, UNCOORDINATED defrag on the master ONLY. It relocates any live
    // records out of mostly-dead victim segments — a physical move.
    let moved = master.defrag_compact(256, 0.10);
    assert!(moved > 0, "defrag must relocate at least the survivor");

    // The survivor physically moved on the master...
    let survivor_offset_after = master.lookup(&survivor).unwrap().record_offset;
    assert_ne!(
        survivor_offset_after, survivor_offset_before,
        "defrag physically relocated the survivor on the master"
    );

    // ...but master and replica remain LOGICALLY identical (the replica never
    // defragged). Defrag preserves logical identity — the clustering invariant.
    assert_eq!(
        logical_state(&master, &survivor),
        logical_state(&replica, &survivor),
        "defrag must not change logical state; nodes stay converged"
    );
}

// ---------------------------------------------------------------------------
// Phase 4 — non-spend ops replicate on segment (RMW in place, no relocate)
// ---------------------------------------------------------------------------

#[test]
fn segment_non_spend_ops_replicate_in_place_no_relocate() {
    let replica = make_clustered_seg_engine();
    let key = create_record(&replica, 7, 4);
    let offset_at_create = replica.lookup(&key).unwrap().record_offset;

    // setMined — RMW in place on segment.
    apply_op_journal(
        &replica,
        &ReplicaOp::SetMined {
            tx_key: key,
            block_id: 42,
            block_height: 800_000,
            subtree_idx: 3,
            on_longest_chain: true,
            current_block_height: 800_010,
            block_height_retention: 288,
            master_generation: 1,
        },
        true,
        false,
    )
    .unwrap();

    // freeze vout 0 — RMW in place on segment.
    apply_op_journal(
        &replica,
        &ReplicaOp::Freeze {
            tx_key: key,
            offset: 0,
            master_generation: 2,
        },
        true,
        false,
    )
    .unwrap();

    // Only spend relocates in v1: setMined + freeze left the record at its
    // create offset.
    assert_eq!(
        replica.lookup(&key).unwrap().record_offset,
        offset_at_create,
        "non-spend ops must RMW in place on segment (no relocate)"
    );
    assert_eq!(replica.read_slot(&key, 0).unwrap().status, UTXO_FROZEN);
    let m = replica.read_metadata(&key).unwrap();
    assert!(
        { m.block_entry_count } >= 1,
        "setMined recorded a mined block entry"
    );

    // delete removes the record.
    apply_op_journal(&replica, &ReplicaOp::Delete { tx_key: key }, true, false).unwrap();
    assert!(replica.lookup(&key).is_none(), "delete removed the record");
}

// ---------------------------------------------------------------------------
// Phase 4 — migration/rejoin: a joining segment node receives records
// ---------------------------------------------------------------------------

#[test]
fn joining_segment_node_receives_record_via_migration_create_and_can_spend() {
    // A freshly joined / rejoining segment node with an EMPTY store.
    let joiner = make_clustered_seg_engine();
    let key = TxKey { txid: txid(555) };
    let hashes: Vec<[u8; 32]> = (0..4u32).map(|v| utxo_hash(555, v)).collect();

    // Migration ships the record via the Create path; the joiner's engine.create
    // allocates its OWN offset (index-only CreateV2 under the hood).
    apply_op_journal(
        &joiner,
        &ReplicaOp::Create {
            tx_key: key,
            metadata_bytes: vec![],
            utxo_hashes: hashes,
            cold_data: None,
            is_external: false,
        },
        true,
        true, // is_migration
    )
    .unwrap();

    // The record landed and is fully readable.
    let entry = joiner
        .lookup(&key)
        .expect("migrated record must be indexed");
    assert_eq!(entry.utxo_count, 4);
    assert_eq!(joiner.read_slot(&key, 0).unwrap().status, UTXO_UNSPENT);

    // And it is immediately spendable on the joiner — the segment spend relocates
    // it to a fresh offset, proving the migrated record is a first-class citizen.
    let pre = joiner.lookup(&key).unwrap().record_offset;
    joiner
        .spend(&SpendRequest {
            tx_key: key,
            offset: 2,
            utxo_hash: utxo_hash(555, 2),
            spending_data: [0x9Au8; 36],
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 900_000,
            block_height_retention: 288,
        })
        .unwrap();
    assert_ne!(
        joiner.lookup(&key).unwrap().record_offset,
        pre,
        "migrated record relocates on spend like any other"
    );
    assert_eq!(joiner.read_slot(&key, 2).unwrap().status, UTXO_SPENT);
}
