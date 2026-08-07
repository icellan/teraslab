//! E-4 — inter-node TCP frame replay defense (documented + audited path).
//!
//! The TCP frame auth layer (`cluster::auth::{sign_frame, verify_frame}`)
//! has an HMAC tag + a 5-minute clock-skew window but NO per-connection
//! nonce / monotonic sequence number under the HMAC. A captured valid
//! frame therefore re-verifies for the whole skew window — i.e. the auth
//! layer ACCEPTS a verbatim replay.
//!
//! Per the E-4 decision, replay defense for the TCP path is delegated to
//! per-opcode idempotency (documented in `src/cluster/auth.rs`). This
//! test exercises the representative mutating opcode `OP_REPLICA_BATCH`
//! end-to-end and proves both halves of that decision:
//!
//! 1. The auth layer DOES accept the verbatim replayed signed frame
//!    (`verify_frame` returns Ok on the identical bytes a second time) —
//!    this pins the gap the documentation describes.
//! 2. Applying the replayed batch is a no-op at the engine level: the
//!    per-stream applied-sequence journal short-circuits it, so the
//!    record generation does not move. Replay is therefore
//!    indistinguishable from a benign retry.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use teraslab::allocator::SlotAllocator;
use teraslab::cluster::auth;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, TxKey};
use teraslab::locks::StripedLocks;
use teraslab::ops::create::CreateRequest;
use teraslab::ops::engine::Engine;
use teraslab::protocol::frame::RequestFrame;
use teraslab::protocol::opcodes::{OP_REPLICA_BATCH, STATUS_OK};
use teraslab::record::UTXO_SPENT;
use teraslab::replication::durable::ReplicaAppliedTracker;
use teraslab::replication::protocol::{ReplicaAck, ReplicaBatch, ReplicaOp};
use teraslab::replication::receiver::handle_replica_batch_with_tracker;

fn make_engine() -> Arc<Engine> {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
    let alloc = SlotAllocator::new(dev.clone()).unwrap();
    let index = Index::new(10_000).unwrap();
    Arc::new(Engine::new(
        dev,
        index,
        alloc,
        StripedLocks::new(1024),
        DahIndex::new(),
    ))
}

fn key(n: u64) -> TxKey {
    let mut txid = [0u8; 32];
    txid[0..8].copy_from_slice(&n.to_le_bytes());
    TxKey { txid }
}

fn create_record(engine: &Engine, k: TxKey, utxo_count: u32) {
    let hashes: Vec<[u8; 32]> = (0..utxo_count)
        .map(|v| {
            // Production-shaped NON-ZERO hashes (marker byte + index). The
            // V3 wire encodes `prior_utxo_hash: None` as the all-zero
            // sentinel, so a fixture record whose offset-0 hash is all
            // zeroes (the old `0u32` pattern) had its §4.9 prior guard
            // silently stripped in transit — and under F-7's enforcement
            // guard such a prior-less Reassign is refused. Real utxo
            // hashes are never all-zero; keep the fixture realistic.
            let mut h = [0u8; 32];
            h[0] = 0xE4;
            h[4..8].copy_from_slice(&v.to_le_bytes());
            h
        })
        .collect();
    let req = CreateRequest {
        tx_id: k.txid,
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
}

/// Build a signed OP_REPLICA_BATCH wire frame for `batch`.
fn signed_replica_frame(secret: &[u8], batch: &ReplicaBatch, request_id: u64) -> Vec<u8> {
    signed_replica_frame_with_flags(secret, batch, request_id, 0)
}

/// [`signed_replica_frame`] with explicit frame flags (e.g.
/// `FLAG_MIGRATION_BATCH` for the §4.7 migration-delta replay test).
fn signed_replica_frame_with_flags(
    secret: &[u8],
    batch: &ReplicaBatch,
    request_id: u64,
    flags: u16,
) -> Vec<u8> {
    let frame = RequestFrame {
        request_id,
        op_code: OP_REPLICA_BATCH,
        flags,
        payload: batch.serialize().into(),
    };
    auth::sign_frame(secret, &frame.encode()).expect("sign_frame")
}

#[test]
fn replica_batch_replay_is_idempotent() {
    let secret = b"e4-cluster-secret".to_vec();
    let engine = make_engine();
    create_record(&engine, key(7), 3);

    // A spend batch at sequence 10 (watermark seeded to 9 so 10 is next).
    let mut sd = [0u8; 36];
    sd[0] = 0xC3;
    let batch = ReplicaBatch {
        first_sequence: 10,
        ops: vec![ReplicaOp::Spend {
            tx_key: key(7),
            offset: 0,
            spending_data: sd,
            current_block_height: 1000,
            block_height_retention: 288,
            master_generation: 0,
        }],
        trace_ctx: None,
        source_node_id: None,
        cluster_key: 0,
        regime_table: None,
    };

    let tracker = ReplicaAppliedTracker::in_memory();
    let stream_key = "peer-A:6000";
    tracker.set(stream_key, 9);
    let last_applied = Arc::new(AtomicU64::new(0));

    // Sign a frame on the wire, then verify it through the auth layer.
    let signed = signed_replica_frame(&secret, &batch, 1);
    let verified_first = auth::verify_frame(&secret, &signed).expect("first verify");
    let (decoded_first, _) = RequestFrame::decode(&verified_first).expect("decode first");
    assert_eq!(decoded_first.op_code, OP_REPLICA_BATCH);

    // First application: the spend takes effect.
    let resp_1 = handle_replica_batch_with_tracker(
        &decoded_first,
        &engine,
        &last_applied,
        Some(&tracker),
        stream_key,
        0,
    );
    assert_eq!(resp_1.status, STATUS_OK);
    let ack_1 = ReplicaAck::deserialize(&resp_1.payload).unwrap();
    assert_eq!(
        ack_1,
        ReplicaAck::Ok {
            through_sequence: 10
        }
    );
    assert_eq!(engine.read_slot(&key(7), 0).unwrap().status, UTXO_SPENT);
    let gen_after_first = engine.read_metadata(&key(7)).unwrap().generation;

    // ----- REPLAY: feed the IDENTICAL signed bytes again. -----
    //
    // Part 1 of the E-4 decision: the auth layer ACCEPTS the verbatim
    // replay (no nonce). `verify_frame` on the same bytes succeeds.
    let verified_replay = auth::verify_frame(&secret, &signed)
        .expect("E-4: auth layer accepts a verbatim replayed frame (documented gap)");
    assert_eq!(
        verified_replay, verified_first,
        "replayed frame verifies to the identical body"
    );
    let (decoded_replay, _) = RequestFrame::decode(&verified_replay).expect("decode replay");

    // Part 2: applying the replay is a no-op — the applied-sequence
    // journal short-circuits it before touching the engine.
    let resp_2 = handle_replica_batch_with_tracker(
        &decoded_replay,
        &engine,
        &last_applied,
        Some(&tracker),
        stream_key,
        0,
    );
    assert_eq!(resp_2.status, STATUS_OK);
    let ack_2 = ReplicaAck::deserialize(&resp_2.payload).unwrap();
    assert_eq!(
        ack_2,
        ReplicaAck::Ok {
            through_sequence: 10
        },
        "replay re-ACKs the existing watermark"
    );

    let gen_after_replay = engine.read_metadata(&key(7)).unwrap().generation;
    assert_eq!(
        gen_after_replay, gen_after_first,
        "E-4: replayed batch must NOT mutate engine state (idempotency-under-replay)"
    );
    assert_eq!(tracker.get(stream_key), 10);
}

/// P1 §4.7 companion test — the migration-delta replay gap, closed by the
/// `cluster_key` gate in I8's PRECISE form.
///
/// Migration-delta batches (`FLAG_MIGRATION_BATCH`, `first_sequence == 0`)
/// BYPASS the applied-sequence journal, so the dedup that neutralized the
/// replay in `replica_batch_replay_is_idempotent` does not exist here. The
/// defenses on this path (per the corrected `src/cluster/auth.rs` table)
/// are the `cluster_key` gate, the generation guard, and the §4.9
/// prior-hash-guarded Reassign.
///
/// I8's precise form, asserted exactly: a captured V3 delta frame carrying
/// a `Reassign`, replayed against a receiver that HAS INSTALLED the
/// promoting commit (committed term + regime advanced past the frame's
/// stamps), is rejected WHOLESALE at the existing
/// `cluster_key < local_cluster_key` gate — before any tracker or engine
/// work. (Not the stronger "dies everywhere" form: a receiver that has NOT
/// installed the promoting commit takes the accept-newer arm, but such a
/// receiver is not serving the promoted mastership either.)
#[test]
fn migration_delta_reassign_replay_rejected_wholesale_after_promotion() {
    use std::collections::BTreeMap;
    use teraslab::cluster::shards::{NodeId, ShardTable};
    use teraslab::cluster::topology::{
        PersistedTopologyState, RegimeArray, RegimeBlock, TopologyAuthority,
    };
    use teraslab::protocol::opcodes::{ERR_STALE_EPOCH, FLAG_MIGRATION_BATCH, STATUS_ERROR};
    use teraslab::record::{UTXO_FROZEN, UTXO_UNSPENT};
    use teraslab::replication::receiver::{apply_op, handle_replica_batch_regime_gated};

    /// Committed state at `term` with `regime[shard] = term`, enforcement
    /// active (committed flag + secret — I11).
    fn authority_at(term: u64, shard: u16) -> TopologyAuthority {
        let authority = TopologyAuthority::new(NodeId(1), std::time::Duration::from_secs(1));
        let mut regime = RegimeArray::default();
        regime.set(shard, term);
        authority.restore(&PersistedTopologyState {
            peak_cluster_size: 2,
            committed_term: term,
            committed_members: vec![NodeId(1), NodeId(2)],
            committed_voters: vec![NodeId(1), NodeId(2)],
            voted_term: term,
            incarnation: 1,
            committed_voter_ever_seen: vec![NodeId(1), NodeId(2)],
            committed_placement_version: 1,
            committed_peak: 2,
            regime_block: RegimeBlock {
                override_map: BTreeMap::new(),
                regime,
                regime_enforced: true,
                promotion_enabled: false,
                rebase: false,
            },
            data_epoch: None,
        });
        authority.set_secret_configured(true);
        authority
    }

    let secret = b"e4-regime-secret".to_vec();
    let engine = make_engine();
    let k = key(21);
    create_record(&engine, k, 2);
    let shard = ShardTable::shard_for_key(&k);

    // Freeze slot 0 so the captured reassign is genuinely applicable.
    apply_op(
        &engine,
        &ReplicaOp::Freeze {
            tx_key: k,
            offset: 0,
            master_generation: 0,
        },
    )
    .expect("freeze applies");
    let prior_hash = engine.read_slot(&k, 0).unwrap().hash;
    assert_eq!(engine.read_slot(&k, 0).unwrap().status, UTXO_FROZEN);

    // The captured migration-delta frame, stamped at pre-failover term 5:
    // V3 (regime table [(shard, 5)]), cluster_key 5, out-of-band
    // first_sequence 0, carrying the §4.9 prior-hash-guarded Reassign.
    let new_hash = [0x6C; 32];
    let delta = ReplicaBatch {
        first_sequence: 0,
        ops: vec![ReplicaOp::Reassign {
            tx_key: k,
            offset: 0,
            new_hash,
            block_height: 800_000,
            spendable_after: 1_000,
            master_generation: 1,
            prior_utxo_hash: Some(prior_hash),
        }],
        trace_ctx: None,
        source_node_id: None,
        cluster_key: 5,
        regime_table: Some(vec![(shard, 5)]),
    };
    let signed = signed_replica_frame_with_flags(&secret, &delta, 3, FLAG_MIGRATION_BATCH);

    let tracker = ReplicaAppliedTracker::in_memory();
    let stream_key = "peer-B:6000";
    let last_applied = Arc::new(AtomicU64::new(0));

    // First delivery, receiver at term 5 (pre-promotion): applies.
    let verified_first = auth::verify_frame(&secret, &signed).expect("first verify");
    let (decoded_first, _) = RequestFrame::decode(&verified_first).expect("decode first");
    assert_eq!(
        decoded_first.flags & FLAG_MIGRATION_BATCH,
        FLAG_MIGRATION_BATCH
    );
    let pre_promotion = authority_at(5, shard);
    let resp_1 = handle_replica_batch_regime_gated(
        &decoded_first,
        &engine,
        &last_applied,
        Some(&tracker),
        stream_key,
        5,
        Some(&pre_promotion),
        None,
    );
    assert_eq!(resp_1.status, STATUS_OK, "pre-promotion delivery applies");
    let slot = engine.read_slot(&k, 0).unwrap();
    assert_eq!(slot.status, UTXO_UNSPENT);
    assert_eq!(slot.hash, new_hash, "the delta's reassign took effect");
    let gen_after_first = engine.read_metadata(&k).unwrap().generation;
    let count_after_first = { engine.read_metadata(&k).unwrap().reassignment_count };

    // ----- REPLAY, post-promotion. -----
    // The auth layer still accepts the verbatim bytes (the documented E-4
    // gap this file pins)...
    let verified_replay = auth::verify_frame(&secret, &signed)
        .expect("E-4: auth layer accepts a verbatim replayed frame (documented gap)");
    assert_eq!(verified_replay, verified_first);
    let (decoded_replay, _) = RequestFrame::decode(&verified_replay).expect("decode replay");

    // ...but the receiver has installed the promoting commit: committed
    // term 6, regime[shard] = 6. The frame dies WHOLESALE at the
    // cluster_key gate (5 < 6) — I8's precise form — before the migration
    // bypass, the tracker, or the engine see it.
    let post_promotion = authority_at(6, shard);
    let resp_2 = handle_replica_batch_regime_gated(
        &decoded_replay,
        &engine,
        &last_applied,
        Some(&tracker),
        stream_key,
        6,
        Some(&post_promotion),
        None,
    );
    assert_eq!(
        resp_2.status, STATUS_ERROR,
        "replayed pre-failover delta must be rejected wholesale",
    );
    let code = u16::from_le_bytes([resp_2.payload[0], resp_2.payload[1]]);
    assert_eq!(
        code, ERR_STALE_EPOCH,
        "the reject fires at the existing cluster_key gate (I8's precise form)",
    );

    // Nothing moved: engine state and the (bypassed) watermark unchanged.
    let slot = engine.read_slot(&k, 0).unwrap();
    assert_eq!(slot.hash, new_hash);
    let gen_after_replay = { engine.read_metadata(&k).unwrap().generation };
    assert_eq!(gen_after_replay, gen_after_first);
    assert_eq!(
        { engine.read_metadata(&k).unwrap().reassignment_count },
        count_after_first,
        "the replayed reassign must not re-apply",
    );
    assert_eq!(
        tracker.get(stream_key),
        0,
        "an out-of-band delta never advances the watermark — and the reject certainly must not",
    );
}
