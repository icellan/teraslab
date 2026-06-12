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
use teraslab::index::{DahIndex, Index, TxKey, UnminedIndex};
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
        UnminedIndex::new(),
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
            let mut h = [0u8; 32];
            h[0..4].copy_from_slice(&v.to_le_bytes());
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
    let frame = RequestFrame {
        request_id,
        op_code: OP_REPLICA_BATCH,
        flags: 0,
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
