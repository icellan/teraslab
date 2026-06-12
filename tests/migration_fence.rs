//! F-02 — migration write-fence + read-passthrough integration test.
//!
//! F-02: the migration fence
//! (`MigrationManager::fence_shard` / `RunningCluster::is_shard_write_fenced`,
//! enforced in `dispatch::check_shard_ownership`) had no end-to-end
//! integration coverage asserting BOTH arms on the SAME fenced shard of
//! a live cluster: a mutation must fail with code 19
//! (`ERR_MIGRATION_IN_PROGRESS`) while a read of the same key still
//! succeeds, and the mutation must succeed once the fence lifts.
//!
//! Determinism note: a real delta-streaming migration holds the Fenced
//! state only for the brief delta+completion window, and no pacing knob
//! can hold it open without sleeps-as-synchronization. Per the task
//! guidance, the fence state is therefore driven through the test-only
//! hooks `RunningCluster::test_fence_shard` / `test_unfence_shard`
//! (gated behind `cfg(any(test, feature = "fault-injection"))`), which
//! set exactly the state the migration worker sets via `mark_fenced` /
//! completion. The dispatch path under test — live TCP server, cluster
//! routing, fence enforcement, read passthrough — is the production
//! path end to end.
//!
//! Requires the `fault-injection` feature:
//!
//! ```bash
//! cargo test --features fault-injection --test migration_fence
//! ```

#![cfg(feature = "fault-injection")]
#![allow(clippy::disallowed_macros)]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use teraslab::allocator::SlotAllocator;
use teraslab::cluster::coordinator::{
    ClusterConfig, ClusterCoordinator, MasterQueryResult, ReplicationRuntimeConfig, RunningCluster,
};
use teraslab::cluster::shards::{MigrationTask, NodeId, ShardTable};
use teraslab::cluster::topology::ClusterId;
use teraslab::config::ServerConfig;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, TxKey, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::engine::Engine;
use teraslab::protocol::codec::{
    WireCreateItem, WireGetSpendItem, decode_get_spend_response, encode_create_batch,
    encode_get_spend_batch,
};
use teraslab::protocol::frame::*;
use teraslab::protocol::opcodes::*;
use teraslab::server::Server;

const TEST_CLUSTER_ID: ClusterId = ClusterId([0xA5; 16]);

struct TestNode {
    server: Arc<Server>,
    cluster: Arc<RunningCluster>,
    tcp_port: u16,
    swim_port: u16,
}

fn reserve_tcp_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

fn reserve_udp_port() -> u16 {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let port = socket.local_addr().unwrap().port();
    drop(socket);
    port
}

/// Same construction as `tests/cluster_tcp.rs::create_node` (RF
/// parameterized, ephemeral ports).
fn create_node(node_id: u64, seed_swim_ports: &[u16], rf: u8) -> TestNode {
    let tcp_port = reserve_tcp_port();
    let mut swim_port = reserve_udp_port();
    while swim_port == tcp_port {
        swim_port = reserve_udp_port();
    }

    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(32 * 1024 * 1024, 4096).unwrap());
    let alloc = SlotAllocator::new(dev.clone()).unwrap();
    let index = Index::new(1000).unwrap();
    let engine = Arc::new(Engine::new(
        dev,
        index,
        alloc,
        StripedLocks::new(256),
        DahIndex::new(),
        UnminedIndex::new(),
    ));

    let seeds: Vec<std::net::SocketAddr> = seed_swim_ports
        .iter()
        .map(|p| format!("127.0.0.1:{p}").parse().unwrap())
        .collect();

    let cluster_config = ClusterConfig {
        self_id: NodeId(node_id),
        self_addr: format!("127.0.0.1:{tcp_port}").parse().unwrap(),
        swim_bind: format!("127.0.0.1:{swim_port}").parse().unwrap(),
        swim_advertise_addr: None,
        seed_nodes: seeds,
        replication_factor: rf,
        probe_interval: Duration::from_millis(100),
        suspicion_timeout: Duration::from_secs(2),
        cluster_secret: None,
        max_migration_threads: 16,
        topology_propose_timeout: Duration::from_millis(300),
        migration_pool_size: 4,
        migration_batch_size: 100,
        persisted_incarnation: 0,
        cluster_id: TEST_CLUSTER_ID,
    };

    let coordinator = ClusterCoordinator::new(cluster_config, 1);
    let running = Arc::new(coordinator.start(
        engine.clone(),
        None,
        None,
        ReplicationRuntimeConfig {
            ack_policy: None,
            best_effort: true,
            timeout: Duration::from_secs(3),
            timeout_during_migration: Duration::from_secs(30),
        },
    ));

    let config = ServerConfig {
        listen_addr: format!("127.0.0.1:{tcp_port}"),
        max_connections: 64,
        max_batch_size: 4096,
        node_id,
        strict_auth: false,
        ..Default::default()
    };
    let server = Arc::new(Server::new(engine, config).with_cluster(running.clone()));
    let server_clone = server.clone();
    std::thread::spawn(move || {
        let _ = server_clone.run();
    });

    let swim_target: std::net::SocketAddr = format!("127.0.0.1:{swim_port}").parse().unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < deadline {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").ok();
        let bound = match probe {
            Some(s) => s.connect(swim_target).is_ok(),
            None => false,
        };
        if bound {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    std::thread::sleep(Duration::from_millis(100));

    TestNode {
        server,
        cluster: running,
        tcp_port,
        swim_port,
    }
}

fn shutdown_node(node: &TestNode) {
    node.cluster.shutdown();
    node.server.shutdown();
}

fn wait_until<F: FnMut() -> bool>(
    mut predicate: F,
    timeout: Duration,
) -> std::result::Result<(), ()> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if predicate() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    if predicate() { Ok(()) } else { Err(()) }
}

fn make_txid(seed: u32) -> [u8; 32] {
    let mut txid = [0u8; 32];
    txid[0..4].copy_from_slice(&seed.to_le_bytes());
    for (i, byte) in txid.iter_mut().enumerate().skip(4) {
        *byte = (seed.wrapping_mul(7).wrapping_add(i as u32) & 0xFF) as u8;
    }
    txid
}

fn encode_create_payload(txid: &[u8; 32], utxo_hash: &[u8; 32]) -> Vec<u8> {
    encode_create_batch(&[WireCreateItem {
        txid: *txid,
        tx_version: 2,
        locktime: 0,
        fee: 1000,
        size_in_bytes: 250,
        extended_size: 0,
        is_coinbase: false,
        spending_height: 0,
        created_at: 1700000000000,
        flags: 0,
        utxo_hashes: vec![*utxo_hash],
        cold_data: vec![],
        block_height: 0,
        mined_block_id: None,
        mined_block_height: None,
        mined_subtree_idx: None,
        parent_txids: vec![],
    }])
}

/// SpendBatch wire payload for one item (format pinned in
/// `tests/cluster_tcp.rs::spend_routed_to_correct_master`).
fn encode_spend_payload(
    txid: &[u8; 32],
    utxo_hash: &[u8; 32],
    spending_data: &[u8; 36],
) -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(&1u32.to_le_bytes()); // count
    p.push(0); // ignore_conflicting
    p.push(0); // ignore_locked
    p.extend_from_slice(&100u32.to_le_bytes()); // current_block_height
    p.extend_from_slice(&0u32.to_le_bytes()); // block_height_retention
    p.extend_from_slice(txid);
    p.extend_from_slice(&0u32.to_le_bytes()); // vout = 0
    p.extend_from_slice(utxo_hash);
    p.extend_from_slice(spending_data);
    p
}

fn send_request(stream: &mut TcpStream, frame: &RequestFrame) -> ResponseFrame {
    let bytes = frame.encode();
    stream.write_all(&bytes).unwrap();
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).unwrap();
    let total_length = u32::from_le_bytes(len_buf) as usize;
    let mut body = vec![0u8; total_length];
    stream.read_exact(&mut body).unwrap();
    let mut full = Vec::with_capacity(4 + total_length);
    full.extend_from_slice(&len_buf);
    full.extend_from_slice(&body);
    let (response, _) = ResponseFrame::decode(&full).unwrap();
    response
}

/// Decode the single sparse error of a `STATUS_PARTIAL_ERROR` response
/// (`[count:4][item_index:4][error_code:2][data_len:2][data:N]`) and
/// return the error code.
fn single_sparse_error_code(resp: &ResponseFrame, context: &str) -> u16 {
    assert_eq!(
        resp.status,
        STATUS_PARTIAL_ERROR,
        "{context}: expected STATUS_PARTIAL_ERROR, got status={} payload_len={}",
        resp.status,
        resp.payload.len()
    );
    assert!(
        resp.payload.len() >= 10,
        "{context}: sparse error payload too short ({} bytes)",
        resp.payload.len()
    );
    let count = u32::from_le_bytes(resp.payload[0..4].try_into().unwrap());
    assert_eq!(count, 1, "{context}: expected exactly one sparse error");
    u16::from_le_bytes(resp.payload[8..10].try_into().unwrap())
}

/// Both arms of the F-02 fence contract on the SAME fenced shard of a
/// live two-node cluster, plus the post-completion success arm:
///
/// (a) SPEND on the fenced shard → code 19 (`ERR_MIGRATION_IN_PROGRESS`)
///     and the UTXO is NOT spent (the fence really blocked the write);
/// (b) GET_SPEND for the same key on the same node → `STATUS_OK`
///     (reads pass through the write fence);
/// (c) once the fence lifts (migration completion), the identical SPEND
///     succeeds and the spend is durably visible.
#[test]
fn fenced_shard_rejects_spend_serves_read_then_spend_succeeds_after_fence_lifts() {
    // RF=1 two-node cluster: replication is irrelevant to the fence and
    // RF=1 keeps the write path single-copy (same as the migration
    // tests in cluster_tcp.rs).
    let node1 = create_node(441, &[], 1);
    let node2 = create_node(442, &[node1.swim_port], 1);

    wait_until(
        || {
            node1.cluster.committed_topology_members().len() == 2
                && node2.cluster.committed_topology_members().len() == 2
        },
        Duration::from_secs(20),
    )
    .expect("2-node topology should commit on both nodes");

    // A key node1 masters in the rebalanced 2-node table.
    let mut key_txid = None;
    for i in 0..8192u32 {
        let txid = make_txid(940_000 + i);
        if matches!(
            node1.cluster.is_master(&TxKey { txid }),
            MasterQueryResult::Yes
        ) {
            key_txid = Some(txid);
            break;
        }
    }
    let txid = key_txid.expect("node1 should master at least one of 8192 candidate keys");
    let utxo_hash = make_txid(950_001);
    let key = TxKey { txid };
    let shard = ShardTable::shard_for_key(&key);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", node1.tcp_port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Seed the record while the shard is unfenced.
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1,
            op_code: OP_CREATE_BATCH,
            flags: 0,
            payload: encode_create_payload(&txid, &utxo_hash).into(),
        },
    );
    assert_eq!(resp.status, STATUS_OK, "create before the fence");

    // Engage the write fence exactly as the delta-streaming migration
    // does when the shard enters MigrationState::Fenced.
    node1.cluster.test_fence_shard(shard);
    assert!(
        node1.cluster.is_shard_write_fenced(&key),
        "fence must be visible on the hot-path bitmap"
    );

    // (a) SPEND on the fenced shard → per-item code 19.
    let spending_data = [0xC4u8; 36];
    let spend_payload = encode_spend_payload(&txid, &utxo_hash, &spending_data);
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 2,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload: spend_payload.clone().into(),
        },
    );
    let code = single_sparse_error_code(&resp, "spend on fenced shard");
    assert_eq!(
        code, ERR_MIGRATION_IN_PROGRESS,
        "fenced-shard SPEND must fail with code 19 (ERR_MIGRATION_IN_PROGRESS), got {code}"
    );

    // (b) GET_SPEND on the SAME key on the SAME node → reads pass
    // through the fence and prove the blocked spend did not land.
    let query = encode_get_spend_batch(&[WireGetSpendItem {
        txid,
        vout: 0,
        utxo_hash,
    }]);
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 3,
            op_code: OP_GET_SPEND_BATCH,
            flags: 0,
            payload: query.clone().into(),
        },
    );
    assert_eq!(
        resp.status, STATUS_OK,
        "GET_SPEND on a write-fenced shard must succeed"
    );
    let results = decode_get_spend_response(&resp.payload)
        .expect("get_spend response must decode while fenced");
    assert_eq!(results.len(), 1, "one queried key, one result");
    assert_eq!(results[0].status, 0, "fenced read must find the record");
    assert_eq!(
        results[0].spending_data, [0u8; 36],
        "UTXO must still be unspent — the fenced SPEND must not have applied"
    );

    // (c) Fence lifts (migration completion) → the identical SPEND
    // succeeds and is visible.
    node1.cluster.test_unfence_shard(shard);
    assert!(
        !node1.cluster.is_shard_write_fenced(&key),
        "fence must be cleared after unfence"
    );
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 4,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload: spend_payload.into(),
        },
    );
    assert_eq!(
        resp.status,
        STATUS_OK,
        "the same SPEND must succeed after the fence lifts (payload_len={})",
        resp.payload.len()
    );
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 5,
            op_code: OP_GET_SPEND_BATCH,
            flags: 0,
            payload: query.into(),
        },
    );
    assert_eq!(resp.status, STATUS_OK, "post-spend GET_SPEND");
    let results =
        decode_get_spend_response(&resp.payload).expect("post-spend response must decode");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].status, 0, "spent record still readable");
    assert_eq!(
        results[0].spending_data, spending_data,
        "spend applied after the fence lifted must be visible"
    );

    shutdown_node(&node1);
    shutdown_node(&node2);
}

/// Acked-write-loss regression for the migration completion window:
/// a client write racing the completion of an outbound shard migration
/// must either be rejected (`ERR_MIGRATION_IN_PROGRESS` while fenced, or
/// `ERR_REDIRECT` once ownership has transferred) or reach the new
/// master — it must NEVER be ACKed `STATUS_OK` by the source between
/// manifest acceptance and the ownership commit, because such a write
/// exists only on the source and is destroyed by orphan cleanup.
///
/// The test drives the PRODUCTION completion transition
/// (`complete_migration_task_current_epoch`) for a deterministically
/// fenced shard via the fault-injection hooks, and fires a real wire
/// SPEND at the sync-point between the completion's two state
/// transitions. With the historical unfence-before-commit ordering the
/// spend lands in the gap and is ACKed `STATUS_OK` — exactly the lost
/// write — so this test fails against that ordering.
#[test]
fn write_racing_completion_window_is_never_acked_then_lost() {
    let node1 = create_node(443, &[], 1);
    let node2 = create_node(444, &[node1.swim_port], 1);

    wait_until(
        || {
            node1.cluster.committed_topology_members().len() == 2
                && node2.cluster.committed_topology_members().len() == 2
        },
        Duration::from_secs(20),
    )
    .expect("2-node topology should commit on both nodes");

    // A key node1 masters in the committed 2-node table.
    let mut key_txid = None;
    for i in 0..8192u32 {
        let txid = make_txid(960_000 + i);
        if matches!(
            node1.cluster.is_master(&TxKey { txid }),
            MasterQueryResult::Yes
        ) {
            key_txid = Some(txid);
            break;
        }
    }
    let txid = key_txid.expect("node1 should master at least one of 8192 candidate keys");
    let utxo_hash = make_txid(970_001);
    let key = TxKey { txid };
    let shard = ShardTable::shard_for_key(&key);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", node1.tcp_port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Seed the record before the migration fences the shard.
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1,
            op_code: OP_CREATE_BATCH,
            flags: 0,
            payload: encode_create_payload(&txid, &utxo_hash).into(),
        },
    );
    assert_eq!(resp.status, STATUS_OK, "create before the migration");

    // Track + fence the outbound task exactly as the pipelined migration
    // worker does after baseline streaming; from here the migration is at
    // the point where the target has verified the manifest and the source
    // runs the completion transition.
    let task = MigrationTask {
        shard,
        from_node: NodeId(443),
        to_node: NodeId(444),
        is_master: true,
    };
    node1.cluster.test_track_outbound_fenced(&task);
    assert!(
        node1.cluster.is_shard_write_fenced(&key),
        "fence must be visible on the hot-path bitmap"
    );

    // Run the production completion; at the midpoint between its two
    // state transitions, fire a real wire SPEND at the source. No locks
    // are held at the sync-point, so the request is served end-to-end by
    // the production dispatch path.
    let spending_data = [0xD7u8; 36];
    let spend_payload = encode_spend_payload(&txid, &utxo_hash, &spending_data);
    let node1_port = node1.tcp_port;
    let mut midpoint_resp: Option<ResponseFrame> = None;
    let completed = node1
        .cluster
        .test_complete_outbound_migration_with_midpoint(&task, true, || {
            let mut racer = TcpStream::connect(format!("127.0.0.1:{node1_port}")).unwrap();
            racer
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            midpoint_resp = Some(send_request(
                &mut racer,
                &RequestFrame {
                    request_id: 2,
                    op_code: OP_SPEND_BATCH,
                    flags: 0,
                    payload: spend_payload.clone().into(),
                },
            ));
        });
    assert!(completed, "tracked current-epoch completion must succeed");

    let resp = midpoint_resp.expect("midpoint spend must have run");
    assert_ne!(
        resp.status, STATUS_OK,
        "LOST ACK: a write racing migration completion was ACKed STATUS_OK \
         by the source after manifest acceptance — it exists nowhere after \
         orphan cleanup"
    );
    let code = single_sparse_error_code(&resp, "spend racing migration completion");
    assert!(
        code == ERR_MIGRATION_IN_PROGRESS || code == ERR_REDIRECT,
        "racing write must be fenced (code {ERR_MIGRATION_IN_PROGRESS}) or \
         redirected (code {ERR_REDIRECT}), got {code}"
    );

    // After completion the fence is lifted; this node remains the routed
    // master here (no table handoff was armed), so the identical SPEND now
    // succeeds and is durably visible — the client retry path works.
    assert!(
        !node1.cluster.is_shard_write_fenced(&key),
        "fence must be cleared after completion"
    );
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 3,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload: spend_payload.into(),
        },
    );
    assert_eq!(
        resp.status,
        STATUS_OK,
        "retried SPEND after completion must succeed (payload_len={})",
        resp.payload.len()
    );
    let query = encode_get_spend_batch(&[WireGetSpendItem {
        txid,
        vout: 0,
        utxo_hash,
    }]);
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 4,
            op_code: OP_GET_SPEND_BATCH,
            flags: 0,
            payload: query.into(),
        },
    );
    assert_eq!(resp.status, STATUS_OK, "post-spend GET_SPEND");
    let results =
        decode_get_spend_response(&resp.payload).expect("post-spend response must decode");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].spending_data, spending_data,
        "retried spend must be durably visible"
    );

    shutdown_node(&node1);
    shutdown_node(&node2);
}
