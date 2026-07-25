//! In-process coverage for DELETE under RF > 1 — the cluster contract the
//! Docker scenario `scenario_03_replication_correctness` step 3.5 asserts:
//! a client delete must remove the record from EVERY holder, master and
//! replica alike.
//!
//! Why this file exists (the structural gap): `tests/cluster_tcp.rs` drives
//! writes at ONE node with `best_effort: true`, so it never observes what the
//! peer holding the replica copy did with a delete. Nothing at the Rust level
//! read the replica back after a delete, so a master-only prune looked healthy
//! for as long as you only ever asked the master.

#![allow(clippy::disallowed_macros)] // integration tests may use eprintln! for diagnostics

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use teraslab::cluster::coordinator::{
    ClusterConfig, ClusterCoordinator, ReplicationRuntimeConfig, RunningCluster,
};
use teraslab::cluster::shards::NodeId;
use teraslab::cluster::topology::ClusterId;
use teraslab::config::ServerConfig;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, TxKey};
use teraslab::locks::StripedLocks;
use teraslab::ops::engine::Engine;
use teraslab::protocol::codec::{
    FieldMask, WireCreateItem, encode_create_batch, encode_get_batch, encode_txid_batch,
};
use teraslab::protocol::frame::{RequestFrame, ResponseFrame};
use teraslab::protocol::opcodes::{
    FLAG_LOCAL_READ, OP_CREATE_BATCH, OP_DELETE_BATCH, OP_GET_BATCH,
    OP_PROCESS_EXPIRED_PRESERVATIONS, OP_SET_MINED_BATCH, OP_SPEND_BATCH, STATUS_OK,
};
use teraslab::redo::RedoLog;
use teraslab::segment_allocator::SegmentAllocator;
use teraslab::server::Server;

const TEST_CLUSTER_ID: ClusterId = ClusterId([0xC7; 16]);
const TEST_SEGMENT_SIZE: u64 = 16 * 4096;
const ACK_TIMEOUT: Duration = Duration::from_secs(3);

struct TestNode {
    server: Arc<Server>,
    cluster: Arc<RunningCluster>,
    engine: Arc<Engine>,
    tcp_port: u16,
    swim_port: u16,
    shutdown: Arc<AtomicBool>,
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

/// Build a clustered SEGMENT node with the production replication runtime the
/// Docker E2E nodes get: strict ACK enforcement (`best_effort = false`) and the
/// 3 s foreground ACK timeout, so a write that returns `STATUS_OK` has provably
/// been applied by its replica.
fn create_cluster_node(node_id: u64, seed_swim_ports: &[u16]) -> TestNode {
    let tcp_port = reserve_tcp_port();
    let mut swim_port = reserve_udp_port();
    while swim_port == tcp_port {
        swim_port = reserve_udp_port();
    }

    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(32 * 1024 * 1024, 4096).unwrap());
    let seg = SegmentAllocator::new(dev.clone(), TEST_SEGMENT_SIZE).unwrap();
    let engine = Arc::new(Engine::new(
        dev,
        Index::new(1000).unwrap(),
        seg,
        StripedLocks::new(256),
        DahIndex::new(),
    ));
    let log_dev: Arc<dyn BlockDevice> =
        Arc::new(MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap());
    let redo = Arc::new(parking_lot::Mutex::new(
        RedoLog::open(log_dev, 0, 16 * 1024 * 1024).unwrap(),
    ));
    engine.set_redo_logs(vec![redo.clone()]);
    engine.set_buffered_durability(true);

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
        replication_factor: 2,
        probe_interval: Duration::from_millis(100),
        suspicion_timeout: Duration::from_secs(2),
        cluster_secret: None,
        max_migration_threads: 16,
        topology_propose_timeout: Duration::from_millis(300),
        topology_debounce: Duration::from_millis(100),
        migration_pool_size: 4,
        migration_batch_size: 100,
        persisted_incarnation: 0,
        cluster_id: TEST_CLUSTER_ID,
        reverse_heal_online: false,
        heal_deadline: Duration::from_secs(300),
        heal_deadline_action: teraslab::config::HealDeadlineAction::AlertAndHold,
    };

    let replication = ReplicationRuntimeConfig {
        ack_policy: None,
        best_effort: false,
        timeout: ACK_TIMEOUT,
        timeout_during_migration: Duration::from_secs(30),
    };

    let coordinator = ClusterCoordinator::new(cluster_config, 1);
    let running = Arc::new(coordinator.start(engine.clone(), None, None, replication));

    let config = ServerConfig {
        listen_addr: format!("127.0.0.1:{tcp_port}"),
        max_connections: 64,
        max_batch_size: 4096,
        node_id,
        strict_auth: false,
        ..Default::default()
    };
    let server = Arc::new(Server::new(engine.clone(), config).with_cluster(running.clone()));
    let server_clone = server.clone();
    std::thread::spawn(move || {
        let _ = server_clone.run();
    });

    let swim_target: std::net::SocketAddr = format!("127.0.0.1:{swim_port}").parse().unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let bound = std::net::UdpSocket::bind("127.0.0.1:0")
            .ok()
            .is_some_and(|s| s.connect(swim_target).is_ok());
        if bound {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    std::thread::sleep(Duration::from_millis(100));

    TestNode {
        server,
        cluster: running,
        engine,
        tcp_port,
        swim_port,
        shutdown: Arc::new(AtomicBool::new(false)),
    }
}

fn shutdown_node(node: &TestNode) {
    node.shutdown.store(true, Ordering::Relaxed);
    node.cluster.shutdown();
    node.server.shutdown();
}

fn send_request(stream: &mut TcpStream, frame: &RequestFrame) -> ResponseFrame {
    stream.write_all(&frame.encode()).unwrap();
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).unwrap();
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).unwrap();
    let mut full = Vec::with_capacity(4 + len);
    full.extend_from_slice(&len_buf);
    full.extend_from_slice(&body);
    let (response, _) = ResponseFrame::decode(&full).unwrap();
    response
}

fn request_at(port: u16, op_code: u16, flags: u16, payload: Vec<u8>) -> ResponseFrame {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .unwrap();
    send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1,
            op_code,
            flags,
            payload: payload.into(),
        },
    )
}

fn make_txid(seed: u32) -> [u8; 32] {
    let mut txid = [0u8; 32];
    txid[0..4].copy_from_slice(&seed.to_le_bytes());
    for (i, byte) in txid.iter_mut().enumerate().skip(4) {
        *byte = (seed.wrapping_mul(11).wrapping_add(i as u32) & 0xFF) as u8;
    }
    txid
}

fn utxo_hash_for(seed: u32) -> [u8; 32] {
    make_txid(seed.wrapping_add(900_000))
}

fn encode_batch(seeds: &[u32]) -> Vec<u8> {
    let items: Vec<WireCreateItem> = seeds
        .iter()
        .map(|s| WireCreateItem {
            txid: make_txid(*s),
            tx_version: 2,
            locktime: 0,
            fee: 1000,
            size_in_bytes: 250,
            extended_size: 0,
            is_coinbase: false,
            spending_height: 0,
            created_at: 1700000000000,
            flags: 0,
            utxo_hashes: vec![utxo_hash_for(*s)],
            cold_data: vec![],
            block_height: 0,
            mined_block_id: None,
            mined_block_height: None,
            mined_subtree_idx: None,
            parent_txids: vec![],
        })
        .collect();
    encode_create_batch(&items)
}

fn wait_for_settled_three_node_topology(nodes: &[&TestNode]) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut stable_since: Option<(u64, Instant)> = None;
    loop {
        let all_committed = nodes
            .iter()
            .all(|n| n.cluster.committed_topology_members().len() == 3);
        let counts_ok = nodes.iter().all(|n| {
            let table = n.cluster.shard_table();
            let t = table.read();
            let counts = t.shard_counts();
            counts.len() == 3 && counts.values().all(|&c| c > 0) && t.pending_handoff_count() == 0
        });
        let version = nodes
            .iter()
            .map(|n| n.cluster.shard_table_version())
            .max()
            .unwrap_or(0);
        if all_committed && counts_ok {
            match stable_since {
                Some((v, since)) if v == version => {
                    if since.elapsed() >= Duration::from_millis(500) {
                        return;
                    }
                }
                _ => stable_since = Some((version, Instant::now())),
            }
        } else {
            stable_since = None;
        }
        assert!(
            Instant::now() < deadline,
            "3-node topology never settled (committed={all_committed}, counts_ok={counts_ok})"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Collect `count` txid seeds that `node` is the MASTER for.
fn owned_seeds(node: &TestNode, count: usize) -> Vec<u32> {
    let self_id = node.cluster.self_id();
    let table = node.cluster.shard_table();
    let mut owned = Vec::with_capacity(count);
    let mut probe = 0u32;
    while owned.len() < count {
        let key = TxKey {
            txid: make_txid(probe),
        };
        if table.read().master_for_key(&key) == self_id {
            owned.push(probe);
        }
        probe += 1;
        assert!(
            probe < 5_000_000,
            "could not find {count} keys mastered by this node"
        );
    }
    owned
}

/// Indices into `nodes` whose LOCAL store still holds `txid`, read exactly the
/// way the Docker scenario reads it: `OP_GET_BATCH` with `FLAG_LOCAL_READ`, so
/// shard routing cannot silently answer from a different node.
///
/// Response layout: `[count:4]` then per item `[status:1][data_len:4][data]`.
fn local_holders(nodes: &[&TestNode], txid: &[u8; 32]) -> Vec<usize> {
    let mut holders = Vec::new();
    for (i, node) in nodes.iter().enumerate() {
        let resp = request_at(
            node.tcp_port,
            OP_GET_BATCH,
            FLAG_LOCAL_READ,
            encode_get_batch(FieldMask::ALL, std::slice::from_ref(txid)),
        );
        if resp.status != STATUS_OK || resp.payload.len() < 5 {
            continue;
        }
        let count = u32::from_le_bytes(resp.payload[0..4].try_into().unwrap());
        if count >= 1 && resp.payload[4] == 0 {
            holders.push(i);
        }
    }
    holders
}

/// Mark `seed`'s record mined on the longest chain at its master.
///
/// Sweep eligibility (`Engine::sweep_eligible_with_mined`) is
/// `all_spent && has_blocks && on_longest_chain`, so a record must be mined
/// before it can ever be a DAH-sweep candidate.
fn set_mined(master_port: u16, seed: u32, current_height: u32, retention: u32) {
    let mut payload = Vec::new();
    payload.extend_from_slice(&1u32.to_le_bytes()); // count
    payload.extend_from_slice(&42u32.to_le_bytes()); // block_id
    payload.extend_from_slice(&current_height.to_le_bytes()); // block_height
    payload.extend_from_slice(&0u32.to_le_bytes()); // subtree_idx
    payload.push(1); // on_longest_chain
    payload.push(0); // unset_mined
    payload.extend_from_slice(&current_height.to_le_bytes());
    payload.extend_from_slice(&retention.to_le_bytes());
    payload.extend_from_slice(&make_txid(seed));
    let resp = request_at(master_port, OP_SET_MINED_BATCH, 0, payload);
    assert_eq!(
        resp.status, STATUS_OK,
        "set_mined of seed {seed} must succeed (status {})",
        resp.status
    );
}

/// Spend every UTXO of `seed`'s record at its master and set a delete-at-height
/// in the past, so the record is a legitimate DAH-sweep candidate on every node
/// that holds it.
fn spend_to_all_spent(master_port: u16, seed: u32, current_height: u32, retention: u32) {
    let txid = make_txid(seed);
    let mut payload = Vec::new();
    // Shared header for OP_SPEND_BATCH: see codec::decode_spend_batch_checked.
    payload.extend_from_slice(&1u32.to_le_bytes()); // count
    payload.push(0); // ignore_conflicting
    payload.push(0); // ignore_locked
    payload.extend_from_slice(&current_height.to_le_bytes());
    payload.extend_from_slice(&retention.to_le_bytes());
    payload.extend_from_slice(&txid);
    payload.extend_from_slice(&0u32.to_le_bytes()); // vout
    payload.extend_from_slice(&utxo_hash_for(seed));
    payload.extend_from_slice(&[0u8; 36]); // spending_data
    let resp = request_at(master_port, OP_SPEND_BATCH, 0, payload);
    assert_eq!(
        resp.status, STATUS_OK,
        "spend of seed {seed} must succeed (status {})",
        resp.status
    );
}

/// **The `scenario_03` step-3.5 contract, in process.** A client
/// `OP_DELETE_BATCH` that returns `STATUS_OK` must leave the record on NO node:
/// not on the master that served it, and not on the replica that was shipped
/// the create.
///
/// The delete is acknowledged synchronously (`best_effort = false`), so
/// `STATUS_OK` IS the terminal condition — there is nothing to wait for. If the
/// replica still answers a `FLAG_LOCAL_READ` for the key after that ACK, the
/// delete was never propagated, and the two holders have permanently diverged:
/// nothing in the running system ever revisits the replica's copy.
#[test]
fn client_delete_removes_the_record_from_every_holder() {
    let node1 = create_cluster_node(861, &[]);
    let seed = [node1.swim_port];
    let node2 = create_cluster_node(862, &seed);
    let node3 = create_cluster_node(863, &seed);
    let nodes = [&node1, &node2, &node3];
    wait_for_settled_three_node_topology(&nodes);

    const COUNT: usize = 20;
    let seeds = owned_seeds(&node1, COUNT);

    let resp = request_at(node1.tcp_port, OP_CREATE_BATCH, 0, encode_batch(&seeds));
    assert_eq!(
        resp.status, STATUS_OK,
        "create batch must be accepted and replicated (status {})",
        resp.status
    );

    // Pre-condition: under RF = 2 each record lives on exactly two nodes. If
    // this does not hold the delete assertion below would be vacuous.
    let before: Vec<Vec<usize>> = seeds
        .iter()
        .map(|s| local_holders(&nodes, &make_txid(*s)))
        .collect();
    for (s, holders) in seeds.iter().zip(&before) {
        assert_eq!(
            holders.len(),
            2,
            "seed {s} must be on exactly 2 nodes before the delete, found {holders:?}"
        );
    }

    let txids: Vec<[u8; 32]> = seeds.iter().map(|s| make_txid(*s)).collect();
    let resp = request_at(
        node1.tcp_port,
        OP_DELETE_BATCH,
        0,
        encode_txid_batch(&txids, &[]),
    );
    let delete_status = resp.status;

    let after: Vec<Vec<usize>> = txids.iter().map(|t| local_holders(&nodes, t)).collect();

    shutdown_node(&node1);
    shutdown_node(&node2);
    shutdown_node(&node3);

    assert_eq!(
        delete_status, STATUS_OK,
        "delete batch must be accepted by the master (status {delete_status})"
    );

    let survivors: Vec<String> = seeds
        .iter()
        .zip(&before)
        .zip(&after)
        .filter(|((_, _), a)| !a.is_empty())
        .map(|((s, b), a)| format!("seed {s}: holders before={b:?} after={a:?}"))
        .collect();
    assert!(
        survivors.is_empty(),
        "{}/{COUNT} deleted records are still present on at least one node after the \
         delete was ACKed — a delete that reaches only the master leaves the replica \
         copy live forever (no background pass ever revisits it): {}",
        survivors.len(),
        survivors.join(" | ")
    );
}

/// The DAH sweep (`OP_PROCESS_EXPIRED_PRESERVATIONS`) is the pruner's path, and
/// it must also converge across holders: a record that is all-spent and past
/// its delete-at-height must end up gone from master AND replica.
///
/// This is the second half of the same contract. The sweep is driven per node
/// by the pruner, so this test fires it at EVERY node — if replica copies were
/// reclaimed by each node's own sweep, that would be enough to converge without
/// replicating anything.
#[test]
fn dah_sweep_removes_the_record_from_every_holder() {
    let node1 = create_cluster_node(871, &[]);
    let seed = [node1.swim_port];
    let node2 = create_cluster_node(872, &seed);
    let node3 = create_cluster_node(873, &seed);
    let nodes = [&node1, &node2, &node3];
    wait_for_settled_three_node_topology(&nodes);

    const COUNT: usize = 10;
    const HEIGHT: u32 = 900_000;
    const RETENTION: u32 = 100;
    let seeds = owned_seeds(&node1, COUNT);

    let resp = request_at(node1.tcp_port, OP_CREATE_BATCH, 0, encode_batch(&seeds));
    assert_eq!(resp.status, STATUS_OK, "create batch must be accepted");

    // Make every record a genuine sweep candidate: mined on the longest chain
    // and all-spent, which plants a DAH at `HEIGHT + RETENTION` — in the past
    // relative to the sweep height used below.
    for s in &seeds {
        set_mined(node1.tcp_port, *s, HEIGHT, RETENTION);
        spend_to_all_spent(node1.tcp_port, *s, HEIGHT, RETENTION);
    }

    let before: Vec<Vec<usize>> = seeds
        .iter()
        .map(|s| local_holders(&nodes, &make_txid(*s)))
        .collect();
    for (s, holders) in seeds.iter().zip(&before) {
        assert_eq!(
            holders.len(),
            2,
            "seed {s} must be on exactly 2 nodes before the sweep, found {holders:?}"
        );
    }

    // Fire the pruner at every node, well past every record's DAH.
    let sweep_height = HEIGHT + RETENTION + 1;
    // Per-node sweep INPUT: how many of these records each node's DAH index
    // offers as candidates. This separates "the replica never learned the
    // record was due" from "the replica saw it and declined to prune it".
    let dah_candidates: Vec<usize> = nodes
        .iter()
        .map(|n| {
            let due = n.engine.dah_index().range_query(sweep_height);
            seeds
                .iter()
                .filter(|s| {
                    due.contains(&TxKey {
                        txid: make_txid(**s),
                    })
                })
                .count()
        })
        .collect();
    let mut payload = Vec::with_capacity(8);
    payload.extend_from_slice(&sweep_height.to_le_bytes());
    payload.extend_from_slice(&RETENTION.to_le_bytes());
    let mut sweep_statuses = Vec::new();
    for node in &nodes {
        let resp = request_at(
            node.tcp_port,
            OP_PROCESS_EXPIRED_PRESERVATIONS,
            0,
            payload.clone(),
        );
        sweep_statuses.push(resp.status);
    }

    let after: Vec<Vec<usize>> = seeds
        .iter()
        .map(|s| local_holders(&nodes, &make_txid(*s)))
        .collect();

    shutdown_node(&node1);
    shutdown_node(&node2);
    shutdown_node(&node3);

    for (i, status) in sweep_statuses.iter().enumerate() {
        assert_eq!(
            *status, STATUS_OK,
            "sweep at node {i} must succeed (status {status})"
        );
    }

    let survivors: Vec<String> = seeds
        .iter()
        .zip(&before)
        .zip(&after)
        .filter(|((_, _), a)| !a.is_empty())
        .map(|((s, b), a)| format!("seed {s}: holders before={b:?} after={a:?}"))
        .collect();
    assert!(
        survivors.is_empty(),
        "{}/{COUNT} swept records survive on at least one node after every node ran the \
         DAH sweep — replica copies are never reclaimed, so a store's replica half grows \
         without bound. Per-node DAH candidates offered to the sweep: {dah_candidates:?} \
         (a non-zero entry for a node that kept its copy means the node SAW the record as \
         due and still declined to prune it): {}",
        survivors.len(),
        survivors.join(" | ")
    );
}
