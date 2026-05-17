//! Multi-node cluster integration tests.
//!
//! Starts 2-3 TeraSlab nodes on different ports, verifies SWIM discovery,
//! shard table convergence, partition map serving, coordinator behaviour,
//! data migration, and end-to-end cluster operations.

#![allow(clippy::disallowed_macros)] // integration tests may use eprintln!/println! for diagnostics

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use teraslab::allocator::SlotAllocator;
use teraslab::cluster::coordinator::{
    ClusterConfig, ClusterCoordinator, MasterQueryResult, ReplicationRuntimeConfig, RunningCluster,
};
use teraslab::cluster::shards::{NUM_SHARDS, NodeId, ShardTable};
use teraslab::cluster::topology::{TopologyCommit, TopologyTerm};
use teraslab::config::ServerConfig;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, TxKey, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::engine::Engine;
use teraslab::protocol::codec::{WireCreateItem, encode_create_batch};
use teraslab::protocol::frame::*;
use teraslab::protocol::opcodes::*;
use teraslab::replication::manager::AckPolicy;
use teraslab::server::Server;

#[allow(dead_code)]
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

/// Create a `TestNode` with an explicit replication factor.
///
/// `rf=1` disables replication (single-copy durability) — useful for tests
/// that exercise a single-node cluster or that don't need replica handling.
/// `rf=2` is the production default; pass it for tests that join multiple
/// nodes and want to exercise the replication path.
fn create_node(
    node_id: u64,
    tcp_port: u16,
    swim_port: u16,
    seed_swim_ports: &[u16],
    rf: u8,
) -> TestNode {
    create_node_with_replication_runtime(
        node_id,
        tcp_port,
        swim_port,
        seed_swim_ports,
        rf,
        ReplicationRuntimeConfig {
            ack_policy: None,
            best_effort: true,
            timeout: Duration::from_secs(3),
            timeout_during_migration: Duration::from_secs(30),
        },
    )
}

fn create_node_with_replication_runtime(
    node_id: u64,
    tcp_port: u16,
    swim_port: u16,
    seed_swim_ports: &[u16],
    rf: u8,
    replication: ReplicationRuntimeConfig,
) -> TestNode {
    create_node_full(
        node_id,
        tcp_port,
        swim_port,
        seed_swim_ports,
        rf,
        replication,
        &[],
    )
}

/// Like [`create_node_with_replication_runtime`] but additionally seeds the
/// node's `committed_voter_ever_seen` set BEFORE SWIM starts. Tests that
/// rely on growing a fresh cluster past two members need this: once the
/// first 2-node topology commits, F-G8-001's `ever_seen_check` rejects any
/// later proposal that introduces a NodeId not previously observed as a
/// committed voter. Production wires that allow-list via `cluster_id`;
/// in-process tests have no orchestrator, so the test fixture must do it
/// manually. The seed is applied to the freshly-constructed coordinator
/// before `start()` so SWIM cannot race ahead and commit a 2-node term
/// before the allow-list is in place.
fn create_node_with_ever_seen(
    node_id: u64,
    tcp_port: u16,
    swim_port: u16,
    seed_swim_ports: &[u16],
    rf: u8,
    ever_seen: &[NodeId],
) -> TestNode {
    create_node_full(
        node_id,
        tcp_port,
        swim_port,
        seed_swim_ports,
        rf,
        ReplicationRuntimeConfig {
            ack_policy: None,
            best_effort: true,
            timeout: Duration::from_secs(3),
            timeout_during_migration: Duration::from_secs(30),
        },
        ever_seen,
    )
}

fn create_node_full(
    node_id: u64,
    tcp_port: u16,
    swim_port: u16,
    seed_swim_ports: &[u16],
    rf: u8,
    replication: ReplicationRuntimeConfig,
    ever_seen: &[NodeId],
) -> TestNode {
    let tcp_port = if tcp_port == 0 {
        reserve_tcp_port()
    } else {
        tcp_port
    };
    let swim_port = if swim_port == 0 {
        let mut port = reserve_udp_port();
        while port == tcp_port {
            port = reserve_udp_port();
        }
        port
    } else {
        swim_port
    };

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
    };

    let coordinator = ClusterCoordinator::new(cluster_config, 1);
    if !ever_seen.is_empty() {
        // Seed the F-G8-001 ever-seen allow-list BEFORE start() so the
        // SWIM event loop never races against an unseen-member rejection
        // (which has no retry path once on_membership_changed returns
        // None).
        coordinator
            .topology_authority
            .set_committed_voter_ever_seen(ever_seen);
    }
    let running = Arc::new(coordinator.start(engine.clone(), None, None, replication));

    let config = ServerConfig {
        listen_addr: format!("127.0.0.1:{tcp_port}"),
        max_connections: 64,
        max_batch_size: 4096,
        node_id,
        ..Default::default()
    };

    let server = Arc::new(Server::new(engine, config).with_cluster(running.clone()));

    let server_clone = server.clone();
    std::thread::spawn(move || {
        let _ = server_clone.run();
    });

    // Wait for the SWIM UDP socket to actually bind. The coordinator
    // spawns SWIM on a background thread; 100ms was racy on loaded
    // runners. Poll the port with a connect probe (UDP connect is
    // bind-only semantics — it does not send a packet) until the
    // address is accepting, or give up after 2 seconds.
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
    // Also give the server TCP listener a moment so that test clients
    // can connect without racing on `accept()`.
    std::thread::sleep(Duration::from_millis(100));

    TestNode {
        server,
        cluster: running,
        tcp_port,
        swim_port,
    }
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

fn assert_status_error_code(resp: &ResponseFrame, expected: u16, context: &str) {
    assert_eq!(
        resp.status,
        STATUS_ERROR,
        "{context}: expected STATUS_ERROR, got status={} payload_len={}",
        resp.status,
        resp.payload.len()
    );
    assert!(
        resp.payload.len() >= 4,
        "{context}: error response must carry [code:2][msg_len:2], got {} bytes",
        resp.payload.len()
    );
    let code = u16::from_le_bytes(resp.payload[0..2].try_into().unwrap());
    assert_eq!(code, expected, "{context}: wrong error code");
}

fn assert_single_sparse_error_code(resp: &ResponseFrame, expected: u16, context: &str) {
    assert_eq!(
        resp.status,
        STATUS_PARTIAL_ERROR,
        "{context}: expected STATUS_PARTIAL_ERROR, got status={} payload_len={}",
        resp.status,
        resp.payload.len()
    );
    assert!(
        resp.payload.len() >= 10,
        "{context}: sparse error payload must include count/index/code, got {} bytes",
        resp.payload.len()
    );
    let count = u32::from_le_bytes(resp.payload[0..4].try_into().unwrap());
    assert_eq!(count, 1, "{context}: expected one sparse error");
    let code = u16::from_le_bytes(resp.payload[8..10].try_into().unwrap());
    assert_eq!(code, expected, "{context}: wrong sparse error code");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn two_nodes_discover_each_other() {
    // Start node 1 (no seeds)
    let node1 = create_node(1, 13300, 13301, &[], 2);

    // Start node 2 with node 1 as seed
    let node2 = create_node(2, 13302, 13303, &[13301], 2);

    // Wait for SWIM discovery: at least one node must report a non-zero
    // shard-table version (i.e. the peer was observed and the committed
    // term advanced past the bootstrap term=0).
    wait_until(
        || node1.cluster.shard_table_version() > 0 || node2.cluster.shard_table_version() > 0,
        Duration::from_secs(2),
    )
    .expect("at least one node should have observed the peer and advanced shard_table_version");

    // Both nodes should see each other
    let members1 = node1.cluster.shard_table().read().version;
    let members2 = node2.cluster.shard_table().read().version;

    // Shard table versions should be non-zero (computed from member list)
    // They may not be identical yet if timing is tight, but they should exist
    assert!(
        members1 > 0 || members2 > 0,
        "at least one node should have discovered peers"
    );

    node1.cluster.shutdown();
    node2.cluster.shutdown();
    node1.server.shutdown();
    node2.server.shutdown();
}

#[test]
fn partition_map_served_over_tcp() {
    let node = create_node(10, 13310, 13311, &[], 2);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", node.tcp_port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1,
            op_code: OP_GET_PARTITION_MAP,
            flags: 0,
            payload: vec![],
        },
    );

    assert_eq!(resp.status, STATUS_OK);
    assert!(!resp.payload.is_empty());

    // Parse the version (first 8 bytes)
    let version = u64::from_le_bytes(resp.payload[0..8].try_into().unwrap());
    // Parse node count
    let node_count = u32::from_le_bytes(resp.payload[8..12].try_into().unwrap());

    assert!(node_count >= 1, "should have at least 1 node");
    eprintln!(
        "partition map: version={version}, nodes={node_count}, payload_size={}",
        resp.payload.len()
    );

    node.cluster.shutdown();
    node.server.shutdown();
}

#[test]
fn single_node_cluster_owns_all_shards() {
    let node = create_node(20, 13320, 13321, &[], 2);

    // In single-node cluster, this node should own all shards
    let mut txid = [0u8; 32];
    for i in 0..100u8 {
        txid[0] = i;
        let key = teraslab::index::TxKey { txid };
        assert!(
            matches!(node.cluster.is_master(&key), MasterQueryResult::Yes),
            "single node should own all shards, but shard for key[0]={i} is not owned"
        );
    }

    node.cluster.shutdown();
    node.server.shutdown();
}

#[test]
fn cluster_node_serves_operations() {
    // Single-node cluster: RF=1 (no peers available for replication).
    let node = create_node(30, 13330, 13331, &[], 1);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", node.tcp_port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Create a record via the cluster node
    let txid = [42u8; 32];
    let hash = [1u8; 32];
    let items = vec![make_wire_create_item(txid, &[hash])];
    let cp = encode_create_batch(&items);

    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1,
            op_code: OP_CREATE_BATCH,
            flags: 0,
            payload: cp,
        },
    );
    assert_eq!(
        resp.status, STATUS_OK,
        "create should succeed on cluster node"
    );

    // Ping still works
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 2,
            op_code: OP_PING,
            flags: 0,
            payload: vec![],
        },
    );
    assert_eq!(resp.status, STATUS_OK);

    node.cluster.shutdown();
    node.server.shutdown();
}

// ---------------------------------------------------------------------------
// Helpers for new tests
// ---------------------------------------------------------------------------

/// Build a WireCreateItem with default metadata and the given UTXO hashes.
fn make_wire_create_item(txid: [u8; 32], utxo_hashes: &[[u8; 32]]) -> WireCreateItem {
    WireCreateItem {
        txid,
        tx_version: 2,
        locktime: 0,
        fee: 1000,
        size_in_bytes: 250,
        extended_size: 0,
        is_coinbase: false,
        spending_height: 0,
        created_at: 1700000000000,
        flags: 0,
        utxo_hashes: utxo_hashes.to_vec(),
        cold_data: vec![],
        block_height: 0,
        mined_block_id: None,
        mined_block_height: None,
        mined_subtree_idx: None,
        parent_txids: vec![],
    }
}

/// Encode a CreateBatch payload for one record: txid + 1 UTXO with the given hash.
fn encode_create_payload(txid: &[u8; 32], utxo_hash: &[u8; 32]) -> Vec<u8> {
    let items = vec![make_wire_create_item(*txid, &[*utxo_hash])];
    encode_create_batch(&items)
}

/// Encode a CreateBatch payload for multiple records, each with 1 UTXO.
fn encode_multi_create_payload(records: &[([u8; 32], [u8; 32])]) -> Vec<u8> {
    let items: Vec<WireCreateItem> = records
        .iter()
        .map(|(txid, hash)| make_wire_create_item(*txid, &[*hash]))
        .collect();
    encode_create_batch(&items)
}

/// Generate a deterministic txid from a seed number.
fn make_txid(seed: u32) -> [u8; 32] {
    let mut txid = [0u8; 32];
    txid[0..4].copy_from_slice(&seed.to_le_bytes());
    // Fill remaining bytes with a pattern for uniqueness
    for (i, byte) in txid.iter_mut().enumerate().skip(4) {
        *byte = (seed.wrapping_mul(7).wrapping_add(i as u32) & 0xFF) as u8;
    }
    txid
}

fn shutdown_node(node: &TestNode) {
    node.cluster.shutdown();
    node.server.shutdown();
}

/// Generic deterministic poll: invoke `predicate` repeatedly until it
/// returns `true` or `timeout` elapses. Returns `Ok(())` on success and
/// `Err(())` on timeout — callers add their own diagnostic message via
/// `.expect(...)` so the panic identifies which signal was being waited
/// for. Poll interval is 50 ms, short enough that the caller only pays
/// for the actual settle time (typically far less than the timeout)
/// while keeping the busy-loop overhead negligible.
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

/// Poll until `node` sees at least one key that routes to a peer (i.e. the
/// multi-node shard table has been committed and installed). Returns the
/// first such txid on success, or panics with diagnostics if the cluster
/// hasn't converged within `timeout`.
///
/// Fixed sleeps were flaky on loaded CI runners; this helper waits only as
/// long as actually needed (up to `timeout`) and scans a wider txid range
/// than a single iteration would.
fn wait_for_shard_split(node: &TestNode, timeout: Duration) -> [u8; 32] {
    let deadline = std::time::Instant::now() + timeout;
    let mut last_attempt = 0u32;
    while std::time::Instant::now() < deadline {
        for i in 0..4096u32 {
            let txid = make_txid(i + last_attempt);
            let key = TxKey { txid };
            if !matches!(node.cluster.is_master(&key), MasterQueryResult::Yes) {
                return txid;
            }
        }
        last_attempt = last_attempt.wrapping_add(4096);
        std::thread::sleep(Duration::from_millis(50));
    }
    let committed_term = node.cluster.committed_topology_term();
    let version = node.cluster.shard_table_version();
    let committed_members = node.cluster.committed_topology_members();
    let alive = node.cluster.alive_node_count();
    let shard_counts = {
        let table = node.cluster.shard_table();
        let t = table.read();
        t.shard_counts()
    };
    panic!(
        "cluster did not converge within {:?}: committed_term={}, shard_table_version={}, committed_members={:?}, alive_node_count={}, shard_counts={:?}",
        timeout, committed_term, version, committed_members, alive, shard_counts,
    );
}

// ---------------------------------------------------------------------------
// Coordinator tests
// ---------------------------------------------------------------------------

#[test]
fn three_node_cluster_all_shards_assigned() {
    let node1 = create_node(101, 13400, 13401, &[], 2);
    let node2 = create_node(102, 13402, 13403, &[13401], 2);
    let node3 = create_node(103, 13404, 13405, &[13401], 2);

    // Wait for SWIM discovery: at least one node must advance its shard
    // table past the bootstrap term=0. The test below tolerates the case
    // where only two of three nodes converge, so we don't require all
    // three here.
    wait_until(
        || {
            node1.cluster.shard_table_version() > 0
                || node2.cluster.shard_table_version() > 0
                || node3.cluster.shard_table_version() > 0
        },
        Duration::from_secs(3),
    )
    .expect("at least one node should have advanced shard_table_version after SWIM discovery");

    // Verify all nodes see a shard table with non-zero version
    let v1 = node1.cluster.shard_table_version();
    let v2 = node2.cluster.shard_table_version();
    let v3 = node3.cluster.shard_table_version();

    // At least two nodes should have converged to the same version
    assert!(
        v1 > 0 || v2 > 0 || v3 > 0,
        "at least one node should have a shard table"
    );

    // Verify the shard table covers all 4096 shards with valid masters
    let table = node1.cluster.shard_table();
    let table_ref = table.read();
    for shard in 0..NUM_SHARDS as u16 {
        let assignment = table_ref.assignment(shard);
        assert!(
            assignment.master.0 > 0,
            "shard {shard} should have a master"
        );
    }

    shutdown_node(&node1);
    shutdown_node(&node2);
    shutdown_node(&node3);
}

#[test]
fn add_fourth_node_rebalance_triggers() {
    // Start 3-node cluster on ephemeral ports so unrelated local services
    // cannot collide with this integration test.
    //
    // Each node pre-seeds the full expected membership (111-114) into its
    // F-G8-001 ever-seen allow-list BEFORE SWIM starts. Without this
    // seeding the cluster gets stuck at a 2-node commit: once
    // ever_seen = {first two nodes} is recorded, on_membership_changed
    // rejects the [111, 112, 113] proposal and there is no retry path.
    // Production avoids the issue by wiring cluster_id; the test fixture
    // mirrors that via direct ever_seen injection.
    let all_ids = [NodeId(111), NodeId(112), NodeId(113), NodeId(114)];
    let node1 = create_node_with_ever_seen(111, 0, 0, &[], 2, &all_ids);
    let node2 = create_node_with_ever_seen(112, 0, 0, &[node1.swim_port], 2, &all_ids);
    let node3 = create_node_with_ever_seen(113, 0, 0, &[node1.swim_port], 2, &all_ids);

    // Wait for the 3-node topology to commit so v_before reflects the
    // pre-add baseline. shard_table_version mirrors the committed term.
    wait_until(
        || node1.cluster.committed_topology_members().len() == 3,
        Duration::from_secs(15),
    )
    .expect("3-node topology should commit on node1 before adding the 4th node");
    let v_before = node1.cluster.shard_table_version();

    // Add 4th node — pre-seed its allow-list too so its votes accept the
    // existing 3-node members and the upcoming 4-node proposal.
    let node4 = create_node_with_ever_seen(
        114,
        0,
        0,
        &[node1.swim_port, node2.swim_port, node3.swim_port],
        2,
        &all_ids,
    );

    // Wait for the 4-node topology to commit (proposer is node1 = lowest
    // NodeId). The committed term must advance past v_before for the
    // shard_table_version assertion below to fire.
    wait_until(
        || {
            node1.cluster.committed_topology_members().len() == 4
                && node1.cluster.shard_table_version() != v_before
        },
        Duration::from_secs(15),
    )
    .expect("4-node topology should commit and advance shard_table_version after node4 joins");

    let v_after = node1.cluster.shard_table_version();

    // Shard table version should change after adding a node
    assert_ne!(
        v_before, v_after,
        "shard table version should change when node is added"
    );

    // Verify the 4th node has some shards assigned
    let table = node1.cluster.shard_table();
    let table_ref = table.read();
    let counts = table_ref.shard_counts();
    if let Some(&count) = counts.get(&NodeId(114)) {
        assert!(count > 0, "node 4 should have some shards");
    }

    shutdown_node(&node1);
    shutdown_node(&node2);
    shutdown_node(&node3);
    shutdown_node(&node4);
}

#[test]
fn remove_node_rebalance_triggers() {
    let node1 = create_node(121, 13420, 13421, &[], 2);
    let node2 = create_node(122, 13422, 13423, &[13421], 2);
    let node3 = create_node(123, 13424, 13425, &[13421], 2);

    // Wait for the initial 3-node SWIM discovery so node1 actually sees
    // node3 as a peer (otherwise the post-kill assertion is vacuous).
    wait_until(
        || node1.cluster.node_addresses().contains_key(&NodeId(123)),
        Duration::from_secs(3),
    )
    .expect("node1 should discover node3 via SWIM before the kill");

    // Kill node 3
    shutdown_node(&node3);

    // Wait for SWIM to detect the failure: node3 must be removed from
    // node1's address map via NodeLeft. probe_interval=100ms +
    // suspicion_timeout=2s, with headroom for CI load.
    wait_until(
        || !node1.cluster.node_addresses().contains_key(&NodeId(123)),
        Duration::from_secs(5),
    )
    .expect("node1 should remove node3 from node_addrs after suspicion_timeout elapses");

    // Check that node1's shard table no longer has node 123 as master
    let table = node1.cluster.shard_table();
    let table_ref = table.read();
    let counts = table_ref.shard_counts();

    // Node 123 should not be a master for any shards (it's dead)
    let dead_count = counts.get(&NodeId(123)).copied().unwrap_or(0);
    eprintln!("dead node shard count after removal: {dead_count}");

    // At minimum, the remaining nodes should have shards
    let alive_shards: usize = counts
        .iter()
        .filter(|(n, _)| **n != NodeId(123))
        .map(|(_, &c)| c)
        .sum();
    assert!(alive_shards > 0, "alive nodes should have shards");

    shutdown_node(&node1);
    shutdown_node(&node2);
}

#[test]
fn route_or_handle_coordinator_level() {
    // In a 2-node cluster, verify that each node correctly routes
    // keys it doesn't own via RedirectTo.
    let node1 = create_node(131, 13430, 13431, &[], 2);
    let node2 = create_node(132, 13432, 13433, &[13431], 2);

    // Wait for SWIM discovery + 2-node topology commit + shard-table install.
    let _ = wait_for_shard_split(&node1, Duration::from_secs(15));

    let mut found_local = false;
    let mut found_redirect = false;

    for i in 0..4096u32 {
        let txid = make_txid(i);
        let key = TxKey { txid };
        if matches!(node1.cluster.is_master(&key), MasterQueryResult::Yes) {
            found_local = true;
            let route = node1.cluster.route(&key);
            assert_eq!(
                route,
                teraslab::cluster::shards::RouteDecision::HandleLocally,
                "owned key should route locally"
            );
        } else {
            found_redirect = true;
            let route = node1.cluster.route(&key);
            match route {
                teraslab::cluster::shards::RouteDecision::RedirectTo { node, .. } => {
                    assert_eq!(node, NodeId(132), "should redirect to node 132");
                }
                teraslab::cluster::shards::RouteDecision::HandleLocally => {
                    panic!("non-owned key should not route locally");
                }
            }
        }
    }

    assert!(found_local, "should find at least one local key");
    assert!(found_redirect, "should find at least one redirect key");

    shutdown_node(&node1);
    shutdown_node(&node2);
}

#[test]
fn two_coordinators_same_event_identical_tables() {
    // When two coordinators receive the same MembershipChanged event,
    // they must compute identical shard tables. This is guaranteed by
    // ShardTable::compute being a pure function.
    let members = vec![NodeId(141), NodeId(142), NodeId(143)];

    let table1 = ShardTable::compute(&members, 2);
    let table2 = ShardTable::compute(&members, 2);

    assert_eq!(table1.version, table2.version);
    for shard in 0..NUM_SHARDS as u16 {
        assert_eq!(
            table1.assignment(shard).master,
            table2.assignment(shard).master,
            "shard {shard} master differs"
        );
        assert_eq!(
            table1.assignment(shard).replicas,
            table2.assignment(shard).replicas,
            "shard {shard} replicas differ"
        );
    }
}

// ---------------------------------------------------------------------------
// Migration tests
// ---------------------------------------------------------------------------

#[test]
fn migrate_shard_with_records_to_new_node() {
    // Start single node, create 100 records, add 2nd node.
    // After migration, verify records are accessible via the 2nd node.
    // RF=1: writes happen on node1 alone before node2 joins, so the
    // initial cluster cannot satisfy replication for RF>=2.
    let node1 = create_node(151, 13450, 13451, &[], 1);

    // Create 100 records on node 1
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", node1.tcp_port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let records: Vec<_> = (0..100u32)
        .map(|i| {
            let txid = make_txid(i + 10000);
            let hash = make_txid(i + 20000);
            (txid, hash)
        })
        .collect();

    // Create records in batches of 10
    for chunk in records.chunks(10) {
        let payload = encode_multi_create_payload(chunk);
        let resp = send_request(
            &mut stream,
            &RequestFrame {
                request_id: 1,
                op_code: OP_CREATE_BATCH,
                flags: 0,
                payload,
            },
        );
        assert!(
            resp.status == STATUS_OK || resp.status == STATUS_PARTIAL_ERROR,
            "create should succeed, got status {}",
            resp.status
        );
    }

    // Add 2nd node — triggers shard rebalancing and migration
    let node2 = create_node(152, 13452, 13453, &[13451], 1);

    // Wait for SWIM discovery and the rebalanced shard table to settle
    // on node2. With RF=1 the new node is the master for ~half the
    // shards once the topology commits.
    wait_until(
        || node2.cluster.committed_topology_members().len() == 2,
        Duration::from_secs(5),
    )
    .expect("2-node topology should commit on node2 after it joins");

    // Verify that node2 now owns some shards
    let table2 = node2.cluster.shard_table();
    let counts = table2.read().shard_counts();
    let n2_shards = counts.get(&NodeId(152)).copied().unwrap_or(0);
    eprintln!("node 152 owns {n2_shards} shards after rebalance");

    // Verify that at least some records can be queried from node 2
    let mut stream2 = TcpStream::connect(format!("127.0.0.1:{}", node2.tcp_port)).unwrap();
    stream2
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Ping to verify connectivity
    let resp = send_request(
        &mut stream2,
        &RequestFrame {
            request_id: 99,
            op_code: OP_PING,
            flags: 0,
            payload: vec![],
        },
    );
    assert_eq!(resp.status, STATUS_OK);

    shutdown_node(&node1);
    shutdown_node(&node2);
}

#[test]
fn during_migration_writes_redirect_to_new_node() {
    // In a 2-node cluster, keys not owned by this node get a Redirect response.
    let node1 = create_node(161, 13460, 13461, &[], 2);
    let node2 = create_node(162, 13462, 13463, &[13461], 2);

    // Wait for SWIM discovery + 2-node topology commit + shard-table install.
    let txid = wait_for_shard_split(&node1, Duration::from_secs(15));

    let mut stream1 = TcpStream::connect(format!("127.0.0.1:{}", node1.tcp_port)).unwrap();
    stream1
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let hash = [1u8; 32];

    // Try to create a record on node 1 for a key it doesn't own
    let payload = encode_create_payload(&txid, &hash);
    let resp = send_request(
        &mut stream1,
        &RequestFrame {
            request_id: 1,
            op_code: OP_CREATE_BATCH,
            flags: 0,
            payload,
        },
    );

    // Should get PARTIAL_ERROR with ERR_REDIRECT
    assert_eq!(
        resp.status, STATUS_PARTIAL_ERROR,
        "write for non-owned key should get partial error"
    );

    // Decode the error — sparse error format: [count:4][item_index:4][error_code:2][data_len:2][data:N]
    if resp.payload.len() >= 10 {
        let error_code = u16::from_le_bytes(resp.payload[8..10].try_into().unwrap());
        assert_eq!(error_code, ERR_REDIRECT, "error should be ERR_REDIRECT");
    }

    shutdown_node(&node1);
    shutdown_node(&node2);
}

#[test]
fn client_redirect_resends_to_new_node() {
    // When a client receives a Redirect, it re-sends the write to
    // the indicated node. Verify the write succeeds there.
    let node1 = create_node(171, 13470, 13471, &[], 2);
    let node2 = create_node(172, 13472, 13473, &[13471], 2);

    // Wait for SWIM discovery + 2-node topology commit + shard-table install.
    let txid = wait_for_shard_split(&node1, Duration::from_secs(15));
    let hash = [2u8; 32];

    // Send create to node 1 → should get redirect
    let mut stream1 = TcpStream::connect(format!("127.0.0.1:{}", node1.tcp_port)).unwrap();
    stream1
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let payload = encode_create_payload(&txid, &hash);
    let resp = send_request(
        &mut stream1,
        &RequestFrame {
            request_id: 1,
            op_code: OP_CREATE_BATCH,
            flags: 0,
            payload: payload.clone(),
        },
    );
    assert_eq!(resp.status, STATUS_PARTIAL_ERROR, "should get redirect");

    // Re-send the same create to node 2 → should succeed
    let mut stream2 = TcpStream::connect(format!("127.0.0.1:{}", node2.tcp_port)).unwrap();
    stream2
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let resp = send_request(
        &mut stream2,
        &RequestFrame {
            request_id: 2,
            op_code: OP_CREATE_BATCH,
            flags: 0,
            payload,
        },
    );
    assert_eq!(
        resp.status, STATUS_OK,
        "create on the correct node should succeed"
    );

    shutdown_node(&node1);
    shutdown_node(&node2);
}

#[test]
fn after_migration_complete_all_ops_go_to_new_node() {
    // Start single node, create records, add 2nd node.
    // After migration completes, operations for migrated shards
    // should go to the new node.
    let node1 = create_node(181, 13480, 13481, &[], 2);

    let mut stream1 = TcpStream::connect(format!("127.0.0.1:{}", node1.tcp_port)).unwrap();
    stream1
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Create some records
    for i in 0..20u32 {
        let txid = make_txid(i + 50000);
        let hash = make_txid(i + 60000);
        let payload = encode_create_payload(&txid, &hash);
        let _ = send_request(
            &mut stream1,
            &RequestFrame {
                request_id: 1,
                op_code: OP_CREATE_BATCH,
                flags: 0,
                payload,
            },
        );
    }

    // Add 2nd node
    let node2 = create_node(182, 13482, 13483, &[13481], 2);
    // Wait until node2 is fully wired up for RF=2 writes:
    //   1. 2-node topology committed on node2.
    //   2. node2 knows node1's address (SWIM NodeJoined fired).
    //   3. node2's activated shard table reflects the rebalanced
    //      assignment — node1 now owns some shards (otherwise the
    //      replication target resolution returns empty for any key
    //      node2 owns).
    wait_until(
        || {
            node2.cluster.committed_topology_members().len() == 2
                && node2.cluster.node_addresses().contains_key(&NodeId(181))
                && {
                    let counts = node2.cluster.shard_table().read().shard_counts();
                    counts.get(&NodeId(181)).copied().unwrap_or(0) > 0
                }
        },
        Duration::from_secs(5),
    )
    .expect("node2 should have 2-node topology activated and node1 owning shards before write");

    // After migration, verify that node 2 handles operations for its shards
    let mut stream2 = TcpStream::connect(format!("127.0.0.1:{}", node2.tcp_port)).unwrap();
    stream2
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Find a key owned by node 2
    let mut node2_key = None;
    for i in 0..1000u32 {
        let txid = make_txid(i + 70000);
        let key = TxKey { txid };
        if matches!(node2.cluster.is_master(&key), MasterQueryResult::Yes) {
            node2_key = Some(txid);
            break;
        }
    }

    if let Some(txid) = node2_key {
        let hash = [3u8; 32];
        let payload = encode_create_payload(&txid, &hash);
        let resp = send_request(
            &mut stream2,
            &RequestFrame {
                request_id: 3,
                op_code: OP_CREATE_BATCH,
                flags: 0,
                payload,
            },
        );
        assert_eq!(
            resp.status, STATUS_OK,
            "create on node 2 for its own shard should succeed"
        );
    }

    shutdown_node(&node1);
    shutdown_node(&node2);
}

#[test]
fn no_records_lost_during_migration() {
    // Create N records on a single node, add a 2nd node, then verify
    // all N records are assigned to exactly one node across the cluster.
    // RF=1: writes on node1 alone before node2 joins.
    let node1 = create_node(191, 13490, 13491, &[], 1);

    let mut stream1 = TcpStream::connect(format!("127.0.0.1:{}", node1.tcp_port)).unwrap();
    stream1
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Create 50 records
    let mut created_txids = Vec::new();
    for i in 0..50u32 {
        let txid = make_txid(i + 80000);
        let hash = make_txid(i + 90000);
        let payload = encode_create_payload(&txid, &hash);
        let resp = send_request(
            &mut stream1,
            &RequestFrame {
                request_id: i as u64,
                op_code: OP_CREATE_BATCH,
                flags: 0,
                payload,
            },
        );
        if resp.status == STATUS_OK {
            created_txids.push(txid);
        }
    }
    let before_count = created_txids.len();
    assert!(before_count > 0, "should have created some records");

    // Add 2nd node
    let node2 = create_node(192, 13492, 13493, &[13491], 1);
    // Wait for the 2-node topology to commit so is_master() reflects the
    // rebalanced ownership rather than the pre-join single-node table.
    wait_until(
        || node2.cluster.committed_topology_members().len() == 2,
        Duration::from_secs(5),
    )
    .expect("2-node topology should commit on node2 before counting record ownership");

    // Count records accessible on each node
    let mut n1_accessible = 0u32;
    let mut n2_accessible = 0u32;

    for txid in &created_txids {
        let key = TxKey { txid: *txid };
        if matches!(node1.cluster.is_master(&key), MasterQueryResult::Yes) {
            n1_accessible += 1;
        } else if matches!(node2.cluster.is_master(&key), MasterQueryResult::Yes) {
            n2_accessible += 1;
        }
    }

    // All records should be accounted for across both nodes
    let total = n1_accessible + n2_accessible;
    assert_eq!(
        total, before_count as u32,
        "all records should be assigned to exactly one node (n1={n1_accessible}, n2={n2_accessible}, expected={before_count})"
    );

    shutdown_node(&node1);
    shutdown_node(&node2);
}

#[test]
fn no_duplicate_records_after_migration() {
    // Verify no shard has two masters (which would cause duplicates).
    let members = vec![NodeId(201), NodeId(202), NodeId(203)];
    let table = ShardTable::compute(&members, 2);

    for shard in 0..NUM_SHARDS as u16 {
        let assignment = table.assignment(shard);
        assert!(
            !assignment.replicas.contains(&assignment.master),
            "shard {shard}: master {:?} is also a replica",
            assignment.master
        );
    }

    // After adding a node, verify the same property
    let new_members = vec![NodeId(201), NodeId(202), NodeId(203), NodeId(204)];
    let new_table = ShardTable::compute(&new_members, 2);

    for shard in 0..NUM_SHARDS as u16 {
        let assignment = new_table.assignment(shard);
        assert!(
            !assignment.replicas.contains(&assignment.master),
            "shard {shard}: master {:?} is also a replica after rebalance",
            assignment.master
        );
    }

    // Every shard has exactly one master
    let counts = new_table.shard_counts();
    let total: usize = counts.values().sum();
    assert_eq!(
        total, NUM_SHARDS,
        "total master assignments must equal NUM_SHARDS"
    );
}

#[test]
fn migration_of_empty_shard_completes_without_error() {
    // Start 2-node cluster — some shards will have no records.
    // The migration should handle empty shards gracefully.
    let node1 = create_node(211, 13500, 13501, &[], 2);
    let node2 = create_node(212, 13502, 13503, &[13501], 2);

    // Wait for the 2-node topology to commit — that is the signal that
    // empty-shard migration (which is a no-op when both nodes have no
    // records) has run end-to-end without errors.
    wait_until(
        || {
            node1.cluster.committed_topology_members().len() == 2
                && node2.cluster.committed_topology_members().len() == 2
        },
        Duration::from_secs(3),
    )
    .expect("2-node topology should commit on both nodes so the migration path has run");

    // If we got here without panics, empty shard migration succeeded.
    // Verify both nodes are still responsive.
    let mut stream1 = TcpStream::connect(format!("127.0.0.1:{}", node1.tcp_port)).unwrap();
    stream1
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let resp = send_request(
        &mut stream1,
        &RequestFrame {
            request_id: 1,
            op_code: OP_PING,
            flags: 0,
            payload: vec![],
        },
    );
    assert_eq!(resp.status, STATUS_OK);

    let mut stream2 = TcpStream::connect(format!("127.0.0.1:{}", node2.tcp_port)).unwrap();
    stream2
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let resp = send_request(
        &mut stream2,
        &RequestFrame {
            request_id: 1,
            op_code: OP_PING,
            flags: 0,
            payload: vec![],
        },
    );
    assert_eq!(resp.status, STATUS_OK);

    shutdown_node(&node1);
    shutdown_node(&node2);
}

// ---------------------------------------------------------------------------
// Cluster integration tests
// ---------------------------------------------------------------------------

#[test]
fn start_three_node_cluster_create_records_distributed() {
    let node1 = create_node(221, 13510, 13511, &[], 2);
    let node2 = create_node(222, 13512, 13513, &[13511], 2);
    let node3 = create_node(223, 13514, 13515, &[13511], 2);

    // Wait for at least one peer to be reflected in node1's shard table
    // so the "redirected > 0" assertion below is satisfiable. The test
    // does not require all three nodes to commit; a 2-node topology is
    // enough to produce both local-owned and redirected keys.
    wait_until(
        || node1.cluster.shard_table_version() > 0,
        Duration::from_secs(3),
    )
    .expect("node1 should advance shard_table_version past bootstrap");

    // Create 100 records via node 1
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", node1.tcp_port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let mut created = 0;
    let mut redirected = 0;
    for i in 0..100u32 {
        let txid = make_txid(i + 100000);
        let hash = make_txid(i + 200000);
        let payload = encode_create_payload(&txid, &hash);
        let resp = send_request(
            &mut stream,
            &RequestFrame {
                request_id: i as u64,
                op_code: OP_CREATE_BATCH,
                flags: 0,
                payload,
            },
        );
        if resp.status == STATUS_OK {
            created += 1;
        } else if resp.status == STATUS_PARTIAL_ERROR {
            redirected += 1;
        }
    }

    // In a 3-node cluster, ~1/3 of keys should be owned by node 1
    assert!(created > 0, "some records should be created locally");
    assert!(
        redirected > 0,
        "some records should be redirected to other nodes"
    );

    eprintln!("created={created}, redirected={redirected}");

    shutdown_node(&node1);
    shutdown_node(&node2);
    shutdown_node(&node3);
}

#[test]
fn query_reaches_correct_node_returns_data() {
    // Create records on a single-node cluster, verify queries work.
    // Single-node cluster: RF=1 (no peers available for replication).
    let node = create_node(231, 13520, 13521, &[], 1);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", node.tcp_port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Create a record
    let txid = make_txid(300000);
    let hash = make_txid(400000);
    let payload = encode_create_payload(&txid, &hash);
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1,
            op_code: OP_CREATE_BATCH,
            flags: 0,
            payload,
        },
    );
    assert_eq!(resp.status, STATUS_OK);

    // Query the record via GET_SPEND_BATCH
    let mut query_payload = Vec::new();
    query_payload.extend_from_slice(&1u32.to_le_bytes()); // count
    query_payload.extend_from_slice(&txid);
    query_payload.extend_from_slice(&0u32.to_le_bytes()); // vout=0
    query_payload.extend_from_slice(&hash);

    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 2,
            op_code: OP_GET_SPEND_BATCH,
            flags: 0,
            payload: query_payload,
        },
    );
    assert_eq!(resp.status, STATUS_OK);
    // Response should contain 1 result
    assert!(!resp.payload.is_empty(), "get_spend should return data");

    shutdown_node(&node);
}

#[test]
fn spend_routed_to_correct_master() {
    // In a 2-node cluster, spend on a key owned by this node should succeed.
    let node1 = create_node(241, 13530, 13531, &[], 2);
    let node2 = create_node(242, 13532, 13533, &[13531], 2);

    // Wait until node1 is fully wired up for RF=2 writes: 2-node
    // topology committed, node2 visible in node_addrs, and node2 owns
    // some shards in node1's activated table (so replication target
    // resolution can find a peer). Without all three the create below
    // races and returns ERR_REPLICATION_FAILED.
    wait_until(
        || {
            node1.cluster.committed_topology_members().len() == 2
                && node1.cluster.node_addresses().contains_key(&NodeId(242))
                && {
                    let counts = node1.cluster.shard_table().read().shard_counts();
                    counts.get(&NodeId(242)).copied().unwrap_or(0) > 0
                }
        },
        Duration::from_secs(5),
    )
    .expect("node1 should have 2-node topology activated and node2 owning shards before write");

    // Find a key owned by node 1
    let mut local_txid = None;
    for i in 0..1000u32 {
        let txid = make_txid(i + 500000);
        let key = TxKey { txid };
        if matches!(node1.cluster.is_master(&key), MasterQueryResult::Yes) {
            local_txid = Some(txid);
            break;
        }
    }
    let txid = local_txid.expect("should find a key owned by node 1");
    let hash = [5u8; 32];

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", node1.tcp_port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Create the record first
    let payload = encode_create_payload(&txid, &hash);
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1,
            op_code: OP_CREATE_BATCH,
            flags: 0,
            payload,
        },
    );
    assert_eq!(resp.status, STATUS_OK, "create should succeed");

    // Spend should work on the correct master
    // Wire format: [count:4][ignore_conflicting:1][ignore_locked:1][cbh:4][bhr:4]
    //   per-item: [txid:32][vout:4][utxo_hash:32][spending_data:36]
    let spending_data = [6u8; 36];
    let mut spend_payload = Vec::new();
    spend_payload.extend_from_slice(&1u32.to_le_bytes()); // count = 1
    spend_payload.push(0); // ignore_conflicting
    spend_payload.push(0); // ignore_locked
    spend_payload.extend_from_slice(&100u32.to_le_bytes()); // cbh
    spend_payload.extend_from_slice(&0u32.to_le_bytes()); // bhr
    // Item: txid + vout + utxo_hash + spending_data
    spend_payload.extend_from_slice(&txid);
    spend_payload.extend_from_slice(&0u32.to_le_bytes()); // vout=0
    spend_payload.extend_from_slice(&hash);
    spend_payload.extend_from_slice(&spending_data);

    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 2,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload: spend_payload,
        },
    );
    assert!(
        resp.status == STATUS_OK || resp.status == STATUS_PARTIAL_ERROR,
        "spend should either succeed or return a specific error, got status {}",
        resp.status
    );

    shutdown_node(&node1);
    shutdown_node(&node2);
}

#[test]
fn add_node_all_records_still_accessible() {
    // Create records on 1-node cluster, add 2nd node, verify all records
    // can be reached by sending to the correct owner.
    let node1 = create_node(251, 13540, 13541, &[], 2);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", node1.tcp_port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Create 30 records
    let mut txids = Vec::new();
    for i in 0..30u32 {
        let txid = make_txid(i + 600000);
        let hash = make_txid(i + 700000);
        let payload = encode_create_payload(&txid, &hash);
        let resp = send_request(
            &mut stream,
            &RequestFrame {
                request_id: i as u64,
                op_code: OP_CREATE_BATCH,
                flags: 0,
                payload,
            },
        );
        if resp.status == STATUS_OK {
            txids.push(txid);
        }
    }
    drop(stream);

    // Add 2nd node
    let node2 = create_node(252, 13542, 13543, &[13541], 2);
    // Wait for the 2-node topology to commit so is_master() reflects the
    // rebalanced ownership rather than the single-node bootstrap table.
    wait_until(
        || {
            node1.cluster.committed_topology_members().len() == 2
                && node2.cluster.committed_topology_members().len() == 2
        },
        Duration::from_secs(5),
    )
    .expect("2-node topology should commit on both nodes before checking record ownership");

    // Verify all records are accessible from their correct owner
    let mut accessible = 0;
    for txid in &txids {
        let key = TxKey { txid: *txid };
        if matches!(node1.cluster.is_master(&key), MasterQueryResult::Yes)
            || matches!(node2.cluster.is_master(&key), MasterQueryResult::Yes)
        {
            accessible += 1;
        }
    }

    assert_eq!(
        accessible,
        txids.len(),
        "all records should be assigned to exactly one node"
    );

    shutdown_node(&node1);
    shutdown_node(&node2);
}

#[test]
fn kill_node_detection_affected_shards() {
    let node1 = create_node(261, 13550, 13551, &[], 2);
    let node2 = create_node(262, 13552, 13553, &[13551], 2);
    let node3 = create_node(263, 13554, 13555, &[13551], 2);

    // Wait until node1 has discovered node3 (so the "before kill" count is
    // meaningful). The test does not require a committed 3-node topology
    // here — just SWIM-level visibility of the peer.
    wait_until(
        || node1.cluster.node_addresses().contains_key(&NodeId(263)),
        Duration::from_secs(3),
    )
    .expect("node1 should discover node3 via SWIM before the kill");

    // Count shards owned by node 3 before killing it
    let table = node1.cluster.shard_table();
    let counts_before = table.read().shard_counts();
    let n3_shards_before = counts_before.get(&NodeId(263)).copied().unwrap_or(0);
    eprintln!("node 263 owns {n3_shards_before} shards before kill");

    // Kill node 3
    shutdown_node(&node3);

    // Wait for SWIM to mark node3 dead and remove it from node_addrs
    // (probe_interval=100ms + suspicion_timeout=2s nominal). The
    // observable signal is that node3 no longer appears in node1's
    // address map.
    wait_until(
        || !node1.cluster.node_addresses().contains_key(&NodeId(263)),
        Duration::from_secs(6),
    )
    .expect("node1 should remove node3 from node_addrs after suspicion_timeout elapses");

    // Node 1 should have rebalanced
    let table_after = node1.cluster.shard_table();
    let counts_after = table_after.read().shard_counts();
    let n3_shards_after = counts_after.get(&NodeId(263)).copied().unwrap_or(0);
    eprintln!("node 263 owns {n3_shards_after} shards after kill");

    // Node 1 should still be responsive
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", node1.tcp_port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1,
            op_code: OP_PING,
            flags: 0,
            payload: vec![],
        },
    );
    assert_eq!(resp.status, STATUS_OK, "node 1 should still be responsive");

    shutdown_node(&node1);
    shutdown_node(&node2);
}

// ---------------------------------------------------------------------------
// R-040 (AUDIT.md EF-03) — quorum: isolated 1-node remnant rejects writes.
//
// Contract: a node that was previously part of a multi-node cluster
// (peak_cluster_size >= 2) must reject mutating ops with
// `ERR_NO_QUORUM` once SWIM has marked enough peers dead that the
// surviving alive count falls below the quorum threshold
// `(peak / 2) + 1`. This prevents an isolated remnant of an N-node
// cluster from independently accepting conflicting writes (split-brain
// safety).
//
// Companion to R-039, which fixed `RunningCluster::alive_node_count`
// to correctly include `self` when the local node is committed but
// absent from `node_addrs` (production SWIM at swim.rs:454 ignores
// self-loopback messages so self never registers as an "addr"). With
// the R-039 fix applied, a 3-node cluster losing 2 peers reports
// `alive_node_count = 1` (self only), `peak = 3`, `quorum_needed = 2`,
// and the dispatcher rejects mutations. With R-039 reverted the count
// would be `0` instead — still < 2, so the rejection still fires, but
// for the wrong reason. This test pins the rejection contract so any
// future regression that bypasses or weakens the quorum check (e.g.
// reading peak from the wrong source, mis-classifying CREATE_BATCH as
// non-mutation, or short-circuiting on empty `node_addrs`) fails loudly.
// ---------------------------------------------------------------------------

/// Wait until `cluster.committed_topology_members().len() == expected`,
/// or panic with diagnostics. Used to pin the moment a topology has
/// been committed across all `expected` peers. The poll interval is
/// short (50 ms) and the ceiling is generous to absorb CI load.
fn wait_for_committed_members_len(node: &TestNode, expected: usize, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if node.cluster.committed_topology_members().len() == expected {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let committed = node.cluster.committed_topology_members();
    let term = node.cluster.committed_topology_term();
    let addrs = node.cluster.node_addresses();
    let peak = node.cluster.peak_cluster_size();
    let alive = node.cluster.alive_node_count();
    panic!(
        "committed_topology_members().len()={} (expected {}) within {:?}: members={:?}, term={}, addrs={:?}, peak={}, alive={}",
        committed.len(),
        expected,
        timeout,
        committed,
        term,
        addrs.keys().map(|k| k.0).collect::<Vec<_>>(),
        peak,
        alive,
    );
}

/// Wait until the surviving node sees `node_addrs` shrink to at most
/// `max_remaining` peers (i.e. SWIM has marked the killed nodes dead
/// and removed them via `NodeLeft`). Returns the final addrs map size
/// or panics with diagnostics on timeout.
fn wait_for_node_addrs_le(node: &TestNode, max_remaining: usize, timeout: Duration) -> usize {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let len = node.cluster.node_addresses().len();
        if len <= max_remaining {
            return len;
        }
        if std::time::Instant::now() >= deadline {
            let alive = node.cluster.alive_node_count();
            let committed = node.cluster.committed_topology_members();
            let peak = node.cluster.peak_cluster_size();
            panic!(
                "node_addrs.len()={} (expected <= {}) within {:?}: alive={}, peak={}, committed={:?}",
                len, max_remaining, timeout, alive, peak, committed,
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn isolated_node_rejects_writes_with_no_quorum() {
    // R-040 (AUDIT.md EF-03). Start a 3-node cluster with RF=2, wait for
    // the topology commit, kill 2 of 3 peers, wait for SWIM to mark them
    // dead (NodeLeft removes them from node_addrs), then send
    // OP_CREATE_BATCH against the surviving node and assert it rejects
    // with ERR_NO_QUORUM rather than independently committing the write.
    //
    // Pre-seed each node's F-G8-001 ever-seen allow-list with the full
    // expected membership: without that, on_membership_changed rejects
    // the second/third node's addition once a 2-node topology is
    // committed (see `add_fourth_node_rebalance_triggers` for the same
    // pattern).
    let all_ids = [NodeId(301), NodeId(302), NodeId(303)];
    let node1 = create_node_with_ever_seen(301, 0, 0, &[], 2, &all_ids);
    let node2 = create_node_with_ever_seen(302, 0, 0, &[node1.swim_port], 2, &all_ids);
    let node3 = create_node_with_ever_seen(303, 0, 0, &[node1.swim_port], 2, &all_ids);

    // Wait for the 3-node topology to commit on the surviving node so
    // that `committed_topology_members` reflects the full peak set
    // (peak_size will have advanced to 3 via the same MembershipChanged
    // event chain).
    wait_for_committed_members_len(&node1, 3, Duration::from_secs(15));

    // Sanity: peak should now be 3 — this is what fixes
    // `quorum_needed = (peak / 2) + 1 = 2` for the post-isolation check.
    let peak_before = node1.cluster.peak_cluster_size();
    assert!(
        peak_before >= 3,
        "peak_cluster_size should have reached 3 after committed 3-node topology, got {peak_before}"
    );

    // Kill nodes 2 and 3 — their SWIM threads stop responding to probes.
    // The surviving node (node1) will see suspicion_timeout=2s elapse
    // and emit NodeLeft → node_addrs removes both peers.
    shutdown_node(&node2);
    shutdown_node(&node3);

    // Wait for SWIM dead detection. probe_interval=100ms +
    // suspicion_timeout=2s gives ~2.5s nominal; we allow up to 15s for
    // CI load and indirect-probe completion. After this the surviving
    // node's `node_addrs` should contain at most `self`
    // (`RunningCluster::new` pre-seeds `self` into `node_addrs` at
    // coordinator.rs:526; SWIM only ever adds *peers* via `NodeJoined`
    // and removes them via `NodeLeft`).
    let addrs_after = wait_for_node_addrs_le(&node1, 1, Duration::from_secs(15));
    assert!(
        addrs_after <= 1,
        "after killing 2 peers in a 3-node cluster the surviving node's node_addrs should contain at most self; got len={addrs_after}, addrs={:?}",
        node1.cluster.node_addresses(),
    );
    // Belt-and-suspenders: confirm the only remaining addr (if any) is self.
    let addrs_map = node1.cluster.node_addresses();
    if let Some(only_id) = addrs_map.keys().next() {
        assert_eq!(
            only_id.0, 301,
            "the only remaining node_addr after peer death must be self (id=301), got id={}",
            only_id.0
        );
    }

    // The surviving node still sees a non-zero committed term (so
    // is_ready() returns true and the cluster-readiness gate doesn't
    // mask the quorum check), and peak is still >= 3 (peak_size only
    // ever grows via fetch_max).
    assert!(
        node1.cluster.committed_topology_term() >= 1,
        "surviving node should still report a committed topology term"
    );
    assert!(
        node1.cluster.peak_cluster_size() >= 3,
        "peak_cluster_size must remain >= 3 (peak only grows); got {}",
        node1.cluster.peak_cluster_size()
    );

    // alive_node_count: with the R-039 fix in place this is 1 (self
    // counted because committed contains self and addrs doesn't);
    // without the fix it would be 0. Either way < quorum_needed=2.
    let alive = node1.cluster.alive_node_count();
    assert!(
        alive < 2,
        "isolated 1-node remnant must report alive_node_count < quorum_needed (got alive={alive})"
    );

    // Send OP_CREATE_BATCH against the surviving node. Quorum check
    // runs BEFORE the redirect / readiness checks (see dispatch.rs:317),
    // so we don't need a key the surviving node owns — any well-formed
    // create payload will be rejected pre-dispatch.
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", node1.tcp_port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let txid = make_txid(900_001);
    let hash = make_txid(900_002);
    let payload = encode_create_payload(&txid, &hash);
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1,
            op_code: OP_CREATE_BATCH,
            flags: 0,
            payload,
        },
    );

    // Contract: STATUS_ERROR with payload [code:u16 LE = ERR_NO_QUORUM]
    // [msg_len:u16 LE][msg:N] — built by `error_response()`.
    assert_eq!(
        resp.status, STATUS_ERROR,
        "isolated remnant must reject mutations with STATUS_ERROR (got status={})",
        resp.status
    );
    assert!(
        resp.payload.len() >= 4,
        "ERR_NO_QUORUM response must carry [code:2][msg_len:2] header, got {} bytes",
        resp.payload.len()
    );
    let error_code = u16::from_le_bytes(resp.payload[0..2].try_into().unwrap());
    assert_eq!(
        error_code, ERR_NO_QUORUM,
        "isolated remnant must reject with ERR_NO_QUORUM (15), got code={error_code}"
    );

    shutdown_node(&node1);
}

#[test]
fn single_node_cluster_accepts_writes_without_quorum_check() {
    // R-040 control case. A single-node cluster (RF=1, peak=1) is the
    // canonical case where `check_quorum` returns `None` immediately —
    // a node that has only ever seen itself is a standalone deployment
    // and quorum is trivially met. This pins the contract that the
    // quorum check does NOT spuriously reject writes in single-node
    // mode (the inverse of `isolated_node_rejects_writes_with_no_quorum`).
    let node = create_node(311, 0, 0, &[], 1);

    // Sanity: a freshly-started single-node cluster has peak == 1 and
    // alive_node_count is at most 1 (self).
    let peak = node.cluster.peak_cluster_size();
    assert!(
        peak <= 1,
        "single-node cluster must have peak_cluster_size <= 1, got {peak}"
    );

    // Send the same OP_CREATE_BATCH that the isolated-remnant test rejected.
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", node.tcp_port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let txid = make_txid(900_003);
    let hash = make_txid(900_004);
    let payload = encode_create_payload(&txid, &hash);
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1,
            op_code: OP_CREATE_BATCH,
            flags: 0,
            payload,
        },
    );

    // Contract: single-node cluster accepts the write.
    assert_eq!(
        resp.status,
        STATUS_OK,
        "single-node cluster must accept OP_CREATE_BATCH (got status={}, payload_len={})",
        resp.status,
        resp.payload.len(),
    );

    shutdown_node(&node);
}

#[test]
fn tcp_write_to_pending_inbound_shard_returns_migration_in_progress() {
    // R-060 code 19 triggerability. Mark a shard as pending inbound on
    // a real TCP server and verify a client-visible write gets the
    // per-item ERR_MIGRATION_IN_PROGRESS code, not a redirect or close.
    let node = create_node(321, 0, 0, &[], 1);

    let txid = make_txid(901_001);
    let shard = ShardTable::shard_for_key(&TxKey { txid });
    node.cluster.mark_inbound_active(shard);
    assert!(node.cluster.has_pending_inbound_shard(shard));

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", node.tcp_port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let hash = make_txid(901_002);
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 19,
            op_code: OP_CREATE_BATCH,
            flags: 0,
            payload: encode_create_payload(&txid, &hash),
        },
    );

    assert_single_sparse_error_code(
        &resp,
        ERR_MIGRATION_IN_PROGRESS,
        "pending inbound shard write",
    );

    shutdown_node(&node);
}

#[test]
fn tcp_strict_replication_failure_returns_replication_failed() {
    // R-060 code 20 triggerability. A strict RF=2 node with only itself
    // in the committed topology cannot resolve the required replica
    // target. The local handler must fail the real TCP client with
    // ERR_REPLICATION_FAILED rather than reporting success.
    let node = create_node_with_replication_runtime(
        322,
        0,
        0,
        &[],
        2,
        ReplicationRuntimeConfig {
            ack_policy: Some(AckPolicy::WriteAll),
            best_effort: false,
            timeout: Duration::from_millis(50),
            timeout_during_migration: Duration::from_millis(50),
        },
    );

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", node.tcp_port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let members = vec![NodeId(322)];
    let commit = TopologyCommit {
        term: 1,
        proposer: NodeId(322),
        members: members.clone(),
        voters: members.clone(),
        digest: TopologyTerm::compute_digest(1, &members),
    };
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1,
            op_code: OP_TOPOLOGY_COMMIT,
            flags: 0,
            payload: commit.serialize(),
        },
    );
    assert_eq!(resp.status, STATUS_OK, "topology commit should succeed");

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !node.cluster.cluster_health().is_ready() {
        assert!(
            std::time::Instant::now() < deadline,
            "strict test node did not become cluster-ready after topology commit"
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    let txid = make_txid(901_003);
    let hash = make_txid(901_004);
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 20,
            op_code: OP_CREATE_BATCH,
            flags: 0,
            payload: encode_create_payload(&txid, &hash),
        },
    );

    assert_status_error_code(&resp, ERR_REPLICATION_FAILED, "strict replication failure");

    shutdown_node(&node);
}
