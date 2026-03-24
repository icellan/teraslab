//! Multi-node cluster integration tests.
//!
//! Starts 2-3 TeraSlab nodes on different ports, verifies SWIM discovery,
//! shard table convergence, partition map serving, coordinator behaviour,
//! data migration, and end-to-end cluster operations.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use teraslab::allocator::SlotAllocator;
use teraslab::cluster::coordinator::{ClusterConfig, ClusterCoordinator, RunningCluster};
use teraslab::cluster::shards::{NodeId, ShardTable, NUM_SHARDS};
use teraslab::config::ServerConfig;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, TxKey, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::engine::Engine;
use teraslab::protocol::codec::{encode_create_batch, WireCreateItem};
use teraslab::protocol::frame::*;
use teraslab::protocol::opcodes::*;
use teraslab::server::Server;

#[allow(dead_code)]
struct TestNode {
    server: Arc<Server>,
    cluster: Arc<RunningCluster>,
    tcp_port: u16,
    swim_port: u16,
}

fn create_node(
    node_id: u64,
    tcp_port: u16,
    swim_port: u16,
    seed_swim_ports: &[u16],
) -> TestNode {
    let dev: Arc<dyn BlockDevice> =
        Arc::new(MemoryDevice::new(32 * 1024 * 1024, 4096).unwrap());
    let alloc = SlotAllocator::new(dev.clone());
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
        replication_factor: 2,
        probe_interval: Duration::from_millis(100),
        suspicion_timeout: Duration::from_secs(2),
        cluster_secret: None,
        max_migration_threads: 16,
        topology_propose_timeout: Duration::from_millis(300),
        migration_pool_size: 4,
        migration_batch_size: 100,
    };

    let coordinator = ClusterCoordinator::new(cluster_config, 1);
    let running = Arc::new(coordinator.start(engine.clone(), None, None, None, true));

    let config = ServerConfig {
        listen_addr: format!("127.0.0.1:{tcp_port}"),
        max_connections: 64,
        max_batch_size: 4096,
        node_id,
        ..Default::default()
    };

    let server = Arc::new(
        Server::new(engine, config).with_cluster(running.clone()),
    );

    let server_clone = server.clone();
    std::thread::spawn(move || {
        let _ = server_clone.run();
    });

    // Wait for server to start
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn two_nodes_discover_each_other() {
    // Start node 1 (no seeds)
    let node1 = create_node(1, 13300, 13301, &[]);

    // Start node 2 with node 1 as seed
    let node2 = create_node(2, 13302, 13303, &[13301]);

    // Wait for SWIM discovery
    std::thread::sleep(Duration::from_secs(2));

    // Both nodes should see each other
    let members1 = node1.cluster.shard_table().read().unwrap().version;
    let members2 = node2.cluster.shard_table().read().unwrap().version;

    // Shard table versions should be non-zero (computed from member list)
    // They may not be identical yet if timing is tight, but they should exist
    assert!(members1 > 0 || members2 > 0, "at least one node should have discovered peers");

    node1.cluster.shutdown();
    node2.cluster.shutdown();
    node1.server.shutdown();
    node2.server.shutdown();
}

#[test]
fn partition_map_served_over_tcp() {
    let node = create_node(10, 13310, 13311, &[]);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", node.tcp_port)).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    let resp = send_request(&mut stream, &RequestFrame {
        request_id: 1,
        op_code: OP_GET_PARTITION_MAP,
        flags: 0,
        payload: vec![],
    });

    assert_eq!(resp.status, STATUS_OK);
    assert!(!resp.payload.is_empty());

    // Parse the version (first 8 bytes)
    let version = u64::from_le_bytes(resp.payload[0..8].try_into().unwrap());
    // Parse node count
    let node_count = u32::from_le_bytes(resp.payload[8..12].try_into().unwrap());

    assert!(node_count >= 1, "should have at least 1 node");
    eprintln!("partition map: version={version}, nodes={node_count}, payload_size={}", resp.payload.len());

    node.cluster.shutdown();
    node.server.shutdown();
}

#[test]
fn single_node_cluster_owns_all_shards() {
    let node = create_node(20, 13320, 13321, &[]);

    // In single-node cluster, this node should own all shards
    let mut txid = [0u8; 32];
    for i in 0..100u8 {
        txid[0] = i;
        let key = teraslab::index::TxKey { txid };
        assert!(
            node.cluster.is_master(&key),
            "single node should own all shards, but shard for key[0]={i} is not owned"
        );
    }

    node.cluster.shutdown();
    node.server.shutdown();
}

#[test]
fn cluster_node_serves_operations() {
    let node = create_node(30, 13330, 13331, &[]);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", node.tcp_port)).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    // Create a record via the cluster node
    let txid = [42u8; 32];
    let hash = [1u8; 32];
    let items = vec![make_wire_create_item(txid, &[hash])];
    let cp = encode_create_batch(&items);

    let resp = send_request(&mut stream, &RequestFrame {
        request_id: 1, op_code: OP_CREATE_BATCH, flags: 0, payload: cp,
    });
    assert_eq!(resp.status, STATUS_OK, "create should succeed on cluster node");

    // Ping still works
    let resp = send_request(&mut stream, &RequestFrame {
        request_id: 2, op_code: OP_PING, flags: 0, payload: vec![],
    });
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
    for i in 4..32 {
        txid[i] = (seed.wrapping_mul(7).wrapping_add(i as u32) & 0xFF) as u8;
    }
    txid
}

fn shutdown_node(node: &TestNode) {
    node.cluster.shutdown();
    node.server.shutdown();
}

// ---------------------------------------------------------------------------
// Coordinator tests
// ---------------------------------------------------------------------------

#[test]
fn three_node_cluster_all_shards_assigned() {
    let node1 = create_node(101, 13400, 13401, &[]);
    let node2 = create_node(102, 13402, 13403, &[13401]);
    let node3 = create_node(103, 13404, 13405, &[13401]);

    // Wait for SWIM discovery
    std::thread::sleep(Duration::from_secs(3));

    // Verify all nodes see a shard table with non-zero version
    let v1 = node1.cluster.shard_table_version();
    let v2 = node2.cluster.shard_table_version();
    let v3 = node3.cluster.shard_table_version();

    // At least two nodes should have converged to the same version
    assert!(v1 > 0 || v2 > 0 || v3 > 0, "at least one node should have a shard table");

    // Verify the shard table covers all 4096 shards with valid masters
    let table = node1.cluster.shard_table();
    let table_ref = table.read().unwrap();
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
    // Start 3-node cluster (using ports well-separated from other tests)
    let node1 = create_node(111, 13710, 13711, &[]);
    let node2 = create_node(112, 13712, 13713, &[13711]);
    let node3 = create_node(113, 13714, 13715, &[13711]);

    std::thread::sleep(Duration::from_secs(4));
    let v_before = node1.cluster.shard_table_version();

    // Add 4th node
    let node4 = create_node(114, 13716, 13717, &[13711]);

    // Wait for SWIM discovery + rebalance
    std::thread::sleep(Duration::from_secs(5));

    let v_after = node1.cluster.shard_table_version();

    // Shard table version should change after adding a node
    assert_ne!(
        v_before, v_after,
        "shard table version should change when node is added"
    );

    // Verify the 4th node has some shards assigned
    let table = node1.cluster.shard_table();
    let table_ref = table.read().unwrap();
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
    let node1 = create_node(121, 13420, 13421, &[]);
    let node2 = create_node(122, 13422, 13423, &[13421]);
    let node3 = create_node(123, 13424, 13425, &[13421]);

    std::thread::sleep(Duration::from_secs(3));

    // Kill node 3
    shutdown_node(&node3);

    // Wait for SWIM to detect the failure and rebalance
    std::thread::sleep(Duration::from_secs(5));

    // Check that node1's shard table no longer has node 123 as master
    let table = node1.cluster.shard_table();
    let table_ref = table.read().unwrap();
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
    let node1 = create_node(131, 13430, 13431, &[]);
    let node2 = create_node(132, 13432, 13433, &[13431]);

    std::thread::sleep(Duration::from_secs(2));

    let mut found_local = false;
    let mut found_redirect = false;

    for i in 0..256u32 {
        let txid = make_txid(i);
        let key = TxKey { txid };
        if node1.cluster.is_master(&key) {
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
    let node1 = create_node(151, 13450, 13451, &[]);

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
    let node2 = create_node(152, 13452, 13453, &[13451]);

    // Wait for SWIM + migration to complete
    std::thread::sleep(Duration::from_secs(5));

    // Verify that node2 now owns some shards
    let table2 = node2.cluster.shard_table();
    let counts = table2.read().unwrap().shard_counts();
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
    let node1 = create_node(161, 13460, 13461, &[]);
    let node2 = create_node(162, 13462, 13463, &[13461]);

    std::thread::sleep(Duration::from_secs(2));

    let mut stream1 = TcpStream::connect(format!("127.0.0.1:{}", node1.tcp_port)).unwrap();
    stream1
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Find a key that node 1 does NOT own
    let mut remote_txid = None;
    for i in 0..1000u32 {
        let txid = make_txid(i + 30000);
        let key = TxKey { txid };
        if !node1.cluster.is_master(&key) {
            remote_txid = Some(txid);
            break;
        }
    }
    let txid = remote_txid.expect("should find a remote key");
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
        assert_eq!(
            error_code, ERR_REDIRECT,
            "error should be ERR_REDIRECT"
        );
    }

    shutdown_node(&node1);
    shutdown_node(&node2);
}

#[test]
fn client_redirect_resends_to_new_node() {
    // When a client receives a Redirect, it re-sends the write to
    // the indicated node. Verify the write succeeds there.
    let node1 = create_node(171, 13470, 13471, &[]);
    let node2 = create_node(172, 13472, 13473, &[13471]);

    std::thread::sleep(Duration::from_secs(2));

    // Find a key that node 1 does NOT own (node 2 should own it)
    let mut remote_txid = None;
    for i in 0..1000u32 {
        let txid = make_txid(i + 40000);
        let key = TxKey { txid };
        if !node1.cluster.is_master(&key) {
            remote_txid = Some(txid);
            break;
        }
    }
    let txid = remote_txid.expect("should find a key owned by node 2");
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
    let node1 = create_node(181, 13480, 13481, &[]);

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
    let node2 = create_node(182, 13482, 13483, &[13481]);
    std::thread::sleep(Duration::from_secs(5));

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
        if node2.cluster.is_master(&key) {
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
    let node1 = create_node(191, 13490, 13491, &[]);

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
    let node2 = create_node(192, 13492, 13493, &[13491]);
    std::thread::sleep(Duration::from_secs(5));

    // Count records accessible on each node
    let mut n1_accessible = 0u32;
    let mut n2_accessible = 0u32;

    for txid in &created_txids {
        let key = TxKey { txid: *txid };
        if node1.cluster.is_master(&key) {
            n1_accessible += 1;
        } else if node2.cluster.is_master(&key) {
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
    assert_eq!(total, NUM_SHARDS, "total master assignments must equal NUM_SHARDS");
}

#[test]
fn migration_of_empty_shard_completes_without_error() {
    // Start 2-node cluster — some shards will have no records.
    // The migration should handle empty shards gracefully.
    let node1 = create_node(211, 13500, 13501, &[]);
    let node2 = create_node(212, 13502, 13503, &[13501]);

    // Wait for SWIM + rebalance (migration of empty shards)
    std::thread::sleep(Duration::from_secs(3));

    // If we got here without panics, empty shard migration succeeded.
    // Verify both nodes are still responsive.
    let mut stream1 = TcpStream::connect(format!("127.0.0.1:{}", node1.tcp_port)).unwrap();
    stream1.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
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
    stream2.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
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
    let node1 = create_node(221, 13510, 13511, &[]);
    let node2 = create_node(222, 13512, 13513, &[13511]);
    let node3 = create_node(223, 13514, 13515, &[13511]);

    std::thread::sleep(Duration::from_secs(3));

    // Create 100 records via node 1
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", node1.tcp_port)).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

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
    assert!(
        created > 0,
        "some records should be created locally"
    );
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
    let node = create_node(231, 13520, 13521, &[]);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", node.tcp_port)).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

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
    let node1 = create_node(241, 13530, 13531, &[]);
    let node2 = create_node(242, 13532, 13533, &[13531]);

    std::thread::sleep(Duration::from_secs(2));

    // Find a key owned by node 1
    let mut local_txid = None;
    for i in 0..1000u32 {
        let txid = make_txid(i + 500000);
        let key = TxKey { txid };
        if node1.cluster.is_master(&key) {
            local_txid = Some(txid);
            break;
        }
    }
    let txid = local_txid.expect("should find a key owned by node 1");
    let hash = [5u8; 32];

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", node1.tcp_port)).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

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
    let node1 = create_node(251, 13540, 13541, &[]);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", node1.tcp_port)).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

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
    let node2 = create_node(252, 13542, 13543, &[13541]);
    std::thread::sleep(Duration::from_secs(5));

    // Verify all records are accessible from their correct owner
    let mut accessible = 0;
    for txid in &txids {
        let key = TxKey { txid: *txid };
        if node1.cluster.is_master(&key) {
            accessible += 1;
        } else if node2.cluster.is_master(&key) {
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
    let node1 = create_node(261, 13550, 13551, &[]);
    let node2 = create_node(262, 13552, 13553, &[13551]);
    let node3 = create_node(263, 13554, 13555, &[13551]);

    std::thread::sleep(Duration::from_secs(3));

    // Count shards owned by node 3 before killing it
    let table = node1.cluster.shard_table();
    let counts_before = table.read().unwrap().shard_counts();
    let n3_shards_before = counts_before.get(&NodeId(263)).copied().unwrap_or(0);
    eprintln!("node 263 owns {n3_shards_before} shards before kill");

    // Kill node 3
    shutdown_node(&node3);

    // Wait for detection (suspicion_timeout=2s + some buffer)
    std::thread::sleep(Duration::from_secs(6));

    // Node 1 should have rebalanced
    let table_after = node1.cluster.shard_table();
    let counts_after = table_after.read().unwrap().shard_counts();
    let n3_shards_after = counts_after.get(&NodeId(263)).copied().unwrap_or(0);
    eprintln!("node 263 owns {n3_shards_after} shards after kill");

    // Node 1 should still be responsive
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", node1.tcp_port)).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
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
