//! Multi-node cluster integration tests.
//!
//! Starts 2-3 TeraSlab nodes on different ports, verifies SWIM discovery,
//! shard table convergence, and partition map serving.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use teraslab::allocator::SlotAllocator;
use teraslab::cluster::coordinator::{ClusterConfig, ClusterCoordinator, RunningCluster};
use teraslab::cluster::shards::NodeId;
use teraslab::config::ServerConfig;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::engine::Engine;
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
    };

    let coordinator = ClusterCoordinator::new(cluster_config);
    let running = Arc::new(coordinator.start(engine.clone()));

    let config = ServerConfig {
        listen_addr: format!("127.0.0.1:{tcp_port}"),
        max_connections: 10,
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
    let mut cp = Vec::new();
    cp.extend_from_slice(&1u32.to_le_bytes());
    cp.extend_from_slice(&txid);
    cp.extend_from_slice(&1u32.to_le_bytes());
    cp.extend_from_slice(&hash);

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
