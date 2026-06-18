//! Integration tests for P1.1 — `cluster_id` wired through the
//! topology authority.
//!
//! These tests run real in-process clusters (TCP + SWIM) to verify that:
//!
//! 1. A fresh 3-node cluster scales to 4 nodes when all four share a
//!    cluster_id, with no F-G8-001 ever-seen pre-seed — the original
//!    bug that P1.1 fixes.
//!
//! 2. Two independently bootstrapped 2-node clusters with DIFFERENT
//!    cluster_ids refuse each other's topology proposals at the
//!    follower-side `handle_propose` gate, so no cross-cluster commit
//!    can be laundered through quorum.
//!
//! Unit-level coverage for the matrix lives in
//! `tests/g8_split_brain.rs` and the `cluster::topology::tests` module.

#![allow(clippy::disallowed_macros)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use teraslab::allocator::SlotAllocator;
use teraslab::cluster::coordinator::{
    ClusterConfig, ClusterCoordinator, ReplicationRuntimeConfig, RunningCluster,
};
use teraslab::cluster::shards::NodeId;
use teraslab::cluster::topology::{ClusterId, TopologyTerm};
use teraslab::config::ServerConfig;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::engine::Engine;
use teraslab::server::Server;

struct TestNode {
    server: Arc<Server>,
    cluster: Arc<RunningCluster>,
    #[allow(dead_code)]
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

fn shutdown_node(node: &TestNode) {
    node.cluster.shutdown();
    node.server.shutdown();
}

fn wait_until<F: FnMut() -> bool>(mut cond: F, timeout: Duration) -> Result<(), ()> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if cond() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    if cond() { Ok(()) } else { Err(()) }
}

/// Spin up a single node with an explicit `cluster_id`. Mirrors
/// `cluster_tcp::create_node_full` but with the cluster_id surface
/// exposed so the two-cluster test below can hand each side a
/// different id.
fn create_node_with_cluster_id(
    node_id: u64,
    seed_swim_ports: &[u16],
    rf: u8,
    cluster_id: ClusterId,
) -> TestNode {
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
        // Short debounce keeps these in-process tests fast while still
        // exercising the W3.3 coalescing path.
        topology_debounce: Duration::from_millis(100),
        migration_pool_size: 4,
        migration_batch_size: 100,
        persisted_incarnation: 0,
        cluster_id,
        tombstone_gc_enabled: false,
        rejoin_grace_blocks: 100_000,
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
        // F-X-002: opt out of the strict_auth=true default so a
        // cluster_secret-less test cluster doesn't get fataled at
        // safe-defaults validation.
        strict_auth: false,
        ..Default::default()
    };

    let server = Arc::new(Server::new(engine, config).with_cluster(running.clone()));

    let server_clone = server.clone();
    std::thread::spawn(move || {
        let _ = server_clone.run();
    });

    // Wait for the SWIM UDP socket to bind.
    let swim_target: std::net::SocketAddr = format!("127.0.0.1:{swim_port}").parse().unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if let Some(s) = std::net::UdpSocket::bind("127.0.0.1:0").ok()
            && s.connect(swim_target).is_ok()
        {
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

const CLUSTER_A: ClusterId = ClusterId([0x11; 16]);
const CLUSTER_B: ClusterId = ClusterId([0x22; 16]);

/// P1.1 headline: with cluster_id properly wired, a fresh 3-node
/// cluster scales to 4 nodes without any F-G8-001 ever-seen
/// pre-seeding. Prior to the fix, on_membership_changed silently
/// rejected the third node's proposal because no committed voter had
/// ever seen that NodeId.
#[test]
fn scale_up_3_to_4_succeeds_without_pre_seed() {
    let node1 = create_node_with_cluster_id(411, &[], 2, CLUSTER_A);
    let node2 = create_node_with_cluster_id(412, &[node1.swim_port], 2, CLUSTER_A);
    let node3 = create_node_with_cluster_id(413, &[node1.swim_port], 2, CLUSTER_A);

    // Confirm cluster_id stamped on every node.
    assert_eq!(node1.cluster.topology_authority().cluster_id(), CLUSTER_A);
    assert_eq!(node2.cluster.topology_authority().cluster_id(), CLUSTER_A);
    assert_eq!(node3.cluster.topology_authority().cluster_id(), CLUSTER_A);

    // The 3-node topology must commit without intervention — this is
    // the case that previously hung at a 2-node commit because the
    // third node was "unseen".
    wait_until(
        || node1.cluster.committed_topology_members().len() == 3,
        Duration::from_secs(15),
    )
    .expect("3-node topology should commit on the seed node without ever-seen pre-seed");

    // Now add a 4th node sharing the same cluster_id.
    let node4 = create_node_with_cluster_id(
        414,
        &[node1.swim_port, node2.swim_port, node3.swim_port],
        2,
        CLUSTER_A,
    );

    wait_until(
        || node1.cluster.committed_topology_members().len() == 4,
        Duration::from_secs(15),
    )
    .expect("4-node topology should commit after the fourth join");

    let committed: std::collections::HashSet<u64> = node1
        .cluster
        .committed_topology_members()
        .iter()
        .map(|n| n.0)
        .collect();
    assert!(
        committed.contains(&411)
            && committed.contains(&412)
            && committed.contains(&413)
            && committed.contains(&414),
        "committed members must include all four configured nodes: got {committed:?}",
    );

    shutdown_node(&node1);
    shutdown_node(&node2);
    shutdown_node(&node3);
    shutdown_node(&node4);
}

/// Two independently bootstrapped clusters with distinct cluster_ids
/// must refuse each other's topology proposals. We verify this at the
/// authority level using a synthetic `TopologyTerm` carrying cluster B's
/// id against a node configured with cluster A's id — exactly the
/// scenario where a SWIM gossip leak (or an attacker who knows the
/// shared cluster_secret) tries to merge two unrelated clusters.
#[test]
fn two_distinct_cluster_ids_refuse_superset() {
    let node_a1 = create_node_with_cluster_id(511, &[], 2, CLUSTER_A);
    let node_a2 = create_node_with_cluster_id(512, &[node_a1.swim_port], 2, CLUSTER_A);

    let node_b1 = create_node_with_cluster_id(611, &[], 2, CLUSTER_B);
    let node_b2 = create_node_with_cluster_id(612, &[node_b1.swim_port], 2, CLUSTER_B);

    // Wait for both clusters to converge independently.
    wait_until(
        || node_a1.cluster.committed_topology_members().len() == 2,
        Duration::from_secs(15),
    )
    .expect("cluster A should reach a 2-node commit on its own");
    wait_until(
        || node_b1.cluster.committed_topology_members().len() == 2,
        Duration::from_secs(15),
    )
    .expect("cluster B should reach a 2-node commit on its own");

    // Confirm the two sides really do have different cluster_ids.
    let id_a = node_a1.cluster.topology_authority().cluster_id();
    let id_b = node_b1.cluster.topology_authority().cluster_id();
    assert_ne!(id_a, id_b);
    assert_eq!(id_a, CLUSTER_A);
    assert_eq!(id_b, CLUSTER_B);

    // Synthesize a TopologyTerm from cluster B's proposer with a
    // membership that LOOKS like a superset attack against cluster A.
    // The proposal carries cluster B's id; cluster A's topology
    // authority must reject it on the cluster_id mismatch alone,
    // without ever consulting the ever-seen heuristic.
    let merged_members = vec![NodeId(511), NodeId(512), NodeId(611), NodeId(612)];
    let foreign_propose = TopologyTerm::new(99, merged_members.clone(), NodeId(611), CLUSTER_B, 1);
    let vote = node_a1
        .cluster
        .topology_authority()
        .handle_propose(&foreign_propose);
    assert!(
        !vote.accepted,
        "cluster A must reject cluster B's proposal on cluster_id mismatch",
    );

    // Symmetric case: cluster B refuses a cluster A proposal.
    let foreign_propose_reverse = TopologyTerm::new(99, merged_members, NodeId(511), CLUSTER_A, 1);
    let vote_reverse = node_b1
        .cluster
        .topology_authority()
        .handle_propose(&foreign_propose_reverse);
    assert!(
        !vote_reverse.accepted,
        "cluster B must reject cluster A's proposal on cluster_id mismatch",
    );

    // The committed membership on each side must remain intact — no
    // poisoning from the rejected proposals.
    assert_eq!(node_a1.cluster.committed_topology_members().len(), 2);
    assert_eq!(node_b1.cluster.committed_topology_members().len(), 2);

    shutdown_node(&node_a1);
    shutdown_node(&node_a2);
    shutdown_node(&node_b1);
    shutdown_node(&node_b2);
}
