//! N-05 / E-01 — live-cluster chaos tests through the partition/loss
//! network proxy fixture (`tests/net_proxy`).
//!
//! Unlike `tests/cluster_tcp.rs` (clean full-node shutdown only) and
//! `tests/g8_split_brain.rs` (pure-function split-brain defenses), these
//! tests interpose a per-link proxy on every inter-node SWIM (UDP) and
//! TCP path of a live cluster and toggle drop/partition rules at
//! runtime.
//!
//! Headline test: `partitioned_minority_never_self_activates_topology`
//! is the live-partition regression test for E-01 (peak-derived
//! topology-activation quorum, `TopologyAuthority::activation_quorum_needed`).
//!
//! The chaos tests are `#[serial]`: they assert SWIM failure-detection
//! timing (100 ms probes, 2 s suspicion) on three concurrent in-process
//! nodes, and sharing cores with other multi-node tests makes those
//! windows flap.

#![allow(clippy::disallowed_macros)] // integration tests may use eprintln! for diagnostics

mod net_proxy;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use net_proxy::{ProxyEndpoints, ProxyNet};
use serial_test::serial;
use teraslab::allocator::SlotAllocator;
use teraslab::cluster::coordinator::{
    ClusterConfig, ClusterCoordinator, MasterQueryResult, ReplicationRuntimeConfig, RunningCluster,
};
use teraslab::cluster::shards::NodeId;
use teraslab::cluster::topology::ClusterId;
use teraslab::config::ServerConfig;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, TxKey, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::engine::Engine;
use teraslab::protocol::codec::{
    WireCreateItem, decode_get_spend_response, encode_create_batch, encode_get_spend_batch,
};
use teraslab::protocol::codec::WireGetSpendItem;
use teraslab::protocol::frame::*;
use teraslab::protocol::opcodes::*;
use teraslab::server::Server;

/// Same cluster id as `tests/cluster_tcp.rs` — P1.1 matching-cluster_id
/// fast path for membership-change safety.
const TEST_CLUSTER_ID: ClusterId = ClusterId([0xA5; 16]);

/// Shared HMAC secret: SWIM datagrams and inter-node TCP frames are
/// authenticated, proving the proxy forwards signed traffic verbatim.
const CLUSTER_SECRET: &str = "n05-partition-proxy-secret";

struct ProxiedNode {
    server: Arc<Server>,
    cluster: Arc<RunningCluster>,
    /// Real TCP port (test clients connect here, bypassing the proxy).
    real_tcp_port: u16,
    /// Proxy endpoints advertised to peers.
    proxy: ProxyEndpoints,
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

/// Create a node whose entire inter-node traffic (SWIM UDP + cluster
/// TCP) is routed through `net`'s per-node proxies: the node binds its
/// real sockets on private ports, advertises the proxy endpoints
/// (`swim_advertise_addr` / `self_addr`), and seeds at the *peers'*
/// proxy UDP endpoints.
fn create_proxied_node(
    net: &ProxyNet,
    node_id: u64,
    rf: u8,
    seed_swim: &[std::net::SocketAddr],
) -> ProxiedNode {
    let real_tcp_port = reserve_tcp_port();
    let mut real_swim_port = reserve_udp_port();
    while real_swim_port == real_tcp_port {
        real_swim_port = reserve_udp_port();
    }
    let real_tcp: std::net::SocketAddr = format!("127.0.0.1:{real_tcp_port}").parse().unwrap();
    let real_swim: std::net::SocketAddr = format!("127.0.0.1:{real_swim_port}").parse().unwrap();

    let proxy = net.register(node_id, real_swim, real_tcp);

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

    let cluster_config = ClusterConfig {
        self_id: NodeId(node_id),
        // Advertised TCP address = proxy endpoint; peers' replication,
        // migration, and topology RPC all dial through the proxy.
        self_addr: proxy.tcp,
        swim_bind: real_swim,
        swim_advertise_addr: Some(proxy.swim),
        seed_nodes: seed_swim.to_vec(),
        replication_factor: rf,
        probe_interval: Duration::from_millis(100),
        suspicion_timeout: Duration::from_secs(2),
        cluster_secret: Some(CLUSTER_SECRET.as_bytes().to_vec()),
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
        listen_addr: format!("127.0.0.1:{real_tcp_port}"),
        max_connections: 64,
        max_batch_size: 4096,
        node_id,
        // The server signs/verifies inter-node frames with the same
        // secret as the coordinator (ServerConfig carries its own copy).
        cluster_secret: Some(teraslab::config::Secret::new(CLUSTER_SECRET)),
        // Client-facing strictness is not under test; test clients send
        // unsigned frames on the real port.
        strict_auth: false,
        ..Default::default()
    };
    let server = Arc::new(Server::new(engine, config).with_cluster(running.clone()));
    let server_clone = server.clone();
    std::thread::spawn(move || {
        let _ = server_clone.run();
    });

    // Wait for the SWIM UDP socket to bind (same poll as cluster_tcp.rs).
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < deadline {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").ok();
        let bound = match probe {
            Some(s) => s.connect(real_swim).is_ok(),
            None => false,
        };
        if bound {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    std::thread::sleep(Duration::from_millis(100));

    ProxiedNode {
        server,
        cluster: running,
        real_tcp_port,
        proxy,
    }
}

fn shutdown_node(node: &ProxiedNode) {
    node.cluster.shutdown();
    node.server.shutdown();
}

/// Deterministic poll (same contract as `tests/cluster_tcp.rs`).
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

fn encode_create_payload(txid: &[u8; 32], utxo_hash: &[u8; 32]) -> Vec<u8> {
    encode_create_batch(&[make_wire_create_item(*txid, &[*utxo_hash])])
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

fn connect(port: u16) -> TcpStream {
    let stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
}

fn ping_ok(stream: &mut TcpStream) -> bool {
    let resp = send_request(
        stream,
        &RequestFrame {
            request_id: 7,
            op_code: OP_PING,
            flags: 0,
            payload: vec![].into(),
        },
    );
    resp.status == STATUS_OK
}

/// Diagnostic snapshot of a node's cluster view for panic messages.
fn cluster_diag(label: &str, node: &ProxiedNode) -> String {
    format!(
        "{label}: alive={} addrs={:?} committed_members={:?} term={} stv={}",
        node.cluster.alive_node_count(),
        node.cluster
            .node_addresses()
            .keys()
            .map(|k| k.0)
            .collect::<Vec<_>>(),
        node.cluster.committed_topology_members(),
        node.cluster.committed_topology_term(),
        node.cluster.shard_table_version(),
    )
}

/// Find a txid for which `node` reports `MasterQueryResult::Yes`.
fn find_key_mastered_by(node: &ProxiedNode, seed_base: u32) -> [u8; 32] {
    for i in 0..8192u32 {
        let txid = make_txid(seed_base + i);
        if matches!(
            node.cluster.is_master(&TxKey { txid }),
            MasterQueryResult::Yes
        ) {
            return txid;
        }
    }
    panic!(
        "no key mastered by node {} found in 8192 candidates (committed_members={:?})",
        node.cluster.self_id().0,
        node.cluster.committed_topology_members(),
    );
}

// ---------------------------------------------------------------------------
// N-05 fixture smoke tests
// ---------------------------------------------------------------------------

/// Two nodes converge with ALL inter-node traffic (HMAC-signed SWIM +
/// TCP) relayed through the proxy; the per-node TCP block kills both
/// established and new relay connections, leaves direct client traffic
/// untouched, and unblocking restores service.
#[test]
#[serial]
fn proxied_cluster_converges_and_tcp_block_partitions_inbound() {
    let net = ProxyNet::new();
    let node1 = create_proxied_node(&net, 421, 2, &[]);
    let node2 = create_proxied_node(&net, 422, 2, &[node1.proxy.swim]);

    // TCP relay carries real protocol traffic even before convergence:
    // PING through the proxy endpoint answers like the real port.
    let mut via_proxy_early = connect(node1.proxy.tcp.port());
    assert!(
        ping_ok(&mut via_proxy_early),
        "PING through TCP proxy relay (pre-convergence)"
    );
    drop(via_proxy_early);

    wait_until(
        || {
            node1.cluster.committed_topology_members().len() == 2
                && node2.cluster.committed_topology_members().len() == 2
        },
        Duration::from_secs(20),
    )
    .unwrap_or_else(|_| {
        panic!(
            "2-node proxied cluster should commit a 2-node topology on both nodes\n{}\n{}",
            cluster_diag("node421", &node1),
            cluster_diag("node422", &node2),
        )
    });

    // TCP relay carries real protocol traffic: PING through the proxy
    // endpoint answers exactly like the real port.
    let mut via_proxy = connect(node1.proxy.tcp.port());
    assert!(ping_ok(&mut via_proxy), "PING through TCP proxy relay");

    // Engage the inbound block: the established relay connection dies.
    net.block_tcp_inbound(421);
    let ping = RequestFrame {
        request_id: 8,
        op_code: OP_PING,
        flags: 0,
        payload: vec![].into(),
    }
    .encode();
    // The write may land in a buffer, but no response can ever arrive.
    let _ = via_proxy.write_all(&ping);
    let mut buf = [0u8; 4];
    via_proxy
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let read_result = via_proxy.read(&mut buf);
    let dead = matches!(read_result, Ok(0) | Err(_));
    assert!(
        dead,
        "established relay connection must be torn down by the TCP block, got {read_result:?}"
    );

    // New connections through the proxy are accepted then dropped: a
    // request never gets a response.
    let mut blocked = connect(node1.proxy.tcp.port());
    blocked
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let _ = blocked.write_all(&ping);
    let read_result = blocked.read(&mut buf);
    assert!(
        matches!(read_result, Ok(0) | Err(_)),
        "new relay connection must be dropped while blocked, got {read_result:?}"
    );

    // Direct client traffic (real port) is NOT affected by the block.
    let mut direct = connect(node1.real_tcp_port);
    assert!(
        ping_ok(&mut direct),
        "client traffic on the real port must bypass the inter-node TCP block"
    );

    // Unblock: relay service restored.
    net.unblock_tcp_inbound(421);
    let mut restored = connect(node1.proxy.tcp.port());
    assert!(ping_ok(&mut restored), "PING after unblocking TCP inbound");

    shutdown_node(&node1);
    shutdown_node(&node2);
}

/// One-way UDP drop (asymmetric partition): with 431→432 dropped,
/// node 432 stops hearing 431 (no pings arrive, its own pings get no
/// ACK back) and declares it dead, while 431 keeps seeing 432 alive
/// through 432's still-delivered probes. Healing the direction
/// resurrects the dead view without restarting anything.
#[test]
#[serial]
fn one_way_udp_drop_creates_asymmetric_partition_and_heals() {
    let net = ProxyNet::new();
    let node1 = create_proxied_node(&net, 431, 2, &[]);
    let node2 = create_proxied_node(&net, 432, 2, &[node1.proxy.swim]);

    wait_until(
        || {
            node1.cluster.committed_topology_members().len() == 2
                && node2.cluster.committed_topology_members().len() == 2
        },
        Duration::from_secs(20),
    )
    .expect("2-node proxied cluster should converge before the asymmetric drop");

    net.drop_udp_one_way(431, 432);

    // 432's failure detector fires: alive count collapses to self.
    wait_until(|| node2.cluster.alive_node_count() == 1, Duration::from_secs(20))
        .expect("node 432 should mark node 431 dead under the one-way drop");

    // The reverse direction still passes: 431 keeps hearing 432's
    // probes and holds it alive (suspicion from its own unACKed pings
    // is continuously refuted by 432's direct contact).
    assert_eq!(
        node1.cluster.alive_node_count(),
        2,
        "node 431 must still see node 432 alive — the drop is one-way"
    );

    // E-01 guard side-effect: the 1-of-2 remnant (peak=2 → quorum 2)
    // must NOT commit a shrunken single-node topology.
    assert_eq!(
        node2.cluster.committed_topology_members().len(),
        2,
        "node 432 must not self-activate a 1-node topology (peak-derived quorum)"
    );

    // Heal the direction: 431's traffic reaches 432 again and the dead
    // entry resurrects at the same incarnation.
    net.pass_udp_one_way(431, 432);
    wait_until(
        || node1.cluster.alive_node_count() == 2 && node2.cluster.alive_node_count() == 2,
        Duration::from_secs(30),
    )
    .expect("both nodes should see each other alive again after healing the drop");

    shutdown_node(&node1);
    shutdown_node(&node2);
}

// ---------------------------------------------------------------------------
// E-01 — live partition regression test (the audit's #1 follow-up)
// ---------------------------------------------------------------------------

/// 3-node cluster with a cluster_secret, every link through the proxy.
/// Partition node 1 from {2,3} and assert, after the SWIM suspicion
/// window:
///
/// 1. node 1 does NOT self-activate a new topology — its committed
///    topology stays the stale 3-node one at the same term, and it does
///    not become master of all shards (E-01 peak-derived activation
///    quorum: `max((proposal/2)+1, (peak/2)+1)` = 2 votes, but the
///    isolated remnant only ever has its own);
/// 2. a write sent to node 1 fails with `ERR_NO_QUORUM` (code 15) —
///    the peak-derived write gate;
/// 3. the majority side {2,3} re-commits a 2-node topology and still
///    accepts writes for shards it masters;
/// 4. after healing, node 1 rejoins (3-node topology re-commits on all
///    nodes) and the record written on the majority side during the
///    partition is still readable from its current master — no
///    divergence.
#[test]
#[serial]
fn partitioned_minority_never_self_activates_topology() {
    let net = ProxyNet::new();
    let node1 = create_proxied_node(&net, 401, 2, &[]);
    let node2 = create_proxied_node(&net, 402, 2, &[node1.proxy.swim]);
    let node3 = create_proxied_node(
        &net,
        403,
        2,
        &[node1.proxy.swim, node2.proxy.swim],
    );
    let nodes = [&node1, &node2, &node3];

    // Full 3-node convergence on every node.
    wait_until(
        || {
            nodes
                .iter()
                .all(|n| n.cluster.committed_topology_members().len() == 3)
        },
        Duration::from_secs(30),
    )
    .unwrap_or_else(|_| {
        panic!(
            "3-node proxied cluster did not converge: members1={:?} members2={:?} members3={:?}",
            node1.cluster.committed_topology_members(),
            node2.cluster.committed_topology_members(),
            node3.cluster.committed_topology_members(),
        )
    });
    let term_before = node1.cluster.committed_topology_term();
    assert!(
        node1.cluster.peak_cluster_size() >= 3,
        "peak_cluster_size must be >= 3 after the 3-node commit, got {}",
        node1.cluster.peak_cluster_size()
    );

    // Partition node 1 from {2,3}: SWIM dropped in both directions on
    // both links, inter-node TCP inbound to node 1 blocked.
    net.isolate(401, &[402, 403]);

    // Wait past the SWIM suspicion timeout on both sides of the cut:
    // node 1 sees only itself; the majority re-commits a 2-node topology.
    wait_until(|| node1.cluster.alive_node_count() == 1, Duration::from_secs(20))
        .expect("partitioned node 1 should mark both peers dead (alive_node_count == 1)");
    wait_until(
        || {
            node2.cluster.committed_topology_members().len() == 2
                && node3.cluster.committed_topology_members().len() == 2
        },
        Duration::from_secs(30),
    )
    .expect("majority side {2,3} should re-commit a 2-node topology");

    // (1) Bounded negative check: give node 1 a further grace window in
    // which a (buggy) self-activation would land — with the E-01 guard
    // it must never commit a shrunken topology. Under the sabotage
    // check (activation quorum derived from the live shrunken set
    // instead of the peak), node 1 self-commits a 1-node topology
    // within ~topology_propose_timeout of dead detection, well inside
    // this window, and this assertion fails.
    let self_activated = wait_until(
        || node1.cluster.committed_topology_members().len() < 3,
        Duration::from_secs(3),
    );
    assert!(
        self_activated.is_err(),
        "isolated minority self-activated a topology: members={:?} term={} (was {})",
        node1.cluster.committed_topology_members(),
        node1.cluster.committed_topology_term(),
        term_before,
    );
    assert_eq!(
        node1.cluster.committed_topology_term(),
        term_before,
        "isolated minority must not advance its committed topology term"
    );

    // Node 1 must not have become master of all shards: a key the
    // majority side masters must not be `Yes` on node 1.
    let majority_key = find_key_mastered_by(&node2, 910_000);
    assert!(
        !matches!(
            node1.cluster.is_master(&TxKey { txid: majority_key }),
            MasterQueryResult::Yes
        ),
        "partitioned node 1 claims mastership of a majority-side shard"
    );

    // (2) A write sent to node 1 returns ERR_NO_QUORUM (code 15).
    let mut stream1 = connect(node1.real_tcp_port);
    let resp = send_request(
        &mut stream1,
        &RequestFrame {
            request_id: 1,
            op_code: OP_CREATE_BATCH,
            flags: 0,
            payload: encode_create_payload(&make_txid(920_001), &make_txid(920_002)).into(),
        },
    );
    assert_eq!(
        resp.status, STATUS_ERROR,
        "minority write must fail outright (got status={})",
        resp.status
    );
    assert!(resp.payload.len() >= 4, "error payload must carry a code");
    let code = u16::from_le_bytes(resp.payload[0..2].try_into().unwrap());
    assert_eq!(
        code, ERR_NO_QUORUM,
        "minority write must be rejected with ERR_NO_QUORUM (15), got {code}"
    );

    // (3) The majority side still accepts writes for shards it masters.
    let majority_hash = make_txid(930_001);
    let mut stream2 = connect(node2.real_tcp_port);
    let resp = send_request(
        &mut stream2,
        &RequestFrame {
            request_id: 2,
            op_code: OP_CREATE_BATCH,
            flags: 0,
            payload: encode_create_payload(&majority_key, &majority_hash).into(),
        },
    );
    assert_eq!(
        resp.status, STATUS_OK,
        "majority-side write for a self-mastered shard must succeed (payload_len={})",
        resp.payload.len()
    );

    // (4) Heal the partition: node 1 rejoins without divergence.
    net.heal_all();
    wait_until(
        || {
            nodes
                .iter()
                .all(|n| n.cluster.committed_topology_members().len() == 3)
        },
        Duration::from_secs(60),
    )
    .unwrap_or_else(|_| {
        panic!(
            "cluster did not re-converge to 3 nodes after heal: members1={:?} members2={:?} members3={:?}",
            node1.cluster.committed_topology_members(),
            node2.cluster.committed_topology_members(),
            node3.cluster.committed_topology_members(),
        )
    });

    // The partition-era record must be readable from its current master
    // (post-heal rebalance/migration may move it; poll until the
    // authoritative copy answers).
    let query = encode_get_spend_batch(&[WireGetSpendItem {
        txid: majority_key,
        vout: 0,
        utxo_hash: majority_hash,
    }]);
    let mut read_ok = false;
    let read_agrees = wait_until(
        || {
            for node in nodes {
                if !matches!(
                    node.cluster.is_master(&TxKey { txid: majority_key }),
                    MasterQueryResult::Yes
                ) {
                    continue;
                }
                let mut stream = connect(node.real_tcp_port);
                let resp = send_request(
                    &mut stream,
                    &RequestFrame {
                        request_id: 3,
                        op_code: OP_GET_SPEND_BATCH,
                        flags: 0,
                        payload: query.clone().into(),
                    },
                );
                if resp.status != STATUS_OK {
                    return false;
                }
                let results = match decode_get_spend_response(&resp.payload) {
                    Some(r) => r,
                    None => return false,
                };
                if results.len() == 1 && results[0].status == 0 {
                    read_ok = true;
                    return true;
                }
                return false;
            }
            false
        },
        Duration::from_secs(30),
    );
    assert!(
        read_agrees.is_ok() && read_ok,
        "partition-era record must be readable from its post-heal master (no divergence)"
    );

    shutdown_node(&node1);
    shutdown_node(&node2);
    shutdown_node(&node3);
}
