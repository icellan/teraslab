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
use teraslab::index::{DahIndex, Index, TxKey};
use teraslab::locks::StripedLocks;
use teraslab::ops::engine::Engine;
use teraslab::protocol::codec::WireGetSpendItem;
use teraslab::protocol::codec::{
    WireCreateItem, decode_get_spend_response, encode_create_batch, encode_get_spend_batch,
};
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
    /// The node's engine — exposed so a reboot can assert the on-disk
    /// recovery directly (physical presence of every acked record),
    /// independent of cluster read routing.
    engine: Arc<Engine>,
    /// Real TCP port (test clients connect here, bypassing the proxy).
    real_tcp_port: u16,
    /// Real SWIM/UDP port — retained so the node can be REBOOTED over the
    /// same sockets (an on-disk boot reuses the identical proxy relay, so
    /// peers keep dialing the unchanged advertised endpoint).
    real_swim_port: u16,
    /// Proxy endpoints advertised to peers.
    proxy: ProxyEndpoints,
    /// Node identity + replication factor, kept for the reboot path.
    node_id: u64,
    rf: u8,
    /// The node's backing "disk". A `MemoryDevice::new` retains its buffer
    /// while the `Arc` is alive, so dropping the server/cluster (process
    /// death) and reconstructing an engine over THIS device models a crash
    /// followed by a boot from persisted on-disk state.
    data_dev: Arc<MemoryDevice>,
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

    // Registering spawns this node's proxy relay threads and returns the
    // endpoints peers dial. A reboot REUSES these (see
    // `reboot_proxied_node_from_disk`) — it does not re-register — so the
    // relay keeps pointing at the same real ports and peers never learn a
    // new address.
    let proxy = net.register(node_id, real_swim, real_tcp);

    let data_dev = Arc::new(MemoryDevice::new(32 * 1024 * 1024, 4096).unwrap());
    let alloc = SlotAllocator::new(data_dev.clone() as Arc<dyn BlockDevice>).unwrap();
    let index = Index::new(1000).unwrap();
    let dah = DahIndex::new();

    spawn_proxied_server(
        node_id,
        rf,
        seed_swim,
        data_dev,
        index,
        alloc,
        dah,
        real_tcp_port,
        real_swim_port,
        real_swim,
        proxy,
        0, // fresh boot: incarnation 0
    )
}

/// Boot a node from its persisted on-disk state after a crash.
///
/// The caller must have already shut the old instance down (freeing the real
/// sockets). This reconstructs the engine by RECOVERING the allocator and
/// REBUILDING the primary + secondary indexes from `old.data_dev` (the exact
/// production cold-start path), then restarts the coordinator + server on the
/// SAME node id, real ports, and proxy relay — a genuine reboot rather than a
/// replacement node. The SWIM incarnation is bumped so peers that marked the
/// crashed instance dead accept the rebooted one as alive.
fn reboot_proxied_node_from_disk(
    old: &ProxiedNode,
    seed_swim: &[std::net::SocketAddr],
) -> ProxiedNode {
    let data_dev = old.data_dev.clone();
    let dev_dyn = data_dev.clone() as Arc<dyn BlockDevice>;

    // Production cold start: recover the persisted allocator high-water, then
    // scan the device to rebuild the primary + DAH secondary indexes.
    let (alloc, origin) = teraslab::server::startup::recover_or_create_allocator(dev_dyn.clone())
        .expect("allocator must recover from the crashed node's device");
    assert_eq!(
        origin,
        teraslab::server::startup::AllocatorOrigin::Recovered,
        "the crashed node must have persisted its allocator before the crash \
         (else the device scan sees no records and the recovery is meaningless)"
    );
    let index = Index::rebuild(&*dev_dyn, &alloc).expect("rebuild primary index from device");
    let dah_index =
        Index::rebuild_secondary(&*dev_dyn, &alloc).expect("rebuild DAH secondary from device");

    // Wait for the old real ports to free up before rebinding them.
    let real_swim: std::net::SocketAddr =
        format!("127.0.0.1:{}", old.real_swim_port).parse().unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        let tcp_free =
            std::net::TcpListener::bind(format!("127.0.0.1:{}", old.real_tcp_port)).is_ok();
        let udp_free = std::net::UdpSocket::bind(real_swim).is_ok();
        if tcp_free && udp_free {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    spawn_proxied_server(
        old.node_id,
        old.rf,
        seed_swim,
        data_dev,
        index,
        alloc,
        dah_index,
        old.real_tcp_port,
        old.real_swim_port,
        real_swim,
        old.proxy,
        1, // rebooted instance: bump incarnation to refute stale "dead"
    )
}

/// Shared node-spawn used by both the fresh boot and the reboot path. Binds
/// the real sockets on the GIVEN ports and wires the engine, coordinator, and
/// server together; the proxy relay is passed in (never re-registered) so a
/// reboot keeps the same advertised endpoints.
#[allow(clippy::too_many_arguments)]
fn spawn_proxied_server(
    node_id: u64,
    rf: u8,
    seed_swim: &[std::net::SocketAddr],
    data_dev: Arc<MemoryDevice>,
    index: impl Into<teraslab::index::PrimaryBackend>,
    alloc: SlotAllocator,
    dah: impl Into<teraslab::index::DahBackend>,
    real_tcp_port: u16,
    real_swim_port: u16,
    real_swim: std::net::SocketAddr,
    proxy: ProxyEndpoints,
    persisted_incarnation: u64,
) -> ProxiedNode {
    let engine = Arc::new(Engine::new(
        data_dev.clone() as Arc<dyn BlockDevice>,
        index,
        alloc,
        StripedLocks::new(256),
        dah,
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
        // Short debounce keeps these in-process tests fast while still
        // exercising the W3.3 coalescing path.
        topology_debounce: Duration::from_millis(100),
        migration_pool_size: 4,
        migration_batch_size: 100,
        persisted_incarnation,
        cluster_id: TEST_CLUSTER_ID,
        reverse_heal_online: false,
        heal_deadline: Duration::from_secs(300),
        heal_deadline_action: teraslab::config::HealDeadlineAction::AlertAndHold,
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
    let server = Arc::new(Server::new(engine.clone(), config).with_cluster(running.clone()));
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
        engine,
        real_tcp_port,
        real_swim_port,
        proxy,
        node_id,
        rf,
        data_dev,
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
    // The inbound block tears down the relay connection. On a fast/loaded
    // host the socket may already be dead by the time we get here, so
    // `set_read_timeout` can itself fail — that IS the "connection torn
    // down" outcome the test asserts, so treat a failed set_read_timeout as
    // dead rather than unwrapping (which raced into a panic under CI load).
    let dead = match via_proxy.set_read_timeout(Some(Duration::from_secs(2))) {
        Ok(()) => matches!(via_proxy.read(&mut buf), Ok(0) | Err(_)),
        Err(_) => true,
    };
    assert!(
        dead,
        "established relay connection must be torn down by the TCP block"
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

/// One-way UDP drop (asymmetric partition): with 431→432 dropped, node
/// 432 stops hearing 431 entirely (no pings arrive, and 431's ACKs to
/// 432's own pings are dropped on the way back) and reaps it —
/// permanently, because nothing 431 sends can ever reach it again. 431
/// meanwhile keeps *receiving* 432's datagrams, so every suspicion it
/// raises from its own un-ACKed probes is refuted and 432 is re-admitted.
/// Healing the direction restores the symmetric view without restarting
/// anything.
///
/// The asymmetry has to be asserted as an ONGOING directional property,
/// never sampled once. `alive_node_count()` deliberately excludes SWIM
/// `Suspect` peers (G1 fail-closed write quorum) and 431's probes to 432
/// can NEVER be ACKed under this drop, so 431 re-suspects 432 on every
/// probe round and its count sawtooths 2 → 1 → 2 for as long as the drop
/// is installed. Measured over 30 s: 431 read 2 on only ~9% of samples
/// and re-admitted 432 nineteen separate times. An immediate
/// `assert_eq!(node1.cluster.alive_node_count(), 2)` after the bounded
/// wait on 432 therefore sampled a transient — that was this test's flake
/// (~2 in 6 locally, ~3 in 6 under CI load).
///
/// What is terminal AND asymmetric, and is what this test asserts:
///   * 432 never counts 431 alive again — nothing can reach it. Monotone:
///     sampling more only makes this harder to satisfy, never easier.
///   * 431 drops 432 and then RE-ADMITS it — a `< 2` → `2` transition
///     observed after the drop, which only the still-delivered 432→431
///     direction can produce.
///
/// Negative control: under a TWO-way drop 431 re-admits 432 zero times in
/// 30 s (it falls to 1 at ~0.3 s and stays there), so this predicate does
/// reject the symmetric partition it exists to distinguish.
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
    wait_until(
        || node2.cluster.alive_node_count() == 1,
        Duration::from_secs(20),
    )
    .unwrap_or_else(|_| {
        panic!(
            "node 432 should stop counting node 431 under the one-way drop\n{}\n{}",
            cluster_diag("node431", &node1),
            cluster_diag("node432", &node2),
        )
    });

    // Observe the asymmetric steady state (see the doc comment for why a
    // single sample cannot express it).
    //
    // 432's half is terminal and is checked on EVERY sample: its alive
    // count must stay at 1, and it must not shrink its committed topology
    // — the E-01 guard, peak=2 → activation quorum 2, which the 1-of-2
    // remnant can never reach on its own vote.
    //
    // 431's half is the liveness half: it must be seen dropping 432 and
    // then taking it back. Measured cadence: first re-admission 0.26 s
    // after the drop and 19 within 30 s; the slowest is one per ~5 s once
    // 432 has reaped 431 and only 432's backed-off seed JOINs still reach
    // it, so the 30 s deadline is >5x the worst observed gap. The window
    // also runs for a 3 s settle floor — past 432's 2 s suspicion timeout,
    // so its Suspect → Dead transition happens inside the window where the
    // no-regression and no-shrink checks are live.
    let observe_start = std::time::Instant::now();
    let settle = Duration::from_secs(3);
    let observe_deadline = observe_start + Duration::from_secs(30);
    let mut n1_dropped_432 = false;
    let mut n1_readmitted_432 = false;
    let mut n2_alive_regression: Option<usize> = None;
    let mut n2_shrunk_topology: Option<usize> = None;
    while std::time::Instant::now() < observe_deadline {
        let n2_alive = node2.cluster.alive_node_count();
        if n2_alive != 1 {
            n2_alive_regression = Some(n2_alive);
            break;
        }
        let n2_members = node2.cluster.committed_topology_members().len();
        if n2_members != 2 {
            n2_shrunk_topology = Some(n2_members);
            break;
        }
        if node1.cluster.alive_node_count() < 2 {
            n1_dropped_432 = true;
        } else if n1_dropped_432 {
            n1_readmitted_432 = true;
        }
        if n1_readmitted_432 && observe_start.elapsed() >= settle {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    assert_eq!(
        n2_alive_regression,
        None,
        "nothing node 431 sends can reach node 432 under the drop, so 432's \
         alive count must stay at 1\n{}\n{}",
        cluster_diag("node431", &node1),
        cluster_diag("node432", &node2),
    );
    // E-01 guard side-effect: the 1-of-2 remnant (peak=2 → quorum 2)
    // must NOT commit a shrunken single-node topology.
    assert_eq!(
        n2_shrunk_topology,
        None,
        "node 432 must not self-activate a 1-node topology (peak-derived quorum)\n{}",
        cluster_diag("node432", &node2),
    );
    // The reverse direction still passes: 431 keeps hearing 432 and takes
    // it back after its own un-ACKed probes drop it. Under a two-way drop
    // the re-admission never happens.
    assert!(
        n1_dropped_432 && n1_readmitted_432,
        "node 431 must keep hearing node 432 and re-admit it — the drop is \
         one-way (dropped={n1_dropped_432} readmitted={n1_readmitted_432})\n{}\n{}",
        cluster_diag("node431", &node1),
        cluster_diag("node432", &node2),
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
    let node3 = create_proxied_node(&net, 403, 2, &[node1.proxy.swim, node2.proxy.swim]);
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
    wait_until(
        || node1.cluster.alive_node_count() == 1,
        Duration::from_secs(20),
    )
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
        resp.status,
        STATUS_OK,
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

// ---------------------------------------------------------------------------
// C30 — combined PARTITION + CRASH + on-disk BOOT recovery
// ---------------------------------------------------------------------------

/// The three failure modes stacked on one node: it is PARTITIONED from the
/// majority, CRASHES while isolated, then BOOTS from its persisted on-disk
/// state and rejoins the healed cluster. Asserts it recovers to a CONSISTENT
/// state:
///
///   * **No lost acked writes** — every record acked before the crash is
///     physically present after the device-scan rebuild, with intact metadata
///     and utxo slot (asserted directly against the recovered engine).
///   * **No fabrication** — the rebuild invents no records.
///   * **Correct ownership / no dual-master** — after the cluster re-converges
///     to a single 3-node topology, no key is mastered by two nodes at once.
///   * **No divergence** — every acked record is still readable from its
///     post-heal master.
///
/// Combines the live partition + topology-change path of
/// `partitioned_minority_never_self_activates_topology` with a genuine on-disk
/// boot: [`reboot_proxied_node_from_disk`] recovers the persisted allocator
/// and rebuilds the primary + DAH indexes from the SAME `MemoryDevice`, then
/// restarts the coordinator + server on the same identity, ports, and proxy.
///
/// The "crash" is process death with a retained device buffer (a
/// `MemoryDevice::new` is non-volatile once written), so this exercises a
/// crash whose acked writes had reached durable storage — the on-disk BOOT is
/// what is under test, not sub-record write tearing (covered by the volatile
/// `simulate_power_loss` engine/redo tests). See the returned note.
#[test]
#[serial]
fn partition_then_crash_then_on_disk_boot_recovers_consistently() {
    let net = ProxyNet::new();
    let node1 = create_proxied_node(&net, 441, 2, &[]);
    let node2 = create_proxied_node(&net, 442, 2, &[node1.proxy.swim]);
    let node3 = create_proxied_node(&net, 443, 2, &[node1.proxy.swim, node2.proxy.swim]);

    // Full 3-node convergence.
    wait_until(
        || {
            [&node1, &node2, &node3]
                .iter()
                .all(|n| n.cluster.committed_topology_members().len() == 3)
        },
        Duration::from_secs(30),
    )
    .unwrap_or_else(|_| {
        panic!(
            "3-node cluster did not converge: {:?} {:?} {:?}",
            node1.cluster.committed_topology_members(),
            node2.cluster.committed_topology_members(),
            node3.cluster.committed_topology_members(),
        )
    });

    // Right after the topology commits, node1's LOCAL shard table can still
    // lag the committed term — every shard then resolves to `NodeId(0)` and
    // `is_master` answers `Transitioning`, never `Yes`. Wait until node1 has
    // actually applied its table and masters at least one shard before writing
    // to keys it owns.
    wait_until(
        || {
            (0..4096u32).any(|i| {
                matches!(
                    node1.cluster.is_master(&TxKey {
                        txid: make_txid(440_000 + i)
                    }),
                    MasterQueryResult::Yes
                )
            })
        },
        Duration::from_secs(30),
    )
    .expect("node1 should master at least one shard once its committed table applies");

    // Write records to keys node1 masters. Each ack means the primary copy is
    // durable on node1's device (plus one replica under RF=2). Seed bases are
    // spaced past the 8192-candidate search window so the keys are distinct.
    let written: Vec<([u8; 32], [u8; 32])> = (0..8u32)
        .map(|i| {
            let txid = find_key_mastered_by(&node1, 440_000 + i * 10_000);
            let hash = make_txid(441_500 + i);
            let mut stream = connect(node1.real_tcp_port);
            let resp = send_request(
                &mut stream,
                &RequestFrame {
                    request_id: 100 + i as u64,
                    op_code: OP_CREATE_BATCH,
                    flags: 0,
                    payload: encode_create_payload(&txid, &hash).into(),
                },
            );
            assert_eq!(
                resp.status, STATUS_OK,
                "pre-partition create for a node1-mastered key must be acked"
            );
            (txid, hash)
        })
        .collect();

    // Checkpoint node1's allocator so its on-disk state is recoverable by the
    // cold-start device scan (`recover_or_create_allocator` needs a persisted
    // header; without it the reboot would see a fresh allocator and scan zero
    // records — a false "loss").
    node1
        .engine
        .allocator()
        .lock()
        .persist()
        .expect("persist node1 allocator before crash");
    node1
        .data_dev
        .sync()
        .expect("sync node1 device before crash");

    // --- PARTITION node1 from {2,3}; the majority re-commits a 2-node topology. ---
    net.isolate(441, &[442, 443]);
    wait_until(
        || node1.cluster.alive_node_count() == 1,
        Duration::from_secs(20),
    )
    .expect("partitioned node1 should mark both peers dead");
    wait_until(
        || {
            node2.cluster.committed_topology_members().len() == 2
                && node3.cluster.committed_topology_members().len() == 2
        },
        Duration::from_secs(30),
    )
    .expect("majority side {2,3} should re-commit a 2-node topology");
    // The committed term advancing to the 2-node topology does not mean node2's
    // LOCAL shard table has applied it yet — until it does, every shard still
    // resolves to `NodeId(0)` and `is_master` answers `Transitioning`, never
    // `Yes` (the same lag guarded for node1 after the initial 3-node commit).
    // Wait until node2 actually masters a shard under the 2-node table before
    // searching for a key it owns.
    wait_until(
        || {
            (0..8192u32).any(|i| {
                matches!(
                    node2.cluster.is_master(&TxKey {
                        txid: make_txid(600_000 + i)
                    }),
                    MasterQueryResult::Yes
                )
            })
        },
        Duration::from_secs(30),
    )
    .expect("node2 should master at least one shard once the 2-node table applies");
    // No dual-master DURING the partition: a majority-mastered key is not `Yes`
    // on the isolated node.
    let majority_key = find_key_mastered_by(&node2, 600_000);
    assert!(
        !matches!(
            node1.cluster.is_master(&TxKey { txid: majority_key }),
            MasterQueryResult::Yes
        ),
        "isolated node1 must not claim mastership of a majority-side shard"
    );

    // --- CRASH node1 while isolated (process death; its device buffer survives). ---
    shutdown_node(&node1);

    // --- BOOT node1 from its on-disk state (recover allocator + rebuild index). ---
    let node1b = reboot_proxied_node_from_disk(&node1, &[node2.proxy.swim, node3.proxy.swim]);

    // (A) NO LOST ACKED WRITES — every pre-crash record is physically present
    //     after the device-scan rebuild, with intact metadata + slot. This is
    //     asserted against the recovered engine directly, so it holds
    //     regardless of cluster read routing.
    for (txid, hash) in &written {
        let k = TxKey { txid: *txid };
        assert!(
            node1b.engine.lookup(&k).is_some(),
            "acked pre-crash record missing after on-disk boot: {txid:?}",
        );
        let meta = node1b
            .engine
            .read_metadata(&k)
            .expect("recovered record metadata must be readable");
        let utxo_count = { meta.utxo_count };
        assert!(
            utxo_count >= 1,
            "recovered record must retain its utxo slot (count={utxo_count})",
        );
        let slot = node1b
            .engine
            .read_slot(&k, 0)
            .expect("recovered utxo slot must be readable");
        assert_eq!(
            &slot.hash, hash,
            "recovered utxo hash must match the acked write for {txid:?}",
        );
    }
    // (A') NO FABRICATION — a txid that was never written must be absent.
    assert!(
        node1b
            .engine
            .lookup(&TxKey {
                txid: make_txid(999_999)
            })
            .is_none(),
        "device rebuild must not fabricate records",
    );

    // --- HEAL + rejoin: the cluster re-converges to a single 3-node topology. ---
    net.heal_all();
    let nodes = [&node1b, &node2, &node3];
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
            "cluster did not re-converge to 3 nodes after crash+boot+heal: {:?} {:?} {:?}",
            node1b.cluster.committed_topology_members(),
            node2.cluster.committed_topology_members(),
            node3.cluster.committed_topology_members(),
        )
    });

    // (B) NO DUAL-MASTER — after reconvergence no key is mastered by two nodes
    //     at once. Sample a spread of keys; each may have at most one `Yes`.
    let no_dual = wait_until(
        || {
            (0..256u32).all(|i| {
                let k = TxKey {
                    txid: make_txid(700_000 + i),
                };
                nodes
                    .iter()
                    .filter(|n| matches!(n.cluster.is_master(&k), MasterQueryResult::Yes))
                    .count()
                    <= 1
            })
        },
        Duration::from_secs(30),
    );
    assert!(
        no_dual.is_ok(),
        "a key was mastered by two nodes after reconvergence (dual-master)"
    );

    // (C) NO DIVERGENCE — every acked record stays readable from its post-heal
    //     master (rebalance/migration may relocate it; poll the authority).
    for (txid, hash) in &written {
        let query = encode_get_spend_batch(&[WireGetSpendItem {
            txid: *txid,
            vout: 0,
            utxo_hash: *hash,
        }]);
        let mut read_ok = false;
        let read_agrees = wait_until(
            || {
                for node in nodes {
                    if !matches!(
                        node.cluster.is_master(&TxKey { txid: *txid }),
                        MasterQueryResult::Yes
                    ) {
                        continue;
                    }
                    let mut stream = connect(node.real_tcp_port);
                    let resp = send_request(
                        &mut stream,
                        &RequestFrame {
                            request_id: 900,
                            op_code: OP_GET_SPEND_BATCH,
                            flags: 0,
                            payload: query.clone().into(),
                        },
                    );
                    if resp.status != STATUS_OK {
                        return false;
                    }
                    match decode_get_spend_response(&resp.payload) {
                        Some(r) if r.len() == 1 && r[0].status == 0 => {
                            read_ok = true;
                            return true;
                        }
                        _ => return false,
                    }
                }
                false
            },
            Duration::from_secs(30),
        );
        assert!(
            read_agrees.is_ok() && read_ok,
            "acked record unreadable from its post-heal master (divergence/loss): {txid:?}",
        );
    }

    shutdown_node(&node1b);
    shutdown_node(&node2);
    shutdown_node(&node3);
}

// ---------------------------------------------------------------------------
// N-05 residual gap #3 — seeded delay/reorder on the SWIM (UDP) plane
// ---------------------------------------------------------------------------

/// Apply a moderate per-datagram delay plus seeded reorder to every
/// directed inter-node SWIM link of a 3-node cluster.
///
/// The delay (40 ms each way) and reorder (60% of datagrams pulled up to
/// 80 ms earlier) sit comfortably inside SWIM's failure-detection budget
/// (direct-probe timeout 100 ms, then indirect rounds at 200/400/800 ms
/// before Suspect), so a robust membership protocol must still:
///
/// 1. converge the full 3-node committed topology on every node despite
///    datagrams arriving late and out of order (incarnation numbers, not
///    arrival order, decide truth);
/// 2. NOT mark any peer permanently dead — after settling, every node
///    reports all 3 alive (no spurious false-dead from reordering);
/// 3. NOT let any node self-activate a shrunken topology — the committed
///    membership stays 3 on every node.
///
/// Determinism: all reorder decisions come from the fixture's fixed-seed
/// PRNG; correctness is asserted by polling for the converged state, not
/// by sleeping then reading.
#[test]
#[serial]
fn swim_converges_under_heavy_udp_delay_and_reorder() {
    let net = ProxyNet::new();
    // Inject delay+reorder on every directed link BEFORE the nodes start
    // gossiping, so bootstrap itself runs over the degraded plane.
    let ids = [441u64, 442, 443];
    let delay = Duration::from_millis(40);
    let window = Duration::from_millis(80);
    for &a in &ids {
        for &b in &ids {
            if a != b {
                net.delay_udp_one_way(a, b, delay);
                net.reorder_udp_one_way(a, b, 0.6, window);
            }
        }
    }

    let node1 = create_proxied_node(&net, 441, 2, &[]);
    let node2 = create_proxied_node(&net, 442, 2, &[node1.proxy.swim]);
    let node3 = create_proxied_node(&net, 443, 2, &[node1.proxy.swim, node2.proxy.swim]);
    let nodes = [&node1, &node2, &node3];

    // (1) Full 3-node convergence despite the degraded SWIM plane.
    wait_until(
        || {
            nodes
                .iter()
                .all(|n| n.cluster.committed_topology_members().len() == 3)
        },
        Duration::from_secs(45),
    )
    .unwrap_or_else(|_| {
        panic!(
            "3-node cluster must converge under delay+reorder: m1={:?} m2={:?} m3={:?}",
            node1.cluster.committed_topology_members(),
            node2.cluster.committed_topology_members(),
            node3.cluster.committed_topology_members(),
        )
    });

    // (2) After convergence, every node must see all 3 alive and hold
    // there — a delayed-but-not-dropped link must not produce a permanent
    // false-dead. Poll for the all-alive state, then confirm it is stable
    // across a further settle window.
    wait_until(
        || nodes.iter().all(|n| n.cluster.alive_node_count() == 3),
        Duration::from_secs(20),
    )
    .unwrap_or_else(|_| {
        panic!(
            "all nodes must see 3 alive under delay+reorder: {} | {} | {}",
            cluster_diag("node441", &node1),
            cluster_diag("node442", &node2),
            cluster_diag("node443", &node3),
        )
    });
    // Stability: alive==3 must remain true across several probe cycles.
    let stable = wait_until(
        || nodes.iter().any(|n| n.cluster.alive_node_count() != 3),
        Duration::from_secs(3),
    );
    assert!(
        stable.is_err(),
        "alive view flapped off 3 under delay+reorder: {} | {} | {}",
        cluster_diag("node441", &node1),
        cluster_diag("node442", &node2),
        cluster_diag("node443", &node3),
    );

    // (3) No node may have self-activated a shrunken topology.
    for n in nodes {
        assert_eq!(
            n.cluster.committed_topology_members().len(),
            3,
            "node {} must keep the 3-node topology under delay+reorder, got {:?}",
            n.cluster.self_id().0,
            n.cluster.committed_topology_members(),
        );
    }

    shutdown_node(&node1);
    shutdown_node(&node2);
    shutdown_node(&node3);
}

/// Detection-power test for the delay fault: prove the injected delay
/// actually perturbs SWIM failure-detection timing, not just "doesn't
/// crash".
///
/// A 2-node cluster converges over a clean plane. Then a delay LARGER
/// than the entire direct+indirect probe budget (which sums to roughly
/// 100+200+400+800+1600 ms ≈ 3.1 s before Suspect, plus the 2 s
/// suspicion timeout before Dead) is applied to BOTH directions of the
/// 451↔452 link, so every probe and ACK is stalled well past the point
/// where 452 must declare 451 dead. We assert 452's alive count
/// collapses to 1 (451 declared dead) — the delay alone, with zero
/// datagrams dropped, drives a topology-relevant state change.
///
/// Control: the symmetric reverse expectation. Before injecting the
/// delay we confirm the link is healthy (both see 2). The contrast
/// between "healthy → both see 2" and "heavy delay → 452 sees 1" is the
/// detection-power evidence: the same plane, same nodes, only the delay
/// magnitude changed.
///
/// Finally, clearing the delay must heal the dead view — proving the
/// effect was the transient delay, not a real loss.
#[test]
#[serial]
fn udp_delay_perturbs_failure_detection_observably() {
    let net = ProxyNet::new();
    let node1 = create_proxied_node(&net, 451, 2, &[]);
    let node2 = create_proxied_node(&net, 452, 2, &[node1.proxy.swim]);

    wait_until(
        || {
            node1.cluster.committed_topology_members().len() == 2
                && node2.cluster.committed_topology_members().len() == 2
        },
        Duration::from_secs(20),
    )
    .expect("2-node cluster should converge over a clean plane first");

    // Control observation: on the clean plane both nodes see 2 alive.
    wait_until(
        || node1.cluster.alive_node_count() == 2 && node2.cluster.alive_node_count() == 2,
        Duration::from_secs(10),
    )
    .expect("clean plane: both nodes must see 2 alive before the delay");

    // Inject a delay far past the full failure-detection budget on BOTH
    // directions of the link. No datagram is dropped — they are merely
    // stalled ~5 s, long past when 452 must give up on 451.
    let huge = Duration::from_millis(5000);
    net.delay_udp_one_way(451, 452, huge);
    net.delay_udp_one_way(452, 451, huge);

    // The delay alone drives 452 to declare 451 dead (alive → 1). This
    // would NOT happen on the pass-through link (asserted as the control
    // above), so the fault is observably perturbing timing.
    wait_until(
        || node2.cluster.alive_node_count() == 1,
        Duration::from_secs(15),
    )
    .unwrap_or_else(|_| {
        panic!(
            "heavy delay must drive 452 to mark 451 dead (alive==1), got {} | {}",
            cluster_diag("node451", &node1),
            cluster_diag("node452", &node2),
        )
    });

    // E-01 side-effect under the delay: the 1-of-2 remnant must NOT
    // self-activate a shrunken topology (peak=2 → activation quorum 2).
    assert_eq!(
        node2.cluster.committed_topology_members().len(),
        2,
        "node 452 must not self-activate a 1-node topology under the delay"
    );

    // Heal: clear the delay. Datagrams flow promptly again and the dead
    // view resurrects — proving the effect was the transient delay.
    net.clear_udp_timing(451, 452);
    net.clear_udp_timing(452, 451);
    wait_until(
        || node1.cluster.alive_node_count() == 2 && node2.cluster.alive_node_count() == 2,
        Duration::from_secs(30),
    )
    .expect("clearing the delay must heal the dead view back to 2 alive on both nodes");

    shutdown_node(&node1);
    shutdown_node(&node2);
}

/// Detection-power test for the per-node inbound TCP delay: a relayed
/// request through the proxy endpoint must take measurably longer with a
/// delay set than without, while client traffic on the real port (which
/// bypasses the relay) is unaffected.
///
/// Granularity note: the delay is applied per forwarded request frame at
/// the inbound relay (see `tests/net_proxy` module docs), so a single
/// PING round-trip incurs one delay quantum on the request leg.
#[test]
#[serial]
fn tcp_inbound_delay_slows_relayed_request_only() {
    let net = ProxyNet::new();
    let node1 = create_proxied_node(&net, 461, 2, &[]);

    // Baseline: PING through the proxy relay with no delay is fast.
    let mut via_proxy = connect(node1.proxy.tcp.port());
    let t0 = std::time::Instant::now();
    assert!(
        ping_ok(&mut via_proxy),
        "baseline relayed PING must succeed"
    );
    let baseline = t0.elapsed();
    drop(via_proxy);

    // Inject a 600 ms inbound delay on the request frame.
    let delay = Duration::from_millis(600);
    net.delay_tcp_inbound(461, delay);

    // A fresh relayed PING must now take at least the injected delay.
    let mut delayed = connect(node1.proxy.tcp.port());
    delayed
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let t1 = std::time::Instant::now();
    assert!(
        ping_ok(&mut delayed),
        "delayed relayed PING must still eventually succeed"
    );
    let delayed_rt = t1.elapsed();
    assert!(
        delayed_rt >= delay,
        "relayed PING under a {delay:?} delay must take at least that long, took {delayed_rt:?} (baseline {baseline:?})"
    );

    // Client traffic on the real port bypasses the relay and is fast even
    // while the relay delay is engaged.
    let mut direct = connect(node1.real_tcp_port);
    let t2 = std::time::Instant::now();
    assert!(ping_ok(&mut direct), "direct PING must succeed");
    let direct_rt = t2.elapsed();
    assert!(
        direct_rt < delay,
        "direct client PING must bypass the relay delay, took {direct_rt:?}"
    );

    // Clear the delay: relayed PING is fast again.
    net.delay_tcp_inbound(461, Duration::ZERO);
    let mut cleared = connect(node1.proxy.tcp.port());
    let t3 = std::time::Instant::now();
    assert!(ping_ok(&mut cleared), "PING after clearing TCP delay");
    let cleared_rt = t3.elapsed();
    assert!(
        cleared_rt < delay,
        "relayed PING after clearing the delay must be fast again, took {cleared_rt:?}"
    );

    shutdown_node(&node1);
}
