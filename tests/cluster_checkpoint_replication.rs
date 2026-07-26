//! In-process regression coverage for a clustered SEGMENT node serving
//! replicated writes — the combination the Docker cluster E2E suite is the only
//! other thing that exercises.
//!
//! Two gaps this closes:
//!
//! 1. **Replication-ring deadlock (the nightly failure).** With RF > 1 every
//!    node is simultaneously a master for its own shards and a replica for a
//!    peer's. `tests/cluster_tcp.rs` only ever drives writes at ONE node at a
//!    time, so that ring is never closed and a mutation handler that holds the
//!    engine-wide visibility barrier across its replication round-trip looks
//!    perfectly healthy. [`replicated_creates_do_not_deadlock_across_a_replication_ring`]
//!    closes the ring.
//! 2. **The checkpoint/compaction loop.** `tests/cluster_tcp.rs` never attaches
//!    `spawn_checkpoint_task`, so nothing at the Rust level ran
//!    checkpoint + defrag concurrently with `OP_REPLICA_BATCH` traffic.

#![allow(clippy::disallowed_macros)] // integration tests may use eprintln! for diagnostics

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use teraslab::checkpoint::{CheckpointConfig, spawn_checkpoint_task};
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
use teraslab::protocol::codec::{WireCreateItem, encode_create_batch};
use teraslab::protocol::frame::{RequestFrame, ResponseFrame};
use teraslab::protocol::opcodes::{OP_CREATE_BATCH, STATUS_OK};
use teraslab::redo::RedoLog;
use teraslab::segment_allocator::SegmentAllocator;
use teraslab::server::Server;

const TEST_CLUSTER_ID: ClusterId = ClusterId([0xA5; 16]);

/// Segment size for the test nodes: 16 record slots at the 4 KiB test record
/// size, so a few create batches seal several segments and the checkpoint's
/// defrag pass has a realistic layout to work over.
const TEST_SEGMENT_SIZE: u64 = 16 * 4096;

/// The production foreground replication ACK timeout
/// (`ServerConfig::replication_timeout_ms` default). A node that stalls the
/// replica-apply path for longer than this fails every replicated write.
const ACK_TIMEOUT: Duration = Duration::from_secs(3);

struct TestNode {
    server: Arc<Server>,
    cluster: Arc<RunningCluster>,
    engine: Arc<Engine>,
    tcp_port: u16,
    swim_port: u16,
    snapshot_path: std::path::PathBuf,
    checkpoint_shutdown: Arc<AtomicBool>,
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
/// Docker E2E nodes get: strict ACK enforcement (`best_effort = false`, i.e.
/// `ack_policy = "auto"`) and the 3 s foreground ACK timeout, plus the
/// background checkpoint task.
fn create_checkpointing_segment_node(
    node_id: u64,
    seed_swim_ports: &[u16],
    checkpoint_interval: Duration,
) -> TestNode {
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

    let data_dir = std::env::temp_dir().join(format!("teraslab-ckpt-repl-{node_id}-{tcp_port}"));
    std::fs::create_dir_all(&data_dir).unwrap();
    let snapshot_path = data_dir.join("index.snap");
    let mut ckpt = CheckpointConfig::new(snapshot_path.clone());
    ckpt.poll_interval = Duration::from_millis(50);
    ckpt.max_checkpoint_interval = Some(checkpoint_interval);
    let checkpoint_shutdown = Arc::new(AtomicBool::new(false));
    spawn_checkpoint_task(ckpt, engine.clone(), redo, checkpoint_shutdown.clone());

    // Wait for the SWIM socket to bind (a UDP connect is bind-only semantics —
    // it sends no packet).
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
        snapshot_path,
        checkpoint_shutdown,
    }
}

fn shutdown_node(node: &TestNode) {
    node.checkpoint_shutdown.store(true, Ordering::Relaxed);
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

fn make_txid(seed: u32) -> [u8; 32] {
    let mut txid = [0u8; 32];
    txid[0..4].copy_from_slice(&seed.to_le_bytes());
    for (i, byte) in txid.iter_mut().enumerate().skip(4) {
        *byte = (seed.wrapping_mul(7).wrapping_add(i as u32) & 0xFF) as u8;
    }
    txid
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
            utxo_hashes: vec![make_txid(s.wrapping_add(500_000))],
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

/// Block until every node agrees on a committed 3-member topology with no
/// migration in flight, and the shard table has stopped moving. Without this
/// the ownership sets computed below can go stale mid-test and the batches come
/// back as per-item redirects, which would mask the outcome under test.
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
                    // Require the table to hold still briefly so a
                    // recompute in flight cannot land mid-workload.
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

/// Collect `count` txid seeds that `node` is the MASTER for, so a batch built
/// from them is served locally instead of coming back as a redirect.
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

/// **The nightly regression.** Under RF > 1 the three nodes form a replication
/// ring: each is master for its own shards and replica for a peer's. When every
/// node is serving a client create at the same time — exactly what one
/// shard-spanning client batch produces — each node must still be able to APPLY
/// the peer batch arriving at it.
///
/// A mutation handler that holds the engine-wide visibility barrier
/// (`Engine::visibility().global_read()`, the SHARED side) across its own
/// replication round-trip blocks the inbound `OP_REPLICA_BATCH` apply, which
/// takes the EXCLUSIVE side of that same lock. Around the ring that is a
/// circular wait broken only by the ACK timeout: every node's ACK times out at
/// exactly the timeout, simultaneously, and every replicated write fails with
/// `ERR_REPLICATION_FAILED` forever.
///
/// This is invisible to `tests/cluster_tcp.rs` because those tests only ever
/// write at one node, leaving the ring open.
#[test]
fn replicated_creates_do_not_deadlock_across_a_replication_ring() {
    let node1 = create_checkpointing_segment_node(821, &[], Duration::from_secs(60));
    let seed = [node1.swim_port];
    let node2 = create_checkpointing_segment_node(822, &seed, Duration::from_secs(60));
    let node3 = create_checkpointing_segment_node(823, &seed, Duration::from_secs(60));
    wait_for_settled_three_node_topology(&[&node1, &node2, &node3]);

    const PER_NODE: usize = 25;
    let plans: Vec<(u16, Vec<u32>)> = [&node1, &node2, &node3]
        .iter()
        .enumerate()
        .map(|(i, n)| {
            // Disjoint seed ranges per node so the three batches never collide
            // on a key (each node picks from its OWN ownership set).
            let seeds: Vec<u32> = owned_seeds(n, PER_NODE * (i + 1))
                .into_iter()
                .skip(PER_NODE * i)
                .collect();
            (n.tcp_port, seeds)
        })
        .collect();

    // Fire one create batch at EVERY node at the same instant. This is what
    // closes the ring: all three nodes are masters mid-fan-out while all three
    // must also apply an inbound peer batch.
    let started = Instant::now();
    let results: Vec<(u16, u8, Duration)> = std::thread::scope(|scope| {
        let handles: Vec<_> = plans
            .iter()
            .map(|(port, seeds)| {
                let port = *port;
                let payload = encode_batch(seeds);
                scope.spawn(move || {
                    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
                    stream
                        .set_read_timeout(Some(Duration::from_secs(60)))
                        .unwrap();
                    let t0 = Instant::now();
                    let resp = send_request(
                        &mut stream,
                        &RequestFrame {
                            request_id: port as u64,
                            op_code: OP_CREATE_BATCH,
                            flags: 0,
                            payload: payload.into(),
                        },
                    );
                    (port, resp.status, t0.elapsed())
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let elapsed = started.elapsed();

    let statuses: Vec<String> = results
        .iter()
        .map(|(p, s, d)| format!("port {p}: status {s} in {d:?}"))
        .collect();
    eprintln!("ring batches: {} | total {elapsed:?}", statuses.join(", "));

    let counts = [
        node1.engine.index_len(),
        node2.engine.index_len(),
        node3.engine.index_len(),
    ];
    shutdown_node(&node1);
    shutdown_node(&node2);
    shutdown_node(&node3);

    for (port, status, dt) in &results {
        assert_eq!(
            *status,
            STATUS_OK,
            "node on port {port} failed its create batch (status {status}) after {dt:?}; \
             all three nodes wrote concurrently, so each had to apply a peer's \
             OP_REPLICA_BATCH while its own create was replicating: {}",
            statuses.join(", ")
        );
    }
    // A ring deadlock is broken only by the ACK timeout, so the giveaway is a
    // batch that takes at least that long. Healthy is single-digit ms.
    for (port, _, dt) in &results {
        assert!(
            *dt < ACK_TIMEOUT,
            "node on port {port} took {dt:?} for a {PER_NODE}-record create batch — \
             at or beyond the {ACK_TIMEOUT:?} replica ACK timeout, which means the \
             replica-apply path was blocked behind a concurrent local mutation: {}",
            statuses.join(", ")
        );
    }
    // Every record is on its master AND its replica: 3 * PER_NODE records, each
    // held twice across the cluster under RF = 2.
    let total_copies: usize = counts.iter().sum();
    assert_eq!(
        total_copies,
        2 * 3 * PER_NODE,
        "each of the {} created records must exist on its master and its replica \
         (per-node index sizes: {counts:?})",
        3 * PER_NODE
    );
}

/// The checkpoint/compaction loop must keep running against a clustered segment
/// node under live replication without stalling the replica-apply path, and the
/// defrag pass must not rewrite live records when there is no dead space to
/// reclaim.
///
/// `tests/cluster_tcp.rs` builds clustered segment nodes but never attaches
/// `spawn_checkpoint_task`, so this combination had no Rust-level coverage at
/// all — only the ~2 h Docker nightly.
#[test]
fn segment_cluster_serves_replicated_creates_while_checkpointing() {
    // Aggressive checkpoint cadence so several complete during the workload.
    let node1 = create_checkpointing_segment_node(801, &[], Duration::from_millis(200));
    let seed = [node1.swim_port];
    let node2 = create_checkpointing_segment_node(802, &seed, Duration::from_millis(200));
    let node3 = create_checkpointing_segment_node(803, &seed, Duration::from_millis(200));
    wait_for_settled_three_node_topology(&[&node1, &node2, &node3]);

    let batches = 20usize;
    let per_batch = 50usize;
    let seeds = owned_seeds(&node1, batches * per_batch);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", node1.tcp_port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .unwrap();

    let mut worst = Duration::ZERO;
    let mut failures: Vec<String> = Vec::new();
    for b in 0..batches {
        let payload = encode_batch(&seeds[b * per_batch..(b + 1) * per_batch]);
        let t0 = Instant::now();
        let resp = send_request(
            &mut stream,
            &RequestFrame {
                request_id: b as u64,
                op_code: OP_CREATE_BATCH,
                flags: 0,
                payload: payload.into(),
            },
        );
        let dt = t0.elapsed();
        worst = worst.max(dt);
        if resp.status != STATUS_OK {
            failures.push(format!("batch {b}: status {} after {dt:?}", resp.status));
        }
        // Space the batches so checkpoints interleave with live traffic rather
        // than all landing after the workload.
        std::thread::sleep(Duration::from_millis(25));
    }

    // Give the defrag pass several more checkpoints over the populated store.
    std::thread::sleep(Duration::from_millis(800));

    let (compacted, _reclaimed) = node1.engine.last_checkpoint_defrag();
    let live_master = node1.engine.index_len();
    let live_replica = node2.engine.index_len();
    let checkpointed = node1.snapshot_path.exists();
    eprintln!(
        "checkpointing workload: worst_batch={worst:?} compacted={compacted} \
         master_live={live_master} replica_live={live_replica} snapshot={checkpointed}"
    );

    shutdown_node(&node1);
    shutdown_node(&node2);
    shutdown_node(&node3);

    assert!(
        failures.is_empty(),
        "every replicated create batch must ACK while the checkpoint task runs: {failures:?}"
    );
    assert!(
        checkpointed,
        "the checkpoint task must have completed at least one checkpoint \
         (no index snapshot was written) — otherwise this test is vacuous"
    );
    assert_eq!(
        live_master,
        batches * per_batch,
        "master must hold every created record after the checkpoint/defrag passes"
    );
    assert_eq!(
        live_replica,
        batches * per_batch,
        "replica must hold every replicated record after the checkpoint/defrag passes"
    );
    // Nothing was spent or deleted, so the store has no dead bytes and no
    // segment can qualify as a compaction victim. A non-zero count here means
    // the defrag pass is relocating LIVE records for no reclaim — the write
    // amplification the Docker nodes exhibited (`compacted: 306` of 336 live
    // records on a 0.12 %-full device).
    assert_eq!(
        compacted, 0,
        "defrag compaction must not relocate live records when there is no dead space"
    );
}
