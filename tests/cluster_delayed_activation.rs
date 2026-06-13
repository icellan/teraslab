//! W1.1 regression test — delayed topology activation must not leave
//! shards permanently masterless.
//!
//! Reproduces the CI-confirmed 3-node formation deadlock (run 27421955272)
//! deterministically. In CI, the slow node3 caught up to the 2-member
//! term-1 table, then missed the 3-member term-2 activation: node1/node2
//! ran their (empty-shard) migration plans, got blind acknowledgements,
//! and committed the master moves; when node3 finally re-activated term 2
//! it built an inbound-only plan (`outbound=0 inbound=2730`) and waited
//! forever for pushes that the push-based protocol would never re-send —
//! 1365 shards stayed masterless.
//!
//! This test replays that ordering with explicit control instead of a CPU
//! race:
//! 1. node1+node2 form a settled 2-member cluster (term 1);
//! 2. node3 joins with its coordinator *activation* held
//!    (`activation_hold_handle`) — it still votes and records the
//!    committed 3-member term through dispatch, exactly like the starved
//!    CI node;
//! 3. the sources commit term 2 and run their plans against the held
//!    target (with W1.1 FIX A the completion handshakes are rejected as
//!    `ERR_MIGRATION_TARGET_NOT_READY` and retried-then-failed);
//! 4. node3's active table is set to the 2-member term-1 snapshot via
//!    `test_install_active_routing_snapshot` — the deterministic stand-in
//!    for the catch-up fetch the CI node won 37% of the time;
//! 5. node3 is released. Its term-2 activation now builds the CI's exact
//!    inbound-only plan. W1.1 FIX B (the pull-based
//!    `OP_MIGRATION_TRANSFER_REQUEST` path) must drive the cluster to one
//!    consistent table with Σ masters == 4096 within a bounded time.
//!
//! Requires the `fault-injection` feature:
//!
//! ```bash
//! cargo test --features fault-injection --test cluster_delayed_activation
//! ```

#![cfg(feature = "fault-injection")]
#![allow(clippy::disallowed_macros)] // integration tests may print diagnostics

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use teraslab::allocator::SlotAllocator;
use teraslab::cluster::coordinator::{
    ClusterConfig, ClusterCoordinator, MasterQueryResult, ReplicationRuntimeConfig, RunningCluster,
};
use teraslab::cluster::shards::{NUM_SHARDS, NodeId, ShardTable};
use teraslab::cluster::topology::ClusterId;
use teraslab::config::ServerConfig;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, TxKey, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::engine::Engine;
use teraslab::server::Server;

const TEST_CLUSTER_ID: ClusterId = ClusterId([0xB7; 16]);

struct TestNode {
    #[allow(dead_code)]
    server: Arc<Server>,
    cluster: Arc<RunningCluster>,
    /// Activation-hold gate for this node's coordinator event loop.
    hold: Arc<AtomicBool>,
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

/// Start an in-process node. When `hold_activation` is true the
/// coordinator's activation processing is gated from the very first
/// event-loop iteration (ordering control — no sleeps).
fn create_node(node_id: u64, seed_swim_ports: &[u16], hold_activation: bool) -> TestNode {
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
        replication_factor: 2,
        probe_interval: Duration::from_millis(100),
        suspicion_timeout: Duration::from_secs(5),
        cluster_secret: None,
        max_migration_threads: 16,
        topology_propose_timeout: Duration::from_millis(300),
        migration_pool_size: 4,
        migration_batch_size: 100,
        persisted_incarnation: 0,
        cluster_id: TEST_CLUSTER_ID,
    };

    let coordinator = ClusterCoordinator::new(cluster_config, 1);
    let hold = coordinator.activation_hold_handle();
    hold.store(hold_activation, Ordering::Release);
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

    // Wait for the SWIM UDP socket to bind (poll, no fixed sleep).
    let swim_target: std::net::SocketAddr = format!("127.0.0.1:{swim_port}").parse().unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < deadline {
        let bound = std::net::UdpSocket::bind("127.0.0.1:0")
            .map(|s| s.connect(swim_target).is_ok())
            .unwrap_or(false);
        if bound {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    TestNode {
        server,
        cluster: running,
        hold,
        swim_port,
    }
}

fn wait_until<F: FnMut() -> bool>(
    mut condition: F,
    timeout: Duration,
    what: &str,
) -> Result<(), String> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if condition() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err(format!("timed out waiting for: {what}"))
}

/// A node's view is settled when its activated shard table matches the
/// quorum-committed term with no pending handoffs and no pending inbound
/// migrations.
fn node_settled(node: &TestNode) -> bool {
    let committed = node.cluster.shard_table_version();
    let table = node.cluster.shard_table();
    let table = table.read();
    table.version == committed
        && table.pending_handoff_count() == 0
        && node.cluster.inbound_pending_count() == 0
}

/// Build a txid that hashes to `shard` (the shard hash reads the first
/// two txid bytes).
fn txid_for_shard(shard: u16) -> [u8; 32] {
    let mut txid = [0u8; 32];
    let bytes = shard.to_le_bytes();
    txid[0] = bytes[0];
    txid[1] = bytes[1];
    debug_assert_eq!(ShardTable::shard_for_key(&TxKey { txid }), shard);
    txid
}

#[test]
fn delayed_third_node_activation_converges_without_masterless_shards() {
    // Surface coordinator logs when RUST_LOG is set (diagnostics only).
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    // Phase 1 — two nodes form a settled 2-node cluster (term 1).
    let n1 = create_node(1, &[], false);
    let n2 = create_node(2, &[n1.swim_port], false);
    wait_until(
        || {
            n1.cluster.shard_table_version() >= 1
                && n2.cluster.shard_table_version() >= 1
                && node_settled(&n1)
                && node_settled(&n2)
                && n1.cluster.active_migrations() == 0
                && n2.cluster.active_migrations() == 0
        },
        Duration::from_secs(30),
        "2-node cluster to settle",
    )
    .unwrap();
    let two_member_term = n1.cluster.shard_table_version();

    // Phase 2 — third node joins with its activation HELD. It votes and
    // records the committed 3-member term (dispatch side), but never
    // installs the term's shard table.
    let n3 = create_node(3, &[n1.swim_port, n2.swim_port], true);
    wait_until(
        || {
            n1.cluster.shard_table_version() > two_member_term
                && n2.cluster.shard_table_version() > two_member_term
        },
        Duration::from_secs(30),
        "3-member topology term to commit on the sources",
    )
    .unwrap();
    let three_member_term = n1.cluster.shard_table_version();

    // Phase 3 — let the sources run their (empty-shard) plans against the
    // held target and go idle again. With FIX A their completion
    // handshakes are rejected with ERR_MIGRATION_TARGET_NOT_READY and
    // retried within a bounded window (~4 s) before failing — pre-fix the
    // rejections were silently treated as delivered. Either way the
    // sources' active tables already name node 3 for its round-robin
    // share (empty shards swap assignments eagerly), which is exactly the
    // CI state: sources that believe the handoff is done.
    wait_until(
        || {
            if n1.cluster.active_migrations() != 0 || n2.cluster.active_migrations() != 0 {
                return false;
            }
            let t1 = n1.cluster.shard_table();
            let t1 = t1.read();
            t1.version == three_member_term
                && (0..NUM_SHARDS as u16).any(|s| t1.target_assignment(s).master == NodeId(3))
        },
        Duration::from_secs(60),
        "sources to finish their migration attempts against the held target",
    )
    .unwrap();
    // Hold the target past the sender's documented completion-handshake
    // retry bound (MAX_RETRIES × RETRY_DELAY = 40 × 100 ms = 4 s, see
    // `send_completion_only_handshakes`), so the sources' in-flight
    // retries EXHAUST before release. Convergence must then come from the
    // pull-based FIX B repair, not from a retry that happened to straddle
    // the release — this is the regime the CPU-starved CI node was in
    // (≈30 s of starvation vs the 4 s retry window).
    let retry_window_elapsed = std::time::Instant::now() + Duration::from_secs(6);
    wait_until(
        || std::time::Instant::now() >= retry_window_elapsed,
        Duration::from_secs(10),
        "the sources' bounded handshake retry window to expire",
    )
    .unwrap();

    // The held node must not have activated the 3-member term.
    {
        let table = n3.cluster.shard_table();
        let v = table.read().version;
        assert!(
            v < three_member_term,
            "activation hold failed: n3 activated term {v} >= {three_member_term}"
        );
    }

    // Phase 3.5 — deterministic stand-in for the CI node3's topology
    // catch-up: its active table becomes the 2-member term-1 snapshot.
    // From here node3 is byte-for-byte in the CI deadlock precondition:
    // active table = old term without itself, committed term = 3-member
    // term, activation pending.
    assert!(
        n3.cluster
            .test_install_active_routing_snapshot(&[NodeId(1), NodeId(2)], two_member_term),
        "routing snapshot install must succeed"
    );

    // Phase 4 — release the third node. Its (re-)activation of the
    // 3-member term builds the CI's inbound-only migration plan
    // (outbound=0): pre-fix this waited forever on pushes the sources
    // would never re-send. With FIX B the node requests the missing
    // transfers (re-firing every 10 s), and with FIX A the sources'
    // re-sent completion handshakes are only accepted once this node is
    // on the term. 90 s covers several full repair cycles.
    n3.hold.store(false, Ordering::Release);

    let nodes = [&n1, &n2, &n3];
    wait_until(
        || {
            if !nodes
                .iter()
                .all(|n| node_settled(n) && n.cluster.shard_table_version() >= three_member_term)
            {
                return false;
            }
            // Identical master assignments on every node (Σ masters ==
            // 4096; nothing masterless or doubly-owned) and node 3 owns
            // its round-robin share.
            let t1 = n1.cluster.shard_table();
            let t2 = n2.cluster.shard_table();
            let t3 = n3.cluster.shard_table();
            let (t1, t2, t3) = (t1.read(), t2.read(), t3.read());
            let mut n3_masters = 0usize;
            for s in 0..NUM_SHARDS as u16 {
                let m1 = t1.target_assignment(s).master;
                if m1 != t2.target_assignment(s).master || m1 != t3.target_assignment(s).master {
                    return false;
                }
                if m1 == NodeId(3) {
                    n3_masters += 1;
                }
            }
            n3_masters > 1000
        },
        Duration::from_secs(90),
        "cluster to converge to one consistent 3-node table",
    )
    .unwrap_or_else(|e| {
        for (i, n) in nodes.iter().enumerate() {
            let table = n.cluster.shard_table();
            let table = table.read();
            eprintln!(
                "node{}: committed={} table_version={} pending_handoffs={} pending_inbound={} active_migrations={}",
                i + 1,
                n.cluster.shard_table_version(),
                table.version,
                table.pending_handoff_count(),
                n.cluster.inbound_pending_count(),
                n.cluster.active_migrations(),
            );
        }
        panic!("{e}");
    });

    // The late node must serve as an authoritative (non-transitioning)
    // master for a shard it now owns: pre-fix it stayed `Transitioning`
    // (subset master with pending inbound) forever.
    let n3_shard = {
        let t3 = n3.cluster.shard_table();
        let t3 = t3.read();
        (0..NUM_SHARDS as u16)
            .find(|&s| t3.target_assignment(s).master == NodeId(3))
            .expect("n3 must master at least one shard")
    };
    let key = TxKey {
        txid: txid_for_shard(n3_shard),
    };
    assert_eq!(
        n3.cluster.is_master(&key),
        MasterQueryResult::Yes,
        "the late node must be an authoritative master, not Transitioning"
    );

    for n in nodes {
        n.cluster.shutdown();
    }
}

/// W1.5 regression — the late node must activate a newly-committed topology
/// term *promptly* (within one event-loop tick), independent of the 30 s
/// same-term reactivation cooldown.
///
/// Root cause of the residual 3-node formation deadlock: the late node's
/// authority commits the 3-member term (its dispatch worker applies
/// `OP_TOPOLOGY_COMMIT`, advancing `committed_term`), but the
/// migration-bearing activation never runs on the event loop — the commit
/// signal is deduped against a `topology_epoch` bump or the loop is starved
/// past the channel drain. The active shard table then lags the committed
/// term, and the ONLY pre-fix recovery was the 30 s reactivation cooldown.
/// The sources, meanwhile, abandon the handoff after their FIX-A handshake
/// budget (≈4 s), so ~1365 shards strand masterless until the 30 s timer.
///
/// This test reproduces that precondition deterministically and WITHOUT a
/// queued commit signal driving the recovery:
/// 1. node1+node2 form a settled 2-member cluster (term 1);
/// 2. node3 joins with activation HELD — it votes and the sources commit the
///    3-member term, but node3 never installs it;
/// 3. node3's active table is forced to the 2-member term-1 snapshot
///    (`test_install_active_routing_snapshot`) — active_version = term 1;
/// 4. node3's authority is advanced to the 3-member term by applying the
///    quorum-proof commit DIRECTLY (`handle_commit`), exactly as the catch-up
///    path does — but no `signal_topology_committed` is issued, modelling the
///    lost / deduped commit signal;
/// 5. node3 is released. With committed_term (term 3) strictly ahead of its
///    active table version (term 1) and NO pending commit signal, only the
///    prompt per-tick catch-up can recover it before the 30 s cooldown.
///
/// Assertion: Σ master_shard_count == 4096 across all three nodes within
/// **15 s** — far under both the 30 s cooldown and the 120 s CI budget.
/// FAILS on pre-fix code (node3 stays masterless until the 30 s reactivation,
/// blowing the 15 s bound); PASSES after the prompt-activation fix.
#[test]
fn committed_term_ahead_of_active_table_activates_promptly() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    // Phase 1 — two-node settled cluster (term 1).
    let n1 = create_node(1, &[], false);
    let n2 = create_node(2, &[n1.swim_port], false);
    wait_until(
        || {
            n1.cluster.shard_table_version() >= 1
                && n2.cluster.shard_table_version() >= 1
                && node_settled(&n1)
                && node_settled(&n2)
                && n1.cluster.active_migrations() == 0
                && n2.cluster.active_migrations() == 0
        },
        Duration::from_secs(30),
        "2-node cluster to settle",
    )
    .unwrap();
    let two_member_term = n1.cluster.shard_table_version();

    // Phase 2 — third node joins with activation HELD *and* with its commit
    // signal dropped. It still votes and its authority commits the 3-member
    // term via the dispatch `OP_TOPOLOGY_COMMIT` apply (advancing
    // committed_term), but `signal_topology_committed` is a no-op — so NO
    // activation is ever queued for its event loop. This is the production
    // race the prompt catch-up must close: committed_term advances while the
    // migration-bearing activation never runs.
    let n3 = create_node(3, &[n1.swim_port, n2.swim_port], true);
    n3.cluster
        .drop_commit_signals_handle()
        .store(true, Ordering::Release);
    wait_until(
        || {
            n1.cluster.shard_table_version() > two_member_term
                && n2.cluster.shard_table_version() > two_member_term
        },
        Duration::from_secs(30),
        "3-member topology term to commit on the sources",
    )
    .unwrap();
    let three_member_term = n1.cluster.shard_table_version();

    // Let the sources run their plans against the held target and go idle —
    // the CI state where the sources believe the handoff is done.
    wait_until(
        || n1.cluster.active_migrations() == 0 && n2.cluster.active_migrations() == 0,
        Duration::from_secs(60),
        "sources to finish their migration attempts against the held target",
    )
    .unwrap();

    // node3's authority must have committed the 3-member term via the
    // dispatch apply (its signal was dropped, but `handle_commit` still ran).
    wait_until(
        || n3.cluster.shard_table_version() >= three_member_term,
        Duration::from_secs(30),
        "node3 authority to commit the 3-member term (signal dropped)",
    )
    .unwrap();

    // node3 must not have activated the 3-member term yet (hold in effect).
    {
        let v = n3.cluster.shard_table().read().version;
        assert!(
            v < three_member_term,
            "activation hold failed: n3 activated term {v} >= {three_member_term}"
        );
    }

    // Phase 3 — force node3 into the exact deadlock precondition:
    //   active table   = 2-member term-1 snapshot (active_version = term 1)
    //   committed_term = 3-member term (advanced via dispatch handle_commit)
    //   pending signal = NONE (dropped at the source)
    assert!(
        n3.cluster
            .test_install_active_routing_snapshot(&[NodeId(1), NodeId(2)], two_member_term),
        "routing snapshot install must succeed"
    );
    assert_eq!(
        n3.cluster.shard_table_version(),
        three_member_term,
        "precondition: committed_term ahead of active table version",
    );
    assert!(
        n3.cluster.shard_table().read().version < three_member_term,
        "precondition: active table still on the old term",
    );

    // Phase 4 — release node3. With committed_term strictly ahead of the
    // active table version and NO pending commit signal, only the prompt
    // per-tick catch-up can converge the cluster before the 30 s cooldown.
    n3.hold.store(false, Ordering::Release);

    let nodes = [&n1, &n2, &n3];
    wait_until(
        || {
            if !nodes
                .iter()
                .all(|n| node_settled(n) && n.cluster.shard_table_version() >= three_member_term)
            {
                return false;
            }
            let t1 = n1.cluster.shard_table();
            let t2 = n2.cluster.shard_table();
            let t3 = n3.cluster.shard_table();
            let (t1, t2, t3) = (t1.read(), t2.read(), t3.read());
            let mut n3_masters = 0usize;
            for s in 0..NUM_SHARDS as u16 {
                let m1 = t1.target_assignment(s).master;
                if m1 != t2.target_assignment(s).master || m1 != t3.target_assignment(s).master {
                    return false;
                }
                if m1 == NodeId(3) {
                    n3_masters += 1;
                }
            }
            n3_masters > 1000
        },
        // 15 s: comfortably above the prompt path's true latency (one 100 ms
        // tick + the handoff round-trips) yet far below the 30 s reactivation
        // cooldown that is the ONLY pre-fix recovery here.
        Duration::from_secs(15),
        "cluster to converge promptly to one consistent 3-node table",
    )
    .unwrap_or_else(|e| {
        for (i, n) in nodes.iter().enumerate() {
            let table = n.cluster.shard_table();
            let table = table.read();
            eprintln!(
                "node{}: committed={} table_version={} pending_handoffs={} pending_inbound={} active_migrations={}",
                i + 1,
                n.cluster.shard_table_version(),
                table.version,
                table.pending_handoff_count(),
                n.cluster.inbound_pending_count(),
                n.cluster.active_migrations(),
            );
        }
        panic!("{e}");
    });

    for n in nodes {
        n.cluster.shutdown();
    }
}
