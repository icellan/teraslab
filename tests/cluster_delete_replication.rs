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
use teraslab::ops::remaining::PreserveUntilRequest;
use teraslab::ops::tombstone::TombstoneLog;
use teraslab::protocol::codec::{
    FieldMask, WireCreateItem, encode_create_batch, encode_get_batch, encode_txid_batch,
};
use teraslab::protocol::frame::{RequestFrame, ResponseFrame};
use teraslab::protocol::opcodes::{
    FLAG_LOCAL_READ, OP_CREATE_BATCH, OP_DELETE_BATCH, OP_GET_BATCH, OP_PRESERVE_UNTIL_BATCH,
    OP_PROCESS_EXPIRED_PRESERVATIONS, OP_SET_CONFLICTING_BATCH, OP_SET_MINED_BATCH, OP_SPEND_BATCH,
    STATUS_OK,
};
use teraslab::redo::RedoLog;
use teraslab::replication::manager::AckPolicy;
use teraslab::replication::protocol::ReplicaOp;
use teraslab::replication::receiver::apply_op_journal;
use teraslab::segment_allocator::SegmentAllocator;
use teraslab::server::Server;

use serial_test::serial;

const TEST_CLUSTER_ID: ClusterId = ClusterId([0xC7; 16]);
const TEST_SEGMENT_SIZE: u64 = 16 * 4096;
const ACK_TIMEOUT: Duration = Duration::from_secs(3);

/// Install the process-wide replication metrics once so the missing-record
/// repair counter is observable. Like [`dispatch_metrics`], `init_*` writes a
/// `OnceLock`, so repeated calls are harmless and the handle returned is the
/// one the replication fan-out increments.
///
/// The counter is process-global, so the three tests in this file that can move
/// it are `#[serial(missing_record_repair)]`.
fn repl_metrics() -> &'static teraslab::metrics::ReplicationMetrics {
    static METRICS: std::sync::OnceLock<teraslab::metrics::ReplicationMetrics> =
        std::sync::OnceLock::new();
    let m = METRICS.get_or_init(teraslab::metrics::ReplicationMetrics::new);
    teraslab::metrics::init_replication_metrics(m);
    m
}

/// Install the process-wide dispatch metrics once so the sweep's held-copy
/// counter is observable. `init_dispatch_metrics` writes a `OnceLock`, so
/// repeated calls from parallel tests are harmless; the returned handle is the
/// same one the dispatcher increments.
fn dispatch_metrics() -> &'static teraslab::metrics::ThreadMetrics {
    static METRICS: std::sync::OnceLock<teraslab::metrics::ThreadMetrics> =
        std::sync::OnceLock::new();
    let m = METRICS.get_or_init(teraslab::metrics::ThreadMetrics::new);
    teraslab::server::dispatch::init_dispatch_metrics(m);
    m
}

struct TestNode {
    server: Arc<Server>,
    cluster: Arc<RunningCluster>,
    engine: Arc<Engine>,
    tcp_port: u16,
    swim_port: u16,
    shutdown: Arc<AtomicBool>,
    /// Backing directory for this node's tombstone log; must outlive the engine.
    _tombstone_dir: tempfile::TempDir,
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
    create_cluster_node_with_ack_policy(node_id, seed_swim_ports, None)
}

/// [`create_cluster_node`] with an explicit replica-ACK policy.
///
/// `ack_policy: None` means `required_replica_acks == 0` — a fan-out that gets
/// ZERO ACKs is still classified `Durable` and the client still sees
/// `STATUS_OK`. That is fine for the tests above, which assert on where records
/// physically live rather than on replication verdicts, but it is NOT what a
/// production RF = 2 node runs: `ServerConfig::resolved_ack_policy` maps the
/// default `ack_policy = "auto"` at `replication_factor = 2` to
/// [`AckPolicy::WriteAll`], i.e. one required ACK. A test that needs a replica
/// NAK to actually fail the client's write must pass that policy explicitly.
fn create_cluster_node_with_ack_policy(
    node_id: u64,
    seed_swim_ports: &[u16],
    ack_policy: Option<AckPolicy>,
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
    // Deletion tombstones ON, matching what these nodes get in production:
    // `ReverseHealConfig::tombstones_enabled` defaults to ON for a clustered
    // node (RF > 1), and RF is 2 here. Without this the sweep would run with
    // the whole tombstone subsystem inert and the F2 held-copy-vs-master
    // distinction (`Engine::reclaim_held_copy`) would be untested.
    let tomb_dir = tempfile::tempdir().unwrap();
    engine.set_tombstone_log(TombstoneLog::new(
        tomb_dir.path().join("teraslab.tombstones"),
        engine.index_seed(),
        engine.index_shard_count(),
        1000,
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
        ack_policy,
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
        _tombstone_dir: tomb_dir,
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

/// **The most dangerous interaction in the delete-replication change, end to
/// end through the production path.**
///
/// The replica has already reclaimed its held copy (F2 replica-side GC), and
/// the client then deletes the record at its master. The replicated
/// `ReplicaOp::Delete` therefore targets a record the replica does NOT have.
///
/// That must be an idempotent success. The alternative — NAKing
/// `ReplicaAck::MissingRecord`, the way `preserve_until` / `set_conflicting`
/// legitimately do — routes into `repair_missing_record_target`, whose job is to
/// re-ship the record's full current image. For a delete that means the master
/// RESURRECTS on the replica the very record it is deleting, and the resurrected
/// copy then outlives the master's own (which is dropped immediately afterwards)
/// because nothing in the running system revisits a replica copy.
///
/// So the assertion is not merely "the client's delete returned OK": it is that
/// the record is on NO node afterwards. A repair round-trip would leave it on
/// the replica, which is the exact permanent divergence this file exists to
/// catch — just reached from the opposite direction.
///
/// `AckPolicy::WriteAll` is explicit so a replica NAK would genuinely fail the
/// client's delete (with `required_replica_acks == 0` a NAK is still classified
/// `Durable`, and the resurrection would pass unnoticed).
///
/// The `replica_missing_record_repaired` delta pins the MECHANISM, not just the
/// outcome: it counts records the master re-shipped in response to a
/// `MissingRecord` NAK. It must not move here. Without it this test could pass
/// on a build that never replicated the delete at all — which is exactly the
/// behaviour this file exists to reject.
#[test]
#[serial(missing_record_repair)]
fn client_delete_of_a_record_the_replica_reclaimed_does_not_resurrect_it() {
    let node1 = create_cluster_node_with_ack_policy(911, &[], Some(AckPolicy::WriteAll));
    let seed = [node1.swim_port];
    let node2 = create_cluster_node_with_ack_policy(912, &seed, Some(AckPolicy::WriteAll));
    let node3 = create_cluster_node_with_ack_policy(913, &seed, Some(AckPolicy::WriteAll));
    let nodes = [&node1, &node2, &node3];
    wait_for_settled_three_node_topology(&nodes);

    const COUNT: usize = 6;
    const HEIGHT: u32 = 900_000;
    const RETENTION: u32 = 100;
    let seeds = owned_seeds(&node1, COUNT);

    let resp = request_at(node1.tcp_port, OP_CREATE_BATCH, 0, encode_batch(&seeds));
    assert_eq!(resp.status, STATUS_OK, "create batch must be accepted");
    for s in &seeds {
        set_mined(node1.tcp_port, *s, HEIGHT, RETENTION);
        spend_to_all_spent(node1.tcp_port, *s, HEIGHT, RETENTION);
    }

    // The replica drops its copy; the master (never swept) keeps its own. This
    // asserts the reclaim actually happened, so the delete below really does hit
    // an absent record on the replica.
    force_replica_reclaim(&nodes, &seeds, HEIGHT + RETENTION + 1, RETENTION);

    // Monotonic process-global counter. The only other tests in this binary that
    // can move it are the two `master_repairs_*` tests, and all three are
    // `#[serial(missing_record_repair)]`, so a zero delta here is exact rather
    // than a race.
    let repaired_before = repl_metrics().replica_missing_record_repaired.get();

    let txids: Vec<[u8; 32]> = seeds.iter().map(|s| make_txid(*s)).collect();
    let resp = request_at(
        node1.tcp_port,
        OP_DELETE_BATCH,
        0,
        encode_txid_batch(&txids, &[]),
    );
    let delete_status = resp.status;
    let after: Vec<Vec<usize>> = txids.iter().map(|t| local_holders(&nodes, t)).collect();
    let repaired_delta = repl_metrics()
        .replica_missing_record_repaired
        .get()
        .saturating_sub(repaired_before);

    shutdown_node(&node1);
    shutdown_node(&node2);
    shutdown_node(&node3);

    assert_eq!(
        delete_status, STATUS_OK,
        "deleting a record the replica already reclaimed must SUCCEED — the delete's \
         post-condition (record absent) already holds on the replica, so there is \
         nothing to repair and nothing to fail (status {delete_status})"
    );
    assert_eq!(
        repaired_delta, 0,
        "the master must not have re-shipped ANY record while deleting: a re-ship is \
         driven by a MissingRecord NAK, and for a Delete that means shipping back the \
         record being deleted. Saw {repaired_delta} repair(s)"
    );
    let resurrected: Vec<String> = seeds
        .iter()
        .zip(&after)
        .filter(|(_, h)| !h.is_empty())
        .map(|(s, h)| format!("seed {s}: holders {h:?}"))
        .collect();
    assert!(
        resurrected.is_empty(),
        "{}/{COUNT} deleted records are still present somewhere. If they are on the \
         REPLICA the master re-shipped a record it was deleting: a replicated Delete for \
         an absent record must never NAK MissingRecord, because that NAK is what drives \
         `repair_missing_record_target` to re-ship the full record image: {}",
        resurrected.len(),
        resurrected.join(" | ")
    );
}

/// Serialize a source record into the migration-baseline `ReplicaOp::Create`,
/// reproducing the coordinator's `stream_shard_baseline` wire layout (70-byte
/// metadata prefix + utxo hashes). This is the path a master's re-delivery of a
/// record actually takes, and the ONE path RULE-DS
/// (`Engine::tombstone_blocks_heal_apply`) gates.
fn build_migration_create_op(source: &Engine, k: &TxKey) -> ReplicaOp {
    let meta = source.read_metadata(k).unwrap();
    let utxo_count = { meta.utxo_count };
    let mut utxo_hashes = Vec::with_capacity(utxo_count as usize);
    for v in 0..utxo_count {
        utxo_hashes.push(source.read_slot(k, v).unwrap().hash);
    }
    let mut meta_buf = Vec::with_capacity(70);
    meta_buf.extend_from_slice(&{ meta.tx_version }.to_le_bytes());
    meta_buf.extend_from_slice(&{ meta.locktime }.to_le_bytes());
    meta_buf.extend_from_slice(&{ meta.fee }.to_le_bytes());
    meta_buf.extend_from_slice(&{ meta.size_in_bytes }.to_le_bytes());
    meta_buf.extend_from_slice(&{ meta.extended_size }.to_le_bytes());
    let (is_coinbase, wire_flags) =
        teraslab::replication::protocol::create_metadata_flag_bytes(meta.flags);
    meta_buf.push(is_coinbase);
    meta_buf.extend_from_slice(&{ meta.spending_height }.to_le_bytes());
    meta_buf.extend_from_slice(&{ meta.created_at }.to_le_bytes());
    meta_buf.push(wire_flags);
    meta_buf.extend_from_slice(&{ meta.generation }.to_le_bytes());
    meta_buf.extend_from_slice(&{ meta.updated_at }.to_le_bytes());
    meta_buf.extend_from_slice(&{ meta.unmined_since }.to_le_bytes());
    meta_buf.extend_from_slice(&{ meta.delete_at_height }.to_le_bytes());
    meta_buf.extend_from_slice(&{ meta.preserve_until }.to_le_bytes());
    ReplicaOp::Create {
        tx_key: *k,
        metadata_bytes: meta_buf,
        utxo_hashes,
        cold_data: None,
        is_external: false,
    }
}

/// **The `PreserveUntil` hazard, and why replica-side GC is still safe.**
///
/// Concrete race: a client preserves a DAH-due record at its master; before the
/// replicated `PreserveUntil` lands, the replica's own pruner fires and sees a
/// record its local state still says is due. The replica drops a copy the
/// master just decided to keep.
///
/// This test forces exactly that window — `preserve_until` is applied to the
/// MASTER's engine only, never shipped — and pins down the outcome:
///
///   1. the master KEEPS the record (its own KO-3 under-lock re-validation
///      refuses the sweep), so the data is never lost cluster-wide;
///   2. the replica DOES drop its copy (the hazard is real, not hypothetical —
///      asserted so the rest of this test cannot pass vacuously);
///   3. the replica records NO tombstone for it, so nothing vetoes its return;
///   4. re-delivering the master's copy through the real migration-baseline
///      apply path — the path RULE-DS gates — RESTORES it.
///
/// A CONTROL record that the master legitimately sweeps proves the tombstone
/// subsystem is live in this cluster: the master holds a blocking tombstone for
/// it, the replica holds none for the copy it reclaimed. Without that contrast,
/// assertion 3 would pass on a node where tombstones were simply switched off.
#[test]
fn preserved_on_master_survives_a_replica_sweep_and_is_restorable() {
    let node1 = create_cluster_node(881, &[]);
    let seed = [node1.swim_port];
    let node2 = create_cluster_node(882, &seed);
    let node3 = create_cluster_node(883, &seed);
    let nodes = [&node1, &node2, &node3];
    wait_for_settled_three_node_topology(&nodes);

    const COUNT: usize = 8;
    const HEIGHT: u32 = 900_000;
    const RETENTION: u32 = 100;
    let all_seeds = owned_seeds(&node1, COUNT);
    // First half races a preserve; second half is the swept control.
    let (raced, control) = all_seeds.split_at(COUNT / 2);

    let resp = request_at(node1.tcp_port, OP_CREATE_BATCH, 0, encode_batch(&all_seeds));
    assert_eq!(resp.status, STATUS_OK, "create batch must be accepted");
    for s in &all_seeds {
        set_mined(node1.tcp_port, *s, HEIGHT, RETENTION);
        spend_to_all_spent(node1.tcp_port, *s, HEIGHT, RETENTION);
    }

    // Which node holds the replica copy? Under RF = 2 every record has exactly
    // one holder besides its master (node index 0).
    let holders_before: Vec<Vec<usize>> = all_seeds
        .iter()
        .map(|s| local_holders(&nodes, &make_txid(*s)))
        .collect();
    for (s, holders) in all_seeds.iter().zip(&holders_before) {
        assert_eq!(
            holders.len(),
            2,
            "seed {s} must be on exactly 2 nodes before the sweep, found {holders:?}"
        );
        assert!(holders.contains(&0), "seed {s} must be mastered by node 0");
    }
    let replica_idx = holders_before[0]
        .iter()
        .copied()
        .find(|i| *i != 0)
        .expect("a replica holder must exist");
    let replica = nodes[replica_idx];

    // The generation the REPLICA's copy carries right now. The tombstone
    // assertion below uses it because it is the worst case: a re-delivery at the
    // same generation is exactly what a `Dah` tombstone would refuse.
    let replica_generations: Vec<u32> = raced
        .iter()
        .map(|s| {
            let k = TxKey {
                txid: make_txid(*s),
            };
            let m = replica.engine.read_metadata(&k).expect("replica holds it");
            m.generation
        })
        .collect();

    // FORCE THE RACE: preserve on the MASTER's engine only. This is precisely
    // the in-flight window — the master has decided to keep the record and the
    // replica has not heard yet.
    for s in raced {
        node1
            .engine
            .preserve_until(&PreserveUntilRequest {
                tx_key: TxKey {
                    txid: make_txid(*s),
                },
                block_height: HEIGHT + RETENTION + 10_000,
            })
            .expect("master-side preserve must succeed");
    }

    let sweep_height = HEIGHT + RETENTION + 1;
    let mut payload = Vec::with_capacity(8);
    payload.extend_from_slice(&sweep_height.to_le_bytes());
    payload.extend_from_slice(&RETENTION.to_le_bytes());
    for node in &nodes {
        let resp = request_at(
            node.tcp_port,
            OP_PROCESS_EXPIRED_PRESERVATIONS,
            0,
            payload.clone(),
        );
        assert_eq!(resp.status, STATUS_OK, "sweep must succeed");
    }

    // (1) + (2): master kept the raced records; the replica dropped them.
    let mut master_lost = Vec::new();
    let mut replica_kept = Vec::new();
    for s in raced {
        let holders = local_holders(&nodes, &make_txid(*s));
        if !holders.contains(&0) {
            master_lost.push(*s);
        }
        if holders.contains(&replica_idx) {
            replica_kept.push(*s);
        }
    }

    // (3): no tombstone on the replica for what it reclaimed.
    let blocking: Vec<u32> = raced
        .iter()
        .zip(&replica_generations)
        .filter(|(s, generation)| {
            replica.engine.tombstone_blocks_heal_apply(
                &TxKey {
                    txid: make_txid(**s),
                },
                **generation,
            )
        })
        .map(|(s, _)| *s)
        .collect();

    // CONTROL: the master's own sweep must still be authoritative — record gone
    // everywhere, master holds a blocking tombstone, replica holds none.
    let control_survivors: Vec<u32> = control
        .iter()
        .filter(|s| !local_holders(&nodes, &make_txid(**s)).is_empty())
        .copied()
        .collect();
    let control_master_tombstones = control
        .iter()
        .filter(|s| {
            node1.engine.tombstone_blocks_heal_apply(
                &TxKey {
                    txid: make_txid(**s),
                },
                u32::MAX,
            )
        })
        .count();
    let control_replica_tombstones = control
        .iter()
        .filter(|s| {
            replica
                .engine
                .tombstone_lookup(&TxKey {
                    txid: make_txid(**s),
                })
                .is_some()
        })
        .count();

    // (4): re-deliver the master's surviving copy through the migration-baseline
    // apply path and check it lands on the replica.
    let mut restored = 0usize;
    for s in raced {
        let k = TxKey {
            txid: make_txid(*s),
        };
        if node1.engine.lookup(&k).is_none() {
            continue;
        }
        let op = build_migration_create_op(&node1.engine, &k);
        apply_op_journal(&replica.engine, &op, false, true).expect("re-delivery must apply");
        if replica.engine.lookup(&k).is_some() {
            restored += 1;
        }
    }

    shutdown_node(&node1);
    shutdown_node(&node2);
    shutdown_node(&node3);

    assert!(
        master_lost.is_empty(),
        "the MASTER must keep every record it preserved — the sweep's under-lock \
         re-validation is what protects the authoritative copy; lost: {master_lost:?}"
    );
    assert!(
        replica_kept.is_empty(),
        "the replica must have reclaimed its copies, otherwise this test proves nothing \
         about the hazard (it never materialized); still held: {replica_kept:?}"
    );
    assert!(
        blocking.is_empty(),
        "a held-copy reclaim must leave NO tombstone that blocks the master re-delivering \
         the record — a replica is not the authority on whether the key exists, and a \
         tombstone there turns this transient divergence into permanent loss; blocked: \
         {blocking:?}"
    );
    assert!(
        control_survivors.is_empty(),
        "control records the master legitimately swept must be gone everywhere: \
         {control_survivors:?}"
    );
    assert_eq!(
        control_master_tombstones,
        control.len(),
        "the MASTER's sweep must still record a blocking Dah tombstone for every control \
         record — this is what proves the tombstone subsystem is live here, so the \
         replica's empty tombstone set above is a real result and not a disabled feature"
    );
    assert_eq!(
        control_replica_tombstones, 0,
        "the replica must record no tombstone for the control copies it reclaimed either"
    );
    assert_eq!(
        restored,
        raced.len(),
        "every record the master still holds must be restorable onto the replica through \
         the real migration-baseline apply path"
    );
}

/// Reclaim every held REPLICA copy of `seeds` by firing the DAH sweep at both
/// non-master nodes, and assert the reclaim actually happened.
///
/// Fired at nodes 1 and 2 (never the master at index 0) because a given
/// master's shards do not all share the same replica — whichever of the two
/// holds the copy reclaims it, and the other is a no-op. On return every seed
/// must be down to exactly one holder, the master. If that does not hold, the
/// caller's assertions about the repair would be vacuous.
fn force_replica_reclaim(nodes: &[&TestNode], seeds: &[u32], sweep_height: u32, retention: u32) {
    let mut payload = Vec::with_capacity(8);
    payload.extend_from_slice(&sweep_height.to_le_bytes());
    payload.extend_from_slice(&retention.to_le_bytes());
    for node in &nodes[1..] {
        let resp = request_at(
            node.tcp_port,
            OP_PROCESS_EXPIRED_PRESERVATIONS,
            0,
            payload.clone(),
        );
        assert_eq!(resp.status, STATUS_OK, "replica-side sweep must succeed");
    }
    let still_replicated: Vec<String> = seeds
        .iter()
        .map(|s| (s, local_holders(nodes, &make_txid(*s))))
        .filter(|(_, h)| h.as_slice() != [0])
        .map(|(s, h)| format!("seed {s}: holders {h:?}"))
        .collect();
    assert!(
        still_replicated.is_empty(),
        "the replica-side sweep must have reclaimed EVERY replica copy, leaving the master \
         as the sole holder — otherwise the repair assertions below prove nothing because \
         there was never anything missing to repair: {}",
        still_replicated.join(" | ")
    );
}

/// **The C15 repair contract, end to end through the production path.**
///
/// A replica legitimately reclaims its held copy of a DAH-due record (F2
/// replica-side GC). Before the master's own pruner reaches the same record, a
/// client preserves it at the master — the normal Teranode case, where parent
/// preservation (1440 blocks) far outlives the DAH default (288).
///
/// The master now replicates `PreserveUntil` for a record the replica does not
/// have. At RF = 2 that key has exactly ONE replica target, so a NAK is 0/1
/// acked — a deterministic quorum failure that `best_effort` cannot rescue
/// (config rejects `best_effort` when `replication_factor > 1`). Before the
/// repair existed, the outcome was: the client's preserve REJECTED, the master's
/// own mutation rolled back by compensation to `preserve_until(0)` — which
/// clears the DAH *and* the preserve index, so `record_due_for_sweep` returns
/// false forever and the record is immortal on the master — and the key silently
/// left on one of two holders.
///
/// What must happen instead: the replica's NAK names the missing record, the
/// master re-ships that record's full current image and re-sends the batch, and
/// the client's preserve succeeds with the record present on BOTH holders.
///
/// This drives the REAL path end to end: a client TCP request, the production
/// replication fan-out, the replica's own receiver. Nothing here calls
/// `apply_op_journal` by hand.
#[test]
#[serial(missing_record_repair)]
fn master_repairs_a_replica_that_reclaimed_a_record_it_then_preserves() {
    let node1 = create_cluster_node_with_ack_policy(891, &[], Some(AckPolicy::WriteAll));
    let seed = [node1.swim_port];
    let node2 = create_cluster_node_with_ack_policy(892, &seed, Some(AckPolicy::WriteAll));
    let node3 = create_cluster_node_with_ack_policy(893, &seed, Some(AckPolicy::WriteAll));
    let nodes = [&node1, &node2, &node3];
    wait_for_settled_three_node_topology(&nodes);

    const COUNT: usize = 6;
    const HEIGHT: u32 = 900_000;
    const RETENTION: u32 = 100;
    const PRESERVE_TO: u32 = HEIGHT + RETENTION + 10_000;
    let seeds = owned_seeds(&node1, COUNT);

    let resp = request_at(node1.tcp_port, OP_CREATE_BATCH, 0, encode_batch(&seeds));
    assert_eq!(resp.status, STATUS_OK, "create batch must be accepted");
    for s in &seeds {
        set_mined(node1.tcp_port, *s, HEIGHT, RETENTION);
        spend_to_all_spent(node1.tcp_port, *s, HEIGHT, RETENTION);
    }
    for (s, holders) in seeds
        .iter()
        .zip(seeds.iter().map(|s| local_holders(&nodes, &make_txid(*s))))
    {
        assert_eq!(
            holders.len(),
            2,
            "seed {s} must be on exactly 2 nodes before the replica sweep, found {holders:?}"
        );
        assert!(holders.contains(&0), "seed {s} must be mastered by node 0");
    }

    // The replica reclaims; the master (never swept) keeps its copy.
    force_replica_reclaim(&nodes, &seeds, HEIGHT + RETENTION + 1, RETENTION);

    // The client preserve that the replica cannot apply without the record.
    let txids: Vec<[u8; 32]> = seeds.iter().map(|s| make_txid(*s)).collect();
    let resp = request_at(
        node1.tcp_port,
        OP_PRESERVE_UNTIL_BATCH,
        0,
        encode_txid_batch(&txids, &PRESERVE_TO.to_le_bytes()),
    );
    let preserve_status = resp.status;

    let after: Vec<Vec<usize>> = txids.iter().map(|t| local_holders(&nodes, t)).collect();
    // Non-vacuity: the master's preserve must actually have been applied and
    // KEPT (not compensated back to 0), so a "both holders present" result
    // cannot come from a preserve that silently did nothing.
    let master_preserve: Vec<u32> = seeds
        .iter()
        .map(|s| {
            node1
                .engine
                .read_metadata(&TxKey {
                    txid: make_txid(*s),
                })
                .map(|m| m.preserve_until)
                .unwrap_or(0)
        })
        .collect();

    shutdown_node(&node1);
    shutdown_node(&node2);
    shutdown_node(&node3);

    assert_eq!(
        preserve_status, STATUS_OK,
        "the client's preserve must SUCCEED: the only reason the replica could not apply it \
         is that it had already reclaimed the record, and the master — that record's own \
         master — still holds it and can re-ship it. Failing here rejects a client operation \
         for a condition the cluster can repair itself, and the compensation that follows \
         clears the master's DAH and preserve index, leaking the record forever \
         (status {preserve_status})"
    );
    let unpreserved: Vec<String> = seeds
        .iter()
        .zip(&master_preserve)
        .filter(|(_, p)| **p != PRESERVE_TO)
        .map(|(s, p)| format!("seed {s}: preserve_until={p}"))
        .collect();
    assert!(
        unpreserved.is_empty(),
        "every record must carry the requested preserve_until on the master afterwards — a \
         STATUS_OK with a rolled-back preserve would be worse than an honest failure: {}",
        unpreserved.join(" | ")
    );
    let not_repaired: Vec<String> = seeds
        .iter()
        .zip(&after)
        .filter(|(_, h)| h.len() != 2)
        .map(|(s, h)| format!("seed {s}: holders {h:?}"))
        .collect();
    assert!(
        not_repaired.is_empty(),
        "{}/{COUNT} records are still on fewer than 2 holders after an ACKed preserve — the \
         master must have re-shipped the record the replica was missing, restoring RF=2. A \
         key left silently single-copy is lost outright if the master then fails: {}",
        not_repaired.len(),
        not_repaired.join(" | ")
    );
}

/// The same repair must cover the other mutations that hit an absent replica
/// record — the review names `unspend`, `mark_on_longest_chain(false)`,
/// `set_conflicting`, `set_locked` and `set_mined` alongside `preserve_until`.
/// They share ONE code path (`missing_record_apply_outcome` → the typed NAK →
/// the master's re-ship), so covering a second op with a completely different
/// handler is what proves the repair is in the shared fan-out and not special-
/// cased for preserve.
///
/// `set_conflicting` is the second op chosen deliberately: like `preserve_until`
/// it has no block-depth protection, so unlike the reorg variants it is
/// reachable at any height.
#[test]
#[serial(missing_record_repair)]
fn master_repairs_a_replica_that_reclaimed_a_record_it_then_marks_conflicting() {
    let node1 = create_cluster_node_with_ack_policy(901, &[], Some(AckPolicy::WriteAll));
    let seed = [node1.swim_port];
    let node2 = create_cluster_node_with_ack_policy(902, &seed, Some(AckPolicy::WriteAll));
    let node3 = create_cluster_node_with_ack_policy(903, &seed, Some(AckPolicy::WriteAll));
    let nodes = [&node1, &node2, &node3];
    wait_for_settled_three_node_topology(&nodes);

    const COUNT: usize = 4;
    const HEIGHT: u32 = 900_000;
    const RETENTION: u32 = 100;
    let seeds = owned_seeds(&node1, COUNT);

    let resp = request_at(node1.tcp_port, OP_CREATE_BATCH, 0, encode_batch(&seeds));
    assert_eq!(resp.status, STATUS_OK, "create batch must be accepted");
    for s in &seeds {
        set_mined(node1.tcp_port, *s, HEIGHT, RETENTION);
        spend_to_all_spent(node1.tcp_port, *s, HEIGHT, RETENTION);
    }
    force_replica_reclaim(&nodes, &seeds, HEIGHT + RETENTION + 1, RETENTION);

    // OP_SET_CONFLICTING_BATCH shared header: value(1) + current_height(4) +
    // retention(4). Retention 0 keeps the DAH untouched, so the assertion below
    // is about the repair and nothing else.
    let txids: Vec<[u8; 32]> = seeds.iter().map(|s| make_txid(*s)).collect();
    let mut shared = Vec::with_capacity(9);
    shared.push(1u8);
    shared.extend_from_slice(&(HEIGHT + RETENTION + 1).to_le_bytes());
    shared.extend_from_slice(&0u32.to_le_bytes());
    let resp = request_at(
        node1.tcp_port,
        OP_SET_CONFLICTING_BATCH,
        0,
        encode_txid_batch(&txids, &shared),
    );
    let status = resp.status;
    let after: Vec<Vec<usize>> = txids.iter().map(|t| local_holders(&nodes, t)).collect();
    let master_conflicting = seeds
        .iter()
        .filter(|s| {
            node1
                .engine
                .read_metadata(&TxKey {
                    txid: make_txid(**s),
                })
                .map(|m| m.flags.contains(teraslab::record::TxFlags::CONFLICTING))
                .unwrap_or(false)
        })
        .count();

    shutdown_node(&node1);
    shutdown_node(&node2);
    shutdown_node(&node3);

    assert_eq!(
        status, STATUS_OK,
        "set_conflicting against a record the replica already reclaimed must succeed via the \
         same re-ship repair as preserve_until (status {status})"
    );
    assert_eq!(
        master_conflicting,
        seeds.len(),
        "every record must still be marked conflicting on the master — a compensated rollback \
         would leave the flag cleared"
    );
    let not_repaired: Vec<String> = seeds
        .iter()
        .zip(&after)
        .filter(|(_, h)| h.len() != 2)
        .map(|(s, h)| format!("seed {s}: holders {h:?}"))
        .collect();
    assert!(
        not_repaired.is_empty(),
        "the repair must be in the shared replication fan-out, not special-cased per op: {}",
        not_repaired.join(" | ")
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
    // Monotonic counter; other tests in this binary can only ADD to it, so a
    // `>=` delta is exact enough to prove the held-copy path was taken and not
    // flaky under parallel execution.
    let held_copy_before = dispatch_metrics().deletes_held_copy_reclaimed.get();

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
    let held_copy_delta = dispatch_metrics()
        .deletes_held_copy_reclaimed
        .get()
        .saturating_sub(held_copy_before);

    shutdown_node(&node1);
    shutdown_node(&node2);
    shutdown_node(&node3);

    assert!(
        held_copy_delta >= COUNT as u64,
        "each of the {COUNT} records had exactly one REPLICA holder, so the sweep must have \
         reported at least {COUNT} held-copy reclaims — the operator-facing signal that \
         replica-side GC ran at all; saw {held_copy_delta}"
    );

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

/// Fetch one record's raw `FIELD_ALL` item data from ONE node's local store.
///
/// Same read the Docker consistency oracle uses when it compares two holders'
/// copies byte-for-byte (`teraslab-tests/client/tests/common::payloads_match`):
/// `OP_GET_BATCH` + `FieldMask::ALL` + `FLAG_LOCAL_READ`, envelope stripped.
/// Response layout: `[count:4]` then per item `[status:1][data_len:4][data]`.
fn local_item_payload(node: &TestNode, txid: &[u8; 32]) -> Option<Vec<u8>> {
    let resp = request_at(
        node.tcp_port,
        OP_GET_BATCH,
        FLAG_LOCAL_READ,
        encode_get_batch(FieldMask::ALL, std::slice::from_ref(txid)),
    );
    if resp.status != STATUS_OK || resp.payload.len() < 9 {
        return None;
    }
    let count = u32::from_le_bytes(resp.payload[0..4].try_into().unwrap());
    if count < 1 || resp.payload[4] != 0 {
        return None;
    }
    let len = u32::from_le_bytes(resp.payload[5..9].try_into().unwrap()) as usize;
    resp.payload.get(9..9 + len).map(<[u8]>::to_vec)
}

/// The repo's own cross-holder consistency oracle, in process: byte-compare two
/// holders' `FIELD_ALL` payloads masking ONLY `updated_at` (item-data offsets
/// 61..69, each node stamps its own local clock). `delete_at_height` sits at
/// 73..77 and IS compared — holders must agree on it.
///
/// Mirrors `teraslab-tests/client/tests/common::payloads_match` exactly; if that
/// mask ever changes, this must change with it (and never the other way around
/// — widening the oracle would weaken the check this test exists to pin).
fn holder_payloads_match(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut a_copy = a.to_vec();
    let mut b_copy = b.to_vec();
    if a_copy.len() >= 69 {
        a_copy[61..69].fill(0);
        b_copy[61..69].fill(0);
    }
    a_copy == b_copy
}

/// **R1 — the preservation-expiry transition must reach every holder.**
///
/// A record is preserved (`OP_PRESERVE_UNTIL_BATCH`, which IS replicated), so
/// both holders carry `preserve_until = P`. When the pruner runs past `P`, the
/// master's Phase-0 expiry clears `preserve_until` and plants the replacement
/// `delete_at_height`. Pre-fix that phase was master-gated and replicated
/// nothing, so the replica's copy kept `preserve_until = P` forever:
///
///  * `record_due_for_sweep` returns false on the first line while a
///    preservation stands, so the copy is invisible to the replica's own DAH
///    sweep — the F2 held-copy reclaim cannot see it;
///  * every DAH-planting path (`evaluate_delete_at_height`) also early-returns
///    while preserved, so no later replicated mutation rescues it;
///  * a restart re-derives the preserve index from the device footer, so it
///    comes straight back.
///
/// The record therefore leaks on that node forever, and the two holders' DAHs
/// diverge — which the repo's own consistency oracle (`payloads_match`, which
/// masks only `updated_at`) treats as a mismatch.
///
/// Asserted here, in order: (1) both holders really are preserved first
/// (non-vacuity), (2) after the pruner every holder carries the SAME
/// `delete_at_height` and no preservation, (3) the two holders' full `FIELD_ALL`
/// payloads are byte-identical under the oracle's mask, and (4) the record is
/// then actually reclaimed everywhere once that shared DAH arrives — the leak is
/// closed, not merely relabelled.
#[test]
fn expired_preservation_transition_reaches_every_holder() {
    let node1 = create_cluster_node(891, &[]);
    let seed = [node1.swim_port];
    let node2 = create_cluster_node(892, &seed);
    let node3 = create_cluster_node(893, &seed);
    let nodes = [&node1, &node2, &node3];
    wait_for_settled_three_node_topology(&nodes);

    const COUNT: usize = 6;
    const HEIGHT: u32 = 900_000;
    const RETENTION: u32 = 100;
    // Preserve well past the DAH the spend plants, so the preservation — not the
    // spend's DAH — is what governs the record until the pruner expires it.
    const PRESERVE_TO: u32 = HEIGHT + 500;

    let seeds = owned_seeds(&node1, COUNT);
    let txids: Vec<[u8; 32]> = seeds.iter().map(|s| make_txid(*s)).collect();

    let resp = request_at(node1.tcp_port, OP_CREATE_BATCH, 0, encode_batch(&seeds));
    assert_eq!(resp.status, STATUS_OK, "create batch must be accepted");

    // Make each record sweep-ELIGIBLE (mined on the longest chain + all-spent),
    // so the master's expiry takes the branch that plants a fresh DAH.
    for s in &seeds {
        set_mined(node1.tcp_port, *s, HEIGHT, RETENTION);
        spend_to_all_spent(node1.tcp_port, *s, HEIGHT, RETENTION);
    }

    let holders_before: Vec<Vec<usize>> = txids.iter().map(|t| local_holders(&nodes, t)).collect();
    for (s, holders) in seeds.iter().zip(&holders_before) {
        assert_eq!(
            holders.len(),
            2,
            "seed {s} must be on exactly 2 nodes before the sweep, found {holders:?}"
        );
        assert!(holders.contains(&0), "seed {s} must be mastered by node 0");
    }
    let replica_idx = holders_before[0]
        .iter()
        .copied()
        .find(|i| *i != 0)
        .expect("a replica holder must exist");
    let replica = nodes[replica_idx];

    // Preserve through the CLIENT path so the replica genuinely receives the
    // preservation (this half is already replicated today).
    let resp = request_at(
        node1.tcp_port,
        OP_PRESERVE_UNTIL_BATCH,
        0,
        encode_txid_batch(&txids, &PRESERVE_TO.to_le_bytes()),
    );
    assert_eq!(
        resp.status, STATUS_OK,
        "client preserve must be accepted (status {})",
        resp.status
    );

    // NON-VACUITY: the replica must actually be preserved, otherwise everything
    // below would pass for the wrong reason.
    let replica_preserved: Vec<u32> = seeds
        .iter()
        .map(|s| {
            replica
                .engine
                .read_metadata(&TxKey {
                    txid: make_txid(*s),
                })
                .map(|m| m.preserve_until)
                .unwrap_or(0)
        })
        .collect();
    assert!(
        replica_preserved.iter().all(|p| *p == PRESERVE_TO),
        "the replica must carry the replicated preservation before the expiry runs, \
         else this test proves nothing; saw {replica_preserved:?}"
    );

    // Fire the pruner at EVERY node, at the preservation's expiry height.
    let expiry_height = PRESERVE_TO;
    let mut payload = Vec::with_capacity(8);
    payload.extend_from_slice(&expiry_height.to_le_bytes());
    payload.extend_from_slice(&RETENTION.to_le_bytes());
    for node in &nodes {
        let resp = request_at(
            node.tcp_port,
            OP_PROCESS_EXPIRED_PRESERVATIONS,
            0,
            payload.clone(),
        );
        assert_eq!(
            resp.status, STATUS_OK,
            "expiry sweep must succeed (status {})",
            resp.status
        );
    }

    // (2) Every holder agrees on the post-expiry lifecycle state.
    let mut lifecycle_divergent = Vec::new();
    let mut still_preserved = Vec::new();
    for (s, holders) in seeds.iter().zip(&holders_before) {
        let key = TxKey {
            txid: make_txid(*s),
        };
        let states: Vec<(usize, u32, u32)> = holders
            .iter()
            .filter_map(|i| {
                nodes[*i]
                    .engine
                    .read_metadata(&key)
                    .ok()
                    .map(|m| (*i, m.preserve_until, m.delete_at_height))
            })
            .collect();
        if states.iter().any(|(_, preserve, _)| *preserve != 0) {
            still_preserved.push(format!("seed {s}: {states:?}"));
        }
        let dahs: Vec<u32> = states.iter().map(|(_, _, dah)| *dah).collect();
        if dahs.windows(2).any(|w| w[0] != w[1]) || dahs.contains(&0) {
            lifecycle_divergent.push(format!("seed {s}: (node, preserve, dah) = {states:?}"));
        }
    }
    // (3) The repo's own byte-level oracle over both holders.
    let mut payload_mismatches = Vec::new();
    for (s, holders) in seeds.iter().zip(&holders_before) {
        let txid = make_txid(*s);
        let images: Vec<(usize, Vec<u8>)> = holders
            .iter()
            .filter_map(|i| local_item_payload(nodes[*i], &txid).map(|p| (*i, p)))
            .collect();
        if images.len() == 2 && !holder_payloads_match(&images[0].1, &images[1].1) {
            payload_mismatches.push(format!(
                "seed {s}: node {} vs node {} differ outside updated_at",
                images[0].0, images[1].0
            ));
        }
    }

    // (4) The transition must actually lead to a reclaim on every holder once
    // the fresh DAH arrives.
    let final_height = PRESERVE_TO + RETENTION + 1;
    let mut final_payload = Vec::with_capacity(8);
    final_payload.extend_from_slice(&final_height.to_le_bytes());
    final_payload.extend_from_slice(&RETENTION.to_le_bytes());
    for node in &nodes {
        let resp = request_at(
            node.tcp_port,
            OP_PROCESS_EXPIRED_PRESERVATIONS,
            0,
            final_payload.clone(),
        );
        assert_eq!(resp.status, STATUS_OK, "final sweep must succeed");
    }
    let survivors: Vec<String> = seeds
        .iter()
        .map(|s| (s, local_holders(&nodes, &make_txid(*s))))
        .filter(|(_, h)| !h.is_empty())
        .map(|(s, h)| format!("seed {s}: still on {h:?}"))
        .collect();

    shutdown_node(&node1);
    shutdown_node(&node2);
    shutdown_node(&node3);

    assert!(
        still_preserved.is_empty(),
        "a holder still carries `preserve_until` after the pruner expired the preservation. \
         A stale preservation makes the copy invisible to the DAH sweep AND blocks every \
         later DAH-planting path, so it leaks on that node forever: {}",
        still_preserved.join(" | ")
    );
    assert!(
        lifecycle_divergent.is_empty(),
        "holders disagree on `delete_at_height` after the preservation expired (or one \
         holder got no DAH at all). Every holder must converge on the master's scheduled \
         deletion height: {}",
        lifecycle_divergent.join(" | ")
    );
    assert!(
        payload_mismatches.is_empty(),
        "two holders' FIELD_ALL payloads differ outside `updated_at` after the expiry — \
         this is exactly what the Docker consistency oracle (`payloads_match`) fails on: {}",
        payload_mismatches.join(" | ")
    );
    assert!(
        survivors.is_empty(),
        "records survive on a holder after their post-expiry DAH came due — the \
         preservation-expiry leak is still open: {}",
        survivors.join(" | ")
    );
}

/// **R1, the other sub-population: an INELIGIBLE expiry.**
///
/// At expiry the master plants a DAH only when the record is sweep-eligible.
/// A record that is NOT all-spent just has its `preserve_until` cleared and
/// reverts to the normal lifecycle — it re-acquires a DAH later via the spend /
/// setMined path. That clear must reach holders too, and for a sharper reason
/// than the eligible case: a replica whose `preserve_until` is stale has EVERY
/// later DAH-planting path blocked (`evaluate_delete_at_height` early-returns
/// while preserved), so its copy can never become sweepable again by any route.
///
/// Asserts both holders end at `preserve_until == 0` with no DAH, and that the
/// replica's copy is genuinely reachable by a later DAH-planting mutation
/// (spend to all-spent) — i.e. the clear restored the normal lifecycle rather
/// than just moving the leak.
#[test]
fn ineligible_preservation_expiry_clears_the_preservation_on_every_holder() {
    let node1 = create_cluster_node(894, &[]);
    let seed = [node1.swim_port];
    let node2 = create_cluster_node(895, &seed);
    let node3 = create_cluster_node(896, &seed);
    let nodes = [&node1, &node2, &node3];
    wait_for_settled_three_node_topology(&nodes);

    const COUNT: usize = 6;
    const HEIGHT: u32 = 900_000;
    const RETENTION: u32 = 100;
    const PRESERVE_TO: u32 = HEIGHT + 500;

    let seeds = owned_seeds(&node1, COUNT);
    let txids: Vec<[u8; 32]> = seeds.iter().map(|s| make_txid(*s)).collect();

    let resp = request_at(node1.tcp_port, OP_CREATE_BATCH, 0, encode_batch(&seeds));
    assert_eq!(resp.status, STATUS_OK, "create batch must be accepted");
    // Mined but NOT spent → `sweep_eligible_with_mined` is false, so the master
    // takes the ineligible branch: clear the preservation, plant no DAH.
    for s in &seeds {
        set_mined(node1.tcp_port, *s, HEIGHT, RETENTION);
    }

    let holders_before: Vec<Vec<usize>> = txids.iter().map(|t| local_holders(&nodes, t)).collect();
    for (s, holders) in seeds.iter().zip(&holders_before) {
        assert_eq!(
            holders.len(),
            2,
            "seed {s} must be on 2 nodes, saw {holders:?}"
        );
        assert!(holders.contains(&0), "seed {s} must be mastered by node 0");
    }

    let resp = request_at(
        node1.tcp_port,
        OP_PRESERVE_UNTIL_BATCH,
        0,
        encode_txid_batch(&txids, &PRESERVE_TO.to_le_bytes()),
    );
    assert_eq!(resp.status, STATUS_OK, "client preserve must be accepted");

    let mut payload = Vec::with_capacity(8);
    payload.extend_from_slice(&PRESERVE_TO.to_le_bytes());
    payload.extend_from_slice(&RETENTION.to_le_bytes());
    for node in &nodes {
        let resp = request_at(
            node.tcp_port,
            OP_PROCESS_EXPIRED_PRESERVATIONS,
            0,
            payload.clone(),
        );
        assert_eq!(resp.status, STATUS_OK, "expiry sweep must succeed");
    }

    let mut still_preserved = Vec::new();
    let mut unexpected_dah = Vec::new();
    for (s, holders) in seeds.iter().zip(&holders_before) {
        let key = TxKey {
            txid: make_txid(*s),
        };
        for i in holders {
            let Ok(m) = nodes[*i].engine.read_metadata(&key) else {
                continue;
            };
            let (preserve, dah) = ({ m.preserve_until }, { m.delete_at_height });
            if preserve != 0 {
                still_preserved.push(format!("seed {s} node {i}: preserve_until = {preserve}"));
            }
            if dah != 0 {
                unexpected_dah.push(format!("seed {s} node {i}: delete_at_height = {dah}"));
            }
        }
    }

    // The clear must restore the normal lifecycle on BOTH holders: a later spend
    // to all-spent plants a DAH, which a subsequent sweep can act on. If the
    // replica were still preserved, `evaluate_delete_at_height` would refuse to
    // plant anything and the copy would be immortal.
    for s in &seeds {
        spend_to_all_spent(node1.tcp_port, *s, PRESERVE_TO, RETENTION);
    }
    let final_height = PRESERVE_TO + RETENTION + 1;
    let mut final_payload = Vec::with_capacity(8);
    final_payload.extend_from_slice(&final_height.to_le_bytes());
    final_payload.extend_from_slice(&RETENTION.to_le_bytes());
    for node in &nodes {
        let resp = request_at(
            node.tcp_port,
            OP_PROCESS_EXPIRED_PRESERVATIONS,
            0,
            final_payload.clone(),
        );
        assert_eq!(resp.status, STATUS_OK, "final sweep must succeed");
    }
    let survivors: Vec<String> = seeds
        .iter()
        .map(|s| (s, local_holders(&nodes, &make_txid(*s))))
        .filter(|(_, h)| !h.is_empty())
        .map(|(s, h)| format!("seed {s}: still on {h:?}"))
        .collect();

    shutdown_node(&node1);
    shutdown_node(&node2);
    shutdown_node(&node3);

    assert!(
        still_preserved.is_empty(),
        "an INELIGIBLE expiry left `preserve_until` set on a holder. That copy can never \
         acquire a DAH again by any route — every DAH-planting path early-returns while a \
         preservation stands: {}",
        still_preserved.join(" | ")
    );
    assert!(
        unexpected_dah.is_empty(),
        "an INELIGIBLE expiry must plant NO DAH (the record is not sweepable yet); a DAH \
         here is an immortal index entry that starves the per-call sweep cap: {}",
        unexpected_dah.join(" | ")
    );
    assert!(
        survivors.is_empty(),
        "after the preservation cleared and the record became all-spent, its fresh DAH \
         must reclaim it on every holder: {}",
        survivors.join(" | ")
    );
}
