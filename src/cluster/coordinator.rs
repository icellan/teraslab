//! Cluster coordinator: reacts to membership events, recomputes shard
//! table, and coordinates data migration.

use crate::cluster::membership::ClusterEvent;
use crate::cluster::migration::MigrationManager;
use crate::cluster::shards::*;
use crate::cluster::swim::{SwimConfig, SwimRunner};
use crate::index::TxKey;
use crate::ops::engine::Engine;
use crate::protocol::frame::{RequestFrame, ResponseFrame};
use crate::protocol::opcodes::*;
use crate::redo::RedoLog;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
// MigrationManager uses std::sync::Mutex; redo log uses parking_lot::Mutex.
use std::sync::Mutex;
type ParkingMutex<T> = parking_lot::Mutex<T>;
/// parking_lot RwLock for the shard table hot path: better reader throughput
/// than std::sync::RwLock under high contention, and no poisoning on panic.
type ShardTableLock<T> = parking_lot::RwLock<T>;
/// std::sync::RwLock for non-hot-path data (node_addrs, etc.).
use std::sync::RwLock;
use std::time::Duration;

const MIGRATION_PRESSURE_GRACE: Duration = Duration::from_secs(120);
const MIGRATION_TCP_TIMEOUT_FLOOR: Duration = Duration::from_secs(60);

fn debug_shard_set() -> &'static std::collections::HashSet<u16> {
    static SET: std::sync::OnceLock<std::collections::HashSet<u16>> = std::sync::OnceLock::new();
    SET.get_or_init(|| {
        std::env::var("TERASLAB_DEBUG_SHARDS")
            .ok()
            .map(|raw| {
                raw.split(',')
                    .filter_map(|part| part.trim().parse::<u16>().ok())
                    .collect()
            })
            .unwrap_or_default()
    })
}

fn debug_shard_enabled(shard: u16) -> bool {
    debug_shard_set().contains(&shard)
}

fn debug_shard_log(shard: u16, message: impl AsRef<str>) {
    if debug_shard_enabled(shard) {
        tracing::debug!(shard, message = message.as_ref(), "cluster: debug shard");
    }
}

fn now_millis_since_epoch() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn migration_stream_timeout(batch_size: usize) -> Duration {
    let timeout_ms = (5000 + batch_size as u64 * 50).min(60_000);
    Duration::from_millis(timeout_ms).max(MIGRATION_TCP_TIMEOUT_FLOOR)
}

fn sync_atomic_migration_bitmaps(
    mgr: &MigrationManager,
    fenced_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
    migrating_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
    inbound_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
) {
    fenced_bm.load_from(mgr.fenced_bitmap());
    inbound_bm.load_from(mgr.inbound_bitmap());
    migrating_bm.clear_all();
    for progress in mgr.active_migrations() {
        if !progress.is_complete()
            && progress.state != crate::cluster::migration::MigrationState::Failed
        {
            migrating_bm.set(progress.shard);
        }
    }
}

/// Decide whether a migration task scheduled at `topology_epoch` is still
/// current.
///
/// This deliberately compares the **per-node** `topology_epoch` (the local
/// shard-table version) — NOT the quorum-committed cluster_key. Migration
/// tasks are local lifecycle objects: a task scheduled at the local epoch
/// must complete (or fail) against that same local epoch, otherwise it is
/// stale and must be discarded. The cluster_key gate (used by
/// cross-node replication) is a separate concept — see
/// `RunningCluster::local_cluster_key`.
fn migration_epoch_current(
    shard_table: &Arc<ShardTableLock<ShardTable>>,
    topology_epoch: u64,
) -> bool {
    shard_table.read().version == topology_epoch
}

fn fail_migration_task_current_epoch(
    migration: &Arc<Mutex<MigrationManager>>,
    shard_table: &Arc<ShardTableLock<ShardTable>>,
    fenced_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
    migrating_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
    task: &MigrationTask,
    topology_epoch: u64,
    rollback: bool,
) -> bool {
    if !migration_epoch_current(shard_table, topology_epoch) {
        if let Some(m) = crate::metrics::migration_metrics() {
            m.topology_epoch_mismatch.inc();
        }
        tracing::info!(
            shard = task.shard,
            task_epoch = topology_epoch,
            current_epoch = shard_table.read().version,
            "cluster: ignoring stale migration failure",
        );
        return false;
    }
    {
        let mut mgr = migration.lock().unwrap();
        let tracked = mgr.active_migrations().iter().any(|p| {
            p.shard == task.shard
                && p.from_node == task.from_node
                && p.to_node == task.to_node
                && p.is_master == task.is_master
        });
        if !tracked {
            tracing::info!(
                shard = task.shard,
                task_epoch = topology_epoch,
                "cluster: ignoring untracked migration failure",
            );
            return false;
        }
        mgr.mark_failed(task);
        if !mgr.is_shard_fenced(task.shard) {
            fenced_bm.clear(task.shard);
        }
        migrating_bm.clear(task.shard);
    }
    if rollback {
        shard_table.write().rollback_shard(task.shard);
    }
    true
}

fn complete_migration_task_current_epoch(
    migration: &Arc<Mutex<MigrationManager>>,
    shard_table: &Arc<ShardTableLock<ShardTable>>,
    fenced_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
    migrating_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
    task: &MigrationTask,
    topology_epoch: u64,
    commit: bool,
) -> bool {
    if !migration_epoch_current(shard_table, topology_epoch) {
        if let Some(m) = crate::metrics::migration_metrics() {
            m.topology_epoch_mismatch.inc();
        }
        tracing::info!(
            shard = task.shard,
            task_epoch = topology_epoch,
            current_epoch = shard_table.read().version,
            "cluster: ignoring stale migration completion",
        );
        return false;
    }
    {
        let mut mgr = migration.lock().unwrap();
        let tracked = mgr.active_migrations().iter().any(|p| {
            p.shard == task.shard
                && p.from_node == task.from_node
                && p.to_node == task.to_node
                && p.is_master == task.is_master
        });
        if !tracked {
            tracing::info!(
                shard = task.shard,
                task_epoch = topology_epoch,
                "cluster: ignoring untracked migration completion",
            );
            return false;
        }
        mgr.mark_complete(task);
        if !mgr.is_shard_fenced(task.shard) {
            fenced_bm.clear(task.shard);
        }
        migrating_bm.clear(task.shard);
    }
    if commit {
        shard_table.write().commit_shard(task.shard);
    }
    true
}

fn committed_topology_reactivation_metrics(
    table: &ShardTable,
    committed_members: &[NodeId],
    rf: u8,
    _committed_term: u64,
) -> (u32, usize) {
    let expected = ShardTable::compute_with_epoch(committed_members, rf, 0);
    let mismatched = (0..crate::cluster::shards::NUM_SHARDS as u16)
        .filter(|&shard| {
            table.target_assignment(shard).master != expected.target_assignment(shard).master
        })
        .count() as u32;
    (mismatched, table.pending_handoff_count())
}

#[allow(clippy::too_many_arguments)]
fn install_active_routing_snapshot(
    routing: &crate::cluster::routing::RoutingInfo,
    rf: u8,
    shard_table: &Arc<ShardTableLock<ShardTable>>,
    migration: &Arc<Mutex<MigrationManager>>,
    fenced_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
    migrating_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
    inbound_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
    active_topology_members: &Arc<RwLock<Vec<NodeId>>>,
    inbound_state_path: Option<&std::path::PathBuf>,
    outbound_state_path: Option<&std::path::PathBuf>,
) -> bool {
    if routing.committed_members.is_empty() {
        return false;
    }

    let snapshot =
        ShardTable::compute_with_epoch(&routing.committed_members, rf, routing.shard_table_version);
    *shard_table.write() = snapshot;
    *active_topology_members.write().unwrap() = routing.committed_members.clone();

    {
        let mut mgr = migration.lock().unwrap();
        *mgr = MigrationManager::new();
        if let Some(path) = inbound_state_path {
            crate::cluster::migration::persist_inbound_state(path, &mgr);
        }
        if let Some(path) = outbound_state_path {
            crate::cluster::migration::persist_outbound_state(path, &mgr);
        }
    }

    fenced_bm.clear_all();
    migrating_bm.clear_all();
    inbound_bm.clear_all();
    true
}

fn old_master_available_for_handoff(
    old_master: NodeId,
    committed_members: &[NodeId],
    live_addrs: &std::collections::HashMap<NodeId, SocketAddr>,
) -> bool {
    committed_members.contains(&old_master) && live_addrs.contains_key(&old_master)
}

// ---------------------------------------------------------------------------
// Phase D: exchange phase before migration
// ---------------------------------------------------------------------------

/// One node's view of a single shard's local data state.
///
/// Reported by every alive peer during the post-commit exchange phase so the
/// coordinator can build a migration plan that reflects the *actual* on-disk
/// distribution rather than a topology-derived guess.
///
/// `flags` packs two booleans:
/// - bit 0 (`0b01`): this node believes it is the master of `shard` in the
///   currently active shard table.
/// - bit 1 (`0b10`): this node has a pending inbound migration for `shard`
///   (i.e. is a subset master receiving data).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionVersionEntry {
    /// Shard number (0..NUM_SHARDS).
    pub shard: u16,
    /// Bit-packed flags. See struct doc-comment for layout.
    pub flags: u8,
    /// Number of holders this node sees locally for the shard.
    pub replica_count: u8,
    /// Last replication sequence applied for this shard (0 if unknown).
    pub last_applied_seq: u64,
}

/// In-progress collection of `PartitionVersionEntry` reports from cluster
/// peers, anchored to a specific topology term.
///
/// Pure state with no I/O — fully unit-testable. The owning event loop is
/// responsible for both recording reports and detecting timeout.
#[derive(Debug)]
pub struct ExchangePhase {
    /// Topology term this exchange is anchored to.
    pub term: u64,
    expected: usize,
    deadline: std::time::Instant,
    received: std::collections::HashMap<NodeId, Vec<PartitionVersionEntry>>,
}

impl ExchangePhase {
    /// Begin a new exchange for `term` expecting `expected_nodes` reports
    /// (typically `members.len()`), with a wall-clock `timeout`.
    pub fn new(term: u64, expected_nodes: usize, timeout: std::time::Duration) -> Self {
        Self {
            term,
            expected: expected_nodes,
            deadline: std::time::Instant::now() + timeout,
            received: std::collections::HashMap::with_capacity(expected_nodes),
        }
    }

    /// Record a report from `node`. Returns `true` once `expected` distinct
    /// reports have been collected. Duplicate reports for the same node
    /// overwrite the previous entry but never advance the count beyond
    /// `expected`.
    pub fn record(&mut self, node: NodeId, entries: Vec<PartitionVersionEntry>) -> bool {
        self.received.insert(node, entries);
        self.is_complete()
    }

    /// `true` once `expected` distinct reports have arrived.
    pub fn is_complete(&self) -> bool {
        self.received.len() >= self.expected
    }

    /// `true` once the configured deadline has elapsed.
    pub fn is_timed_out(&self) -> bool {
        std::time::Instant::now() >= self.deadline
    }

    /// Borrow the collected per-node partition view.
    pub fn partition_view(&self) -> &std::collections::HashMap<NodeId, Vec<PartitionVersionEntry>> {
        &self.received
    }
}

/// Build a migration plan that takes the collected partition view into
/// account.
///
/// Starts from the topology-derived `ShardTable::migration_plan(old, new)`
/// and refines it:
///
/// - If the partition view shows the *new master* already has data for a
///   shard (`last_applied_seq > 0` reported by the new master itself), the
///   migration task for that shard is skipped — the data is already in
///   place.
/// - If the partition view shows the planned source has no data
///   (`last_applied_seq == 0`) but a replica reports `last_applied_seq > 0`,
///   the source is rewritten to the replica.
/// - If `partition_view` is empty (e.g. the exchange timed out), the
///   topology-derived plan is returned unchanged.
pub fn build_plan_from_partition_view(
    old_table: &ShardTable,
    new_table: &ShardTable,
    partition_view: &std::collections::HashMap<NodeId, Vec<PartitionVersionEntry>>,
    _self_id: NodeId,
) -> Vec<MigrationTask> {
    let base = ShardTable::migration_plan(old_table, new_table);
    if partition_view.is_empty() {
        return base;
    }

    // Index: (node, shard) -> last_applied_seq.
    let mut seq_by_node_shard: std::collections::HashMap<(NodeId, u16), u64> =
        std::collections::HashMap::new();
    for (node, entries) in partition_view {
        for e in entries {
            seq_by_node_shard.insert((*node, e.shard), e.last_applied_seq);
        }
    }

    let mut refined = Vec::with_capacity(base.len());
    for task in base {
        // Skip migration if the new master itself already reports data for this shard.
        let new_master_has_data = seq_by_node_shard
            .get(&(task.to_node, task.shard))
            .copied()
            .unwrap_or(0)
            > 0;
        if new_master_has_data {
            continue;
        }

        // If the planned source reports no data but a known replica reports data,
        // rewrite the source to that replica.
        let source_has_data = seq_by_node_shard
            .get(&(task.from_node, task.shard))
            .copied()
            .unwrap_or(0)
            > 0;
        if !source_has_data {
            let old_assignment = old_table.target_assignment(task.shard);
            let mut better_source: Option<NodeId> = None;
            for r in &old_assignment.replicas {
                if seq_by_node_shard
                    .get(&(*r, task.shard))
                    .copied()
                    .unwrap_or(0)
                    > 0
                {
                    better_source = Some(*r);
                    break;
                }
            }
            if let Some(src) = better_source {
                refined.push(MigrationTask {
                    shard: task.shard,
                    from_node: src,
                    to_node: task.to_node,
                    is_master: task.is_master,
                });
                continue;
            }
        }

        refined.push(task);
    }
    refined
}

/// Cluster coordinator configuration.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub self_id: NodeId,
    pub self_addr: SocketAddr,
    pub swim_bind: SocketAddr,
    pub seed_nodes: Vec<SocketAddr>,
    pub replication_factor: u8,
    pub probe_interval: Duration,
    pub suspicion_timeout: Duration,
    /// Shared secret for HMAC authentication of SWIM and inter-node traffic.
    pub cluster_secret: Option<Vec<u8>>,
    /// Maximum concurrent migration threads per topology change.
    pub max_migration_threads: usize,
    /// Timeout for topology proposal (how long non-proposer waits).
    /// Default: 3x probe_interval.
    pub topology_propose_timeout: Duration,
    /// Number of parallel TCP connections per migration target. Default: 4.
    pub migration_pool_size: usize,
    /// Records per baseline streaming batch during migration. Default: 100.
    pub migration_batch_size: usize,
    /// Persisted SWIM incarnation from a previous run. The SWIM runner will
    /// start from `persisted_incarnation + 1` to guarantee monotonicity.
    pub persisted_incarnation: u64,
}

/// The cluster coordinator. Manages membership, shard table, and migrations.
pub struct ClusterCoordinator {
    self_id: NodeId,
    pub shard_table: Arc<ShardTableLock<ShardTable>>,
    swim: Option<SwimRunner>,
    migration: Arc<Mutex<MigrationManager>>,
    replication_factor: u8,
    node_addrs: Arc<RwLock<std::collections::HashMap<NodeId, SocketAddr>>>,
    shutdown: Arc<AtomicBool>,
    initial_peak: usize,
    max_migration_threads: usize,
    /// Monotonic topology epoch counter. Incremented on every membership
    /// change. Used as the shard table version / fencing token.
    pub topology_epoch: Arc<std::sync::atomic::AtomicU64>,
    /// Topology authority for quorum-committed term management.
    pub topology_authority: Arc<crate::cluster::topology::TopologyAuthority>,
    /// Atomic mirror of `topology_authority.committed_term()` — the
    /// cluster_key value stamped on outbound `OP_REPLICA_BATCH` traffic and
    /// gated on inbound traffic.
    ///
    /// Sourced directly from `topology_authority.committed_term_shared()`,
    /// so every successful `handle_commit` advance is observable here
    /// without an explicit setter. This is intentionally NOT
    /// `topology_epoch`: `topology_epoch` is per-node and starts diverged
    /// across the cluster (initialized from the local member-list view),
    /// which would break cross-node replication batches with
    /// `ERR_STALE_EPOCH`. The committed term, in contrast, converges on
    /// the same value across all peers after each `OP_TOPOLOGY_COMMIT`.
    pub committed_cluster_key: Arc<std::sync::atomic::AtomicU64>,
    /// Members corresponding to the currently activated shard table.
    active_topology_members: Arc<RwLock<Vec<NodeId>>>,
    /// Parallel connections per migration target.
    migration_pool_size: usize,
    /// Records per baseline streaming batch.
    migration_batch_size: usize,
    /// SWIM incarnation counter for persisting alongside topology state.
    swim_incarnation: Arc<std::sync::atomic::AtomicU64>,
}

impl ClusterCoordinator {
    /// Create a new coordinator. Does NOT start the SWIM loop yet.
    ///
    /// `initial_peak` is the persisted peak cluster size from a previous run.
    /// Pass 1 for a fresh node or when no persisted state exists.
    pub fn new(config: ClusterConfig, initial_peak: usize) -> Self {
        let mut members = vec![config.self_id];
        members.sort();
        // Bootstrap with the initial topology term so partition maps served
        // before the first membership event still use term-based versioning.
        let initial_table = ShardTable::compute_with_epoch(&members, config.replication_factor, 1);

        let topology_authority = Arc::new(crate::cluster::topology::TopologyAuthority::new(
            config.self_id,
            config.topology_propose_timeout,
        ));
        let active_topology_members = Arc::new(RwLock::new(members.clone()));
        let swim = SwimRunner::new(SwimConfig {
            self_id: config.self_id,
            self_addr: config.self_addr,
            bind_addr: config.swim_bind,
            seed_nodes: config.seed_nodes.clone(),
            probe_interval: config.probe_interval,
            suspicion_timeout: config.suspicion_timeout,
            cluster_secret: config.cluster_secret.clone(),
            persisted_incarnation: config.persisted_incarnation,
            committed_term: topology_authority.committed_term_shared(),
        });
        let swim_incarnation = Arc::new(std::sync::atomic::AtomicU64::new(swim.incarnation()));

        let mut addrs = std::collections::HashMap::new();
        addrs.insert(config.self_id, config.self_addr);

        Self {
            self_id: config.self_id,
            shard_table: Arc::new(ShardTableLock::new(initial_table)),
            swim: Some(swim),
            migration: Arc::new(Mutex::new(MigrationManager::new())),
            replication_factor: config.replication_factor,
            node_addrs: Arc::new(RwLock::new(addrs)),
            shutdown: Arc::new(AtomicBool::new(false)),
            initial_peak,
            max_migration_threads: config.max_migration_threads,
            // Initial value 0 (NOT 1): `last_activated_term` in the event
            // loop is raised to `topology_epoch.load()` after every event.
            // If this atomic started at 1, the first real commit with
            // `term == 1` (produced by on_membership_changed when two
            // nodes discover each other) would be classified as a
            // "duplicate" against the baked-in 1 and the shard table
            // would never be activated. The initial single-node shard
            // table still uses a hard-coded `version = 1` in its own
            // `compute_with_epoch` call — that's unrelated to this
            // counter.
            topology_epoch: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            // Mirror the topology authority's committed-term atomic so the
            // cluster_key gate observes every quorum-committed advance
            // without an explicit setter call.
            committed_cluster_key: topology_authority.committed_term_shared(),
            topology_authority,
            active_topology_members,
            migration_pool_size: config.migration_pool_size.max(1),
            migration_batch_size: config.migration_batch_size.max(1),
            swim_incarnation,
        }
    }

    /// Start the coordinator: launches SWIM and the event processing loop.
    ///
    /// `cluster_state_path` is where the peak cluster size is persisted
    /// for quorum safety across restarts. Pass `None` for test setups.
    pub fn start(
        mut self,
        engine: Arc<Engine>,
        cluster_state_path: Option<std::path::PathBuf>,
        redo_log: Option<Arc<ParkingMutex<RedoLog>>>,
        repl_ack_policy: Option<crate::replication::manager::AckPolicy>,
        repl_best_effort: bool,
        repl_timeout: Duration,
    ) -> RunningCluster {
        let swim = self.swim.take().expect("swim already started");
        let (swim_shutdown, swim_handle, event_rx) = swim.start();

        let shard_table = self.shard_table.clone();
        let migration = self.migration.clone();
        let node_addrs = self.node_addrs.clone();
        let self_id = self.self_id;
        let rf = self.replication_factor;
        let shutdown = self.shutdown.clone();
        let peak_size = Arc::new(std::sync::atomic::AtomicUsize::new(self.initial_peak));
        let peak_size_event = peak_size.clone();
        let max_migration_threads = self.max_migration_threads;
        let topology_epoch = self.topology_epoch.clone();
        let migration_pool_size = self.migration_pool_size;
        let migration_batch_size = self.migration_batch_size;
        let topology_epoch_for_cluster = topology_epoch.clone();
        let redo_for_events = redo_log;
        let swim_incarnation_event = self.swim_incarnation.clone();
        let swim_incarnation_for_cluster = self.swim_incarnation.clone();
        let active_topology_members_event = self.active_topology_members.clone();
        let active_topology_members_for_cluster = self.active_topology_members.clone();

        // Atomic bitmaps shared between event loop, migration threads, and hot path.
        let fenced_bitmap = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());
        let inbound_atomic = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());
        let migrating_bitmap = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());
        let fenced_bm_event = fenced_bitmap.clone();
        let migrating_bm_event = migrating_bitmap.clone();
        let inbound_bm_event = inbound_atomic.clone();
        let (topology_commit_tx, topology_commit_rx) = std::sync::mpsc::channel();
        let topology_commit_tx_event = topology_commit_tx.clone();
        // Phase D: exchange phase channel.
        // After every multi-node topology commit, an exchange thread collects
        // OP_PARTITION_VERSION_REPORT from peers, then signals back here so
        // the event loop can build the migration plan against the actual
        // distribution rather than a topology-derived guess.
        type ExchangeResult = (
            Vec<NodeId>,
            u64,
            std::collections::HashMap<NodeId, Vec<PartitionVersionEntry>>,
        );
        let (exchange_complete_tx, exchange_complete_rx) =
            std::sync::mpsc::channel::<ExchangeResult>();

        // Derive inbound/topology state paths before cluster_state_path is moved into the closure.
        let inbound_state_path = cluster_state_path.as_ref().map(|p| {
            let mut s = p.as_os_str().to_os_string();
            s.push(".inbound");
            std::path::PathBuf::from(s)
        });
        let inbound_state_path_event = inbound_state_path.clone();
        let outbound_state_path = cluster_state_path.as_ref().map(|p| {
            let mut s = p.as_os_str().to_os_string();
            s.push(".outbound");
            std::path::PathBuf::from(s)
        });
        let topology_state_path = cluster_state_path.as_ref().map(|p| {
            let mut s = p.as_os_str().to_os_string();
            s.push(".topo");
            std::path::PathBuf::from(s)
        });
        let topo_state_path_event = topology_state_path.clone();
        let outbound_state_path_event = outbound_state_path.clone();
        let startup_reactivation_needed = Arc::new(AtomicBool::new(false));
        let startup_reactivation_event = startup_reactivation_needed.clone();

        // Topology authority and cluster secret for the event loop.
        let topo_authority_event = self.topology_authority.clone();
        let node_addrs_for_topo = self.node_addrs.clone();

        // Event processing thread
        let event_handle = std::thread::spawn(move || {
            let mut last_reactivation_at = std::time::Instant::now();
            let mut last_activation_at = std::time::Instant::now();
            let mut last_inbound_clear = std::time::Instant::now();
            // Track the last topology term that was activated to prevent
            // duplicate activations when the same commit signal arrives
            // through multiple channels (e.g., deterministic proposer
            // commit + fallback proposer timeout firing simultaneously).
            let mut last_activated_term: u64 = 0;
            while !shutdown.load(Ordering::Relaxed) {
                match event_rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(event) => {
                        if let ClusterEvent::MembershipChanged(members) = &event {
                            let current = members.len();
                            peak_size_event.fetch_max(current, Ordering::Relaxed);
                        }
                        Self::handle_event(
                            &event,
                            self_id,
                            rf,
                            max_migration_threads,
                            &shard_table,
                            &migration,
                            &node_addrs,
                            &engine,
                            &redo_for_events,
                            &topology_epoch,
                            migration_pool_size,
                            migration_batch_size,
                            &fenced_bm_event,
                            &migrating_bm_event,
                            &inbound_bm_event,
                            &topo_authority_event,
                            &node_addrs_for_topo,
                            &topology_commit_tx_event,
                            &topo_state_path_event,
                            &inbound_state_path_event,
                            &outbound_state_path_event,
                            &peak_size_event,
                            &swim_incarnation_event,
                            &active_topology_members_event,
                        );
                        let activated_term = topology_epoch.load(Ordering::Relaxed);
                        if activated_term > last_activated_term {
                            last_activated_term = activated_term;
                        }
                        // Track last activation for settle-time guard.
                        // handle_event may call activate_topology for
                        // MembershipChanged or TopologyStale events.
                        last_activation_at = std::time::Instant::now();
                        if matches!(&event, ClusterEvent::MembershipChanged(_))
                            && let Some(ref path) = cluster_state_path
                        {
                            let peak = peak_size_event.load(Ordering::Relaxed) as u64;
                            let epoch = topology_epoch.load(Ordering::Relaxed);
                            persist_cluster_state(path, peak, epoch);
                        }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        // Poll fallback proposer timeout: if we're not the
                        // deterministic proposer and the timeout has elapsed
                        // without a commit, step up as fallback proposer.
                        let members: Vec<NodeId> = {
                            let addrs = node_addrs.read().unwrap();
                            let mut m: Vec<NodeId> = addrs.keys().copied().collect();
                            m.sort();
                            m
                        };
                        if let Some(fallback_proposal) =
                            topo_authority_event.check_timeout(&members)
                        {
                            tracing::info!(
                                term = fallback_proposal.term,
                                "cluster: fallback proposer stepping up",
                            );
                            if let Some(ref path) = topo_state_path_event {
                                let peak = peak_size_event.load(Ordering::Relaxed) as u64;
                                let inc = swim_incarnation_event.load(Ordering::Relaxed);
                                let _ = persist_topology_state(
                                    path,
                                    &topo_authority_event.persisted_state(peak, inc),
                                );
                            }
                            // Check single-node quorum (self-vote already recorded).
                            let self_vote = crate::cluster::topology::TopologyVote {
                                term: fallback_proposal.term,
                                digest: fallback_proposal.digest,
                                voter: self_id,
                                accepted: true,
                                voter_current_term: topo_authority_event.committed_term(),
                            };
                            if let Some(commit) = topo_authority_event.handle_vote(&self_vote) {
                                let active_members =
                                    active_topology_members_event.read().unwrap().clone();
                                if topology_commit_already_activated(
                                    commit.term,
                                    last_activated_term,
                                    &active_members,
                                    &commit.members,
                                ) {
                                    tracing::debug!(
                                        term = commit.term,
                                        last_activated_term,
                                        members = commit.members.len(),
                                        "cluster: skipping duplicate self-vote activation",
                                    );
                                } else {
                                    last_activated_term = commit.term;
                                    topology_epoch.store(commit.term, Ordering::Relaxed);
                                    Self::activate_topology(
                                        &commit.members,
                                        commit.term,
                                        self_id,
                                        rf,
                                        &shard_table,
                                        &migration,
                                        &node_addrs,
                                        &engine,
                                        &redo_for_events,
                                        max_migration_threads,
                                        migration_pool_size,
                                        migration_batch_size,
                                        &fenced_bm_event,
                                        &migrating_bm_event,
                                        &inbound_bm_event,
                                        &active_topology_members_event,
                                    );
                                    last_activation_at = std::time::Instant::now();
                                }
                                topo_authority_event.handle_commit(&commit);
                            } else {
                                // Multi-node: spawn proposer thread.
                                let ta = topo_authority_event.clone();
                                let na = node_addrs_for_topo.clone();
                                let tx = topology_commit_tx_event.clone();
                                let tp = topo_state_path_event.clone();
                                let ps = peak_size_event.clone();
                                let si = swim_incarnation_event.clone();
                                std::thread::spawn(move || {
                                    run_topology_proposer(
                                        fallback_proposal,
                                        ta,
                                        na,
                                        self_id,
                                        tx,
                                        tp,
                                        ps,
                                        si,
                                    );
                                });
                            }
                        }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                }

                // Periodically prune completed inbound migrations so the
                // lock-free bitmap stays compact without dropping active
                // write fences mid-handoff.
                {
                    let mut mgr = migration.lock().unwrap();
                    mgr.cleanup_completed();
                    sync_atomic_migration_bitmaps(
                        &mgr,
                        &fenced_bm_event,
                        &migrating_bm_event,
                        &inbound_bm_event,
                    );
                    if mgr.inbound_count() > 0
                        && last_inbound_clear.elapsed() >= Duration::from_secs(5)
                    {
                        let clear_settled_inbound = mgr.active_count() == 0 && {
                            let table = shard_table.read();
                            table.pending_handoff_count() == 0
                        };
                        let removed = if clear_settled_inbound {
                            let settled_shards: std::collections::HashSet<u16> = mgr
                                .pending_inbound_entries()
                                .into_iter()
                                .map(|(shard, _)| shard)
                                .collect();
                            mgr.clear_pending_inbound_for_shards(&settled_shards)
                        } else {
                            mgr.clear_stale_inbound(Duration::from_secs(30))
                        };
                        if removed > 0 {
                            sync_atomic_migration_bitmaps(
                                &mgr,
                                &fenced_bm_event,
                                &migrating_bm_event,
                                &inbound_bm_event,
                            );
                            if let Some(ref path) = inbound_state_path_event {
                                crate::cluster::migration::persist_inbound_state(path, &mgr);
                            }
                            if clear_settled_inbound {
                                tracing::info!(
                                    removed,
                                    "cluster: cleared settled inbound migrations — no active migrations or handoffs remain",
                                );
                            } else {
                                tracing::info!(
                                    removed,
                                    "cluster: cleared stale inbound migrations"
                                );
                            }
                        }
                        last_inbound_clear = std::time::Instant::now();
                    }
                }

                // Re-activate topology if the shard table has rolled-back shards
                // from failed migrations that don't match the committed topology.
                // Only fires when: no active migrations, cooldown elapsed, and
                // the committed topology has been stable (no SWIM changes).
                // 15s cooldown balances fast recovery against migration storms:
                // short enough for Docker test scenarios, long enough to let a
                // topology change fully settle before retrying.
                let startup_reactivation_due = startup_reactivation_event.load(Ordering::Acquire)
                    && last_activation_at.elapsed() >= Duration::from_secs(5);
                let normal_reactivation_due = migration.lock().unwrap().active_count() == 0
                    && last_reactivation_at.elapsed() >= Duration::from_secs(15)
                    && last_activation_at.elapsed() >= Duration::from_secs(30);
                if startup_reactivation_due || normal_reactivation_due {
                    // Use the committed topology members, not SWIM live members.
                    // This avoids false mismatches during topology transitions.
                    let committed_members = topo_authority_event.committed_members();
                    if committed_members.len() > 1 {
                        let committed_term = topo_authority_event.committed_term();
                        let (mismatched, pending_handoffs) = {
                            let table = shard_table.read();
                            committed_topology_reactivation_metrics(
                                &table,
                                &committed_members,
                                rf,
                                committed_term,
                            )
                        };

                        if should_trigger_topology_reactivation(
                            startup_reactivation_due,
                            normal_reactivation_due,
                            mismatched,
                            pending_handoffs,
                        ) {
                            topology_epoch.store(committed_term, Ordering::Relaxed);
                            if startup_reactivation_due {
                                startup_reactivation_event.store(false, Ordering::Release);
                                tracing::info!(
                                    term = committed_term,
                                    pending_handoffs,
                                    mismatched,
                                    "cluster: re-activating topology after restored outbound migration state",
                                );
                            } else {
                                tracing::info!(
                                    term = committed_term,
                                    pending_handoffs,
                                    mismatched,
                                    "cluster: re-activating topology",
                                );
                            }
                            last_reactivation_at = std::time::Instant::now();
                            last_activation_at = std::time::Instant::now();
                            Self::activate_topology(
                                &committed_members,
                                committed_term,
                                self_id,
                                rf,
                                &shard_table,
                                &migration,
                                &node_addrs,
                                &engine,
                                &redo_for_events,
                                max_migration_threads,
                                migration_pool_size,
                                migration_batch_size,
                                &fenced_bm_event,
                                &migrating_bm_event,
                                &inbound_bm_event,
                                &active_topology_members_event,
                            );
                        }
                    }
                }

                // Poll topology commit signals from dispatch or proposer threads.
                while let Ok((members, term)) = topology_commit_rx.try_recv() {
                    // Guard: skip if this term was already activated. This
                    // prevents double activation when two commit signals for
                    // the same term arrive close together (e.g., deterministic
                    // proposer commit + fallback proposer timeout).
                    let active_members = active_topology_members_event.read().unwrap().clone();
                    if topology_commit_already_activated(
                        term,
                        last_activated_term,
                        &active_members,
                        &members,
                    ) {
                        tracing::debug!(
                            term,
                            last_activated_term,
                            members = members.len(),
                            "cluster: skipping duplicate topology commit",
                        );
                        continue;
                    }

                    // Phase D: for multi-node clusters, run the exchange phase
                    // before activating. The activation itself happens later
                    // when the exchange thread reports results into
                    // `exchange_complete_rx`. Single-node clusters skip the
                    // exchange entirely — there are no peers to query and no
                    // data distribution to discover.
                    if members.len() > 1 {
                        let exchange_tx = exchange_complete_tx.clone();
                        let node_addrs_x = node_addrs.clone();
                        let engine_x = engine.clone();
                        let shard_table_x = shard_table.clone();
                        let inbound_bm_x = inbound_bm_event.clone();
                        let cluster_key = term;
                        let members_x = members.clone();
                        std::thread::spawn(move || {
                            let view = Self::run_exchange_phase(
                                &members_x,
                                self_id,
                                cluster_key,
                                &node_addrs_x,
                                &engine_x,
                                &shard_table_x,
                                &inbound_bm_x,
                                std::time::Duration::from_millis(2000),
                            );
                            let _ = exchange_tx.send((members_x, term, view));
                        });
                        continue;
                    }

                    // Single-node cluster: activate immediately.
                    last_activated_term = term;
                    topology_epoch.store(term, Ordering::Relaxed);
                    tracing::info!(
                        term,
                        epoch = term,
                        "cluster: activating topology from commit signal (single-node, no exchange)",
                    );
                    Self::activate_topology(
                        &members,
                        term,
                        self_id,
                        rf,
                        &shard_table,
                        &migration,
                        &node_addrs,
                        &engine,
                        &redo_for_events,
                        max_migration_threads,
                        migration_pool_size,
                        migration_batch_size,
                        &fenced_bm_event,
                        &migrating_bm_event,
                        &inbound_bm_event,
                        &active_topology_members_event,
                    );
                    last_activation_at = std::time::Instant::now();
                    if let Some(ref path) = cluster_state_path {
                        let peak = peak_size_event.load(Ordering::Relaxed) as u64;
                        persist_cluster_state(path, peak, term);
                    }
                    if let Some(ref path) = outbound_state_path_event {
                        crate::cluster::migration::persist_outbound_state(
                            path,
                            &migration.lock().unwrap(),
                        );
                    }
                }

                // Phase D: poll exchange-phase results. When the exchange
                // thread completes (or times out, returning a partial view),
                // build the migration plan against the collected partition
                // view and activate.
                while let Ok((members, term, partition_view)) = exchange_complete_rx.try_recv() {
                    let active_members = active_topology_members_event.read().unwrap().clone();
                    if topology_commit_already_activated(
                        term,
                        last_activated_term,
                        &active_members,
                        &members,
                    ) {
                        tracing::debug!(
                            term,
                            last_activated_term,
                            members = members.len(),
                            "cluster: skipping duplicate exchange-phase activation",
                        );
                        continue;
                    }
                    last_activated_term = term;
                    topology_epoch.store(term, Ordering::Relaxed);
                    tracing::info!(
                        term,
                        epoch = term,
                        view_size = partition_view.len(),
                        "cluster: activating topology after exchange phase",
                    );
                    Self::activate_topology_with_view(
                        &members,
                        term,
                        self_id,
                        rf,
                        &shard_table,
                        &migration,
                        &node_addrs,
                        &engine,
                        &redo_for_events,
                        max_migration_threads,
                        migration_pool_size,
                        migration_batch_size,
                        &fenced_bm_event,
                        &migrating_bm_event,
                        &inbound_bm_event,
                        &active_topology_members_event,
                        &partition_view,
                    );
                    last_activation_at = std::time::Instant::now();
                    if let Some(ref path) = cluster_state_path {
                        let peak = peak_size_event.load(Ordering::Relaxed) as u64;
                        persist_cluster_state(path, peak, term);
                    }
                    if let Some(ref path) = outbound_state_path_event {
                        crate::cluster::migration::persist_outbound_state(
                            path,
                            &migration.lock().unwrap(),
                        );
                    }
                }
            }
        });

        RunningCluster {
            self_id,
            shard_table: self.shard_table.clone(),
            migration: self.migration.clone(),
            node_addrs: self.node_addrs.clone(),
            swim_shutdown,
            shutdown: self.shutdown.clone(),
            peak_size,
            topology_epoch: topology_epoch_for_cluster,
            repl_ack_policy,
            repl_best_effort,
            repl_timeout: repl_timeout.max(Duration::from_millis(1)),
            last_migration_pressure_ms: Arc::new(AtomicU64::new(0)),
            committed_cluster_key: self.committed_cluster_key.clone(),
            topology_authority: self.topology_authority.clone(),
            active_topology_members: active_topology_members_for_cluster,
            inbound_state_path,
            outbound_state_path,
            fenced_bitmap,
            inbound_atomic,
            migrating_bitmap,
            topology_commit_tx,
            topology_state_path,
            swim_incarnation: swim_incarnation_for_cluster,
            startup_reactivation_needed,
            _swim_handle: swim_handle,
            _event_handle: event_handle,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_event(
        event: &ClusterEvent,
        self_id: NodeId,
        rf: u8,
        max_migration_threads: usize,
        shard_table: &Arc<ShardTableLock<ShardTable>>,
        migration: &Arc<Mutex<MigrationManager>>,
        node_addrs: &Arc<RwLock<std::collections::HashMap<NodeId, SocketAddr>>>,
        engine: &Arc<Engine>,
        redo_for_events: &Option<Arc<ParkingMutex<RedoLog>>>,
        topology_epoch: &Arc<std::sync::atomic::AtomicU64>,
        migration_pool_size: usize,
        migration_batch_size: usize,
        fenced_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
        migrating_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
        inbound_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
        topology_authority: &Arc<crate::cluster::topology::TopologyAuthority>,
        node_addrs_for_topo: &Arc<RwLock<std::collections::HashMap<NodeId, SocketAddr>>>,
        topology_commit_tx: &std::sync::mpsc::Sender<(Vec<NodeId>, u64)>,
        topology_state_path: &Option<std::path::PathBuf>,
        inbound_state_path: &Option<std::path::PathBuf>,
        outbound_state_path: &Option<std::path::PathBuf>,
        peak_size: &Arc<std::sync::atomic::AtomicUsize>,
        swim_incarnation: &Arc<std::sync::atomic::AtomicU64>,
        active_topology_members: &Arc<RwLock<Vec<NodeId>>>,
    ) {
        match event {
            ClusterEvent::NodeJoined(node, addr) => {
                tracing::info!(?node, %addr, "cluster: node joined");
                node_addrs.write().unwrap().insert(*node, *addr);

                // Retry any previously failed migrations — the newly
                // joined node may be the target that was unavailable.
                let retry_tasks = migration.lock().unwrap().take_failed_tasks();
                if !retry_tasks.is_empty() {
                    tracing::info!(
                        count = retry_tasks.len(),
                        "cluster: retrying failed migrations"
                    );
                    let epoch = topology_epoch.load(Ordering::Relaxed);
                    let retry_shards: std::collections::HashSet<u16> =
                        retry_tasks.iter().map(|t| t.shard).collect();
                    let keys_map = engine.keys_by_shard_filtered(&retry_shards);
                    let all_keys: Vec<TxKey> =
                        keys_map.values().flat_map(|v| v.iter().copied()).collect();
                    let migration_ref = migration.clone();
                    let node_addrs_ref = node_addrs.clone();
                    let eng = engine.clone();
                    let redo = redo_for_events.clone();
                    let st = shard_table.clone();
                    let fb = fenced_bm.clone();
                    let mb = migrating_bm.clone();
                    let ib = inbound_bm.clone();
                    std::thread::spawn(move || {
                        Self::run_migration_tasks_with_global_limit(
                            retry_tasks,
                            all_keys,
                            node_addrs_ref,
                            eng,
                            migration_ref,
                            st,
                            redo,
                            epoch,
                            max_migration_threads,
                            migration_pool_size,
                            migration_batch_size,
                            fb,
                            mb,
                            ib,
                            self_id,
                        );
                    });
                }
            }
            ClusterEvent::NodeLeft(node) => {
                tracing::info!(?node, "cluster: node left");
                node_addrs.write().unwrap().remove(node);
            }
            ClusterEvent::MembershipChanged(members) => {
                tracing::info!(
                    count = members.len(),
                    ?members,
                    "cluster: membership changed"
                );

                // Gate through topology authority: only the deterministic
                // proposer (lowest NodeId) initiates the quorum protocol.
                // The shard table is NOT activated until quorum commits.
                if let Some(proposal) = topology_authority.on_membership_changed(members) {
                    tracing::info!(
                        term = proposal.term,
                        members = proposal.members.len(),
                        "cluster: proposing topology",
                    );
                    // Persist voted_term before broadcasting.
                    if let Some(path) = topology_state_path {
                        let peak = peak_size.load(Ordering::Relaxed) as u64;
                        let inc = swim_incarnation.load(Ordering::Relaxed);
                        let _ = persist_topology_state(
                            path,
                            &topology_authority.persisted_state(peak, inc),
                        );
                    }

                    // Check if self-vote already achieves quorum (single-node cluster).
                    let self_vote = crate::cluster::topology::TopologyVote {
                        term: proposal.term,
                        digest: proposal.digest,
                        voter: self_id,
                        accepted: true,
                        voter_current_term: topology_authority.committed_term(),
                    };
                    if let Some(commit) = topology_authority.handle_vote(&self_vote) {
                        // Single-node: quorum met immediately. Activate directly.
                        topology_epoch.store(commit.term, Ordering::Relaxed);
                        tracing::info!(
                            term = commit.term,
                            "cluster: single-node quorum — activating"
                        );
                        topology_authority.handle_commit(&commit);
                        Self::activate_topology(
                            &commit.members,
                            commit.term,
                            self_id,
                            rf,
                            shard_table,
                            migration,
                            node_addrs,
                            engine,
                            redo_for_events,
                            max_migration_threads,
                            migration_pool_size,
                            migration_batch_size,
                            fenced_bm,
                            migrating_bm,
                            inbound_bm,
                            active_topology_members,
                        );
                        if let Some(path) = topology_state_path {
                            let peak = peak_size.load(Ordering::Relaxed) as u64;
                            let inc = swim_incarnation.load(Ordering::Relaxed);
                            let _ = persist_topology_state(
                                path,
                                &topology_authority.persisted_state(peak, inc),
                            );
                        }
                    } else {
                        // Multi-node: spawn proposer thread to broadcast proposal,
                        // collect votes, and signal commit via channel.
                        let ta = topology_authority.clone();
                        let na = node_addrs_for_topo.clone();
                        let tx = topology_commit_tx.clone();
                        let tp = topology_state_path.clone();
                        let ps = peak_size.clone();
                        let si = swim_incarnation.clone();
                        std::thread::spawn(move || {
                            run_topology_proposer(proposal, ta, na, self_id, tx, tp, ps, si);
                        });
                    }
                }
                // Non-proposer nodes do nothing here — they will receive
                // OP_TOPOLOGY_COMMIT via dispatch and signal_topology_committed().
            }
            ClusterEvent::NodeSuspect(node) => {
                tracing::info!(?node, "cluster: node suspected");
            }
            ClusterEvent::TopologyStale(remote_term) => {
                let local_term = topology_authority.committed_term();
                if *remote_term > local_term {
                    // Spawn catch-up in a background thread so the event loop
                    // stays responsive to SWIM probes and suspect expiration.
                    // Previously, synchronous TCP connections to dead peers
                    // blocked the event loop for 500ms-3s per peer, delaying
                    // failure detection by 10-20x the configured timeout.
                    let catch_up_ta = topology_authority.clone();
                    let catch_up_addrs = node_addrs_for_topo.clone();
                    let catch_up_st = shard_table.clone();
                    let catch_up_mig = migration.clone();
                    let catch_up_fenced = fenced_bm.clone();
                    let catch_up_migrating = migrating_bm.clone();
                    let catch_up_inbound = inbound_bm.clone();
                    let catch_up_atm = active_topology_members.clone();
                    let catch_up_inb_path = inbound_state_path.clone();
                    let catch_up_out_path = outbound_state_path.clone();
                    let catch_up_tx = topology_commit_tx.clone();
                    let catch_up_topo_path = topology_state_path.clone();
                    let catch_up_peak = peak_size.clone();
                    let catch_up_si = swim_incarnation.clone();
                    let remote_term = *remote_term;
                    std::thread::spawn(move || {
                        tracing::info!(
                            local_term,
                            remote_term,
                            "cluster: topology stale — catch-up thread started",
                        );
                        let topology_authority = &catch_up_ta;
                        let node_addrs_for_topo = &catch_up_addrs;
                        let shard_table = &catch_up_st;
                        let migration = &catch_up_mig;
                        let fenced_bm = &catch_up_fenced;
                        let migrating_bm = &catch_up_migrating;
                        let inbound_bm = &catch_up_inbound;
                        let active_topology_members = &catch_up_atm;
                        let inbound_state_path = &catch_up_inb_path;
                        let outbound_state_path = &catch_up_out_path;
                        let topology_commit_tx = &catch_up_tx;
                        let topology_state_path = &catch_up_topo_path;
                        let peak_size = &catch_up_peak;
                        let swim_incarnation = &catch_up_si;
                        let committed_members = topology_authority.committed_members();
                        let peers: Vec<SocketAddr> = {
                            let addrs = node_addrs_for_topo.read().unwrap();
                            addrs
                                .iter()
                                .filter(|(id, _)| **id != self_id)
                                .filter(|(id, _)| {
                                    committed_members.is_empty() || committed_members.contains(id)
                                })
                                .map(|(_, &addr)| addr)
                                .collect()
                        };
                        let local_active_version = { shard_table.read().version };
                        for peer_addr in &peers {
                            if let Ok(payload) =
                                send_topology_frame(*peer_addr, OP_GET_PARTITION_MAP, &[])
                                && let Some(routing) =
                                    crate::cluster::routing::RoutingInfo::decode(&payload)
                                && routing.shard_table_version > local_active_version
                                && !routing.committed_members.is_empty()
                            {
                                let mut snapshot_members = routing.committed_members.clone();
                                snapshot_members.sort();
                                if routing.shard_table_version > topology_authority.committed_term()
                                {
                                    let synthetic = crate::cluster::topology::TopologyCommit {
                                        term: routing.shard_table_version,
                                        proposer: snapshot_members[0],
                                        members: snapshot_members.clone(),
                                        digest:
                                            crate::cluster::topology::TopologyTerm::compute_digest(
                                                routing.shard_table_version,
                                                &snapshot_members,
                                            ),
                                    };
                                    let _ = topology_authority.handle_commit(&synthetic);
                                }
                                if install_active_routing_snapshot(
                                    &routing,
                                    rf,
                                    shard_table,
                                    migration,
                                    fenced_bm,
                                    migrating_bm,
                                    inbound_bm,
                                    active_topology_members,
                                    inbound_state_path.as_ref(),
                                    outbound_state_path.as_ref(),
                                ) {
                                    tracing::info!(
                                        term = routing.shard_table_version,
                                        %peer_addr,
                                        "cluster: catch-up: installed active routing snapshot",
                                    );
                                }
                                break;
                            }
                        }

                        let local_term = topology_authority.committed_term();
                        let mut caught_up = false;
                        for peer_addr in &peers {
                            if let Ok(payload) =
                                send_topology_frame(*peer_addr, OP_GET_COMMITTED_TOPOLOGY, &[])
                                && let Some(commit) =
                                    crate::cluster::topology::TopologyCommit::deserialize(&payload)
                            {
                                let remote_members = commit.members.clone();
                                if remote_members.len() <= 1 {
                                    continue; // Peer is single-node, skip
                                }
                                // Skip if the committed term isn't higher
                                // than ours — the peer may not have advanced yet.
                                if commit.term <= local_term {
                                    continue;
                                }
                                if let Some(applied_term) =
                                    topology_authority.handle_commit(&commit)
                                {
                                    tracing::info!(
                                        term = applied_term,
                                        %peer_addr,
                                        members = remote_members.len(),
                                        "cluster: catch-up: applied term from peer",
                                    );
                                    if let Some(ref path) = *topology_state_path {
                                        let peak = peak_size.load(Ordering::Relaxed) as u64;
                                        let inc = swim_incarnation.load(Ordering::Relaxed);
                                        let _ = persist_topology_state(
                                            path,
                                            &topology_authority.persisted_state(peak, inc),
                                        );
                                    }
                                    // Signal the event loop to activate the topology.
                                    let _ = topology_commit_tx
                                        .send((remote_members.clone(), commit.term));
                                    caught_up = true;
                                    break;
                                }
                            }
                        }

                        // If direct fetch didn't work, fall back to the re-proposal path.
                        // This always converges: the new proposal will collect votes from
                        // peers that have already committed a higher term.
                        if !caught_up {
                            let members: Vec<NodeId> = {
                                let addrs = node_addrs_for_topo.read().unwrap();
                                let mut m: Vec<NodeId> = addrs.keys().copied().collect();
                                m.sort();
                                m
                            };
                            topology_authority.reset_membership_timer();
                            if let Some(proposal) =
                                topology_authority.on_membership_changed(&members)
                            {
                                tracing::info!(
                                    term = proposal.term,
                                    members = proposal.members.len(),
                                    "cluster: catch-up: re-proposing topology",
                                );
                                if let Some(path) = topology_state_path {
                                    let peak = peak_size.load(Ordering::Relaxed) as u64;
                                    let inc = swim_incarnation.load(Ordering::Relaxed);
                                    let _ = persist_topology_state(
                                        path,
                                        &topology_authority.persisted_state(peak, inc),
                                    );
                                }
                                let self_vote = crate::cluster::topology::TopologyVote {
                                    term: proposal.term,
                                    digest: proposal.digest,
                                    voter: self_id,
                                    accepted: true,
                                    voter_current_term: topology_authority.committed_term(),
                                };
                                if let Some(commit) = topology_authority.handle_vote(&self_vote) {
                                    // Single-node quorum: signal the event loop to activate.
                                    topology_authority.handle_commit(&commit);
                                    let _ = topology_commit_tx
                                        .send((commit.members.clone(), commit.term));
                                } else {
                                    let ta = topology_authority.clone();
                                    let na = node_addrs_for_topo.clone();
                                    let tx = topology_commit_tx.clone();
                                    let tp = topology_state_path.clone();
                                    let ps = peak_size.clone();
                                    let si = swim_incarnation.clone();
                                    run_topology_proposer(
                                        proposal, ta, na, self_id, tx, tp, ps, si,
                                    );
                                }
                            }
                        }
                    }); // end of catch-up thread
                }
            }
        }
    }

    /// Activate a new topology without a collected partition view.
    ///
    /// Preserved as a thin wrapper for callers that bypass the exchange phase
    /// (e.g. single-node bootstrap, fast-path activation, fallback proposer
    /// self-vote). Equivalent to calling `activate_topology_with_view` with
    /// an empty `partition_view`, which causes
    /// [`build_plan_from_partition_view`] to fall back to the
    /// topology-derived plan.
    #[allow(clippy::too_many_arguments)]
    fn activate_topology(
        members: &[NodeId],
        epoch: u64,
        self_id: NodeId,
        rf: u8,
        shard_table: &Arc<ShardTableLock<ShardTable>>,
        migration: &Arc<Mutex<MigrationManager>>,
        node_addrs: &Arc<RwLock<std::collections::HashMap<NodeId, SocketAddr>>>,
        engine: &Arc<Engine>,
        redo_for_events: &Option<Arc<ParkingMutex<RedoLog>>>,
        max_parallel_migrations: usize,
        migration_pool_size: usize,
        migration_batch_size: usize,
        fenced_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
        migrating_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
        inbound_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
        active_topology_members: &Arc<RwLock<Vec<NodeId>>>,
    ) {
        let empty_view: std::collections::HashMap<NodeId, Vec<PartitionVersionEntry>> =
            std::collections::HashMap::new();
        Self::activate_topology_with_view(
            members,
            epoch,
            self_id,
            rf,
            shard_table,
            migration,
            node_addrs,
            engine,
            redo_for_events,
            max_parallel_migrations,
            migration_pool_size,
            migration_batch_size,
            fenced_bm,
            migrating_bm,
            inbound_bm,
            active_topology_members,
            &empty_view,
        );
    }

    /// Activate a new topology, refining the migration plan with the supplied
    /// partition view (collected during the post-commit exchange phase).
    ///
    /// When `partition_view` is empty, behaves identically to the legacy
    /// pre-Phase-D logic — the migration plan is computed solely from the
    /// topology diff. When populated, [`build_plan_from_partition_view`]
    /// uses the per-node `last_applied_seq` data to skip migrations whose
    /// destination already has the data, and to retarget the source onto a
    /// replica when the planned source has none.
    #[allow(clippy::too_many_arguments)]
    fn activate_topology_with_view(
        members: &[NodeId],
        epoch: u64,
        self_id: NodeId,
        rf: u8,
        shard_table: &Arc<ShardTableLock<ShardTable>>,
        migration: &Arc<Mutex<MigrationManager>>,
        node_addrs: &Arc<RwLock<std::collections::HashMap<NodeId, SocketAddr>>>,
        engine: &Arc<Engine>,
        redo_for_events: &Option<Arc<ParkingMutex<RedoLog>>>,
        max_parallel_migrations: usize,
        migration_pool_size: usize,
        migration_batch_size: usize,
        fenced_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
        migrating_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
        inbound_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
        active_topology_members: &Arc<RwLock<Vec<NodeId>>>,
        partition_view: &std::collections::HashMap<NodeId, Vec<PartitionVersionEntry>>,
    ) {
        *active_topology_members.write().unwrap() = members.to_vec();

        // Fast path: when the engine has zero records AND the current shard
        // table is single-node (this node only), skip the expensive migration
        // machinery entirely. There's no data to migrate, no handoff to
        // coordinate, and no completion handshakes to send. Just install the
        // new shard table directly.
        //
        // This eliminates ~5461 empty-shard TCP handshakes per node during
        // initial cluster formation. The condition is conservative: only
        // fires on fresh startup (single-node bootstrap, empty engine) to
        // avoid interfering with scale-up scenarios where existing nodes
        // need to migrate data TO this node.
        {
            let old_table = shard_table.read();
            let old_unique: std::collections::HashSet<NodeId> =
                old_table.shard_counts().keys().copied().collect();
            let is_single_node_bootstrap = old_unique.len() == 1 && old_unique.contains(&self_id);
            drop(old_table);

            if engine.index_len() == 0 && is_single_node_bootstrap {
                let new_table = ShardTable::compute_with_epoch(members, rf, epoch);
                tracing::info!(
                    epoch,
                    members = members.len(),
                    "cluster: empty engine — fast-path shard table install",
                );
                *shard_table.write() = new_table;
                {
                    let mut mgr = migration.lock().unwrap();
                    *mgr = MigrationManager::new();
                }
                fenced_bm.clear_all();
                migrating_bm.clear_all();
                inbound_bm.clear_all();
                return;
            }
        }

        // Compute new migration plan FIRST so we know which existing
        // migrations can be preserved (same source, target, and type).
        let old_table_snap = shard_table.read().clone();
        let old_epoch = old_table_snap.version;
        let new_table = ShardTable::compute_with_epoch(members, rf, epoch);
        // Phase D: when a partition view is available, use it to skip
        // migrations whose destination already has the data and to redirect
        // the source onto a replica when the planned source has none.
        // An empty view (e.g. exchange skipped or timed out) reduces to the
        // legacy topology-derived plan.
        let new_plan =
            build_plan_from_partition_view(&old_table_snap, &new_table, partition_view, self_id);
        let new_replica_plan = ShardTable::replica_migration_plan(&old_table_snap, &new_table);
        drop(old_table_snap);

        let populated_shards: std::collections::HashSet<u16> = (0..NUM_SHARDS as u16)
            .filter(|&s| engine.shard_record_count(s) > 0)
            .collect();
        let local_store_empty = populated_shards.is_empty();
        let all_new_tasks = build_topology_activation_tasks(
            &new_plan,
            &new_replica_plan,
            &populated_shards,
            &new_table,
            self_id,
        );

        // Build a set of (shard, from, to, is_master) for the new plan.
        let new_task_set: std::collections::HashSet<(u16, NodeId, NodeId, bool)> = all_new_tasks
            .iter()
            .map(|t| (t.shard, t.from_node, t.to_node, t.is_master))
            .collect();

        // A worker spawned for a prior topology epoch self-aborts once it
        // observes the epoch has advanced. Preserve active tasks only for
        // same-epoch reactivation; cross-epoch tasks must be cancelled and
        // respawned so they have a live worker for the new epoch.
        let preserve_active_workers = migration_workers_can_be_preserved(old_epoch, epoch);

        // Determine which existing migrations can be preserved.
        let preserved_tasks: std::collections::HashSet<(u16, NodeId, NodeId, bool)>;
        {
            let mut mgr = migration.lock().unwrap();
            let old_inbound = mgr.inbound_count();
            let old_active = mgr.active_count();
            let old_failed = mgr.failed_count();

            // Identify preservable migrations: active, not complete/failed,
            // and appearing in the new plan with same source/target.
            preserved_tasks = mgr
                .active_migrations()
                .iter()
                .filter(|p| {
                    preserve_active_workers
                        && p.state != crate::cluster::migration::MigrationState::Complete
                        && p.state != crate::cluster::migration::MigrationState::Failed
                        && new_task_set.contains(&(p.shard, p.from_node, p.to_node, p.is_master))
                })
                .map(|p| (p.shard, p.from_node, p.to_node, p.is_master))
                .collect();

            // Cancel only non-preserved migrations.
            let stale_tasks: Vec<MigrationTask> = mgr
                .active_migrations()
                .iter()
                .filter(|p| {
                    p.state != crate::cluster::migration::MigrationState::Complete
                        && p.state != crate::cluster::migration::MigrationState::Failed
                        && !preserved_tasks.contains(&(
                            p.shard,
                            p.from_node,
                            p.to_node,
                            p.is_master,
                        ))
                })
                .map(|p| MigrationTask {
                    shard: p.shard,
                    from_node: p.from_node,
                    to_node: p.to_node,
                    is_master: p.is_master,
                })
                .collect();

            for t in &stale_tasks {
                mgr.mark_failed(t);
            }

            // Clear inbound for non-preserved shards only.
            // We can't selectively clear inbound_migrations easily, so
            // clear all and re-register preserved ones after.
            mgr.clear_inbound();
            mgr.cleanup_completed();

            let preserved_count = preserved_tasks.len();
            let cancelled = old_active.saturating_sub(preserved_count);
            if old_inbound > 0 || cancelled > 0 || old_failed > 0 {
                tracing::info!(
                    preserved = preserved_count,
                    cancelled,
                    old_failed,
                    old_inbound,
                    preserve_active_workers,
                    "cluster: topology change — preserved, cancelled, and cleared migrations",
                );
            }
        }

        // Reset atomic bitmaps for non-preserved shards.
        // For preserved shards, keep their fenced/migrating state.
        if preserved_tasks.is_empty() {
            fenced_bm.clear_all();
            migrating_bm.clear_all();
            inbound_bm.clear_all();
        } else {
            // Reload fenced bitmap from the manager (preserves fences for active migrations).
            let mgr = migration.lock().unwrap();
            fenced_bm.load_from(mgr.fenced_bitmap());
            // Inbound was already cleared in the manager, so clear its atomic.
            inbound_bm.clear_all();
            // Rebuild migrating bitmap from the active migration list.
            migrating_bm.clear_all();
            for p in mgr.active_migrations() {
                if p.state != crate::cluster::migration::MigrationState::Complete
                    && p.state != crate::cluster::migration::MigrationState::Failed
                {
                    migrating_bm.set(p.shard);
                }
            }
            drop(mgr);
        }

        let plan = new_plan;
        let replica_plan = new_replica_plan;

        let backfill_tasks = all_new_tasks
            .len()
            .saturating_sub(plan.len() + replica_plan.len());

        if all_new_tasks.is_empty() {
            *shard_table.write() = new_table;
        } else {
            let all_tasks = all_new_tasks;

            let outbound_tasks: Vec<MigrationTask> = all_tasks
                .iter()
                .filter(|t| {
                    t.from_node == self_id
                        && !preserved_tasks.contains(&(
                            t.shard,
                            t.from_node,
                            t.to_node,
                            t.is_master,
                        ))
                })
                .cloned()
                .collect();
            let inbound = all_tasks.iter().filter(|t| t.to_node == self_id).count();
            let master_out = outbound_tasks.iter().filter(|t| t.is_master).count();
            let replica_out = outbound_tasks.iter().filter(|t| !t.is_master).count();
            tracing::info!(
                masters = plan.len(),
                replicas = replica_plan.len(),
                backfill = backfill_tasks,
                outbound = outbound_tasks.len(),
                master_out,
                replica_out,
                inbound,
                "cluster: migration plan",
            );

            let outbound_shard_set: std::collections::HashSet<u16> =
                outbound_tasks.iter().map(|t| t.shard).collect();
            let outbound_master_source_shards: std::collections::HashSet<u16> = outbound_tasks
                .iter()
                .filter(|t| t.is_master)
                .map(|t| t.shard)
                .collect();

            // Build the set of shards that have MASTER migration tasks.
            // Only master migrations need the old master to keep serving
            // during handoff (Copying state). Shards with only replica
            // tasks (or no tasks) go directly to ServingNew — the new
            // master already has the data and can serve immediately.
            let master_migration_shards: std::collections::HashSet<u16> = all_tasks
                .iter()
                .filter(|t| t.is_master)
                .map(|t| t.shard)
                .collect();

            // Phase 1 (event loop, fast): begin handoff and register
            // migration tasks. Holds the shard_table write lock briefly
            // for O(shards) work, not O(records).
            {
                let mut table = shard_table.write();
                // Snapshot old masters before the handoff swaps assignments.
                let alive_addrs = node_addrs.read().unwrap();
                let old_masters: Vec<NodeId> = (0..crate::cluster::shards::NUM_SHARDS as u16)
                    .map(|s| table.target_assignment(s).master)
                    .collect();
                table.begin_handoff_with(&new_table, |s| {
                    // Only enter Copying if there's actually a migration task
                    // for this shard. Otherwise the shard would be stuck in
                    // Copying indefinitely with nothing to complete it.
                    if !master_migration_shards.contains(&s) {
                        return false;
                    }
                    // Fresh bootstrap nodes can briefly become the nominal
                    // source for empty shards before the next topology term
                    // supersedes them. Skip local handoff in that case.
                    if local_store_empty && outbound_master_source_shards.contains(&s) {
                        return false;
                    }
                    let local_has_data = engine.shard_record_count(s) > 0;
                    let old_master = old_masters[s as usize];
                    should_begin_handoff_for_shard(
                        s,
                        self_id,
                        old_master,
                        local_has_data,
                        old_master_available_for_handoff(old_master, members, &alive_addrs),
                        &outbound_master_source_shards,
                    )
                });
                drop(alive_addrs);
            }

            {
                let mut mgr = migration.lock().unwrap();
                let new_tasks: Vec<MigrationTask> = all_tasks
                    .iter()
                    .filter(|t| {
                        let preserved = preserved_tasks.contains(&(
                            t.shard,
                            t.from_node,
                            t.to_node,
                            t.is_master,
                        ));
                        let self_source_and_empty = local_store_empty && t.from_node == self_id;
                        !preserved && !self_source_and_empty
                    })
                    .cloned()
                    .collect();
                mgr.start_outbound(&new_tasks, self_id, &populated_shards);
            }

            // Phase 2 (worker thread): key snapshot + migration spawning.
            // keys_by_shard_filtered scans the full index and can take
            // tens of milliseconds at millions of records. Running it on
            // a worker thread keeps the event loop responsive to SWIM
            // probes and topology commits.
            let engine_w = engine.clone();
            let node_addrs_w = node_addrs.clone();
            let migration_w = migration.clone();
            let shard_table_w = shard_table.clone();
            let redo_w = redo_for_events.clone();
            let fenced_bm_w = fenced_bm.clone();
            let migrating_bm_w = migrating_bm.clone();
            let inbound_bm_w = inbound_bm.clone();

            std::thread::spawn(move || {
                let pre_swap_keys_by_shard = engine_w.keys_by_shard_filtered(&outbound_shard_set);
                let pre_swap_keys: Vec<TxKey> = pre_swap_keys_by_shard
                    .values()
                    .flat_map(|v| v.iter().copied())
                    .collect();

                Self::run_migration_tasks_with_global_limit(
                    outbound_tasks,
                    pre_swap_keys,
                    node_addrs_w,
                    engine_w,
                    migration_w,
                    shard_table_w,
                    redo_w,
                    epoch,
                    max_parallel_migrations,
                    migration_pool_size,
                    migration_batch_size,
                    fenced_bm_w,
                    migrating_bm_w,
                    inbound_bm_w,
                    self_id,
                );
            });
        }
    }

    /// Phase D: collect `OP_PARTITION_VERSION_REPORT` from every alive peer
    /// (and the local node) and return the per-node partition view.
    ///
    /// Self-report is computed locally without TCP. Peers are queried in
    /// parallel; an unreachable peer is treated as "no data" rather than
    /// blocking the full per-peer timeout. The total wall-clock budget is
    /// bounded by `total_timeout`.
    #[allow(clippy::too_many_arguments)]
    fn run_exchange_phase(
        members: &[NodeId],
        self_id: NodeId,
        cluster_key: u64,
        node_addrs: &Arc<RwLock<std::collections::HashMap<NodeId, SocketAddr>>>,
        engine: &Arc<Engine>,
        shard_table: &Arc<ShardTableLock<ShardTable>>,
        inbound_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
        total_timeout: std::time::Duration,
    ) -> std::collections::HashMap<NodeId, Vec<PartitionVersionEntry>> {
        let mut phase = ExchangePhase::new(cluster_key, members.len(), total_timeout);

        // Self-report (no TCP).
        let self_entries =
            build_self_partition_version_entries(self_id, engine.as_ref(), shard_table, inbound_bm);
        phase.record(self_id, self_entries);

        // Snapshot peer addresses up front.
        let peer_addrs: Vec<(NodeId, SocketAddr)> = {
            let addrs = node_addrs.read().unwrap();
            members
                .iter()
                .filter(|n| **n != self_id)
                .filter_map(|n| addrs.get(n).copied().map(|a| (*n, a)))
                .collect()
        };

        if peer_addrs.is_empty() {
            return phase.partition_view().clone();
        }

        // Query peers in parallel. Each peer thread sends its result over a
        // shared channel; the collecting loop drains the channel until the
        // total deadline elapses or every expected reply has arrived.
        type PeerResult = (NodeId, Vec<PartitionVersionEntry>);
        let (tx, rx) = std::sync::mpsc::channel::<PeerResult>();
        for (peer, addr) in &peer_addrs {
            let tx = tx.clone();
            let peer = *peer;
            let addr = *addr;
            std::thread::spawn(move || {
                let entries = match send_topology_frame(
                    addr,
                    OP_PARTITION_VERSION_REPORT,
                    &cluster_key.to_le_bytes(),
                ) {
                    Ok(payload) => parse_partition_version_response(&payload).unwrap_or_default(),
                    Err(_) => Vec::new(),
                };
                let _ = tx.send((peer, entries));
            });
        }
        drop(tx);

        let deadline = std::time::Instant::now() + total_timeout;
        for _ in 0..peer_addrs.len() {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match rx.recv_timeout(remaining) {
                Ok((peer, entries)) => {
                    phase.record(peer, entries);
                }
                Err(_) => break,
            }
        }

        phase.partition_view().clone()
    }

    #[allow(clippy::too_many_arguments)]
    fn run_migration_tasks_with_global_limit(
        tasks: Vec<MigrationTask>,
        all_keys: Vec<TxKey>,
        node_addrs: Arc<RwLock<std::collections::HashMap<NodeId, SocketAddr>>>,
        engine: Arc<Engine>,
        migration: Arc<Mutex<MigrationManager>>,
        shard_table: Arc<ShardTableLock<ShardTable>>,
        redo_log: Option<Arc<ParkingMutex<RedoLog>>>,
        topology_epoch: u64,
        max_parallel_migrations: usize,
        migration_pool_size: usize,
        batch_size: usize,
        fenced_bm: Arc<crate::cluster::migration::AtomicShardBitmap>,
        migrating_bm: Arc<crate::cluster::migration::AtomicShardBitmap>,
        inbound_bm: Arc<crate::cluster::migration::AtomicShardBitmap>,
        self_id: NodeId,
    ) {
        if tasks.is_empty() {
            return;
        }

        // Pre-group keys by shard once (O(keys) total).
        let mut keys_by_shard: std::collections::HashMap<u16, Vec<TxKey>> =
            std::collections::HashMap::new();
        for key in all_keys {
            keys_by_shard
                .entry(ShardTable::shard_for_key(&key))
                .or_default()
                .push(key);
        }

        // Group all tasks by target node. Each target gets ALL its shards
        // dispatched to run_migration_batch which handles them with a
        // parallel connection pool — no per-shard TCP overhead.
        let mut tasks_by_target: std::collections::HashMap<NodeId, Vec<MigrationTask>> =
            std::collections::HashMap::new();
        for task in tasks {
            tasks_by_target.entry(task.to_node).or_default().push(task);
        }

        // Flatten all keys needed for each target batch.
        let mut batches: Vec<(NodeId, Vec<MigrationTask>)> = tasks_by_target.into_iter().collect();
        batches.sort_by_key(|(node, _)| *node);

        let addrs = node_addrs.read().unwrap().clone();
        let max_parallel_migrations = max_parallel_migrations.max(1);
        let migration_pool_size = migration_pool_size.max(1);

        for target_group in batches.chunks(max_parallel_migrations) {
            std::thread::scope(|scope| {
                for (target_node, target_tasks) in target_group {
                    let target_addr = addrs.get(target_node).copied();
                    let keys_by_shard = &keys_by_shard;
                    let engine = engine.clone();
                    let migration = migration.clone();
                    let shard_table = shard_table.clone();
                    let redo_log = redo_log.clone();
                    let fenced_bm = fenced_bm.clone();
                    let migrating_bm = migrating_bm.clone();
                    let inbound_bm = inbound_bm.clone();

                    // Collect all keys for shards going to this target.
                    let mut target_keys: Vec<TxKey> = Vec::new();
                    for task in target_tasks {
                        if let Some(shard_keys) = keys_by_shard.get(&task.shard) {
                            target_keys.extend_from_slice(shard_keys);
                        }
                    }

                    scope.spawn(move || {
                        run_migration_batch(
                            target_tasks.clone(),
                            target_addr,
                            &target_keys,
                            engine,
                            &migration,
                            &shard_table,
                            &redo_log,
                            topology_epoch,
                            migration_pool_size,
                            batch_size,
                            fenced_bm,
                            migrating_bm,
                            inbound_bm,
                            self_id,
                        );
                    });
                }
            });
        }
    }
}

fn should_begin_handoff_for_shard(
    shard: u16,
    self_id: NodeId,
    old_master: NodeId,
    local_has_data: bool,
    old_master_alive: bool,
    outbound_source_shards: &std::collections::HashSet<u16>,
) -> bool {
    let is_outbound_source = outbound_source_shards.contains(&shard);
    is_outbound_source || (old_master_alive && local_has_data && old_master == self_id)
}

fn should_skip_already_serving_migration(
    _task: &MigrationTask,
    handoff: ShardHandoff,
    has_snapshot_data: bool,
) -> bool {
    handoff == ShardHandoff::ServingNew && !has_snapshot_data
}

fn split_already_serving_migration_tasks(
    tasks: Vec<MigrationTask>,
    table: &ShardTable,
    data_shards: &std::collections::HashSet<u16>,
) -> (Vec<MigrationTask>, Vec<MigrationTask>) {
    let mut active = Vec::with_capacity(tasks.len());
    let mut skipped = Vec::new();
    for task in tasks {
        let has_snapshot_data = data_shards.contains(&task.shard);
        if should_skip_already_serving_migration(
            &task,
            table.shard_handoff_state(task.shard),
            has_snapshot_data,
        ) {
            skipped.push(task);
        } else {
            active.push(task);
        }
    }
    (active, skipped)
}

fn should_trigger_topology_reactivation(
    startup_reactivation_due: bool,
    normal_reactivation_due: bool,
    mismatched_shards: u32,
    pending_handoffs: usize,
) -> bool {
    startup_reactivation_due
        || (normal_reactivation_due && (mismatched_shards > 0 || pending_handoffs > 0))
}

fn migration_workers_can_be_preserved(current_table_epoch: u64, activation_epoch: u64) -> bool {
    current_table_epoch == activation_epoch
}

fn build_topology_activation_tasks(
    new_plan: &[MigrationTask],
    new_replica_plan: &[MigrationTask],
    populated_shards: &std::collections::HashSet<u16>,
    new_table: &ShardTable,
    self_id: NodeId,
) -> Vec<MigrationTask> {
    let mut tasks = new_plan.to_vec();
    tasks.extend(new_replica_plan.iter().cloned());
    add_local_holder_backfill_tasks(&mut tasks, populated_shards, new_table, self_id);
    tasks
}

fn add_local_holder_backfill_tasks(
    tasks: &mut Vec<MigrationTask>,
    populated_shards: &std::collections::HashSet<u16>,
    new_table: &ShardTable,
    self_id: NodeId,
) {
    let mut existing: std::collections::HashSet<(u16, NodeId, NodeId)> = tasks
        .iter()
        .map(|t| (t.shard, t.from_node, t.to_node))
        .collect();

    for &shard in populated_shards {
        let target = new_table.target_assignment(shard);

        let mut holders = Vec::with_capacity(1 + target.replicas.len());
        holders.push(target.master);
        holders.extend(target.replicas.iter().copied());
        holders.sort_by_key(|node| node.0);
        holders.dedup();

        for holder in holders {
            if holder == self_id {
                continue;
            }
            if existing.insert((shard, self_id, holder)) {
                tasks.push(MigrationTask {
                    shard,
                    from_node: self_id,
                    to_node: holder,
                    is_master: holder == target.master,
                });
            }
        }
    }
}

fn topology_commit_already_activated(
    term: u64,
    last_activated_term: u64,
    active_members: &[NodeId],
    commit_members: &[NodeId],
) -> bool {
    term < last_activated_term || (term == last_activated_term && active_members == commit_members)
}

/// Topology proposer thread: broadcasts a proposal to all peers, collects
/// votes, and on quorum broadcasts the commit and signals the event loop.
///
/// Runs in a dedicated thread so TCP round-trips don't block SWIM event
/// processing. Uses the standard TeraSlab framed TCP protocol (same
/// `RequestFrame`/`ResponseFrame` as migration and replication).
#[allow(clippy::too_many_arguments)]
fn run_topology_proposer(
    proposal: crate::cluster::topology::TopologyTerm,
    topology_authority: Arc<crate::cluster::topology::TopologyAuthority>,
    node_addrs: Arc<RwLock<std::collections::HashMap<NodeId, SocketAddr>>>,
    self_id: NodeId,
    topology_commit_tx: std::sync::mpsc::Sender<(Vec<NodeId>, u64)>,
    topology_state_path: Option<std::path::PathBuf>,
    peak_size: Arc<std::sync::atomic::AtomicUsize>,
    swim_incarnation: Arc<std::sync::atomic::AtomicU64>,
) {
    // Retry up to 5 times on quorum failure — peers may be momentarily
    // behind (e.g. catching up a lower term) when the first proposal lands,
    // but will accept the retry once they finish their own state updates.
    // Each retry generates a fresh term so peers whose voted_term advanced
    // during the previous attempt can still accept.
    let mut proposal = proposal;
    for attempt in 0..5u32 {
        if attempt > 0 {
            // Exponential-ish backoff: 200ms, 500ms, 1s, 2s.
            let delay_ms = 200u64 * (1u64 << (attempt - 1).min(3));
            std::thread::sleep(std::time::Duration::from_millis(delay_ms));
            match topology_authority.retry_proposal() {
                Some(fresh) => {
                    tracing::info!(
                        term = fresh.term,
                        attempt,
                        "cluster: retry topology proposal",
                    );
                    proposal = fresh;
                }
                None => {
                    // Either no longer the proposer, cluster already committed,
                    // or observed membership empty — nothing to retry.
                    return;
                }
            }
        }
        if try_run_topology_proposal(
            &proposal,
            &topology_authority,
            &node_addrs,
            self_id,
            &topology_commit_tx,
            &topology_state_path,
            &peak_size,
            &swim_incarnation,
        ) {
            return;
        }
    }
    tracing::warn!(
        term = proposal.term,
        "cluster: topology proposer exhausted retries",
    );
}

#[allow(clippy::too_many_arguments)]
fn try_run_topology_proposal(
    proposal: &crate::cluster::topology::TopologyTerm,
    topology_authority: &Arc<crate::cluster::topology::TopologyAuthority>,
    node_addrs: &Arc<RwLock<std::collections::HashMap<NodeId, SocketAddr>>>,
    self_id: NodeId,
    topology_commit_tx: &std::sync::mpsc::Sender<(Vec<NodeId>, u64)>,
    topology_state_path: &Option<std::path::PathBuf>,
    peak_size: &Arc<std::sync::atomic::AtomicUsize>,
    swim_incarnation: &Arc<std::sync::atomic::AtomicU64>,
) -> bool {
    let peers: Vec<(NodeId, SocketAddr)> = {
        let addrs = node_addrs.read().unwrap();
        addrs
            .iter()
            .filter(|(id, _)| **id != self_id)
            .map(|(&id, &addr)| (id, addr))
            .collect()
    };

    if peers.is_empty() {
        // No peers — single-node case should have been handled before spawning.
        return true;
    }

    let propose_payload = proposal.serialize();

    // Send proposals to ALL peers in parallel. Each thread handles one
    // peer's TCP round-trip independently. This reduces topology change
    // latency from O(peers × timeout) to O(timeout).
    let votes: Vec<Option<crate::cluster::topology::TopologyVote>> = std::thread::scope(|scope| {
        let handles: Vec<_> = peers.iter().map(|(peer_id, peer_addr)| {
            let payload = &propose_payload;
            let pid = *peer_id;
            let paddr = *peer_addr;
            scope.spawn(move || -> Option<crate::cluster::topology::TopologyVote> {
                match send_topology_frame(paddr, OP_TOPOLOGY_PROPOSE, payload) {
                    Ok(response_payload) => {
                        match crate::cluster::topology::TopologyVote::deserialize(&response_payload) {
                            Some(v) => Some(v),
                            None => {
                                tracing::warn!(?pid, %paddr, "cluster: topology propose — malformed vote");
                                None
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(?pid, %paddr, err = %e, "cluster: topology propose failed");
                        None
                    }
                }
            })
        }).collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap_or(None))
            .collect()
    });

    // Feed all collected votes to the topology authority.
    let mut commit_result = None;
    for vote in votes.into_iter().flatten() {
        if let Some(commit) = topology_authority.handle_vote(&vote) {
            commit_result = Some(commit);
            break; // Quorum reached
        }
    }

    let commit = match commit_result {
        Some(c) => c,
        None => {
            tracing::warn!(
                term = proposal.term,
                peers = peers.len(),
                "cluster: topology quorum not reached",
            );
            return false;
        }
    };

    tracing::info!(
        term = commit.term,
        "cluster: quorum reached — broadcasting commit"
    );

    // Broadcast OP_TOPOLOGY_COMMIT to all peers in parallel with retry.
    let commit_payload = commit.serialize();
    let failed_addrs: Vec<SocketAddr> = std::thread::scope(|scope| {
        let handles: Vec<_> = peers.iter().map(|(_, addr)| {
            let payload = &commit_payload;
            let a = *addr;
            scope.spawn(move || -> Option<SocketAddr> {
                if let Err(e) = send_topology_frame(a, OP_TOPOLOGY_COMMIT, payload) {
                    tracing::warn!(addr = %a, err = %e, "cluster: topology commit broadcast failed");
                    Some(a)
                } else {
                    None
                }
            })
        }).collect();
        handles
            .into_iter()
            .filter_map(|h| h.join().unwrap_or(None))
            .collect()
    });

    // Retry failed broadcasts sequentially (transient failures).
    let mut still_failed = failed_addrs;
    for (retry, delay_ms) in [(1u32, 50u64), (2, 200)] {
        if still_failed.is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(delay_ms));
        still_failed.retain(|addr| {
            if let Err(e) = send_topology_frame(*addr, OP_TOPOLOGY_COMMIT, &commit_payload) {
                tracing::warn!(retry, %addr, err = %e, "cluster: topology commit retry failed");
                true
            } else {
                false
            }
        });
    }
    if !still_failed.is_empty() {
        tracing::warn!(
            unreachable = still_failed.len(),
            "cluster: topology commit: nodes unreachable after retries",
        );
    }

    // Apply commit locally.
    topology_authority.handle_commit(&commit);
    if let Some(path) = topology_state_path {
        let peak = peak_size.load(Ordering::Relaxed) as u64;
        let inc = swim_incarnation.load(Ordering::Relaxed);
        let _ = persist_topology_state(path, &topology_authority.persisted_state(peak, inc));
    }

    // Signal the event loop to activate the shard table.
    let _ = topology_commit_tx.send((commit.members.clone(), commit.term));
    true
}

/// Send a request frame on an existing TCP stream and read the response.
///
/// Validates the response length against `MAX_FRAME_SIZE` before allocating
/// the receive buffer, preventing OOM from malicious or buggy peers.
fn exchange_frame(stream: &mut TcpStream, request: &RequestFrame) -> Result<ResponseFrame, String> {
    stream
        .write_all(&request.encode())
        .map_err(|e| format!("write: {e}"))?;

    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .map_err(|e| format!("read length: {e}"))?;
    let total_length = u32::from_le_bytes(len_buf) as usize;
    if total_length > crate::protocol::opcodes::MAX_FRAME_SIZE as usize {
        return Err(format!(
            "response too large: {total_length} bytes (max {})",
            crate::protocol::opcodes::MAX_FRAME_SIZE,
        ));
    }
    let mut body = vec![0u8; total_length];
    stream
        .read_exact(&mut body)
        .map_err(|e| format!("read body: {e}"))?;

    let mut full = Vec::with_capacity(4 + total_length);
    full.extend_from_slice(&len_buf);
    full.extend_from_slice(&body);
    let (response, _) = ResponseFrame::decode(&full).map_err(|e| format!("decode: {e}"))?;

    Ok(response)
}

/// Send a topology-protocol frame to a peer and return the response payload.
///
/// Uses the standard TeraSlab framed TCP protocol with a 3-second connect
/// timeout and 5-second read timeout.
fn send_topology_frame(addr: SocketAddr, op_code: u16, payload: &[u8]) -> Result<Vec<u8>, String> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(500))
        .map_err(|e| format!("connect: {e}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|e| format!("set timeout: {e}"))?;
    crate::replication::tcp_transport::configure_tcp_keepalive(&stream);

    let request = RequestFrame {
        request_id: 0,
        op_code,
        flags: 0,
        payload: payload.to_vec(),
    };
    let response = exchange_frame(&mut stream, &request)?;
    Ok(response.payload)
}

/// Phase D: build this node's `PartitionVersionEntry` list by reading the
/// local shard table and engine state.
///
/// Mirrors the logic in the `OP_PARTITION_VERSION_REPORT` dispatch handler so
/// the in-process self-report is byte-equivalent to what a peer would receive
/// over the wire. Empty shards on which this node has no role are excluded
/// to keep the view compact.
fn build_self_partition_version_entries(
    self_id: NodeId,
    engine: &Engine,
    shard_table: &Arc<ShardTableLock<ShardTable>>,
    inbound_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
) -> Vec<PartitionVersionEntry> {
    let table = shard_table.read();
    let mut out = Vec::with_capacity(NUM_SHARDS);
    for shard in 0..NUM_SHARDS as u16 {
        let count = engine.shard_record_count(shard);
        let assignment = table.target_assignment(shard);
        let is_master = assignment.master == self_id;
        let is_subset = inbound_bm.test(shard);
        let is_replica = assignment.replicas.contains(&self_id);
        if !is_master && !is_replica && !is_subset && count == 0 {
            continue;
        }
        let mut flags = 0u8;
        if is_master {
            flags |= 0b01;
        }
        if is_subset {
            flags |= 0b10;
        }
        let replica_count = u8::try_from(assignment.replicas.len().min(255)).unwrap_or(255);
        out.push(PartitionVersionEntry {
            shard,
            flags,
            replica_count,
            last_applied_seq: count,
        });
    }
    out
}

/// Phase D: parse an `OP_PARTITION_VERSION_REPORT` response payload into a
/// list of [`PartitionVersionEntry`].
///
/// Returns `None` if the payload is truncated or `entry_count * 12` does not
/// match the trailing bytes — callers treat this as "no data" so a malformed
/// peer does not corrupt the partition view.
fn parse_partition_version_response(payload: &[u8]) -> Option<Vec<PartitionVersionEntry>> {
    if payload.len() < 20 {
        return None;
    }
    let entry_count = u32::from_le_bytes(payload[16..20].try_into().ok()?) as usize;
    let expected = 20 + entry_count * PARTITION_VERSION_ENTRY_SIZE;
    if payload.len() != expected {
        return None;
    }
    let mut entries = Vec::with_capacity(entry_count);
    for i in 0..entry_count {
        let off = 20 + i * PARTITION_VERSION_ENTRY_SIZE;
        let shard = u16::from_le_bytes(payload[off..off + 2].try_into().ok()?);
        let flags = payload[off + 2];
        let replica_count = payload[off + 3];
        let last_applied_seq = u64::from_le_bytes(payload[off + 4..off + 12].try_into().ok()?);
        entries.push(PartitionVersionEntry {
            shard,
            flags,
            replica_count,
            last_applied_seq,
        });
    }
    Some(entries)
}

// ---------------------------------------------------------------------------
// Shard manifest hash — order-independent content fingerprint
// ---------------------------------------------------------------------------

/// SHA-256-based shard manifest hash for migration verification.
///
/// Accumulates `(txid, generation)` pairs, sorts them by txid for
/// deterministic ordering, and produces a SHA-256 digest. This is
/// strictly stronger than XOR-based hashing which can suffer from
/// accidental collisions (e.g., two records swapping generation values).
///
/// The sort is O(n log n) but runs only once per shard migration —
/// negligible cost compared to the I/O of streaming records.
///
/// Used by both the migration source (coordinator) and the migration target
/// (dispatch OP_MIGRATION_COMPLETE handler) to verify shard content equality.
#[derive(Clone)]
pub struct ManifestHasher {
    entries: Vec<([u8; 32], u32)>,
}

impl ManifestHasher {
    /// Create an empty manifest.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Fold a `(txid, generation)` pair into the manifest.
    pub fn fold(&mut self, txid: &[u8; 32], generation: u32) {
        self.entries.push((*txid, generation));
    }

    /// Finalize: sort entries by txid for deterministic ordering,
    /// then SHA-256 hash the concatenated `(txid ++ generation_le)` pairs.
    pub fn finalize(&self) -> [u8; 32] {
        let mut sorted = self.entries.clone();
        sorted.sort_by(|a, b| a.0.cmp(&b.0));

        let mut buf = Vec::with_capacity(sorted.len() * 36);
        for (txid, generation) in &sorted {
            buf.extend_from_slice(txid);
            buf.extend_from_slice(&generation.to_le_bytes());
        }
        crate::cluster::auth::sha256(&buf)
    }
}

impl Default for ManifestHasher {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute the manifest hash for a shard by reading all records from the engine.
///
fn collect_manifest_entries(
    engine: &Engine,
    shard: u16,
    keys: &[TxKey],
) -> std::result::Result<Vec<(TxKey, u32)>, String> {
    let mut entries = Vec::with_capacity(keys.len());
    for key in keys {
        let meta = match engine.read_metadata(key) {
            Ok(meta) => meta,
            Err(crate::ops::error::SpendError::TxNotFound) => {
                tracing::debug!(
                    shard,
                    key = ?key,
                    "cluster: skipping stale key during manifest collection",
                );
                continue;
            }
            Err(e) => {
                return Err(format!(
                    "manifest read_metadata shard {shard} key {:?}: {e:?}",
                    key
                ));
            }
        };
        entries.push((*key, meta.generation));
    }
    Ok(entries)
}

fn compute_manifest_for_entries(entries: &[(TxKey, u32)]) -> [u8; 32] {
    let mut manifest = ManifestHasher::new();
    for (key, generation) in entries {
        manifest.fold(&key.txid, *generation);
    }
    manifest.finalize()
}

// ---------------------------------------------------------------------------
// Batched migration
// ---------------------------------------------------------------------------

/// 1. Streams baseline records
/// 2. Fences source writes
/// 3. Streams redo deltas
/// 4. Sends OP_MIGRATION_COMPLETE
///
/// This is orders of magnitude faster than per-shard connections:
/// a 3-node cluster migrating 1300 shards uses 1 connection instead of 1300.
#[allow(clippy::too_many_arguments)]
fn run_migration_batch(
    tasks: Vec<MigrationTask>,
    target_addr: Option<SocketAddr>,
    all_keys: &[TxKey],
    engine: Arc<Engine>,
    migration: &Arc<Mutex<MigrationManager>>,
    shard_table: &Arc<ShardTableLock<ShardTable>>,
    redo_log: &Option<Arc<ParkingMutex<RedoLog>>>,
    topology_epoch: u64,
    pool_size: usize,
    batch_size: usize,
    fenced_bm: Arc<crate::cluster::migration::AtomicShardBitmap>,
    migrating_bm: Arc<crate::cluster::migration::AtomicShardBitmap>,
    inbound_bm: Arc<crate::cluster::migration::AtomicShardBitmap>,
    self_id: NodeId,
) {
    let addr = match target_addr {
        Some(a) => a,
        None => {
            tracing::warn!(
                tasks = tasks.len(),
                "cluster: no address for target, cannot migrate shards"
            );
            for task in &tasks {
                fail_migration_task_current_epoch(
                    migration,
                    shard_table,
                    &fenced_bm,
                    &migrating_bm,
                    task,
                    topology_epoch,
                    true,
                );
            }
            if migration_epoch_current(shard_table, topology_epoch) {
                migration.lock().unwrap().cleanup_completed();
            }
            return;
        }
    };

    let completed = std::sync::atomic::AtomicU32::new(0);
    let failed = std::sync::atomic::AtomicU32::new(0);
    if !migration_epoch_current(shard_table, topology_epoch) {
        tracing::info!(
            task_epoch = topology_epoch,
            current_epoch = shard_table.read().version,
            "cluster: skipping stale migration batch",
        );
        return;
    }

    // Pre-group keys by shard ONCE. Without this, each shard does an
    // O(N) scan of all keys, making total cost O(shards × keys).
    // With pre-grouping, total cost is O(keys) for the grouping +
    // O(shard_keys) per shard for the actual migration.
    let mut keys_by_shard: std::collections::HashMap<u16, Vec<&TxKey>> =
        std::collections::HashMap::new();
    for key in all_keys {
        let shard = ShardTable::shard_for_key(key);
        keys_by_shard.entry(shard).or_default().push(key);
    }

    // Separate empty shards from shards with data using the pre-grouped
    // map (O(1) per shard instead of O(all_keys) per shard).
    // Pre-filter: skip master shards already in ServingNew (already committed
    // by a previous topology cycle or by the begin_handoff_with callback).
    // Replica-only migrations intentionally enter ServingNew immediately
    // because they do not block the master route, but they still must stream
    // data to maintain RF when the source snapshot contains records.
    // Send OP_MIGRATION_COMPLETE to the target so it clears its inbound
    // state and unblocks writes for these shards.
    let data_shards: std::collections::HashSet<u16> = keys_by_shard.keys().copied().collect();
    let (tasks, skipped_tasks) = {
        let table = shard_table.read();
        split_already_serving_migration_tasks(tasks, &table, &data_shards)
    };
    if !skipped_tasks.is_empty() {
        tracing::info!(
            shards = skipped_tasks.len(),
            %addr,
            "cluster: shards already serving — sending completion handshakes",
        );
        let delivered = send_completion_only_handshakes(addr, &skipped_tasks, self_id);
        for (task, delivered) in skipped_tasks.iter().zip(delivered) {
            if delivered {
                if complete_migration_task_current_epoch(
                    migration,
                    shard_table,
                    &fenced_bm,
                    &migrating_bm,
                    task,
                    topology_epoch,
                    false,
                ) {
                    completed.fetch_add(1, Ordering::Relaxed);
                }
            } else if fail_migration_task_current_epoch(
                migration,
                shard_table,
                &fenced_bm,
                &migrating_bm,
                task,
                topology_epoch,
                false,
            ) {
                failed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    let mut empty_tasks: Vec<MigrationTask> = Vec::new();
    let mut data_tasks: Vec<MigrationTask> = Vec::new();
    for task in &tasks {
        if keys_by_shard.contains_key(&task.shard) {
            data_tasks.push((*task).clone());
        } else {
            empty_tasks.push((*task).clone());
        }
    }

    // Empty shards: fence writes, then re-verify emptiness before
    // committing. A record could have been created between the
    // pre_swap_keys snapshot and now, so we must block writes and
    // re-check. If the shard is no longer empty, promote it to the
    // data path for full migration.
    if !empty_tasks.is_empty() {
        let mut promoted = Vec::new();
        let mut ready_empty_tasks: Vec<MigrationTask> = Vec::new();
        let empty_shards: std::collections::HashSet<u16> =
            empty_tasks.iter().map(|t| t.shard).collect();
        {
            let mut mgr = migration.lock().unwrap();
            for task in &empty_tasks {
                mgr.fence_shard(task.shard);
                fenced_bm.set(task.shard);
            }

            let fenced_keys_by_shard = engine.keys_by_shard_filtered(&empty_shards);

            for task in &empty_tasks {
                if !fenced_keys_by_shard.contains_key(&task.shard) {
                    ready_empty_tasks.push(task.clone());
                } else {
                    if engine.shard_record_count(task.shard) == 0 {
                        let key_count = fenced_keys_by_shard
                            .get(&task.shard)
                            .map(|v| v.len())
                            .unwrap_or(0);
                        tracing::warn!(
                            shard = task.shard,
                            keys = key_count,
                            "cluster: shard empty recheck found keys despite zero shard count",
                        );
                    }
                    // Records appeared between snapshot and fence.
                    // Must go through full migration path.
                    mgr.unfence_shard(task.shard);
                    fenced_bm.clear(task.shard);
                    promoted.push(task.clone());
                }
            }
        }
        let instant_count = ready_empty_tasks.len();
        if instant_count > 0 {
            tracing::info!(shards = instant_count, %addr, "cluster: empty shards committed instantly");
            let delivered = send_completion_only_handshakes(addr, &ready_empty_tasks, self_id);
            for (task, delivered) in ready_empty_tasks.iter().zip(delivered) {
                if delivered {
                    let should_commit_local_handoff = {
                        let table = shard_table.read();
                        let target_assignment = table.target_assignment(task.shard);
                        task.is_master
                            || target_assignment.master == task.from_node
                            || target_assignment.replicas.contains(&task.from_node)
                    };
                    if complete_migration_task_current_epoch(
                        migration,
                        shard_table,
                        &fenced_bm,
                        &migrating_bm,
                        task,
                        topology_epoch,
                        should_commit_local_handoff,
                    ) {
                        completed.fetch_add(1, Ordering::Relaxed);
                        cleanup_orphaned_shard_if_settled(
                            self_id,
                            &engine,
                            shard_table,
                            migration,
                            task.shard,
                            topology_epoch,
                        );
                    }
                } else if fail_migration_task_current_epoch(
                    migration,
                    shard_table,
                    &fenced_bm,
                    &migrating_bm,
                    task,
                    topology_epoch,
                    true,
                ) {
                    failed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        // Promoted shards are not in keys_by_shard since they were empty
        // at snapshot time. The baseline will see zero keys for them, but
        // the Create redo entries (now included in delta streaming per 1A)
        // will transmit the newly created records during the delta phase.
        // This is correct: the records were created after the snapshot, so
        // they belong in the delta, not the baseline.
        data_tasks.extend(promoted);
    }

    let total = data_tasks.len();
    if total == 0 {
        if !migration_epoch_current(shard_table, topology_epoch) {
            tracing::info!(
                task_epoch = topology_epoch,
                current_epoch = shard_table.read().version,
                "cluster: skipping stale empty migration completion",
            );
            return;
        }
        migration.lock().unwrap().cleanup_completed();
        let c = completed.load(Ordering::Relaxed);
        let f = failed.load(Ordering::Relaxed);
        tracing::info!(%addr, completed = c, failed = f, "cluster: batch migration finished");
        if f == 0 {
            let ce = engine.clone();
            let cs = shard_table.clone();
            let cm = migration.clone();
            std::thread::spawn(move || {
                run_orphan_cleanup(self_id, &ce, &cs, &cm, topology_epoch);
            });
        }
        return;
    }

    // Split data tasks across a pool of parallel connections.
    // More connections = more throughput for large migrations.
    let pool_size = pool_size.max(1);
    let chunk_size = total.div_ceil(pool_size);

    let total_keys: usize = data_tasks
        .iter()
        .map(|t| keys_by_shard.get(&t.shard).map(|v| v.len()).unwrap_or(0))
        .sum();
    tracing::info!(
        shards = total,
        records = total_keys,
        %addr,
        connections = pool_size.min(total),
        batch_size,
        "cluster: migrating data shards",
    );

    let completed = Arc::new(std::sync::atomic::AtomicU32::new(
        completed.load(Ordering::Relaxed),
    ));
    let failed = Arc::new(std::sync::atomic::AtomicU32::new(
        failed.load(Ordering::Relaxed),
    ));

    let migration_start = std::time::Instant::now();
    let keys_ref = &keys_by_shard;
    // Scale TCP timeouts based on batch size and keep a high floor for live
    // migrations. Under high parallelism the target can take tens of seconds
    // to service a queued migration frame; timing out earlier causes retry
    // churn that is slower than waiting for the in-flight response.
    let tcp_timeout = migration_stream_timeout(batch_size);

    std::thread::scope(|scope| {
        for chunk in data_tasks.chunks(chunk_size) {
            let completed = completed.clone();
            let failed = failed.clone();
            let migration = migration.clone();
            let fenced_bm = fenced_bm.clone();
            let migrating_bm = migrating_bm.clone();
            let engine = engine.clone();

            scope.spawn(move || {
                // Exponential backoff delays for connection retries.
                const CONNECT_RETRY_DELAYS_MS: [u64; 3] = [10, 50, 200];

                let mut stream = None;
                for (attempt, &delay_ms) in CONNECT_RETRY_DELAYS_MS.iter().enumerate() {
                    match TcpStream::connect_timeout(&addr, Duration::from_secs(3)) {
                        Ok(s) => {
                            let _ = s.set_read_timeout(Some(tcp_timeout));
                            let _ = s.set_write_timeout(Some(tcp_timeout));
                            crate::replication::tcp_transport::configure_tcp_keepalive(&s);
                            stream = Some(s);
                            break;
                        }
                        Err(e) => {
                            tracing::warn!(
                                %addr,
                                attempt = attempt + 1,
                                err = %e,
                                "cluster: connect failed",
                            );
                            if attempt < 2 {
                                std::thread::sleep(Duration::from_millis(delay_ms));
                            }
                        }
                    }
                }

                // Helper to establish a fresh connection.
                let new_conn = || -> Option<TcpStream> {
                    for (attempt, &delay_ms) in [10u64, 50, 200].iter().enumerate() {
                        match TcpStream::connect_timeout(&addr, Duration::from_secs(3)) {
                            Ok(s) => {
                                let _ = s.set_read_timeout(Some(tcp_timeout));
                                let _ = s.set_write_timeout(Some(tcp_timeout));
                                crate::replication::tcp_transport::configure_tcp_keepalive(&s);
                                return Some(s);
                            }
                            Err(_) => {
                                if attempt < 2 {
                                    std::thread::sleep(Duration::from_millis(delay_ms));
                                }
                            }
                        }
                    }
                    None
                };

                let mut stream = match stream {
                    Some(s) => s,
                    None => {
                        tracing::warn!(%addr, "cluster: connect failed after retries");
                        for task in chunk {
                            if fail_migration_task_current_epoch(
                                &migration,
                                shard_table,
                                &fenced_bm,
                                &migrating_bm,
                                task,
                                topology_epoch,
                                true,
                            ) {
                                failed.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        return;
                    }
                };

                // Pipelined migration: process shards in sub-batches of
                // up to 32 shards. For each sub-batch:
                // 1. Stream ALL baselines (one TCP round-trip per shard)
                // 2. Fence ALL shards at once (one lock acquisition)
                // 3. Send batched completion (one TCP round-trip total)
                // This reduces per-shard lock overhead from ~700ms to ~10ms.
                // Process ALL shards on this connection in one pass to avoid
                // redundant fence/unfence cycles across sub-batches.
                let pipeline_batch: usize = chunk.len();
                let mut task_idx = 0;
                while task_idx < chunk.len() {
                    if !migration_epoch_current(shard_table, topology_epoch) {
                        tracing::info!(
                            task_epoch = topology_epoch,
                            current_epoch = shard_table.read().version,
                            "cluster: aborting stale migration worker",
                        );
                        return;
                    }
                    let sub_end = (task_idx + pipeline_batch).min(chunk.len());
                    let sub_batch = &chunk[task_idx..sub_end];
                    let sub_count = sub_batch.len();

                    // Phase 1: Stream baselines for all shards in sub-batch.
                    let empty_keys: Vec<&TxKey> = Vec::new();
                    let mut streamed: Vec<bool> = Vec::with_capacity(sub_count);
                    let mut snapshot_seqs: Vec<u64> = Vec::with_capacity(sub_count);
                    for task in sub_batch {
                        if !migration_epoch_current(shard_table, topology_epoch) {
                            tracing::info!(
                                task_epoch = topology_epoch,
                                current_epoch = shard_table.read().version,
                                "cluster: aborting stale migration snapshot",
                            );
                            return;
                        }
                        let snapshot_seq = redo_log.as_ref()
                            .map(|rl| rl.lock().current_sequence())
                            .unwrap_or(0);
                        {
                            let mut mgr = migration.lock().unwrap();
                            mgr.set_snapshot_sequence(task, snapshot_seq);
                        }
                        snapshot_seqs.push(snapshot_seq);
                        let shard_keys = keys_ref.get(&task.shard).unwrap_or(&empty_keys);
                        let ok = match stream_shard_baseline(task, shard_keys, &engine, &mut stream, batch_size, topology_epoch) {
                            Ok(_) => true,
                            Err(e) => {
                                tracing::warn!(shard = task.shard, err = %e, "cluster: shard baseline failed");
                                false
                            }
                        };
                        streamed.push(ok);
                    }

                    // Phase 2: Fence all shards in sub-batch (one lock).
                    if !migration_epoch_current(shard_table, topology_epoch) {
                        tracing::info!(
                            task_epoch = topology_epoch,
                            current_epoch = shard_table.read().version,
                            "cluster: aborting stale migration before fence",
                        );
                        return;
                    }
                    {
                        let mut mgr = migration.lock().unwrap();
                        for (i, task) in sub_batch.iter().enumerate() {
                            if streamed[i] {
                                mgr.fence_shard(task.shard);
                                fenced_bm.set(task.shard);
                            }
                        }
                    }
                    let fence_seq = redo_log
                        .as_ref()
                        .map(|rl| rl.lock().current_sequence())
                        .unwrap_or(0);
                    {
                        let mut mgr = migration.lock().unwrap();
                        for (i, task) in sub_batch.iter().enumerate() {
                            if streamed[i] {
                                mgr.mark_fenced(task, fence_seq);
                            }
                        }
                    }

                    // Phase 3: Verify each shard manifest, then clear inbound
                    // migration state in one durable batch. Verification stays
                    // per shard so a single corrupt/missing shard is isolated,
                    // but the target avoids one fsync per successful shard.
                    let mut verified_tasks: Vec<MigrationTask> = Vec::new();
                    for (i, task) in sub_batch.iter().enumerate() {
                        if !streamed[i] {
                            if fail_migration_task_current_epoch(
                                &migration,
                                shard_table,
                                &fenced_bm,
                                &migrating_bm,
                                task,
                                topology_epoch,
                                true,
                            ) {
                                failed.fetch_add(1, Ordering::Relaxed);
                            }
                            continue;
                        }
                        // Get current keys for manifest (use count fast-path).
                        let shard_keys_snapshot = keys_ref.get(&task.shard).unwrap_or(&empty_keys);
                        let fenced_keys: Vec<TxKey> = {
                            let count = engine.shard_record_count(task.shard) as usize;
                            if count == shard_keys_snapshot.len() {
                                shard_keys_snapshot.iter().map(|k| **k).collect()
                            } else {
                                engine.keys_for_shard(task.shard)
                            }
                        };
                        // Close the window between Phase 1 snapshot and Phase 2
                        // fence: any key present after fence but NOT streamed
                        // in Phase 1 is a late write whose data never made it
                        // to the destination. Stream those late records' data
                        // before sending the completion handshake, otherwise
                        // the manifest would list keys the receiver has no
                        // payload for and the record would silently disappear.
                        let snapshot_set: std::collections::HashSet<TxKey> = shard_keys_snapshot
                            .iter()
                            .map(|k| **k)
                            .collect();
                        let late_keys: Vec<TxKey> = fenced_keys
                            .iter()
                            .copied()
                            .filter(|k| !snapshot_set.contains(k))
                            .collect();
                        if !late_keys.is_empty() {
                            let late_refs: Vec<&TxKey> = late_keys.iter().collect();
                            if let Err(e) = stream_shard_baseline(
                                task, &late_refs, &engine, &mut stream, batch_size, topology_epoch,
                            ) {
                                tracing::warn!(
                                    shard = task.shard,
                                    err = %e,
                                    "cluster: shard late-key streaming failed",
                                );
                                if fail_migration_task_current_epoch(
                                    &migration,
                                    shard_table,
                                    &fenced_bm,
                                    &migrating_bm,
                                    task,
                                    topology_epoch,
                                    true,
                                ) {
                                    failed.fetch_add(1, Ordering::Relaxed);
                                }
                                if let Some(s) = new_conn() { stream = s; }
                                continue;
                            }
                        }
                        let snapshot_seq = snapshot_seqs[i];
                        let mut delta_failed = false;
                        match collect_migration_delta_ops(
                            redo_log,
                            snapshot_seq,
                            fence_seq,
                            task.shard,
                            &engine,
                        ) {
                            Ok(delta_ops) => {
                                if !delta_ops.is_empty()
                                    && let Err(e) = send_delta_ops(
                                        &mut stream,
                                        task.shard,
                                        &delta_ops,
                                        topology_epoch,
                                    )
                                {
                                    tracing::warn!(
                                        shard = task.shard,
                                        err = %e,
                                        "cluster: batched migration delta streaming failed",
                                    );
                                    delta_failed = true;
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    shard = task.shard,
                                    err = %e,
                                    "cluster: batched migration delta unavailable",
                                );
                                delta_failed = true;
                            }
                        }
                        if delta_failed {
                            if fail_migration_task_current_epoch(
                                &migration,
                                shard_table,
                                &fenced_bm,
                                &migrating_bm,
                                task,
                                topology_epoch,
                                true,
                            ) {
                                failed.fetch_add(1, Ordering::Relaxed);
                            }
                            if let Some(s) = new_conn() { stream = s; }
                            continue;
                        }
                        let manifest_entries = match collect_manifest_entries(&engine, task.shard, &fenced_keys) {
                            Ok(e) => e,
                            Err(e) => {
                                tracing::warn!(shard = task.shard, err = %e, "cluster: shard manifest failed");
                                if fail_migration_task_current_epoch(
                                    &migration,
                                    shard_table,
                                    &fenced_bm,
                                    &migrating_bm,
                                    task,
                                    topology_epoch,
                                    true,
                                ) {
                                    failed.fetch_add(1, Ordering::Relaxed);
                                }
                                continue;
                            }
                        };
                        let manifest_hash = compute_manifest_for_entries(&manifest_entries);
                        if let Err(e) = send_migration_complete(addr, task.shard, task.from_node, manifest_entries.len() as u64, fence_seq, topology_epoch, Some(&mut stream), &manifest_hash, &manifest_entries, true) {
                            tracing::warn!(shard = task.shard, err = %e, "cluster: shard completion failed");
                            if fail_migration_task_current_epoch(
                                &migration,
                                shard_table,
                                &fenced_bm,
                                &migrating_bm,
                                task,
                                topology_epoch,
                                true,
                            ) {
                                failed.fetch_add(1, Ordering::Relaxed);
                            }
                            // Reconnect — stream may be broken.
                            if let Some(s) = new_conn() { stream = s; }
                            continue;
                        }
                        verified_tasks.push(task.clone());
                    }

                    if !verified_tasks.is_empty() {
                        let delivered =
                            send_completion_only_handshakes(addr, &verified_tasks, self_id);
                        for (task, delivered) in verified_tasks.iter().zip(delivered.into_iter()) {
                            if !delivered {
                                tracing::warn!(
                                    shard = task.shard,
                                    "cluster: batched verified completion failed",
                                );
                                if fail_migration_task_current_epoch(
                                    &migration,
                                    shard_table,
                                    &fenced_bm,
                                    &migrating_bm,
                                    task,
                                    topology_epoch,
                                    true,
                                ) {
                                    failed.fetch_add(1, Ordering::Relaxed);
                                }
                                continue;
                            }

                            // Success: mark complete and commit.
                        let should_commit = {
                            let table = shard_table.read();
                            task.is_master
                                || table.target_assignment(task.shard).master == task.from_node
                                || table.target_assignment(task.shard).replicas.contains(&task.from_node)
                        };
                        if complete_migration_task_current_epoch(
                            &migration,
                            shard_table,
                            &fenced_bm,
                            &migrating_bm,
                            task,
                            topology_epoch,
                            should_commit,
                        ) {
                            completed.fetch_add(1, Ordering::Relaxed);
                            cleanup_orphaned_shard_if_settled(
                                self_id,
                                &engine,
                                shard_table,
                                &migration,
                                task.shard,
                                topology_epoch,
                            );
                        }
                    }
                    }

                    // Unfence completed shards.
                    if migration_epoch_current(shard_table, topology_epoch) {
                        let mut mgr = migration.lock().unwrap();
                        for (i, task) in sub_batch.iter().enumerate() {
                            if streamed[i] {
                                mgr.unfence_shard(task.shard);
                                fenced_bm.clear(task.shard);
                            }
                        }
                    }

                    task_idx = sub_end;
                }

            });
        }
    });

    let c = completed.load(Ordering::Relaxed);
    let f = failed.load(Ordering::Relaxed);
    let elapsed = migration_start.elapsed();

    let batch_epoch_current = migration_epoch_current(shard_table, topology_epoch);
    let retry_tasks = if batch_epoch_current {
        let mut mgr = migration.lock().unwrap();
        let tasks = mgr.take_failed_tasks();
        mgr.cleanup_completed();
        tasks
    } else {
        tracing::info!(
            task_epoch = topology_epoch,
            current_epoch = shard_table.read().version,
            "cluster: skipping stale migration retry bookkeeping",
        );
        Vec::new()
    };
    let has_retry_tasks = !retry_tasks.is_empty();
    let rate = if elapsed.as_secs_f64() > 0.0 {
        total_keys as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };
    tracing::info!(
        %addr,
        completed = c,
        failed = f,
        elapsed_secs = elapsed.as_secs_f64(),
        records_per_sec = rate,
        "cluster: batch migration finished",
    );

    if has_retry_tasks {
        let retry_shards: std::collections::HashSet<u16> =
            retry_tasks.iter().map(|t| t.shard).collect();
        let retry_keys = engine
            .keys_by_shard_filtered(&retry_shards)
            .values()
            .flat_map(|v| v.iter().copied())
            .collect::<Vec<TxKey>>();
        let retry_engine = engine.clone();
        let retry_migration = migration.clone();
        let retry_shard_table = shard_table.clone();
        let retry_redo = redo_log.clone();
        let retry_fenced_bm = fenced_bm.clone();
        let retry_migrating_bm = migrating_bm.clone();
        let retry_inbound_bm = inbound_bm.clone();
        let retry_epoch = shard_table.read().version;
        tracing::info!(
            count = retry_tasks.len(),
            "cluster: requeueing failed migrations for immediate retry",
        );
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            run_migration_batch(
                retry_tasks,
                Some(addr),
                &retry_keys,
                retry_engine,
                &retry_migration,
                &retry_shard_table,
                &retry_redo,
                retry_epoch,
                pool_size,
                batch_size,
                retry_fenced_bm,
                retry_migrating_bm,
                retry_inbound_bm,
                self_id,
            );
        });
    } else if f == 0 && batch_epoch_current {
        let cleanup_engine = engine.clone();
        let cleanup_st = shard_table.clone();
        let cleanup_mig = migration.clone();
        std::thread::spawn(move || {
            run_orphan_cleanup(
                self_id,
                &cleanup_engine,
                &cleanup_st,
                &cleanup_mig,
                topology_epoch,
            );
        });
    }

    // Clear stale inbound migrations. Use staleness-based eviction
    // (30s) rather than blanket clear to avoid removing entries for
    // shards that are legitimately receiving data from other nodes.
    {
        let mut mgr = migration.lock().unwrap();
        if batch_epoch_current
            && f == 0
            && !has_retry_tasks
            && mgr.active_count() == 0
            && mgr.inbound_count() > 0
        {
            let removed = mgr.clear_stale_inbound(Duration::from_secs(30));
            if removed > 0 {
                inbound_bm.load_from(mgr.inbound_bitmap());
                tracing::info!(
                    removed,
                    "cluster: cleared stale inbound migrations — no active outbound migrations remain"
                );
            }
        }
        drop(mgr);
    }

    if f > 0 && batch_epoch_current {
        tracing::warn!(
            failed = f,
            "cluster: migrations failed — will re-attempt on next topology cycle"
        );
    }
}

/// Delete records for shards this node no longer owns after migration.
///
/// After outbound migrations complete, some records remain on the source
/// node for shards that have moved away. This function identifies those
/// orphaned records and deletes them.
///
/// Safety guards:
/// - Skips if other migrations are still active (`active_count > 0`).
/// - Checks the topology epoch before each shard — aborts if it changed.
/// - `TxNotFound` during delete is non-fatal (concurrent ops may delete first).
/// - Idempotent: running twice is safe.
fn run_orphan_cleanup(
    self_id: NodeId,
    engine: &Arc<Engine>,
    shard_table: &Arc<ShardTableLock<ShardTable>>,
    migration: &Arc<Mutex<MigrationManager>>,
    topology_epoch: u64,
) {
    use crate::cluster::shards::NUM_SHARDS;
    use crate::ops::remaining::DeleteRequest;

    // Guard: skip if migrations are still active or failed work remains.
    {
        let mgr = migration.lock().unwrap();
        if mgr.active_count() > 0 || mgr.failed_count() > 0 {
            return;
        }
    }

    // Guard: topology must not have changed since the migration started.
    let current_epoch = shard_table.read().version;
    if current_epoch != topology_epoch {
        return;
    }

    let mut orphaned_shards: Vec<u16> = Vec::new();
    {
        let table = shard_table.read();
        for shard in 0..NUM_SHARDS as u16 {
            let assignment = table.effective_assignment(shard);
            let owned = assignment.master == self_id || assignment.replicas.contains(&self_id);
            if !owned && engine.shard_record_count(shard) > 0 {
                debug_shard_log(
                    shard,
                    format!(
                        "orphan_cleanup candidate self={} master={} replicas={:?} records={}",
                        self_id.0,
                        assignment.master.0,
                        assignment.replicas.iter().map(|n| n.0).collect::<Vec<_>>(),
                        engine.shard_record_count(shard),
                    ),
                );
                orphaned_shards.push(shard);
            }
        }
    }

    if orphaned_shards.is_empty() {
        return;
    }

    let total_orphaned: u64 = orphaned_shards
        .iter()
        .map(|&s| engine.shard_record_count(s))
        .sum();
    tracing::info!(
        shards = orphaned_shards.len(),
        records = total_orphaned,
        "cluster: orphan cleanup — deleting orphaned records",
    );

    let mut total_deleted: u64 = 0;
    for &shard in &orphaned_shards {
        // Re-check epoch before each shard.
        if shard_table.read().version != topology_epoch {
            tracing::info!("cluster: orphan cleanup aborted — topology epoch changed");
            break;
        }

        let keys = engine.keys_for_shard(shard);
        debug_shard_log(
            shard,
            format!("orphan_cleanup deleting {} key(s)", keys.len(),),
        );
        for key in &keys {
            match engine.delete(&DeleteRequest { tx_key: *key }) {
                Ok(()) => total_deleted += 1,
                Err(crate::ops::error::SpendError::TxNotFound) => {}
                Err(e) => {
                    tracing::warn!(shard, err = ?e, "cluster: orphan cleanup delete error");
                }
            }
        }
    }

    tracing::info!(
        deleted = total_deleted,
        shards = orphaned_shards.len(),
        "cluster: orphan cleanup complete",
    );
}

/// Delete records for one shard as soon as this source has finished every
/// outbound task for that shard and the current topology no longer assigns it
/// here. This keeps "migration complete" from leaving a transient third RF=2
/// holder that strict direct-local checks can observe before the broad cleanup
/// sweep runs.
fn cleanup_orphaned_shard_if_settled(
    self_id: NodeId,
    engine: &Arc<Engine>,
    shard_table: &Arc<ShardTableLock<ShardTable>>,
    migration: &Arc<Mutex<MigrationManager>>,
    shard: u16,
    topology_epoch: u64,
) {
    use crate::ops::remaining::DeleteRequest;

    if shard_table.read().version != topology_epoch {
        return;
    }

    {
        let mgr = migration.lock().unwrap();
        if mgr.active_count() > 0 || mgr.failed_count() > 0 {
            return;
        }
        let shard_still_active = mgr.active_migrations().iter().any(|p| {
            p.shard == shard
                && !p.is_complete()
                && p.state != crate::cluster::migration::MigrationState::Failed
        });
        let shard_failed = mgr.active_migrations().iter().any(|p| {
            p.shard == shard && p.state == crate::cluster::migration::MigrationState::Failed
        });
        if shard_still_active || shard_failed {
            return;
        }
    }

    let owned = {
        let table = shard_table.read();
        let assignment = table.effective_assignment(shard);
        assignment.master == self_id || assignment.replicas.contains(&self_id)
    };
    if owned || engine.shard_record_count(shard) == 0 {
        return;
    }

    let keys = engine.keys_for_shard(shard);
    let mut deleted = 0u64;
    for key in &keys {
        match engine.delete(&DeleteRequest { tx_key: *key }) {
            Ok(()) => deleted += 1,
            Err(crate::ops::error::SpendError::TxNotFound) => {}
            Err(e) => {
                tracing::warn!(shard, err = ?e, "cluster: per-shard orphan cleanup delete error");
            }
        }
    }
    if deleted > 0 {
        tracing::info!(shard, deleted, "cluster: per-shard orphan cleanup complete",);
    }
}

/// Migrate a single shard: baseline → fence → deltas → complete handshake.
///
/// Checks the shard table version before fencing and before the complete
/// handshake. If the topology has changed (epoch advanced), the migration
/// is aborted early — the new topology's coordinator will re-plan.
///
/// Returns `true` if the shard was migrated successfully, `false` if it failed.
/// On failure the TCP stream may be broken and should be reconnected by the caller.
///
/// Kept as reference for the per-shard migration flow with redo-log delta
/// replay. The production path is `run_migration_batch`, which pipelines
/// baseline + late-key streaming + manifest handshake in one TCP session
/// across many shards.
#[allow(dead_code)]
#[allow(clippy::too_many_arguments)]
fn migrate_single_shard(
    task: &MigrationTask,
    keys_by_shard: &std::collections::HashMap<u16, Vec<&TxKey>>,
    engine: &Engine,
    migration: &Arc<Mutex<MigrationManager>>,
    shard_table: &Arc<ShardTableLock<ShardTable>>,
    redo_log: &Option<Arc<ParkingMutex<RedoLog>>>,
    stream: &mut TcpStream,
    addr: SocketAddr,
    completed: &Arc<std::sync::atomic::AtomicU32>,
    failed: &Arc<std::sync::atomic::AtomicU32>,
    topology_epoch: u64,
    batch_size: usize,
    fenced_bm: &crate::cluster::migration::AtomicShardBitmap,
    migrating_bm: &crate::cluster::migration::AtomicShardBitmap,
) -> bool {
    let empty_vec = Vec::new();
    let shard_keys = keys_by_shard.get(&task.shard).unwrap_or(&empty_vec);

    // Helper: mark task failed, rollback shard table, sync bitmaps.
    let fail_shard = |migration: &Arc<Mutex<MigrationManager>>,
                      shard_table: &Arc<ShardTableLock<ShardTable>>,
                      failed: &Arc<std::sync::atomic::AtomicU32>,
                      clear_target_inbound: bool| {
        debug_shard_log(
            task.shard,
            format!(
                "fail from={} to={} is_master={}",
                task.from_node.0, task.to_node.0, task.is_master,
            ),
        );
        if clear_target_inbound {
            let _ = send_migration_complete(
                addr,
                task.shard,
                task.from_node,
                0,
                0,
                0,
                None,
                &[0u8; 32],
                &[],
                false,
            );
        }
        let mut mgr = migration.lock().unwrap();
        mgr.mark_failed(task);
        if !mgr.is_shard_fenced(task.shard) {
            fenced_bm.clear(task.shard);
        }
        migrating_bm.clear(task.shard);
        drop(mgr);
        shard_table.write().rollback_shard(task.shard);
        failed.fetch_add(1, Ordering::Relaxed);
    };

    // Retry loop: up to 3 attempts with exponential backoff for transient
    // TCP failures. Handoff state resets are non-retryable (abort immediately).
    const RETRY_DELAYS_MS: [u64; 3] = [0, 50, 200];
    let mut last_err = String::new();

    for (attempt, &delay_ms) in RETRY_DELAYS_MS.iter().enumerate() {
        if attempt == 0 {
            debug_shard_log(
                task.shard,
                format!(
                    "start from={} to={} is_master={} snapshot_keys={}",
                    task.from_node.0,
                    task.to_node.0,
                    task.is_master,
                    shard_keys.len(),
                ),
            );
        }
        if attempt > 0 {
            tracing::info!(
                shard = task.shard,
                attempt = attempt + 1,
                delay_ms,
                "cluster: shard retry attempt",
            );
            // Unfence before retry — the fence will be re-set in phase 2.
            migration.lock().unwrap().unfence_shard(task.shard);
            fenced_bm.clear(task.shard);
            std::thread::sleep(Duration::from_millis(delay_ms));
        }

        let snapshot_seq = redo_log
            .as_ref()
            .map(|rl| {
                let guard = rl.lock();
                guard.current_sequence()
            })
            .unwrap_or(0);
        {
            let mut mgr = migration.lock().unwrap();
            mgr.set_snapshot_sequence(task, snapshot_seq);
        }

        // Phase 1: baseline
        let _baseline_manifest = match stream_shard_baseline(
            task,
            shard_keys,
            engine,
            stream,
            batch_size,
            topology_epoch,
        ) {
            Ok(m) => m,
            Err(e) => {
                last_err = format!("baseline: {e}");
                if attempt < 2 {
                    continue;
                }
                tracing::warn!(shard = task.shard, err = %last_err, "cluster: shard migration failed (final attempt)");
                fail_shard(migration, shard_table, failed, true);
                return false;
            }
        };

        // Handoff state check: if the shard has already been committed or
        // rolled back (ServingNew), abort — a newer topology has superseded
        // this migration.  But if the shard is still in Copying/CommitReady,
        // the migration is still valid even if the epoch was bumped by a
        // re-activation, so continue.
        {
            let table = shard_table.read();
            let handoff = table.shard_handoff_state(task.shard);
            if handoff == ShardHandoff::ServingNew {
                tracing::info!(
                    shard = task.shard,
                    "cluster: shard migration aborted — shard already committed/rolled back"
                );
                drop(table);
                // Shard is already being served by the new master. Send
                // OP_MIGRATION_COMPLETE to the RECEIVER so it clears the
                // inbound entry and stops blocking writes. Use record_count=0
                // to signal this is a no-data completion.
                let _ = send_migration_complete(
                    addr,
                    task.shard,
                    task.from_node,
                    0,
                    0,
                    0,
                    None,
                    &[0u8; 32],
                    &[],
                    false,
                );
                fail_shard(migration, shard_table, failed, false);
                return false;
            }
        }

        // Phase 2: Fence writes BEFORE capturing the redo sequence.
        // This guarantees no write can slip through between the sequence
        // capture and the fence. Any write that arrives between the
        // baseline snapshot and the fence is captured in the delta stream.
        let fence_seq;
        {
            let mut mgr = migration.lock().unwrap();
            mgr.fence_shard(task.shard);
            fenced_bm.set(task.shard);
            drop(mgr);
        }
        // Drain in-flight writes: acquire and release the redo lock to
        // ensure any write that started before the fence has committed its
        // redo entry.
        {
            fence_seq = redo_log
                .as_ref()
                .map(|rl| {
                    let guard = rl.lock();
                    guard.current_sequence()
                })
                .unwrap_or(0);
            let mut mgr = migration.lock().unwrap();
            mgr.mark_fenced(task, fence_seq);
        }

        let snapshot_keys: std::collections::HashSet<TxKey> =
            shard_keys.iter().copied().copied().collect();
        // Fast check: if the shard record count matches the snapshot,
        // no new keys appeared during the baseline and we can skip
        // the expensive full index scan for late keys.
        let fenced_count = engine.shard_record_count(task.shard);
        let mut fenced_keys = if fenced_count as usize == snapshot_keys.len() {
            shard_keys.iter().map(|k| **k).collect::<Vec<TxKey>>()
        } else {
            engine.keys_for_shard(task.shard)
        };
        let late_keys: Vec<TxKey> = fenced_keys
            .iter()
            .copied()
            .filter(|k| !snapshot_keys.contains(k))
            .collect();
        debug_shard_log(
            task.shard,
            format!(
                "fenced snapshot_keys={} fenced_keys={} late_keys={} snapshot_seq={} fence_seq={}",
                snapshot_keys.len(),
                fenced_keys.len(),
                late_keys.len(),
                snapshot_seq,
                fence_seq,
            ),
        );
        if !late_keys.is_empty() {
            tracing::info!(
                shard = task.shard,
                late_keys = late_keys.len(),
                "cluster: shard fenced re-scan found missing pre-snapshot keys",
            );
            let late_key_refs: Vec<&TxKey> = late_keys.iter().collect();
            if let Err(e) = stream_shard_baseline(
                task,
                &late_key_refs,
                engine,
                stream,
                batch_size,
                topology_epoch,
            ) {
                last_err = format!("late baseline: {e}");
                if attempt < 2 {
                    continue;
                }
                tracing::warn!(shard = task.shard, err = %last_err, "cluster: shard migration failed (final attempt)");
                fail_shard(migration, shard_table, failed, true);
                return false;
            }
        }

        // Phase 3: Stream deltas (writes between baseline snapshot and fence).
        // For quiesce migrations where snapshot_seq was not set (== 0),
        // there's no delta window: the baseline already captured all keys
        // that existed at activation time, and any writes between activation
        // and fence are captured here if snapshot_seq > 0.
        let mut delta_failed = false;
        match collect_migration_delta_ops(redo_log, snapshot_seq, fence_seq, task.shard, engine) {
            Ok(delta_ops) => {
                debug_shard_log(
                    task.shard,
                    format!(
                        "delta_ops={} snapshot_seq={} fence_seq={}",
                        delta_ops.len(),
                        snapshot_seq,
                        fence_seq,
                    ),
                );
                if !delta_ops.is_empty() {
                    tracing::debug!(
                        shard = task.shard,
                        delta_ops = delta_ops.len(),
                        snapshot_seq,
                        fence_seq,
                        "cluster: shard streaming delta ops",
                    );
                    if let Err(e) = send_delta_ops(stream, task.shard, &delta_ops, topology_epoch) {
                        tracing::warn!(shard = task.shard, err = %e, "cluster: shard delta streaming failed");
                        last_err = e;
                        delta_failed = true;
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    shard = task.shard,
                    err = %e,
                    "cluster: shard migration must restart due to unavailable redo delta",
                );
                last_err = e;
                delta_failed = true;
            }
        }
        if delta_failed {
            if attempt < 2 {
                continue;
            }
            tracing::warn!(shard = task.shard, err = %last_err, "cluster: shard migration failed (final attempt)");
            fail_shard(migration, shard_table, failed, true);
            return false;
        }

        let mut known_fenced_keys: std::collections::HashSet<TxKey> =
            fenced_keys.iter().copied().collect();
        for pass in 0..3 {
            // Fast check: if the shard count hasn't changed since the
            // fence, no new keys appeared and we can skip the full scan.
            let current_count = engine.shard_record_count(task.shard) as usize;
            if current_count == known_fenced_keys.len() {
                break;
            }
            let post_delta_keys = engine.keys_for_shard(task.shard);
            let post_delta_late_keys: Vec<TxKey> = post_delta_keys
                .iter()
                .copied()
                .filter(|k| !known_fenced_keys.contains(k))
                .collect();
            if post_delta_late_keys.is_empty() {
                fenced_keys = post_delta_keys;
                break;
            }
            tracing::info!(
                shard = task.shard,
                pass = pass + 1,
                new_keys = post_delta_late_keys.len(),
                "cluster: shard post-delta stabilization found newly appeared keys",
            );
            let late_key_refs: Vec<&TxKey> = post_delta_late_keys.iter().collect();
            if let Err(e) = stream_shard_baseline(
                task,
                &late_key_refs,
                engine,
                stream,
                batch_size,
                topology_epoch,
            ) {
                last_err = format!("post-delta baseline: {e}");
                if attempt < 2 {
                    continue;
                }
                tracing::warn!(shard = task.shard, err = %last_err, "cluster: shard migration failed (final attempt)");
                fail_shard(migration, shard_table, failed, true);
                return false;
            }
            known_fenced_keys.extend(post_delta_late_keys.iter().copied());
            fenced_keys = post_delta_keys;
            if pass == 2 {
                tracing::warn!(
                    shard = task.shard,
                    "cluster: shard post-delta stabilization did not converge after 3 passes",
                );
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        // Handoff state check before final handshake: if the shard was
        // committed or rolled back by a newer topology, abort.  Otherwise
        // continue — the migration is still valid.
        {
            let table = shard_table.read();
            let handoff = table.shard_handoff_state(task.shard);
            if handoff == ShardHandoff::ServingNew {
                tracing::info!(
                    shard = task.shard,
                    "cluster: shard migration aborted before complete — shard already committed/rolled back"
                );
                drop(table);
                let _ = send_migration_complete(
                    addr,
                    task.shard,
                    task.from_node,
                    0,
                    0,
                    0,
                    Some(stream),
                    &[0u8; 32],
                    &[],
                    false,
                );
                fail_shard(migration, shard_table, failed, false);
                return false;
            }
        }

        // Compute final manifest hash from engine state (post-fence, post-delta).
        // This is the authoritative fingerprint of the shard's content.
        // The target will compute the same hash from its local state to verify.
        let manifest_entries = match collect_manifest_entries(engine, task.shard, &fenced_keys) {
            Ok(entries) => entries,
            Err(e) => {
                last_err = e;
                if attempt < 2 {
                    continue;
                }
                tracing::warn!(shard = task.shard, err = %last_err, "cluster: shard migration failed (final attempt)");
                fail_shard(migration, shard_table, failed, true);
                return false;
            }
        };
        let manifest_hash = compute_manifest_for_entries(&manifest_entries);

        debug_shard_log(
            task.shard,
            format!(
                "handshake from={} to={} fence_seq={} records={} manifest_entries={} epoch={}",
                task.from_node.0,
                task.to_node.0,
                fence_seq,
                manifest_entries.len(),
                manifest_entries.len(),
                topology_epoch,
            ),
        );
        if let Err(e) = send_migration_complete(
            addr,
            task.shard,
            task.from_node,
            manifest_entries.len() as u64,
            fence_seq,
            topology_epoch,
            Some(stream),
            &manifest_hash,
            &manifest_entries,
            false,
        ) {
            last_err = format!("handshake: {e}");
            if attempt < 2 {
                continue;
            }
            tracing::warn!(shard = task.shard, err = %last_err, "cluster: shard migration failed (final attempt)");
            fail_shard(migration, shard_table, failed, true);
            return false;
        }

        // Success: mark complete and commit.
        let mut mgr = migration.lock().unwrap();
        mgr.mark_complete(task);
        if !mgr.is_shard_fenced(task.shard) {
            fenced_bm.clear(task.shard);
        }
        migrating_bm.clear(task.shard);
        drop(mgr);
        // Two-phase activation: commit this shard so routing switches
        // from the old master to the new master.
        let should_commit_local_handoff = {
            let table = shard_table.read();
            let target_assignment = table.target_assignment(task.shard);
            task.is_master
                || target_assignment.master == task.from_node
                || target_assignment.replicas.contains(&task.from_node)
        };
        debug_shard_log(
            task.shard,
            format!(
                "complete from={} to={} commit_local={} fenced_keys={}",
                task.from_node.0,
                task.to_node.0,
                should_commit_local_handoff,
                fenced_keys.len(),
            ),
        );
        if should_commit_local_handoff {
            shard_table.write().commit_shard(task.shard);
        }
        completed.fetch_add(1, Ordering::Relaxed);
        return true; // success — exit retry loop
    }
    false // All retries exhausted
}

/// Stream baseline records for one shard on an existing TCP connection.
///
/// Returns the manifest hash accumulated over all streamed records
/// (txid XOR generation for each record). The hash is order-independent
/// so the target can verify content equality regardless of apply order.
#[allow(clippy::too_many_arguments)]
fn stream_shard_baseline(
    task: &MigrationTask,
    shard_keys: &[&TxKey],
    engine: &Engine,
    stream: &mut TcpStream,
    batch_size: usize,
    cluster_key: u64,
) -> std::result::Result<ManifestHasher, String> {
    use crate::record::{UTXO_FROZEN, UTXO_SPENT};
    use crate::replication::protocol::{ReplicaBatch, ReplicaOp};

    let batch_size = batch_size.max(1);
    let mut manifest = ManifestHasher::new();
    for chunk in shard_keys.chunks(batch_size) {
        let mut ops = Vec::with_capacity(chunk.len() * 2);
        for key in chunk {
            let meta = match engine.read_metadata(key) {
                Ok(meta) => meta,
                Err(crate::ops::error::SpendError::TxNotFound) => {
                    tracing::debug!(
                        shard = task.shard,
                        key = ?key,
                        "cluster: skipping stale key during baseline stream",
                    );
                    continue;
                }
                Err(e) => {
                    return Err(format!(
                        "baseline read_metadata shard {} key {:?}: {e:?}",
                        task.shard, key
                    ));
                }
            };
            // Accumulate (txid, generation) into the manifest hash.
            manifest.fold(&key.txid, meta.generation);

            let utxo_count = meta.utxo_count;
            let mut utxo_hashes = Vec::with_capacity(utxo_count as usize);
            let mut slots = Vec::with_capacity(utxo_count as usize);
            for v in 0..utxo_count {
                let slot = engine.read_slot(key, v).map_err(|e| {
                    format!(
                        "baseline read_slot shard {} key {:?} offset {}: {e:?}",
                        task.shard, key, v
                    )
                })?;
                utxo_hashes.push(slot.hash);
                slots.push(slot);
            }

            // Serialize metadata (70 bytes with extended fields).
            let mut meta_buf = Vec::with_capacity(70);
            meta_buf.extend_from_slice(&meta.tx_version.to_le_bytes());
            meta_buf.extend_from_slice(&meta.locktime.to_le_bytes());
            meta_buf.extend_from_slice(&meta.fee.to_le_bytes());
            meta_buf.extend_from_slice(&meta.size_in_bytes.to_le_bytes());
            meta_buf.extend_from_slice(&meta.extended_size.to_le_bytes());
            let (is_coinbase, wire_flags) =
                crate::replication::protocol::create_metadata_flag_bytes(meta.flags);
            meta_buf.push(is_coinbase);
            meta_buf.extend_from_slice(&meta.spending_height.to_le_bytes());
            meta_buf.extend_from_slice(&meta.created_at.to_le_bytes());
            meta_buf.push(wire_flags);
            meta_buf.extend_from_slice(&meta.generation.to_le_bytes());
            meta_buf.extend_from_slice(&meta.updated_at.to_le_bytes());
            meta_buf.extend_from_slice(&meta.unmined_since.to_le_bytes());
            meta_buf.extend_from_slice(&meta.delete_at_height.to_le_bytes());
            meta_buf.extend_from_slice(&meta.preserve_until.to_le_bytes());

            let cold_data = if meta.flags.contains(crate::record::TxFlags::EXTERNAL) {
                engine
                    .blob_store()
                    .and_then(|bs| bs.get(&key.txid).ok().flatten())
            } else {
                None
            };

            ops.push(ReplicaOp::Create {
                tx_key: **key,
                metadata_bytes: meta_buf,
                utxo_hashes,
                cold_data,
                is_external: meta.flags.contains(crate::record::TxFlags::EXTERNAL),
            });

            // Replay spent/frozen slot state so the replica matches the master.
            // Use the master's generation for all replay ops so the replica
            // ends up with the same generation counter.
            let tx_key = **key;
            let record_gen = { meta.generation };
            for (v, slot) in slots.iter().enumerate() {
                if slot.status == UTXO_SPENT {
                    ops.push(ReplicaOp::Spend {
                        tx_key,
                        offset: v as u32,
                        spending_data: slot.spending_data,
                        master_generation: record_gen,
                    });
                } else if slot.status == UTXO_FROZEN {
                    ops.push(ReplicaOp::Freeze {
                        tx_key,
                        offset: v as u32,
                        master_generation: record_gen,
                    });
                }
            }

            // Replay block entries (mined state)
            for i in 0..meta.block_entry_count as usize {
                if i < crate::record::INLINE_BLOCK_ENTRIES {
                    let be = &meta.block_entries_inline[i];
                    if be.block_id != 0 || be.block_height != 0 {
                        ops.push(ReplicaOp::SetMined {
                            tx_key,
                            block_id: be.block_id,
                            block_height: be.block_height,
                            subtree_idx: be.subtree_idx,
                            on_longest_chain: true,
                            master_generation: record_gen,
                        });
                    }
                }
            }

            // Replay conflicting/locked flags
            if meta.flags.contains(crate::record::TxFlags::CONFLICTING) {
                ops.push(ReplicaOp::SetConflicting {
                    tx_key,
                    value: true,
                    current_block_height: 0,
                    retention: 0,
                    master_generation: record_gen,
                });
            }
            if meta.flags.contains(crate::record::TxFlags::LOCKED) {
                ops.push(ReplicaOp::SetLocked {
                    tx_key,
                    value: true,
                    master_generation: record_gen,
                });
            }
        }

        if ops.is_empty() {
            continue;
        }

        let batch = ReplicaBatch {
            first_sequence: 0,
            ops,
            trace_ctx: crate::observability::WireTraceContext::from_current_span(),
            source_node_id: Some(task.from_node.0),
            // Phase B3: stamped with the source's live coordinator epoch
            // so a topology-change race aborts the migration via the
            // receiver's stale-epoch gate instead of corrupting state.
            cluster_key,
        };

        // Send as OP_REPLICA_BATCH with FLAG_MIGRATION_BATCH so the
        // target registers the shard as receiving inbound migration data.
        let request = RequestFrame {
            request_id: task.shard as u64,
            op_code: OP_REPLICA_BATCH,
            flags: FLAG_MIGRATION_BATCH,
            payload: batch.serialize(),
        };

        let response = exchange_frame(stream, &request)?;

        if response.status != STATUS_OK {
            return Err(format!(
                "migration batch failed with status {}",
                response.status
            ));
        }

        // The receiver always returns STATUS_OK and encodes the real
        // result in the ReplicaAck payload.  A deserialization or
        // apply_op failure produces ReplicaAck::Error, which we must
        // detect — otherwise we'd mark the migration as successful
        // when the target silently rejected the data.
        use crate::replication::protocol::ReplicaAck;
        if !response.payload.is_empty() {
            match ReplicaAck::deserialize(&response.payload) {
                Ok(ReplicaAck::Error {
                    failed_sequence,
                    message,
                }) => {
                    return Err(format!(
                        "migration batch: replica reported error at seq {failed_sequence}: {message}"
                    ));
                }
                Ok(ReplicaAck::Ok { .. }) => {} // success
                Err(e) => {
                    return Err(format!("migration batch: failed to parse replica ack: {e}"));
                }
            }
        }
    }

    Ok(manifest)
}

/// Send the OP_MIGRATION_COMPLETE handshake on an existing or new stream.
///
/// The payload includes the expected record count, fence sequence, and
/// topology epoch so the target can perform a stronger verification than
/// a simple count check.
///
/// If `stream` is Some, reuses it (avoids a new TCP connection).
/// Otherwise opens a fresh connection.
#[allow(clippy::too_many_arguments)]
fn send_migration_complete(
    target_addr: SocketAddr,
    shard: u16,
    from_node: NodeId,
    record_count: u64,
    fence_sequence: u64,
    topology_epoch: u64,
    stream: Option<&mut TcpStream>,
    manifest_hash: &[u8; 32],
    manifest_entries: &[(TxKey, u32)],
    verify_only: bool,
) -> std::result::Result<(), String> {
    // Use existing stream or create new one.
    let mut owned;
    let s: &mut TcpStream = match stream {
        Some(s) => s,
        None => {
            owned = TcpStream::connect_timeout(&target_addr, Duration::from_secs(3))
                .map_err(|e| format!("connect: {e}"))?;
            owned
                .set_read_timeout(Some(Duration::from_secs(5)))
                .map_err(|e| format!("set read timeout: {e}"))?;
            crate::replication::tcp_transport::configure_tcp_keepalive(&owned);
            &mut owned
        }
    };

    // Wire layout (all little-endian):
    //   [0..8]   record_count:    u64
    //   [8..16]  fence_sequence:  u64
    //   [16..24] topology_epoch:  u64
    //   [24..56] manifest_hash:   [u8; 32]
    //   [56..60] entry_count (N): u32
    //   [60..60+N*36] manifest entries, each 36 bytes:
    //       [0..32] txid: [u8; 32]
    //       [32..36] generation: u32
    //   [60+N*36..68+N*36] from_node: u64
    let mut payload = Vec::with_capacity(68 + manifest_entries.len() * 36);
    payload.extend_from_slice(&record_count.to_le_bytes());
    payload.extend_from_slice(&fence_sequence.to_le_bytes());
    payload.extend_from_slice(&topology_epoch.to_le_bytes());
    payload.extend_from_slice(manifest_hash);
    payload.extend_from_slice(&(manifest_entries.len() as u32).to_le_bytes());
    for (key, generation) in manifest_entries {
        payload.extend_from_slice(&key.txid);
        payload.extend_from_slice(&generation.to_le_bytes());
    }
    payload.extend_from_slice(&from_node.0.to_le_bytes());

    let request = RequestFrame {
        request_id: shard as u64,
        op_code: OP_MIGRATION_COMPLETE,
        flags: if verify_only {
            FLAG_MIGRATION_VERIFY_ONLY
        } else {
            0
        },
        payload,
    };
    let response = exchange_frame(s, &request)?;

    if response.status != STATUS_OK {
        let detail = if response.payload.is_empty() {
            String::new()
        } else {
            // Error payload: [code:2][msg_len:2][msg:N]
            if response.payload.len() >= 4 {
                let code = u16::from_le_bytes(response.payload[..2].try_into().unwrap());
                let msg_len =
                    u16::from_le_bytes(response.payload[2..4].try_into().unwrap()) as usize;
                let msg = std::str::from_utf8(
                    &response.payload[4..4 + msg_len.min(response.payload.len() - 4)],
                )
                .unwrap_or("(non-utf8)");
                format!(" (code={code}: {msg})")
            } else {
                format!(" (payload: {:?})", &response.payload)
            }
        };
        return Err(format!(
            "target rejected: status {}{detail}",
            response.status
        ));
    }
    Ok(())
}

/// Send batched migration-complete handshakes for multiple shards in a
/// single TCP frame. This replaces the old per-shard sequential approach
/// that required one TCP round-trip per shard (~2730 round-trips ≈ 3-15s).
/// The batched version sends all shard IDs in one `OP_MIGRATION_BATCH_COMPLETE`
/// frame for a single round-trip (~1ms).
fn send_completion_only_handshakes(
    target_addr: SocketAddr,
    tasks: &[MigrationTask],
    from_node: NodeId,
) -> Vec<bool> {
    if tasks.is_empty() {
        return Vec::new();
    }

    const MAX_RETRIES: usize = 40;
    const CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
    // Batches can contain 2000+ shards; the receiver iterates all of them
    // calling mark_inbound_complete + commit_shard per shard. Allow 30s
    // to avoid EAGAIN (os error 11) on large batches.
    const IO_TIMEOUT: Duration = Duration::from_secs(30);
    const RETRY_DELAY: Duration = Duration::from_millis(100);

    // Collect all shard IDs for the batch.
    let shards: Vec<u16> = tasks.iter().map(|t| t.shard).collect();

    // Wire format: [shard_count:4][shard_id:2 × count][from_node:8]
    let mut payload = Vec::with_capacity(4 + shards.len() * 2 + 8);
    payload.extend_from_slice(&(shards.len() as u32).to_le_bytes());
    for &shard in &shards {
        payload.extend_from_slice(&shard.to_le_bytes());
    }
    payload.extend_from_slice(&from_node.0.to_le_bytes());

    let request = RequestFrame {
        request_id: 0,
        op_code: OP_MIGRATION_BATCH_COMPLETE,
        flags: 0,
        payload,
    };

    for attempt in 0..MAX_RETRIES {
        if attempt > 0 {
            std::thread::sleep(RETRY_DELAY);
        }
        let mut stream = match TcpStream::connect_timeout(&target_addr, CONNECT_TIMEOUT) {
            Ok(s) => {
                let _ = s.set_read_timeout(Some(IO_TIMEOUT));
                let _ = s.set_write_timeout(Some(IO_TIMEOUT));
                crate::replication::tcp_transport::configure_tcp_keepalive(&s);
                s
            }
            Err(e) => {
                tracing::warn!(
                    %target_addr,
                    attempt = attempt + 1,
                    max_retries = MAX_RETRIES,
                    err = %e,
                    "cluster: batch-complete connect failed",
                );
                continue;
            }
        };

        match exchange_frame(&mut stream, &request) {
            Ok(response) => {
                if response.status == STATUS_OK {
                    return vec![true; tasks.len()];
                }
                // Parse per-shard results from response payload if present.
                // Response format: [count:4][shard:2 + ok:1] × count
                if response.payload.len() >= 4 {
                    let count =
                        u32::from_le_bytes(response.payload[..4].try_into().unwrap()) as usize;
                    if count == tasks.len() && response.payload.len() >= 4 + count * 3 {
                        let mut delivered = vec![false; tasks.len()];
                        for (i, slot) in delivered.iter_mut().enumerate().take(count) {
                            let off = 4 + i * 3;
                            // shard at off..off+2, ok at off+2
                            *slot = response.payload[off + 2] != 0;
                        }
                        return delivered;
                    }
                }
                // Fallback: treat entire batch as delivered on non-OK
                // (the target processed what it could).
                return vec![true; tasks.len()];
            }
            Err(e) => {
                tracing::warn!(
                    %target_addr,
                    attempt = attempt + 1,
                    max_retries = MAX_RETRIES,
                    err = %e,
                    "cluster: batch-complete failed",
                );
            }
        }
    }

    tracing::warn!(
        %target_addr,
        max_retries = MAX_RETRIES,
        shards = tasks.len(),
        "cluster: batch-complete exhausted retries",
    );
    vec![false; tasks.len()]
}

/// Convert a redo log entry to a ReplicaOp if it belongs to the given shard.
///
/// Returns None for entries belonging to other shards, or for non-replicatable
/// ops like Checkpoint and MarkOnLongestChain. Create and Delete ops are
/// converted so that records created or deleted after the baseline snapshot
/// are captured in the delta phase.
pub fn redo_entry_to_replica_op(
    entry: &crate::redo::RedoEntry,
    shard: u16,
    engine: &Engine,
) -> Option<crate::replication::protocol::ReplicaOp> {
    use crate::redo::RedoOp;
    use crate::replication::protocol::ReplicaOp;

    // Helper: read current generation for a key.
    let gen_for = |k: &TxKey| -> u32 { engine.read_metadata(k).map(|m| m.generation).unwrap_or(0) };

    match &entry.op {
        RedoOp::Spend {
            tx_key,
            offset,
            spending_data,
            ..
        } => {
            if ShardTable::shard_for_key(tx_key) != shard {
                return None;
            }
            Some(ReplicaOp::Spend {
                tx_key: *tx_key,
                offset: *offset,
                spending_data: *spending_data,
                master_generation: gen_for(tx_key),
            })
        }
        RedoOp::Unspend { tx_key, offset, .. } => {
            if ShardTable::shard_for_key(tx_key) != shard {
                return None;
            }
            Some(ReplicaOp::Unspend {
                tx_key: *tx_key,
                offset: *offset,
                master_generation: gen_for(tx_key),
            })
        }
        RedoOp::SetMined {
            tx_key,
            block_id,
            block_height,
            subtree_idx,
            unset,
        } => {
            if ShardTable::shard_for_key(tx_key) != shard {
                return None;
            }
            if *unset {
                Some(ReplicaOp::UnsetMined {
                    tx_key: *tx_key,
                    block_id: *block_id,
                    master_generation: gen_for(tx_key),
                })
            } else {
                Some(ReplicaOp::SetMined {
                    tx_key: *tx_key,
                    block_id: *block_id,
                    block_height: *block_height,
                    subtree_idx: *subtree_idx,
                    on_longest_chain: true,
                    master_generation: gen_for(tx_key),
                })
            }
        }
        RedoOp::Freeze { tx_key, offset } => {
            if ShardTable::shard_for_key(tx_key) != shard {
                return None;
            }
            Some(ReplicaOp::Freeze {
                tx_key: *tx_key,
                offset: *offset,
                master_generation: gen_for(tx_key),
            })
        }
        RedoOp::Unfreeze { tx_key, offset } => {
            if ShardTable::shard_for_key(tx_key) != shard {
                return None;
            }
            Some(ReplicaOp::Unfreeze {
                tx_key: *tx_key,
                offset: *offset,
                master_generation: gen_for(tx_key),
            })
        }
        RedoOp::Reassign {
            tx_key,
            offset,
            new_hash,
            block_height,
            spendable_after,
        } => {
            if ShardTable::shard_for_key(tx_key) != shard {
                return None;
            }
            Some(ReplicaOp::Reassign {
                tx_key: *tx_key,
                offset: *offset,
                new_hash: *new_hash,
                block_height: *block_height,
                spendable_after: *spendable_after,
                master_generation: gen_for(tx_key),
            })
        }
        RedoOp::SetConflicting {
            tx_key,
            value,
            current_block_height,
            block_height_retention,
        } => {
            if ShardTable::shard_for_key(tx_key) != shard {
                return None;
            }
            Some(ReplicaOp::SetConflicting {
                tx_key: *tx_key,
                value: *value,
                current_block_height: *current_block_height,
                retention: *block_height_retention,
                master_generation: gen_for(tx_key),
            })
        }
        RedoOp::SetLocked { tx_key, value } => {
            if ShardTable::shard_for_key(tx_key) != shard {
                return None;
            }
            Some(ReplicaOp::SetLocked {
                tx_key: *tx_key,
                value: *value,
                master_generation: gen_for(tx_key),
            })
        }
        RedoOp::PreserveUntil {
            tx_key,
            block_height,
        } => {
            if ShardTable::shard_for_key(tx_key) != shard {
                return None;
            }
            Some(ReplicaOp::PreserveUntil {
                tx_key: *tx_key,
                block_height: *block_height,
                master_generation: gen_for(tx_key),
            })
        }
        RedoOp::PruneSlot { tx_key, offset } => {
            if ShardTable::shard_for_key(tx_key) != shard {
                return None;
            }
            Some(ReplicaOp::PruneSlot {
                tx_key: *tx_key,
                offset: *offset,
            })
        }
        RedoOp::Create { tx_key, .. } => {
            // A record created after the baseline snapshot must be sent as a
            // delta, otherwise the target never receives it. We read the full
            // current record state from the engine (metadata, UTXOs, cold data)
            // and emit a ReplicaOp::Create. Any subsequent mutations (Spend,
            // SetMined, etc.) within the delta range have their own redo
            // entries which are already converted above, and applying them
            // twice on the target is harmless (all ops are idempotent).
            if ShardTable::shard_for_key(tx_key) != shard {
                return None;
            }
            let meta = match engine.read_metadata(tx_key) {
                Ok(m) => m,
                Err(_) => return None, // record may have been deleted since
            };

            let utxo_count = meta.utxo_count;
            let mut utxo_hashes = Vec::with_capacity(utxo_count as usize);
            for v in 0..utxo_count {
                match engine.read_slot(tx_key, v) {
                    Ok(slot) => utxo_hashes.push(slot.hash),
                    Err(_) => utxo_hashes.push([0u8; 32]),
                }
            }

            // Serialize metadata in the same format as stream_shard_baseline.
            let mut meta_buf = Vec::with_capacity(70);
            meta_buf.extend_from_slice(&meta.tx_version.to_le_bytes());
            meta_buf.extend_from_slice(&meta.locktime.to_le_bytes());
            meta_buf.extend_from_slice(&meta.fee.to_le_bytes());
            meta_buf.extend_from_slice(&meta.size_in_bytes.to_le_bytes());
            meta_buf.extend_from_slice(&meta.extended_size.to_le_bytes());
            let (is_coinbase, wire_flags) =
                crate::replication::protocol::create_metadata_flag_bytes(meta.flags);
            meta_buf.push(is_coinbase);
            meta_buf.extend_from_slice(&meta.spending_height.to_le_bytes());
            meta_buf.extend_from_slice(&meta.created_at.to_le_bytes());
            meta_buf.push(wire_flags);
            // Extended metadata for full failover state:
            meta_buf.extend_from_slice(&meta.generation.to_le_bytes());
            meta_buf.extend_from_slice(&meta.updated_at.to_le_bytes());
            meta_buf.extend_from_slice(&meta.unmined_since.to_le_bytes());
            meta_buf.extend_from_slice(&meta.delete_at_height.to_le_bytes());
            meta_buf.extend_from_slice(&meta.preserve_until.to_le_bytes());

            let cold_data = if meta.flags.contains(crate::record::TxFlags::EXTERNAL) {
                engine
                    .blob_store()
                    .and_then(|bs| bs.get(&tx_key.txid).ok().flatten())
            } else {
                None
            };

            Some(ReplicaOp::Create {
                tx_key: *tx_key,
                metadata_bytes: meta_buf,
                utxo_hashes,
                cold_data,
                is_external: meta.flags.contains(crate::record::TxFlags::EXTERNAL),
            })
        }
        RedoOp::Delete { tx_key, .. } => {
            // A delete after the baseline snapshot must be forwarded so the
            // target removes the record. Without this, deleted records would
            // be resurrected on the target.
            if ShardTable::shard_for_key(tx_key) != shard {
                return None;
            }
            Some(ReplicaOp::Delete { tx_key: *tx_key })
        }
        // Checkpoint is a no-op. MarkOnLongestChain is a secondary index
        // operation that gets rebuilt. SecondaryUnminedUpdate and
        // SecondaryDahUpdate are local durability-intent records for the
        // redb secondary indexes — replicas rebuild their secondaries from
        // their own metadata replay. AllocateRegion/FreeRegion are local
        // allocator-journal records with no replicated effect — replicas
        // allocate their own regions independently. HashtableResizeBegin /
        // HashtableResizeCommit are local file-backed-index durability
        // records — replicas resize their own indexes independently.
        RedoOp::Checkpoint
        | RedoOp::MarkOnLongestChain { .. }
        | RedoOp::SecondaryUnminedUpdate { .. }
        | RedoOp::SecondaryDahUpdate { .. }
        | RedoOp::AllocateRegion { .. }
        | RedoOp::FreeRegion { .. }
        | RedoOp::HashtableResizeBegin { .. }
        | RedoOp::HashtableResizeCommit { .. } => None,
    }
}

fn collect_migration_delta_ops(
    redo_log: &Option<Arc<ParkingMutex<RedoLog>>>,
    snapshot_seq: u64,
    fence_seq: u64,
    shard: u16,
    engine: &Engine,
) -> std::result::Result<Vec<crate::replication::protocol::ReplicaOp>, String> {
    if snapshot_seq == 0 || fence_seq <= snapshot_seq {
        return Ok(Vec::new());
    }
    let Some(rl) = redo_log else {
        return Ok(Vec::new());
    };

    let entries = rl
        .lock()
        .read_from_sequence(snapshot_seq)
        .map_err(|e| format!("read redo from seq {snapshot_seq}: {e}"))?;
    let first_entry_seq = entries.first().map(|e| e.sequence);
    crate::replication::durable::check_redo_truncation(first_entry_seq, snapshot_seq)?;

    Ok(entries
        .iter()
        .filter(|e| e.sequence < fence_seq)
        .filter_map(|e| redo_entry_to_replica_op(e, shard, engine))
        .collect())
}

/// Send delta ReplicaOps to the target on an existing stream and validate ACK.
///
/// Used by both the reference `migrate_single_shard` path and the pipelined
/// batch migration to close the baseline-to-fence redo window.
#[allow(dead_code)]
fn send_delta_ops(
    stream: &mut TcpStream,
    shard: u16,
    ops: &[crate::replication::protocol::ReplicaOp],
    cluster_key: u64,
) -> std::result::Result<(), String> {
    use crate::replication::protocol::{ReplicaAck, ReplicaBatch};

    let batch = ReplicaBatch {
        first_sequence: 0,
        ops: ops.to_vec(),
        trace_ctx: crate::observability::WireTraceContext::from_current_span(),
        source_node_id: None,
        // Phase B3: stamped with the source's live coordinator epoch.
        cluster_key,
    };
    let request = RequestFrame {
        request_id: shard as u64,
        op_code: OP_REPLICA_BATCH,
        flags: FLAG_MIGRATION_BATCH,
        payload: batch.serialize(),
    };
    let response = exchange_frame(stream, &request)?;

    if response.status != STATUS_OK {
        return Err(format!("delta batch rejected: status {}", response.status));
    }

    // Validate ReplicaAck payload
    if !response.payload.is_empty() {
        match ReplicaAck::deserialize(&response.payload) {
            Ok(ReplicaAck::Error {
                failed_sequence,
                message,
            }) => {
                return Err(format!(
                    "delta apply error at seq {failed_sequence}: {message}"
                ));
            }
            Ok(ReplicaAck::Ok { .. }) => {}
            Err(e) => {
                return Err(format!("failed to parse delta ack: {e}"));
            }
        }
    }

    Ok(())
}

/// Global counter for cluster state persistence failures.
///
/// Exposed via [`persist_failure_count`] so monitoring/health checks can
/// detect when critical state (peak cluster size, topology term) is not
/// being written to disk. A non-zero count means a restart could lose
/// the quorum guarantee or committed topology, risking split-brain.
static PERSIST_FAILURES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Number of cluster state persistence failures since process start.
pub fn persist_failure_count() -> u64 {
    PERSIST_FAILURES.load(Ordering::Relaxed)
}

/// Persist the cluster state (peak size + topology epoch) to disk.
///
/// File format: `[peak:8 LE][epoch:8 LE]`.
/// Errors are logged and counted via [`PERSIST_FAILURES`] but do not
/// propagate — the cluster continues operating but a restart may lose
/// the quorum guarantee.
fn persist_cluster_state(path: &std::path::Path, peak: u64, epoch: u64) {
    use std::io::Write as _;
    let tmp = path.with_extension("cluster.tmp");
    let result = (|| -> std::io::Result<()> {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&peak.to_le_bytes())?;
        f.write_all(&epoch.to_le_bytes())?;
        f.sync_all()?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    })();
    if let Err(e) = result {
        PERSIST_FAILURES.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(err = %e, "cluster: failed to persist cluster state");
    }
}

/// Persist the full topology state (new format with committed members).
///
/// Returns `Ok(())` on durable write + fsync + atomic rename. Errors are
/// counted in [`PERSIST_FAILURES`] and logged, then surfaced to the caller.
/// Safety-critical callers (the vote handler) MUST fail the request rather
/// than reply when this returns `Err` — see H10: a voter must never
/// advertise a vote it could lose across a crash.
fn persist_topology_state(
    path: &std::path::Path,
    state: &crate::cluster::topology::PersistedTopologyState,
) -> std::io::Result<()> {
    use std::io::Write as _;
    let tmp = path.with_extension("cluster.tmp");
    let data = state.serialize();
    let result = (|| -> std::io::Result<()> {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&data)?;
        f.sync_all()?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    })();
    if let Err(ref e) = result {
        PERSIST_FAILURES.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(err = %e, "cluster: failed to persist topology state");
    }
    result
}

/// Load the full topology state from disk (backward-compatible).
pub fn load_topology_state(
    path: &std::path::Path,
) -> crate::cluster::topology::PersistedTopologyState {
    match std::fs::read(path) {
        Ok(data) => crate::cluster::topology::PersistedTopologyState::deserialize(&data),
        _ => crate::cluster::topology::PersistedTopologyState {
            peak_cluster_size: 1,
            committed_term: 0,
            committed_members: Vec::new(),
            voted_term: 0,
            incarnation: 0,
        },
    }
}

/// Backward-compatible alias for callers that only persist peak.
#[allow(dead_code)]
fn persist_peak_cluster_size(path: &std::path::Path, peak: u64) {
    persist_cluster_state(path, peak, 0);
}

/// Load the persisted cluster state from disk.
///
/// Returns `(peak_cluster_size, topology_epoch)`.
/// Falls back to (1, 0) if the file doesn't exist or is corrupted.
/// Backward-compatible: reads 8-byte files as peak-only (epoch=0).
pub fn load_cluster_state(path: &std::path::Path) -> (usize, u64) {
    match std::fs::read(path) {
        Ok(data) if data.len() >= 16 => {
            let peak = u64::from_le_bytes(data[..8].try_into().unwrap_or([0; 8]));
            let epoch = u64::from_le_bytes(data[8..16].try_into().unwrap_or([0; 8]));
            ((peak as usize).max(1), epoch)
        }
        Ok(data) if data.len() >= 8 => {
            let peak = u64::from_le_bytes(data[..8].try_into().unwrap_or([0; 8]));
            ((peak as usize).max(1), 0)
        }
        _ => (1, 0),
    }
}

/// Load the persisted peak cluster size from disk (backward-compat wrapper).
pub fn load_peak_cluster_size(path: &std::path::Path) -> usize {
    load_cluster_state(path).0
}

/// Aerospike-style scoring for master election candidates.
///
/// Scores: previous_master = 3, full_replica = 3, subset = 2, evicted = 0.
/// The caller breaks ties by preferring the lower `NodeId`.
///
/// A node that is `is_subset` (new master still receiving inbound migration
/// data) scores 2 so that the previous master or any full replica is
/// preferred over it. An evicted node always scores 0 and must not be
/// elected master under any circumstances.
///
/// Not yet wired into the coordinator election driver — that is Phase F.
pub fn rank_master_candidate(
    node_id: NodeId,
    prev_master: NodeId,
    is_subset: bool,
    was_evicted: bool,
) -> u8 {
    if was_evicted {
        return 0;
    }
    if node_id == prev_master {
        return 3;
    }
    if is_subset { 2 } else { 3 }
}

/// Result of a [`RunningCluster::is_master`] query.
///
/// Returned in place of a bare `bool` so that callers (specifically the
/// dispatcher) can distinguish a cleanly-known non-master answer from the
/// transient *gap* between a topology proposal/membership change and its
/// quorum-committed activation. During that gap the local view of who owns
/// a shard is unreliable: the dispatcher must steer clients away from
/// following a possibly-wrong `REDIRECT` and toward retrying.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MasterQueryResult {
    /// This node is the authoritative master for the queried shard, in the
    /// currently quorum-committed topology. The dispatcher handles the
    /// request locally.
    Yes,
    /// Some other live node is the authoritative master for the queried
    /// shard. The dispatcher returns `ERR_REDIRECT` with that node's
    /// address.
    No,
    /// Topology is in flux: the local `topology_epoch` (peak observed
    /// proposed term) is ahead of the last quorum-committed term, so the
    /// shard table consulted to derive ownership is not yet authoritative.
    /// The dispatcher returns `ERR_MIGRATION_IN_PROGRESS` to instruct
    /// clients to retry rather than chase a stale redirect.
    ///
    /// `last_known_term` reports the most recent quorum-committed term —
    /// useful for diagnostics, metrics, and (eventually) hinting clients
    /// to refresh their partition map past that point.
    Transitioning {
        /// The most recent quorum-committed topology term observed by this
        /// node. The shard table at that term is the last authoritative
        /// view; anything bumped on top of it is in-flight.
        last_known_term: u64,
    },
}

pub struct RunningCluster {
    self_id: NodeId,
    shard_table: Arc<ShardTableLock<ShardTable>>,
    migration: Arc<Mutex<MigrationManager>>,
    node_addrs: Arc<RwLock<std::collections::HashMap<NodeId, SocketAddr>>>,
    swim_shutdown: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
    /// Highest observed cluster size (for quorum calculations).
    peak_size: Arc<std::sync::atomic::AtomicUsize>,
    /// Monotonic topology epoch for ownership fencing.
    topology_epoch: Arc<std::sync::atomic::AtomicU64>,
    /// Resolved ACK policy for replication durability enforcement.
    repl_ack_policy: Option<crate::replication::manager::AckPolicy>,
    /// Whether replication failures are tolerated (best_effort degraded mode).
    repl_best_effort: bool,
    /// Timeout for foreground replication ACKs.
    repl_timeout: Duration,
    /// Last time this node observed local migration pressure.
    last_migration_pressure_ms: Arc<AtomicU64>,
    /// Topology authority for quorum-committed term management.
    topology_authority: Arc<crate::cluster::topology::TopologyAuthority>,
    /// Atomic mirror of `topology_authority.committed_term()` — the
    /// cluster_key value stamped on outbound `OP_REPLICA_BATCH` traffic and
    /// gated on inbound traffic.
    ///
    /// Sourced directly from `topology_authority.committed_term_shared()`,
    /// so every successful `handle_commit` advance is observable here
    /// without an explicit setter. Intentionally NOT `topology_epoch`:
    /// per-node `topology_epoch` values diverge at startup, which would
    /// reject legitimate cross-node replication batches with
    /// `ERR_STALE_EPOCH`. The committed term converges on the same value
    /// across all peers after each `OP_TOPOLOGY_COMMIT`.
    committed_cluster_key: Arc<std::sync::atomic::AtomicU64>,
    /// Members corresponding to the currently activated shard table.
    active_topology_members: Arc<RwLock<Vec<NodeId>>>,
    /// Path for persisting inbound migration state across restarts.
    /// When set, every inbound state change is durably flushed so a
    /// crashed target knows which shards were mid-migration on recovery.
    inbound_state_path: Option<std::path::PathBuf>,
    /// Path for persisting outbound migration state across restarts.
    /// Enables source crash recovery: a restarted node knows which
    /// shards it was streaming to which targets.
    outbound_state_path: Option<std::path::PathBuf>,
    /// Lock-free bitmap: shards with write fence active (outbound migration).
    /// Shadow of `MigrationManager::fenced_shards` — updated atomically
    /// so the dispatch hot path avoids the migration mutex entirely.
    fenced_bitmap: Arc<crate::cluster::migration::AtomicShardBitmap>,
    /// Lock-free bitmap: shards with pending inbound migration data.
    /// Shadow of `MigrationManager::inbound_bitmap`.
    inbound_atomic: Arc<crate::cluster::migration::AtomicShardBitmap>,
    /// Lock-free bitmap: shards actively migrating outbound.
    migrating_bitmap: Arc<crate::cluster::migration::AtomicShardBitmap>,
    /// Channel for signaling topology commits from dispatch or proposer threads.
    /// The event loop receives these and activates the shard table.
    topology_commit_tx: std::sync::mpsc::Sender<(Vec<NodeId>, u64)>,
    /// Path for persisting full topology state (voted_term, committed_members).
    topology_state_path: Option<std::path::PathBuf>,
    /// SWIM incarnation counter shared with the event loop for persistence.
    swim_incarnation: Arc<std::sync::atomic::AtomicU64>,
    startup_reactivation_needed: Arc<AtomicBool>,
    _swim_handle: std::thread::JoinHandle<()>,
    _event_handle: std::thread::JoinHandle<()>,
}

impl RunningCluster {
    fn preferred_master_for_shard(
        table: &ShardTable,
        live_nodes: &std::collections::HashMap<NodeId, SocketAddr>,
        shard: u16,
    ) -> NodeId {
        let effective = table.effective_assignment(shard).master;
        let target = table.target_assignment(shard).master;

        if effective != target && live_nodes.contains_key(&effective) {
            effective
        } else {
            target
        }
    }

    fn authoritative_master_for_shard(&self, shard: u16) -> NodeId {
        let table = self.shard_table.read();
        let committed = self.topology_authority.committed_term();
        if table.version < committed {
            return NodeId(0);
        }
        let addrs = self.node_addrs.read().unwrap();
        Self::preferred_master_for_shard(&table, &addrs, shard)
    }

    /// This node's ID.
    pub fn self_id(&self) -> NodeId {
        self.self_id
    }

    /// Get the current shard table.
    pub fn shard_table(&self) -> Arc<ShardTableLock<ShardTable>> {
        self.shard_table.clone()
    }

    /// Determine whether this node is the master for the given key.
    ///
    /// Returns:
    /// - [`MasterQueryResult::Yes`] when this node is the authoritative
    ///   master for the key's shard in the currently quorum-committed
    ///   topology.
    /// - [`MasterQueryResult::No`] when another node holds authoritative
    ///   ownership.
    /// - [`MasterQueryResult::Transitioning`] when the local
    ///   `topology_epoch` (peak observed proposed term) is ahead of the
    ///   last quorum-committed term — i.e. a membership change has been
    ///   proposed/observed but not yet quorum-committed locally. In that
    ///   window the local shard-table view of ownership is unreliable
    ///   and the caller must surface a *retryable* error rather than a
    ///   redirect to a possibly-wrong target.
    ///
    /// Note: when the local shard table is *behind* the committed topology
    /// term (e.g. a freshly-rejoined node), this still returns
    /// [`MasterQueryResult::No`]. That case is handled inside
    /// [`Self::authoritative_master_for_shard`], which returns `NodeId(0)`
    /// (a sentinel that never matches `self_id`) so the dispatcher
    /// redirects with `NodeId(0)` and the client refetches its partition
    /// map.
    pub fn is_master(&self, key: &TxKey) -> MasterQueryResult {
        let committed = self.topology_authority.committed_term();
        let observed = self.topology_epoch.load(Ordering::Acquire);
        if observed > committed {
            return MasterQueryResult::Transitioning {
                last_known_term: committed,
            };
        }
        let shard = ShardTable::shard_for_key(key);
        let auth_master = self.authoritative_master_for_shard(shard);
        // A node that is the authoritative master for a shard but still has
        // pending inbound migration data is a subset master: it must not
        // serve requests until migration completes.
        if self.inbound_atomic.test(shard) && auth_master == self.self_id {
            return MasterQueryResult::Transitioning {
                last_known_term: committed,
            };
        }
        if auth_master == self.self_id {
            MasterQueryResult::Yes
        } else {
            MasterQueryResult::No
        }
    }

    /// Determine how to route a request for the given key.
    ///
    /// If the local shard table is behind the committed topology, returns
    /// a redirect with `NodeId(0)` to signal the client should re-fetch
    /// the partition map.
    pub fn route(&self, key: &TxKey) -> RouteDecision {
        let shard = ShardTable::shard_for_key(key);
        let table = self.shard_table.read();
        let committed = self.topology_authority.committed_term();
        if table.version < committed {
            return RouteDecision::RedirectTo {
                node: NodeId(0),
                shard_table_version: table.version,
            };
        }
        let version = table.version;
        let addrs = self.node_addrs.read().unwrap();
        let master = Self::preferred_master_for_shard(&table, &addrs, shard);

        if master == self.self_id {
            RouteDecision::HandleLocally
        } else {
            RouteDecision::RedirectTo {
                node: master,
                shard_table_version: version,
            }
        }
    }

    /// Get the effective (old) master for a shard during handoff.
    ///
    /// During two-phase handoff, the effective master is the OLD master
    /// (from before the topology change). Returns `None` if no handoff
    /// is in progress, or if the effective master is the same as the
    /// target master (no actual change).
    pub fn effective_master_for_redirect(&self, key: &TxKey) -> Option<NodeId> {
        let shard = ShardTable::shard_for_key(key);
        let table = self.shard_table.read();
        let effective = table.effective_assignment(shard).master;
        let target = table.target_assignment(shard).master;
        if effective != target && effective != self.self_id {
            Some(effective)
        } else {
            None
        }
    }

    /// Check if this node is actively migrating a shard outbound.
    ///
    /// During outbound migration, reads can still be served locally
    /// (the data hasn't been removed yet), but writes should redirect
    /// to the new master.
    ///
    /// Lock-free: reads an atomic bitmap instead of taking the migration mutex.
    pub fn is_migrating_outbound(&self, key: &TxKey) -> bool {
        let shard = ShardTable::shard_for_key(key);
        self.migrating_bitmap.test(shard)
    }

    /// Check if this node is expecting inbound migration data for the given key's shard.
    ///
    /// Lock-free: reads an atomic bitmap instead of taking the migration mutex.
    pub fn has_pending_inbound(&self, key: &TxKey) -> bool {
        let shard = ShardTable::shard_for_key(key);
        self.inbound_atomic.test(shard)
    }

    /// Check if this node is still expecting inbound migration data for a shard.
    pub fn has_pending_inbound_shard(&self, shard: u16) -> bool {
        self.inbound_atomic.test(shard)
    }

    /// Check if writes are fenced for the given key's shard.
    ///
    /// Returns true when this node is the source of an outbound migration
    /// and the shard has entered the Fenced state (baseline complete,
    /// streaming deltas). Writes are rejected; reads continue.
    ///
    /// Lock-free: reads an atomic bitmap instead of taking the migration mutex.
    pub fn is_shard_write_fenced(&self, key: &TxKey) -> bool {
        let shard = ShardTable::shard_for_key(key);
        self.fenced_bitmap.test(shard)
    }

    /// Mark an inbound shard migration as complete (data has arrived).
    ///
    /// Persists the updated inbound state to disk so a crash after
    /// completion doesn't resurrect stale pending-inbound entries.
    /// Syncs the atomic bitmap so the hot path sees the change immediately.
    pub fn mark_inbound_complete(&self, shard: u16) {
        let mgr = &mut self.migration.lock().unwrap();
        mgr.mark_inbound_complete(shard);
        self.inbound_atomic.load_from(mgr.inbound_bitmap());
        if let Some(ref path) = self.inbound_state_path {
            crate::cluster::migration::persist_inbound_state(path, mgr);
        }
    }

    pub fn mark_inbound_complete_all(&self, shard: u16) {
        let mgr = &mut self.migration.lock().unwrap();
        mgr.mark_inbound_complete_all(shard);
        self.inbound_atomic.load_from(mgr.inbound_bitmap());
        if let Some(ref path) = self.inbound_state_path {
            crate::cluster::migration::persist_inbound_state(path, mgr);
        }
    }

    pub fn mark_inbound_complete_from_source(&self, shard: u16, from_node: NodeId) {
        let mgr = &mut self.migration.lock().unwrap();
        mgr.mark_inbound_complete_from_source(shard, from_node);
        self.inbound_atomic.load_from(mgr.inbound_bitmap());
        if let Some(ref path) = self.inbound_state_path {
            crate::cluster::migration::persist_inbound_state(path, mgr);
        }
    }

    pub fn mark_inbound_complete_many_from_source(&self, shards: &[u16], from_node: NodeId) {
        if shards.is_empty() {
            return;
        }
        let mgr = &mut self.migration.lock().unwrap();
        mgr.mark_inbound_complete_many_from_source(shards.iter().copied(), from_node);
        self.inbound_atomic.load_from(mgr.inbound_bitmap());
        if let Some(ref path) = self.inbound_state_path {
            crate::cluster::migration::persist_inbound_state(path, mgr);
        }
    }

    #[cfg(test)]
    pub(crate) fn register_test_inbound_from_source(&self, shard: u16, from_node: NodeId) {
        let mgr = &mut self.migration.lock().unwrap();
        let task = MigrationTask {
            shard,
            from_node,
            to_node: self.self_id,
            is_master: true,
        };
        mgr.start_outbound(
            std::slice::from_ref(&task),
            self.self_id,
            &std::collections::HashSet::new(),
        );
        self.inbound_atomic.load_from(mgr.inbound_bitmap());
    }

    /// Register a shard as actively receiving inbound migration data.
    ///
    /// Called when the first `OP_REPLICA_BATCH` for this shard arrives
    /// so the read/write path knows to wait for migration completion.
    /// Persists to disk so a crash mid-migration blocks the shard on restart.
    /// Syncs the atomic bitmap so the hot path sees the change immediately.
    pub fn mark_inbound_active(&self, shard: u16) {
        let mgr = &mut self.migration.lock().unwrap();
        let changed = mgr.mark_inbound_active(shard);
        if changed {
            self.inbound_atomic.set(shard);
        }
        if changed && let Some(ref path) = self.inbound_state_path {
            crate::cluster::migration::persist_inbound_state(path, mgr);
        }
    }

    /// Get the address of a node.
    pub fn node_addr(&self, node: &NodeId) -> Option<SocketAddr> {
        self.node_addrs.read().unwrap().get(node).copied()
    }

    /// Get the current shard table version.
    ///
    /// Returns the committed topology term (globally agreed) rather than
    /// the local epoch counter, so all nodes that committed the same term
    /// report the same version.
    pub fn shard_table_version(&self) -> u64 {
        // Use committed_term for consistency: all nodes that committed the
        // same topology term will report the same version. Using table.version
        // would cause disagreement during the brief window between commit and
        // event-loop activation.
        self.topology_authority.committed_term()
    }

    /// Get active migration count.
    pub fn active_migrations(&self) -> usize {
        self.migration.lock().unwrap().active_count()
    }

    /// Encode the partition map for client consumption.
    pub fn encode_partition_map(&self) -> Vec<u8> {
        let table = self.shard_table.read();
        let addrs = self.node_addrs.read().unwrap();
        let active_members = self.active_topology_members.read().unwrap().clone();

        let mut buf = Vec::new();
        // Partition maps must be a self-consistent snapshot: version,
        // assignments, and committed_members must all describe the same
        // activated topology. The authority may commit a newer term before
        // the event loop installs its shard table, so do not advertise that
        // newer term here.
        buf.extend_from_slice(&table.version.to_le_bytes());

        // Node count + node info
        let nodes: Vec<_> = addrs.iter().collect();
        buf.extend_from_slice(&(nodes.len() as u32).to_le_bytes());
        for &(&node_id, &addr) in &nodes {
            buf.extend_from_slice(&node_id.0.to_le_bytes());
            let addr_str = addr.to_string();
            let addr_bytes = addr_str.as_bytes();
            buf.extend_from_slice(&(addr_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(addr_bytes);
            buf.push(1); // is_alive — required by RoutingInfo::decode format
        }

        // Shard assignments (4096 entries, each is just the master node_id).
        // During live handoff, advertise the same authoritative master used by
        // route()/is_master() so clients and servers agree. If the old master
        // is gone, fall back to the target assignment immediately.
        for shard in 0..crate::cluster::shards::NUM_SHARDS as u16 {
            let master = Self::preferred_master_for_shard(&table, &addrs, shard);
            buf.extend_from_slice(&master.0.to_le_bytes());
        }

        // Append the members that correspond to the activated shard table.
        // Catch-up reconstructs a synthetic commit from this exact snapshot,
        // so it must not observe a newer committed term than the assignments.
        buf.extend_from_slice(&(active_members.len() as u32).to_le_bytes());
        for m in &active_members {
            buf.extend_from_slice(&m.0.to_le_bytes());
        }

        buf
    }

    /// Encode the latest quorum-committed topology for catch-up.
    pub fn encode_committed_topology(&self) -> Vec<u8> {
        let term = self.topology_authority.committed_term();
        let mut members = self.topology_authority.committed_members();
        if term == 0 || members.is_empty() {
            return Vec::new();
        }
        members.sort();
        crate::cluster::topology::TopologyCommit {
            term,
            proposer: members[0],
            members: members.clone(),
            digest: crate::cluster::topology::TopologyTerm::compute_digest(term, &members),
        }
        .serialize()
    }

    /// Number of alive nodes in the cluster.
    ///
    /// Uses the committed topology members when a topology has been
    /// committed (non-empty member list). Falls back to the SWIM-derived
    /// `node_addrs` length during initial single-node startup before any
    /// topology has been committed.
    pub fn alive_node_count(&self) -> usize {
        let committed = self.topology_authority.committed_members();
        if committed.is_empty() {
            self.node_addrs.read().unwrap().len()
        } else {
            let addrs = self.node_addrs.read().unwrap();
            committed
                .iter()
                .filter(|node| addrs.contains_key(node))
                .count()
        }
    }

    /// Snapshot of all known node addresses keyed by node ID.
    pub fn node_addresses(&self) -> std::collections::HashMap<NodeId, SocketAddr> {
        self.node_addrs.read().unwrap().clone()
    }

    /// Highest cluster size ever observed (for quorum calculation).
    pub fn peak_cluster_size(&self) -> usize {
        self.peak_size.load(Ordering::Relaxed)
    }

    /// Current monotonic topology epoch.
    ///
    /// Every membership change increments this counter. Used as a
    /// fencing token for ownership validation.
    pub fn topology_epoch(&self) -> u64 {
        self.topology_epoch.load(Ordering::Relaxed)
    }

    /// Build a [`crate::cluster::migration::KeyDiagnosis`] for `shard` from
    /// this node's point of view, filling in the routing/shard-table /
    /// epoch fields. The dispatcher then completes `has_local_data` from
    /// the index before emitting the response.
    ///
    /// Used by `OP_ADMIN_DIAGNOSE_KEY`. The returned struct's
    /// `has_local_data` is always `false`; callers must overwrite it.
    pub fn diagnose_key_routing(&self, shard: u16) -> crate::cluster::migration::KeyDiagnosis {
        // Tracker-side fields (inbound, fenced, migrating).
        let mut diag = self.migration.lock().unwrap().diagnose_key_routing(shard);
        // Routing fields from the local shard table.
        let table = self.shard_table.read();
        let local_master = table.target_assignment(shard).master;
        diag.this_node_id = self.self_id.0;
        diag.local_view_canonical_master_id = local_master.0;
        diag.is_local_master_of_shard = local_master == self.self_id;
        diag.topology_epoch = self.topology_epoch();
        diag
    }

    /// Shared handle to the per-node monotonic topology epoch.
    ///
    /// Distinct from [`cluster_key_handle`](Self::cluster_key_handle):
    /// the topology epoch is per-node and used for local fencing (e.g.
    /// `is_master`'s Transitioning check), whereas `cluster_key_handle`
    /// returns the quorum-committed term used for cross-node replication
    /// gating. Tests use this accessor to simulate a local epoch bump
    /// without affecting the committed term.
    pub fn topology_epoch_handle(&self) -> Arc<std::sync::atomic::AtomicU64> {
        self.topology_epoch.clone()
    }

    /// Receiver-side cluster epoch for replication batch gating
    /// ([Phase B3](crate::replication::receiver::handle_replica_batch_with_tracker)).
    ///
    /// Returns the **quorum-committed term** — i.e. the value advanced by
    /// every successful `topology_authority.handle_commit` (an applied
    /// `OP_TOPOLOGY_COMMIT`). All peers converge on the same value after a
    /// commit, so cross-node `OP_REPLICA_BATCH` traffic carries a
    /// cluster_key that matches the receiver's view.
    ///
    /// This is **not** `topology_epoch`: per-node `topology_epoch` values
    /// are seeded from the local member-list snapshot at startup and so
    /// diverge across the cluster, which would reject legitimate
    /// cross-node batches with `ERR_STALE_EPOCH`. Until the first quorum
    /// commit lands, this value is `0` (V1-compat / unknown — gating is
    /// effectively a no-op).
    pub fn local_cluster_key(&self) -> u64 {
        self.committed_cluster_key.load(Ordering::Acquire)
    }

    /// Shared `Arc<AtomicU64>` handle backing the local cluster_key.
    ///
    /// The coordinator passes this clone into the local
    /// [`ReplicationManager`](crate::replication::manager::ReplicationManager)
    /// (so every outbound batch is stamped with the live committed term)
    /// and into the local
    /// [`ReplicationReceiver`](crate::replication::receiver::ReplicationReceiver)
    /// (so the gate sees commits without a setter call). The atomic is
    /// the same instance as `topology_authority.committed_term_shared()`,
    /// so any `handle_commit` advance is visible here lock-free.
    pub fn cluster_key_handle(&self) -> Arc<std::sync::atomic::AtomicU64> {
        self.committed_cluster_key.clone()
    }

    /// Resolved replication ACK policy. None means best-effort (no enforcement).
    pub fn ack_policy(&self) -> Option<crate::replication::manager::AckPolicy> {
        self.repl_ack_policy
    }

    /// Whether replication failures should be tolerated (best_effort degraded mode).
    pub fn is_replication_best_effort(&self) -> bool {
        self.repl_best_effort
    }

    /// Timeout used when waiting for foreground replication ACKs.
    pub fn replication_timeout(&self) -> Duration {
        self.repl_timeout
    }

    /// Whether migration is currently likely to contend with foreground
    /// replication on this node.
    pub fn migration_pressure_active(&self) -> bool {
        let local_pressure = {
            let mgr = self.migration.lock().unwrap();
            mgr.active_count() > 0 || mgr.inbound_count() > 0
        } || self.shard_table.read().pending_handoff_count() > 0;

        let now = now_millis_since_epoch();
        if local_pressure {
            self.last_migration_pressure_ms
                .store(now, Ordering::Relaxed);
            return true;
        }

        let last = self.last_migration_pressure_ms.load(Ordering::Relaxed);
        last != 0 && now.saturating_sub(last) <= MIGRATION_PRESSURE_GRACE.as_millis() as u64
    }

    /// Access the topology authority for handling propose/vote/commit messages.
    pub fn topology_authority(&self) -> &crate::cluster::topology::TopologyAuthority {
        &self.topology_authority
    }

    /// Committed topology term number.
    pub fn committed_topology_term(&self) -> u64 {
        self.topology_authority.committed_term()
    }

    /// Members of the committed topology term.
    pub fn committed_topology_members(&self) -> Vec<NodeId> {
        self.topology_authority.committed_members()
    }

    /// Trigger graceful shard drain (quiesce).
    ///
    /// Recomputes the shard table as if this node has left the cluster,
    /// causing all master shards to migrate to other nodes. Uses
    /// two-phase handoff so the old masters continue serving each shard
    /// until data has been durably migrated to the new owner.
    pub fn quiesce(&self) {
        let addrs = self.node_addrs.read().unwrap();
        let other_members: Vec<NodeId> = addrs
            .keys()
            .filter(|&&id| id != self.self_id)
            .copied()
            .collect();
        let peer_addrs: Vec<SocketAddr> = addrs
            .iter()
            .filter(|&(&id, _)| id != self.self_id)
            .map(|(_, &addr)| addr)
            .collect();
        drop(addrs);

        if other_members.is_empty() {
            tracing::warn!("cluster: cannot quiesce — no other nodes");
            return;
        }

        // Let the normal topology-activation path compute the shard plan and
        // register migrations. Pre-mutating local state here can race the
        // event loop and produce duplicate or stale migration bookkeeping.
        let mut members_for_new_table: Vec<NodeId> = other_members;
        members_for_new_table.sort();

        // Commit a topology change that removes this node from the cluster.
        // This ensures the surviving nodes know we're leaving WITHOUT relying
        // on SWIM failure detection (which is unreliable in containerized
        // environments where killed containers' IPs silently drop UDP).
        let committed = self.topology_authority.committed_term();
        let new_term = committed + 1;
        let new_members = members_for_new_table.clone();
        let commit = crate::cluster::topology::TopologyCommit {
            term: new_term,
            proposer: new_members[0],
            members: new_members.clone(),
            digest: crate::cluster::topology::TopologyTerm::compute_digest(new_term, &new_members),
        };
        // Apply locally first.
        self.topology_authority.handle_commit(&commit);
        self.signal_topology_committed(new_members.clone(), new_term);
        // Broadcast to all peers so they activate the new topology.
        let commit_payload = commit.serialize();
        for &addr in &peer_addrs {
            let _ = send_topology_frame(addr, OP_TOPOLOGY_COMMIT, &commit_payload);
        }
        tracing::info!(
            term = new_term,
            members = new_members.len(),
            "cluster: quiesce: committed topology (excluding self)",
        );
    }

    /// Get a snapshot of active migration progress.
    pub fn migration_status(&self) -> Vec<crate::cluster::migration::MigrationProgress> {
        self.migration.lock().unwrap().active_migrations().to_vec()
    }

    /// Number of shards pending inbound migration data.
    pub fn inbound_pending_count(&self) -> usize {
        self.migration.lock().unwrap().inbound_count()
    }

    /// Snapshot the pending inbound migration entries.
    pub fn pending_inbound_entries(&self) -> Vec<(u16, NodeId)> {
        self.migration.lock().unwrap().pending_inbound_entries()
    }

    /// Number of shards with write fences active.
    pub fn fenced_shard_count(&self) -> usize {
        self.migration.lock().unwrap().fenced_count()
    }

    /// Phase E — additional NodeIds that should also receive replica
    /// batches for `shard` while it is migrating outbound from this node.
    /// Returns an empty Vec when no migration is in flight for the shard.
    ///
    /// Used by [`replicate_all_ops`](crate::server::dispatch) to fan
    /// writes out to BOTH the old replica set (from the shard table) and
    /// the new master / replica destinations (from the migration tracker)
    /// during the migration window. This protects durability when the new
    /// master is promoted before the migration has finished streaming.
    pub fn dual_write_targets_for_shard(&self, shard: u16) -> Vec<NodeId> {
        self.migration
            .lock()
            .unwrap()
            .dual_write_targets_for_shard(shard)
            .to_vec()
    }

    /// Test-only: register an outbound migration task on this cluster's
    /// `MigrationManager` so the dual-write window opens for `shard` with
    /// `dest` as the new master / destination node.
    ///
    /// Used by dispatch-level tests that need to verify replica fan-out
    /// expands during migration without spinning up a full migration
    /// pipeline.
    #[cfg(test)]
    pub(crate) fn test_open_dual_write_window(&self, shard: u16, dest: NodeId) {
        let task = MigrationTask {
            shard,
            from_node: self.self_id,
            to_node: dest,
            is_master: true,
        };
        self.migration.lock().unwrap().start_outbound(
            std::slice::from_ref(&task),
            self.self_id,
            &std::collections::HashSet::new(),
        );
    }

    /// Restore inbound migration state from a previous run.
    ///
    /// Call during startup BEFORE accepting client requests. Shards
    /// that were mid-migration when the node crashed will remain blocked
    /// until the source node re-initiates migration or a topology change
    /// supersedes them.
    pub fn restore_inbound_state(&self) {
        if let Some(ref path) = self.inbound_state_path {
            let data = crate::cluster::migration::load_inbound_state(path);
            if !data.is_empty() {
                let mut mgr = self.migration.lock().unwrap();
                mgr.restore_inbound(&data);
                self.inbound_atomic.load_from(mgr.inbound_bitmap());
                let count = mgr.inbound_count();
                if count > 0 {
                    tracing::info!(
                        count,
                        "cluster: restored pending inbound migrations from disk"
                    );
                }
            }
        }
    }

    /// Restore outbound migration state from a previous run.
    ///
    /// Call during startup alongside `restore_inbound_state()`. The
    /// restored entries inform the coordinator which shards were
    /// mid-migration when the node crashed. The next topology activation
    /// will either resume or re-plan these migrations.
    pub fn restore_outbound_state(&self) {
        if let Some(ref path) = self.outbound_state_path {
            let data = crate::cluster::migration::load_outbound_state(path);
            if !data.is_empty() {
                let mut mgr = self.migration.lock().unwrap();
                mgr.restore_outbound(&data);
                let restored_tasks: Vec<MigrationTask> = mgr
                    .active_migrations()
                    .iter()
                    .filter(|p| {
                        !p.is_complete()
                            && p.state != crate::cluster::migration::MigrationState::Failed
                    })
                    .map(|p| MigrationTask {
                        shard: p.shard,
                        from_node: p.from_node,
                        to_node: p.to_node,
                        is_master: p.is_master,
                    })
                    .collect();
                let count = restored_tasks.len();
                for task in &restored_tasks {
                    mgr.mark_failed(task);
                }
                mgr.cleanup_completed();
                if count > 0 {
                    self.startup_reactivation_needed
                        .store(true, Ordering::Release);
                    tracing::info!(
                        count,
                        "cluster: restored pending outbound migrations from disk; scheduling topology re-activation"
                    );
                }
                crate::cluster::migration::persist_outbound_state(path, &mgr);
                self.fenced_bitmap.load_from(mgr.fenced_bitmap());
                self.migrating_bitmap.clear_all();
                for p in mgr.active_migrations() {
                    if !p.is_complete()
                        && p.state != crate::cluster::migration::MigrationState::Failed
                    {
                        self.migrating_bitmap.set(p.shard);
                    }
                }
            }
        }
    }

    /// Synchronize atomic bitmaps from the MigrationManager state.
    ///
    /// Call after any batch mutation on the migration manager (e.g.,
    /// `start_outbound`, `cleanup_completed`) to keep the lock-free
    /// hot-path bitmaps consistent.
    pub fn sync_migration_bitmaps(&self) {
        let mgr = self.migration.lock().unwrap();
        sync_atomic_migration_bitmaps(
            &mgr,
            &self.fenced_bitmap,
            &self.migrating_bitmap,
            &self.inbound_atomic,
        );
    }

    /// Signal that a topology term was committed (from dispatch or proposer).
    ///
    /// The coordinator event loop picks this up and activates the shard
    /// table with the committed members, triggering any needed migrations.
    pub fn signal_topology_committed(&self, members: Vec<NodeId>, term: u64) {
        let _ = self.topology_commit_tx.send((members, term));
    }

    /// Persist the full topology state to disk.
    ///
    /// Writes `voted_term` and `committed_members` durably so that after a
    /// crash the node does not double-vote or lose track of the committed
    /// topology. Returns `Ok(())` if the fsync'd rename succeeded, or an
    /// `io::Error` otherwise. Safety-critical callers — the
    /// `OP_TOPOLOGY_PROPOSE` and `OP_TOPOLOGY_COMMIT` handlers — MUST check
    /// this result and refuse to reply on failure (see H10: a voter must
    /// never advertise a vote it could lose across a crash).
    ///
    /// When no topology-state path is configured (pure in-memory test
    /// fixtures), returns `Ok(())` — there is nothing to persist and
    /// callers can proceed to reply.
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn persist_topology(&self) -> std::io::Result<()> {
        if let Some(ref path) = self.topology_state_path {
            let peak = self.peak_size.load(Ordering::Relaxed) as u64;
            let inc = self.swim_incarnation.load(Ordering::Relaxed);
            let state = self.topology_authority.persisted_state(peak, inc);
            persist_topology_state(path, &state)
        } else {
            Ok(())
        }
    }

    /// Access the fenced-shards atomic bitmap directly.
    pub fn fenced_bitmap(&self) -> &Arc<crate::cluster::migration::AtomicShardBitmap> {
        &self.fenced_bitmap
    }

    /// Access the inbound-migration atomic bitmap directly.
    pub fn inbound_bitmap(&self) -> &Arc<crate::cluster::migration::AtomicShardBitmap> {
        &self.inbound_atomic
    }

    /// Shut down the cluster.
    ///
    /// Persists the current topology state to disk before stopping so
    /// that on restart the node resumes with the correct committed term
    /// and voted term. Without this, topology changes received between
    /// the last event-driven persist and shutdown would be lost.
    pub fn shutdown(&self) {
        // Best-effort persist at shutdown — the safety-critical persists
        // happen inline in the vote handler (see `OP_TOPOLOGY_PROPOSE` /
        // `OP_TOPOLOGY_COMMIT` in `server/dispatch.rs`). Any error here is
        // already counted in `PERSIST_FAILURES` and logged.
        let _ = self.persist_topology();
        self.shutdown.store(true, Ordering::Relaxed);
        self.swim_shutdown.store(true, Ordering::Relaxed);
    }
}

/// Test-only: construct a [`RunningCluster`] with an explicit
/// `topology_state_path`. Used by H10 tests that need to observe the
/// on-disk persist of `voted_term` / `committed_term`.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn new_test_running_cluster_with_topology_path(
    self_id: NodeId,
    table: ShardTable,
    live_nodes: &[(NodeId, SocketAddr)],
    committed_members: &[NodeId],
    inbound_shards: &[u16],
    migrating_shards: &[u16],
    fenced_shards: &[u16],
    peak_size: usize,
    topology_state_path: Option<std::path::PathBuf>,
) -> RunningCluster {
    let mut c = new_test_running_cluster(
        self_id,
        table,
        live_nodes,
        committed_members,
        inbound_shards,
        migrating_shards,
        fenced_shards,
        peak_size,
    );
    c.topology_state_path = topology_state_path;
    c
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn new_test_running_cluster(
    self_id: NodeId,
    table: ShardTable,
    live_nodes: &[(NodeId, SocketAddr)],
    committed_members: &[NodeId],
    inbound_shards: &[u16],
    migrating_shards: &[u16],
    fenced_shards: &[u16],
    peak_size: usize,
) -> RunningCluster {
    let migration = Arc::new(Mutex::new(MigrationManager::new()));
    {
        let mgr = &mut migration.lock().unwrap();
        for shard in inbound_shards {
            mgr.mark_inbound_active(*shard);
        }
        for shard in fenced_shards {
            mgr.fence_shard(*shard);
        }
    }

    let inbound_atomic = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());
    let fenced_bitmap = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());
    {
        let mgr = migration.lock().unwrap();
        inbound_atomic.load_from(mgr.inbound_bitmap());
        fenced_bitmap.load_from(mgr.fenced_bitmap());
    }

    let migrating_bitmap = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());
    for shard in migrating_shards {
        migrating_bitmap.set(*shard);
    }

    let topology_authority = Arc::new(crate::cluster::topology::TopologyAuthority::new(
        self_id,
        Duration::from_secs(1),
    ));
    let active_topology_members = Arc::new(RwLock::new(if committed_members.is_empty() {
        vec![self_id]
    } else {
        committed_members.to_vec()
    }));
    if !committed_members.is_empty() {
        let commit = crate::cluster::topology::TopologyCommit {
            term: table.version,
            proposer: self_id,
            members: committed_members.to_vec(),
            digest: crate::cluster::topology::TopologyTerm::compute_digest(
                table.version,
                committed_members,
            ),
        };
        let _ = topology_authority.handle_commit(&commit);
    }

    let mut node_addrs = std::collections::HashMap::new();
    for (node, addr) in live_nodes {
        node_addrs.insert(*node, *addr);
    }

    let (topology_commit_tx, _topology_commit_rx) = std::sync::mpsc::channel();

    RunningCluster {
        self_id,
        shard_table: Arc::new(ShardTableLock::new(table.clone())),
        migration,
        node_addrs: Arc::new(RwLock::new(node_addrs)),
        swim_shutdown: Arc::new(AtomicBool::new(true)),
        shutdown: Arc::new(AtomicBool::new(true)),
        peak_size: Arc::new(std::sync::atomic::AtomicUsize::new(peak_size)),
        topology_epoch: Arc::new(std::sync::atomic::AtomicU64::new(table.version)),
        repl_ack_policy: None,
        repl_best_effort: false,
        repl_timeout: Duration::from_secs(3),
        last_migration_pressure_ms: Arc::new(AtomicU64::new(0)),
        committed_cluster_key: topology_authority.committed_term_shared(),
        topology_authority,
        active_topology_members,
        inbound_state_path: None,
        outbound_state_path: None,
        fenced_bitmap,
        inbound_atomic,
        migrating_bitmap,
        topology_commit_tx,
        topology_state_path: None,
        swim_incarnation: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        startup_reactivation_needed: Arc::new(AtomicBool::new(false)),
        _swim_handle: std::thread::spawn(|| {}),
        _event_handle: std::thread::spawn(|| {}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_engine() -> Engine {
        let dev: Arc<dyn crate::device::BlockDevice> =
            Arc::new(crate::device::MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let alloc = crate::allocator::SlotAllocator::new(dev.clone()).unwrap();
        Engine::new(
            dev,
            crate::index::Index::new(1024).unwrap(),
            alloc,
            crate::locks::StripedLocks::new(64),
            crate::index::DahIndex::new(),
            crate::index::UnminedIndex::new(),
        )
    }

    fn tx_key_for_shard(shard: u16, salt: u8) -> TxKey {
        let mut txid = [0u8; 32];
        let bytes = (shard & 0x0FFF).to_le_bytes();
        txid[0] = bytes[0];
        txid[1] = bytes[1];
        txid[2] = salt;
        TxKey { txid }
    }

    fn create_test_record(engine: &Engine, key: TxKey) {
        let utxo_hashes = [[0x44u8; 32]];
        engine
            .create(&crate::ops::create::CreateRequest {
                tx_id: key.txid,
                tx_version: 1,
                locktime: 0,
                fee: 100,
                size_in_bytes: 100,
                extended_size: 0,
                is_coinbase: false,
                spending_height: 0,
                utxo_hashes: &utxo_hashes,
                inputs: None,
                outputs: None,
                inpoints: None,
                is_external: false,
                created_at: 1710000000000,
                block_height: 0,
                mined_block_infos: &[],
                frozen: false,
                conflicting: false,
                locked: false,
                parent_txids: &[],
            })
            .unwrap();
    }

    #[test]
    fn collect_manifest_entries_skips_missing_snapshot_keys() {
        let engine = test_engine();
        let shard = 73u16;
        let live_key = tx_key_for_shard(shard, 1);
        let deleted_key = tx_key_for_shard(shard, 2);
        create_test_record(&engine, live_key);
        create_test_record(&engine, deleted_key);
        engine
            .delete(&crate::ops::remaining::DeleteRequest {
                tx_key: deleted_key,
            })
            .unwrap();

        let entries = collect_manifest_entries(&engine, shard, &[live_key, deleted_key]).unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, live_key);
    }

    #[test]
    fn stream_shard_baseline_skips_missing_snapshot_keys() {
        use std::io::{Read, Write};

        let engine = test_engine();
        let shard = 74u16;
        let live_key = tx_key_for_shard(shard, 1);
        let deleted_key = tx_key_for_shard(shard, 2);
        create_test_record(&engine, live_key);
        create_test_record(&engine, deleted_key);
        engine
            .delete(&crate::ops::remaining::DeleteRequest {
                tx_key: deleted_key,
            })
            .unwrap();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let receiver = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut header = [0u8; 4];
            stream.read_exact(&mut header).unwrap();
            let payload_len = u32::from_le_bytes(header) as usize;
            let mut body = vec![0u8; payload_len];
            stream.read_exact(&mut body).unwrap();

            let mut frame_bytes = header.to_vec();
            frame_bytes.extend_from_slice(&body);
            let (request, _) = crate::protocol::frame::RequestFrame::decode(&frame_bytes).unwrap();
            let batch =
                crate::replication::protocol::ReplicaBatch::deserialize(&request.payload).unwrap();

            let response = crate::protocol::frame::ResponseFrame {
                request_id: request.request_id,
                status: crate::protocol::opcodes::STATUS_OK,
                payload: crate::replication::protocol::ReplicaAck::Ok {
                    through_sequence: 0,
                }
                .serialize(),
            };
            stream.write_all(&response.encode()).unwrap();
            (request, batch)
        });

        let mut stream = std::net::TcpStream::connect(addr).unwrap();
        let task = MigrationTask {
            shard,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        stream_shard_baseline(
            &task,
            &[&live_key, &deleted_key],
            &engine,
            &mut stream,
            64,
            /* cluster_key */ 0,
        )
        .unwrap();

        let (request, batch) = receiver.join().unwrap();
        assert_eq!(request.op_code, crate::protocol::opcodes::OP_REPLICA_BATCH);
        assert_eq!(
            batch
                .ops
                .iter()
                .filter(|op| matches!(op, crate::replication::protocol::ReplicaOp::Create { .. }))
                .count(),
            1
        );
        assert!(batch.ops.iter().any(|op| matches!(
            op,
            crate::replication::protocol::ReplicaOp::Create { tx_key, .. } if *tx_key == live_key
        )));
    }

    #[test]
    fn stale_migration_failure_does_not_clear_new_epoch_state() {
        let old_table = ShardTable::compute_with_epoch(&[NodeId(1), NodeId(2)], 2, 1);
        let new_table = ShardTable::compute_with_epoch(&[NodeId(2), NodeId(1)], 2, 2);
        let shard = (0..NUM_SHARDS as u16)
            .find(|&s| {
                old_table.target_assignment(s).master != new_table.target_assignment(s).master
            })
            .expect("test topology should move at least one shard");
        let mut table = old_table;
        table.begin_handoff_with(&new_table, |_| true);
        let pending_before = table.pending_handoff_count();
        let effective_before = table.effective_assignment(shard).master;
        let shard_table = Arc::new(ShardTableLock::new(table));

        let migration = Arc::new(Mutex::new(MigrationManager::new()));
        let fenced_bm = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());
        let migrating_bm = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());
        {
            let mut mgr = migration.lock().unwrap();
            mgr.fence_shard(shard);
        }
        fenced_bm.set(shard);
        migrating_bm.set(shard);

        let task = MigrationTask {
            shard,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        assert!(!fail_migration_task_current_epoch(
            &migration,
            &shard_table,
            &fenced_bm,
            &migrating_bm,
            &task,
            1,
            true,
        ));

        assert!(migration.lock().unwrap().is_shard_fenced(shard));
        assert!(fenced_bm.test(shard));
        assert!(migrating_bm.test(shard));
        let table = shard_table.read();
        assert_eq!(table.pending_handoff_count(), pending_before);
        assert_eq!(table.effective_assignment(shard).master, effective_before);
    }

    #[test]
    fn current_migration_failure_marks_failed_and_clears_bitmaps() {
        let shard = 7;
        let shard_table = Arc::new(ShardTableLock::new(ShardTable::compute_with_epoch(
            &[NodeId(1), NodeId(2)],
            2,
            3,
        )));
        let migration = Arc::new(Mutex::new(MigrationManager::new()));
        let fenced_bm = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());
        let migrating_bm = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());
        let task = MigrationTask {
            shard,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        {
            let mut mgr = migration.lock().unwrap();
            mgr.start_outbound(
                std::slice::from_ref(&task),
                NodeId(1),
                &std::collections::HashSet::new(),
            );
            mgr.mark_fenced(&task, 10);
        }
        fenced_bm.set(shard);
        migrating_bm.set(shard);

        assert!(fail_migration_task_current_epoch(
            &migration,
            &shard_table,
            &fenced_bm,
            &migrating_bm,
            &task,
            3,
            false,
        ));

        let mgr = migration.lock().unwrap();
        assert_eq!(mgr.failed_count(), 1);
        assert!(!mgr.is_shard_fenced(shard));
        assert!(!fenced_bm.test(shard));
        assert!(!migrating_bm.test(shard));
    }

    #[test]
    fn running_cluster_reports_inbound_migration_pressure() {
        let table = ShardTable::compute_with_epoch(&[NodeId(1), NodeId(2)], 2, 1);
        let addr: SocketAddr = "127.0.0.1:3300".parse().unwrap();
        let cluster = new_test_running_cluster(
            NodeId(1),
            table,
            &[
                (NodeId(1), addr),
                (NodeId(2), "127.0.0.1:3301".parse().unwrap()),
            ],
            &[NodeId(1), NodeId(2)],
            &[7],
            &[],
            &[],
            2,
        );

        assert!(cluster.migration_pressure_active());
        assert_eq!(cluster.replication_timeout(), Duration::from_secs(3));
    }

    #[test]
    fn running_cluster_batch_inbound_completion_persists_remaining_state() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir.path().join("inbound.state");
        let table = ShardTable::compute_with_epoch(&[NodeId(1), NodeId(2), NodeId(3)], 2, 1);
        let mut cluster = new_test_running_cluster(
            NodeId(1),
            table,
            &[
                (NodeId(1), "127.0.0.1:3300".parse().unwrap()),
                (NodeId(2), "127.0.0.1:3301".parse().unwrap()),
                (NodeId(3), "127.0.0.1:3302".parse().unwrap()),
            ],
            &[NodeId(1), NodeId(2), NodeId(3)],
            &[],
            &[],
            &[],
            3,
        );
        cluster.inbound_state_path = Some(path.clone());
        cluster.register_test_inbound_from_source(10, NodeId(2));
        cluster.register_test_inbound_from_source(11, NodeId(2));
        cluster.register_test_inbound_from_source(12, NodeId(3));

        cluster.mark_inbound_complete_many_from_source(&[10, 11, 12], NodeId(2));

        assert!(!cluster.has_pending_inbound_shard(10));
        assert!(!cluster.has_pending_inbound_shard(11));
        assert!(
            cluster.has_pending_inbound_shard(12),
            "batch completion from node2 must not clear node3's inbound task"
        );

        let data = crate::cluster::migration::load_inbound_state(&path);
        let mut restored = MigrationManager::new();
        restored.restore_inbound(&data);
        assert!(!restored.has_pending_inbound(10));
        assert!(!restored.has_pending_inbound(11));
        assert!(restored.has_pending_inbound(12));
        assert_eq!(restored.inbound_count(), 1);
    }

    #[test]
    fn running_cluster_keeps_migration_pressure_grace_after_local_clear() {
        let table = ShardTable::compute_with_epoch(&[NodeId(1), NodeId(2)], 2, 1);
        let cluster = new_test_running_cluster(
            NodeId(1),
            table,
            &[
                (NodeId(1), "127.0.0.1:3300".parse().unwrap()),
                (NodeId(2), "127.0.0.1:3301".parse().unwrap()),
            ],
            &[NodeId(1), NodeId(2)],
            &[7],
            &[],
            &[],
            2,
        );

        assert!(cluster.migration_pressure_active());
        cluster.mark_inbound_complete(7);
        assert!(
            cluster.migration_pressure_active(),
            "foreground replication should keep the migration timeout briefly after local inbound work clears"
        );

        let expired = now_millis_since_epoch()
            .saturating_sub(MIGRATION_PRESSURE_GRACE.as_millis() as u64 + 1);
        cluster
            .last_migration_pressure_ms
            .store(expired, Ordering::Relaxed);
        assert!(!cluster.migration_pressure_active());
    }

    #[test]
    fn migration_stream_timeout_has_high_parallelism_floor() {
        assert_eq!(migration_stream_timeout(1), Duration::from_secs(60));
        assert_eq!(migration_stream_timeout(500), Duration::from_secs(60));
        assert_eq!(migration_stream_timeout(10_000), Duration::from_secs(60));
    }

    #[test]
    fn already_serving_migration_still_streams_snapshot_data() {
        let replica_task = MigrationTask {
            shard: 7,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: false,
        };
        let master_task = MigrationTask {
            is_master: true,
            ..replica_task.clone()
        };

        assert!(
            !should_skip_already_serving_migration(&replica_task, ShardHandoff::ServingNew, true),
            "replica migrations may be ServingNew but still need snapshot data copied for RF"
        );
        assert!(
            !should_skip_already_serving_migration(&master_task, ShardHandoff::ServingNew, true),
            "repair/backfill master migrations may be ServingNew but still need payloads copied"
        );
        assert!(should_skip_already_serving_migration(
            &replica_task,
            ShardHandoff::ServingNew,
            false
        ));
        assert!(!should_skip_already_serving_migration(
            &master_task,
            ShardHandoff::Copying,
            true
        ));
    }

    #[test]
    fn already_serving_skip_is_task_local_not_shard_wide() {
        let shard = 7u16;
        let table = ShardTable::compute_with_epoch(&[NodeId(1), NodeId(2), NodeId(3)], 2, 9);
        let master_task = MigrationTask {
            shard,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        let replica_task = MigrationTask {
            shard,
            from_node: NodeId(1),
            to_node: NodeId(3),
            is_master: false,
        };
        let data_shards = std::collections::HashSet::from([shard]);

        let (active, skipped) = split_already_serving_migration_tasks(
            vec![master_task.clone(), replica_task.clone()],
            &table,
            &data_shards,
        );

        assert!(skipped.is_empty());
        assert_eq!(
            active,
            vec![master_task, replica_task],
            "all data-bearing tasks for an already-serving shard must still stream for repair"
        );
    }

    #[test]
    fn migration_delta_ops_include_windowed_same_shard_mutations() {
        let engine = test_engine();
        let shard = 77u16;
        let same_shard = tx_key_for_shard(shard, 1);
        let other_shard = tx_key_for_shard(shard + 1, 2);
        let redo_dev: Arc<dyn crate::device::BlockDevice> =
            Arc::new(crate::device::MemoryDevice::new(1024 * 1024, 4096).unwrap());
        let redo = Arc::new(ParkingMutex::new(
            RedoLog::open(redo_dev, 0, 1024 * 1024).unwrap(),
        ));

        let snapshot_seq = redo.lock().current_sequence();
        redo.lock()
            .append_and_flush(crate::redo::RedoOp::SetLocked {
                tx_key: same_shard,
                value: true,
            })
            .unwrap();
        redo.lock()
            .append_and_flush(crate::redo::RedoOp::SetLocked {
                tx_key: other_shard,
                value: false,
            })
            .unwrap();
        let fence_seq = redo.lock().current_sequence();
        let redo_log = Some(redo);

        let ops = collect_migration_delta_ops(&redo_log, snapshot_seq, fence_seq, shard, &engine)
            .expect("delta collection should succeed");
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            crate::replication::protocol::ReplicaOp::SetLocked {
                tx_key,
                value,
                master_generation,
            } => {
                assert_eq!(*tx_key, same_shard);
                assert!(*value);
                assert_eq!(*master_generation, 0);
            }
            other => panic!("expected SetLocked delta, got {other:?}"),
        }
    }

    #[test]
    fn surviving_replica_source_keeps_shard_in_handoff_when_old_master_is_dead() {
        let outbound_source_shards: std::collections::HashSet<u16> = [42u16].into_iter().collect();

        assert!(should_begin_handoff_for_shard(
            42,
            NodeId(1),
            NodeId(2),
            true,
            false,
            &outbound_source_shards,
        ));
        assert!(!should_begin_handoff_for_shard(
            43,
            NodeId(1),
            NodeId(2),
            true,
            false,
            &outbound_source_shards,
        ));
        assert!(should_begin_handoff_for_shard(
            42,
            NodeId(1),
            NodeId(2),
            false,
            false,
            &outbound_source_shards,
        ));
        assert!(!should_begin_handoff_for_shard(
            44,
            NodeId(1),
            NodeId(2),
            true,
            false,
            &outbound_source_shards,
        ));
    }

    #[test]
    fn replica_holder_does_not_enter_handoff_when_old_master_is_still_serving() {
        let outbound_source_shards = std::collections::HashSet::new();

        assert!(
            !should_begin_handoff_for_shard(
                42,
                NodeId(3),
                NodeId(1),
                true,
                true,
                &outbound_source_shards,
            ),
            "a replica holder with local data but no outbound migration task must not stay in Copying; \
             only the serving source node should wait for commit"
        );
    }

    #[test]
    fn activate_topology_uses_committed_membership_for_old_master_liveness() {
        let committed_members = vec![NodeId(1), NodeId(2)];
        let live_addrs = std::collections::HashMap::from([
            (NodeId(1), "127.0.0.1:11011".parse().unwrap()),
            (NodeId(2), "127.0.0.1:11012".parse().unwrap()),
            // Node 3 is still known to SWIM/address bookkeeping.
            (NodeId(3), "127.0.0.1:11013".parse().unwrap()),
        ]);

        assert!(
            old_master_available_for_handoff(NodeId(2), &committed_members, &live_addrs),
            "a committed node with a known address should still count as an available old master"
        );
        assert!(
            !old_master_available_for_handoff(NodeId(3), &committed_members, &live_addrs),
            "nodes that are no longer in the committed topology must not keep replicas stuck in Copying"
        );
    }

    #[test]
    fn topology_commit_duplicate_guard_skips_same_term_same_members() {
        let active_members = vec![NodeId(1), NodeId(2), NodeId(3)];
        let commit_members = active_members.clone();

        assert!(topology_commit_already_activated(
            9,
            9,
            &active_members,
            &commit_members,
        ));
    }

    #[test]
    fn topology_commit_duplicate_guard_allows_same_term_changed_members() {
        let active_members = vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
        let commit_members = vec![NodeId(1), NodeId(2), NodeId(3)];

        assert!(
            !topology_commit_already_activated(9, 9, &active_members, &commit_members),
            "quiesce may reuse the latest table term with a changed member set; it must still activate"
        );
    }

    #[test]
    fn topology_commit_duplicate_guard_skips_older_term_even_with_changed_members() {
        let active_members = vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
        let commit_members = vec![NodeId(1), NodeId(2), NodeId(3)];

        assert!(topology_commit_already_activated(
            8,
            9,
            &active_members,
            &commit_members,
        ));
    }

    #[test]
    fn migration_workers_only_preserve_for_same_epoch() {
        assert!(migration_workers_can_be_preserved(7, 7));
        assert!(
            !migration_workers_can_be_preserved(7, 8),
            "workers from an older epoch self-abort and must be respawned for the new epoch"
        );
    }

    #[test]
    fn topology_activation_tasks_include_local_holder_backfill() {
        let new_table = ShardTable::compute_with_epoch(&[NodeId(2), NodeId(3)], 2, 11);
        let shard = 42u16;
        let populated = std::collections::HashSet::from([shard]);
        let target = new_table.target_assignment(shard);
        let existing = MigrationTask {
            shard,
            from_node: NodeId(1),
            to_node: target.master,
            is_master: true,
        };

        let tasks = build_topology_activation_tasks(
            std::slice::from_ref(&existing),
            &[],
            &populated,
            &new_table,
            NodeId(1),
        );
        let mut expected = vec![target.master];
        expected.extend(target.replicas.iter().copied());
        expected.sort_by_key(|node| node.0);
        expected.dedup();

        let mut actual: Vec<NodeId> = tasks.iter().map(|t| t.to_node).collect();
        actual.sort_by_key(|node| node.0);
        actual.dedup();

        assert_eq!(actual, expected);
        assert_eq!(
            tasks
                .iter()
                .filter(|t| t.shard == shard
                    && t.from_node == NodeId(1)
                    && t.to_node == target.master)
                .count(),
            1,
            "backfill composition must not duplicate a normal migration task"
        );
        assert!(tasks.contains(&existing));
    }

    #[test]
    fn local_holder_backfill_tasks_stream_to_all_target_holders() {
        let new_table = ShardTable::compute_with_epoch(&[NodeId(2), NodeId(3)], 2, 11);
        let shard = 42u16;
        let populated = std::collections::HashSet::from([shard]);
        let mut tasks = Vec::new();

        add_local_holder_backfill_tasks(&mut tasks, &populated, &new_table, NodeId(1));

        let target = new_table.target_assignment(shard);
        let mut expected = vec![target.master];
        expected.extend(target.replicas.iter().copied());
        expected.sort_by_key(|node| node.0);
        expected.dedup();

        let mut actual: Vec<NodeId> = tasks.iter().map(|t| t.to_node).collect();
        actual.sort_by_key(|node| node.0);

        assert_eq!(actual, expected);
        assert!(tasks.iter().all(|t| t.shard == shard));
        assert!(tasks.iter().all(|t| t.from_node == NodeId(1)));
        assert!(
            tasks
                .iter()
                .any(|t| t.to_node == target.master && t.is_master)
        );
        assert!(
            tasks
                .iter()
                .all(|t| t.to_node == target.master || !t.is_master)
        );
    }

    #[test]
    fn local_holder_backfill_tasks_repair_other_holder_when_still_owned_locally() {
        let new_table = ShardTable::compute_with_epoch(&[NodeId(1), NodeId(2), NodeId(3)], 2, 11);
        let shard = (0..NUM_SHARDS as u16)
            .find(|&s| {
                let target = new_table.target_assignment(s);
                target.master == NodeId(1) && !target.replicas.is_empty()
            })
            .expect("node1 should master some target shard");
        let populated = std::collections::HashSet::from([shard]);
        let mut tasks = Vec::new();

        add_local_holder_backfill_tasks(&mut tasks, &populated, &new_table, NodeId(1));

        let target = new_table.target_assignment(shard);
        let mut expected = target.replicas.clone();
        expected.sort_by_key(|node| node.0);
        let mut actual: Vec<NodeId> = tasks.iter().map(|t| t.to_node).collect();
        actual.sort_by_key(|node| node.0);

        assert_eq!(actual, expected);
        assert!(tasks.iter().all(|t| !t.is_master));
    }

    #[test]
    fn per_shard_orphan_cleanup_deletes_settled_unowned_shard() {
        let old_table = ShardTable::compute_with_epoch(&[NodeId(1), NodeId(2), NodeId(3)], 2, 10);
        let new_table = ShardTable::compute_with_epoch(&[NodeId(2), NodeId(3)], 2, 11);
        let shard = (0..NUM_SHARDS as u16)
            .find(|&s| {
                let old = old_table.target_assignment(s);
                old.master == NodeId(1) || old.replicas.contains(&NodeId(1))
            })
            .expect("node1 should hold at least one shard before removal");
        let engine = Arc::new(test_engine());
        let key = tx_key_for_shard(shard, 44);
        create_test_record(&engine, key);
        assert_eq!(engine.shard_record_count(shard), 1);

        let shard_table = Arc::new(ShardTableLock::new(new_table.clone()));
        let migration = Arc::new(Mutex::new(MigrationManager::new()));

        cleanup_orphaned_shard_if_settled(
            NodeId(1),
            &engine,
            &shard_table,
            &migration,
            shard,
            new_table.version,
        );

        assert_eq!(engine.shard_record_count(shard), 0);
    }

    #[test]
    fn per_shard_orphan_cleanup_waits_for_active_same_shard_task() {
        let old_table = ShardTable::compute_with_epoch(&[NodeId(1), NodeId(2), NodeId(3)], 2, 10);
        let new_table = ShardTable::compute_with_epoch(&[NodeId(2), NodeId(3)], 2, 11);
        let shard = (0..NUM_SHARDS as u16)
            .find(|&s| {
                let old = old_table.target_assignment(s);
                old.master == NodeId(1) || old.replicas.contains(&NodeId(1))
            })
            .expect("node1 should hold at least one shard before removal");
        let engine = Arc::new(test_engine());
        let key = tx_key_for_shard(shard, 45);
        create_test_record(&engine, key);

        let shard_table = Arc::new(ShardTableLock::new(new_table.clone()));
        let migration = Arc::new(Mutex::new(MigrationManager::new()));
        let task = MigrationTask {
            shard,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        migration.lock().unwrap().start_outbound(
            &[task],
            NodeId(1),
            &std::collections::HashSet::from([shard]),
        );

        cleanup_orphaned_shard_if_settled(
            NodeId(1),
            &engine,
            &shard_table,
            &migration,
            shard,
            new_table.version,
        );

        assert_eq!(engine.shard_record_count(shard), 1);
    }

    #[test]
    fn per_shard_orphan_cleanup_waits_for_any_active_epoch_work() {
        let old_table = ShardTable::compute_with_epoch(&[NodeId(1), NodeId(2), NodeId(3)], 2, 10);
        let new_table = ShardTable::compute_with_epoch(&[NodeId(2), NodeId(3)], 2, 11);
        let shard = (0..NUM_SHARDS as u16)
            .find(|&s| {
                let old = old_table.target_assignment(s);
                old.master == NodeId(1) || old.replicas.contains(&NodeId(1))
            })
            .expect("node1 should hold at least one shard before removal");
        let other_shard = (0..NUM_SHARDS as u16)
            .find(|&s| s != shard)
            .expect("expected a distinct shard");
        let engine = Arc::new(test_engine());
        let key = tx_key_for_shard(shard, 46);
        create_test_record(&engine, key);

        let shard_table = Arc::new(ShardTableLock::new(new_table.clone()));
        let migration = Arc::new(Mutex::new(MigrationManager::new()));
        let task = MigrationTask {
            shard: other_shard,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        migration.lock().unwrap().start_outbound(
            std::slice::from_ref(&task),
            NodeId(1),
            &std::collections::HashSet::from([other_shard]),
        );

        cleanup_orphaned_shard_if_settled(
            NodeId(1),
            &engine,
            &shard_table,
            &migration,
            shard,
            new_table.version,
        );

        assert_eq!(engine.shard_record_count(shard), 1);
    }

    #[test]
    fn quiesce_signals_topology_activation_without_pre_mutating_local_state() {
        let members = vec![NodeId(1), NodeId(2), NodeId(3)];
        let table = ShardTable::compute_with_epoch(&members, 2, 7);
        let live_nodes = [
            (NodeId(1), "127.0.0.1:11001".parse().unwrap()),
            (NodeId(2), "127.0.0.1:11002".parse().unwrap()),
            (NodeId(3), "127.0.0.1:11003".parse().unwrap()),
        ];
        let mut cluster = new_test_running_cluster(
            NodeId(1),
            table.clone(),
            &live_nodes,
            &members,
            &[],
            &[],
            &[],
            members.len(),
        );
        let (topology_commit_tx, topology_commit_rx) = std::sync::mpsc::channel();
        cluster.topology_commit_tx = topology_commit_tx;

        cluster.quiesce();

        let (new_members, new_term) = topology_commit_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("quiesce must signal the committed topology for event-loop activation");
        assert_eq!(new_members, vec![NodeId(2), NodeId(3)]);
        assert_eq!(new_term, table.version + 1);
        assert_eq!(
            cluster.committed_topology_members(),
            vec![NodeId(2), NodeId(3)]
        );
        assert_eq!(cluster.committed_topology_term(), table.version + 1);
        assert_eq!(
            cluster.shard_table.read().version,
            table.version,
            "quiesce should leave shard-table activation to the event loop"
        );
        assert_eq!(
            cluster.shard_table.read().pending_handoff_count(),
            0,
            "quiesce should not begin handoff synchronously"
        );
        assert_eq!(
            cluster.migration.lock().unwrap().active_count(),
            0,
            "quiesce should not pre-register outbound migrations outside activate_topology"
        );
        assert_eq!(cluster.inbound_pending_count(), 0);
        assert_eq!(cluster.fenced_shard_count(), 0);
    }

    #[test]
    fn empty_shard_completion_failure_rolls_back_master_handoff() {
        let old_members = vec![NodeId(1), NodeId(2)];
        let new_members = vec![NodeId(1), NodeId(2), NodeId(3)];
        let old_table = ShardTable::compute_with_epoch(&old_members, 2, 1);
        let new_table = ShardTable::compute_with_epoch(&new_members, 2, 2);
        let shard = (0..NUM_SHARDS as u16)
            .find(|&s| {
                old_table.target_assignment(s).master != new_table.target_assignment(s).master
            })
            .expect("expected a shard whose master moves");
        let old_master = old_table.target_assignment(shard).master;
        let new_master = new_table.target_assignment(shard).master;
        let task = MigrationTask {
            shard,
            from_node: old_master,
            to_node: new_master,
            is_master: true,
        };

        let mut handoff = old_table.clone();
        handoff.begin_handoff_with(&new_table, |s| s == shard);
        let shard_table = Arc::new(ShardTableLock::new(handoff));

        let dev: Arc<dyn crate::device::BlockDevice> =
            Arc::new(crate::device::MemoryDevice::new(4 * 1024 * 1024, 4096).unwrap());
        let alloc = crate::allocator::SlotAllocator::new(dev.clone()).unwrap();
        let index = crate::index::Index::new(128).unwrap();
        let engine = Arc::new(crate::ops::engine::Engine::new(
            dev,
            index,
            alloc,
            crate::locks::StripedLocks::new(64),
            crate::index::DahIndex::new(),
            crate::index::UnminedIndex::new(),
        ));

        let migration = Arc::new(Mutex::new(MigrationManager::new()));
        migration.lock().unwrap().start_outbound(
            std::slice::from_ref(&task),
            old_master,
            &std::collections::HashSet::new(),
        );

        let fenced_bm = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());
        let migrating_bm = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());
        migrating_bm.set(shard);
        let inbound_bm = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let target_addr = listener.local_addr().unwrap();
        drop(listener);

        run_migration_batch(
            vec![task],
            Some(target_addr),
            &[],
            engine,
            &migration,
            &shard_table,
            &None,
            new_table.version,
            1,
            100,
            fenced_bm,
            migrating_bm,
            inbound_bm,
            old_master,
        );

        let table = shard_table.read();
        assert_eq!(
            table.target_assignment(shard).master,
            old_master,
            "the source must not commit a master handoff when the completion signal cannot be delivered"
        );
    }

    #[test]
    fn empty_shard_completion_retries_until_target_is_ready() {
        let old_members = vec![NodeId(1), NodeId(2)];
        let new_members = vec![NodeId(1), NodeId(2), NodeId(3)];
        let old_table = ShardTable::compute_with_epoch(&old_members, 2, 1);
        let new_table = ShardTable::compute_with_epoch(&new_members, 2, 2);
        let shard = (0..NUM_SHARDS as u16)
            .find(|&s| {
                old_table.target_assignment(s).master != new_table.target_assignment(s).master
            })
            .expect("expected a shard whose master moves");
        let old_master = old_table.target_assignment(shard).master;
        let new_master = new_table.target_assignment(shard).master;
        let task = MigrationTask {
            shard,
            from_node: old_master,
            to_node: new_master,
            is_master: true,
        };

        let mut handoff = old_table.clone();
        handoff.begin_handoff_with(&new_table, |s| s == shard);
        assert_eq!(
            handoff.shard_handoff_state(shard),
            ShardHandoff::Copying,
            "the shard must take the empty-shard handoff path"
        );
        let shard_table = Arc::new(ShardTableLock::new(handoff));

        let dev: Arc<dyn crate::device::BlockDevice> =
            Arc::new(crate::device::MemoryDevice::new(4 * 1024 * 1024, 4096).unwrap());
        let alloc = crate::allocator::SlotAllocator::new(dev.clone()).unwrap();
        let index = crate::index::Index::new(128).unwrap();
        let engine = Arc::new(crate::ops::engine::Engine::new(
            dev,
            index,
            alloc,
            crate::locks::StripedLocks::new(64),
            crate::index::DahIndex::new(),
            crate::index::UnminedIndex::new(),
        ));

        let migration = Arc::new(Mutex::new(MigrationManager::new()));
        migration.lock().unwrap().start_outbound(
            std::slice::from_ref(&task),
            old_master,
            &std::collections::HashSet::new(),
        );

        let fenced_bm = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());
        let migrating_bm = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());
        migrating_bm.set(shard);
        let inbound_bm = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let target_addr = listener.local_addr().unwrap();
        drop(listener);
        let (request_tx, request_rx) = std::sync::mpsc::channel();
        let receiver = std::thread::spawn(move || {
            use std::io::{Read, Write};

            // Fresh Docker nodes can take a couple of seconds before they
            // accept migration-complete handshakes during cluster bootstrap.
            std::thread::sleep(Duration::from_millis(2500));
            let listener = std::net::TcpListener::bind(target_addr).unwrap();
            let (mut stream, _) = listener.accept().unwrap();

            let mut header = [0u8; 4];
            stream.read_exact(&mut header).unwrap();
            let payload_len = u32::from_le_bytes(header) as usize;
            let mut rest = vec![0u8; payload_len];
            stream.read_exact(&mut rest).unwrap();

            let mut frame_bytes = header.to_vec();
            frame_bytes.extend_from_slice(&rest);
            let (request, _) = RequestFrame::decode(&frame_bytes).unwrap();
            request_tx
                .send((request.op_code, request.request_id))
                .unwrap();

            let response = ResponseFrame {
                request_id: request.request_id,
                status: STATUS_OK,
                payload: Vec::new(),
            };
            stream.write_all(&response.encode()).unwrap();
        });

        run_migration_batch(
            vec![task.clone()],
            Some(target_addr),
            &[],
            engine,
            &migration,
            &shard_table,
            &None,
            new_table.version,
            1,
            100,
            fenced_bm,
            migrating_bm,
            inbound_bm,
            old_master,
        );

        let (op, _req_id) = request_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(
            op == OP_MIGRATION_COMPLETE || op == OP_MIGRATION_BATCH_COMPLETE,
            "empty-shard migrations must retry the completion handshake until the target can acknowledge it (got op {op})"
        );
        receiver.join().unwrap();
    }

    #[test]
    #[ignore] // TODO: rewrite for pipelined migration flow
    fn failed_data_migration_sends_abort_completion_handshake() {
        let old_members = vec![NodeId(1), NodeId(2)];
        let new_members = vec![NodeId(1), NodeId(2), NodeId(3)];
        let old_table = ShardTable::compute_with_epoch(&old_members, 2, 1);
        let new_table = ShardTable::compute_with_epoch(&new_members, 2, 2);
        let shard = (0..NUM_SHARDS as u16)
            .find(|&s| {
                old_table.target_assignment(s).master != new_table.target_assignment(s).master
            })
            .expect("expected a shard whose master moves");
        let old_master = old_table.target_assignment(shard).master;
        let new_master = new_table.target_assignment(shard).master;
        let task = MigrationTask {
            shard,
            from_node: old_master,
            to_node: new_master,
            is_master: true,
        };

        let mut handoff = old_table.clone();
        handoff.begin_handoff_with(&new_table, |s| s == shard);
        assert_eq!(
            handoff.shard_handoff_state(shard),
            ShardHandoff::Copying,
            "the shard must take the data-migration path"
        );
        let shard_table = Arc::new(ShardTableLock::new(handoff));

        let dev: Arc<dyn crate::device::BlockDevice> =
            Arc::new(crate::device::MemoryDevice::new(4 * 1024 * 1024, 4096).unwrap());
        let alloc = crate::allocator::SlotAllocator::new(dev.clone()).unwrap();
        let index = crate::index::Index::new(128).unwrap();
        let engine = Arc::new(crate::ops::engine::Engine::new(
            dev,
            index,
            alloc,
            crate::locks::StripedLocks::new(64),
            crate::index::DahIndex::new(),
            crate::index::UnminedIndex::new(),
        ));

        let tx_id = {
            let mut nonce = 0u64;
            loop {
                let mut tx_id = [0u8; 32];
                tx_id[..8].copy_from_slice(&nonce.to_le_bytes());
                if ShardTable::shard_for_key(&TxKey { txid: tx_id }) == shard {
                    break tx_id;
                }
                nonce += 1;
            }
        };
        let key = TxKey { txid: tx_id };
        let utxo_hashes = [[0x33u8; 32]];
        engine
            .create(&crate::ops::create::CreateRequest {
                tx_id,
                tx_version: 1,
                locktime: 0,
                fee: 100,
                size_in_bytes: 100,
                extended_size: 0,
                is_coinbase: false,
                spending_height: 0,
                utxo_hashes: &utxo_hashes,
                inputs: None,
                outputs: None,
                inpoints: None,
                is_external: false,
                created_at: 1710000000000,
                block_height: 0,
                mined_block_infos: &[],
                frozen: false,
                conflicting: false,
                locked: false,
                parent_txids: &[],
            })
            .unwrap();
        assert_eq!(engine.shard_record_count(shard), 1);

        let migration = Arc::new(Mutex::new(MigrationManager::new()));
        let populated: std::collections::HashSet<u16> = [shard].into_iter().collect();
        migration.lock().unwrap().start_outbound(
            std::slice::from_ref(&task),
            old_master,
            &populated,
        );

        let fenced_bm = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());
        let migrating_bm = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());
        migrating_bm.set(shard);
        let inbound_bm = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let target_addr = listener.local_addr().unwrap();
        let (request_tx, request_rx) = std::sync::mpsc::channel();
        let receiver = std::thread::spawn(move || {
            use std::io::{ErrorKind, Read, Write};

            listener.set_nonblocking(true).unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while std::time::Instant::now() < deadline {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let mut header = [0u8; 4];
                        stream.read_exact(&mut header).unwrap();
                        let payload_len = u32::from_le_bytes(header) as usize;
                        let mut rest = vec![0u8; payload_len];
                        stream.read_exact(&mut rest).unwrap();

                        let mut frame_bytes = header.to_vec();
                        frame_bytes.extend_from_slice(&rest);
                        let (request, _) = RequestFrame::decode(&frame_bytes).unwrap();
                        request_tx
                            .send((request.op_code, request.request_id))
                            .unwrap();

                        if request.op_code == OP_MIGRATION_COMPLETE {
                            let response = ResponseFrame {
                                request_id: request.request_id,
                                status: STATUS_OK,
                                payload: Vec::new(),
                            };
                            stream.write_all(&response.encode()).unwrap();
                            return;
                        }
                        // Drop the connection without replying so the source
                        // treats the shard migration as failed after retries.
                    }
                    Err(e) if e.kind() == ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(e) => panic!("accept failed: {e}"),
                }
            }
        });

        run_migration_batch(
            vec![task.clone()],
            Some(target_addr),
            &[key],
            engine,
            &migration,
            &shard_table,
            &None,
            new_table.version,
            1,
            100,
            fenced_bm,
            migrating_bm,
            inbound_bm,
            old_master,
        );

        let mut seen = Vec::new();
        while let Ok(request) = request_rx.recv_timeout(Duration::from_millis(200)) {
            seen.push(request);
            if request.0 == OP_MIGRATION_COMPLETE {
                break;
            }
        }
        assert!(
            seen.contains(&(OP_MIGRATION_COMPLETE, shard as u64)),
            "when a data migration gives up after partial streaming, the source must send a final completion/abort handshake to clear the target's provisional inbound state; saw {seen:?}"
        );
        receiver.join().unwrap();
    }

    #[test]
    fn data_migration_verifies_manifest_before_batched_completion() {
        let old_members = vec![NodeId(1), NodeId(2)];
        let new_members = vec![NodeId(1), NodeId(2), NodeId(3)];
        let old_table = ShardTable::compute_with_epoch(&old_members, 2, 1);
        let new_table = ShardTable::compute_with_epoch(&new_members, 2, 2);
        let shard = (0..NUM_SHARDS as u16)
            .find(|&s| {
                old_table.target_assignment(s).master != new_table.target_assignment(s).master
            })
            .expect("expected a shard whose master moves");
        let old_master = old_table.target_assignment(shard).master;
        let new_master = new_table.target_assignment(shard).master;
        let task = MigrationTask {
            shard,
            from_node: old_master,
            to_node: new_master,
            is_master: true,
        };

        let mut handoff = old_table.clone();
        handoff.begin_handoff_with(&new_table, |s| s == shard);
        let shard_table = Arc::new(ShardTableLock::new(handoff));

        let dev: Arc<dyn crate::device::BlockDevice> =
            Arc::new(crate::device::MemoryDevice::new(4 * 1024 * 1024, 4096).unwrap());
        let alloc = crate::allocator::SlotAllocator::new(dev.clone()).unwrap();
        let index = crate::index::Index::new(128).unwrap();
        let engine = Arc::new(crate::ops::engine::Engine::new(
            dev,
            index,
            alloc,
            crate::locks::StripedLocks::new(64),
            crate::index::DahIndex::new(),
            crate::index::UnminedIndex::new(),
        ));

        let tx_id = {
            let mut nonce = 0u64;
            loop {
                let mut tx_id = [0u8; 32];
                tx_id[..8].copy_from_slice(&nonce.to_le_bytes());
                if ShardTable::shard_for_key(&TxKey { txid: tx_id }) == shard {
                    break tx_id;
                }
                nonce += 1;
            }
        };
        let key = TxKey { txid: tx_id };
        let utxo_hashes = [[0x44u8; 32]];
        engine
            .create(&crate::ops::create::CreateRequest {
                tx_id,
                tx_version: 1,
                locktime: 0,
                fee: 100,
                size_in_bytes: 100,
                extended_size: 0,
                is_coinbase: false,
                spending_height: 0,
                utxo_hashes: &utxo_hashes,
                inputs: None,
                outputs: None,
                inpoints: None,
                is_external: false,
                created_at: 1710000000000,
                block_height: 0,
                mined_block_infos: &[],
                frozen: false,
                conflicting: false,
                locked: false,
                parent_txids: &[],
            })
            .unwrap();

        let migration = Arc::new(Mutex::new(MigrationManager::new()));
        let populated: std::collections::HashSet<u16> = [shard].into_iter().collect();
        migration.lock().unwrap().start_outbound(
            std::slice::from_ref(&task),
            old_master,
            &populated,
        );

        let fenced_bm = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());
        let migrating_bm = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());
        migrating_bm.set(shard);
        let inbound_bm = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let target_addr = listener.local_addr().unwrap();
        let (request_tx, request_rx) = std::sync::mpsc::channel();
        let receiver = std::thread::spawn(move || {
            use std::io::{Read, Write};

            for _ in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_millis(200)))
                    .unwrap();
                loop {
                    let mut header = [0u8; 4];
                    match stream.read_exact(&mut header) {
                        Ok(()) => {}
                        Err(e)
                            if e.kind() == std::io::ErrorKind::WouldBlock
                                || e.kind() == std::io::ErrorKind::TimedOut =>
                        {
                            break;
                        }
                        Err(_) => break,
                    }
                    let payload_len = u32::from_le_bytes(header) as usize;
                    let mut rest = vec![0u8; payload_len];
                    stream.read_exact(&mut rest).unwrap();

                    let mut frame_bytes = header.to_vec();
                    frame_bytes.extend_from_slice(&rest);
                    let (request, _) = RequestFrame::decode(&frame_bytes).unwrap();
                    request_tx
                        .send((request.op_code, request.flags, request.request_id))
                        .unwrap();

                    let response = ResponseFrame {
                        request_id: request.request_id,
                        status: STATUS_OK,
                        payload: Vec::new(),
                    };
                    stream.write_all(&response.encode()).unwrap();

                    if request.op_code == OP_MIGRATION_COMPLETE {
                        break;
                    }
                }
            }
        });

        run_migration_batch(
            vec![task.clone()],
            Some(target_addr),
            &[key],
            engine,
            &migration,
            &shard_table,
            &None,
            new_table.version,
            1,
            100,
            fenced_bm,
            migrating_bm,
            inbound_bm,
            old_master,
        );

        let mut seen = Vec::new();
        while let Ok(request) = request_rx.recv_timeout(Duration::from_millis(200)) {
            seen.push(request);
            if request.0 == OP_MIGRATION_BATCH_COMPLETE {
                break;
            }
        }
        assert!(
            seen.iter()
                .any(|(op, flags, req_id)| *op == OP_MIGRATION_COMPLETE
                    && *flags & FLAG_MIGRATION_VERIFY_ONLY != 0
                    && *req_id == shard as u64),
            "data migrations must verify the exact manifest before batched completion; saw {seen:?}"
        );
        assert!(
            seen.iter()
                .any(|(op, _flags, _req_id)| *op == OP_MIGRATION_BATCH_COMPLETE),
            "verified data migrations must clear inbound state with a batched completion; saw {seen:?}"
        );
        receiver.join().unwrap();
    }

    #[test]
    fn already_serving_completion_retries_until_target_is_ready() {
        let old_members = vec![NodeId(1), NodeId(2)];
        let new_members = vec![NodeId(1), NodeId(2), NodeId(3)];
        let old_table = ShardTable::compute_with_epoch(&old_members, 2, 1);
        let new_table = ShardTable::compute_with_epoch(&new_members, 2, 2);
        let shard = (0..NUM_SHARDS as u16)
            .find(|&s| {
                old_table.target_assignment(s).master != new_table.target_assignment(s).master
            })
            .expect("expected a shard whose master moves");
        let old_master = old_table.target_assignment(shard).master;
        let new_master = new_table.target_assignment(shard).master;
        let task = MigrationTask {
            shard,
            from_node: old_master,
            to_node: new_master,
            is_master: true,
        };

        let mut handoff = old_table.clone();
        handoff.begin_handoff_with(&new_table, |_| false);
        assert_eq!(
            handoff.shard_handoff_state(shard),
            ShardHandoff::ServingNew,
            "the shard must use the completion-only fast path"
        );
        let shard_table = Arc::new(ShardTableLock::new(handoff));

        let dev: Arc<dyn crate::device::BlockDevice> =
            Arc::new(crate::device::MemoryDevice::new(4 * 1024 * 1024, 4096).unwrap());
        let alloc = crate::allocator::SlotAllocator::new(dev.clone()).unwrap();
        let index = crate::index::Index::new(128).unwrap();
        let engine = Arc::new(crate::ops::engine::Engine::new(
            dev,
            index,
            alloc,
            crate::locks::StripedLocks::new(64),
            crate::index::DahIndex::new(),
            crate::index::UnminedIndex::new(),
        ));

        let migration = Arc::new(Mutex::new(MigrationManager::new()));
        migration.lock().unwrap().start_outbound(
            std::slice::from_ref(&task),
            old_master,
            &std::collections::HashSet::new(),
        );

        let fenced_bm = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());
        let migrating_bm = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());
        migrating_bm.set(shard);
        let inbound_bm = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let target_addr = listener.local_addr().unwrap();
        drop(listener);
        let (request_tx, request_rx) = std::sync::mpsc::channel();
        let receiver = std::thread::spawn(move || {
            use std::io::{Read, Write};

            std::thread::sleep(Duration::from_millis(300));
            let listener = std::net::TcpListener::bind(target_addr).unwrap();
            let (mut stream, _) = listener.accept().unwrap();

            let mut header = [0u8; 4];
            stream.read_exact(&mut header).unwrap();
            let payload_len = u32::from_le_bytes(header) as usize;
            let mut rest = vec![0u8; payload_len];
            stream.read_exact(&mut rest).unwrap();

            let mut frame_bytes = header.to_vec();
            frame_bytes.extend_from_slice(&rest);
            let (request, _) = RequestFrame::decode(&frame_bytes).unwrap();
            request_tx
                .send((request.op_code, request.request_id))
                .unwrap();

            let response = ResponseFrame {
                request_id: request.request_id,
                status: STATUS_OK,
                payload: Vec::new(),
            };
            stream.write_all(&response.encode()).unwrap();
        });

        run_migration_batch(
            vec![task.clone()],
            Some(target_addr),
            &[],
            engine,
            &migration,
            &shard_table,
            &None,
            new_table.version,
            1,
            100,
            fenced_bm,
            migrating_bm,
            inbound_bm,
            old_master,
        );

        let (op, _req_id) = request_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(
            op == OP_MIGRATION_COMPLETE || op == OP_MIGRATION_BATCH_COMPLETE,
            "the source must retry completion-only handshakes until the target can acknowledge them (got op {op})"
        );
        receiver.join().unwrap();
    }

    #[test]
    fn restored_outbound_state_forces_reactivation_even_without_assignment_mismatch() {
        assert!(should_trigger_topology_reactivation(true, false, 0, 0));
        assert!(should_trigger_topology_reactivation(false, true, 0, 1));
        assert!(should_trigger_topology_reactivation(false, true, 2, 0));
        assert!(!should_trigger_topology_reactivation(false, true, 0, 0));
    }

    #[test]
    fn committed_reactivation_metrics_use_committed_term_assignments() {
        let members = vec![NodeId(1), NodeId(2), NodeId(3)];
        let rf = 2;
        let committed_term = 4;
        let table = ShardTable::compute_with_epoch(&members, rf, committed_term);

        let (mismatched, pending_handoffs) =
            committed_topology_reactivation_metrics(&table, &members, rf, committed_term);

        assert_eq!(
            mismatched, 0,
            "a shard table that already matches the committed term must not trigger reactivation"
        );
        assert_eq!(pending_handoffs, 0);
    }

    #[test]
    fn cleanup_completed_clears_atomic_fence_shadow() {
        let task = MigrationTask {
            shard: 42,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        let populated: std::collections::HashSet<u16> = [42u16].into_iter().collect();
        let migration = Arc::new(Mutex::new(MigrationManager::new()));
        let fenced_bitmap = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());
        let migrating_bitmap = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());
        let inbound_bitmap = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());

        {
            let mgr = &mut migration.lock().unwrap();
            mgr.start_outbound(std::slice::from_ref(&task), NodeId(1), &populated);
            mgr.fence_shard(42);
            fenced_bitmap.load_from(mgr.fenced_bitmap());
            for progress in mgr.active_migrations() {
                if !progress.is_complete()
                    && progress.state != crate::cluster::migration::MigrationState::Failed
                {
                    migrating_bitmap.set(progress.shard);
                }
            }
            mgr.mark_complete(&task);
        }

        {
            let mut mgr = migration.lock().unwrap();
            mgr.cleanup_completed();
            sync_atomic_migration_bitmaps(&mgr, &fenced_bitmap, &migrating_bitmap, &inbound_bitmap);
        }

        assert!(!fenced_bitmap.test(42));
        assert!(!migrating_bitmap.test(42));
        assert!(!inbound_bitmap.test(42));
    }

    #[test]
    fn preserved_reactivation_filters_tasks_by_full_identity() {
        let master_task = MigrationTask {
            shard: 42,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        let replica_task = MigrationTask {
            shard: 42,
            from_node: NodeId(1),
            to_node: NodeId(3),
            is_master: false,
        };
        let populated: std::collections::HashSet<u16> = [42u16].into_iter().collect();

        let mut mgr = MigrationManager::new();
        mgr.start_outbound(
            &[master_task.clone(), replica_task.clone()],
            NodeId(1),
            &populated,
        );

        let new_task_set: std::collections::HashSet<(u16, NodeId, NodeId, bool)> =
            [(42u16, NodeId(1), NodeId(2), true)].into_iter().collect();

        let preserved_tasks: std::collections::HashSet<(u16, NodeId, NodeId, bool)> = mgr
            .active_migrations()
            .iter()
            .filter(|p| {
                p.state != crate::cluster::migration::MigrationState::Complete
                    && p.state != crate::cluster::migration::MigrationState::Failed
                    && new_task_set.contains(&(p.shard, p.from_node, p.to_node, p.is_master))
            })
            .map(|p| (p.shard, p.from_node, p.to_node, p.is_master))
            .collect();

        let stale_tasks: Vec<MigrationTask> = mgr
            .active_migrations()
            .iter()
            .filter(|p| {
                p.state != crate::cluster::migration::MigrationState::Complete
                    && p.state != crate::cluster::migration::MigrationState::Failed
                    && !preserved_tasks.contains(&(p.shard, p.from_node, p.to_node, p.is_master))
            })
            .map(|p| MigrationTask {
                shard: p.shard,
                from_node: p.from_node,
                to_node: p.to_node,
                is_master: p.is_master,
            })
            .collect();

        assert_eq!(preserved_tasks.len(), 1);
        assert!(preserved_tasks.contains(&(42, NodeId(1), NodeId(2), true)));
        assert_eq!(stale_tasks, vec![replica_task]);
    }

    #[test]
    fn peak_cluster_size_persists_and_loads() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir.path().join("cluster.state");

        // Initially no file → returns 1
        assert_eq!(load_peak_cluster_size(&path), 1);

        // Persist and reload
        persist_peak_cluster_size(&path, 5);
        assert_eq!(load_peak_cluster_size(&path), 5);

        // Higher value persisted over lower
        persist_peak_cluster_size(&path, 3);
        assert_eq!(load_peak_cluster_size(&path), 3); // actually stores whatever we pass

        // Zero or corrupt data → returns 1
        std::fs::write(&path, [0u8; 4]).unwrap(); // too short
        assert_eq!(load_peak_cluster_size(&path), 1);

        std::fs::write(&path, 0u64.to_le_bytes()).unwrap(); // zero
        assert_eq!(load_peak_cluster_size(&path), 1); // max(0, 1) = 1
    }

    #[test]
    fn persist_failure_increments_counter() {
        let before = persist_failure_count();
        // Attempt to persist to a path that cannot be written
        // (directory that doesn't exist).
        let bad_path = std::path::Path::new("/nonexistent_dir_xyz/cluster.state");
        persist_cluster_state(bad_path, 5, 1);
        assert!(
            persist_failure_count() > before,
            "persist_failure_count should increment on failure",
        );
    }

    #[test]
    fn exchange_frame_rejects_oversized_response() {
        use std::io::Write as _;
        use std::net::TcpListener;

        // Spin up a mock server that sends a response with a huge length prefix.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let server = std::thread::spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            // Read and discard the request.
            let mut discard = [0u8; 4096];
            let _ = conn.read(&mut discard);
            // Send a response with total_length = MAX_FRAME_SIZE + 1.
            let huge: u32 = crate::protocol::opcodes::MAX_FRAME_SIZE + 1;
            conn.write_all(&huge.to_le_bytes()).unwrap();
        });

        let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(1)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let request = RequestFrame {
            request_id: 0,
            op_code: 0,
            flags: 0,
            payload: vec![],
        };
        let result = exchange_frame(&mut stream, &request);
        assert!(result.is_err(), "should reject oversized response");
        let err = result.unwrap_err();
        assert!(err.contains("response too large"), "error: {err}");
        server.join().unwrap();
    }

    fn key_for_shard(shard: u16) -> TxKey {
        let mut txid = [0u8; 32];
        txid[..2].copy_from_slice(&shard.to_le_bytes());
        TxKey { txid }
    }

    #[test]
    fn route_prefers_effective_master_while_old_master_is_alive() {
        let old_members = vec![NodeId(1), NodeId(2)];
        let new_members = vec![NodeId(1), NodeId(2), NodeId(3)];
        let old_table = ShardTable::compute_with_epoch(&old_members, 2, 1);
        let new_table = ShardTable::compute_with_epoch(&new_members, 2, 2);
        let shard = (0..NUM_SHARDS as u16)
            .find(|&s| {
                old_table.target_assignment(s).master != new_table.target_assignment(s).master
            })
            .unwrap();
        let old_master = old_table.target_assignment(shard).master;
        let new_master = new_table.target_assignment(shard).master;

        let mut handoff = old_table.clone();
        handoff.begin_handoff_with(&new_table, |_| true);

        let cluster = new_test_running_cluster(
            new_master,
            handoff,
            &[
                (NodeId(1), "127.0.0.1:4101".parse().unwrap()),
                (NodeId(2), "127.0.0.1:4102".parse().unwrap()),
                (NodeId(3), "127.0.0.1:4103".parse().unwrap()),
            ],
            &new_members,
            &[],
            &[],
            &[],
            3,
        );

        let key = key_for_shard(shard);
        assert_eq!(cluster.is_master(&key), MasterQueryResult::No);
        assert_eq!(
            cluster.route(&key),
            RouteDecision::RedirectTo {
                node: old_master,
                shard_table_version: 2,
            }
        );
    }

    #[test]
    fn route_falls_back_to_target_when_old_master_is_dead() {
        let old_members = vec![NodeId(1), NodeId(2)];
        let new_members = vec![NodeId(1), NodeId(2), NodeId(3)];
        let old_table = ShardTable::compute_with_epoch(&old_members, 2, 1);
        let new_table = ShardTable::compute_with_epoch(&new_members, 2, 2);
        let shard = (0..NUM_SHARDS as u16)
            .find(|&s| {
                old_table.target_assignment(s).master != new_table.target_assignment(s).master
            })
            .unwrap();
        let old_master = old_table.target_assignment(shard).master;
        let new_master = new_table.target_assignment(shard).master;

        let mut handoff = old_table.clone();
        handoff.begin_handoff_with(&new_table, |_| true);

        let live_nodes: Vec<_> = [
            (NodeId(1), "127.0.0.1:4201".parse().unwrap()),
            (NodeId(2), "127.0.0.1:4202".parse().unwrap()),
            (NodeId(3), "127.0.0.1:4203".parse().unwrap()),
        ]
        .into_iter()
        .filter(|(node, _)| *node != old_master)
        .collect();
        let cluster = new_test_running_cluster(
            new_master,
            handoff,
            &live_nodes,
            &new_members,
            &[],
            &[],
            &[],
            3,
        );

        let key = key_for_shard(shard);
        assert_eq!(cluster.is_master(&key), MasterQueryResult::Yes);
        assert_eq!(cluster.route(&key), RouteDecision::HandleLocally);
    }

    #[test]
    fn partition_map_prefers_effective_master_while_old_master_is_alive() {
        let old_members = vec![NodeId(1), NodeId(2)];
        let new_members = vec![NodeId(1), NodeId(2), NodeId(3)];
        let old_table = ShardTable::compute_with_epoch(&old_members, 2, 1);
        let new_table = ShardTable::compute_with_epoch(&new_members, 2, 2);
        let shard = (0..NUM_SHARDS as u16)
            .find(|&s| {
                old_table.target_assignment(s).master != new_table.target_assignment(s).master
            })
            .unwrap();
        let old_master = old_table.target_assignment(shard).master;
        let new_master = new_table.target_assignment(shard).master;

        let mut handoff = old_table.clone();
        handoff.begin_handoff_with(&new_table, |_| true);

        let cluster = new_test_running_cluster(
            new_master,
            handoff,
            &[
                (NodeId(1), "127.0.0.1:4401".parse().unwrap()),
                (NodeId(2), "127.0.0.1:4402".parse().unwrap()),
                (NodeId(3), "127.0.0.1:4403".parse().unwrap()),
            ],
            &new_members,
            &[],
            &[],
            &[],
            3,
        );

        let routing = crate::cluster::routing::RoutingInfo::decode(&cluster.encode_partition_map())
            .expect("partition map should decode");
        let advertised = routing.shard_assignments[shard as usize].1;

        assert_eq!(advertised, old_master);
    }

    #[test]
    fn partition_map_falls_back_to_target_when_old_master_is_dead() {
        let old_members = vec![NodeId(1), NodeId(2)];
        let new_members = vec![NodeId(1), NodeId(2), NodeId(3)];
        let old_table = ShardTable::compute_with_epoch(&old_members, 2, 1);
        let new_table = ShardTable::compute_with_epoch(&new_members, 2, 2);
        let shard = (0..NUM_SHARDS as u16)
            .find(|&s| {
                old_table.target_assignment(s).master != new_table.target_assignment(s).master
            })
            .unwrap();
        let old_master = old_table.target_assignment(shard).master;
        let new_master = new_table.target_assignment(shard).master;

        let mut handoff = old_table.clone();
        handoff.begin_handoff_with(&new_table, |_| true);

        let live_nodes: Vec<_> = [
            (NodeId(1), "127.0.0.1:4501".parse().unwrap()),
            (NodeId(2), "127.0.0.1:4502".parse().unwrap()),
            (NodeId(3), "127.0.0.1:4503".parse().unwrap()),
        ]
        .into_iter()
        .filter(|(node, _)| *node != old_master)
        .collect();
        let cluster = new_test_running_cluster(
            new_master,
            handoff,
            &live_nodes,
            &new_members,
            &[],
            &[],
            &[],
            3,
        );

        let routing = crate::cluster::routing::RoutingInfo::decode(&cluster.encode_partition_map())
            .expect("partition map should decode");
        let advertised = routing.shard_assignments[shard as usize].1;

        assert_eq!(advertised, new_master);
    }

    #[test]
    fn partition_map_committed_members_match_active_table_version() {
        let active_members = vec![NodeId(1), NodeId(3)];
        let active_table = ShardTable::compute_with_epoch(&active_members, 2, 3);
        let cluster = new_test_running_cluster(
            NodeId(3),
            active_table,
            &[
                (NodeId(1), "127.0.0.1:4601".parse().unwrap()),
                (NodeId(2), "127.0.0.1:4602".parse().unwrap()),
                (NodeId(3), "127.0.0.1:4603".parse().unwrap()),
            ],
            &active_members,
            &[],
            &[],
            &[],
            3,
        );

        let next_members = vec![NodeId(1), NodeId(2), NodeId(3)];
        let next_commit = crate::cluster::topology::TopologyCommit {
            term: 4,
            proposer: NodeId(1),
            members: next_members.clone(),
            digest: crate::cluster::topology::TopologyTerm::compute_digest(4, &next_members),
        };
        assert_eq!(
            cluster.topology_authority().handle_commit(&next_commit),
            Some(4)
        );

        let routing = crate::cluster::routing::RoutingInfo::decode(&cluster.encode_partition_map())
            .expect("partition map should decode");

        assert_eq!(routing.shard_table_version, 3);
        assert_eq!(routing.committed_members, active_members);
    }

    #[test]
    fn committed_topology_encoding_tracks_latest_committed_term() {
        let active_members = vec![NodeId(1), NodeId(3)];
        let active_table = ShardTable::compute_with_epoch(&active_members, 2, 3);
        let cluster = new_test_running_cluster(
            NodeId(3),
            active_table,
            &[
                (NodeId(1), "127.0.0.1:4701".parse().unwrap()),
                (NodeId(2), "127.0.0.1:4702".parse().unwrap()),
                (NodeId(3), "127.0.0.1:4703".parse().unwrap()),
            ],
            &active_members,
            &[],
            &[],
            &[],
            3,
        );

        let committed_members = vec![NodeId(1), NodeId(2), NodeId(3)];
        let next_commit = crate::cluster::topology::TopologyCommit {
            term: 4,
            proposer: NodeId(1),
            members: committed_members.clone(),
            digest: crate::cluster::topology::TopologyTerm::compute_digest(4, &committed_members),
        };
        assert_eq!(
            cluster.topology_authority().handle_commit(&next_commit),
            Some(4)
        );

        let encoded = cluster.encode_committed_topology();
        let decoded = crate::cluster::topology::TopologyCommit::deserialize(&encoded)
            .expect("committed topology should decode");

        assert_eq!(decoded.term, 4);
        assert_eq!(decoded.members, committed_members);
        assert_eq!(
            decoded.digest,
            crate::cluster::topology::TopologyTerm::compute_digest(4, &committed_members),
        );
    }

    #[test]
    fn alive_node_count_only_counts_live_committed_members() {
        let members = vec![NodeId(1), NodeId(2), NodeId(3)];
        let table = ShardTable::compute_with_epoch(&members, 2, 7);
        let cluster = new_test_running_cluster(
            NodeId(1),
            table,
            &[
                (NodeId(1), "127.0.0.1:4301".parse().unwrap()),
                (NodeId(2), "127.0.0.1:4302".parse().unwrap()),
            ],
            &members,
            &[],
            &[],
            &[],
            3,
        );

        assert_eq!(cluster.alive_node_count(), 2);
    }

    // ----------------------------------------------------------------------
    // Phase B3 — migration completion gating + dispatch cluster_key plumbing
    //
    // 1. `complete_migration_task_current_epoch` must reject a task whose
    //    epoch does not match the live `shard_table.version` (i.e. the
    //    coordinator's `topology_epoch`) and bump the
    //    `topology_epoch_mismatch` metric.
    // 2. The same call with a matching epoch must succeed and leave the
    //    metric untouched.
    // 3. Dispatch's `OP_REPLICA_BATCH` handler must read the receiver's
    //    cluster_key view from `RunningCluster::local_cluster_key()` rather
    //    than the removed B2 global, observable as an `ERR_STALE_EPOCH`
    //    rejection of an off-epoch batch.
    // 4. The local `ReplicationManager` constructed via the coordinator's
    //    `cluster_key_handle()` must stamp every outbound batch with the
    //    live epoch (proves the source-side leg of the same Arc).
    // ----------------------------------------------------------------------

    fn install_test_migration_metrics() -> &'static crate::metrics::MigrationMetrics {
        use crate::metrics::{MigrationMetrics, init_migration_metrics, migration_metrics};
        use std::sync::OnceLock;
        static TEST_METRICS: OnceLock<MigrationMetrics> = OnceLock::new();
        let m_ref: &'static MigrationMetrics = TEST_METRICS.get_or_init(MigrationMetrics::new);
        init_migration_metrics(m_ref);
        migration_metrics().expect("metrics installed")
    }

    fn make_outbound_master_task(shard: u16, from: NodeId, to: NodeId) -> MigrationTask {
        MigrationTask {
            shard,
            from_node: from,
            to_node: to,
            is_master: true,
        }
    }

    /// `complete_migration_task_current_epoch` returns `false` and increments
    /// `topology_epoch_mismatch` when the caller's epoch does not match the
    /// live shard-table version.
    #[test]
    fn migration_complete_rejected_with_stale_epoch() {
        let metrics = install_test_migration_metrics();
        // Live shard table is at epoch 10; the migration task carries epoch 9.
        let members = vec![NodeId(1), NodeId(2)];
        let table = ShardTable::compute_with_epoch(&members, 1, 10);
        let shard_table: Arc<ShardTableLock<ShardTable>> = Arc::new(ShardTableLock::new(table));
        let migration = Arc::new(Mutex::new(MigrationManager::new()));
        let task = make_outbound_master_task(123, NodeId(1), NodeId(2));
        // Pre-register the task so the "untracked" guard is not what
        // returns false — we want the *epoch* guard to fire.
        {
            let mut mgr = migration.lock().unwrap();
            mgr.start_outbound(
                std::slice::from_ref(&task),
                NodeId(1),
                &std::collections::HashSet::new(),
            );
        }
        let fenced = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());
        let migrating = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());

        let before = metrics.topology_epoch_mismatch.get();
        let accepted = complete_migration_task_current_epoch(
            &migration,
            &shard_table,
            &fenced,
            &migrating,
            &task,
            /* task_epoch */ 9,
            /* commit */ false,
        );
        let after = metrics.topology_epoch_mismatch.get();

        assert!(
            !accepted,
            "stale-epoch completion (task=9, live=10) must be rejected",
        );
        assert_eq!(
            after - before,
            1,
            "stale-epoch rejection must bump `topology_epoch_mismatch`",
        );
    }

    /// `complete_migration_task_current_epoch` accepts the call when the
    /// task's epoch matches the live shard-table version, and leaves
    /// `topology_epoch_mismatch` untouched.
    #[test]
    fn migration_complete_accepted_with_current_epoch() {
        let metrics = install_test_migration_metrics();
        let members = vec![NodeId(1), NodeId(2)];
        let table = ShardTable::compute_with_epoch(&members, 1, 10);
        let shard_table: Arc<ShardTableLock<ShardTable>> = Arc::new(ShardTableLock::new(table));
        let migration = Arc::new(Mutex::new(MigrationManager::new()));
        let task = make_outbound_master_task(124, NodeId(1), NodeId(2));
        {
            let mut mgr = migration.lock().unwrap();
            mgr.start_outbound(
                std::slice::from_ref(&task),
                NodeId(1),
                &std::collections::HashSet::new(),
            );
        }
        let fenced = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());
        let migrating = Arc::new(crate::cluster::migration::AtomicShardBitmap::new());

        let before = metrics.topology_epoch_mismatch.get();
        let accepted = complete_migration_task_current_epoch(
            &migration,
            &shard_table,
            &fenced,
            &migrating,
            &task,
            /* task_epoch */ 10,
            /* commit */ false,
        );
        let after = metrics.topology_epoch_mismatch.get();

        assert!(
            accepted,
            "current-epoch completion (task=10, live=10) must be accepted",
        );
        assert_eq!(
            after, before,
            "matching-epoch completion must NOT bump `topology_epoch_mismatch`",
        );
    }

    /// `RunningCluster::local_cluster_key()` returns the live `topology_epoch`,
    /// and the dispatch `OP_REPLICA_BATCH` handler reads it through the
    /// `cluster: &RunningCluster` parameter — proving the B2 global has
    /// been replaced by coordinator-driven plumbing. We verify by sending
    /// a stale-epoch batch and asserting the dispatch handler returns
    /// `STATUS_ERROR + ERR_STALE_EPOCH`, which is only possible if the
    /// receiver gate sees the cluster's live epoch (not 0).
    #[test]
    fn dispatch_routes_local_cluster_key_from_running_cluster() {
        use crate::protocol::frame::RequestFrame;
        use crate::protocol::opcodes::{ERR_STALE_EPOCH, OP_REPLICA_BATCH, STATUS_ERROR};
        use crate::replication::protocol::ReplicaBatch;

        let members = vec![NodeId(1)];
        // Build a shard table at epoch 42 so cluster.local_cluster_key() == 42.
        let table = ShardTable::compute_with_epoch(&members, 1, 42);
        let cluster = new_test_running_cluster(
            NodeId(1),
            table,
            &[(NodeId(1), "127.0.0.1:4801".parse().unwrap())],
            &members,
            &[],
            &[],
            &[],
            1,
        );

        assert_eq!(
            cluster.local_cluster_key(),
            42,
            "cluster.local_cluster_key() must surface the live topology epoch",
        );
        assert_eq!(
            cluster.cluster_key_handle().load(Ordering::Acquire),
            42,
            "shared Arc must carry the same value as the accessor",
        );

        // Deliberately stamp the wire batch with a stale epoch (5 != 42).
        let batch = ReplicaBatch {
            first_sequence: 100,
            ops: vec![],
            trace_ctx: None,
            source_node_id: None,
            cluster_key: 5,
        };
        let req = RequestFrame {
            request_id: 1,
            op_code: OP_REPLICA_BATCH,
            flags: 0,
            payload: batch.serialize(),
        };
        let mut conn_state = crate::server::ConnectionState::new();
        let resp = crate::server::dispatch::handle_request(
            &req,
            &test_engine(),
            8192,
            Some(&cluster),
            None,
            &mut conn_state,
            None,
        );

        assert_eq!(
            resp.status, STATUS_ERROR,
            "dispatch must reject a stale-epoch batch — proving the gate \
             reads cluster.local_cluster_key() (=42), not the removed global (=0)",
        );
        assert!(
            resp.payload.len() >= 2,
            "STATUS_ERROR payload must carry an error_code prefix",
        );
        let err_code = u16::from_le_bytes([resp.payload[0], resp.payload[1]]);
        assert_eq!(
            err_code, ERR_STALE_EPOCH,
            "dispatch must surface the cluster-key gate's ERR_STALE_EPOCH",
        );
    }

    /// The local `ReplicationManager` constructed with the coordinator's
    /// `cluster_key_handle()` stamps every outbound batch with the live
    /// `topology_epoch`. Mirrors the pattern of
    /// `replication::manager::tests::manager_attaches_current_cluster_key`
    /// but drives the handle from a `RunningCluster` so the coordinator
    /// wiring (Phase B3) is proven end-to-end: the manager sees epoch
    /// bumps to the same `Arc<AtomicU64>` the coordinator owns.
    #[test]
    fn manager_attaches_cluster_key_from_topology_epoch() {
        use crate::replication::manager::{
            InMemoryTransport, ReplicationConfig, ReplicationManager,
        };
        use crate::replication::protocol::{ReplicaAck, ReplicaOp};

        // Mirror `spawn_auto_ack_replica`: a replica thread that recv's,
        // ACKs `Ok { through_sequence }`, and returns the captured batches.
        let members = vec![NodeId(1), NodeId(2)];
        let table = ShardTable::compute_with_epoch(&members, 1, 99);
        let cluster = new_test_running_cluster(
            NodeId(1),
            table,
            &[
                (NodeId(1), "127.0.0.1:4901".parse().unwrap()),
                (NodeId(2), "127.0.0.1:4902".parse().unwrap()),
            ],
            &members,
            &[],
            &[],
            &[],
            2,
        );
        assert_eq!(cluster.local_cluster_key(), 99);

        let (master_t, replica_t) = InMemoryTransport::pair();
        let handle = std::thread::spawn(move || {
            let mut received = Vec::new();
            while let Ok(batch) = replica_t.recv_batch(Duration::from_secs(1)) {
                let ack = ReplicaAck::Ok {
                    through_sequence: batch.last_sequence(),
                };
                replica_t.send_ack(&ack).unwrap();
                received.push(batch);
            }
            received
        });

        let mut mgr = ReplicationManager::with_cluster_key(
            ReplicationConfig::default(),
            vec![Box::new(master_t)],
            cluster.cluster_key_handle(),
        );

        let mut txid = [0u8; 32];
        txid[0] = 1;
        let ops = vec![ReplicaOp::Freeze {
            tx_key: TxKey { txid },
            offset: 0,
            master_generation: 1,
        }];
        mgr.replicate_batch(&ops).expect("replicate batch");

        // Drive an in-place epoch bump on the shared Arc. The manager
        // must observe it on the very next batch, proving it reads the
        // live atomic on every send (not a snapshot at construction).
        cluster.cluster_key_handle().store(123, Ordering::Release);
        mgr.replicate_batch(&ops)
            .expect("replicate batch (post-bump)");

        // Drop the manager so the replica's `recv_batch` returns Err and
        // the thread joins. Without this, the test would block forever
        // because the spawned reader is still listening on the channel.
        drop(mgr);
        let received = handle.join().expect("replica thread joined");
        assert_eq!(received.len(), 2, "manager should send exactly two batches");
        assert_eq!(
            received[0].cluster_key, 99,
            "first batch must be stamped with the coordinator's initial cluster_key (99)",
        );
        assert_eq!(
            received[1].cluster_key, 123,
            "the post-bump batch must carry the new cluster_key (123) — \
             proving the manager reads the live shared Arc, not a snapshot",
        );
    }

    // ── Phase B4: MasterQueryResult ────────────────────────────────

    /// Build a single-node test cluster where this node owns every shard
    /// (single-node committed term == shard table version, no epoch gap).
    fn single_node_cluster_for_master_query_tests() -> RunningCluster {
        let members = vec![NodeId(1)];
        let table = ShardTable::compute_with_epoch(&members, 1, 7);
        new_test_running_cluster(
            NodeId(1),
            table,
            &[(NodeId(1), "127.0.0.1:4801".parse().unwrap())],
            &members,
            &[],
            &[],
            &[],
            1,
        )
    }

    #[test]
    fn is_master_returns_yes_when_local_master() {
        let cluster = single_node_cluster_for_master_query_tests();
        let key = key_for_shard(0);
        match cluster.is_master(&key) {
            MasterQueryResult::Yes => {}
            other => panic!("expected MasterQueryResult::Yes, got {other:?}"),
        }
    }

    #[test]
    fn is_master_returns_no_when_remote_master() {
        let members = vec![NodeId(1), NodeId(2)];
        let table = ShardTable::compute_with_epoch(&members, 2, 5);
        // Find a shard whose target master is NodeId(2), then run as NodeId(1).
        let shard = (0..NUM_SHARDS as u16)
            .find(|&s| table.target_assignment(s).master == NodeId(2))
            .expect("at least one shard owned by NodeId(2)");
        let cluster = new_test_running_cluster(
            NodeId(1),
            table,
            &[
                (NodeId(1), "127.0.0.1:4811".parse().unwrap()),
                (NodeId(2), "127.0.0.1:4812".parse().unwrap()),
            ],
            &members,
            &[],
            &[],
            &[],
            2,
        );
        let key = key_for_shard(shard);
        match cluster.is_master(&key) {
            MasterQueryResult::No => {}
            other => panic!("expected MasterQueryResult::No, got {other:?}"),
        }
    }

    #[test]
    fn is_master_query_returns_transitioning_during_epoch_gap() {
        // Single-node committed-term=5 cluster, then bump topology_epoch to 6
        // (simulating a membership change that has been proposed/observed
        // locally but has not yet quorum-committed).
        let members = vec![NodeId(1)];
        let table = ShardTable::compute_with_epoch(&members, 1, 5);
        let cluster = new_test_running_cluster(
            NodeId(1),
            table,
            &[(NodeId(1), "127.0.0.1:4821".parse().unwrap())],
            &members,
            &[],
            &[],
            &[],
            1,
        );
        // Sanity: committed term is 5 (set by new_test_running_cluster).
        assert_eq!(cluster.topology_authority.committed_term(), 5);
        // Bump the local peak/proposed epoch ahead of the committed term.
        cluster.topology_epoch.store(6, Ordering::Release);

        let key = key_for_shard(0);
        match cluster.is_master(&key) {
            MasterQueryResult::Transitioning { last_known_term } => {
                assert_eq!(
                    last_known_term, 5,
                    "Transitioning must report the last quorum-committed term",
                );
            }
            other => panic!(
                "expected MasterQueryResult::Transitioning {{ last_known_term: 5 }}, got {other:?}"
            ),
        }
    }

    // ── Phase B fixup: cluster_key sourced from quorum-committed term ──

    /// `local_cluster_key()` MUST return the quorum-committed term, NOT the
    /// per-node `topology_epoch`. Each node initializes `topology_epoch`
    /// from its local member-list snapshot, so values diverge across the
    /// cluster at startup; routing the cluster_key through the committed
    /// term ensures all nodes converge on the same value (initially 0)
    /// until the first quorum commit lands. Without this, cross-node
    /// `OP_REPLICA_BATCH` traffic is rejected with `ERR_STALE_EPOCH`
    /// during legitimate operation.
    #[test]
    fn local_cluster_key_returns_committed_term_not_topology_epoch() {
        // Build a cluster where the per-node `topology_epoch` is 10 but
        // the quorum-committed term is 7. Without the fix,
        // `local_cluster_key()` would return 10; with the fix it returns 7.
        let members = vec![NodeId(1)];
        // Shard table version 7 → handle_commit(term=7) inside the helper
        // sets committed_term to 7.
        let table = ShardTable::compute_with_epoch(&members, 1, 7);
        let cluster = new_test_running_cluster(
            NodeId(1),
            table,
            &[(NodeId(1), "127.0.0.1:4801".parse().unwrap())],
            &members,
            &[],
            &[],
            &[],
            1,
        );
        // Force the per-node topology_epoch ahead of the committed term to
        // simulate the divergence seen in multi-node startup.
        cluster.topology_epoch.store(10, Ordering::Release);

        assert_eq!(
            cluster.topology_authority.committed_term(),
            7,
            "precondition: committed_term must be 7",
        );
        assert_eq!(
            cluster.topology_epoch.load(Ordering::Acquire),
            10,
            "precondition: topology_epoch must be 10 (different from committed_term)",
        );
        assert_eq!(
            cluster.local_cluster_key(),
            7,
            "local_cluster_key() must surface the quorum-committed term (7), \
             NOT the per-node topology_epoch (10)",
        );
        assert_eq!(
            cluster.cluster_key_handle().load(Ordering::Acquire),
            7,
            "cluster_key_handle() must back the same value (7) so manager \
             and receiver observe the committed term, not topology_epoch",
        );
    }

    /// Applying an `OP_TOPOLOGY_COMMIT` (via `topology_authority.handle_commit`)
    /// must advance `local_cluster_key()` synchronously, so all nodes
    /// converge on the same key after each quorum-committed term.
    #[test]
    fn committed_cluster_key_advances_on_topology_commit() {
        // Start with no committed term (fresh node, before any quorum).
        let members = vec![NodeId(1)];
        let table = ShardTable::compute_with_epoch(&members, 1, 1);
        let cluster = new_test_running_cluster(
            NodeId(1),
            table,
            &[(NodeId(1), "127.0.0.1:4801".parse().unwrap())],
            // Empty committed_members → helper does NOT call handle_commit.
            &[],
            &[],
            &[],
            &[],
            1,
        );
        assert_eq!(
            cluster.local_cluster_key(),
            0,
            "before any quorum commit, local_cluster_key must be 0 \
             (V1-compat / unknown — gating becomes a no-op)",
        );

        // Trigger application of an OP_TOPOLOGY_COMMIT for term 5.
        let commit_members = vec![NodeId(1), NodeId(2)];
        let commit = crate::cluster::topology::TopologyCommit {
            term: 5,
            proposer: NodeId(1),
            members: commit_members.clone(),
            digest: crate::cluster::topology::TopologyTerm::compute_digest(5, &commit_members),
        };
        let applied = cluster.topology_authority.handle_commit(&commit);
        assert_eq!(applied, Some(5), "commit must be accepted");

        assert_eq!(
            cluster.local_cluster_key(),
            5,
            "after OP_TOPOLOGY_COMMIT for term 5, local_cluster_key must \
             advance to 5 — proving the cluster_key handle tracks the \
             quorum-committed term in lock-step",
        );
        assert_eq!(
            cluster.cluster_key_handle().load(Ordering::Acquire),
            5,
            "cluster_key_handle() must observe the same advance (5) so \
             downstream manager/receiver readers see the new term",
        );
    }

    // ── Phase C: subset master tracking ────────────────────────────────────

    #[test]
    fn election_skips_subset_master() {
        let members = vec![NodeId(1)];
        let table = ShardTable::compute_with_epoch(&members, 1, 5);
        let shard = 0u16;
        let key = key_for_shard(shard);
        let cluster = new_test_running_cluster(
            NodeId(1),
            table,
            &[(NodeId(1), "127.0.0.1:4841".parse().unwrap())],
            &[NodeId(1)],
            &[shard],
            &[],
            &[],
            1,
        );
        match cluster.is_master(&key) {
            MasterQueryResult::Transitioning { .. } => {}
            other => {
                panic!("subset master (has inbound) should return Transitioning, got {other:?}")
            }
        }
    }

    #[test]
    fn rank_master_candidate_scoring() {
        assert!(
            rank_master_candidate(NodeId(1), NodeId(1), false, false)
                > rank_master_candidate(NodeId(2), NodeId(1), true, false),
            "previous master must outscore subset candidate"
        );
        assert!(
            rank_master_candidate(NodeId(2), NodeId(1), false, false)
                > rank_master_candidate(NodeId(3), NodeId(1), true, false),
            "full replica must outscore subset candidate"
        );
        assert_eq!(
            rank_master_candidate(NodeId(4), NodeId(1), false, true),
            0,
            "evicted node must score 0"
        );
    }

    // ── Phase D: exchange phase before migration ───────────────────────────

    #[test]
    fn no_migration_plan_until_exchange_complete() {
        let members = [NodeId(1), NodeId(2), NodeId(3)];
        let mut phase = ExchangePhase::new(5, members.len(), std::time::Duration::from_secs(2));
        assert!(
            !phase.is_complete(),
            "should not be complete before any reports"
        );
        phase.record(NodeId(1), vec![]);
        assert!(
            !phase.is_complete(),
            "should not be complete with 1/3 reports"
        );
        phase.record(NodeId(2), vec![]);
        assert!(
            !phase.is_complete(),
            "should not be complete with 2/3 reports"
        );
        // Duplicate report from NodeId(2) must not double-count toward completion.
        phase.record(NodeId(2), vec![]);
        assert!(
            !phase.is_complete(),
            "duplicate report must not count toward completion"
        );
        phase.record(NodeId(3), vec![]);
        assert!(
            phase.is_complete(),
            "should be complete with 3/3 unique reports"
        );
    }

    #[test]
    fn exchange_phase_collects_per_shard_versions() {
        let mut phase = ExchangePhase::new(1, 2, std::time::Duration::from_secs(2));
        let entries1 = vec![
            PartitionVersionEntry {
                shard: 0,
                flags: 0b01,
                replica_count: 1,
                last_applied_seq: 100,
            },
            PartitionVersionEntry {
                shard: 1,
                flags: 0b00,
                replica_count: 1,
                last_applied_seq: 50,
            },
        ];
        let entries2 = vec![PartitionVersionEntry {
            shard: 0,
            flags: 0b00,
            replica_count: 1,
            last_applied_seq: 90,
        }];
        phase.record(NodeId(10), entries1.clone());
        phase.record(NodeId(20), entries2.clone());
        let view = phase.partition_view();
        assert_eq!(view.get(&NodeId(10)).unwrap(), &entries1);
        assert_eq!(view.get(&NodeId(20)).unwrap(), &entries2);
    }

    #[test]
    fn exchange_timeout_is_detected() {
        let phase = ExchangePhase::new(1, 3, std::time::Duration::from_millis(0));
        // Tick a moment to ensure the deadline has passed.
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert!(
            phase.is_timed_out(),
            "zero-duration exchange must report timed out immediately"
        );
        assert!(
            !phase.is_complete(),
            "timed out exchange must not report complete"
        );
    }

    #[test]
    fn build_plan_uses_partition_view_to_skip_migration() {
        use std::collections::HashMap;
        let members_a = vec![NodeId(1), NodeId(2)];
        let members_b = vec![NodeId(2), NodeId(3)];
        let old_table = ShardTable::compute_with_epoch(&members_a, 2, 1);
        let new_table = ShardTable::compute_with_epoch(&members_b, 2, 2);
        let changed_shard = (0..4096u16)
            .find(|&s| {
                old_table.target_assignment(s).master != new_table.target_assignment(s).master
            })
            .expect("membership change must shift at least one shard");
        let new_master = new_table.target_assignment(changed_shard).master;
        let mut partition_view: HashMap<NodeId, Vec<PartitionVersionEntry>> = HashMap::new();
        partition_view.insert(
            new_master,
            vec![PartitionVersionEntry {
                shard: changed_shard,
                flags: 0b01,
                replica_count: 1,
                last_applied_seq: 42,
            }],
        );
        let tasks =
            build_plan_from_partition_view(&old_table, &new_table, &partition_view, NodeId(1));
        let task_for_shard = tasks.iter().find(|t| t.shard == changed_shard);
        assert!(
            task_for_shard.is_none(),
            "must skip migration for shard {changed_shard} when new master already has data",
        );
    }
}
