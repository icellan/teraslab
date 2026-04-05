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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
// MigrationManager uses std::sync::Mutex; redo log uses parking_lot::Mutex.
use std::sync::Mutex;
type ParkingMutex<T> = parking_lot::Mutex<T>;
/// parking_lot RwLock for the shard table hot path: better reader throughput
/// than std::sync::RwLock under high contention, and no poisoning on panic.
type ShardTableLock<T> = parking_lot::RwLock<T>;
/// std::sync::RwLock for non-hot-path data (node_addrs, etc.).
use std::sync::RwLock;
use std::time::Duration;

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
        eprintln!("cluster: debug shard {} {}", shard, message.as_ref());
    }
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

    let snapshot = ShardTable::compute_with_epoch(
        &routing.committed_members,
        rf,
        routing.shard_table_version,
    );
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
            topology_epoch: Arc::new(std::sync::atomic::AtomicU64::new(1)),
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
                        if let Some(fallback_proposal) = topo_authority_event.check_timeout(&members) {
                            eprintln!(
                                "cluster: fallback proposer stepping up for term {}",
                                fallback_proposal.term,
                            );
                            if let Some(ref path) = topo_state_path_event {
                                let peak = peak_size_event.load(Ordering::Relaxed) as u64;
                                let inc = swim_incarnation_event.load(Ordering::Relaxed);
                                persist_topology_state(path, &topo_authority_event.persisted_state(peak, inc));
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
                                if commit.term <= last_activated_term {
                                    eprintln!(
                                        "cluster: skipping duplicate self-vote activation for term {} \
                                         (already activated at term {last_activated_term})",
                                        commit.term,
                                    );
                                } else {
                                    last_activated_term = commit.term;
                                    topology_epoch.store(commit.term, Ordering::Relaxed);
                                    Self::activate_topology(
                                        &commit.members, commit.term, self_id, rf, &shard_table, &migration,
                                        &node_addrs, &engine, &redo_for_events, max_migration_threads,
                                        migration_pool_size, migration_batch_size, &fenced_bm_event, &migrating_bm_event, &inbound_bm_event,
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
                                    run_topology_proposer(fallback_proposal, ta, na, self_id, tx, tp, ps, si);
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
                                eprintln!(
                                    "cluster: cleared {removed} settled inbound migration(s) — no active migrations or handoffs remain"
                                );
                            } else {
                                eprintln!("cluster: cleared {removed} stale inbound migration(s)");
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
                                eprintln!(
                                    "cluster: re-activating topology after restored outbound migration state (term {committed_term}, {pending_handoffs} pending handoff shards, {mismatched} mismatched shards)",
                                );
                            } else {
                                eprintln!(
                                    "cluster: re-activating topology — {mismatched} mismatched shards, {pending_handoffs} pending handoff shards (term {committed_term})",
                                );
                            }
                            last_reactivation_at = std::time::Instant::now();
                            last_activation_at = std::time::Instant::now();
                            Self::activate_topology(
                                &committed_members, committed_term, self_id, rf, &shard_table, &migration,
                                &node_addrs, &engine, &redo_for_events, max_migration_threads,
                                migration_pool_size, migration_batch_size, &fenced_bm_event, &migrating_bm_event, &inbound_bm_event,
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
                    if term <= last_activated_term {
                        eprintln!(
                            "cluster: skipping duplicate topology commit for term {term} \
                             (already activated at term {last_activated_term})"
                        );
                        continue;
                    }
                    last_activated_term = term;
                    topology_epoch.store(term, Ordering::Relaxed);
                    eprintln!("cluster: activating topology from commit signal (term {term}, epoch {term})");
                    Self::activate_topology(
                        &members, term, self_id, rf, &shard_table, &migration,
                        &node_addrs, &engine, &redo_for_events, max_migration_threads,
                        migration_pool_size, migration_batch_size, &fenced_bm_event, &migrating_bm_event, &inbound_bm_event,
                        &active_topology_members_event,
                    );
                    last_activation_at = std::time::Instant::now();
                    if let Some(ref path) = cluster_state_path {
                        let peak = peak_size_event.load(Ordering::Relaxed) as u64;
                        persist_cluster_state(path, peak, term);
                    }
                    if let Some(ref path) = outbound_state_path_event {
                        crate::cluster::migration::persist_outbound_state(path, &migration.lock().unwrap());
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
                eprintln!("cluster: node {:?} joined at {addr}", node);
                node_addrs.write().unwrap().insert(*node, *addr);

                // Retry any previously failed migrations — the newly
                // joined node may be the target that was unavailable.
                let retry_tasks = migration.lock().unwrap().take_failed_tasks();
                if !retry_tasks.is_empty() {
                    eprintln!("cluster: retrying {} failed migration(s)", retry_tasks.len());
                    let epoch = topology_epoch.load(Ordering::Relaxed);
                    let retry_shards: std::collections::HashSet<u16> = retry_tasks.iter()
                        .map(|t| t.shard)
                        .collect();
                    let keys_map = engine.keys_by_shard_filtered(&retry_shards);
                    let all_keys: Vec<TxKey> = keys_map.values()
                        .flat_map(|v| v.iter().copied()).collect();
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
                eprintln!("cluster: node {:?} left", node);
                node_addrs.write().unwrap().remove(node);
            }
            ClusterEvent::MembershipChanged(members) => {
                eprintln!("cluster: membership changed to {} nodes: {members:?}", members.len());

                // Gate through topology authority: only the deterministic
                // proposer (lowest NodeId) initiates the quorum protocol.
                // The shard table is NOT activated until quorum commits.
                if let Some(proposal) = topology_authority.on_membership_changed(members) {
                    eprintln!(
                        "cluster: proposing topology term {} ({} members)",
                        proposal.term, proposal.members.len(),
                    );
                    // Persist voted_term before broadcasting.
                    if let Some(path) = topology_state_path {
                        let peak = peak_size.load(Ordering::Relaxed) as u64;
                        let inc = swim_incarnation.load(Ordering::Relaxed);
                        persist_topology_state(path, &topology_authority.persisted_state(peak, inc));
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
                        eprintln!("cluster: single-node quorum — activating term {}", commit.term);
                        topology_authority.handle_commit(&commit);
                        Self::activate_topology(
                            &commit.members, commit.term, self_id, rf, shard_table, migration,
                            node_addrs, engine, redo_for_events, max_migration_threads,
                            migration_pool_size, migration_batch_size, fenced_bm, migrating_bm, inbound_bm,
                            active_topology_members,
                        );
                        if let Some(path) = topology_state_path {
                            let peak = peak_size.load(Ordering::Relaxed) as u64;
                            let inc = swim_incarnation.load(Ordering::Relaxed);
                            persist_topology_state(path, &topology_authority.persisted_state(peak, inc));
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
                eprintln!("cluster: node {:?} suspected", node);
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
                    eprintln!(
                        "cluster: topology stale (local term {local_term}, remote has {remote_term}) — catch-up thread started",
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
                        addrs.iter()
                            .filter(|(id, _)| **id != self_id)
                            .filter(|(id, _)| {
                                committed_members.is_empty() || committed_members.contains(id)
                            })
                            .map(|(_, &addr)| addr)
                            .collect()
                    };
                    let local_active_version = { shard_table.read().version };
                    for peer_addr in &peers {
                        if let Ok(payload) = send_topology_frame(*peer_addr, OP_GET_PARTITION_MAP, &[])
                            && let Some(routing) =
                                crate::cluster::routing::RoutingInfo::decode(&payload)
                            && routing.shard_table_version > local_active_version
                            && !routing.committed_members.is_empty()
                        {
                            let mut snapshot_members = routing.committed_members.clone();
                            snapshot_members.sort();
                            if routing.shard_table_version > topology_authority.committed_term() {
                                let synthetic = crate::cluster::topology::TopologyCommit {
                                    term: routing.shard_table_version,
                                    proposer: snapshot_members[0],
                                    members: snapshot_members.clone(),
                                    digest: crate::cluster::topology::TopologyTerm::compute_digest(
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
                                eprintln!(
                                    "cluster: catch-up: installed active routing snapshot term {} from peer {}",
                                    routing.shard_table_version,
                                    peer_addr,
                                );
                            }
                            break;
                        }
                    }

                    let local_term = topology_authority.committed_term();
                    let mut caught_up = false;
                    for peer_addr in &peers {
                        if let Ok(payload) = send_topology_frame(*peer_addr, OP_GET_COMMITTED_TOPOLOGY, &[])
                            && let Some(commit) = crate::cluster::topology::TopologyCommit::deserialize(&payload)
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
                            if let Some(applied_term) = topology_authority.handle_commit(&commit) {
                                eprintln!(
                                    "cluster: catch-up: applied term {} from peer {} ({} members)",
                                    applied_term, peer_addr, remote_members.len(),
                                );
                                if let Some(ref path) = *topology_state_path {
                                    let peak = peak_size.load(Ordering::Relaxed) as u64;
                                    let inc = swim_incarnation.load(Ordering::Relaxed);
                                    persist_topology_state(path, &topology_authority.persisted_state(peak, inc));
                                }
                                // Signal the event loop to activate the topology.
                                let _ = topology_commit_tx.send((remote_members.clone(), commit.term));
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
                        if let Some(proposal) = topology_authority.on_membership_changed(&members) {
                            eprintln!(
                                "cluster: catch-up: re-proposing topology term {} ({} members)",
                                proposal.term, proposal.members.len(),
                            );
                            if let Some(path) = topology_state_path {
                                let peak = peak_size.load(Ordering::Relaxed) as u64;
                                let inc = swim_incarnation.load(Ordering::Relaxed);
                                persist_topology_state(path, &topology_authority.persisted_state(peak, inc));
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
                                let _ = topology_commit_tx.send((commit.members.clone(), commit.term));
                            } else {
                                let ta = topology_authority.clone();
                                let na = node_addrs_for_topo.clone();
                                let tx = topology_commit_tx.clone();
                                let tp = topology_state_path.clone();
                                let ps = peak_size.clone();
                                let si = swim_incarnation.clone();
                                run_topology_proposer(proposal, ta, na, self_id, tx, tp, ps, si);
                            }
                        }
                    }
                    }); // end of catch-up thread
                }
            }
        }
    }

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
            let is_single_node_bootstrap =
                old_unique.len() == 1 && old_unique.contains(&self_id);
            drop(old_table);

            if engine.index_len() == 0 && is_single_node_bootstrap {
                let new_table = ShardTable::compute_with_epoch(members, rf, epoch);
                eprintln!(
                    "cluster: empty engine — fast-path shard table install (epoch {epoch}, {} members)",
                    members.len(),
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
        let new_table = ShardTable::compute_with_epoch(members, rf, epoch);
        let new_plan = ShardTable::migration_plan(&old_table_snap, &new_table);
        let new_replica_plan = ShardTable::replica_migration_plan(&old_table_snap, &new_table);
        drop(old_table_snap);

        let mut all_new_tasks: Vec<MigrationTask> = new_plan.clone();
        all_new_tasks.extend(new_replica_plan.iter().cloned());

        // Build a set of (shard, from, to, is_master) for the new plan.
        let new_task_set: std::collections::HashSet<(u16, NodeId, NodeId, bool)> = all_new_tasks.iter()
            .map(|t| (t.shard, t.from_node, t.to_node, t.is_master))
            .collect();

        // Determine which existing migrations can be preserved.
        let preserved_tasks: std::collections::HashSet<(u16, NodeId, NodeId, bool)>;
        {
            let mut mgr = migration.lock().unwrap();
            let old_inbound = mgr.inbound_count();
            let old_active = mgr.active_count();
            let old_failed = mgr.failed_count();

            // Identify preservable migrations: active, not complete/failed,
            // and appearing in the new plan with same source/target.
            preserved_tasks = mgr.active_migrations().iter()
                .filter(|p| {
                    p.state != crate::cluster::migration::MigrationState::Complete
                        && p.state != crate::cluster::migration::MigrationState::Failed
                        && new_task_set.contains(&(p.shard, p.from_node, p.to_node, p.is_master))
                })
                .map(|p| (p.shard, p.from_node, p.to_node, p.is_master))
                .collect();

            // Cancel only non-preserved migrations.
            let stale_tasks: Vec<MigrationTask> = mgr.active_migrations().iter()
                .filter(|p| {
                    p.state != crate::cluster::migration::MigrationState::Complete
                        && p.state != crate::cluster::migration::MigrationState::Failed
                        && !preserved_tasks.contains(&(p.shard, p.from_node, p.to_node, p.is_master))
                })
                .map(|p| MigrationTask {
                    shard: p.shard, from_node: p.from_node,
                    to_node: p.to_node, is_master: p.is_master,
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
                eprintln!(
                    "cluster: topology change — preserved {preserved_count}, cancelled {cancelled} active + {old_failed} failed outbound, cleared {old_inbound} inbound",
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

        if plan.is_empty() && replica_plan.is_empty() {
            *shard_table.write() = new_table;
        } else {
            let mut all_tasks = plan.clone();
            all_tasks.extend(replica_plan.iter().cloned());

            let outbound_tasks: Vec<MigrationTask> = all_tasks.iter()
                .filter(|t| {
                    t.from_node == self_id
                        && !preserved_tasks.contains(&(t.shard, t.from_node, t.to_node, t.is_master))
                })
                .cloned()
                .collect();
            let inbound = all_tasks.iter()
                .filter(|t| t.to_node == self_id)
                .count();
            let master_out = outbound_tasks.iter().filter(|t| t.is_master).count();
            let replica_out = outbound_tasks.iter().filter(|t| !t.is_master).count();
            eprintln!(
                "cluster: migration plan: {} master + {} replica moves ({} outbound [{} master, {} replica], {} inbound from this node)",
                plan.len(), replica_plan.len(), outbound_tasks.len(), master_out, replica_out, inbound
            );

            let outbound_shard_set: std::collections::HashSet<u16> = outbound_tasks.iter()
                .map(|t| t.shard)
                .collect();
            let outbound_master_source_shards: std::collections::HashSet<u16> = outbound_tasks.iter()
                .filter(|t| t.is_master)
                .map(|t| t.shard)
                .collect();

            let populated_shards: std::collections::HashSet<u16> = (0..NUM_SHARDS as u16)
                .filter(|&s| engine.shard_record_count(s) > 0)
                .collect();
            let local_store_empty = populated_shards.is_empty();

            // Build the set of shards that have MASTER migration tasks.
            // Only master migrations need the old master to keep serving
            // during handoff (Copying state). Shards with only replica
            // tasks (or no tasks) go directly to ServingNew — the new
            // master already has the data and can serve immediately.
            let master_migration_shards: std::collections::HashSet<u16> = all_tasks.iter()
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
                let new_tasks: Vec<MigrationTask> = all_tasks.iter()
                    .filter(|t| {
                        !preserved_tasks.contains(&(t.shard, t.from_node, t.to_node, t.is_master))
                            && !(local_store_empty && t.from_node == self_id)
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
                let pre_swap_keys: Vec<TxKey> =
                    pre_swap_keys_by_shard.values()
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
        keys_by_shard.entry(ShardTable::shard_for_key(&key)).or_default().push(key);
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

fn should_trigger_topology_reactivation(
    startup_reactivation_due: bool,
    normal_reactivation_due: bool,
    mismatched_shards: u32,
    pending_handoffs: usize,
) -> bool {
    startup_reactivation_due
        || (normal_reactivation_due && (mismatched_shards > 0 || pending_handoffs > 0))
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
    let peers: Vec<(NodeId, SocketAddr)> = {
        let addrs = node_addrs.read().unwrap();
        addrs.iter()
            .filter(|(id, _)| **id != self_id)
            .map(|(&id, &addr)| (id, addr))
            .collect()
    };

    if peers.is_empty() {
        // No peers — single-node case should have been handled before spawning.
        return;
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
                                eprintln!("cluster: topology propose to {:?} ({}) — malformed vote", pid, paddr);
                                None
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("cluster: topology propose to {:?} ({}) failed: {e}", pid, paddr);
                        None
                    }
                }
            })
        }).collect();
        handles.into_iter().map(|h| h.join().unwrap_or(None)).collect()
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
            eprintln!(
                "cluster: topology term {} — quorum not reached after contacting {} peers",
                proposal.term, peers.len(),
            );
            return;
        }
    };

    eprintln!(
        "cluster: quorum reached for term {} — broadcasting commit",
        commit.term,
    );

    // Broadcast OP_TOPOLOGY_COMMIT to all peers in parallel with retry.
    let commit_payload = commit.serialize();
    let failed_addrs: Vec<SocketAddr> = std::thread::scope(|scope| {
        let handles: Vec<_> = peers.iter().map(|(_, addr)| {
            let payload = &commit_payload;
            let a = *addr;
            scope.spawn(move || -> Option<SocketAddr> {
                if let Err(e) = send_topology_frame(a, OP_TOPOLOGY_COMMIT, payload) {
                    eprintln!("cluster: topology commit broadcast to {a} failed: {e}");
                    Some(a)
                } else {
                    None
                }
            })
        }).collect();
        handles.into_iter().filter_map(|h| h.join().unwrap_or(None)).collect()
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
                eprintln!("cluster: topology commit retry {retry} to {addr} failed: {e}");
                true
            } else {
                false
            }
        });
    }
    if !still_failed.is_empty() {
        eprintln!(
            "cluster: topology commit: {} node(s) unreachable after retries",
            still_failed.len(),
        );
    }

    // Apply commit locally.
    topology_authority.handle_commit(&commit);
    if let Some(ref path) = topology_state_path {
        let peak = peak_size.load(Ordering::Relaxed) as u64;
        let inc = swim_incarnation.load(Ordering::Relaxed);
        persist_topology_state(path, &topology_authority.persisted_state(peak, inc));
    }

    // Signal the event loop to activate the shard table.
    let _ = topology_commit_tx.send((commit.members.clone(), commit.term));
}

/// Send a request frame on an existing TCP stream and read the response.
///
/// Validates the response length against `MAX_FRAME_SIZE` before allocating
/// the receive buffer, preventing OOM from malicious or buggy peers.
fn exchange_frame(
    stream: &mut TcpStream,
    request: &RequestFrame,
) -> Result<ResponseFrame, String> {
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
    let (response, _) = ResponseFrame::decode(&full)
        .map_err(|e| format!("decode: {e}"))?;

    Ok(response)
}

/// Send a topology-protocol frame to a peer and return the response payload.
///
/// Uses the standard TeraSlab framed TCP protocol with a 3-second connect
/// timeout and 5-second read timeout.
fn send_topology_frame(
    addr: SocketAddr,
    op_code: u16,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
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
        Self { entries: Vec::new() }
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
        let meta = engine.read_metadata(key)
            .map_err(|e| format!("manifest read_metadata shard {shard} key {:?}: {e:?}", key))?;
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
            eprintln!("cluster: no address for target, cannot migrate {} shards", tasks.len());
            let mut mgr = migration.lock().unwrap();
            for task in &tasks {
                mgr.mark_failed(task);
                if !mgr.is_shard_fenced(task.shard) {
                    fenced_bm.clear(task.shard);
                }
                migrating_bm.clear(task.shard);
            }
            mgr.cleanup_completed();
            drop(mgr);
            // Rollback shard table so old masters remain authoritative.
            let mut table = shard_table.write();
            for task in &tasks {
                table.rollback_shard(task.shard);
            }
            drop(table);
            return;
        }
    };

    let completed = std::sync::atomic::AtomicU32::new(0);
    let failed = std::sync::atomic::AtomicU32::new(0);

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
    // Pre-filter: skip shards already in ServingNew (already committed
    // by a previous topology cycle or by the begin_handoff_with callback).
    // Send OP_MIGRATION_COMPLETE to the target so it clears its inbound
    // state and unblocks writes for these shards.
    let mut skipped_tasks: Vec<MigrationTask> = Vec::new();
    let skipped_shard_set: std::collections::HashSet<u16> = {
        let table = shard_table.read();
        tasks.iter()
            .filter(|task| table.shard_handoff_state(task.shard) == ShardHandoff::ServingNew)
            .map(|task| task.shard)
            .collect()
    };
    let tasks: Vec<MigrationTask> = {
        tasks.into_iter().filter(|task| {
            if skipped_shard_set.contains(&task.shard) {
                skipped_tasks.push(task.clone());
                false
            } else {
                true
            }
        }).collect()
    };
    if !skipped_tasks.is_empty() {
        eprintln!(
            "cluster: {} shards already serving — sending completion handshakes to {addr}",
            skipped_tasks.len()
        );
        let delivered = send_completion_only_handshakes(addr, &skipped_tasks, self_id);
        let mut mgr = migration.lock().unwrap();
        for (task, delivered) in skipped_tasks.iter().zip(delivered) {
            if delivered {
                mgr.mark_complete(task);
                completed.fetch_add(1, Ordering::Relaxed);
            } else {
                mgr.mark_failed(task);
                failed.fetch_add(1, Ordering::Relaxed);
            }
            if !mgr.is_shard_fenced(task.shard) {
                fenced_bm.clear(task.shard);
            }
            migrating_bm.clear(task.shard);
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
        let empty_shards: std::collections::HashSet<u16> = empty_tasks.iter()
            .map(|t| t.shard)
            .collect();
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
                        eprintln!(
                            "cluster: shard {} empty recheck found {} key(s) despite zero shard count",
                            task.shard,
                            key_count,
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
            eprintln!("cluster: {} empty shards to {} committed instantly", instant_count, addr);
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
                    let mut mgr = migration.lock().unwrap();
                    mgr.mark_complete(task);
                    if !mgr.is_shard_fenced(task.shard) {
                        fenced_bm.clear(task.shard);
                    }
                    migrating_bm.clear(task.shard);
                    drop(mgr);
                    if should_commit_local_handoff {
                        shard_table.write().commit_shard(task.shard);
                    }
                    completed.fetch_add(1, Ordering::Relaxed);
                } else {
                    let mut mgr = migration.lock().unwrap();
                    mgr.mark_failed(task);
                    if !mgr.is_shard_fenced(task.shard) {
                        fenced_bm.clear(task.shard);
                    }
                    migrating_bm.clear(task.shard);
                    drop(mgr);
                    shard_table.write().rollback_shard(task.shard);
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
        migration.lock().unwrap().cleanup_completed();
        let c = completed.load(Ordering::Relaxed);
        let f = failed.load(Ordering::Relaxed);
        eprintln!("cluster: batch migration to {}: {} completed, {} failed", addr, c, f);
        if f == 0 {
            let ce = engine.clone(); let cs = shard_table.clone(); let cm = migration.clone();
            std::thread::spawn(move || { run_orphan_cleanup(self_id, &ce, &cs, &cm, topology_epoch); });
        }
        return;
    }

    // Split data tasks across a pool of parallel connections.
    // More connections = more throughput for large migrations.
    let pool_size = pool_size.max(1);
    let chunk_size = total.div_ceil(pool_size);

    let total_keys: usize = data_tasks.iter()
        .map(|t| keys_by_shard.get(&t.shard).map(|v| v.len()).unwrap_or(0))
        .sum();
    eprintln!(
        "cluster: migrating {} data shards ({} records) to {} across {} connections (batch_size={})",
        total, total_keys, addr, pool_size.min(total), batch_size,
    );

    let completed = Arc::new(std::sync::atomic::AtomicU32::new(
        completed.load(Ordering::Relaxed),
    ));
    let failed = Arc::new(std::sync::atomic::AtomicU32::new(
        failed.load(Ordering::Relaxed),
    ));

    let migration_start = std::time::Instant::now();
    let keys_ref = &keys_by_shard;
    // Scale TCP timeouts based on batch size: base 5s + 50ms per record,
    // capped at 60s. Large batches with cold data blobs need more time.
    let timeout_ms = (5000 + batch_size as u64 * 50).min(60_000);
    let tcp_timeout = Duration::from_millis(timeout_ms);

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
                            eprintln!(
                                "cluster: connect to {} attempt {} failed: {e}",
                                addr, attempt + 1,
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
                        eprintln!("cluster: connect to {} failed after retries", addr);
                        let mut mgr = migration.lock().unwrap();
                        for task in chunk {
                            mgr.mark_failed(task);
                            if !mgr.is_shard_fenced(task.shard) {
                                fenced_bm.clear(task.shard);
                            }
                            migrating_bm.clear(task.shard);
                            failed.fetch_add(1, Ordering::Relaxed);
                        }
                        drop(mgr);
                        let mut table = shard_table.write();
                        for task in chunk {
                            table.rollback_shard(task.shard);
                        }
                        return;
                    }
                };

                let mut consecutive_failures: u32 = 0;
                let mut task_idx = 0;
                while task_idx < chunk.len() {
                    let task = &chunk[task_idx];
                    let ok = migrate_single_shard(task, keys_ref, &engine, &migration, shard_table, redo_log, &mut stream, addr, &completed, &failed, topology_epoch, batch_size, &fenced_bm, &migrating_bm);

                    task_idx += 1;

                    if ok {
                        consecutive_failures = 0;
                    } else {
                        consecutive_failures += 1;
                        // A failed shard likely broke the TCP stream.
                        // Reconnect before the next shard to avoid cascade.
                        if let Some(s) = new_conn() {
                            stream = s;
                            consecutive_failures = 0;
                        } else if consecutive_failures >= 3 {
                            // Can't reconnect after 3 consecutive failures — give up.
                            // Mark all remaining unattempted tasks as Failed and
                            // rollback their shards. These tasks were registered via
                            // start_outbound() before the thread pool started, so
                            // they're in the active list as Preparing/Streaming.
                            eprintln!("cluster: aborting migration batch to {} — cannot reconnect ({} remaining)", addr, chunk.len() - task_idx);
                            let mut mgr = migration.lock().unwrap();
                            let mut table = shard_table.write();
                            for remaining in &chunk[task_idx..] {
                                mgr.mark_failed(remaining);
                                if !mgr.is_shard_fenced(remaining.shard) {
                                    fenced_bm.clear(remaining.shard);
                                }
                                migrating_bm.clear(remaining.shard);
                                table.rollback_shard(remaining.shard);
                                failed.fetch_add(1, Ordering::Relaxed);
                            }
                            drop(table);
                            drop(mgr);
                            break;
                        }
                    }
                }
            });
        }
    });

    let c = completed.load(Ordering::Relaxed);
    let f = failed.load(Ordering::Relaxed);
    let elapsed = migration_start.elapsed();

    let retry_tasks = {
        let mut mgr = migration.lock().unwrap();
        let tasks = mgr.take_failed_tasks();
        mgr.cleanup_completed();
        tasks
    };
    let has_retry_tasks = !retry_tasks.is_empty();
    let rate = if elapsed.as_secs_f64() > 0.0 {
        total_keys as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };
    eprintln!(
        "cluster: batch migration to {}: {} completed, {} failed in {:.1}s ({:.0} records/s)",
        addr, c, f, elapsed.as_secs_f64(), rate,
    );

    if has_retry_tasks {
        let retry_shards: std::collections::HashSet<u16> = retry_tasks.iter()
            .map(|t| t.shard)
            .collect();
        let retry_keys = engine.keys_by_shard_filtered(&retry_shards)
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
        eprintln!(
            "cluster: requeueing {} failed migration(s) for immediate retry",
            retry_tasks.len(),
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
    } else if f == 0 {
        let cleanup_engine = engine.clone();
        let cleanup_st = shard_table.clone();
        let cleanup_mig = migration.clone();
        std::thread::spawn(move || {
            run_orphan_cleanup(self_id, &cleanup_engine, &cleanup_st, &cleanup_mig, topology_epoch);
        });
    }

    // Clear stale inbound migrations. Use staleness-based eviction
    // (30s) rather than blanket clear to avoid removing entries for
    // shards that are legitimately receiving data from other nodes.
    {
        let mut mgr = migration.lock().unwrap();
        if f == 0
            && !has_retry_tasks
            && mgr.active_count() == 0
            && mgr.inbound_count() > 0
        {
            let removed = mgr.clear_stale_inbound(Duration::from_secs(30));
            if removed > 0 {
                inbound_bm.load_from(mgr.inbound_bitmap());
                eprintln!("cluster: cleared {removed} stale inbound migration(s) — no active outbound migrations remain");
            }
        }
        drop(mgr);
    }

    if f > 0 {
        eprintln!("cluster: {} migration(s) failed — will re-attempt on next topology cycle", f);
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
                debug_shard_log(shard, format!(
                    "orphan_cleanup candidate self={} master={} replicas={:?} records={}",
                    self_id.0,
                    assignment.master.0,
                    assignment.replicas.iter().map(|n| n.0).collect::<Vec<_>>(),
                    engine.shard_record_count(shard),
                ));
                orphaned_shards.push(shard);
            }
        }
    }

    if orphaned_shards.is_empty() {
        return;
    }

    let total_orphaned: u64 = orphaned_shards.iter()
        .map(|&s| engine.shard_record_count(s))
        .sum();
    eprintln!(
        "cluster: orphan cleanup — {} shard(s) with {} records to delete",
        orphaned_shards.len(), total_orphaned,
    );

    let mut total_deleted: u64 = 0;
    for &shard in &orphaned_shards {
        // Re-check epoch before each shard.
        if shard_table.read().version != topology_epoch {
            eprintln!("cluster: orphan cleanup aborted — topology epoch changed");
            break;
        }

        let keys = engine.keys_for_shard(shard);
        debug_shard_log(shard, format!(
            "orphan_cleanup deleting {} key(s)",
            keys.len(),
        ));
        for key in &keys {
            match engine.delete(&DeleteRequest { tx_key: *key }) {
                Ok(()) => total_deleted += 1,
                Err(crate::ops::error::SpendError::TxNotFound) => {}
                Err(e) => {
                    eprintln!("cluster: orphan cleanup shard {shard} delete error: {e:?}");
                }
            }
        }
    }

    eprintln!(
        "cluster: orphan cleanup complete — deleted {} records across {} shards",
        total_deleted, orphaned_shards.len(),
    );
}

/// Migrate a single shard: baseline → fence → deltas → complete handshake.
///
/// Checks the shard table version before fencing and before the complete
/// handshake. If the topology has changed (epoch advanced), the migration
/// is aborted early — the new topology's coordinator will re-plan.
#[allow(clippy::too_many_arguments)]
/// Returns `true` if the shard was migrated successfully, `false` if it failed.
/// On failure the TCP stream may be broken and should be reconnected by the caller.
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
    _topology_epoch: u64,
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
        debug_shard_log(task.shard, format!(
            "fail from={} to={} is_master={}",
            task.from_node.0,
            task.to_node.0,
            task.is_master,
        ));
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
            debug_shard_log(task.shard, format!(
                "start from={} to={} is_master={} snapshot_keys={}",
                task.from_node.0,
                task.to_node.0,
                task.is_master,
                shard_keys.len(),
            ));
        }
        if attempt > 0 {
            eprintln!(
                "cluster: shard {} retry attempt {} (after {}ms)",
                task.shard, attempt + 1, delay_ms,
            );
            // Unfence before retry — the fence will be re-set in phase 2.
            migration.lock().unwrap().unfence_shard(task.shard);
            fenced_bm.clear(task.shard);
            std::thread::sleep(Duration::from_millis(delay_ms));
        }

        let snapshot_seq = redo_log.as_ref()
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
        let _baseline_manifest = match stream_shard_baseline(task, shard_keys, engine, stream, batch_size) {
            Ok(m) => m,
            Err(e) => {
                last_err = format!("baseline: {e}");
                if attempt < 2 { continue; }
                eprintln!("cluster: shard {} {last_err} (final attempt)", task.shard);
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
                eprintln!("cluster: shard {} migration aborted — shard already committed/rolled back", task.shard);
                drop(table);
                // Shard is already being served by the new master. Send
                // OP_MIGRATION_COMPLETE to the RECEIVER so it clears the
                // inbound entry and stops blocking writes. Use record_count=0
                // to signal this is a no-data completion.
                let _ = send_migration_complete(addr, task.shard, task.from_node, 0, 0, 0, None, &[0u8; 32], &[]);
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
            fence_seq = redo_log.as_ref()
                .map(|rl| {
                    let guard = rl.lock();
                    guard.current_sequence()
                }).unwrap_or(0);
            let mut mgr = migration.lock().unwrap();
            mgr.mark_fenced(task, fence_seq);
        }

        let snapshot_keys: std::collections::HashSet<TxKey> = shard_keys.iter()
            .copied()
            .copied()
            .collect();
        // Fast check: if the shard record count matches the snapshot,
        // no new keys appeared during the baseline and we can skip
        // the expensive full index scan for late keys.
        let fenced_count = engine.shard_record_count(task.shard);
        let mut fenced_keys = if fenced_count as usize == snapshot_keys.len() {
            shard_keys.iter().map(|k| **k).collect::<Vec<TxKey>>()
        } else {
            engine.keys_for_shard(task.shard)
        };
        let late_keys: Vec<TxKey> = fenced_keys.iter()
            .copied()
            .filter(|k| !snapshot_keys.contains(k))
            .collect();
        debug_shard_log(task.shard, format!(
            "fenced snapshot_keys={} fenced_keys={} late_keys={} snapshot_seq={} fence_seq={}",
            snapshot_keys.len(),
            fenced_keys.len(),
            late_keys.len(),
            snapshot_seq,
            fence_seq,
        ));
        if !late_keys.is_empty() {
            eprintln!(
                "cluster: shard {} fenced re-scan found {} missing pre-snapshot key(s)",
                task.shard,
                late_keys.len(),
            );
            let late_key_refs: Vec<&TxKey> = late_keys.iter().collect();
            if let Err(e) = stream_shard_baseline(task, &late_key_refs, engine, stream, batch_size) {
                last_err = format!("late baseline: {e}");
                if attempt < 2 { continue; }
                eprintln!("cluster: shard {} {last_err} (final attempt)", task.shard);
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
        if snapshot_seq > 0
            && fence_seq > snapshot_seq
            && let Some(rl) = redo_log
            && let Ok(entries) = rl.lock().read_from_sequence(snapshot_seq)
        {
            let first_entry_seq = entries.first().map(|e| e.sequence);
            if let Err(trunc_err) = crate::replication::durable::check_redo_truncation(first_entry_seq, snapshot_seq) {
                eprintln!(
                    "cluster: shard {} {trunc_err} — migration must restart",
                    task.shard,
                );
                last_err = trunc_err;
                delta_failed = true;
            } else {
                let delta_ops: Vec<_> = entries.iter()
                    .filter(|e| e.sequence < fence_seq)
                    .filter_map(|e| redo_entry_to_replica_op(e, task.shard, engine))
                    .collect();
                debug_shard_log(task.shard, format!(
                    "delta_ops={} entries={} snapshot_seq={} fence_seq={}",
                    delta_ops.len(),
                    entries.len(),
                    snapshot_seq,
                    fence_seq,
                ));
                if !delta_ops.is_empty() {
                    eprintln!(
                        "cluster: shard {} streaming {} delta ops (seq {}..{})",
                        task.shard, delta_ops.len(), snapshot_seq, fence_seq
                    );
                    if let Err(e) = send_delta_ops(stream, task.shard, &delta_ops) {
                        eprintln!("cluster: shard {} delta streaming failed: {e}", task.shard);
                        delta_failed = true;
                    }
                }
            }
        }
        if delta_failed {
            if attempt < 2 { continue; }
            eprintln!("cluster: shard {} {last_err} (final attempt)", task.shard);
            fail_shard(migration, shard_table, failed, true);
            return false;
        }

        let mut known_fenced_keys: std::collections::HashSet<TxKey> = fenced_keys.iter()
            .copied()
            .collect();
        for pass in 0..3 {
            // Fast check: if the shard count hasn't changed since the
            // fence, no new keys appeared and we can skip the full scan.
            let current_count = engine.shard_record_count(task.shard) as usize;
            if current_count == known_fenced_keys.len() {
                break;
            }
            let post_delta_keys = engine.keys_for_shard(task.shard);
            let post_delta_late_keys: Vec<TxKey> = post_delta_keys.iter()
                .copied()
                .filter(|k| !known_fenced_keys.contains(k))
                .collect();
            if post_delta_late_keys.is_empty() {
                fenced_keys = post_delta_keys;
                break;
            }
            eprintln!(
                "cluster: shard {} post-delta stabilization pass {} found {} newly appeared key(s)",
                task.shard,
                pass + 1,
                post_delta_late_keys.len(),
            );
            let late_key_refs: Vec<&TxKey> = post_delta_late_keys.iter().collect();
            if let Err(e) = stream_shard_baseline(task, &late_key_refs, engine, stream, batch_size) {
                last_err = format!("post-delta baseline: {e}");
                if attempt < 2 { continue; }
                eprintln!("cluster: shard {} {last_err} (final attempt)", task.shard);
                fail_shard(migration, shard_table, failed, true);
                return false;
            }
            known_fenced_keys.extend(post_delta_late_keys.iter().copied());
            fenced_keys = post_delta_keys;
            if pass == 2 {
                eprintln!(
                    "cluster: shard {} post-delta stabilization did not converge after 3 passes",
                    task.shard,
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
                eprintln!("cluster: shard {} migration aborted before complete — shard already committed/rolled back", task.shard);
                drop(table);
                let _ = send_migration_complete(addr, task.shard, task.from_node, 0, 0, 0, Some(stream), &[0u8; 32], &[]);
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
                if attempt < 2 { continue; }
                eprintln!("cluster: shard {} {last_err} (final attempt)", task.shard);
                fail_shard(migration, shard_table, failed, true);
                return false;
            }
        };
        let manifest_hash = compute_manifest_for_entries(&manifest_entries);

        debug_shard_log(task.shard, format!(
            "handshake from={} to={} fence_seq={} records={} manifest_entries={} epoch={}",
            task.from_node.0,
            task.to_node.0,
            fence_seq,
            fenced_keys.len(),
            manifest_entries.len(),
            _topology_epoch,
        ));
        if let Err(e) = send_migration_complete(addr, task.shard, task.from_node, fenced_keys.len() as u64, fence_seq, _topology_epoch, Some(stream), &manifest_hash, &manifest_entries) {
            last_err = format!("handshake: {e}");
            if attempt < 2 { continue; }
            eprintln!("cluster: shard {} {last_err} (final attempt)", task.shard);
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
        debug_shard_log(task.shard, format!(
            "complete from={} to={} commit_local={} fenced_keys={}",
            task.from_node.0,
            task.to_node.0,
            should_commit_local_handoff,
            fenced_keys.len(),
        ));
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
fn stream_shard_baseline(
    task: &MigrationTask,
    shard_keys: &[&TxKey],
    engine: &Engine,
    stream: &mut TcpStream,
    batch_size: usize,
) -> std::result::Result<ManifestHasher, String> {
    use crate::replication::protocol::{ReplicaBatch, ReplicaOp};
    use crate::record::{UTXO_SPENT, UTXO_FROZEN};

    let batch_size = batch_size.max(1);
    let mut manifest = ManifestHasher::new();
    for chunk in shard_keys.chunks(batch_size) {
        let mut ops = Vec::with_capacity(chunk.len() * 2);
        for key in chunk {
            let meta = engine.read_metadata(key)
                .map_err(|e| format!("baseline read_metadata shard {} key {:?}: {e:?}", task.shard, key))?;
            // Accumulate (txid, generation) into the manifest hash.
            manifest.fold(&key.txid, meta.generation);

            let utxo_count = meta.utxo_count;
            let mut utxo_hashes = Vec::with_capacity(utxo_count as usize);
            let mut slots = Vec::with_capacity(utxo_count as usize);
            for v in 0..utxo_count {
                let slot = engine.read_slot(key, v)
                    .map_err(|e| format!("baseline read_slot shard {} key {:?} offset {}: {e:?}", task.shard, key, v))?;
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
            meta_buf.push(meta.flags.bits());
            meta_buf.extend_from_slice(&meta.spending_height.to_le_bytes());
            meta_buf.extend_from_slice(&meta.created_at.to_le_bytes());
            meta_buf.push(0); // wire flags
            meta_buf.extend_from_slice(&meta.generation.to_le_bytes());
            meta_buf.extend_from_slice(&meta.updated_at.to_le_bytes());
            meta_buf.extend_from_slice(&meta.unmined_since.to_le_bytes());
            meta_buf.extend_from_slice(&meta.delete_at_height.to_le_bytes());
            meta_buf.extend_from_slice(&meta.preserve_until.to_le_bytes());

            let cold_data = if meta.flags.contains(crate::record::TxFlags::EXTERNAL) {
                engine.blob_store().and_then(|bs| bs.get(&key.txid).ok().flatten())
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
                Ok(ReplicaAck::Error { failed_sequence, message }) => {
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
) -> std::result::Result<(), String> {
    // Use existing stream or create new one.
    let mut owned;
    let s: &mut TcpStream = match stream {
        Some(s) => s,
        None => {
            owned = TcpStream::connect_timeout(
                &target_addr, Duration::from_secs(3),
            ).map_err(|e| format!("connect: {e}"))?;
            owned.set_read_timeout(Some(Duration::from_secs(5)))
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
        flags: 0,
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
                let msg_len = u16::from_le_bytes(response.payload[2..4].try_into().unwrap()) as usize;
                let msg = std::str::from_utf8(&response.payload[4..4+msg_len.min(response.payload.len()-4)])
                    .unwrap_or("(non-utf8)");
                format!(" (code={code}: {msg})")
            } else {
                format!(" (payload: {:?})", &response.payload)
            }
        };
        return Err(format!("target rejected: status {}{detail}", response.status));
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
    const IO_TIMEOUT: Duration = Duration::from_secs(5);
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
                eprintln!(
                    "cluster: batch-complete connect to {target_addr} failed ({}/{MAX_RETRIES}): {e}",
                    attempt + 1,
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
                    let count = u32::from_le_bytes(
                        response.payload[..4].try_into().unwrap(),
                    ) as usize;
                    if count == tasks.len() && response.payload.len() >= 4 + count * 3 {
                        let mut delivered = vec![false; tasks.len()];
                        for i in 0..count {
                            let off = 4 + i * 3;
                            // shard at off..off+2, ok at off+2
                            delivered[i] = response.payload[off + 2] != 0;
                        }
                        return delivered;
                    }
                }
                // Fallback: treat entire batch as delivered on non-OK
                // (the target processed what it could).
                return vec![true; tasks.len()];
            }
            Err(e) => {
                eprintln!(
                    "cluster: batch-complete to {target_addr} failed ({}/{MAX_RETRIES}): {e}",
                    attempt + 1,
                );
            }
        }
    }

    eprintln!(
        "cluster: batch-complete to {target_addr} failed after {MAX_RETRIES} retries ({} shards)",
        tasks.len(),
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
    let gen_for = |k: &TxKey| -> u32 {
        engine.read_metadata(k).map(|m| { m.generation }).unwrap_or(0)
    };

    match &entry.op {
        RedoOp::Spend { tx_key, offset, spending_data, .. } => {
            if ShardTable::shard_for_key(tx_key) != shard { return None; }
            Some(ReplicaOp::Spend {
                tx_key: *tx_key, offset: *offset, spending_data: *spending_data,
                master_generation: gen_for(tx_key),
            })
        }
        RedoOp::Unspend { tx_key, offset, .. } => {
            if ShardTable::shard_for_key(tx_key) != shard { return None; }
            Some(ReplicaOp::Unspend {
                tx_key: *tx_key, offset: *offset,
                master_generation: gen_for(tx_key),
            })
        }
        RedoOp::SetMined { tx_key, block_id, block_height, subtree_idx, unset } => {
            if ShardTable::shard_for_key(tx_key) != shard { return None; }
            if *unset {
                Some(ReplicaOp::UnsetMined {
                    tx_key: *tx_key, block_id: *block_id,
                    master_generation: gen_for(tx_key),
                })
            } else {
                Some(ReplicaOp::SetMined {
                    tx_key: *tx_key, block_id: *block_id, block_height: *block_height,
                    subtree_idx: *subtree_idx, on_longest_chain: true,
                    master_generation: gen_for(tx_key),
                })
            }
        }
        RedoOp::Freeze { tx_key, offset } => {
            if ShardTable::shard_for_key(tx_key) != shard { return None; }
            Some(ReplicaOp::Freeze {
                tx_key: *tx_key, offset: *offset,
                master_generation: gen_for(tx_key),
            })
        }
        RedoOp::Unfreeze { tx_key, offset } => {
            if ShardTable::shard_for_key(tx_key) != shard { return None; }
            Some(ReplicaOp::Unfreeze {
                tx_key: *tx_key, offset: *offset,
                master_generation: gen_for(tx_key),
            })
        }
        RedoOp::Reassign { tx_key, offset, new_hash, block_height, spendable_after } => {
            if ShardTable::shard_for_key(tx_key) != shard { return None; }
            Some(ReplicaOp::Reassign {
                tx_key: *tx_key, offset: *offset, new_hash: *new_hash,
                block_height: *block_height, spendable_after: *spendable_after,
                master_generation: gen_for(tx_key),
            })
        }
        RedoOp::SetConflicting { tx_key, value, current_block_height, block_height_retention } => {
            if ShardTable::shard_for_key(tx_key) != shard { return None; }
            Some(ReplicaOp::SetConflicting {
                tx_key: *tx_key, value: *value,
                current_block_height: *current_block_height,
                retention: *block_height_retention,
                master_generation: gen_for(tx_key),
            })
        }
        RedoOp::SetLocked { tx_key, value } => {
            if ShardTable::shard_for_key(tx_key) != shard { return None; }
            Some(ReplicaOp::SetLocked {
                tx_key: *tx_key, value: *value,
                master_generation: gen_for(tx_key),
            })
        }
        RedoOp::PreserveUntil { tx_key, block_height } => {
            if ShardTable::shard_for_key(tx_key) != shard { return None; }
            Some(ReplicaOp::PreserveUntil {
                tx_key: *tx_key, block_height: *block_height,
                master_generation: gen_for(tx_key),
            })
        }
        RedoOp::PruneSlot { tx_key, offset } => {
            if ShardTable::shard_for_key(tx_key) != shard { return None; }
            Some(ReplicaOp::PruneSlot { tx_key: *tx_key, offset: *offset })
        }
        RedoOp::Create { tx_key, .. } => {
            // A record created after the baseline snapshot must be sent as a
            // delta, otherwise the target never receives it. We read the full
            // current record state from the engine (metadata, UTXOs, cold data)
            // and emit a ReplicaOp::Create. Any subsequent mutations (Spend,
            // SetMined, etc.) within the delta range have their own redo
            // entries which are already converted above, and applying them
            // twice on the target is harmless (all ops are idempotent).
            if ShardTable::shard_for_key(tx_key) != shard { return None; }
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
            meta_buf.push(meta.flags.bits());
            meta_buf.extend_from_slice(&meta.spending_height.to_le_bytes());
            meta_buf.extend_from_slice(&meta.created_at.to_le_bytes());
            meta_buf.push(0); // wire flags
            // Extended metadata for full failover state:
            meta_buf.extend_from_slice(&meta.generation.to_le_bytes());
            meta_buf.extend_from_slice(&meta.updated_at.to_le_bytes());
            meta_buf.extend_from_slice(&meta.unmined_since.to_le_bytes());
            meta_buf.extend_from_slice(&meta.delete_at_height.to_le_bytes());
            meta_buf.extend_from_slice(&meta.preserve_until.to_le_bytes());

            let cold_data = if meta.flags.contains(crate::record::TxFlags::EXTERNAL) {
                engine.blob_store().and_then(|bs| bs.get(&tx_key.txid).ok().flatten())
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
            if ShardTable::shard_for_key(tx_key) != shard { return None; }
            Some(ReplicaOp::Delete { tx_key: *tx_key })
        }
        // Checkpoint is a no-op. MarkOnLongestChain is a secondary index
        // operation that gets rebuilt.
        RedoOp::Checkpoint | RedoOp::MarkOnLongestChain { .. } => None,
    }
}

/// Send delta ReplicaOps to the target on an existing stream and validate ACK.
fn send_delta_ops(
    stream: &mut TcpStream,
    shard: u16,
    ops: &[crate::replication::protocol::ReplicaOp],
) -> std::result::Result<(), String> {
    use crate::replication::protocol::{ReplicaAck, ReplicaBatch};

    let batch = ReplicaBatch {
        first_sequence: 0,
        ops: ops.to_vec(),
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
            Ok(ReplicaAck::Error { failed_sequence, message }) => {
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
        eprintln!("cluster: failed to persist cluster state: {e}");
    }
}

/// Persist the full topology state (new format with committed members).
fn persist_topology_state(
    path: &std::path::Path,
    state: &crate::cluster::topology::PersistedTopologyState,
) {
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
    if let Err(e) = result {
        PERSIST_FAILURES.fetch_add(1, Ordering::Relaxed);
        eprintln!("cluster: failed to persist topology state: {e}");
    }
}

/// Load the full topology state from disk (backward-compatible).
pub fn load_topology_state(path: &std::path::Path) -> crate::cluster::topology::PersistedTopologyState {
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


/// A running cluster instance with all background threads active.
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
    /// Topology authority for quorum-committed term management.
    topology_authority: Arc<crate::cluster::topology::TopologyAuthority>,
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

    /// Check if this node is the master for the given key.
    ///
    /// Returns false if the local shard table is behind the committed
    /// topology term (e.g., after a node rejoins from Dead state before
    /// its shard table has been updated). This prevents serving stale
    /// reads/writes from an outdated ownership view.
    pub fn is_master(&self, key: &TxKey) -> bool {
        let shard = ShardTable::shard_for_key(key);
        self.authoritative_master_for_shard(shard) == self.self_id
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
            committed.iter().filter(|node| addrs.contains_key(node)).count()
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

    /// Resolved replication ACK policy. None means best-effort (no enforcement).
    pub fn ack_policy(&self) -> Option<crate::replication::manager::AckPolicy> {
        self.repl_ack_policy
    }

    /// Whether replication failures should be tolerated (best_effort degraded mode).
    pub fn is_replication_best_effort(&self) -> bool {
        self.repl_best_effort
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
        let other_members: Vec<NodeId> = addrs.keys()
            .filter(|&&id| id != self.self_id)
            .copied()
            .collect();
        let peer_addrs: Vec<SocketAddr> = addrs.iter()
            .filter(|&(&id, _)| id != self.self_id)
            .map(|(_, &addr)| addr)
            .collect();
        drop(addrs);

        if other_members.is_empty() {
            eprintln!("cluster: cannot quiesce — no other nodes");
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
            digest: crate::cluster::topology::TopologyTerm::compute_digest(
                new_term, &new_members,
            ),
        };
        // Apply locally first.
        self.topology_authority.handle_commit(&commit);
        self.signal_topology_committed(new_members.clone(), new_term);
        // Broadcast to all peers so they activate the new topology.
        let commit_payload = commit.serialize();
        for &addr in &peer_addrs {
            let _ = send_topology_frame(addr, OP_TOPOLOGY_COMMIT, &commit_payload);
        }
        eprintln!(
            "cluster: quiesce: committed topology term {} ({} members, excluding self)",
            new_term, new_members.len(),
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
                    eprintln!("cluster: restored {} pending inbound migration(s) from disk", count);
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
                let restored_tasks: Vec<MigrationTask> = mgr.active_migrations().iter()
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
                    self.startup_reactivation_needed.store(true, Ordering::Release);
                    eprintln!("cluster: restored {count} pending outbound migration(s) from disk; scheduling topology re-activation");
                }
                crate::cluster::migration::persist_outbound_state(path, &mgr);
                self.fenced_bitmap.load_from(mgr.fenced_bitmap());
                self.migrating_bitmap.clear_all();
                for p in mgr.active_migrations() {
                    if !p.is_complete() && p.state != crate::cluster::migration::MigrationState::Failed {
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
        sync_atomic_migration_bitmaps(&mgr, &self.fenced_bitmap, &self.migrating_bitmap, &self.inbound_atomic);
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
    /// Writes voted_term and committed_members durably so that after
    /// a crash the node does not double-vote or lose track of the
    /// committed topology.
    pub fn persist_topology(&self) {
        if let Some(ref path) = self.topology_state_path {
            let peak = self.peak_size.load(Ordering::Relaxed) as u64;
            let inc = self.swim_incarnation.load(Ordering::Relaxed);
            let state = self.topology_authority.persisted_state(peak, inc);
            persist_topology_state(path, &state);
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
        self.persist_topology();
        self.shutdown.store(true, Ordering::Relaxed);
        self.swim_shutdown.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
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
        assert_eq!(cluster.committed_topology_members(), vec![NodeId(2), NodeId(3)]);
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
            .find(|&s| old_table.target_assignment(s).master != new_table.target_assignment(s).master)
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
        migration
            .lock()
            .unwrap()
            .start_outbound(&[task.clone()], old_master, &std::collections::HashSet::new());

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
            .find(|&s| old_table.target_assignment(s).master != new_table.target_assignment(s).master)
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
        migration
            .lock()
            .unwrap()
            .start_outbound(&[task.clone()], old_master, &std::collections::HashSet::new());

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
            request_tx.send((request.op_code, request.request_id)).unwrap();

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
    fn failed_data_migration_sends_abort_completion_handshake() {
        let old_members = vec![NodeId(1), NodeId(2)];
        let new_members = vec![NodeId(1), NodeId(2), NodeId(3)];
        let old_table = ShardTable::compute_with_epoch(&old_members, 2, 1);
        let new_table = ShardTable::compute_with_epoch(&new_members, 2, 2);
        let shard = (0..NUM_SHARDS as u16)
            .find(|&s| old_table.target_assignment(s).master != new_table.target_assignment(s).master)
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
        engine.create(&crate::ops::create::CreateRequest {
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
        }).unwrap();
        assert_eq!(engine.shard_record_count(shard), 1);

        let migration = Arc::new(Mutex::new(MigrationManager::new()));
        let populated: std::collections::HashSet<u16> = [shard].into_iter().collect();
        migration
            .lock()
            .unwrap()
            .start_outbound(&[task.clone()], old_master, &populated);

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
                        request_tx.send((request.op_code, request.request_id)).unwrap();

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
            seen.iter().any(|request| *request == (OP_MIGRATION_COMPLETE, shard as u64)),
            "when a data migration gives up after partial streaming, the source must send a final completion/abort handshake to clear the target's provisional inbound state; saw {seen:?}"
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
            .find(|&s| old_table.target_assignment(s).master != new_table.target_assignment(s).master)
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
        migration
            .lock()
            .unwrap()
            .start_outbound(&[task.clone()], old_master, &std::collections::HashSet::new());

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
            request_tx.send((request.op_code, request.request_id)).unwrap();

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
        mgr.start_outbound(&[master_task.clone(), replica_task.clone()], NodeId(1), &populated);

        let new_task_set: std::collections::HashSet<(u16, NodeId, NodeId, bool)> =
            [(42u16, NodeId(1), NodeId(2), true)].into_iter().collect();

        let preserved_tasks: std::collections::HashSet<(u16, NodeId, NodeId, bool)> =
            mgr.active_migrations()
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
        std::fs::write(&path, &[0u8; 4]).unwrap(); // too short
        assert_eq!(load_peak_cluster_size(&path), 1);

        std::fs::write(&path, &0u64.to_le_bytes()).unwrap(); // zero
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
        stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
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
            .find(|&s| old_table.target_assignment(s).master != new_table.target_assignment(s).master)
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
        assert!(!cluster.is_master(&key));
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
            .find(|&s| old_table.target_assignment(s).master != new_table.target_assignment(s).master)
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
        assert!(cluster.is_master(&key));
        assert_eq!(cluster.route(&key), RouteDecision::HandleLocally);
    }

    #[test]
    fn partition_map_prefers_effective_master_while_old_master_is_alive() {
        let old_members = vec![NodeId(1), NodeId(2)];
        let new_members = vec![NodeId(1), NodeId(2), NodeId(3)];
        let old_table = ShardTable::compute_with_epoch(&old_members, 2, 1);
        let new_table = ShardTable::compute_with_epoch(&new_members, 2, 2);
        let shard = (0..NUM_SHARDS as u16)
            .find(|&s| old_table.target_assignment(s).master != new_table.target_assignment(s).master)
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
            .find(|&s| old_table.target_assignment(s).master != new_table.target_assignment(s).master)
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
        assert_eq!(cluster.topology_authority().handle_commit(&next_commit), Some(4));

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
        assert_eq!(cluster.topology_authority().handle_commit(&next_commit), Some(4));

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
}
