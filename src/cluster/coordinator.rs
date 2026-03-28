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
        let initial_table = ShardTable::compute(&members, config.replication_factor);

        let topology_authority = Arc::new(crate::cluster::topology::TopologyAuthority::new(
            config.self_id,
            config.topology_propose_timeout,
        ));
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
                            &peak_size_event,
                            &swim_incarnation_event,
                        );
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
                                        migration_batch_size, &fenced_bm_event, &migrating_bm_event, &inbound_bm_event,
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

                // Periodically evict stale inbound migrations. Entries older
                // than 30s are removed to prevent indefinite write-blocking
                // from abandoned migrations. Fresh entries (< 30s) are
                // preserved so shards actively receiving data stay protected.
                {
                    let mut mgr = migration.lock().unwrap();
                    mgr.cleanup_completed();
                    if mgr.inbound_count() > 0
                        && last_inbound_clear.elapsed() >= Duration::from_secs(5)
                    {
                        let removed = mgr.clear_stale_inbound(Duration::from_secs(30));
                        if removed > 0 {
                            inbound_bm_event.load_from(mgr.inbound_bitmap());
                            eprintln!("cluster: cleared {removed} stale inbound migration(s)");
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
                        let expected = ShardTable::compute_with_epoch(&committed_members, rf, 0);
                        let (mismatched, pending_handoffs) = {
                            let table = shard_table.read();
                            let mismatched = (0..crate::cluster::shards::NUM_SHARDS as u16)
                                .filter(|&shard| {
                                    table.target_assignment(shard).master
                                        != expected.target_assignment(shard).master
                                })
                                .count() as u32;
                            (mismatched, table.pending_handoff_count())
                        };

                        if should_trigger_topology_reactivation(
                            startup_reactivation_due,
                            normal_reactivation_due,
                            mismatched,
                            pending_handoffs,
                        ) {
                            let committed_term = topo_authority_event.committed_term();
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
                                migration_batch_size, &fenced_bm_event, &migrating_bm_event, &inbound_bm_event,
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
                        migration_batch_size, &fenced_bm_event, &migrating_bm_event, &inbound_bm_event,
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
        _migration_pool_size: usize,
        migration_batch_size: usize,
        fenced_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
        migrating_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
        inbound_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
        topology_authority: &Arc<crate::cluster::topology::TopologyAuthority>,
        node_addrs_for_topo: &Arc<RwLock<std::collections::HashMap<NodeId, SocketAddr>>>,
        topology_commit_tx: &std::sync::mpsc::Sender<(Vec<NodeId>, u64)>,
        topology_state_path: &Option<std::path::PathBuf>,
        peak_size: &Arc<std::sync::atomic::AtomicUsize>,
        swim_incarnation: &Arc<std::sync::atomic::AtomicU64>,
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
                            migration_batch_size, fenced_bm, migrating_bm, inbound_bm,
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
                    eprintln!(
                        "cluster: topology stale (local term {local_term}, remote has {remote_term}) — fetching committed topology from peer",
                    );
                    // Fetch the committed topology directly from a reachable
                    // peer instead of re-proposing a new term. This is faster
                    // (no voting round) and doesn't increment the term counter
                    // unnecessarily. We try each peer until one responds with
                    // a valid commit for a term higher than ours.
                    let peers: Vec<SocketAddr> = {
                        let addrs = node_addrs_for_topo.read().unwrap();
                        addrs.iter()
                            .filter(|(id, _)| **id != self_id)
                            .map(|(_, &addr)| addr)
                            .collect()
                    };
                    let mut caught_up = false;
                    for peer_addr in &peers {
                        // Fetch the peer's committed topology via
                        // OP_GET_PARTITION_MAP, which returns the full routing
                        // info including all node addresses and the shard table
                        // version. We extract the member list from the routing
                        // info and construct a synthetic commit to apply locally.
                        if let Ok(payload) = send_topology_frame(*peer_addr, OP_GET_PARTITION_MAP, &[])
                            && let Some(routing) = crate::cluster::routing::RoutingInfo::decode(&payload)
                        {
                            // Use the committed_members from the routing info
                            // (appended by encode_partition_map). These are the
                            // EXACT members that were committed with the remote
                            // term, so the digest will match. If the peer
                            // doesn't include committed_members (older server),
                            // skip it — constructing a synthetic commit from
                            // SWIM-alive nodes could install a topology that
                            // was never quorum-committed. The re-proposal
                            // fallback path (below) will converge safely.
                            let remote_members = if !routing.committed_members.is_empty() {
                                let mut m = routing.committed_members.clone();
                                m.sort();
                                m
                            } else {
                                continue;
                            };
                            if remote_members.len() <= 1 {
                                continue; // Peer is single-node, skip
                            }
                            // Skip if the partition map's term isn't higher
                            // than ours — the peer may not have advanced yet.
                            if routing.shard_table_version <= local_term {
                                continue;
                            }

                            // Construct and apply a synthetic commit.
                            // Use the partition map's version (which equals
                            // the peer's committed_term at the time the map
                            // was built) rather than the SWIM gossip value.
                            // The SWIM value may have advanced beyond the
                            // partition map's snapshot, causing a digest
                            // mismatch if we mix the two.
                            let commit_term = routing.shard_table_version;
                            let synthetic = crate::cluster::topology::TopologyCommit {
                                term: commit_term,
                                proposer: remote_members[0], // deterministic proposer
                                members: remote_members.clone(),
                                digest: crate::cluster::topology::TopologyTerm::compute_digest(
                                    commit_term, &remote_members,
                                ),
                            };
                            if let Some(applied_term) = topology_authority.handle_commit(&synthetic) {
                                eprintln!(
                                    "cluster: catch-up: applied term {} from peer {} ({} members)",
                                    applied_term, peer_addr, remote_members.len(),
                                );
                                if let Some(path) = topology_state_path {
                                    let peak = peak_size.load(Ordering::Relaxed) as u64;
                                    let inc = swim_incarnation.load(Ordering::Relaxed);
                                    persist_topology_state(path, &topology_authority.persisted_state(peak, inc));
                                }
                                topology_epoch.store(commit_term, Ordering::Relaxed);
                                Self::activate_topology(
                                    &remote_members, commit_term, self_id, rf, shard_table, migration,
                                    node_addrs, engine, redo_for_events, max_migration_threads,
                                    migration_batch_size, fenced_bm, migrating_bm, inbound_bm,
                                );
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
                                topology_epoch.store(commit.term, Ordering::Relaxed);
                                topology_authority.handle_commit(&commit);
                                Self::activate_topology(
                                    &commit.members, commit.term, self_id, rf, shard_table, migration,
                                    node_addrs, engine, redo_for_events, max_migration_threads,
                                    migration_batch_size, fenced_bm, migrating_bm, inbound_bm,
                                );
                            } else {
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
                    }
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
        migration_batch_size: usize,
        fenced_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
        migrating_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
        inbound_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
    ) {
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
        let preserved_shards: std::collections::HashSet<u16>;
        {
            let mut mgr = migration.lock().unwrap();
            let old_inbound = mgr.inbound_count();
            let old_active = mgr.active_count();
            let old_failed = mgr.failed_count();

            // Identify preservable migrations: active, not complete/failed,
            // and appearing in the new plan with same source/target.
            preserved_shards = mgr.active_migrations().iter()
                .filter(|p| {
                    p.state != crate::cluster::migration::MigrationState::Complete
                        && p.state != crate::cluster::migration::MigrationState::Failed
                        && new_task_set.contains(&(p.shard, p.from_node, p.to_node, p.is_master))
                })
                .map(|p| p.shard)
                .collect();

            // Cancel only non-preserved migrations.
            let stale_tasks: Vec<MigrationTask> = mgr.active_migrations().iter()
                .filter(|p| {
                    p.state != crate::cluster::migration::MigrationState::Complete
                        && p.state != crate::cluster::migration::MigrationState::Failed
                        && !preserved_shards.contains(&p.shard)
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

            let preserved_count = preserved_shards.len();
            let cancelled = old_active.saturating_sub(preserved_count);
            if old_inbound > 0 || cancelled > 0 || old_failed > 0 {
                eprintln!(
                    "cluster: topology change — preserved {preserved_count}, cancelled {cancelled} active + {old_failed} failed outbound, cleared {old_inbound} inbound",
                );
            }
        }

        // Reset atomic bitmaps for non-preserved shards.
        // For preserved shards, keep their fenced/migrating state.
        if preserved_shards.is_empty() {
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
                .filter(|t| t.from_node == self_id && !preserved_shards.contains(&t.shard))
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

            let snapshot_seq: u64 = redo_for_events
                .as_ref()
                .map(|rl| rl.lock().current_sequence())
                .unwrap_or(0);

            let outbound_shard_set: std::collections::HashSet<u16> = outbound_tasks.iter()
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
                    let local_has_data = engine.shard_record_count(s) > 0;
                    let old_master = old_masters[s as usize];
                    should_begin_handoff_for_shard(
                        s,
                        self_id,
                        old_master,
                        local_has_data,
                        alive_addrs.contains_key(&old_master),
                        &outbound_shard_set,
                    )
                });
                drop(alive_addrs);
            }

            let populated_shards: std::collections::HashSet<u16> = (0..NUM_SHARDS as u16)
                .filter(|&s| engine.shard_record_count(s) > 0)
                .collect();

            {
                let mut mgr = migration.lock().unwrap();
                let new_tasks: Vec<MigrationTask> = all_tasks.iter()
                    .filter(|t| !preserved_shards.contains(&t.shard))
                    .cloned()
                    .collect();
                mgr.start_outbound(&new_tasks, self_id, &populated_shards);
                for t in new_tasks.iter().filter(|t| t.from_node == self_id) {
                    mgr.set_snapshot_sequence(t, snapshot_seq);
                }
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
    batch_size: usize,
    fenced_bm: Arc<crate::cluster::migration::AtomicShardBitmap>,
    migrating_bm: Arc<crate::cluster::migration::AtomicShardBitmap>,
    inbound_bm: Arc<crate::cluster::migration::AtomicShardBitmap>,
    self_id: NodeId,
) {
    if tasks.is_empty() {
        return;
    }

    let mut keys_by_shard: std::collections::HashMap<u16, Vec<TxKey>> =
        std::collections::HashMap::new();
    for key in all_keys {
        keys_by_shard.entry(ShardTable::shard_for_key(&key)).or_default().push(key);
    }

    let mut tasks_by_shard: std::collections::BTreeMap<u16, Vec<MigrationTask>> =
        std::collections::BTreeMap::new();
    for task in tasks {
        tasks_by_shard.entry(task.shard).or_default().push(task);
    }
    let shard_pipelines: Vec<Vec<MigrationTask>> = tasks_by_shard
        .into_values()
        .map(|mut shard_tasks| {
            shard_tasks.sort_by_key(|t| !t.is_master);
            shard_tasks
        })
        .collect();

    let worker_count = shard_pipelines.len().min(max_parallel_migrations.max(1));
    let pending = std::sync::Mutex::new(std::collections::VecDeque::from(shard_pipelines));

    std::thread::scope(|scope| {
        for _ in 0..worker_count {
            let pending = &pending;
            let keys_by_shard = &keys_by_shard;
            let node_addrs = node_addrs.clone();
            let engine = engine.clone();
            let migration = migration.clone();
            let shard_table = shard_table.clone();
            let redo_log = redo_log.clone();
            let fenced_bm = fenced_bm.clone();
            let migrating_bm = migrating_bm.clone();
            let inbound_bm = inbound_bm.clone();

            scope.spawn(move || loop {
                let shard_tasks = {
                    pending.lock().unwrap().pop_front()
                };
                let Some(shard_tasks) = shard_tasks else {
                    break;
                };

                for task in shard_tasks {
                    let target_addr = node_addrs.read().unwrap().get(&task.to_node).copied();
                    let shard_keys = keys_by_shard.get(&task.shard).map(Vec::as_slice).unwrap_or(&[]);
                    run_migration_batch(
                        vec![task],
                        target_addr,
                        shard_keys,
                        engine.clone(),
                        &migration,
                        &shard_table,
                        &redo_log,
                        topology_epoch,
                        1,
                        batch_size,
                        fenced_bm.clone(),
                        migrating_bm.clone(),
                        inbound_bm.clone(),
                        self_id,
                    );
                }
            });
        }
    });
}

    #[allow(dead_code, clippy::too_many_arguments)]
    fn obsolete_handle_event_duplicate(
        event: &ClusterEvent,
        self_id: NodeId,
        rf: u8,
        _max_migration_threads: usize,
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
        peak_size: &Arc<std::sync::atomic::AtomicUsize>,
        swim_incarnation: &Arc<std::sync::atomic::AtomicU64>,
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
                    // Group retries by target and use batched migration.
                    let mut by_target: std::collections::HashMap<NodeId, Vec<MigrationTask>> =
                        std::collections::HashMap::new();
                    for t in retry_tasks {
                        by_target.entry(t.to_node).or_default().push(t);
                    }
                    let epoch = topology_epoch.load(Ordering::Relaxed);
                    for (target_node, tasks) in by_target {
                        let target_addr = node_addrs.read().unwrap().get(&target_node).copied();
                        let migration_ref = migration.clone();
                        // Only snapshot keys for the shards being retried.
                        let retry_shards: std::collections::HashSet<u16> = tasks.iter()
                            .map(|t| t.shard).collect();
                        let keys_map = engine.keys_by_shard_filtered(&retry_shards);
                        let all_keys: Vec<TxKey> = keys_map.values()
                            .flat_map(|v| v.iter().copied()).collect();
                        let eng = engine.clone();
                        let redo = redo_for_events.clone();
                        let st = shard_table.clone();
                        let fb = fenced_bm.clone();
                        let mb = migrating_bm.clone();
                        let ib = inbound_bm.clone();
                        std::thread::spawn(move || {
                            run_migration_batch(tasks, target_addr, &all_keys, eng, &migration_ref, &st, &redo, epoch, migration_pool_size, migration_batch_size, fb, mb, ib, self_id);
                        });
                    }
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
                            node_addrs, engine, redo_for_events, migration_pool_size,
                            migration_batch_size, fenced_bm, migrating_bm, inbound_bm,
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
                    eprintln!(
                        "cluster: topology stale (local term {local_term}, remote has {remote_term}) — fetching committed topology from peer",
                    );
                    // Fetch the committed topology directly from a reachable
                    // peer instead of re-proposing a new term. This is faster
                    // (no voting round) and doesn't increment the term counter
                    // unnecessarily. We try each peer until one responds with
                    // a valid commit for a term higher than ours.
                    let peers: Vec<SocketAddr> = {
                        let addrs = node_addrs_for_topo.read().unwrap();
                        addrs.iter()
                            .filter(|(id, _)| **id != self_id)
                            .map(|(_, &addr)| addr)
                            .collect()
                    };
                    let mut caught_up = false;
                    for peer_addr in &peers {
                        // Fetch the peer's committed topology via
                        // OP_GET_PARTITION_MAP, which returns the full routing
                        // info including all node addresses and the shard table
                        // version. We extract the member list from the routing
                        // info and construct a synthetic commit to apply locally.
                        if let Ok(payload) = send_topology_frame(*peer_addr, OP_GET_PARTITION_MAP, &[])
                            && let Some(routing) = crate::cluster::routing::RoutingInfo::decode(&payload)
                        {
                            // Use the committed_members from the routing info
                            // (appended by encode_partition_map). These are the
                            // EXACT members that were committed with the remote
                            // term, so the digest will match. If the peer
                            // doesn't include committed_members (older server),
                            // skip it — constructing a synthetic commit from
                            // SWIM-alive nodes could install a topology that
                            // was never quorum-committed. The re-proposal
                            // fallback path (below) will converge safely.
                            let remote_members = if !routing.committed_members.is_empty() {
                                let mut m = routing.committed_members.clone();
                                m.sort();
                                m
                            } else {
                                continue;
                            };
                            if remote_members.len() <= 1 {
                                continue; // Peer is single-node, skip
                            }
                            // Skip if the partition map's term isn't higher
                            // than ours — the peer may not have advanced yet.
                            if routing.shard_table_version <= local_term {
                                continue;
                            }

                            // Construct and apply a synthetic commit.
                            // Use the partition map's version (which equals
                            // the peer's committed_term at the time the map
                            // was built) rather than the SWIM gossip value.
                            // The SWIM value may have advanced beyond the
                            // partition map's snapshot, causing a digest
                            // mismatch if we mix the two.
                            let commit_term = routing.shard_table_version;
                            let synthetic = crate::cluster::topology::TopologyCommit {
                                term: commit_term,
                                proposer: remote_members[0], // deterministic proposer
                                members: remote_members.clone(),
                                digest: crate::cluster::topology::TopologyTerm::compute_digest(
                                    commit_term, &remote_members,
                                ),
                            };
                            if let Some(applied_term) = topology_authority.handle_commit(&synthetic) {
                                eprintln!(
                                    "cluster: catch-up: applied term {} from peer {} ({} members)",
                                    applied_term, peer_addr, remote_members.len(),
                                );
                                if let Some(path) = topology_state_path {
                                    let peak = peak_size.load(Ordering::Relaxed) as u64;
                                    let inc = swim_incarnation.load(Ordering::Relaxed);
                                    persist_topology_state(path, &topology_authority.persisted_state(peak, inc));
                                }
                                topology_epoch.store(commit_term, Ordering::Relaxed);
                                Self::activate_topology(
                                    &remote_members, commit_term, self_id, rf, shard_table, migration,
                                    node_addrs, engine, redo_for_events, migration_pool_size,
                                    migration_batch_size, fenced_bm, migrating_bm, inbound_bm,
                                );
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
                                topology_epoch.store(commit.term, Ordering::Relaxed);
                                topology_authority.handle_commit(&commit);
                                Self::activate_topology(
                                    &commit.members, commit.term, self_id, rf, shard_table, migration,
                                    node_addrs, engine, redo_for_events, migration_pool_size,
                                    migration_batch_size, fenced_bm, migrating_bm, inbound_bm,
                                );
                            } else {
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
                    }
                }
            }
        }
    }

    /// Activate a topology: recompute shard table, plan migrations,
    /// begin two-phase handoff, and spawn migration threads.
    ///
    /// Called from the event loop when a MembershipChanged fires.
    /// Extracted as a separate function so it can also be invoked from
    /// a topology commit signal when the quorum protocol is active.
    #[allow(dead_code, clippy::too_many_arguments)]
    fn obsolete_activate_topology_duplicate(
        members: &[NodeId],
        epoch: u64,
        self_id: NodeId,
        rf: u8,
        shard_table: &Arc<ShardTableLock<ShardTable>>,
        migration: &Arc<Mutex<MigrationManager>>,
        node_addrs: &Arc<RwLock<std::collections::HashMap<NodeId, SocketAddr>>>,
        engine: &Arc<Engine>,
        redo_for_events: &Option<Arc<ParkingMutex<RedoLog>>>,
        migration_pool_size: usize,
        migration_batch_size: usize,
        fenced_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
        migrating_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
        inbound_bm: &Arc<crate::cluster::migration::AtomicShardBitmap>,
    ) {
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
        let preserved_shards: std::collections::HashSet<u16>;
        {
            let mut mgr = migration.lock().unwrap();
            let old_inbound = mgr.inbound_count();
            let old_active = mgr.active_count();
            let old_failed = mgr.failed_count();

            // Identify preservable migrations: active, not complete/failed,
            // and appearing in the new plan with same source/target.
            preserved_shards = mgr.active_migrations().iter()
                .filter(|p| {
                    p.state != crate::cluster::migration::MigrationState::Complete
                        && p.state != crate::cluster::migration::MigrationState::Failed
                        && new_task_set.contains(&(p.shard, p.from_node, p.to_node, p.is_master))
                })
                .map(|p| p.shard)
                .collect();

            // Cancel only non-preserved migrations.
            let stale_tasks: Vec<MigrationTask> = mgr.active_migrations().iter()
                .filter(|p| {
                    p.state != crate::cluster::migration::MigrationState::Complete
                        && p.state != crate::cluster::migration::MigrationState::Failed
                        && !preserved_shards.contains(&p.shard)
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

            let preserved_count = preserved_shards.len();
            let cancelled = old_active.saturating_sub(preserved_count);
            if old_inbound > 0 || cancelled > 0 || old_failed > 0 {
                eprintln!(
                    "cluster: topology change — preserved {preserved_count}, cancelled {cancelled} active + {old_failed} failed outbound, cleared {old_inbound} inbound",
                );
            }
        }

        // Reset atomic bitmaps for non-preserved shards.
        // For preserved shards, keep their fenced/migrating state.
        if preserved_shards.is_empty() {
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
                .filter(|t| t.from_node == self_id && !preserved_shards.contains(&t.shard))
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

            let snapshot_seq: u64 = redo_for_events
                .as_ref()
                .map(|rl| rl.lock().current_sequence())
                .unwrap_or(0);

            let outbound_shard_set: std::collections::HashSet<u16> = outbound_tasks.iter()
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
                    let local_has_data = engine.shard_record_count(s) > 0;
                    let old_master = old_masters[s as usize];
                    should_begin_handoff_for_shard(
                        s,
                        self_id,
                        old_master,
                        local_has_data,
                        alive_addrs.contains_key(&old_master),
                        &outbound_shard_set,
                    )
                });
                drop(alive_addrs);
            }

            let populated_shards: std::collections::HashSet<u16> = (0..NUM_SHARDS as u16)
                .filter(|&s| engine.shard_record_count(s) > 0)
                .collect();

            {
                let mut mgr = migration.lock().unwrap();
                let new_tasks: Vec<MigrationTask> = all_tasks.iter()
                    .filter(|t| !preserved_shards.contains(&t.shard))
                    .cloned()
                    .collect();
                mgr.start_outbound(&new_tasks, self_id, &populated_shards);
                for t in new_tasks.iter().filter(|t| t.from_node == self_id) {
                    mgr.set_snapshot_sequence(t, snapshot_seq);
                }
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
                // Key snapshot: O(index_size) scan, potentially slow.
                let pre_swap_keys_by_shard = engine_w.keys_by_shard_filtered(&outbound_shard_set);
                let pre_swap_keys: Arc<Vec<TxKey>> = Arc::new(
                    pre_swap_keys_by_shard.values()
                        .flat_map(|v| v.iter().copied())
                        .collect()
                );

                let master_tasks: Vec<MigrationTask> = outbound_tasks.iter()
                    .filter(|t| t.is_master).cloned().collect();
                let replica_tasks: Vec<MigrationTask> = outbound_tasks.iter()
                    .filter(|t| !t.is_master).cloned().collect();

                let mut masters_by_target: std::collections::HashMap<NodeId, Vec<MigrationTask>> =
                    std::collections::HashMap::new();
                for t in master_tasks {
                    masters_by_target.entry(t.to_node).or_default().push(t);
                }

                let mut master_handles = Vec::new();
                for (target_node, tasks) in masters_by_target {
                    let target_addr = node_addrs_w.read().unwrap().get(&target_node).copied();
                    let migration_ref = migration_w.clone();
                    let all_keys = pre_swap_keys.clone();
                    let eng = engine_w.clone();
                    let redo = redo_w.clone();
                    let st = shard_table_w.clone();
                    let fb = fenced_bm_w.clone();
                    let mb = migrating_bm_w.clone();
                    let ib = inbound_bm_w.clone();

                    let h = std::thread::spawn(move || {
                        run_migration_batch(tasks, target_addr, &all_keys, eng, &migration_ref, &st, &redo, epoch, migration_pool_size, migration_batch_size, fb, mb, ib, self_id);
                    });
                    master_handles.push(h);
                }

                if !replica_tasks.is_empty() {
                    for h in master_handles {
                        let _ = h.join();
                    }

                    let mut replicas_by_target: std::collections::HashMap<NodeId, Vec<MigrationTask>> =
                        std::collections::HashMap::new();
                    for t in replica_tasks {
                        replicas_by_target.entry(t.to_node).or_default().push(t);
                    }

                    let mut handles = Vec::new();
                    for (target_node, tasks) in replicas_by_target {
                        let target_addr = node_addrs_w.read().unwrap().get(&target_node).copied();
                        let migration_ref = migration_w.clone();
                        let all_keys = pre_swap_keys.clone();
                        let eng = engine_w.clone();
                        let redo = redo_w.clone();
                        let st = shard_table_w.clone();
                        let fb = fenced_bm_w.clone();
                        let mb = migrating_bm_w.clone();
                        let ib = inbound_bm_w.clone();

                        handles.push(std::thread::spawn(move || {
                            run_migration_batch(tasks, target_addr, &all_keys, eng, &migration_ref, &st, &redo, epoch, migration_pool_size, migration_batch_size, fb, mb, ib, self_id);
                        }));
                    }
                    for h in handles {
                        let _ = h.join();
                    }
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
    local_has_data
        && (old_master_alive || old_master == self_id || outbound_source_shards.contains(&shard))
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

/// Send a topology-protocol frame to a peer and return the response payload.
///
/// Uses the standard TeraSlab framed TCP protocol with a 3-second connect
/// timeout and 5-second read timeout.
fn send_topology_frame(
    addr: SocketAddr,
    op_code: u16,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    use crate::protocol::frame::{RequestFrame, ResponseFrame};

    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(3))
        .map_err(|e| format!("connect: {e}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| format!("set timeout: {e}"))?;
    crate::replication::tcp_transport::configure_tcp_keepalive(&stream);

    let request = RequestFrame {
        request_id: 0,
        op_code,
        flags: 0,
        payload: payload.to_vec(),
    };
    stream
        .write_all(&request.encode())
        .map_err(|e| format!("write: {e}"))?;

    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .map_err(|e| format!("read length: {e}"))?;
    let total_length = u32::from_le_bytes(len_buf) as usize;
    let mut body = vec![0u8; total_length];
    stream
        .read_exact(&mut body)
        .map_err(|e| format!("read body: {e}"))?;

    let mut full = Vec::with_capacity(4 + total_length);
    full.extend_from_slice(&len_buf);
    full.extend_from_slice(&body);
    let (response, _) = ResponseFrame::decode(&full)
        .map_err(|e| format!("decode: {e}"))?;

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
/// Uses `keys_for_shard()` to get keys, reads each record's generation,
/// and folds `(txid, generation)` into the manifest hash.
/// The result is order-independent and reflects the exact shard content.
fn compute_shard_manifest(engine: &Engine, shard: u16) -> [u8; 32] {
    let mut manifest = ManifestHasher::new();
    for key in engine.keys_for_shard(shard) {
        if let Ok(meta) = engine.read_metadata(&key) {
            manifest.fold(&key.txid, meta.generation);
        }
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
            // Try cleanup even after failure — other batches may have succeeded.
            let ce = engine.clone(); let cs = shard_table.clone(); let cm = migration.clone();
            std::thread::spawn(move || { run_orphan_cleanup(self_id, &ce, &cs, &cm, topology_epoch); });
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
        {
            let mut mgr = migration.lock().unwrap();
            let mut table = shard_table.write();
            for task in &empty_tasks {
                mgr.fence_shard(task.shard);
                fenced_bm.set(task.shard);
                let count = engine.shard_record_count(task.shard);
                if count == 0 {
                    mgr.mark_complete(task);
                    fenced_bm.clear(task.shard);
                    migrating_bm.clear(task.shard);
                    table.commit_shard(task.shard);
                    completed.fetch_add(1, Ordering::Relaxed);
                } else {
                    // Records appeared between snapshot and fence.
                    // Must go through full migration path.
                    mgr.unfence_shard(task.shard);
                    fenced_bm.clear(task.shard);
                    promoted.push(task.clone());
                }
            }
        }
        let instant_count = empty_tasks.len() - promoted.len();
        if instant_count > 0 {
            eprintln!("cluster: {} empty shards to {} committed instantly", instant_count, addr);
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
        let ce = engine.clone(); let cs = shard_table.clone(); let cm = migration.clone();
        std::thread::spawn(move || { run_orphan_cleanup(self_id, &ce, &cs, &cm, topology_epoch); });
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

    migration.lock().unwrap().cleanup_completed();
    let rate = if elapsed.as_secs_f64() > 0.0 {
        total_keys as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };
    eprintln!(
        "cluster: batch migration to {}: {} completed, {} failed in {:.1}s ({:.0} records/s)",
        addr, c, f, elapsed.as_secs_f64(), rate,
    );

    // After migrations complete, spawn orphan cleanup in the background.
    // The active_count() guard inside run_orphan_cleanup ensures only the
    // last batch to finish actually performs the cleanup.
    let cleanup_engine = engine.clone();
    let cleanup_st = shard_table.clone();
    let cleanup_mig = migration.clone();
    std::thread::spawn(move || {
        run_orphan_cleanup(self_id, &cleanup_engine, &cleanup_st, &cleanup_mig, topology_epoch);
    });

    // Clear stale inbound migrations. Use staleness-based eviction
    // (30s) rather than blanket clear to avoid removing entries for
    // shards that are legitimately receiving data from other nodes.
    {
        let mut mgr = migration.lock().unwrap();
        if mgr.active_count() == 0 && mgr.inbound_count() > 0 {
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

    // Guard: skip if migrations are still active.
    {
        let mgr = migration.lock().unwrap();
        if mgr.active_count() > 0 {
            return;
        }
    }

    // Guard: topology must not have changed since the migration started.
    let current_epoch = shard_table.read().version;
    if current_epoch != topology_epoch {
        return;
    }

    let owned_shards = shard_table.read().shards_owned_by(self_id);

    let mut orphaned_shards: Vec<u16> = Vec::new();
    for shard in 0..NUM_SHARDS as u16 {
        if !owned_shards.contains(&shard) && engine.shard_record_count(shard) > 0 {
            orphaned_shards.push(shard);
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
                      failed: &Arc<std::sync::atomic::AtomicU32>| {
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

        // Phase 1: baseline
        let _baseline_manifest = match stream_shard_baseline(task, shard_keys, engine, stream, batch_size) {
            Ok(m) => m,
            Err(e) => {
                last_err = format!("baseline: {e}");
                if attempt < 2 { continue; }
                eprintln!("cluster: shard {} {last_err} (final attempt)", task.shard);
                fail_shard(migration, shard_table, failed);
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
                let _ = send_migration_complete(addr, task.shard, 0, 0, 0, None, &[0u8; 32]);
                fail_shard(migration, shard_table, failed);
                return false;
            }
        }

        // Phase 2: Fence writes BEFORE capturing the redo sequence.
        // This guarantees no write can slip through between the sequence
        // capture and the fence. Any write that arrives between the
        // baseline snapshot and the fence is captured in the delta stream.
        let snapshot_seq;
        let fence_seq;
        {
            let mut mgr = migration.lock().unwrap();
            snapshot_seq = mgr.find_task_mut(task)
                .map(|p| p.snapshot_sequence).unwrap_or(0)
                // If snapshot_sequence was never set (e.g., quiesce path),
                // default to 1 so delta streaming covers all redo entries
                // between baseline and fence. Without this, writes between
                // the key capture and the fence are silently lost.
                .max(1);
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
            fail_shard(migration, shard_table, failed);
            return false;
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
                let _ = send_migration_complete(addr, task.shard, 0, 0, 0, Some(stream), &[0u8; 32]);
                fail_shard(migration, shard_table, failed);
                return false;
            }
        }

        // Compute final manifest hash from engine state (post-fence, post-delta).
        // This is the authoritative fingerprint of the shard's content.
        // The target will compute the same hash from its local state to verify.
        let manifest_hash = compute_shard_manifest(engine, task.shard);

        // Phase 4: complete handshake (now includes manifest hash).
        // Use the current shard table version, not the original topology_epoch,
        // because the epoch may have been bumped by a re-activation cycle.
        let handshake_epoch = shard_table.read().version;
        if let Err(e) = send_migration_complete(addr, task.shard, shard_keys.len() as u64, fence_seq, handshake_epoch, Some(stream), &manifest_hash) {
            last_err = format!("handshake: {e}");
            if attempt < 2 { continue; }
            eprintln!("cluster: shard {} {last_err} (final attempt)", task.shard);
            fail_shard(migration, shard_table, failed);
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
        shard_table.write().commit_shard(task.shard);
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
            // Read the record's metadata and UTXO slots from the engine
            let meta = match engine.read_metadata(key) {
                Ok(m) => m,
                Err(_) => continue,
            };
            // Accumulate (txid, generation) into the manifest hash.
            manifest.fold(&key.txid, meta.generation);

            let utxo_count = meta.utxo_count;
            let mut utxo_hashes = Vec::with_capacity(utxo_count as usize);
            let mut slots = Vec::with_capacity(utxo_count as usize);
            for v in 0..utxo_count {
                match engine.read_slot(key, v) {
                    Ok(slot) => {
                        utxo_hashes.push(slot.hash);
                        slots.push(slot);
                    }
                    Err(_) => {
                        utxo_hashes.push([0u8; 32]);
                        slots.push(crate::record::UtxoSlot::new_unspent([0u8; 32]));
                    }
                }
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

        let frame_bytes = request.encode();
        stream.write_all(&frame_bytes)
            .map_err(|e| format!("write replica batch: {e}"))?;

        // Read response
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf)
            .map_err(|e| format!("read response length: {e}"))?;
        let total_length = u32::from_le_bytes(len_buf) as usize;
        let mut body = vec![0u8; total_length];
        stream.read_exact(&mut body)
            .map_err(|e| format!("read response body: {e}"))?;

        let mut full = Vec::with_capacity(4 + total_length);
        full.extend_from_slice(&len_buf);
        full.extend_from_slice(&body);
        let (response, _) = ResponseFrame::decode(&full)
            .map_err(|e| format!("decode response: {e}"))?;

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
fn send_migration_complete(
    target_addr: SocketAddr,
    shard: u16,
    record_count: u64,
    fence_sequence: u64,
    topology_epoch: u64,
    stream: Option<&mut TcpStream>,
    manifest_hash: &[u8; 32],
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

    // Payload: [record_count:8][fence_sequence:8][topology_epoch:8][manifest_hash:32]
    let mut payload = Vec::with_capacity(56);
    payload.extend_from_slice(&record_count.to_le_bytes());
    payload.extend_from_slice(&fence_sequence.to_le_bytes());
    payload.extend_from_slice(&topology_epoch.to_le_bytes());
    payload.extend_from_slice(manifest_hash);

    let request = RequestFrame {
        request_id: shard as u64,
        op_code: OP_MIGRATION_COMPLETE,
        flags: 0,
        payload,
    };
    s.write_all(&request.encode())
        .map_err(|e| format!("write: {e}"))?;

    let mut len_buf = [0u8; 4];
    s.read_exact(&mut len_buf)
        .map_err(|e| format!("read response length: {e}"))?;
    let total_length = u32::from_le_bytes(len_buf) as usize;
    let mut body = vec![0u8; total_length];
    s.read_exact(&mut body)
        .map_err(|e| format!("read response body: {e}"))?;

    let mut full = Vec::with_capacity(4 + total_length);
    full.extend_from_slice(&len_buf);
    full.extend_from_slice(&body);
    let (response, _) = ResponseFrame::decode(&full)
        .map_err(|e| format!("decode response: {e}"))?;

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
    stream.write_all(&request.encode())
        .map_err(|e| format!("write delta batch: {e}"))?;

    // Read and validate response — same contract as baseline migration.
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)
        .map_err(|e| format!("read delta response length: {e}"))?;
    let total_length = u32::from_le_bytes(len_buf) as usize;
    let mut body = vec![0u8; total_length];
    stream.read_exact(&mut body)
        .map_err(|e| format!("read delta response body: {e}"))?;

    let mut full = Vec::with_capacity(4 + total_length);
    full.extend_from_slice(&len_buf);
    full.extend_from_slice(&body);
    let (response, _) = ResponseFrame::decode(&full)
        .map_err(|e| format!("decode delta response: {e}"))?;

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

/// Persist the peak cluster size to disk (atomic write: temp file + rename).
///
/// Best-effort: errors are logged but do not propagate. The cluster will
/// still function correctly but a restart may lose the quorum guarantee.
/// Persist the cluster state (peak size + topology epoch) to disk.
///
/// File format: `[peak:8 LE][epoch:8 LE]`.
/// Best-effort: errors are logged but do not propagate.
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

/// Migrate all records belonging to a shard to the target node.
///
/// Reads full record data from the local engine and sends it to the target
/// via `OP_REPLICA_BATCH` frames so the target receives complete records
/// (not dummy placeholders).
/// Stream baseline records for a shard to the target node.
///
/// Returns `(record_count, tcp_stream)`. The stream is kept open so the
/// caller can reuse it for delta streaming and the completion handshake,
/// avoiding 3 separate TCP connections per shard.
///
/// For empty shards, returns `(0, None)` — no connection needed.
#[allow(dead_code)]
fn migrate_shard(
    task: &MigrationTask,
    target_addr: SocketAddr,
    all_keys: &[TxKey],
    engine: &Engine,
) -> std::result::Result<(usize, Option<TcpStream>), String> {
    // Filter keys belonging to this shard
    let shard_keys: Vec<&TxKey> = all_keys.iter()
        .filter(|k| ShardTable::shard_for_key(k) == task.shard)
        .collect();

    if shard_keys.is_empty() {
        eprintln!("cluster: shard {} has no records to migrate", task.shard);
        return Ok((0, None));
    }

    eprintln!(
        "cluster: migrating shard {} ({} records) to {}",
        task.shard, shard_keys.len(), target_addr
    );

    // Connect to target node
    let mut stream = TcpStream::connect_timeout(
        &target_addr,
        Duration::from_secs(3),
    ).map_err(|e| format!("connect to {target_addr}: {e}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| format!("set read timeout: {e}"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| format!("set write timeout: {e}"))?;
    crate::replication::tcp_transport::configure_tcp_keepalive(&stream);

    // Build ReplicaOps for each record: Create + Spend/Freeze/SetMined as needed.
    // This ensures the replica receives the full record state, not just the
    // initial creation state.
    use crate::replication::protocol::{ReplicaBatch, ReplicaOp};
    use crate::record::{UTXO_SPENT, UTXO_FROZEN};

    let batch_size = 100;
    for chunk in shard_keys.chunks(batch_size) {
        let mut ops = Vec::with_capacity(chunk.len() * 2);
        for key in chunk {
            // Read the record's metadata and UTXO slots from the engine
            let meta = match engine.read_metadata(key) {
                Ok(m) => m,
                Err(_) => continue, // Record not found locally, skip
            };

            let utxo_count = meta.utxo_count;
            let mut utxo_hashes = Vec::with_capacity(utxo_count as usize);
            let mut slots = Vec::with_capacity(utxo_count as usize);
            for v in 0..utxo_count {
                match engine.read_slot(key, v) {
                    Ok(slot) => {
                        utxo_hashes.push(slot.hash);
                        slots.push(slot);
                    }
                    Err(_) => {
                        utxo_hashes.push([0u8; 32]);
                        slots.push(crate::record::UtxoSlot::new_unspent([0u8; 32]));
                    }
                }
            }

            // Serialize metadata for the replica.
            // Layout: tx_version(4) + locktime(4) + fee(8) + size_in_bytes(8) +
            //   extended_size(8) + flags(1) + spending_height(4) + created_at(8) +
            //   wire_flags(1) + generation(4) + updated_at(8) + unmined_since(4) +
            //   delete_at_height(4) + preserve_until(4) = 70 bytes
            // The receiver parses the first 46 bytes for the core fields and
            // uses remaining bytes for lifecycle/counter state if present.
            let mut meta_buf = Vec::with_capacity(70);
            meta_buf.extend_from_slice(&meta.tx_version.to_le_bytes());
            meta_buf.extend_from_slice(&meta.locktime.to_le_bytes());
            meta_buf.extend_from_slice(&meta.fee.to_le_bytes());
            meta_buf.extend_from_slice(&meta.size_in_bytes.to_le_bytes());
            meta_buf.extend_from_slice(&meta.extended_size.to_le_bytes());
            meta_buf.push(meta.flags.bits());
            meta_buf.extend_from_slice(&meta.spending_height.to_le_bytes());
            meta_buf.extend_from_slice(&meta.created_at.to_le_bytes());
            meta_buf.push(0); // wire flags byte for create
            // Extended metadata for full failover state:
            meta_buf.extend_from_slice(&meta.generation.to_le_bytes());
            meta_buf.extend_from_slice(&meta.updated_at.to_le_bytes());
            meta_buf.extend_from_slice(&meta.unmined_since.to_le_bytes());
            meta_buf.extend_from_slice(&meta.delete_at_height.to_le_bytes());
            meta_buf.extend_from_slice(&meta.preserve_until.to_le_bytes());

            // Include cold data from the blobstore if the record has external
            // data. Without this, replicas/migration targets lose the blob.
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

        let frame_bytes = request.encode();
        stream.write_all(&frame_bytes)
            .map_err(|e| format!("write replica batch: {e}"))?;

        // Read response
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf)
            .map_err(|e| format!("read response length: {e}"))?;
        let total_length = u32::from_le_bytes(len_buf) as usize;
        let mut body = vec![0u8; total_length];
        stream.read_exact(&mut body)
            .map_err(|e| format!("read response body: {e}"))?;

        let mut full = Vec::with_capacity(4 + total_length);
        full.extend_from_slice(&len_buf);
        full.extend_from_slice(&body);
        let (response, _) = ResponseFrame::decode(&full)
            .map_err(|e| format!("decode response: {e}"))?;

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

    // NOTE: OP_MIGRATION_COMPLETE is NOT sent here. It is sent by
    // run_migration_task() AFTER delta streaming completes, so the
    // target does not clear pending-inbound before all data arrives.

    eprintln!("cluster: shard {} baseline streamed ({} records)", task.shard, shard_keys.len());
    Ok((shard_keys.len(), Some(stream)))
}

/// Encode a batch of TxKeys as a CreateBatch payload for migration.
///
/// Uses the standard wire format (`encode_create_batch`) so the target
/// node's `decode_create_batch` can parse it. Each record is created with
/// a single dummy UTXO slot — the actual record data will be synchronized
/// via replication.
#[allow(dead_code)]
fn encode_migration_create_batch(keys: &[&TxKey]) -> Vec<u8> {
    use crate::protocol::codec::{WireCreateItem, encode_create_batch};

    let items: Vec<WireCreateItem> = keys.iter().map(|key| WireCreateItem {
        txid: key.txid,
        tx_version: 1,
        locktime: 0,
        fee: 0,
        size_in_bytes: 0,
        extended_size: 0,
        is_coinbase: false,
        spending_height: 0,
        created_at: 0,
        flags: 0,
        utxo_hashes: vec![[0u8; 32]], // single dummy UTXO
        cold_data: vec![],
        block_height: 0,
        mined_block_id: None,
        mined_block_height: None,
        mined_subtree_idx: None,
        parent_txids: vec![],
    }).collect();

    encode_create_batch(&items)
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
        let table = self.shard_table.read();
        let committed = self.topology_authority.committed_term();
        // If our local shard table was computed from an older topology
        // than the cluster's committed topology, our ownership view is
        // stale. Reject until we've caught up via activate_topology().
        if table.version < committed {
            return false;
        }
        // Use target_assignment so the NEW master accepts requests immediately
        // after topology change, even while handoff is in progress. Using
        // effective_assignment would redirect to the OLD master (which may be
        // dead after a node kill).
        table.target_assignment(shard).master == self.self_id
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
        let assignment = table.target_assignment(shard);

        if assignment.master == self.self_id {
            RouteDecision::HandleLocally
        } else {
            RouteDecision::RedirectTo {
                node: assignment.master,
                shard_table_version: table.version,
            }
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

    /// Register a shard as actively receiving inbound migration data.
    ///
    /// Called when the first `OP_REPLICA_BATCH` for this shard arrives
    /// so the read/write path knows to wait for migration completion.
    /// Persists to disk so a crash mid-migration blocks the shard on restart.
    /// Syncs the atomic bitmap so the hot path sees the change immediately.
    pub fn mark_inbound_active(&self, shard: u16) {
        let mgr = &mut self.migration.lock().unwrap();
        mgr.mark_inbound_active(shard);
        self.inbound_atomic.set(shard);
        if let Some(ref path) = self.inbound_state_path {
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

        let mut buf = Vec::new();
        // Version: use committed topology term (globally agreed) so all
        // nodes that committed the same term report the same version.
        let version = self.topology_authority.committed_term();
        buf.extend_from_slice(&version.to_le_bytes());

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
        // Use target_assignment (committed layout) so clients route to the NEW
        // master immediately. effective_assignment returns the OLD master for
        // shards in Copying state, which may be a dead node — causing clients
        // to get redirected to an unreachable address.
        for shard in 0..crate::cluster::shards::NUM_SHARDS as u16 {
            let master = table.target_assignment(shard).master;
            buf.extend_from_slice(&master.0.to_le_bytes());
        }

        // Append committed topology members for catch-up. This is backward
        // compatible: clients that don't understand the extra data just ignore
        // it. The catch-up code uses these members to construct a synthetic
        // commit with the correct digest (matching the original committed term).
        let committed = self.topology_authority.committed_members();
        buf.extend_from_slice(&(committed.len() as u32).to_le_bytes());
        for m in &committed {
            buf.extend_from_slice(&m.0.to_le_bytes());
        }

        buf
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
            committed.len()
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
        drop(addrs);

        if other_members.is_empty() {
            eprintln!("cluster: cannot quiesce — no other nodes");
            return;
        }

        // Recompute shard table without this node
        let mut members_for_new_table: Vec<NodeId> = other_members;
        members_for_new_table.sort();
        let old_table = self.shard_table.read().clone();
        let new_table = ShardTable::compute(&members_for_new_table, old_table.replication_factor());
        let plan = ShardTable::migration_plan(&old_table, &new_table);

        if plan.is_empty() {
            // No migration needed — just swap.
            *self.shard_table.write() = new_table;
        } else {
            // Two-phase handoff: old masters continue serving until
            // migration completes per-shard.
            let mut table = self.shard_table.write();
            table.begin_handoff(&new_table);

            let outbound: Vec<MigrationTask> = plan.iter()
                .filter(|t| t.from_node == self.self_id)
                .cloned()
                .collect();
            eprintln!(
                "cluster: quiesce initiated — {} outbound migrations (two-phase handoff)",
                outbound.len()
            );
            let all_shards: std::collections::HashSet<u16> = (0..4096).collect();
            drop(table);
            self.migration.lock().unwrap().start_outbound(&plan, self.self_id, &all_shards);
            self.sync_migration_bitmaps();
        }
    }

    /// Get a snapshot of active migration progress.
    pub fn migration_status(&self) -> Vec<crate::cluster::migration::MigrationProgress> {
        self.migration.lock().unwrap().active_migrations().to_vec()
    }

    /// Number of shards pending inbound migration data.
    pub fn inbound_pending_count(&self) -> usize {
        self.migration.lock().unwrap().inbound_count()
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
        self.fenced_bitmap.load_from(mgr.fenced_bitmap());
        self.inbound_atomic.load_from(mgr.inbound_bitmap());
        // Rebuild migrating bitmap from active migrations.
        self.migrating_bitmap.clear_all();
        for p in mgr.active_migrations() {
            if !p.is_complete() && p.state != crate::cluster::migration::MigrationState::Failed {
                self.migrating_bitmap.set(p.shard);
            }
        }
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
            42,
            NodeId(1),
            NodeId(2),
            false,
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
    }

    #[test]
    fn restored_outbound_state_forces_reactivation_even_without_assignment_mismatch() {
        assert!(should_trigger_topology_reactivation(true, false, 0, 0));
        assert!(should_trigger_topology_reactivation(false, true, 0, 1));
        assert!(should_trigger_topology_reactivation(false, true, 2, 0));
        assert!(!should_trigger_topology_reactivation(false, true, 0, 0));
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
}
