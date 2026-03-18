//! Cluster coordinator: reacts to membership events, recomputes shard
//! table, and coordinates data migration.

use crate::cluster::membership::ClusterEvent;
use crate::cluster::migration::MigrationManager;
use crate::cluster::shards::*;
use crate::cluster::swim::{SwimConfig, SwimRunner};
use crate::index::TxKey;
use crate::ops::engine::Engine;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
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
}

/// The cluster coordinator. Manages membership, shard table, and migrations.
pub struct ClusterCoordinator {
    self_id: NodeId,
    shard_table: Arc<RwLock<ShardTable>>,
    swim: Option<SwimRunner>,
    migration: Arc<Mutex<MigrationManager>>,
    replication_factor: u8,
    node_addrs: Arc<RwLock<std::collections::HashMap<NodeId, SocketAddr>>>,
    shutdown: Arc<AtomicBool>,
}

impl ClusterCoordinator {
    /// Create a new coordinator. Does NOT start the SWIM loop yet.
    pub fn new(config: ClusterConfig) -> Self {
        let mut members = vec![config.self_id];
        members.sort();
        let initial_table = ShardTable::compute(&members, config.replication_factor);

        let swim = SwimRunner::new(SwimConfig {
            self_id: config.self_id,
            self_addr: config.self_addr,
            bind_addr: config.swim_bind,
            seed_nodes: config.seed_nodes,
            probe_interval: config.probe_interval,
            suspicion_timeout: config.suspicion_timeout,
        });

        let mut addrs = std::collections::HashMap::new();
        addrs.insert(config.self_id, config.self_addr);

        Self {
            self_id: config.self_id,
            shard_table: Arc::new(RwLock::new(initial_table)),
            swim: Some(swim),
            migration: Arc::new(Mutex::new(MigrationManager::new())),
            replication_factor: config.replication_factor,
            node_addrs: Arc::new(RwLock::new(addrs)),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Start the coordinator: launches SWIM and the event processing loop.
    pub fn start(mut self, engine: Arc<Engine>) -> RunningCluster {
        let swim = self.swim.take().expect("swim already started");
        let (swim_shutdown, swim_handle, event_rx) = swim.start();

        let shard_table = self.shard_table.clone();
        let migration = self.migration.clone();
        let node_addrs = self.node_addrs.clone();
        let self_id = self.self_id;
        let rf = self.replication_factor;
        let shutdown = self.shutdown.clone();

        // Event processing thread
        let event_handle = std::thread::spawn(move || {
            while !shutdown.load(Ordering::Relaxed) {
                match event_rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(event) => {
                        Self::handle_event(
                            &event,
                            self_id,
                            rf,
                            &shard_table,
                            &migration,
                            &node_addrs,
                            &engine,
                        );
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
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
            _swim_handle: swim_handle,
            _event_handle: event_handle,
        }
    }

    fn handle_event(
        event: &ClusterEvent,
        self_id: NodeId,
        rf: u8,
        shard_table: &RwLock<ShardTable>,
        migration: &Mutex<MigrationManager>,
        node_addrs: &RwLock<std::collections::HashMap<NodeId, SocketAddr>>,
        _engine: &Engine,
    ) {
        match event {
            ClusterEvent::NodeJoined(node, addr) => {
                eprintln!("cluster: node {:?} joined at {addr}", node);
                node_addrs.write().unwrap().insert(*node, *addr);
            }
            ClusterEvent::NodeLeft(node) => {
                eprintln!("cluster: node {:?} left", node);
                node_addrs.write().unwrap().remove(node);
            }
            ClusterEvent::MembershipChanged(members) => {
                eprintln!("cluster: membership changed to {} nodes: {members:?}", members.len());

                let old_table = shard_table.read().unwrap();
                let new_table = ShardTable::compute(members, rf);
                let plan = ShardTable::migration_plan(&old_table, &new_table);
                drop(old_table);

                *shard_table.write().unwrap() = new_table;

                if !plan.is_empty() {
                    let outbound = plan.iter()
                        .filter(|t| t.from_node == self_id)
                        .count();
                    let inbound = plan.iter()
                        .filter(|t| t.to_node == self_id)
                        .count();
                    eprintln!(
                        "cluster: migration plan: {} total moves ({} outbound, {} inbound from this node)",
                        plan.len(), outbound, inbound
                    );

                    migration.lock().unwrap().start_outbound(&plan, self_id);
                    // Migration execution would scan index + stream records here.
                    // For now, mark migrations as complete immediately.
                    for task in &plan {
                        if task.from_node == self_id {
                            migration.lock().unwrap().mark_complete(task.shard);
                        }
                    }
                    migration.lock().unwrap().cleanup_completed();
                }
            }
            ClusterEvent::NodeSuspect(node) => {
                eprintln!("cluster: node {:?} suspected", node);
            }
        }
    }
}

/// A running cluster instance with all background threads active.
pub struct RunningCluster {
    self_id: NodeId,
    shard_table: Arc<RwLock<ShardTable>>,
    migration: Arc<Mutex<MigrationManager>>,
    node_addrs: Arc<RwLock<std::collections::HashMap<NodeId, SocketAddr>>>,
    swim_shutdown: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
    _swim_handle: std::thread::JoinHandle<()>,
    _event_handle: std::thread::JoinHandle<()>,
}

impl RunningCluster {
    /// This node's ID.
    pub fn self_id(&self) -> NodeId {
        self.self_id
    }

    /// Get the current shard table.
    pub fn shard_table(&self) -> Arc<RwLock<ShardTable>> {
        self.shard_table.clone()
    }

    /// Check if this node is the master for the given key.
    pub fn is_master(&self, key: &TxKey) -> bool {
        let shard = ShardTable::shard_for_key(key);
        let table = self.shard_table.read().unwrap();
        table.assignment(shard).master == self.self_id
    }

    /// Determine how to route a request for the given key.
    pub fn route(&self, key: &TxKey) -> RouteDecision {
        let shard = ShardTable::shard_for_key(key);
        let table = self.shard_table.read().unwrap();
        let assignment = table.assignment(shard);

        if assignment.master == self.self_id {
            RouteDecision::HandleLocally
        } else {
            RouteDecision::RedirectTo {
                node: assignment.master,
                shard_table_version: table.version,
            }
        }
    }

    /// Get the address of a node.
    pub fn node_addr(&self, node: &NodeId) -> Option<SocketAddr> {
        self.node_addrs.read().unwrap().get(node).copied()
    }

    /// Get the current shard table version.
    pub fn shard_table_version(&self) -> u64 {
        self.shard_table.read().unwrap().version
    }

    /// Get active migration count.
    pub fn active_migrations(&self) -> usize {
        self.migration.lock().unwrap().active_count()
    }

    /// Encode the partition map for client consumption.
    pub fn encode_partition_map(&self) -> Vec<u8> {
        let table = self.shard_table.read().unwrap();
        let addrs = self.node_addrs.read().unwrap();

        let mut buf = Vec::new();
        // Version
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
        }

        // Shard assignments (4096 entries, each is just the master node_id)
        for shard in 0..NUM_SHARDS as u16 {
            let master = table.assignment(shard).master;
            buf.extend_from_slice(&master.0.to_le_bytes());
        }

        buf
    }

    /// Shut down the cluster.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.swim_shutdown.store(true, Ordering::Relaxed);
    }
}
