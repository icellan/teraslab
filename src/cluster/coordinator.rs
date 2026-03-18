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
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
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
        migration: &Arc<Mutex<MigrationManager>>,
        node_addrs: &RwLock<std::collections::HashMap<NodeId, SocketAddr>>,
        engine: &Engine,
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
                    let outbound_tasks: Vec<MigrationTask> = plan.iter()
                        .filter(|t| t.from_node == self_id)
                        .cloned()
                        .collect();
                    let inbound = plan.iter()
                        .filter(|t| t.to_node == self_id)
                        .count();
                    eprintln!(
                        "cluster: migration plan: {} total moves ({} outbound, {} inbound from this node)",
                        plan.len(), outbound_tasks.len(), inbound
                    );

                    migration.lock().unwrap().start_outbound(&plan, self_id);

                    // Spawn background threads for each outbound migration
                    for task in outbound_tasks {
                        let target_addr = node_addrs.read().unwrap().get(&task.to_node).copied();
                        let migration_ref = migration.clone();
                        let all_keys = engine.all_keys();

                        std::thread::spawn(move || {
                            match target_addr {
                                Some(addr) => {
                                    if let Err(e) = migrate_shard(&task, addr, &all_keys) {
                                        eprintln!(
                                            "cluster: migration of shard {} to {:?} failed: {e}",
                                            task.shard, task.to_node
                                        );
                                    }
                                }
                                None => {
                                    eprintln!(
                                        "cluster: no address for target node {:?}, cannot migrate shard {}",
                                        task.to_node, task.shard
                                    );
                                }
                            }
                            // Mark complete regardless (best-effort migration)
                            let mut mgr = migration_ref.lock().unwrap();
                            mgr.mark_complete(task.shard);
                            mgr.cleanup_completed();
                        });
                    }
                }
            }
            ClusterEvent::NodeSuspect(node) => {
                eprintln!("cluster: node {:?} suspected", node);
            }
        }
    }
}

/// Migrate all records belonging to a shard to the target node.
///
/// Scans the provided key list for records in the given shard, batches them
/// into CreateBatch frames (~100 records each), and sends them to the target
/// node via TCP.
fn migrate_shard(
    task: &MigrationTask,
    target_addr: SocketAddr,
    all_keys: &[TxKey],
) -> Result<(), String> {
    // Filter keys belonging to this shard
    let shard_keys: Vec<&TxKey> = all_keys.iter()
        .filter(|k| ShardTable::shard_for_key(k) == task.shard)
        .collect();

    if shard_keys.is_empty() {
        eprintln!("cluster: shard {} has no records to migrate", task.shard);
        return Ok(());
    }

    eprintln!(
        "cluster: migrating shard {} ({} records) to {}",
        task.shard, shard_keys.len(), target_addr
    );

    // Connect to target node
    let mut stream = TcpStream::connect_timeout(
        &target_addr,
        Duration::from_secs(10),
    ).map_err(|e| format!("connect to {target_addr}: {e}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .map_err(|e| format!("set read timeout: {e}"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(30)))
        .map_err(|e| format!("set write timeout: {e}"))?;

    // Send records in batches of up to 100
    let batch_size = 100;
    for chunk in shard_keys.chunks(batch_size) {
        let payload = encode_migration_create_batch(chunk);

        let request = RequestFrame {
            request_id: task.shard as u64,
            op_code: OP_CREATE_BATCH,
            flags: 0,
            payload,
        };

        let frame_bytes = request.encode();
        stream
            .write_all(&frame_bytes)
            .map_err(|e| format!("write create batch: {e}"))?;

        // Read response
        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .map_err(|e| format!("read response length: {e}"))?;
        let total_length = u32::from_le_bytes(len_buf) as usize;
        let mut body = vec![0u8; total_length];
        stream
            .read_exact(&mut body)
            .map_err(|e| format!("read response body: {e}"))?;

        let mut full = Vec::with_capacity(4 + total_length);
        full.extend_from_slice(&len_buf);
        full.extend_from_slice(&body);
        let (response, _) = ResponseFrame::decode(&full)
            .map_err(|e| format!("decode response: {e}"))?;

        if response.status != STATUS_OK && response.status != STATUS_PARTIAL_ERROR {
            return Err(format!(
                "migration batch failed with status {}",
                response.status
            ));
        }
    }

    eprintln!("cluster: shard {} migration complete ({} records)", task.shard, shard_keys.len());
    Ok(())
}

/// Encode a batch of TxKeys as a minimal CreateBatch payload.
///
/// Each record is created with a single dummy UTXO slot. This is used during
/// shard migration to register the txid on the target node. The actual record
/// data will be synchronized via replication.
///
/// Format: `[count:4][items: txid(32) + utxo_count(4) + utxo_hashes(32*N)]`
fn encode_migration_create_batch(keys: &[&TxKey]) -> Vec<u8> {
    // Each item: txid(32) + utxo_count(4) + 1 hash(32) = 68 bytes
    let mut buf = Vec::with_capacity(4 + keys.len() * 68);
    buf.extend_from_slice(&(keys.len() as u32).to_le_bytes());
    for key in keys {
        buf.extend_from_slice(&key.txid);
        buf.extend_from_slice(&1u32.to_le_bytes()); // utxo_count = 1
        buf.extend_from_slice(&[0u8; 32]); // dummy utxo hash
    }
    buf
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
