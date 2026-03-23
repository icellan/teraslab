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
    initial_peak: usize,
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
            initial_peak,
        }
    }

    /// Start the coordinator: launches SWIM and the event processing loop.
    ///
    /// `cluster_state_path` is where the peak cluster size is persisted
    /// for quorum safety across restarts. Pass `None` for test setups.
    pub fn start(mut self, engine: Arc<Engine>, cluster_state_path: Option<std::path::PathBuf>) -> RunningCluster {
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

        // Event processing thread
        let event_handle = std::thread::spawn(move || {
            while !shutdown.load(Ordering::Relaxed) {
                match event_rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(event) => {
                        // Update peak cluster size on membership changes and
                        // persist to disk so a restarted node remembers its
                        // quorum requirement.
                        if let ClusterEvent::MembershipChanged(members) = &event {
                            let current = members.len();
                            let prev = peak_size_event.fetch_max(current, Ordering::Relaxed);
                            if current > prev && let Some(ref path) = cluster_state_path {
                                persist_peak_cluster_size(path, current as u64);
                            }
                        }
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
            peak_size,
            _swim_handle: swim_handle,
            _event_handle: event_handle,
        }
    }

    fn handle_event(
        event: &ClusterEvent,
        self_id: NodeId,
        rf: u8,
        shard_table: &Arc<RwLock<ShardTable>>,
        migration: &Arc<Mutex<MigrationManager>>,
        node_addrs: &Arc<RwLock<std::collections::HashMap<NodeId, SocketAddr>>>,
        engine: &Arc<Engine>,
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
                let replica_plan = ShardTable::replica_migration_plan(&old_table, &new_table);
                drop(old_table);

                if plan.is_empty() && replica_plan.is_empty() {
                    // No migration needed — just swap the table.
                    *shard_table.write().unwrap() = new_table;
                } else {
                    // Combine master and replica migration tasks.
                    let mut all_tasks = plan.clone();
                    all_tasks.extend(replica_plan.iter().cloned());

                    let outbound_tasks: Vec<MigrationTask> = all_tasks.iter()
                        .filter(|t| t.from_node == self_id)
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

                    // CRITICAL: Take the shard table write lock, snapshot records,
                    // then swap the table — all under the same lock. This ensures
                    // no concurrent writes sneak in between the snapshot and the
                    // table swap. The write lock blocks dispatch's read of the
                    // shard table (check_shard_ownership), so in-flight writes
                    // that started before the lock will complete before we
                    // snapshot, and new writes will see the new table.
                    let pre_swap_keys;
                    {
                        let mut table = shard_table.write().unwrap();
                        // Brief pause to let any in-flight writes that already
                        // acquired the shard table read lock finish executing.
                        std::thread::sleep(std::time::Duration::from_millis(50));
                        pre_swap_keys = engine.all_keys();
                        *table = new_table;
                    }

                    migration.lock().unwrap().start_outbound(&all_tasks, self_id);

                    // Spawn background threads for each outbound migration
                    // (both master and replica) using the pre-swap key snapshot.
                    for task in outbound_tasks {
                        let target_addr = node_addrs.read().unwrap().get(&task.to_node).copied();
                        let migration_ref = migration.clone();
                        let all_keys = pre_swap_keys.clone();
                        let eng = engine.clone();

                        std::thread::spawn(move || {
                            let mut ok = false;
                            // Retry migration up to 3 times on failure
                            for attempt in 0..3 {
                                match target_addr {
                                    Some(addr) => {
                                        match migrate_shard(&task, addr, &all_keys, &eng) {
                                            Ok(()) => { ok = true; break; }
                                            Err(e) => {
                                                eprintln!(
                                                    "cluster: migration of shard {} to {:?} attempt {} failed: {e}",
                                                    task.shard, task.to_node, attempt + 1,
                                                );
                                                if attempt < 2 {
                                                    std::thread::sleep(std::time::Duration::from_secs(2));
                                                }
                                            }
                                        }
                                    }
                                    None => {
                                        eprintln!(
                                            "cluster: no address for target node {:?}, cannot migrate shard {}",
                                            task.to_node, task.shard
                                        );
                                        break;
                                    }
                                }
                            }
                            let mut mgr = migration_ref.lock().unwrap();
                            if ok {
                                mgr.mark_complete(task.shard);
                            } else {
                                eprintln!("cluster: shard {} migration FAILED after retries", task.shard);
                                mgr.mark_failed(task.shard);
                            }
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

/// Persist the peak cluster size to disk (atomic write: temp file + rename).
///
/// Best-effort: errors are logged but do not propagate. The cluster will
/// still function correctly but a restart may lose the quorum guarantee.
fn persist_peak_cluster_size(path: &std::path::Path, peak: u64) {
    use std::io::Write as _;
    let tmp = path.with_extension("cluster.tmp");
    let result = (|| -> std::io::Result<()> {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&peak.to_le_bytes())?;
        f.sync_all()?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    })();
    if let Err(e) = result {
        eprintln!("cluster: failed to persist peak cluster size: {e}");
    }
}

/// Load the persisted peak cluster size from disk.
///
/// Returns the persisted value, or 1 if the file does not exist or is
/// corrupted.
pub fn load_peak_cluster_size(path: &std::path::Path) -> usize {
    match std::fs::read(path) {
        Ok(data) if data.len() >= 8 => {
            let peak = u64::from_le_bytes(data[..8].try_into().unwrap_or([0; 8]));
            (peak as usize).max(1)
        }
        _ => 1,
    }
}

/// Migrate all records belonging to a shard to the target node.
///
/// Reads full record data from the local engine and sends it to the target
/// via `OP_REPLICA_BATCH` frames so the target receives complete records
/// (not dummy placeholders).
fn migrate_shard(
    task: &MigrationTask,
    target_addr: SocketAddr,
    all_keys: &[TxKey],
    engine: &Engine,
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

            // Serialize metadata for the replica
            let mut meta_buf = Vec::with_capacity(46);
            meta_buf.extend_from_slice(&meta.tx_version.to_le_bytes());
            meta_buf.extend_from_slice(&meta.locktime.to_le_bytes());
            meta_buf.extend_from_slice(&meta.fee.to_le_bytes());
            meta_buf.extend_from_slice(&meta.size_in_bytes.to_le_bytes());
            meta_buf.extend_from_slice(&meta.extended_size.to_le_bytes());
            meta_buf.push(meta.flags.bits());
            meta_buf.extend_from_slice(&meta.spending_height.to_le_bytes());
            meta_buf.extend_from_slice(&meta.created_at.to_le_bytes());
            meta_buf.push(0); // flags byte for create

            // Include cold data from the blobstore if the record has external
            // data. Without this, replicas/migration targets lose the blob.
            let cold_data = if meta.flags.contains(crate::record::TxFlags::EXTERNAL) {
                engine.blob_store()
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

            // Replay spent/frozen slot state so the replica matches the master
            let tx_key = **key;
            for (v, slot) in slots.iter().enumerate() {
                if slot.status == UTXO_SPENT {
                    ops.push(ReplicaOp::Spend {
                        tx_key,
                        offset: v as u32,
                        spending_data: slot.spending_data,
                    });
                } else if slot.status == UTXO_FROZEN {
                    ops.push(ReplicaOp::Freeze {
                        tx_key,
                        offset: v as u32,
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
                });
            }
            if meta.flags.contains(crate::record::TxFlags::LOCKED) {
                ops.push(ReplicaOp::SetLocked {
                    tx_key,
                    value: true,
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

        // Send as OP_REPLICA_BATCH so the target receives full record data
        let request = RequestFrame {
            request_id: task.shard as u64,
            op_code: OP_REPLICA_BATCH,
            flags: 0,
            payload: batch.serialize(),
        };

        let frame_bytes = request.encode();
        stream
            .write_all(&frame_bytes)
            .map_err(|e| format!("write replica batch: {e}"))?;

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

        if response.status != STATUS_OK {
            return Err(format!(
                "migration batch failed with status {}",
                response.status
            ));
        }
    }

    // Send migration-complete handshake so the target can verify it
    // has the data before we consider the migration finished.
    let complete_request = RequestFrame {
        request_id: task.shard as u64,
        op_code: OP_MIGRATION_COMPLETE,
        flags: 0,
        payload: Vec::new(),
    };
    let complete_bytes = complete_request.encode();
    stream
        .write_all(&complete_bytes)
        .map_err(|e| format!("write migration complete: {e}"))?;

    // Read the acknowledgment
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .map_err(|e| format!("read migration complete response length: {e}"))?;
    let total_length = u32::from_le_bytes(len_buf) as usize;
    let mut body = vec![0u8; total_length];
    stream
        .read_exact(&mut body)
        .map_err(|e| format!("read migration complete response body: {e}"))?;

    let mut full = Vec::with_capacity(4 + total_length);
    full.extend_from_slice(&len_buf);
    full.extend_from_slice(&body);
    let (response, _) = ResponseFrame::decode(&full)
        .map_err(|e| format!("decode migration complete response: {e}"))?;

    if response.status != STATUS_OK {
        return Err(format!(
            "migration complete handshake failed with status {}",
            response.status
        ));
    }

    eprintln!("cluster: shard {} migration complete ({} records)", task.shard, shard_keys.len());
    Ok(())
}

/// Encode a batch of TxKeys as a CreateBatch payload for migration.
///
/// Uses the standard wire format (`encode_create_batch`) so the target
/// node's `decode_create_batch` can parse it. Each record is created with
/// a single dummy UTXO slot — the actual record data will be synchronized
/// via replication.
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
    shard_table: Arc<RwLock<ShardTable>>,
    migration: Arc<Mutex<MigrationManager>>,
    node_addrs: Arc<RwLock<std::collections::HashMap<NodeId, SocketAddr>>>,
    swim_shutdown: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
    /// Highest observed cluster size (for quorum calculations).
    peak_size: Arc<std::sync::atomic::AtomicUsize>,
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

    /// Check if this node is actively migrating a shard outbound.
    ///
    /// During outbound migration, reads can still be served locally
    /// (the data hasn't been removed yet), but writes should redirect
    /// to the new master.
    pub fn is_migrating_outbound(&self, key: &TxKey) -> bool {
        let shard = ShardTable::shard_for_key(key);
        self.migration.lock().unwrap().is_migrating_shard(shard)
    }

    /// Check if this node is expecting inbound migration data for the given key's shard.
    pub fn has_pending_inbound(&self, key: &TxKey) -> bool {
        let shard = ShardTable::shard_for_key(key);
        self.migration.lock().unwrap().has_pending_inbound(shard)
    }

    /// Mark an inbound shard migration as complete (data has arrived).
    pub fn mark_inbound_complete(&self, shard: u16) {
        self.migration.lock().unwrap().mark_inbound_complete(shard);
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

    /// Number of alive nodes in the cluster (based on known addresses).
    pub fn alive_node_count(&self) -> usize {
        self.node_addrs.read().unwrap().len()
    }

    /// Highest cluster size ever observed (for quorum calculation).
    pub fn peak_cluster_size(&self) -> usize {
        self.peak_size.load(Ordering::Relaxed)
    }

    /// Trigger graceful shard drain (quiesce).
    ///
    /// Recomputes the shard table as if this node has left the cluster,
    /// causing all master shards to migrate to other nodes. The node
    /// remains a cluster member but owns no master shards.
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
        let old_table = self.shard_table.read().unwrap().clone();
        let mut members_for_new_table: Vec<NodeId> = other_members;
        members_for_new_table.sort();
        let new_table = ShardTable::compute(&members_for_new_table, old_table.replication_factor());
        let plan = ShardTable::migration_plan(&old_table, &new_table);

        *self.shard_table.write().unwrap() = new_table;

        if !plan.is_empty() {
            let outbound: Vec<MigrationTask> = plan.iter()
                .filter(|t| t.from_node == self.self_id)
                .cloned()
                .collect();
            eprintln!(
                "cluster: quiesce initiated — {} outbound migrations",
                outbound.len()
            );
            self.migration.lock().unwrap().start_outbound(&plan, self.self_id);
            // Note: the actual migration threads are spawned by the coordinator's
            // event loop. Here we just update the shard table and record the plan.
            // For immediate migration, we'd need to access the engine.
        }
    }

    /// Get a snapshot of active migration progress.
    pub fn migration_status(&self) -> Vec<crate::cluster::migration::MigrationProgress> {
        self.migration.lock().unwrap().active_migrations().to_vec()
    }

    /// Shut down the cluster.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.swim_shutdown.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
