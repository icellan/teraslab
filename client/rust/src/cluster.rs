//! Cluster-aware routing for TeraSlab.
//!
//! The [`Cluster`] manages a partition map that maps each of the 4096 shards
//! to a specific node. It maintains per-node connection pools and a background
//! refresh task that periodically updates the partition map.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use teraslab::protocol::opcodes::{OP_GET_PARTITION_MAP, STATUS_OK};
use tokio::task::JoinHandle;

use crate::errors::ClientError;
use crate::pool::{ConnPool, PoolConfig};
use crate::types::{NodeInfo, PartitionMap, TxID, NUM_SHARDS};

/// Configuration for cluster-aware routing.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// Seed node addresses for initial bootstrap.
    pub seeds: Vec<String>,
    /// Per-node pool configuration.
    pub pool_config: PoolConfig,
    /// How often to refresh the partition map (default: 30s).
    pub refresh_interval: Duration,
    /// Maximum number of redirect retries per request (default: 3).
    pub max_redirects: u32,
    /// Optional address mapping: server-advertised address -> host-reachable address.
    ///
    /// In Docker or NAT environments, the server advertises its container-internal
    /// address (e.g. `172.30.0.11:3300`) but the client needs to connect via host-mapped
    /// ports (e.g. `127.0.0.1:13300`). This map provides that translation.
    ///
    /// If empty, server-advertised addresses are used as-is.
    pub addr_map: HashMap<String, String>,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            seeds: Vec::new(),
            pool_config: PoolConfig::default(),
            refresh_interval: Duration::from_secs(30),
            max_redirects: 3,
            addr_map: HashMap::new(),
        }
    }
}

impl ClusterConfig {
    /// Resolve a server-advertised address to a host-reachable address
    /// using the addr_map. Returns the input unchanged if no mapping exists.
    pub fn resolve_addr<'a>(&'a self, addr: &'a str) -> &'a str {
        self.addr_map.get(addr).map(|s| s.as_str()).unwrap_or(addr)
    }

    /// Apply defaults for any zero/unset fields.
    fn with_defaults(mut self) -> Self {
        if self.refresh_interval == Duration::ZERO {
            self.refresh_interval = Duration::from_secs(30);
        }
        if self.max_redirects == 0 {
            self.max_redirects = 3;
        }
        self
    }
}

/// Compute the shard number for a transaction ID.
///
/// Matches the Rust server and Go client implementation:
/// `u16::from_le_bytes([txid[0], txid[1]]) & 0x0FFF`
pub fn shard_for_txid(txid: &TxID) -> u16 {
    u16::from_le_bytes([txid[0], txid[1]]) & 0x0FFF
}

/// Cluster manager that routes requests to the correct node based on shard ownership.
///
/// Maintains a partition map, per-node connection pools, and a background
/// refresh task.
pub(crate) struct Cluster {
    /// Cluster configuration.
    config: ClusterConfig,
    /// Current partition map (atomically swapped on refresh).
    part_map: RwLock<Option<PartitionMap>>,
    /// Per-node connection pools, keyed by node ID.
    pools: RwLock<HashMap<u64, Arc<ConnPool>>>,
    /// Mapping from address to node ID (for redirect lookup).
    addr_to_node: RwLock<HashMap<String, u64>>,
    /// Handle to the background refresh task.
    _refresh_task: JoinHandle<()>,
    /// Channel to stop the refresh task.
    close_tx: tokio::sync::watch::Sender<bool>,
}

impl Cluster {
    /// Create a new cluster manager, connecting to seeds and fetching the
    /// initial partition map.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Connection`] if no seed is reachable, or
    /// [`ClientError::Protocol`] if the partition map cannot be decoded.
    pub async fn new(config: ClusterConfig) -> Result<Self, ClientError> {
        let config = config.with_defaults();
        let (close_tx, _close_rx) = tokio::sync::watch::channel(false);

        let pools: RwLock<HashMap<u64, Arc<ConnPool>>> = RwLock::new(HashMap::new());
        let addr_to_node: RwLock<HashMap<String, u64>> = RwLock::new(HashMap::new());
        let part_map: RwLock<Option<PartitionMap>> = RwLock::new(None);

        let cluster = Self {
            config,
            part_map,
            pools,
            addr_to_node,
            _refresh_task: tokio::spawn(async {}), // placeholder, replaced below
            close_tx,
        };

        // Bootstrap from seed nodes.
        cluster.bootstrap_from_seeds().await?;

        // Start background refresh loop. We need to create a new cluster
        // and move the shared state into the task. Since Cluster is not Clone
        // and the task needs access, we use a different approach: extract the
        // shared state into Arcs.
        //
        // Actually, since Cluster is behind an Arc in the Client anyway, and
        // the refresh task is part of Cluster, we'll restructure slightly.
        // The task captures references to the RwLock fields via raw pointers
        // (safe because Cluster outlives the task via JoinHandle).
        //
        // For a cleaner approach, we'll have the refresh task accept cloned
        // config and the RwLock references. But since we can't share &self
        // with a spawned task, let's use a helper approach similar to pool.rs.

        Ok(cluster)
    }

    /// Start the background refresh task. Must be called after construction.
    ///
    /// This is separated from `new` because we need the Cluster to be in an
    /// Arc before we can share it with the task.
    pub fn start_refresh(self: &Arc<Self>) -> JoinHandle<()> {
        let cluster = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(cluster.config.refresh_interval);
            interval.tick().await; // consume immediate first tick
            let mut close_rx = cluster.close_tx.subscribe();
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let _ = cluster.refresh_partition_map().await;
                    }
                    _ = close_rx.changed() => {
                        return;
                    }
                }
            }
        })
    }

    /// Bootstrap the cluster by connecting to seed nodes and fetching the
    /// initial partition map.
    async fn bootstrap_from_seeds(&self) -> Result<(), ClientError> {
        let mut last_err = None;

        for addr in &self.config.seeds {
            let pool = ConnPool::new(addr.clone(), self.config.pool_config.clone());
            let conn = match pool.get().await {
                Ok(c) => c,
                Err(e) => {
                    pool.close().await;
                    last_err = Some(e);
                    continue;
                }
            };

            let resp = match conn
                .round_trip(OP_GET_PARTITION_MAP, 0, Vec::new())
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    pool.close().await;
                    last_err = Some(e);
                    continue;
                }
            };

            if resp.status != STATUS_OK {
                pool.close().await;
                last_err = Some(ClientError::Protocol(format!(
                    "partition map: status {}",
                    resp.status
                )));
                continue;
            }

            let pm = match decode_partition_map(&resp.payload) {
                Ok(pm) => pm,
                Err(e) => {
                    pool.close().await;
                    last_err = Some(e);
                    continue;
                }
            };

            // Set up pools for all nodes in the partition map.
            {
                let mut pools = self.pools.write();
                let mut atn = self.addr_to_node.write();
                for node in &pm.nodes {
                    let resolved = self.config.resolve_addr(&node.addr).to_string();
                    pools.entry(node.id).or_insert_with(|| {
                        Arc::new(ConnPool::new(
                            resolved,
                            self.config.pool_config.clone(),
                        ))
                    });
                    atn.insert(node.addr.clone(), node.id);
                }
            }

            *self.part_map.write() = Some(pm);

            // Close bootstrap pool if it's not one of the known nodes.
            let found = self
                .part_map
                .read()
                .as_ref()
                .map(|pm| pm.nodes.iter().any(|n| n.addr == *addr))
                .unwrap_or(false);
            if !found {
                pool.close().await;
            }

            return Ok(());
        }

        Err(ClientError::Connection(format!(
            "failed to connect to any seed: {}",
            last_err
                .map(|e| e.to_string())
                .unwrap_or_else(|| "no seeds provided".to_string())
        )))
    }

    /// Return the connection pool for the node that owns the given txid's shard.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::NoPartitionMap`] if no map is available, or
    /// [`ClientError::Connection`] if no pool exists for the target node.
    pub fn pool_for_txid(&self, txid: &TxID) -> Result<Arc<ConnPool>, ClientError> {
        self.pool_for_shard(shard_for_txid(txid))
    }

    /// Return a clone of the cached partition map, or `None` if not yet bootstrapped.
    pub fn cached_partition_map(&self) -> Option<PartitionMap> {
        self.part_map.read().clone()
    }

    /// Return a connection pool for any available node (for non-routed operations).
    pub fn any_pool(&self) -> Result<Arc<ConnPool>, ClientError> {
        let pools = self.pools.read();
        pools
            .values()
            .next()
            .cloned()
            .ok_or_else(|| ClientError::Connection("no pools available".to_string()))
    }

    /// Return the connection pool for the master of the given shard.
    fn pool_for_shard(&self, shard: u16) -> Result<Arc<ConnPool>, ClientError> {
        let pm = self.part_map.read();
        let pm = pm.as_ref().ok_or(ClientError::NoPartitionMap)?;

        let node_id = pm.assignments[shard as usize];
        let pools = self.pools.read();
        pools
            .get(&node_id)
            .cloned()
            .ok_or_else(|| {
                ClientError::Connection(format!(
                    "no pool for node {} (shard {})",
                    node_id, shard
                ))
            })
    }

    /// Refresh the partition map by querying any known node.
    ///
    /// # Errors
    ///
    /// Returns an error if no node could provide a valid partition map.
    pub async fn refresh_partition_map(&self) -> Result<(), ClientError> {
        let pools: Vec<Arc<ConnPool>> = {
            let pools = self.pools.read();
            pools.values().cloned().collect()
        };

        let mut last_err = None;

        for pool in &pools {
            let conn = match pool.get().await {
                Ok(c) => c,
                Err(e) => {
                    last_err = Some(e);
                    continue;
                }
            };

            let resp = match conn
                .round_trip(OP_GET_PARTITION_MAP, 0, Vec::new())
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    last_err = Some(e);
                    continue;
                }
            };

            if resp.status != STATUS_OK {
                last_err = Some(ClientError::Protocol(format!(
                    "partition map: status {}",
                    resp.status
                )));
                continue;
            }

            let pm = match decode_partition_map(&resp.payload) {
                Ok(pm) => pm,
                Err(e) => {
                    last_err = Some(e);
                    continue;
                }
            };

            // Update pools for new nodes.
            {
                let mut pools_map = self.pools.write();
                let mut atn = self.addr_to_node.write();
                for node in &pm.nodes {
                    let resolved = self.config.resolve_addr(&node.addr).to_string();
                    pools_map.entry(node.id).or_insert_with(|| {
                        Arc::new(ConnPool::new(
                            resolved,
                            self.config.pool_config.clone(),
                        ))
                    });
                    atn.insert(node.addr.clone(), node.id);
                }
            }

            *self.part_map.write() = Some(pm);
            return Ok(());
        }

        Err(ClientError::Connection(format!(
            "refresh partition map: {}",
            last_err
                .map(|e| e.to_string())
                .unwrap_or_else(|| "no pools".to_string())
        )))
    }

    /// Close all pools and stop the refresh task.
    pub async fn close(&self) {
        let _ = self.close_tx.send(true);

        let pools: Vec<Arc<ConnPool>> = {
            let mut pools = self.pools.write();
            let drained: Vec<Arc<ConnPool>> = pools.drain().map(|(_, v)| v).collect();
            drained
        };
        for p in pools {
            p.close().await;
        }
    }
}

// ---------------------------------------------------------------------------
// Partition map decoding
// ---------------------------------------------------------------------------

/// Decode a partition map from a response payload.
///
/// Format: `[version:8][node_count:4][nodes: id(8)+addr_len(2)+addr x count][assignments: 4096 x 8]`
///
/// # Errors
///
/// Returns [`ClientError::Protocol`] if the data is truncated or malformed.
pub(crate) fn decode_partition_map(data: &[u8]) -> Result<PartitionMap, ClientError> {
    if data.len() < 12 {
        return Err(ClientError::Protocol(format!(
            "partition map: need 12 bytes, have {}",
            data.len()
        )));
    }
    let version = u64::from_le_bytes(data[0..8].try_into().unwrap());
    let node_count = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;
    let mut pos = 12;

    let mut nodes = Vec::with_capacity(node_count);
    for i in 0..node_count {
        if pos + 10 > data.len() {
            return Err(ClientError::Protocol(format!(
                "partition map: truncated node {}",
                i
            )));
        }
        let node_id = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        let addr_len =
            u16::from_le_bytes(data[pos + 8..pos + 10].try_into().unwrap()) as usize;
        pos += 10;
        if pos + addr_len > data.len() {
            return Err(ClientError::Protocol(format!(
                "partition map: truncated node addr {}",
                i
            )));
        }
        let addr = String::from_utf8_lossy(&data[pos..pos + addr_len]).to_string();
        pos += addr_len;
        nodes.push(NodeInfo { id: node_id, addr });
    }

    if pos + NUM_SHARDS * 8 > data.len() {
        return Err(ClientError::Protocol(
            "partition map: truncated shard assignments".to_string(),
        ));
    }
    let mut assignments = Vec::with_capacity(NUM_SHARDS);
    for _ in 0..NUM_SHARDS {
        assignments.push(u64::from_le_bytes(
            data[pos..pos + 8].try_into().unwrap(),
        ));
        pos += 8;
    }

    Ok(PartitionMap {
        version,
        nodes,
        assignments,
    })
}
