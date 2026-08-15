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
use crate::types::{NUM_SHARDS, NodeInfo, PartitionMap, TxID};

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
    /// Optional shared cluster secret for HMAC-signing inter-node opcodes.
    ///
    /// `OP_GET_PARTITION_MAP` is an inter-node auth opcode. When the cluster
    /// runs with `strict_auth` (the production default), the partition-map
    /// bootstrap/refresh requests must carry an HMAC-SHA256 signature or the
    /// server rejects them with `ERR_CLUSTER_AUTH_FAILED`. Set this to the
    /// same secret the cluster nodes use. When `None`, requests are sent
    /// unsigned (only valid against trusted-overlay clusters).
    pub cluster_secret: Option<Vec<u8>>,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            seeds: Vec::new(),
            pool_config: PoolConfig::default(),
            refresh_interval: Duration::from_secs(30),
            max_redirects: 3,
            addr_map: HashMap::new(),
            cluster_secret: None,
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

    /// Fetch and decode the partition map from a single pool.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Connection`] if no connection can be obtained,
    /// or [`ClientError::Protocol`] on a non-OK status or a malformed map.
    async fn fetch_map_from_pool(&self, pool: &ConnPool) -> Result<PartitionMap, ClientError> {
        let conn = pool.get().await?;

        // Strict-auth clusters require the whole inter-node frame to be
        // HMAC-signed (request_id||op||flags||payload); sign via the
        // server's own sign_frame so it verifies byte-for-byte. Unsecured
        // clusters send it unsigned (trusted-overlay default).
        let secret = self
            .config
            .cluster_secret
            .as_deref()
            .filter(|s| !s.is_empty());
        let resp = match secret {
            Some(s) => {
                conn.round_trip_signed(OP_GET_PARTITION_MAP, 0, Vec::new(), s)
                    .await?
            }
            None => conn.round_trip(OP_GET_PARTITION_MAP, 0, Vec::new()).await?,
        };

        if resp.status != STATUS_OK {
            return Err(ClientError::Protocol(format!(
                "partition map: status {}",
                resp.status
            )));
        }
        decode_partition_map(&resp.payload)
    }

    /// Ensure a connection pool (and address mapping) exists for every node
    /// the map advertises as ALIVE. Dead-advertised nodes get no pool:
    /// dialing a node the cluster's failure detector has declared dead only
    /// hands a stale minority view a channel back into the client.
    fn ensure_pools_for_alive_nodes(&self, pm: &PartitionMap) {
        let mut pools = self.pools.write();
        let mut atn = self.addr_to_node.write();
        for node in pm.nodes.iter().filter(|n| n.is_alive) {
            let resolved = self.config.resolve_addr(&node.addr).to_string();
            pools.entry(node.id).or_insert_with(|| {
                Arc::new(ConnPool::new(resolved, self.config.pool_config.clone()))
            });
            atn.insert(node.addr.clone(), node.id);
        }
    }

    /// Drop pools (and address mappings) for nodes that are absent from —
    /// or advertised dead in — the adopted map, so a pruned node cannot
    /// keep winning future refreshes. Returns the removed pools; the caller
    /// closes them outside the lock. In-flight requests holding an `Arc`
    /// clone keep their pool valid until they finish; `close()` wakes its
    /// waiters with [`ClientError::PoolClosed`].
    fn drop_stale_pools(&self, alive_ids: &std::collections::HashSet<u64>) -> Vec<Arc<ConnPool>> {
        let mut removed = Vec::new();
        {
            let mut pools = self.pools.write();
            pools.retain(|id, pool| {
                if alive_ids.contains(id) {
                    true
                } else {
                    removed.push(Arc::clone(pool));
                    false
                }
            });
        }
        self.addr_to_node
            .write()
            .retain(|_, id| alive_ids.contains(id));
        removed
    }

    /// Bootstrap the cluster by connecting to seed nodes and fetching the
    /// initial partition map.
    async fn bootstrap_from_seeds(&self) -> Result<(), ClientError> {
        let mut last_err = None;

        for addr in &self.config.seeds {
            let pool = ConnPool::new(addr.clone(), self.config.pool_config.clone());
            let pm = match self.fetch_map_from_pool(&pool).await {
                Ok(pm) => pm,
                Err(e) => {
                    pool.close().await;
                    last_err = Some(e);
                    continue;
                }
            };

            // Set up pools for the alive nodes in the partition map.
            self.ensure_pools_for_alive_nodes(&pm);

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

    /// Return a connection pool for a server-advertised redirect address.
    ///
    /// The address is first resolved through `addr_map`. If it corresponds to a
    /// known node, the cached pool for that node is reused. Otherwise a short-
    /// lived pool is created for the resolved address.
    pub fn pool_for_redirect_addr(&self, addr: &str) -> Result<Arc<ConnPool>, ClientError> {
        if addr.is_empty() {
            return Err(ClientError::Connection(
                "redirect did not include a target address".to_string(),
            ));
        }

        if let Some(node_id) = self.addr_to_node.read().get(addr).copied()
            && let Some(pool) = self.pools.read().get(&node_id)
        {
            return Ok(pool.clone());
        }

        let resolved = self.config.resolve_addr(addr).to_string();
        let node_id = self.part_map.read().as_ref().and_then(|pm| {
            pm.nodes
                .iter()
                .find(|node| node.addr == addr || self.config.resolve_addr(&node.addr) == resolved)
                .map(|node| node.id)
        });

        if let Some(node_id) = node_id {
            let pool = {
                let mut pools = self.pools.write();
                pools
                    .entry(node_id)
                    .or_insert_with(|| {
                        Arc::new(ConnPool::new(
                            resolved.clone(),
                            self.config.pool_config.clone(),
                        ))
                    })
                    .clone()
            };
            self.addr_to_node.write().insert(addr.to_string(), node_id);
            return Ok(pool);
        }

        Ok(Arc::new(ConnPool::new(
            resolved,
            self.config.pool_config.clone(),
        )))
    }

    /// Maximum number of redirect-retry hops the client should take when a
    /// mutation is redirected (stale shard table). Comes from
    /// [`ClusterConfig::max_redirects`] (defaulted to 3 when unset).
    pub fn max_redirects(&self) -> u32 {
        self.config.max_redirects
    }

    /// Return a clone of the cached partition map, or `None` if not yet bootstrapped.
    pub fn cached_partition_map(&self) -> Option<PartitionMap> {
        self.part_map.read().clone()
    }

    /// Return a connection pool for each distinct node currently known to the
    /// cluster.
    ///
    /// Used by cluster-wide fan-out queries (`query_old_unmined` /
    /// `query_conflicting`): the server filters each node's response to the
    /// shards it masters, so the deduplicated union across every node's pool is
    /// the cluster-wide answer. The order is unspecified (backed by a
    /// `HashMap`), which is fine because the caller dedups.
    pub fn all_pools(&self) -> Vec<Arc<ConnPool>> {
        self.pools.read().values().cloned().collect()
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

    /// Test-only: force the cached partition map to assign `shard` to
    /// `node_id`. Used to reproduce a stale/rebalancing map where a shard
    /// points at a node the client has no pool for (node-down state).
    #[cfg(test)]
    pub(crate) fn test_assign_shard(&self, shard: u16, node_id: u64) {
        if let Some(pm) = self.part_map.write().as_mut() {
            pm.assignments[shard as usize] = node_id;
        }
    }

    /// Return the connection pool for the master of the given shard.
    fn pool_for_shard(&self, shard: u16) -> Result<Arc<ConnPool>, ClientError> {
        let pm = self.part_map.read();
        let pm = pm.as_ref().ok_or(ClientError::NoPartitionMap)?;

        let node_id = pm.assignments[shard as usize];
        let pools = self.pools.read();
        pools.get(&node_id).cloned().ok_or_else(|| {
            ClientError::Connection(format!("no pool for node {} (shard {})", node_id, shard))
        })
    }

    /// Refresh the partition map by polling EVERY known node and adopting
    /// the highest-version map.
    ///
    /// The topology version is globally monotonic, so the highest version is
    /// the freshest cluster view. Polling all pools (instead of returning on
    /// the first answer) prevents a stale minority-side node — still
    /// reachable from the client even though the majority has fenced it —
    /// from pinning the client to its pre-partition shard table on every
    /// refresh. Individual pool failures are tolerated; pools for nodes the
    /// adopted map no longer lists as alive are dropped so a pruned node
    /// cannot win a future refresh.
    ///
    /// # Errors
    ///
    /// Returns an error only if no node (nor seed) could provide a valid
    /// partition map.
    pub async fn refresh_partition_map(&self) -> Result<(), ClientError> {
        let pools: Vec<Arc<ConnPool>> = {
            let pools = self.pools.read();
            pools.values().cloned().collect()
        };

        let mut best: Option<(String, PartitionMap)> = None;
        let mut last_err = None;

        for pool in &pools {
            match self.fetch_map_from_pool(pool).await {
                Ok(pm) => {
                    if best.as_ref().is_none_or(|(_, b)| pm.version > b.version) {
                        best = Some((pool.addr().to_string(), pm));
                    }
                }
                Err(e) => last_err = Some(e),
            }
        }

        // All known pools failed — fall back to seed nodes, again adopting
        // the highest version among the answers. This handles the case where
        // all cached pools point to dead nodes but the surviving nodes are
        // reachable via the original seeds.
        if best.is_none() {
            for seed in &self.config.seeds {
                let pool = ConnPool::new(seed.clone(), self.config.pool_config.clone());
                let res = self.fetch_map_from_pool(&pool).await;
                pool.close().await;
                match res {
                    Ok(pm) => {
                        if best.as_ref().is_none_or(|(_, b)| pm.version > b.version) {
                            best = Some((seed.clone(), pm));
                        }
                    }
                    Err(e) => last_err = Some(e),
                }
            }
        }

        let Some((source_addr, pm)) = best else {
            return Err(ClientError::Connection(format!(
                "refresh partition map: {}",
                last_err
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "no pools".to_string())
            )));
        };

        // Log partition map details (and which node's map won) for debugging.
        let unique_masters: std::collections::BTreeSet<u64> =
            pm.assignments.iter().copied().collect();
        let node_ids: std::collections::BTreeSet<u64> =
            pm.nodes.iter().map(|node| node.id).collect();
        let dangling_masters: Vec<u64> = unique_masters
            .iter()
            .copied()
            .filter(|master| !node_ids.contains(master))
            .collect();
        tracing::debug!(
            version = pm.version,
            nodes = pm.nodes.len(),
            ?node_ids,
            ?unique_masters,
            ?dangling_masters,
            source = %source_addr,
            "client: refreshed partition map (freshest of all answering nodes)",
        );

        // Create pools for newly-alive nodes BEFORE swapping the map in (so
        // the new map never routes to a missing pool), and drop stale pools
        // AFTER (so the old map never routes to a dropped pool).
        self.ensure_pools_for_alive_nodes(&pm);
        let alive_ids: std::collections::HashSet<u64> = pm
            .nodes
            .iter()
            .filter(|n| n.is_alive)
            .map(|n| n.id)
            .collect();
        *self.part_map.write() = Some(pm);
        let stale = self.drop_stale_pools(&alive_ids);
        for pool in stale {
            pool.close().await;
        }
        Ok(())
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
        let addr_len = u16::from_le_bytes(data[pos + 8..pos + 10].try_into().unwrap()) as usize;
        pos += 10;
        if pos + addr_len > data.len() {
            return Err(ClientError::Protocol(format!(
                "partition map: truncated node addr {}",
                i
            )));
        }
        let addr = String::from_utf8_lossy(&data[pos..pos + addr_len]).to_string();
        pos += addr_len;
        // is_alive byte (part of the RoutingInfo wire format): the server
        // advertises 0 for nodes its failure detector has declared dead
        // (C21). Tolerate its absence by defaulting to alive, matching the
        // previous lenient skip.
        let is_alive = if pos < data.len() {
            let alive = data[pos] != 0;
            pos += 1;
            alive
        } else {
            true
        };
        nodes.push(NodeInfo {
            id: node_id,
            addr,
            is_alive,
        });
    }

    if pos + NUM_SHARDS * 8 > data.len() {
        return Err(ClientError::Protocol(
            "partition map: truncated shard assignments".to_string(),
        ));
    }
    let mut assignments = Vec::with_capacity(NUM_SHARDS);
    for _ in 0..NUM_SHARDS {
        assignments.push(u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()));
        pos += 8;
    }

    Ok(PartitionMap {
        version,
        nodes,
        assignments,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use teraslab::protocol::frame::RequestFrame;

    /// Reproduce exactly what `PipeConn::send_frame` writes for a signed
    /// inter-node opcode: encode the frame, then `sign_frame` the whole encoded
    /// body. This is the canonical helper used by the test below.
    fn client_signed_frame(secret: &[u8], request_id: u64) -> Vec<u8> {
        let frame = RequestFrame {
            request_id,
            op_code: OP_GET_PARTITION_MAP,
            flags: 0,
            payload: Vec::new().into(),
        };
        teraslab::cluster::auth::sign_frame(secret, &frame.encode())
            .expect("sign_frame must succeed")
    }

    /// REL-010: the bytes the client puts on the wire for a signed
    /// `OP_GET_PARTITION_MAP` must pass the SERVER's whole-frame verify gate
    /// (`verify_frame`, the same primitive `verify_signed_body_streaming` uses),
    /// recovering the original frame byte-for-byte. Signing only the payload (the
    /// previous approach) fails this gate because the server HMACs the entire
    /// frame body `request_id||op_code||flags||payload`.
    #[test]
    fn signed_frame_verifies_against_server_gate() {
        let secret = b"super-secret-cluster-key";
        let frame = RequestFrame {
            request_id: 7,
            op_code: OP_GET_PARTITION_MAP,
            flags: 0,
            payload: Vec::new().into(),
        };
        let encoded = frame.encode();
        let signed =
            teraslab::cluster::auth::sign_frame(secret, &encoded).expect("sign_frame must succeed");

        let recovered = teraslab::cluster::auth::verify_frame(secret, &signed)
            .expect("server gate must accept the client-signed frame");
        assert_eq!(
            recovered, encoded,
            "verify_frame must recover the exact original frame (incl. request_id)",
        );
    }

    /// A different secret must NOT verify — confirms the tag is keyed.
    #[test]
    fn signed_frame_wrong_secret_rejected() {
        let signed = client_signed_frame(b"right-key", 1);
        let err = teraslab::cluster::auth::verify_frame(b"wrong-key", &signed)
            .expect_err("verification under the wrong key must fail");
        assert_eq!(
            err.kind(),
            std::io::ErrorKind::PermissionDenied,
            "wrong-key verification must be rejected as PermissionDenied",
        );
    }

    /// The signature covers the `request_id`: tampering with it after signing
    /// breaks verification. This is precisely why payload-only signing failed —
    /// the server HMACs the request_id that arrives on the wire.
    #[test]
    fn signed_frame_covers_request_id() {
        let secret = b"cluster-key";
        let mut signed = client_signed_frame(secret, 42);
        // Frame layout: [length:4][request_id:8][op:2][flags:2][payload..][ts:8][tag:32].
        // Flip a bit in the request_id region (bytes 4..12).
        signed[4] ^= 0x01;
        let err = teraslab::cluster::auth::verify_frame(secret, &signed)
            .expect_err("a tampered request_id must fail whole-frame verification");
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
    }

    // -----------------------------------------------------------------------
    // Partition-map freshness + liveness (CI scenario 08: a stale minority
    // node kept winning every refresh, pinning the client's routing table to
    // the minority-side shard assignments for the whole partition window).
    // -----------------------------------------------------------------------

    /// Encode a partition map in the wire format `decode_partition_map`
    /// expects: `[version:8][node_count:4][id:8 + addr_len:2 + addr +
    /// is_alive:1 per node][4096 x master:8]`. Shard assignments are
    /// round-robined across `masters`.
    fn encode_map(version: u64, nodes: &[(u64, &str, bool)], masters: &[u64]) -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&version.to_le_bytes());
        p.extend_from_slice(&(nodes.len() as u32).to_le_bytes());
        for (id, addr, alive) in nodes {
            p.extend_from_slice(&id.to_le_bytes());
            p.extend_from_slice(&(addr.len() as u16).to_le_bytes());
            p.extend_from_slice(addr.as_bytes());
            p.push(u8::from(*alive));
        }
        for shard in 0..NUM_SHARDS {
            p.extend_from_slice(&masters[shard % masters.len()].to_le_bytes());
        }
        p
    }

    /// In-process mock node: answers every `OP_GET_PARTITION_MAP` with the
    /// current contents of `payload` (swappable mid-test) and any other op
    /// with an empty OK. `kill()` drops the listener and every accepted
    /// socket so the node becomes fully unreachable.
    struct MapServer {
        addr: String,
        payload: Arc<RwLock<Vec<u8>>>,
        kill_tx: tokio::sync::watch::Sender<bool>,
    }

    impl MapServer {
        fn set_map(&self, map: Vec<u8>) {
            *self.payload.write() = map;
        }

        fn kill(&self) {
            let _ = self.kill_tx.send(true);
        }
    }

    async fn spawn_map_server() -> MapServer {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let payload: Arc<RwLock<Vec<u8>>> = Arc::new(RwLock::new(Vec::new()));
        let (kill_tx, kill_rx) = tokio::sync::watch::channel(false);
        let served = Arc::clone(&payload);
        let mut accept_kill = kill_rx.clone();
        tokio::spawn(async move {
            loop {
                let accepted = tokio::select! {
                    r = listener.accept() => r,
                    _ = accept_kill.changed() => return,
                };
                let (mut sock, _) = match accepted {
                    Ok(x) => x,
                    Err(_) => return,
                };
                let served = Arc::clone(&served);
                let mut conn_kill = kill_rx.clone();
                tokio::spawn(async move {
                    loop {
                        let mut len_buf = [0u8; 4];
                        let read = tokio::select! {
                            r = sock.read_exact(&mut len_buf) => r,
                            _ = conn_kill.changed() => return,
                        };
                        if read.is_err() {
                            return;
                        }
                        let total = u32::from_le_bytes(len_buf) as usize;
                        if total < 12 {
                            return;
                        }
                        let mut body = vec![0u8; total];
                        if sock.read_exact(&mut body).await.is_err() {
                            return;
                        }
                        let request_id = u64::from_le_bytes(body[0..8].try_into().unwrap());
                        let op_code = u16::from_le_bytes([body[8], body[9]]);
                        let resp_payload = match op_code {
                            OP_GET_PARTITION_MAP => served.read().clone(),
                            _ => Vec::new(),
                        };
                        // Response frame: [inner_len:4][request_id:8][status:1][payload].
                        let inner_len = 8 + 1 + resp_payload.len();
                        let mut out = (inner_len as u32).to_le_bytes().to_vec();
                        out.extend_from_slice(&request_id.to_le_bytes());
                        out.push(STATUS_OK);
                        out.extend_from_slice(&resp_payload);
                        if sock.write_all(&out).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        MapServer {
            addr,
            payload,
            kill_tx,
        }
    }

    /// Build a bootstrapped `Cluster` (no background refresh task) seeded
    /// from the given addresses, with short dial timeouts for test speed.
    async fn new_test_cluster(seeds: Vec<String>) -> Cluster {
        Cluster::new(ClusterConfig {
            seeds,
            pool_config: PoolConfig {
                min_conns: 1,
                max_conns: 2,
                dial_timeout: Duration::from_millis(500),
                health_check: Duration::from_secs(3600),
                request_timeout: Duration::from_secs(5),
            },
            refresh_interval: Duration::from_secs(3600),
            ..ClusterConfig::default()
        })
        .await
        .expect("bootstrap must succeed")
    }

    fn cached_masters(cluster: &Cluster) -> std::collections::BTreeSet<u64> {
        cluster
            .cached_partition_map()
            .expect("map must be cached")
            .assignments
            .iter()
            .copied()
            .collect()
    }

    /// The `is_alive` byte the server encodes per node (C21: dead nodes are
    /// advertised `0`) must survive decoding instead of being discarded.
    #[test]
    fn decode_partition_map_preserves_is_alive() {
        let payload = encode_map(
            7,
            &[
                (1, "10.0.0.1:3300", true),
                (2, "10.0.0.2:3300", false),
                (3, "10.0.0.3:3300", true),
            ],
            &[1, 3],
        );
        let pm = decode_partition_map(&payload).expect("map must decode");
        assert_eq!(pm.version, 7);
        let liveness: Vec<(u64, bool)> = pm.nodes.iter().map(|n| (n.id, n.is_alive)).collect();
        assert_eq!(
            liveness,
            vec![(1, true), (2, false), (3, true)],
            "decode must carry the advertised is_alive byte through to NodeInfo",
        );
    }

    /// Scenario-08 regression: two nodes still answer with a STALE v2 map
    /// (3 masters, including the pruned node 3) while one node answers with
    /// the FRESH v3 map (2 masters). The refresh must adopt v3 regardless of
    /// pool iteration order, and must drop the pruned node's pool so it
    /// cannot win any future refresh. Each loop iteration builds a fresh
    /// `Cluster` (fresh randomly-seeded pool HashMap), driving many
    /// iteration orders; seeding from both a stale and a fresh node drives
    /// both bootstrap orders.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refresh_adopts_freshest_map_regardless_of_pool_order() {
        let s1 = spawn_map_server().await;
        let s2 = spawn_map_server().await;
        let s3 = spawn_map_server().await;
        let stale_v2 = encode_map(
            2,
            &[
                (1, &s1.addr, true),
                (2, &s2.addr, true),
                (3, &s3.addr, true),
            ],
            &[1, 2, 3],
        );
        let fresh_v3 = encode_map(3, &[(1, &s1.addr, true), (2, &s2.addr, true)], &[1, 2]);
        s1.set_map(stale_v2.clone());
        s3.set_map(stale_v2);
        s2.set_map(fresh_v3);

        for seed in [&s1.addr, &s2.addr] {
            for _ in 0..4 {
                let cluster = new_test_cluster(vec![seed.clone()]).await;
                cluster
                    .refresh_partition_map()
                    .await
                    .expect("refresh must succeed while nodes answer");
                let pm = cluster.cached_partition_map().expect("map must be cached");
                assert_eq!(
                    pm.version, 3,
                    "refresh must adopt the freshest map, not the first answer",
                );
                assert!(
                    !cached_masters(&cluster).contains(&3),
                    "no shard may still route to the pruned node 3",
                );
                assert!(
                    !cluster.pools.read().contains_key(&3),
                    "the pruned node's pool must be dropped after adopting the fresh map",
                );
                // The pruned node must not be able to win a later refresh.
                for _ in 0..2 {
                    cluster
                        .refresh_partition_map()
                        .await
                        .expect("repeat refresh must succeed");
                    assert_eq!(
                        cluster.cached_partition_map().expect("cached").version,
                        3,
                        "a later refresh must never regress to the stale map",
                    );
                }
                cluster.close().await;
            }
        }
        s1.kill();
        s2.kill();
        s3.kill();
    }

    /// Liveness plumbing: a node the map advertises dead (`is_alive = 0`)
    /// must not get a pool at bootstrap, and an existing pool for it must be
    /// dropped when a refresh adopts a map advertising it dead — while the
    /// node stays visible in `pm.nodes` for observability.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dead_advertised_node_gets_no_pool_and_stale_pool_dropped() {
        let s1 = spawn_map_server().await;
        let s2 = spawn_map_server().await;
        let s3 = spawn_map_server().await;
        let all_alive_v2 = encode_map(
            2,
            &[
                (1, &s1.addr, true),
                (2, &s2.addr, true),
                (3, &s3.addr, true),
            ],
            &[1, 2],
        );
        let dead3_v3 = encode_map(
            3,
            &[
                (1, &s1.addr, true),
                (2, &s2.addr, true),
                (3, &s3.addr, false),
            ],
            &[1, 2],
        );
        s1.set_map(all_alive_v2.clone());
        s2.set_map(all_alive_v2.clone());
        s3.set_map(all_alive_v2);

        // Phase 1: node 3 alive -> it gets a pool.
        let cluster = new_test_cluster(vec![s1.addr.clone()]).await;
        assert!(
            cluster.pools.read().contains_key(&3),
            "an alive-advertised node must get a pool at bootstrap",
        );

        // Phase 2: the cluster now advertises node 3 dead at v3.
        let expected_v3 = 3u64;
        s1.set_map(dead3_v3.clone());
        s2.set_map(dead3_v3.clone());
        cluster
            .refresh_partition_map()
            .await
            .expect("refresh must succeed");
        let pm = cluster.cached_partition_map().expect("map must be cached");
        assert_eq!(pm.version, expected_v3);
        let node3 = pm
            .nodes
            .iter()
            .find(|n| n.id == 3)
            .expect("dead node stays visible in pm.nodes");
        assert!(!node3.is_alive, "node 3 must be carried as dead");
        assert!(
            !cluster.pools.read().contains_key(&3),
            "the dead-advertised node's pool must be dropped by the refresh",
        );
        cluster.close().await;

        // Phase 3: a fresh bootstrap straight into the dead-advertised map
        // must not create a pool for the dead node at all.
        let cluster2 = new_test_cluster(vec![s1.addr.clone()]).await;
        {
            let pools = cluster2.pools.read();
            assert!(
                pools.contains_key(&1) && pools.contains_key(&2),
                "alive nodes must get pools at bootstrap",
            );
            assert!(
                !pools.contains_key(&3),
                "a dead-advertised node must get no pool at bootstrap",
            );
        }
        cluster2.close().await;
        s1.kill();
        s2.kill();
        s3.kill();
    }

    /// Existing behavior preserved: when NO pool (and no seed) answers, the
    /// refresh must surface a `ClientError::Connection` instead of silently
    /// keeping going.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refresh_errors_when_no_node_answers() {
        let s1 = spawn_map_server().await;
        s1.set_map(encode_map(1, &[(1, &s1.addr, true)], &[1]));
        let cluster = new_test_cluster(vec![s1.addr.clone()]).await;

        // Kill the only node (listener + accepted sockets) and give the
        // spawned tasks a beat to unwind so redials are refused.
        s1.kill();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let err = cluster
            .refresh_partition_map()
            .await
            .expect_err("refresh must error when no source answers");
        match err {
            ClientError::Connection(msg) => {
                assert!(
                    msg.contains("refresh partition map"),
                    "error must identify the refresh path, got: {msg}",
                );
            }
            other => panic!("want ClientError::Connection, got {other:?}"),
        }
        cluster.close().await;
    }
}
