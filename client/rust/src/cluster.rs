//! Cluster-aware routing for TeraSlab.
//!
//! The [`Cluster`] manages a partition map that maps each of the 4096 shards
//! to a specific node. It maintains per-node connection pools and a background
//! refresh task that periodically updates the partition map.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
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
    /// Single-flight gate for map refreshes: concurrent callers queue here
    /// so interleaved poll/install rounds cannot reinstate a stale map.
    refresh_lock: tokio::sync::Mutex<()>,
    /// Bumped on every successful map adoption. A caller that queued behind
    /// an in-flight refresh compares epochs after acquiring the lock and
    /// returns without re-polling when the winner already adopted a map.
    refresh_epoch: AtomicU64,
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
            refresh_lock: tokio::sync::Mutex::new(()),
            refresh_epoch: AtomicU64::new(0),
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

    /// Node ids the adopted map can route to: nodes advertised alive UNION
    /// every master named in the shard table.
    ///
    /// The server's `is_alive` derives from its STRICTLY-Alive SWIM view
    /// (Suspect excluded) while `assignments` come from the committed
    /// topology table, where a suspect stays a member for the full suspicion
    /// window. A node mastering any shard MUST keep a pool — otherwise one
    /// missed probe turns every request to its shards into a non-retryable
    /// "no pool for node" error until the suspicion resolves.
    fn routable_ids(pm: &PartitionMap) -> HashSet<u64> {
        pm.nodes
            .iter()
            .filter(|n| n.is_alive)
            .map(|n| n.id)
            .chain(pm.assignments.iter().copied())
            .collect()
    }

    /// Ensure a connection pool (and address mapping) exists for every
    /// routable node (see [`Self::routable_ids`]) the map carries an address
    /// for. Dead-advertised nodes that master no shard get no pool: dialing
    /// a node the cluster has both declared dead and fully reassigned only
    /// hands a stale minority view a channel back into the client.
    ///
    /// The two write locks are taken in SEPARATE scopes (never nested):
    /// `pool_for_redirect_addr` reads `addr_to_node` and `pools` in the
    /// opposite order, and parking_lot's fairness turns nested opposite-
    /// order acquisition into a real ABBA deadlock.
    fn ensure_pools_for_routable_nodes(&self, pm: &PartitionMap, routable: &HashSet<u64>) {
        let targets: Vec<(u64, String, String)> = pm
            .nodes
            .iter()
            .filter(|n| routable.contains(&n.id))
            .map(|n| {
                (
                    n.id,
                    n.addr.clone(),
                    self.config.resolve_addr(&n.addr).to_string(),
                )
            })
            .collect();
        {
            let mut pools = self.pools.write();
            for (id, _, resolved) in &targets {
                pools.entry(*id).or_insert_with(|| {
                    Arc::new(ConnPool::new(
                        resolved.clone(),
                        self.config.pool_config.clone(),
                    ))
                });
            }
        }
        {
            let mut atn = self.addr_to_node.write();
            for (id, addr, _) in &targets {
                atn.insert(addr.clone(), *id);
            }
        }
    }

    /// Drop pools (and address mappings) for nodes that are neither
    /// advertised alive nor master any shard in the adopted map, so a
    /// pruned node cannot keep winning future refreshes. Returns the
    /// removed pools; the caller hands them to [`close_after_grace`].
    fn drop_stale_pools(&self, routable: &HashSet<u64>) -> Vec<Arc<ConnPool>> {
        let mut removed = Vec::new();
        {
            let mut pools = self.pools.write();
            pools.retain(|id, pool| {
                if routable.contains(id) {
                    true
                } else {
                    removed.push(Arc::clone(pool));
                    false
                }
            });
        }
        self.addr_to_node
            .write()
            .retain(|_, id| routable.contains(id));
        removed
    }

    /// Install an adopted partition map: create pools for its routable
    /// nodes, swap the map in, and prune pools the map no longer references.
    ///
    /// Ordering: new pools are created BEFORE the map swap (so the new map
    /// never routes to a missing pool) and stale pools are dropped AFTER it
    /// (so the old map never routes to a dropped pool).
    ///
    /// Two guards apply:
    /// - Monotonic install: a map older than the currently-installed one is
    ///   refused (the poll's best answer can still be stale when only stale
    ///   nodes answered this round). Same-process only — this is not a
    ///   cross-rebuild fence; a restarted client starts from scratch.
    /// - Uncorroborated maps: a multi-node map advertising NO alive node
    ///   besides its source (a just-restarted node's cold SWIM table says
    ///   everyone-else-dead while it may hold the highest term) is adopted,
    ///   but the drop phase is skipped so the client keeps its channels to
    ///   the rest of the cluster until a corroborated map arrives.
    fn adopt_map(&self, source_addr: &str, pm: PartitionMap) {
        let routable = Self::routable_ids(&pm);
        let alive_count = pm.nodes.iter().filter(|n| n.is_alive).count();
        let uncorroborated = pm.nodes.len() > 1 && alive_count <= 1;

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
            uncorroborated,
            source = %source_addr,
            "client: refreshed partition map (freshest of all answering nodes)",
        );

        // Monotonic pre-check (cheap, before creating any pools) …
        {
            let guard = self.part_map.read();
            if guard.as_ref().is_some_and(|cur| cur.version > pm.version) {
                self.refuse_older_map(guard.as_ref().map(|cur| cur.version), pm.version);
                return;
            }
        }

        self.ensure_pools_for_routable_nodes(&pm, &routable);

        // … and the authoritative check under the write lock.
        {
            let mut guard = self.part_map.write();
            if guard.as_ref().is_some_and(|cur| cur.version > pm.version) {
                let installed = guard.as_ref().map(|cur| cur.version);
                drop(guard);
                self.refuse_older_map(installed, pm.version);
                return;
            }
            *guard = Some(pm);
        }

        if !uncorroborated {
            for pool in self.drop_stale_pools(&routable) {
                close_after_grace(pool);
            }
        }
        self.refresh_epoch.fetch_add(1, Ordering::AcqRel);
    }

    /// Record a refused (older-than-installed) map: the freshest known map
    /// stays installed, and queued refreshers must not re-poll for it.
    fn refuse_older_map(&self, installed: Option<u64>, offered: u64) {
        tracing::debug!(
            ?installed,
            offered,
            "client: refusing to install an older partition map",
        );
        self.refresh_epoch.fetch_add(1, Ordering::AcqRel);
    }

    /// Bootstrap the cluster by polling EVERY seed node concurrently and
    /// adopting the highest-version partition map among the answers.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Connection`] if no seed answers with a valid
    /// partition map.
    async fn bootstrap_from_seeds(&self) -> Result<(), ClientError> {
        let secret = self.config.cluster_secret.clone().filter(|s| !s.is_empty());
        let sources: Vec<(String, Arc<ConnPool>)> = self
            .config
            .seeds
            .iter()
            .map(|seed| {
                (
                    seed.clone(),
                    Arc::new(ConnPool::new(seed.clone(), self.config.pool_config.clone())),
                )
            })
            .collect();
        let throwaway: Vec<Arc<ConnPool>> = sources.iter().map(|(_, p)| Arc::clone(p)).collect();
        let (answers, last_err) = poll_sources(sources, secret).await;
        // The bootstrap pools are throwaway: registered nodes get their own
        // fresh pools from `adopt_map`, so close them all (nothing is in
        // flight on them once the poll returns).
        for pool in throwaway {
            pool.close().await;
        }

        let Some((source_addr, pm)) = select_freshest(answers) else {
            return Err(ClientError::Connection(format!(
                "failed to connect to any seed: {}",
                last_err
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "no seeds provided".to_string())
            )));
        };

        self.adopt_map(&source_addr, pm);
        Ok(())
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

        // Copy the id out first so the `addr_to_node` read guard is dropped
        // before `pools` is locked — the pool-sync writers touch the same
        // two locks and must never be part of a nested opposite-order pair.
        let known_id = self.addr_to_node.read().get(addr).copied();
        if let Some(node_id) = known_id
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
    ///
    /// # W11 FIX 4 — do not keep dialling a master that is gone
    ///
    /// CI @ 3a38dc2 scenario 07: the adopted map named a REMOVED node master
    /// of 1024 of 4096 shards at its dead address. [`Self::routable_ids`]
    /// unions in every master, so the client kept a pool for it and every
    /// request to those shards paid a full dial timeout before failing.
    ///
    /// A master the map advertises DEAD is therefore refused here — but only
    /// once a dial to it has actually been OBSERVED to fail
    /// ([`ConnPool::dial_failing`]). The advertised flag alone is not
    /// sufficient evidence: the server's `is_alive` is its STRICTLY-Alive
    /// SWIM view, so a merely SUSPECT master is advertised dead for the whole
    /// suspicion window while staying perfectly reachable, and refusing it on
    /// the flag alone would turn every request to ~1/N of the shards into an
    /// error — the P0 that [`Self::routable_ids`] and
    /// `dead_advertised_master_keeps_its_pool` exist to prevent. Requiring
    /// observed unreachability keeps the suspect case routing untouched and
    /// costs the removed case exactly ONE dial before it fails fast.
    ///
    /// The map carries only MASTERS (no replicas), so there is no alternative
    /// holder to fall back to: the only options are the master's pool or an
    /// error, and an immediate error beats one burnt dial timeout per
    /// request. The pool's own health loop keeps re-dialling in the
    /// background, so a node that comes back clears the flag and starts
    /// routing again without waiting for a new map.
    fn pool_for_shard(&self, shard: u16) -> Result<Arc<ConnPool>, ClientError> {
        let pm = self.part_map.read();
        let pm = pm.as_ref().ok_or(ClientError::NoPartitionMap)?;

        let node_id = pm.assignments[shard as usize];
        let advertised_dead = pm
            .nodes
            .iter()
            .find(|n| n.id == node_id)
            .is_some_and(|n| !n.is_alive);
        let pools = self.pools.read();
        let pool = pools.get(&node_id).cloned().ok_or_else(|| {
            ClientError::Connection(format!("no pool for node {} (shard {})", node_id, shard))
        })?;
        if advertised_dead && pool.dial_failing() {
            return Err(ClientError::Connection(format!(
                "node {node_id} is advertised dead and unreachable (shard {shard}); \
                 refusing to re-dial — refresh the partition map"
            )));
        }
        Ok(pool)
    }

    /// Refresh the partition map by polling EVERY known node concurrently
    /// and adopting the highest-version map.
    ///
    /// The topology version is globally monotonic, so the highest version is
    /// the freshest cluster view. Polling all pools (instead of returning on
    /// the first answer) prevents a stale minority-side node — still
    /// reachable from the client even though the majority has fenced it —
    /// from pinning the client to its pre-partition shard table on every
    /// refresh. The poll fans out concurrently, so the inline cost is
    /// bounded by the slowest single node (~one dial timeout for a
    /// blackholed peer), not the sum over dead nodes.
    ///
    /// Refreshes are single-flight: concurrent callers queue behind the
    /// in-flight one and return as soon as it has adopted a map, instead of
    /// racing their own poll/install rounds (which could reinstate a stale
    /// map by interleaving). Installs are additionally monotonic in-process
    /// (see [`Self::adopt_map`]).
    ///
    /// Individual pool failures are tolerated; pools for nodes the adopted
    /// map neither advertises alive nor assigns shards to are dropped so a
    /// pruned node cannot win a future refresh.
    ///
    /// # Errors
    ///
    /// Returns an error only if no node (nor seed) could provide a valid
    /// partition map.
    pub async fn refresh_partition_map(&self) -> Result<(), ClientError> {
        // Single-flight: if a refresh completed while we waited for the
        // lock, its result is fresher than anything we could poll now.
        let epoch_before = self.refresh_epoch.load(Ordering::Acquire);
        let _flight = self.refresh_lock.lock().await;
        if self.refresh_epoch.load(Ordering::Acquire) != epoch_before {
            return Ok(());
        }

        let secret = self.config.cluster_secret.clone().filter(|s| !s.is_empty());

        let sources: Vec<(String, Arc<ConnPool>)> = {
            let pools = self.pools.read();
            pools
                .values()
                .map(|p| (p.addr().to_string(), Arc::clone(p)))
                .collect()
        };
        let (answers, mut last_err) = poll_sources(sources, secret.clone()).await;
        let mut best = select_freshest(answers);

        // All known pools failed — fall back to seed nodes, again adopting
        // the highest version among the answers. This handles the case where
        // all cached pools point to dead nodes but the surviving nodes are
        // reachable via the original seeds.
        if best.is_none() {
            let seed_sources: Vec<(String, Arc<ConnPool>)> = self
                .config
                .seeds
                .iter()
                .map(|seed| {
                    (
                        seed.clone(),
                        Arc::new(ConnPool::new(seed.clone(), self.config.pool_config.clone())),
                    )
                })
                .collect();
            let throwaway: Vec<Arc<ConnPool>> =
                seed_sources.iter().map(|(_, p)| Arc::clone(p)).collect();
            let (answers, seed_err) = poll_sources(seed_sources, secret).await;
            for pool in throwaway {
                pool.close().await;
            }
            if seed_err.is_some() {
                last_err = seed_err;
            }
            best = select_freshest(answers);
        }

        let Some((source_addr, pm)) = best else {
            return Err(ClientError::Connection(format!(
                "refresh partition map: {}",
                last_err
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "no pools".to_string())
            )));
        };

        self.adopt_map(&source_addr, pm);
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
// Partition map polling
// ---------------------------------------------------------------------------

/// Fetch and decode the partition map from a single pool.
///
/// # Errors
///
/// Returns [`ClientError::Connection`] if no connection can be obtained, or
/// [`ClientError::Protocol`] on a non-OK status or a malformed map.
async fn fetch_map(pool: &ConnPool, secret: Option<&[u8]>) -> Result<PartitionMap, ClientError> {
    let conn = pool.get().await?;

    // Strict-auth clusters require the whole inter-node frame to be
    // HMAC-signed (request_id||op||flags||payload); sign via the server's
    // own sign_frame so it verifies byte-for-byte. Unsecured clusters send
    // it unsigned (trusted-overlay default).
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

/// Concurrently fetch the partition map from every `(label, pool)` source.
///
/// Returns every successful `(label, map)` answer plus the last error seen
/// (for diagnostics when nothing answered). Fanning out bounds the inline
/// cost to the slowest single source instead of the sum over dead peers.
async fn poll_sources(
    sources: Vec<(String, Arc<ConnPool>)>,
    secret: Option<Vec<u8>>,
) -> (Vec<(String, PartitionMap)>, Option<ClientError>) {
    let mut set = tokio::task::JoinSet::new();
    for (label, pool) in sources {
        let secret = secret.clone();
        set.spawn(async move {
            let res = fetch_map(&pool, secret.as_deref()).await;
            (label, res)
        });
    }
    let mut answers = Vec::new();
    let mut last_err = None;
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((label, Ok(pm))) => answers.push((label, pm)),
            Ok((_, Err(e))) => last_err = Some(e),
            Err(e) => last_err = Some(ClientError::Connection(format!("map poll task: {e}"))),
        }
    }
    (answers, last_err)
}

/// Select the answer with the highest map version (the topology term is
/// globally monotonic, so the highest version is the freshest view). Ties
/// keep the first answer seen; a committed term never has two different
/// tables, so equal versions should carry identical maps. Returns `None`
/// for no answers.
fn select_freshest(answers: Vec<(String, PartitionMap)>) -> Option<(String, PartitionMap)> {
    let mut best: Option<(String, PartitionMap)> = None;
    for (label, pm) in answers {
        if best.as_ref().is_none_or(|(_, b)| pm.version > b.version) {
            best = Some((label, pm));
        }
    }
    best
}

/// Close a pruned pool AFTER one request-timeout grace period.
///
/// `ConnPool::close` clears every connection's pending map, so a waiter
/// still in flight gets `Connection("connection closed")` even though the
/// server may still apply the request — an ambiguous outcome for mutations
/// already on the wire. The grace lets everything that was in flight at
/// prune time either complete or hit its own request timeout first.
/// Best-effort: a request started from a stale `all_pools()` snapshot just
/// before the grace expires can still be aborted.
fn close_after_grace(pool: Arc<ConnPool>) {
    tokio::spawn(async move {
        tokio::time::sleep(pool.request_timeout()).await;
        pool.close().await;
    });
}

// ---------------------------------------------------------------------------
// Partition map decoding
// ---------------------------------------------------------------------------

/// Decode a partition map from a response payload.
///
/// Format: `[version:8][node_count:4][nodes: id(8)+addr_len(2)+addr+is_alive(1) x count][assignments: 4096 x 8]`
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
        // Each node entry carries addr + a REQUIRED is_alive byte (the
        // server advertises 0 for nodes its failure detector has declared
        // dead, C21) — mirror the server's `RoutingInfo::decode` strictness.
        if pos + addr_len + 1 > data.len() {
            return Err(ClientError::Protocol(format!(
                "partition map: truncated node {} (addr + is_alive)",
                i
            )));
        }
        let addr = String::from_utf8_lossy(&data[pos..pos + addr_len]).to_string();
        pos += addr_len;
        let is_alive = data[pos] != 0;
        pos += 1;
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
    /// with an empty OK. Counts served map fetches and can delay each map
    /// response (for the single-flight test). `kill()` drops the listener
    /// and every accepted socket so the node becomes fully unreachable.
    struct MapServer {
        addr: String,
        payload: Arc<RwLock<Vec<u8>>>,
        fetches: Arc<std::sync::atomic::AtomicUsize>,
        delay: Arc<RwLock<Duration>>,
        kill_tx: tokio::sync::watch::Sender<bool>,
    }

    impl MapServer {
        fn set_map(&self, map: Vec<u8>) {
            *self.payload.write() = map;
        }

        fn set_delay(&self, delay: Duration) {
            *self.delay.write() = delay;
        }

        fn fetches(&self) -> usize {
            self.fetches.load(std::sync::atomic::Ordering::SeqCst)
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
        let fetches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let delay: Arc<RwLock<Duration>> = Arc::new(RwLock::new(Duration::ZERO));
        let (kill_tx, kill_rx) = tokio::sync::watch::channel(false);
        let served = Arc::clone(&payload);
        let fetch_ctr = Arc::clone(&fetches);
        let serve_delay = Arc::clone(&delay);
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
                let fetch_ctr = Arc::clone(&fetch_ctr);
                let serve_delay = Arc::clone(&serve_delay);
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
                            OP_GET_PARTITION_MAP => {
                                fetch_ctr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                let pause = *serve_delay.read();
                                if pause > Duration::ZERO {
                                    tokio::time::sleep(pause).await;
                                }
                                served.read().clone()
                            }
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
            fetches,
            delay,
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

    /// W11 FIX 4 — a master the adopted map advertises DEAD and that the
    /// client has PROVEN unreachable must stop being handed out for routing.
    ///
    /// CI @ 3a38dc2 scenario 07: the adopted v4 map named node4 master of
    /// 1024 of 4096 shards at its (removed) address. `routable_ids` unions in
    /// every master, so the client kept a pool for it and every request to
    /// those shards paid a full dial timeout before failing — the chunk loop
    /// in `scenario_07_scale_down_graceful.rs` broke on the first one with
    /// `checked = 0`.
    ///
    /// The gate is `advertised dead` AND `a dial to it has been observed to
    /// fail`, never the advertised flag alone: `is_alive` is the server's
    /// STRICTLY-Alive SWIM view, so a merely SUSPECT master is advertised
    /// dead while still perfectly reachable, and refusing it outright would
    /// re-open the P0 that `dead_advertised_master_keeps_its_pool` pins.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn proven_unreachable_dead_advertised_master_is_not_dialled_again() {
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
            &[1, 2, 3],
        );
        for s in [&s1, &s2, &s3] {
            s.set_map(all_alive_v2.clone());
        }
        let cluster = new_test_cluster(vec![s1.addr.clone()]).await;

        // Node 3 is REMOVED: gone from the network, advertised dead, but the
        // committed table this map carries still names it master.
        s3.kill();
        let dead3_v3 = encode_map(
            3,
            &[
                (1, &s1.addr, true),
                (2, &s2.addr, true),
                (3, &s3.addr, false),
            ],
            &[1, 2, 3],
        );
        for s in [&s1, &s2] {
            s.set_map(dead3_v3.clone());
        }
        cluster
            .refresh_partition_map()
            .await
            .expect("refresh must succeed from the two survivors");
        assert_eq!(cluster.cached_partition_map().expect("cached").version, 3);

        // The pool is still RETAINED — the P0 rule is unchanged, and the
        // client cannot know a node is gone without trying it once.
        let pool3 = cluster
            .pools
            .read()
            .get(&3)
            .cloned()
            .expect("a dead-advertised master keeps its pool");
        assert!(
            pool3.get().await.is_err(),
            "node 3 is gone — dialling it must fail",
        );
        assert!(
            pool3.dial_failing(),
            "the failed dial must be recorded on the pool",
        );

        // Shard 2 round-robins to node 3. With proof in hand the client must
        // surface a routing error instead of burning another dial timeout.
        match cluster.pool_for_shard(2) {
            Err(ClientError::Connection(msg)) => assert!(
                msg.contains("advertised dead") && msg.contains("shard 2"),
                "the error must name the reason and the shard, got: {msg}",
            ),
            Err(other) => panic!("expected a Connection routing error, got {other:?}"),
            Ok(_) => panic!(
                "a proven-unreachable dead-advertised master must not be handed \
                 out for routing"
            ),
        }

        cluster.close().await;
        s1.kill();
        s2.kill();
    }

    /// The freshest-map selection is a pure function; drive BOTH answer
    /// orders deterministically (the integration test can only randomize
    /// pool iteration order probabilistically).
    #[test]
    fn select_freshest_is_order_independent() {
        let map = |version: u64| {
            decode_partition_map(&encode_map(version, &[(1, "10.0.0.1:3300", true)], &[1]))
                .expect("test map must decode")
        };
        let stale = map(2);
        let fresh = map(3);
        for answers in [
            vec![
                ("a".to_string(), stale.clone()),
                ("b".to_string(), fresh.clone()),
            ],
            vec![
                ("b".to_string(), fresh.clone()),
                ("a".to_string(), stale.clone()),
            ],
        ] {
            let (label, pm) = select_freshest(answers).expect("two answers must select one");
            assert_eq!(pm.version, 3, "the highest version must win in any order");
            assert_eq!(label, "b", "the winner's label must identify its source");
        }
        // Ties keep the first answer seen (equal committed versions carry
        // identical maps).
        let (label, pm) = select_freshest(vec![
            ("a".to_string(), fresh.clone()),
            ("b".to_string(), fresh),
        ])
        .expect("tie must still select");
        assert_eq!(pm.version, 3);
        assert_eq!(label, "a", "a version tie keeps the first answer");
        assert!(
            select_freshest(Vec::new()).is_none(),
            "no answers must select nothing",
        );
    }

    /// The is_alive byte is REQUIRED (mirrors the server's
    /// `RoutingInfo::decode`): a node entry ending exactly after its addr
    /// must be rejected as truncated, not silently defaulted.
    #[test]
    fn decode_partition_map_requires_is_alive_byte() {
        let mut p = Vec::new();
        p.extend_from_slice(&1u64.to_le_bytes()); // version
        p.extend_from_slice(&1u32.to_le_bytes()); // node_count
        p.extend_from_slice(&1u64.to_le_bytes()); // node id
        p.extend_from_slice(&4u16.to_le_bytes()); // addr_len
        p.extend_from_slice(b"a:12"); // addr, then NO is_alive byte
        let err = decode_partition_map(&p).expect_err("a missing is_alive byte must be rejected");
        match err {
            ClientError::Protocol(msg) => assert!(
                msg.contains("truncated node"),
                "error must identify the truncated node entry, got: {msg}",
            ),
            other => panic!("want ClientError::Protocol, got {other:?}"),
        }
    }

    /// Finding 1 (P0): the server's `is_alive` derives from its STRICTLY-
    /// Alive SWIM view (Suspect excluded) while `assignments` come from the
    /// committed table — a suspected node can be advertised dead while
    /// still mastering shards for the whole suspicion window. Its pool must
    /// be created/retained: dropping it turns every request to ~1/N of the
    /// shards into a non-retryable "no pool for node" error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dead_advertised_master_keeps_its_pool() {
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
            &[1, 2, 3],
        );
        for s in [&s1, &s2, &s3] {
            s.set_map(all_alive_v2.clone());
        }
        let cluster = new_test_cluster(vec![s1.addr.clone()]).await;

        // Node 3 becomes Suspect: advertised dead, but the committed table
        // still assigns it shards.
        let suspect3_v3 = encode_map(
            3,
            &[
                (1, &s1.addr, true),
                (2, &s2.addr, true),
                (3, &s3.addr, false),
            ],
            &[1, 2, 3],
        );
        for s in [&s1, &s2, &s3] {
            s.set_map(suspect3_v3.clone());
        }
        cluster
            .refresh_partition_map()
            .await
            .expect("refresh must succeed");
        assert_eq!(cluster.cached_partition_map().expect("cached").version, 3);
        assert!(
            cluster.pools.read().contains_key(&3),
            "a dead-advertised node still mastering shards must keep its pool",
        );
        // Shard 2 is round-robined to node 3; routing must still work.
        let pool = cluster
            .pool_for_shard(2)
            .expect("shard 2 must stay routable");
        assert_eq!(pool.addr(), s3.addr, "shard 2 must route to node 3's pool");
        cluster.close().await;

        // A fresh bootstrap straight into the suspect map must also create
        // the mastering node's pool.
        let cluster2 = new_test_cluster(vec![s1.addr.clone()]).await;
        assert!(
            cluster2.pools.read().contains_key(&3),
            "bootstrap must create a pool for a dead-advertised node that masters shards",
        );
        cluster2.close().await;
        s1.kill();
        s2.kill();
        s3.kill();
    }

    /// Finding 2 (P0): a just-restarted node can hold the HIGHEST topology
    /// version while its SWIM table still marks every peer dead. Such a map
    /// (multi-node, no alive node besides its source) is adopted — the
    /// version is authoritative — but treated as UNCORROBORATED: the drop
    /// phase is skipped so the client keeps its channels to the rest of
    /// the cluster instead of pruning down to the lone survivor.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lone_survivor_map_is_adopted_but_drops_no_pools() {
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
            &[1, 2, 3],
        );
        for s in [&s1, &s2, &s3] {
            s.set_map(all_alive_v2.clone());
        }
        let cluster = new_test_cluster(vec![s1.addr.clone()]).await;

        // Node 1 restarts with a higher term but a cold SWIM table: only
        // itself alive, all shards self-assigned. Nodes 2 and 3 still serve
        // the older map.
        let lone_v9 = encode_map(
            9,
            &[
                (1, &s1.addr, true),
                (2, &s2.addr, false),
                (3, &s3.addr, false),
            ],
            &[1],
        );
        s1.set_map(lone_v9);
        cluster
            .refresh_partition_map()
            .await
            .expect("refresh must succeed");
        assert_eq!(
            cluster.cached_partition_map().expect("cached").version,
            9,
            "the highest version wins adoption",
        );
        {
            let pools = cluster.pools.read();
            for id in [1u64, 2, 3] {
                assert!(
                    pools.contains_key(&id),
                    "an uncorroborated lone-survivor map must not prune pool {id}",
                );
            }
        }
        cluster.close().await;
        s1.kill();
        s2.kill();
        s3.kill();
    }

    /// Finding 3 (P1, monotonic half): once a map is installed, a refresh
    /// whose best answer is OLDER (e.g. only stale nodes answered this
    /// round) must not replace it. Same-process only — a client restart
    /// starts from scratch.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refresh_never_installs_an_older_map() {
        let s1 = spawn_map_server().await;
        let s2 = spawn_map_server().await;
        let fresh_v3 = encode_map(3, &[(1, &s1.addr, true), (2, &s2.addr, true)], &[1, 2]);
        let stale_v2 = encode_map(2, &[(1, &s1.addr, true), (2, &s2.addr, true)], &[1, 2]);
        s1.set_map(fresh_v3.clone());
        s2.set_map(fresh_v3);
        let cluster = new_test_cluster(vec![s1.addr.clone()]).await;
        assert_eq!(cluster.cached_partition_map().expect("cached").version, 3);

        // Every node now answers with an older map (e.g. a stale minority
        // is the only thing left answering).
        s1.set_map(stale_v2.clone());
        s2.set_map(stale_v2);
        cluster
            .refresh_partition_map()
            .await
            .expect("refresh must still succeed");
        assert_eq!(
            cluster.cached_partition_map().expect("cached").version,
            3,
            "an older map must never replace a newer one in-process",
        );
        cluster.close().await;
        s1.kill();
        s2.kill();
    }

    /// Finding 3 (P1, single-flight half): concurrent refreshes must not
    /// each run their own poll round (interleaved installs could reinstate
    /// a stale map by pure concurrency). Callers that queued behind an
    /// in-flight refresh adopt its result instead of re-polling — with a
    /// 100ms per-fetch delay, 8 concurrent refreshes over 3 nodes must
    /// serve far fewer than the unguarded 8 x 3 = 24 fetches.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_refreshes_are_single_flight() {
        let s1 = spawn_map_server().await;
        let s2 = spawn_map_server().await;
        let s3 = spawn_map_server().await;
        let v3 = encode_map(
            3,
            &[
                (1, &s1.addr, true),
                (2, &s2.addr, true),
                (3, &s3.addr, true),
            ],
            &[1, 2, 3],
        );
        for s in [&s1, &s2, &s3] {
            s.set_map(v3.clone());
        }
        let cluster = Arc::new(new_test_cluster(vec![s1.addr.clone()]).await);
        let baseline: usize = [&s1, &s2, &s3].iter().map(|s| s.fetches()).sum();
        for s in [&s1, &s2, &s3] {
            s.set_delay(Duration::from_millis(100));
        }

        let mut handles = Vec::new();
        for _ in 0..8 {
            let cl = Arc::clone(&cluster);
            handles.push(tokio::spawn(
                async move { cl.refresh_partition_map().await },
            ));
        }
        for h in handles {
            h.await
                .expect("refresh task must not panic")
                .expect("concurrent refresh must succeed");
        }

        let total: usize = [&s1, &s2, &s3].iter().map(|s| s.fetches()).sum::<usize>() - baseline;
        assert!(
            total <= 9,
            "concurrent refreshes must single-flight (served {total} map fetches, unguarded would be 24)",
        );
        assert_eq!(cluster.cached_partition_map().expect("cached").version, 3);
        cluster.close().await;
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
