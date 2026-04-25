//! TeraSlab Rust client library.
//!
//! Provides a production-quality async client for the TeraSlab binary wire
//! protocol with connection pooling, request pipelining, and cluster-aware
//! shard routing.
//!
//! # Single-node usage
//!
//! ```no_run
//! # use teraslab_client::*;
//! # async fn example() -> Result<(), ClientError> {
//! let client = Client::new(ClientConfig {
//!     addr: Some("localhost:3300".to_string()),
//!     ..Default::default()
//! }).await?;
//!
//! let rtt = client.ping().await?;
//! println!("pong: {:?}", rtt);
//!
//! client.close().await;
//! # Ok(())
//! # }
//! ```
//!
//! # Cluster usage
//!
//! ```no_run
//! # use teraslab_client::*;
//! # async fn example() -> Result<(), ClientError> {
//! let client = Client::new(ClientConfig {
//!     seeds: vec!["node1:3300".into(), "node2:3300".into()],
//!     ..Default::default()
//! }).await?;
//!
//! client.close().await;
//! # Ok(())
//! # }
//! ```
//!
//! All batch operations are async and safe for concurrent use from multiple
//! Tokio tasks. The [`Client`] is `Send + Sync`.

mod cluster;
mod conn;
pub mod errors;
mod pool;
pub mod types;

pub use cluster::ClusterConfig;
pub use errors::*;
pub use pool::PoolConfig;
pub use types::*;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use teraslab::protocol::codec;
use teraslab::protocol::opcodes::*;

/// Threshold for switching from inline cold_data to chunked blob upload.
/// Transactions with cold_data larger than this are uploaded via
/// OP_STREAM_CHUNK/OP_STREAM_END before the CREATE request.
const BLOB_UPLOAD_THRESHOLD: usize = 1024 * 1024; // 1 MiB

/// Size of each chunk sent during blob upload.
const BLOB_CHUNK_SIZE: usize = 4 * 1024 * 1024; // 4 MiB

use crate::cluster::Cluster;
use crate::pool::ConnPool;

/// A group of items destined for the same pool, keyed by Arc pointer identity.
/// Maps `pool_ptr_as_usize -> (pool, original_batch_indices)`.
type PoolGroupMap = HashMap<usize, (Arc<ConnPool>, Vec<usize>)>;

/// Configuration for a TeraSlab client.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Server address for single-node mode (e.g., "localhost:3300").
    pub addr: Option<String>,
    /// Seed node addresses for cluster mode. If non-empty, overrides `addr`.
    pub seeds: Vec<String>,
    /// Per-node connection pool configuration.
    pub pool: PoolConfig,
    /// How often to refresh the cluster partition map (default: 30s).
    pub cluster_refresh_interval: Duration,
    /// Maximum redirect retries per request in cluster mode (default: 3).
    pub max_redirects: u32,
    /// Optional address mapping for Docker/NAT environments.
    ///
    /// Maps server-advertised internal addresses to host-reachable addresses.
    /// For example: `{"172.30.0.11:3300": "127.0.0.1:13300"}`.
    pub addr_map: std::collections::HashMap<String, String>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            addr: None,
            seeds: Vec::new(),
            pool: PoolConfig::default(),
            cluster_refresh_interval: Duration::from_secs(30),
            max_redirects: 3,
            addr_map: std::collections::HashMap::new(),
        }
    }
}

/// A thread-safe, async TeraSlab client.
///
/// Supports both single-node and cluster modes. In cluster mode, batch
/// operations are automatically routed to the correct node(s) by txid shard.
pub struct Client {
    /// Cluster manager (set in cluster mode).
    cluster: Option<Arc<Cluster>>,
    /// Single-node connection pool (set in single-node mode).
    pool: Option<Arc<ConnPool>>,
    /// Kept alive for the cluster refresh task.
    _refresh_task: Option<tokio::task::JoinHandle<()>>,
}

impl Client {
    /// Create a new client and connect to the server(s).
    ///
    /// In cluster mode (`seeds` non-empty), the initial partition map is
    /// fetched from a seed node. In single-node mode, a connection pool is
    /// created for the given `addr`.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Connection`] if no server is reachable, or
    /// if neither `addr` nor `seeds` is provided.
    pub async fn new(cfg: ClientConfig) -> Result<Self, ClientError> {
        if !cfg.seeds.is_empty() {
            let cl = Arc::new(
                Cluster::new(ClusterConfig {
                    seeds: cfg.seeds,
                    pool_config: cfg.pool,
                    refresh_interval: cfg.cluster_refresh_interval,
                    max_redirects: cfg.max_redirects,
                    addr_map: cfg.addr_map,
                })
                .await?,
            );
            let refresh_task = cl.start_refresh();
            Ok(Self {
                cluster: Some(cl),
                pool: None,
                _refresh_task: Some(refresh_task),
            })
        } else if let Some(addr) = cfg.addr {
            let pool = Arc::new(ConnPool::new(addr, cfg.pool));
            Ok(Self {
                cluster: None,
                pool: Some(pool),
                _refresh_task: None,
            })
        } else {
            Err(ClientError::Connection(
                "either addr or seeds must be set".to_string(),
            ))
        }
    }

    /// Close all connections and background tasks.
    pub async fn close(&self) {
        if let Some(cl) = &self.cluster {
            cl.close().await;
        }
        if let Some(pool) = &self.pool {
            pool.close().await;
        }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Get a connection from the pool.
    ///
    /// In single-node mode, uses the single pool.
    /// In cluster mode, picks any available node's pool (for non-routed ops
    /// like ping, health, get_partition_map).
    async fn get_conn(&self) -> Result<Arc<crate::conn::PipeConn>, ClientError> {
        if let Some(pool) = &self.pool {
            return pool.get().await;
        }
        if let Some(cl) = &self.cluster {
            let pool = cl.any_pool()?;
            return pool.get().await;
        }
        Err(ClientError::Connection("no pool available".to_string()))
    }

    /// Get a connection routed by txid shard.
    async fn get_conn_for_txid(
        &self,
        txid: &TxID,
    ) -> Result<Arc<crate::conn::PipeConn>, ClientError> {
        if let Some(cl) = &self.cluster {
            let pool = cl.pool_for_txid(txid)?;
            return pool.get().await;
        }
        self.get_conn().await
    }

    /// Get a connection for the first txid in a slice (convenience for
    /// operations that route to a single node).
    async fn get_conn_for_any_txid(
        &self,
        txids: &[TxID],
    ) -> Result<Arc<crate::conn::PipeConn>, ClientError> {
        if self.cluster.is_some() && !txids.is_empty() {
            return self.get_conn_for_txid(&txids[0]).await;
        }
        self.get_conn().await
    }

    // -----------------------------------------------------------------------
    // Response handling
    // -----------------------------------------------------------------------

    /// Handle a mutation response (OK, Error, NotFound, Redirect, PartialError).
    fn handle_mutation_response(
        resp: &teraslab::protocol::frame::ResponseFrame,
    ) -> Result<BatchResult, ClientError> {
        match resp.status {
            STATUS_OK => Ok(BatchResult { errors: Vec::new() }),
            STATUS_ERROR => {
                let (code, msg) = decode_error_payload(&resp.payload)?;
                Err(ClientError::Server { code, message: msg })
            }
            STATUS_NOT_FOUND => Err(ClientError::NotFound),
            STATUS_REDIRECT => {
                let addr = decode_redirect(&resp.payload)?;
                Err(ClientError::Redirect(addr))
            }
            STATUS_PARTIAL_ERROR => {
                let errs = decode_sparse_errors(&resp.payload)?;
                Err(ClientError::Partial(PartialError {
                    successes: Vec::new(),
                    errors: errs,
                }))
            }
            other => Err(ClientError::Protocol(format!("unknown status: {}", other))),
        }
    }

    /// Handle a signal response (SpendBatch/SetMinedBatch with success signals).
    /// Decode a spend- or set-mined-style response that may carry per-item
    /// success signals.
    ///
    /// `item_count` is the number of items in the originating request. When
    /// the server responds with `STATUS_OK` and an empty payload (the common
    /// fully-successful, no-signals case for spend_batch), we synthesize one
    /// `BatchItemSuccess` per request index so callers can rely on
    /// `successes.len()` to reflect what actually happened on the cluster.
    /// Likewise on `STATUS_PARTIAL_ERROR` the implicit successes (items
    /// whose index is not in the sparse error list) are reconstructed so
    /// every request item shows up in exactly one of `successes` or
    /// `errors`, with no silent drops.
    fn handle_signal_response(
        resp: &teraslab::protocol::frame::ResponseFrame,
        item_count: usize,
    ) -> Result<SpendBatchResponse, ClientError> {
        match resp.status {
            STATUS_OK => {
                if !resp.payload.is_empty() {
                    let (successes, errs) = decode_partial_with_signals(&resp.payload)?;
                    if !errs.is_empty() {
                        return Err(ClientError::Partial(PartialError {
                            successes,
                            errors: errs,
                        }));
                    }
                    Ok(SpendBatchResponse {
                        successes,
                        errors: Vec::new(),
                    })
                } else {
                    // Server convention: empty payload on STATUS_OK means
                    // every request item succeeded and there are no
                    // per-item signals to report. Synthesize one
                    // `BatchItemSuccess` per input index so callers do not
                    // have to special-case the wire format.
                    let successes = (0..item_count as u32)
                        .map(|item_index| BatchItemSuccess {
                            item_index,
                            signal: SIGNAL_NONE,
                            block_ids: Vec::new(),
                        })
                        .collect();
                    Ok(SpendBatchResponse {
                        successes,
                        errors: Vec::new(),
                    })
                }
            }
            STATUS_ERROR => {
                let (code, msg) = decode_error_payload(&resp.payload)?;
                Err(ClientError::Server { code, message: msg })
            }
            STATUS_NOT_FOUND => Err(ClientError::NotFound),
            STATUS_REDIRECT => {
                let addr = decode_redirect(&resp.payload)?;
                Err(ClientError::Redirect(addr))
            }
            STATUS_PARTIAL_ERROR => {
                // PARTIAL_ERROR encodes only the failing items sparsely;
                // any index in `0..item_count` that isn't in the error list
                // succeeded. Reconstruct the implicit successes so the
                // PartialError carries a complete per-item picture.
                let errs = decode_sparse_errors(&resp.payload)?;
                let failed: std::collections::HashSet<u32> =
                    errs.iter().map(|e| e.item_index).collect();
                let successes = (0..item_count as u32)
                    .filter(|i| !failed.contains(i))
                    .map(|item_index| BatchItemSuccess {
                        item_index,
                        signal: SIGNAL_NONE,
                        block_ids: Vec::new(),
                    })
                    .collect();
                Err(ClientError::Partial(PartialError {
                    successes,
                    errors: errs,
                }))
            }
            other => Err(ClientError::Protocol(format!("unknown status: {}", other))),
        }
    }

    // -----------------------------------------------------------------------
    // Cluster-aware batch routing
    // -----------------------------------------------------------------------

    /// Group txids by their target pool (for cluster-aware batch operations).
    /// Returns None if not in cluster mode.
    fn group_txids(&self, txids: &[TxID]) -> Option<PoolGroupMap> {
        let cluster = self.cluster.as_ref()?;
        // Use a HashMap keyed by pool address (via pointer identity of Arc).
        // We'll key by the pool's Arc pointer as a usize.
        let mut groups: PoolGroupMap = HashMap::new();
        for (i, txid) in txids.iter().enumerate() {
            if let Ok(pool) = cluster.pool_for_txid(txid) {
                let key = Arc::as_ptr(&pool) as usize;
                groups
                    .entry(key)
                    .or_insert_with(|| (pool, Vec::new()))
                    .1
                    .push(i);
            }
        }
        Some(groups)
    }

    /// Send a txid-list batch operation with cluster-aware routing.
    async fn send_txid_batch<F>(
        &self,
        op_code: u16,
        txids: &[TxID],
        encode_payload: &F,
    ) -> Result<BatchResult, ClientError>
    where
        F: Fn(&[TxID]) -> Vec<u8>,
    {
        if self.cluster.is_some() {
            return self
                .send_txid_batch_cluster(op_code, txids, encode_payload)
                .await;
        }
        let payload = encode_payload(txids);
        let conn = self
            .pool
            .as_ref()
            .ok_or(ClientError::PoolClosed)?
            .get()
            .await?;
        let resp = conn.round_trip(op_code, 0, payload).await?;
        Self::handle_mutation_response(&resp)
    }

    /// Cluster-aware version of send_txid_batch.
    async fn send_txid_batch_cluster<F>(
        &self,
        op_code: u16,
        txids: &[TxID],
        encode_payload: &F,
    ) -> Result<BatchResult, ClientError>
    where
        F: Fn(&[TxID]) -> Vec<u8>,
    {
        let groups = self.group_txids(txids);

        // If single node or no cluster, just send directly.
        if groups.is_none() || groups.as_ref().is_some_and(|g| g.len() <= 1) {
            let payload = encode_payload(txids);
            let conn = if let Some(groups) = &groups {
                if let Some((pool, _)) = groups.values().next() {
                    pool.get().await?
                } else {
                    self.get_conn().await?
                }
            } else {
                self.get_conn().await?
            };
            let resp = conn.round_trip(op_code, 0, payload).await?;
            return Self::handle_mutation_response(&resp);
        }

        let groups = groups.unwrap();

        // Multiple nodes -- send in parallel and merge.
        let mut handles = Vec::with_capacity(groups.len());

        for (_, (pool, idx_map)) in groups {
            let sub_txids: Vec<TxID> = idx_map.iter().map(|&i| txids[i]).collect();
            let payload = encode_payload(&sub_txids);
            let idx_map_clone = idx_map;

            handles.push(tokio::spawn(async move {
                let conn = pool.get().await?;
                let resp = conn.round_trip(op_code, 0, payload).await?;
                let result = Self::handle_mutation_response(&resp);
                Ok::<(Result<BatchResult, ClientError>, Vec<usize>), ClientError>((
                    result,
                    idx_map_clone,
                ))
            }));
        }

        // Collect results and retry redirect errors.
        let mut all_errors: Vec<BatchItemError> = Vec::new();
        let mut got_no_quorum = false;

        for handle in handles {
            let (result, idx_map) = handle
                .await
                .map_err(|e| ClientError::Connection(format!("join: {}", e)))??;

            match result {
                Ok(_) => {
                    // All items succeeded for this sub-batch.
                }
                Err(ClientError::Partial(pe)) => {
                    // Separate redirect errors from real errors.
                    // Redirect errors mean the shard table is stale — refresh
                    // routing and retry those items on the correct node.
                    let mut redirected_indices: Vec<usize> = Vec::new();
                    for err in pe.errors {
                        if err.code == ERR_REDIRECT {
                            redirected_indices.push(idx_map[err.item_index as usize]);
                        } else {
                            let remapped = remap_batch_errors(vec![err], &idx_map);
                            all_errors.extend(remapped);
                        }
                    }
                    if !redirected_indices.is_empty() {
                        // Refresh routing to get the updated shard table, then
                        // retry the redirected items. Single retry — if it fails
                        // again, propagate the error.
                        let _ = self.refresh_routing().await;
                        let retry_txids: Vec<TxID> =
                            redirected_indices.iter().map(|&i| txids[i]).collect();
                        let retry_payload = encode_payload(&retry_txids);
                        if let Ok(conn) = self.get_conn_for_txid(&retry_txids[0]).await {
                            match conn.round_trip(op_code, 0, retry_payload).await {
                                Ok(retry_resp) => {
                                    if let Err(ClientError::Partial(retry_pe)) =
                                        Self::handle_mutation_response(&retry_resp)
                                    {
                                        let retry_remapped = remap_batch_errors(
                                            retry_pe.errors,
                                            &redirected_indices,
                                        );
                                        all_errors.extend(retry_remapped);
                                    }
                                    // If Ok or the retry succeeded, no errors to add
                                }
                                Err(_) => {
                                    // Retry connection failed — add all as errors
                                    for &orig_idx in &redirected_indices {
                                        all_errors.push(BatchItemError {
                                            item_index: orig_idx as u32,
                                            code: ERR_REDIRECT,
                                            data: vec![],
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
                Err(ClientError::Server { code, ref message })
                    if code == 15 || message.contains("no quorum") =>
                {
                    got_no_quorum = true;
                }
                Err(e) => return Err(e),
            }
        }

        if got_no_quorum {
            let _ = self.refresh_routing().await;
            return Err(ClientError::Server {
                code: 15,
                message: "no quorum (routing refreshed, retry recommended)".to_string(),
            });
        }

        if !all_errors.is_empty() {
            return Err(ClientError::Partial(PartialError {
                successes: Vec::new(),
                errors: all_errors,
            }));
        }

        Ok(BatchResult { errors: Vec::new() })
    }

    // -----------------------------------------------------------------------
    // Mutation operations
    // -----------------------------------------------------------------------

    /// Send a batch spend request.
    ///
    /// Returns [`SpendBatchResponse`] with success signals. Returns
    /// [`ClientError::Partial`] if some items failed (inspect the error
    /// for per-item details).
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Server`] on global error, [`ClientError::Partial`]
    /// on mixed success/failure, or [`ClientError::Connection`] on I/O failure.
    pub async fn spend_batch(
        &self,
        params: &SpendBatchParams,
        items: &[SpendItem],
    ) -> Result<SpendBatchResponse, ClientError> {
        if self.cluster.is_some() {
            return self.spend_batch_cluster(params, items).await;
        }
        let payload = encode_spend_batch_payload(params, items);
        let pool = self.pool.as_ref().ok_or(ClientError::PoolClosed)?;
        let conn = pool.get().await?;
        let resp = conn.round_trip(OP_SPEND_BATCH, 0, payload).await?;
        Self::handle_signal_response(&resp, items.len())
    }

    /// Cluster-aware spend batch: group items by target node, send in parallel,
    /// merge results with index remapping.
    async fn spend_batch_cluster(
        &self,
        params: &SpendBatchParams,
        items: &[SpendItem],
    ) -> Result<SpendBatchResponse, ClientError> {
        let cluster = self.cluster.as_ref().ok_or(ClientError::NoPartitionMap)?;

        // Group by target pool.
        let mut groups: PoolGroupMap = HashMap::new();
        for (i, item) in items.iter().enumerate() {
            let pool = cluster.pool_for_txid(&item.txid)?;
            let key = Arc::as_ptr(&pool) as usize;
            groups
                .entry(key)
                .or_insert_with(|| (pool, Vec::new()))
                .1
                .push(i);
        }

        if groups.len() == 1 {
            // All items go to one node.
            let (pool, idx_map) = groups.into_values().next().unwrap();
            let sub_items: Vec<SpendItem> = idx_map.iter().map(|&i| items[i].clone()).collect();
            let payload = encode_spend_batch_payload(params, &sub_items);
            let conn = pool.get().await?;
            let resp = conn.round_trip(OP_SPEND_BATCH, 0, payload).await?;
            let result = Self::handle_signal_response(&resp, sub_items.len());
            return match result {
                Ok(mut r) => {
                    remap_signal_result(&mut r, &idx_map);
                    Ok(r)
                }
                Err(ClientError::Partial(mut pe)) => {
                    // Check for redirect errors and retry after routing refresh.
                    let mut redirect_items: Vec<(usize, SpendItem)> = Vec::new();
                    pe.errors.retain(|e| {
                        if e.code == ERR_REDIRECT && (e.item_index as usize) < idx_map.len() {
                            let orig_idx = idx_map[e.item_index as usize];
                            redirect_items.push((orig_idx, items[orig_idx].clone()));
                            false // remove from errors
                        } else {
                            true // keep
                        }
                    });
                    // Drop the synthetic success entries for the indices
                    // we are about to retry — they will be re-added (with
                    // real success information) if the retry succeeds.
                    let retry_src_indices: std::collections::HashSet<u32> = redirect_items
                        .iter()
                        .filter_map(|(orig_idx, _)| {
                            idx_map
                                .iter()
                                .position(|&i| i == *orig_idx)
                                .map(|p| p as u32)
                        })
                        .collect();
                    pe.successes
                        .retain(|s| !retry_src_indices.contains(&s.item_index));
                    if !redirect_items.is_empty() {
                        let _ = self.refresh_routing().await;
                        for (orig_idx, spend_item) in redirect_items {
                            let retry_payload = encode_spend_batch_payload(params, &[spend_item]);
                            if let Ok(retry_pool) = cluster.pool_for_txid(&items[orig_idx].txid)
                                && let Ok(retry_conn) = retry_pool.get().await
                                && let Ok(retry_resp) = retry_conn
                                    .round_trip(OP_SPEND_BATCH, 0, retry_payload)
                                    .await
                            {
                                match Self::handle_signal_response(&retry_resp, 1) {
                                    Ok(r) => {
                                        for mut s in r.successes {
                                            s.item_index = orig_idx as u32;
                                            pe.successes.push(s);
                                        }
                                    }
                                    Err(ClientError::Partial(retry_pe)) => {
                                        for mut s in retry_pe.successes {
                                            s.item_index = orig_idx as u32;
                                            pe.successes.push(s);
                                        }
                                        for mut e in retry_pe.errors {
                                            e.item_index = orig_idx as u32;
                                            pe.errors.push(e);
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                    remap_partial_items(&mut pe, &idx_map);
                    if pe.errors.is_empty() {
                        // All errors were redirects that succeeded on retry
                        Ok(SpendBatchResponse {
                            successes: pe.successes,
                            errors: vec![],
                        })
                    } else {
                        Err(ClientError::Partial(pe))
                    }
                }
                Err(e) => Err(e),
            };
        }

        // Multiple nodes -- send in parallel.
        let mut handles = Vec::with_capacity(groups.len());

        for (_, (pool, idx_map)) in groups {
            let sub_items: Vec<SpendItem> = idx_map.iter().map(|&i| items[i].clone()).collect();
            let payload = encode_spend_batch_payload(params, &sub_items);

            let sub_len = sub_items.len();
            handles.push(tokio::spawn(async move {
                let conn = pool.get().await?;
                let resp = conn.round_trip(OP_SPEND_BATCH, 0, payload).await?;
                let result = Self::handle_signal_response(&resp, sub_len);
                Ok::<(Result<SpendBatchResponse, ClientError>, Vec<usize>), ClientError>((
                    result, idx_map,
                ))
            }));
        }

        // Merge results.
        let mut merged = SpendBatchResponse {
            successes: Vec::new(),
            errors: Vec::new(),
        };
        let mut all_errors: Vec<BatchItemError> = Vec::new();

        for handle in handles {
            let (result, idx_map) = handle
                .await
                .map_err(|e| ClientError::Connection(format!("join: {}", e)))??;

            match result {
                Ok(r) => {
                    for mut s in r.successes {
                        if (s.item_index as usize) < idx_map.len() {
                            s.item_index = idx_map[s.item_index as usize] as u32;
                        }
                        merged.successes.push(s);
                    }
                }
                Err(ClientError::Partial(pe)) => {
                    // Separate redirect errors from real errors before
                    // copying implicit successes over, so items that are
                    // about to be retried do not appear in `merged.successes`
                    // twice.
                    let mut redirect_items: Vec<(usize, SpendItem)> = Vec::new();
                    let mut retry_sub_indices: std::collections::HashSet<u32> =
                        std::collections::HashSet::new();
                    for e in pe.errors {
                        if e.code == ERR_REDIRECT && (e.item_index as usize) < idx_map.len() {
                            let orig_idx = idx_map[e.item_index as usize];
                            redirect_items.push((orig_idx, items[orig_idx].clone()));
                            retry_sub_indices.insert(e.item_index);
                        } else {
                            let mut remapped = e;
                            if (remapped.item_index as usize) < idx_map.len() {
                                remapped.item_index = idx_map[remapped.item_index as usize] as u32;
                            }
                            all_errors.push(remapped);
                        }
                    }
                    for mut s in pe.successes {
                        if retry_sub_indices.contains(&s.item_index) {
                            continue;
                        }
                        if (s.item_index as usize) < idx_map.len() {
                            s.item_index = idx_map[s.item_index as usize] as u32;
                        }
                        merged.successes.push(s);
                    }
                    if !redirect_items.is_empty() {
                        // Retry redirected spends after routing refresh.
                        let _ = self.refresh_routing().await;
                        for (orig_idx, spend_item) in redirect_items {
                            let retry_payload = encode_spend_batch_payload(params, &[spend_item]);
                            if let Ok(pool) = cluster.pool_for_txid(&items[orig_idx].txid)
                                && let Ok(conn) = pool.get().await
                                && let Ok(retry_resp) =
                                    conn.round_trip(OP_SPEND_BATCH, 0, retry_payload).await
                            {
                                match Self::handle_signal_response(&retry_resp, 1) {
                                    Ok(r) => {
                                        for mut s in r.successes {
                                            s.item_index = orig_idx as u32;
                                            merged.successes.push(s);
                                        }
                                    }
                                    Err(ClientError::Partial(retry_pe)) => {
                                        for mut s in retry_pe.successes {
                                            s.item_index = orig_idx as u32;
                                            merged.successes.push(s);
                                        }
                                        for mut e in retry_pe.errors {
                                            e.item_index = orig_idx as u32;
                                            all_errors.push(e);
                                        }
                                    }
                                    Err(_) => {
                                        all_errors.push(BatchItemError {
                                            item_index: orig_idx as u32,
                                            code: ERR_REDIRECT,
                                            data: vec![],
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => return Err(e),
            }
        }

        merged.errors = all_errors.clone();
        if !all_errors.is_empty() {
            return Err(ClientError::Partial(PartialError {
                successes: merged.successes,
                errors: all_errors,
            }));
        }

        Ok(merged)
    }

    /// Send a batch unspend request.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Server`] on global error, [`ClientError::Partial`]
    /// on mixed success/failure, or [`ClientError::Connection`] on I/O failure.
    pub async fn unspend_batch(
        &self,
        params: &UnspendBatchParams,
        items: &[UnspendItem],
    ) -> Result<BatchResult, ClientError> {
        let payload = encode_unspend_batch_payload(params, items);
        let conn = if self.cluster.is_some() && !items.is_empty() {
            self.get_conn_for_txid(&items[0].txid).await?
        } else {
            self.get_conn().await?
        };
        let resp = conn.round_trip(OP_UNSPEND_BATCH, 0, payload).await?;
        Self::handle_mutation_response(&resp)
    }

    /// Mark transactions as mined in a specific block.
    ///
    /// Returns [`SpendBatchResponse`] with success signals and block IDs.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Server`] on global error, [`ClientError::Partial`]
    /// on mixed success/failure.
    pub async fn set_mined_batch(
        &self,
        params: &SetMinedBatchParams,
        txids: &[TxID],
    ) -> Result<BatchResult, ClientError> {
        let params = params.clone();
        self.send_txid_batch(OP_SET_MINED_BATCH, txids, &move |t: &[TxID]| {
            encode_set_mined_batch_payload(&params, t)
        })
        .await
    }

    /// Generic cluster-aware mutation batch: groups items by target node,
    /// sends sub-batches in parallel, merges results with index remapping.
    ///
    /// `get_txid` extracts the routing txid from each item.
    /// `encode_sub` encodes a sub-batch of items selected by index.
    async fn send_item_batch_cluster<T>(
        &self,
        op_code: u16,
        items: &[T],
        get_txid: impl Fn(&T) -> &TxID,
        encode_sub: impl Fn(&[T], &[usize]) -> Vec<u8> + Send + Sync + 'static,
    ) -> Result<BatchResult, ClientError>
    where
        T: Clone + Send + Sync + 'static,
    {
        if self.cluster.is_none() || items.is_empty() {
            let all_idx: Vec<usize> = (0..items.len()).collect();
            let payload = encode_sub(items, &all_idx);
            let conn = self.get_conn().await?;
            let resp = conn.round_trip(op_code, 0, payload).await?;
            return Self::handle_mutation_response(&resp);
        }

        let encode_sub_arc = Arc::new(encode_sub);

        // Retry once on transient errors (dead node / replication failure)
        // after routing refresh.
        for attempt in 0..=(TRANSIENT_MUTATION_RETRY_DELAYS_MS.len() as u32) {
            let result = self
                .send_item_batch_cluster_inner(op_code, items, &get_txid, &encode_sub_arc)
                .await;
            match result {
                Err(ClientError::Connection(msg)) if attempt == 0 => {
                    tracing::warn!(error = %msg, "client: retry after connection error");
                    let _ = self.refresh_routing().await;
                    continue;
                }
                Err(ClientError::Partial(pe))
                    if pe.errors.len() == items.len()
                        && all_errors_have_code(&pe.errors, ERR_MIGRATION_IN_PROGRESS)
                        && (attempt as usize) < TRANSIENT_MUTATION_RETRY_DELAYS_MS.len() =>
                {
                    tokio::time::sleep(Duration::from_millis(
                        TRANSIENT_MUTATION_RETRY_DELAYS_MS[attempt as usize],
                    ))
                    .await;
                    let _ = self.refresh_routing().await;
                    continue;
                }
                Err(ClientError::Partial(pe))
                    if pe.errors.len() == items.len()
                        && all_errors_have_code(&pe.errors, ERR_REDIRECT)
                        && (attempt as usize) < TRANSIENT_MUTATION_RETRY_DELAYS_MS.len() =>
                {
                    if let Some(redirect_groups) =
                        collect_redirect_groups(&pe.errors, &(0..items.len()).collect::<Vec<_>>())
                    {
                        match self
                            .retry_redirected_mutation_items(
                                op_code,
                                items,
                                redirect_groups,
                                &encode_sub_arc,
                            )
                            .await
                        {
                            Ok(result) => return Ok(result),
                            Err(ClientError::Partial(retry_pe))
                                if retry_pe.errors.len() < items.len() =>
                            {
                                return Err(ClientError::Partial(retry_pe));
                            }
                            Err(_) => {}
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(
                        TRANSIENT_MUTATION_RETRY_DELAYS_MS[attempt as usize],
                    ))
                    .await;
                    let _ = self.refresh_routing().await;
                    continue;
                }
                Err(ClientError::Partial(pe)) if attempt == 0 && pe.errors.len() == items.len() => {
                    let code_summary = summarize_error_codes(&pe.errors);
                    if let Some(redirect_groups) =
                        collect_redirect_groups(&pe.errors, &(0..items.len()).collect::<Vec<_>>())
                    {
                        match self
                            .retry_redirected_mutation_items(
                                op_code,
                                items,
                                redirect_groups,
                                &encode_sub_arc,
                            )
                            .await
                        {
                            Ok(result) => return Ok(result),
                            Err(ClientError::Partial(retry_pe))
                                if retry_pe.errors.len() < items.len() =>
                            {
                                return Err(ClientError::Partial(retry_pe));
                            }
                            Err(_) => {}
                        }
                    }
                    tracing::warn!(codes = %code_summary, "client: retry after all-items-failed partial error");
                    let _ = self.refresh_routing().await;
                    continue;
                }
                Err(ClientError::Server { code, .. }) if attempt == 0 && code == 15 => {
                    let _ = self.refresh_routing().await;
                    continue;
                }
                other => return other,
            }
        }
        unreachable!()
    }

    /// Inner implementation of cluster batch send. Separated so the outer
    /// function can retry on connection errors after routing refresh.
    async fn send_item_batch_cluster_inner<T>(
        &self,
        op_code: u16,
        items: &[T],
        get_txid: &impl Fn(&T) -> &TxID,
        encode_sub: &Arc<impl Fn(&[T], &[usize]) -> Vec<u8> + Send + Sync + 'static>,
    ) -> Result<BatchResult, ClientError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let cluster = self.cluster.as_ref().unwrap();

        // Group by target pool.
        let mut groups: PoolGroupMap = HashMap::new();
        for (i, item) in items.iter().enumerate() {
            let pool = cluster.pool_for_txid(get_txid(item))?;
            let key = Arc::as_ptr(&pool) as usize;
            groups
                .entry(key)
                .or_insert_with(|| (pool, Vec::new()))
                .1
                .push(i);
        }

        if groups.len() == 1 {
            let (pool, idx_map) = groups.into_values().next().unwrap();
            let payload = encode_sub(items, &idx_map);
            let conn = pool.get().await?;
            let resp = conn.round_trip(op_code, 0, payload).await?;
            return match Self::handle_mutation_response(&resp) {
                Ok(r) => Ok(r),
                Err(ClientError::Partial(mut pe)) => {
                    if let Some(redirect_groups) = collect_redirect_groups(&pe.errors, &idx_map) {
                        return self
                            .retry_redirected_mutation_items(
                                op_code,
                                items,
                                redirect_groups,
                                encode_sub,
                            )
                            .await;
                    }
                    remap_partial_items(&mut pe, &idx_map);
                    Err(ClientError::Partial(pe))
                }
                Err(e) => Err(e),
            };
        }

        // Multiple nodes — send in parallel.
        let items_arc = Arc::new(items.to_vec());
        let mut handles = Vec::with_capacity(groups.len());

        for (_, (pool, idx_map)) in groups {
            let items_ref = Arc::clone(&items_arc);
            let encoder = Arc::clone(encode_sub);

            handles.push(tokio::spawn(async move {
                let payload = encoder(&items_ref, &idx_map);
                let conn = pool.get().await?;
                let resp = conn.round_trip(op_code, 0, payload).await?;
                let result = Self::handle_mutation_response(&resp);
                Ok::<(Result<BatchResult, ClientError>, Vec<usize>), ClientError>((result, idx_map))
            }));
        }

        let mut all_errors: Vec<BatchItemError> = Vec::new();
        let mut got_no_quorum = false;
        let mut had_connection_error = false;
        for handle in handles {
            let join_result = handle
                .await
                .map_err(|e| ClientError::Connection(format!("join: {e}")))?;
            match join_result {
                Ok((result, idx_map)) => match result {
                    Ok(_) => {}
                    Err(ClientError::Partial(pe)) => {
                        all_errors.extend(remap_batch_errors(pe.errors, &idx_map));
                    }
                    Err(ClientError::Server { code, ref message })
                        if code == 15 || message.contains("no quorum") =>
                    {
                        got_no_quorum = true;
                    }
                    Err(e) => return Err(e),
                },
                Err(ClientError::Connection(_)) => {
                    had_connection_error = true;
                }
                Err(e) => return Err(e),
            }
        }

        if had_connection_error {
            let _ = self.refresh_routing().await;
            return Err(ClientError::Connection(
                "sub-batch to unreachable node (routing refreshed)".to_string(),
            ));
        }

        if got_no_quorum {
            let _ = self.refresh_routing().await;
            return Err(ClientError::Server {
                code: 15,
                message: "no quorum (routing refreshed, retry recommended)".to_string(),
            });
        }

        if !all_errors.is_empty() {
            return Err(ClientError::Partial(PartialError {
                successes: Vec::new(),
                errors: all_errors,
            }));
        }
        Ok(BatchResult { errors: Vec::new() })
    }

    async fn retry_redirected_mutation_items<T>(
        &self,
        op_code: u16,
        items: &[T],
        redirect_groups: HashMap<String, Vec<usize>>,
        encode_sub: &Arc<impl Fn(&[T], &[usize]) -> Vec<u8> + Send + Sync + 'static>,
    ) -> Result<BatchResult, ClientError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let cluster = self.cluster.as_ref().ok_or(ClientError::NoPartitionMap)?;
        let items_arc = Arc::new(items.to_vec());
        let mut handles = Vec::with_capacity(redirect_groups.len());

        for (addr, idx_map) in redirect_groups {
            let pool = cluster.pool_for_redirect_addr(&addr)?;
            let items_ref = Arc::clone(&items_arc);
            let encoder = Arc::clone(encode_sub);
            handles.push(tokio::spawn(async move {
                let payload = encoder(&items_ref, &idx_map);
                let conn = pool.get().await?;
                let resp = conn.round_trip(op_code, 0, payload).await?;
                let result = Self::handle_mutation_response(&resp);
                Ok::<(Result<BatchResult, ClientError>, Vec<usize>), ClientError>((result, idx_map))
            }));
        }

        let mut all_errors: Vec<BatchItemError> = Vec::new();
        let mut got_no_quorum = false;
        let mut had_connection_error = false;

        for handle in handles {
            let join_result = handle
                .await
                .map_err(|e| ClientError::Connection(format!("join: {e}")))?;
            match join_result {
                Ok((result, idx_map)) => match result {
                    Ok(_) => {}
                    Err(ClientError::Partial(pe)) => {
                        all_errors.extend(remap_batch_errors(pe.errors, &idx_map));
                    }
                    Err(ClientError::Server { code, ref message })
                        if code == 15 || message.contains("no quorum") =>
                    {
                        got_no_quorum = true;
                    }
                    Err(ClientError::Connection(_)) => {
                        had_connection_error = true;
                    }
                    Err(e) => return Err(e),
                },
                Err(ClientError::Connection(_)) => {
                    had_connection_error = true;
                }
                Err(e) => return Err(e),
            }
        }

        if had_connection_error {
            return Err(ClientError::Connection(
                "redirect retry connection error".to_string(),
            ));
        }

        if got_no_quorum {
            return Err(ClientError::Server {
                code: 15,
                message: "no quorum during redirect retry".to_string(),
            });
        }

        if !all_errors.is_empty() {
            return Err(ClientError::Partial(PartialError {
                successes: Vec::new(),
                errors: all_errors,
            }));
        }

        Ok(BatchResult { errors: Vec::new() })
    }

    /// Upload large cold_data as a blob in chunks before CREATE.
    ///
    /// Sends the data in [`BLOB_CHUNK_SIZE`] chunks via `OP_STREAM_CHUNK`,
    /// then finalizes with `OP_STREAM_END`. The chunks are routed to the
    /// shard master for the given txid (same node that will handle the CREATE).
    ///
    /// After `upload_blob` succeeds, the caller should send CREATE with empty
    /// `cold_data` and the `FLAG_EXTERNAL_BLOB` flag set (bit 3 = 0x08).
    ///
    /// # Parameters
    ///
    /// - `txid`: The 32-byte transaction ID that the blob is associated with.
    /// - `data`: The full blob data to upload.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Server`] if any chunk or finalize request fails,
    /// or [`ClientError::Connection`] on I/O failure.
    pub async fn upload_blob(&self, txid: &[u8; 32], data: &[u8]) -> Result<(), ClientError> {
        // All chunks must go to the SAME TCP connection because the server
        // tracks stream sessions per-connection. Acquire once and reuse.
        let conn = self.get_conn_for_txid(txid).await?;
        let mut offset: u64 = 0;

        for chunk in data.chunks(BLOB_CHUNK_SIZE) {
            let payload = codec::encode_stream_chunk(txid, offset, chunk);
            let resp = conn.round_trip(OP_STREAM_CHUNK, 0, payload).await?;
            if resp.status != STATUS_OK {
                if resp.status == STATUS_ERROR {
                    let (code, msg) = decode_error_payload(&resp.payload)?;
                    return Err(ClientError::Server { code, message: msg });
                }
                return Err(ClientError::Protocol(format!(
                    "stream chunk: unexpected status {}",
                    resp.status
                )));
            }
            offset += chunk.len() as u64;
        }

        // Finalize the stream on the same connection.
        let payload = codec::encode_stream_end(txid, data.len() as u64);
        let resp = conn.round_trip(OP_STREAM_END, 0, payload).await?;
        if resp.status != STATUS_OK {
            if resp.status == STATUS_ERROR {
                let (code, msg) = decode_error_payload(&resp.payload)?;
                return Err(ClientError::Server { code, message: msg });
            }
            return Err(ClientError::Protocol(format!(
                "stream end: unexpected status {}",
                resp.status
            )));
        }

        Ok(())
    }

    /// Create new transaction records.
    ///
    /// In cluster mode, items are automatically grouped by txid shard and
    /// sent to the correct nodes in parallel.
    ///
    /// Items with `cold_data` larger than [`BLOB_UPLOAD_THRESHOLD`] (1 MiB)
    /// are automatically uploaded via chunked blob streaming before the
    /// CREATE request. The wire item is sent with empty `cold_data` and the
    /// [`FLAG_EXTERNAL_BLOB`] flag set so the server knows to fetch from
    /// the blobstore.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Server`] on global error, [`ClientError::Partial`]
    /// on mixed success/failure, or [`ClientError::Connection`] on I/O failure.
    pub async fn create_batch(&self, items: &[CreateItem]) -> Result<BatchResult, ClientError> {
        // Check if any items need blob upload.
        let has_large_blobs = items
            .iter()
            .any(|i| i.cold_data.len() > BLOB_UPLOAD_THRESHOLD);

        if !has_large_blobs {
            // Fast path: no large blobs, send directly.
            return self
                .send_item_batch_cluster(
                    OP_CREATE_BATCH,
                    items,
                    |item| &item.txid,
                    |items, indices| {
                        let sub: Vec<CreateItem> =
                            indices.iter().map(|&i| items[i].clone()).collect();
                        encode_create_batch_payload(&sub)
                    },
                )
                .await;
        }

        // Slow path: upload large blobs first, then send modified items.
        let mut modified_items: Vec<CreateItem> = items.to_vec();

        for item in &mut modified_items {
            if item.cold_data.len() > BLOB_UPLOAD_THRESHOLD {
                // Upload the blob via chunked streaming.
                self.upload_blob(&item.txid, &item.cold_data).await?;
                // Clear cold_data and set the EXTERNAL_BLOB flag.
                item.cold_data = Vec::new();
                item.flags |= FLAG_EXTERNAL_BLOB;
            }
        }

        self.send_item_batch_cluster(
            OP_CREATE_BATCH,
            &modified_items,
            |item| &item.txid,
            |items, indices| {
                let sub: Vec<CreateItem> = indices.iter().map(|&i| items[i].clone()).collect();
                encode_create_batch_payload(&sub)
            },
        )
        .await
    }

    /// Freeze specific UTXO slots.
    ///
    /// In cluster mode, items are automatically grouped by txid shard.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Server`] on global error, [`ClientError::Partial`]
    /// on mixed success/failure.
    pub async fn freeze_batch(&self, items: &[FreezeItem]) -> Result<BatchResult, ClientError> {
        self.send_item_batch_cluster(
            OP_FREEZE_BATCH,
            items,
            |item| &item.txid,
            |items, indices| {
                let sub: Vec<FreezeItem> = indices.iter().map(|&i| items[i].clone()).collect();
                encode_freeze_batch_payload(&sub)
            },
        )
        .await
    }

    /// Unfreeze specific UTXO slots.
    ///
    /// In cluster mode, items are automatically grouped by txid shard.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Server`] on global error, [`ClientError::Partial`]
    /// on mixed success/failure.
    pub async fn unfreeze_batch(&self, items: &[FreezeItem]) -> Result<BatchResult, ClientError> {
        self.send_item_batch_cluster(
            OP_UNFREEZE_BATCH,
            items,
            |item| &item.txid,
            |items, indices| {
                let sub: Vec<FreezeItem> = indices.iter().map(|&i| items[i].clone()).collect();
                encode_freeze_batch_payload(&sub)
            },
        )
        .await
    }

    /// Reassign frozen UTXO slots with new hashes.
    ///
    /// In cluster mode, items are automatically grouped by txid shard.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Server`] on global error, [`ClientError::Partial`]
    /// on mixed success/failure.
    pub async fn reassign_batch(
        &self,
        params: &ReassignBatchParams,
        items: &[ReassignItem],
    ) -> Result<BatchResult, ClientError> {
        let params = params.clone();
        self.send_item_batch_cluster(
            OP_REASSIGN_BATCH,
            items,
            |item| &item.txid,
            move |items, indices| {
                let sub: Vec<ReassignItem> = indices.iter().map(|&i| items[i].clone()).collect();
                encode_reassign_batch_payload(&params, &sub)
            },
        )
        .await
    }

    /// Set or clear the conflicting flag on transactions.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Server`] on global error, [`ClientError::Partial`]
    /// on mixed success/failure.
    pub async fn set_conflicting_batch(
        &self,
        params: &SetConflictingParams,
        txids: &[TxID],
    ) -> Result<BatchResult, ClientError> {
        self.send_txid_batch(OP_SET_CONFLICTING_BATCH, txids, &|t| {
            encode_set_conflicting_payload(params, t)
        })
        .await
    }

    /// Set or clear the locked flag on transactions.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Server`] on global error, [`ClientError::Partial`]
    /// on mixed success/failure.
    pub async fn set_locked_batch(
        &self,
        value: bool,
        txids: &[TxID],
    ) -> Result<BatchResult, ClientError> {
        self.send_txid_batch(OP_SET_LOCKED_BATCH, txids, &|t| {
            encode_set_locked_payload(value, t)
        })
        .await
    }

    /// Set preserve_until on transactions.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Server`] on global error, [`ClientError::Partial`]
    /// on mixed success/failure.
    pub async fn preserve_until_batch(
        &self,
        block_height: u32,
        txids: &[TxID],
    ) -> Result<BatchResult, ClientError> {
        self.send_txid_batch(OP_PRESERVE_UNTIL_BATCH, txids, &|t| {
            encode_preserve_until_payload(block_height, t)
        })
        .await
    }

    /// Delete transactions.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Server`] on global error, [`ClientError::Partial`]
    /// on mixed success/failure.
    pub async fn delete_batch(&self, txids: &[TxID]) -> Result<BatchResult, ClientError> {
        self.send_txid_batch(OP_DELETE_BATCH, txids, &|t| encode_delete_payload(t))
            .await
    }

    /// Update longest-chain status for transactions.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Server`] on global error, [`ClientError::Partial`]
    /// on mixed success/failure.
    pub async fn mark_longest_chain_batch(
        &self,
        params: &MarkLongestChainParams,
        txids: &[TxID],
    ) -> Result<BatchResult, ClientError> {
        self.send_txid_batch(OP_MARK_LONGEST_CHAIN_BATCH, txids, &|t| {
            encode_mark_longest_chain_payload(params, t)
        })
        .await
    }

    // -----------------------------------------------------------------------
    // Read operations
    // -----------------------------------------------------------------------

    /// Retrieve transaction data for multiple txids.
    ///
    /// The `field_mask` controls which fields are returned
    /// ([`FIELD_ALL_METADATA`], [`FIELD_UTXO_SLOTS`], [`FIELD_COLD_DATA`],
    /// [`FIELD_BLOCK_ENTRIES`], or [`FIELD_ALL`]).
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Server`] on global error, or
    /// [`ClientError::Redirect`] if the server redirects.
    pub async fn get_batch(
        &self,
        field_mask: u32,
        txids: &[TxID],
    ) -> Result<GetBatchResult, ClientError> {
        // Retry once on connection error (dead node) after routing refresh.
        for attempt in 0..2u32 {
            match self.get_batch_inner(field_mask, txids).await {
                Ok(result) => return Ok(result),
                Err(ClientError::Connection(ref msg)) if attempt == 0 => {
                    tracing::warn!(error = %msg, "client: get_batch retry after connection error");
                    let _ = self.refresh_routing().await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!()
    }

    /// Inner get_batch implementation. Separated for retry on connection errors.
    async fn get_batch_inner(
        &self,
        field_mask: u32,
        txids: &[TxID],
    ) -> Result<GetBatchResult, ClientError> {
        let groups = self.group_txids(txids);

        // Single node or no cluster — send directly.
        if groups.is_none() || groups.as_ref().is_some_and(|g| g.len() <= 1) {
            let payload = encode_get_batch_payload(field_mask, txids);
            let conn = if let Some(ref groups) = groups {
                if let Some((pool, _)) = groups.values().next() {
                    pool.get().await?
                } else {
                    self.get_conn().await?
                }
            } else {
                self.get_conn_for_any_txid(txids).await?
            };
            let resp = conn.round_trip(OP_GET_BATCH, 0, payload).await?;
            return match resp.status {
                STATUS_OK => {
                    let items = decode_get_response(&resp.payload)?;
                    Ok(GetBatchResult { field_mask, items })
                }
                STATUS_ERROR => {
                    let (code, msg) = decode_error_payload(&resp.payload)?;
                    Err(ClientError::Server { code, message: msg })
                }
                STATUS_REDIRECT => {
                    let addr = decode_redirect(&resp.payload)?;
                    Err(ClientError::Redirect(addr))
                }
                other => Err(ClientError::Protocol(format!("unexpected status: {other}"))),
            };
        }

        // Multiple nodes — send sub-batches in parallel and reassemble.
        let groups = groups.unwrap();
        let total = txids.len();
        let mut handles = Vec::with_capacity(groups.len());

        for (_, (pool, idx_map)) in groups {
            let sub_txids: Vec<TxID> = idx_map.iter().map(|&i| txids[i]).collect();
            let payload = encode_get_batch_payload(field_mask, &sub_txids);

            handles.push(tokio::spawn(async move {
                let conn = pool.get().await?;
                let resp = conn.round_trip(OP_GET_BATCH, 0, payload).await?;
                let results = match resp.status {
                    STATUS_OK => decode_get_response(&resp.payload)?,
                    STATUS_ERROR => {
                        let (code, msg) = decode_error_payload(&resp.payload)?;
                        return Err(ClientError::Server { code, message: msg });
                    }
                    other => {
                        return Err(ClientError::Protocol(format!("unexpected status: {other}")));
                    }
                };
                Ok::<(Vec<GetResult>, Vec<usize>), ClientError>((results, idx_map))
            }));
        }

        let mut merged: Vec<Option<GetResult>> = (0..total).map(|_| None).collect();
        let mut had_connection_error = false;
        for handle in handles {
            let join_result = handle
                .await
                .map_err(|e| ClientError::Connection(format!("join: {e}")))?;
            match join_result {
                Ok((results, idx_map)) => {
                    for (sub_idx, result) in results.into_iter().enumerate() {
                        if sub_idx < idx_map.len() {
                            merged[idx_map[sub_idx]] = Some(result);
                        }
                    }
                }
                Err(ClientError::Connection(_)) => {
                    had_connection_error = true;
                }
                Err(e) => return Err(e),
            }
        }

        if had_connection_error {
            let _ = self.refresh_routing().await;
            return Err(ClientError::Connection(
                "sub-batch to unreachable node (routing refreshed)".to_string(),
            ));
        }

        let items = merged
            .into_iter()
            .map(|r| {
                r.unwrap_or(GetResult {
                    status: 1,
                    data: Vec::new(),
                })
            })
            .collect();
        Ok(GetBatchResult { field_mask, items })
    }

    /// Look up spend status for specific UTXO slots.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Server`] on global error.
    pub async fn get_spend_batch(
        &self,
        items: &[GetSpendItem],
    ) -> Result<Vec<GetSpendResult>, ClientError> {
        let payload = encode_get_spend_batch_payload(items);
        let conn = if self.cluster.is_some() && !items.is_empty() {
            self.get_conn_for_txid(&items[0].txid).await?
        } else {
            self.get_conn().await?
        };
        let resp = conn.round_trip(OP_GET_SPEND_BATCH, 0, payload).await?;
        match resp.status {
            STATUS_OK => decode_get_spend_response(&resp.payload),
            STATUS_ERROR => {
                let (code, msg) = decode_error_payload(&resp.payload)?;
                Err(ClientError::Server { code, message: msg })
            }
            other => Err(ClientError::Protocol(format!(
                "unexpected status: {}",
                other
            ))),
        }
    }

    // -----------------------------------------------------------------------
    // Pruner operations
    // -----------------------------------------------------------------------

    /// Query transactions that have been unmined since before `cutoff_height`.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Server`] on error.
    pub async fn query_old_unmined(&self, cutoff_height: u32) -> Result<Vec<TxID>, ClientError> {
        let payload = cutoff_height.to_le_bytes().to_vec();
        let conn = self.get_conn().await?;
        let resp = conn.round_trip(OP_QUERY_OLD_UNMINED, 0, payload).await?;
        if resp.status != STATUS_OK {
            if resp.status == STATUS_ERROR {
                let (code, msg) = decode_error_payload(&resp.payload)?;
                return Err(ClientError::Server { code, message: msg });
            }
            return Err(ClientError::Protocol(format!(
                "unexpected status: {}",
                resp.status
            )));
        }
        decode_query_old_unmined_response(&resp.payload)
    }

    /// Preserve transactions until the given block height.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Server`] on global error, [`ClientError::Partial`]
    /// on mixed success/failure.
    pub async fn preserve_transactions(
        &self,
        block_height: u32,
        txids: &[TxID],
    ) -> Result<BatchResult, ClientError> {
        let payload = encode_preserve_transactions_payload(block_height, txids);
        let conn = self.get_conn_for_any_txid(txids).await?;
        let resp = conn
            .round_trip(OP_PRESERVE_TRANSACTIONS, 0, payload)
            .await?;
        Self::handle_mutation_response(&resp)
    }

    /// Trigger deletion of expired preserved transactions.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Server`] on error.
    pub async fn process_expired_preservations(
        &self,
        current_height: u32,
    ) -> Result<ProcessExpiredResult, ClientError> {
        let payload = current_height.to_le_bytes().to_vec();
        let conn = self.get_conn().await?;
        let resp = conn
            .round_trip(OP_PROCESS_EXPIRED_PRESERVATIONS, 0, payload)
            .await?;
        if resp.status != STATUS_OK {
            if resp.status == STATUS_ERROR {
                let (code, msg) = decode_error_payload(&resp.payload)?;
                return Err(ClientError::Server { code, message: msg });
            }
            return Err(ClientError::Protocol(format!(
                "unexpected status: {}",
                resp.status
            )));
        }
        decode_process_expired_response(&resp.payload)
    }

    // -----------------------------------------------------------------------
    // Admin operations
    // -----------------------------------------------------------------------

    /// Send a ping and return the round-trip time.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Connection`] on I/O failure.
    pub async fn ping(&self) -> Result<Duration, ClientError> {
        let start = Instant::now();
        let conn = self.get_conn().await?;
        let resp = conn.round_trip(OP_PING, 0, Vec::new()).await?;
        if resp.status != STATUS_OK {
            return Err(ClientError::Protocol(format!(
                "ping: status {}",
                resp.status
            )));
        }
        Ok(start.elapsed())
    }

    /// Check the server health.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Connection`] on I/O failure, or a protocol
    /// error if the server returns a non-OK status.
    pub async fn health(&self) -> Result<(), ClientError> {
        let conn = self.get_conn().await?;
        let resp = conn.round_trip(OP_HEALTH, 0, Vec::new()).await?;
        if resp.status != STATUS_OK {
            return Err(ClientError::Protocol(format!(
                "health: status {}",
                resp.status
            )));
        }
        Ok(())
    }

    /// Fetch the current cluster partition map.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Server`] on error, or [`ClientError::Protocol`]
    /// if the partition map cannot be decoded.
    pub async fn get_partition_map(&self) -> Result<PartitionMap, ClientError> {
        // In cluster mode, return the cached partition map (already bootstrapped).
        if let Some(cl) = &self.cluster {
            return cl.cached_partition_map().ok_or(ClientError::NoPartitionMap);
        }

        // Single-node mode: fetch from the server.
        let conn = self.get_conn().await?;
        let resp = conn.round_trip(OP_GET_PARTITION_MAP, 0, Vec::new()).await?;
        if resp.status != STATUS_OK {
            if resp.status == STATUS_ERROR {
                let (code, msg) = decode_error_payload(&resp.payload)?;
                return Err(ClientError::Server { code, message: msg });
            }
            return Err(ClientError::Protocol(format!(
                "partition map: status {}",
                resp.status
            )));
        }
        cluster::decode_partition_map(&resp.payload)
    }

    /// Refresh the cluster routing table by re-fetching the partition map.
    ///
    /// In cluster mode this triggers an immediate partition map refresh.
    /// In single-node mode this is a no-op.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Connection`] if the refresh fails.
    pub async fn refresh_routing(&self) -> Result<(), ClientError> {
        if let Some(cl) = &self.cluster {
            return cl.refresh_partition_map().await;
        }
        Ok(())
    }

    /// Send a raw request to a specific server address and return the
    /// response status and payload.
    ///
    /// Creates a temporary connection to the given address, sends a single
    /// request frame, and returns `(status, payload)`. This is intended for
    /// test scenarios that need to bypass cluster routing (e.g., to read
    /// from a specific replica node with `FLAG_LOCAL_READ`).
    ///
    /// # Parameters
    ///
    /// - `addr`: The `host:port` address to connect to.
    /// - `op_code`: The operation code for the request.
    /// - `flags`: Request flags (e.g., `FLAG_LOCAL_READ`).
    /// - `payload`: The raw request payload bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Connection`] if the connection or request fails.
    pub async fn send_to_addr(
        &self,
        addr: &str,
        op_code: u16,
        flags: u16,
        payload: Vec<u8>,
    ) -> Result<(u8, Vec<u8>), ClientError> {
        let dial_timeout = Duration::from_secs(5);
        let conn = crate::conn::PipeConn::dial(addr, dial_timeout).await?;
        let resp = conn.round_trip(op_code, flags, payload).await?;
        Ok((resp.status, resp.payload))
    }
}

// ===========================================================================
// Payload encoding helpers (client types -> wire bytes)
// ===========================================================================

/// Encode a SpendBatch request payload from client types.
fn encode_spend_batch_payload(params: &SpendBatchParams, items: &[SpendItem]) -> Vec<u8> {
    let wire_params = codec::SpendBatchParams {
        ignore_conflicting: params.ignore_conflicting,
        ignore_locked: params.ignore_locked,
        current_block_height: params.current_block_height,
        block_height_retention: params.block_height_retention,
    };
    let wire_items: Vec<codec::WireSpendItem> = items
        .iter()
        .map(|i| codec::WireSpendItem {
            txid: i.txid,
            vout: i.vout,
            utxo_hash: i.utxo_hash,
            spending_data: i.spending_data,
        })
        .collect();
    codec::encode_spend_batch(&wire_params, &wire_items)
}

/// Encode an UnspendBatch request payload from client types.
fn encode_unspend_batch_payload(params: &UnspendBatchParams, items: &[UnspendItem]) -> Vec<u8> {
    let wire_params = codec::UnspendBatchParams {
        current_block_height: params.current_block_height,
        block_height_retention: params.block_height_retention,
    };
    let wire_items: Vec<codec::WireSlotItem> = items
        .iter()
        .map(|i| codec::WireSlotItem {
            txid: i.txid,
            vout: i.vout,
            utxo_hash: i.utxo_hash,
        })
        .collect();
    codec::encode_unspend_batch(&wire_params, &wire_items)
}

/// Encode a SetMinedBatch request payload from client types.
fn encode_set_mined_batch_payload(params: &SetMinedBatchParams, txids: &[TxID]) -> Vec<u8> {
    let wire_params = codec::SetMinedBatchParams {
        block_id: params.block_id,
        block_height: params.block_height,
        subtree_idx: params.subtree_idx,
        on_longest_chain: params.on_longest_chain,
        unset_mined: params.unset_mined,
        current_block_height: params.current_block_height,
        block_height_retention: params.block_height_retention,
    };
    codec::encode_set_mined_batch(&wire_params, txids)
}

/// Encode a CreateBatch request payload from client types.
fn encode_create_batch_payload(items: &[CreateItem]) -> Vec<u8> {
    let wire_items: Vec<codec::WireCreateItem> = items
        .iter()
        .map(|i| codec::WireCreateItem {
            txid: i.txid,
            tx_version: i.tx_version,
            locktime: i.locktime,
            fee: i.fee,
            size_in_bytes: i.size_in_bytes,
            extended_size: i.extended_size,
            is_coinbase: i.is_coinbase,
            spending_height: i.spending_height,
            created_at: i.created_at,
            flags: i.flags,
            utxo_hashes: i.utxo_hashes.clone(),
            cold_data: i.cold_data.clone(),
            block_height: i.mined_block_height.unwrap_or(0),
            mined_block_id: i.mined_block_id,
            mined_block_height: i.mined_block_height,
            mined_subtree_idx: i.mined_subtree_idx,
            parent_txids: i.parent_txids.clone(),
        })
        .collect();
    codec::encode_create_batch(&wire_items)
}

/// Encode a Freeze/Unfreeze batch request payload from client types.
fn encode_freeze_batch_payload(items: &[FreezeItem]) -> Vec<u8> {
    let wire_items: Vec<codec::WireSlotItem> = items
        .iter()
        .map(|i| codec::WireSlotItem {
            txid: i.txid,
            vout: i.vout,
            utxo_hash: i.utxo_hash,
        })
        .collect();
    codec::encode_slot_item_batch(&wire_items)
}

/// Encode a ReassignBatch request payload from client types.
fn encode_reassign_batch_payload(params: &ReassignBatchParams, items: &[ReassignItem]) -> Vec<u8> {
    let wire_params = codec::ReassignBatchParams {
        block_height: params.block_height,
        spendable_after: params.spendable_after,
    };
    let wire_items: Vec<codec::WireReassignItem> = items
        .iter()
        .map(|i| codec::WireReassignItem {
            txid: i.txid,
            vout: i.vout,
            utxo_hash: i.utxo_hash,
            new_utxo_hash: i.new_utxo_hash,
        })
        .collect();
    codec::encode_reassign_batch(&wire_params, &wire_items)
}

/// Encode a SetConflicting batch request payload.
fn encode_set_conflicting_payload(params: &SetConflictingParams, txids: &[TxID]) -> Vec<u8> {
    let mut shared = Vec::with_capacity(9);
    shared.push(u8::from(params.value));
    shared.extend_from_slice(&params.current_block_height.to_le_bytes());
    shared.extend_from_slice(&params.block_height_retention.to_le_bytes());
    codec::encode_txid_batch(txids, &shared)
}

/// Encode a SetLocked batch request payload.
fn encode_set_locked_payload(value: bool, txids: &[TxID]) -> Vec<u8> {
    let shared = vec![u8::from(value)];
    codec::encode_txid_batch(txids, &shared)
}

/// Encode a PreserveUntil batch request payload.
fn encode_preserve_until_payload(block_height: u32, txids: &[TxID]) -> Vec<u8> {
    let shared = block_height.to_le_bytes().to_vec();
    codec::encode_txid_batch(txids, &shared)
}

/// Encode a Delete batch request payload.
fn encode_delete_payload(txids: &[TxID]) -> Vec<u8> {
    codec::encode_txid_batch(txids, &[])
}

/// Encode a MarkLongestChain batch request payload.
fn encode_mark_longest_chain_payload(params: &MarkLongestChainParams, txids: &[TxID]) -> Vec<u8> {
    let mut shared = Vec::with_capacity(9);
    shared.push(u8::from(params.on_longest_chain));
    shared.extend_from_slice(&params.current_block_height.to_le_bytes());
    shared.extend_from_slice(&params.block_height_retention.to_le_bytes());
    codec::encode_txid_batch(txids, &shared)
}

/// Encode a GetBatch request payload.
fn encode_get_batch_payload(field_mask: u32, txids: &[TxID]) -> Vec<u8> {
    codec::encode_get_batch(field_mask, txids)
}

/// Encode a GetSpendBatch request payload.
fn encode_get_spend_batch_payload(items: &[GetSpendItem]) -> Vec<u8> {
    let wire_items: Vec<codec::WireGetSpendItem> = items
        .iter()
        .map(|i| codec::WireGetSpendItem {
            txid: i.txid,
            vout: i.vout,
        })
        .collect();
    codec::encode_get_spend_batch(&wire_items)
}

/// Encode a PreserveTransactions request payload.
fn encode_preserve_transactions_payload(block_height: u32, txids: &[TxID]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8 + txids.len() * 32);
    buf.extend_from_slice(&(txids.len() as u32).to_le_bytes());
    buf.extend_from_slice(&block_height.to_le_bytes());
    for txid in txids {
        buf.extend_from_slice(txid);
    }
    buf
}

// ===========================================================================
// Response decoding helpers (wire bytes -> client types)
// ===========================================================================

/// Decode a global error response payload.
fn decode_error_payload(data: &[u8]) -> Result<(u16, String), ClientError> {
    codec::decode_error_payload(data)
        .ok_or_else(|| ClientError::Protocol("malformed error payload".to_string()))
}

/// Decode a redirect response payload.
fn decode_redirect(data: &[u8]) -> Result<String, ClientError> {
    codec::decode_redirect(data)
        .ok_or_else(|| ClientError::Protocol("malformed redirect payload".to_string()))
}

/// Decode a sparse error list from a PartialError response.
fn decode_sparse_errors(data: &[u8]) -> Result<Vec<BatchItemError>, ClientError> {
    let wire_errors = codec::decode_sparse_errors(data)
        .ok_or_else(|| ClientError::Protocol("malformed sparse errors".to_string()))?;
    Ok(wire_errors
        .into_iter()
        .map(|e| BatchItemError {
            item_index: e.item_index,
            code: e.error_code,
            data: e.error_data,
        })
        .collect())
}

/// Decode a partial response with success signals and errors.
fn decode_partial_with_signals(
    data: &[u8],
) -> Result<(Vec<BatchItemSuccess>, Vec<BatchItemError>), ClientError> {
    let (wire_successes, wire_errors) = codec::decode_partial_with_signals(data)
        .ok_or_else(|| ClientError::Protocol("malformed partial signals".to_string()))?;
    let successes = wire_successes
        .into_iter()
        .map(|s| BatchItemSuccess {
            item_index: s.item_index,
            signal: s.signal,
            block_ids: s.block_ids,
        })
        .collect();
    let errors = wire_errors
        .into_iter()
        .map(|e| BatchItemError {
            item_index: e.item_index,
            code: e.error_code,
            data: e.error_data,
        })
        .collect();
    Ok((successes, errors))
}

/// Decode a GetBatch response payload.
fn decode_get_response(data: &[u8]) -> Result<Vec<GetResult>, ClientError> {
    let wire_results = codec::decode_get_response(data)
        .ok_or_else(|| ClientError::Protocol("malformed get response".to_string()))?;
    Ok(wire_results
        .into_iter()
        .map(|r| GetResult {
            status: r.status,
            data: r.data,
        })
        .collect())
}

/// Decode a GetSpendBatch response payload.
fn decode_get_spend_response(data: &[u8]) -> Result<Vec<GetSpendResult>, ClientError> {
    let wire_results = codec::decode_get_spend_response(data)
        .ok_or_else(|| ClientError::Protocol("malformed get spend response".to_string()))?;
    Ok(wire_results
        .into_iter()
        .map(|r| GetSpendResult {
            status: r.status,
            error_code: r.error_code,
            slot_status: r.slot_status,
            spending_data: r.spending_data,
        })
        .collect())
}

/// Decode a QueryOldUnmined response payload.
fn decode_query_old_unmined_response(data: &[u8]) -> Result<Vec<TxID>, ClientError> {
    if data.len() < 4 {
        return Err(ClientError::Protocol(format!(
            "query old unmined: need 4 bytes, have {}",
            data.len()
        )));
    }
    let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    if data.len() < 4 + count * 32 {
        return Err(ClientError::Protocol(
            "query old unmined: truncated".to_string(),
        ));
    }
    let mut txids = Vec::with_capacity(count);
    let mut pos = 4;
    for _ in 0..count {
        let mut txid = [0u8; 32];
        txid.copy_from_slice(&data[pos..pos + 32]);
        txids.push(txid);
        pos += 32;
    }
    Ok(txids)
}

/// Decode a ProcessExpiredPreservations response.
fn decode_process_expired_response(data: &[u8]) -> Result<ProcessExpiredResult, ClientError> {
    if data.len() < 8 {
        return Err(ClientError::Protocol(format!(
            "process expired: need 8 bytes, have {}",
            data.len()
        )));
    }
    Ok(ProcessExpiredResult {
        deleted: u32::from_le_bytes(data[0..4].try_into().unwrap()),
        failed: u32::from_le_bytes(data[4..8].try_into().unwrap()),
    })
}

// ===========================================================================
// Index remapping helpers for cluster-aware batching
// ===========================================================================

/// Remap batch item errors from sub-batch indices to original batch indices.
fn remap_batch_errors(errors: Vec<BatchItemError>, idx_map: &[usize]) -> Vec<BatchItemError> {
    errors
        .into_iter()
        .map(|mut e| {
            if (e.item_index as usize) < idx_map.len() {
                e.item_index = idx_map[e.item_index as usize] as u32;
            }
            e
        })
        .collect()
}

/// Remap signal result indices from sub-batch to original batch.
fn remap_signal_result(result: &mut SpendBatchResponse, idx_map: &[usize]) {
    for s in &mut result.successes {
        if (s.item_index as usize) < idx_map.len() {
            s.item_index = idx_map[s.item_index as usize] as u32;
        }
    }
    for e in &mut result.errors {
        if (e.item_index as usize) < idx_map.len() {
            e.item_index = idx_map[e.item_index as usize] as u32;
        }
    }
}

/// Remap partial error indices from sub-batch to original batch.
fn remap_partial_items(pe: &mut PartialError, idx_map: &[usize]) {
    for s in &mut pe.successes {
        if (s.item_index as usize) < idx_map.len() {
            s.item_index = idx_map[s.item_index as usize] as u32;
        }
    }
    for e in &mut pe.errors {
        if (e.item_index as usize) < idx_map.len() {
            e.item_index = idx_map[e.item_index as usize] as u32;
        }
    }
}

fn collect_redirect_groups(
    errors: &[BatchItemError],
    idx_map: &[usize],
) -> Option<HashMap<String, Vec<usize>>> {
    let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
    for err in errors {
        if err.code != ERR_REDIRECT || err.data.is_empty() {
            return None;
        }
        let addr = std::str::from_utf8(&err.data).ok()?.trim();
        if addr.is_empty() || (err.item_index as usize) >= idx_map.len() {
            return None;
        }
        groups
            .entry(addr.to_string())
            .or_default()
            .push(idx_map[err.item_index as usize]);
    }
    Some(groups)
}

fn summarize_error_codes(errors: &[BatchItemError]) -> String {
    let mut code_counts = std::collections::BTreeMap::new();
    for err in errors {
        *code_counts.entry(err.code).or_insert(0usize) += 1;
    }
    code_counts
        .iter()
        .map(|(code, count)| format!("{}={count}", crate::errors::error_code_string(*code)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn all_errors_have_code(errors: &[BatchItemError], code: u16) -> bool {
    !errors.is_empty() && errors.iter().all(|err| err.code == code)
}

const TRANSIENT_MUTATION_RETRY_DELAYS_MS: &[u64] = &[
    10, 25, 50, 100, 200, 400, 800, 1600, 3200, 5000, 5000, 5000, 5000, 5000,
];

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use teraslab::allocator::SlotAllocator;
    use teraslab::cluster::coordinator::{
        ClusterConfig as ServerClusterConfig, ClusterCoordinator, RunningCluster,
    };
    use teraslab::cluster::shards::{NodeId, ShardTable};
    use teraslab::config::ServerConfig;
    use teraslab::device::{BlockDevice, MemoryDevice};
    use teraslab::index::{DahIndex, Index, UnminedIndex};
    use teraslab::locks::StripedLocks;
    use teraslab::ops::engine::Engine;
    use teraslab::server::Server;

    struct TestNode {
        server: Arc<Server>,
        cluster: Arc<RunningCluster>,
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

    #[test]
    fn transient_mutation_retry_budget_covers_live_rebalance_window() {
        let total_ms: u64 = TRANSIENT_MUTATION_RETRY_DELAYS_MS.iter().sum();
        assert!(
            total_ms >= 30_000,
            "migration fences during Docker scale-up can exceed the old 6.4s retry window"
        );
        assert_eq!(TRANSIENT_MUTATION_RETRY_DELAYS_MS.last(), Some(&5000));
    }

    fn create_node(
        node_id: u64,
        tcp_port: u16,
        swim_port: u16,
        seed_swim_ports: &[u16],
    ) -> TestNode {
        create_node_with_rf(node_id, tcp_port, swim_port, seed_swim_ports, 2)
    }

    fn create_node_with_rf(
        node_id: u64,
        tcp_port: u16,
        swim_port: u16,
        seed_swim_ports: &[u16],
        replication_factor: u8,
    ) -> TestNode {
        let dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(32 * 1024 * 1024, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone()).unwrap();
        let index = Index::new(1000).unwrap();
        let engine = Arc::new(Engine::new(
            dev,
            index,
            alloc,
            StripedLocks::new(256),
            DahIndex::new(),
            UnminedIndex::new(),
        ));

        let seeds: Vec<std::net::SocketAddr> = seed_swim_ports
            .iter()
            .map(|port| format!("127.0.0.1:{port}").parse().unwrap())
            .collect();

        let cluster_config = ServerClusterConfig {
            self_id: NodeId(node_id),
            self_addr: format!("127.0.0.1:{tcp_port}").parse().unwrap(),
            swim_bind: format!("127.0.0.1:{swim_port}").parse().unwrap(),
            seed_nodes: seeds,
            replication_factor,
            probe_interval: Duration::from_millis(100),
            suspicion_timeout: Duration::from_secs(2),
            cluster_secret: None,
            max_migration_threads: 16,
            topology_propose_timeout: Duration::from_millis(300),
            migration_pool_size: 4,
            migration_batch_size: 100,
            persisted_incarnation: 0,
        };

        let coordinator = ClusterCoordinator::new(cluster_config, 1);
        let running = Arc::new(coordinator.start(
            engine.clone(),
            None,
            None,
            None,
            true,
            Duration::from_secs(3),
        ));

        let config = ServerConfig {
            listen_addr: format!("127.0.0.1:{tcp_port}"),
            max_connections: 64,
            max_batch_size: 4096,
            node_id,
            ..Default::default()
        };

        let server = Arc::new(Server::new(engine, config).with_cluster(running.clone()));
        let server_clone = server.clone();
        std::thread::spawn(move || {
            let _ = server_clone.run();
        });
        std::thread::sleep(Duration::from_millis(100));

        TestNode {
            server,
            cluster: running,
        }
    }

    fn shutdown_node(node: &TestNode) {
        node.cluster.shutdown();
        node.server.shutdown();
    }

    fn txid_for_shard(shard: u16) -> TxID {
        let mut txid = [0u8; 32];
        txid[..2].copy_from_slice(&shard.to_le_bytes());
        txid
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn create_inner_follows_redirect_target_without_partition_map_refresh() {
        let tcp1 = reserve_tcp_port();
        let tcp2 = reserve_tcp_port();
        let tcp3 = reserve_tcp_port();
        let swim1 = reserve_udp_port();
        let swim2 = reserve_udp_port();
        let swim3 = reserve_udp_port();

        let node1 = create_node(1, tcp1, swim1, &[]);
        let node2 = create_node(2, tcp2, swim2, &[swim1]);
        let node3 = create_node(3, tcp3, swim3, &[swim1]);

        tokio::time::sleep(Duration::from_secs(3)).await;

        let current_table = node2.cluster.shard_table().read().clone();
        let stale_table =
            ShardTable::compute_with_epoch(&[NodeId(1), NodeId(3), NodeId(2)], 2, 999);
        let (shard, actual_master, stale_master) = (0..teraslab::cluster::shards::NUM_SHARDS
            as u16)
            .find_map(|shard| {
                let actual = current_table.target_assignment(shard).master;
                let stale = stale_table.target_assignment(shard).master;
                (actual != stale && actual != NodeId(1) && stale != NodeId(1))
                    .then_some((shard, actual, stale))
            })
            .expect("should find a shard whose stale route points at the wrong remote node");

        {
            let shard_table = node1.cluster.shard_table();
            let mut guard = shard_table.write();
            guard.begin_handoff(&stale_table);
            guard.commit_shard(shard);
        }

        let client = Client::new(ClientConfig {
            seeds: vec![format!("127.0.0.1:{tcp1}")],
            cluster_refresh_interval: Duration::from_secs(3600),
            ..Default::default()
        })
        .await
        .expect("client should bootstrap from node1");

        let item = CreateItem {
            txid: txid_for_shard(shard),
            utxo_hashes: vec![[0x42; 32]],
            tx_version: 1,
            locktime: 0,
            fee: 100,
            size_in_bytes: 100,
            extended_size: 0,
            is_coinbase: false,
            spending_height: 0,
            created_at: 1710000000000,
            flags: 0,
            cold_data: vec![],
            mined_block_id: None,
            mined_block_height: None,
            mined_subtree_idx: None,
            parent_txids: vec![],
        };

        let result = client
            .send_item_batch_cluster_inner(
                OP_CREATE_BATCH,
                std::slice::from_ref(&item),
                &|item| &item.txid,
                &Arc::new(|items: &[CreateItem], indices: &[usize]| {
                    let sub: Vec<CreateItem> = indices.iter().map(|&i| items[i].clone()).collect();
                    encode_create_batch_payload(&sub)
                }),
            )
            .await;

        assert!(
            result.is_ok(),
            "stale node1 map routed shard {shard} to {stale_master:?}, but node {actual_master:?} should still be reachable via direct redirect follow: {result:?}"
        );

        client.close().await;
        shutdown_node(&node1);
        shutdown_node(&node2);
        shutdown_node(&node3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn create_batch_retries_migration_in_progress_until_fence_clears() {
        let tcp1 = reserve_tcp_port();
        let swim1 = reserve_udp_port();
        let node1 = create_node_with_rf(1, tcp1, swim1, &[], 1);

        let client = Client::new(ClientConfig {
            seeds: vec![format!("127.0.0.1:{tcp1}")],
            cluster_refresh_interval: Duration::from_secs(3600),
            ..Default::default()
        })
        .await
        .expect("client should bootstrap from node1");

        let txid = txid_for_shard(123);
        let shard = crate::cluster::shard_for_txid(&txid);
        node1.cluster.fenced_bitmap().set(shard);

        let cluster = Arc::clone(&node1.cluster);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(75)).await;
            cluster.fenced_bitmap().clear(shard);
        });

        let item = CreateItem {
            txid,
            utxo_hashes: vec![[0x24; 32]],
            tx_version: 1,
            locktime: 0,
            fee: 100,
            size_in_bytes: 100,
            extended_size: 0,
            is_coinbase: false,
            spending_height: 0,
            created_at: 1710000000000,
            flags: 0,
            cold_data: vec![],
            mined_block_id: None,
            mined_block_height: None,
            mined_subtree_idx: None,
            parent_txids: vec![],
        };

        let result = tokio::time::timeout(Duration::from_secs(2), client.create_batch(&[item]))
            .await
            .expect("create_batch should not hang");

        assert!(
            result.is_ok(),
            "client should retry transient MIGRATION_IN_PROGRESS until the fence clears: {result:?}"
        );

        client.close().await;
        shutdown_node(&node1);
    }

    #[tokio::test]
    async fn create_batch_retries_migration_in_progress_for_long_rebalance_window() {
        let tcp1 = reserve_tcp_port();
        let swim1 = reserve_udp_port();
        let node1 = create_node_with_rf(1, tcp1, swim1, &[], 1);

        let client = Client::new(ClientConfig {
            seeds: vec![format!("127.0.0.1:{tcp1}")],
            cluster_refresh_interval: Duration::from_secs(3600),
            ..Default::default()
        })
        .await
        .expect("client should bootstrap from node1");

        let txid = txid_for_shard(321);
        let shard = crate::cluster::shard_for_txid(&txid);
        node1.cluster.fenced_bitmap().set(shard);

        let cluster = Arc::clone(&node1.cluster);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(4500)).await;
            cluster.fenced_bitmap().clear(shard);
        });

        let item = CreateItem {
            txid,
            utxo_hashes: vec![[0x42; 32]],
            tx_version: 1,
            locktime: 0,
            fee: 100,
            size_in_bytes: 100,
            extended_size: 0,
            is_coinbase: false,
            spending_height: 0,
            created_at: 1710000000000,
            flags: 0,
            cold_data: vec![],
            mined_block_id: None,
            mined_block_height: None,
            mined_subtree_idx: None,
            parent_txids: vec![],
        };

        let result = tokio::time::timeout(Duration::from_secs(8), client.create_batch(&[item]))
            .await
            .expect("create_batch should not hang");

        assert!(
            result.is_ok(),
            "client should keep retrying MIGRATION_IN_PROGRESS long enough for scale-up windows: {result:?}"
        );

        client.close().await;
        shutdown_node(&node1);
    }

    /// Pattern D regression: `spend_batch` on a fully-successful batch must
    /// return `successes` populated with one entry per input item. The
    /// server sends an empty payload on `STATUS_OK` (per-item detail is
    /// only encoded on partial failure), so the client is responsible for
    /// reconstructing per-item success information from the request. If
    /// `successes` is empty here, any caller that guards on
    /// `!resp.successes.is_empty()` (as scenario 10 did) silently drops
    /// every successful spend from its metrics and from its verifier.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn spend_batch_populates_successes_on_full_success() {
        let tcp1 = reserve_tcp_port();
        let swim1 = reserve_udp_port();
        let node1 = create_node_with_rf(1, tcp1, swim1, &[], 1);

        let client = Client::new(ClientConfig {
            seeds: vec![format!("127.0.0.1:{tcp1}")],
            cluster_refresh_interval: Duration::from_secs(3600),
            ..Default::default()
        })
        .await
        .expect("client should bootstrap from node1");

        // Create a record with two UTXO slots so the spend batch below
        // exercises multi-item success reporting.
        let txid = txid_for_shard(7);
        let utxo_a = [0xAA; 32];
        let utxo_b = [0xBB; 32];
        let create_item = CreateItem {
            txid,
            utxo_hashes: vec![utxo_a, utxo_b],
            tx_version: 1,
            locktime: 0,
            fee: 100,
            size_in_bytes: 100,
            extended_size: 0,
            is_coinbase: false,
            spending_height: 0,
            created_at: 1710000000000,
            flags: 0,
            cold_data: vec![],
            mined_block_id: None,
            mined_block_height: None,
            mined_subtree_idx: None,
            parent_txids: vec![],
        };
        client
            .create_batch(&[create_item])
            .await
            .expect("create_batch should succeed on a freshly-started node");

        let spend_items = vec![
            SpendItem {
                txid,
                vout: 0,
                utxo_hash: utxo_a,
                spending_data: [0xC1; 36],
            },
            SpendItem {
                txid,
                vout: 1,
                utxo_hash: utxo_b,
                spending_data: [0xC2; 36],
            },
        ];
        let params = SpendBatchParams {
            ignore_conflicting: true,
            ignore_locked: true,
            current_block_height: 1000,
            block_height_retention: 288,
        };

        let resp = client
            .spend_batch(&params, &spend_items)
            .await
            .expect("fully-successful spend_batch must return Ok");

        assert!(
            resp.errors.is_empty(),
            "expected no per-item errors on a fully-successful spend_batch, got {:?}",
            resp.errors,
        );
        assert_eq!(
            resp.successes.len(),
            spend_items.len(),
            "fully-successful spend_batch must report per-item success for each request item; \
             got {} success entries for a batch of {} items",
            resp.successes.len(),
            spend_items.len(),
        );
        let mut idxs: Vec<u32> = resp.successes.iter().map(|s| s.item_index).collect();
        idxs.sort_unstable();
        assert_eq!(
            idxs,
            vec![0, 1],
            "success entries should reference each input index"
        );

        client.close().await;
        shutdown_node(&node1);
    }
}
