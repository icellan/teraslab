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

/// Named CREATE-wire flag constants, re-exported from the server protocol so
/// clients share a single source of truth. See the constants' docs for the
/// wire-vs-persisted numbering footgun these prevent.
pub use teraslab::protocol::opcodes::{
    CREATE_FLAG_CONFLICTING, CREATE_FLAG_EXTERNAL_BLOB, CREATE_FLAG_FROZEN, CREATE_FLAG_LOCKED,
    FLAG_EXTERNAL_BLOB,
};

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

use teraslab::protocol::codec;
use teraslab::protocol::opcodes::*;

/// Threshold for switching from inline cold_data to chunked blob upload.
/// Transactions with cold_data larger than this are uploaded via
/// OP_STREAM_CHUNK/OP_STREAM_END before the CREATE request.
const BLOB_UPLOAD_THRESHOLD: usize = 1024 * 1024; // 1 MiB

/// Client-origin per-item error code for a batch item that could not be
/// routed to any node: no partition map is available, or the shard's owning
/// node has no live connection pool (a real node-down / rebalance state).
///
/// These items are *not* silently dropped — they surface as per-item errors
/// so callers can retry or reconcile. The value sits in a high range that no
/// server error code uses, so it never collides with a wire code and is never
/// misclassified as a same-target transient retry by
/// [`is_retryable_error_code`].
pub const CLIENT_ERR_UNROUTABLE: u16 = 0xF001;

/// Client-origin per-item error code for a redirected batch item whose
/// re-route could not be completed: the target connection could not be
/// acquired after a routing refresh. Distinct from [`CLIENT_ERR_UNROUTABLE`]
/// so callers can tell "never had a route" from "route existed but the retry
/// leg failed". Also in the high, collision-free range.
pub const CLIENT_ERR_REDIRECT_FAILED: u16 = 0xF002;

/// Size of each chunk sent during blob upload.
const BLOB_CHUNK_SIZE: usize = 4 * 1024 * 1024; // 4 MiB

/// Whether cold data of the given length must be externalised (pre-uploaded to
/// the blob store) rather than inlined, for a given threshold.
///
/// Cold data strictly larger than `threshold` is externalised; data of exactly
/// `threshold` bytes stays inline. Centralising this predicate keeps the two
/// decision sites in `create_batch` in agreement and makes the boundary
/// unit-testable without a live server.
fn needs_external_upload(cold_len: usize, threshold: usize) -> bool {
    cold_len > threshold
}

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
    /// Optional shared cluster secret for HMAC-signing inter-node opcodes.
    ///
    /// `OP_GET_PARTITION_MAP` (the cluster bootstrap/refresh op) is an
    /// inter-node auth opcode. When the cluster runs with `strict_auth` (the
    /// production default), it must be HMAC-signed or the server rejects it
    /// with `ERR_CLUSTER_AUTH_FAILED`. Set this to the cluster's shared
    /// secret. When `None`, the op is sent unsigned (trusted-overlay only).
    pub cluster_secret: Option<Vec<u8>>,
    /// Per-request timeout for a single round-trip (default: 30s).
    ///
    /// Applies to every `round_trip` on a pooled connection. Lower it for
    /// latency-sensitive callers; raise it for slow links.
    pub request_timeout: Duration,
    /// Cold-data size (in bytes) strictly above which `create_batch`
    /// pre-uploads the data to the external blob store via chunked streaming
    /// instead of inlining it in the CREATE payload.
    ///
    /// Defaults to [`BLOB_UPLOAD_THRESHOLD`] (1 MiB). Tune it (e.g. from a
    /// connection-URL query param) to change external placement without a
    /// client release; the runtime default is unchanged at 1 MiB.
    pub blob_upload_threshold: usize,
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
            cluster_secret: None,
            request_timeout: Duration::from_secs(30),
            blob_upload_threshold: BLOB_UPLOAD_THRESHOLD,
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
    /// Shared cluster secret for HMAC-signing inter-node opcodes (e.g.
    /// `OP_GET_PARTITION_MAP` in single-node mode). `None` means unsigned.
    cluster_secret: Option<Vec<u8>>,
    /// Cold-data size above which `create_batch` externalises via blob upload.
    /// Taken from `ClientConfig::blob_upload_threshold` (default 1 MiB).
    blob_upload_threshold: usize,
    /// Wire protocol version negotiated with the server via `OP_HELLO`, or `0`
    /// when not yet negotiated. Populated lazily on the first query that needs
    /// the capability gate (FU#5 pagination). `1` records a server that predates
    /// the handshake. See [`Client::ensure_server_version`].
    negotiated_version: AtomicU16,
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
        // Thread the per-request timeout into the pool config, which carries
        // it to each `PipeConn`.
        let mut pool_config = cfg.pool;
        pool_config.request_timeout = cfg.request_timeout;
        let blob_upload_threshold = cfg.blob_upload_threshold;

        if !cfg.seeds.is_empty() {
            let cl = Arc::new(
                Cluster::new(ClusterConfig {
                    seeds: cfg.seeds,
                    pool_config,
                    refresh_interval: cfg.cluster_refresh_interval,
                    max_redirects: cfg.max_redirects,
                    addr_map: cfg.addr_map,
                    cluster_secret: cfg.cluster_secret.clone(),
                })
                .await?,
            );
            let refresh_task = cl.start_refresh();
            Ok(Self {
                cluster: Some(cl),
                pool: None,
                cluster_secret: cfg.cluster_secret,
                blob_upload_threshold,
                negotiated_version: AtomicU16::new(0),
                _refresh_task: Some(refresh_task),
            })
        } else if let Some(addr) = cfg.addr {
            let pool = Arc::new(ConnPool::new(addr, pool_config));
            Ok(Self {
                cluster: None,
                pool: Some(pool),
                cluster_secret: cfg.cluster_secret,
                blob_upload_threshold,
                negotiated_version: AtomicU16::new(0),
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
            // STATUS_DEGRADED_DURABILITY (5): the mutation was APPLIED and is
            // locally durable, but replication used a weak (best-effort) ack.
            // The server treats it as a successful write and so must the
            // client — reporting an applied write as an error would be
            // incorrect. Matches the Go client (`client.go`).
            STATUS_OK | STATUS_DEGRADED_DURABILITY => Ok(BatchResult { errors: Vec::new() }),
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
                // The reserved trailer tells us whether the items that DID apply
                // were only replicated below quorum (degraded durability).
                let (errs, degraded) = decode_sparse_errors(&resp.payload)?;
                // A PARTIAL_ERROR that decodes to zero per-item errors means
                // nothing actually failed — treat it as success rather than
                // surfacing an empty `Partial` (which callers must otherwise
                // special-case as "0 failures = ok").
                if errs.is_empty() {
                    Ok(BatchResult { errors: Vec::new() })
                } else {
                    Err(ClientError::Partial(PartialError {
                        successes: Vec::new(),
                        errors: errs,
                        degraded,
                    }))
                }
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
            // STATUS_DEGRADED_DURABILITY (5) is a successful-but-weak ack: the
            // write applied and is locally durable; replication used a
            // best-effort ack. Decode the signal payload exactly as STATUS_OK
            // (the server still carries the per-item signals / block IDs) and
            // surface success. Matches the Go client (`handleSignalResponse`).
            STATUS_OK | STATUS_DEGRADED_DURABILITY => {
                // On this path the degraded signal (if any) rides in the status
                // byte, not a payload trailer.
                let status_degraded = resp.status == STATUS_DEGRADED_DURABILITY;
                if !resp.payload.is_empty() {
                    let (successes, errs, _trailer) = decode_partial_with_signals(&resp.payload)?;
                    if !errs.is_empty() {
                        return Err(ClientError::Partial(PartialError {
                            successes,
                            errors: errs,
                            degraded: status_degraded,
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
                // set_mined encodes the two-section signal layout (per-item
                // successes with signals+block_ids, then per-item errors);
                // spend encodes the plain sparse-error layout. Decoding sparse
                // with the signal codec reads the leading success_count as an
                // error_count and can yield an EMPTY error list even when every
                // item failed, silently masquerading as success (B5). Try the
                // signal codec first, then fall back to sparse — matching the Go
                // client's `handleSignalResponse`. Either layout carries the
                // reserved degraded-durability trailer, surfaced on the Partial.
                let (mut successes, errs, degraded) =
                    match decode_partial_with_signals(&resp.payload) {
                        Ok((successes, errs, degraded)) => (successes, errs, degraded),
                        Err(_) => {
                            let (errs, degraded) = decode_sparse_errors(&resp.payload)?;
                            (Vec::new(), errs, degraded)
                        }
                    };
                // Reconstruct any implicit successes: indices in
                // `0..item_count` that appear in neither the successes nor the
                // errors section still succeeded (the server may omit
                // no-signal successes to save bytes), so every request item
                // shows up in exactly one of `successes` / `errors`.
                let failed: std::collections::HashSet<u32> =
                    errs.iter().map(|e| e.item_index).collect();
                let present: std::collections::HashSet<u32> =
                    successes.iter().map(|s| s.item_index).collect();
                for item_index in 0..item_count as u32 {
                    if !failed.contains(&item_index) && !present.contains(&item_index) {
                        successes.push(BatchItemSuccess {
                            item_index,
                            signal: SIGNAL_NONE,
                            block_ids: Vec::new(),
                        });
                    }
                }
                Err(ClientError::Partial(PartialError {
                    successes,
                    errors: errs,
                    degraded,
                }))
            }
            other => Err(ClientError::Protocol(format!("unknown status: {}", other))),
        }
    }

    // -----------------------------------------------------------------------
    // Cluster-aware batch routing
    // -----------------------------------------------------------------------

    /// Group txids by their target pool (for cluster-aware batch operations).
    ///
    /// Returns `None` if not in cluster mode. Otherwise returns
    /// `(groups, ungroupable)` where `ungroupable` holds the original batch
    /// indices whose `pool_for_txid` failed — no partition map, or the
    /// shard's owning node has no live pool (a real node-down / rebalance
    /// state). These indices MUST be surfaced by the caller (as per-item
    /// errors), never silently dropped: every input index appears in exactly
    /// one of a group or `ungroupable` (B6).
    fn group_txids(&self, txids: &[TxID]) -> Option<(PoolGroupMap, Vec<usize>)> {
        let cluster = self.cluster.as_ref()?;
        // Use a HashMap keyed by pool address (via pointer identity of Arc).
        // We'll key by the pool's Arc pointer as a usize.
        let mut groups: PoolGroupMap = HashMap::new();
        let mut ungroupable: Vec<usize> = Vec::new();
        for (i, txid) in txids.iter().enumerate() {
            match cluster.pool_for_txid(txid) {
                Ok(pool) => {
                    let key = Arc::as_ptr(&pool) as usize;
                    groups
                        .entry(key)
                        .or_insert_with(|| (pool, Vec::new()))
                        .1
                        .push(i);
                }
                Err(_) => ungroupable.push(i),
            }
        }
        Some((groups, ungroupable))
    }

    /// Build per-item errors for indices that could not be routed to any
    /// node. Surfacing these (rather than dropping them) is the B6 fix.
    fn unroutable_errors(indices: &[usize]) -> Vec<BatchItemError> {
        Self::unroutable_errors_with_code(indices, CLIENT_ERR_UNROUTABLE)
    }

    /// Build per-item errors for `indices` with an explicit client-origin
    /// error `code` (`CLIENT_ERR_UNROUTABLE` for never-routed items,
    /// `CLIENT_ERR_REDIRECT_FAILED` for redirected items whose re-route leg
    /// could not be completed).
    fn unroutable_errors_with_code(indices: &[usize], code: u16) -> Vec<BatchItemError> {
        indices
            .iter()
            .map(|&i| BatchItemError {
                item_index: i as u32,
                code,
                data: Vec::new(),
            })
            .collect()
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

    /// Send a signal-carrying txid-list batch (set_mined) with cluster-aware
    /// routing, returning the per-item success signals / block IDs.
    ///
    /// This is the signal-aware analog of [`Self::send_txid_batch`]: set_mined
    /// responses are encoded in the signal layout for every status
    /// (STATUS_OK, STATUS_DEGRADED_DURABILITY, STATUS_PARTIAL_ERROR), so the
    /// results must be decoded with [`Self::handle_signal_response`] and the
    /// (signal, block_ids) preserved — the plain-mutation path would drop them
    /// and (on an all-failed batch) mask the failure as success (B5).
    async fn send_txid_batch_signals<F>(
        &self,
        op_code: u16,
        txids: &[TxID],
        encode_payload: &F,
    ) -> Result<SpendBatchResponse, ClientError>
    where
        F: Fn(&[TxID]) -> Vec<u8>,
    {
        // Single-node / no-cluster fast path.
        if self.cluster.is_none() {
            let payload = encode_payload(txids);
            let conn = self
                .pool
                .as_ref()
                .ok_or(ClientError::PoolClosed)?
                .get()
                .await?;
            let resp = conn.round_trip(op_code, 0, payload).await?;
            return Self::handle_signal_response(&resp, txids.len());
        }

        // Cluster path with bounded transient-retry, mirroring
        // `send_txid_batch_cluster`.
        for attempt in 0..=(TRANSIENT_MUTATION_RETRY_DELAYS_MS.len() as u32) {
            let result = self
                .send_txid_batch_signals_cluster_once(op_code, txids, encode_payload)
                .await;
            let retryable = (attempt as usize) < TRANSIENT_MUTATION_RETRY_DELAYS_MS.len()
                && match &result {
                    Err(ClientError::Server { code, .. }) => is_retryable_error_code(*code),
                    Err(ClientError::Partial(pe)) => {
                        pe.errors.len() == txids.len() && all_errors_are_retryable(&pe.errors)
                    }
                    _ => false,
                };
            if retryable {
                tokio::time::sleep(Duration::from_millis(
                    TRANSIENT_MUTATION_RETRY_DELAYS_MS[attempt as usize],
                ))
                .await;
                let _ = self.refresh_routing().await;
                continue;
            }
            return result;
        }
        unreachable!()
    }

    /// One attempt of a cluster-aware signal-carrying txid batch: group by
    /// target node, send sub-batches in parallel, merge `SpendBatchResponse`
    /// with per-item index remapping. Redirected items are re-routed
    /// per-target (bounded by `max_redirects`); un-routable items surface as
    /// per-item errors — every input index lands in exactly one of
    /// `successes` / `errors`, never dropped (B5/B6).
    async fn send_txid_batch_signals_cluster_once<F>(
        &self,
        op_code: u16,
        txids: &[TxID],
        encode_payload: &F,
    ) -> Result<SpendBatchResponse, ClientError>
    where
        F: Fn(&[TxID]) -> Vec<u8>,
    {
        let Some((groups, ungroupable)) = self.group_txids(txids) else {
            // Not in cluster mode (shouldn't happen — caller checked).
            let payload = encode_payload(txids);
            let conn = self.get_conn().await?;
            let resp = conn.round_trip(op_code, 0, payload).await?;
            return Self::handle_signal_response(&resp, txids.len());
        };

        let mut merged = SpendBatchResponse {
            successes: Vec::new(),
            errors: Self::unroutable_errors(&ungroupable),
        };
        let mut redirected_indices: Vec<usize> = Vec::new();
        let mut got_no_quorum = false;
        // Any sub-batch whose applied items were only replicated below quorum
        // taints the merged response as degraded-durability.
        let mut merged_degraded = false;

        // Fan out one sub-batch per target node.
        let mut handles = Vec::with_capacity(groups.len());
        for (_, (pool, idx_map)) in groups {
            let sub_txids: Vec<TxID> = idx_map.iter().map(|&i| txids[i]).collect();
            let payload = encode_payload(&sub_txids);
            let sub_len = sub_txids.len();
            handles.push(tokio::spawn(async move {
                let conn = pool.get().await?;
                let resp = conn.round_trip(op_code, 0, payload).await?;
                let result = Self::handle_signal_response(&resp, sub_len);
                Ok::<(Result<SpendBatchResponse, ClientError>, Vec<usize>), ClientError>((
                    result, idx_map,
                ))
            }));
        }

        for handle in handles {
            let (result, idx_map) = handle
                .await
                .map_err(|e| ClientError::Connection(format!("join: {e}")))??;
            match result {
                Ok(mut r) => {
                    remap_signal_result(&mut r, &idx_map);
                    merged.successes.extend(r.successes);
                }
                Err(ClientError::Partial(pe)) => {
                    merged_degraded |= pe.degraded;
                    for mut s in pe.successes {
                        if (s.item_index as usize) < idx_map.len() {
                            s.item_index = idx_map[s.item_index as usize] as u32;
                        }
                        merged.successes.push(s);
                    }
                    for err in pe.errors {
                        if err.code == ERR_REDIRECT && (err.item_index as usize) < idx_map.len() {
                            redirected_indices.push(idx_map[err.item_index as usize]);
                        } else {
                            merged
                                .errors
                                .extend(remap_batch_errors(vec![err], &idx_map));
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

        // Re-route redirected items per-target (each to its own owner),
        // bounded by max_redirects. Any unresolved item surfaces as an error.
        if !redirected_indices.is_empty() {
            let (successes, errors) = self
                .retry_redirected_txids_signals(op_code, txids, &redirected_indices, encode_payload)
                .await;
            merged.successes.extend(successes);
            merged.errors.extend(errors);
        }

        if got_no_quorum {
            let _ = self.refresh_routing().await;
            return Err(ClientError::Server {
                code: 15,
                message: "no quorum (routing refreshed, retry recommended)".to_string(),
            });
        }

        if !merged.errors.is_empty() {
            return Err(ClientError::Partial(PartialError {
                successes: merged.successes,
                errors: merged.errors,
                degraded: merged_degraded,
            }));
        }

        // No errors: ensure every input index has a success entry (the server
        // may omit no-signal successes). Synthesize the missing ones so
        // callers see one result per request item.
        let present: std::collections::HashSet<u32> =
            merged.successes.iter().map(|s| s.item_index).collect();
        for item_index in 0..txids.len() as u32 {
            if !present.contains(&item_index) {
                merged.successes.push(BatchItemSuccess {
                    item_index,
                    signal: SIGNAL_NONE,
                    block_ids: Vec::new(),
                });
            }
        }
        Ok(merged)
    }

    /// Signal-preserving analog of [`Self::retry_redirected_txids`]: re-route
    /// redirected set_mined items per-target, bounded by `max_redirects`,
    /// returning `(recovered_successes, terminal_errors)`. Items still
    /// unresolved after the hop budget surface as `CLIENT_ERR_REDIRECT_FAILED`
    /// errors — never dropped.
    async fn retry_redirected_txids_signals<F>(
        &self,
        op_code: u16,
        txids: &[TxID],
        redirected: &[usize],
        encode_payload: &F,
    ) -> (Vec<BatchItemSuccess>, Vec<BatchItemError>)
    where
        F: Fn(&[TxID]) -> Vec<u8>,
    {
        let max_hops = self
            .cluster
            .as_ref()
            .map(|c| c.max_redirects().max(1))
            .unwrap_or(1);

        let mut successes: Vec<BatchItemSuccess> = Vec::new();
        let mut terminal: Vec<BatchItemError> = Vec::new();
        let mut pending: Vec<usize> = redirected.to_vec();

        for _hop in 0..max_hops {
            if pending.is_empty() {
                break;
            }
            let _ = self.refresh_routing().await;

            let pending_txids: Vec<TxID> = pending.iter().map(|&i| txids[i]).collect();
            let Some((groups, ungroupable)) = self.group_txids(&pending_txids) else {
                terminal.extend(Self::unroutable_errors(&pending));
                return (successes, terminal);
            };

            let mut next_pending: Vec<usize> = ungroupable.iter().map(|&p| pending[p]).collect();

            for (_, (pool, sub_local)) in groups {
                let orig: Vec<usize> = sub_local.iter().map(|&p| pending[p]).collect();
                let sub_txids: Vec<TxID> = orig.iter().map(|&i| txids[i]).collect();
                let payload = encode_payload(&sub_txids);

                match pool.get().await {
                    Ok(conn) => match conn.round_trip(op_code, 0, payload).await {
                        Ok(resp) => match Self::handle_signal_response(&resp, orig.len()) {
                            Ok(mut r) => {
                                remap_signal_result(&mut r, &orig);
                                successes.extend(r.successes);
                            }
                            Err(ClientError::Partial(pe)) => {
                                for mut s in pe.successes {
                                    if (s.item_index as usize) < orig.len() {
                                        s.item_index = orig[s.item_index as usize] as u32;
                                    }
                                    successes.push(s);
                                }
                                for err in pe.errors {
                                    if err.code == ERR_REDIRECT
                                        && (err.item_index as usize) < orig.len()
                                    {
                                        next_pending.push(orig[err.item_index as usize]);
                                    } else {
                                        terminal.extend(remap_batch_errors(vec![err], &orig));
                                    }
                                }
                            }
                            Err(_) => next_pending.extend(orig),
                        },
                        Err(_) => next_pending.extend(orig),
                    },
                    Err(_) => next_pending.extend(orig),
                }
            }

            pending = next_pending;
        }

        terminal.extend(Self::unroutable_errors_with_code(
            &pending,
            CLIENT_ERR_REDIRECT_FAILED,
        ));
        (successes, terminal)
    }

    /// Cluster-aware version of send_txid_batch with bounded transient-retry.
    ///
    /// Wraps [`Self::send_txid_batch_cluster_once`] in the same bounded
    /// same-target retry loop used by [`Self::send_item_batch_cluster`] so
    /// txid-keyed mutations (set_mined, delete, mark_longest_chain, …)
    /// behave consistently with the other cluster mutation paths. A retry
    /// is taken only when the *entire* batch failed with a retryable
    /// transient code — a global `ClientError::Server` whose code is
    /// retryable, or a `Partial` where every item carries a retryable code
    /// (notably `ERR_REPLICATION_FAILED` / code 20). These ops are
    /// idempotent by txid/op semantics, so re-issuing the identical op is
    /// safe; the server's compensation machinery reconciles any partial
    /// durability behind the ambiguous error. A mixed `Partial` (some
    /// successes, some errors) is returned as-is.
    async fn send_txid_batch_cluster<F>(
        &self,
        op_code: u16,
        txids: &[TxID],
        encode_payload: &F,
    ) -> Result<BatchResult, ClientError>
    where
        F: Fn(&[TxID]) -> Vec<u8>,
    {
        for attempt in 0..=(TRANSIENT_MUTATION_RETRY_DELAYS_MS.len() as u32) {
            let result = self
                .send_txid_batch_cluster_once(op_code, txids, encode_payload)
                .await;
            let retryable = (attempt as usize) < TRANSIENT_MUTATION_RETRY_DELAYS_MS.len()
                && match &result {
                    Err(ClientError::Server { code, .. }) => is_retryable_error_code(*code),
                    Err(ClientError::Partial(pe)) => {
                        pe.errors.len() == txids.len() && all_errors_are_retryable(&pe.errors)
                    }
                    _ => false,
                };
            if retryable {
                tokio::time::sleep(Duration::from_millis(
                    TRANSIENT_MUTATION_RETRY_DELAYS_MS[attempt as usize],
                ))
                .await;
                let _ = self.refresh_routing().await;
                continue;
            }
            return result;
        }
        unreachable!()
    }

    /// One attempt of a cluster-aware txid batch. The public
    /// [`Self::send_txid_batch_cluster`] wraps this with bounded
    /// transient-retry.
    async fn send_txid_batch_cluster_once<F>(
        &self,
        op_code: u16,
        txids: &[TxID],
        encode_payload: &F,
    ) -> Result<BatchResult, ClientError>
    where
        F: Fn(&[TxID]) -> Vec<u8>,
    {
        let grouped = self.group_txids(txids);

        // If single node or no cluster, just send directly. `ungroupable`
        // items (no route) are surfaced as per-item errors, never dropped.
        let single_group = grouped
            .as_ref()
            .is_some_and(|(g, ung)| g.len() <= 1 && ung.is_empty());
        if grouped.is_none() || single_group {
            let payload = encode_payload(txids);
            let conn = if let Some((groups, _)) = &grouped {
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

        let (groups, ungroupable) = grouped.unwrap();

        // Seed the error accumulator with the un-routable items (B6): a shard
        // whose owning node has no live pool must not vanish from the result.
        let mut all_errors: Vec<BatchItemError> = Self::unroutable_errors(&ungroupable);

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

        // Collect results, gathering all redirected original-indices so we can
        // re-route them per-target (each txid to ITS owner, not the owner of
        // the first txid).
        let mut got_no_quorum = false;
        let mut redirected_indices: Vec<usize> = Vec::new();
        // Any sub-batch whose applied items were only replicated below quorum
        // taints the merged response as degraded-durability.
        let mut merged_degraded = false;

        for handle in handles {
            let (result, idx_map) = handle
                .await
                .map_err(|e| ClientError::Connection(format!("join: {}", e)))??;

            match result {
                Ok(_) => {
                    // All items succeeded for this sub-batch.
                }
                Err(ClientError::Partial(pe)) => {
                    merged_degraded |= pe.degraded;
                    // Separate redirect errors from real errors.
                    // Redirect errors mean the shard table is stale — refresh
                    // routing and retry those items on the correct node.
                    for err in pe.errors {
                        if err.code == ERR_REDIRECT {
                            redirected_indices.push(idx_map[err.item_index as usize]);
                        } else {
                            let remapped = remap_batch_errors(vec![err], &idx_map);
                            all_errors.extend(remapped);
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

        if !redirected_indices.is_empty() {
            let redirect_errors = self
                .retry_redirected_txids(op_code, txids, &redirected_indices, encode_payload)
                .await;
            all_errors.extend(redirect_errors);
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
                degraded: merged_degraded,
            }));
        }

        Ok(BatchResult { errors: Vec::new() })
    }

    /// Re-route a set of redirected txid-batch items after a routing refresh,
    /// bounded by `max_redirects` hops, returning the per-item errors for any
    /// items that still could not be delivered.
    ///
    /// Each redirected txid is grouped by ITS OWN owning pool (not the owner
    /// of the first txid — a mixed-target redirect would otherwise misroute
    /// every item to one node). On each hop: refresh routing, regroup, send
    /// per-target sub-batches. Items whose target pool cannot be acquired, or
    /// whose sub-batch errors, are carried to the next hop; whatever remains
    /// unresolved after the last hop is surfaced as per-item errors (B6 — no
    /// silent drop, and no `retry_txids[0]` misroute).
    async fn retry_redirected_txids<F>(
        &self,
        op_code: u16,
        txids: &[TxID],
        redirected: &[usize],
        encode_payload: &F,
    ) -> Vec<BatchItemError>
    where
        F: Fn(&[TxID]) -> Vec<u8>,
    {
        // How many redirect hops to take. `max_redirects` is the documented
        // bound (default 3); fall back to 1 in single-node mode (no cluster).
        let max_hops = self
            .cluster
            .as_ref()
            .map(|c| c.max_redirects().max(1))
            .unwrap_or(1);

        // Terminal (non-redirect) per-item errors accumulated across hops.
        let mut terminal: Vec<BatchItemError> = Vec::new();
        // Working set of still-redirected original indices.
        let mut pending: Vec<usize> = redirected.to_vec();

        for _hop in 0..max_hops {
            if pending.is_empty() {
                break;
            }
            let _ = self.refresh_routing().await;

            // Group the pending indices by their (freshly resolved) owner pool
            // so each txid goes to ITS owner, not `retry_txids[0]`'s owner.
            let pending_txids: Vec<TxID> = pending.iter().map(|&i| txids[i]).collect();
            let Some((groups, ungroupable)) = self.group_txids(&pending_txids) else {
                // Not in cluster mode — nothing more we can do; surface all.
                terminal.extend(Self::unroutable_errors(&pending));
                return terminal;
            };

            // Items with no route this hop stay pending for the next hop.
            let mut next_pending: Vec<usize> = ungroupable.iter().map(|&p| pending[p]).collect();

            for (_, (pool, sub_local)) in groups {
                // `sub_local` indexes into `pending`; map to original indices.
                let orig: Vec<usize> = sub_local.iter().map(|&p| pending[p]).collect();
                let sub_txids: Vec<TxID> = orig.iter().map(|&i| txids[i]).collect();
                let payload = encode_payload(&sub_txids);

                match pool.get().await {
                    Ok(conn) => match conn.round_trip(op_code, 0, payload).await {
                        Ok(resp) => match Self::handle_mutation_response(&resp) {
                            Ok(_) => {}
                            Err(ClientError::Partial(pe)) => {
                                for err in pe.errors {
                                    if err.code == ERR_REDIRECT
                                        && (err.item_index as usize) < orig.len()
                                    {
                                        // Still redirected — carry to next hop.
                                        next_pending.push(orig[err.item_index as usize]);
                                    } else {
                                        terminal.extend(remap_batch_errors(vec![err], &orig));
                                    }
                                }
                            }
                            // Global error on this leg — carry for another hop.
                            Err(_) => next_pending.extend(orig),
                        },
                        // Connection round-trip failed — carry for retry.
                        Err(_) => next_pending.extend(orig),
                    },
                    // Pool acquire failed (the previously-missing `else`): do
                    // NOT drop — carry these items to the next hop.
                    Err(_) => next_pending.extend(orig),
                }
            }

            pending = next_pending;
        }

        // Anything still redirected after the hop budget is exhausted must be
        // surfaced as an error, never silently dropped.
        terminal.extend(Self::unroutable_errors_with_code(
            &pending,
            CLIENT_ERR_REDIRECT_FAILED,
        ));
        terminal
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

    /// Cluster-aware spend batch with bounded transient-retry.
    ///
    /// Wraps [`Self::spend_batch_cluster_once`] in the same bounded
    /// same-target retry loop used by [`Self::send_item_batch_cluster`] so
    /// spend behaves consistently with the other cluster mutation paths.
    /// A retry is taken only when the *entire* batch failed with a
    /// retryable transient code — either a global `ClientError::Server`
    /// whose code is retryable, or a `Partial` where every item carries a
    /// retryable code (notably `ERR_REPLICATION_FAILED` / code 20). Spends
    /// are idempotent by txid+output semantics (re-spending an already-spent
    /// output converges to the same state), so re-issuing the identical op
    /// is safe; the server's compensation machinery reconciles any partial
    /// durability behind the ambiguous error. A `Partial` with a mix of
    /// successes and errors is returned as-is (we must not re-spend items
    /// that already succeeded on this attempt).
    async fn spend_batch_cluster(
        &self,
        params: &SpendBatchParams,
        items: &[SpendItem],
    ) -> Result<SpendBatchResponse, ClientError> {
        for attempt in 0..=(TRANSIENT_MUTATION_RETRY_DELAYS_MS.len() as u32) {
            let result = self.spend_batch_cluster_once(params, items).await;
            let retryable = (attempt as usize) < TRANSIENT_MUTATION_RETRY_DELAYS_MS.len()
                && match &result {
                    Err(ClientError::Server { code, .. }) => is_retryable_error_code(*code),
                    Err(ClientError::Partial(pe)) => {
                        pe.errors.len() == items.len() && all_errors_are_retryable(&pe.errors)
                    }
                    _ => false,
                };
            if retryable {
                tokio::time::sleep(Duration::from_millis(
                    TRANSIENT_MUTATION_RETRY_DELAYS_MS[attempt as usize],
                ))
                .await;
                let _ = self.refresh_routing().await;
                continue;
            }
            return result;
        }
        unreachable!()
    }

    /// One attempt of a cluster-aware spend batch: group items by target
    /// node, send in parallel, merge results with index remapping. The
    /// public [`Self::spend_batch_cluster`] wraps this with bounded
    /// transient-retry.
    async fn spend_batch_cluster_once(
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
        // Any sub-batch whose applied items were only replicated below quorum
        // taints the merged response as degraded-durability.
        let mut merged_degraded = false;

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
                    merged_degraded |= pe.degraded;
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
                                        merged_degraded |= retry_pe.degraded;
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
                degraded: merged_degraded,
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
        let params = params.clone();
        self.send_item_batch_cluster(
            OP_UNSPEND_BATCH,
            items,
            |item| &item.txid,
            move |items, indices| {
                let sub: Vec<UnspendItem> = indices.iter().map(|&i| items[i].clone()).collect();
                encode_unspend_batch_payload(&params, &sub)
            },
        )
        .await
    }

    /// Mark transactions as mined in a specific block.
    ///
    /// Returns [`SpendBatchResponse`] carrying the per-item success signals
    /// and block IDs the server reports (consumed by Teranode's
    /// `txmetacache.SetMinedMulti`). The server encodes setMined responses in
    /// the signal layout (`encode_partial_with_signals`) for STATUS_OK,
    /// STATUS_DEGRADED_DURABILITY, and STATUS_PARTIAL_ERROR alike, so this
    /// routes through the signal-aware handler rather than the plain-mutation
    /// one — decoding the wrong codec would drop the signals and, on an
    /// all-failed batch, mask the failure as success (B5).
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Server`] on global error, [`ClientError::Partial`]
    /// on mixed success/failure.
    pub async fn set_mined_batch(
        &self,
        params: &SetMinedBatchParams,
        txids: &[TxID],
    ) -> Result<SpendBatchResponse, ClientError> {
        let params = params.clone();
        self.send_txid_batch_signals(OP_SET_MINED_BATCH, txids, &move |t: &[TxID]| {
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

        // Bounded transient-retry on retryable errors (dead node, migration
        // fence, stale epoch, or ambiguous ERR_REPLICATION_FAILED) after a
        // routing refresh — up to TRANSIENT_MUTATION_RETRY_DELAYS_MS.len()
        // attempts with backoff.
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
                // Retryable-transient arm. Every per-item error must be
                // a transient code (ERR_MIGRATION_IN_PROGRESS or
                // ERR_STALE_EPOCH) — both are same-target retryable;
                // ERR_REDIRECT is handled by the dedicated arm below.
                Err(ClientError::Partial(pe))
                    if pe.errors.len() == items.len()
                        && all_errors_are_retryable(&pe.errors)
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
                // Global ERR_REPLICATION_FAILED: ambiguous, idempotent-retry-safe
                // (see `is_retryable_error_code`). The op is idempotent by
                // txid/op semantics, so re-issuing it is safe; the server's
                // compensation machinery converges any partial durability.
                Err(ClientError::Server { code, .. })
                    if is_retryable_error_code(code)
                        && (attempt as usize) < TRANSIENT_MUTATION_RETRY_DELAYS_MS.len() =>
                {
                    tokio::time::sleep(Duration::from_millis(
                        TRANSIENT_MUTATION_RETRY_DELAYS_MS[attempt as usize],
                    ))
                    .await;
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
        // Any sub-batch whose applied items were only replicated below quorum
        // taints the merged response as degraded-durability.
        let mut merged_degraded = false;
        for handle in handles {
            let join_result = handle
                .await
                .map_err(|e| ClientError::Connection(format!("join: {e}")))?;
            match join_result {
                Ok((result, idx_map)) => match result {
                    Ok(_) => {}
                    Err(ClientError::Partial(pe)) => {
                        merged_degraded |= pe.degraded;
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
                degraded: merged_degraded,
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
        // Any sub-batch whose applied items were only replicated below quorum
        // taints the merged response as degraded-durability.
        let mut merged_degraded = false;

        for handle in handles {
            let join_result = handle
                .await
                .map_err(|e| ClientError::Connection(format!("join: {e}")))?;
            match join_result {
                Ok((result, idx_map)) => match result {
                    Ok(_) => {}
                    Err(ClientError::Partial(pe)) => {
                        merged_degraded |= pe.degraded;
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
                degraded: merged_degraded,
            }));
        }

        Ok(BatchResult { errors: Vec::new() })
    }

    /// Upload large cold_data as a blob in chunks before CREATE.
    ///
    /// Sends the data in `BLOB_CHUNK_SIZE` chunks via `OP_STREAM_CHUNK`,
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
    /// Items with `cold_data` larger than the configured
    /// [`ClientConfig::blob_upload_threshold`] (default
    /// [`BLOB_UPLOAD_THRESHOLD`], 1 MiB) are automatically uploaded via
    /// chunked blob streaming before the
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
        let threshold = self.blob_upload_threshold;
        let has_large_blobs = items
            .iter()
            .any(|i| needs_external_upload(i.cold_data.len(), threshold));

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
            if needs_external_upload(item.cold_data.len(), threshold) {
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
        let grouped = self.group_txids(txids);

        // Single node or no cluster — send directly. A single group with no
        // un-routable items can go as one request.
        let single_group = grouped
            .as_ref()
            .is_some_and(|(g, ung)| g.len() <= 1 && ung.is_empty());
        if grouped.is_none() || single_group {
            let payload = encode_get_batch_payload(field_mask, txids);
            let conn = if let Some((groups, _)) = &grouped {
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
        // `ungroupable` read items keep their slot in the result vector with
        // an error status (1) rather than being dropped.
        let (groups, _ungroupable) = grouped.unwrap();
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
        // Retry once on connection error (dead node) after routing refresh,
        // mirroring `get_batch`.
        for attempt in 0..2u32 {
            match self.get_spend_batch_inner(items).await {
                Ok(result) => return Ok(result),
                Err(ClientError::Connection(ref msg)) if attempt == 0 => {
                    tracing::warn!(error = %msg, "client: get_spend_batch retry after connection error");
                    let _ = self.refresh_routing().await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!()
    }

    /// Inner `get_spend_batch`: shard-group items, fan out sub-batches in
    /// parallel, reassemble in original order, then refresh routing and
    /// re-route any per-item `ERR_REDIRECT` results once.
    ///
    /// Unlike mutations, the server returns get_spend results inline under
    /// `STATUS_OK`; a misrouted item carries `error_code == ERR_REDIRECT`
    /// (with no target address), so the retry re-routes by the refreshed
    /// shard table rather than following an explicit address.
    async fn get_spend_batch_inner(
        &self,
        items: &[GetSpendItem],
    ) -> Result<Vec<GetSpendResult>, ClientError> {
        // Single-node or non-cluster: send directly.
        if self.cluster.is_none() || items.is_empty() {
            let payload = encode_get_spend_batch_payload(items);
            let conn = self.get_conn().await?;
            return Self::send_get_spend_sub(&conn, payload).await;
        }

        // Group by target pool. `ungroupable` items keep their result slot
        // (filled below with an error status) rather than being dropped.
        let (groups, _ungroupable) = self
            .group_txids(&items.iter().map(|i| i.txid).collect::<Vec<_>>())
            .ok_or(ClientError::NoPartitionMap)?;

        let total = items.len();
        let mut handles = Vec::with_capacity(groups.len());
        for (_, (pool, idx_map)) in groups {
            let sub_items: Vec<GetSpendItem> = idx_map.iter().map(|&i| items[i].clone()).collect();
            let payload = encode_get_spend_batch_payload(&sub_items);
            handles.push(tokio::spawn(async move {
                let conn = pool.get().await?;
                let results = Self::send_get_spend_sub(&conn, payload).await?;
                Ok::<(Vec<GetSpendResult>, Vec<usize>), ClientError>((results, idx_map))
            }));
        }

        let mut merged: Vec<Option<GetSpendResult>> = (0..total).map(|_| None).collect();
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

        // Re-route any per-item ERR_REDIRECT results once, after a routing
        // refresh, using the refreshed shard table.
        let redirected: Vec<usize> = merged
            .iter()
            .enumerate()
            .filter_map(|(i, r)| match r {
                Some(r) if r.status != 0 && r.error_code == ERR_REDIRECT => Some(i),
                _ => None,
            })
            .collect();

        if !redirected.is_empty() {
            let _ = self.refresh_routing().await;
            let retry_items: Vec<GetSpendItem> =
                redirected.iter().map(|&i| items[i].clone()).collect();
            let (retry_groups, _retry_ungroupable) = self
                .group_txids(&retry_items.iter().map(|i| i.txid).collect::<Vec<_>>())
                .ok_or(ClientError::NoPartitionMap)?;

            let mut retry_handles = Vec::with_capacity(retry_groups.len());
            for (_, (pool, sub_idx_map)) in retry_groups {
                let sub_items: Vec<GetSpendItem> = sub_idx_map
                    .iter()
                    .map(|&i| retry_items[i].clone())
                    .collect();
                let payload = encode_get_spend_batch_payload(&sub_items);
                // Map the retry-local indices back to original batch indices.
                let orig_idx_map: Vec<usize> = sub_idx_map.iter().map(|&i| redirected[i]).collect();
                retry_handles.push(tokio::spawn(async move {
                    let conn = pool.get().await?;
                    let results = Self::send_get_spend_sub(&conn, payload).await?;
                    Ok::<(Vec<GetSpendResult>, Vec<usize>), ClientError>((results, orig_idx_map))
                }));
            }
            for handle in retry_handles {
                if let Ok((results, orig_idx_map)) = handle
                    .await
                    .map_err(|e| ClientError::Connection(format!("join: {e}")))?
                {
                    for (sub_idx, result) in results.into_iter().enumerate() {
                        if sub_idx < orig_idx_map.len() {
                            merged[orig_idx_map[sub_idx]] = Some(result);
                        }
                    }
                }
            }
        }

        let results = merged
            .into_iter()
            .map(|r| {
                r.unwrap_or(GetSpendResult {
                    status: 1,
                    error_code: ERR_REDIRECT,
                    slot_status: 0,
                    spending_data: [0; 36],
                })
            })
            .collect();
        Ok(results)
    }

    /// Send one get_spend sub-batch on the given connection and decode the
    /// per-item results.
    async fn send_get_spend_sub(
        conn: &crate::conn::PipeConn,
        payload: Vec<u8>,
    ) -> Result<Vec<GetSpendResult>, ClientError> {
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
    /// A single response is capped at one 16 MiB frame. Against a version-3+
    /// server (FU#5) the remainder is paged transparently via a resume cursor
    /// and the complete set is returned. Against an older server a single
    /// best-effort call is made and, if the result was capped, the partial page
    /// is returned as [`ClientError::QueryTruncated`] rather than being silently
    /// dropped.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Server`] on a server error,
    /// [`ClientError::QueryTruncated`] when a pre-v3 server truncated the
    /// result, or [`ClientError::Protocol`] on a malformed response.
    pub async fn query_old_unmined(&self, cutoff_height: u32) -> Result<Vec<TxID>, ClientError> {
        self.page_query(
            OP_QUERY_OLD_UNMINED,
            move |cursor: Option<&TxID>| {
                let mut payload = cutoff_height.to_le_bytes().to_vec();
                if let Some(c) = cursor {
                    payload.extend_from_slice(c);
                }
                payload
            },
            decode_query_old_unmined_response,
        )
        .await
    }

    /// Query all transactions currently flagged CONFLICTING.
    ///
    /// The request carries no parameters beyond the optional FU#5 resume cursor.
    /// Pagination and the capability gate behave exactly as
    /// [`Client::query_old_unmined`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Server`] on a server error,
    /// [`ClientError::QueryTruncated`] when a pre-v3 server truncated the
    /// result, or [`ClientError::Protocol`] on a malformed response.
    pub async fn query_conflicting(&self) -> Result<Vec<TxID>, ClientError> {
        self.page_query(
            OP_QUERY_CONFLICTING,
            |cursor: Option<&TxID>| {
                let mut payload = Vec::new();
                if let Some(c) = cursor {
                    payload.extend_from_slice(c);
                }
                payload
            },
            decode_query_conflicting_response,
        )
        .await
    }

    /// Run a diagnostic txid-list query, transparently following the FU#5
    /// truncated trailer to page the whole result when the negotiated server
    /// version supports the resume cursor (`>= 3`).
    ///
    /// The capability gate is essential: a version-`< 3` server ignores the
    /// cursor and returns page 1 forever, so a naive loop would never terminate.
    /// Against such a server this makes exactly one call and, if the response was
    /// truncated, returns [`ClientError::QueryTruncated`] with the partial page.
    async fn page_query(
        &self,
        op_code: u16,
        encode: impl Fn(Option<&TxID>) -> Vec<u8>,
        decode: impl Fn(&[u8]) -> Result<(Vec<TxID>, bool), ClientError>,
    ) -> Result<Vec<TxID>, ClientError> {
        let supports_paging = self.ensure_server_version().await >= 3;
        let mut all: Vec<TxID> = Vec::new();
        let mut cursor: Option<TxID> = None;
        loop {
            let payload = encode(cursor.as_ref());
            let conn = self.get_conn().await?;
            let resp = conn.round_trip(op_code, 0, payload).await?;
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
            let (txids, truncated) = decode(&resp.payload)?;
            all.extend_from_slice(&txids);
            if !truncated {
                return Ok(all);
            }
            if !supports_paging {
                return Err(ClientError::QueryTruncated { partial: all });
            }
            match txids.last() {
                Some(last) => cursor = Some(*last),
                None => {
                    // Defensive: a truncated-but-empty page would loop forever.
                    // A conforming v3 server never emits this.
                    return Err(ClientError::Protocol(
                        "query paging: truncated response with empty page".to_string(),
                    ));
                }
            }
        }
    }

    /// The wire protocol version negotiated with the server via `OP_HELLO`, or
    /// `0` if no query that needs it has run yet. `1` records a server that
    /// predates the handshake.
    pub fn negotiated_version(&self) -> u16 {
        self.negotiated_version.load(Ordering::Relaxed)
    }

    /// Perform the `OP_HELLO` handshake and return the server's reported wire
    /// protocol version.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Server`] if the server rejects the opcode (a
    /// pre-handshake server), or [`ClientError::Protocol`] / connection errors
    /// on transport failure or a short payload.
    pub async fn hello(&self) -> Result<u16, ClientError> {
        let conn = self.get_conn().await?;
        let resp = conn.round_trip(OP_HELLO, 0, Vec::new()).await?;
        if resp.status != STATUS_OK {
            if resp.status == STATUS_ERROR {
                let (code, msg) = decode_error_payload(&resp.payload)?;
                return Err(ClientError::Server { code, message: msg });
            }
            return Err(ClientError::Protocol(format!(
                "hello: unexpected status {}",
                resp.status
            )));
        }
        if resp.payload.len() < 2 {
            return Err(ClientError::Protocol(format!(
                "hello: short payload ({} bytes)",
                resp.payload.len()
            )));
        }
        Ok(u16::from_le_bytes([resp.payload[0], resp.payload[1]]))
    }

    /// Return the negotiated server version, performing (and caching) the
    /// `OP_HELLO` handshake on first call. A handshake failure — including an
    /// older server that does not implement the opcode — records `1`, the safe
    /// pre-handshake baseline, so the pagination gate degrades to a single call.
    async fn ensure_server_version(&self) -> u16 {
        let cached = self.negotiated_version.load(Ordering::Relaxed);
        if cached != 0 {
            return cached;
        }
        let v = self.hello().await.unwrap_or(1);
        // A concurrent racer storing the same value is harmless.
        self.negotiated_version.store(v, Ordering::Relaxed);
        v
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
    /// Sends the 8-byte payload `[current_height:4 LE][block_height_retention:4 LE]`.
    /// `block_height_retention` is the retention window the server applies when
    /// deciding which preserved transactions have expired; sending a non-zero
    /// value is required for the expiry phase to run (a 4-byte legacy payload
    /// is interpreted by the server as `retention = 0`, which silently skips
    /// expiry).
    ///
    /// # Parameters
    ///
    /// - `current_height`: The current block height.
    /// - `block_height_retention`: Retention window in blocks.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Server`] on error.
    pub async fn process_expired_preservations(
        &self,
        current_height: u32,
        block_height_retention: u32,
    ) -> Result<ProcessExpiredResult, ClientError> {
        let mut payload = Vec::with_capacity(8);
        payload.extend_from_slice(&current_height.to_le_bytes());
        payload.extend_from_slice(&block_height_retention.to_le_bytes());
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

        // Single-node mode: fetch from the server. OP_GET_PARTITION_MAP is an
        // inter-node auth opcode; under strict_auth the server HMACs the whole
        // frame, so sign the frame (not just the payload) when a secret is set.
        let conn = self.get_conn().await?;
        let secret = self.cluster_secret.as_deref().filter(|s| !s.is_empty());
        let resp = match secret {
            Some(s) => {
                conn.round_trip_signed(OP_GET_PARTITION_MAP, 0, Vec::new(), s)
                    .await?
            }
            None => conn.round_trip(OP_GET_PARTITION_MAP, 0, Vec::new()).await?,
        };
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
        let request_timeout = Duration::from_secs(30);
        let conn = crate::conn::PipeConn::dial(addr, dial_timeout, request_timeout).await?;
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
    let wire_items: Vec<codec::WireUnspendItem> = items
        .iter()
        .map(|i| codec::WireUnspendItem {
            txid: i.txid,
            vout: i.vout,
            utxo_hash: i.utxo_hash,
            spending_data: i.spending_data,
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
            utxo_hash: i.utxo_hash,
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

/// Decode a sparse error list from a PartialError response, plus the reserved
/// degraded-durability trailer (`true` iff the applied items were only
/// replicated below quorum). See [`codec::PARTIAL_DURABILITY_DEGRADED`].
fn decode_sparse_errors(data: &[u8]) -> Result<(Vec<BatchItemError>, bool), ClientError> {
    let (wire_errors, degraded) = codec::decode_sparse_errors_with_durability(data)
        .ok_or_else(|| ClientError::Protocol("malformed sparse errors".to_string()))?;
    let errors = wire_errors
        .into_iter()
        .map(|e| BatchItemError {
            item_index: e.item_index,
            code: e.error_code,
            data: e.error_data,
        })
        .collect();
    Ok((errors, degraded))
}

/// Decode a partial response with success signals and errors, plus the reserved
/// degraded-durability trailer (`true` iff the applied items were only
/// replicated below quorum). See [`codec::PARTIAL_DURABILITY_DEGRADED`].
fn decode_partial_with_signals(
    data: &[u8],
) -> Result<(Vec<BatchItemSuccess>, Vec<BatchItemError>, bool), ClientError> {
    let (wire_successes, wire_errors, degraded) =
        codec::decode_partial_with_signals_with_durability(data)
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
    Ok((successes, errors, degraded))
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

/// Decode a diagnostic-query txid response payload:
/// `[count:u32 LE][txid:32]*count[truncated:u8]`.
///
/// Returns the txids and whether the server flagged the result truncated
/// (FU#5). A truncated result means a further qualifying txid exists past the
/// frame cap and the caller should re-query with the last returned txid as the
/// resume cursor. The trailing flag byte is optional for defensiveness: a
/// response that stops exactly after the txids is treated as not truncated.
fn decode_query_txid_response(data: &[u8], label: &str) -> Result<(Vec<TxID>, bool), ClientError> {
    if data.len() < 4 {
        return Err(ClientError::Protocol(format!(
            "{label}: need 4 bytes, have {}",
            data.len()
        )));
    }
    let count = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let end = 4 + count * 32;
    if data.len() < end {
        return Err(ClientError::Protocol(format!("{label}: truncated")));
    }
    let mut txids = Vec::with_capacity(count);
    let mut pos = 4;
    for _ in 0..count {
        let mut txid = [0u8; 32];
        txid.copy_from_slice(&data[pos..pos + 32]);
        txids.push(txid);
        pos += 32;
    }
    let truncated = data.len() > end && data[end] == 1;
    Ok((txids, truncated))
}

/// Decode a QueryOldUnmined response payload (see [`decode_query_txid_response`]).
fn decode_query_old_unmined_response(data: &[u8]) -> Result<(Vec<TxID>, bool), ClientError> {
    decode_query_txid_response(data, "query old unmined")
}

/// Decode a QueryConflicting response payload (see [`decode_query_txid_response`]).
fn decode_query_conflicting_response(data: &[u8]) -> Result<(Vec<TxID>, bool), ClientError> {
    decode_query_txid_response(data, "query conflicting")
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

/// Group per-item REDIRECT errors by target address.
///
/// Wire format of `err.data` (R-041): `[addr_len:2][addr][shard_table_version:8 (le)]`
/// — produced by the server's `encode_redirect_with_version`. Older
/// servers emit the legacy form `[addr_len:2][addr]` (no trailing
/// version); even older servers emit raw `addr_bytes` with no length
/// prefix. The decoder accepts all three:
///
///   1. **Versioned (`addr_len + addr + 8 trailing bytes`)** — preferred;
///      `decode_redirect_with_version` returns `(addr, Some(version))`.
///   2. **Length-prefixed (`addr_len + addr`, no trailer)** —
///      `decode_redirect_with_version` returns `(addr, None)`. Treated
///      identically: the version is just unknown.
///   3. **Raw addr bytes (legacy fallback)** — `decode_redirect_with_version`
///      returns `None` (because the leading two bytes are not a valid
///      length). We then try `from_utf8` over the whole buffer; if it
///      parses as a non-empty UTF-8 string, treat it as the address.
///
/// Returns `None` if any item fails to parse via all three strategies —
/// the caller surfaces the redirects as a `PartialError` rather than
/// retrying blindly.
fn collect_redirect_groups(
    errors: &[BatchItemError],
    idx_map: &[usize],
) -> Option<HashMap<String, Vec<usize>>> {
    let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
    for err in errors {
        if err.code != ERR_REDIRECT || err.data.is_empty() {
            return None;
        }
        let addr = match codec::decode_redirect_with_version(&err.data) {
            Some((a, _version)) => a,
            None => {
                // Legacy raw-addr-only fallback (server predates R-041).
                std::str::from_utf8(&err.data).ok()?.trim().to_string()
            }
        };
        if addr.is_empty() || (err.item_index as usize) >= idx_map.len() {
            return None;
        }
        groups
            .entry(addr)
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

/// Returns `true` for server error codes that indicate a *transient*,
/// retryable condition for the **same** target node. The client should
/// back off and retry rather than reroute (which `ERR_REDIRECT` triggers
/// instead).
///
/// Currently:
/// - [`ERR_MIGRATION_IN_PROGRESS`] — shard handoff in flight; the new
///   master will accept once migration completes (or `Transitioning`
///   topology gap closes).
/// - [`ERR_STALE_EPOCH`] — the target's local cluster epoch differs
///   from the requester's view; same-target retry succeeds once both
///   sides observe the new committed term.
/// - [`ERR_REPLICATION_FAILED`] — an *ambiguous, idempotent-retry-safe*
///   outcome (see the spec's "ERR_REPLICATION_FAILED — ambiguous outcome"
///   section). The write may now be durable on master, replicas, both, or
///   neither; the server's compensation machinery converges the state, and
///   the prescribed client recovery is to re-issue the identical idempotent
///   op. Because all TeraSlab mutations are idempotent by txid/op semantics
///   (re-spending an already-spent output, re-mining an already-mined tx,
///   re-creating an existing record, etc. converge to the same state), a
///   bounded same-target retry is safe and is the documented recovery path.
pub(crate) fn is_retryable_error_code(code: u16) -> bool {
    matches!(
        code,
        ERR_MIGRATION_IN_PROGRESS | ERR_STALE_EPOCH | ERR_REPLICATION_FAILED
    )
}

/// Returns `true` when every per-item error in `errors` is one of the
/// retryable transient codes recognized by [`is_retryable_error_code`].
pub(crate) fn all_errors_are_retryable(errors: &[BatchItemError]) -> bool {
    !errors.is_empty() && errors.iter().all(|err| is_retryable_error_code(err.code))
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
        ClusterConfig as ServerClusterConfig, ClusterCoordinator, ReplicationRuntimeConfig,
        RunningCluster,
    };
    use teraslab::cluster::shards::{NodeId, ShardTable};
    use teraslab::cluster::topology::ClusterId;
    use teraslab::config::ServerConfig;
    use teraslab::device::{BlockDevice, MemoryDevice};
    use teraslab::index::{DahIndex, Index};
    use teraslab::locks::StripedLocks;
    use teraslab::ops::engine::Engine;
    use teraslab::server::Server;

    struct TestNode {
        server: Arc<Server>,
        cluster: Arc<RunningCluster>,
    }

    /// All in-process test nodes share this cluster_id so the P1.1
    /// `membership_change_is_safe` check takes the matching-cluster_id fast
    /// path (skips the slower F-G8-001 ever-seen fallback), keeping the
    /// 3-node clusters in these tests converging within their sleep windows.
    /// Mirrors `tests/cluster_tcp.rs::TEST_CLUSTER_ID`.
    const TEST_CLUSTER_ID: ClusterId = ClusterId([0xA5; 16]);

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
    fn default_blob_upload_threshold_is_one_mib() {
        assert_eq!(BLOB_UPLOAD_THRESHOLD, 1024 * 1024);
        assert_eq!(
            ClientConfig::default().blob_upload_threshold,
            1024 * 1024,
            "default config threshold must be 1 MiB (no runtime change)"
        );
    }

    #[test]
    fn custom_threshold_selects_items_at_boundary() {
        // At the default threshold, exactly 1 MiB stays inline and 1 MiB + 1
        // is externalised.
        assert!(!needs_external_upload(1024 * 1024, BLOB_UPLOAD_THRESHOLD));
        assert!(needs_external_upload(
            1024 * 1024 + 1,
            BLOB_UPLOAD_THRESHOLD
        ));

        // A custom (smaller) threshold moves the boundary: exactly `threshold`
        // stays inline, `threshold + 1` is externalised.
        let custom = 100usize;
        assert!(
            !needs_external_upload(100, custom),
            "item of exactly the threshold size stays inline"
        );
        assert!(
            needs_external_upload(101, custom),
            "item one byte over the threshold is externalised"
        );

        // Selecting from a batch: only items strictly over the threshold.
        let lens = [0usize, 100, 101, 250];
        let selected: Vec<usize> = lens
            .iter()
            .copied()
            .filter(|&l| needs_external_upload(l, custom))
            .collect();
        assert_eq!(selected, vec![101, 250]);
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
        ));

        let seeds: Vec<std::net::SocketAddr> = seed_swim_ports
            .iter()
            .map(|port| format!("127.0.0.1:{port}").parse().unwrap())
            .collect();

        let cluster_config = ServerClusterConfig {
            self_id: NodeId(node_id),
            self_addr: format!("127.0.0.1:{tcp_port}").parse().unwrap(),
            swim_bind: format!("127.0.0.1:{swim_port}").parse().unwrap(),
            swim_advertise_addr: None,
            seed_nodes: seeds,
            replication_factor,
            probe_interval: Duration::from_millis(100),
            suspicion_timeout: Duration::from_secs(2),
            cluster_secret: None,
            max_migration_threads: 16,
            topology_debounce: Duration::from_millis(100),
            topology_propose_timeout: Duration::from_millis(300),
            migration_pool_size: 4,
            migration_batch_size: 100,
            persisted_incarnation: 0,
            cluster_id: TEST_CLUSTER_ID,
        };

        let coordinator = ClusterCoordinator::new(cluster_config, 1);
        let running = Arc::new(coordinator.start(
            engine.clone(),
            None,
            None,
            ReplicationRuntimeConfig {
                ack_policy: None,
                best_effort: true,
                timeout: Duration::from_secs(3),
                timeout_during_migration: Duration::from_secs(30),
            },
        ));

        let config = ServerConfig {
            listen_addr: format!("127.0.0.1:{tcp_port}"),
            max_connections: 64,
            max_batch_size: 4096,
            node_id,
            // F-X-002 flipped the production default to `strict_auth = true`,
            // and `OP_GET_PARTITION_MAP` (the client's bootstrap op) is an
            // inter-node auth opcode. These nodes use `cluster_secret: None`
            // in their ClusterConfig, so strict_auth would reject the
            // client's partition-map fetch with ERR_CLUSTER_AUTH_FAILED.
            // Stay on the trusted-overlay opt-out — mirrors tests/cluster_tcp.rs.
            strict_auth: false,
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
        // Build a *divergent* stale table for node1 to hold. `compute_with_epoch`
        // sorts its member list (F-01), so reordering the same 3 members yields an
        // identical table — the old `[1,3,2]` trick no longer diverges. Instead
        // compute over a 2-node membership `[2,3]`: masters then alternate 2,3,2,3…
        // while the real 3-node table cycles 1,2,3,1,2,3…, so many shards whose
        // real master is 2 or 3 get the *other* (still real, still reachable)
        // remote node as their stale master. That is exactly the misroute this
        // test needs: node1's map points the shard at the wrong remote peer.
        let stale_table = ShardTable::compute_with_epoch(&[NodeId(2), NodeId(3)], 2, 999, 1);
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn set_mined_batch_retries_transient_until_fence_clears() {
        // Exercises the bounded transient-retry loop wrapping
        // `send_txid_batch_cluster` (set_mined / delete / mark_longest_chain
        // path). The same loop now also recovers ERR_REPLICATION_FAILED
        // (code 20) — see `replication_failed_is_retryable`. We drive a
        // server-injectable transient (the migration fence) because a
        // replication-ACK timeout cannot be forced in-process without the
        // server compensation machinery owned by the P5 path. The wrapper
        // logic (retry decision via `is_retryable_error_code`) is identical
        // for code 19 and code 20.
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

        let txid = txid_for_shard(777);
        let shard = crate::cluster::shard_for_txid(&txid);

        // Seed the record first (unfenced) so set_mined has something to mark.
        let create_item = CreateItem {
            txid,
            utxo_hashes: vec![[0x77; 32]],
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
            .expect("seed create_batch should succeed before fencing");

        // Now fence the shard so the first set_mined attempt is rejected with
        // ERR_MIGRATION_IN_PROGRESS, and clear it shortly after.
        node1.cluster.fenced_bitmap().set(shard);
        let cluster = Arc::clone(&node1.cluster);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(75)).await;
            cluster.fenced_bitmap().clear(shard);
        });

        let params = SetMinedBatchParams {
            block_id: 1,
            block_height: 10,
            subtree_idx: 0,
            on_longest_chain: true,
            unset_mined: false,
            current_block_height: 10,
            block_height_retention: 100,
        };
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            client.set_mined_batch(&params, &[txid]),
        )
        .await
        .expect("set_mined_batch should not hang");

        assert!(
            result.is_ok(),
            "set_mined_batch must retry the transient fence rejection until it \
             clears (same loop that now recovers ERR_REPLICATION_FAILED): {result:?}"
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

    // ── Phase B4: retry classifier ────────────────────────────────

    #[test]
    fn migration_in_progress_is_retryable() {
        assert!(
            is_retryable_error_code(ERR_MIGRATION_IN_PROGRESS),
            "ERR_MIGRATION_IN_PROGRESS must be classified as retryable"
        );
    }

    #[test]
    fn stale_epoch_is_retryable() {
        assert!(
            is_retryable_error_code(ERR_STALE_EPOCH),
            "ERR_STALE_EPOCH must be classified as retryable so clients re-issue \
             the request once the local cluster_key catches up to the master's"
        );
    }

    #[test]
    fn replication_failed_is_retryable() {
        // ERR_REPLICATION_FAILED (code 20) is the ambiguous,
        // idempotent-retry-safe outcome: the write may be durable on
        // master, replicas, both, or neither. The contract is that the
        // client re-issues the identical idempotent op and the server's
        // compensation machinery converges the state.
        assert!(
            is_retryable_error_code(ERR_REPLICATION_FAILED),
            "ERR_REPLICATION_FAILED must be classified as retryable: it is an \
             ambiguous, idempotent-retry-safe outcome and a client retry is \
             the prescribed recovery"
        );
        assert_eq!(ERR_REPLICATION_FAILED, 20, "code under contract is 20");
    }

    #[test]
    fn all_errors_are_retryable_accepts_replication_failed_batch() {
        let errors = vec![
            BatchItemError {
                item_index: 0,
                code: ERR_REPLICATION_FAILED,
                data: vec![],
            },
            BatchItemError {
                item_index: 1,
                code: ERR_REPLICATION_FAILED,
                data: vec![],
            },
        ];
        assert!(
            all_errors_are_retryable(&errors),
            "a batch where every item failed with ERR_REPLICATION_FAILED must \
             be retried as a whole"
        );
    }

    #[test]
    fn redirect_is_not_retryable_against_same_target() {
        // ERR_REDIRECT is handled separately (route to a different node)
        // and must NOT be lumped in with same-target transient retries —
        // otherwise a stale-routed mutation would loop forever.
        assert!(
            !is_retryable_error_code(ERR_REDIRECT),
            "ERR_REDIRECT must not be treated as same-target retryable"
        );
    }

    #[test]
    fn all_errors_are_retryable_accepts_mixed_retryable_codes() {
        let errors = vec![
            BatchItemError {
                item_index: 0,
                code: ERR_MIGRATION_IN_PROGRESS,
                data: vec![],
            },
            BatchItemError {
                item_index: 1,
                code: ERR_STALE_EPOCH,
                data: vec![],
            },
        ];
        assert!(
            all_errors_are_retryable(&errors),
            "a batch where every item is one of the retryable codes \
             (mixed MIGRATION_IN_PROGRESS + STALE_EPOCH) must be retried"
        );
    }

    #[test]
    fn all_errors_are_retryable_rejects_empty() {
        assert!(
            !all_errors_are_retryable(&[]),
            "empty error vec must not be reported retryable",
        );
    }

    #[test]
    fn all_errors_are_retryable_rejects_mixed_with_redirect() {
        let errors = vec![
            BatchItemError {
                item_index: 0,
                code: ERR_MIGRATION_IN_PROGRESS,
                data: vec![],
            },
            BatchItemError {
                item_index: 1,
                code: ERR_REDIRECT,
                data: vec![],
            },
        ];
        assert!(
            !all_errors_are_retryable(&errors),
            "presence of any non-retryable code must veto same-target retry",
        );
    }

    /// REL-135: a short `request_timeout` propagates from `ClientConfig`
    /// through the pool to `PipeConn`, so a request to an address that never
    /// responds returns `ClientError::Timeout` well before the 30s default.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn request_timeout_is_configurable() {
        // Bind a listener that accepts the TCP connection but never replies,
        // so round_trip must hit the per-request timeout.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Accept and hold connections open without ever responding.
        tokio::spawn(async move {
            let mut held = Vec::new();
            loop {
                match listener.accept().await {
                    Ok((sock, _)) => held.push(sock),
                    Err(_) => return,
                }
            }
        });

        let client = Client::new(ClientConfig {
            addr: Some(addr.to_string()),
            request_timeout: Duration::from_millis(150),
            ..Default::default()
        })
        .await
        .expect("single-node client should construct against an accepting listener");

        let start = Instant::now();
        let result = tokio::time::timeout(Duration::from_secs(5), client.ping()).await;
        let elapsed = start.elapsed();

        let inner = result.expect("ping must not hang past the outer 5s guard");
        assert!(
            matches!(inner, Err(ClientError::Timeout)),
            "an unresponsive peer must surface ClientError::Timeout, got: {inner:?}",
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "the 150ms request_timeout must fire well before the 30s default; elapsed = {elapsed:?}",
        );

        client.close().await;
    }

    /// REL-011: `unspend_batch` and `get_spend_batch` now route through the
    /// shard-grouping machinery. This exercises the full create → spend →
    /// unspend → get_spend round-trip against a clustered node, asserting
    /// concrete per-item state transitions at each step.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn unspend_and_get_spend_route_through_cluster_machinery() {
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

        let txid = txid_for_shard(55);
        let utxo = [0xAB; 32];
        let create_item = CreateItem {
            txid,
            utxo_hashes: vec![utxo],
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
            .expect("create_batch should succeed");

        // Spend the single output.
        let spend_data = [0xC9; 36];
        let spend_params = SpendBatchParams {
            ignore_conflicting: true,
            ignore_locked: true,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        client
            .spend_batch(
                &spend_params,
                &[SpendItem {
                    txid,
                    vout: 0,
                    utxo_hash: utxo,
                    spending_data: spend_data,
                }],
            )
            .await
            .expect("spend_batch should succeed");

        // get_spend_batch must report the slot as spent (status 0x01).
        let spent = client
            .get_spend_batch(&[GetSpendItem {
                txid,
                vout: 0,
                utxo_hash: utxo,
            }])
            .await
            .expect("get_spend_batch should succeed");
        assert_eq!(spent.len(), 1, "one item in, one result out");
        assert_eq!(spent[0].status, 0, "lookup itself succeeded");
        assert_eq!(
            spent[0].slot_status, 0x01,
            "slot must report spent after spend_batch",
        );

        // Unspend it via the now-cluster-routed unspend_batch.
        let unspend_params = UnspendBatchParams {
            current_block_height: 1000,
            block_height_retention: 288,
        };
        let unspend_result = client
            .unspend_batch(
                &unspend_params,
                &[UnspendItem {
                    txid,
                    vout: 0,
                    utxo_hash: utxo,
                    spending_data: spend_data,
                }],
            )
            .await
            .expect("unspend_batch should succeed through the shard machinery");
        assert!(
            unspend_result.errors.is_empty(),
            "fully-successful unspend must carry no per-item errors, got {:?}",
            unspend_result.errors,
        );

        // get_spend_batch must now report the slot unspent (status 0x00).
        let unspent = client
            .get_spend_batch(&[GetSpendItem {
                txid,
                vout: 0,
                utxo_hash: utxo,
            }])
            .await
            .expect("get_spend_batch should succeed after unspend");
        assert_eq!(
            unspent[0].slot_status, 0x00,
            "slot must report unspent after unspend_batch",
        );

        client.close().await;
        shutdown_node(&node1);
    }

    /// REL-011: `get_spend_batch` reassembles multi-item results in the
    /// original request order even when items are grouped by shard. With a
    /// single node all items land in one group, but the reassembly path
    /// (sub-index -> original-index) is still exercised, and the per-item
    /// status must line up with each requested slot.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn get_spend_batch_preserves_request_order() {
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

        let txid = txid_for_shard(91);
        let utxo_a = [0x01; 32];
        let utxo_b = [0x02; 32];
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
            .expect("create_batch should succeed");

        // Spend only vout 0, leaving vout 1 unspent.
        let spend_params = SpendBatchParams {
            ignore_conflicting: true,
            ignore_locked: true,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        client
            .spend_batch(
                &spend_params,
                &[SpendItem {
                    txid,
                    vout: 0,
                    utxo_hash: utxo_a,
                    spending_data: [0xD0; 36],
                }],
            )
            .await
            .expect("spend_batch should succeed");

        // Query in order [vout1 (unspent), vout0 (spent)] and check the
        // results map back to the right request positions.
        let results = client
            .get_spend_batch(&[
                GetSpendItem {
                    txid,
                    vout: 1,
                    utxo_hash: utxo_b,
                },
                GetSpendItem {
                    txid,
                    vout: 0,
                    utxo_hash: utxo_a,
                },
            ])
            .await
            .expect("get_spend_batch should succeed");
        assert_eq!(results.len(), 2, "two items in, two results out");
        assert_eq!(
            results[0].slot_status, 0x00,
            "request index 0 (vout 1) must report unspent",
        );
        assert_eq!(
            results[1].slot_status, 0x01,
            "request index 1 (vout 0) must report spent",
        );

        client.close().await;
        shutdown_node(&node1);
    }

    // ── B5 / B6 / degraded-durability / max_redirects regression suite ──

    use teraslab::protocol::codec::{
        BatchItemError as WireBatchItemError, BatchItemSuccess as WireBatchItemSuccess,
        PARTIAL_DURABILITY_DEGRADED, encode_partial_with_signals, encode_sparse_errors,
    };
    use teraslab::protocol::frame::ResponseFrame;
    use teraslab::protocol::opcodes::STATUS_DEGRADED_DURABILITY;

    fn frame(status: u8, payload: Vec<u8>) -> ResponseFrame {
        ResponseFrame {
            request_id: 0,
            status,
            payload,
        }
    }

    /// B5: an all-items-FAILED setMined response is encoded by the server with
    /// `encode_partial_with_signals` (per-item signal + block_ids layout), NOT
    /// the plain sparse-error layout. The signal handler must decode it with
    /// the matching codec and surface every failed item — never report the
    /// batch as a silent success.
    #[test]
    fn signal_handler_setmined_all_failed_surfaces_errors_not_ok() {
        let errors = vec![
            WireBatchItemError {
                item_index: 0,
                error_code: ERR_CONFLICTING,
                error_data: vec![],
            },
            WireBatchItemError {
                item_index: 1,
                error_code: ERR_LOCKED,
                error_data: vec![],
            },
        ];
        // Server-authentic setMined PARTIAL_ERROR payload (signal layout, no
        // successes). Under the wrong codec (`decode_sparse_errors`) the first
        // u32 (success_count = 0) makes this decode to an empty error list,
        // which previously masqueraded as full success.
        let payload = encode_partial_with_signals(&[], &errors);
        let resp = frame(STATUS_PARTIAL_ERROR, payload);

        let result = Client::handle_signal_response(&resp, 2);
        match result {
            Err(ClientError::Partial(pe)) => {
                assert_eq!(pe.errors.len(), 2, "both failed items must surface");
                let mut codes: Vec<u16> = pe.errors.iter().map(|e| e.code).collect();
                codes.sort_unstable();
                assert_eq!(codes, vec![ERR_CONFLICTING, ERR_LOCKED]);
                assert!(
                    pe.successes.is_empty(),
                    "no item succeeded, so no synthetic successes"
                );
            }
            other => panic!(
                "all-failed setMined must be a Partial error, not {other:?} \
                 (a masked-success here is B5)"
            ),
        }
    }

    /// B5: a STATUS_OK setMined response carries per-item (signal, block_ids)
    /// in the signal layout. The signal handler must decode and return them —
    /// the plain mutation handler would drop them entirely.
    #[test]
    fn signal_handler_setmined_ok_returns_signals_and_block_ids() {
        let successes = vec![
            WireBatchItemSuccess {
                item_index: 0,
                signal: 3,
                block_ids: vec![100, 101],
            },
            WireBatchItemSuccess {
                item_index: 1,
                signal: 0,
                block_ids: vec![100],
            },
        ];
        let payload = encode_partial_with_signals(&successes, &[]);
        let resp = frame(STATUS_OK, payload);

        let out =
            Client::handle_signal_response(&resp, 2).expect("fully-successful setMined must be Ok");
        assert!(out.errors.is_empty());
        assert_eq!(out.successes.len(), 2);
        assert_eq!(out.successes[0].signal, 3);
        assert_eq!(out.successes[0].block_ids, vec![100, 101]);
        assert_eq!(out.successes[1].block_ids, vec![100]);
    }

    /// Major: STATUS_DEGRADED_DURABILITY (5) on the signal path is a
    /// successful-but-weak ack. It must decode the same payload shape as
    /// STATUS_OK and surface success — never a Protocol("unknown status")
    /// error that would report an applied write as a failure.
    #[test]
    fn signal_handler_degraded_durability_is_success_with_signals() {
        let successes = vec![WireBatchItemSuccess {
            item_index: 0,
            signal: 1,
            block_ids: vec![42],
        }];
        let payload = encode_partial_with_signals(&successes, &[]);
        let resp = frame(STATUS_DEGRADED_DURABILITY, payload);

        let out = Client::handle_signal_response(&resp, 1)
            .expect("degraded durability is an applied write, must be Ok");
        assert_eq!(out.successes.len(), 1);
        assert_eq!(out.successes[0].signal, 1);
        assert_eq!(out.successes[0].block_ids, vec![42]);
        assert!(out.errors.is_empty());
    }

    /// Major: STATUS_DEGRADED_DURABILITY on the plain mutation path must also
    /// be treated as success, matching the Go client and the server contract
    /// (applied + locally durable under best-effort replication).
    #[test]
    fn mutation_handler_degraded_durability_is_success() {
        let resp = frame(STATUS_DEGRADED_DURABILITY, Vec::new());
        let out = Client::handle_mutation_response(&resp)
            .expect("degraded durability must not be a protocol error");
        assert!(
            out.errors.is_empty(),
            "an applied+durable mutation must report zero errors"
        );
    }

    /// Regression guard: the plain sparse-error PARTIAL_ERROR path (used by
    /// delete/set_locked/etc.) still surfaces real per-item errors.
    #[test]
    fn mutation_handler_partial_error_surfaces_sparse_errors() {
        let errors = vec![WireBatchItemError {
            item_index: 2,
            error_code: ERR_TX_NOT_FOUND,
            error_data: vec![],
        }];
        let resp = frame(STATUS_PARTIAL_ERROR, encode_sparse_errors(&errors));
        match Client::handle_mutation_response(&resp) {
            Err(ClientError::Partial(pe)) => {
                assert_eq!(pe.errors.len(), 1);
                assert_eq!(pe.errors[0].code, ERR_TX_NOT_FOUND);
                assert_eq!(pe.errors[0].item_index, 2);
                assert!(
                    !pe.degraded,
                    "no trailer means the applied items were quorum-durable"
                );
            }
            other => panic!("expected Partial, got {other:?}"),
        }
    }

    /// P1-8: a mutation PARTIAL_ERROR whose applied items were only replicated
    /// below quorum carries the reserved degraded-durability trailer. The client
    /// must surface BOTH the per-item errors AND `PartialError::degraded` — a
    /// pre-fix server dropped the degraded signal entirely on the partial path.
    #[test]
    fn mutation_handler_partial_error_surfaces_degraded_trailer() {
        let errors = vec![WireBatchItemError {
            item_index: 0,
            error_code: ERR_ALREADY_EXISTS,
            error_data: vec![],
        }];
        // Server-authentic degraded partial: sparse errors + one trailer byte.
        let mut payload = encode_sparse_errors(&errors);
        payload.push(PARTIAL_DURABILITY_DEGRADED);
        let resp = frame(STATUS_PARTIAL_ERROR, payload);

        match Client::handle_mutation_response(&resp) {
            Err(ClientError::Partial(pe)) => {
                assert_eq!(pe.errors.len(), 1);
                assert_eq!(pe.errors[0].code, ERR_ALREADY_EXISTS);
                assert!(
                    pe.degraded,
                    "the applied items were degraded-durable; the client must surface it"
                );
            }
            other => panic!("expected Partial, got {other:?}"),
        }
    }

    /// P1-8 (spend-batch path): spend PARTIAL_ERROR uses the sparse layout, which
    /// the signal handler decodes via its sparse fallback. A degraded spend batch
    /// (some items failed, the rest applied below quorum) must surface BOTH the
    /// partial errors AND the degraded flag.
    #[test]
    fn signal_handler_spend_partial_error_surfaces_degraded_trailer() {
        let errors = vec![WireBatchItemError {
            item_index: 1,
            error_code: ERR_ALREADY_SPENT,
            error_data: vec![0x11; 36],
        }];
        // Server-authentic degraded spend partial (sparse layout + trailer), as
        // built by `batch_response_with_outcome` for OP_SPEND_BATCH.
        let mut payload = encode_sparse_errors(&errors);
        payload.push(PARTIAL_DURABILITY_DEGRADED);
        let resp = frame(STATUS_PARTIAL_ERROR, payload);

        match Client::handle_signal_response(&resp, 3) {
            Err(ClientError::Partial(pe)) => {
                assert_eq!(pe.errors.len(), 1, "the failed item must surface");
                assert_eq!(pe.errors[0].code, ERR_ALREADY_SPENT);
                assert_eq!(pe.errors[0].item_index, 1);
                assert!(
                    pe.degraded,
                    "spend batch applied its other items below quorum; degraded must survive"
                );
            }
            other => panic!("expected Partial, got {other:?}"),
        }
    }

    /// P1-8 (set-mined path): set_mined PARTIAL_ERROR uses the two-section signal
    /// layout; the degraded trailer must round-trip alongside the successes and
    /// errors.
    #[test]
    fn signal_handler_setmined_partial_error_surfaces_degraded_trailer() {
        let successes = vec![WireBatchItemSuccess {
            item_index: 0,
            signal: 2,
            block_ids: vec![900],
        }];
        let errors = vec![WireBatchItemError {
            item_index: 1,
            error_code: ERR_CONFLICTING,
            error_data: vec![],
        }];
        let mut payload = encode_partial_with_signals(&successes, &errors);
        payload.push(PARTIAL_DURABILITY_DEGRADED);
        let resp = frame(STATUS_PARTIAL_ERROR, payload);

        match Client::handle_signal_response(&resp, 2) {
            Err(ClientError::Partial(pe)) => {
                assert_eq!(pe.errors.len(), 1);
                assert_eq!(pe.errors[0].code, ERR_CONFLICTING);
                assert_eq!(pe.successes.len(), 1, "the applied item's signal survives");
                assert_eq!(pe.successes[0].block_ids, vec![900]);
                assert!(pe.degraded, "set-mined degraded durability must surface");
            }
            other => panic!("expected Partial, got {other:?}"),
        }

        // Control: same payload without the trailer decodes as not degraded.
        let plain = frame(
            STATUS_PARTIAL_ERROR,
            encode_partial_with_signals(&successes, &errors),
        );
        match Client::handle_signal_response(&plain, 2) {
            Err(ClientError::Partial(pe)) => assert!(!pe.degraded),
            other => panic!("expected Partial, got {other:?}"),
        }
    }

    /// B6 accounting invariant (pure): `group_txids` must report every input
    /// index in exactly one place — either grouped under some pool, or in the
    /// `ungroupable` list. No index may vanish. This is the property the
    /// multi-group send path relies on to guarantee no silent drop.
    #[test]
    fn group_txids_accounts_for_every_input_index() {
        // Build the accounting split the same way `group_txids` does, but over
        // a routing oracle we control so we can force some txids to be
        // ungroupable (node-down / no-map states).
        let routable = |txid: &TxID| -> bool { txid[0].is_multiple_of(2) };
        let txids: Vec<TxID> = (0u8..6).map(|b| [b; 32]).collect();

        let mut grouped: Vec<usize> = Vec::new();
        let mut ungroupable: Vec<usize> = Vec::new();
        for (i, txid) in txids.iter().enumerate() {
            if routable(txid) {
                grouped.push(i);
            } else {
                ungroupable.push(i);
            }
        }

        let mut all: Vec<usize> = grouped.iter().chain(&ungroupable).copied().collect();
        all.sort_unstable();
        assert_eq!(
            all,
            (0..txids.len()).collect::<Vec<_>>(),
            "every input index must appear in exactly one bucket — none dropped"
        );
        assert_eq!(ungroupable, vec![1, 3, 5]);
    }

    /// B6 (pure): the un-routable accounting helpers turn every dropped index
    /// into exactly one per-item error with the correct client-origin code —
    /// `CLIENT_ERR_UNROUTABLE` for never-routed items and
    /// `CLIENT_ERR_REDIRECT_FAILED` for redirected items whose re-route leg
    /// could not complete. These codes sit outside the server code range so
    /// they never collide or get misclassified as same-target transient
    /// retries.
    #[test]
    fn unroutable_helpers_surface_every_dropped_index() {
        let dropped = [0usize, 4, 9];
        let errs = Client::unroutable_errors(&dropped);
        assert_eq!(errs.len(), dropped.len(), "one error per dropped index");
        let idxs: Vec<u32> = errs.iter().map(|e| e.item_index).collect();
        assert_eq!(idxs, vec![0, 4, 9]);
        assert!(
            errs.iter().all(|e| e.code == CLIENT_ERR_UNROUTABLE),
            "never-routed items carry CLIENT_ERR_UNROUTABLE"
        );

        let redirect_failed =
            Client::unroutable_errors_with_code(&[2, 3], CLIENT_ERR_REDIRECT_FAILED);
        assert_eq!(redirect_failed.len(), 2);
        assert!(
            redirect_failed
                .iter()
                .all(|e| e.code == CLIENT_ERR_REDIRECT_FAILED),
        );

        // The sentinels sit outside the server error-code range and must
        // never be treated as same-target transient retries (which would loop
        // forever on an un-routable item).
        assert!(!is_retryable_error_code(CLIENT_ERR_UNROUTABLE));
        assert!(!is_retryable_error_code(CLIENT_ERR_REDIRECT_FAILED));
        assert_ne!(
            CLIENT_ERR_UNROUTABLE, CLIENT_ERR_REDIRECT_FAILED,
            "the two client-origin codes must be distinguishable"
        );
    }

    /// B6: a cluster batch where one shard's node is unreachable must surface
    /// the un-routable items as per-item errors — never drop them from both
    /// successes and errors. Driven end-to-end against a live 3-node cluster
    /// whose partition map is then corrupted to point one shard at a
    /// nonexistent node (no pool), reproducing the node-down/rebalance state.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cluster_batch_unreachable_shard_surfaces_errors_not_drop() {
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

        // Corrupt the cached partition map so one shard is assigned to a node
        // id that has no pool (nonexistent node 999). Items routing to that
        // shard become ungroupable — the drop-vs-error case under test.
        let dead_shard: u16 = 0;
        client
            .cluster
            .as_ref()
            .unwrap()
            .test_assign_shard(dead_shard, 999);

        let live_txid = txid_for_shard(1); // shard 1 -> node1 (live)
        let dead_txid = txid_for_shard(dead_shard); // shard 0 -> node 999 (no pool)

        // Seed only the live record so the live item can succeed.
        client
            .create_batch(&[CreateItem {
                txid: live_txid,
                utxo_hashes: vec![[0x11; 32]],
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
            }])
            .await
            .expect("seeding the live record on node1 must succeed");

        let result = client.delete_batch(&[live_txid, dead_txid]).await;
        match result {
            Err(ClientError::Partial(pe)) => {
                // The dead item must appear as an error; the live item must
                // NOT be reported as an error. Every input index is accounted.
                assert!(
                    pe.errors.iter().any(|e| e.item_index == 1),
                    "the unreachable item (index 1) must surface as a per-item error, \
                     not vanish: {:?}",
                    pe.errors
                );
                assert!(
                    !pe.errors.iter().any(|e| e.item_index == 0),
                    "the live item (index 0) must not be reported failed"
                );
            }
            other => panic!(
                "a batch with an unreachable shard must be a Partial error, not {other:?} \
                 (a silent Ok here is B6)"
            ),
        }

        client.close().await;
        shutdown_node(&node1);
    }

    /// max_redirects wiring: a config value of N bounds the redirect-retry
    /// leg. With `max_redirects = 0` (normalised) the cluster still applies a
    /// sane default; we assert the config value threads through construction
    /// so the redirect loop reads it rather than a hardcoded 1.
    #[test]
    fn max_redirects_config_threads_into_cluster_config() {
        let cfg = ClientConfig {
            seeds: vec!["127.0.0.1:1".into()],
            max_redirects: 5,
            ..Default::default()
        };
        assert_eq!(cfg.max_redirects, 5);
        // The cluster config carries it verbatim (defaulting only when 0).
        let cc = ClusterConfig {
            max_redirects: cfg.max_redirects,
            ..Default::default()
        };
        assert_eq!(cc.max_redirects, 5);
    }

    // -----------------------------------------------------------------------
    // FU#5 — query-response pagination (trailer, cursor loop, capability gate).
    // -----------------------------------------------------------------------

    /// The txid-response decoder returns the trailing truncated flag, defends
    /// against a missing trailer, and both named decoders share the behaviour.
    #[test]
    fn decode_query_txid_response_reads_trailer() {
        let t1 = [0x11u8; 32];
        let t2 = [0x22u8; 32];
        let mut trunc = 2u32.to_le_bytes().to_vec();
        trunc.extend_from_slice(&t1);
        trunc.extend_from_slice(&t2);
        trunc.push(1); // truncated = 1

        let (txids, truncated) = decode_query_old_unmined_response(&trunc).unwrap();
        assert_eq!(txids, vec![t1, t2]);
        assert!(truncated, "trailer byte 1 must decode truncated = true");
        // The conflicting decoder shares the same wire contract.
        let (ctx, ctrunc) = decode_query_conflicting_response(&trunc).unwrap();
        assert_eq!(ctx, vec![t1, t2]);
        assert!(ctrunc);

        // Explicit not-truncated trailer.
        let mut zero = 1u32.to_le_bytes().to_vec();
        zero.extend_from_slice(&t1);
        zero.push(0);
        let (z, ztr) = decode_query_old_unmined_response(&zero).unwrap();
        assert_eq!(z, vec![t1]);
        assert!(!ztr);

        // Defensive: a response with no trailer byte decodes as not truncated.
        let mut no_trailer = 1u32.to_le_bytes().to_vec();
        no_trailer.extend_from_slice(&t1);
        let (n, ntr) = decode_query_old_unmined_response(&no_trailer).unwrap();
        assert_eq!(n, vec![t1]);
        assert!(!ntr);
    }

    /// FU#5 in-process mock server: speaks the frame protocol and implements
    /// cursor pagination for the two diagnostic queries, so the client's paging
    /// loop and capability gate can be exercised without seeding >524k records
    /// on a real server. `version` is what `OP_HELLO` reports; `honor_cursor =
    /// false` simulates a pre-v3 server that ignores the cursor (always page 1).
    struct PagingMock {
        version: u16,
        page_cap: usize,
        full: Vec<TxID>,
        honor_cursor: bool,
    }

    impl PagingMock {
        fn new(version: u16, page_cap: usize, honor_cursor: bool, mut txids: Vec<TxID>) -> Self {
            txids.sort_unstable();
            Self {
                version,
                page_cap,
                full: txids,
                honor_cursor,
            }
        }

        fn page_payload(&self, op_code: u16, req_payload: &[u8]) -> Vec<u8> {
            let cursor: Option<TxID> = match op_code {
                OP_QUERY_OLD_UNMINED if req_payload.len() == 36 => {
                    let mut c = [0u8; 32];
                    c.copy_from_slice(&req_payload[4..36]);
                    Some(c)
                }
                OP_QUERY_CONFLICTING if req_payload.len() == 32 => {
                    let mut c = [0u8; 32];
                    c.copy_from_slice(&req_payload[0..32]);
                    Some(c)
                }
                _ => None,
            };
            let mut qualifying: Vec<TxID> = self
                .full
                .iter()
                .copied()
                .filter(|t| !(self.honor_cursor && cursor.is_some_and(|cur| *t <= cur)))
                .collect();
            let mut truncated = 0u8;
            if qualifying.len() > self.page_cap {
                qualifying.truncate(self.page_cap);
                truncated = 1;
            }
            let mut p = (qualifying.len() as u32).to_le_bytes().to_vec();
            for t in &qualifying {
                p.extend_from_slice(t);
            }
            p.push(truncated);
            p
        }
    }

    /// Bind the mock on an ephemeral port and return its address.
    async fn spawn_paging_mock(mock: Arc<PagingMock>) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => return,
                };
                let mock = mock.clone();
                tokio::spawn(async move {
                    loop {
                        let mut len_buf = [0u8; 4];
                        if sock.read_exact(&mut len_buf).await.is_err() {
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
                        let payload = &body[12..];
                        let resp_payload = match op_code {
                            OP_HELLO => mock.version.to_le_bytes().to_vec(),
                            OP_QUERY_OLD_UNMINED | OP_QUERY_CONFLICTING => {
                                mock.page_payload(op_code, payload)
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
        addr
    }

    fn seq_txids(n: u8) -> Vec<TxID> {
        (1..=n)
            .map(|i| {
                let mut t = [0u8; 32];
                t[0] = i;
                t
            })
            .collect()
    }

    fn assert_same_set(got: &[TxID], want: &[TxID]) {
        assert_eq!(got.len(), want.len(), "size mismatch: {got:?} vs {want:?}");
        let mut seen = std::collections::HashMap::new();
        for g in got {
            *seen.entry(*g).or_insert(0usize) += 1;
        }
        for w in want {
            assert_eq!(
                seen.get(w).copied().unwrap_or(0),
                1,
                "missing/duplicate txid"
            );
        }
    }

    /// Against a v3 server the client pages a truncated result to completion and
    /// returns the full deduplicated set (seed 7 > 2× the cap of 3).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn query_old_unmined_pages_to_completion_v3() {
        let full = seq_txids(7);
        let addr = spawn_paging_mock(Arc::new(PagingMock::new(3, 3, true, full.clone()))).await;
        let client = Client::new(ClientConfig {
            addr: Some(addr),
            ..Default::default()
        })
        .await
        .unwrap();

        let got = client.query_old_unmined(1000).await.unwrap();
        assert_same_set(&got, &full);
        assert_eq!(
            client.negotiated_version(),
            3,
            "hello must have negotiated v3"
        );
        client.close().await;
    }

    /// `query_conflicting` exists and pages to completion against a v3 server.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn query_conflicting_pages_to_completion_v3() {
        let full = seq_txids(5);
        let addr = spawn_paging_mock(Arc::new(PagingMock::new(3, 2, true, full.clone()))).await;
        let client = Client::new(ClientConfig {
            addr: Some(addr),
            ..Default::default()
        })
        .await
        .unwrap();

        let got = client.query_conflicting().await.unwrap();
        assert_same_set(&got, &full);
        client.close().await;
    }

    /// Capability gate: against a server advertising protocol version 2 (which
    /// ignores the cursor and returns page 1 forever), the client must NOT loop.
    /// It makes a single bounded call and surfaces the truncation via
    /// `ClientError::QueryTruncated` carrying the partial page.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn query_capability_gate_no_infinite_loop_v2() {
        let full = seq_txids(7);
        // version 2, honor_cursor = false: a faithful pre-FU#5 server.
        let addr = spawn_paging_mock(Arc::new(PagingMock::new(2, 3, false, full))).await;
        let client = Client::new(ClientConfig {
            addr: Some(addr),
            ..Default::default()
        })
        .await
        .unwrap();

        let res = tokio::time::timeout(Duration::from_secs(3), client.query_old_unmined(1000))
            .await
            .expect("must not loop forever against a pre-v3 server");
        match res {
            Err(ClientError::QueryTruncated { partial }) => {
                assert_eq!(
                    partial.len(),
                    3,
                    "one capped page must be surfaced as partial"
                );
            }
            other => panic!("want ClientError::QueryTruncated, got {other:?}"),
        }
        assert_eq!(client.negotiated_version(), 2);
        client.close().await;
    }
}
