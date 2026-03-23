//! Request dispatch: maps wire protocol opcodes to Engine methods.
//!
//! In clustered mode, the dispatcher checks shard ownership before
//! processing key-based operations. If this node doesn't own the shard,
//! it returns a Redirect response.
//!
//! After successful mutations:
//! - Redo log entries are appended for crash recovery.
//! - Replication ops are sent to replica nodes (if in cluster mode with RF > 1).

use crate::cluster::coordinator::RunningCluster;
use crate::cluster::shards::ShardTable;
use crate::index::TxKey;
use crate::ops::create::*;
use crate::ops::engine::Engine;
use crate::record::METADATA_SIZE;
use crate::ops::error::SpendError;
use crate::ops::mark_longest_chain::*;
use crate::ops::remaining::*;
use crate::ops::set_mined::*;
use crate::ops::spend::*;
use crate::ops::unspend::*;
use crate::protocol::codec::*;
use crate::protocol::frame::*;
use crate::protocol::opcodes::*;
use crate::redo::{RedoLog, RedoOp};
use crate::replication::manager::ReplicaTransport;
use crate::replication::protocol::{ReplicaAck, ReplicaBatch, ReplicaOp};
use crate::replication::receiver::handle_replica_batch;
use crate::replication::tcp_transport::TcpReplicaTransport;
use crate::storage::blobstore::BlobStore;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU64;
use std::sync::LazyLock;
use std::time::Duration;

/// Global connection pool for replication TCP connections.
///
/// Maps each replica's `SocketAddr` to a persistent `TcpReplicaTransport`.
/// Connections are reused across dispatches to avoid the overhead of opening
/// a new TCP connection per batch. When a connection fails, it is discarded
/// and a fresh one is created on the next attempt.
static REPL_POOL: LazyLock<Mutex<HashMap<SocketAddr, TcpReplicaTransport>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Dispatch a request frame to the appropriate Engine method.
///
/// If `cluster` is Some, shard ownership is checked for key-based operations.
/// Requests for keys not owned by this node get a Redirect response.
///
/// If `redo_log` is Some, successful mutations are logged for crash recovery.
/// Redo log writes are best-effort: failures are logged but do not fail the
/// client request (the data has already been applied to the engine).
pub(crate) fn handle_request(
    request: &RequestFrame,
    engine: &Engine,
    max_batch_size: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
    conn_state: &mut super::ConnectionState,
    blob_store: Option<&dyn BlobStore>,
) -> ResponseFrame {
    // Reject mutations when the cluster lacks quorum to prevent split-brain.
    if is_mutation_opcode(request.op_code) {
        if let Some(err_resp) = check_quorum(cluster, request.request_id) {
            return err_resp;
        }
    }

    match request.op_code {
        OP_SPEND_BATCH => handle_spend_batch(request, engine, max_batch_size, cluster, redo_log),
        OP_UNSPEND_BATCH => handle_unspend_batch(request, engine, max_batch_size, cluster, redo_log),
        OP_SET_MINED_BATCH => handle_set_mined_batch(request, engine, max_batch_size, cluster, redo_log),
        OP_CREATE_BATCH => handle_create_batch(request, engine, max_batch_size, cluster, redo_log, blob_store),
        OP_FREEZE_BATCH => handle_freeze_batch(request, engine, max_batch_size, cluster, redo_log),
        OP_UNFREEZE_BATCH => handle_unfreeze_batch(request, engine, max_batch_size, cluster, redo_log),
        OP_REASSIGN_BATCH => handle_reassign_batch(request, engine, max_batch_size, cluster, redo_log),
        OP_SET_CONFLICTING_BATCH => handle_set_conflicting_batch(request, engine, max_batch_size, cluster, redo_log),
        OP_SET_LOCKED_BATCH => handle_set_locked_batch(request, engine, max_batch_size, cluster, redo_log),
        OP_PRESERVE_UNTIL_BATCH => handle_preserve_until_batch(request, engine, max_batch_size, cluster, redo_log),
        OP_DELETE_BATCH => handle_delete_batch(request, engine, max_batch_size, cluster, redo_log),
        OP_MARK_LONGEST_CHAIN_BATCH => handle_mark_longest_chain_batch(request, engine, max_batch_size, cluster, redo_log),
        OP_GET_BATCH => handle_get_batch(request, engine, max_batch_size, cluster),
        OP_GET_SPEND_BATCH => handle_get_spend_batch(request, engine, max_batch_size, cluster),
        OP_QUERY_OLD_UNMINED => handle_query_old_unmined(request, engine),
        OP_PRESERVE_TRANSACTIONS => handle_preserve_transactions(request, engine, max_batch_size, cluster, redo_log),
        OP_PROCESS_EXPIRED_PRESERVATIONS => handle_process_expired(request, engine, redo_log),
        OP_GET_PARTITION_MAP => handle_get_partition_map(request, cluster),
        OP_PING => ResponseFrame {
            request_id: request.request_id,
            status: STATUS_OK,
            payload: vec![],
        },
        OP_HEALTH => ResponseFrame {
            request_id: request.request_id,
            status: STATUS_OK,
            payload: b"ok".to_vec(),
        },
        OP_INCREMENT_SPENT_EXTRA_RECS => ResponseFrame {
            request_id: request.request_id,
            status: STATUS_OK,
            payload: vec![], // No-op compatibility shim
        },
        OP_STREAM_CHUNK => handle_stream_chunk(request, conn_state, blob_store, cluster),
        OP_STREAM_END => handle_stream_end(request, conn_state),
        OP_REPLICA_BATCH => {
            // Dispatch replication batch to the receiver's apply logic.
            thread_local! {
                static DISPATCH_LAST_APPLIED: AtomicU64 = const { AtomicU64::new(0) };
            }
            let resp = DISPATCH_LAST_APPLIED.with(|la| {
                handle_replica_batch(request, engine, la)
            });

            // After processing a replica batch, mark any inbound shards as
            // received so the read path stops waiting for migration data.
            if resp.status == STATUS_OK {
                if let Some(cluster) = cluster {
                    // Parse the batch to find which shards were included.
                    // The request_id is set to the shard number during migration.
                    cluster.mark_inbound_complete(request.request_id as u16);
                }
            }

            resp
        }
        OP_MIGRATION_COMPLETE => {
            // Migration-complete handshake: the source has finished
            // streaming all batches for a shard and wants confirmation
            // that we received the data. The request_id carries the shard.
            // We simply acknowledge — the data was already applied via
            // OP_REPLICA_BATCH handlers above.
            if let Some(cluster) = cluster {
                cluster.mark_inbound_complete(request.request_id as u16);
            }
            ResponseFrame {
                request_id: request.request_id,
                status: STATUS_OK,
                payload: Vec::new(),
            }
        }
        _ => error_response(request.request_id, ERR_INTERNAL, "unknown opcode"),
    }
}

// ---------------------------------------------------------------------------
// Redo log helper
// ---------------------------------------------------------------------------

/// Append redo ops to the log and flush. Best-effort: errors are logged
/// to stderr but do not propagate. The data is already on the device via
/// the engine, so redo logging failures only affect crash recovery coverage.
fn write_redo_ops(redo_log: Option<&Mutex<RedoLog>>, ops: &[RedoOp]) {
    let redo = match redo_log {
        Some(r) => r,
        None => return,
    };
    let mut log = redo.lock();
    for op in ops {
        if let Err(e) = log.append(op.clone()) {
            eprintln!("redo log append error: {e}");
            return;
        }
    }
    if let Err(e) = log.flush() {
        eprintln!("redo log flush error: {e}");
    }
}

// ---------------------------------------------------------------------------
// Replication helper
// ---------------------------------------------------------------------------

/// Send replication operations to replica nodes for the given key.
///
/// For each replica node that owns a copy of the key's shard, the batch is
/// sent synchronously on the current dispatch thread using a persistent TCP
/// connection from the global pool. Replication failures are logged but do
/// not affect the client response — the replica will catch up from the redo
/// log or via anti-entropy.
///
/// Synchronous sending ensures data exists on replicas before the client
/// gets the ACK, which is required for RF=2 durability. The dispatch thread
/// is dedicated to this client connection, so blocking here is acceptable.
fn replicate_all_ops(
    cluster: Option<&RunningCluster>,
    ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)>,
) {
    let cluster = match cluster {
        Some(c) => c,
        None => return,
    };
    if ops_by_key.is_empty() {
        return;
    }

    // Group all ops by target replica address
    let table = cluster.shard_table();
    let table_guard = table.read().unwrap();
    let mut by_addr: HashMap<SocketAddr, Vec<ReplicaOp>> = HashMap::new();

    for (key, ops) in &ops_by_key {
        let shard = ShardTable::shard_for_key(key);
        let assignment = table_guard.assignment(shard);
        for replica_id in &assignment.replicas {
            if let Some(addr) = cluster.node_addr(replica_id) {
                by_addr.entry(addr).or_default().extend(ops.clone());
            }
        }
    }
    drop(table_guard);

    // Send one batch per replica node synchronously using persistent connections.
    for (addr, ops) in by_addr {
        if ops.is_empty() {
            continue;
        }
        let batch = ReplicaBatch {
            first_sequence: 0,
            ops,
        };
        if let Err(e) = send_replica_batch_to(addr, &batch) {
            eprintln!("replication to {addr} failed: {e}");
        }
    }
}

/// Send a `ReplicaBatch` to a replica node via TCP using the wire protocol.
///
/// Reuses a persistent connection from the global `REPL_POOL`. If no cached
/// connection exists or the cached connection has failed, a fresh TCP
/// connection is established. On success the connection is returned to the
/// pool for future reuse.
fn send_replica_batch_to(
    addr: SocketAddr,
    batch: &ReplicaBatch,
) -> std::result::Result<(), String> {
    // Take an existing connection from the pool (if any).
    // We remove it so other threads don't try to use it concurrently,
    // and we drop the lock before doing any I/O.
    let existing = REPL_POOL.lock().remove(&addr);

    let mut transport = match existing {
        Some(t) if t.is_connected() => t,
        _ => {
            // No cached connection or the cached one is dead — create a fresh one.
            TcpReplicaTransport::connect(&addr.to_string(), Duration::from_secs(5))
                .map_err(|e| format!("connect: {e}"))?
        }
    };

    // Attempt to send the batch and receive acknowledgment.
    if let Err(e) = transport.send_batch(batch) {
        // Connection broke during send — discard it and do NOT return to pool.
        return Err(format!("send: {e}"));
    }

    match transport.recv_ack(Duration::from_secs(10)) {
        Ok(ReplicaAck::Ok { .. }) => {
            // Success — return connection to the pool for reuse.
            REPL_POOL.lock().insert(addr, transport);
            Ok(())
        }
        Ok(ReplicaAck::Error { message, .. }) => {
            // Application-level error — connection is still usable.
            REPL_POOL.lock().insert(addr, transport);
            Err(format!("replica error: {message}"))
        }
        Err(e) => {
            // Transport-level error — discard the broken connection.
            Err(format!("recv_ack: {e}"))
        }
    }
}

// ---------------------------------------------------------------------------
// Quorum check
// ---------------------------------------------------------------------------

/// Check if the cluster has quorum (majority of nodes are alive).
///
/// Returns `None` if quorum is met or no cluster is configured (single-node mode).
/// Returns `Some(ResponseFrame)` with an error if quorum is not met, meaning
/// this node cannot safely accept mutations.
///
/// In a clustered deployment, a node must see at least 2 alive nodes (including
/// itself) to accept writes. This prevents split-brain scenarios where isolated
/// nodes diverge by independently accepting conflicting writes.
fn check_quorum(cluster: Option<&RunningCluster>, request_id: u64) -> Option<ResponseFrame> {
    let cluster = cluster?;
    let alive = cluster.alive_node_count();
    let peak = cluster.peak_cluster_size();

    // A node that has only ever seen itself (peak=1) is a standalone cluster
    // node — quorum is trivially met. This covers single-node test setups
    // and bootstrap scenarios.
    if peak <= 1 {
        return None;
    }

    // For a node that was previously part of a multi-node cluster, require
    // a majority (more than half of the peak observed cluster size) to prevent
    // split-brain. With 3 nodes, need >= 2. With 5 nodes, need >= 3.
    let quorum_needed = (peak / 2) + 1;
    if alive < quorum_needed {
        return Some(error_response(request_id, ERR_NO_QUORUM, "no quorum"));
    }
    None
}

/// Returns true if the given opcode is a mutation that requires quorum.
fn is_mutation_opcode(op: u16) -> bool {
    matches!(
        op,
        OP_SPEND_BATCH
            | OP_UNSPEND_BATCH
            | OP_SET_MINED_BATCH
            | OP_CREATE_BATCH
            | OP_FREEZE_BATCH
            | OP_UNFREEZE_BATCH
            | OP_REASSIGN_BATCH
            | OP_SET_CONFLICTING_BATCH
            | OP_SET_LOCKED_BATCH
            | OP_PRESERVE_UNTIL_BATCH
            | OP_DELETE_BATCH
            | OP_MARK_LONGEST_CHAIN_BATCH
            | OP_PRESERVE_TRANSACTIONS
            | OP_PROCESS_EXPIRED_PRESERVATIONS
    )
}

// ---------------------------------------------------------------------------
// Shard ownership check
// ---------------------------------------------------------------------------

/// Check if a txid belongs to a shard owned by this node.
///
/// Returns `None` if the key is local (or no cluster is configured).
/// Returns `Some(BatchItemError)` with a redirect error if the key belongs
/// to a remote node, including the target node's address in `error_data`.
///
/// When `allow_if_migrating` is true (for read operations), the check
/// allows local handling if this node is actively migrating the shard
/// outbound — the data is still present locally until migration completes.
fn check_shard_ownership(
    txid: &[u8; 32],
    item_index: u32,
    cluster: Option<&RunningCluster>,
    allow_if_migrating: bool,
) -> Option<BatchItemError> {
    let cluster = cluster?;
    let key = TxKey { txid: *txid };
    if cluster.is_master(&key) {
        // If we're the new master but still waiting for inbound migration
        // data, reject mutations so clients retry after migration completes.
        // Reads are handled separately with a wait loop.
        if !allow_if_migrating && cluster.has_pending_inbound(&key) {
            return Some(BatchItemError {
                item_index,
                error_code: ERR_MIGRATION_IN_PROGRESS,
                error_data: Vec::new(),
            });
        }
        return None;
    }
    // During outbound migration, reads can still be served locally
    // because the data hasn't been removed yet.
    if allow_if_migrating && cluster.is_migrating_outbound(&key) {
        return None;
    }
    // Determine the target node address for the redirect
    let route = cluster.route(&key);
    let error_data = match route {
        crate::cluster::shards::RouteDecision::RedirectTo { node, .. } => {
            match cluster.node_addr(&node) {
                Some(addr) => addr.to_string().into_bytes(),
                None => Vec::new(),
            }
        }
        crate::cluster::shards::RouteDecision::HandleLocally => return None,
    };
    Some(BatchItemError {
        item_index,
        error_code: ERR_REDIRECT,
        error_data,
    })
}

// ---------------------------------------------------------------------------
// Spend
// ---------------------------------------------------------------------------

fn handle_spend_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    let (params, items) = match decode_spend_batch(&req.payload) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed spend batch"),
    };
    if items.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }

    // Group items by txid for efficient locking
    let mut by_txid: HashMap<[u8; 32], Vec<(usize, &WireSpendItem)>> = HashMap::new();
    for (i, item) in items.iter().enumerate() {
        by_txid.entry(item.txid).or_default().push((i, item));
    }

    let mut errors: Vec<BatchItemError> = Vec::new();
    let mut redo_ops: Vec<RedoOp> = Vec::new();
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();

    for (txid, group) in &by_txid {
        // Check shard ownership for the first item in the group (all share the same txid)
        if let Some(redirect_err) = check_shard_ownership(txid, group[0].0 as u32, cluster, false) {
            // All items in this group get redirect errors
            for &(i, _) in group {
                errors.push(BatchItemError {
                    item_index: i as u32,
                    error_code: redirect_err.error_code,
                    error_data: redirect_err.error_data.clone(),
                });
            }
            continue;
        }

        let spend_items: Vec<SpendItem> = group
            .iter()
            .map(|(i, item)| SpendItem {
                offset: item.vout,
                utxo_hash: item.utxo_hash,
                spending_data: item.spending_data,
                idx: *i as u32,
            })
            .collect();

        let multi_req = SpendMultiRequest {
            tx_key: TxKey { txid: *txid },
            spends: spend_items,
            ignore_conflicting: params.ignore_conflicting,
            ignore_locked: params.ignore_locked,
            current_block_height: params.current_block_height,
            block_height_retention: params.block_height_retention,
        };

        match engine.spend_multi(&multi_req) {
            Ok(resp) => {
                // Collect successful items for redo/replication
                let error_indices: std::collections::HashSet<u32> =
                    resp.errors.keys().copied().collect();

                let key = TxKey { txid: *txid };
                let mut key_repl_ops = Vec::new();

                for &(i, item) in group {
                    if !error_indices.contains(&(i as u32)) {
                        redo_ops.push(RedoOp::Spend {
                            tx_key: key,
                            offset: item.vout,
                            spending_data: item.spending_data,
                            new_spent_count: 0, // Precise count not tracked per-item here
                        });
                        key_repl_ops.push(ReplicaOp::Spend {
                            tx_key: key,
                            offset: item.vout,
                            spending_data: item.spending_data,
                        });
                    }
                }

                if !key_repl_ops.is_empty() {
                    repl_ops_by_key.push((key, key_repl_ops));
                }

                for (idx, err) in resp.errors {
                    errors.push(spend_error_to_batch_error(idx, &err));
                }
            }
            Err(err) => {
                // Record-level error applies to all items in this group
                for &(i, _) in group {
                    errors.push(spend_error_to_batch_error(i as u32, &err));
                }
            }
        }
    }

    write_redo_ops(redo_log, &redo_ops);
    replicate_all_ops(cluster, repl_ops_by_key);

    if errors.is_empty() {
        ResponseFrame {
            request_id: req.request_id,
            status: STATUS_OK,
            payload: vec![],
        }
    } else {
        errors.sort_by_key(|e| e.item_index);
        ResponseFrame {
            request_id: req.request_id,
            status: STATUS_PARTIAL_ERROR,
            payload: encode_sparse_errors(&errors),
        }
    }
}

// ---------------------------------------------------------------------------
// Unspend
// ---------------------------------------------------------------------------

fn handle_unspend_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    let (params, items) = match decode_unspend_batch(&req.payload) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed unspend batch"),
    };
    if items.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }

    let mut errors = Vec::new();
    let mut redo_ops: Vec<RedoOp> = Vec::new();
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();

    for (i, item) in items.iter().enumerate() {
        if let Some(redirect_err) = check_shard_ownership(&item.txid, i as u32, cluster, false) {
            errors.push(redirect_err);
            continue;
        }
        let key = TxKey { txid: item.txid };
        let result = engine.unspend(&UnspendRequest {
            tx_key: key,
            offset: item.vout,
            utxo_hash: item.utxo_hash,
            current_block_height: params.current_block_height,
            block_height_retention: params.block_height_retention,
        });
        match result {
            Ok(_) => {
                redo_ops.push(RedoOp::Unspend {
                    tx_key: key,
                    offset: item.vout,
                    new_spent_count: 0,
                });
                repl_ops_by_key.push((key, vec![ReplicaOp::Unspend {
                    tx_key: key,
                    offset: item.vout,
                }]));
            }
            Err(err) => {
                errors.push(spend_error_to_batch_error(i as u32, &err));
            }
        }
    }

    write_redo_ops(redo_log, &redo_ops);
    replicate_all_ops(cluster, repl_ops_by_key);

    batch_response(req.request_id, &errors)
}

// ---------------------------------------------------------------------------
// SetMined
// ---------------------------------------------------------------------------

fn handle_set_mined_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    let (params, txids) = match decode_set_mined_batch(&req.payload) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed set_mined batch"),
    };
    if txids.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }

    let mut errors = Vec::new();
    let mut redo_ops: Vec<RedoOp> = Vec::new();
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();

    for (i, txid) in txids.iter().enumerate() {
        if let Some(redirect_err) = check_shard_ownership(txid, i as u32, cluster, false) {
            errors.push(redirect_err);
            continue;
        }
        let key = TxKey { txid: *txid };
        let result = engine.set_mined(&SetMinedRequest {
            tx_key: key,
            block_id: params.block_id,
            block_height: params.block_height,
            subtree_idx: params.subtree_idx,
            current_block_height: params.current_block_height,
            block_height_retention: params.block_height_retention,
            on_longest_chain: params.on_longest_chain,
            unset_mined: params.unset_mined,
        });
        match result {
            Ok(_) => {
                redo_ops.push(RedoOp::SetMined {
                    tx_key: key,
                    block_id: params.block_id,
                    block_height: params.block_height,
                    subtree_idx: params.subtree_idx,
                    unset: params.unset_mined,
                });
                if params.unset_mined {
                    repl_ops_by_key.push((key, vec![ReplicaOp::UnsetMined {
                        tx_key: key,
                        block_id: params.block_id,
                    }]));
                } else {
                    repl_ops_by_key.push((key, vec![ReplicaOp::SetMined {
                        tx_key: key,
                        block_id: params.block_id,
                        block_height: params.block_height,
                        subtree_idx: params.subtree_idx,
                        on_longest_chain: params.on_longest_chain,
                    }]));
                }
            }
            Err(err) => {
                errors.push(spend_error_to_batch_error(i as u32, &err));
            }
        }
    }

    write_redo_ops(redo_log, &redo_ops);
    replicate_all_ops(cluster, repl_ops_by_key);

    batch_response(req.request_id, &errors)
}

// ---------------------------------------------------------------------------
// Create
// ---------------------------------------------------------------------------

/// Parse the wire cold_data blob into separate inputs/outputs/inpoints fields.
/// Wire format: [inputs_len:4 LE][inputs][outputs_len:4 LE][outputs][inpoints_len:4 LE][inpoints]
fn parse_cold_data_fields(cold_data: &[u8]) -> (Option<Vec<u8>>, Option<Vec<u8>>, Option<Vec<u8>>) {
    if cold_data.len() < 12 {
        return (None, None, None);
    }
    let mut pos = 0usize;

    let il = u32::from_le_bytes(cold_data[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    if pos + il > cold_data.len() { return (None, None, None); }
    let inputs = cold_data[pos..pos + il].to_vec();
    pos += il;

    if pos + 4 > cold_data.len() { return (Some(inputs).filter(|v| !v.is_empty()), None, None); }
    let ol = u32::from_le_bytes(cold_data[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    if pos + ol > cold_data.len() { return (Some(inputs).filter(|v| !v.is_empty()), None, None); }
    let outputs = cold_data[pos..pos + ol].to_vec();
    pos += ol;

    if pos + 4 > cold_data.len() { return (Some(inputs).filter(|v| !v.is_empty()), Some(outputs).filter(|v| !v.is_empty()), None); }
    let pl = u32::from_le_bytes(cold_data[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    if pos + pl > cold_data.len() { return (Some(inputs).filter(|v| !v.is_empty()), Some(outputs).filter(|v| !v.is_empty()), None); }
    let inpoints = cold_data[pos..pos + pl].to_vec();

    (
        Some(inputs).filter(|v| !v.is_empty()),
        Some(outputs).filter(|v| !v.is_empty()),
        Some(inpoints).filter(|v| !v.is_empty()),
    )
}

fn handle_create_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
    blob_store: Option<&dyn BlobStore>,
) -> ResponseFrame {
    let items = match decode_create_batch(&req.payload) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed create batch"),
    };
    if items.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }

    let mut errors = Vec::new();
    let mut redo_ops: Vec<RedoOp> = Vec::new();
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();

    for (i, item) in items.iter().enumerate() {
        if let Some(redirect_err) = check_shard_ownership(&item.txid, i as u32, cluster, false) {
            errors.push(redirect_err);
            continue;
        }

        // Check whether this item uses an externally-uploaded blob.
        let is_ext = item.flags & FLAG_EXTERNAL_BLOB != 0;
        if is_ext {
            if let Some(bs) = blob_store {
                match bs.exists(&item.txid) {
                    Ok(true) => {}
                    Ok(false) => {
                        errors.push(BatchItemError {
                            item_index: i as u32,
                            error_code: ERR_BLOB_NOT_FOUND,
                            error_data: vec![],
                        });
                        continue;
                    }
                    Err(_) => {
                        errors.push(BatchItemError {
                            item_index: i as u32,
                            error_code: ERR_INTERNAL,
                            error_data: vec![],
                        });
                        continue;
                    }
                }
            }
        }

        let mined_block_infos = if let Some(block_id) = item.mined_block_id {
            vec![crate::ops::create::MinedBlockInfo {
                block_id,
                block_height: item.mined_block_height.unwrap_or(0),
                subtree_idx: item.mined_subtree_idx.unwrap_or(0),
            }]
        } else {
            vec![]
        };

        // Parse cold_data into inputs/outputs/inpoints for the engine.
        // For external blobs, cold_data is not stored inline — the engine
        // record just has the is_external flag so reads fetch from blobstore.
        let (inputs, outputs, inpoints) = if is_ext {
            (None, None, None)
        } else {
            parse_cold_data_fields(&item.cold_data)
        };

        let create_req = CreateRequest {
            tx_id: item.txid,
            tx_version: item.tx_version,
            locktime: item.locktime,
            fee: item.fee,
            size_in_bytes: item.size_in_bytes,
            extended_size: item.extended_size,
            is_coinbase: item.is_coinbase,
            spending_height: item.spending_height,
            utxo_hashes: item.utxo_hashes.clone(),
            inputs,
            outputs,
            inpoints,
            is_external: is_ext,
            created_at: item.created_at,
            block_height: item.block_height,
            mined_block_infos,
            frozen: item.flags & 0x04 != 0,
            conflicting: item.flags & 0x02 != 0,
            locked: item.flags & 0x01 != 0,
            parent_txids: item.parent_txids.clone(),
        };

        match engine.create(&create_req) {
            Ok(_) => {
                let key = TxKey { txid: item.txid };
                // Look up the just-created record to get its offset for the redo log
                if let Some(entry) = engine.lookup(&key) {
                    redo_ops.push(RedoOp::Create {
                        tx_key: key,
                        record_offset: entry.record_offset,
                        utxo_count: item.utxo_hashes.len() as u32,
                    });
                }
                // Serialize metadata for the replica: tx_version(4) + locktime(4) +
                // fee(8) + size_in_bytes(8) + extended_size(8) + is_coinbase(1) +
                // spending_height(4) + created_at(8) + flags(1) = 46 bytes.
                let mut meta_buf = Vec::with_capacity(46);
                meta_buf.extend_from_slice(&item.tx_version.to_le_bytes());
                meta_buf.extend_from_slice(&item.locktime.to_le_bytes());
                meta_buf.extend_from_slice(&item.fee.to_le_bytes());
                meta_buf.extend_from_slice(&item.size_in_bytes.to_le_bytes());
                meta_buf.extend_from_slice(&item.extended_size.to_le_bytes());
                meta_buf.push(if item.is_coinbase { 1 } else { 0 });
                meta_buf.extend_from_slice(&item.spending_height.to_le_bytes());
                meta_buf.extend_from_slice(&item.created_at.to_le_bytes());
                meta_buf.push(item.flags);

                repl_ops_by_key.push((key, vec![ReplicaOp::Create {
                    tx_key: key,
                    metadata_bytes: meta_buf,
                    utxo_hashes: item.utxo_hashes.clone(),
                    cold_data: if item.cold_data.is_empty() { None } else { Some(item.cold_data.clone()) },
                    is_external: item.flags & FLAG_EXTERNAL_BLOB != 0,
                }]));
            }
            Err(CreateError::DuplicateTxId) => {
                errors.push(BatchItemError {
                    item_index: i as u32,
                    error_code: ERR_ALREADY_EXISTS,
                    error_data: vec![],
                });
            }
            Err(_) => {
                errors.push(BatchItemError {
                    item_index: i as u32,
                    error_code: ERR_INTERNAL,
                    error_data: vec![],
                });
            }
        }
    }

    write_redo_ops(redo_log, &redo_ops);
    replicate_all_ops(cluster, repl_ops_by_key);

    batch_response(req.request_id, &errors)
}

// ---------------------------------------------------------------------------
// Freeze / Unfreeze / Delete / SetLocked / etc — simple dispatch
// ---------------------------------------------------------------------------

fn handle_freeze_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    let items = match decode_slot_item_batch(&req.payload) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed"),
    };
    if items.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }
    let mut errors = Vec::new();
    let mut redo_ops: Vec<RedoOp> = Vec::new();
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();

    for (i, item) in items.iter().enumerate() {
        if let Some(redirect_err) = check_shard_ownership(&item.txid, i as u32, cluster, false) {
            errors.push(redirect_err);
            continue;
        }
        let key = TxKey { txid: item.txid };
        match engine.freeze(&FreezeRequest {
            tx_key: key,
            offset: item.vout,
            utxo_hash: item.utxo_hash,
        }) {
            Ok(()) => {
                redo_ops.push(RedoOp::Freeze { tx_key: key, offset: item.vout });
                repl_ops_by_key.push((key, vec![ReplicaOp::Freeze {
                    tx_key: key,
                    offset: item.vout,
                }]));
            }
            Err(err) => {
                errors.push(spend_error_to_batch_error(i as u32, &err));
            }
        }
    }

    write_redo_ops(redo_log, &redo_ops);
    replicate_all_ops(cluster, repl_ops_by_key);

    batch_response(req.request_id, &errors)
}

fn handle_unfreeze_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    let items = match decode_slot_item_batch(&req.payload) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed"),
    };
    if items.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }
    let mut errors = Vec::new();
    let mut redo_ops: Vec<RedoOp> = Vec::new();
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();

    for (i, item) in items.iter().enumerate() {
        if let Some(redirect_err) = check_shard_ownership(&item.txid, i as u32, cluster, false) {
            errors.push(redirect_err);
            continue;
        }
        let key = TxKey { txid: item.txid };
        match engine.unfreeze(&UnfreezeRequest {
            tx_key: key,
            offset: item.vout,
            utxo_hash: item.utxo_hash,
        }) {
            Ok(()) => {
                redo_ops.push(RedoOp::Unfreeze { tx_key: key, offset: item.vout });
                repl_ops_by_key.push((key, vec![ReplicaOp::Unfreeze {
                    tx_key: key,
                    offset: item.vout,
                }]));
            }
            Err(err) => {
                errors.push(spend_error_to_batch_error(i as u32, &err));
            }
        }
    }

    write_redo_ops(redo_log, &redo_ops);
    replicate_all_ops(cluster, repl_ops_by_key);

    batch_response(req.request_id, &errors)
}

fn handle_reassign_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    let (params, items) = match decode_reassign_batch(&req.payload) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed"),
    };
    if items.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }
    let mut errors = Vec::new();
    let mut redo_ops: Vec<RedoOp> = Vec::new();
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();

    for (i, item) in items.iter().enumerate() {
        if let Some(redirect_err) = check_shard_ownership(&item.txid, i as u32, cluster, false) {
            errors.push(redirect_err);
            continue;
        }
        let key = TxKey { txid: item.txid };
        match engine.reassign(&ReassignRequest {
            tx_key: key,
            offset: item.vout,
            utxo_hash: item.utxo_hash,
            new_utxo_hash: item.new_utxo_hash,
            block_height: params.block_height,
            spendable_after: params.spendable_after,
        }) {
            Ok(()) => {
                redo_ops.push(RedoOp::Reassign {
                    tx_key: key,
                    offset: item.vout,
                    new_hash: item.new_utxo_hash,
                    block_height: params.block_height,
                    spendable_after: params.spendable_after,
                });
                repl_ops_by_key.push((key, vec![ReplicaOp::Reassign {
                    tx_key: key,
                    offset: item.vout,
                    new_hash: item.new_utxo_hash,
                    block_height: params.block_height,
                    spendable_after: params.spendable_after,
                }]));
            }
            Err(err) => {
                errors.push(spend_error_to_batch_error(i as u32, &err));
            }
        }
    }

    write_redo_ops(redo_log, &redo_ops);
    replicate_all_ops(cluster, repl_ops_by_key);

    batch_response(req.request_id, &errors)
}

fn handle_set_conflicting_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    let (shared, txids) = match decode_txid_batch(&req.payload, 9) { // value(1) + cbh(4) + bhr(4)
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed"),
    };
    if txids.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }
    let value = shared[0] != 0;
    let cbh = u32::from_le_bytes(shared[1..5].try_into().unwrap());
    let bhr = u32::from_le_bytes(shared[5..9].try_into().unwrap());

    let mut errors = Vec::new();
    let mut redo_ops: Vec<RedoOp> = Vec::new();
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();

    for (i, txid) in txids.iter().enumerate() {
        if let Some(redirect_err) = check_shard_ownership(txid, i as u32, cluster, false) {
            errors.push(redirect_err);
            continue;
        }
        let key = TxKey { txid: *txid };
        match engine.set_conflicting(&SetConflictingRequest {
            tx_key: key,
            value,
            current_block_height: cbh,
            block_height_retention: bhr,
        }) {
            Ok(_) => {
                redo_ops.push(RedoOp::SetConflicting {
                    tx_key: key,
                    value,
                    current_block_height: cbh,
                    block_height_retention: bhr,
                });
                repl_ops_by_key.push((key, vec![ReplicaOp::SetConflicting {
                    tx_key: key,
                    value,
                    current_block_height: cbh,
                    retention: bhr,
                }]));
            }
            Err(err) => {
                errors.push(spend_error_to_batch_error(i as u32, &err));
            }
        }
    }

    write_redo_ops(redo_log, &redo_ops);
    replicate_all_ops(cluster, repl_ops_by_key);

    batch_response(req.request_id, &errors)
}

fn handle_set_locked_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    let (shared, txids) = match decode_txid_batch(&req.payload, 1) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed"),
    };
    if txids.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }
    let value = shared[0] != 0;

    let mut errors = Vec::new();
    let mut redo_ops: Vec<RedoOp> = Vec::new();
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();

    for (i, txid) in txids.iter().enumerate() {
        if let Some(redirect_err) = check_shard_ownership(txid, i as u32, cluster, false) {
            errors.push(redirect_err);
            continue;
        }
        let key = TxKey { txid: *txid };
        match engine.set_locked(&SetLockedRequest {
            tx_key: key,
            value,
        }) {
            Ok(()) => {
                redo_ops.push(RedoOp::SetLocked { tx_key: key, value });
                repl_ops_by_key.push((key, vec![ReplicaOp::SetLocked {
                    tx_key: key,
                    value,
                }]));
            }
            Err(err) => {
                errors.push(spend_error_to_batch_error(i as u32, &err));
            }
        }
    }

    write_redo_ops(redo_log, &redo_ops);
    replicate_all_ops(cluster, repl_ops_by_key);

    batch_response(req.request_id, &errors)
}

fn handle_preserve_until_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    let (shared, txids) = match decode_txid_batch(&req.payload, 4) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed"),
    };
    if txids.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }
    let height = u32::from_le_bytes(shared[0..4].try_into().unwrap());

    let mut errors = Vec::new();
    let mut redo_ops: Vec<RedoOp> = Vec::new();
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();

    for (i, txid) in txids.iter().enumerate() {
        if let Some(redirect_err) = check_shard_ownership(txid, i as u32, cluster, false) {
            errors.push(redirect_err);
            continue;
        }
        let key = TxKey { txid: *txid };
        match engine.preserve_until(&PreserveUntilRequest {
            tx_key: key,
            block_height: height,
        }) {
            Ok(_) => {
                redo_ops.push(RedoOp::PreserveUntil {
                    tx_key: key,
                    block_height: height,
                });
                repl_ops_by_key.push((key, vec![ReplicaOp::PreserveUntil {
                    tx_key: key,
                    block_height: height,
                }]));
            }
            Err(err) => {
                errors.push(spend_error_to_batch_error(i as u32, &err));
            }
        }
    }

    write_redo_ops(redo_log, &redo_ops);
    replicate_all_ops(cluster, repl_ops_by_key);

    batch_response(req.request_id, &errors)
}

fn handle_delete_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    let (_, txids) = match decode_txid_batch(&req.payload, 0) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed"),
    };
    if txids.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }
    let mut errors = Vec::new();
    let mut redo_ops: Vec<RedoOp> = Vec::new();
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();

    for (i, txid) in txids.iter().enumerate() {
        if let Some(redirect_err) = check_shard_ownership(txid, i as u32, cluster, false) {
            errors.push(redirect_err);
            continue;
        }
        let key = TxKey { txid: *txid };
        // Look up the record offset before deletion for the redo log
        let entry_info = engine.lookup(&key);
        match engine.delete(&DeleteRequest { tx_key: key }) {
            Ok(()) => {
                let (record_offset, record_size) = match entry_info {
                    Some(e) => (e.record_offset, 0u64), // Size not tracked in index; use 0
                    None => (0, 0),
                };
                redo_ops.push(RedoOp::Delete {
                    tx_key: key,
                    record_offset,
                    record_size,
                });
                repl_ops_by_key.push((key, vec![ReplicaOp::Delete { tx_key: key }]));
            }
            Err(err) => {
                errors.push(spend_error_to_batch_error(i as u32, &err));
            }
        }
    }

    write_redo_ops(redo_log, &redo_ops);
    replicate_all_ops(cluster, repl_ops_by_key);

    batch_response(req.request_id, &errors)
}

fn handle_mark_longest_chain_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    let (shared, txids) = match decode_txid_batch(&req.payload, 9) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed"),
    };
    if txids.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }
    let on_longest_chain = shared[0] != 0;
    let cbh = u32::from_le_bytes(shared[1..5].try_into().unwrap());
    let bhr = u32::from_le_bytes(shared[5..9].try_into().unwrap());

    let mut errors = Vec::new();
    let mut redo_ops: Vec<RedoOp> = Vec::new();

    for (i, txid) in txids.iter().enumerate() {
        if let Some(redirect_err) = check_shard_ownership(txid, i as u32, cluster, false) {
            errors.push(redirect_err);
            continue;
        }
        let key = TxKey { txid: *txid };
        match engine.mark_on_longest_chain(&MarkOnLongestChainRequest {
            tx_key: key,
            on_longest_chain,
            current_block_height: cbh,
            block_height_retention: bhr,
        }) {
            Ok(_) => {
                redo_ops.push(RedoOp::MarkOnLongestChain {
                    tx_key: key,
                    on_longest_chain,
                    current_block_height: cbh,
                    block_height_retention: bhr,
                });
                // MarkOnLongestChain is metadata-only; no dedicated ReplicaOp
                // needed — the SetMined replication already covers block tracking.
            }
            Err(err) => {
                errors.push(spend_error_to_batch_error(i as u32, &err));
            }
        }
    }

    write_redo_ops(redo_log, &redo_ops);

    batch_response(req.request_id, &errors)
}

// ---------------------------------------------------------------------------
// GetBatch
// ---------------------------------------------------------------------------

fn handle_get_batch(req: &RequestFrame, engine: &Engine, max_batch: u32, cluster: Option<&RunningCluster>) -> ResponseFrame {
    let (field_mask, txids) = match decode_get_batch(&req.payload) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed get batch"),
    };
    if txids.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }

    let local_read = req.flags & FLAG_LOCAL_READ != 0;

    let mut results = Vec::with_capacity(txids.len());
    for txid in &txids {
        let key = TxKey { txid: *txid };

        // In cluster mode, serve reads if we're master OR if the record is
        // available locally (handles the migration window where shard tables
        // may be inconsistent across nodes).
        if !local_read
            && let Some(cluster) = cluster
        {
            let is_master = cluster.is_master(&key);
            let is_migrating_out = cluster.is_migrating_outbound(&key);

            if !is_master && !is_migrating_out {
                // Not master — check if we have it locally before rejecting.
                if engine.read_metadata(&key).is_err() {
                    results.push(WireGetResult { status: 1, data: vec![] });
                    continue;
                }
                // We have it locally — serve it despite not being master.
            }

            // If we're master but don't have the data, and there's a pending
            // inbound migration for this shard, wait briefly for it to arrive.
            if is_master && engine.read_metadata(&key).is_err() && cluster.has_pending_inbound(&key) {
                let mut found = false;
                for _ in 0..50 { // up to 5 seconds (50 * 100ms)
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    if engine.read_metadata(&key).is_ok() {
                        found = true;
                        break;
                    }
                    if !cluster.has_pending_inbound(&key) {
                        break; // migration finished but data didn't include this record
                    }
                }
                if !found {
                    // Return migration-in-progress instead of NotFound so
                    // clients know to retry rather than treating the record
                    // as permanently absent.
                    results.push(WireGetResult { status: ERR_MIGRATION_IN_PROGRESS as u8, data: vec![] });
                    continue;
                }
            }
        }
        match engine.read_metadata(&key) {
            Ok(meta) => {
                let mut data = Vec::new();
                if field_mask.has(FieldMask::RAW_METADATA) {
                    // Raw debug mode: dump the full on-disk struct as-is.
                    let mut buf = vec![0u8; METADATA_SIZE];
                    meta.to_bytes(&mut buf);
                    data.extend_from_slice(&buf);
                } else {
                    // Per-field metadata serialization.
                    if field_mask.has(FieldMask::TX_VERSION)       { data.extend_from_slice(&{ meta.tx_version }.to_le_bytes()); }
                    if field_mask.has(FieldMask::LOCKTIME)          { data.extend_from_slice(&{ meta.locktime }.to_le_bytes()); }
                    if field_mask.has(FieldMask::FEE)               { data.extend_from_slice(&{ meta.fee }.to_le_bytes()); }
                    if field_mask.has(FieldMask::SIZE_IN_BYTES)     { data.extend_from_slice(&{ meta.size_in_bytes }.to_le_bytes()); }
                    if field_mask.has(FieldMask::EXTENDED_SIZE)      { data.extend_from_slice(&{ meta.extended_size }.to_le_bytes()); }
                    if field_mask.has(FieldMask::FLAGS)              { data.push({ meta.flags }.bits()); }
                    if field_mask.has(FieldMask::SPENDING_HEIGHT)   { data.extend_from_slice(&{ meta.spending_height }.to_le_bytes()); }
                    if field_mask.has(FieldMask::CREATED_AT)        { data.extend_from_slice(&{ meta.created_at }.to_le_bytes()); }
                    if field_mask.has(FieldMask::SPENT_UTXOS)       { data.extend_from_slice(&{ meta.spent_utxos }.to_le_bytes()); }
                    if field_mask.has(FieldMask::PRUNED_UTXOS)      { data.extend_from_slice(&{ meta.pruned_utxos }.to_le_bytes()); }
                    if field_mask.has(FieldMask::UTXO_COUNT)        { data.extend_from_slice(&{ meta.utxo_count }.to_le_bytes()); }
                    if field_mask.has(FieldMask::GENERATION)        { data.extend_from_slice(&{ meta.generation }.to_le_bytes()); }
                    if field_mask.has(FieldMask::UPDATED_AT)        { data.extend_from_slice(&{ meta.updated_at }.to_le_bytes()); }
                    if field_mask.has(FieldMask::UNMINED_SINCE)      { data.extend_from_slice(&{ meta.unmined_since }.to_le_bytes()); }
                    if field_mask.has(FieldMask::DELETE_AT_HEIGHT)  { data.extend_from_slice(&{ meta.delete_at_height }.to_le_bytes()); }
                    if field_mask.has(FieldMask::PRESERVE_UNTIL)    { data.extend_from_slice(&{ meta.preserve_until }.to_le_bytes()); }
                    if field_mask.has(FieldMask::EXTERNAL_REF) {
                        let ext = { meta.external_ref };
                        data.push(ext.store_type);
                        data.extend_from_slice(&ext.content_hash);
                        data.extend_from_slice(&ext.total_size.to_le_bytes());
                        data.extend_from_slice(&ext.input_count.to_le_bytes());
                        data.extend_from_slice(&ext.output_count.to_le_bytes());
                        data.extend_from_slice(&ext.inputs_offset.to_le_bytes());
                        data.extend_from_slice(&ext.outputs_offset.to_le_bytes());
                    }
                    if field_mask.has(FieldMask::REASSIGNMENT_COUNT) { data.push(meta.reassignment_count); }
                    if field_mask.has(FieldMask::BLOCK_ENTRY_COUNT)  { data.push(meta.block_entry_count); }
                }
                if field_mask.has(FieldMask::UTXO_SLOTS) {
                    let utxo_count = { meta.utxo_count };
                    data.extend_from_slice(&utxo_count.to_le_bytes());
                    for v in 0..utxo_count {
                        match engine.read_slot(&key, v) {
                            Ok(slot) => {
                                data.extend_from_slice(&slot.hash);
                                data.push(slot.status);
                                data.extend_from_slice(&slot.spending_data);
                            }
                            Err(_) => {
                                // Slot read error — fill with zeros
                                data.extend_from_slice(&[0u8; 69]);
                            }
                        }
                    }
                }
                if field_mask.has(FieldMask::COLD_DATA) {
                    match engine.read_cold_data(&key) {
                        Ok(cold) => {
                            data.extend_from_slice(&(cold.len() as u32).to_le_bytes());
                            data.extend_from_slice(&cold);
                        }
                        Err(_) => {
                            data.extend_from_slice(&0u32.to_le_bytes());
                        }
                    }
                }
                if field_mask.has(FieldMask::BLOCK_ENTRIES) {
                    let count = { meta.block_entry_count };
                    data.push(count);
                    let inline_count = count.min(3);
                    for i in 0..inline_count as usize {
                        let be = { meta.block_entries_inline[i] };
                        data.extend_from_slice(&{ be.block_id }.to_le_bytes());
                        data.extend_from_slice(&{ be.block_height }.to_le_bytes());
                        data.extend_from_slice(&{ be.subtree_idx }.to_le_bytes());
                    }
                }
                if field_mask.has(FieldMask::CONFLICTING_CHILDREN) {
                    match engine.read_conflicting_children(&key) {
                        Ok(children) => {
                            data.push(children.len() as u8);
                            for child in &children {
                                data.extend_from_slice(child);
                            }
                        }
                        Err(_) => {
                            data.push(0u8);
                        }
                    }
                }
                results.push(WireGetResult { status: 0, data });
            }
            Err(SpendError::TxNotFound) => {
                results.push(WireGetResult { status: 1, data: vec![] });
            }
            Err(_) => {
                results.push(WireGetResult { status: 1, data: vec![] });
            }
        }
    }

    ResponseFrame {
        request_id: req.request_id,
        status: STATUS_OK,
        payload: encode_get_response(&results),
    }
}

// ---------------------------------------------------------------------------
// Pruner operations
// ---------------------------------------------------------------------------

fn handle_query_old_unmined(req: &RequestFrame, engine: &Engine) -> ResponseFrame {
    // Payload: [cutoff_height:4]
    if req.payload.len() < 4 {
        return error_response(req.request_id, ERR_INTERNAL, "malformed query");
    }
    let cutoff = u32::from_le_bytes(req.payload[0..4].try_into().unwrap());
    let keys = engine.unmined_index().range_query(cutoff);

    let mut payload = Vec::with_capacity(4 + keys.len() * 32);
    payload.extend_from_slice(&(keys.len() as u32).to_le_bytes());
    for key in &keys {
        payload.extend_from_slice(&key.txid);
    }

    ResponseFrame {
        request_id: req.request_id,
        status: STATUS_OK,
        payload,
    }
}

fn handle_preserve_transactions(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    // Same format as PreserveUntilBatch: [count:4][block_height:4][txids]
    let (shared, txids) = match decode_txid_batch(&req.payload, 4) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed"),
    };
    if txids.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }
    let height = u32::from_le_bytes(shared[0..4].try_into().unwrap());

    let mut errors = Vec::new();
    let mut redo_ops: Vec<RedoOp> = Vec::new();
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();

    for (i, txid) in txids.iter().enumerate() {
        if let Some(redirect_err) = check_shard_ownership(txid, i as u32, cluster, false) {
            errors.push(redirect_err);
            continue;
        }
        let key = TxKey { txid: *txid };
        match engine.preserve_until(&PreserveUntilRequest {
            tx_key: key,
            block_height: height,
        }) {
            Ok(_) => {
                redo_ops.push(RedoOp::PreserveUntil {
                    tx_key: key,
                    block_height: height,
                });
                repl_ops_by_key.push((key, vec![ReplicaOp::PreserveUntil {
                    tx_key: key,
                    block_height: height,
                }]));
            }
            Err(err) => {
                errors.push(spend_error_to_batch_error(i as u32, &err));
            }
        }
    }

    write_redo_ops(redo_log, &redo_ops);
    replicate_all_ops(cluster, repl_ops_by_key);

    batch_response(req.request_id, &errors)
}

fn handle_process_expired(
    req: &RequestFrame,
    engine: &Engine,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    // Payload: [current_height:4]
    if req.payload.len() < 4 {
        return error_response(req.request_id, ERR_INTERNAL, "malformed");
    }
    let current_height = u32::from_le_bytes(req.payload[0..4].try_into().unwrap());

    // Query DAH index for transactions due for deletion
    let keys = engine.dah_index().range_query(current_height);
    let mut deleted = 0u32;
    let mut failed = 0u32;
    let mut redo_ops: Vec<RedoOp> = Vec::new();

    for key in &keys {
        let entry_info = engine.lookup(key);
        match engine.delete(&DeleteRequest { tx_key: *key }) {
            Ok(()) => {
                deleted += 1;
                let (record_offset, record_size) = match entry_info {
                    Some(e) => (e.record_offset, 0u64),
                    None => (0, 0),
                };
                redo_ops.push(RedoOp::Delete {
                    tx_key: *key,
                    record_offset,
                    record_size,
                });
            }
            Err(_) => failed += 1,
        }
    }

    write_redo_ops(redo_log, &redo_ops);

    let mut payload = Vec::with_capacity(8);
    payload.extend_from_slice(&deleted.to_le_bytes());
    payload.extend_from_slice(&failed.to_le_bytes());

    ResponseFrame {
        request_id: req.request_id,
        status: STATUS_OK,
        payload,
    }
}

// ---------------------------------------------------------------------------
// GetSpend
// ---------------------------------------------------------------------------

fn handle_get_spend_batch(req: &RequestFrame, engine: &Engine, max_batch: u32, cluster: Option<&RunningCluster>) -> ResponseFrame {
    let items = match decode_get_spend_batch(&req.payload) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed"),
    };
    if items.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }

    let local_read = req.flags & FLAG_LOCAL_READ != 0;

    let mut results = Vec::with_capacity(items.len());
    for item in &items {
        // Check shard ownership — reads are allowed during outbound migration
        // because this node still holds the data until migration completes.
        // FLAG_LOCAL_READ bypasses this check for replication verification.
        if !local_read
            && let Some(cluster) = cluster
        {
            let key = TxKey { txid: item.txid };
            if !cluster.is_master(&key) && !cluster.is_migrating_outbound(&key) {
                results.push(WireGetSpendResult {
                    status: 1,
                    error_code: ERR_REDIRECT,
                    slot_status: 0,
                    spending_data: [0; 36],
                });
                continue;
            }
        }

        // GetSpend needs the utxo_hash for validation. Since the wire format
        // only sends txid+vout, we skip hash validation at this level and
        // return whatever is at that slot offset.
        let key = TxKey { txid: item.txid };
        match engine.read_metadata(&key) {
            Ok(meta) => {
                let utxo_count = { meta.utxo_count };
                if item.vout >= utxo_count {
                    results.push(WireGetSpendResult {
                        status: 1,
                        error_code: ERR_VOUT_OUT_OF_RANGE,
                        slot_status: 0,
                        spending_data: [0; 36],
                    });
                } else {
                    match engine.read_slot(&key, item.vout) {
                        Ok(slot) => {
                            results.push(WireGetSpendResult {
                                status: 0,
                                error_code: ERR_OK,
                                slot_status: slot.status,
                                spending_data: slot.spending_data,
                            });
                        }
                        Err(_) => {
                            results.push(WireGetSpendResult {
                                status: 1,
                                error_code: ERR_INTERNAL,
                                slot_status: 0,
                                spending_data: [0; 36],
                            });
                        }
                    }
                }
            }
            Err(SpendError::TxNotFound) => {
                results.push(WireGetSpendResult {
                    status: 1,
                    error_code: ERR_TX_NOT_FOUND,
                    slot_status: 0,
                    spending_data: [0; 36],
                });
            }
            Err(_) => {
                results.push(WireGetSpendResult {
                    status: 1,
                    error_code: ERR_INTERNAL,
                    slot_status: 0,
                    spending_data: [0; 36],
                });
            }
        }
    }

    ResponseFrame {
        request_id: req.request_id,
        status: STATUS_OK,
        payload: encode_get_spend_response(&results),
    }
}

// ---------------------------------------------------------------------------
// Streaming blob upload
// ---------------------------------------------------------------------------

/// Handle a single chunk of a streaming blob upload.
///
/// Looks up or creates an active stream session for the given txid in the
/// per-connection state. Validates the chunk offset matches the expected
/// position (no gaps or overlaps). On write error the stream is aborted and
/// removed from the connection state.
fn handle_stream_chunk(
    req: &RequestFrame,
    conn_state: &mut super::ConnectionState,
    blob_store: Option<&dyn BlobStore>,
    cluster: Option<&RunningCluster>,
) -> ResponseFrame {
    let chunk = match decode_stream_chunk(&req.payload) {
        Some(c) => c,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed stream chunk"),
    };

    // Check shard ownership — streaming writes are mutations on the master.
    if let Some(redirect_err) = check_shard_ownership(&chunk.txid, 0, cluster, false) {
        return error_response(req.request_id, redirect_err.error_code, "shard not owned");
    }

    let blob_store = match blob_store {
        Some(bs) => bs,
        None => return error_response(req.request_id, ERR_INTERNAL, "blobstore not configured"),
    };

    // Get or create the stream session for this txid.
    if !conn_state.streams.contains_key(&chunk.txid) {
        match blob_store.begin_stream(&chunk.txid) {
            Ok(writer) => {
                conn_state.streams.insert(chunk.txid, super::ActiveStream {
                    writer,
                    bytes_received: 0,
                });
            }
            Err(e) => return error_response(req.request_id, ERR_INTERNAL, &format!("begin_stream: {e}")),
        }
    }

    let stream = conn_state.streams.get_mut(&chunk.txid).expect("just inserted");

    // Verify chunk offset matches expected position.
    if chunk.offset != stream.bytes_received {
        return error_response(
            req.request_id,
            ERR_STREAM_OFFSET_MISMATCH,
            &format!("expected offset {}, got {}", stream.bytes_received, chunk.offset),
        );
    }

    // Write the chunk data.
    if let Err(e) = stream.writer.write_chunk(chunk.data) {
        // Abort the stream on write error.
        if let Some(s) = conn_state.streams.remove(&chunk.txid) {
            let _ = s.writer.abort();
        }
        return error_response(req.request_id, ERR_INTERNAL, &format!("write_chunk: {e}"));
    }

    stream.bytes_received += chunk.data.len() as u64;

    ResponseFrame {
        request_id: req.request_id,
        status: STATUS_OK,
        payload: vec![],
    }
}

/// Finalize a streaming blob upload.
///
/// Removes the active stream session from the connection state, verifies
/// the total bytes received match the declared total, and calls `finish`
/// on the blob stream writer to atomically commit the blob.
fn handle_stream_end(
    req: &RequestFrame,
    conn_state: &mut super::ConnectionState,
) -> ResponseFrame {
    let end = match decode_stream_end(&req.payload) {
        Some(e) => e,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed stream end"),
    };

    let stream = match conn_state.streams.remove(&end.txid) {
        Some(s) => s,
        None => return error_response(req.request_id, ERR_STREAM_NOT_FOUND, "no active stream for txid"),
    };

    // Verify total size matches what was received.
    if stream.bytes_received != end.total_size {
        let _ = stream.writer.abort();
        return error_response(
            req.request_id,
            ERR_INTERNAL,
            &format!("size mismatch: received {} bytes, expected {}", stream.bytes_received, end.total_size),
        );
    }

    // Finalize the blob — makes it available for reads.
    match stream.writer.finish() {
        Ok(_total) => ResponseFrame {
            request_id: req.request_id,
            status: STATUS_OK,
            payload: vec![],
        },
        Err(e) => error_response(req.request_id, ERR_INTERNAL, &format!("finish: {e}")),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn error_response(request_id: u64, code: u16, msg: &str) -> ResponseFrame {
    let mut payload = Vec::new();
    payload.extend_from_slice(&code.to_le_bytes());
    payload.extend_from_slice(&(msg.len() as u16).to_le_bytes());
    payload.extend_from_slice(msg.as_bytes());
    ResponseFrame {
        request_id,
        status: STATUS_ERROR,
        payload,
    }
}

fn batch_response(request_id: u64, errors: &[BatchItemError]) -> ResponseFrame {
    if errors.is_empty() {
        ResponseFrame {
            request_id,
            status: STATUS_OK,
            payload: vec![],
        }
    } else {
        ResponseFrame {
            request_id,
            status: STATUS_PARTIAL_ERROR,
            payload: encode_sparse_errors(errors),
        }
    }
}

fn spend_error_to_batch_error(item_index: u32, err: &SpendError) -> BatchItemError {
    let (code, data) = match err {
        SpendError::TxNotFound => (ERR_TX_NOT_FOUND, vec![]),
        SpendError::Conflicting => (ERR_CONFLICTING, vec![]),
        SpendError::Locked => (ERR_LOCKED, vec![]),
        SpendError::CoinbaseImmature { spending_height, .. } => {
            (ERR_COINBASE_IMMATURE, spending_height.to_le_bytes().to_vec())
        }
        SpendError::UtxoNotFound { .. } => (ERR_VOUT_OUT_OF_RANGE, vec![]),
        SpendError::UtxoHashMismatch { .. } => (ERR_UTXO_HASH_MISMATCH, vec![]),
        SpendError::AlreadySpent { spending_data, .. } => {
            (ERR_ALREADY_SPENT, spending_data.to_vec())
        }
        SpendError::Frozen { .. } => (ERR_FROZEN, vec![]),
        SpendError::FrozenUntil { .. } => (ERR_FROZEN_UNTIL, vec![]),
        SpendError::InvalidSpend { spending_data, .. } => {
            (ERR_INVALID_SPEND, spending_data.to_vec())
        }
        SpendError::Pruned { .. } => (ERR_INVALID_SPEND, vec![]),
        SpendError::AlreadyFrozen { .. } => (ERR_ALREADY_FROZEN, vec![]),
        SpendError::NotFrozen { .. } => (ERR_UTXO_NOT_FROZEN, vec![]),
        SpendError::StorageError { .. } => (ERR_INTERNAL, vec![]),
    };
    BatchItemError { item_index, error_code: code, error_data: data }
}


// ---------------------------------------------------------------------------
// Partition map
// ---------------------------------------------------------------------------

fn handle_get_partition_map(
    req: &RequestFrame,
    cluster: Option<&RunningCluster>,
) -> ResponseFrame {
    match cluster {
        Some(c) => ResponseFrame {
            request_id: req.request_id,
            status: STATUS_OK,
            payload: c.encode_partition_map(),
        },
        None => {
            // Single-node mode: return a trivial partition map
            let mut payload = Vec::new();
            payload.extend_from_slice(&0u64.to_le_bytes()); // version = 0
            payload.extend_from_slice(&1u32.to_le_bytes()); // 1 node
            payload.extend_from_slice(&0u64.to_le_bytes()); // node_id = 0
            let addr = b"127.0.0.1:3300";
            payload.extend_from_slice(&(addr.len() as u16).to_le_bytes());
            payload.extend_from_slice(addr);
            // All 4096 shards map to node 0
            for _ in 0..4096u16 {
                payload.extend_from_slice(&0u64.to_le_bytes());
            }
            ResponseFrame {
                request_id: req.request_id,
                status: STATUS_OK,
                payload,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests — Layer 1 dispatch tests (no TCP, no Docker)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::SlotAllocator;
    use crate::device::{BlockDevice, MemoryDevice};
    use crate::index::{DahIndex, Index, UnminedIndex};
    use crate::locks::StripedLocks;
    use crate::ops::engine::Engine;
    use std::sync::Arc;

    /// Test harness for Layer 1 dispatch testing.
    ///
    /// Creates an in-memory Engine with no network, no Docker. Tests handler
    /// logic directly by calling `handle_request()`.
    struct DispatchTestHarness {
        engine: Engine,
    }

    impl DispatchTestHarness {
        /// Create a new harness with a 64 MB in-memory device.
        fn new() -> Self {
            let dev: Arc<dyn BlockDevice> =
                Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
            let alloc = SlotAllocator::new(dev.clone());
            let index = Index::new(10000).unwrap();
            let locks = StripedLocks::new(1024);
            let dah = DahIndex::new();
            let unmined = UnminedIndex::new();
            let engine = Engine::new(dev, index, alloc, locks, dah, unmined);
            Self { engine }
        }

        /// Dispatch a request and return the response.
        fn request(&self, op_code: u16, payload: Vec<u8>) -> ResponseFrame {
            self.request_with_max_batch(op_code, payload, 8192)
        }

        /// Dispatch a request with a custom max_batch_size.
        fn request_with_max_batch(
            &self,
            op_code: u16,
            payload: Vec<u8>,
            max_batch_size: u32,
        ) -> ResponseFrame {
            let req = RequestFrame {
                request_id: 1,
                op_code,
                flags: 0,
                payload,
            };
            let mut conn_state = crate::server::ConnectionState::new();
            handle_request(&req, &self.engine, max_batch_size, None, None, &mut conn_state, None)
        }

        /// Create a single transaction with the given utxo_count via OP_CREATE_BATCH.
        fn create_tx(&self, txid: [u8; 32], utxo_count: u32) -> ResponseFrame {
            let hashes: Vec<[u8; 32]> = (0..utxo_count)
                .map(|i| {
                    let mut h = [0u8; 32];
                    h[0] = (i & 0xFF) as u8;
                    h[1] = ((i >> 8) & 0xFF) as u8;
                    h
                })
                .collect();

            let item = WireCreateItem {
                txid,
                tx_version: 1,
                locktime: 0,
                fee: 500,
                size_in_bytes: 250,
                extended_size: 250,
                is_coinbase: false,
                spending_height: 0,
                created_at: 1700000000000,
                flags: 0,
                utxo_hashes: hashes,
                cold_data: vec![],
                block_height: 0,
                mined_block_id: None,
                mined_block_height: None,
                mined_subtree_idx: None,
                parent_txids: vec![],
            };
            let payload = encode_create_batch(&[item]);
            self.request(OP_CREATE_BATCH, payload)
        }

        /// Generate a deterministic txid from a byte value.
        fn make_txid(n: u8) -> [u8; 32] {
            let mut txid = [0u8; 32];
            txid[0] = n;
            txid[31] = n.wrapping_mul(7); // mix a second byte to reduce collisions
            txid
        }
    }

    // -----------------------------------------------------------------------
    // 1a. handle_query_old_unmined — matching txids returned
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_query_old_unmined_returns_matching_txids() {
        let h = DispatchTestHarness::new();
        let txid_a = DispatchTestHarness::make_txid(1);
        let txid_b = DispatchTestHarness::make_txid(2);
        let txid_c = DispatchTestHarness::make_txid(3);

        // Create 3 txs
        assert_eq!(h.create_tx(txid_a, 2).status, STATUS_OK);
        assert_eq!(h.create_tx(txid_b, 2).status, STATUS_OK);
        assert_eq!(h.create_tx(txid_c, 2).status, STATUS_OK);

        // Manually insert into unmined index at different heights
        {
            let mut ui = h.engine.unmined_index();
            ui.insert(100, TxKey { txid: txid_a });
            ui.insert(200, TxKey { txid: txid_b });
            ui.insert(300, TxKey { txid: txid_c });
        }

        // Query with cutoff_height=200 — should return txid_a (100) and txid_b (200)
        let mut payload = Vec::new();
        payload.extend_from_slice(&200u32.to_le_bytes());
        let resp = h.request(OP_QUERY_OLD_UNMINED, payload);
        assert_eq!(resp.status, STATUS_OK);

        // Parse response: [count:4][txids × count]
        let count = u32::from_le_bytes(resp.payload[0..4].try_into().unwrap());
        assert_eq!(count, 2);

        let mut returned_txids: Vec<[u8; 32]> = Vec::new();
        for i in 0..count as usize {
            let start = 4 + i * 32;
            let mut txid = [0u8; 32];
            txid.copy_from_slice(&resp.payload[start..start + 32]);
            returned_txids.push(txid);
        }
        assert!(returned_txids.contains(&txid_a));
        assert!(returned_txids.contains(&txid_b));
        assert!(!returned_txids.contains(&txid_c));
    }

    // -----------------------------------------------------------------------
    // 1b. handle_query_old_unmined — malformed payload
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_query_old_unmined_malformed_payload() {
        let h = DispatchTestHarness::new();
        let resp = h.request(OP_QUERY_OLD_UNMINED, vec![0xAA, 0xBB]);
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_INTERNAL);
        assert!(msg.contains("malformed"), "expected 'malformed' in: {msg}");
    }

    // -----------------------------------------------------------------------
    // 1c. handle_preserve_transactions — preserves records
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_preserve_transactions_preserves_records() {
        let h = DispatchTestHarness::new();
        let txid_a = DispatchTestHarness::make_txid(10);
        let txid_b = DispatchTestHarness::make_txid(11);
        let txid_c = DispatchTestHarness::make_txid(12);

        assert_eq!(h.create_tx(txid_a, 1).status, STATUS_OK);
        assert_eq!(h.create_tx(txid_b, 1).status, STATUS_OK);
        assert_eq!(h.create_tx(txid_c, 1).status, STATUS_OK);

        // Send OP_PRESERVE_TRANSACTIONS with height=1000
        let preserve_height: u32 = 1000;
        let mut shared = Vec::new();
        shared.extend_from_slice(&preserve_height.to_le_bytes());
        let payload = encode_txid_batch(&[txid_a, txid_b, txid_c], &shared);
        let resp = h.request(OP_PRESERVE_TRANSACTIONS, payload);
        assert_eq!(resp.status, STATUS_OK);

        // Read back each tx and verify preserve_until is set
        for txid in &[txid_a, txid_b, txid_c] {
            let key = TxKey { txid: *txid };
            let meta = h.engine.read_metadata(&key).unwrap();
            assert_eq!(
                { meta.preserve_until },
                1000,
                "preserve_until should be 1000 for txid starting with {:?}",
                txid[0]
            );
        }
    }

    // -----------------------------------------------------------------------
    // 1d. handle_preserve_transactions — malformed payload
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_preserve_transactions_malformed_payload() {
        let h = DispatchTestHarness::new();
        let resp = h.request(OP_PRESERVE_TRANSACTIONS, vec![]);
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_INTERNAL);
        assert!(msg.contains("malformed"), "expected 'malformed' in: {msg}");
    }

    // -----------------------------------------------------------------------
    // 1e. handle_process_expired — deletes eligible records
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_process_expired_deletes_eligible() {
        let h = DispatchTestHarness::new();
        let txid_a = DispatchTestHarness::make_txid(20);
        let txid_b = DispatchTestHarness::make_txid(21);
        let txid_c = DispatchTestHarness::make_txid(22);

        assert_eq!(h.create_tx(txid_a, 2).status, STATUS_OK);
        assert_eq!(h.create_tx(txid_b, 2).status, STATUS_OK);
        assert_eq!(h.create_tx(txid_c, 2).status, STATUS_OK);

        // Set DAH on txid_a and txid_b by inserting into the DAH index directly
        {
            let mut dah = h.engine.dah_index();
            dah.insert(500, TxKey { txid: txid_a });
            dah.insert(600, TxKey { txid: txid_b });
        }

        // Send OP_PROCESS_EXPIRED_PRESERVATIONS with current_height=700
        // (above both DAH entries)
        let mut payload = Vec::new();
        payload.extend_from_slice(&700u32.to_le_bytes());
        let resp = h.request(OP_PROCESS_EXPIRED_PRESERVATIONS, payload);
        assert_eq!(resp.status, STATUS_OK);
        assert!(resp.payload.len() >= 8);

        let deleted = u32::from_le_bytes(resp.payload[0..4].try_into().unwrap());
        let failed = u32::from_le_bytes(resp.payload[4..8].try_into().unwrap());
        assert_eq!(deleted, 2, "expected 2 deleted");
        assert_eq!(failed, 0, "expected 0 failed");

        // Verify txid_a and txid_b are gone, txid_c still exists
        assert!(h.engine.lookup(&TxKey { txid: txid_a }).is_none());
        assert!(h.engine.lookup(&TxKey { txid: txid_b }).is_none());
        assert!(h.engine.lookup(&TxKey { txid: txid_c }).is_some());
    }

    // -----------------------------------------------------------------------
    // 1f. handle_process_expired — malformed payload
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_process_expired_malformed_payload() {
        let h = DispatchTestHarness::new();
        let resp = h.request(OP_PROCESS_EXPIRED_PRESERVATIONS, vec![]);
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_INTERNAL);
        assert!(msg.contains("malformed"), "expected 'malformed' in: {msg}");
    }

    // -----------------------------------------------------------------------
    // 2g. Quorum failure — cannot construct RunningCluster without threads
    // -----------------------------------------------------------------------

    // Skipped (with explanation, not #[ignore]):
    //
    // `RunningCluster` has private fields and its constructor spawns SWIM
    // protocol and event-loop threads, making it impossible to construct a
    // lightweight mock in unit tests. A proper quorum-failure test requires
    // either:
    //   (a) Extracting a `QuorumChecker` trait from `RunningCluster`, or
    //   (b) Testing via the integration test layer (tests/cluster_tcp.rs).
    //
    // The `check_quorum()` function is thoroughly tested by inspection of
    // its three code paths (no cluster, peak<=1, alive < quorum_needed).

    // -----------------------------------------------------------------------
    // 3h. Unknown opcode returns error
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_unknown_opcode_returns_error() {
        let h = DispatchTestHarness::new();
        // Use opcode 999 which is not defined
        let resp = h.request(999, vec![]);
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_INTERNAL);
        assert!(msg.contains("unknown opcode"), "expected 'unknown opcode' in: {msg}");
    }

    // -----------------------------------------------------------------------
    // 4i. Spend malformed payload
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_spend_malformed_payload() {
        let h = DispatchTestHarness::new();
        let resp = h.request(OP_SPEND_BATCH, vec![0xDE, 0xAD, 0xBE]);
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_INTERNAL);
        assert!(msg.contains("malformed"), "expected 'malformed' in: {msg}");
    }

    // -----------------------------------------------------------------------
    // 4j. Create malformed payload
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_create_malformed_payload() {
        let h = DispatchTestHarness::new();
        let resp = h.request(OP_CREATE_BATCH, vec![0x01, 0x02, 0x03]);
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_INTERNAL);
        assert!(msg.contains("malformed"), "expected 'malformed' in: {msg}");
    }

    // -----------------------------------------------------------------------
    // 4k. Get malformed payload
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_get_malformed_payload() {
        let h = DispatchTestHarness::new();
        let resp = h.request(OP_GET_BATCH, vec![0xAA, 0xBB, 0xCC]);
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_INTERNAL);
        assert!(msg.contains("malformed"), "expected 'malformed' in: {msg}");
    }

    // -----------------------------------------------------------------------
    // 4l. SetMined malformed payload
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_set_mined_malformed_payload() {
        let h = DispatchTestHarness::new();
        let resp = h.request(OP_SET_MINED_BATCH, vec![0x01, 0x02, 0x03]);
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_INTERNAL);
        assert!(msg.contains("malformed"), "expected 'malformed' in: {msg}");
    }

    // -----------------------------------------------------------------------
    // 4m. Delete malformed payload
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_delete_malformed_payload() {
        let h = DispatchTestHarness::new();
        // decode_txid_batch with shared_len=0 requires at least 4 bytes
        let resp = h.request(OP_DELETE_BATCH, vec![0xAA, 0xBB]);
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_INTERNAL);
        assert!(msg.contains("malformed"), "expected 'malformed' in: {msg}");
    }

    // -----------------------------------------------------------------------
    // 5n. Create then Get — all fields round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_create_then_get_all_fields() {
        let h = DispatchTestHarness::new();
        let txid = DispatchTestHarness::make_txid(40);

        // Create 1 tx with 3 UTXOs
        let resp = h.create_tx(txid, 3);
        assert_eq!(resp.status, STATUS_OK);

        // Get it back with ALL_METADATA mask
        let get_payload = encode_get_batch(FieldMask::ALL_METADATA, &[txid]);
        let resp = h.request(OP_GET_BATCH, get_payload);
        assert_eq!(resp.status, STATUS_OK);

        let results = decode_get_response(&resp.payload).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].status, 0, "expected found (status=0)");

        // Decode metadata fields in order:
        // tx_version(4) + locktime(4) + fee(8) + size_in_bytes(8) + extended_size(8)
        // + flags(1) + spending_height(4) + created_at(8) + spent_utxos(4) + pruned_utxos(4)
        // + utxo_count(4) + generation(4) + updated_at(8) + unmined_since(4)
        // + delete_at_height(4) + preserve_until(4) + external_ref(65)
        // + reassignment_count(1) + block_entry_count(1)
        let data = &results[0].data;
        let mut pos = 0;

        let tx_version = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()); pos += 4;
        assert_eq!(tx_version, 1);

        let locktime = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()); pos += 4;
        assert_eq!(locktime, 0);

        let fee = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()); pos += 8;
        assert_eq!(fee, 500);

        let size_in_bytes = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()); pos += 8;
        assert_eq!(size_in_bytes, 250);

        let _extended_size = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()); pos += 8;

        let _flags = data[pos]; pos += 1;

        let _spending_height = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()); pos += 4;

        let _created_at = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()); pos += 8;

        let spent_utxos = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()); pos += 4;
        assert_eq!(spent_utxos, 0, "no UTXOs should be spent");

        let _pruned_utxos = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()); pos += 4;

        let utxo_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        assert_eq!(utxo_count, 3, "utxo_count should be 3");
        let _ = pos; // silence unused warning
    }

    // -----------------------------------------------------------------------
    // 5o. Create, Spend, then Get — verify spent_utxos=1
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_create_spend_then_get() {
        let h = DispatchTestHarness::new();
        let txid = DispatchTestHarness::make_txid(50);

        // Create with 3 UTXOs
        let resp = h.create_tx(txid, 3);
        assert_eq!(resp.status, STATUS_OK);

        // Spend UTXO at vout=0
        let mut utxo_hash = [0u8; 32];
        utxo_hash[0] = 0; // matches the hash generated in create_tx for vout=0
        let spend_params = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        let spend_item = WireSpendItem {
            txid,
            vout: 0,
            utxo_hash,
            spending_data: {
                let mut sd = [0u8; 36];
                sd[0] = 0xAB;
                sd[32..36].copy_from_slice(&1u32.to_le_bytes());
                sd
            },
        };
        let spend_payload = encode_spend_batch(&spend_params, &[spend_item]);
        let resp = h.request(OP_SPEND_BATCH, spend_payload);
        assert_eq!(resp.status, STATUS_OK);

        // Get and verify spent_utxos=1
        let get_payload = encode_get_batch(FieldMask::SPENT_UTXOS, &[txid]);
        let resp = h.request(OP_GET_BATCH, get_payload);
        assert_eq!(resp.status, STATUS_OK);

        let results = decode_get_response(&resp.payload).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].status, 0);
        let spent = u32::from_le_bytes(results[0].data[0..4].try_into().unwrap());
        assert_eq!(spent, 1, "spent_utxos should be 1 after spending 1 UTXO");
    }

    // -----------------------------------------------------------------------
    // 5p. Create, SetMined, then Get — verify block_entry_count > 0
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_create_set_mined_then_get() {
        let h = DispatchTestHarness::new();
        let txid = DispatchTestHarness::make_txid(60);

        let resp = h.create_tx(txid, 2);
        assert_eq!(resp.status, STATUS_OK);

        // SetMined
        let set_mined_params = SetMinedBatchParams {
            block_id: 42,
            block_height: 1000,
            subtree_idx: 0,
            on_longest_chain: true,
            unset_mined: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        let set_mined_payload = encode_set_mined_batch(&set_mined_params, &[txid]);
        let resp = h.request(OP_SET_MINED_BATCH, set_mined_payload);
        assert_eq!(resp.status, STATUS_OK);

        // Get with BLOCK_ENTRY_COUNT field
        let get_payload = encode_get_batch(FieldMask::BLOCK_ENTRY_COUNT, &[txid]);
        let resp = h.request(OP_GET_BATCH, get_payload);
        assert_eq!(resp.status, STATUS_OK);

        let results = decode_get_response(&resp.payload).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].status, 0);
        let block_entry_count = results[0].data[0];
        assert!(
            block_entry_count > 0,
            "block_entry_count should be > 0 after SetMined, got {block_entry_count}"
        );
    }

    // -----------------------------------------------------------------------
    // 5q. Create, Delete, then Get — not found
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_create_delete_then_get_not_found() {
        let h = DispatchTestHarness::new();
        let txid = DispatchTestHarness::make_txid(70);

        let resp = h.create_tx(txid, 2);
        assert_eq!(resp.status, STATUS_OK);

        // Delete
        let delete_payload = encode_txid_batch(&[txid], &[]);
        let resp = h.request(OP_DELETE_BATCH, delete_payload);
        assert_eq!(resp.status, STATUS_OK);

        // Get — should return status=1 (not found)
        let get_payload = encode_get_batch(FieldMask::ALL_METADATA, &[txid]);
        let resp = h.request(OP_GET_BATCH, get_payload);
        assert_eq!(resp.status, STATUS_OK); // overall response is OK

        let results = decode_get_response(&resp.payload).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].status, 1, "expected not-found status=1 after delete");
    }

    // -----------------------------------------------------------------------
    // 5r. Ping returns OK
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_ping_returns_ok() {
        let h = DispatchTestHarness::new();
        let resp = h.request(OP_PING, vec![]);
        assert_eq!(resp.status, STATUS_OK);
        assert!(resp.payload.is_empty(), "PING payload should be empty");
    }

    // -----------------------------------------------------------------------
    // 5s. Health returns OK
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_health_returns_ok() {
        let h = DispatchTestHarness::new();
        let resp = h.request(OP_HEALTH, vec![]);
        assert_eq!(resp.status, STATUS_OK);
        assert_eq!(resp.payload, b"ok", "HEALTH payload should be b\"ok\"");
    }

    // -----------------------------------------------------------------------
    // 6t. Batch too large rejected
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_batch_too_large_rejected() {
        let h = DispatchTestHarness::new();

        // Build a create batch with 20 items, but set max_batch_size=10
        let items: Vec<WireCreateItem> = (0..20u8)
            .map(|i| {
                let txid = DispatchTestHarness::make_txid(100 + i);
                let mut hash = [0u8; 32];
                hash[0] = i;
                WireCreateItem {
                    txid,
                    tx_version: 1,
                    locktime: 0,
                    fee: 100,
                    size_in_bytes: 100,
                    extended_size: 100,
                    is_coinbase: false,
                    spending_height: 0,
                    created_at: 1700000000000,
                    flags: 0,
                    utxo_hashes: vec![hash],
                    cold_data: vec![],
                    block_height: 0,
                    mined_block_id: None,
                    mined_block_height: None,
                    mined_subtree_idx: None,
                    parent_txids: vec![],
                }
            })
            .collect();
        let payload = encode_create_batch(&items);
        let resp = h.request_with_max_batch(OP_CREATE_BATCH, payload, 10);
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_INTERNAL);
        assert!(msg.contains("batch too large"), "expected 'batch too large' in: {msg}");
    }
}
