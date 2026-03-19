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
use crate::replication::protocol::{ReplicaBatch, ReplicaOp};
use crate::replication::receiver::handle_replica_batch;
use crate::replication::tcp_transport::TcpReplicaTransport;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

/// Dispatch a request frame to the appropriate Engine method.
///
/// If `cluster` is Some, shard ownership is checked for key-based operations.
/// Requests for keys not owned by this node get a Redirect response.
///
/// If `redo_log` is Some, successful mutations are logged for crash recovery.
/// Redo log writes are best-effort: failures are logged but do not fail the
/// client request (the data has already been applied to the engine).
pub fn handle_request(
    request: &RequestFrame,
    engine: &Engine,
    max_batch_size: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
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
        OP_CREATE_BATCH => handle_create_batch(request, engine, max_batch_size, cluster, redo_log),
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
        OP_REPLICA_BATCH => {
            // Dispatch replication batch to the receiver's apply logic.
            // Uses a thread-local AtomicU64 for sequence tracking at the
            // dispatch level (the real receiver manages its own counter).
            thread_local! {
                static DISPATCH_LAST_APPLIED: AtomicU64 = const { AtomicU64::new(0) };
            }
            DISPATCH_LAST_APPLIED.with(|la| {
                handle_replica_batch(request, engine, la)
            })
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
/// For each replica node that owns a copy of the key's shard, a background
/// thread is spawned to send the batch via TCP. Replication failures are
/// logged but do not affect the client response — the replica will catch up
/// from the redo log or via anti-entropy.
fn replicate_ops(cluster: Option<&RunningCluster>, tx_key: &TxKey, ops: Vec<ReplicaOp>) {
    let cluster = match cluster {
        Some(c) => c,
        None => {
            return;
        }
    };
    if ops.is_empty() {
        return;
    }

    let shard = ShardTable::shard_for_key(tx_key);
    let table = cluster.shard_table();
    let table_guard = table.read().unwrap();
    let assignment = table_guard.assignment(shard);

    if assignment.replicas.is_empty() {
        return;
    }

    // Collect replica addresses before dropping the lock
    let mut replica_addrs = Vec::new();
    for replica_id in &assignment.replicas {
        if let Some(addr) = cluster.node_addr(replica_id) {
            replica_addrs.push(addr);
        }
    }
    drop(table_guard);

    if replica_addrs.is_empty() {
        return;
    }

    let batch = ReplicaBatch {
        first_sequence: 0, // Sequence tracking handled by ReplicationManager for formal replication
        ops,
    };

    for addr in replica_addrs {
        let batch_clone = batch.clone();
        std::thread::spawn(move || {
            match send_replica_batch_to(addr, &batch_clone) {
                Ok(()) => {}
                Err(e) => {
                    eprintln!("replication to {addr} failed: {e}");
                }
            }
        });
    }
}

/// Send a `ReplicaBatch` to a replica node via TCP using the wire protocol.
fn send_replica_batch_to(
    addr: std::net::SocketAddr,
    batch: &ReplicaBatch,
) -> std::result::Result<(), String> {
    use crate::replication::manager::ReplicaTransport;

    let mut transport = TcpReplicaTransport::connect(
        &addr.to_string(),
        Duration::from_secs(5),
    ).map_err(|e| format!("connect: {e}"))?;

    transport.send_batch(batch).map_err(|e| format!("send: {e}"))?;

    match transport.recv_ack(Duration::from_secs(5)) {
        Ok(crate::replication::protocol::ReplicaAck::Ok { .. }) => Ok(()),
        Ok(crate::replication::protocol::ReplicaAck::Error { message, .. }) => {
            Err(format!("replica error: {message}"))
        }
        Err(e) => Err(format!("recv_ack: {e}")),
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
        return Some(error_response(request_id, ERR_INTERNAL, "no quorum"));
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
    for (key, ops) in repl_ops_by_key {
        replicate_ops(cluster, &key, ops);
    }

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
    for (key, ops) in repl_ops_by_key {
        replicate_ops(cluster, &key, ops);
    }

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
    for (key, ops) in repl_ops_by_key {
        replicate_ops(cluster, &key, ops);
    }

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
        let (inputs, outputs, inpoints) = parse_cold_data_fields(&item.cold_data);

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
            is_external: false,
            created_at: item.created_at,
            block_height: item.mined_block_height.unwrap_or(0),
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
    for (key, ops) in repl_ops_by_key {
        replicate_ops(cluster, &key, ops);
    }

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
    for (key, ops) in repl_ops_by_key {
        replicate_ops(cluster, &key, ops);
    }

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
    for (key, ops) in repl_ops_by_key {
        replicate_ops(cluster, &key, ops);
    }

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
    for (key, ops) in repl_ops_by_key {
        replicate_ops(cluster, &key, ops);
    }

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
    for (key, ops) in repl_ops_by_key {
        replicate_ops(cluster, &key, ops);
    }

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
    for (key, ops) in repl_ops_by_key {
        replicate_ops(cluster, &key, ops);
    }

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
    for (key, ops) in repl_ops_by_key {
        replicate_ops(cluster, &key, ops);
    }

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
    for (key, ops) in repl_ops_by_key {
        replicate_ops(cluster, &key, ops);
    }

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
        // Reads allowed during outbound migration or when FLAG_LOCAL_READ is set
        if !local_read
            && let Some(cluster) = cluster
        {
            let key = TxKey { txid: *txid };
            if !cluster.is_master(&key) && !cluster.is_migrating_outbound(&key) {
                results.push(WireGetResult { status: 1, data: vec![] });
                continue;
            }
        }

        let key = TxKey { txid: *txid };
        match engine.read_metadata(&key) {
            Ok(meta) => {
                let mut data = Vec::new();
                if field_mask.has(FieldMask::METADATA) {
                    // Serialize key metadata fields
                    data.extend_from_slice(&{ meta.tx_version }.to_le_bytes());
                    data.extend_from_slice(&{ meta.locktime }.to_le_bytes());
                    data.extend_from_slice(&{ meta.fee }.to_le_bytes());
                    data.extend_from_slice(&{ meta.size_in_bytes }.to_le_bytes());
                    data.extend_from_slice(&{ meta.extended_size }.to_le_bytes());
                    data.push({ meta.flags }.bits());
                    data.extend_from_slice(&{ meta.spending_height }.to_le_bytes());
                    data.extend_from_slice(&{ meta.created_at }.to_le_bytes());
                    data.extend_from_slice(&{ meta.spent_utxos }.to_le_bytes());
                    data.extend_from_slice(&{ meta.pruned_utxos }.to_le_bytes());
                    data.extend_from_slice(&{ meta.utxo_count }.to_le_bytes());
                    data.extend_from_slice(&{ meta.generation }.to_le_bytes());
                    data.extend_from_slice(&{ meta.updated_at }.to_le_bytes());
                    data.extend_from_slice(&{ meta.unmined_since }.to_le_bytes());
                    data.extend_from_slice(&{ meta.delete_at_height }.to_le_bytes());
                    data.extend_from_slice(&{ meta.preserve_until }.to_le_bytes());
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
    for (key, ops) in repl_ops_by_key {
        replicate_ops(cluster, &key, ops);
    }

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
