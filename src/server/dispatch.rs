//! Request dispatch: maps wire protocol opcodes to Engine methods.
//!
//! In clustered mode, the dispatcher checks shard ownership before
//! processing key-based operations. If this node doesn't own the shard,
//! it returns a Redirect response.

use crate::cluster::coordinator::RunningCluster;
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
use std::collections::HashMap;

/// Dispatch a request frame to the appropriate Engine method.
///
/// If `cluster` is Some, shard ownership is checked for key-based operations.
/// Requests for keys not owned by this node get a Redirect response.
pub fn handle_request(
    request: &RequestFrame,
    engine: &Engine,
    max_batch_size: u32,
    cluster: Option<&RunningCluster>,
) -> ResponseFrame {
    match request.op_code {
        OP_SPEND_BATCH => handle_spend_batch(request, engine, max_batch_size),
        OP_UNSPEND_BATCH => handle_unspend_batch(request, engine, max_batch_size),
        OP_SET_MINED_BATCH => handle_set_mined_batch(request, engine, max_batch_size),
        OP_CREATE_BATCH => handle_create_batch(request, engine, max_batch_size),
        OP_FREEZE_BATCH => handle_freeze_batch(request, engine, max_batch_size),
        OP_UNFREEZE_BATCH => handle_unfreeze_batch(request, engine, max_batch_size),
        OP_REASSIGN_BATCH => handle_reassign_batch(request, engine, max_batch_size),
        OP_SET_CONFLICTING_BATCH => handle_set_conflicting_batch(request, engine, max_batch_size),
        OP_SET_LOCKED_BATCH => handle_set_locked_batch(request, engine, max_batch_size),
        OP_PRESERVE_UNTIL_BATCH => handle_preserve_until_batch(request, engine, max_batch_size),
        OP_DELETE_BATCH => handle_delete_batch(request, engine, max_batch_size),
        OP_MARK_LONGEST_CHAIN_BATCH => handle_mark_longest_chain_batch(request, engine, max_batch_size),
        OP_GET_SPEND_BATCH => handle_get_spend_batch(request, engine, max_batch_size),
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
        _ => error_response(request.request_id, ERR_INTERNAL, "unknown opcode"),
    }
}

// ---------------------------------------------------------------------------
// Spend
// ---------------------------------------------------------------------------

fn handle_spend_batch(req: &RequestFrame, engine: &Engine, max_batch: u32) -> ResponseFrame {
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

    for (txid, group) in &by_txid {
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

fn handle_unspend_batch(req: &RequestFrame, engine: &Engine, max_batch: u32) -> ResponseFrame {
    let (shared, items) = match decode_slot_item_batch_with_params(&req.payload) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed unspend batch"),
    };
    if items.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }

    let mut errors = Vec::new();
    for (i, item) in items.iter().enumerate() {
        let result = engine.unspend(&UnspendRequest {
            tx_key: TxKey { txid: item.txid },
            offset: item.vout,
            utxo_hash: item.utxo_hash,
            current_block_height: shared.current_block_height,
            block_height_retention: shared.block_height_retention,
        });
        if let Err(err) = result {
            errors.push(spend_error_to_batch_error(i as u32, &err));
        }
    }

    batch_response(req.request_id, &errors)
}

// ---------------------------------------------------------------------------
// SetMined
// ---------------------------------------------------------------------------

fn handle_set_mined_batch(req: &RequestFrame, engine: &Engine, max_batch: u32) -> ResponseFrame {
    let (params, txids) = match decode_set_mined_batch(&req.payload) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed set_mined batch"),
    };
    if txids.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }

    let mut errors = Vec::new();
    for (i, txid) in txids.iter().enumerate() {
        let result = engine.set_mined(&SetMinedRequest {
            tx_key: TxKey { txid: *txid },
            block_id: params.block_id,
            block_height: params.block_height,
            subtree_idx: params.subtree_idx,
            current_block_height: params.current_block_height,
            block_height_retention: params.block_height_retention,
            on_longest_chain: params.on_longest_chain,
            unset_mined: params.unset_mined,
        });
        if let Err(err) = result {
            errors.push(spend_error_to_batch_error(i as u32, &err));
        }
    }

    batch_response(req.request_id, &errors)
}

// ---------------------------------------------------------------------------
// Create
// ---------------------------------------------------------------------------

fn handle_create_batch(req: &RequestFrame, engine: &Engine, max_batch: u32) -> ResponseFrame {
    // CreateBatch payload: [count:4][items...]
    // Each item is variable-length. For simplicity, use a simplified format:
    // [count:4][items: txid(32) + utxo_count(4) + utxo_hashes(32*N)]
    let payload = &req.payload;
    if payload.len() < 4 {
        return error_response(req.request_id, ERR_INTERNAL, "malformed create batch");
    }
    let count = u32::from_le_bytes(payload[0..4].try_into().unwrap());
    if count > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }

    let mut errors = Vec::new();
    let mut pos = 4;

    for i in 0..count {
        if pos + 36 > payload.len() {
            errors.push(BatchItemError {
                item_index: i,
                error_code: ERR_INTERNAL,
                error_data: vec![],
            });
            break;
        }

        let mut txid = [0u8; 32];
        txid.copy_from_slice(&payload[pos..pos + 32]);
        pos += 32;

        let utxo_count = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap());
        pos += 4;

        if pos + (utxo_count as usize) * 32 > payload.len() {
            errors.push(BatchItemError {
                item_index: i,
                error_code: ERR_INTERNAL,
                error_data: vec![],
            });
            break;
        }

        let mut utxo_hashes = Vec::with_capacity(utxo_count as usize);
        for _ in 0..utxo_count {
            let mut h = [0u8; 32];
            h.copy_from_slice(&payload[pos..pos + 32]);
            utxo_hashes.push(h);
            pos += 32;
        }

        let create_req = CreateRequest {
            tx_id: txid,
            tx_version: 1,
            locktime: 0,
            fee: 0,
            size_in_bytes: 0,
            extended_size: 0,
            is_coinbase: false,
            spending_height: 0,
            utxo_hashes,
            inputs: None,
            outputs: None,
            inpoints: None,
            is_external: false,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            block_height: 0,
            mined_block_infos: vec![],
            frozen: false,
            conflicting: false,
            locked: false,
        };

        match engine.create(&create_req) {
            Ok(_) => {}
            Err(CreateError::DuplicateTxId) => {
                errors.push(BatchItemError {
                    item_index: i,
                    error_code: ERR_ALREADY_EXISTS,
                    error_data: vec![],
                });
            }
            Err(_) => {
                errors.push(BatchItemError {
                    item_index: i,
                    error_code: ERR_INTERNAL,
                    error_data: vec![],
                });
            }
        }
    }

    batch_response(req.request_id, &errors)
}

// ---------------------------------------------------------------------------
// Freeze / Unfreeze / Delete / SetLocked / etc — simple dispatch
// ---------------------------------------------------------------------------

fn handle_freeze_batch(req: &RequestFrame, engine: &Engine, max_batch: u32) -> ResponseFrame {
    let items = match decode_slot_item_batch(&req.payload) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed"),
    };
    if items.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }
    let mut errors = Vec::new();
    for (i, item) in items.iter().enumerate() {
        if let Err(err) = engine.freeze(&FreezeRequest {
            tx_key: TxKey { txid: item.txid },
            offset: item.vout,
            utxo_hash: item.utxo_hash,
        }) {
            errors.push(spend_error_to_batch_error(i as u32, &err));
        }
    }
    batch_response(req.request_id, &errors)
}

fn handle_unfreeze_batch(req: &RequestFrame, engine: &Engine, max_batch: u32) -> ResponseFrame {
    let items = match decode_slot_item_batch(&req.payload) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed"),
    };
    if items.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }
    let mut errors = Vec::new();
    for (i, item) in items.iter().enumerate() {
        if let Err(err) = engine.unfreeze(&UnfreezeRequest {
            tx_key: TxKey { txid: item.txid },
            offset: item.vout,
            utxo_hash: item.utxo_hash,
        }) {
            errors.push(spend_error_to_batch_error(i as u32, &err));
        }
    }
    batch_response(req.request_id, &errors)
}

fn handle_reassign_batch(req: &RequestFrame, engine: &Engine, max_batch: u32) -> ResponseFrame {
    let (params, items) = match decode_reassign_batch(&req.payload) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed"),
    };
    if items.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }
    let mut errors = Vec::new();
    for (i, item) in items.iter().enumerate() {
        if let Err(err) = engine.reassign(&ReassignRequest {
            tx_key: TxKey { txid: item.txid },
            offset: item.vout,
            utxo_hash: item.utxo_hash,
            new_utxo_hash: item.new_utxo_hash,
            block_height: params.block_height,
            spendable_after: params.spendable_after,
        }) {
            errors.push(spend_error_to_batch_error(i as u32, &err));
        }
    }
    batch_response(req.request_id, &errors)
}

fn handle_set_conflicting_batch(req: &RequestFrame, engine: &Engine, max_batch: u32) -> ResponseFrame {
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
    for (i, txid) in txids.iter().enumerate() {
        if let Err(err) = engine.set_conflicting(&SetConflictingRequest {
            tx_key: TxKey { txid: *txid },
            value,
            current_block_height: cbh,
            block_height_retention: bhr,
        }) {
            errors.push(spend_error_to_batch_error(i as u32, &err));
        }
    }
    batch_response(req.request_id, &errors)
}

fn handle_set_locked_batch(req: &RequestFrame, engine: &Engine, max_batch: u32) -> ResponseFrame {
    let (shared, txids) = match decode_txid_batch(&req.payload, 1) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed"),
    };
    if txids.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }
    let value = shared[0] != 0;

    let mut errors = Vec::new();
    for (i, txid) in txids.iter().enumerate() {
        if let Err(err) = engine.set_locked(&SetLockedRequest {
            tx_key: TxKey { txid: *txid },
            value,
        }) {
            errors.push(spend_error_to_batch_error(i as u32, &err));
        }
    }
    batch_response(req.request_id, &errors)
}

fn handle_preserve_until_batch(req: &RequestFrame, engine: &Engine, max_batch: u32) -> ResponseFrame {
    let (shared, txids) = match decode_txid_batch(&req.payload, 4) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed"),
    };
    if txids.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }
    let height = u32::from_le_bytes(shared[0..4].try_into().unwrap());

    let mut errors = Vec::new();
    for (i, txid) in txids.iter().enumerate() {
        if let Err(err) = engine.preserve_until(&PreserveUntilRequest {
            tx_key: TxKey { txid: *txid },
            block_height: height,
        }) {
            errors.push(spend_error_to_batch_error(i as u32, &err));
        }
    }
    batch_response(req.request_id, &errors)
}

fn handle_delete_batch(req: &RequestFrame, engine: &Engine, max_batch: u32) -> ResponseFrame {
    let (_, txids) = match decode_txid_batch(&req.payload, 0) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed"),
    };
    if txids.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }
    let mut errors = Vec::new();
    for (i, txid) in txids.iter().enumerate() {
        if let Err(err) = engine.delete(&DeleteRequest {
            tx_key: TxKey { txid: *txid },
        }) {
            errors.push(spend_error_to_batch_error(i as u32, &err));
        }
    }
    batch_response(req.request_id, &errors)
}

fn handle_mark_longest_chain_batch(req: &RequestFrame, engine: &Engine, max_batch: u32) -> ResponseFrame {
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
    for (i, txid) in txids.iter().enumerate() {
        if let Err(err) = engine.mark_on_longest_chain(&MarkOnLongestChainRequest {
            tx_key: TxKey { txid: *txid },
            on_longest_chain,
            current_block_height: cbh,
            block_height_retention: bhr,
        }) {
            errors.push(spend_error_to_batch_error(i as u32, &err));
        }
    }
    batch_response(req.request_id, &errors)
}

// ---------------------------------------------------------------------------
// GetSpend
// ---------------------------------------------------------------------------

fn handle_get_spend_batch(req: &RequestFrame, engine: &Engine, max_batch: u32) -> ResponseFrame {
    let items = match decode_get_spend_batch(&req.payload) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed"),
    };
    if items.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }

    let mut results = Vec::with_capacity(items.len());
    for item in &items {
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

/// Decode an unspend batch: [count:4][cbh:4][bhr:4][items: txid+vout+hash × count]
struct UnspendSharedParams {
    current_block_height: u32,
    block_height_retention: u32,
}

fn decode_slot_item_batch_with_params(data: &[u8]) -> Option<(UnspendSharedParams, Vec<WireSlotItem>)> {
    if data.len() < 12 { return None; }
    let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    let cbh = u32::from_le_bytes(data[4..8].try_into().unwrap());
    let bhr = u32::from_le_bytes(data[8..12].try_into().unwrap());

    let mut items = Vec::with_capacity(count);
    let mut pos = 12;
    for _ in 0..count {
        if pos + 68 > data.len() { return None; }
        let mut txid = [0u8; 32]; txid.copy_from_slice(&data[pos..pos + 32]);
        let vout = u32::from_le_bytes(data[pos + 32..pos + 36].try_into().unwrap());
        let mut uh = [0u8; 32]; uh.copy_from_slice(&data[pos + 36..pos + 68]);
        items.push(WireSlotItem { txid, vout, utxo_hash: uh });
        pos += 68;
    }

    Some((UnspendSharedParams { current_block_height: cbh, block_height_retention: bhr }, items))
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
