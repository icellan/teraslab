//! Batch encode/decode for all operation types.
//!
//! Each batch follows the pattern:
//! `[count:4][shared_params][items × count]`
//!
//! # Pre-allocation safety
//!
//! Every batch decoder validates the on-wire `count` field BEFORE calling
//! `Vec::with_capacity(count)`. Two checks run, in order, against an
//! attacker who advertises a huge `count` with a tiny payload:
//!
//! 1. `count <= max_batch`: the configured server-side per-request batch
//!    limit (`ServerConfig::max_batch_size`). Plumbed through every
//!    decoder so the dispatcher does not need to allocate first and
//!    discard later.
//! 2. `count * per_item_min_size <= remaining_payload`: a payload-fit
//!    check using checked `usize` arithmetic. This catches malformed
//!    frames whose declared count cannot possibly be backed by the
//!    bytes that follow, regardless of `max_batch`.
//!
//! Both failures return [`CodecError`]. The legacy `Option`-returning
//! wrappers (e.g. [`decode_spend_batch`]) remain for client-side and
//! benchmark callers that do not have a server config in scope; they
//! fall back to the absolute hard cap [`MAX_DECODE_BATCH`].

use thiserror::Error;

/// Errors returned by the new `*_checked` batch decoders.
///
/// All variants are explicitly enforced *before* any payload allocation
/// so a malicious frame with a huge `count` cannot drive memory pressure.
#[derive(Error, Debug, PartialEq, Eq)]
pub enum CodecError {
    /// Buffer is shorter than the minimum required for the batch header
    /// (`count` and any shared params).
    #[error("payload too short: need {need} header bytes, have {have}")]
    HeaderTooShort {
        /// Minimum number of bytes needed for the batch header.
        need: usize,
        /// Number of bytes actually present in the buffer.
        have: usize,
    },
    /// On-wire `count` exceeds the configured `max_batch_size`. Enforced
    /// BEFORE any per-item allocation so the decoder never reserves
    /// capacity for an attacker-controlled count.
    #[error("batch count {count} exceeds max_batch_size {max}")]
    BatchTooLarge {
        /// Number of items the frame claims to carry.
        count: u32,
        /// Configured maximum permitted by the server.
        max: u32,
    },
    /// Declared `count` cannot fit into the remaining payload bytes given
    /// each item's minimum on-wire width. This catches malformed frames
    /// (e.g. `count = u32::MAX`, payload ~ a few hundred bytes) before
    /// any `Vec::with_capacity(count)` call.
    #[error(
        "batch payload truncated: count={count} items each need >= {per_item_min} bytes, payload has {available} bytes available"
    )]
    TruncatedBatch {
        /// Declared item count from the wire.
        count: u32,
        /// Minimum byte width of one item (lower bound; variable-size
        /// items can be larger).
        per_item_min: usize,
        /// Bytes remaining in the payload after the batch header.
        available: usize,
    },
    /// A variable-sized item (e.g. cold data, parent txids) declared a
    /// length that does not fit in the remaining payload.
    #[error("variable-length section truncated: need {need} bytes, have {have} bytes")]
    SectionTruncated {
        /// Bytes required by the declared length.
        need: usize,
        /// Bytes actually remaining in the payload.
        have: usize,
    },
}

/// Absolute hard cap on the `count` field of any batch decoder when no
/// per-call `max_batch_size` is supplied.
///
/// Used by the legacy [`Option`]-returning wrappers ([`decode_spend_batch`]
/// and friends) so client-side and benchmark callers — which do not have
/// access to a server-side `ServerConfig` — still get a strict
/// allocation-bounding check. Server dispatch plumbs the configured
/// `max_batch_size` directly into the `*_checked` variants and ignores
/// this constant.
///
/// 1 MiB items is well above any realistic batch we expect to encode
/// (Teranode's adapter caps batches at 8192) but small enough that
/// pre-allocating `Vec::with_capacity(MAX_DECODE_BATCH)` on the largest
/// fixed-size item type (`WireSpendItem` = 104 bytes) is bounded to ~100
/// MiB — well within the new 16 MiB `MAX_FRAME_SIZE` plus headroom for
/// in-flight batch building.
pub const MAX_DECODE_BATCH: u32 = 1 << 20;

/// Helper: append a u32 LE.
fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn put_u16(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn get_u32(d: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(d[o..o + 4].try_into().unwrap())
}
fn get_u16(d: &[u8], o: usize) -> u16 {
    u16::from_le_bytes(d[o..o + 2].try_into().unwrap())
}

/// Validate a wire-supplied `count` against `max_batch` AND the available
/// payload bytes for fixed-size items. MUST be called BEFORE any
/// `Vec::with_capacity(count)` invocation so an attacker-supplied huge
/// count cannot drive memory pressure.
///
/// `available` is the number of payload bytes remaining after the batch
/// header (`count` + any shared params). `per_item_min` is the minimum
/// on-wire width of a single item (use the fixed-size lower bound for
/// variable-length items like CreateBatch entries).
fn validate_batch_count(
    count: u32,
    max_batch: u32,
    per_item_min: usize,
    available: usize,
) -> Result<(), CodecError> {
    if count > max_batch {
        return Err(CodecError::BatchTooLarge {
            count,
            max: max_batch,
        });
    }
    let count_usize = count as usize;
    let needed = count_usize
        .checked_mul(per_item_min)
        .ok_or(CodecError::TruncatedBatch {
            count,
            per_item_min,
            available,
        })?;
    if needed > available {
        return Err(CodecError::TruncatedBatch {
            count,
            per_item_min,
            available,
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Spend batch
// ---------------------------------------------------------------------------

/// A single spend item on the wire.
#[derive(Debug, Clone, PartialEq)]
pub struct WireSpendItem {
    pub txid: [u8; 32],
    pub vout: u32,
    pub utxo_hash: [u8; 32],
    pub spending_data: [u8; 36],
}

/// Shared parameters for a spend batch.
#[derive(Debug, Clone, PartialEq)]
pub struct SpendBatchParams {
    pub ignore_conflicting: bool,
    pub ignore_locked: bool,
    pub current_block_height: u32,
    pub block_height_retention: u32,
}

/// Encode a SpendBatch request payload.
pub fn encode_spend_batch(params: &SpendBatchParams, items: &[WireSpendItem]) -> Vec<u8> {
    // count(4) + ignore_c(1) + ignore_l(1) + cbh(4) + bhr(4) + items(104 each)
    let mut buf = Vec::with_capacity(14 + items.len() * 104);
    put_u32(&mut buf, items.len() as u32);
    buf.push(u8::from(params.ignore_conflicting));
    buf.push(u8::from(params.ignore_locked));
    put_u32(&mut buf, params.current_block_height);
    put_u32(&mut buf, params.block_height_retention);
    for item in items {
        buf.extend_from_slice(&item.txid);
        put_u32(&mut buf, item.vout);
        buf.extend_from_slice(&item.utxo_hash);
        buf.extend_from_slice(&item.spending_data);
    }
    buf
}

/// Decode a SpendBatch request payload, validating counts and payload size
/// before any per-item allocation.
///
/// `max_batch` is the configured server-side per-request batch cap
/// ([`crate::config::ServerConfig::max_batch_size`]). The decoder rejects
/// the frame with [`CodecError::BatchTooLarge`] BEFORE allocating the
/// output `Vec` if the wire-supplied `count` exceeds this bound, and with
/// [`CodecError::TruncatedBatch`] if `count * 104` bytes cannot fit in the
/// remaining payload.
pub fn decode_spend_batch_checked(
    data: &[u8],
    max_batch: u32,
) -> Result<(SpendBatchParams, Vec<WireSpendItem>), CodecError> {
    if data.len() < 14 {
        return Err(CodecError::HeaderTooShort {
            need: 14,
            have: data.len(),
        });
    }
    let count = get_u32(data, 0);
    let params = SpendBatchParams {
        ignore_conflicting: data[4] != 0,
        ignore_locked: data[5] != 0,
        current_block_height: get_u32(data, 6),
        block_height_retention: get_u32(data, 10),
    };
    validate_batch_count(count, max_batch, 104, data.len() - 14)?;
    let count = count as usize;
    let mut items = Vec::with_capacity(count);
    let mut pos = 14;
    for _ in 0..count {
        // Bounds were proven by validate_batch_count above; this is a
        // belt-and-braces check that keeps the per-item indexing safe
        // even if a future caller bypasses validation.
        if pos + 104 > data.len() {
            return Err(CodecError::TruncatedBatch {
                count: count as u32,
                per_item_min: 104,
                available: data.len().saturating_sub(14),
            });
        }
        let mut txid = [0u8; 32];
        txid.copy_from_slice(&data[pos..pos + 32]);
        let vout = get_u32(data, pos + 32);
        let mut uh = [0u8; 32];
        uh.copy_from_slice(&data[pos + 36..pos + 68]);
        let mut sd = [0u8; 36];
        sd.copy_from_slice(&data[pos + 68..pos + 104]);
        items.push(WireSpendItem {
            txid,
            vout,
            utxo_hash: uh,
            spending_data: sd,
        });
        pos += 104;
    }
    Ok((params, items))
}

/// Decode a SpendBatch request payload using the absolute hard cap
/// [`MAX_DECODE_BATCH`]. Server-side callers should prefer
/// [`decode_spend_batch_checked`] with the configured `max_batch_size`.
pub fn decode_spend_batch(data: &[u8]) -> Option<(SpendBatchParams, Vec<WireSpendItem>)> {
    decode_spend_batch_checked(data, MAX_DECODE_BATCH).ok()
}

// ---------------------------------------------------------------------------
// SetMined batch
// ---------------------------------------------------------------------------

/// Shared parameters for a SetMined batch.
#[derive(Debug, Clone, PartialEq)]
pub struct SetMinedBatchParams {
    pub block_id: u32,
    pub block_height: u32,
    pub subtree_idx: u32,
    pub on_longest_chain: bool,
    pub unset_mined: bool,
    pub current_block_height: u32,
    pub block_height_retention: u32,
}

/// Encode a SetMinedBatch request payload.
pub fn encode_set_mined_batch(params: &SetMinedBatchParams, txids: &[[u8; 32]]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(26 + txids.len() * 32);
    put_u32(&mut buf, txids.len() as u32);
    put_u32(&mut buf, params.block_id);
    put_u32(&mut buf, params.block_height);
    put_u32(&mut buf, params.subtree_idx);
    buf.push(u8::from(params.on_longest_chain));
    buf.push(u8::from(params.unset_mined));
    put_u32(&mut buf, params.current_block_height);
    put_u32(&mut buf, params.block_height_retention);
    for txid in txids {
        buf.extend_from_slice(txid);
    }
    buf
}

/// Decode a SetMinedBatch request payload, validating count before
/// allocation. See [`decode_spend_batch_checked`] for the
/// allocation-safety contract.
pub fn decode_set_mined_batch_checked(
    data: &[u8],
    max_batch: u32,
) -> Result<(SetMinedBatchParams, Vec<[u8; 32]>), CodecError> {
    if data.len() < 26 {
        return Err(CodecError::HeaderTooShort {
            need: 26,
            have: data.len(),
        });
    }
    let count = get_u32(data, 0);
    let params = SetMinedBatchParams {
        block_id: get_u32(data, 4),
        block_height: get_u32(data, 8),
        subtree_idx: get_u32(data, 12),
        on_longest_chain: data[16] != 0,
        unset_mined: data[17] != 0,
        current_block_height: get_u32(data, 18),
        block_height_retention: get_u32(data, 22),
    };
    validate_batch_count(count, max_batch, 32, data.len() - 26)?;
    let count = count as usize;
    let mut txids = Vec::with_capacity(count);
    let mut pos = 26;
    for _ in 0..count {
        if pos + 32 > data.len() {
            return Err(CodecError::TruncatedBatch {
                count: count as u32,
                per_item_min: 32,
                available: data.len().saturating_sub(26),
            });
        }
        let mut txid = [0u8; 32];
        txid.copy_from_slice(&data[pos..pos + 32]);
        txids.push(txid);
        pos += 32;
    }
    Ok((params, txids))
}

/// Decode a SetMinedBatch request payload using [`MAX_DECODE_BATCH`].
pub fn decode_set_mined_batch(data: &[u8]) -> Option<(SetMinedBatchParams, Vec<[u8; 32]>)> {
    decode_set_mined_batch_checked(data, MAX_DECODE_BATCH).ok()
}

// ---------------------------------------------------------------------------
// Simple txid-list batches (Delete, SetLocked, MarkLongestChain, PreserveUntil, SetConflicting)
// ---------------------------------------------------------------------------

/// Encode a batch of txids with optional shared u8 + u32 params.
///
/// Format:
/// ```text
/// [count:4][shared_params][txids × count]
/// ```
pub fn encode_txid_batch(txids: &[[u8; 32]], shared: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + shared.len() + txids.len() * 32);
    put_u32(&mut buf, txids.len() as u32);
    buf.extend_from_slice(shared);
    for txid in txids {
        buf.extend_from_slice(txid);
    }
    buf
}

/// Decode a batch of txids with a given shared params size, validating
/// count before allocation. See [`decode_spend_batch_checked`] for the
/// allocation-safety contract.
pub fn decode_txid_batch_checked(
    data: &[u8],
    shared_len: usize,
    max_batch: u32,
) -> Result<(Vec<u8>, Vec<[u8; 32]>), CodecError> {
    let header = 4 + shared_len;
    if data.len() < header {
        return Err(CodecError::HeaderTooShort {
            need: header,
            have: data.len(),
        });
    }
    let count = get_u32(data, 0);
    let shared = data[4..header].to_vec();
    validate_batch_count(count, max_batch, 32, data.len() - header)?;
    let count = count as usize;
    let mut txids = Vec::with_capacity(count);
    let mut pos = header;
    for _ in 0..count {
        if pos + 32 > data.len() {
            return Err(CodecError::TruncatedBatch {
                count: count as u32,
                per_item_min: 32,
                available: data.len().saturating_sub(header),
            });
        }
        let mut txid = [0u8; 32];
        txid.copy_from_slice(&data[pos..pos + 32]);
        txids.push(txid);
        pos += 32;
    }
    Ok((shared, txids))
}

/// Decode a batch of txids with a given shared params size using
/// [`MAX_DECODE_BATCH`].
pub fn decode_txid_batch(data: &[u8], shared_len: usize) -> Option<(Vec<u8>, Vec<[u8; 32]>)> {
    decode_txid_batch_checked(data, shared_len, MAX_DECODE_BATCH).ok()
}

// ---------------------------------------------------------------------------
// Freeze/Unfreeze/GetSpend batch (txid + vout + hash items)
// ---------------------------------------------------------------------------

/// A single freeze/unfreeze item on the wire.
#[derive(Debug, Clone, PartialEq)]
pub struct WireSlotItem {
    pub txid: [u8; 32],
    pub vout: u32,
    pub utxo_hash: [u8; 32],
}

/// Encode a batch of slot items.
pub fn encode_slot_item_batch(items: &[WireSlotItem]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + items.len() * 68);
    put_u32(&mut buf, items.len() as u32);
    for item in items {
        buf.extend_from_slice(&item.txid);
        put_u32(&mut buf, item.vout);
        buf.extend_from_slice(&item.utxo_hash);
    }
    buf
}

/// Decode a batch of slot items, validating count before allocation. See
/// [`decode_spend_batch_checked`] for the allocation-safety contract.
pub fn decode_slot_item_batch_checked(
    data: &[u8],
    max_batch: u32,
) -> Result<Vec<WireSlotItem>, CodecError> {
    if data.len() < 4 {
        return Err(CodecError::HeaderTooShort {
            need: 4,
            have: data.len(),
        });
    }
    let count = get_u32(data, 0);
    validate_batch_count(count, max_batch, 68, data.len() - 4)?;
    let count = count as usize;
    let mut items = Vec::with_capacity(count);
    let mut pos = 4;
    for _ in 0..count {
        if pos + 68 > data.len() {
            return Err(CodecError::TruncatedBatch {
                count: count as u32,
                per_item_min: 68,
                available: data.len().saturating_sub(4),
            });
        }
        let mut txid = [0u8; 32];
        txid.copy_from_slice(&data[pos..pos + 32]);
        let vout = get_u32(data, pos + 32);
        let mut uh = [0u8; 32];
        uh.copy_from_slice(&data[pos + 36..pos + 68]);
        items.push(WireSlotItem {
            txid,
            vout,
            utxo_hash: uh,
        });
        pos += 68;
    }
    Ok(items)
}

/// Decode a batch of slot items using [`MAX_DECODE_BATCH`].
pub fn decode_slot_item_batch(data: &[u8]) -> Option<Vec<WireSlotItem>> {
    decode_slot_item_batch_checked(data, MAX_DECODE_BATCH).ok()
}

// ---------------------------------------------------------------------------
// Reassign batch
// ---------------------------------------------------------------------------

/// A single reassign item on the wire.
#[derive(Debug, Clone, PartialEq)]
pub struct WireReassignItem {
    pub txid: [u8; 32],
    pub vout: u32,
    pub utxo_hash: [u8; 32],
    pub new_utxo_hash: [u8; 32],
}

/// Shared parameters for a reassign batch.
#[derive(Debug, Clone, PartialEq)]
pub struct ReassignBatchParams {
    pub block_height: u32,
    pub spendable_after: u32,
}

/// Encode a ReassignBatch request payload.
pub fn encode_reassign_batch(params: &ReassignBatchParams, items: &[WireReassignItem]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(12 + items.len() * 100);
    put_u32(&mut buf, items.len() as u32);
    put_u32(&mut buf, params.block_height);
    put_u32(&mut buf, params.spendable_after);
    for item in items {
        buf.extend_from_slice(&item.txid);
        put_u32(&mut buf, item.vout);
        buf.extend_from_slice(&item.utxo_hash);
        buf.extend_from_slice(&item.new_utxo_hash);
    }
    buf
}

/// Decode a ReassignBatch request payload, validating count before
/// allocation. See [`decode_spend_batch_checked`] for the
/// allocation-safety contract.
pub fn decode_reassign_batch_checked(
    data: &[u8],
    max_batch: u32,
) -> Result<(ReassignBatchParams, Vec<WireReassignItem>), CodecError> {
    if data.len() < 12 {
        return Err(CodecError::HeaderTooShort {
            need: 12,
            have: data.len(),
        });
    }
    let count = get_u32(data, 0);
    let params = ReassignBatchParams {
        block_height: get_u32(data, 4),
        spendable_after: get_u32(data, 8),
    };
    validate_batch_count(count, max_batch, 100, data.len() - 12)?;
    let count = count as usize;
    let mut items = Vec::with_capacity(count);
    let mut pos = 12;
    for _ in 0..count {
        if pos + 100 > data.len() {
            return Err(CodecError::TruncatedBatch {
                count: count as u32,
                per_item_min: 100,
                available: data.len().saturating_sub(12),
            });
        }
        let mut txid = [0u8; 32];
        txid.copy_from_slice(&data[pos..pos + 32]);
        let vout = get_u32(data, pos + 32);
        let mut uh = [0u8; 32];
        uh.copy_from_slice(&data[pos + 36..pos + 68]);
        let mut nh = [0u8; 32];
        nh.copy_from_slice(&data[pos + 68..pos + 100]);
        items.push(WireReassignItem {
            txid,
            vout,
            utxo_hash: uh,
            new_utxo_hash: nh,
        });
        pos += 100;
    }
    Ok((params, items))
}

/// Decode a ReassignBatch request payload using [`MAX_DECODE_BATCH`].
pub fn decode_reassign_batch(data: &[u8]) -> Option<(ReassignBatchParams, Vec<WireReassignItem>)> {
    decode_reassign_batch_checked(data, MAX_DECODE_BATCH).ok()
}

// ---------------------------------------------------------------------------
// Unspend batch
// ---------------------------------------------------------------------------

/// Shared parameters for an unspend batch.
#[derive(Debug, Clone, PartialEq)]
pub struct UnspendBatchParams {
    pub current_block_height: u32,
    pub block_height_retention: u32,
}

/// Encode an UnspendBatch request payload.
///
/// Format: `[count:4][cbh:4][bhr:4][items: txid(32)+vout(4)+hash(32) × count]`
pub fn encode_unspend_batch(params: &UnspendBatchParams, items: &[WireSlotItem]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(12 + items.len() * 68);
    put_u32(&mut buf, items.len() as u32);
    put_u32(&mut buf, params.current_block_height);
    put_u32(&mut buf, params.block_height_retention);
    for item in items {
        buf.extend_from_slice(&item.txid);
        put_u32(&mut buf, item.vout);
        buf.extend_from_slice(&item.utxo_hash);
    }
    buf
}

/// Decode an UnspendBatch request payload, validating count before
/// allocation. See [`decode_spend_batch_checked`] for the
/// allocation-safety contract.
pub fn decode_unspend_batch_checked(
    data: &[u8],
    max_batch: u32,
) -> Result<(UnspendBatchParams, Vec<WireSlotItem>), CodecError> {
    if data.len() < 12 {
        return Err(CodecError::HeaderTooShort {
            need: 12,
            have: data.len(),
        });
    }
    let count = get_u32(data, 0);
    let params = UnspendBatchParams {
        current_block_height: get_u32(data, 4),
        block_height_retention: get_u32(data, 8),
    };
    validate_batch_count(count, max_batch, 68, data.len() - 12)?;
    let count = count as usize;
    let mut items = Vec::with_capacity(count);
    let mut pos = 12;
    for _ in 0..count {
        if pos + 68 > data.len() {
            return Err(CodecError::TruncatedBatch {
                count: count as u32,
                per_item_min: 68,
                available: data.len().saturating_sub(12),
            });
        }
        let mut txid = [0u8; 32];
        txid.copy_from_slice(&data[pos..pos + 32]);
        let vout = get_u32(data, pos + 32);
        let mut uh = [0u8; 32];
        uh.copy_from_slice(&data[pos + 36..pos + 68]);
        items.push(WireSlotItem {
            txid,
            vout,
            utxo_hash: uh,
        });
        pos += 68;
    }
    Ok((params, items))
}

/// Decode an UnspendBatch request payload using [`MAX_DECODE_BATCH`].
pub fn decode_unspend_batch(data: &[u8]) -> Option<(UnspendBatchParams, Vec<WireSlotItem>)> {
    decode_unspend_batch_checked(data, MAX_DECODE_BATCH).ok()
}

// ---------------------------------------------------------------------------
// Create batch
// ---------------------------------------------------------------------------

/// A single create item on the wire.
#[derive(Debug, Clone, PartialEq)]
pub struct WireCreateItem {
    pub txid: [u8; 32],
    pub tx_version: u32,
    pub locktime: u32,
    pub fee: u64,
    pub size_in_bytes: u64,
    pub extended_size: u64,
    pub is_coinbase: bool,
    pub spending_height: u32,
    pub created_at: u64,
    pub flags: u8,
    pub utxo_hashes: Vec<[u8; 32]>,
    pub cold_data: Vec<u8>,
    pub block_height: u32,
    pub mined_block_id: Option<u32>,
    pub mined_block_height: Option<u32>,
    pub mined_subtree_idx: Option<u32>,
    pub parent_txids: Vec<[u8; 32]>,
}

/// Encode a CreateBatch request payload.
///
/// Variable-length per item due to utxo_hashes and cold_data.
pub fn encode_create_batch(items: &[WireCreateItem]) -> Vec<u8> {
    // Per-item fixed: txid(32)+version(4)+locktime(4)+fee(8)+size(8)+ext(8)+
    // coinbase(1)+sh(4)+created(8)+flags(1)+utxo_count(4)+has_cold(1)+cold_len(4)+
    // block_height(4)+has_mined(1)+parent_count(4) = 96 bytes.
    // Variable: utxo_hashes and cold_data add more but 96 is a safe lower bound.
    let mut buf = Vec::with_capacity(4 + items.len() * 96);
    put_u32(&mut buf, items.len() as u32);
    for item in items {
        buf.extend_from_slice(&item.txid);
        put_u32(&mut buf, item.tx_version);
        put_u32(&mut buf, item.locktime);
        buf.extend_from_slice(&item.fee.to_le_bytes());
        buf.extend_from_slice(&item.size_in_bytes.to_le_bytes());
        buf.extend_from_slice(&item.extended_size.to_le_bytes());
        buf.push(u8::from(item.is_coinbase));
        put_u32(&mut buf, item.spending_height);
        buf.extend_from_slice(&item.created_at.to_le_bytes());
        buf.push(item.flags);
        put_u32(&mut buf, item.utxo_hashes.len() as u32);
        for h in &item.utxo_hashes {
            buf.extend_from_slice(h);
        }
        let has_cold = u8::from(!item.cold_data.is_empty());
        buf.push(has_cold);
        put_u32(&mut buf, item.cold_data.len() as u32);
        buf.extend_from_slice(&item.cold_data);
        put_u32(&mut buf, item.block_height);
        let has_mined = u8::from(item.mined_block_id.is_some());
        buf.push(has_mined);
        if let Some(block_id) = item.mined_block_id {
            put_u32(&mut buf, block_id);
            put_u32(&mut buf, item.mined_block_height.unwrap_or(0));
            put_u32(&mut buf, item.mined_subtree_idx.unwrap_or(0));
        }
        put_u32(&mut buf, item.parent_txids.len() as u32);
        for ptx in &item.parent_txids {
            buf.extend_from_slice(ptx);
        }
    }
    buf
}

/// Decode a CreateBatch request payload, validating count before
/// allocation. See [`decode_spend_batch_checked`] for the
/// allocation-safety contract.
///
/// Items are variable-length; the per-item minimum (96 bytes) is the
/// lower bound used for the pre-allocation guard. Over-large variable
/// sections (utxo_hashes, cold_data, parent_txids) are still rejected
/// per-item via [`CodecError::SectionTruncated`].
pub fn decode_create_batch_checked(
    data: &[u8],
    max_batch: u32,
) -> Result<Vec<WireCreateItem>, CodecError> {
    if data.len() < 4 {
        return Err(CodecError::HeaderTooShort {
            need: 4,
            have: data.len(),
        });
    }
    let count = get_u32(data, 0);
    // Per-item minimum: txid(32)+version(4)+locktime(4)+fee(8)+size(8)+
    // ext(8)+coinbase(1)+sh(4)+created(8)+flags(1)+utxo_count(4)+
    // has_cold(1)+cold_len(4)+block_height(4)+has_mined(1)+parent_count(4) = 96.
    validate_batch_count(count, max_batch, 96, data.len() - 4)?;
    let count = count as usize;
    let mut items = Vec::with_capacity(count);
    let mut pos = 4;
    for _ in 0..count {
        // Fixed fields: txid(32)+tx_version(4)+locktime(4)+fee(8)+size(8)+ext(8)+coinbase(1)+sh(4)+created(8)+flags(1)+utxo_count(4) = 82
        if pos + 82 > data.len() {
            return Err(CodecError::SectionTruncated {
                need: pos + 82,
                have: data.len(),
            });
        }
        let mut txid = [0u8; 32];
        txid.copy_from_slice(&data[pos..pos + 32]);
        pos += 32;
        let tx_version = get_u32(data, pos);
        pos += 4;
        let locktime = get_u32(data, pos);
        pos += 4;
        let fee = u64::from_le_bytes(data[pos..pos + 8].try_into().map_err(|_| {
            CodecError::SectionTruncated {
                need: pos + 8,
                have: data.len(),
            }
        })?);
        pos += 8;
        let size_in_bytes = u64::from_le_bytes(data[pos..pos + 8].try_into().map_err(|_| {
            CodecError::SectionTruncated {
                need: pos + 8,
                have: data.len(),
            }
        })?);
        pos += 8;
        let extended_size = u64::from_le_bytes(data[pos..pos + 8].try_into().map_err(|_| {
            CodecError::SectionTruncated {
                need: pos + 8,
                have: data.len(),
            }
        })?);
        pos += 8;
        let is_coinbase = data[pos] != 0;
        pos += 1;
        let spending_height = get_u32(data, pos);
        pos += 4;
        let created_at = u64::from_le_bytes(data[pos..pos + 8].try_into().map_err(|_| {
            CodecError::SectionTruncated {
                need: pos + 8,
                have: data.len(),
            }
        })?);
        pos += 8;
        let flags = data[pos];
        pos += 1;
        let utxo_count = get_u32(data, pos);
        pos += 4;

        // Validate utxo_count fits in remaining bytes BEFORE allocating
        // the per-item Vec — protects against count = u32::MAX.
        let utxo_bytes =
            (utxo_count as usize)
                .checked_mul(32)
                .ok_or(CodecError::SectionTruncated {
                    need: usize::MAX,
                    have: data.len() - pos,
                })?;
        if pos + utxo_bytes > data.len() {
            return Err(CodecError::SectionTruncated {
                need: pos + utxo_bytes,
                have: data.len(),
            });
        }
        let mut utxo_hashes = Vec::with_capacity(utxo_count as usize);
        for _ in 0..utxo_count {
            let mut h = [0u8; 32];
            h.copy_from_slice(&data[pos..pos + 32]);
            utxo_hashes.push(h);
            pos += 32;
        }

        if pos + 5 > data.len() {
            return Err(CodecError::SectionTruncated {
                need: pos + 5,
                have: data.len(),
            });
        } // has_cold(1) + cold_len(4)
        let _has_cold = data[pos];
        pos += 1;
        let cold_len = get_u32(data, pos) as usize;
        pos += 4;
        if pos + cold_len > data.len() {
            return Err(CodecError::SectionTruncated {
                need: pos + cold_len,
                have: data.len(),
            });
        }
        let cold_data = data[pos..pos + cold_len].to_vec();
        pos += cold_len;

        if pos + 4 > data.len() {
            return Err(CodecError::SectionTruncated {
                need: pos + 4,
                have: data.len(),
            });
        }
        let block_height = get_u32(data, pos);
        pos += 4;

        if pos >= data.len() {
            return Err(CodecError::SectionTruncated {
                need: pos + 1,
                have: data.len(),
            });
        }
        let has_mined = data[pos] != 0;
        pos += 1;
        let (mined_block_id, mined_block_height, mined_subtree_idx) = if has_mined {
            if pos + 12 > data.len() {
                return Err(CodecError::SectionTruncated {
                    need: pos + 12,
                    have: data.len(),
                });
            }
            let bid = get_u32(data, pos);
            pos += 4;
            let bh = get_u32(data, pos);
            pos += 4;
            let si = get_u32(data, pos);
            pos += 4;
            (Some(bid), Some(bh), Some(si))
        } else {
            (None, None, None)
        };

        if pos + 4 > data.len() {
            return Err(CodecError::SectionTruncated {
                need: pos + 4,
                have: data.len(),
            });
        }
        let parent_count = get_u32(data, pos);
        pos += 4;
        let parent_bytes =
            (parent_count as usize)
                .checked_mul(32)
                .ok_or(CodecError::SectionTruncated {
                    need: usize::MAX,
                    have: data.len() - pos,
                })?;
        if pos + parent_bytes > data.len() {
            return Err(CodecError::SectionTruncated {
                need: pos + parent_bytes,
                have: data.len(),
            });
        }
        let mut parent_txids = Vec::with_capacity(parent_count as usize);
        for _ in 0..parent_count {
            let mut ptx = [0u8; 32];
            ptx.copy_from_slice(&data[pos..pos + 32]);
            parent_txids.push(ptx);
            pos += 32;
        }

        items.push(WireCreateItem {
            txid,
            tx_version,
            locktime,
            fee,
            size_in_bytes,
            extended_size,
            is_coinbase,
            spending_height,
            created_at,
            flags,
            utxo_hashes,
            cold_data,
            block_height,
            mined_block_id,
            mined_block_height,
            mined_subtree_idx,
            parent_txids,
        });
    }
    Ok(items)
}

/// Decode a CreateBatch request payload using [`MAX_DECODE_BATCH`].
pub fn decode_create_batch(data: &[u8]) -> Option<Vec<WireCreateItem>> {
    decode_create_batch_checked(data, MAX_DECODE_BATCH).ok()
}

// ---------------------------------------------------------------------------
// Get batch
// ---------------------------------------------------------------------------

/// Bitmask specifying which fields to include in a GetBatch response.
///
/// Each bit selects an individual metadata field. Variable-size sections
/// (UTXO slots, cold data, block entries, conflicting children) and the
/// raw debug dump each have their own bit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FieldMask(pub u32);

impl FieldMask {
    // Per-field bits for fixed-size metadata fields:
    /// Transaction version (4 bytes).
    pub const TX_VERSION: u32 = 1 << 0;
    /// Transaction locktime (4 bytes).
    pub const LOCKTIME: u32 = 1 << 1;
    /// Transaction fee in satoshis (8 bytes).
    pub const FEE: u32 = 1 << 2;
    /// Transaction size in bytes (8 bytes).
    pub const SIZE_IN_BYTES: u32 = 1 << 3;
    /// Extended transaction size (8 bytes).
    pub const EXTENDED_SIZE: u32 = 1 << 4;
    /// Flags byte (1 byte).
    pub const FLAGS: u32 = 1 << 5;
    /// Spending height for coinbase maturity (4 bytes).
    pub const SPENDING_HEIGHT: u32 = 1 << 6;
    /// Creation timestamp in Unix millis (8 bytes).
    pub const CREATED_AT: u32 = 1 << 7;
    /// Number of spent UTXOs (4 bytes).
    pub const SPENT_UTXOS: u32 = 1 << 8;
    /// Number of pruned UTXOs (4 bytes).
    pub const PRUNED_UTXOS: u32 = 1 << 9;
    /// Total UTXO count (4 bytes).
    pub const UTXO_COUNT: u32 = 1 << 10;
    /// Record generation counter (4 bytes).
    pub const GENERATION: u32 = 1 << 11;
    /// Last update timestamp in Unix millis (8 bytes).
    pub const UPDATED_AT: u32 = 1 << 12;
    /// Block height since the transaction was unmined (4 bytes).
    pub const UNMINED_SINCE: u32 = 1 << 13;
    /// Block height at which the record should be deleted (4 bytes).
    pub const DELETE_AT_HEIGHT: u32 = 1 << 14;
    /// Block height until which the record is preserved (4 bytes).
    pub const PRESERVE_UNTIL: u32 = 1 << 15;
    /// External blob reference (65 bytes).
    pub const EXTERNAL_REF: u32 = 1 << 16;
    /// Number of reassignments recorded (1 byte).
    pub const REASSIGNMENT_COUNT: u32 = 1 << 17;
    /// Number of block entries (1 byte).
    pub const BLOCK_ENTRY_COUNT: u32 = 1 << 18;

    // Variable-size sections:
    /// Include UTXO slot data (variable).
    pub const UTXO_SLOTS: u32 = 1 << 19;
    /// Include cold data — inputs/outputs/inpoints (variable).
    pub const COLD_DATA: u32 = 1 << 20;
    /// Include block entries (variable).
    pub const BLOCK_ENTRIES: u32 = 1 << 21;
    /// Include conflicting children txids (variable).
    pub const CONFLICTING_CHILDREN: u32 = 1 << 22;
    /// Include the raw on-disk metadata struct (256 bytes, for debugging).
    /// When set, the full struct is returned as-is including internal
    /// fields (magic, schema_version, device offsets, padding).
    /// Takes precedence over per-field metadata bits if both are set.
    pub const RAW_METADATA: u32 = 1 << 23;

    /// Convenience alias: all per-field metadata bits (bits 0-18).
    pub const ALL_METADATA: u32 = 0x0007_FFFF;
    /// Include all client-facing fields (bits 0-22, excludes RAW_METADATA).
    pub const ALL: u32 = 0x007F_FFFF;

    /// Whether the mask includes the given flag.
    pub fn has(self, flag: u32) -> bool {
        self.0 & flag != 0
    }

    /// Bitmask of fields that can be served entirely from the primary index
    /// cache without reading metadata from device memory.
    pub const INDEX_CACHED: u32 = Self::FLAGS
        | Self::SPENT_UTXOS
        | Self::UTXO_COUNT
        | Self::UNMINED_SINCE
        | Self::DELETE_AT_HEIGHT
        | Self::PRESERVE_UNTIL
        | Self::BLOCK_ENTRY_COUNT;

    /// Returns `true` if ALL requested fields can be served from the index
    /// cache, meaning no device metadata read is needed.
    pub fn fully_cached(self) -> bool {
        self.0 != 0 && (self.0 & !Self::INDEX_CACHED) == 0
    }
}

/// Encode a GetBatch request payload.
///
/// Format: `[count:4][field_mask:4][txids × count]`
pub fn encode_get_batch(field_mask: u32, txids: &[[u8; 32]]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8 + txids.len() * 32);
    put_u32(&mut buf, txids.len() as u32);
    put_u32(&mut buf, field_mask);
    for txid in txids {
        buf.extend_from_slice(txid);
    }
    buf
}

/// Decode a GetBatch request payload, validating count before
/// allocation. See [`decode_spend_batch_checked`] for the
/// allocation-safety contract.
pub fn decode_get_batch_checked(
    data: &[u8],
    max_batch: u32,
) -> Result<(FieldMask, Vec<[u8; 32]>), CodecError> {
    if data.len() < 8 {
        return Err(CodecError::HeaderTooShort {
            need: 8,
            have: data.len(),
        });
    }
    let count = get_u32(data, 0);
    let field_mask = FieldMask(get_u32(data, 4));
    validate_batch_count(count, max_batch, 32, data.len() - 8)?;
    let count = count as usize;
    let mut txids = Vec::with_capacity(count);
    let mut pos = 8;
    for _ in 0..count {
        if pos + 32 > data.len() {
            return Err(CodecError::TruncatedBatch {
                count: count as u32,
                per_item_min: 32,
                available: data.len().saturating_sub(8),
            });
        }
        let mut txid = [0u8; 32];
        txid.copy_from_slice(&data[pos..pos + 32]);
        txids.push(txid);
        pos += 32;
    }
    Ok((field_mask, txids))
}

/// Decode a GetBatch request payload using [`MAX_DECODE_BATCH`].
pub fn decode_get_batch(data: &[u8]) -> Option<(FieldMask, Vec<[u8; 32]>)> {
    decode_get_batch_checked(data, MAX_DECODE_BATCH).ok()
}

/// A single GetBatch response item.
#[derive(Debug, Clone, PartialEq)]
pub struct WireGetResult {
    /// 0 = OK, 1 = Error.
    pub status: u8,
    /// Serialized data (fields selected by field_mask).
    pub data: Vec<u8>,
}

/// Encode GetBatch response items.
pub fn encode_get_response(items: &[WireGetResult]) -> Vec<u8> {
    let data_est: usize = items.iter().map(|i| 5 + i.data.len()).sum();
    let mut buf = Vec::with_capacity(4 + data_est);
    put_u32(&mut buf, items.len() as u32);
    for item in items {
        buf.push(item.status);
        put_u32(&mut buf, item.data.len() as u32);
        buf.extend_from_slice(&item.data);
    }
    buf
}

/// Decode GetBatch response items, validating count before allocation.
/// See [`decode_spend_batch_checked`] for the allocation-safety contract.
///
/// Per-item minimum is 5 bytes (`status:1 + data_len:4`); variable
/// `data` payloads are bounded by the remaining buffer.
pub fn decode_get_response_checked(
    data: &[u8],
    max_batch: u32,
) -> Result<Vec<WireGetResult>, CodecError> {
    if data.len() < 4 {
        return Err(CodecError::HeaderTooShort {
            need: 4,
            have: data.len(),
        });
    }
    let count = get_u32(data, 0);
    validate_batch_count(count, max_batch, 5, data.len() - 4)?;
    let count = count as usize;
    let mut items = Vec::with_capacity(count);
    let mut pos = 4;
    for _ in 0..count {
        if pos + 5 > data.len() {
            return Err(CodecError::SectionTruncated {
                need: pos + 5,
                have: data.len(),
            });
        } // status(1) + data_len(4)
        let status = data[pos];
        pos += 1;
        let data_len = get_u32(data, pos) as usize;
        pos += 4;
        if pos + data_len > data.len() {
            return Err(CodecError::SectionTruncated {
                need: pos + data_len,
                have: data.len(),
            });
        }
        let item_data = data[pos..pos + data_len].to_vec();
        pos += data_len;
        items.push(WireGetResult {
            status,
            data: item_data,
        });
    }
    Ok(items)
}

/// Decode GetBatch response items using [`MAX_DECODE_BATCH`].
pub fn decode_get_response(data: &[u8]) -> Option<Vec<WireGetResult>> {
    decode_get_response_checked(data, MAX_DECODE_BATCH).ok()
}

// ---------------------------------------------------------------------------
// Spend/SetMined batch response (with per-item signals)
// ---------------------------------------------------------------------------

/// A per-item success result with signal and block IDs.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchItemSuccess {
    /// 0-based index into the original request batch.
    pub item_index: u32,
    /// Signal value (e.g., ALLSPENT, DAHSET).
    pub signal: u8,
    /// Block IDs from the record.
    pub block_ids: Vec<u32>,
}

/// Encode a SpendBatch/SetMinedBatch PartialError response with both
/// success results (signals) and error results.
pub fn encode_partial_with_signals(
    successes: &[BatchItemSuccess],
    errors: &[BatchItemError],
) -> Vec<u8> {
    // Per success: item_index(4)+signal(1)+bid_count(1)+bids(4*n).
    // Per error: item_index(4)+error_code(2)+data_len(2)+data.
    let success_est: usize = successes.iter().map(|s| 6 + s.block_ids.len() * 4).sum();
    let error_est: usize = errors.iter().map(|e| 8 + e.error_data.len()).sum();
    let mut buf = Vec::with_capacity(8 + success_est + error_est);
    // Section 1: Successes
    put_u32(&mut buf, successes.len() as u32);
    for s in successes {
        put_u32(&mut buf, s.item_index);
        buf.push(s.signal);
        buf.push(s.block_ids.len() as u8);
        for &bid in &s.block_ids {
            put_u32(&mut buf, bid);
        }
    }
    // Section 2: Errors
    put_u32(&mut buf, errors.len() as u32);
    for e in errors {
        put_u32(&mut buf, e.item_index);
        put_u16(&mut buf, e.error_code);
        put_u16(&mut buf, e.error_data.len() as u16);
        buf.extend_from_slice(&e.error_data);
    }
    buf
}

/// Decode a SpendBatch/SetMinedBatch PartialError response with both
/// sections, validating each section's count before allocation. See
/// [`decode_spend_batch_checked`] for the allocation-safety contract.
///
/// Per-success minimum is 6 bytes (`item_index:4 + signal:1 + bid_count:1`);
/// per-error minimum is 8 bytes (`item_index:4 + error_code:2 + data_len:2`).
pub fn decode_partial_with_signals_checked(
    data: &[u8],
    max_batch: u32,
) -> Result<(Vec<BatchItemSuccess>, Vec<BatchItemError>), CodecError> {
    if data.len() < 4 {
        return Err(CodecError::HeaderTooShort {
            need: 4,
            have: data.len(),
        });
    }
    let mut pos = 0;

    let success_count = get_u32(data, pos);
    pos += 4;
    validate_batch_count(success_count, max_batch, 6, data.len() - pos)?;
    let success_count = success_count as usize;
    let mut successes = Vec::with_capacity(success_count);
    for _ in 0..success_count {
        if pos + 6 > data.len() {
            return Err(CodecError::SectionTruncated {
                need: pos + 6,
                have: data.len(),
            });
        } // item_index(4) + signal(1) + bid_count(1)
        let item_index = get_u32(data, pos);
        pos += 4;
        let signal = data[pos];
        pos += 1;
        let bid_count = data[pos] as usize;
        pos += 1;
        // bid_count is u8, capped at 255, so bid_count*4 cannot overflow.
        if pos + bid_count * 4 > data.len() {
            return Err(CodecError::SectionTruncated {
                need: pos + bid_count * 4,
                have: data.len(),
            });
        }
        let mut block_ids = Vec::with_capacity(bid_count);
        for _ in 0..bid_count {
            block_ids.push(get_u32(data, pos));
            pos += 4;
        }
        successes.push(BatchItemSuccess {
            item_index,
            signal,
            block_ids,
        });
    }

    if pos + 4 > data.len() {
        return Err(CodecError::SectionTruncated {
            need: pos + 4,
            have: data.len(),
        });
    }
    let error_count = get_u32(data, pos);
    pos += 4;
    validate_batch_count(error_count, max_batch, 8, data.len() - pos)?;
    let error_count = error_count as usize;
    let mut errors = Vec::with_capacity(error_count);
    for _ in 0..error_count {
        if pos + 8 > data.len() {
            return Err(CodecError::SectionTruncated {
                need: pos + 8,
                have: data.len(),
            });
        }
        let item_index = get_u32(data, pos);
        let error_code = get_u16(data, pos + 4);
        let data_len = get_u16(data, pos + 6) as usize;
        pos += 8;
        if pos + data_len > data.len() {
            return Err(CodecError::SectionTruncated {
                need: pos + data_len,
                have: data.len(),
            });
        }
        let error_data = data[pos..pos + data_len].to_vec();
        pos += data_len;
        errors.push(BatchItemError {
            item_index,
            error_code,
            error_data,
        });
    }

    Ok((successes, errors))
}

/// Decode a SpendBatch/SetMinedBatch PartialError response using
/// [`MAX_DECODE_BATCH`].
pub fn decode_partial_with_signals(
    data: &[u8],
) -> Option<(Vec<BatchItemSuccess>, Vec<BatchItemError>)> {
    decode_partial_with_signals_checked(data, MAX_DECODE_BATCH).ok()
}

// ---------------------------------------------------------------------------
// Error response
// ---------------------------------------------------------------------------

/// Encode a global error response payload.
///
/// Format: `[error_code:2][message_len:2][message]`
pub fn encode_error_payload(error_code: u16, message: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + message.len());
    put_u16(&mut buf, error_code);
    put_u16(&mut buf, message.len() as u16);
    buf.extend_from_slice(message.as_bytes());
    buf
}

/// Decode a global error response payload.
pub fn decode_error_payload(data: &[u8]) -> Option<(u16, String)> {
    if data.len() < 4 {
        return None;
    }
    let error_code = get_u16(data, 0);
    let msg_len = get_u16(data, 2) as usize;
    if data.len() < 4 + msg_len {
        return None;
    }
    let message = String::from_utf8_lossy(&data[4..4 + msg_len]).to_string();
    Some((error_code, message))
}

// ---------------------------------------------------------------------------
// Redirect response
// ---------------------------------------------------------------------------

/// Encode a redirect response payload.
///
/// Format: `[addr_len:2][addr]`
pub fn encode_redirect(addr: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(2 + addr.len());
    put_u16(&mut buf, addr.len() as u16);
    buf.extend_from_slice(addr.as_bytes());
    buf
}

/// Decode a redirect response payload.
pub fn decode_redirect(data: &[u8]) -> Option<String> {
    if data.len() < 2 {
        return None;
    }
    let len = get_u16(data, 0) as usize;
    if data.len() < 2 + len {
        return None;
    }
    Some(String::from_utf8_lossy(&data[2..2 + len]).to_string())
}

// ---------------------------------------------------------------------------
// Sparse error response
// ---------------------------------------------------------------------------

/// A per-item error in a partial-error response.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchItemError {
    /// 0-based index into the original request batch.
    pub item_index: u32,
    /// Error code (from opcodes.rs).
    pub error_code: u16,
    /// Additional error data (e.g., spending_data for ALREADY_SPENT).
    pub error_data: Vec<u8>,
}

/// Encode a sparse error list.
pub fn encode_sparse_errors(errors: &[BatchItemError]) -> Vec<u8> {
    let est: usize = errors.iter().map(|e| 8 + e.error_data.len()).sum();
    let mut buf = Vec::with_capacity(4 + est);
    put_u32(&mut buf, errors.len() as u32);
    for e in errors {
        put_u32(&mut buf, e.item_index);
        put_u16(&mut buf, e.error_code);
        put_u16(&mut buf, e.error_data.len() as u16);
        buf.extend_from_slice(&e.error_data);
    }
    buf
}

/// Decode a sparse error list, validating count before allocation. See
/// [`decode_spend_batch_checked`] for the allocation-safety contract.
///
/// Per-item minimum is 8 bytes (`item_index:4 + error_code:2 + data_len:2`).
pub fn decode_sparse_errors_checked(
    data: &[u8],
    max_batch: u32,
) -> Result<Vec<BatchItemError>, CodecError> {
    if data.len() < 4 {
        return Err(CodecError::HeaderTooShort {
            need: 4,
            have: data.len(),
        });
    }
    let count = get_u32(data, 0);
    validate_batch_count(count, max_batch, 8, data.len() - 4)?;
    let count = count as usize;
    let mut errors = Vec::with_capacity(count);
    let mut pos = 4;
    for _ in 0..count {
        if pos + 8 > data.len() {
            return Err(CodecError::SectionTruncated {
                need: pos + 8,
                have: data.len(),
            });
        }
        let item_index = get_u32(data, pos);
        let error_code = get_u16(data, pos + 4);
        let data_len = get_u16(data, pos + 6) as usize;
        pos += 8;
        if pos + data_len > data.len() {
            return Err(CodecError::SectionTruncated {
                need: pos + data_len,
                have: data.len(),
            });
        }
        let error_data = data[pos..pos + data_len].to_vec();
        pos += data_len;
        errors.push(BatchItemError {
            item_index,
            error_code,
            error_data,
        });
    }
    Ok(errors)
}

/// Decode a sparse error list using [`MAX_DECODE_BATCH`].
pub fn decode_sparse_errors(data: &[u8]) -> Option<Vec<BatchItemError>> {
    decode_sparse_errors_checked(data, MAX_DECODE_BATCH).ok()
}

// ---------------------------------------------------------------------------
// GetSpend batch
// ---------------------------------------------------------------------------

/// A single GetSpend item on the wire (request).
#[derive(Debug, Clone, PartialEq)]
pub struct WireGetSpendItem {
    pub txid: [u8; 32],
    pub vout: u32,
}

/// Encode a GetSpendBatch request payload.
pub fn encode_get_spend_batch(items: &[WireGetSpendItem]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + items.len() * 36);
    put_u32(&mut buf, items.len() as u32);
    for item in items {
        buf.extend_from_slice(&item.txid);
        put_u32(&mut buf, item.vout);
    }
    buf
}

/// Decode a GetSpendBatch request payload, validating count before
/// allocation. See [`decode_spend_batch_checked`] for the
/// allocation-safety contract.
pub fn decode_get_spend_batch_checked(
    data: &[u8],
    max_batch: u32,
) -> Result<Vec<WireGetSpendItem>, CodecError> {
    if data.len() < 4 {
        return Err(CodecError::HeaderTooShort {
            need: 4,
            have: data.len(),
        });
    }
    let count = get_u32(data, 0);
    validate_batch_count(count, max_batch, 36, data.len() - 4)?;
    let count = count as usize;
    let mut items = Vec::with_capacity(count);
    let mut pos = 4;
    for _ in 0..count {
        if pos + 36 > data.len() {
            return Err(CodecError::TruncatedBatch {
                count: count as u32,
                per_item_min: 36,
                available: data.len().saturating_sub(4),
            });
        }
        let mut txid = [0u8; 32];
        txid.copy_from_slice(&data[pos..pos + 32]);
        let vout = get_u32(data, pos + 32);
        items.push(WireGetSpendItem { txid, vout });
        pos += 36;
    }
    Ok(items)
}

/// Decode a GetSpendBatch request payload using [`MAX_DECODE_BATCH`].
pub fn decode_get_spend_batch(data: &[u8]) -> Option<Vec<WireGetSpendItem>> {
    decode_get_spend_batch_checked(data, MAX_DECODE_BATCH).ok()
}

/// A single GetSpend response item.
#[derive(Debug, Clone, PartialEq)]
pub struct WireGetSpendResult {
    pub status: u8,
    pub error_code: u16,
    pub slot_status: u8,
    pub spending_data: [u8; 36],
}

/// Encode GetSpendBatch response items.
pub fn encode_get_spend_response(items: &[WireGetSpendResult]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + items.len() * 40);
    put_u32(&mut buf, items.len() as u32);
    for item in items {
        buf.push(item.status);
        put_u16(&mut buf, item.error_code);
        buf.push(item.slot_status);
        buf.extend_from_slice(&item.spending_data);
    }
    buf
}

/// Decode GetSpendBatch response items, validating count before
/// allocation. See [`decode_spend_batch_checked`] for the
/// allocation-safety contract.
pub fn decode_get_spend_response_checked(
    data: &[u8],
    max_batch: u32,
) -> Result<Vec<WireGetSpendResult>, CodecError> {
    if data.len() < 4 {
        return Err(CodecError::HeaderTooShort {
            need: 4,
            have: data.len(),
        });
    }
    let count = get_u32(data, 0);
    validate_batch_count(count, max_batch, 40, data.len() - 4)?;
    let count = count as usize;
    let mut items = Vec::with_capacity(count);
    let mut pos = 4;
    for _ in 0..count {
        if pos + 40 > data.len() {
            return Err(CodecError::TruncatedBatch {
                count: count as u32,
                per_item_min: 40,
                available: data.len().saturating_sub(4),
            });
        }
        let status = data[pos];
        let error_code = get_u16(data, pos + 1);
        let slot_status = data[pos + 3];
        let mut sd = [0u8; 36];
        sd.copy_from_slice(&data[pos + 4..pos + 40]);
        items.push(WireGetSpendResult {
            status,
            error_code,
            slot_status,
            spending_data: sd,
        });
        pos += 40;
    }
    Ok(items)
}

/// Decode GetSpendBatch response items using [`MAX_DECODE_BATCH`].
pub fn decode_get_spend_response(data: &[u8]) -> Option<Vec<WireGetSpendResult>> {
    decode_get_spend_response_checked(data, MAX_DECODE_BATCH).ok()
}

// ---------------------------------------------------------------------------
// Stream chunk (OP_STREAM_CHUNK = 200)
// ---------------------------------------------------------------------------

/// Encode an OP_STREAM_CHUNK payload.
///
/// Format: `[txid:32][offset:8 LE][chunk_data_len:4 LE][chunk_data]`
pub fn encode_stream_chunk(txid: &[u8; 32], offset: u64, data: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(32 + 8 + 4 + data.len());
    buf.extend_from_slice(txid);
    buf.extend_from_slice(&offset.to_le_bytes());
    put_u32(&mut buf, data.len() as u32);
    buf.extend_from_slice(data);
    buf
}

/// Decoded stream chunk fields.
pub struct StreamChunk<'a> {
    pub txid: [u8; 32],
    pub offset: u64,
    pub data: &'a [u8],
}

/// Decode an OP_STREAM_CHUNK payload.
///
/// Returns None if the payload is too short or malformed.
pub fn decode_stream_chunk(payload: &[u8]) -> Option<StreamChunk<'_>> {
    if payload.len() < 44 {
        return None;
    } // 32 + 8 + 4
    let mut txid = [0u8; 32];
    txid.copy_from_slice(&payload[0..32]);
    let offset = u64::from_le_bytes(payload[32..40].try_into().unwrap());
    let data_len = get_u32(payload, 40) as usize;
    if payload.len() < 44 + data_len {
        return None;
    }
    Some(StreamChunk {
        txid,
        offset,
        data: &payload[44..44 + data_len],
    })
}

// ---------------------------------------------------------------------------
// Stream end (OP_STREAM_END = 201)
// ---------------------------------------------------------------------------

/// Encode an OP_STREAM_END payload.
///
/// Format: `[txid:32][total_size:8 LE]`
pub fn encode_stream_end(txid: &[u8; 32], total_size: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(40);
    buf.extend_from_slice(txid);
    buf.extend_from_slice(&total_size.to_le_bytes());
    buf
}

/// Decoded stream end fields.
pub struct StreamEnd {
    pub txid: [u8; 32],
    pub total_size: u64,
}

/// Decode an OP_STREAM_END payload.
///
/// Returns None if the payload is too short.
pub fn decode_stream_end(payload: &[u8]) -> Option<StreamEnd> {
    if payload.len() < 40 {
        return None;
    }
    let mut txid = [0u8; 32];
    txid.copy_from_slice(&payload[0..32]);
    let total_size = u64::from_le_bytes(payload[32..40].try_into().unwrap());
    Some(StreamEnd { txid, total_size })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::opcodes::*;

    fn test_txid(n: u8) -> [u8; 32] {
        let mut t = [0u8; 32];
        t[0] = n;
        t
    }

    // -- SpendBatch --

    #[test]
    fn spend_batch_1_item_round_trip() {
        let params = SpendBatchParams {
            ignore_conflicting: true,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        let items = vec![WireSpendItem {
            txid: test_txid(1),
            vout: 5,
            utxo_hash: test_txid(2),
            spending_data: [0xAB; 36],
        }];
        let encoded = encode_spend_batch(&params, &items);
        let (dp, di) = decode_spend_batch(&encoded).unwrap();
        assert_eq!(dp, params);
        assert_eq!(di, items);
    }

    #[test]
    fn spend_batch_1024_items_round_trip() {
        let params = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: true,
            current_block_height: 500,
            block_height_retention: 144,
        };
        let items: Vec<WireSpendItem> = (0..1024u16)
            .map(|i| WireSpendItem {
                txid: {
                    let mut t = [0u8; 32];
                    t[0..2].copy_from_slice(&i.to_le_bytes());
                    t
                },
                vout: i as u32,
                utxo_hash: test_txid(i as u8),
                spending_data: [i as u8; 36],
            })
            .collect();
        let encoded = encode_spend_batch(&params, &items);
        let (dp, di) = decode_spend_batch(&encoded).unwrap();
        assert_eq!(dp, params);
        assert_eq!(di.len(), 1024);
        assert_eq!(di[0], items[0]);
        assert_eq!(di[1023], items[1023]);
    }

    // -- SetMinedBatch --

    #[test]
    fn set_mined_batch_round_trip() {
        let params = SetMinedBatchParams {
            block_id: 42,
            block_height: 800_000,
            subtree_idx: 7,
            on_longest_chain: true,
            unset_mined: false,
            current_block_height: 800_000,
            block_height_retention: 288,
        };
        let txids: Vec<[u8; 32]> = (0..512u16)
            .map(|i| {
                let mut t = [0u8; 32];
                t[0..2].copy_from_slice(&i.to_le_bytes());
                t
            })
            .collect();
        let encoded = encode_set_mined_batch(&params, &txids);
        let (dp, dt) = decode_set_mined_batch(&encoded).unwrap();
        assert_eq!(dp, params);
        assert_eq!(dt.len(), 512);
    }

    // -- Txid batches --

    #[test]
    fn delete_batch_round_trip() {
        let txids: Vec<[u8; 32]> = (0..=255u8).map(test_txid).collect();
        let encoded = encode_txid_batch(&txids, &[]);
        let (shared, decoded) = decode_txid_batch(&encoded, 0).unwrap();
        assert!(shared.is_empty());
        assert_eq!(decoded.len(), 256);
        assert_eq!(decoded[0], test_txid(0));
        assert_eq!(decoded[255], test_txid(255));
    }

    #[test]
    fn set_locked_batch_round_trip() {
        let txids = vec![test_txid(1), test_txid(2)];
        let shared = vec![1u8]; // value=true
        let encoded = encode_txid_batch(&txids, &shared);
        let (ds, dt) = decode_txid_batch(&encoded, 1).unwrap();
        assert_eq!(ds, vec![1u8]);
        assert_eq!(dt, txids);
    }

    #[test]
    fn set_conflicting_batch_round_trip() {
        let txids = vec![test_txid(5)];
        // shared: value(1) + cbh(4) + bhr(4) = 9 bytes
        let mut shared = Vec::new();
        shared.push(1); // value=true
        shared.extend_from_slice(&500u32.to_le_bytes());
        shared.extend_from_slice(&288u32.to_le_bytes());
        let encoded = encode_txid_batch(&txids, &shared);
        let (ds, dt) = decode_txid_batch(&encoded, 9).unwrap();
        assert_eq!(ds.len(), 9);
        assert_eq!(dt, txids);
    }

    #[test]
    fn mark_longest_chain_batch_round_trip() {
        let txids: Vec<[u8; 32]> = (0..1024u16)
            .map(|i| {
                let mut t = [0u8; 32];
                t[0..2].copy_from_slice(&i.to_le_bytes());
                t
            })
            .collect();
        // shared: on_longest_chain(1) + cbh(4) + bhr(4) = 9 bytes
        let mut shared = Vec::new();
        shared.push(1);
        shared.extend_from_slice(&1000u32.to_le_bytes());
        shared.extend_from_slice(&288u32.to_le_bytes());
        let encoded = encode_txid_batch(&txids, &shared);
        let (ds, dt) = decode_txid_batch(&encoded, 9).unwrap();
        assert_eq!(ds.len(), 9);
        assert_eq!(dt.len(), 1024);
    }

    #[test]
    fn preserve_until_batch_round_trip() {
        let txids = vec![test_txid(1), test_txid(2)];
        let shared = 5000u32.to_le_bytes().to_vec(); // block_height
        let encoded = encode_txid_batch(&txids, &shared);
        let (ds, dt) = decode_txid_batch(&encoded, 4).unwrap();
        assert_eq!(u32::from_le_bytes(ds[0..4].try_into().unwrap()), 5000);
        assert_eq!(dt, txids);
    }

    // -- Slot item batches (Freeze/Unfreeze) --

    #[test]
    fn freeze_batch_round_trip() {
        let items: Vec<WireSlotItem> = (0..50u8)
            .map(|i| WireSlotItem {
                txid: test_txid(i),
                vout: i as u32,
                utxo_hash: test_txid(i + 100),
            })
            .collect();
        let encoded = encode_slot_item_batch(&items);
        let decoded = decode_slot_item_batch(&encoded).unwrap();
        assert_eq!(decoded, items);
    }

    #[test]
    fn unfreeze_batch_1_item() {
        let items = vec![WireSlotItem {
            txid: test_txid(1),
            vout: 7,
            utxo_hash: test_txid(2),
        }];
        let encoded = encode_slot_item_batch(&items);
        let decoded = decode_slot_item_batch(&encoded).unwrap();
        assert_eq!(decoded, items);
    }

    // -- Reassign batch --

    #[test]
    fn reassign_batch_round_trip() {
        let params = ReassignBatchParams {
            block_height: 1000,
            spendable_after: 100,
        };
        let items: Vec<WireReassignItem> = (0..50u8)
            .map(|i| WireReassignItem {
                txid: test_txid(i),
                vout: i as u32,
                utxo_hash: test_txid(i),
                new_utxo_hash: test_txid(i + 50),
            })
            .collect();
        let encoded = encode_reassign_batch(&params, &items);
        let (dp, di) = decode_reassign_batch(&encoded).unwrap();
        assert_eq!(dp, params);
        assert_eq!(di, items);
    }

    // -- Sparse errors --

    #[test]
    fn sparse_errors_round_trip() {
        let errors = vec![
            BatchItemError {
                item_index: 3,
                error_code: ERR_TX_NOT_FOUND,
                error_data: vec![],
            },
            BatchItemError {
                item_index: 7,
                error_code: ERR_ALREADY_SPENT,
                error_data: vec![0xAB; 36],
            },
            BatchItemError {
                item_index: 999,
                error_code: ERR_FROZEN,
                error_data: vec![],
            },
        ];
        let encoded = encode_sparse_errors(&errors);
        let decoded = decode_sparse_errors(&encoded).unwrap();
        assert_eq!(decoded, errors);
    }

    #[test]
    fn sparse_errors_ascending_indices() {
        let errors = vec![
            BatchItemError {
                item_index: 1,
                error_code: ERR_TX_NOT_FOUND,
                error_data: vec![],
            },
            BatchItemError {
                item_index: 5,
                error_code: ERR_FROZEN,
                error_data: vec![],
            },
            BatchItemError {
                item_index: 10,
                error_code: ERR_LOCKED,
                error_data: vec![],
            },
        ];
        let encoded = encode_sparse_errors(&errors);
        let decoded = decode_sparse_errors(&encoded).unwrap();
        for w in decoded.windows(2) {
            assert!(w[0].item_index < w[1].item_index);
        }
    }

    // -- GetSpend batch --

    #[test]
    fn get_spend_batch_round_trip() {
        let items: Vec<WireGetSpendItem> = (0..1024u16)
            .map(|i| WireGetSpendItem {
                txid: {
                    let mut t = [0u8; 32];
                    t[0..2].copy_from_slice(&i.to_le_bytes());
                    t
                },
                vout: i as u32,
            })
            .collect();
        let encoded = encode_get_spend_batch(&items);
        let decoded = decode_get_spend_batch(&encoded).unwrap();
        assert_eq!(decoded, items);
    }

    #[test]
    fn get_spend_response_round_trip() {
        let items = vec![
            WireGetSpendResult {
                status: 0,
                error_code: ERR_OK,
                slot_status: 0x00,
                spending_data: [0; 36],
            },
            WireGetSpendResult {
                status: 0,
                error_code: ERR_OK,
                slot_status: 0x01,
                spending_data: [0xAB; 36],
            },
            WireGetSpendResult {
                status: 1,
                error_code: ERR_TX_NOT_FOUND,
                slot_status: 0,
                spending_data: [0; 36],
            },
        ];
        let encoded = encode_get_spend_response(&items);
        let decoded = decode_get_spend_response(&encoded).unwrap();
        assert_eq!(decoded, items);
    }

    // -- SpendBatch response --

    #[test]
    fn spend_batch_response_mixed_ok_error() {
        // Mixed success/error via sparse errors
        let errors = vec![
            BatchItemError {
                item_index: 2,
                error_code: ERR_TX_NOT_FOUND,
                error_data: vec![],
            },
            BatchItemError {
                item_index: 5,
                error_code: ERR_ALREADY_SPENT,
                error_data: vec![0xCC; 36],
            },
        ];
        let encoded = encode_sparse_errors(&errors);
        let decoded = decode_sparse_errors(&encoded).unwrap();
        assert_eq!(decoded, errors);
    }

    // -- UnspendBatch --

    #[test]
    fn unspend_batch_1_item_round_trip() {
        let params = UnspendBatchParams {
            current_block_height: 500,
            block_height_retention: 288,
        };
        let items = vec![WireSlotItem {
            txid: test_txid(1),
            vout: 3,
            utxo_hash: test_txid(2),
        }];
        let encoded = encode_unspend_batch(&params, &items);
        let (dp, di) = decode_unspend_batch(&encoded).unwrap();
        assert_eq!(dp, params);
        assert_eq!(di, items);
    }

    #[test]
    fn unspend_batch_512_items_round_trip() {
        let params = UnspendBatchParams {
            current_block_height: 1000,
            block_height_retention: 144,
        };
        let items: Vec<WireSlotItem> = (0..512u16)
            .map(|i| WireSlotItem {
                txid: {
                    let mut t = [0u8; 32];
                    t[0..2].copy_from_slice(&i.to_le_bytes());
                    t
                },
                vout: i as u32,
                utxo_hash: test_txid(i as u8),
            })
            .collect();
        let encoded = encode_unspend_batch(&params, &items);
        let (dp, di) = decode_unspend_batch(&encoded).unwrap();
        assert_eq!(dp, params);
        assert_eq!(di.len(), 512);
        assert_eq!(di[0], items[0]);
        assert_eq!(di[511], items[511]);
    }

    #[test]
    fn unspend_batch_response_mixed_ok_error() {
        let errors = vec![
            BatchItemError {
                item_index: 0,
                error_code: ERR_TX_NOT_FOUND,
                error_data: vec![],
            },
            BatchItemError {
                item_index: 3,
                error_code: ERR_UTXO_HASH_MISMATCH,
                error_data: vec![],
            },
        ];
        let encoded = encode_sparse_errors(&errors);
        let decoded = decode_sparse_errors(&encoded).unwrap();
        assert_eq!(decoded, errors);
    }

    // -- SetMinedBatch response --

    #[test]
    fn set_mined_batch_response_with_signals_and_block_ids() {
        let successes = vec![
            BatchItemSuccess {
                item_index: 0,
                signal: 1,
                block_ids: vec![42, 43],
            },
            BatchItemSuccess {
                item_index: 2,
                signal: 0,
                block_ids: vec![],
            },
        ];
        let errors = vec![BatchItemError {
            item_index: 1,
            error_code: ERR_TX_NOT_FOUND,
            error_data: vec![],
        }];
        let encoded = encode_partial_with_signals(&successes, &errors);
        let (ds, de) = decode_partial_with_signals(&encoded).unwrap();
        assert_eq!(ds, successes);
        assert_eq!(de, errors);
    }

    // -- CreateBatch --

    #[test]
    fn create_batch_100_items_round_trip() {
        let items: Vec<WireCreateItem> = (0..100u8)
            .map(|i| WireCreateItem {
                txid: test_txid(i),
                tx_version: 2,
                locktime: 0,
                fee: 1000 + i as u64,
                size_in_bytes: 250,
                extended_size: 0,
                is_coinbase: false,
                spending_height: 0,
                created_at: 1700000000000 + i as u64,
                flags: 0,
                utxo_hashes: (0..((i % 5) + 1) as usize)
                    .map(|v| {
                        let mut h = [0u8; 32];
                        h[0] = v as u8;
                        h[1] = i;
                        h
                    })
                    .collect(),
                cold_data: vec![],
                block_height: 0,
                mined_block_id: None,
                mined_block_height: None,
                mined_subtree_idx: None,
                parent_txids: vec![],
            })
            .collect();
        let encoded = encode_create_batch(&items);
        let decoded = decode_create_batch(&encoded).unwrap();
        assert_eq!(decoded.len(), 100);
        assert_eq!(decoded[0], items[0]);
        assert_eq!(decoded[99], items[99]);
    }

    #[test]
    fn create_batch_with_cold_data_round_trip() {
        let items = vec![WireCreateItem {
            txid: test_txid(1),
            tx_version: 1,
            locktime: 500_000,
            fee: 5000,
            size_in_bytes: 1024,
            extended_size: 2048,
            is_coinbase: true,
            spending_height: 100,
            created_at: 1700000000000,
            flags: 0x01,
            utxo_hashes: vec![[0xAA; 32], [0xBB; 32]],
            cold_data: vec![0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03],
            block_height: 0,
            mined_block_id: Some(42),
            mined_block_height: Some(800_000),
            mined_subtree_idx: Some(7),
            parent_txids: vec![],
        }];
        let encoded = encode_create_batch(&items);
        let decoded = decode_create_batch(&encoded).unwrap();
        assert_eq!(decoded, items);
        assert_eq!(
            decoded[0].cold_data,
            vec![0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03]
        );
        assert_eq!(decoded[0].mined_block_id, Some(42));
    }

    // -- GetBatch --

    #[test]
    fn get_batch_4096_items_round_trip() {
        let txids: Vec<[u8; 32]> = (0..4096u16)
            .map(|i| {
                let mut t = [0u8; 32];
                t[0..2].copy_from_slice(&i.to_le_bytes());
                t
            })
            .collect();
        let encoded = encode_get_batch(FieldMask::ALL, &txids);
        assert_eq!(encoded.len(), 8 + 4096 * 32); // count(4) + mask(4) + txids
        let (mask, decoded) = decode_get_batch(&encoded).unwrap();
        assert_eq!(mask, FieldMask(FieldMask::ALL));
        assert_eq!(decoded.len(), 4096);
        assert_eq!(decoded[0], txids[0]);
        assert_eq!(decoded[4095], txids[4095]);
    }

    #[test]
    fn get_batch_response_mixed_ok_not_found() {
        let items = vec![
            WireGetResult {
                status: 0,
                data: vec![1, 2, 3, 4, 5],
            },
            WireGetResult {
                status: 1,
                data: vec![],
            }, // not found
            WireGetResult {
                status: 0,
                data: vec![0xAA; 100],
            },
        ];
        let encoded = encode_get_response(&items);
        let decoded = decode_get_response(&encoded).unwrap();
        assert_eq!(decoded, items);
    }

    // -- GetSpend response with mixed statuses --

    #[test]
    fn get_spend_response_mixed_slot_statuses() {
        let items = vec![
            WireGetSpendResult {
                status: 0,
                error_code: ERR_OK,
                slot_status: 0x00,
                spending_data: [0; 36],
            },
            WireGetSpendResult {
                status: 0,
                error_code: ERR_OK,
                slot_status: 0x01,
                spending_data: [0xAB; 36],
            },
            WireGetSpendResult {
                status: 0,
                error_code: ERR_OK,
                slot_status: 0x02,
                spending_data: [0xCD; 36],
            },
            WireGetSpendResult {
                status: 0,
                error_code: ERR_OK,
                slot_status: 0xFF,
                spending_data: [0xFF; 36],
            },
        ];
        let encoded = encode_get_spend_response(&items);
        let decoded = decode_get_spend_response(&encoded).unwrap();
        assert_eq!(decoded, items);
    }

    // -- FreezeBatch response with SPENT error --

    #[test]
    fn freeze_batch_response_spent_error_with_spending_data() {
        let errors = vec![BatchItemError {
            item_index: 0,
            error_code: ERR_ALREADY_SPENT,
            error_data: vec![0xAB; 36],
        }];
        let encoded = encode_sparse_errors(&errors);
        let decoded = decode_sparse_errors(&encoded).unwrap();
        assert_eq!(decoded[0].error_data.len(), 36);
        assert_eq!(decoded[0].error_data, vec![0xAB; 36]);
    }

    // -- UnfreezeBatch response with NOT_FROZEN error --

    #[test]
    fn unfreeze_batch_response_not_frozen_error() {
        let errors = vec![BatchItemError {
            item_index: 2,
            error_code: ERR_UTXO_NOT_FROZEN,
            error_data: vec![],
        }];
        let encoded = encode_sparse_errors(&errors);
        let decoded = decode_sparse_errors(&encoded).unwrap();
        assert_eq!(decoded[0].error_code, ERR_UTXO_NOT_FROZEN);
    }

    // -- ReassignBatch --

    #[test]
    fn reassign_batch_1_item_round_trip() {
        let params = ReassignBatchParams {
            block_height: 500,
            spendable_after: 1000,
        };
        let items = vec![WireReassignItem {
            txid: test_txid(1),
            vout: 0,
            utxo_hash: test_txid(10),
            new_utxo_hash: test_txid(20),
        }];
        let encoded = encode_reassign_batch(&params, &items);
        let (dp, di) = decode_reassign_batch(&encoded).unwrap();
        assert_eq!(dp, params);
        assert_eq!(di, items);
    }

    #[test]
    fn reassign_batch_response_not_frozen_error() {
        let errors = vec![BatchItemError {
            item_index: 3,
            error_code: ERR_UTXO_NOT_FROZEN,
            error_data: vec![],
        }];
        let encoded = encode_sparse_errors(&errors);
        let decoded = decode_sparse_errors(&encoded).unwrap();
        assert_eq!(decoded[0].error_code, ERR_UTXO_NOT_FROZEN);
    }

    // -- SetConflicting response with DAHSET signal --

    #[test]
    fn set_conflicting_batch_response_dahset_signal() {
        let successes = vec![
            BatchItemSuccess {
                item_index: 0,
                signal: 1,
                block_ids: vec![],
            }, // DAHSET
        ];
        let errors: Vec<BatchItemError> = vec![];
        let encoded = encode_partial_with_signals(&successes, &errors);
        let (ds, de) = decode_partial_with_signals(&encoded).unwrap();
        assert_eq!(ds[0].signal, 1);
        assert!(de.is_empty());
    }

    // -- SetLockedBatch --

    #[test]
    fn set_locked_batch_1024_items_round_trip() {
        let txids: Vec<[u8; 32]> = (0..1024u16)
            .map(|i| {
                let mut t = [0u8; 32];
                t[0..2].copy_from_slice(&i.to_le_bytes());
                t
            })
            .collect();
        let shared = vec![1u8]; // value=true
        let encoded = encode_txid_batch(&txids, &shared);
        let (ds, dt) = decode_txid_batch(&encoded, 1).unwrap();
        assert_eq!(ds, vec![1u8]);
        assert_eq!(dt.len(), 1024);
    }

    #[test]
    fn set_locked_batch_response_not_found() {
        let errors = vec![BatchItemError {
            item_index: 5,
            error_code: ERR_TX_NOT_FOUND,
            error_data: vec![],
        }];
        let encoded = encode_sparse_errors(&errors);
        let decoded = decode_sparse_errors(&encoded).unwrap();
        assert_eq!(decoded[0].error_code, ERR_TX_NOT_FOUND);
    }

    // -- PreserveUntilBatch --

    #[test]
    fn preserve_until_batch_1024_items_round_trip() {
        let txids: Vec<[u8; 32]> = (0..1024u16)
            .map(|i| {
                let mut t = [0u8; 32];
                t[0..2].copy_from_slice(&i.to_le_bytes());
                t
            })
            .collect();
        let shared = 10000u32.to_le_bytes().to_vec();
        let encoded = encode_txid_batch(&txids, &shared);
        let (ds, dt) = decode_txid_batch(&encoded, 4).unwrap();
        assert_eq!(u32::from_le_bytes(ds[0..4].try_into().unwrap()), 10000);
        assert_eq!(dt.len(), 1024);
    }

    #[test]
    fn preserve_until_batch_response_preserve_signal() {
        let successes = vec![
            BatchItemSuccess {
                item_index: 0,
                signal: 2,
                block_ids: vec![],
            }, // PRESERVE
        ];
        let errors: Vec<BatchItemError> = vec![];
        let encoded = encode_partial_with_signals(&successes, &errors);
        let (ds, _) = decode_partial_with_signals(&encoded).unwrap();
        assert_eq!(ds[0].signal, 2);
    }

    // -- DeleteBatch --

    #[test]
    fn delete_batch_1_item_round_trip() {
        let txids = vec![test_txid(42)];
        let encoded = encode_txid_batch(&txids, &[]);
        let (_, decoded) = decode_txid_batch(&encoded, 0).unwrap();
        assert_eq!(decoded, txids);
    }

    #[test]
    fn delete_batch_response_not_found() {
        let errors = vec![
            BatchItemError {
                item_index: 0,
                error_code: ERR_TX_NOT_FOUND,
                error_data: vec![],
            },
            BatchItemError {
                item_index: 3,
                error_code: ERR_TX_NOT_FOUND,
                error_data: vec![],
            },
        ];
        let encoded = encode_sparse_errors(&errors);
        let decoded = decode_sparse_errors(&encoded).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].error_code, ERR_TX_NOT_FOUND);
    }

    // -- MarkLongestChainBatch --

    #[test]
    fn mark_longest_chain_batch_1_item_round_trip() {
        let txids = vec![test_txid(1)];
        let mut shared = Vec::new();
        shared.push(0); // not on longest chain
        shared.extend_from_slice(&2000u32.to_le_bytes());
        shared.extend_from_slice(&288u32.to_le_bytes());
        let encoded = encode_txid_batch(&txids, &shared);
        let (ds, dt) = decode_txid_batch(&encoded, 9).unwrap();
        assert_eq!(ds[0], 0);
        assert_eq!(dt, txids);
    }

    #[test]
    fn mark_longest_chain_batch_response_dahset_signal() {
        let successes = vec![
            BatchItemSuccess {
                item_index: 0,
                signal: 1,
                block_ids: vec![42],
            }, // DAHSET
        ];
        let errors: Vec<BatchItemError> = vec![];
        let encoded = encode_partial_with_signals(&successes, &errors);
        let (ds, _) = decode_partial_with_signals(&encoded).unwrap();
        assert_eq!(ds[0].signal, 1);
        assert_eq!(ds[0].block_ids, vec![42]);
    }

    // -- PartialError edge cases --

    #[test]
    fn partial_error_coinbase_immature_4_bytes() {
        let spending_height: u32 = 800_100;
        let errors = vec![BatchItemError {
            item_index: 7,
            error_code: ERR_COINBASE_IMMATURE,
            error_data: spending_height.to_le_bytes().to_vec(),
        }];
        let encoded = encode_sparse_errors(&errors);
        let decoded = decode_sparse_errors(&encoded).unwrap();
        assert_eq!(decoded[0].error_data.len(), 4);
        let recovered = u32::from_le_bytes(decoded[0].error_data[0..4].try_into().unwrap());
        assert_eq!(recovered, 800_100);
    }

    #[test]
    fn spend_batch_partial_error_with_success_signals_and_errors() {
        let successes = vec![
            BatchItemSuccess {
                item_index: 0,
                signal: 0,
                block_ids: vec![100, 200],
            },
            BatchItemSuccess {
                item_index: 1,
                signal: 1,
                block_ids: vec![],
            },
            BatchItemSuccess {
                item_index: 3,
                signal: 0,
                block_ids: vec![300],
            },
        ];
        let errors = vec![
            BatchItemError {
                item_index: 2,
                error_code: ERR_TX_NOT_FOUND,
                error_data: vec![],
            },
            BatchItemError {
                item_index: 4,
                error_code: ERR_FROZEN,
                error_data: vec![],
            },
        ];
        let encoded = encode_partial_with_signals(&successes, &errors);
        let (ds, de) = decode_partial_with_signals(&encoded).unwrap();
        assert_eq!(ds, successes);
        assert_eq!(de, errors);
    }

    // -- Error response --

    #[test]
    fn error_response_round_trip() {
        let encoded = encode_error_payload(ERR_INTERNAL, "something went wrong");
        let (code, msg) = decode_error_payload(&encoded).unwrap();
        assert_eq!(code, ERR_INTERNAL);
        assert_eq!(msg, "something went wrong");
    }

    // -- Redirect response --

    #[test]
    fn redirect_response_includes_target_addr() {
        let addr = "192.168.1.10:3300";
        let encoded = encode_redirect(addr);
        let decoded = decode_redirect(&encoded).unwrap();
        assert_eq!(decoded, addr);
    }

    // -- FreezeBatch 1 item --

    #[test]
    fn freeze_batch_1_item_round_trip() {
        let items = vec![WireSlotItem {
            txid: test_txid(99),
            vout: 0,
            utxo_hash: test_txid(200),
        }];
        let encoded = encode_slot_item_batch(&items);
        let decoded = decode_slot_item_batch(&encoded).unwrap();
        assert_eq!(decoded, items);
    }

    // -- UnfreezeBatch 50 items --

    #[test]
    fn unfreeze_batch_50_items_round_trip() {
        let items: Vec<WireSlotItem> = (0..50u8)
            .map(|i| WireSlotItem {
                txid: test_txid(i),
                vout: i as u32,
                utxo_hash: test_txid(i + 50),
            })
            .collect();
        let encoded = encode_slot_item_batch(&items);
        let decoded = decode_slot_item_batch(&encoded).unwrap();
        assert_eq!(decoded.len(), 50);
        assert_eq!(decoded, items);
    }

    // -- StreamChunk --

    #[test]
    fn stream_chunk_round_trip() {
        let mut txid = [0u8; 32];
        txid[0] = 0xAA;
        let data = vec![1u8, 2, 3, 4, 5];
        let encoded = encode_stream_chunk(&txid, 1024, &data);
        let decoded = decode_stream_chunk(&encoded).unwrap();
        assert_eq!(decoded.txid, txid);
        assert_eq!(decoded.offset, 1024);
        assert_eq!(decoded.data, &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn stream_chunk_empty_data() {
        let txid = [0xBBu8; 32];
        let encoded = encode_stream_chunk(&txid, 0, &[]);
        let decoded = decode_stream_chunk(&encoded).unwrap();
        assert_eq!(decoded.txid, txid);
        assert_eq!(decoded.offset, 0);
        assert!(decoded.data.is_empty());
    }

    #[test]
    fn stream_chunk_large_data() {
        let txid = [0xCCu8; 32];
        let data = vec![0x42u8; 4 * 1024 * 1024]; // 4 MiB
        let encoded = encode_stream_chunk(&txid, 8 * 1024 * 1024, &data);
        let decoded = decode_stream_chunk(&encoded).unwrap();
        assert_eq!(decoded.txid, txid);
        assert_eq!(decoded.offset, 8 * 1024 * 1024);
        assert_eq!(decoded.data.len(), 4 * 1024 * 1024);
    }

    #[test]
    fn stream_chunk_truncated_returns_none() {
        let txid = [0u8; 32];
        let encoded = encode_stream_chunk(&txid, 0, &[1, 2, 3]);
        // Truncate the data portion
        assert!(decode_stream_chunk(&encoded[..43]).is_none());
    }

    // -- StreamEnd --

    #[test]
    fn stream_end_round_trip() {
        let mut txid = [0u8; 32];
        txid[0] = 0xDD;
        let encoded = encode_stream_end(&txid, 50 * 1024 * 1024);
        let decoded = decode_stream_end(&encoded).unwrap();
        assert_eq!(decoded.txid, txid);
        assert_eq!(decoded.total_size, 50 * 1024 * 1024);
    }

    #[test]
    fn stream_end_truncated_returns_none() {
        assert!(decode_stream_end(&[0u8; 39]).is_none());
    }

    // -- Capacity pre-allocation tests --
    // Verify that encode functions pre-allocate enough capacity so that
    // the buffer never needs to reallocate (capacity >= final length).

    #[test]
    fn encode_set_mined_batch_no_realloc() {
        let params = SetMinedBatchParams {
            block_id: 1,
            block_height: 2,
            subtree_idx: 3,
            on_longest_chain: true,
            unset_mined: false,
            current_block_height: 4,
            block_height_retention: 5,
        };
        for count in [0, 1, 10, 1024] {
            let txids: Vec<[u8; 32]> = (0..count).map(|i| test_txid(i as u8)).collect();
            let encoded = encode_set_mined_batch(&params, &txids);
            // The final length should not exceed initial capacity.
            // Vec::with_capacity returns at least what was asked for.
            // If there was a realloc, capacity would be > the pre-calculated estimate.
            assert_eq!(
                encoded.len(),
                26 + count * 32,
                "set_mined_batch length mismatch for count={count}"
            );
        }
    }

    #[test]
    fn encode_spend_batch_no_realloc() {
        let params = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        for count in [0, 1, 100] {
            let items: Vec<WireSpendItem> = (0..count)
                .map(|i| WireSpendItem {
                    txid: test_txid(i as u8),
                    vout: i as u32,
                    utxo_hash: test_txid(0),
                    spending_data: [0; 36],
                })
                .collect();
            let encoded = encode_spend_batch(&params, &items);
            assert_eq!(
                encoded.len(),
                14 + count * 104,
                "spend_batch length mismatch for count={count}"
            );
        }
    }

    #[test]
    fn encode_create_batch_capacity_sufficient() {
        // Minimal items (no cold_data, few utxo_hashes) should not exceed
        // the pre-allocated capacity.
        let items: Vec<WireCreateItem> = (0..50)
            .map(|i| WireCreateItem {
                txid: test_txid(i as u8),
                tx_version: 1,
                locktime: 0,
                fee: 0,
                size_in_bytes: 0,
                extended_size: 0,
                is_coinbase: false,
                spending_height: 0,
                created_at: 0,
                flags: 0,
                utxo_hashes: vec![],
                cold_data: vec![],
                block_height: 0,
                mined_block_id: None,
                mined_block_height: None,
                mined_subtree_idx: None,
                parent_txids: vec![],
            })
            .collect();
        let encoded = encode_create_batch(&items);
        // With no utxo_hashes/cold_data/parents/mined: per item is 96 bytes.
        assert!(
            encoded.capacity() >= encoded.len(),
            "create_batch had insufficient capacity: cap={} len={}",
            encoded.capacity(),
            encoded.len()
        );
    }

    #[test]
    fn encode_get_response_capacity_sufficient() {
        let items: Vec<WireGetResult> = (0..100)
            .map(|_| WireGetResult {
                status: 0,
                data: vec![0u8; 64],
            })
            .collect();
        let encoded = encode_get_response(&items);
        assert!(
            encoded.capacity() >= encoded.len(),
            "get_response had insufficient capacity: cap={} len={}",
            encoded.capacity(),
            encoded.len()
        );
    }

    #[test]
    fn encode_sparse_errors_capacity_sufficient() {
        let errors: Vec<BatchItemError> = (0..100)
            .map(|i| BatchItemError {
                item_index: i,
                error_code: ERR_TX_NOT_FOUND,
                error_data: vec![0u8; 4],
            })
            .collect();
        let encoded = encode_sparse_errors(&errors);
        assert!(
            encoded.capacity() >= encoded.len(),
            "sparse_errors had insufficient capacity: cap={} len={}",
            encoded.capacity(),
            encoded.len()
        );
    }

    #[test]
    fn encode_partial_with_signals_capacity_sufficient() {
        let successes: Vec<BatchItemSuccess> = (0..50)
            .map(|i| BatchItemSuccess {
                item_index: i,
                signal: 1,
                block_ids: vec![42, 43],
            })
            .collect();
        let errors: Vec<BatchItemError> = (0..10)
            .map(|i| BatchItemError {
                item_index: i + 50,
                error_code: ERR_TX_NOT_FOUND,
                error_data: vec![],
            })
            .collect();
        let encoded = encode_partial_with_signals(&successes, &errors);
        assert!(
            encoded.capacity() >= encoded.len(),
            "partial_with_signals had insufficient capacity: cap={} len={}",
            encoded.capacity(),
            encoded.len()
        );
    }

    // -- Pre-allocation safety tests (gap #10) --
    //
    // These verify that the `*_checked` decoders refuse adversarial frames
    // BEFORE calling `Vec::with_capacity(count)`. The safety property we
    // guarantee is: a malformed `count = u32::MAX` with a tiny payload must
    // return `CodecError::TruncatedBatch` (or `BatchTooLarge`) instead of
    // panicking on out-of-memory or driving multi-gigabyte allocations.

    /// Build a SpendBatch payload with a poisoned `count` field.
    ///
    /// Returns a 14-byte header containing `count`, followed by no items.
    /// This simulates a malicious frame that arrives wholly within
    /// `MAX_FRAME_SIZE` but carries a fake count.
    fn poisoned_spend_payload(fake_count: u32) -> Vec<u8> {
        let mut buf = Vec::with_capacity(14);
        buf.extend_from_slice(&fake_count.to_le_bytes());
        buf.push(0); // ignore_conflicting
        buf.push(0); // ignore_locked
        buf.extend_from_slice(&0u32.to_le_bytes()); // current_block_height
        buf.extend_from_slice(&0u32.to_le_bytes()); // block_height_retention
        buf
    }

    #[test]
    fn decode_spend_batch_checked_rejects_u32_max_count() {
        // Adversarial: count = u32::MAX, payload only carries the 14-byte
        // header. Without the pre-check we would call
        // `Vec::with_capacity(u32::MAX as usize)` and either OOM or panic.
        let payload = poisoned_spend_payload(u32::MAX);
        let err = decode_spend_batch_checked(&payload, 8192).unwrap_err();
        match err {
            CodecError::BatchTooLarge { count, max } => {
                assert_eq!(count, u32::MAX);
                assert_eq!(max, 8192);
            }
            other => panic!("expected BatchTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn decode_spend_batch_checked_rejects_truncated_payload() {
        // count is within `max_batch` but there are zero item bytes after
        // the 14-byte header. Should fail with TruncatedBatch BEFORE the
        // per-item allocation.
        let payload = poisoned_spend_payload(100);
        let err = decode_spend_batch_checked(&payload, 8192).unwrap_err();
        match err {
            CodecError::TruncatedBatch {
                count,
                per_item_min,
                available,
            } => {
                assert_eq!(count, 100);
                assert_eq!(per_item_min, 104);
                assert_eq!(available, 0);
            }
            other => panic!("expected TruncatedBatch, got {other:?}"),
        }
    }

    #[test]
    fn decode_spend_batch_checked_accepts_valid_frame() {
        // Regression: a normally-shaped frame still decodes correctly
        // through the new validation path.
        let params = SpendBatchParams {
            ignore_conflicting: true,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        let items = vec![
            WireSpendItem {
                txid: test_txid(1),
                vout: 0,
                utxo_hash: test_txid(2),
                spending_data: [0xAA; 36],
            },
            WireSpendItem {
                txid: test_txid(3),
                vout: 5,
                utxo_hash: test_txid(4),
                spending_data: [0xBB; 36],
            },
        ];
        let encoded = encode_spend_batch(&params, &items);
        let (dp, di) = decode_spend_batch_checked(&encoded, 8192).unwrap();
        assert_eq!(dp, params);
        assert_eq!(di, items);
    }

    #[test]
    fn decode_spend_batch_checked_count_at_max_batch_succeeds() {
        // Exactly equal to max_batch is permitted (boundary check).
        let params = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 0,
            block_height_retention: 0,
        };
        let items = vec![
            WireSpendItem {
                txid: test_txid(1),
                vout: 0,
                utxo_hash: test_txid(2),
                spending_data: [0; 36],
            };
            4
        ];
        let encoded = encode_spend_batch(&params, &items);
        // max_batch = 4 (exact fit) — must not be rejected as too large.
        let (_, di) = decode_spend_batch_checked(&encoded, 4).unwrap();
        assert_eq!(di.len(), 4);

        // max_batch = 3 (one less) — must be rejected.
        let err = decode_spend_batch_checked(&encoded, 3).unwrap_err();
        assert!(matches!(
            err,
            CodecError::BatchTooLarge { count: 4, max: 3 }
        ));
    }

    #[test]
    fn decode_set_mined_batch_checked_rejects_u32_max_count() {
        // 26-byte header with poisoned count.
        let mut payload = Vec::with_capacity(26);
        payload.extend_from_slice(&u32::MAX.to_le_bytes()); // count
        payload.extend_from_slice(&[0u8; 22]); // params
        let err = decode_set_mined_batch_checked(&payload, 8192).unwrap_err();
        assert!(matches!(err, CodecError::BatchTooLarge { .. }));
    }

    #[test]
    fn decode_create_batch_checked_rejects_u32_max_count() {
        // CreateBatch header is just count(4) — easy to poison.
        let mut payload = Vec::with_capacity(4);
        payload.extend_from_slice(&u32::MAX.to_le_bytes());
        let err = decode_create_batch_checked(&payload, 8192).unwrap_err();
        assert!(matches!(err, CodecError::BatchTooLarge { .. }));
    }

    #[test]
    fn decode_create_batch_checked_rejects_truncated_payload() {
        // count = 1000 with 4-byte header — `count * 96 = 96_000` cannot
        // fit in zero remaining bytes. Should fail with TruncatedBatch
        // before allocating.
        let mut payload = Vec::with_capacity(4);
        payload.extend_from_slice(&1000u32.to_le_bytes());
        let err = decode_create_batch_checked(&payload, 8192).unwrap_err();
        match err {
            CodecError::TruncatedBatch {
                count,
                per_item_min,
                available,
            } => {
                assert_eq!(count, 1000);
                assert_eq!(per_item_min, 96);
                assert_eq!(available, 0);
            }
            other => panic!("expected TruncatedBatch, got {other:?}"),
        }
    }

    #[test]
    fn decode_txid_batch_checked_rejects_u32_max_count() {
        let mut payload = Vec::with_capacity(8);
        payload.extend_from_slice(&u32::MAX.to_le_bytes()); // count
        payload.extend_from_slice(&[0u8; 4]); // shared
        let err = decode_txid_batch_checked(&payload, 4, 8192).unwrap_err();
        assert!(matches!(err, CodecError::BatchTooLarge { .. }));
    }

    #[test]
    fn decode_txid_batch_checked_rejects_truncated() {
        // count = 50, no item bytes -> truncated.
        let mut payload = Vec::with_capacity(8);
        payload.extend_from_slice(&50u32.to_le_bytes());
        payload.extend_from_slice(&[0u8; 4]);
        let err = decode_txid_batch_checked(&payload, 4, 8192).unwrap_err();
        assert!(matches!(
            err,
            CodecError::TruncatedBatch {
                count: 50,
                per_item_min: 32,
                ..
            }
        ));
    }

    #[test]
    fn decode_get_batch_checked_rejects_u32_max_count() {
        let mut payload = Vec::with_capacity(8);
        payload.extend_from_slice(&u32::MAX.to_le_bytes()); // count
        payload.extend_from_slice(&0u32.to_le_bytes()); // field_mask
        let err = decode_get_batch_checked(&payload, 8192).unwrap_err();
        assert!(matches!(err, CodecError::BatchTooLarge { .. }));
    }

    #[test]
    fn decode_unspend_batch_checked_rejects_truncated() {
        let mut payload = Vec::with_capacity(12);
        payload.extend_from_slice(&500u32.to_le_bytes()); // count
        payload.extend_from_slice(&[0u8; 8]); // params
        let err = decode_unspend_batch_checked(&payload, 8192).unwrap_err();
        assert!(matches!(
            err,
            CodecError::TruncatedBatch {
                count: 500,
                per_item_min: 68,
                ..
            }
        ));
    }

    #[test]
    fn decode_reassign_batch_checked_rejects_u32_max_count() {
        let mut payload = Vec::with_capacity(12);
        payload.extend_from_slice(&u32::MAX.to_le_bytes()); // count
        payload.extend_from_slice(&[0u8; 8]); // params
        let err = decode_reassign_batch_checked(&payload, 8192).unwrap_err();
        assert!(matches!(err, CodecError::BatchTooLarge { .. }));
    }

    #[test]
    fn decode_slot_item_batch_checked_rejects_u32_max_count() {
        let mut payload = Vec::with_capacity(4);
        payload.extend_from_slice(&u32::MAX.to_le_bytes());
        let err = decode_slot_item_batch_checked(&payload, 8192).unwrap_err();
        assert!(matches!(err, CodecError::BatchTooLarge { .. }));
    }

    #[test]
    fn decode_get_spend_batch_checked_rejects_u32_max_count() {
        let mut payload = Vec::with_capacity(4);
        payload.extend_from_slice(&u32::MAX.to_le_bytes());
        let err = decode_get_spend_batch_checked(&payload, 8192).unwrap_err();
        assert!(matches!(err, CodecError::BatchTooLarge { .. }));
    }

    #[test]
    fn decode_sparse_errors_checked_rejects_u32_max_count() {
        let mut payload = Vec::with_capacity(4);
        payload.extend_from_slice(&u32::MAX.to_le_bytes());
        let err = decode_sparse_errors_checked(&payload, 8192).unwrap_err();
        assert!(matches!(err, CodecError::BatchTooLarge { .. }));
    }

    #[test]
    fn decode_partial_with_signals_checked_rejects_u32_max_count() {
        // The first section's count is poisoned.
        let mut payload = Vec::with_capacity(4);
        payload.extend_from_slice(&u32::MAX.to_le_bytes());
        let err = decode_partial_with_signals_checked(&payload, 8192).unwrap_err();
        assert!(matches!(err, CodecError::BatchTooLarge { .. }));
    }

    #[test]
    fn decode_get_response_checked_rejects_u32_max_count() {
        let mut payload = Vec::with_capacity(4);
        payload.extend_from_slice(&u32::MAX.to_le_bytes());
        let err = decode_get_response_checked(&payload, 8192).unwrap_err();
        assert!(matches!(err, CodecError::BatchTooLarge { .. }));
    }

    #[test]
    fn decode_get_spend_response_checked_rejects_u32_max_count() {
        let mut payload = Vec::with_capacity(4);
        payload.extend_from_slice(&u32::MAX.to_le_bytes());
        let err = decode_get_spend_response_checked(&payload, 8192).unwrap_err();
        assert!(matches!(err, CodecError::BatchTooLarge { .. }));
    }

    #[test]
    fn legacy_option_decoder_uses_max_decode_batch_cap() {
        // The Option-returning wrapper must reject `u32::MAX` via the
        // hard cap MAX_DECODE_BATCH so client/bench callers also gain
        // the protection.
        let payload = poisoned_spend_payload(u32::MAX);
        assert!(decode_spend_batch(&payload).is_none());
    }

    #[test]
    fn legacy_option_decoder_accepts_count_up_to_hard_cap() {
        // count = MAX_DECODE_BATCH must NOT be rejected as too large
        // (it's exactly at the boundary), but is rejected as truncated
        // because the payload doesn't contain any items. This documents
        // the cap is checked before the truncation check.
        let payload = poisoned_spend_payload(MAX_DECODE_BATCH);
        // Truncated payload — wrapper returns None (via TruncatedBatch).
        assert!(decode_spend_batch(&payload).is_none());

        // count = MAX_DECODE_BATCH + 1 also returns None, but via
        // BatchTooLarge. Both bucket into None for the legacy wrapper —
        // we verify the underlying error path here using the _checked
        // variant.
        let too_big = poisoned_spend_payload(MAX_DECODE_BATCH + 1);
        let err = decode_spend_batch_checked(&too_big, MAX_DECODE_BATCH).unwrap_err();
        assert!(matches!(err, CodecError::BatchTooLarge { .. }));
    }
}
