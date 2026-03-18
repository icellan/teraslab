//! Batch encode/decode for all operation types.
//!
//! Each batch follows the pattern:
//! `[count:4][shared_params][items × count]`

/// Helper: append a u32 LE.
fn put_u32(buf: &mut Vec<u8>, v: u32) { buf.extend_from_slice(&v.to_le_bytes()); }
fn put_u16(buf: &mut Vec<u8>, v: u16) { buf.extend_from_slice(&v.to_le_bytes()); }
fn get_u32(d: &[u8], o: usize) -> u32 { u32::from_le_bytes(d[o..o+4].try_into().unwrap()) }
fn get_u16(d: &[u8], o: usize) -> u16 { u16::from_le_bytes(d[o..o+2].try_into().unwrap()) }

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

/// Decode a SpendBatch request payload.
pub fn decode_spend_batch(data: &[u8]) -> Option<(SpendBatchParams, Vec<WireSpendItem>)> {
    if data.len() < 14 { return None; }
    let count = get_u32(data, 0) as usize;
    let params = SpendBatchParams {
        ignore_conflicting: data[4] != 0,
        ignore_locked: data[5] != 0,
        current_block_height: get_u32(data, 6),
        block_height_retention: get_u32(data, 10),
    };
    let mut items = Vec::with_capacity(count);
    let mut pos = 14;
    for _ in 0..count {
        if pos + 104 > data.len() { return None; }
        let mut txid = [0u8; 32]; txid.copy_from_slice(&data[pos..pos+32]);
        let vout = get_u32(data, pos+32);
        let mut uh = [0u8; 32]; uh.copy_from_slice(&data[pos+36..pos+68]);
        let mut sd = [0u8; 36]; sd.copy_from_slice(&data[pos+68..pos+104]);
        items.push(WireSpendItem { txid, vout, utxo_hash: uh, spending_data: sd });
        pos += 104;
    }
    Some((params, items))
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
    let mut buf = Vec::with_capacity(22 + txids.len() * 32);
    put_u32(&mut buf, txids.len() as u32);
    put_u32(&mut buf, params.block_id);
    put_u32(&mut buf, params.block_height);
    put_u32(&mut buf, params.subtree_idx);
    buf.push(u8::from(params.on_longest_chain));
    buf.push(u8::from(params.unset_mined));
    put_u32(&mut buf, params.current_block_height);
    put_u32(&mut buf, params.block_height_retention);
    for txid in txids { buf.extend_from_slice(txid); }
    buf
}

/// Decode a SetMinedBatch request payload.
pub fn decode_set_mined_batch(data: &[u8]) -> Option<(SetMinedBatchParams, Vec<[u8; 32]>)> {
    if data.len() < 26 { return None; }
    let count = get_u32(data, 0) as usize;
    let params = SetMinedBatchParams {
        block_id: get_u32(data, 4),
        block_height: get_u32(data, 8),
        subtree_idx: get_u32(data, 12),
        on_longest_chain: data[16] != 0,
        unset_mined: data[17] != 0,
        current_block_height: get_u32(data, 18),
        block_height_retention: get_u32(data, 22),
    };
    let mut txids = Vec::with_capacity(count);
    let mut pos = 26;
    for _ in 0..count {
        if pos + 32 > data.len() { return None; }
        let mut txid = [0u8; 32]; txid.copy_from_slice(&data[pos..pos+32]);
        txids.push(txid);
        pos += 32;
    }
    Some((params, txids))
}

// ---------------------------------------------------------------------------
// Simple txid-list batches (Delete, SetLocked, MarkLongestChain, PreserveUntil, SetConflicting)
// ---------------------------------------------------------------------------

/// Encode a batch of txids with optional shared u8 + u32 params.
/// Format: [count:4][shared_params][txids × count]
pub fn encode_txid_batch(txids: &[[u8; 32]], shared: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + shared.len() + txids.len() * 32);
    put_u32(&mut buf, txids.len() as u32);
    buf.extend_from_slice(shared);
    for txid in txids { buf.extend_from_slice(txid); }
    buf
}

/// Decode a batch of txids with a given shared params size.
pub fn decode_txid_batch(data: &[u8], shared_len: usize) -> Option<(Vec<u8>, Vec<[u8; 32]>)> {
    if data.len() < 4 + shared_len { return None; }
    let count = get_u32(data, 0) as usize;
    let shared = data[4..4+shared_len].to_vec();
    let mut txids = Vec::with_capacity(count);
    let mut pos = 4 + shared_len;
    for _ in 0..count {
        if pos + 32 > data.len() { return None; }
        let mut txid = [0u8; 32]; txid.copy_from_slice(&data[pos..pos+32]);
        txids.push(txid);
        pos += 32;
    }
    Some((shared, txids))
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

/// Decode a batch of slot items.
pub fn decode_slot_item_batch(data: &[u8]) -> Option<Vec<WireSlotItem>> {
    if data.len() < 4 { return None; }
    let count = get_u32(data, 0) as usize;
    let mut items = Vec::with_capacity(count);
    let mut pos = 4;
    for _ in 0..count {
        if pos + 68 > data.len() { return None; }
        let mut txid = [0u8; 32]; txid.copy_from_slice(&data[pos..pos+32]);
        let vout = get_u32(data, pos+32);
        let mut uh = [0u8; 32]; uh.copy_from_slice(&data[pos+36..pos+68]);
        items.push(WireSlotItem { txid, vout, utxo_hash: uh });
        pos += 68;
    }
    Some(items)
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

/// Decode a ReassignBatch request payload.
pub fn decode_reassign_batch(data: &[u8]) -> Option<(ReassignBatchParams, Vec<WireReassignItem>)> {
    if data.len() < 12 { return None; }
    let count = get_u32(data, 0) as usize;
    let params = ReassignBatchParams {
        block_height: get_u32(data, 4),
        spendable_after: get_u32(data, 8),
    };
    let mut items = Vec::with_capacity(count);
    let mut pos = 12;
    for _ in 0..count {
        if pos + 100 > data.len() { return None; }
        let mut txid = [0u8; 32]; txid.copy_from_slice(&data[pos..pos+32]);
        let vout = get_u32(data, pos+32);
        let mut uh = [0u8; 32]; uh.copy_from_slice(&data[pos+36..pos+68]);
        let mut nh = [0u8; 32]; nh.copy_from_slice(&data[pos+68..pos+100]);
        items.push(WireReassignItem { txid, vout, utxo_hash: uh, new_utxo_hash: nh });
        pos += 100;
    }
    Some((params, items))
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
    let mut buf = Vec::new();
    put_u32(&mut buf, errors.len() as u32);
    for e in errors {
        put_u32(&mut buf, e.item_index);
        put_u16(&mut buf, e.error_code);
        put_u16(&mut buf, e.error_data.len() as u16);
        buf.extend_from_slice(&e.error_data);
    }
    buf
}

/// Decode a sparse error list.
pub fn decode_sparse_errors(data: &[u8]) -> Option<Vec<BatchItemError>> {
    if data.len() < 4 { return None; }
    let count = get_u32(data, 0) as usize;
    let mut errors = Vec::with_capacity(count);
    let mut pos = 4;
    for _ in 0..count {
        if pos + 8 > data.len() { return None; }
        let item_index = get_u32(data, pos);
        let error_code = get_u16(data, pos + 4);
        let data_len = get_u16(data, pos + 6) as usize;
        pos += 8;
        if pos + data_len > data.len() { return None; }
        let error_data = data[pos..pos + data_len].to_vec();
        pos += data_len;
        errors.push(BatchItemError { item_index, error_code, error_data });
    }
    Some(errors)
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

/// Decode a GetSpendBatch request payload.
pub fn decode_get_spend_batch(data: &[u8]) -> Option<Vec<WireGetSpendItem>> {
    if data.len() < 4 { return None; }
    let count = get_u32(data, 0) as usize;
    let mut items = Vec::with_capacity(count);
    let mut pos = 4;
    for _ in 0..count {
        if pos + 36 > data.len() { return None; }
        let mut txid = [0u8; 32]; txid.copy_from_slice(&data[pos..pos+32]);
        let vout = get_u32(data, pos + 32);
        items.push(WireGetSpendItem { txid, vout });
        pos += 36;
    }
    Some(items)
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

/// Decode GetSpendBatch response items.
pub fn decode_get_spend_response(data: &[u8]) -> Option<Vec<WireGetSpendResult>> {
    if data.len() < 4 { return None; }
    let count = get_u32(data, 0) as usize;
    let mut items = Vec::with_capacity(count);
    let mut pos = 4;
    for _ in 0..count {
        if pos + 40 > data.len() { return None; }
        let status = data[pos];
        let error_code = get_u16(data, pos + 1);
        let slot_status = data[pos + 3];
        let mut sd = [0u8; 36]; sd.copy_from_slice(&data[pos+4..pos+40]);
        items.push(WireGetSpendResult { status, error_code, slot_status, spending_data: sd });
        pos += 40;
    }
    Some(items)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::opcodes::*;

    fn test_txid(n: u8) -> [u8; 32] { let mut t = [0u8; 32]; t[0] = n; t }

    // -- SpendBatch --

    #[test]
    fn spend_batch_1_item_round_trip() {
        let params = SpendBatchParams {
            ignore_conflicting: true, ignore_locked: false,
            current_block_height: 1000, block_height_retention: 288,
        };
        let items = vec![WireSpendItem {
            txid: test_txid(1), vout: 5, utxo_hash: test_txid(2), spending_data: [0xAB; 36],
        }];
        let encoded = encode_spend_batch(&params, &items);
        let (dp, di) = decode_spend_batch(&encoded).unwrap();
        assert_eq!(dp, params);
        assert_eq!(di, items);
    }

    #[test]
    fn spend_batch_1024_items_round_trip() {
        let params = SpendBatchParams {
            ignore_conflicting: false, ignore_locked: true,
            current_block_height: 500, block_height_retention: 144,
        };
        let items: Vec<WireSpendItem> = (0..1024u16).map(|i| WireSpendItem {
            txid: { let mut t = [0u8; 32]; t[0..2].copy_from_slice(&i.to_le_bytes()); t },
            vout: i as u32, utxo_hash: test_txid(i as u8), spending_data: [i as u8; 36],
        }).collect();
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
            block_id: 42, block_height: 800_000, subtree_idx: 7,
            on_longest_chain: true, unset_mined: false,
            current_block_height: 800_000, block_height_retention: 288,
        };
        let txids: Vec<[u8; 32]> = (0..512u16).map(|i| {
            let mut t = [0u8; 32]; t[0..2].copy_from_slice(&i.to_le_bytes()); t
        }).collect();
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
        let txids: Vec<[u8; 32]> = (0..1024u16).map(|i| {
            let mut t = [0u8; 32]; t[0..2].copy_from_slice(&i.to_le_bytes()); t
        }).collect();
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
        let items: Vec<WireSlotItem> = (0..50u8).map(|i| WireSlotItem {
            txid: test_txid(i), vout: i as u32, utxo_hash: test_txid(i + 100),
        }).collect();
        let encoded = encode_slot_item_batch(&items);
        let decoded = decode_slot_item_batch(&encoded).unwrap();
        assert_eq!(decoded, items);
    }

    #[test]
    fn unfreeze_batch_1_item() {
        let items = vec![WireSlotItem { txid: test_txid(1), vout: 7, utxo_hash: test_txid(2) }];
        let encoded = encode_slot_item_batch(&items);
        let decoded = decode_slot_item_batch(&encoded).unwrap();
        assert_eq!(decoded, items);
    }

    // -- Reassign batch --

    #[test]
    fn reassign_batch_round_trip() {
        let params = ReassignBatchParams { block_height: 1000, spendable_after: 100 };
        let items: Vec<WireReassignItem> = (0..50u8).map(|i| WireReassignItem {
            txid: test_txid(i), vout: i as u32, utxo_hash: test_txid(i), new_utxo_hash: test_txid(i + 50),
        }).collect();
        let encoded = encode_reassign_batch(&params, &items);
        let (dp, di) = decode_reassign_batch(&encoded).unwrap();
        assert_eq!(dp, params);
        assert_eq!(di, items);
    }

    // -- Sparse errors --

    #[test]
    fn sparse_errors_round_trip() {
        let errors = vec![
            BatchItemError { item_index: 3, error_code: ERR_TX_NOT_FOUND, error_data: vec![] },
            BatchItemError { item_index: 7, error_code: ERR_ALREADY_SPENT, error_data: vec![0xAB; 36] },
            BatchItemError { item_index: 999, error_code: ERR_FROZEN, error_data: vec![] },
        ];
        let encoded = encode_sparse_errors(&errors);
        let decoded = decode_sparse_errors(&encoded).unwrap();
        assert_eq!(decoded, errors);
    }

    #[test]
    fn sparse_errors_ascending_indices() {
        let errors = vec![
            BatchItemError { item_index: 1, error_code: ERR_TX_NOT_FOUND, error_data: vec![] },
            BatchItemError { item_index: 5, error_code: ERR_FROZEN, error_data: vec![] },
            BatchItemError { item_index: 10, error_code: ERR_LOCKED, error_data: vec![] },
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
        let items: Vec<WireGetSpendItem> = (0..1024u16).map(|i| WireGetSpendItem {
            txid: { let mut t = [0u8; 32]; t[0..2].copy_from_slice(&i.to_le_bytes()); t },
            vout: i as u32,
        }).collect();
        let encoded = encode_get_spend_batch(&items);
        let decoded = decode_get_spend_batch(&encoded).unwrap();
        assert_eq!(decoded, items);
    }

    #[test]
    fn get_spend_response_round_trip() {
        let items = vec![
            WireGetSpendResult { status: 0, error_code: ERR_OK, slot_status: 0x00, spending_data: [0; 36] },
            WireGetSpendResult { status: 0, error_code: ERR_OK, slot_status: 0x01, spending_data: [0xAB; 36] },
            WireGetSpendResult { status: 1, error_code: ERR_TX_NOT_FOUND, slot_status: 0, spending_data: [0; 36] },
        ];
        let encoded = encode_get_spend_response(&items);
        let decoded = decode_get_spend_response(&encoded).unwrap();
        assert_eq!(decoded, items);
    }
}
