package teraslab

import "fmt"

// All encode functions take a buf []byte parameter and append to it,
// returning the grown slice. This lets callers control allocation
// (stack, pooled, or pre-sized) instead of forcing a heap escape.

// ---------------------------------------------------------------------------
// Spend batch
// ---------------------------------------------------------------------------

// encodeSpendBatch appends a SpendBatch request payload to buf.
// Format: [count:4][ignore_c:1][ignore_l:1][cbh:4][bhr:4][items: 104 each]
func encodeSpendBatch(buf []byte, params SpendBatchParams, items []SpendItem) []byte {
	buf = appendU32(buf, uint32(len(items)))
	buf = appendBool(buf, params.IgnoreConflicting)
	buf = appendBool(buf, params.IgnoreLocked)
	buf = appendU32(buf, params.CurrentBlockHeight)
	buf = appendU32(buf, params.BlockHeightRetention)
	for i := range items {
		buf = append(buf, items[i].TxID[:]...)
		buf = appendU32(buf, items[i].Vout)
		buf = append(buf, items[i].UtxoHash[:]...)
		buf = append(buf, items[i].SpendingData[:]...)
	}
	return buf
}

// spendBatchSize returns the exact payload size for a SpendBatch request.
func spendBatchSize(n int) int { return 14 + n*104 }

// ---------------------------------------------------------------------------
// Unspend batch
// ---------------------------------------------------------------------------

// encodeUnspendBatch appends an UnspendBatch request payload to buf.
// Format: [count:4][cbh:4][bhr:4][items: txid(32)+vout(4 LE)+utxo_hash(32)+spending_data(36) x count]
// Per-item size = 104 bytes. Header = 12 bytes.
//
// The spending_data field was added by audit fix A-04 to prevent unauthorized
// erasure: the server rejects unspend items whose spending_data does not match
// the recorded value. Wire layout must match src/protocol/codec.rs
// encode_unspend_batch.
func encodeUnspendBatch(buf []byte, params UnspendBatchParams, items []UnspendItem) []byte {
	buf = appendU32(buf, uint32(len(items)))
	buf = appendU32(buf, params.CurrentBlockHeight)
	buf = appendU32(buf, params.BlockHeightRetention)
	for i := range items {
		buf = append(buf, items[i].TxID[:]...)
		buf = appendU32(buf, items[i].Vout)
		buf = append(buf, items[i].UtxoHash[:]...)
		buf = append(buf, items[i].SpendingData[:]...)
	}
	return buf
}

// unspendBatchSize returns the exact payload size for an UnspendBatch request.
func unspendBatchSize(n int) int { return 12 + n*104 }

// ---------------------------------------------------------------------------
// SetMined batch
// ---------------------------------------------------------------------------

// encodeSetMinedBatch appends a SetMinedBatch request payload to buf.
func encodeSetMinedBatch(buf []byte, params SetMinedBatchParams, txids []TxID) []byte {
	buf = appendU32(buf, uint32(len(txids)))
	buf = appendU32(buf, params.BlockID)
	buf = appendU32(buf, params.BlockHeight)
	buf = appendU32(buf, params.SubtreeIdx)
	buf = appendBool(buf, params.OnLongestChain)
	buf = appendBool(buf, params.UnsetMined)
	buf = appendU32(buf, params.CurrentBlockHeight)
	buf = appendU32(buf, params.BlockHeightRetention)
	for i := range txids {
		buf = append(buf, txids[i][:]...)
	}
	return buf
}

// ---------------------------------------------------------------------------
// Create batch
// ---------------------------------------------------------------------------

// encodeCreateBatch appends a CreateBatch request payload to buf.
func encodeCreateBatch(buf []byte, items []CreateItem) []byte {
	buf = appendU32(buf, uint32(len(items)))
	for i := range items {
		item := &items[i]
		buf = append(buf, item.TxID[:]...)
		buf = appendU32(buf, item.TxVersion)
		buf = appendU32(buf, item.Locktime)
		buf = appendU64(buf, item.Fee)
		buf = appendU64(buf, item.SizeInBytes)
		buf = appendU64(buf, item.ExtendedSize)
		buf = appendBool(buf, item.IsCoinbase)
		buf = appendU32(buf, item.SpendingHeight)
		buf = appendU64(buf, item.CreatedAt)
		buf = append(buf, item.Flags)
		buf = appendU32(buf, uint32(len(item.UtxoHashes)))
		for j := range item.UtxoHashes {
			buf = append(buf, item.UtxoHashes[j][:]...)
		}
		// Encode TxData as a single cold_data blob for the wire protocol.
		// Format: [has_cold:1][total_len:4][inputs_len:4][inputs][outputs_len:4][outputs][inpoints_len:4][inpoints]
		hasTxData := len(item.TxData.Inputs) > 0 || len(item.TxData.Outputs) > 0 || len(item.TxData.Inpoints) > 0
		buf = appendBool(buf, hasTxData)
		coldLen := 4 + len(item.TxData.Inputs) + 4 + len(item.TxData.Outputs) + 4 + len(item.TxData.Inpoints)
		if !hasTxData {
			coldLen = 0
		}
		buf = appendU32(buf, uint32(coldLen))
		if hasTxData {
			buf = appendU32(buf, uint32(len(item.TxData.Inputs)))
			buf = append(buf, item.TxData.Inputs...)
			buf = appendU32(buf, uint32(len(item.TxData.Outputs)))
			buf = append(buf, item.TxData.Outputs...)
			buf = appendU32(buf, uint32(len(item.TxData.Inpoints)))
			buf = append(buf, item.TxData.Inpoints...)
		}
		buf = appendU32(buf, item.BlockHeight)
		hasMined := item.MinedBlockID != nil
		buf = appendBool(buf, hasMined)
		if hasMined {
			buf = appendU32(buf, *item.MinedBlockID)
			h := uint32(0)
			if item.MinedBlockHeight != nil {
				h = *item.MinedBlockHeight
			}
			buf = appendU32(buf, h)
			s := uint32(0)
			if item.MinedSubtreeIdx != nil {
				s = *item.MinedSubtreeIdx
			}
			buf = appendU32(buf, s)
		}
		buf = appendU32(buf, uint32(len(item.ParentTxIDs)))
		for j := range item.ParentTxIDs {
			buf = append(buf, item.ParentTxIDs[j][:]...)
		}
	}
	return buf
}

// ---------------------------------------------------------------------------
// Slot item batch (Freeze/Unfreeze)
// ---------------------------------------------------------------------------

// encodeSlotItemBatch appends a Freeze/Unfreeze batch request payload to buf.
func encodeSlotItemBatch(buf []byte, items []FreezeItem) []byte {
	buf = appendU32(buf, uint32(len(items)))
	for i := range items {
		buf = append(buf, items[i].TxID[:]...)
		buf = appendU32(buf, items[i].Vout)
		buf = append(buf, items[i].UtxoHash[:]...)
	}
	return buf
}

// ---------------------------------------------------------------------------
// Reassign batch
// ---------------------------------------------------------------------------

// encodeReassignBatch appends a ReassignBatch request payload to buf.
func encodeReassignBatch(buf []byte, params ReassignBatchParams, items []ReassignItem) []byte {
	buf = appendU32(buf, uint32(len(items)))
	buf = appendU32(buf, params.BlockHeight)
	buf = appendU32(buf, params.SpendableAfter)
	for i := range items {
		buf = append(buf, items[i].TxID[:]...)
		buf = appendU32(buf, items[i].Vout)
		buf = append(buf, items[i].UtxoHash[:]...)
		buf = append(buf, items[i].NewUtxoHash[:]...)
	}
	return buf
}

// ---------------------------------------------------------------------------
// Simple txid-list batches
// ---------------------------------------------------------------------------

// encodeTxIDBatch appends a batch of txids with optional shared parameters to buf.
func encodeTxIDBatch(buf []byte, txids []TxID, shared []byte) []byte {
	buf = appendU32(buf, uint32(len(txids)))
	buf = append(buf, shared...)
	for i := range txids {
		buf = append(buf, txids[i][:]...)
	}
	return buf
}

// encodeDeleteBatch appends a DeleteBatch request payload to buf.
func encodeDeleteBatch(buf []byte, txids []TxID) []byte {
	return encodeTxIDBatch(buf, txids, nil)
}

// encodeSetLockedBatch appends a SetLockedBatch request payload to buf.
func encodeSetLockedBatch(buf []byte, value bool, txids []TxID) []byte {
	var shared [1]byte
	if value {
		shared[0] = 1
	}
	return encodeTxIDBatch(buf, txids, shared[:])
}

// encodeSetConflictingBatch appends a SetConflictingBatch request payload to buf.
func encodeSetConflictingBatch(buf []byte, params SetConflictingParams, txids []TxID) []byte {
	var shared [9]byte
	if params.Value {
		shared[0] = 1
	}
	putU32(shared[1:5], params.CurrentBlockHeight)
	putU32(shared[5:9], params.BlockHeightRetention)
	return encodeTxIDBatch(buf, txids, shared[:])
}

// encodePreserveUntilBatch appends a PreserveUntilBatch request payload to buf.
func encodePreserveUntilBatch(buf []byte, blockHeight uint32, txids []TxID) []byte {
	var shared [4]byte
	putU32(shared[:], blockHeight)
	return encodeTxIDBatch(buf, txids, shared[:])
}

// encodeMarkLongestChainBatch appends a MarkLongestChainBatch request payload to buf.
func encodeMarkLongestChainBatch(buf []byte, params MarkLongestChainParams, txids []TxID) []byte {
	var shared [9]byte
	if params.OnLongestChain {
		shared[0] = 1
	}
	putU32(shared[1:5], params.CurrentBlockHeight)
	putU32(shared[5:9], params.BlockHeightRetention)
	return encodeTxIDBatch(buf, txids, shared[:])
}

// ---------------------------------------------------------------------------
// Get batch
// ---------------------------------------------------------------------------

// encodeGetBatch appends a GetBatch request payload to buf.
func encodeGetBatch(buf []byte, fieldMask uint32, txids []TxID) []byte {
	buf = appendU32(buf, uint32(len(txids)))
	buf = appendU32(buf, fieldMask)
	for i := range txids {
		buf = append(buf, txids[i][:]...)
	}
	return buf
}

// ---------------------------------------------------------------------------
// GetSpend batch
// ---------------------------------------------------------------------------

// encodeGetSpendBatch appends a GetSpendBatch request payload to buf.
// Format: [count:4][items: txid(32)+vout(4 LE)+utxo_hash(32) x count]
// Per-item size = 68 bytes. Header = 4 bytes.
// Wire layout must match src/protocol/codec.rs encode_get_spend_batch.
func encodeGetSpendBatch(buf []byte, items []GetSpendItem) []byte {
	buf = appendU32(buf, uint32(len(items)))
	for i := range items {
		buf = append(buf, items[i].TxID[:]...)
		buf = appendU32(buf, items[i].Vout)
		buf = append(buf, items[i].UtxoHash[:]...)
	}
	return buf
}

// getSpendBatchSize returns the exact payload size for a GetSpendBatch request.
func getSpendBatchSize(n int) int { return 4 + n*68 }

// ---------------------------------------------------------------------------
// Pruner operations
// ---------------------------------------------------------------------------

// encodeQueryOldUnmined appends a QueryOldUnmined request payload to buf.
func encodeQueryOldUnmined(buf []byte, cutoffHeight uint32) []byte {
	return appendU32(buf, cutoffHeight)
}

// encodeQueryConflicting appends a QueryConflicting request payload to buf.
// The request carries no parameters (CONFLICTING is a boolean flag with no
// cutoff), so buf is returned unchanged. Wire layout must match the Rust
// server's handle_query_conflicting (empty request payload).
func encodeQueryConflicting(buf []byte) []byte {
	return buf
}

// decodeQueryConflictingResponse decodes a QueryConflicting response payload.
// The format is identical to QueryOldUnmined: [count:u32 LE][txid:32]*count.
func decodeQueryConflictingResponse(data []byte) ([]TxID, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("query conflicting: need 4 bytes, have %d", len(data))
	}
	count := int(getU32(data[0:4]))
	if len(data) < 4+count*32 {
		return nil, fmt.Errorf("query conflicting: truncated")
	}
	txids := make([]TxID, count)
	pos := 4
	for i := 0; i < count; i++ {
		copy(txids[i][:], data[pos:pos+32])
		pos += 32
	}
	return txids, nil
}

// encodeRemoveConflictingChildBatch appends a RemoveConflictingChildBatch
// request payload to buf. Format: [count:u32 LE] then count ×
// [parent_txid:32][child_txid:32] (64 bytes/item, parent first). No shared
// params. Wire layout must match src/protocol/codec.rs
// encode_conflicting_child_pair_batch.
func encodeRemoveConflictingChildBatch(buf []byte, pairs []ConflictingChildPair) []byte {
	buf = appendU32(buf, uint32(len(pairs)))
	for i := range pairs {
		buf = append(buf, pairs[i].Parent[:]...)
		buf = append(buf, pairs[i].Child[:]...)
	}
	return buf
}

// encodePreserveTransactions appends a PreserveTransactions request payload to buf.
func encodePreserveTransactions(buf []byte, blockHeight uint32, txids []TxID) []byte {
	buf = appendU32(buf, uint32(len(txids)))
	buf = appendU32(buf, blockHeight)
	for i := range txids {
		buf = append(buf, txids[i][:]...)
	}
	return buf
}

// encodeProcessExpired appends a ProcessExpiredPreservations request payload to buf.
func encodeProcessExpired(buf []byte, currentHeight uint32) []byte {
	return appendU32(buf, currentHeight)
}

// ---------------------------------------------------------------------------
// Streaming blob upload
// ---------------------------------------------------------------------------

// encodeStreamChunk encodes an OP_STREAM_CHUNK payload.
// Format: [txid:32][offset:8 LE][chunk_data_len:4 LE][chunk_data]
func encodeStreamChunk(buf []byte, txid TxID, offset uint64, data []byte) []byte {
	buf = append(buf, txid[:]...)
	buf = appendU64(buf, offset)
	buf = appendU32(buf, uint32(len(data)))
	buf = append(buf, data...)
	return buf
}

// encodeStreamEnd encodes an OP_STREAM_END payload.
// Format: [txid:32][total_size:8 LE]
func encodeStreamEnd(buf []byte, txid TxID, totalSize uint64) []byte {
	buf = append(buf, txid[:]...)
	buf = appendU64(buf, totalSize)
	return buf
}

// encodeColdData encodes a CreateItem's TxData into the cold_data wire format.
// Format: [inputs_len:4][inputs][outputs_len:4][outputs][inpoints_len:4][inpoints]
// Returns nil if the TxData has no content.
func encodeColdData(item *CreateItem) []byte {
	hasTxData := len(item.TxData.Inputs) > 0 || len(item.TxData.Outputs) > 0 || len(item.TxData.Inpoints) > 0
	if !hasTxData {
		return nil
	}
	coldLen := 4 + len(item.TxData.Inputs) + 4 + len(item.TxData.Outputs) + 4 + len(item.TxData.Inpoints)
	buf := make([]byte, 0, coldLen)
	buf = appendU32(buf, uint32(len(item.TxData.Inputs)))
	buf = append(buf, item.TxData.Inputs...)
	buf = appendU32(buf, uint32(len(item.TxData.Outputs)))
	buf = append(buf, item.TxData.Outputs...)
	buf = appendU32(buf, uint32(len(item.TxData.Inpoints)))
	buf = append(buf, item.TxData.Inpoints...)
	return buf
}

// coldDataSize returns the wire size of a CreateItem's cold_data section.
// Returns 0 if the TxData has no content.
func coldDataSize(item *CreateItem) int {
	hasTxData := len(item.TxData.Inputs) > 0 || len(item.TxData.Outputs) > 0 || len(item.TxData.Inpoints) > 0
	if !hasTxData {
		return 0
	}
	return 4 + len(item.TxData.Inputs) + 4 + len(item.TxData.Outputs) + 4 + len(item.TxData.Inpoints)
}

// ===========================================================================
// Response decoders
// ===========================================================================

// checkElemCount guards a slice allocation that is sized from a count read off
// the wire. A response can never hold more than remaining/minElemBytes elements,
// so a larger declared count means the frame is malformed or misframed (for
// example when a sparse-error payload is speculatively decoded with the
// signal-format decoder). Returning an error here — instead of make()-ing the
// slice — prevents a corrupt or hostile response from forcing an unbounded
// (multi-gigabyte) allocation before the per-element bounds checks ever run.
func checkElemCount(what string, count, minElemBytes, remaining int) error {
	if count < 0 {
		return fmt.Errorf("%s: negative element count %d", what, count)
	}
	if minElemBytes > 0 && count > remaining/minElemBytes {
		return fmt.Errorf("%s: declared count %d exceeds %d remaining payload bytes", what, count, remaining)
	}
	return nil
}

// decodeSparseErrors decodes a sparse error list from a PartialError response.
func decodeSparseErrors(data []byte) ([]BatchItemError, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("sparse errors: need 4 bytes, have %d", len(data))
	}
	count := int(getU32(data[0:4]))
	// Each error entry is at least 8 bytes (index:4 + code:2 + dataLen:2).
	if err := checkElemCount("sparse errors", count, 8, len(data)-4); err != nil {
		return nil, err
	}
	errors := make([]BatchItemError, 0, count)
	pos := 4
	for i := 0; i < count; i++ {
		if pos+8 > len(data) {
			return nil, fmt.Errorf("sparse errors: truncated at item %d", i)
		}
		itemIndex := getU32(data[pos : pos+4])
		errCode := getU16(data[pos+4 : pos+6])
		dataLen := int(getU16(data[pos+6 : pos+8]))
		pos += 8
		if pos+dataLen > len(data) {
			return nil, fmt.Errorf("sparse errors: truncated data at item %d", i)
		}
		// Sub-slice the response payload instead of copying. The payload
		// is already an isolated copy owned by this response, so the
		// sub-slice is safe to hold without pinning extra memory.
		errData := data[pos : pos+dataLen : pos+dataLen]
		pos += dataLen
		errors = append(errors, BatchItemError{
			ItemIndex: itemIndex,
			Code:      errCode,
			Data:      errData,
		})
	}
	return errors, nil
}

// decodePartialWithSignals decodes a response with both success signals and errors.
func decodePartialWithSignals(data []byte) ([]BatchItemSuccess, []BatchItemError, error) {
	if len(data) < 4 {
		return nil, nil, fmt.Errorf("partial signals: need 4 bytes, have %d", len(data))
	}
	pos := 0

	successCount := int(getU32(data[pos : pos+4]))
	pos += 4
	// Each success entry is at least 6 bytes (index:4 + signal:1 + bidCount:1).
	if err := checkElemCount("partial signals successes", successCount, 6, len(data)-pos); err != nil {
		return nil, nil, err
	}
	successes := make([]BatchItemSuccess, successCount)
	for i := 0; i < successCount; i++ {
		if pos+6 > len(data) {
			return nil, nil, fmt.Errorf("partial signals: truncated success at %d", i)
		}
		successes[i].ItemIndex = getU32(data[pos : pos+4])
		successes[i].Signal = data[pos+4]
		bidCount := int(data[pos+5])
		pos += 6
		if pos+bidCount*4 > len(data) {
			return nil, nil, fmt.Errorf("partial signals: truncated block_ids at %d", i)
		}
		if bidCount > 0 {
			successes[i].BlockIDs = make([]uint32, bidCount)
			for j := 0; j < bidCount; j++ {
				successes[i].BlockIDs[j] = getU32(data[pos : pos+4])
				pos += 4
			}
		}
	}

	if pos+4 > len(data) {
		return nil, nil, fmt.Errorf("partial signals: truncated error count")
	}
	errorCount := int(getU32(data[pos : pos+4]))
	pos += 4
	// Each error entry is at least 8 bytes (index:4 + code:2 + dataLen:2). This
	// is the guard that stops a sparse payload — misread by this signal decoder —
	// from yielding a garbage errorCount and a multi-gigabyte allocation.
	if err := checkElemCount("partial signals errors", errorCount, 8, len(data)-pos); err != nil {
		return nil, nil, err
	}
	errors := make([]BatchItemError, errorCount)
	for i := 0; i < errorCount; i++ {
		if pos+8 > len(data) {
			return nil, nil, fmt.Errorf("partial signals: truncated error at %d", i)
		}
		errors[i].ItemIndex = getU32(data[pos : pos+4])
		errors[i].Code = getU16(data[pos+4 : pos+6])
		dataLen := int(getU16(data[pos+6 : pos+8]))
		pos += 8
		if pos+dataLen > len(data) {
			return nil, nil, fmt.Errorf("partial signals: truncated error data at %d", i)
		}
		errors[i].Data = data[pos : pos+dataLen : pos+dataLen]
		pos += dataLen
	}

	return successes, errors, nil
}

// decodeErrorPayload decodes a global error response payload.
func decodeErrorPayload(data []byte) (uint16, string, error) {
	if len(data) < 4 {
		return 0, "", fmt.Errorf("error payload: need 4 bytes, have %d", len(data))
	}
	code := getU16(data[0:2])
	msgLen := int(getU16(data[2:4]))
	if len(data) < 4+msgLen {
		return 0, "", fmt.Errorf("error payload: truncated message")
	}
	return code, string(data[4 : 4+msgLen]), nil
}

// decodeRedirect decodes a redirect response payload.
func decodeRedirect(data []byte) (string, error) {
	if len(data) < 2 {
		return "", fmt.Errorf("redirect: need 2 bytes, have %d", len(data))
	}
	addrLen := int(getU16(data[0:2]))
	if len(data) < 2+addrLen {
		return "", fmt.Errorf("redirect: truncated address")
	}
	return string(data[2 : 2+addrLen]), nil
}

// decodeGetResponse decodes a GetBatch response payload.
func decodeGetResponse(data []byte) ([]GetResult, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("get response: need 4 bytes, have %d", len(data))
	}
	count := int(getU32(data[0:4]))
	// Each result is at least 5 bytes (status:1 + dataLen:4).
	if err := checkElemCount("get response", count, 5, len(data)-4); err != nil {
		return nil, err
	}
	results := make([]GetResult, count)
	pos := 4
	for i := 0; i < count; i++ {
		if pos+5 > len(data) {
			return nil, fmt.Errorf("get response: truncated at item %d", i)
		}
		results[i].Status = data[pos]
		dataLen := int(getU32(data[pos+1 : pos+5]))
		pos += 5
		if pos+dataLen > len(data) {
			return nil, fmt.Errorf("get response: truncated data at item %d", i)
		}
		// Sub-slice the response payload.
		results[i].Data = data[pos : pos+dataLen : pos+dataLen]
		pos += dataLen
	}
	return results, nil
}

// decodeGetSpendResponse decodes a GetSpendBatch response payload.
func decodeGetSpendResponse(data []byte) ([]GetSpendResult, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("get spend response: need 4 bytes, have %d", len(data))
	}
	count := int(getU32(data[0:4]))
	// Each result is a fixed 40 bytes.
	if err := checkElemCount("get spend response", count, 40, len(data)-4); err != nil {
		return nil, err
	}
	results := make([]GetSpendResult, count)
	pos := 4
	for i := 0; i < count; i++ {
		if pos+40 > len(data) {
			return nil, fmt.Errorf("get spend response: truncated at item %d", i)
		}
		results[i].Status = data[pos]
		results[i].ErrorCode = getU16(data[pos+1 : pos+3])
		results[i].SlotStatus = data[pos+3]
		copy(results[i].SpendingData[:], data[pos+4:pos+40])
		pos += 40
	}
	return results, nil
}

// decodePartitionMap decodes a partition map response payload.
func decodePartitionMap(data []byte) (*PartitionMap, error) {
	if len(data) < 12 {
		return nil, fmt.Errorf("partition map: need 12 bytes, have %d", len(data))
	}
	pm := &PartitionMap{
		Version: getU64(data[0:8]),
	}
	nodeCount := int(getU32(data[8:12]))
	pos := 12

	// Each node is at least 10 bytes (id:8 + addrLen:2).
	if err := checkElemCount("partition map nodes", nodeCount, 10, len(data)-pos); err != nil {
		return nil, err
	}
	pm.Nodes = make([]NodeInfo, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		if pos+10 > len(data) {
			return nil, fmt.Errorf("partition map: truncated node %d", i)
		}
		nodeID := getU64(data[pos : pos+8])
		addrLen := int(getU16(data[pos+8 : pos+10]))
		pos += 10
		if pos+addrLen > len(data) {
			return nil, fmt.Errorf("partition map: truncated node addr %d", i)
		}
		addr := string(data[pos : pos+addrLen])
		pos += addrLen
		pm.Nodes = append(pm.Nodes, NodeInfo{ID: nodeID, Addr: addr})
	}

	if pos+NumShards*8 > len(data) {
		return nil, fmt.Errorf("partition map: truncated shard assignments")
	}
	for i := 0; i < NumShards; i++ {
		pm.Assignments[i] = getU64(data[pos : pos+8])
		pos += 8
	}

	return pm, nil
}

// decodeQueryOldUnminedResponse decodes a QueryOldUnmined response payload.
func decodeQueryOldUnminedResponse(data []byte) ([]TxID, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("query old unmined: need 4 bytes, have %d", len(data))
	}
	count := int(getU32(data[0:4]))
	if len(data) < 4+count*32 {
		return nil, fmt.Errorf("query old unmined: truncated")
	}
	txids := make([]TxID, count)
	pos := 4
	for i := 0; i < count; i++ {
		copy(txids[i][:], data[pos:pos+32])
		pos += 32
	}
	return txids, nil
}

// decodeProcessExpiredResponse decodes a ProcessExpiredPreservations response.
func decodeProcessExpiredResponse(data []byte) (uint32, uint32, error) {
	if len(data) < 8 {
		return 0, 0, fmt.Errorf("process expired: need 8 bytes, have %d", len(data))
	}
	return getU32(data[0:4]), getU32(data[4:8]), nil
}

// ---------------------------------------------------------------------------
// GetBatch data field parsers
// ---------------------------------------------------------------------------

// MetadataAllSize is the byte size when ALL metadata fields (bits 0-18) are requested.
const MetadataAllSize = 148

// ExternalRefSize is the byte size of the ExternalRef section (FieldExternalRef, bit 16).
const ExternalRefSize = 65

// DecodeTxMetadata parses the metadata section from a GetResult.Data field.
// Only fields whose bits are set in fieldMask are present in the data.
// Returns the parsed metadata, the number of bytes consumed, and any error.
func DecodeTxMetadata(fieldMask uint32, data []byte) (*TxMetadata, int, error) {
	md := &TxMetadata{}
	pos := 0

	if fieldMask&FieldTxVersion != 0 {
		if pos+4 > len(data) {
			return nil, 0, fmt.Errorf("tx metadata: truncated at TxVersion")
		}
		md.TxVersion = getU32(data[pos : pos+4])
		pos += 4
	}
	if fieldMask&FieldLocktime != 0 {
		if pos+4 > len(data) {
			return nil, 0, fmt.Errorf("tx metadata: truncated at Locktime")
		}
		md.Locktime = getU32(data[pos : pos+4])
		pos += 4
	}
	if fieldMask&FieldFee != 0 {
		if pos+8 > len(data) {
			return nil, 0, fmt.Errorf("tx metadata: truncated at Fee")
		}
		md.Fee = getU64(data[pos : pos+8])
		pos += 8
	}
	if fieldMask&FieldSizeInBytes != 0 {
		if pos+8 > len(data) {
			return nil, 0, fmt.Errorf("tx metadata: truncated at SizeInBytes")
		}
		md.SizeInBytes = getU64(data[pos : pos+8])
		pos += 8
	}
	if fieldMask&FieldExtendedSize != 0 {
		if pos+8 > len(data) {
			return nil, 0, fmt.Errorf("tx metadata: truncated at ExtendedSize")
		}
		md.ExtendedSize = getU64(data[pos : pos+8])
		pos += 8
	}
	if fieldMask&FieldFlags != 0 {
		if pos+1 > len(data) {
			return nil, 0, fmt.Errorf("tx metadata: truncated at Flags")
		}
		md.Flags = data[pos]
		pos++
	}
	if fieldMask&FieldSpendingHeight != 0 {
		if pos+4 > len(data) {
			return nil, 0, fmt.Errorf("tx metadata: truncated at SpendingHeight")
		}
		md.SpendingHeight = getU32(data[pos : pos+4])
		pos += 4
	}
	if fieldMask&FieldCreatedAt != 0 {
		if pos+8 > len(data) {
			return nil, 0, fmt.Errorf("tx metadata: truncated at CreatedAt")
		}
		md.CreatedAt = getU64(data[pos : pos+8])
		pos += 8
	}
	if fieldMask&FieldSpentUtxos != 0 {
		if pos+4 > len(data) {
			return nil, 0, fmt.Errorf("tx metadata: truncated at SpentUtxos")
		}
		md.SpentUtxos = getU32(data[pos : pos+4])
		pos += 4
	}
	if fieldMask&FieldPrunedUtxos != 0 {
		if pos+4 > len(data) {
			return nil, 0, fmt.Errorf("tx metadata: truncated at PrunedUtxos")
		}
		md.PrunedUtxos = getU32(data[pos : pos+4])
		pos += 4
	}
	if fieldMask&FieldUtxoCount != 0 {
		if pos+4 > len(data) {
			return nil, 0, fmt.Errorf("tx metadata: truncated at UtxoCount")
		}
		md.UtxoCount = getU32(data[pos : pos+4])
		pos += 4
	}
	if fieldMask&FieldGeneration != 0 {
		if pos+4 > len(data) {
			return nil, 0, fmt.Errorf("tx metadata: truncated at Generation")
		}
		md.Generation = getU32(data[pos : pos+4])
		pos += 4
	}
	if fieldMask&FieldUpdatedAt != 0 {
		if pos+8 > len(data) {
			return nil, 0, fmt.Errorf("tx metadata: truncated at UpdatedAt")
		}
		md.UpdatedAt = getU64(data[pos : pos+8])
		pos += 8
	}
	if fieldMask&FieldUnminedSince != 0 {
		if pos+4 > len(data) {
			return nil, 0, fmt.Errorf("tx metadata: truncated at UnminedSince")
		}
		md.UnminedSince = getU32(data[pos : pos+4])
		pos += 4
	}
	if fieldMask&FieldDeleteAtHeight != 0 {
		if pos+4 > len(data) {
			return nil, 0, fmt.Errorf("tx metadata: truncated at DeleteAtHeight")
		}
		md.DeleteAtHeight = getU32(data[pos : pos+4])
		pos += 4
	}
	if fieldMask&FieldPreserveUntil != 0 {
		if pos+4 > len(data) {
			return nil, 0, fmt.Errorf("tx metadata: truncated at PreserveUntil")
		}
		md.PreserveUntil = getU32(data[pos : pos+4])
		pos += 4
	}
	if fieldMask&FieldExternalRef != 0 {
		if pos+ExternalRefSize > len(data) {
			return nil, 0, fmt.Errorf("tx metadata: truncated at ExternalRef")
		}
		md.ExternalRef.StoreType = data[pos]
		copy(md.ExternalRef.ContentHash[:], data[pos+1:pos+33])
		md.ExternalRef.TotalSize = getU64(data[pos+33 : pos+41])
		md.ExternalRef.InputCount = getU32(data[pos+41 : pos+45])
		md.ExternalRef.OutputCount = getU32(data[pos+45 : pos+49])
		md.ExternalRef.InputsOffset = getU64(data[pos+49 : pos+57])
		md.ExternalRef.OutputsOffset = getU64(data[pos+57 : pos+65])
		pos += ExternalRefSize
	}
	if fieldMask&FieldReassignCount != 0 {
		if pos+1 > len(data) {
			return nil, 0, fmt.Errorf("tx metadata: truncated at ReassignCount")
		}
		md.ReassignCount = data[pos]
		pos++
	}
	if fieldMask&FieldBlockEntryCount != 0 {
		if pos+1 > len(data) {
			return nil, 0, fmt.Errorf("tx metadata: truncated at BlockEntryCount")
		}
		md.BlockEntryCount = data[pos]
		pos++
	}

	return md, pos, nil
}

// RawMetadataSize is the byte size of the full on-disk metadata struct (FIELD_RAW_METADATA).
const RawMetadataSize = 256

// DecodeTxMetadataRaw parses the full 256-byte on-disk metadata struct returned
// by FIELD_RAW_METADATA. The raw bytes are preserved for inspection, and key
// internal fields are parsed into convenience accessors. For debugging only.
func DecodeTxMetadataRaw(data []byte) (*TxMetadataRaw, error) {
	if len(data) < RawMetadataSize {
		return nil, fmt.Errorf("raw metadata: need %d bytes, have %d", RawMetadataSize, len(data))
	}
	raw := &TxMetadataRaw{
		Magic:                     getU32(data[0:4]),
		SchemaVersion:             getU32(data[4:8]),
		RecordSize:                getU32(data[8:12]),
		UtxoCount:                 getU32(data[12:16]),
		Flags:                     data[80],
		SpentUtxos:                getU32(data[93:97]),
		BlockEntryCount:           data[113],
		BlockOverflowOffset:       getU64(data[150:158]),
		ReassignmentOffset:        getU64(data[158:166]),
		ReassignmentCount:         data[166],
		ConflictingChildrenCount:  data[244],
		ConflictingChildrenOffset: getU64(data[245:253]),
	}
	copy(raw.Bytes[:], data[:RawMetadataSize])
	copy(raw.TxID[:], data[16:48])
	return raw, nil
}

// DecodeUtxoSlots parses the UTXO slots section from a GetResult.Data field.
func DecodeUtxoSlots(data []byte) ([]UtxoSlot, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("utxo slots: need 4 bytes, have %d", len(data))
	}
	count := int(getU32(data[0:4]))
	pos := 4
	if pos+count*69 > len(data) {
		return nil, fmt.Errorf("utxo slots: need %d bytes, have %d", pos+count*69, len(data))
	}
	slots := make([]UtxoSlot, count)
	for i := 0; i < count; i++ {
		copy(slots[i].Hash[:], data[pos:pos+32])
		slots[i].Status = data[pos+32]
		copy(slots[i].SpendingData[:], data[pos+33:pos+69])
		pos += 69
	}
	return slots, nil
}

// DecodeBlockEntries parses the block entries section from a GetResult.Data field.
func DecodeBlockEntries(data []byte) ([]BlockEntry, error) {
	if len(data) < 1 {
		return nil, fmt.Errorf("block entries: need 1 byte, have %d", len(data))
	}
	count := int(data[0])
	pos := 1
	inlineCount := count
	if inlineCount > 3 {
		inlineCount = 3
	}
	if pos+inlineCount*12 > len(data) {
		return nil, fmt.Errorf("block entries: truncated")
	}
	entries := make([]BlockEntry, inlineCount)
	for i := 0; i < inlineCount; i++ {
		entries[i] = BlockEntry{
			BlockID:     getU32(data[pos : pos+4]),
			BlockHeight: getU32(data[pos+4 : pos+8]),
			SubtreeIdx:  getU32(data[pos+8 : pos+12]),
		}
		pos += 12
	}
	return entries, nil
}
