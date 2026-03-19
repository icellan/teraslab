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
// Format: [count:4][cbh:4][bhr:4][items: txid(32)+vout(4)+hash(32) x count]
func encodeUnspendBatch(buf []byte, params UnspendBatchParams, items []UnspendItem) []byte {
	buf = appendU32(buf, uint32(len(items)))
	buf = appendU32(buf, params.CurrentBlockHeight)
	buf = appendU32(buf, params.BlockHeightRetention)
	for i := range items {
		buf = append(buf, items[i].TxID[:]...)
		buf = appendU32(buf, items[i].Vout)
		buf = append(buf, items[i].UtxoHash[:]...)
	}
	return buf
}

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
func encodeGetBatch(buf []byte, fieldMask uint16, txids []TxID) []byte {
	buf = appendU32(buf, uint32(len(txids)))
	buf = appendU16(buf, fieldMask)
	for i := range txids {
		buf = append(buf, txids[i][:]...)
	}
	return buf
}

// ---------------------------------------------------------------------------
// GetSpend batch
// ---------------------------------------------------------------------------

// encodeGetSpendBatch appends a GetSpendBatch request payload to buf.
func encodeGetSpendBatch(buf []byte, items []GetSpendItem) []byte {
	buf = appendU32(buf, uint32(len(items)))
	for i := range items {
		buf = append(buf, items[i].TxID[:]...)
		buf = appendU32(buf, items[i].Vout)
	}
	return buf
}

// ---------------------------------------------------------------------------
// Pruner operations
// ---------------------------------------------------------------------------

// encodeQueryOldUnmined appends a QueryOldUnmined request payload to buf.
func encodeQueryOldUnmined(buf []byte, cutoffHeight uint32) []byte {
	return appendU32(buf, cutoffHeight)
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

// ===========================================================================
// Response decoders
// ===========================================================================

// decodeSparseErrors decodes a sparse error list from a PartialError response.
func decodeSparseErrors(data []byte) ([]BatchItemError, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("sparse errors: need 4 bytes, have %d", len(data))
	}
	count := int(getU32(data[0:4]))
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

// DecodeTxMetadata parses the metadata section from a GetResult.Data field.
func DecodeTxMetadata(data []byte) (*TxMetadata, error) {
	if len(data) < 81 {
		return nil, fmt.Errorf("tx metadata: need 81 bytes, have %d", len(data))
	}
	return &TxMetadata{
		TxVersion:      getU32(data[0:4]),
		Locktime:       getU32(data[4:8]),
		Fee:            getU64(data[8:16]),
		SizeInBytes:    getU64(data[16:24]),
		ExtendedSize:   getU64(data[24:32]),
		Flags:          data[32],
		SpendingHeight: getU32(data[33:37]),
		CreatedAt:      getU64(data[37:45]),
		SpentUtxos:     getU32(data[45:49]),
		PrunedUtxos:    getU32(data[49:53]),
		UtxoCount:      getU32(data[53:57]),
		Generation:     getU32(data[57:61]),
		UpdatedAt:      getU64(data[61:69]),
		UnminedSince:   getU32(data[69:73]),
		DeleteAtHeight: getU32(data[73:77]),
		PreserveUntil:  getU32(data[77:81]),
	}, nil
}

// MetadataSize is the byte size of the serialized metadata section.
const MetadataSize = 81

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
