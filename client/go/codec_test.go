package teraslab

import (
	"bytes"
	"testing"
)

func testTxID(n byte) TxID {
	var t TxID
	t[0] = n
	return t
}

func testUtxoHash(n byte) UtxoHash {
	var h UtxoHash
	h[0] = n
	return h
}

// ---------------------------------------------------------------------------
// SpendBatch
// ---------------------------------------------------------------------------

func TestSpendBatch1ItemRoundTrip(t *testing.T) {
	params := SpendBatchParams{
		IgnoreConflicting:    true,
		IgnoreLocked:         false,
		CurrentBlockHeight:   1000,
		BlockHeightRetention: 288,
	}
	items := []SpendItem{{
		TxID:         testTxID(1),
		Vout:         5,
		UtxoHash:     testUtxoHash(2),
		SpendingData: [36]byte{0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB},
	}}
	encoded := encodeSpendBatch(nil, params, items)

	// Verify structure: count(4) + params(10) + 1 item(104) = 118
	if len(encoded) != 14+104 {
		t.Fatalf("encoded length = %d, want %d", len(encoded), 14+104)
	}
	count := getU32(encoded[0:4])
	if count != 1 {
		t.Errorf("count = %d, want 1", count)
	}
	if encoded[4] != 1 { // ignore_conflicting = true
		t.Errorf("ignore_conflicting = %d, want 1", encoded[4])
	}
	if encoded[5] != 0 { // ignore_locked = false
		t.Errorf("ignore_locked = %d, want 0", encoded[5])
	}
	cbh := getU32(encoded[6:10])
	if cbh != 1000 {
		t.Errorf("current_block_height = %d, want 1000", cbh)
	}
}

func TestSpendBatch1024ItemsSize(t *testing.T) {
	params := SpendBatchParams{
		CurrentBlockHeight:   500,
		BlockHeightRetention: 144,
	}
	items := make([]SpendItem, 1024)
	for i := range items {
		items[i].TxID[0] = byte(i)
		items[i].TxID[1] = byte(i >> 8)
		items[i].Vout = uint32(i)
	}
	encoded := encodeSpendBatch(nil, params, items)
	if len(encoded) != 14+1024*104 {
		t.Fatalf("encoded length = %d, want %d", len(encoded), 14+1024*104)
	}
}

// ---------------------------------------------------------------------------
// SetMinedBatch
// ---------------------------------------------------------------------------

func TestSetMinedBatchRoundTrip(t *testing.T) {
	params := SetMinedBatchParams{
		BlockID:              42,
		BlockHeight:          800_000,
		SubtreeIdx:           7,
		OnLongestChain:       true,
		UnsetMined:           false,
		CurrentBlockHeight:   800_000,
		BlockHeightRetention: 288,
	}
	txids := make([]TxID, 512)
	for i := range txids {
		txids[i][0] = byte(i)
		txids[i][1] = byte(i >> 8)
	}
	encoded := encodeSetMinedBatch(nil, params, txids)
	if len(encoded) != 26+512*32 {
		t.Fatalf("encoded length = %d, want %d", len(encoded), 26+512*32)
	}
	count := getU32(encoded[0:4])
	if count != 512 {
		t.Errorf("count = %d, want 512", count)
	}
}

// ---------------------------------------------------------------------------
// Delete / SetLocked / SetConflicting / PreserveUntil / MarkLongestChain
// ---------------------------------------------------------------------------

func TestDeleteBatchRoundTrip(t *testing.T) {
	txids := make([]TxID, 256)
	for i := range txids {
		txids[i] = testTxID(byte(i))
	}
	encoded := encodeDeleteBatch(nil, txids)
	// count(4) + 256 * 32 = 8196
	if len(encoded) != 4+256*32 {
		t.Fatalf("encoded length = %d, want %d", len(encoded), 4+256*32)
	}
}

func TestSetLockedBatchRoundTrip(t *testing.T) {
	txids := []TxID{testTxID(1), testTxID(2)}
	encoded := encodeSetLockedBatch(nil, true, txids)
	// count(4) + shared(1) + 2*32 = 69
	if len(encoded) != 4+1+2*32 {
		t.Fatalf("encoded length = %d, want %d", len(encoded), 4+1+2*32)
	}
	if encoded[4] != 1 { // value=true
		t.Errorf("value = %d, want 1", encoded[4])
	}
}

func TestSetConflictingBatchRoundTrip(t *testing.T) {
	params := SetConflictingParams{Value: true, CurrentBlockHeight: 500, BlockHeightRetention: 288}
	txids := []TxID{testTxID(5)}
	encoded := encodeSetConflictingBatch(nil, params, txids)
	// count(4) + shared(9) + 1*32 = 45
	if len(encoded) != 4+9+32 {
		t.Fatalf("encoded length = %d, want %d", len(encoded), 4+9+32)
	}
}

func TestPreserveUntilBatchRoundTrip(t *testing.T) {
	txids := []TxID{testTxID(1), testTxID(2)}
	encoded := encodePreserveUntilBatch(nil, 5000, txids)
	if len(encoded) != 4+4+2*32 {
		t.Fatalf("encoded length = %d, want %d", len(encoded), 4+4+2*32)
	}
	bh := getU32(encoded[4:8])
	if bh != 5000 {
		t.Errorf("block_height = %d, want 5000", bh)
	}
}

func TestMarkLongestChainBatchRoundTrip(t *testing.T) {
	params := MarkLongestChainParams{OnLongestChain: true, CurrentBlockHeight: 1000, BlockHeightRetention: 288}
	txids := make([]TxID, 1024)
	encoded := encodeMarkLongestChainBatch(nil, params, txids)
	if len(encoded) != 4+9+1024*32 {
		t.Fatalf("encoded length = %d, want %d", len(encoded), 4+9+1024*32)
	}
}

// ---------------------------------------------------------------------------
// Freeze/Unfreeze
// ---------------------------------------------------------------------------

func TestFreezeSlotItemBatch(t *testing.T) {
	items := make([]FreezeItem, 50)
	for i := range items {
		items[i] = FreezeItem{
			TxID:     testTxID(byte(i)),
			Vout:     uint32(i),
			UtxoHash: testUtxoHash(byte(i + 100)),
		}
	}
	encoded := encodeSlotItemBatch(nil, items)
	if len(encoded) != 4+50*68 {
		t.Fatalf("encoded length = %d, want %d", len(encoded), 4+50*68)
	}
}

// ---------------------------------------------------------------------------
// Reassign batch
// ---------------------------------------------------------------------------

func TestReassignBatchRoundTrip(t *testing.T) {
	params := ReassignBatchParams{BlockHeight: 1000, SpendableAfter: 100}
	items := make([]ReassignItem, 50)
	for i := range items {
		items[i] = ReassignItem{
			TxID:        testTxID(byte(i)),
			Vout:        uint32(i),
			UtxoHash:    testUtxoHash(byte(i)),
			NewUtxoHash: testUtxoHash(byte(i + 50)),
		}
	}
	encoded := encodeReassignBatch(nil, params, items)
	if len(encoded) != 12+50*100 {
		t.Fatalf("encoded length = %d, want %d", len(encoded), 12+50*100)
	}
}

// ---------------------------------------------------------------------------
// Unspend batch
// ---------------------------------------------------------------------------

func TestUnspendBatch1ItemRoundTrip(t *testing.T) {
	params := UnspendBatchParams{CurrentBlockHeight: 500, BlockHeightRetention: 288}
	items := []UnspendItem{{TxID: testTxID(1), Vout: 3, UtxoHash: testUtxoHash(2)}}
	encoded := encodeUnspendBatch(nil, params, items)
	if len(encoded) != 12+68 {
		t.Fatalf("encoded length = %d, want %d", len(encoded), 12+68)
	}
}

func TestUnspendBatch512Items(t *testing.T) {
	params := UnspendBatchParams{CurrentBlockHeight: 1000, BlockHeightRetention: 144}
	items := make([]UnspendItem, 512)
	encoded := encodeUnspendBatch(nil, params, items)
	if len(encoded) != 12+512*68 {
		t.Fatalf("encoded length = %d, want %d", len(encoded), 12+512*68)
	}
}

// ---------------------------------------------------------------------------
// Create batch
// ---------------------------------------------------------------------------

func TestCreateBatch100Items(t *testing.T) {
	items := make([]CreateItem, 100)
	for i := range items {
		n := byte(i)
		utxoCount := (i % 5) + 1
		hashes := make([]UtxoHash, utxoCount)
		for v := range hashes {
			hashes[v][0] = byte(v)
			hashes[v][1] = n
		}
		items[i] = CreateItem{
			TxID:           testTxID(n),
			TxVersion:      2,
			Fee:            1000 + uint64(i),
			SizeInBytes:    250,
			CreatedAt:      1700000000000 + uint64(i),
			UtxoHashes:     hashes,
		}
	}
	encoded := encodeCreateBatch(nil, items)
	count := getU32(encoded[0:4])
	if count != 100 {
		t.Errorf("count = %d, want 100", count)
	}
	// Just verify it doesn't panic and produces output.
	if len(encoded) < 4+100*82 {
		t.Errorf("encoded too short: %d", len(encoded))
	}
}

func TestCreateBatchWithColdData(t *testing.T) {
	bid := uint32(42)
	bh := uint32(800_000)
	si := uint32(7)
	items := []CreateItem{{
		TxID:             testTxID(1),
		TxVersion:        1,
		Locktime:         500_000,
		Fee:              5000,
		SizeInBytes:      1024,
		ExtendedSize:     2048,
		IsCoinbase:       true,
		SpendingHeight:   100,
		CreatedAt:        1700000000000,
		Flags:            0x01,
		UtxoHashes:       []UtxoHash{{0xAA}, {0xBB}},
		ColdData:         []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03},
		MinedBlockID:     &bid,
		MinedBlockHeight: &bh,
		MinedSubtreeIdx:  &si,
	}}
	encoded := encodeCreateBatch(nil, items)
	if getU32(encoded[0:4]) != 1 {
		t.Fatalf("count = %d, want 1", getU32(encoded[0:4]))
	}
	if len(encoded) == 0 {
		t.Fatal("empty encoded")
	}
}

// ---------------------------------------------------------------------------
// Get batch
// ---------------------------------------------------------------------------

func TestGetBatch4096Items(t *testing.T) {
	txids := make([]TxID, 4096)
	for i := range txids {
		txids[i][0] = byte(i)
		txids[i][1] = byte(i >> 8)
	}
	encoded := encodeGetBatch(nil, FieldAll, txids)
	if len(encoded) != 6+4096*32 {
		t.Fatalf("encoded length = %d, want %d", len(encoded), 6+4096*32)
	}
	count := getU32(encoded[0:4])
	mask := getU16(encoded[4:6])
	if count != 4096 {
		t.Errorf("count = %d, want 4096", count)
	}
	if mask != FieldAll {
		t.Errorf("mask = %d, want %d", mask, FieldAll)
	}
}

// ---------------------------------------------------------------------------
// GetSpend batch
// ---------------------------------------------------------------------------

func TestGetSpendBatch1024Items(t *testing.T) {
	items := make([]GetSpendItem, 1024)
	for i := range items {
		items[i].TxID[0] = byte(i)
		items[i].TxID[1] = byte(i >> 8)
		items[i].Vout = uint32(i)
	}
	encoded := encodeGetSpendBatch(nil, items)
	if len(encoded) != 4+1024*36 {
		t.Fatalf("encoded length = %d, want %d", len(encoded), 4+1024*36)
	}
}

// ---------------------------------------------------------------------------
// Sparse errors decode
// ---------------------------------------------------------------------------

func TestSparseErrorsRoundTrip(t *testing.T) {
	// Build sparse error payload manually (as server would).
	var buf []byte
	buf = appendU32(buf, 3) // error_count
	// Error 1
	buf = appendU32(buf, 3)                // item_index
	buf = appendU16(buf, ErrCodeTxNotFound) // error_code
	buf = appendU16(buf, 0)                // data_len
	// Error 2
	buf = appendU32(buf, 7)
	buf = appendU16(buf, ErrCodeAlreadySpent)
	buf = appendU16(buf, 36)
	buf = append(buf, bytes.Repeat([]byte{0xAB}, 36)...)
	// Error 3
	buf = appendU32(buf, 999)
	buf = appendU16(buf, ErrCodeFrozen)
	buf = appendU16(buf, 0)

	errors, err := decodeSparseErrors(buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(errors) != 3 {
		t.Fatalf("got %d errors, want 3", len(errors))
	}
	if errors[0].ItemIndex != 3 || errors[0].Code != ErrCodeTxNotFound {
		t.Errorf("error[0] = %v", errors[0])
	}
	if errors[1].ItemIndex != 7 || errors[1].Code != ErrCodeAlreadySpent || len(errors[1].Data) != 36 {
		t.Errorf("error[1] = %v", errors[1])
	}
	if errors[2].ItemIndex != 999 || errors[2].Code != ErrCodeFrozen {
		t.Errorf("error[2] = %v", errors[2])
	}
}

func TestSparseErrorsAscendingIndices(t *testing.T) {
	var buf []byte
	buf = appendU32(buf, 3)
	for _, idx := range []uint32{1, 5, 10} {
		buf = appendU32(buf, idx)
		buf = appendU16(buf, ErrCodeTxNotFound)
		buf = appendU16(buf, 0)
	}
	errors, err := decodeSparseErrors(buf)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(errors); i++ {
		if errors[i].ItemIndex <= errors[i-1].ItemIndex {
			t.Errorf("indices not ascending: %d <= %d", errors[i].ItemIndex, errors[i-1].ItemIndex)
		}
	}
}

// ---------------------------------------------------------------------------
// Partial with signals decode
// ---------------------------------------------------------------------------

func TestPartialWithSignalsRoundTrip(t *testing.T) {
	var buf []byte
	// Successes section
	buf = appendU32(buf, 2) // success_count
	// Success 1: item_index=0, signal=1 (AllSpent), 2 block_ids
	buf = appendU32(buf, 0)
	buf = append(buf, 1) // signal
	buf = append(buf, 2) // bid_count
	buf = appendU32(buf, 42)
	buf = appendU32(buf, 43)
	// Success 2: item_index=2, signal=0 (None), 0 block_ids
	buf = appendU32(buf, 2)
	buf = append(buf, 0)
	buf = append(buf, 0)
	// Errors section
	buf = appendU32(buf, 1) // error_count
	buf = appendU32(buf, 1) // item_index
	buf = appendU16(buf, ErrCodeTxNotFound)
	buf = appendU16(buf, 0) // data_len

	successes, errors, err := decodePartialWithSignals(buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(successes) != 2 {
		t.Fatalf("got %d successes, want 2", len(successes))
	}
	if successes[0].Signal != 1 || len(successes[0].BlockIDs) != 2 {
		t.Errorf("success[0] = %v", successes[0])
	}
	if successes[0].BlockIDs[0] != 42 || successes[0].BlockIDs[1] != 43 {
		t.Errorf("block_ids = %v", successes[0].BlockIDs)
	}
	if len(errors) != 1 {
		t.Fatalf("got %d errors, want 1", len(errors))
	}
	if errors[0].Code != ErrCodeTxNotFound {
		t.Errorf("error code = %d", errors[0].Code)
	}
}

// ---------------------------------------------------------------------------
// Error payload decode
// ---------------------------------------------------------------------------

func TestErrorPayloadDecode(t *testing.T) {
	var buf []byte
	msg := "something went wrong"
	buf = appendU16(buf, ErrCodeInternal)
	buf = appendU16(buf, uint16(len(msg)))
	buf = append(buf, msg...)

	code, message, err := decodeErrorPayload(buf)
	if err != nil {
		t.Fatal(err)
	}
	if code != ErrCodeInternal {
		t.Errorf("code = %d, want %d", code, ErrCodeInternal)
	}
	if message != msg {
		t.Errorf("message = %q, want %q", message, msg)
	}
}

// ---------------------------------------------------------------------------
// Redirect decode
// ---------------------------------------------------------------------------

func TestRedirectDecode(t *testing.T) {
	addr := "192.168.1.10:3300"
	var buf []byte
	buf = appendU16(buf, uint16(len(addr)))
	buf = append(buf, addr...)

	decoded, err := decodeRedirect(buf)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != addr {
		t.Errorf("addr = %q, want %q", decoded, addr)
	}
}

// ---------------------------------------------------------------------------
// GetBatch response decode
// ---------------------------------------------------------------------------

func TestGetResponseMixedOKNotFound(t *testing.T) {
	var buf []byte
	buf = appendU32(buf, 3) // count
	// Item 1: OK with data
	buf = append(buf, 0) // status OK
	buf = appendU32(buf, 5)
	buf = append(buf, 1, 2, 3, 4, 5)
	// Item 2: error (not found)
	buf = append(buf, 1) // status Error
	buf = appendU32(buf, 0)
	// Item 3: OK with data
	buf = append(buf, 0)
	buf = appendU32(buf, 3)
	buf = append(buf, 0xAA, 0xBB, 0xCC)

	results, err := decodeGetResponse(buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}
	if results[0].Status != 0 || len(results[0].Data) != 5 {
		t.Errorf("result[0] = %v", results[0])
	}
	if results[1].Status != 1 || len(results[1].Data) != 0 {
		t.Errorf("result[1] = %v", results[1])
	}
	if results[2].Status != 0 || len(results[2].Data) != 3 {
		t.Errorf("result[2] = %v", results[2])
	}
}

// ---------------------------------------------------------------------------
// GetSpend response decode
// ---------------------------------------------------------------------------

func TestGetSpendResponseMixedStatuses(t *testing.T) {
	var buf []byte
	buf = appendU32(buf, 4) // count
	// Item 1: unspent
	buf = append(buf, 0)
	buf = appendU16(buf, ErrCodeOK)
	buf = append(buf, SlotUnspent)
	buf = append(buf, make([]byte, 36)...)
	// Item 2: spent
	buf = append(buf, 0)
	buf = appendU16(buf, ErrCodeOK)
	buf = append(buf, SlotSpent)
	buf = append(buf, bytes.Repeat([]byte{0xAB}, 36)...)
	// Item 3: pruned
	buf = append(buf, 0)
	buf = appendU16(buf, ErrCodeOK)
	buf = append(buf, SlotPruned)
	buf = append(buf, bytes.Repeat([]byte{0xCD}, 36)...)
	// Item 4: frozen
	buf = append(buf, 0)
	buf = appendU16(buf, ErrCodeOK)
	buf = append(buf, SlotFrozen)
	buf = append(buf, bytes.Repeat([]byte{0xFF}, 36)...)

	results, err := decodeGetSpendResponse(buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 4 {
		t.Fatalf("got %d results, want 4", len(results))
	}
	if results[0].SlotStatus != SlotUnspent {
		t.Errorf("result[0] slot = %d", results[0].SlotStatus)
	}
	if results[1].SlotStatus != SlotSpent {
		t.Errorf("result[1] slot = %d", results[1].SlotStatus)
	}
	if results[3].SlotStatus != SlotFrozen {
		t.Errorf("result[3] slot = %d", results[3].SlotStatus)
	}
}

// ---------------------------------------------------------------------------
// Coinbase immature error data
// ---------------------------------------------------------------------------

func TestCoinbaseImmatureErrorData(t *testing.T) {
	var buf []byte
	spendingHeight := uint32(800_100)
	buf = appendU32(buf, 1) // error_count
	buf = appendU32(buf, 7) // item_index
	buf = appendU16(buf, ErrCodeCoinbaseImmature)
	buf = appendU16(buf, 4) // data_len
	buf = appendU32(buf, spendingHeight)

	errors, err := decodeSparseErrors(buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(errors) != 1 {
		t.Fatalf("got %d errors, want 1", len(errors))
	}
	if len(errors[0].Data) != 4 {
		t.Fatalf("data len = %d, want 4", len(errors[0].Data))
	}
	recovered := getU32(errors[0].Data)
	if recovered != 800_100 {
		t.Errorf("spending_height = %d, want 800100", recovered)
	}
}

// ---------------------------------------------------------------------------
// Partition map decode
// ---------------------------------------------------------------------------

func TestPartitionMapDecodeSingleNode(t *testing.T) {
	// Build a single-node partition map as the server would.
	var buf []byte
	buf = appendU64(buf, 0) // version
	buf = appendU32(buf, 1) // 1 node
	buf = appendU64(buf, 0) // node_id
	addr := "127.0.0.1:3300"
	buf = appendU16(buf, uint16(len(addr)))
	buf = append(buf, addr...)
	// All 4096 shards -> node 0
	for range NumShards {
		buf = appendU64(buf, 0)
	}

	pm, err := decodePartitionMap(buf)
	if err != nil {
		t.Fatal(err)
	}
	if pm.Version != 0 {
		t.Errorf("version = %d, want 0", pm.Version)
	}
	if len(pm.Nodes) != 1 {
		t.Fatalf("nodes = %d, want 1", len(pm.Nodes))
	}
	if pm.Nodes[0].Addr != addr {
		t.Errorf("addr = %q, want %q", pm.Nodes[0].Addr, addr)
	}
	for i, a := range pm.Assignments {
		if a != 0 {
			t.Errorf("shard %d = %d, want 0", i, a)
			break
		}
	}
}

// ---------------------------------------------------------------------------
// QueryOldUnmined / ProcessExpired
// ---------------------------------------------------------------------------

func TestQueryOldUnminedEncode(t *testing.T) {
	encoded := encodeQueryOldUnmined(nil, 5000)
	if len(encoded) != 4 {
		t.Fatalf("encoded length = %d, want 4", len(encoded))
	}
	if getU32(encoded) != 5000 {
		t.Errorf("cutoff = %d, want 5000", getU32(encoded))
	}
}

func TestQueryOldUnminedResponseDecode(t *testing.T) {
	t1, t2, t3 := testTxID(1), testTxID(2), testTxID(3)
	var buf []byte
	buf = appendU32(buf, 3)
	buf = append(buf, t1[:]...)
	buf = append(buf, t2[:]...)
	buf = append(buf, t3[:]...)

	txids, err := decodeQueryOldUnminedResponse(buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(txids) != 3 {
		t.Fatalf("got %d txids, want 3", len(txids))
	}
	if txids[0] != testTxID(1) || txids[2] != testTxID(3) {
		t.Error("txid mismatch")
	}
}

func TestProcessExpiredResponseDecode(t *testing.T) {
	var buf []byte
	buf = appendU32(buf, 42)
	buf = appendU32(buf, 3)

	deleted, failed, err := decodeProcessExpiredResponse(buf)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 42 {
		t.Errorf("deleted = %d, want 42", deleted)
	}
	if failed != 3 {
		t.Errorf("failed = %d, want 3", failed)
	}
}
