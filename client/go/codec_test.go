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
	var sd SpendingData
	for i := range sd {
		sd[i] = byte(i + 1)
	}
	items := []UnspendItem{{
		TxID:         testTxID(1),
		Vout:         3,
		UtxoHash:     testUtxoHash(2),
		SpendingData: sd,
	}}
	encoded := encodeUnspendBatch(nil, params, items)
	// Header = 12 bytes, per-item = 104 bytes (txid32 + vout4 + utxo32 + spending36).
	if len(encoded) != 12+104 {
		t.Fatalf("encoded length = %d, want %d", len(encoded), 12+104)
	}
	// Parse fields back out and verify each one round-trips byte-for-byte.
	if got := getU32(encoded[0:4]); got != 1 {
		t.Errorf("count = %d, want 1", got)
	}
	if got := getU32(encoded[4:8]); got != 500 {
		t.Errorf("cbh = %d, want 500", got)
	}
	if got := getU32(encoded[8:12]); got != 288 {
		t.Errorf("bhr = %d, want 288", got)
	}
	if !bytes.Equal(encoded[12:44], items[0].TxID[:]) {
		t.Errorf("txid mismatch")
	}
	if got := getU32(encoded[44:48]); got != 3 {
		t.Errorf("vout = %d, want 3", got)
	}
	if !bytes.Equal(encoded[48:80], items[0].UtxoHash[:]) {
		t.Errorf("utxo_hash mismatch")
	}
	if !bytes.Equal(encoded[80:116], items[0].SpendingData[:]) {
		t.Errorf("spending_data mismatch")
	}
}

func TestUnspendBatch512Items(t *testing.T) {
	params := UnspendBatchParams{CurrentBlockHeight: 1000, BlockHeightRetention: 144}
	items := make([]UnspendItem, 512)
	encoded := encodeUnspendBatch(nil, params, items)
	if len(encoded) != 12+512*104 {
		t.Fatalf("encoded length = %d, want %d", len(encoded), 12+512*104)
	}
}

// TestUnspendBatchMatchesRustWireSize is a regression guard for the A-04 fix.
// The Rust server decode (src/protocol/codec.rs::decode_unspend_batch) requires
// each item to be exactly 104 bytes. Anything shorter is rejected as
// CodecError::TruncatedBatch.
func TestUnspendBatchMatchesRustWireSize(t *testing.T) {
	const rustPerItemBytes = 104
	const rustHeaderBytes = 12
	if unspendBatchSize(0) != rustHeaderBytes {
		t.Errorf("unspendBatchSize(0) = %d, want %d", unspendBatchSize(0), rustHeaderBytes)
	}
	if unspendBatchSize(7) != rustHeaderBytes+7*rustPerItemBytes {
		t.Errorf("unspendBatchSize(7) = %d, want %d", unspendBatchSize(7), rustHeaderBytes+7*rustPerItemBytes)
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
			TxID:        testTxID(n),
			TxVersion:   2,
			Fee:         1000 + uint64(i),
			SizeInBytes: 250,
			CreatedAt:   1700000000000 + uint64(i),
			UtxoHashes:  hashes,
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

func TestCreateBatchWithTxData(t *testing.T) {
	bid := uint32(42)
	bh := uint32(800_000)
	si := uint32(7)
	items := []CreateItem{{
		TxID:           testTxID(1),
		TxVersion:      1,
		Locktime:       500_000,
		Fee:            5000,
		SizeInBytes:    1024,
		ExtendedSize:   2048,
		IsCoinbase:     true,
		SpendingHeight: 100,
		CreatedAt:      1700000000000,
		Flags:          0x01,
		UtxoHashes:     []UtxoHash{{0xAA}, {0xBB}},
		TxData: TxData{
			Inputs:  []byte{0xDE, 0xAD, 0xBE, 0xEF},
			Outputs: []byte{0x01, 0x02, 0x03},
		},
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
	if len(encoded) != 8+4096*32 {
		t.Fatalf("encoded length = %d, want %d", len(encoded), 8+4096*32)
	}
	count := getU32(encoded[0:4])
	mask := getU32(encoded[4:8])
	if count != 4096 {
		t.Errorf("count = %d, want 4096", count)
	}
	if mask != FieldAll {
		t.Errorf("mask = 0x%08X, want 0x%08X", mask, FieldAll)
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
		items[i].UtxoHash[0] = byte(i >> 16)
		items[i].UtxoHash[31] = byte(i)
	}
	encoded := encodeGetSpendBatch(nil, items)
	if len(encoded) != 4+1024*68 {
		t.Fatalf("encoded length = %d, want %d", len(encoded), 4+1024*68)
	}
}

// TestGetSpendBatchByteLayout parses encoded payload back field-by-field,
// matching the Rust server's decode logic in
// src/protocol/codec.rs::decode_get_spend_batch.
func TestGetSpendBatchByteLayout(t *testing.T) {
	items := []GetSpendItem{
		{TxID: testTxID(0xAA), Vout: 0, UtxoHash: testUtxoHash(0x11)},
		{TxID: testTxID(0xBB), Vout: 0xDEADBEEF, UtxoHash: testUtxoHash(0x22)},
	}
	encoded := encodeGetSpendBatch(nil, items)

	if len(encoded) != 4+2*68 {
		t.Fatalf("encoded length = %d, want %d", len(encoded), 4+2*68)
	}
	if got := getU32(encoded[0:4]); got != 2 {
		t.Errorf("count = %d, want 2", got)
	}
	for i, item := range items {
		base := 4 + i*68
		if !bytes.Equal(encoded[base:base+32], item.TxID[:]) {
			t.Errorf("item %d txid mismatch", i)
		}
		if got := getU32(encoded[base+32 : base+36]); got != item.Vout {
			t.Errorf("item %d vout = %d, want %d", i, got, item.Vout)
		}
		if !bytes.Equal(encoded[base+36:base+68], item.UtxoHash[:]) {
			t.Errorf("item %d utxo_hash mismatch", i)
		}
	}
}

// TestGetSpendBatchMatchesRustWireSize is a regression guard. Rust decode
// requires 68 bytes/item; the pre-fix Go client emitted 36, so every batch
// failed with TruncatedBatch.
func TestGetSpendBatchMatchesRustWireSize(t *testing.T) {
	const rustPerItemBytes = 68
	const rustHeaderBytes = 4
	if getSpendBatchSize(0) != rustHeaderBytes {
		t.Errorf("getSpendBatchSize(0) = %d, want %d", getSpendBatchSize(0), rustHeaderBytes)
	}
	if getSpendBatchSize(11) != rustHeaderBytes+11*rustPerItemBytes {
		t.Errorf("getSpendBatchSize(11) = %d, want %d", getSpendBatchSize(11), rustHeaderBytes+11*rustPerItemBytes)
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
	buf = appendU32(buf, 3)                 // item_index
	buf = appendU16(buf, ErrCodeTxNotFound) // error_code
	buf = appendU16(buf, 0)                 // data_len
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
	// Build a single-node partition map EXACTLY as the server does, including
	// the per-node is_alive byte: [id:8][addr_len:2][addr:N][is_alive:1].
	var buf []byte
	buf = appendU64(buf, 0) // version
	buf = appendU32(buf, 1) // 1 node
	buf = appendU64(buf, 0) // node_id
	addr := "127.0.0.1:3300"
	buf = appendU16(buf, uint16(len(addr)))
	buf = append(buf, addr...)
	buf = append(buf, 1) // is_alive
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

// Regression guard for the is_alive wire-format byte: a multi-node map with
// NON-ZERO node ids and a VARIED shard→node assignment, encoded exactly as the
// server does (per-node is_alive byte). If the decoder fails to consume is_alive,
// every node address after the first and every shard assignment is read 1 byte
// misaligned — which this test detects (the all-zero single-node test above
// cannot, since shifted zeros are still zero).
func TestPartitionMapDecodeConsumesIsAlive(t *testing.T) {
	nodes := []NodeInfo{
		{ID: 5, Addr: "10.0.0.5:3300"},
		{ID: 9, Addr: "10.0.0.9:3301"},
	}
	// shard i -> nodes[i%2].ID (5,9,5,9,...) — a shift would scramble these.
	pm, err := decodePartitionMap(encodePartitionMapAssign(7, nodes, func(shard int) uint64 {
		return nodes[shard%2].ID
	}))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if pm.Version != 7 {
		t.Errorf("version = %d, want 7", pm.Version)
	}
	if len(pm.Nodes) != 2 {
		t.Fatalf("nodes = %d, want 2", len(pm.Nodes))
	}
	if pm.Nodes[0].ID != 5 || pm.Nodes[0].Addr != "10.0.0.5:3300" {
		t.Errorf("node0 = (%d,%q), want (5,10.0.0.5:3300)", pm.Nodes[0].ID, pm.Nodes[0].Addr)
	}
	if pm.Nodes[1].ID != 9 || pm.Nodes[1].Addr != "10.0.0.9:3301" {
		t.Errorf("node1 = (%d,%q), want (9,10.0.0.9:3301)", pm.Nodes[1].ID, pm.Nodes[1].Addr)
	}
	for i, a := range pm.Assignments {
		want := nodes[i%2].ID
		if a != want {
			t.Fatalf("shard %d -> %d, want %d (is_alive byte not consumed?)", i, a, want)
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

func TestQueryConflictingEncode(t *testing.T) {
	encoded := encodeQueryConflicting(nil)
	if len(encoded) != 0 {
		t.Fatalf("encoded length = %d, want 0 (empty payload)", len(encoded))
	}
}

func TestQueryConflictingResponseDecode(t *testing.T) {
	t1, t2, t3 := testTxID(1), testTxID(2), testTxID(3)
	var buf []byte
	buf = appendU32(buf, 3)
	buf = append(buf, t1[:]...)
	buf = append(buf, t2[:]...)
	buf = append(buf, t3[:]...)

	txids, err := decodeQueryConflictingResponse(buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(txids) != 3 {
		t.Fatalf("got %d txids, want 3", len(txids))
	}
	if txids[0] != testTxID(1) || txids[1] != testTxID(2) || txids[2] != testTxID(3) {
		t.Error("txid mismatch")
	}
}

func TestRemoveConflictingChildBatchRoundTrip(t *testing.T) {
	pairs := []ConflictingChildPair{
		{Parent: testTxID(1), Child: testTxID(11)},
		{Parent: testTxID(2), Child: testTxID(22)},
	}
	encoded := encodeRemoveConflictingChildBatch(nil, pairs)

	// count(4) + 2 * (parent(32) + child(32)) = 4 + 128 = 132
	if len(encoded) != 4+2*64 {
		t.Fatalf("encoded length = %d, want %d", len(encoded), 4+2*64)
	}
	if getU32(encoded[0:4]) != 2 {
		t.Errorf("count = %d, want 2", getU32(encoded[0:4]))
	}

	// Verify byte layout: parent comes first within each 64-byte item.
	pos := 4
	for i, p := range pairs {
		var gotParent, gotChild TxID
		copy(gotParent[:], encoded[pos:pos+32])
		copy(gotChild[:], encoded[pos+32:pos+64])
		if gotParent != p.Parent {
			t.Errorf("pair %d: parent mismatch", i)
		}
		if gotChild != p.Child {
			t.Errorf("pair %d: child mismatch", i)
		}
		pos += 64
	}
}

func TestRemoveConflictingChildBatchEmpty(t *testing.T) {
	encoded := encodeRemoveConflictingChildBatch(nil, nil)
	if len(encoded) != 4 {
		t.Fatalf("encoded length = %d, want 4 (just count)", len(encoded))
	}
	if getU32(encoded[0:4]) != 0 {
		t.Errorf("count = %d, want 0", getU32(encoded[0:4]))
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

// ---------------------------------------------------------------------------
// Stream chunk / end encode
// ---------------------------------------------------------------------------

func TestEncodeStreamChunkLayout(t *testing.T) {
	txid := testTxID(0xAA)
	data := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	offset := uint64(4 * 1024 * 1024)

	encoded := encodeStreamChunk(nil, txid, offset, data)

	// Expected: txid(32) + offset(8) + chunk_data_len(4) + data(5) = 49
	if len(encoded) != 32+8+4+5 {
		t.Fatalf("encoded length = %d, want %d", len(encoded), 49)
	}

	// Verify txid.
	var gotTxID TxID
	copy(gotTxID[:], encoded[0:32])
	if gotTxID != txid {
		t.Errorf("txid mismatch")
	}

	// Verify offset.
	gotOffset := getU64(encoded[32:40])
	if gotOffset != offset {
		t.Errorf("offset = %d, want %d", gotOffset, offset)
	}

	// Verify data length.
	gotDataLen := getU32(encoded[40:44])
	if gotDataLen != 5 {
		t.Errorf("data_len = %d, want 5", gotDataLen)
	}

	// Verify data.
	if !bytes.Equal(encoded[44:49], data) {
		t.Errorf("data mismatch: got %x, want %x", encoded[44:49], data)
	}
}

func TestEncodeStreamChunkLargePayload(t *testing.T) {
	txid := testTxID(1)
	data := make([]byte, BlobChunkSize)
	for i := range data {
		data[i] = byte(i & 0xFF)
	}

	encoded := encodeStreamChunk(nil, txid, 0, data)

	expectedLen := 32 + 8 + 4 + BlobChunkSize
	if len(encoded) != expectedLen {
		t.Fatalf("encoded length = %d, want %d", len(encoded), expectedLen)
	}

	gotDataLen := getU32(encoded[40:44])
	if gotDataLen != uint32(BlobChunkSize) {
		t.Errorf("data_len = %d, want %d", gotDataLen, BlobChunkSize)
	}

	if !bytes.Equal(encoded[44:], data) {
		t.Error("data mismatch in large payload")
	}
}

func TestEncodeStreamEndLayout(t *testing.T) {
	txid := testTxID(0xBB)
	totalSize := uint64(10 * 1024 * 1024)

	encoded := encodeStreamEnd(nil, txid, totalSize)

	// Expected: txid(32) + total_size(8) = 40
	if len(encoded) != 40 {
		t.Fatalf("encoded length = %d, want 40", len(encoded))
	}

	var gotTxID TxID
	copy(gotTxID[:], encoded[0:32])
	if gotTxID != txid {
		t.Errorf("txid mismatch")
	}

	gotTotal := getU64(encoded[32:40])
	if gotTotal != totalSize {
		t.Errorf("total_size = %d, want %d", gotTotal, totalSize)
	}
}

func TestEncodeStreamChunkAppendsToBuffer(t *testing.T) {
	// Verify that encodeStreamChunk appends to the provided buffer
	// rather than replacing it.
	prefix := []byte{0xFF, 0xFE}
	data := []byte{0x01, 0x02}
	encoded := encodeStreamChunk(prefix, testTxID(1), 0, data)

	if encoded[0] != 0xFF || encoded[1] != 0xFE {
		t.Error("prefix bytes were overwritten")
	}
	if len(encoded) != 2+32+8+4+2 {
		t.Fatalf("total length = %d, want %d", len(encoded), 2+32+8+4+2)
	}
}

// ---------------------------------------------------------------------------
// Cold data helpers
// ---------------------------------------------------------------------------

func TestEncodeColdDataWithContent(t *testing.T) {
	item := CreateItem{
		TxData: TxData{
			Inputs:   []byte{0xDE, 0xAD},
			Outputs:  []byte{0xBE, 0xEF, 0x01},
			Inpoints: []byte{0xCA, 0xFE},
		},
	}
	cold := encodeColdData(&item)
	if cold == nil {
		t.Fatal("expected non-nil cold data")
	}

	// Format: [inputs_len:4][inputs:2][outputs_len:4][outputs:3][inpoints_len:4][inpoints:2] = 19
	if len(cold) != 4+2+4+3+4+2 {
		t.Fatalf("cold data length = %d, want %d", len(cold), 19)
	}

	pos := 0
	inputsLen := getU32(cold[pos : pos+4])
	pos += 4
	if inputsLen != 2 {
		t.Errorf("inputs_len = %d, want 2", inputsLen)
	}
	if !bytes.Equal(cold[pos:pos+2], []byte{0xDE, 0xAD}) {
		t.Error("inputs mismatch")
	}
	pos += 2

	outputsLen := getU32(cold[pos : pos+4])
	pos += 4
	if outputsLen != 3 {
		t.Errorf("outputs_len = %d, want 3", outputsLen)
	}
	if !bytes.Equal(cold[pos:pos+3], []byte{0xBE, 0xEF, 0x01}) {
		t.Error("outputs mismatch")
	}
	pos += 3

	inpointsLen := getU32(cold[pos : pos+4])
	pos += 4
	if inpointsLen != 2 {
		t.Errorf("inpoints_len = %d, want 2", inpointsLen)
	}
	if !bytes.Equal(cold[pos:pos+2], []byte{0xCA, 0xFE}) {
		t.Error("inpoints mismatch")
	}
}

func TestEncodeColdDataEmpty(t *testing.T) {
	item := CreateItem{}
	cold := encodeColdData(&item)
	if cold != nil {
		t.Errorf("expected nil cold data for empty TxData, got %d bytes", len(cold))
	}
}

func TestColdDataSizeCalculation(t *testing.T) {
	item := CreateItem{
		TxData: TxData{
			Inputs:   make([]byte, 100),
			Outputs:  make([]byte, 200),
			Inpoints: make([]byte, 50),
		},
	}
	got := coldDataSize(&item)
	want := 4 + 100 + 4 + 200 + 4 + 50
	if got != want {
		t.Errorf("coldDataSize = %d, want %d", got, want)
	}
}

func TestColdDataSizeEmpty(t *testing.T) {
	item := CreateItem{}
	if coldDataSize(&item) != 0 {
		t.Errorf("coldDataSize for empty item = %d, want 0", coldDataSize(&item))
	}
}
