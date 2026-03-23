package teraslab

import (
	"context"
	"net"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Encode path benchmarks — using pooled buffers (production path)
// ---------------------------------------------------------------------------

func BenchmarkEncodeSpendBatch1(b *testing.B) {
	params := SpendBatchParams{CurrentBlockHeight: 1000, BlockHeightRetention: 288}
	items := []SpendItem{{TxID: testTxID(1), Vout: 0, UtxoHash: testUtxoHash(2)}}
	b.ReportAllocs()
	for b.Loop() {
		buf := getBuf(spendBatchSize(len(items)))
		buf = encodeSpendBatch(buf, params, items)
		putBuf(buf)
	}
}

func BenchmarkEncodeSpendBatch1024(b *testing.B) {
	params := SpendBatchParams{CurrentBlockHeight: 1000, BlockHeightRetention: 288}
	items := make([]SpendItem, 1024)
	for i := range items {
		items[i].TxID[0] = byte(i)
		items[i].Vout = uint32(i)
	}
	b.ReportAllocs()
	for b.Loop() {
		buf := getBuf(spendBatchSize(len(items)))
		buf = encodeSpendBatch(buf, params, items)
		putBuf(buf)
	}
}

func BenchmarkEncodeSetMinedBatch1024(b *testing.B) {
	params := SetMinedBatchParams{BlockID: 42, BlockHeight: 800000, SubtreeIdx: 7, OnLongestChain: true, CurrentBlockHeight: 800000, BlockHeightRetention: 288}
	txids := make([]TxID, 1024)
	b.ReportAllocs()
	for b.Loop() {
		buf := getBuf(26 + len(txids)*32)
		buf = encodeSetMinedBatch(buf, params, txids)
		putBuf(buf)
	}
}

func BenchmarkEncodeCreateBatch100(b *testing.B) {
	items := make([]CreateItem, 100)
	for i := range items {
		items[i].TxID[0] = byte(i)
		items[i].TxVersion = 2
		items[i].Fee = 1000
		items[i].UtxoHashes = []UtxoHash{{byte(i)}, {byte(i + 1)}}
	}
	b.ReportAllocs()
	for b.Loop() {
		buf := getBuf(4 + len(items)*128)
		buf = encodeCreateBatch(buf, items)
		putBuf(buf)
	}
}

func BenchmarkEncodeGetBatch4096(b *testing.B) {
	txids := make([]TxID, 4096)
	b.ReportAllocs()
	for b.Loop() {
		buf := getBuf(8 + len(txids)*32)
		buf = encodeGetBatch(buf, FieldAll, txids)
		putBuf(buf)
	}
}

func BenchmarkEncodeGetSpendBatch1024(b *testing.B) {
	items := make([]GetSpendItem, 1024)
	b.ReportAllocs()
	for b.Loop() {
		buf := getBuf(4 + len(items)*36)
		buf = encodeGetSpendBatch(buf, items)
		putBuf(buf)
	}
}

func BenchmarkEncodeDeleteBatch256(b *testing.B) {
	txids := make([]TxID, 256)
	b.ReportAllocs()
	for b.Loop() {
		buf := getBuf(4 + len(txids)*32)
		buf = encodeDeleteBatch(buf, txids)
		putBuf(buf)
	}
}

func BenchmarkEncodeSlotItemBatch50(b *testing.B) {
	items := make([]FreezeItem, 50)
	b.ReportAllocs()
	for b.Loop() {
		buf := getBuf(4 + len(items)*68)
		buf = encodeSlotItemBatch(buf, items)
		putBuf(buf)
	}
}

func BenchmarkEncodeSetLockedBatch1024(b *testing.B) {
	txids := make([]TxID, 1024)
	b.ReportAllocs()
	for b.Loop() {
		buf := getBuf(4 + 1 + len(txids)*32)
		buf = encodeSetLockedBatch(buf, true, txids)
		putBuf(buf)
	}
}

func BenchmarkEncodeSetConflictingBatch(b *testing.B) {
	params := SetConflictingParams{Value: true, CurrentBlockHeight: 500, BlockHeightRetention: 288}
	txids := make([]TxID, 1024)
	b.ReportAllocs()
	for b.Loop() {
		buf := getBuf(4 + 9 + len(txids)*32)
		buf = encodeSetConflictingBatch(buf, params, txids)
		putBuf(buf)
	}
}

func BenchmarkEncodePreserveUntilBatch(b *testing.B) {
	txids := make([]TxID, 1024)
	b.ReportAllocs()
	for b.Loop() {
		buf := getBuf(4 + 4 + len(txids)*32)
		buf = encodePreserveUntilBatch(buf, 5000, txids)
		putBuf(buf)
	}
}

func BenchmarkEncodeMarkLongestChainBatch(b *testing.B) {
	params := MarkLongestChainParams{OnLongestChain: true, CurrentBlockHeight: 1000, BlockHeightRetention: 288}
	txids := make([]TxID, 1024)
	b.ReportAllocs()
	for b.Loop() {
		buf := getBuf(4 + 9 + len(txids)*32)
		buf = encodeMarkLongestChainBatch(buf, params, txids)
		putBuf(buf)
	}
}

// ---------------------------------------------------------------------------
// Wire encode/decode benchmarks
// ---------------------------------------------------------------------------

func BenchmarkEncodeRequest(b *testing.B) {
	payload := make([]byte, 14+1024*104) // SpendBatch 1024 items
	f := &requestFrame{RequestID: 1, OpCode: OpSpendBatch, Payload: payload}
	b.ReportAllocs()
	for b.Loop() {
		buf := getBuf(requestHeaderSize + len(payload))
		buf = encodeRequest(buf, f)
		putBuf(buf)
	}
}

func BenchmarkEncodeRequestSmall(b *testing.B) {
	f := &requestFrame{RequestID: 1, OpCode: OpPing}
	b.ReportAllocs()
	for b.Loop() {
		buf := getBuf(requestHeaderSize)
		buf = encodeRequest(buf, f)
		putBuf(buf)
	}
}

// ---------------------------------------------------------------------------
// Decode response benchmarks
// ---------------------------------------------------------------------------

func BenchmarkDecodeSparseErrors3(b *testing.B) {
	var buf []byte
	buf = appendU32(buf, 3)
	for _, idx := range []uint32{3, 7, 999} {
		buf = appendU32(buf, idx)
		buf = appendU16(buf, ErrCodeTxNotFound)
		buf = appendU16(buf, 0)
	}
	b.ReportAllocs()
	for b.Loop() {
		decodeSparseErrors(buf)
	}
}

func BenchmarkDecodePartialWithSignals(b *testing.B) {
	var buf []byte
	buf = appendU32(buf, 100)
	for i := range 100 {
		buf = appendU32(buf, uint32(i))
		buf = append(buf, 1) // signal
		buf = append(buf, 2) // bid_count
		buf = appendU32(buf, 42)
		buf = appendU32(buf, 43)
	}
	buf = appendU32(buf, 5)
	for _, idx := range []uint32{10, 20, 30, 40, 50} {
		buf = appendU32(buf, idx)
		buf = appendU16(buf, ErrCodeTxNotFound)
		buf = appendU16(buf, 0)
	}
	b.ReportAllocs()
	for b.Loop() {
		decodePartialWithSignals(buf)
	}
}

func BenchmarkDecodeGetResponse10(b *testing.B) {
	var buf []byte
	buf = appendU32(buf, 10)
	for range 10 {
		buf = append(buf, 0) // status OK
		buf = appendU32(buf, 100)
		buf = append(buf, make([]byte, 100)...)
	}
	b.ReportAllocs()
	for b.Loop() {
		decodeGetResponse(buf)
	}
}

func BenchmarkDecodeGetSpendResponse1024(b *testing.B) {
	var buf []byte
	buf = appendU32(buf, 1024)
	for range 1024 {
		buf = append(buf, 0) // status
		buf = appendU16(buf, 0)
		buf = append(buf, 0) // slot_status
		buf = append(buf, make([]byte, 36)...)
	}
	b.ReportAllocs()
	for b.Loop() {
		decodeGetSpendResponse(buf)
	}
}

// ---------------------------------------------------------------------------
// Connection round-trip benchmark (includes channel alloc)
// ---------------------------------------------------------------------------

func BenchmarkRoundTrip(b *testing.B) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		var rbuf []byte
		for {
			lenBuf := make([]byte, 4)
			if _, err := readFull(conn, lenBuf); err != nil {
				return
			}
			totalLen := int(getU32(lenBuf))
			if totalLen < 12 {
				return
			}
			if cap(rbuf) < totalLen {
				rbuf = make([]byte, totalLen)
			}
			rbuf = rbuf[:totalLen]
			if _, err := readFull(conn, rbuf); err != nil {
				return
			}
			reqID := getU64(rbuf[0:8])
			resp := encodeResponseFrame(responseFrame{
				RequestID: reqID,
				Status:    StatusOK,
			})
			if _, err := conn.Write(resp); err != nil {
				return
			}
		}
	}()

	pc, err := dial(context.Background(), ln.Addr().String(), 5*time.Second)
	if err != nil {
		b.Fatal(err)
	}
	defer pc.close()

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		pc.roundTrip(ctx, OpPing, 0, nil)
	}
}

// ---------------------------------------------------------------------------
// readResponse benchmark
// ---------------------------------------------------------------------------

func BenchmarkReadResponse(b *testing.B) {
	payload := make([]byte, 200)
	innerLen := 8 + 1 + len(payload)
	raw := make([]byte, 4+innerLen)
	putU32(raw[0:4], uint32(innerLen))
	putU64(raw[4:12], 1)
	raw[12] = StatusOK
	copy(raw[13:], payload)

	b.ReportAllocs()
	var buf []byte
	for b.Loop() {
		r := &loopReader{data: raw}
		var err error
		_, buf, err = readResponse(r, buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

type loopReader struct {
	data []byte
	pos  int
}

func (r *loopReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		r.pos = 0
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// ---------------------------------------------------------------------------
// ShardForTxID benchmark
// ---------------------------------------------------------------------------

func BenchmarkShardForTxID(b *testing.B) {
	var txid TxID
	txid[0] = 0xAB
	txid[1] = 0xCD
	b.ReportAllocs()
	for b.Loop() {
		ShardForTxID(txid)
	}
}
