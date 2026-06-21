package teraslab

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// blobTracker records stream chunk and stream end calls received by the mock server.
type blobTracker struct {
	mu         sync.Mutex
	chunks     []blobChunkRecord
	ends       []blobEndRecord
	chunkCount atomic.Int64
	endCount   atomic.Int64
}

type blobChunkRecord struct {
	TxID   TxID
	Offset uint64
	Data   []byte
}

type blobEndRecord struct {
	TxID      TxID
	TotalSize uint64
}

// startClientTestServer starts a mock server that handles a few key operations.
func startClientTestServer(t *testing.T) net.Listener {
	t.Helper()
	return startClientTestServerWithTracker(t, nil)
}

// startClientTestServerWithTracker starts a mock server with an optional blob tracker.
func startClientTestServerWithTracker(t *testing.T, tracker *blobTracker) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handleClientTestConn(conn, tracker)
		}
	}()
	return ln
}

func handleClientTestConn(conn net.Conn, tracker *blobTracker) {
	defer conn.Close()
	var buf []byte
	for {
		lenBuf := make([]byte, 4)
		if _, err := readFull(conn, lenBuf); err != nil {
			return
		}
		totalLen := int(getU32(lenBuf))
		if totalLen < 12 {
			return
		}
		if cap(buf) < totalLen {
			buf = make([]byte, totalLen)
		}
		buf = buf[:totalLen]
		if _, err := readFull(conn, buf); err != nil {
			return
		}
		reqID := getU64(buf[0:8])
		opCode := getU16(buf[8:10])
		payload := buf[12:totalLen]

		var resp responseFrame
		switch opCode {
		case OpPing:
			resp = responseFrame{RequestID: reqID, Status: StatusOK}
		case OpHello:
			var p []byte
			p = appendU16(p, ProtocolVersion)
			resp = responseFrame{RequestID: reqID, Status: StatusOK, Payload: p}
		case OpHealth:
			resp = responseFrame{RequestID: reqID, Status: StatusOK, Payload: []byte("ok")}
		case OpGetPartitionMap:
			// Return single-node partition map.
			var p []byte
			p = appendU64(p, 0) // version
			p = appendU32(p, 1) // 1 node
			p = appendU64(p, 0) // node_id
			addr := "127.0.0.1:3300"
			p = appendU16(p, uint16(len(addr)))
			p = append(p, addr...)
			for range NumShards {
				p = appendU64(p, 0)
			}
			resp = responseFrame{RequestID: reqID, Status: StatusOK, Payload: p}
		case OpSpendBatch:
			// Return OK with empty success signals.
			var p []byte
			p = appendU32(p, 0) // 0 successes
			p = appendU32(p, 0) // 0 errors
			resp = responseFrame{RequestID: reqID, Status: StatusOK, Payload: p}
		case OpDeleteBatch:
			resp = responseFrame{RequestID: reqID, Status: StatusOK}
		case OpGetBatch:
			// Return 1 result with empty data.
			var p []byte
			p = appendU32(p, 1) // count=1
			p = append(p, 0)    // status=OK
			p = appendU32(p, 0) // data_len=0
			resp = responseFrame{RequestID: reqID, Status: StatusOK, Payload: p}
		case OpStreamChunk:
			if tracker != nil && len(payload) >= 44 {
				var txid TxID
				copy(txid[:], payload[0:32])
				offset := getU64(payload[32:40])
				dataLen := int(getU32(payload[40:44]))
				data := make([]byte, dataLen)
				if len(payload) >= 44+dataLen {
					copy(data, payload[44:44+dataLen])
				}
				tracker.mu.Lock()
				tracker.chunks = append(tracker.chunks, blobChunkRecord{
					TxID:   txid,
					Offset: offset,
					Data:   data,
				})
				tracker.mu.Unlock()
				tracker.chunkCount.Add(1)
			}
			resp = responseFrame{RequestID: reqID, Status: StatusOK}
		case OpStreamEnd:
			if tracker != nil && len(payload) >= 40 {
				var txid TxID
				copy(txid[:], payload[0:32])
				totalSize := getU64(payload[32:40])
				tracker.mu.Lock()
				tracker.ends = append(tracker.ends, blobEndRecord{
					TxID:      txid,
					TotalSize: totalSize,
				})
				tracker.mu.Unlock()
				tracker.endCount.Add(1)
			}
			resp = responseFrame{RequestID: reqID, Status: StatusOK}
		default:
			resp = responseFrame{RequestID: reqID, Status: StatusOK}
		}
		respBytes := encodeResponseFrame(resp)
		if _, err := conn.Write(respBytes); err != nil {
			return
		}
	}
}

func TestClientNew(t *testing.T) {
	ln := startClientTestServer(t)
	defer ln.Close()

	ctx := context.Background()
	client, err := New(ctx, ClientConfig{
		Addr: ln.Addr().String(),
		Pool: PoolConfig{
			MinConns:    1,
			MaxConns:    2,
			DialTimeout: 2 * time.Second,
			HealthCheck: 1 * time.Hour,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
}

func TestClientPing(t *testing.T) {
	ln := startClientTestServer(t)
	defer ln.Close()

	ctx := context.Background()
	client, err := New(ctx, ClientConfig{
		Addr: ln.Addr().String(),
		Pool: PoolConfig{MinConns: 1, MaxConns: 2, DialTimeout: 2 * time.Second, HealthCheck: 1 * time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	rtt, err := client.Ping(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rtt <= 0 {
		t.Errorf("rtt = %v, want > 0", rtt)
	}
}

func TestClientHealth(t *testing.T) {
	ln := startClientTestServer(t)
	defer ln.Close()

	ctx := context.Background()
	client, err := New(ctx, ClientConfig{
		Addr: ln.Addr().String(),
		Pool: PoolConfig{MinConns: 1, MaxConns: 2, DialTimeout: 2 * time.Second, HealthCheck: 1 * time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Health(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestClientSpendBatch(t *testing.T) {
	ln := startClientTestServer(t)
	defer ln.Close()

	ctx := context.Background()
	client, err := New(ctx, ClientConfig{
		Addr: ln.Addr().String(),
		Pool: PoolConfig{MinConns: 1, MaxConns: 2, DialTimeout: 2 * time.Second, HealthCheck: 1 * time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	params := SpendBatchParams{CurrentBlockHeight: 1000, BlockHeightRetention: 288}
	items := []SpendItem{{TxID: testTxID(1), Vout: 0, UtxoHash: testUtxoHash(2)}}
	resp, err := client.SpendBatch(ctx, params, items)
	if err != nil {
		t.Fatal(err)
	}
	if resp == nil {
		t.Error("expected non-nil response")
	}
}

func TestClientDeleteBatch(t *testing.T) {
	ln := startClientTestServer(t)
	defer ln.Close()

	ctx := context.Background()
	client, err := New(ctx, ClientConfig{
		Addr: ln.Addr().String(),
		Pool: PoolConfig{MinConns: 1, MaxConns: 2, DialTimeout: 2 * time.Second, HealthCheck: 1 * time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	result, err := client.DeleteBatch(ctx, []TxID{testTxID(1)})
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Error("expected non-nil result")
	}
}

func TestClientGetBatch(t *testing.T) {
	ln := startClientTestServer(t)
	defer ln.Close()

	ctx := context.Background()
	client, err := New(ctx, ClientConfig{
		Addr: ln.Addr().String(),
		Pool: PoolConfig{MinConns: 1, MaxConns: 2, DialTimeout: 2 * time.Second, HealthCheck: 1 * time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	batch, err := client.GetBatch(ctx, FieldAll, []TxID{testTxID(1)})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Len() != 1 {
		t.Fatalf("got %d results, want 1", batch.Len())
	}
	if batch.Items[0].Status != 0 {
		t.Errorf("status = %d, want 0", batch.Items[0].Status)
	}
}

func TestClientGetPartitionMap(t *testing.T) {
	ln := startClientTestServer(t)
	defer ln.Close()

	ctx := context.Background()
	client, err := New(ctx, ClientConfig{
		Addr: ln.Addr().String(),
		Pool: PoolConfig{MinConns: 1, MaxConns: 2, DialTimeout: 2 * time.Second, HealthCheck: 1 * time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	pm, err := client.GetPartitionMap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pm.Nodes) != 1 {
		t.Fatalf("nodes = %d, want 1", len(pm.Nodes))
	}
	if pm.Nodes[0].Addr != "127.0.0.1:3300" {
		t.Errorf("addr = %q", pm.Nodes[0].Addr)
	}
}

func TestClientNewRequiresAddrOrSeeds(t *testing.T) {
	_, err := New(context.Background(), ClientConfig{})
	if err == nil {
		t.Error("expected error when neither Addr nor Seeds is set")
	}
}

func TestClientClose(t *testing.T) {
	ln := startClientTestServer(t)
	defer ln.Close()

	ctx := context.Background()
	client, err := New(ctx, ClientConfig{
		Addr: ln.Addr().String(),
		Pool: PoolConfig{MinConns: 1, MaxConns: 2, DialTimeout: 2 * time.Second, HealthCheck: 1 * time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}

	// Operations after close should fail.
	_, err = client.Ping(ctx)
	if err == nil {
		t.Error("expected error after close")
	}
}

// ---------------------------------------------------------------------------
// Blob upload tests
// ---------------------------------------------------------------------------

func TestUploadBlobSingleChunk(t *testing.T) {
	tracker := &blobTracker{}
	ln := startClientTestServerWithTracker(t, tracker)
	defer ln.Close()

	ctx := context.Background()
	client, err := New(ctx, ClientConfig{
		Addr: ln.Addr().String(),
		Pool: PoolConfig{MinConns: 1, MaxConns: 2, DialTimeout: 2 * time.Second, HealthCheck: 1 * time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	txid := testTxID(0xAA)
	// Data smaller than BlobChunkSize -> 1 chunk.
	data := make([]byte, 512*1024)
	for i := range data {
		data[i] = byte(i & 0xFF)
	}

	if err := client.uploadBlob(ctx, txid, data); err != nil {
		t.Fatal(err)
	}

	if got := tracker.chunkCount.Load(); got != 1 {
		t.Errorf("chunk count = %d, want 1", got)
	}
	if got := tracker.endCount.Load(); got != 1 {
		t.Errorf("end count = %d, want 1", got)
	}

	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	if tracker.chunks[0].TxID != txid {
		t.Error("chunk txid mismatch")
	}
	if tracker.chunks[0].Offset != 0 {
		t.Errorf("chunk offset = %d, want 0", tracker.chunks[0].Offset)
	}
	if len(tracker.chunks[0].Data) != len(data) {
		t.Errorf("chunk data len = %d, want %d", len(tracker.chunks[0].Data), len(data))
	}

	if tracker.ends[0].TxID != txid {
		t.Error("end txid mismatch")
	}
	if tracker.ends[0].TotalSize != uint64(len(data)) {
		t.Errorf("end total_size = %d, want %d", tracker.ends[0].TotalSize, len(data))
	}
}

func TestUploadBlobMultipleChunks(t *testing.T) {
	tracker := &blobTracker{}
	ln := startClientTestServerWithTracker(t, tracker)
	defer ln.Close()

	ctx := context.Background()
	client, err := New(ctx, ClientConfig{
		Addr: ln.Addr().String(),
		Pool: PoolConfig{MinConns: 1, MaxConns: 2, DialTimeout: 2 * time.Second, HealthCheck: 1 * time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	txid := testTxID(0xBB)
	// 10 MiB -> ceil(10/4) = 3 chunks (4 + 4 + 2 MiB).
	dataSize := 10 * 1024 * 1024
	data := make([]byte, dataSize)
	for i := range data {
		data[i] = byte(i % 251)
	}

	if err := client.uploadBlob(ctx, txid, data); err != nil {
		t.Fatal(err)
	}

	if got := tracker.chunkCount.Load(); got != 3 {
		t.Errorf("chunk count = %d, want 3", got)
	}
	if got := tracker.endCount.Load(); got != 1 {
		t.Errorf("end count = %d, want 1", got)
	}

	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	// Verify chunk offsets and sizes.
	expectedChunks := []struct {
		offset uint64
		size   int
	}{
		{0, BlobChunkSize},
		{BlobChunkSize, BlobChunkSize},
		{2 * BlobChunkSize, dataSize - 2*BlobChunkSize},
	}

	for i, expected := range expectedChunks {
		if tracker.chunks[i].Offset != expected.offset {
			t.Errorf("chunk[%d] offset = %d, want %d", i, tracker.chunks[i].Offset, expected.offset)
		}
		if len(tracker.chunks[i].Data) != expected.size {
			t.Errorf("chunk[%d] data len = %d, want %d", i, len(tracker.chunks[i].Data), expected.size)
		}
		if tracker.chunks[i].TxID != txid {
			t.Errorf("chunk[%d] txid mismatch", i)
		}
	}

	// Verify end.
	if tracker.ends[0].TotalSize != uint64(dataSize) {
		t.Errorf("end total_size = %d, want %d", tracker.ends[0].TotalSize, dataSize)
	}
}

func TestCreateBatchSmallColdDataNoUpload(t *testing.T) {
	tracker := &blobTracker{}
	ln := startClientTestServerWithTracker(t, tracker)
	defer ln.Close()

	ctx := context.Background()
	client, err := New(ctx, ClientConfig{
		Addr: ln.Addr().String(),
		Pool: PoolConfig{MinConns: 1, MaxConns: 2, DialTimeout: 2 * time.Second, HealthCheck: 1 * time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Cold data well below threshold (< 1 MiB).
	items := []CreateItem{{
		TxID:       testTxID(1),
		TxVersion:  2,
		UtxoHashes: []UtxoHash{{0xAA}},
		TxData: TxData{
			Inputs:  make([]byte, 100),
			Outputs: make([]byte, 200),
		},
	}}

	_, err = client.CreateBatch(ctx, items)
	if err != nil {
		t.Fatal(err)
	}

	// No blob upload should have occurred.
	if got := tracker.chunkCount.Load(); got != 0 {
		t.Errorf("chunk count = %d, want 0 (no blob upload expected)", got)
	}
	if got := tracker.endCount.Load(); got != 0 {
		t.Errorf("end count = %d, want 0", got)
	}

	// Verify original item was not mutated.
	if items[0].Flags&FlagExternalBlob != 0 {
		t.Error("original item flags should not have been modified")
	}
	if len(items[0].TxData.Inputs) != 100 {
		t.Error("original item TxData should not have been modified")
	}
}

func TestCreateBatchLargeColdDataUploadsBlob(t *testing.T) {
	tracker := &blobTracker{}
	ln := startClientTestServerWithTracker(t, tracker)
	defer ln.Close()

	ctx := context.Background()
	client, err := New(ctx, ClientConfig{
		Addr: ln.Addr().String(),
		Pool: PoolConfig{MinConns: 1, MaxConns: 2, DialTimeout: 2 * time.Second, HealthCheck: 1 * time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Cold data above threshold: 1.5 MiB of outputs.
	largeOutputs := make([]byte, BlobUploadThreshold+500*1024)
	for i := range largeOutputs {
		largeOutputs[i] = byte(i % 197)
	}

	items := []CreateItem{{
		TxID:       testTxID(0xCC),
		TxVersion:  2,
		UtxoHashes: []UtxoHash{{0xAA}},
		TxData: TxData{
			Outputs: largeOutputs,
		},
		Flags: 0x01, // pre-existing flag (coinbase)
	}}

	_, err = client.CreateBatch(ctx, items)
	if err != nil {
		t.Fatal(err)
	}

	// Blob upload should have occurred.
	if got := tracker.chunkCount.Load(); got < 1 {
		t.Errorf("chunk count = %d, want >= 1", got)
	}
	if got := tracker.endCount.Load(); got != 1 {
		t.Errorf("end count = %d, want 1", got)
	}

	tracker.mu.Lock()
	// Verify the txid matches.
	if tracker.chunks[0].TxID != testTxID(0xCC) {
		t.Error("blob upload txid mismatch")
	}
	if tracker.ends[0].TxID != testTxID(0xCC) {
		t.Error("blob end txid mismatch")
	}
	tracker.mu.Unlock()

	// Verify original item was NOT mutated.
	if items[0].Flags != 0x01 {
		t.Errorf("original flags = 0x%02X, want 0x01 (should not be mutated)", items[0].Flags)
	}
	if len(items[0].TxData.Outputs) != len(largeOutputs) {
		t.Error("original TxData should not be cleared")
	}
}

func TestCreateBatchMixedSmallAndLargeItems(t *testing.T) {
	tracker := &blobTracker{}
	ln := startClientTestServerWithTracker(t, tracker)
	defer ln.Close()

	ctx := context.Background()
	client, err := New(ctx, ClientConfig{
		Addr: ln.Addr().String(),
		Pool: PoolConfig{MinConns: 1, MaxConns: 2, DialTimeout: 2 * time.Second, HealthCheck: 1 * time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	items := []CreateItem{
		{
			TxID:       testTxID(1),
			TxVersion:  2,
			UtxoHashes: []UtxoHash{{0xAA}},
			TxData: TxData{
				Inputs: make([]byte, 100), // small
			},
		},
		{
			TxID:       testTxID(2),
			TxVersion:  2,
			UtxoHashes: []UtxoHash{{0xBB}},
			TxData: TxData{
				Outputs: make([]byte, BlobUploadThreshold+1), // large
			},
		},
		{
			TxID:       testTxID(3),
			TxVersion:  2,
			UtxoHashes: []UtxoHash{{0xCC}},
			TxData: TxData{
				Inputs: make([]byte, 200), // small
			},
		},
	}

	_, err = client.CreateBatch(ctx, items)
	if err != nil {
		t.Fatal(err)
	}

	// Only item[1] should have been uploaded.
	if got := tracker.endCount.Load(); got != 1 {
		t.Errorf("end count = %d, want 1", got)
	}

	tracker.mu.Lock()
	if tracker.ends[0].TxID != testTxID(2) {
		t.Error("blob upload should be for item[1] (txid 2)")
	}
	tracker.mu.Unlock()

	// Original items should not be mutated.
	if items[1].Flags&FlagExternalBlob != 0 {
		t.Error("original item[1] flags should not be modified")
	}
	if len(items[1].TxData.Outputs) != BlobUploadThreshold+1 {
		t.Error("original item[1] TxData should not be cleared")
	}
}

func TestUploadLargeBlobsDoesNotMutateOriginal(t *testing.T) {
	tracker := &blobTracker{}
	ln := startClientTestServerWithTracker(t, tracker)
	defer ln.Close()

	ctx := context.Background()
	client, err := New(ctx, ClientConfig{
		Addr: ln.Addr().String(),
		Pool: PoolConfig{MinConns: 1, MaxConns: 2, DialTimeout: 2 * time.Second, HealthCheck: 1 * time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	originalOutputs := make([]byte, BlobUploadThreshold+100)
	items := []CreateItem{{
		TxID:       testTxID(1),
		Flags:      0x02, // conflicting flag
		UtxoHashes: []UtxoHash{{0xAA}},
		TxData: TxData{
			Outputs: originalOutputs,
		},
	}}

	result, err := client.uploadLargeBlobs(ctx, items)
	if err != nil {
		t.Fatal(err)
	}

	// The returned slice should be a different slice.
	if &result[0] == &items[0] {
		t.Error("uploadLargeBlobs should return a copy, not the original")
	}

	// Result should have FlagExternalBlob set and TxData cleared.
	if result[0].Flags&FlagExternalBlob == 0 {
		t.Error("result flags should include FlagExternalBlob")
	}
	// The conflicting flag should be preserved.
	if result[0].Flags&0x02 == 0 {
		t.Error("result flags should preserve existing flags")
	}
	if len(result[0].TxData.Outputs) != 0 {
		t.Error("result TxData should be cleared")
	}

	// Original should be untouched.
	if items[0].Flags != 0x02 {
		t.Errorf("original flags = 0x%02X, want 0x02", items[0].Flags)
	}
	if len(items[0].TxData.Outputs) != len(originalOutputs) {
		t.Error("original TxData should not be cleared")
	}
}

func TestUploadBlobExactChunkBoundary(t *testing.T) {
	tracker := &blobTracker{}
	ln := startClientTestServerWithTracker(t, tracker)
	defer ln.Close()

	ctx := context.Background()
	client, err := New(ctx, ClientConfig{
		Addr: ln.Addr().String(),
		Pool: PoolConfig{MinConns: 1, MaxConns: 2, DialTimeout: 2 * time.Second, HealthCheck: 1 * time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	txid := testTxID(0xDD)
	// Exactly 2 chunks (8 MiB = 2 * BlobChunkSize).
	data := make([]byte, 2*BlobChunkSize)

	if err := client.uploadBlob(ctx, txid, data); err != nil {
		t.Fatal(err)
	}

	if got := tracker.chunkCount.Load(); got != 2 {
		t.Errorf("chunk count = %d, want 2", got)
	}
	if got := tracker.endCount.Load(); got != 1 {
		t.Errorf("end count = %d, want 1", got)
	}

	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	if len(tracker.chunks[0].Data) != BlobChunkSize {
		t.Errorf("chunk[0] size = %d, want %d", len(tracker.chunks[0].Data), BlobChunkSize)
	}
	if len(tracker.chunks[1].Data) != BlobChunkSize {
		t.Errorf("chunk[1] size = %d, want %d", len(tracker.chunks[1].Data), BlobChunkSize)
	}
	if tracker.chunks[1].Offset != uint64(BlobChunkSize) {
		t.Errorf("chunk[1] offset = %d, want %d", tracker.chunks[1].Offset, BlobChunkSize)
	}
}
