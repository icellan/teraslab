package teraslab

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// These tests exercise the per-item ERR_REDIRECT path — the shape the server
// actually emits for batch mutations: STATUS_PARTIAL_ERROR carrying one or more
// items whose error code is ErrCodeRedirect. The client must refresh routing and
// re-send ONLY the redirected items to the new owner, merging the results back
// into the original index space, while leaving genuine per-item failures intact.
//
// This is distinct from whole-batch StatusRedirect (status 3), which the server
// only emits for single-payload ops. See redirect_test.go for that case.

// encodeSparseErrorsPayload builds a STATUS_PARTIAL_ERROR sparse-errors payload:
// [count:u32][ (index:u32)(code:u16)(dataLen:u16)(data) ]*. Mirrors
// decodeSparseErrors.
func encodeSparseErrorsPayload(errs []BatchItemError) []byte {
	var buf []byte
	buf = appendU32(buf, uint32(len(errs)))
	for _, e := range errs {
		buf = appendU32(buf, e.ItemIndex)
		buf = appendU16(buf, e.Code)
		buf = appendU16(buf, uint16(len(e.Data)))
		buf = append(buf, e.Data...)
	}
	return buf
}

// encodeSignalsPayload builds a STATUS_PARTIAL_ERROR signals payload:
// [successCount:u32][ (index:u32)(signal:u8)(bidCount:u8)(blockIDs) ]*
// [errorCount:u32][ (index:u32)(code:u16)(dataLen:u16)(data) ]*. Mirrors
// decodePartialWithSignals.
func encodeSignalsPayload(successes []BatchItemSuccess, errs []BatchItemError) []byte {
	var buf []byte
	buf = appendU32(buf, uint32(len(successes)))
	for _, s := range successes {
		buf = appendU32(buf, s.ItemIndex)
		buf = append(buf, s.Signal)
		buf = append(buf, byte(len(s.BlockIDs)))
		for _, bid := range s.BlockIDs {
			buf = appendU32(buf, bid)
		}
	}
	buf = appendU32(buf, uint32(len(errs)))
	for _, e := range errs {
		buf = appendU32(buf, e.ItemIndex)
		buf = appendU16(buf, e.Code)
		buf = appendU16(buf, uint16(len(e.Data)))
		buf = append(buf, e.Data...)
	}
	return buf
}

// buildPartialRedirectClient stands up a seed node + a target node. The seed
// initially owns every shard; the seedHandler is invoked for every non-map
// workload op the seed receives (the first, full-batch send). After the client
// refreshes, the map assigns every shard to the target, so the re-sent
// redirected items land on the target via targetHandler.
//
// retryAfter controls when the refreshed (target-owning) map is served: the
// updated map is returned starting from the (retryAfter+1)-th GetPartitionMap
// call on the seed. With retryAfter=1 the initial bootstrap map points at the
// seed and the first refresh-after-partial-error points at the target.
func buildPartialRedirectClient(
	t *testing.T,
	seedHandler func(req requestFrame) responseFrame,
	targetHandler func(req requestFrame) responseFrame,
) (*Client, *fakeNode, *fakeNode) {
	t.Helper()
	seed := newFakeNode(t)
	target := newFakeNode(t)

	nodes := []NodeInfo{
		{ID: 1, Addr: seed.addr},
		{ID: 2, Addr: target.addr},
	}
	pmInitial := encodePartitionMapForTest(1, nodes, 1) // all shards -> seed
	pmUpdated := encodePartitionMapForTest(2, nodes, 2) // all shards -> target
	pmCalls := atomic.Int32{}

	seed.setHandler(func(req requestFrame) responseFrame {
		if req.OpCode == OpGetPartitionMap {
			n := pmCalls.Add(1)
			if n == 1 {
				return responseFrame{RequestID: req.RequestID, Status: StatusOK, Payload: pmInitial}
			}
			return responseFrame{RequestID: req.RequestID, Status: StatusOK, Payload: pmUpdated}
		}
		return seedHandler(req)
	})
	target.setHandler(func(req requestFrame) responseFrame {
		if req.OpCode == OpGetPartitionMap {
			return responseFrame{RequestID: req.RequestID, Status: StatusOK, Payload: pmUpdated}
		}
		return targetHandler(req)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := New(ctx, ClientConfig{
		Seeds:                  []string{seed.addr},
		Pool:                   PoolConfig{MinConns: 1, MaxConns: 2, DialTimeout: 2 * time.Second},
		ClusterRefreshInterval: time.Hour,
		MaxRedirects:           3,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { cli.Close() })
	return cli, seed, target
}

// TestDeleteBatchFollowsPerItemRedirect verifies that a txid-batch mutation
// (DeleteBatch) whose first response is STATUS_PARTIAL_ERROR with one
// ErrCodeRedirect item and one genuine failure re-sends ONLY the redirected
// item to the new owner and surfaces only the genuine failure.
func TestDeleteBatchFollowsPerItemRedirect(t *testing.T) {
	var targetWorkload atomic.Int32
	cli, seed, _ := buildPartialRedirectClient(t,
		// Seed: item 0 redirected, item 1 is a genuine TxNotFound.
		func(req requestFrame) responseFrame {
			if req.OpCode != OpDeleteBatch {
				t.Errorf("seed unexpected opcode: %d", req.OpCode)
			}
			payload := encodeSparseErrorsPayload([]BatchItemError{
				{ItemIndex: 0, Code: ErrCodeRedirect},
				{ItemIndex: 1, Code: ErrCodeTxNotFound},
			})
			return responseFrame{RequestID: req.RequestID, Status: StatusPartialError, Payload: payload}
		},
		// Target: the re-sent batch must contain exactly the redirected txid.
		func(req requestFrame) responseFrame {
			targetWorkload.Add(1)
			if req.OpCode != OpDeleteBatch {
				t.Errorf("target unexpected opcode: %d", req.OpCode)
			}
			// Payload: [count:u32][txid:32]. Exactly one item re-sent.
			if got := getU32(req.Payload[0:4]); got != 1 {
				t.Errorf("target re-sent %d items, want 1", got)
			}
			return responseFrame{RequestID: req.RequestID, Status: StatusOK}
		})

	txids := []TxID{testTxID(0xA0), testTxID(0xB1)}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cli.DeleteBatch(ctx, txids)

	var pe *PartialError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *PartialError, got %T: %v", err, err)
	}
	if len(pe.Errors) != 1 {
		t.Fatalf("expected 1 residual error, got %d: %v", len(pe.Errors), pe.Errors)
	}
	if pe.Errors[0].Code != ErrCodeTxNotFound {
		t.Errorf("residual error code = %s, want TX_NOT_FOUND", ErrorCodeString(pe.Errors[0].Code))
	}
	if pe.Errors[0].ItemIndex != 1 {
		t.Errorf("residual error item index = %d, want 1", pe.Errors[0].ItemIndex)
	}
	if targetWorkload.Load() != 1 {
		t.Errorf("target received %d workload requests, want 1 (only the redirected item)", targetWorkload.Load())
	}
	if seed.requests.Load() < 2 {
		t.Errorf("expected seed to serve map + workload (>=2), got %d", seed.requests.Load())
	}
}

// TestDeleteBatchPerItemRedirectAllResolved verifies that when every redirected
// item succeeds on the new owner and there are no other failures, the whole
// operation succeeds (no PartialError leaks).
func TestDeleteBatchPerItemRedirectAllResolved(t *testing.T) {
	cli, _, target := buildPartialRedirectClient(t,
		func(req requestFrame) responseFrame {
			payload := encodeSparseErrorsPayload([]BatchItemError{
				{ItemIndex: 0, Code: ErrCodeRedirect},
			})
			return responseFrame{RequestID: req.RequestID, Status: StatusPartialError, Payload: payload}
		},
		func(req requestFrame) responseFrame {
			return responseFrame{RequestID: req.RequestID, Status: StatusOK}
		})

	txids := []TxID{testTxID(0x55)}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := cli.DeleteBatch(ctx, txids)
	if err != nil {
		t.Fatalf("DeleteBatch returned error: %v", err)
	}
	if res == nil {
		t.Fatalf("expected non-nil BatchResult")
	}
	if target.workloadRequests.Load() != 1 {
		t.Errorf("target received %d workload requests, want 1", target.workloadRequests.Load())
	}
}

// TestSpendBatchFollowsPerItemRedirect verifies the signal-bearing spend path:
// the seed returns a partial response with one success (signal preserved), one
// ErrCodeRedirect, and one genuine ALREADY_SPENT. After refresh the redirected
// item is re-sent to the target which returns a success signal. The merged
// result must contain both successes in the original index space and only the
// genuine failure.
func TestSpendBatchFollowsPerItemRedirect(t *testing.T) {
	var targetWorkload atomic.Int32
	cli, _, _ := buildPartialRedirectClient(t,
		func(req requestFrame) responseFrame {
			if req.OpCode != OpSpendBatch {
				t.Errorf("seed unexpected opcode: %d", req.OpCode)
			}
			payload := encodeSignalsPayload(
				[]BatchItemSuccess{{ItemIndex: 0, Signal: SignalAllSpent}},
				[]BatchItemError{
					{ItemIndex: 1, Code: ErrCodeRedirect},
					{ItemIndex: 2, Code: ErrCodeAlreadySpent},
				})
			return responseFrame{RequestID: req.RequestID, Status: StatusPartialError, Payload: payload}
		},
		func(req requestFrame) responseFrame {
			targetWorkload.Add(1)
			if req.OpCode != OpSpendBatch {
				t.Errorf("target unexpected opcode: %d", req.OpCode)
			}
			// Re-sent batch carries exactly one item. SpendBatch payload begins
			// with a header followed by [count:u32]; the count lives at bytes
			// [2:6] (1-byte ignore_conflicting + 1-byte ignore_locked + count).
			// We just assert the success signal round-trips for item 0 of the
			// sub-batch.
			payload := encodeSignalsPayload(
				[]BatchItemSuccess{{ItemIndex: 0, Signal: SignalNotAllSpent}},
				nil)
			return responseFrame{RequestID: req.RequestID, Status: StatusPartialError, Payload: payload}
		})

	mkItem := func(b byte) SpendItem { return SpendItem{TxID: testTxID(b), Vout: 0, UtxoHash: testUtxoHash(b)} }
	items := []SpendItem{mkItem(0x01), mkItem(0x02), mkItem(0x03)}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := cli.SpendBatch(ctx, SpendBatchParams{CurrentBlockHeight: 100}, items)

	var pe *PartialError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *PartialError, got %T: %v", err, err)
	}
	if len(pe.Errors) != 1 {
		t.Fatalf("expected 1 residual error, got %d: %v", len(pe.Errors), pe.Errors)
	}
	if pe.Errors[0].Code != ErrCodeAlreadySpent || pe.Errors[0].ItemIndex != 2 {
		t.Errorf("residual error = (idx %d, %s), want (2, ALREADY_SPENT)",
			pe.Errors[0].ItemIndex, ErrorCodeString(pe.Errors[0].Code))
	}
	if res == nil {
		t.Fatalf("expected non-nil SpendBatchResponse")
	}
	// Two successes: original item 0 (AllSpent) and re-routed item 1 (NotAllSpent).
	if len(res.Successes) != 2 {
		t.Fatalf("expected 2 merged successes, got %d: %+v", len(res.Successes), res.Successes)
	}
	byIdx := map[uint32]uint8{}
	for _, s := range res.Successes {
		byIdx[s.ItemIndex] = s.Signal
	}
	if byIdx[0] != SignalAllSpent {
		t.Errorf("item 0 signal = %d, want SignalAllSpent", byIdx[0])
	}
	if byIdx[1] != SignalNotAllSpent {
		t.Errorf("item 1 signal = %d, want SignalNotAllSpent (re-routed to new owner)", byIdx[1])
	}
	if targetWorkload.Load() != 1 {
		t.Errorf("target received %d workload requests, want 1 (only the redirected item)", targetWorkload.Load())
	}
}

// TestProcessExpiredSendsRetention verifies REL-014: the request payload is the
// 8-byte [currentHeight][blockHeightRetention] form, not the legacy 4-byte form
// that the server interprets as retention=0.
func TestProcessExpiredSendsRetention(t *testing.T) {
	node := newFakeNode(t)
	var gotPayload atomic.Pointer[[]byte]
	node.setHandler(func(req requestFrame) responseFrame {
		if req.OpCode == OpProcessExpiredPreservations {
			p := append([]byte(nil), req.Payload...)
			gotPayload.Store(&p)
			// Response: [deleted:u32][failed:u32].
			var resp []byte
			resp = appendU32(resp, 7)
			resp = appendU32(resp, 0)
			return responseFrame{RequestID: req.RequestID, Status: StatusOK, Payload: resp}
		}
		return responseFrame{RequestID: req.RequestID, Status: StatusError}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := New(ctx, ClientConfig{
		Addr: node.addr,
		Pool: PoolConfig{MinConns: 1, MaxConns: 2, DialTimeout: 2 * time.Second},
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cli.Close()

	res, err := cli.ProcessExpiredPreservations(ctx, 800100, 144)
	if err != nil {
		t.Fatalf("ProcessExpiredPreservations: %v", err)
	}
	if res.Deleted != 7 {
		t.Errorf("deleted = %d, want 7", res.Deleted)
	}

	pp := gotPayload.Load()
	if pp == nil {
		t.Fatal("server never received the request")
	}
	p := *pp
	if len(p) != 8 {
		t.Fatalf("payload len = %d, want 8 (currentHeight + blockHeightRetention)", len(p))
	}
	if got := getU32(p[0:4]); got != 800100 {
		t.Errorf("currentHeight = %d, want 800100", got)
	}
	if got := getU32(p[4:8]); got != 144 {
		t.Errorf("blockHeightRetention = %d, want 144", got)
	}
}
