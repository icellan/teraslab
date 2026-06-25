package teraslab

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// Bug 3 regression: StatusRedirect must be followed transparently in
// cluster mode up to MaxRedirects, not bubbled up as *RedirectError.
//
// These tests stand up TWO net.Listeners — a "seed" node and a "target"
// node — and a fake partition map served by the seed. The first request
// to the seed is answered with StatusRedirect pointing at the target;
// the target then answers normally. We assert:
//   1. The client's UnspendBatch/GetSpendBatch/DeleteBatch calls succeed.
//   2. The target actually received the request (not just the seed).
//   3. Exceeding MaxRedirects surfaces *TooManyRedirectsError.

// fakeNode is a single mock node: listener + the most recent request
// it observed + an injectable handler.
type fakeNode struct {
	ln               net.Listener
	addr             string
	requests         atomic.Int32 // all requests, including OpGetPartitionMap
	workloadRequests atomic.Int32 // requests other than OpGetPartitionMap
	handler          atomic.Pointer[func(req requestFrame) responseFrame]
}

func newFakeNode(t *testing.T) *fakeNode {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	n := &fakeNode{ln: ln, addr: ln.Addr().String()}
	go n.serve(t)
	t.Cleanup(func() { ln.Close() })
	return n
}

func (n *fakeNode) setHandler(h func(req requestFrame) responseFrame) {
	n.handler.Store(&h)
}

func (n *fakeNode) serve(t *testing.T) {
	for {
		conn, err := n.ln.Accept()
		if err != nil {
			return
		}
		go n.handleConn(t, conn)
	}
}

func (n *fakeNode) handleConn(t *testing.T, conn net.Conn) {
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
		req := requestFrame{
			RequestID: getU64(buf[0:8]),
			OpCode:    getU16(buf[8:10]),
			Flags:     getU16(buf[10:12]),
			Payload:   append([]byte{}, buf[12:]...),
		}
		n.requests.Add(1)
		// OP_HELLO is an infrastructure handshake issued automatically by New();
		// answer it directly with the protocol version and don't count it as a
		// workload request so per-test workload assertions stay stable.
		if req.OpCode == OpHello {
			var pl []byte
			pl = appendU16(pl, ProtocolVersion)
			if _, err := conn.Write(encodeResponseFrame(responseFrame{RequestID: req.RequestID, Status: StatusOK, Payload: pl})); err != nil {
				return
			}
			continue
		}
		if req.OpCode != OpGetPartitionMap {
			n.workloadRequests.Add(1)
		}
		hp := n.handler.Load()
		if hp == nil {
			return
		}
		resp := (*hp)(req)
		if _, err := conn.Write(encodeResponseFrame(resp)); err != nil {
			return
		}
	}
}

// encodePartitionMapForTest builds a partition map payload that assigns
// every shard to the given nodeID. Matches decodePartitionMap layout:
// [version:8][node_count:4][node_id:8 + addr_len:2 + addr][assignments: NumShards * u64].
func encodePartitionMapForTest(version uint64, nodes []NodeInfo, ownerID uint64) []byte {
	var buf []byte
	buf = appendU64(buf, version)
	buf = appendU32(buf, uint32(len(nodes)))
	for _, n := range nodes {
		buf = appendU64(buf, n.ID)
		buf = appendU16(buf, uint16(len(n.Addr)))
		buf = append(buf, []byte(n.Addr)...)
		buf = append(buf, 1) // is_alive — matches the server wire format
	}
	for range NumShards {
		buf = appendU64(buf, ownerID)
	}
	return buf
}

// buildRedirectingClient stands up a seed node + a target node. The seed
// always replies to GetPartitionMap with a map assigning every shard to
// the seed itself (so initial routing goes there), then for any other
// op-code returns StatusRedirect -> target. The target accepts and
// returns the desired success response via its installed handler.
func buildRedirectingClient(t *testing.T, targetHandler func(req requestFrame) responseFrame) (*Client, *fakeNode, *fakeNode) {
	t.Helper()
	seed := newFakeNode(t)
	target := newFakeNode(t)

	nodes := []NodeInfo{
		{ID: 1, Addr: seed.addr},
		{ID: 2, Addr: target.addr},
	}
	// Initial partition map routes all shards to seed (ID=1).
	pmInitial := encodePartitionMapForTest(1, nodes, 1)
	// Post-redirect partition map: routes to target (ID=2).
	pmUpdated := encodePartitionMapForTest(2, nodes, 2)
	pmCalls := atomic.Int32{}

	seed.setHandler(func(req requestFrame) responseFrame {
		switch req.OpCode {
		case OpGetPartitionMap:
			// First call: initial; subsequent (refresh-after-redirect): updated.
			n := pmCalls.Add(1)
			if n == 1 {
				return responseFrame{RequestID: req.RequestID, Status: StatusOK, Payload: pmInitial}
			}
			return responseFrame{RequestID: req.RequestID, Status: StatusOK, Payload: pmUpdated}
		default:
			// Everything else: REDIRECT to the target.
			redir := encodeRedirectPayload(target.addr)
			return responseFrame{RequestID: req.RequestID, Status: StatusRedirect, Payload: redir}
		}
	})

	// Target accepts partition-map probes from the post-redirect async refresh
	// loop and delegates everything else to the caller-supplied handler. We
	// always serve the "updated" map so an async refresh races but never
	// trips the user's assertions.
	target.setHandler(func(req requestFrame) responseFrame {
		if req.OpCode == OpGetPartitionMap {
			return responseFrame{RequestID: req.RequestID, Status: StatusOK, Payload: pmUpdated}
		}
		return targetHandler(req)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := New(ctx, ClientConfig{
		Seeds: []string{seed.addr},
		Pool: PoolConfig{
			MinConns:    1,
			MaxConns:    2,
			DialTimeout: 2 * time.Second,
		},
		ClusterRefreshInterval: time.Hour, // disable background refresh during the test
		MaxRedirects:           3,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { cli.Close() })
	return cli, seed, target
}

func encodeRedirectPayload(addr string) []byte {
	var buf []byte
	buf = appendU16(buf, uint16(len(addr)))
	buf = append(buf, []byte(addr)...)
	return buf
}

func TestUnspendBatchFollowsRedirectInCluster(t *testing.T) {
	cli, seed, target := buildRedirectingClient(t, func(req requestFrame) responseFrame {
		if req.OpCode != OpUnspendBatch {
			t.Errorf("target unexpected opcode: %d", req.OpCode)
		}
		// Verify the target received a properly-sized 104-byte item.
		expected := 12 + 104
		if len(req.Payload) != expected {
			t.Errorf("target payload len = %d, want %d", len(req.Payload), expected)
		}
		return responseFrame{RequestID: req.RequestID, Status: StatusOK}
	})

	var sd SpendingData
	for i := range sd {
		sd[i] = byte(0xE0 + i)
	}
	items := []UnspendItem{{
		TxID:         testTxID(0xAB),
		Vout:         7,
		UtxoHash:     testUtxoHash(0xCD),
		SpendingData: sd,
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := cli.UnspendBatch(ctx, UnspendBatchParams{CurrentBlockHeight: 1000, BlockHeightRetention: 144}, items)
	if err != nil {
		t.Fatalf("UnspendBatch returned error: %v", err)
	}
	if result == nil {
		t.Fatalf("expected non-nil BatchResult")
	}
	if seed.requests.Load() < 2 {
		t.Errorf("expected seed to have served partition map + redirect (>=2 requests), got %d", seed.requests.Load())
	}
	if target.workloadRequests.Load() != 1 {
		t.Errorf("expected target to receive exactly 1 workload request, got %d", target.workloadRequests.Load())
	}
}

func TestGetSpendBatchFollowsRedirectInCluster(t *testing.T) {
	// Build a deterministic success payload: 1 result with Status=0, SlotStatus=1.
	var respPayload []byte
	respPayload = appendU32(respPayload, 1)
	respPayload = append(respPayload, 0)                   // status
	respPayload = appendU16(respPayload, 0)                // error_code
	respPayload = append(respPayload, 1)                   // slot_status
	respPayload = append(respPayload, make([]byte, 36)...) // spending_data zero

	cli, _, target := buildRedirectingClient(t, func(req requestFrame) responseFrame {
		if req.OpCode != OpGetSpendBatch {
			t.Errorf("target unexpected opcode: %d", req.OpCode)
		}
		// Verify the target received a properly-sized 68-byte item.
		expected := 4 + 68
		if len(req.Payload) != expected {
			t.Errorf("target payload len = %d, want %d", len(req.Payload), expected)
		}
		return responseFrame{RequestID: req.RequestID, Status: StatusOK, Payload: respPayload}
	})

	items := []GetSpendItem{{TxID: testTxID(0x11), Vout: 2, UtxoHash: testUtxoHash(0x22)}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results, err := cli.GetSpendBatch(ctx, items)
	if err != nil {
		t.Fatalf("GetSpendBatch returned error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("results len = %d, want 1", len(results))
	}
	if results[0].SlotStatus != 1 {
		t.Errorf("slot_status = %d, want 1", results[0].SlotStatus)
	}
	if target.workloadRequests.Load() != 1 {
		t.Errorf("expected target to receive exactly 1 workload request, got %d", target.workloadRequests.Load())
	}
}

func TestDeleteBatchFollowsRedirectInCluster(t *testing.T) {
	cli, _, target := buildRedirectingClient(t, func(req requestFrame) responseFrame {
		if req.OpCode != OpDeleteBatch {
			t.Errorf("target unexpected opcode: %d", req.OpCode)
		}
		return responseFrame{RequestID: req.RequestID, Status: StatusOK}
	})

	txids := []TxID{testTxID(0x42)}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cli.DeleteBatch(ctx, txids); err != nil {
		t.Fatalf("DeleteBatch returned error: %v", err)
	}
	if target.workloadRequests.Load() != 1 {
		t.Errorf("expected target to receive exactly 1 workload request, got %d", target.workloadRequests.Load())
	}
}

func TestTooManyRedirectsSurfacesError(t *testing.T) {
	// Both nodes redirect to each other indefinitely. The client should give
	// up after MaxRedirects and return *TooManyRedirectsError.
	seed := newFakeNode(t)
	target := newFakeNode(t)
	nodes := []NodeInfo{
		{ID: 1, Addr: seed.addr},
		{ID: 2, Addr: target.addr},
	}
	pm := encodePartitionMapForTest(1, nodes, 1)

	seed.setHandler(func(req requestFrame) responseFrame {
		if req.OpCode == OpGetPartitionMap {
			return responseFrame{RequestID: req.RequestID, Status: StatusOK, Payload: pm}
		}
		return responseFrame{RequestID: req.RequestID, Status: StatusRedirect, Payload: encodeRedirectPayload(target.addr)}
	})
	target.setHandler(func(req requestFrame) responseFrame {
		if req.OpCode == OpGetPartitionMap {
			return responseFrame{RequestID: req.RequestID, Status: StatusOK, Payload: pm}
		}
		return responseFrame{RequestID: req.RequestID, Status: StatusRedirect, Payload: encodeRedirectPayload(seed.addr)}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := New(ctx, ClientConfig{
		Seeds:                  []string{seed.addr},
		Pool:                   PoolConfig{MinConns: 1, MaxConns: 2, DialTimeout: 2 * time.Second},
		ClusterRefreshInterval: time.Hour,
		MaxRedirects:           2,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cli.Close()

	_, err = cli.UnspendBatch(ctx, UnspendBatchParams{},
		[]UnspendItem{{TxID: testTxID(1)}})
	if err == nil {
		t.Fatal("expected TooManyRedirectsError, got nil")
	}
	var tre *TooManyRedirectsError
	if !errors.As(err, &tre) {
		t.Fatalf("expected *TooManyRedirectsError, got %T: %v", err, err)
	}
	if tre.Hops != 2 {
		t.Errorf("hops = %d, want 2", tre.Hops)
	}
	if tre.LastAddr == "" {
		t.Errorf("expected non-empty LastAddr")
	}
}
