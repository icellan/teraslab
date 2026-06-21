package teraslab

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// encodePartitionMapAssign builds a partition map payload with per-shard
// ownership decided by assign(shard).
func encodePartitionMapAssign(version uint64, nodes []NodeInfo, assign func(shard int) uint64) []byte {
	var buf []byte
	buf = appendU64(buf, version)
	buf = appendU32(buf, uint32(len(nodes)))
	for _, n := range nodes {
		buf = appendU64(buf, n.ID)
		buf = appendU16(buf, uint16(len(n.Addr)))
		buf = append(buf, []byte(n.Addr)...)
	}
	for i := 0; i < NumShards; i++ {
		buf = appendU64(buf, assign(i))
	}
	return buf
}

// txidForShard returns a TxID whose ShardForTxID equals shard (shard < 4096).
func txidForShard(shard uint16) TxID {
	var t TxID
	t[0] = byte(shard & 0xFF)
	t[1] = byte((shard >> 8) & 0x0F)
	return t
}

func TestTxidForShardHelper(t *testing.T) {
	for _, s := range []uint16{0, 1, 255, 256, 4095} {
		if got := ShardForTxID(txidForShard(s)); got != s {
			t.Fatalf("txidForShard(%d) shard = %d", s, got)
		}
	}
}

// TestItemBatchFanOutSplitsByShard verifies that a multi-shard FreezeBatch is
// split so each owning node receives only its own items (C1).
func TestItemBatchFanOutSplitsByShard(t *testing.T) {
	nodeA := newFakeNode(t)
	nodeB := newFakeNode(t)
	nodes := []NodeInfo{{ID: 1, Addr: nodeA.addr}, {ID: 2, Addr: nodeB.addr}}
	// Shard 0 -> node 1 (A); shard 1 -> node 2 (B); everything else -> A.
	pm := encodePartitionMapAssign(1, nodes, func(shard int) uint64 {
		if shard == 1 {
			return 2
		}
		return 1
	})

	var aItems, bItems atomic.Int32
	mkHandler := func(counter *atomic.Int32) func(req requestFrame) responseFrame {
		return func(req requestFrame) responseFrame {
			if req.OpCode == OpGetPartitionMap {
				return responseFrame{RequestID: req.RequestID, Status: StatusOK, Payload: pm}
			}
			if req.OpCode == OpFreezeBatch {
				// payload: [count:4][items...]; count is the first 4 bytes.
				counter.Add(int32(getU32(req.Payload[0:4])))
			}
			return responseFrame{RequestID: req.RequestID, Status: StatusOK}
		}
	}
	nodeA.setHandler(mkHandler(&aItems))
	nodeB.setHandler(mkHandler(&bItems))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := New(ctx, ClientConfig{
		Seeds:                  []string{nodeA.addr},
		Pool:                   PoolConfig{MinConns: 1, MaxConns: 2, DialTimeout: 2 * time.Second},
		ClusterRefreshInterval: time.Hour,
		MaxRedirects:           3,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cli.Close()

	items := []FreezeItem{
		{TxID: txidForShard(0)}, // -> node A
		{TxID: txidForShard(1)}, // -> node B
		{TxID: txidForShard(0)}, // -> node A
	}
	if _, err := cli.FreezeBatch(ctx, items); err != nil {
		t.Fatalf("FreezeBatch: %v", err)
	}
	if aItems.Load() != 2 {
		t.Errorf("node A received %d items, want 2", aItems.Load())
	}
	if bItems.Load() != 1 {
		t.Errorf("node B received %d items, want 1", bItems.Load())
	}
}

// TestStaleRedirectStops verifies version-based loop detection: a redirect whose
// shard-table version is not newer than the client's is refused (C2).
func TestStaleRedirectStops(t *testing.T) {
	seed := newFakeNode(t)
	target := newFakeNode(t)
	nodes := []NodeInfo{{ID: 1, Addr: seed.addr}, {ID: 2, Addr: target.addr}}
	// Client's known map version is 5.
	pm := encodePartitionMapAssign(5, nodes, func(int) uint64 { return 1 })

	seed.setHandler(func(req requestFrame) responseFrame {
		if req.OpCode == OpGetPartitionMap {
			return responseFrame{RequestID: req.RequestID, Status: StatusOK, Payload: pm}
		}
		// Redirect carrying a STALE version (3 <= 5).
		return responseFrame{RequestID: req.RequestID, Status: StatusRedirect, Payload: encodeRedirectPayloadVersion(target.addr, 3)}
	})
	target.setHandler(func(req requestFrame) responseFrame {
		return responseFrame{RequestID: req.RequestID, Status: StatusOK, Payload: pm}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := New(ctx, ClientConfig{
		Seeds:                  []string{seed.addr},
		Pool:                   PoolConfig{MinConns: 1, MaxConns: 2, DialTimeout: 2 * time.Second},
		ClusterRefreshInterval: time.Hour,
		MaxRedirects:           5,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cli.Close()

	_, err = cli.FreezeBatch(ctx, []FreezeItem{{TxID: txidForShard(0)}})
	if err == nil {
		t.Fatal("expected StaleRedirectError, got nil")
	}
	var sre *StaleRedirectError
	if !errors.As(err, &sre) {
		t.Fatalf("expected *StaleRedirectError, got %T: %v", err, err)
	}
	if sre.ServerVersion != 3 || sre.ClientVersion != 5 {
		t.Errorf("versions = server %d / client %d, want 3 / 5", sre.ServerVersion, sre.ClientVersion)
	}
}

func encodeRedirectPayloadVersion(addr string, version uint64) []byte {
	buf := encodeRedirectPayload(addr)
	return appendU64(buf, version)
}
