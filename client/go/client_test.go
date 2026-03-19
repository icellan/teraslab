package teraslab

import (
	"context"
	"net"
	"testing"
	"time"
)

// startClientTestServer starts a mock server that handles a few key operations.
func startClientTestServer(t *testing.T) net.Listener {
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
			go handleClientTestConn(conn)
		}
	}()
	return ln
}

func handleClientTestConn(conn net.Conn) {
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

		var resp responseFrame
		switch opCode {
		case OpPing:
			resp = responseFrame{RequestID: reqID, Status: StatusOK}
		case OpHealth:
			resp = responseFrame{RequestID: reqID, Status: StatusOK, Payload: []byte("ok")}
		case OpGetPartitionMap:
			// Return single-node partition map.
			var payload []byte
			payload = appendU64(payload, 0) // version
			payload = appendU32(payload, 1) // 1 node
			payload = appendU64(payload, 0) // node_id
			addr := "127.0.0.1:3300"
			payload = appendU16(payload, uint16(len(addr)))
			payload = append(payload, addr...)
			for range NumShards {
				payload = appendU64(payload, 0)
			}
			resp = responseFrame{RequestID: reqID, Status: StatusOK, Payload: payload}
		case OpSpendBatch:
			// Return OK with empty success signals.
			var payload []byte
			payload = appendU32(payload, 0) // 0 successes
			payload = appendU32(payload, 0) // 0 errors
			resp = responseFrame{RequestID: reqID, Status: StatusOK, Payload: payload}
		case OpDeleteBatch:
			resp = responseFrame{RequestID: reqID, Status: StatusOK}
		case OpGetBatch:
			// Return 1 result with empty data.
			var payload []byte
			payload = appendU32(payload, 1) // count=1
			payload = append(payload, 0)    // status=OK
			payload = appendU32(payload, 0) // data_len=0
			resp = responseFrame{RequestID: reqID, Status: StatusOK, Payload: payload}
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

	results, err := client.GetBatch(ctx, FieldAll, []TxID{testTxID(1)})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	if results[0].Status != 0 {
		t.Errorf("status = %d, want 0", results[0].Status)
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
