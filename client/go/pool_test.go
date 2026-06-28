package teraslab

import (
	"context"
	"net"
	"testing"
	"time"
)

func startEchoServer(t *testing.T) net.Listener {
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
			go func(c net.Conn) {
				defer c.Close()
				var buf []byte
				for {
					lenBuf := make([]byte, 4)
					if _, err := readFull(c, lenBuf); err != nil {
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
					if _, err := readFull(c, buf); err != nil {
						return
					}
					reqID := getU64(buf[0:8])
					resp := encodeResponseFrame(responseFrame{
						RequestID: reqID,
						Status:    StatusOK,
						Payload:   []byte("ok"),
					})
					if _, err := c.Write(resp); err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	return ln
}

func TestPoolGetReturnsConnection(t *testing.T) {
	ln := startEchoServer(t)
	defer ln.Close()

	p := newPool(ln.Addr().String(), PoolConfig{
		MinConns:    1,
		MaxConns:    4,
		DialTimeout: 2 * time.Second,
		HealthCheck: 1 * time.Hour, // disable for test
	})
	defer p.close()

	ctx := context.Background()
	c, err := p.get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !c.alive() {
		t.Error("connection should be alive")
	}
}

func TestPoolReusesConnections(t *testing.T) {
	ln := startEchoServer(t)
	defer ln.Close()

	p := newPool(ln.Addr().String(), PoolConfig{
		MinConns:     1,
		MaxConns:     4,
		PrewarmConns: 1,
		PoolShards:   1, // single shard so sequential gets route to the same conn
		DialTimeout:  2 * time.Second,
		HealthCheck:  1 * time.Hour,
	})
	defer p.close()

	// Wait for the single warm connection so the sequential gets below pipeline
	// onto it rather than racing the cold-start dial.
	deadline := time.Now().Add(2 * time.Second)
	for {
		ready := p.connCount() == 1 && !p.dialSemBusy()
		if ready || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}

	ctx := context.Background()
	c1, err := p.get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := p.get(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// A sequential caller pipelines onto the single warm connection: both gets
	// (each below PipelineDepth, the prior request having completed) return it.
	if c1 != c2 {
		t.Error("expected same connection (pipelining reuse)")
	}
}

// TestPoolGrowsOnlyWhenConnsSaturated verifies the pipelining model: the pool
// reuses a connection while it has headroom below PipelineDepth, and only dials
// a new connection once EVERY existing connection is saturated to PipelineDepth
// — and never beyond MaxConns. This replaces the old dial-per-busy-caller
// behavior that caused a dial storm under high concurrency.
func TestPoolGrowsOnlyWhenConnsSaturated(t *testing.T) {
	ln := startEchoServer(t)
	defer ln.Close()

	const maxConns = 4
	const depth = 8
	p := newPool(ln.Addr().String(), PoolConfig{
		MinConns:      1,
		MaxConns:      maxConns,
		PipelineDepth: depth,
		PrewarmConns:  1,
		PoolShards:    1, // single shard so the least-loaded scan is pool-wide here
		DialTimeout:   2 * time.Second,
		HealthCheck:   1 * time.Hour,
	})
	defer p.close()

	ctx := context.Background()

	// Wait for pre-warm to settle so no background dial holds a dial-semaphore
	// slot during the saturation loop below (which would make a grow attempt
	// fall back to an existing conn instead of dialing, flaking the count).
	deadline := time.Now().Add(2 * time.Second)
	for {
		ready := p.connCount() >= 1 && !p.dialSemBusy()
		if ready || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}

	// First get finds the prewarmed connection.
	c1, err := p.get(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Below PipelineDepth, get() must reuse the same connection, not grow.
	c1.inflight.Store(depth - 1)
	c2, err := p.get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c2 != c1 {
		t.Fatalf("expected reuse of the warm connection below PipelineDepth, got a different conn")
	}
	n := p.connCount()
	if n != 1 {
		t.Fatalf("pool grew while a connection had pipeline headroom: size=%d", n)
	}

	// Now saturate every connection to PipelineDepth before each get so the pool
	// is forced to grow. It must grow up to MaxConns and no further.
	seen := map[*pipeConn]bool{c1: true}
	for i := 0; i < maxConns*3; i++ {
		for _, c := range p.allConns() {
			c.inflight.Store(int64(depth))
		}

		c, err := p.get(ctx)
		if err != nil {
			t.Fatal(err)
		}
		seen[c] = true
	}

	if len(seen) != maxConns {
		t.Errorf("expected pool to grow to exactly MaxConns=%d distinct connections, got %d", maxConns, len(seen))
	}
	n = p.connCount()
	if n != maxConns {
		t.Errorf("expected pool size %d, got %d", maxConns, n)
	}
}

func TestPoolCloseClosesAll(t *testing.T) {
	ln := startEchoServer(t)
	defer ln.Close()

	p := newPool(ln.Addr().String(), PoolConfig{
		MinConns:    1,
		MaxConns:    4,
		DialTimeout: 2 * time.Second,
		HealthCheck: 1 * time.Hour,
	})

	ctx := context.Background()
	c, err := p.get(ctx)
	if err != nil {
		t.Fatal(err)
	}

	p.close()

	if c.alive() {
		t.Error("connection should be closed after pool close")
	}

	_, err = p.get(ctx)
	if err == nil {
		t.Error("get after close should return error")
	}
}

func TestPoolRoundTripThroughPool(t *testing.T) {
	ln := startEchoServer(t)
	defer ln.Close()

	p := newPool(ln.Addr().String(), PoolConfig{
		MinConns:    1,
		MaxConns:    4,
		DialTimeout: 2 * time.Second,
		HealthCheck: 1 * time.Hour,
	})
	defer p.close()

	ctx := context.Background()
	c, err := p.get(ctx)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.roundTrip(ctx, OpPing, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != StatusOK {
		t.Errorf("status = %d, want %d", resp.Status, StatusOK)
	}
}

// TestCheckHealthSkipsConnWithInflight is a regression test for big-block sync
// failures: a connection carrying in-flight requests must not be health-probed
// or reaped. The mock server is single-connection and FIFO, so a held data
// request blocks any subsequent ping (head-of-line) — exactly the production
// scenario where, under load, the ping queues behind in-flight work, times out,
// and the pool would close the connection, aborting the legitimate requests
// with "connection closed" (the server then sees a broken pipe).
func TestCheckHealthSkipsConnWithInflight(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	release := make(chan struct{})
	go mockServer(t, ln, func(req requestFrame) responseFrame {
		if req.OpCode == OpPing {
			return responseFrame{RequestID: req.RequestID, Status: StatusOK}
		}
		<-release // hold the data request in flight
		return responseFrame{RequestID: req.RequestID, Status: StatusOK, Payload: []byte("ok")}
	})

	p := newPool(ln.Addr().String(), PoolConfig{
		MinConns:    1,
		MaxConns:    1,
		DialTimeout: 200 * time.Millisecond, // a real probe of a busy conn would time out fast
		HealthCheck: 1 * time.Hour,          // drive checkHealth manually
	})
	defer p.close()

	c, err := p.get(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		resp responseFrame
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := c.roundTrip(context.Background(), OpGetBatch, 0, []byte("k"))
		done <- result{resp, err}
	}()

	// Wait until the request is actually in flight.
	deadline := time.Now().Add(2 * time.Second)
	for !c.hasInflight() {
		if time.Now().After(deadline) {
			t.Fatal("request never became in-flight")
		}
		time.Sleep(time.Millisecond)
	}

	// Health check while the conn is busy must not close or drop it.
	p.checkHealth()

	if !c.alive() {
		t.Fatal("checkHealth closed a connection with an in-flight request")
	}
	conns := p.allConns()
	inPool := len(conns) == 1 && conns[0] == c
	if !inPool {
		t.Fatal("checkHealth removed a busy connection from the pool")
	}

	// Release and confirm the in-flight request completed cleanly.
	close(release)
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("in-flight request failed: %v", r.err)
		}
		if r.resp.Status != StatusOK {
			t.Fatalf("unexpected status: %d", r.resp.Status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight request did not complete")
	}
}
