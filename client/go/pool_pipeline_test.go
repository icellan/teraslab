package teraslab

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingEchoServer is like startEchoServer but counts how many TCP
// connections the client opens (accepted), so tests can assert the pool stays
// bounded and does not storm the listener with dials. The optional perReq hook
// runs on every request before the response is written, letting a test hold
// requests in flight.
func countingEchoServer(t *testing.T, accepted *atomic.Int64, perReq func()) net.Listener {
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
			if accepted != nil {
				accepted.Add(1)
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
					if perReq != nil {
						perReq()
					}
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

// waitFor polls cond until true or the deadline elapses.
func waitFor(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return cond()
}

// TestPoolBoundedUnderConcurrency proves the dial-storm fix: with far more
// concurrent callers than MaxConns, the pool opens at most MaxConns TCP
// connections — concurrency is absorbed by pipelining onto a bounded set, not
// by dialing a connection per caller.
func TestPoolBoundedUnderConcurrency(t *testing.T) {
	var accepted atomic.Int64

	// Hold every request in flight until release, so all callers are concurrent
	// and connections cannot be freed and reused serially.
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	ln := countingEchoServer(t, &accepted, func() { <-release })
	defer ln.Close()

	const maxConns = 8
	p := newPool(ln.Addr().String(), PoolConfig{
		MinConns:           1,
		MaxConns:           maxConns,
		PipelineDepth:      16,
		PrewarmConns:       maxConns,
		MaxConcurrentDials: 4,
		DialTimeout:        2 * time.Second,
		HealthCheck:        1 * time.Hour,
	})
	defer func() { releaseAll(); p.close() }()

	// Wait for pre-warm to finish so the connection set exists before load.
	if !waitFor(t, 3*time.Second, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return len(p.conns) == maxConns
	}) {
		t.Fatalf("pre-warm did not reach MaxConns=%d", maxConns)
	}

	const callers = 500
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, err := p.get(ctx)
			if err != nil {
				return
			}
			_, _ = conn.roundTrip(ctx, OpPing, 0, nil)
		}()
	}

	// Let all callers reach get() and pipeline their requests.
	if !waitFor(t, 3*time.Second, func() bool {
		p.mu.Lock()
		var total int64
		for _, c := range p.conns {
			total += c.inflightCount()
		}
		p.mu.Unlock()
		return total >= callers
	}) {
		t.Fatal("not all callers became in-flight; pool may be blocking on get()")
	}

	// The decisive assertion: the pool never opened more than MaxConns conns.
	if got := accepted.Load(); got > maxConns {
		t.Fatalf("dial storm: opened %d connections, want <= MaxConns=%d", got, maxConns)
	}

	releaseAll()
	wg.Wait()
}

// TestPoolPipelinesMultipleInflight proves get() pipelines: with a single
// warm connection and several concurrent callers below PipelineDepth, all
// requests ride the SAME connection (multiple in-flight at once) rather than
// each caller getting a fresh connection.
func TestPoolPipelinesMultipleInflight(t *testing.T) {
	var accepted atomic.Int64
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	ln := countingEchoServer(t, &accepted, func() { <-release })
	defer ln.Close()

	p := newPool(ln.Addr().String(), PoolConfig{
		MinConns:      1,
		MaxConns:      8,
		PipelineDepth: 16, // well above the number of callers
		PrewarmConns:  1,
		DialTimeout:   2 * time.Second,
		HealthCheck:   1 * time.Hour,
	})
	defer func() { releaseAll(); p.close() }()

	// Ensure exactly one warm connection.
	if !waitFor(t, 3*time.Second, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return len(p.conns) == 1
	}) {
		t.Fatal("pre-warm did not establish the single connection")
	}

	const callers = 5
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, err := p.get(ctx)
			if err != nil {
				return
			}
			_, _ = conn.roundTrip(ctx, OpPing, 0, nil)
		}()
	}

	// All callers should be in flight ON THE SINGLE connection simultaneously.
	if !waitFor(t, 3*time.Second, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return len(p.conns) == 1 && p.conns[0].inflightCount() == callers
	}) {
		p.mu.Lock()
		n := len(p.conns)
		var inflight int64
		if n > 0 {
			inflight = p.conns[0].inflightCount()
		}
		p.mu.Unlock()
		t.Fatalf("expected %d in-flight on one connection, got conns=%d inflight=%d", callers, n, inflight)
	}

	if got := accepted.Load(); got != 1 {
		t.Fatalf("expected exactly 1 TCP connection (pipelined), got %d", got)
	}

	releaseAll()
	wg.Wait()
}

// TestPoolDialFailureNonFatal proves a transient dial failure does not surface
// as an op error when a healthy connection exists: the pool falls back to
// pipelining onto the live connection.
func TestPoolDialFailureNonFatal(t *testing.T) {
	ln := startEchoServer(t)
	defer ln.Close()

	// PipelineDepth=1 so a single in-flight request marks the conn "saturated",
	// forcing get() down the grow path.
	p := newPool(ln.Addr().String(), PoolConfig{
		MinConns:      1,
		MaxConns:      4,
		PipelineDepth: 1,
		PrewarmConns:  1,
		DialTimeout:   2 * time.Second,
		HealthCheck:   1 * time.Hour,
	})
	defer p.close()

	ctx := context.Background()

	// Wait for pre-warm to settle on exactly one healthy connection.
	if !waitFor(t, 3*time.Second, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return len(p.conns) == 1
	}) {
		t.Fatal("pre-warm did not establish the single connection")
	}
	p.mu.Lock()
	healthy := p.conns[0]
	p.mu.Unlock()

	// Saturate it to PipelineDepth so get() tries to grow.
	healthy.inflight.Store(1)

	// Break dialing by pointing the pool at a dead address; the live conn stays.
	deadLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := deadLn.Addr().String()
	deadLn.Close() // nothing listens here now -> dials fail
	p.addr = deadAddr

	// get() must NOT return an error: it falls back to the healthy connection.
	got, err := p.get(ctx)
	if err != nil {
		t.Fatalf("get returned error despite a healthy connection available: %v", err)
	}
	if got != healthy {
		t.Fatalf("expected fallback to the healthy connection on dial failure")
	}
}

// TestPoolCloseDrainsInflight proves Close() wakes in-flight callers (they get
// the connection-closed error rather than hanging) and closes every connection.
func TestPoolCloseDrainsInflight(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	ln := countingEchoServer(t, nil, func() { <-release })
	defer ln.Close()
	defer releaseOnce.Do(func() { close(release) })

	p := newPool(ln.Addr().String(), PoolConfig{
		MinConns:      1,
		MaxConns:      4,
		PipelineDepth: 16,
		PrewarmConns:  1,
		DialTimeout:   2 * time.Second,
		HealthCheck:   1 * time.Hour,
	})

	ctx := context.Background()
	conn, err := p.get(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Launch an in-flight request that the server holds open.
	done := make(chan error, 1)
	go func() {
		_, e := conn.roundTrip(context.Background(), OpPing, 0, nil)
		done <- e
	}()

	// Wait until the request is in flight, then close the pool.
	if !waitFor(t, 2*time.Second, func() bool { return conn.hasInflight() }) {
		t.Fatal("request never became in-flight")
	}

	if err := p.close(); err != nil {
		t.Fatalf("close returned error: %v", err)
	}

	// The in-flight caller must be woken with an error, not left hanging.
	select {
	case e := <-done:
		if e == nil {
			t.Fatal("expected in-flight roundTrip to error after Close, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not drain the in-flight request; caller hung")
	}

	if conn.alive() {
		t.Fatal("connection still alive after pool Close")
	}
}
