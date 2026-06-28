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
		return p.connCount() == maxConns
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
		return p.totalInflight() >= callers
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
		PoolShards:    1, // single shard so all callers route to the one warm conn
		DialTimeout:   2 * time.Second,
		HealthCheck:   1 * time.Hour,
	})
	defer func() { releaseAll(); p.close() }()

	// Ensure exactly one warm connection.
	if !waitFor(t, 3*time.Second, func() bool {
		return p.connCount() == 1
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
		conns := p.allConns()
		return len(conns) == 1 && conns[0].inflightCount() == callers
	}) {
		conns := p.allConns()
		n := len(conns)
		var inflight int64
		if n > 0 {
			inflight = conns[0].inflightCount()
		}
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
		PoolShards:    1, // single shard so the saturated conn is the only fallback
		DialTimeout:   2 * time.Second,
		HealthCheck:   1 * time.Hour,
	})
	defer p.close()

	ctx := context.Background()

	// Wait for pre-warm to settle on exactly one healthy connection.
	if !waitFor(t, 3*time.Second, func() bool {
		return p.connCount() == 1
	}) {
		t.Fatal("pre-warm did not establish the single connection")
	}
	conns := p.allConns()
	healthy := conns[0]

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

// TestPoolShardingSpreadsAcquisition proves the sharding refactor: under
// concurrency the pool spreads connections across MORE THAN ONE shard (no
// single global lock funnels every acquisition through one connection set),
// while the total connection count still respects MaxConns as a hard ceiling
// and Close() drains every shard.
func TestPoolShardingSpreadsAcquisition(t *testing.T) {
	var accepted atomic.Int64
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	// Hold every request in flight so callers cannot serially reuse one conn —
	// the pool must spread them across shards/conns to absorb the concurrency.
	ln := countingEchoServer(t, &accepted, func() { <-release })
	defer ln.Close()

	const (
		maxConns = 16
		shards   = 4
	)
	p := newPool(ln.Addr().String(), PoolConfig{
		MinConns:           1,
		MaxConns:           maxConns,
		PoolShards:         shards,
		PipelineDepth:      1, // 1 in-flight saturates a conn, forcing growth/spread
		PrewarmConns:       shards,
		MaxConcurrentDials: shards,
		DialTimeout:        2 * time.Second,
		HealthCheck:        1 * time.Hour,
	})
	defer func() { releaseAll(); p.close() }()

	if got := len(p.shards); got != shards {
		t.Fatalf("expected %d shards, got %d", shards, got)
	}

	// Wait for pre-warm to seed connections across shards.
	if !waitFor(t, 3*time.Second, func() bool { return p.connCount() == shards }) {
		t.Fatalf("pre-warm did not reach %d conns, got %d", shards, p.connCount())
	}

	const callers = 200
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

	// Wait for the callers to fan out in flight.
	if !waitFor(t, 3*time.Second, func() bool { return p.totalInflight() >= callers }) {
		t.Fatalf("callers did not fan out in flight; got inflight=%d", p.totalInflight())
	}

	// (a) Acquisition is sharded: more than one shard ended up with connections.
	if got := p.shardsWithConns(); got < 2 {
		t.Fatalf("expected acquisition to spread across >1 shard, only %d shard(s) hold conns", got)
	}

	// (b) MaxConns is a hard ceiling across all shards.
	if got := p.connCount(); got > maxConns {
		t.Fatalf("MaxConns breached: %d conns across shards, want <= %d", got, maxConns)
	}
	if got := accepted.Load(); got > maxConns {
		t.Fatalf("dial storm past MaxConns: opened %d, want <= %d", got, maxConns)
	}
	if got := p.totalConns.Load(); got != int64(p.connCount()) {
		t.Fatalf("global counter %d drifted from actual conn count %d", got, p.connCount())
	}

	releaseAll()
	wg.Wait()

	// (c) Close() drains every shard.
	conns := p.allConns()
	if err := p.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	for _, c := range conns {
		if c.alive() {
			t.Fatal("Close left a connection alive in some shard")
		}
	}
	if got := p.connCount(); got != 0 {
		t.Fatalf("Close did not drain all shards: %d conns remain", got)
	}
	if got := p.totalConns.Load(); got != 0 {
		t.Fatalf("Close did not reset global counter: %d", got)
	}
}

// TestPoolShardsDefaultsClamp verifies shard-count derivation and clamping: an
// unset PoolShards derives a sane default (>=1, <= MaxConns), an oversized value
// is clamped to MaxConns, and the global cap holds regardless of shard count.
func TestPoolShardsDefaultsClamp(t *testing.T) {
	cases := []struct {
		name      string
		maxConns  int
		shards    int
		wantMin   int
		wantMaxLE int // shards must be <= this
	}{
		{"derived default", 16, 0, 1, 16},
		{"clamped to maxconns", 4, 100, 1, 4},
		{"explicit small", 8, 2, 2, 2},
		{"maxconns one forces single shard", 1, 8, 1, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := PoolConfig{MaxConns: tc.maxConns, PoolShards: tc.shards}
			cfg.defaults()
			if cfg.PoolShards < tc.wantMin {
				t.Errorf("PoolShards=%d, want >= %d", cfg.PoolShards, tc.wantMin)
			}
			if cfg.PoolShards > tc.wantMaxLE {
				t.Errorf("PoolShards=%d, want <= %d", cfg.PoolShards, tc.wantMaxLE)
			}
			if cfg.PoolShards > cfg.MaxConns {
				t.Errorf("PoolShards=%d exceeds MaxConns=%d", cfg.PoolShards, cfg.MaxConns)
			}
		})
	}
}
