package teraslab

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// PoolConfig configures a per-node connection pool.
type PoolConfig struct {
	// MinConns is the minimum number of connections to maintain, and the number
	// pre-warmed at pool creation (default: 2).
	MinConns int
	// MaxConns is the maximum number of connections (default: 16).
	MaxConns int
	// DialTimeout is the timeout for establishing new connections (default: 5s).
	DialTimeout time.Duration
	// HealthCheck is the interval for pinging idle connections (default: 15s).
	HealthCheck time.Duration
	// PipelineDepth is the target number of concurrent in-flight requests per
	// connection before the pool prefers to grow (dial a new connection) rather
	// than pile more onto an existing one. It matches the server's
	// pipeline_depth so the client keeps every connection busy without a dial
	// storm (default: 16). Once the pool is at MaxConns the depth target is only
	// advisory — the pool pipelines onto the least-loaded connection regardless.
	PipelineDepth int
	// PrewarmConns is the number of connections to dial eagerly at pool creation.
	// It is clamped to [MinConns, MaxConns]. Pre-warming means the connection set
	// exists before load arrives, so a burst of concurrent callers pipelines onto
	// warm connections instead of triggering a dial storm. Defaults to MinConns.
	PrewarmConns int
	// MaxConcurrentDials caps how many connections may be dialed simultaneously,
	// bounding transient growth so it never storms the server's per-IP accept cap
	// (default: 4).
	MaxConcurrentDials int
}

func (c *PoolConfig) defaults() {
	if c.MinConns <= 0 {
		c.MinConns = 2
	}
	if c.MaxConns <= 0 {
		c.MaxConns = 16
	}
	if c.MinConns > c.MaxConns {
		c.MinConns = c.MaxConns
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.HealthCheck <= 0 {
		c.HealthCheck = 15 * time.Second
	}
	if c.PipelineDepth <= 0 {
		c.PipelineDepth = 16
	}
	if c.PrewarmConns <= 0 {
		c.PrewarmConns = c.MinConns
	}
	if c.PrewarmConns < c.MinConns {
		c.PrewarmConns = c.MinConns
	}
	if c.PrewarmConns > c.MaxConns {
		c.PrewarmConns = c.MaxConns
	}
	if c.MaxConcurrentDials <= 0 {
		c.MaxConcurrentDials = 4
	}
	if c.MaxConcurrentDials > c.MaxConns {
		c.MaxConcurrentDials = c.MaxConns
	}
}

// connPool manages a pool of pipelined connections to a single node.
//
// Pipelining model: each pipeConn carries many concurrent in-flight requests
// (matched by request id), so the pool keeps a BOUNDED, pre-warmed connection
// set busy rather than dialing a fresh connection per concurrent caller. get()
// reuses a connection while its in-flight count is below PipelineDepth; it only
// grows the pool (dials) when EVERY existing connection is at/above
// PipelineDepth and the pool is below MaxConns. At MaxConns it pipelines onto
// the least-loaded connection. Dials are bounded by a semaphore so transient
// growth never storms the server, and a dial failure is non-fatal as long as a
// healthy connection exists.
type connPool struct {
	addr   string
	config PoolConfig

	mu    sync.Mutex
	conns []*pipeConn

	dialSem chan struct{} // bounds concurrent dials

	closed  atomic.Bool
	closeCh chan struct{}
	closeWg sync.WaitGroup
}

// newPool creates a connection pool for the given address. It pre-warms the
// pool to PrewarmConns connections (dialed in the background with bounded
// concurrency so creation does not block) and starts a background health-check
// goroutine.
func newPool(addr string, cfg PoolConfig) *connPool {
	cfg.defaults()
	p := &connPool{
		addr:    addr,
		config:  cfg,
		closeCh: make(chan struct{}),
		dialSem: make(chan struct{}, cfg.MaxConcurrentDials),
	}
	p.closeWg.Add(1)
	go p.healthLoop()
	p.closeWg.Add(1)
	go p.prewarm()
	return p
}

// prewarm dials exactly PrewarmConns connections at pool creation, dispatching
// the dials through the dial semaphore so at most MaxConcurrentDials run at
// once — the connection set exists before load arrives, with no mass dialing.
// Dial failures are non-fatal: the health loop replenishes toward MinConns and
// get() dials on demand.
func (p *connPool) prewarm() {
	defer p.closeWg.Done()
	var wg sync.WaitGroup
	for i := 0; i < p.config.PrewarmConns; i++ {
		select {
		case <-p.closeCh:
			wg.Wait()
			return
		case p.dialSem <- struct{}{}:
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-p.dialSem }()
			ctx, cancel := context.WithTimeout(context.Background(), p.config.DialTimeout)
			c, err := dial(ctx, p.addr, p.config.DialTimeout)
			cancel()
			if err != nil {
				return
			}
			p.mu.Lock()
			if len(p.conns) < p.config.MaxConns && !p.closed.Load() {
				p.conns = append(p.conns, c)
				p.mu.Unlock()
			} else {
				p.mu.Unlock()
				c.close()
			}
		}()
	}
	wg.Wait()
}

// get returns a healthy connection from the pool. It prefers an existing
// connection with headroom below PipelineDepth; if every connection is at/above
// the depth target and the pool can still grow, it dials a new one (bounded by
// the dial semaphore); otherwise it pipelines onto the least-loaded connection.
// It never returns an error for lack of a connection while a healthy one exists.
func (p *connPool) get(ctx context.Context) (*pipeConn, error) {
	if p.closed.Load() {
		return nil, fmt.Errorf("pool closed")
	}

	p.mu.Lock()
	p.reapDeadLocked()
	best, bestLoad := p.leastLoadedLocked()
	n := len(p.conns)

	// Reuse an existing connection when it still has pipeline headroom, or when
	// we are already at MaxConns (no room to grow — pipeline onto the
	// least-loaded one). A sequential caller always finds its single warm
	// connection idle (load 0 < PipelineDepth) and reuses it — reuse preserved.
	if best != nil && (bestLoad < int64(p.config.PipelineDepth) || n >= p.config.MaxConns) {
		p.mu.Unlock()
		return best, nil
	}

	// All existing connections are at/above PipelineDepth and we are below
	// MaxConns: try to grow. Hold no lock across the dial.
	canGrow := n < p.config.MaxConns
	p.mu.Unlock()

	if !canGrow {
		// Should not happen (covered above), but stay safe: pipeline if possible.
		if best != nil {
			return best, nil
		}
		return p.dialAndStore(ctx)
	}

	c, err := p.tryGrow(ctx)
	if err == nil {
		return c, nil
	}
	// Dial failed (e.g. transient per-IP cap). Don't surface an op error if we
	// have a healthy connection to pipeline onto — pile onto the least-loaded.
	p.mu.Lock()
	p.reapDeadLocked()
	best, _ = p.leastLoadedLocked()
	p.mu.Unlock()
	if best != nil {
		return best, nil
	}
	return nil, err
}

// leastLoadedLocked returns the alive connection with the fewest in-flight
// requests, and its load. Caller must hold p.mu. Returns (nil, 0) if the pool
// has no alive connections.
func (p *connPool) leastLoadedLocked() (*pipeConn, int64) {
	var best *pipeConn
	var bestLoad int64
	for _, c := range p.conns {
		if !c.alive() {
			continue
		}
		load := c.inflightCount()
		if best == nil || load < bestLoad {
			best, bestLoad = c, load
		}
	}
	return best, bestLoad
}

// reapDeadLocked removes closed connections from the pool. Caller must hold p.mu.
func (p *connPool) reapDeadLocked() {
	alive := p.conns[:0]
	for _, c := range p.conns {
		if c.alive() {
			alive = append(alive, c)
		}
	}
	p.conns = alive
}

// tryGrow dials one new connection, bounded by the dial semaphore, and adds it
// to the pool. The semaphore caps concurrent dials so a burst of callers that
// all observe a saturated pool cannot storm the server. If the semaphore is
// full or another grower already added capacity, it falls back to the
// least-loaded existing connection rather than blocking.
func (p *connPool) tryGrow(ctx context.Context) (*pipeConn, error) {
	select {
	case p.dialSem <- struct{}{}:
		defer func() { <-p.dialSem }()
	default:
		// A dial is already in flight; don't pile on. Pipeline onto an existing
		// connection so the caller never blocks waiting for the dial semaphore.
		p.mu.Lock()
		best, _ := p.leastLoadedLocked()
		p.mu.Unlock()
		if best != nil {
			return best, nil
		}
		// No connections at all yet — wait for a dial slot rather than fail.
		select {
		case p.dialSem <- struct{}{}:
			defer func() { <-p.dialSem }()
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.closeCh:
			return nil, fmt.Errorf("pool closed")
		}
	}

	// Re-check under lock: another grower may have already added capacity, and
	// we must not exceed MaxConns.
	p.mu.Lock()
	if len(p.conns) >= p.config.MaxConns {
		best, _ := p.leastLoadedLocked()
		p.mu.Unlock()
		if best != nil {
			return best, nil
		}
		// At MaxConns but every connection died — fall through to redial.
	} else {
		p.mu.Unlock()
	}

	return p.dialAndStore(ctx)
}

// dialAndStore dials a new connection and appends it to the pool, unless the
// pool reached MaxConns or closed while dialing (in which case the fresh
// connection is closed and the least-loaded existing connection is returned).
func (p *connPool) dialAndStore(ctx context.Context) (*pipeConn, error) {
	c, err := dial(ctx, p.addr, p.config.DialTimeout)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	if p.closed.Load() {
		p.mu.Unlock()
		c.close()
		return nil, fmt.Errorf("pool closed")
	}
	if len(p.conns) >= p.config.MaxConns {
		// Raced past MaxConns — discard the new conn, reuse an existing one.
		best, _ := p.leastLoadedLocked()
		p.mu.Unlock()
		c.close()
		if best != nil {
			return best, nil
		}
		// All existing conns died; keep the one we just dialed instead.
		p.mu.Lock()
		p.conns = append(p.conns, c)
		p.mu.Unlock()
		return c, nil
	}
	p.conns = append(p.conns, c)
	p.mu.Unlock()
	return c, nil
}

// close closes all connections and stops the health check loop.
func (p *connPool) close() error {
	if p.closed.Swap(true) {
		return nil
	}
	close(p.closeCh)
	p.closeWg.Wait()

	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		c.close()
	}
	p.conns = nil
	return nil
}

// healthLoop periodically pings connections and removes dead ones.
func (p *connPool) healthLoop() {
	defer p.closeWg.Done()
	ticker := time.NewTicker(p.config.HealthCheck)
	defer ticker.Stop()
	for {
		select {
		case <-p.closeCh:
			return
		case <-ticker.C:
			p.checkHealth()
		}
	}
}

func (p *connPool) checkHealth() {
	// Snapshot the current connections so the actual ping round-trips happen
	// without holding the pool lock.
	p.mu.Lock()
	snapshot := make([]*pipeConn, len(p.conns))
	copy(snapshot, p.conns)
	p.mu.Unlock()

	// Actively probe each live connection. A connection whose TCP peer has
	// gone away but hasn't been written to since will still report alive()==true
	// (it only tracks the closed flag); an OpPing round-trip surfaces it.
	dead := make(map[*pipeConn]struct{})
	for _, c := range snapshot {
		if !c.alive() {
			dead[c] = struct{}{}
			continue
		}
		// Skip connections with in-flight requests: they are demonstrably in
		// active use, so a liveness probe is unnecessary, and — critically —
		// under load the ping response queues behind the in-flight work and can
		// exceed the timeout, producing a false "dead" verdict that would close
		// the connection and abort those legitimate requests with "connection
		// closed" (the server then sees a broken pipe writing the response). A
		// genuinely failed connection surfaces via its own read/write path.
		if c.hasInflight() {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), p.config.DialTimeout)
		resp, err := c.roundTrip(ctx, OpPing, 0, nil)
		cancel()
		if err != nil || resp.Status != StatusOK {
			dead[c] = struct{}{}
			continue
		}
		recyclePayload(resp.Payload)
	}

	p.mu.Lock()
	// Remove dead/unresponsive connections, but never force-close one that has
	// in-flight requests — closing aborts them mid-flight. Keep it and let the
	// next cycle re-probe once its requests drain.
	alive := p.conns[:0]
	for _, c := range p.conns {
		_, isDead := dead[c]
		if (isDead || !c.alive()) && !c.hasInflight() {
			c.close()
		} else {
			alive = append(alive, c)
		}
	}
	p.conns = alive
	deficit := p.config.MinConns - len(p.conns)
	p.mu.Unlock()

	// Replenish to MinConns.
	for range deficit {
		ctx, cancel := context.WithTimeout(context.Background(), p.config.DialTimeout)
		c, err := dial(ctx, p.addr, p.config.DialTimeout)
		cancel()
		if err != nil {
			break
		}
		p.mu.Lock()
		p.conns = append(p.conns, c)
		p.mu.Unlock()
	}
}
