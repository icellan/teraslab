package teraslab

import (
	"context"
	"fmt"
	"runtime"
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
	// PoolShards is the number of independent sub-pools (shards) the connection
	// pool is split into. Each shard has its own mutex and its own slice of
	// connections, so connection acquisition contends on ~1/PoolShards of the
	// callers instead of a single global lock. Acquisition routes to a shard by a
	// cheap round-robin hint. Defaults to min(MaxConns, GOMAXPROCS), clamped to
	// [1, MaxConns]. The total connection count across all shards still respects
	// MaxConns as a hard ceiling.
	PoolShards int
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
	if c.PoolShards <= 0 {
		c.PoolShards = runtime.GOMAXPROCS(0)
	}
	if c.PoolShards > c.MaxConns {
		c.PoolShards = c.MaxConns
	}
	if c.PoolShards < 1 {
		c.PoolShards = 1
	}
}

// poolShard is one independent sub-pool: its own mutex, its own slice of
// connections, and its own dial semaphore. Acquisition routes to a shard by a
// round-robin hint, so callers contend on a shard's lock rather than a single
// global one, and the least-loaded scan is over a shard's (small) connection
// set rather than every connection in the pool.
type poolShard struct {
	mu      sync.Mutex
	conns   []*pipeConn
	dialSem chan struct{} // bounds concurrent dials within this shard
}

// connPool manages a sharded pool of pipelined connections to a single node.
//
// Sharding: the pool is split into N independent shards (PoolShards), each with
// its own mutex and connection slice. Every get() picks a shard via a per-call
// round-robin hint (robin % N), so concurrent callers spread across shards and
// connection acquisition no longer serializes on a single global lock. MaxConns
// is enforced as a HARD ceiling across all shards via the global totalConns
// counter (incremented optimistically before a dial, rolled back on failure or
// race), independent of how connections happen to distribute across shards.
//
// Pipelining model (per shard): each pipeConn carries many concurrent in-flight
// requests (matched by request id), so the pool keeps a BOUNDED, pre-warmed
// connection set busy rather than dialing a fresh connection per concurrent
// caller. get() reuses a connection in its shard while that connection's
// in-flight count is below PipelineDepth; it only grows (dials) when every
// connection in the shard is at/above PipelineDepth and the global cap allows.
// At the cap it pipelines onto the shard's least-loaded connection. Dials are
// bounded by a per-shard semaphore so transient growth never storms the server,
// and a dial failure is non-fatal as long as a healthy connection exists.
type connPool struct {
	addr   string
	config PoolConfig

	shards []*poolShard
	robin  atomic.Uint64 // per-get round-robin hint counter

	totalConns atomic.Int64 // global live connection count; hard ceiling = MaxConns

	closed  atomic.Bool
	closeCh chan struct{}
	closeWg sync.WaitGroup
}

// newPool creates a connection pool for the given address. It pre-warms the
// pool to PrewarmConns connections (distributed across shards, dialed in the
// background with bounded concurrency so creation does not block) and starts a
// background health-check goroutine.
func newPool(addr string, cfg PoolConfig) *connPool {
	cfg.defaults()
	n := cfg.PoolShards
	// Distribute the concurrent-dial budget across shards, at least one slot each.
	perShardDials := cfg.MaxConcurrentDials / n
	if perShardDials < 1 {
		perShardDials = 1
	}
	p := &connPool{
		addr:    addr,
		config:  cfg,
		shards:  make([]*poolShard, n),
		closeCh: make(chan struct{}),
	}
	for i := range p.shards {
		p.shards[i] = &poolShard{
			dialSem: make(chan struct{}, perShardDials),
		}
	}
	p.closeWg.Add(1)
	go p.healthLoop()
	p.closeWg.Add(1)
	go p.prewarm()
	return p
}

// shardFor returns the shard a fresh acquisition routes to, advancing the
// round-robin hint. The hint spreads concurrent callers across shards so they
// contend on different locks.
func (p *connPool) shardFor() *poolShard {
	idx := p.robin.Add(1) - 1
	return p.shards[idx%uint64(len(p.shards))]
}

// reserveSlot atomically reserves one connection slot against the global
// MaxConns ceiling. It returns true if a slot was reserved (the caller must
// release it via releaseSlot if the dial fails or the connection is discarded),
// false if the pool is already at MaxConns.
func (p *connPool) reserveSlot() bool {
	for {
		cur := p.totalConns.Load()
		if cur >= int64(p.config.MaxConns) {
			return false
		}
		if p.totalConns.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

// releaseSlot returns a previously reserved global connection slot.
func (p *connPool) releaseSlot() {
	p.totalConns.Add(-1)
}

// prewarm dials exactly PrewarmConns connections at pool creation, distributing
// them round-robin across shards and dispatching the dials through each shard's
// dial semaphore so concurrency stays bounded — the connection set exists before
// load arrives, with no mass dialing. Dial failures are non-fatal: the health
// loop replenishes toward MinConns and get() dials on demand.
func (p *connPool) prewarm() {
	defer p.closeWg.Done()
	var wg sync.WaitGroup
	for i := 0; i < p.config.PrewarmConns; i++ {
		shard := p.shards[i%len(p.shards)]
		select {
		case <-p.closeCh:
			wg.Wait()
			return
		case shard.dialSem <- struct{}{}:
		}
		if !p.reserveSlot() {
			<-shard.dialSem
			break
		}
		wg.Add(1)
		go func(s *poolShard) {
			defer wg.Done()
			defer func() { <-s.dialSem }()
			ctx, cancel := context.WithTimeout(context.Background(), p.config.DialTimeout)
			c, err := dial(ctx, p.addr, p.config.DialTimeout)
			cancel()
			if err != nil {
				p.releaseSlot()
				return
			}
			s.mu.Lock()
			if !p.closed.Load() {
				s.conns = append(s.conns, c)
				s.mu.Unlock()
			} else {
				s.mu.Unlock()
				c.close()
				p.releaseSlot()
			}
		}(shard)
	}
	wg.Wait()
}

// get returns a healthy connection from the pool. It routes to a shard by a
// round-robin hint, then prefers an existing connection in that shard with
// headroom below PipelineDepth; if every connection in the shard is at/above the
// depth target and the pool can still grow (global count below MaxConns), it
// dials a new one (bounded by the shard's dial semaphore); otherwise it
// pipelines onto the shard's least-loaded connection. It never returns an error
// for lack of a connection while a healthy one exists in the shard.
func (p *connPool) get(ctx context.Context) (*pipeConn, error) {
	if p.closed.Load() {
		return nil, fmt.Errorf("pool closed")
	}

	s := p.shardFor()

	s.mu.Lock()
	p.reapDeadLocked(s)
	best, bestLoad := p.leastLoadedLocked(s)

	// Reuse an existing connection when it still has pipeline headroom, or when
	// the pool is already at MaxConns (no room to grow — pipeline onto the
	// least-loaded one). A sequential caller routed to a shard with a single warm
	// connection finds it idle (load 0 < PipelineDepth) and reuses it.
	atCap := p.totalConns.Load() >= int64(p.config.MaxConns)
	if best != nil && (bestLoad < int64(p.config.PipelineDepth) || atCap) {
		s.mu.Unlock()
		return best, nil
	}
	s.mu.Unlock()

	// Every connection in this shard is at/above PipelineDepth (or the shard is
	// empty) and the pool may still grow: try to grow. Hold no lock across dial.
	c, err := p.tryGrow(ctx, s)
	if err == nil {
		return c, nil
	}
	// Dial failed (e.g. transient per-IP cap), or no slot was available. Don't
	// surface an op error if we have a healthy connection to pipeline onto — pile
	// onto the least-loaded one in this shard.
	s.mu.Lock()
	p.reapDeadLocked(s)
	best, _ = p.leastLoadedLocked(s)
	s.mu.Unlock()
	if best != nil {
		return best, nil
	}
	return nil, err
}

// leastLoadedLocked returns the alive connection in the shard with the fewest
// in-flight requests, and its load. Caller must hold s.mu. Returns (nil, 0) if
// the shard has no alive connections.
func (p *connPool) leastLoadedLocked(s *poolShard) (*pipeConn, int64) {
	var best *pipeConn
	var bestLoad int64
	for _, c := range s.conns {
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

// reapDeadLocked removes closed connections from the shard and decrements the
// global connection count for each one reaped. Caller must hold s.mu.
func (p *connPool) reapDeadLocked(s *poolShard) {
	alive := s.conns[:0]
	for _, c := range s.conns {
		if c.alive() {
			alive = append(alive, c)
		} else {
			p.releaseSlot()
		}
	}
	s.conns = alive
}

// tryGrow dials one new connection for the shard, bounded by the shard's dial
// semaphore and the global MaxConns ceiling, and adds it to the shard. The
// semaphore caps concurrent dials so a burst of callers that all observe a
// saturated shard cannot storm the server. If the semaphore is full or another
// grower already added capacity, it falls back to the shard's least-loaded
// existing connection rather than blocking.
func (p *connPool) tryGrow(ctx context.Context, s *poolShard) (*pipeConn, error) {
	select {
	case s.dialSem <- struct{}{}:
		defer func() { <-s.dialSem }()
	default:
		// A dial is already in flight for this shard; don't pile on. Pipeline onto
		// an existing connection so the caller never blocks on the dial semaphore.
		s.mu.Lock()
		best, _ := p.leastLoadedLocked(s)
		s.mu.Unlock()
		if best != nil {
			return best, nil
		}
		// No connections in this shard yet — wait for a dial slot rather than fail.
		select {
		case s.dialSem <- struct{}{}:
			defer func() { <-s.dialSem }()
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.closeCh:
			return nil, fmt.Errorf("pool closed")
		}
	}

	// Reserve a global slot. If the pool is at MaxConns, fall back to an existing
	// connection (in this shard, else any shard) rather than exceeding the cap.
	if !p.reserveSlot() {
		if best := p.anyLeastLoaded(s); best != nil {
			return best, nil
		}
		return nil, fmt.Errorf("pool at MaxConns with no live connection")
	}

	return p.dialAndStore(ctx, s)
}

// anyLeastLoaded returns the least-loaded live connection preferring the given
// shard, falling back to a scan of every shard. Used when the pool is at the
// global cap but the preferred shard is empty (its connections may live in other
// shards). Returns nil if no shard has a live connection.
func (p *connPool) anyLeastLoaded(prefer *poolShard) *pipeConn {
	prefer.mu.Lock()
	best, bestLoad := p.leastLoadedLocked(prefer)
	prefer.mu.Unlock()
	if best != nil {
		return best
	}
	for _, s := range p.shards {
		if s == prefer {
			continue
		}
		s.mu.Lock()
		c, load := p.leastLoadedLocked(s)
		s.mu.Unlock()
		if c != nil && (best == nil || load < bestLoad) {
			best, bestLoad = c, load
		}
	}
	return best
}

// dialAndStore dials a new connection and appends it to the shard. A global slot
// must already be reserved by the caller. If the pool closed while dialing, the
// fresh connection is closed and the slot released.
func (p *connPool) dialAndStore(ctx context.Context, s *poolShard) (*pipeConn, error) {
	c, err := dial(ctx, p.addr, p.config.DialTimeout)
	if err != nil {
		p.releaseSlot()
		return nil, err
	}
	s.mu.Lock()
	if p.closed.Load() {
		s.mu.Unlock()
		c.close()
		p.releaseSlot()
		return nil, fmt.Errorf("pool closed")
	}
	s.conns = append(s.conns, c)
	s.mu.Unlock()
	return c, nil
}

// close closes all connections across every shard and stops the health-check
// loop.
func (p *connPool) close() error {
	if p.closed.Swap(true) {
		return nil
	}
	close(p.closeCh)
	p.closeWg.Wait()

	for _, s := range p.shards {
		s.mu.Lock()
		for _, c := range s.conns {
			c.close()
		}
		s.conns = nil
		s.mu.Unlock()
	}
	p.totalConns.Store(0)
	return nil
}

// healthLoop periodically pings connections and removes dead ones across all
// shards.
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

// checkHealth probes connections across every shard, removes dead/unresponsive
// ones (never force-closing a connection with in-flight requests), and
// replenishes the pool toward MinConns.
func (p *connPool) checkHealth() {
	for _, s := range p.shards {
		p.checkShardHealth(s)
	}
	p.replenish()
}

// checkShardHealth probes and reaps one shard's connections. A connection with
// in-flight requests is never probed or force-closed: under load the ping
// response queues behind the in-flight work and can exceed the timeout,
// producing a false "dead" verdict that would close the connection and abort
// legitimate requests. A genuinely failed connection surfaces via its own
// read/write path.
func (p *connPool) checkShardHealth(s *poolShard) {
	// Snapshot the shard's connections so the ping round-trips happen without
	// holding the shard lock.
	s.mu.Lock()
	snapshot := make([]*pipeConn, len(s.conns))
	copy(snapshot, s.conns)
	s.mu.Unlock()

	dead := make(map[*pipeConn]struct{})
	for _, c := range snapshot {
		if !c.alive() {
			dead[c] = struct{}{}
			continue
		}
		if c.hasInflight() {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), p.config.DialTimeout)
		resp, err := c.roundTrip(ctx, OpPing, 0, nil)
		cancel()
		if err != nil || resp.Status != StatusOK {
			// Only mark dead after CONSECUTIVE probe failures: a single timeout is
			// usually a false positive (the idle conn got a request burst right as
			// we pinged it), and killing it here closes it out from under those
			// just-dispatched requests. See recordProbe / probeFailThreshold.
			if c.recordProbe(false) {
				dead[c] = struct{}{}
			}
			continue
		}
		c.recordProbe(true)
		recyclePayload(resp.Payload)
	}

	s.mu.Lock()
	// Remove dead/unresponsive connections, but never force-close one that has
	// in-flight requests — closing aborts them mid-flight. Keep it and let the
	// next cycle re-probe once its requests drain. Each removed connection
	// releases its global slot.
	alive := s.conns[:0]
	for _, c := range s.conns {
		_, isDead := dead[c]
		if (isDead || !c.alive()) && !c.hasInflight() {
			c.close()
			p.releaseSlot()
		} else {
			alive = append(alive, c)
		}
	}
	s.conns = alive
	s.mu.Unlock()
}

// replenish dials toward MinConns if the pool fell below it, distributing the
// replacements round-robin across shards and respecting the global MaxConns
// ceiling.
func (p *connPool) replenish() {
	for i := 0; ; i++ {
		if p.config.MinConns-int(p.totalConns.Load()) <= 0 {
			return
		}
		if !p.reserveSlot() {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), p.config.DialTimeout)
		c, err := dial(ctx, p.addr, p.config.DialTimeout)
		cancel()
		if err != nil {
			p.releaseSlot()
			return
		}
		s := p.shards[i%len(p.shards)]
		s.mu.Lock()
		if p.closed.Load() {
			s.mu.Unlock()
			c.close()
			p.releaseSlot()
			return
		}
		s.conns = append(s.conns, c)
		s.mu.Unlock()
	}
}
