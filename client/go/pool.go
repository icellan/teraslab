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
	// MinConns is the minimum number of idle connections to maintain (default: 2).
	MinConns int
	// MaxConns is the maximum number of connections (default: 16).
	MaxConns int
	// DialTimeout is the timeout for establishing new connections (default: 5s).
	DialTimeout time.Duration
	// HealthCheck is the interval for pinging idle connections (default: 15s).
	HealthCheck time.Duration
}

func (c *PoolConfig) defaults() {
	if c.MinConns <= 0 {
		c.MinConns = 2
	}
	if c.MaxConns <= 0 {
		c.MaxConns = 16
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.HealthCheck <= 0 {
		c.HealthCheck = 15 * time.Second
	}
}

// connPool manages a pool of pipelined connections to a single node.
// Since each pipeConn supports pipelining, multiple goroutines can
// share connections. The pool round-robins across healthy connections.
type connPool struct {
	addr   string
	config PoolConfig

	mu    sync.Mutex
	conns []*pipeConn
	robin atomic.Uint64

	closed   atomic.Bool
	closeCh  chan struct{}
	closeWg  sync.WaitGroup
}

// newPool creates a connection pool for the given address.
// It starts a background health check goroutine.
func newPool(addr string, cfg PoolConfig) *connPool {
	cfg.defaults()
	p := &connPool{
		addr:    addr,
		config:  cfg,
		closeCh: make(chan struct{}),
	}
	p.closeWg.Add(1)
	go p.healthLoop()
	return p
}

// get returns a healthy connection from the pool, creating one if needed.
func (p *connPool) get(ctx context.Context) (*pipeConn, error) {
	if p.closed.Load() {
		return nil, fmt.Errorf("pool closed")
	}

	// Try to find a healthy connection via round-robin.
	p.mu.Lock()
	n := len(p.conns)
	if n > 0 {
		idx := p.robin.Add(1) % uint64(n)
		c := p.conns[idx]
		if c.alive() {
			p.mu.Unlock()
			return c, nil
		}
		// Remove dead connection.
		p.conns[idx] = p.conns[n-1]
		p.conns = p.conns[:n-1]
	}
	p.mu.Unlock()

	// No healthy connection available — create a new one.
	return p.createConn(ctx)
}

func (p *connPool) createConn(ctx context.Context) (*pipeConn, error) {
	p.mu.Lock()
	if len(p.conns) >= p.config.MaxConns {
		// At capacity — try to find any alive one.
		for _, c := range p.conns {
			if c.alive() {
				p.mu.Unlock()
				return c, nil
			}
		}
		// All dead, clear and recreate.
		p.conns = p.conns[:0]
	}
	p.mu.Unlock()

	c, err := dial(ctx, p.addr, p.config.DialTimeout)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
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
	// Remove dead/unresponsive connections.
	alive := p.conns[:0]
	for _, c := range p.conns {
		if _, isDead := dead[c]; isDead || !c.alive() {
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
