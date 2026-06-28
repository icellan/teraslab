package teraslab

// Test-only helpers that aggregate state across the pool's shards. Production
// code never needs a global view of every connection; tests do, to assert
// pool-wide invariants (total connection count, every connection drained on
// Close, distribution across shards).

// allConns returns a snapshot of every live connection across all shards.
func (p *connPool) allConns() []*pipeConn {
	var out []*pipeConn
	for _, s := range p.shards {
		s.mu.Lock()
		out = append(out, s.conns...)
		s.mu.Unlock()
	}
	return out
}

// connCount returns the total number of connections held across all shards.
func (p *connPool) connCount() int {
	n := 0
	for _, s := range p.shards {
		s.mu.Lock()
		n += len(s.conns)
		s.mu.Unlock()
	}
	return n
}

// dialSemBusy reports whether any shard currently holds an outstanding dial
// (a dial-semaphore slot in use), so tests can wait for pre-warm to settle.
func (p *connPool) dialSemBusy() bool {
	for _, s := range p.shards {
		if len(s.dialSem) > 0 {
			return true
		}
	}
	return false
}

// shardsWithConns returns the number of shards that hold at least one
// connection — used to assert acquisition actually spread across shards.
func (p *connPool) shardsWithConns() int {
	n := 0
	for _, s := range p.shards {
		s.mu.Lock()
		if len(s.conns) > 0 {
			n++
		}
		s.mu.Unlock()
	}
	return n
}

// totalInflight sums in-flight requests across every connection in every shard.
func (p *connPool) totalInflight() int64 {
	var total int64
	for _, c := range p.allConns() {
		total += c.inflightCount()
	}
	return total
}
