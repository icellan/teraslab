package teraslab

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ClusterConfig configures cluster-aware routing.
type ClusterConfig struct {
	// Seeds are the initial node addresses to connect to.
	Seeds []string
	// PoolConfig is applied to each per-node pool.
	PoolConfig PoolConfig
	// RefreshInterval controls how often the partition map is refreshed (default: 30s).
	RefreshInterval time.Duration
	// MaxRedirects is the maximum number of redirect retries per request (default: 3).
	MaxRedirects int
	// ClusterSecret HMAC-signs inter-node opcodes (OP_GET_PARTITION_MAP) when set.
	ClusterSecret []byte
}

func (c *ClusterConfig) defaults() {
	if c.RefreshInterval <= 0 {
		c.RefreshInterval = 30 * time.Second
	}
	if c.MaxRedirects <= 0 {
		c.MaxRedirects = 3
	}
}

// ShardForTxID computes the shard number for a transaction ID.
// Matches the Rust implementation: LE u16 from txid[0:2] & 0x0FFF.
func ShardForTxID(txid TxID) uint16 {
	return (uint16(txid[0]) | uint16(txid[1])<<8) & 0x0FFF
}

// cluster manages partition-map-aware routing across multiple nodes.
type cluster struct {
	config  ClusterConfig
	partMap atomic.Pointer[PartitionMap]

	mu         sync.RWMutex
	pools      map[uint64]*connPool // nodeID -> pool
	addrToNode map[string]uint64    // addr -> nodeID (for redirect lookup)

	refreshMu   sync.Mutex // serializes partition map refreshes
	closeCh      chan struct{}
	closeWg      sync.WaitGroup
}

// newCluster creates a cluster manager. It fetches the initial partition map
// from one of the seed nodes.
func newCluster(ctx context.Context, cfg ClusterConfig) (*cluster, error) {
	cfg.defaults()
	c := &cluster{
		config:     cfg,
		pools:      make(map[uint64]*connPool),
		addrToNode: make(map[string]uint64),
		closeCh:    make(chan struct{}),
	}

	// Connect to seeds and fetch partition map.
	if err := c.bootstrapFromSeeds(ctx); err != nil {
		c.closeAllPools()
		return nil, err
	}

	// Start background refresh loop.
	c.closeWg.Add(1)
	go c.refreshLoop()

	return c, nil
}

func (c *cluster) bootstrapFromSeeds(ctx context.Context) error {
	var lastErr error
	for _, addr := range c.config.Seeds {
		pool := newPool(addr, c.config.PoolConfig)
		conn, err := pool.get(ctx)
		if err != nil {
			pool.close()
			lastErr = err
			continue
		}

		// Fetch partition map (HMAC-signed when a cluster secret is configured).
		resp, err := conn.roundTrip(ctx, OpGetPartitionMap, 0, signPartitionMapPayload(c.config.ClusterSecret, nil))
		if err != nil {
			pool.close()
			lastErr = err
			continue
		}
		if resp.Status != StatusOK {
			pool.close()
			lastErr = fmt.Errorf("partition map: status %d", resp.Status)
			continue
		}

		pm, err := decodePartitionMap(resp.Payload)
		if err != nil {
			pool.close()
			lastErr = err
			continue
		}
		c.partMap.Store(pm)

		// Set up pools for all nodes.
		c.mu.Lock()
		for _, node := range pm.Nodes {
			if _, ok := c.pools[node.ID]; !ok {
				c.pools[node.ID] = newPool(node.Addr, c.config.PoolConfig)
			}
			c.addrToNode[node.Addr] = node.ID
		}
		c.mu.Unlock()

		// Close bootstrap pool if it's not one of the known nodes.
		found := false
		for _, node := range pm.Nodes {
			if node.Addr == addr {
				found = true
				break
			}
		}
		if !found {
			pool.close()
		}
		return nil
	}
	return fmt.Errorf("failed to connect to any seed: %w", lastErr)
}

// currentVersion returns the version of the client's last-known partition map,
// or 0 if no map has been loaded yet.
func (c *cluster) currentVersion() uint64 {
	pm := c.partMap.Load()
	if pm == nil {
		return 0
	}
	return pm.Version
}

// allPools returns one connection pool per distinct node address. Used by
// per-node queries (pruner/iterator ops) that must reach every master.
func (c *cluster) allPools() []*connPool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	seen := make(map[string]struct{}, len(c.pools))
	pools := make([]*connPool, 0, len(c.pools))
	for _, p := range c.pools {
		if _, ok := seen[p.addr]; ok {
			continue
		}
		seen[p.addr] = struct{}{}
		pools = append(pools, p)
	}
	return pools
}

// poolForTxID returns the connection pool for the node that owns this txid's shard.
func (c *cluster) poolForTxID(txid TxID) (*connPool, error) {
	return c.poolForShard(ShardForTxID(txid))
}

// poolForShard returns the connection pool for the master of the given shard.
func (c *cluster) poolForShard(shard uint16) (*connPool, error) {
	pm := c.partMap.Load()
	if pm == nil {
		return nil, fmt.Errorf("no partition map")
	}
	nodeID := pm.Assignments[shard]
	c.mu.RLock()
	pool, ok := c.pools[nodeID]
	c.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no pool for node %d (shard %d)", nodeID, shard)
	}
	return pool, nil
}

// handleRedirect processes a redirect to the given address.
// Returns the pool for the target node and triggers an async refresh.
func (c *cluster) handleRedirect(addr string) (*connPool, error) {
	c.mu.Lock()
	if c.pools == nil {
		c.mu.Unlock()
		return nil, fmt.Errorf("cluster closed")
	}
	// Check if we already have a pool for this address.
	if nodeID, ok := c.addrToNode[addr]; ok {
		if pool, ok := c.pools[nodeID]; ok {
			c.mu.Unlock()
			go c.tryRefresh()
			return pool, nil
		}
	}
	// Create a new pool for the unknown address.
	pool := newPool(addr, c.config.PoolConfig)
	// Use a temporary node ID (negative-ish).
	tempID := uint64(0xFFFFFFFF00000000) | uint64(len(c.pools))
	c.pools[tempID] = pool
	c.addrToNode[addr] = tempID
	c.mu.Unlock()

	go c.tryRefresh()
	return pool, nil
}

// refreshPartitionMap fetches and applies a new partition map.
func (c *cluster) refreshPartitionMap(ctx context.Context) error {
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()

	// Try each known node.
	c.mu.RLock()
	pools := make([]*connPool, 0, len(c.pools))
	for _, p := range c.pools {
		pools = append(pools, p)
	}
	c.mu.RUnlock()

	var lastErr error
	for _, pool := range pools {
		conn, err := pool.get(ctx)
		if err != nil {
			lastErr = err
			continue
		}
		resp, err := conn.roundTrip(ctx, OpGetPartitionMap, 0, signPartitionMapPayload(c.config.ClusterSecret, nil))
		if err != nil {
			lastErr = err
			continue
		}
		if resp.Status != StatusOK {
			lastErr = fmt.Errorf("status %d", resp.Status)
			continue
		}
		pm, err := decodePartitionMap(resp.Payload)
		if err != nil {
			lastErr = err
			continue
		}
		c.partMap.Store(pm)

		// Update pools for new nodes. Skip if the cluster has been closed
		// (closeAllPools sets c.pools = nil), which races against in-flight
		// tryRefresh goroutines started by handleRedirect.
		c.mu.Lock()
		if c.pools == nil {
			c.mu.Unlock()
			return nil
		}
		for _, node := range pm.Nodes {
			if _, ok := c.pools[node.ID]; !ok {
				c.pools[node.ID] = newPool(node.Addr, c.config.PoolConfig)
			}
			c.addrToNode[node.Addr] = node.ID
		}
		c.mu.Unlock()
		return nil
	}
	return fmt.Errorf("refresh partition map: %w", lastErr)
}

func (c *cluster) tryRefresh() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = c.refreshPartitionMap(ctx) // best-effort
}

// refreshLoop periodically refreshes the partition map.
func (c *cluster) refreshLoop() {
	defer c.closeWg.Done()
	ticker := time.NewTicker(c.config.RefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.closeCh:
			return
		case <-ticker.C:
			c.tryRefresh()
		}
	}
}

// close stops the refresh loop and closes all pools.
func (c *cluster) close() error {
	close(c.closeCh)
	c.closeWg.Wait()
	c.closeAllPools()
	return nil
}

func (c *cluster) closeAllPools() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range c.pools {
		p.close()
	}
	c.pools = nil
}
