# TeraSlab Tuning Guide

## Key Configuration Parameters

### Index

| Parameter | Default | Effect |
|-----------|---------|--------|
| `index_capacity` | 1M | Initial hash table bucket count. Set to ~2x expected record count for load factor ~0.5. Over-provisioning wastes memory; under-provisioning increases probe distance. |
| Hugepages | auto-detect | Attempts 2 MB hugepages, falls back to regular mmap. Enable hugepages in the OS for large indexes (>1 GB). |

### Locks

| Parameter | Default | Effect |
|-----------|---------|--------|
| `stripe_count` | 1024 | Number of lock stripes. More stripes = less contention for concurrent operations on different txids. Diminishing returns past 4096 for typical workloads. |

### Allocator

| Parameter | Default | Effect |
|-----------|---------|--------|
| Device size | configured | Total device capacity. Allocator manages a freelist over this space. |
| Block alignment | 4096 | All I/O aligned to this boundary (must match device sector size for O_DIRECT). |

### Replication

| Parameter | Default | Effect |
|-----------|---------|--------|
| `replication_factor` | 1 | Number of copies (1 = no replication). RF=2 adds ~20-30% latency overhead for synchronous replication. |
| `sync_replication` | true | If true, writes wait for replica ACK. Set to false for higher throughput at the cost of durability. |

### Cluster

| Parameter | Default | Effect |
|-----------|---------|--------|
| `heartbeat_interval_ms` | 1000 | SWIM protocol heartbeat frequency. Lower = faster failure detection, higher network overhead. |
| `suspicion_timeout_ms` | 5000 | Time before a suspected node is declared dead. |

### Server

| Parameter | Default | Effect |
|-----------|---------|--------|
| `listen_addr` | 0.0.0.0:9100 | TCP listen address for the wire protocol. |
| `http_addr` | 0.0.0.0:9101 | HTTP address for observability endpoints. |
| `max_connections` | 1024 | Maximum concurrent TCP connections. |

## Performance Tuning Checklist

1. **Set index capacity to 2x expected records** — avoids rehashing and keeps probe distance low
2. **Enable hugepages** — reduces TLB misses for large indexes
3. **Use O_DIRECT device** — bypasses page cache for predictable latency
4. **Match stripe count to core count** — `stripe_count = num_cpus * 64` is a good starting point
5. **Pin to NUMA node** — if the NVMe device is on a specific NUMA node, pin the process there
6. **Disable swap** — TeraSlab manages its own memory; swap introduces unpredictable latency
7. **Set `vm.dirty_ratio=5`** — reduces OS background writeback interference (though O_DIRECT bypasses this)

## Monitoring

- `/metrics` endpoint: counters for all operation types, latency histograms
- `/health` endpoint: node status, index load factor, device utilization
- Watch for: index load factor > 0.7 (consider expanding), device utilization > 80% (consider tiered storage or capacity expansion)
