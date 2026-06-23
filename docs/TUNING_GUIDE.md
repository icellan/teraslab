# TeraSlab Tuning Guide

## Key Configuration Parameters

### Index

| Parameter | Default | Effect |
|-----------|---------|--------|
| `expected_records` | 100,000 | Hint for initial hash table sizing. Set to ~2x expected record count for load factor ~0.5. Over-provisioning wastes memory; under-provisioning increases probe distance. |
| Hugepages | auto-detect | Attempts 2 MB hugepages on Linux, falls back to regular mmap. Enable hugepages in the OS for large indexes (>1 GB). Not available on macOS. |

### Locks

| Parameter | Default | Effect |
|-----------|---------|--------|
| `lock_stripes` | 65536 | Number of lock stripes. More stripes = less contention for concurrent operations on different txids. Uses bytes 16-17 of txid for stripe selection (different from index bucket bytes). Diminishing returns past 65536 for typical workloads. |

### Allocator

| Parameter | Default | Effect |
|-----------|---------|--------|
| `device_size` | 1 GiB | Total device capacity. Allocator manages a best-fit freelist over this space with adjacent-region coalescing. Data region starts at 1 MiB offset (header reserved for freelist persistence). |
| `device_alignment` | 4096 | All I/O aligned to this boundary (must match device sector size for O_DIRECT). |

### Replication

| Parameter | Default | Effect |
|-----------|---------|--------|
| `replication_factor` | 1 | Number of copies (1 = no replication). RF=2 adds ~20-30% latency overhead for synchronous replication. |
| `ack_policy` | `"auto"` | Replication acknowledgment policy. `"auto"` selects WriteAll for RF=2, WriteMajority for RF>=3. `"write_all"` waits for all replicas. `"write_majority"` waits for floor(RF/2)+1 copies. `"best_effort"` logs failures without failing the client. |
| `replication_timeout_ms` | 3000 | Timeout in milliseconds for each replication batch ACK. |
| `replication_degraded_mode` | `"reject"` | Behavior when the ack policy cannot be satisfied. `"reject"` fails the mutation. `"best_effort"` logs and succeeds. |

### Cluster

| Parameter | Default | Effect |
|-----------|---------|--------|
| `swim_probe_interval_ms` | 200 | SWIM protocol probe interval. Lower = faster failure detection, higher network overhead. |
| `swim_suspicion_timeout_ms` | 5000 | Time before a suspected node is declared dead. |
| `max_migration_threads` | 16 | Max concurrent migration threads per topology change. Prevents resource exhaustion during rapid churn. |
| `migration_pool_size` | 128 | Parallel TCP connections per migration target. More connections = higher throughput for large migrations. |
| `migration_batch_size` | 500 | Records per baseline streaming batch during migration. Larger batches reduce round-trip overhead. |

### Server

| Parameter | Default | Effect |
|-----------|---------|--------|
| `listen_addr` | `127.0.0.1:3300` | TCP listen address for the binary wire protocol. Loopback by default; non-loopback binds require `enable_remote_bind = true`. |
| `http_listen_addr` | `127.0.0.1:9100` | HTTP address for observability endpoints. Loopback by default. |
| `max_connections` | 1024 | Maximum concurrent TCP connections. |
| `max_batch_size` | 8192 | Maximum items per batch request. |

## Performance Tuning Checklist

1. **Set expected_records to ~2x expected record count** — keeps hash table load factor ~0.5 and probe distance low
2. **Enable hugepages** — reduces TLB misses for large indexes (`echo N > /proc/sys/vm/nr_hugepages`)
3. **Use O_DIRECT device** — bypasses page cache for predictable latency (automatic on Linux with file-backed devices)
4. **Match stripe count to core count** — `lock_stripes = num_cpus * 64` is a good starting point (default 65536 works well for up to ~1024 cores)
5. **Pin to NUMA node** — if the NVMe device is on a specific NUMA node, pin the process there
6. **Disable swap** — TeraSlab manages its own memory; swap introduces unpredictable latency
7. **Set `vm.dirty_ratio=5`** — reduces OS background writeback interference (though O_DIRECT bypasses this)

## Monitoring

- `/metrics` endpoint: Prometheus-format counters for all operation types
- `/admin/top` endpoint: full metrics snapshot including latency histograms, storage utilization, redo log state
- `/ws/top` WebSocket: real-time metrics push (updates every second)
- `/status` endpoint: cluster health overview (JSON)
- `/health/live` and `/health/ready`: liveness and readiness probes
- `teraslab-cli top`: TUI dashboard with live metrics
- Watch for: index load factor > 0.7 (consider expanding), device utilization > 80% (consider tiered storage or capacity expansion)
