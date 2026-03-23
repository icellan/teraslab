# TeraSlab Hardware Recommendations

## NVMe Device

- **Minimum**: 1x NVMe SSD, 1 TB, 500K+ random 4K read IOPS
- **Recommended**: Samsung PM9A3, Intel P5800X, or equivalent datacenter NVMe
- **Endurance**: 1+ DWPD (Drive Writes Per Day) — TeraSlab's sub-block
  write coalescing significantly reduces write amplification vs the previous design
- **Queue depth**: Devices with high QD performance benefit from io_uring batching

## Memory

- **Index**: ~58 bytes per record in the primary hash table
- **For 100M records**: ~5.8 GB index memory
- **For 1B records**: ~58 GB index memory
- **Hugepages**: Enable 2 MB hugepages for the hash table mmap to reduce TLB misses
  - `echo 4096 > /proc/sys/vm/nr_hugepages` for ~8 GB of hugepage-backed index
- **Total system RAM**: Index size + 2 GB for OS/buffers + 1 GB per replication connection

## CPU

- **Minimum**: 4 cores (handles ~200K ops/sec with striped locks)
- **Recommended**: 16+ cores for >1M ops/sec sustained
- Architecture: x86_64 or aarch64 (both supported, no SIMD requirements)

## Network

- **Minimum**: 1 Gbps for single-node deployment
- **Recommended**: 10 Gbps for clustered deployment with replication
- **Latency**: <1ms between cluster nodes for synchronous replication

## Filesystem

- TeraSlab uses O_DIRECT — no filesystem caching overhead
- XFS or ext4 on the NVMe device (for DirectDevice file-backed mode)
- For raw device mode: no filesystem needed, TeraSlab manages the device directly

## Operating System

- Linux 5.10+ (for io_uring support)
- macOS supported for development (falls back to synchronous I/O)
- `ulimit -n 65536` for high connection counts
