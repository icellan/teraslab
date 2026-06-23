# TeraSlab Hardware Recommendations

## NVMe Device

- **Minimum**: 1x NVMe SSD, 1 TB, 500K+ random 4K read IOPS
- **Recommended**: Samsung PM9A3, Intel P5800X, or equivalent datacenter NVMe
- **Endurance**: 1+ DWPD (Drive Writes Per Day) — TeraSlab's sub-block
  write coalescing significantly reduces write amplification vs the previous design
- **Queue depth**: TeraSlab issues synchronous O_DIRECT I/O (`src/device.rs`); the io_uring backend was removed 2026-05-28. Per-connection worker threads still drive useful device concurrency, so devices with strong QD performance help under load.

## Memory

- **Index**: 64 bytes per hash table bucket / one cache line (Robin Hood open-addressing). At load factor 0.5 (recommended), allocate capacity = 2x expected records.
- **For 100M records**: ~12.8 GB allocated (200M buckets x 64 bytes), ~6.4 GB occupied
- **For 1B records**: ~128 GB allocated (2B buckets x 64 bytes), ~64 GB occupied
- **Hugepages**: Enable 2 MB hugepages for the hash table mmap to reduce TLB misses (Linux only)
  - `echo 4096 > /proc/sys/vm/nr_hugepages` for ~8 GB of hugepage-backed index
- **Total system RAM**: Index allocation + 2 GB for OS/buffers + 1 GB per replication connection

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

- Linux (production target; O_DIRECT raw-device and file-backed modes)
- macOS supported for development (file-backed I/O)
- I/O is synchronous O_DIRECT via `src/device.rs`; the io_uring backend was removed 2026-05-28, so no specific kernel version is required for it
- `ulimit -n 65536` for high connection counts
