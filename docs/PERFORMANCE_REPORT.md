# TeraSlab Performance Report

## Test Environment

All benchmarks run on MemoryDevice (in-memory block device) to isolate
algorithmic throughput from I/O hardware. Numbers on real NVMe devices
will differ — run `cargo bench` on target hardware for production baselines.

## How to Run

```bash
# Fast (development) — default, ~4s
cargo test --test e2e_workload

# Full volume (CI nightly) — ~10 minutes
TERASLAB_FULL_WORKLOAD=1 cargo test --test e2e_workload

# Criterion benchmarks (statistical, ~5 minutes)
cargo bench
```

## Operation Throughput (MemoryDevice, single thread, debug build)

| Operation | Ops | Time | Throughput |
|-----------|-----|------|------------|
| Spend (single) | 50K | measured | see test output |
| SpendMulti (batch 10) | 5K batches | measured | see test output |
| SetMined | 10K | measured | see test output |
| Create (10 UTXOs) | 10K | measured | see test output |
| Read metadata | 30K | measured | see test output |

Run `cargo test --test e2e_workload -- --nocapture perf_` to see numbers.

## Concurrent Throughput

| Threads | Operation | Notes |
|---------|-----------|-------|
| 1 | Spend | Baseline |
| 4 | Spend | Near-linear scaling (different txids) |
| 8 | Spend | Diminishing returns on stripe contention |

## Memory Per Record

- Hash table bucket: 72 bytes (1 occupied + 2 probe_distance + 32 txid + 8 fingerprint + 27 TxIndexEntry + padding)
- Core index entry (TxIndexEntry): 27 bytes (device_id, record_offset, utxo_count, cold_offset, cold_size, flags)
- Hash table uses Robin Hood open addressing; at load factor 0.5, total allocated is ~144 bytes per record including empty buckets

## Design Targets (from SPEC_BRIEFING.md)

These targets assume production hardware (NVMe SSD, O_DIRECT, io_uring):

| Metric | Target | Status |
|--------|--------|--------|
| Spend throughput | > 500K ops/sec | Requires NVMe benchmark |
| Spend p99 latency | < 1ms | Requires NVMe benchmark |
| Spend p99.9 latency | < 5ms | Requires NVMe benchmark |
| SpendMulti (batch 10) | > 200K batches/sec | Requires NVMe benchmark |
| SetMined throughput | > 500K ops/sec | Requires NVMe benchmark |
| Create (10 UTXOs) | > 100K ops/sec | Requires NVMe benchmark |
| Memory per record | < 64 bytes | 72 bytes per bucket (see notes) |
| SSD write amplification | < 10x | Requires NVMe measurement |

## Correctness Validation

All tests pass at both fast and full scale:

- Mixed workload (100K ops): zero state mismatches
- 10 concurrent threads: zero mismatches
- Crash injection (10 seeds, 1% crash rate): zero data loss
- Deterministic simulation (10 seeds, 50K ops): reproducible, zero inconsistencies
- Block arrival/reorg/mempool churn: all correct
- Large transaction (1000 UTXOs): all operations correct
- Tiered storage: all tiers work, cleanup on delete verified
- Sustained workload (10 rounds): no state drift
- Device fill + churn: freelist reuse verified
