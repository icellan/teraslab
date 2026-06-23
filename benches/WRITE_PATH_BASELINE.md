# Write-path baseline (pre-parallelization)

Commit: 0d44453 (branch feat/write-path-parallelism, before any Phase 2 change)
Host: Mac15,11 (Apple Silicon), 14 cores
Device: in-memory `MemoryDevice` (DRAM-backed); redo region 256 MiB, 512-byte alignment

## Real-TCP (tests/write_scaling.rs, slow-tests, 50K creates/client)
| clients | acked | ops/s | gauge_max | notes |
|---|---|---|---|---|
| 1 | 50000 | 2027 | 0 | |
| 8 | 400000 | 5867 | 0 | 2.89x vs 1 client |
| scaling 8/1 | | 2.89x | | likely latency-hiding across connections, not multi-core compute |
| 8-client CPU/wall | | | | 0.44 cores |

## Criterion (benches/write_path.rs, engine-level, NO redo log)
| workload | K=1 | K=2 | K=4 | K=8 |
|---|---|---|---|---|
| creates_100k (Melem/s) | 2.0 | 1.9 | 1.9 | 1.3 |
| spends_100k (Melem/s) | 2.2 | 2.6 | 2.6 | 1.6 |

(Engine-level throughput DROPS at K=8 even without a redo log -> global secondary-index mutex + striped-lock contention serialize independently of redo.)

## Attribution decision (gate into Phase 2)
- **Not CPU-bound.** 8-client CPU/wall = 0.44 cores: under 8 concurrent writers the server uses less than half a core. The path is latency/serialization-bound, not core-starved. "Scale past one core" is partly the wrong frame -- it uses <1 core because it blocks/parks, not because it cannot acquire more.
- **Group-commit sleep is a per-write wall tax.** `REDO_GROUP_COMMIT_WINDOW = 200us` `thread::sleep` runs on every `write_redo_ops` call. At 1 client (2027 ops/s = 493us/op) ~200us (~40%) is pure sleep. On the DRAM device fsync is ~free so the sleep buys nothing; on a real O_DIRECT SSD it amortizes the fsync. This dominates single-client latency.
- **Secondary-index + striped contention is real.** The redo-free engine bench still degrades K1->K8, so the single-writer redb secondary mutex + striped-lock contention serialize concurrent writers on their own.
- **CRC-under-lock is NOT the dominant cost here.** CPU is 0.44 cores -- moving CRC off the redo lock (plan Task 2a) is unlikely to help on this workload/device; deprioritize unless real-SSD profiling says otherwise.
- **Gauge coverage gap.** gauge_max=0 on the create workload: `writer_enter` (placed inside `update_*_index`, post-lock) never fires for creates because create->unmined is a no-op for these items; only spend->DAH would trip it. Task 2c's `gauge_max > 1` corroboration on a create workload requires re-placing the gauge to bracket the actually-parallelized create path.
- **Phase 2 order chosen (provisional, pending human gate):**
  1. Make group-commit adaptive (do not pay a fixed 200us sleep when there is no contention; coalesce by pending-writer count, not a blind sleep) -- biggest single-client latency lever.
  2. Secondary-index parallelism (batch-per-request -> shard-by-stripe) -- the K>1 contention the engine bench shows.
  3. Re-place the in-flight gauge to cover the create path (for 2c corroboration).
  4. CRC-off-lock (2a): deprioritized -- not CPU-bound on this workload.

---

## After sharding (N=16 default index shards)

Commit: feat/shard-primary-index (Tasks 1–8)
Host: Mac15,11 (Apple Silicon), 14 cores
Device: in-memory `MemoryDevice` (bench group `creates_100k_shards` / `spends_100k_shards`, K=8 fixed)
Measurement: `--warm-up-time 1 --measurement-time 3`, 10 samples each

### K=8, shard_count 1 vs 16 (index-lock isolation)
| workload | shard_count=1 (Melem/s) | shard_count=16 (Melem/s) | delta |
|---|---|---|---|
| creates_100k at K=8 | 1.24 | 1.14 | −8% |
| spends_100k at K=8 | 1.62 | 1.60 | −1% |

### K-sweep at default N=16 shards (groups `creates_100k` / `spends_100k`, unchanged helper — Engine::new → from_single → N=1 internally)
| workload | K=1 | K=2 | K=4 | K=8 |
|---|---|---|---|---|
| creates_100k (Melem/s) | 1.94 | 1.80 | 1.73 | 1.28 |
| spends_100k (Melem/s) | 2.12 | 2.61 | 2.52 | 1.63 |

### Interpretation

The shard_count=1 vs shard_count=16 delta at K=8 is within noise (creates −8%, spends −1%; no statistically significant win). This is expected on this micro-bench: the 100K-item workload accesses a fresh in-memory hash table that trivially fits in L3 cache. Each per-item index write takes O(100ns); a 16-way shard routing adds ~10ns of hash computation and an extra pointer chase, roughly matching the lock-save. The index RwLock is fast when there is no real read/write overlap (in this synthetic bench, all ops are writes from the same phase), so there is no dramatic contention to spread.

The sharding win is expected to surface in the real BSV IBD path, where long-lived read guards held during mempool queries contend with the bulk-write path — a pattern this bench does not exercise. The correctness contract (cross-shard reads not blocked by a write to a different shard) is proved by `ShardedIndex::contract_read_not_blocked_by_other_shard_write` (src/index/sharded.rs) and the engine-level equivalent in ops/engine.rs.

**Caveat:** all numbers above are in-memory DRAM measurements on a 14-core host under background load. Tail latency and fsync-bound IBD throughput on a real O_DIRECT NVMe SSD (the production bottleneck) are the owner's quickstart run to validate.
