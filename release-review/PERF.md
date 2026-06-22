# TeraSlab v1 — Performance Review

**Mode:** measure-what-host-allows (user choice). **Host:** Apple Silicon, macOS (darwin 25.3.0) — **no Linux, no real NVMe `O_DIRECT`**. Every benchmark below runs against `MemoryDevice` (anonymous `mmap`, no fsync, no redo durability). Claims that require Linux + NVMe + `O_DIRECT` + redo are marked **`unverified-on-this-host`** and are *not* passed.

**Methodology:** `cargo bench --bench <name> -- --warm-up-time 2 --measurement-time 5`, Criterion, release build, run **isolated** (no concurrent workflow/test load — an earlier run under CPU contention produced 2-4× depressed numbers and inflated variance; those are discarded). Numbers are `[lower median upper]` of Criterion's estimate. `Melem/s` = millions of the benchmark's declared elements per second.

> **Caveat on absolute throughput:** these are single-process in-memory ceilings, useful for catching algorithmic regressions, **not** production NVMe numbers. The README (lines 5-7, 22-24) already states this explicitly; this review confirms the code matches the *hedged* claim, not a bold one.

## Measured (MemoryDevice, this host)

| Benchmark | Median | Throughput |
|-----------|-------:|-----------:|
| `single_spend/spend_one` | 119 ns | 8.37 Melem/s |
| `spend_multi/1` | 301 ns | 3.32 Melem/s |
| `spend_multi/5` | 420 ns | 11.9 Melem/s |
| `spend_multi/10` | 436 ns | 22.9 Melem/s |
| `create/utxos/1` | 97 ns | 10.3 Melem/s |
| `create/utxos/10` | 114 ns | 8.75 Melem/s |
| `create/utxos/100` | 208 ns | 4.81 Melem/s |
| `set_mined/set_mined_one` | 1.33 µs | 0.75 Melem/s |
| `read/read_metadata` | 212 ns | 4.71 Melem/s |
| `index_lookup/hit/10k` | 6.18 ns | 161.7 Melem/s |
| `index_lookup/hit/100k` | 10.9 ns | 91.9 Melem/s |
| `index_lookup/hit/500k` | 40.8 ns | 24.5 Melem/s |
| `index_lookup/miss_100k` | 13.2 ns | 75.8 Melem/s |
| `mixed_workload/realistic_ratio` | 16.9 µs | 5.90 Melem/s |
| `codec` encode/decode hot ops | 15-21 ns | 50-1250 Melem/s |
| `spend_threaded/2` | 18.4 µs | 108 Kelem/s |
| `spend_threaded/4` | 34.4 µs | 116 Kelem/s |
| `spend_threaded/8` | 72.9 µs | 110 Kelem/s |

## Claim-by-claim verdict

| README claim | How checked | Result |
|--------------|-------------|--------|
| **10M+ ops/sec sustained** (MemoryDevice ceiling, per the hedged claim) | bench | **Consistent.** Single-core in-memory hot ops land high-single to low-double-digit Melem/s (`single_spend` 8.4M, `create/1` 10.3M, `spend_multi/10` 22.9M, index lookups 24-162M). The "10M+" headline is a *ceiling* figure and the code reaches that order of magnitude single-threaded in memory. |
| **NVMe + O_DIRECT + redo ≈ low-100s of K ops/sec per core** | code + bench | **`unverified-on-this-host`** (no NVMe/Linux). Directionally consistent: `spend_threaded` lands ~110 Kelem/s even on MemoryDevice once thread coordination + allocator serialization dominate. |
| **Spend write = 41-byte slot footer + ~320-byte metadata; low SSD wear** | code (byte accounting) | **Partially.** The on-disk slot is 73 B (compile-asserted `record.rs:910`) and metadata 320 B (`record.rs:916`). But production does **not** write a targeted 41-byte footer — it rewrites the full 73-B slot (`io.rs:947-948`) + full 320-B header (`io.rs:872-874`). On `O_DIRECT` both amplify to one 4096-B sector each regardless, so device wear is unchanged, but the "41-byte in-place write" wording is inaccurate (**REL-101**). |
| **Replication bandwidth ~120 MB/s, operation-based (not full-record)** | code | **Op-based: confirmed** (replication reviewer traced the wire payload; ReplicaOp variants carry op deltas, not full records — REVIEW §3.5 matrix). Bandwidth figure `unverified-on-this-host`. |
| **Memory ~64-byte bucket / ~91 bytes/record in-memory; ~0 with redb** | code (compile-assert) | **Confirmed by accounting, not RSS.** `BUCKET_SIZE == 64` is compile-asserted (`index/hashtable.rs:170-171`); at 0.7 load factor → ~91 B/record arithmetically. redb footprint = page cache (default 256 MiB) — not measured here. Did **not** measure live RSS-vs-record-count (would need a Linux long-run); flagged as residual. |
| **p99.9 latency: low, no CoW/defrag spikes** | — | **Not measured.** Criterion's `[lo med hi]` is a sample-mean band, not a sustained-load tail histogram (p50/p90/p99/p99.9). No tail-latency harness was run; the README itself says "not yet measured on production hardware." Residual for a Linux/NVMe sustained-load run with an HDR histogram, watching for checkpoint / redo-wraparound / migration spikes. |

## Bench coverage of hot paths

Criterion covers the hot paths: spend (`spend_throughput`), create (`mixed_workload`/create group), get/read (`read_metadata`, `index_lookup`), batch dispatch indirectly (`mixed_workload`), codec (`codec_ops`), allocator (`allocator_ops`). **Gap:** no Criterion bench for the full wire round-trip through `server/dispatch` (the benches hit the engine directly, bypassing frame decode + dispatch + lock acquisition), and no bench on the redb backend path (so the README's "redb ~100K-500K ops/sec" is **`unverified-on-this-host`** and also un-benched in-tree). Adding a redb-backend bench and a localhost-loopback dispatch bench would close the measurement gap.

## Footguns spotted (code, not measured)

- The spend/create path serializes through a single `parking_lot::Mutex<SlotAllocator>` (concurrency reviewer). On MemoryDevice this caps multi-thread create/spend scaling — visible as the flat ~110 Kelem/s `spend_threaded` plateau. On NVMe the device write dominates so the mutex is less likely the bottleneck, but it is the structural ceiling for in-memory multi-core throughput. Not a regression; a known design point worth a note in the tuning guide.
- No unnecessary per-op allocation found on the spend hot path by the reviewers; the codec round-trips are zero-copy where it matters (`p3_4_frame_zero_copy_allocs` test exists).

## Not verifiable on this host (explicit)

- DirectDevice / `O_DIRECT` / NVMe throughput, fault-injection device numbers, real SSD wear, replication bandwidth, sustained-load p99.9 tail — all require Linux + NVMe. Marked `unverified-on-this-host`; do **not** read the MemoryDevice numbers as production figures.
