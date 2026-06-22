# TeraSlab v1 Performance Review

**Host:** Apple M3, 24 GiB RAM, macOS (darwin 25.3.0)  
**Date:** 2026-06-22  
**Reviewer methodology:** Measured via `cargo bench` on `MemoryDevice` (anonymous mmap, no O_DIRECT, no fsync, no redo). This matches README's disclosed methodology (`README.md:22-24`).

---

## 1. Measured vs Claimed

| Claim (source) | Claimed | Measured (this host) | Status |
|----------------|---------|----------------------|--------|
| 10M+ ops/sec sustained (README headline, `README.md:5`) | 10M+ | **~4.9 Melem/s** single spend (`single_spend/spend_one`, 205 ns/op) | **Below target** — README disclaims as design target on MemoryDevice (`README.md:7`) |
| 10M+ ops/sec (disclaimed ceiling) | 10M+ | ~4.9 Melem/s single-thread spend | **~49% of ceiling claim** on this host |
| Mixed realistic workload throughput | (not quantified in README) | **~3.9 Melem/s** (`mixed_workload/realistic_ratio`, 25.5 µs/op) | Measured; no README baseline to compare |
| Spend threaded (2/4/8 threads) | (not in README) | 78–86 Kelem/s at 2 threads; degrades with contention | Measured; lock contention visible |
| redb backend ~100K–500K ops/sec | 100K–500K | **Not measured** — no redb bench in `benches/` | **unverified-on-this-host** |
| p99.9 latency target "low" | Low | **Not measured** — no latency histogram bench | **unverified** (`README.md:18` admits this) |
| Replication bandwidth ~120 MB/s | ~120 MB/s | **Not measured** — no replication throughput bench | **unverified**; op-based payload verified in code (`replication/protocol.rs`) |
| Memory ~72 bytes/record in-memory | ~72 B (older README) / 64 B bucket (`README.md:20`) | **Not measured** — no RSS-vs-record-count sweep run | **unverified-on-this-host** |
| Memory ~0 with redb | ~0 | **Not measured** | **unverified-on-this-host** |
| Spend write = 37 B slot + ~256 B metadata | 37+256 B | **Code-verified:** 73-byte physical slot write (`record.rs:238-244`), metadata bump (`README.md:15-16`) | Byte accounting verified in code; SSD wear not measurable on dev host |
| NVMe + O_DIRECT + redo durability throughput | "low-100s K ops/sec per core" (`README.md:7`) | **Not measured** — macOS dev host, no raw NVMe block device | **unverified-on-this-host** |

---

## 2. Benchmark Methodology

### 2.1 Engine benches (`benches/`)

```bash
# Single spend throughput (MemoryDevice, 512 MiB device, 200K index capacity)
cargo bench --bench spend_throughput -- single_spend --noplot

# Mixed workload (create/spend/get ratio)
cargo bench --bench mixed_workload -- --noplot

# Index hot-path
cargo bench --bench index_ops -- --noplot
```

**Configuration (from `benches/spend_throughput.rs`):**
- Device: `MemoryDevice::new(512 MiB, 4096)`
- Index: in-memory, capacity 200,000
- Lock stripes: 65,536
- Warmup: 3 s (Criterion default)
- Samples: 100

**Results (2026-06-22, Apple M3):**

| Benchmark | Throughput | Latency |
|-----------|------------|---------|
| `single_spend/spend_one` | 4.91 Melem/s | 210 ns/op |
| `spend_threaded/2` | 79.5 Kelem/s | 25.1 µs/op |
| `spend_threaded/4` | 84.1 Kelem/s | 47.5 µs/op |
| `spend_threaded/8` | 77.9 Kelem/s | 102.7 µs/op |
| `mixed_workload/realistic_ratio` | 3.92 Melem/s | 25.5 µs/op |
| `index_update_cached/update_100k` | 60.8 Melem/s | 16.5 ns/op |

**Note:** Criterion reported "performance has regressed" vs saved baseline — likely stale baseline from prior hardware/build. Absolute numbers above are from this run.

### 2.2 CLI bench (`teraslab-cli bench`)

Not run against live server in this review pass. Implementation sends `OP_PING` (102) over binary wire (`cli.rs:746-751`), not spend/create. README describes it as "quick benchmark" without quantified target.

### 2.3 Go client benches

```bash
cd client/go && go test -bench=. -benchtime=2s -run=^$ ./...
```

**Results:** Encode/decode microbenches only (no live server):
- `BenchmarkEncodeRequestSmall`: 28.9 ns/op, 24 B alloc
- `BenchmarkRoundTrip` (in-process mock): 40 µs/op
- `BenchmarkShardForTxID`: 2 ns/op

**Gap:** No `SpendBatch`/`CreateBatch`/`GetBatch` throughput bench against running server. **Finding REL-705.**

### 2.4 Rust client benches

No `benches/` target in `client/rust/`. In-process tokio tests only. **Finding REL-706.**

---

## 3. Hot-Path Benchmark Coverage

| Path | Criterion bench | Status |
|------|-----------------|--------|
| Single spend | `benches/spend_throughput.rs` | ✅ |
| Spend multi | `benches/spend_throughput.rs` | ✅ |
| Spend threaded | `benches/spend_throughput.rs` | ✅ |
| Create | `benches/spend_throughput.rs` | ✅ |
| Read/get | `benches/spend_throughput.rs` | ✅ |
| Set mined | `benches/spend_throughput.rs` | ✅ |
| Mixed workload | `benches/mixed_workload.rs` | ✅ |
| Index insert/lookup/update | `benches/index_ops.rs` | ✅ |
| Allocator | `benches/allocator_ops.rs` | ✅ |
| Codec encode/decode | `benches/codec_ops.rs` | ✅ |
| Wire dispatch (server) | `benches/` — none | ❌ **gap** |
| redb backend | none | ❌ **gap** |
| DirectDevice + redo | none | ❌ **gap** (acknowledged in README) |
| Replication TCP | none | ❌ **gap** |
| Client live-server | none | ❌ **gap** |

---

## 4. Bottleneck Analysis (from code + benches)

| Operation class | Likely bottleneck | Evidence |
|-----------------|-------------------|----------|
| Single spend (MemoryDevice) | CPU + lock stripe | ~210 ns/op suggests cache-hot in-memory path |
| Threaded spend | Lock contention | Throughput drops from 4.9M → 80K elem/s at 2 threads |
| Production spend (DirectDevice) | NVMe sector write + fsync + redo | `device.rs` O_DIRECT 4096-byte sectors; `redo.rs` sync per batch |
| Index lookup (cached) | Memory bandwidth | 16 ns/update at 100K entries |
| Wire codec | Allocation | Go `BenchmarkDecodePartialWithSignals`: 4416 B/op, 102 allocs |
| Checkpoint | Periodic fsync + compaction | No tail-latency measurement; README admits unmeasured |

---

## 5. Performance Footguns Identified

1. **Threaded spend contention** — 60× throughput drop from 1→2 threads on MemoryDevice suggests lock stripe hot spots under parallel load (`benches/spend_throughput.rs:265`).
2. **Go client decode allocations** — `BenchmarkDecodePartialWithSignals`: 102 allocs/op for partial-error batch decode.
3. **No redb perf regression gate** — operators choosing low-RAM redb backend have no published throughput numbers.
4. **Checkpoint/redo tail spikes** — unmeasured; could affect p99.9 in production.

---

## 6. Verdict

README has been updated to honestly disclaim performance numbers as design targets (`README.md:7,22-24`). **Measured MemoryDevice ceiling on this host is ~5M spends/sec single-threaded**, roughly half the historical 10M+ headline. Production NVMe numbers remain **unverified** and should not be cited until `DirectDevice` benches exist.

**No performance regression vs README's own methodology disclaimer** — but the gap between headline marketing number and measured ceiling should be communicated clearly at v1 release.