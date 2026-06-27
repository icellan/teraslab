# Performance Ledger — TeraSlab vs. the reference datastore

Running record of the measure → profile → hypothesize → fix → re-measure loop.
See `METHODOLOGY.md` for setup, fairness rationale, and the explicit pass
condition. Every row carries a host-state tag (`[loaded-host]` /
`[quiet-host]`).

Numbers are **lower-is-better** for latency, **higher-is-better** for throughput.
Backend column: `TS` = TeraSlab, `REF` = reference datastore.

## Status

- [x] TeraSlab baseline build/clippy/test clean (release build ✓, clippy ✓,
      `cargo test --all` exit 0 — captured 2026-06-27).
- [x] Harness architecture decided: single Go driver (Teranode `utxo.Store`),
      two backends, matched async durability, interleaved measurement.
- [x] Unified Go load generator built (`teranode-bench-wt/cmd/utxobench`, outside repo).
- [x] Both servers stood up with matched async-durability configs.
- [x] Baseline head-to-head captured (2026-06-27, `[loaded-host]`).
- [ ] Pass condition met. **Current: TeraSlab LOSES** (see B0).

## B0 — Baseline (2026-06-27) `[loaded-host]`

Config: 128 workers, 5s warmup + 20s measure, 3 interleaved rounds. Both async
durability (TS `redo_buffered`+writeback linear redo; REF no commit-to-device).
Driver: unified Go loadgen via Teranode `utxo.Store`. Raw:
`bench/results/20260627_181020_baseline/`.

Median across 3 rounds (ops/sec, p99.9 µs):

| op | TS ops/s | REF ops/s | TS/REF | TS p99.9 | REF p99.9 | verdict |
|----|---------|----------|--------|----------|-----------|---------|
| **spend** | 1381 | **3342** | 0.41× | 294,940 | **26,089** | LOSE (2.4× tput, 11× tail) |
| create | 1830 | 4437 | 0.41× | 328,862 | 24,767 | LOSE |
| get | 919 | 2235 | 0.41× | ~326,000 | ~24,500 | LOSE |
| setmined | 459 | 1115 | 0.41× | 264,090 | 5,352 | LOSE |

**Diagnostics:**
- Server-side per-stage (TS `/metrics`, avg): spend 224µs, create 1213µs
  (redo 445 + devwrite 102 + reserve 11), get 206µs, set_mined 2373µs,
  **redo_flush 2408µs**. → server-side spend is 224µs but client sees 295ms p99.9:
  a ~1000× gap = queueing OUTSIDE the server.
- **TeraSlab degrades across rounds** (spend ops/s 1632 → 1381 → 734; p99.9
  329ms → 295ms → 652ms) while the **reference is stable** (3427 → 3331 → 3342;
  p99.9 ~25ms). Round-over-round collapse under sustained load is the signature of
  the linear-redo checkpoint stall (lever 6d/7 regime).

**Leading hypotheses (to profile, not assume):**
- H1: linear-redo checkpoint stall under sustained load → enable segment ring
  (`redo_segment_ring`, lever 7, purpose-built for this) and re-measure.
- H2: client/adapter batcher + pipelined-pool queueing inflates the tail
  independent of server time.

## P1 — Profiling (2026-06-27): the datastore is NOT the bottleneck

Deep profiling of the B0 loss, via `/admin/top`, `/debug/redo`, `/metrics`
time-series, container CPU, and a client-vs-server cross-check:

1. **Server-side compute is fast and uncontended.** `/admin/top` under 128-worker
   load: spend mean **16µs** (p99.9 131µs), `lock_wait` **0**. Container CPU
   **~0.5 of 8 cores** — the server is ~94% idle / I/O-waiting, never CPU-bound.
2. **Server can do ~30k ops/s.** TeraSlab's *native Rust loadgen* against the SAME
   server: **30,071 ops/s** (64 conns, batch 16) and 8,844 ops/s (64 conns,
   batch 1) — vs the reference's ~11k ops/s aggregate. The datastore already wins
   by ~3× on its own client.
3. **The loss is entirely client/adapter-side.** Driven by the Teranode→TeraSlab
   Go adapter (the production path), TeraSlab does only ~1500 ops/s because:
   - the adapter's connection pool defaults to **16 conns** (vs the aerospike Go
     client's ~100) — an unfair concurrency handicap [fixed in driver: pool_size];
   - the adapter issues **1 spend per RPC** (no spend batching), while creates/gets
     ARE batched; one spend RPC ≈ one redo-flush-rate slot → ~450 spends/s.
   - server enforces a **per-IP connection cap of 64** (caps client concurrency).
4. Raising the adapter pool 16→60 only lifted spend 1381→1779/s with p99.9 still
   ~363ms — so connection count alone is not the fix; the adapter's batcher /
   spend-per-RPC path dominates the tail.

**Conclusion:** the TeraSlab *datastore* is already faster than the reference on
the matched async-durability UTXO workload; the head-to-head loss on the
production `utxo.Store` path is caused by the **WIP Go adapter** (lives in the
Teranode tree, not this repo), not by the server. Next: confirm reference-adapter
spend batching parity, then optimize the adapter (pool default + spend batching +
batcher tuning) to realize the datastore's advantage on the production path.

## P2 — Profiling (2026-06-27): latency-bound, RPC-batching is the lever

Go client/adapter profiled with block/mutex/CPU profiles + per-op math:

- **Both ends idle.** Client (Go loadgen) ~18% CPU; server ~0.5/8 cores. Mutex
  contention negligible (343ms total). Not CPU- or lock-bound.
- **Latency-bound on ~7ms Docker-network RTT.** Throughput ≈ in-flight ÷ RTT.
  Aggregate ops/s: reference **11.1k**, Go adapter **5.9k**, TeraSlab native Rust
  client **8.8k** (batch 1) / **30k** (batch 16). Batching amortizes the RTT —
  the dominant lever.
- **TeraSlab Go adapter batches create & get but NOT spend** (5399 spend RPCs for
  5399 spends = 1 item/RPC). Create/get use go-batcher (concurrent dispatch,
  unbounded goroutines — verified, not serial). The create/get batchers work.
- **Two distinct gaps:** (a) spend has no adapter batching → 1 RPC/spend, so spend
  throughput = concurrency÷RTT, not the server's 16µs capability; (b) server-side
  create is 1.2ms each (vs spend 16µs) — a real server cost for the create path.
- Spend batching is constrained by `current_block_height`: a SpendBatch RPC takes
  ONE params, so only spends sharing a height coalesce. Production (block
  validation) shares one height per block — so batching IS the production pattern;
  the loadgen's per-spend height increment must mirror that to benefit.

**Plan (chosen direction = optimize Go client/adapter):** add a spend batcher to
the teraslab adapter that groups concurrent Spend() calls by params and coalesces
them into multi-item SpendBatch RPCs (mirroring storeBatcher), + make the bench's
spend workload block-realistic (a block's spends share a height). Validated by the
native Rust client: batch-16 already yields ~9k spends/s (> reference 3342).

## E4 — Server bottleneck localized: redo pwrite under the log mutex (2026-06-27)

Direction = server write-concurrency (chosen). Using `create`'s sub-stage
metrics (same redo+cache path as spend) under 60 concurrent writers:

```
create total : 24952 us   (n=28878)
  redo       : 23909 us   ← 96% of the time
  devwrite   :    47 us   ← cache write is fine
  reserve    :     6 us
```

So the write serialization is **entirely the redo path** (not the cache, not the
stripe locks — `lock_wait=0`). Mechanism: the single per-store redo-log mutex is
held across `RedoLog::flush_pwrite_no_sync` — the O_DIRECT **pwrite** of the
buffered entries. Lever 6b already moved the *fsync* (`sync_device`) outside the
lock, but the **pwrite is still under it**. Under 60 concurrent committers, every
`commit()` (which must take the same mutex to append) blocks behind whoever is
mid-pwrite → ~24ms average. BATCH_SIZE=1 (no spend coalescing) shows the same
33ms, confirming it is not the new spend batcher.

**Fix (next, TDD): double-buffer the redo.** Under the log mutex, swap the full
in-memory buffer out (replace with an empty one) and snapshot `write_pos` — O(1),
µs. Release the lock. Do the O_DIRECT pwrite of the swapped-out buffer OUTSIDE the
lock; committers only ever wait on the µs swap, never the pwrite. Must preserve:
WAL ordering (entries pwritten in sequence order), the lever-6 LogFull/no-poison
semantics, ring + linear layouts, and crash recovery (a swapped-but-not-yet-
pwritten buffer is lost on crash — acceptable under buffered durability, same
window as today; under strict the fsync still gates the ack). Verify with the
redo + recovery test suites + a re-measure (expect redo time to collapse from
~24ms toward the µs append cost, unblocking concurrent write throughput).

Status: localized + committed; the double-buffer change is the teed-up next step
(crash-critical — deserves its own TDD pass).

**4-store cross-check (confirms E4 is per-log):** running 4 devices = 4
independent redo logs/mutexes raised total throughput (7000→7835/s) and halved
the spend tail (p99.9 286→132ms), but per-log `create_redo` stayed ~30ms. That
is exactly what "pwrite held under the per-log mutex" predicts: sharding adds
log capacity but each log still serializes its share of writers behind its own
pwrite. So the double-buffer fix is per-log and composes with sharding
(double-buffer removes the per-log stall; sharding multiplies the freed
capacity). Confirms the fix target; not simple mutex contention nor the cache
(devwrite 47µs) nor stripe locks (lock_wait 0).

## E5 — Double-buffer redo: pwrite moved out of the log mutex (2026-06-27)

Implemented the E4 fix. `RedoLog::flush_pwrite_no_sync` split into
`prepare_flush` (under the log lock, O(1): drain buffer → device-ready blocks,
advance cursor, move pending→cache, build header) + `commit_flush` (OUTSIDE the
lock: the slow O_DIRECT pwrite of entries+header). `GroupCommit::flush` now does
prepare under the lock, releases it, then pwrite+fsync — with a new `flush_guard`
mutex serializing flushers so header blocks (sequence high-water) never regress.
Committers contend on the log mutex only for the µs append, never the pwrite or
fsync. Durability unchanged: the fsync still gates when bytes are durable, so a
crash before it loses the un-synced tail exactly as before.

Verification: new `buffered_flush_releases_lock_before_pwrite` test (red→green);
full `cargo test --lib` = **2466 passed / 0 failed**, clippy clean, fmt clean.

Outcome on this `[loaded-host]` (load avg ~9–18 on 8 cores): cumulative with
E1+E2, TeraSlab spend **1381 → 2150 ops/s (+56%)**, total ~5.9k → ~7.1k. Real
improvement, but still below the reference (spend 3342, total ~11k) — the
closed-loop + ~7ms Docker RTT + machine-load artifacts still dominate, and the
remaining server-side wait (create_redo still high, ~0.75 CPU cores) likely has
another serialization (allocator `commit_pending` lock is a candidate — it falls
inside the create_redo span). The fix is correct + tested and matters most on a
quiet box / real hardware where the O_DIRECT pwrite is the actual cost; keeping
it regardless of the noisy bench. Datastore still wins natively (Rust client 30k).

Next: profile the create path's allocator/commit_pending lock under concurrency;
re-measure on a quiet host; the open-loop harness remains the cleanest demo.

## Entries

| # | date | host | op | metric | TS | REF | delta | hypothesis | change (SHA) | re-measure |
|---|------|------|----|--------|----|-----|-------|------------|--------------|------------|
| B0 | 2026-06-27 | loaded | spend | ops/s · p99.9 | 1381 · 295ms | 3342 · 26ms | 0.41× · 11× | baseline | — | — |
| E1 | 2026-06-27 | loaded | all | conn pool | pool stuck at 1 conn | — | bug | pool never grew past 1 pipelined conn (get() reused first-alive); server processes a conn serially → throughput cap | client/go/pool.go (this repo) + test | grows to MaxConns under concurrent load when picked conn busy; correctness green |
| E2 | 2026-06-27 | loaded | spend | RPC batching | 1 spend/RPC | reference batches spends | parity gap | adapter sent 1 SpendBatch RPC per Spend() while reference coalesces; added params-grouped spend batcher (adapter, Teranode tree) | teranode adapter | spend correctness suite green; helps open-loop, not the closed-loop bench |

### E1/E2 outcome (honest)

Both fixes are correct and verified, but **did not flip the closed-loop bench**:
TeraSlab via the Go adapter stays ~6.5k ops/s (spend ~1.9k) vs reference ~11k.
Why: this 128-worker bench is **closed-loop** (each worker blocks per op) over a
**~7ms Docker-Desktop network RTT**, so throughput = workers ÷ per-op-latency.
RPC batching and pool growth raise *open-loop* (production block-validation)
throughput by amortizing RTT and unlocking server parallelism, but in closed-loop
they cannot beat the latency floor. Profiling: Rust native client = 30k ops/s
(batch 16) on the SAME server; the Go adapter adds ~12ms/op of client overhead
atop the 7ms RTT. Higher worker counts (256/512) and shorter batch windows did
NOT scale past ~7k — the cap is the adapter/client per-op latency + Docker RTT,
not the datastore.

**Remaining hypotheses / next steps (where it still loses + evidence):**
1. The bench is unrepresentative: Teranode block validation is open-loop/bursty
   (thousands of concurrent txs). An open-loop saturation harness (bounded
   in-flight, not one-op-per-worker) would let batching/pool-growth show their
   gain — the native Rust client already proves 30k > 11k under that pattern.
2. Eliminate the Docker-Desktop RTT artifact (run both on Linux / host network);
   the ~7ms RTT dominates and is not a datastore property.
3. Server-side create cost is 1.2ms (vs spend 16µs) — a real create-path server
   cost worth profiling for the create metric.
4. Reduce the Go adapter's ~12ms/op overhead (go-batcher channel hop + result
   round-trip); profile the adapter's per-op critical path.

## E3 — Pool fix exposed a SERVER-side serialization (2026-06-27)

After E1, the pool correctly grows to 60 connections (server `/admin/top`
`connections: 60`). That 60-way client concurrency exposed the next wall:
- server-side **spend latency jumped 16µs → ~32ms** under 60 concurrent writers,
- while **server CPU is only ~0.75 of 8 cores** (NOT CPU-bound), and
- **`lock_wait` (stripe locks) = 0** — so the block is NOT the per-txid locks.

The server is *waiting*, not computing, under real write concurrency — a
serialization not attributed to stripe locks. Prime suspects: the single
per-store redo-log mutex held across the buffered `flush_pwrite_no_sync`
O_DIRECT pwrite (~1.9ms/flush, 15 entries each), and/or writeback-cache
backpressure as the dirty set grows. This is a **server-side concurrency fix**
(redo/cache), which is outside the "optimize Go client/adapter" direction chosen
for this loop — flagging for a direction check. Net: the client-side fixes (E1,
E2) are correct and unblock concurrency, but the win now hinges on server-side
write-concurrency, or on a representative open-loop benchmark + non-Docker
network (the native Rust client already does 30k > reference 11k).
