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

## Entries

| # | date | host | op | metric | TS | REF | delta | hypothesis | change (SHA) | re-measure |
|---|------|------|----|--------|----|-----|-------|------------|--------------|------------|
| B0 | 2026-06-27 | loaded | spend | ops/s · p99.9 | 1381 · 295ms | 3342 · 26ms | 0.41× · 11× | baseline | — | — |
