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

## E6 — Open-loop harness: TeraSlab does NOT scale with concurrency (2026-06-27)

Built an open-loop saturation mode (`OPEN_LOOP=1`, `IN_FLIGHT` semaphore + a
dispatcher pool firing ops as goroutines, shared spendable/minted pools) — the
realistic Teranode block-validation pattern (bursty, many concurrent txs), vs the
closed-loop one-op-per-worker model. Swept IN_FLIGHT, `[loaded-host]`:

| IN_FLIGHT | TeraSlab total · spend | Aerospike total · spend |
|-----------|------------------------|-------------------------|
| 256  | 5,371 · 1,607 (p99.9 317ms) | 21,977 · 6,596 (p99.9 55ms) |
| 512  | 6,402 · 1,918 (p99.9 428ms) | 39,359 · 11,800 (p99.9 34ms) |
| 1024 | 4,972 · 1,481 (p99.9 **1256ms**) | 44,336 · 13,295 (p99.9 269ms) |

**The reference scales to ~44k ops/s; TeraSlab peaks ~6.4k then COLLAPSES** at
IN_FLIGHT=1024 (throughput drops, p99.9 blows to 1.26s — classic queue overload
past a ~6k ceiling). This is the honest, important correction to P1's framing:
- The earlier "datastore wins natively, 30k" was the **Rust client with
  `batch16`** (16 items/RPC). At **1 item/RPC concurrency** — how both backends
  run through Teranode's per-op `utxo.Store` calls here — the TeraSlab **server**
  caps at ~6–9k RPCs/s regardless of client (Rust batch1/64c ≈ 8.8k too), while
  the reference server sustains 44k. So the gap is a real **server-side
  concurrent-write scaling deficit (~7×)**, not a client/adapter artifact and not
  a benchmark artifact.
- The double-buffer (E5) helped but did not close it; `create_redo` stayed high
  under concurrency → there is more serialization on the write path beyond the
  pwrite (candidates: committer contention on the single per-store redo mutex
  even for the µs append at 60+ concurrency; the allocator `commit_pending` lock
  inside the create span; writeback interaction). Sharding to 4 stores (E4)
  raised the ceiling but each store still collapses under its share.

**Verdict:** TeraSlab loses the head-to-head, and open-loop shows it loses *more*
than the closed-loop bench implied. The pass condition is NOT met. The remaining
work is fundamental server-side write-concurrency engineering (make a single
store sustain tens of thousands of concurrent 1-item write RPCs/s), not
client/harness tuning. This is the honest stopping point for the profiled
client/adapter direction; closing the gap needs a server concurrency redesign.

## E7 — Audit: the redo mutex lock-WAIT is the concurrency cap (2026-06-27)

Added `teraslab_redo_commit_lock_wait_ns` (time a buffered commit waits to acquire
the per-store redo mutex, excluding the in-lock append). Open-loop IN_FLIGHT=512:

```
redo_commit_lock_wait : 72793 us   ← waiting to ACQUIRE the redo mutex
create_redo           : 78160 us   (= 72ms wait + ~6ms in-lock work)
create_devwrite       :   150 us   (cache write — fine)
redo_flush (fsync)    :  3136 us   (background, off the ack path)
spend (server)        : 95610 us   (same lock-wait domination)
```

So the single per-store redo mutex serializes all committers at ~6k commits/s,
each holding the lock ~167µs. The hold is NOT I/O (double-buffer moved the pwrite
out; fsync is background) — it is **serialization + heap allocation under the
lock**: `append_atomic` serializes every op TWICE (once to measure length for the
lever-6 capacity pre-check, once in `append`), each `serialize()` heap-allocates,
and the create op carries the full record bytes — all under the lock, with the
global allocator contended across 512 threads. That ~167µs × the serialized
queue = the 72ms wait = the ~6k/s ceiling.

**Redesign step 1 (lowest-risk, highest-leverage): serialize OUTSIDE the lock.**
Pre-serialize each entry with a placeholder sequence before taking the mutex;
under the mutex do only the O(1) work — capacity check, draw sequence(s), patch
the 8-byte sequence field into the pre-serialized bytes, memcpy into the buffer,
record pending. This should cut the in-lock hold ~10×+ (no alloc/encode under the
lock), multiplying mutex throughput. Later steps if needed: shard the redo append
per-core / lock-free reserve-then-write ring. Audit-before-fix complete; the
mutex hold-time is the proven target.

## E8 — Redesign step 1: serialize outside the redo lock (2026-06-27)

Implemented the E7 fix. `RedoEntry::pre_encode` encodes the op payload (the
expensive op-encode + heap alloc) OUTSIDE the redo mutex with a placeholder
sequence; `RedoLog::append_preencoded_atomic` finalizes under the lock with only
O(1)/no-alloc work — patch the real sequence over the placeholder, CRC the
payload, frame the length, `extend` the buffer. Buffered commit path routes
through it (ring layout falls back to `append_atomic`). All-or-nothing LogFull /
no-poison semantics preserved.

Verification: full `cargo test --lib` = **2466 passed / 0 failed**, clippy + fmt
clean (recovery round-trips entries written via the pre-encoded path).

Effect (open-loop IF=512, same `[loaded-host]`): `redo_commit_lock_wait`
**72.8ms → 51.3ms** (mutex throughput ~+40%), `create_redo` 78ms → 55ms. The
targeted bottleneck improved, but **end-to-end throughput barely moved** (~5.7k).

Why the ceiling persists — diagnosis sharpened:
- The redo mutex now handles ~10k commits/s but the server only offers ~4.5k
  write-commits/s, so the mutex is no longer saturated — yet lock-wait is still
  51ms. That residual is **scheduler delay, not contention**: TeraSlab uses
  **one OS thread per connection** (60 conn-threads here) on a host at load
  9–18 (oversubscribed 8 cores), so threads park/wake late and every lock
  acquire eats scheduler latency. The reference's event-loop/thread-pool model
  is far less sensitive to oversubscription — a large part of the head-to-head
  gap on THIS box is the threading model × loaded host, not the redo path.

**Next architectural lever (big): thread-per-connection → async / bounded
thread-pool dispatch**, so N connections don't map to N OS threads thrashing a
small (loaded) core count. Plus: re-measure on a quiet host to separate the
threading-model effect from real serialization. The pre-encode fix is correct +
tested and strictly improves the redo path; keeping it.

## E9 — More threads do NOT help; the redo mutex is the serialization (2026-06-27)

Two corrections to the prior diagnosis, both empirical:

1. **The whole benchmark ran with the server's own concurrency feature OFF.**
   `pipeline_depth` defaults to **1** (strict per-connection serial path) and the
   bench config never set it — so the bounded `DispatchPool` (the mechanism that
   decouples request processing from connection count, sized `cores×8`) was never
   created. A methodology gap: TeraSlab was under-configured.

2. **Enabling it does not help — proving the cap is serialization, not threads.**
   `pipeline_depth=16` (DispatchPool active, 64 workers) open-loop: TOTAL ~5.0k
   (IF=256) / ~4.9k (IF=512) — *no better* than the serial path (~6.4k), slightly
   worse. Under the pool: `redo_commit_lock_wait = 58ms ≈ create_redo 58ms` at
   **0.74 of 8 CPU cores** — 64 worker threads parked waiting on the single
   per-store redo mutex. Adding threads just lengthens that queue.

**Conclusion (answers "would tokio help"): NO.** The server already has the
concurrency mechanism async would provide (a bounded pool decoupling processing
from connections); turning it on adds threads that all serialize on the SAME redo
mutex. The bottleneck is **lock serialization on the write path**, not I/O
multiplexing or thread count, and the storage path is deliberately synchronous
O_DIRECT (io_uring removed by design) — so async-over-blocking-I/O would be an
anti-pattern, not a fix. This supersedes E8's "async dispatch" next-lever note.

**Real lever (continue E7/E8): make the redo append lock-light.** Options, in
rough order: (a) finish the double-buffer — `prepare_flush` still COPIES the
buffer under the lock; `mem::take` the buffer O(1) under the lock and build the
device blocks outside it; (b) shard the redo append per-core / lock-free
reserve-then-write ring so concurrent committers don't serialize on one mutex;
(c) right-size the DispatchPool (cores×8=64 is too many for a lock-bound workload)
and re-measure on a QUIET host (the load-9–18 box inflates every park/wake).

## E10 — Double-buffer refinement: swap the buffer, don't copy under the lock

`prepare_flush` no longer allocates the aligned device block, copies the entries
buffer into it, or does the partial-block read-back UNDER the log lock. It now
swaps the buffer out in O(1) (`mem::replace` with a capacity-preserving fresh
buffer — one untouched `with_capacity` alloc, no memcpy) and returns the raw
bytes; `commit_flush` builds the aligned block (alloc + copy + rare read-back)
OFF the lock. `cargo test --lib` = **2466 passed / 0 failed**, clippy + fmt clean.

Effect (open-loop IF=512, on a MORE loaded host — load 21 vs 9–18 earlier, so
conservative): `redo_commit_lock_wait` **51 → 40.6ms**; cumulative across the
three redo fixes (E5 double-buffer, E8 pre-encode, E10 buffer-swap) the wait is
**72 → 40.6ms (−44%)** and the in-lock work fell **~6ms → ~0.4ms** (and most of
that 0.4ms is holder-preemption on the loaded box, not real work). The redo
critical section is now essentially empty — there is nothing left to shave inside
it.

**What remains (structural, not a tuning knob):** with in-lock work ~0, the 40ms
is **pure serialization × concurrency** — ALL writes still pass through ONE
per-store redo mutex, so wait ≈ (concurrent writers) × (tiny hold) + loaded-host
lock-convoy. Throughput stays ~6.9k vs the reference's ~44k. The only way past it
is to STOP funnelling every writer through one mutex:
- **shard the redo append into K sub-logs** (per-core / by txid-stripe), each with
  its own mutex+buffer, merged by sequence on recovery → wait ≈ (writers/K)×hold; or
- a **lock-free reserve-then-write ring** (atomic `fetch_add` for the slot, write
  payload without a mutex).
Plus a **quiet-host** run to strip the convoy confound (load-21 inflates every
measured hold/wait). These are the next dedicated steps; the redo-path
micro-optimizations (E5/E8/E10) are now exhausted.

## E11 — Sharding exploration: redo sharding helps 2×, but the write path is a CHAIN of per-store mutexes (2026-06-27)

Same-host, same-time reference number this session: **37.8k ops/s** (so the host
is NOT the limiter — TeraSlab's cap is its own serialization, as the user noted).

Store-count sweep (multi-store = redo sharding at store granularity; open-loop
IF=512, buffered, load ~6):

| config | total ops/s | redo lock-wait |
|--------|-------------|----------------|
| reference | 37,769 | — |
| TS 1-store | 3,974 | 67.3ms |
| TS 4-store | **7,772** (~2×) | 25.0ms |
| TS 8-store | 7,297 (flat) | 20.4ms |

- **Redo sharding genuinely helps**: 1→4 stores ≈ 2×, redo lock-wait 67→25ms.
  Confirms the redo mutex is a real, dominant bottleneck — sharding it is worth
  building.
- **But it plateaus at ~7.7k** (8 stores ≤ 4; adding request concurrency —
  pipe16 + 256 conns — made it *worse*, 4.4k, p99.9 1.5s, as more writers
  re-contend the shards). And the 4-store saturation profile shows why it is not
  just the redo mutex:

```
CPU = 0.98 of 8 cores (88% idle — pure lock serialization, never compute)
create = 39ms  (redo 37ms  devwrite 0.45ms  reserve 0.02ms)
  └ of redo 37ms: lock-wait 19ms + ~18ms NON-lock-wait
spend  = 44ms
```

**The write path is a chain of per-store mutexes, each serializing concurrent
writers; sharding the redo fixes only the first link:**
1. **Redo log mutex** — the 19ms lock-wait. Sharding addresses this (✓ 2×).
2. **Allocator mutex** (`engine.rs:170`, one `Mutex<SlotAllocator>`/store) — the
   ~18ms in `create_redo`: create's `pending_allocs` → `allocator_for(d).lock()
   .commit_pending()` serializes all creates to a store.
3. **Spend takes the redo lock TWICE** — the spend op + a second
   `commit_dah_batch` redo commit (the DAH delete-at-height update).

**Verdict:** sharding the redo log is necessary and worth ~2×, but NOT sufficient
to reach the reference's ~38–44k — the allocator commit and spend's double-redo
then bind at ~7.7k, and CPU is 88% idle throughout (the reference's event-loop +
lock-light engine is the structural difference). Closing the gap is a
**write-path lock-decontention campaign**, not one change. Recommended sequence:
(1) shard the redo append (in-store K sub-logs, lower overhead than multi-store —
shares one cache/index/allocator/checkpoint); (2) decontend the allocator
`commit_pending` (shard the freelist / commit outside the lock); (3) fold spend's
DAH update into its single redo commit so spend takes the redo lock once. Each is
a contained change; together they unwind the chain.

## E12 — ROOT CAUSE (subagent profile): redo fsync GRANULARITY, not locks/sharding

Decisive experiment — RAM-back the device (tmpfs) and re-measure:

| config | backing | total ops/s | server CPU |
|--------|---------|-------------|-----------|
| TS 1-store | disk  | 7,544  | ~0.70 core (91% idle) |
| TS 1-store | **tmpfs** | **38,618** | ~2.1 cores |
| TS 4-store | disk  | 8,032  | ~1.0 core |
| TS 4-store | **tmpfs** | **34,719** | ~2.3 cores |
| reference  | disk  | ~37,800 | — |

**RAM-backing gives 5×, and RAM-resident TeraSlab (~38.6k) MATCHES/BEATS the
reference (~37.8k).** So the engine is NOT the bottleneck — the **physical-device
sync pattern** is. This is the global ceiling that survived 8-way sharding: all
stores' flushers sync to one volume and split one fixed fsync budget.

Device-I/O pattern at the cap: TeraSlab issues **~600 fsyncs/sec of ~5.3 KiB
(~16 entries) each**, and the flusher spends **87% of wall-clock inside fsync**
(per-fsync 1.44ms @1-store, rising to 4.78ms @4-store = the shared volume
saturating). The reference funnels writes through a 128 MiB post-write-cache
flushed in **128 KiB** chunks → ~24× fewer, ~24× larger device ops on the SAME
disk. That granularity gap is the entire story.

**Crucial corrections to the prior direction:**
- The `redo_commit_lock_wait` (41ms) I optimized in E7/E8/E10 is a **downstream
  symptom**: committers queue on the redo mutex *because the flusher behind it is
  stuck in fsync*. On tmpfs it drops to 0.05ms with NO code change. The redo-mutex
  micro-opts (pre-encode, buffer-swap) are correct hygiene but did not and could
  not move the disk-bound needle.
- **Sharding the redo (E11) does not help** the real bottleneck: more shards = more
  flushers fighting the same fixed device-fsync budget (4.78ms/fsync @4-store
  proves contention). Do NOT build redo sharding for this.
- `lock_wait` (stripe) = 0 throughout; the record data-write is 0.11ms (0.3% of
  create). It is exclusively the redo *journal* fsync.

**Open mechanism (the precise fix target):** the flushes are append-driven —
`entries_per_flush ≈ 16`, NOT the 50ms background interval (raising the interval
to 2000ms barely changed the rate). So a per-~block trigger fsyncs ~600×/s, and I
have not yet pinned the exact call site in buffered mode (the buffered commit
appends without flushing; the background flusher is 20/s; the 580/s remainder is
unlocated). Pinning it (a 5-min instrument-and-rebuild) is step 1 of the fix.

**THE LEVER: coalesce redo fsyncs — group-commit the flush into far fewer, far
larger device syncs** (target ~128 KiB / hundreds of entries per fsync, like the
reference). Buffered-compatible, contained, and the tmpfs ceiling shows the payoff
is **~5× → ~35–40k, at/above the reference on the same disk.** Sequenced after
that: decontend the allocator (E11) and fold spend's double-redo — but fsync
coalescing is THE win.

**Caveat (subagent):** this is Docker-on-macOS; fsync goes through virtualization
(~1.4ms, slow). On bare-metal NVMe the absolute gap shrinks, but the relative
finding — TeraSlab issues ~24× more, ~24× smaller device ops than the reference —
is hardware-independent and is the real lever.

## E13 — FIX LANDED: defer the secondary-index fsync in buffered mode → ~3× (2026-06-28)

Pinned + fixed the E12 root cause (subagent, instrumented backtrace). The ~600
fsync/s was **`setMined`'s two-phase secondary-index durability**:
`update_both_secondary_indexes` / `sync_primary_and_both_secondary_atomic` called
`RedoLog::append_batch_and_flush` — an **unconditional fsync per key** — bypassing
buffered mode (the buffered flag lived only on the `GroupCommit` committers, not
these engine-internal redo paths). setMined is 10% of ops but did 1–2 fsyncs each
→ ~600/s, serializing the whole pipeline.

Fix (`src/ops/engine.rs`): engine-level `redo_buffered` flag + `journal_secondary_ops`
helper — buffered → `append_atomic` (append-only, durability deferred to the
background flusher/checkpoint, coalesced); strict → `append_batch_and_flush`
(unchanged). Durability preserved: secondaries are rebuilt on recovery from the
authoritative primary metadata, and the primary write is itself buffered → primary
+ intent + redb share one flush boundary (consistent prefix on crash). lever-6
LogFull (transient/no-poison) preserved; strict byte-identical. New test
`buffered_secondary_index_updates_coalesce_fsyncs`. **Independently re-verified:
`cargo test --lib` 2467 passed / 0 failed, clippy `-D warnings` clean, fmt clean.**
Committed.

Effect (open-loop, 1-store, disk, buffered+writeback):

| metric | before fix | after fix |
|--------|-----------|-----------|
| total ops/s (IF=256, failed=0, 256MiB redo) | ~7,500 | **~22,473** (~3×) |
| fsyncs/sec | ~437 | ~8.7 |
| entries/flush | ~16 | **~2,445** |
| CPU | ~0.5 core | ~0.5 core (still idle) |

The dominant fsync bottleneck is GONE (entries/flush 16→2445). TeraSlab on disk
went **~7.5k → ~22–24k** (the subagent saw 24.4k at IF=512 with the old 64 MiB
log + transient LogFull; the clean number with a 256 MiB log is ~22.5k, failed=0).
That is **~3× and ~60% of the reference's ~37.8k** (was ~20%). Config: bumped the
matched config's `redo_log_size` 64→256 MiB (the old log backpressured at the new
rate — a fair bump vs the reference's 4 GiB file + 128 MiB cache).

**Remaining gap to the reference (and now-relevant levers):**
- At high concurrency (IF=512) throughput dips and CPU stays idle → the **next
  bottleneck is lock contention** (the redo mutex — now UNMASKED, which is what
  E7/E8/E10's pre-encode + buffer-swap actually address — and the per-store
  allocator `commit_pending`, E11). These were masked by the fsync before; they
  now bind.
- Colder secondary paths still fsync per-op in buffered mode (subagent follow-up
  #1: `update_dah_index`/`update_unmined_index` single-secondary + conflicting/
  deleted-child intents) — not the bottleneck here (0 samples) but share the bug;
  fixing needs a `buffered` flag threaded through the index-backend `insert/remove`
  signatures (~60 call sites).
- The tmpfs ceiling (~38.6k) = the engine's true capacity = matches the reference,
  so the headroom to close the last ~40% is real and lock-bound, not I/O-bound.

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
