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
   - the adapter's connection pool defaults to **16 conns** (vs the reference's Go
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

| IN_FLIGHT | TeraSlab total · spend | Reference total · spend |
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

## E14 — REAL binder found: the single `Mutex<UnminedBackend>` on create (2026-06-28)

RESUME's "next levers" (redo mutex, allocator lock) were WRONG. Profiled the
post-E13 write path properly (subagent, fresh-container IF sweep + create-path
latency decomposition). Findings:
- `redo_commit_lock_wait` 7→19→87µs (<0.3% of create latency) — NOT the redo mutex.
- `create_reserve` (allocator) 18→48µs (flat, ~0.2%) — NOT the allocator.
- **`create_index` 754µs→1.7ms→9ms→22ms as IF doubles** — 78–87% of create
  latency, super-linear = lock-queue signature. Device-independent (tmpfs barely
  moves it) → lock-bound, not I/O-bound. CPU idle → decontention raises the
  ceiling. Peak disk was actually **32.7k @ IF=512 (87% of the reference's
  ~37.8k)**, higher than RESUME's IF=256-only 22.5k.

Two root causes inside `create_index`, found by reading the code:

1. **Fresh-rebuild index under-sizing → resize storm (FIXED, committed).** On a
   wiped volume `ShardedIndex::rebuild_in_memory` sized each of 256 shards to the
   *scanned count* (≈0) → ~183 buckets/shard; steady-state inserts then resized
   repeatedly UNDER the per-shard write guard (`engine.rs:2291`). Fix: thread
   `expected_records` through the rebuild → `basis = count.max(expected_records)`
   (sharded.rs/startup.rs/server.rs). TDD: `rebuild_in_memory_presizes_to_
   expected_records_on_empty_device` (capacity 65536→≥2M). `cargo test --all`
   green, clippy `--all -D warnings` + fmt clean. 521 MiB idle RSS confirms it's
   active. **Correct + verified, but A/B showed it is NOT the dominant binder
   here** (create_index stayed super-linear with resizes eliminated). It still
   matters for a fresh production node under create load; kept.

2. **The single `Mutex<UnminedBackend>` (THE binder, A/B-PROVEN).** The bench
   creates at `height := 1` (driver), so every create hits
   `if meta.unmined_since != 0 → update_unmined_index → self.unmined_index.lock()`
   — ONE global Mutex, all creates funnelling into `by_height[1]`'s single Vec,
   contended by every parallel register thread (`thread::scope`).
   Decisive interleaved A/B (env-gated toggle to skip the create unmined insert,
   3 pairs @ IF=512, same host/window so noise cancels; toggle reverted, never
   committed): **skipping it → total +21% (27.5k→33.4k median), create_index
   −77% (11.4ms→2.6ms), every DIAG run > every CONTROL run.** Toggle verified
   (unmined_index_entries 0 vs 158–189k). Caveat: the toggle *drops* work
   (failures appear under the higher offered rate); the real fix must SHARD the
   index — preserve every insert, failed=0.

**THE NEXT FIX:** shard the secondary indexes by txkey like the primary
`ShardedIndex` (generic `ShardedSecondary<B>`, 256 shards, in-memory variant
only; cold-path range queries fan-out+concat — order doesn't matter, pruner
re-validates). Ship UNMINED first (create 40% hits it), measure, then DAH (spend
30% + setMined 10% hit it). No on-disk/redo/recovery changes (pure in-RAM
layout; recovery rebuilds from primary metadata regardless). Standing this noisy
session: CONTROL ~27.5k @ IF=512 vs reference ~37.8k; unmined-skip ceiling ~33k;
tmpfs ~34k. Closing the gap = unmined + DAH sharding.

## E15 — sharding + redo-ring landed; now ~96% at 15s, blocker = global tail (2026-06-28)

Secondary sharding (E14 fix) committed (f28a96c): create_index 9-22ms → ~3.6ms.
Head-to-head then exposed TWO new things, both diagnosed:
1. **Checkpoint stop-the-world freeze.** No server-side load shedding (all
   *_failed/inflight_rejected = 0); the "failures" were client RPC timeouts during
   a ~10s BLOCKING redo checkpoint (`checkpoint.rs:540-573` holds the exclusive
   dispatch barrier across the full snapshot+compaction). The fuzzy (non-blocking)
   checkpoint can't reclaim as fast as 512 writers append → escalates to blocking.
   Config tuning (1GiB redo, high_water 0.5) only DELAYED it (60s run still froze
   3.3s) — a rate mismatch, not sizing.
   **FIX (config, no rebuild): enable the segment-RING redo layout**
   (`redo_segment_ring=true` — Lever 7, already implemented+tested, default-off
   pending soak). Reclamation = O(freed) pointer advance, independent of index
   size/append rate; the checkpoint loop skips the blocking path for a ring.
   PROVEN: 60s sustained, `blocking:true`=0, NO op >1.07s, failed=0 — freeze GONE.
   (A one-off 49s FUZZY snapshot is background wall-time, barrier held only µs.)
2. **Standing with the ring (interleaved, IF=512, noisy host):**
   - 15s (ckpt never fires): TS 36.4k vs ref 37.5k = **−3% total / −3% spend
     (near-tie throughput)**, but spend p99.9 **188ms vs 44ms (4.3× worse)**.
   - 30s: TS 33.2k vs ref 36.7k = −10%, throughput DECAYS 36→33k (ckpt=0, so NOT
     the snapshot), p99.9 226ms vs 51ms.
   - Progress: TS went from ~20% → ~96% of the reference at 15s.

**REMAINING BLOCKERS (both needed for the win — spend tput ≥ ref AND p99.9 ≤ ref):**
- **(A) Global tail latency 4-10× worse on EVERY op INCLUDING read-only GET** →
  a single GLOBAL periodic stall in request processing (not write-path-specific;
  GET touches no redo/cache-write). Hard pass-condition blocker. PROFILE THIS NEXT.
- **(B) Sustained throughput decay** 36→33k over 30s with ckpt=0 — something
  degrades over time (dataset growth? writeback dirty-set pressure? not checkpoint).
Config committed: 1GiB redo + checkpoint watermarks + `redo_segment_ring=true` in
bench/configs/teraslab-async.toml (reproducible from clean checkout; flipping the
code default is a separate production change pending soak). Caveat: host too noisy
for certification; all numbers RELATIVE, interleaved.

## E16 — the cap chain resolved; remaining gap is CPU EFFICIENCY (2026-06-28)

Chased the ~33k single-store ceiling (CPU idle at ~210%/800% → a serialization,
not CPU). Ruled out, with measurements: fsync cadence (50→1000ms: 20x fewer
fsyncs, ZERO throughput change), connection/pipeline concurrency
(`pipeline_depth` 1→16 + `max_connections_per_ip` 64→1024 + POOL_SIZE 60→512:
no change, CPU stayed ~210%). **FOUND IT: per-store lock-domain serialization.**
`device_split=4` (4 virtual stores on the one disk = 4 independent redo logs +
allocators + index-lock domains; fair = TeraSlab's analogue of the reference's
internal partition parallelism on one file) **broke the ceiling: ~33k→~44k in
isolation, CPU 210%→~420%.** device_split=8 over-fragments (more CPU, ~35k) — 4
is best.

**But that exposed the REAL, fundamental gap — CPU efficiency:**
- Interleaved head-to-head (split=4): reference ~42k @ IF=512 / ~51k @ IF=1024 at
  only **160-260% CPU**; TeraSlab ~29k (interleaved) / ~44k (isolated) at
  **400-540% CPU**. The reference does MORE ops with ~HALF the CPU → it is
  **~2-3x more CPU-efficient per op** (~51µs CPU/op vs TeraSlab ~95µs/op).
- So TeraSlab is now CPU-BOUND and brittle: under host contention its CPU-heavy
  path degrades far more than the reference's CPU-light one.
- WIN verdict: FAIL on all axes (total −31-40%, spend −30-35%, spend p99.9 1.4-5x
  worse). NOT won.

**Progress: TeraSlab went from ~20% → competitive-in-isolation (~44k vs the
reference's ~51k).** Every architecture/config lever is now resolved (fsync
coalescing E13, secondary sharding E14, redo ring E15 kills the freeze,
device_split E16 kills the lock-domain cap). 

**THE REMAINING LEVER = CPU cycles/op.** This is a different class of work
(profiling-driven micro-optimization of the hot create/spend paths — allocations,
memcpies, CRC, cold-data serialization, protocol encode), NOT a config/architecture
knob. It needs (1) a CPU flamegraph to find the hot paths and (2) a QUIET host to
measure gains. BLOCKER: this shared box has a persistent EXTERNAL load (a `perl`
job pinning ~2 cores) — certification is impossible here regardless. Recommend:
re-run the whole suite on a quiet host (load < 1/core), and start the
CPU-efficiency pass from a flamegraph of the create path at IF=16.

Fair-config note: with device_split=4 the redo buffer is 4x per-store (generous to
TeraSlab); a stricter fair config would scale redo_log_size down so the TOTAL ≈ the
reference's 1024M cache — the conclusion (TeraSlab trails on CPU efficiency) holds
either way, since it trails even with the generous buffer.

## E17 — CPU-efficiency pass: redo ring-encode + index single-probe (2026-06-28)

Started attacking the CPU-cycles/op gap (E16). Profiled via the server's built-in
pprof endpoint (perf unavailable on Docker-macOS): baseline **~157 CPU-µs/op** @
IF=256. Top self-CPU buckets: `Vec::from_iter`/encode **~30%**, index lookups
(`get_entry` + `lookup_checked`) **~32%**, crc ~8%, background writeback ~19%.

Two fixes (committed 56884d3, a669244; `cargo test --all` 2962/0):
1. **redo ring-encode reuse** (`redo.rs`): the segment-ring branch of
   `append_preencoded_atomic` was DISCARDING the off-lock pre-encoded body and
   calling `append_atomic`, which re-clones + re-serializes every op 2-3× UNDER
   the lock (E7's "encode outside the lock" was defeated in ring mode — which has
   been the default since E15). Now it consumes the pre-encoded bodies directly
   (patch seq, CRC once, byte-identical frame, MOVE op into pending_entries).
   Byte-identical + recovery roundtrip proven by tests.
2. **index single-probe** (`hashtable.rs`/`mod.rs`/`backend.rs`/`engine.rs`):
   `register_new_with_shard_count` probed the index TWICE per create
   (`lookup_checked` then `insert`). Fused into one Robin-Hood walk
   (`insert_if_absent`, reject-not-overwrite).

A/B (precpu image = 0329d3a vs bench = both fixes; SAME config; interleaved, 5
rounds, fresh containers, IF=256): **MIN (least-contended) CPU-µs/op −11.8%**
(47.4→41.8); median −0.8% and throughput flat (−0.2%) — host noise (per-round
CPU swung 42-55µs) swamps the median, and at IF=256 the server has idle CPU so a
per-op CPU cut does NOT raise throughput (only converts to throughput when
CPU-bound: high device_split + high IF + spare cores). Best-case −12% is the real
signal; steady-state magnitude + any throughput/head-to-head payoff need a QUIET
host (the box still has the external `perl` load eating ~2 cores).

**Remaining CPU targets (same flamegraph, not yet done):** the `engine.rs:933`
redo-op deep clone (`append_redo_ops_routed` takes `&[RedoOp]` because the caller
reuses ops for replication — needs a caller refactor); pre_encode buffer
pre-sizing (the `with_capacity(...+64)` reallocs for >64B records); the ~19%
background writeback CPU. Stacking these + a quiet host is the path to the
reference's ~51µs/op (and, when CPU-bound with spare cores, to out-scaling its
~51k ceiling via device_split).

## E18 — MILESTONE: TeraSlab beats the reference on throughput + p99 (only p99.9 left) (2026-06-28)

Host quieted (external load killed by the user); pushed the harder CPU refactor:
**Arc-ify `RedoOp::Create.record_bytes`** (Vec→Arc<[u8]>; committed 5d3757a). The
dispatch create path now builds the record ONCE into an Arc and SHARES it
(refcount bump) between the device write and the redo op — the per-create deep
copy (the post-E17 encode hot spot, the `engine.rs:933` clone) is gone. On-disk
byte-identical (golden-bytes + Arc::ptr_eq + recovery tests); cargo test --all
2965/0.

**Decisive head-to-head — FAIR matched config** (device_split=4, redo 256MB/store
≈ 1GiB total ≈ reference 1024M cache, ring, buffered), 5 interleaved rounds @
IF=512, **0 failed both sides**, host load ~2-4:
| metric @ IF=512 | TeraSlab | reference | verdict |
|---|---|---|---|
| spend ops/s (median) | **14,095** | 13,013 | **TS +8.3% — PASS** (margin ~23σ; TS stdev 24) |
| total ops/s | **47,014** | 43,453 | **TS +8.2%** |
| spend p99 | **19,352µs** | 20,086µs | **TS wins + tighter** |
| spend p99.9 | 28,980µs | **25,987µs** | TS +11.5% — **FAIL (only gate missed)** |
| create/get/setMined ops/s | +8% each | — | guardrail PASS (TS faster on every op) |

**TeraSlab is now the FASTER store on this workload** (throughput + p99 + every op,
0 failed, huge margin). The strict 4/4 pass condition misses ONLY on spend p99.9,
and that is NOISE-DRIVEN: TS had the BETTER p99.9 in 2/5 rounds; round 3 was a
host-wide spike hitting both (TS 102ms, REF 58ms). The miss = rare ~100ms spend
outliers (occasional checkpoint-snapshot / writeback / GC stall) vs the
reference's smoother background work.

At IF=1024 TS is past its knee (spend p99 144-168ms) and loses tput −8.5% — the
reference scales better there; **IF=512 is TeraSlab's operating point and where it
wins.**

**LAST GATE = the rare spend p99.9 spike.** Next: diagnose the periodic background
event causing the ~100ms outliers (fuzzy checkpoint index-snapshot is the prime
suspect — at 256MB/store it fires ~once/25s and the full-index serialize competes;
also writeback flush / tombstone GC), then smooth/incremental-ize it. Everything
else already passes comfortably, so closing the p99.9 tail = a certifiable win.

## E19 — p99.9 tail pinned to a macOS-Docker O_DIRECT VM-freeze (host artifact, not a TeraSlab flaw) (2026-06-28)

Chased the last gate (spend p99.9). Two diagnostics REFUTED the obvious suspects:
- **Not the checkpoint.** A/B redo 256MB vs 2GB/store: checkpoint NEVER fired in
  either (only ~125MB/store written in 20s, below high-water). Spike present in
  both arms.
- **Not the redo flush per se / not a write-path issue.** The spike hits ALL ops
  INCLUDING read-only GET *in lockstep* (e.g. create 155 / spend 154 / **get
  134** / setMined 144 ms in one round) — the whole macOS-Docker VM freezes.
  Server-side `spend_latency` p99.9 stays ~8ms; only `redo_flush_latency` tracks
  it. Root cause: TeraSlab's **O_DIRECT redo-flush fsync occasionally freezes the
  Docker-for-Mac VM's virtualized I/O for ~100ms**, pausing every thread. The
  reference avoids it by using buffered/page-cache I/O (post-write-cache, async
  writeback) which the VM absorbs smoothly; its only tail is a *structural* ~200ms
  on setMined (its async commit) — which **TeraSlab beats** (~24ms).

Flush-cadence is NOT the lever: tested `redo_flush_interval_ms` ∈ {1000, 50, 20}.
1000ms = WORSE (bigger fsync → deeper VM freeze, median p99.9 ~65ms). 50ms ~29ms.
20ms ~28ms. None reach the reference's ~24-26ms; there is a consistent ~3ms
baseline gap (O_DIRECT flush vs buffered) PLUS intermittent VM-freeze spikes.
Left config at 50ms (best throughput margin; p99.9 misses either way).

**FINAL STANDING (fair config, quiet host, 0 failed both sides, IF=512, the
operating point):**
- Spend throughput: TS **+8.3%** (PASS, margin ~23σ). Total **+8.2%**.
- Spend p99: TS **wins** (19.4 vs 20.1ms) and tighter. create/get/setMined ops/s:
  TS **+8% each** (guardrail PASS). setMined p99.9: TS **wins** (24 vs 200ms).
- Spend p99.9: TS ~28-29 vs ref ~24-26ms = **+13% (FAIL — the lone gate)**, and
  it is a **macOS-Docker O_DIRECT VM-freeze artifact** (freezes reads too), NOT a
  TeraSlab logic/design flaw. On Linux/NVMe (the real Teranode target) O_DIRECT
  fsync is ~0.1ms with no VM freeze, so TS p99.9 → ~its p99 (~19ms) ≤ ref → the
  strict 4/4 passes there.

**Conclusion: TeraSlab is the FASTER store on this UTXO workload** — it beats the
reference on Spend throughput, total throughput, Spend p99, every op's throughput,
and setMined tail, decisively and reproducibly, on a fair matched config with zero
dropped requests. The single strict criterion it misses on macOS-Docker (Spend
p99.9, by ~13%) is a host-virtualization fsync artifact, not a property of
TeraSlab. The committed CPU work that got here: E13 fsync-coalescing, E14 secondary
sharding, E15 redo ring, E16 device_split, E17 ring-encode+index-probe, E18+Arc
record_bytes. **To certify the strict 4/4: re-run the same suite on a
Linux/bare-metal NVMe host (load<1/core).** Do NOT abandon/bound O_DIRECT to chase
the macOS tail — O_DIRECT is core to the 10-50× SSD-wear goal and the freeze hits
reads too (so it wouldn't fully fix it) and would regress the real target.

## E20 — BREAKTHROUGH: PRIMARY condition WON on macOS-Docker (supersedes E18-E19) (2026-06-28)

E18-E19 were WRONG that spend p99.9 needs Linux. The fix was a TeraSlab change:
**buffered/async redo WAL** (commit f522556, config `redo_buffered_io=true`). The
redo was O_DIRECT + per-flush fsync; that fsync froze the macOS-Docker VM ~100ms,
adding ~3ms to spend p99.9. Buffered (page-cache) redo + NO per-flush fsync
(durability via the checkpoint barrier, which already fsyncs the redo before
fencing — reclamation-safe; data device stays O_DIRECT) removed the freeze. This
matches the reference's async page-cache write posture (fair). New TDD + targeted
suites green.

**PRIMARY WON** (fair config, disk, IF=512, 8 interleaved rounds, 0 failed both):
| metric | TeraSlab | reference | verdict |
|---|---|---|---|
| **spend ops/s** | **14,184** | 13,114 | **+8.2% PASS** (margin >2σ; TS σ 307) |
| **spend p99.9** | **24.95ms** | 25.39ms | **≤ ref PASS** (fsync tail gone; tie, TS ahead) |
| spend p99 | 18.0ms | 19.9ms | TS wins |
| total ops/s | 47,338 | 43,794 | +8.1% |
| create/get/setMined ops/s | +8% each | — | guardrail PASS |
| get p99.9 / setMined p99.9 | 22.6 / 12.3ms | 23.9 / 200.7ms | TS wins both |
| **create p99.9** | **29.3ms** | 26.1ms | **+12.3% — LONE gate miss (guardrail)** |

So the Spend op (the one to win) PASSES both throughput AND p99.9. The strict 4/4
misses ONLY the create-p99.9 guardrail (+12.3%, >10%).

**create-p99.9 status (IMPORTANT — don't repeat the dead ends):**
- It is NOT the cache read-modify-write preload. That hypothesis was REFUTED: prior
  commits 4a308fd (coalesce writes) + 5a582d1 (RMW-correct) already make non-packed
  create writes block-aligned/whole-block, so `CachingDevice::pwrite` never preloads
  on the create path (verified by an instrumented probe: 0 inner reads). A
  `pwrite_fresh`/no-preload fix would be DEAD CODE — do not build it.
- It is NOT disk-specific: TS create p99.9 is ~29ms on tmpfs too (+6% there, WITHIN
  the 10% guardrail). The ~25ms floor is a shared Docker-VM/open-loop-scheduler
  artifact (both backends have it). The create extra (~3-4ms) is create-path WORK:
  create is the heaviest op AND the bench creates at block_height=1 so each create
  also does the unmined-index insert (sharded) + its redo intent — work spend
  doesn't do. Plus the residual ~2-3× CPU/op gap.
- It is BORDERLINE: +12.3% disk vs +6% tmpfs is within the reference's own
  create-p99.9 run-to-run variance. May be a statistical tie (not significantly
  worse) → needs more rounds + stdevs to classify.

**THE FAIR WIN CONFIG (bench/configs/teraslab-async.toml, committed):**
device_split=4, redo_log_size=256MB/store (~1GiB total ≈ ref 1024M cache),
redo_buffered=true, redo_buffered_io=true, redo_flush_interval_ms=50,
[cache] writeback=true 4GiB, [index] memory. IF=512 is the operating point
(IF=1024 is past the knee for both — reference scales higher there, TS loses).

**REMAINING WORK for the strict 4/4 (in priority order):**
1. **Settle create-p99.9**: a rigorous 10-round interleaved run reporting per-op
   p99.9 medians + STDEVs. If TS create p99.9 is within ~2σ of ref (statistically
   not-worse) OR median ≤ +10% → guardrail satisfied → 4/4 (within precision). If
   solidly >10% → reduce create-path work (the unmined-insert + its redo intent on
   create is the create-specific extra; the unmined index is rebuildable from
   primary metadata on recovery, so the per-create unmined REDO INTENT may be
   droppable/coalescable — investigate).
2. **De-flake the gate**: `redo_group::tests::concurrent_commits_coalesce_and_get_
   distinct_ranges` is load-flaky (asserts fsyncs<N coalescing; under full --all
   parallel load fewer overlap → more fsyncs → fails). Passes 5/5 isolated. It is
   the STRICT-path GroupCommit (orthogonal to all my buffered-async work). Fix:
   make the coalescing assertion DETERMINISTIC (control leader/follower timing via
   the existing BlockingSyncDev pattern: block the first flush, queue M followers,
   release, assert they coalesce into 1) — NOT masking, keeps the correctness
   (distinct contiguous ranges) signal. Needed because `cargo test --all` must be
   reliably green for certification.
3. **FINAL_REPORT.md** + final green gate (cargo test --all, clippy --all -D
   warnings, Docker e2e) + grep-clean confirm, then it's a documented win.

The arc: E13 fsync-coalescing → E14 secondary-index sharding → E15 redo segment-ring
(killed the 10s checkpoint freeze) → E16 device_split=4 (lock-domain cap) → E17
ring-encode reuse + index single-probe → E18 Arc record_bytes → E20 buffered/async
redo (closed spend p99.9). Commits ae33334, f28a96c, be6b29b, 0329d3a, 56884d3,
a669244, 5d3757a, f522556 (+ ledger/RESUME commits). cargo test --all 2497-2965
pass + the 1 known load-flake.

## E21 — create-path CPU profile + in-flight CPU work (compaction checkpoint) (2026-06-28)

After E20 (PRIMARY won on a quiet host), the lone strict-4/4 gate is create-p99.9
(+12% on the clean h2h11 run) AND p99.9 fragility under host CPU load. DIAGNOSED:
both are the **CPU-efficiency gap** — TS uses more CPU/op; under CPU starvation
(e.g. GoLand at 99% on a core, which contaminated the h2h12 10-round run → TS spend
p999 49-245ms vs ref's robust 24-37ms) TS queues worse; the leaner reference is
robust. So the lever is reducing TS create-path CPU/op (host-independent). The
clean h2h11 (8 rounds) remains the certification-quality data; h2h12 discarded.

**CREATE-path CPU flamegraph (pprof `GET /debug/pprof/profile?seconds=&frequency=`,
admin-gated; SVG output). % of the create subtree (~34% of server CPU):**
- primary-index register ~21% (Robin-Hood `get_entry` probe + `insert_if_absent`)
  — mostly inherent hash-table probe; hard.
- locks (stripe + shard-write) ~12%.
- record build (`build_create_record_bytes`/TxMetadata/UtxoSlot/cold) ~9%.
- early dup-check `lookup_checked` + a metadata read ~8% (the create path probes the
  index for a dup-check, THEN `register_new_with_shard_count` re-probes — redundant
  for the all-unique-txid workload; the fused insert-if-absent already rejects dups).
- **SipHash on the create-batch dedup `HashSet<[u8;32]>`/`bulk_by_store` map ~5%**
  (std SipHash on already-random txids = pure waste).
- unmined in-mem insert ~5.5%; CRC ~3.5%.
- pwrite + RedoOp::Create redo append ≈ 0% (already coalesced — good).
- **Unmined `SecondaryUnminedUpdate` redo intent = 0% here** (the in-memory unmined
  backend appends NO intent; it's redb-only). The "drop the intent" idea is a no-op
  in this config — DO NOT pursue it.

**IN-FLIGHT WORK — NOW COMMITTED (2026-06-28):** both landed on `feat/device-cache`,
`cargo test --all` green (exit 0), tree clean:
1. **De-flake** `redo_group::tests::concurrent_commits_coalesce_and_get_distinct_
   ranges` → commit `7e2c40e` (`src/redo_group.rs`); now deterministic (asserts
   `fsyncs==2` exactly, strictly stronger than the old `< N`); verified 10/10.
2. **SipHash→fast-hasher swap** for the create-batch dedup → commit `8fd086b`
   (`src/server/fast_hash.rs` + `dispatch.rs` 2-site swap + `mod.rs`); ~5% create
   CPU, correctness-neutral. The "fast_hash load-flaky" scare was a concurrent-
   cargo-on-shared-`target/` artifact, NOT a defect (the two flagged tests are pure/
   deterministic and pass clean single-process + under `cargo test --all`).

**NEXT (after committing the in-flight work):**
1. Optional 2nd create-CPU cut: drop the redundant early dup-check `lookup_checked`
   (~3-8%; rely on the authoritative insert-if-absent reject; risk = dup creates do
   alloc+write+rollback, fine for no-dup workload). Higher risk than the SipHash swap.
2. **Quiet-host 4/4 certification** (THE blocker): needs the box idle (GoLand off /
   load <1/core) OR a Linux/NVMe host. Then a 10-round interleaved run with per-op
   p99.9 + STDEVs to confirm create-p99.9 ≤ +10% (or within-noise) → strict 4/4.
3. `bench/FINAL_REPORT.md` + green gate (`cargo test --all`, `clippy --all -D
   warnings`, Docker e2e) + `git grep -i` opponent-name clean.

Do NOT: abandon/bound O_DIRECT (core design; the data-device O_DIRECT is fine — the
p99.9 issue is CPU starvation, not I/O freeze, in buffered mode). Do NOT pursue the
unmined-intent drop (no-op here).

## E22 — BytesMut realloc fixed (1020125); throughput win SOLID, p99.9 gap likely host-noise (2026-06-28)

Linux/NVMe profile (LINUX_NVME_REPORT.md) → global dispatch funnel (~40-44k; the
DispatchPool single `Mutex<VecDeque>`+Condvar queue, mod.rs:1407) + **31% of on-CPU
wasted in `BytesMut::reserve_inner` memcpy** (pipelined read loop reallocated
~256 KiB/frame). Commit **1020125** fixes the memcpy (reclaim-in-place, 13×/frame;
2982 tests + clippy + fmt + client all green).

Head-to-head re-measure (macOS Docker, fair async config, fixed build, 10 rounds,
clean 1-6+9; host load ~5 — NOT idle):
| op | TS ops/s | REF ops/s | Δ | TS p99.9 | REF p99.9 |
|---|---|---|---|---|---|
| create | 19272±149 | 17854±70 | +7.9% | 31.9±4.2 | 24.8±1.7 |
| spend | 14451±109 | 13382±53 | +8.0% | 29.4±3.7 | 23.9±0.7 |
| get | 9654±76 | 8931±34 | +8.1% | 26.4±5.5 | 21.9±0.9 |
| setmined | 4844±35 | 4484±17 | +8.0% | 11.1±2.1 | 200.5±67 |

0 fail both. Strict 4/4: **C1 spend-tput PASS, C4 margin PASS** (Δ1069 > 2σ=218 —
solid), **C2 spend-p99.9 FAIL, C3 guardrail FAIL**. setMined p99.9 = crushing TS
win (11 vs 200ms).

KEY: the p99.9 loss is **likely host-noise, not fundamental** — TS p99.9 σ 3.7-5.5ms
(CPU-contention-sensitive) vs reference σ 0.7-1.7ms (robust); on the QUIET EC2 box
uncontended TS p999 was 13-24ms ≈ reference 24ms. macOS is never idle
(WindowServer ~44%) so it can't settle this. Re-profile of the FIXED build (pprof,
IF=512): memcpy gone; CPU spread over real op work, setMined-heavy (~20%: secondary-
index removes); the ~48k cap is the off-CPU dispatch funnel.

NEXT to settle/win: (1) quiet-host head-to-head (fresh spot Linux box, BOTH
backends) — likely shows TS wins p99.9 too. (2) dispatch-sharding refactor
(txid-last-byte per-store routing) for tail robustness + 10M scaling.

## E23 — Dispatch sharding (Phase 1+2) VALIDATED: +13% throughput, create p99.9 199→38ms (2026-06-28)

Implemented per-store dispatch sharding (txid-last-byte routing):
- **Phase 1** (68c120f): deterministic txid→store placement, opt-in `[storage]
  placement="txid"` (default round_robin). Read path unchanged (index authoritative).
- **Phase 2** (51bb8b2): split the single `DispatchPool` Mutex+Condvar queue into
  per-store shards routed by `hash(txid[24..32]) % num_stores`. Routing is a HINT
  (engine resolves the real store via index/placer → a mis-route can't corrupt).
  Recovery/replication bypass the pool (auth ops serial). 2993+ tests + clippy + fmt
  + client all green.

A/B (macOS Docker, fair async config, TS saturation IF=512, back-to-back, load ~5):
| metric | OLD (single queue, round-robin, 1020125) | NEW (sharded, txid, 51bb8b2) | Δ |
|---|---|---|---|
| total ops/s | 41,648 | **47,201** | **+13.3%** |
| create | 16643 (p999 **199.0ms**) | 18868 (p999 **38.5ms**) | +13%, tail 5.2× |
| spend | 12489 (39.9ms) | 14142 (38.0ms) | +13% |
| get | 8334 (35.9ms) | 9453 (28.0ms) | +13% |
| setmined | 4181 (22.7ms) | 4738 (13.4ms) | +13% |

Single queue caused head-of-line blocking → 199ms create tail; per-store sharding
removed it (→38ms) AND lifted throughput +13%. On **8 SHARED cores** (loadgen
co-resident) this is MUTED; on the 24-core EC2 box the funnel left 70% CPU idle, so
the gain there will be larger. VALIDATED (helps, no regressions). Bench configs now
`placement="txid"` (code default stays round_robin for resharding safety).

NEXT for strict cert: quiet multi-core host head-to-head (NEW vs reference) — macOS
noise (load ~5) still inflates TS p99.9 vs the reference; the tail benefit needs idle
cores to fully show. EC2 24-core is the venue.

## E24 — *** WIN *** TeraSlab beats the reference, strict 4/4 PASS (quiet-host cert) (2026-06-28)

EC2 i3en.6xlarge (24 vCPU, NVMe ext4, both backends fair/async), 10 interleaved
fresh-per-round rounds, IF=512, **0 failed both sides**. Full: bench/FINAL_REPORT.md;
raw: bench/results/20260628-ec2-quiet-cert/.

| op | TS ops/s | REF | Δ tput | TS p99.9 | REF p99.9 | Δ p99.9 |
|---|---|---|---|---|---|---|
| spend | 15012±39 | 13303 | +12.9% | 15.6ms | 22.6ms | -30.9% |
| create | 20038±49 | 17744 | +12.9% | 20.7ms | 23.9ms | -13.2% |
| get | 10039 | 8887 | +13.0% | 13.5ms | 20.6ms | -34.3% |
| setmined | 5038 | 4458 | +13.0% | 1.2ms | 4.4ms | -71.9% |

**STRICT 4/4 PASS:** C1 spend-tput ✓, C2 spend-p99.9 ✓ (15.6≤22.6 — the
macOS-failing condition now wins by 31%), C3 guardrail ✓ (every op better on tput
AND p99.9), C4 margin ✓ (Δ1710 ≫ 2σ=77). Green gate (test/clippy/fmt/client) ✓;
opponent-name grep empty ✓.

Decisive fix: per-store dispatch sharding (51bb8b2) broke the global DispatchPool
funnel (~40-48k cap, CPU 30% idle) → +13% tput + robust tail; built on the BytesMut
realloc fix (1020125, -31% on-CPU) + txid placement (68c120f). Arc: E20 (buffered
redo closed spend-p99.9 on macOS) → E22 (BytesMut) → E23 (sharding validated +13%)
→ E24 (WIN on quiet 24-core host).

## E25 — Recipe loadgen (causal UTXO graph) + 2 server findings (2026-06-28)

Reworked `teraslab-loadgen --recipe` to the realistic workload (utxo-db-benchmark-
recipe.md + the user's causal model): **independent per-op tokio streams**
(create/unlock/spend/read/delete) + a periodic setMined burst, driving a CAUSAL
UTXO graph — create tx LOCKED w/ 1 output → unlock the just-created tx → spend a
prior tx's output (1-in/1-out, check OK) → setMined all txids created since the
last burst (every X min) → delete spent+mined; cold-start = create-only. Per-op
batch sizes 488/329/291/488/1024; read = decorate the parent being spent. Commits
8851997 (first cut — deadlocked on a "furthest-behind" scheduler) → **d1e64fa**
(per-stream rebuild). 62 tests + clippy + fmt green. Steady `--saturate` ~300k
rec/s aggregate, errors=0, causal chain verified, cold-start correct.

FINDINGS surfaced by the realistic workload:
1. **setMined-under-burst = THE bottleneck** (exactly the recipe's "block-found
   burst is the stress point; SetMined is the dominant, most CPU-expensive server
   category — a UDF storm"). One set_mined_batch over ~10k txs takes ~**3.76s** and
   serializes against the write/fsync path → bursts stall create/unlock/delete to
   multi-second p50. STEADY streams never stall (~300k rec/s, sub-10ms). So the #1
   realistic-workload perf target is setMined under the block burst (the secondary-
   index removes + write-path serialization; cf E21 setMined ~20% CPU).
2. **Flag-namespace footgun (correctness)**: the create-WIRE flags byte decodes
   locked=0x01/conflicting=0x02/frozen=0x04 (dispatch.rs:6341, receiver.rs:1783),
   but persisted TxFlags has LOCKED=0x04 (record.rs:420) and the client `is_locked`
   checks 0x04. A create sending wire-0x04 as "locked" actually FREEZES → spends
   then fail FROZEN. The loadgen sends wire-0x01 (correct). Review/align the two
   namespaces — may affect the Teranode-Go path if it uses the persisted bit on the wire.

NEXT: (a) optimize setMined-under-burst (the recipe peak); (b) review the flag
namespace; (c) full realistic benchmark on EC2 (24-core NVMe) for real capacity.
