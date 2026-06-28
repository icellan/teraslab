# Recipe / chain-workload head-to-head — findings (2026-06-28)

**This OVERTURNS the un-batched `FINAL_REPORT.md` "win".** On the realistic,
batched, high-concurrency chain workload (the one the real node runs), TeraSlab
**loses to the reference by ~4–5×**. The earlier 4/4 win was on an un-batched,
low-concurrency driver that masked this.

## The workload (now faithful to the real node)

Rebuilt the head-to-head on the **`bitcoin-sv/teranode-coinbase` `txblaster2`**
scaling generator model: **N goroutine chains** (default 10 000, scales to 100k+),
each building an INDEPENDENT causal UTXO chain — `Create(child, LOCKED) → Spend
(parent) → SetLocked(child,false) → BatchDecorate`, wait for each op, advance to
the child, repeat — plus a SetMined burst overlay and a spent+mined prune stream.
Driven through Teranode `utxo.Store` so BOTH backends run identical load.
Txs are **unsigned** (TeraSlab does not verify scripts; signing is pure client
CPU the store never needs — confirmed with the user).

Harness: `teranode-bench-wt/cmd/utxobench/bench_test.go`, `RECIPE=1`.
Knobs: `RECIPE_WORKERS RECIPE_BURST_INTERVAL_SECS RECIPE_SETMINED_CHUNK
RECIPE_LOCKED RECIPE_GET_EVERY RECIPE_DELETE_WORKERS BATCH_DUR_MS BATCH_SIZE
POOL_SIZE`. This model has **0 failures on both backends** (the earlier
op-stream model's phantom-parent TX_NOT_FOUND races are gone — each chain only
ever spends its own parent).

## Result (EC2 i3en.6xlarge, 24 vCPU, NVMe, both async, 10 000 chains)

| op | TeraSlab ops/s | TeraSlab p50 | TeraSlab p99 | Reference ops/s | Ref p50 | Ref p99 |
|---|---|---|---|---|---|---|
| create | ~9,400 | 128 ms | **4,600 ms** | ~40,700 | 76 ms | 130 ms |
| spend  | ~9,400 | 142 ms | 3,700 ms | ~40,800 | 65 ms | 116 ms |
| (all ops track together) | | | | | | |

Reference ≈ **40.7k links/s, tight tails**; TeraSlab ≈ **9.4k links/s with
multi-second tails**. (`bench/results/20260628-recipe-chain/`.)

## Root cause — it is NOT hardware or device speed

- **Server is CPU-idle under load: <1 of 24 cores, all dispatch threads ~0%.**
  TeraSlab is **starved / latency-bound**, not capacity-bound.
- The throughput ceiling is the **request/connection-parallelism path**:
  - The Go client (`client/go/pool.go`) gives each concurrent caller its OWN
    connection (it avoids pipelining a busy conn until `MaxConns`), on the
    now-stale assumption "the server processes each connection's requests
    serially." So it **opens far more connections than `pool_size`** (231 conns
    observed at `pool_size=64`) and **storms past the server's per-IP cap
    (default 1024)** at higher pool sizes → mass connection failures.
  - Effective in-flight concurrency ≈ connection count, so the server's
    pipelined dispatch pool (192 workers, `pipeline_depth=16`) sits idle.
    `10 000 closed-loop ops ÷ ~64–231 conns × ~per-op ms` ≈ the observed
    ~128 ms p50.

## What moved the needle (and what didn't)

- **Raising the server connection caps** (`max_connections[_per_ip]` →16384) +
  `pool_size`→512–1024: **fixed the tail** (create p99 4,600 ms → **450–570 ms**,
  p50 → **50–84 ms**) and lifted server CPU to ~1.8 cores — BUT throughput stayed
  ~7.5k and it **introduced ~2,000 op failures** (dial storm during ramp; each
  failure kills a chain → fewer live chains → throughput stays capped).
- **TCP_NODELAY on the Go client** (`client/go/conn.go`): the client never
  disabled Nagle though the server does. Fixed it (correct hygiene; the server's
  own comment documents the 40 ms–3 s Nagle/delayed-ACK tax). **No measurable
  gain here** — the client pipelines/batches, so its writes aren't Nagle-starved.
- **redo flush / writeback interval 50 ms → 5 ms**: minor, ~+15%.
- **batch window 20 ms → 3 ms**: minor.

## ⭐ PINNED root cause (perf profile of the server under load)

`perf record` of the server (bc512, srvCPU ~1.8 cores) shows the hot thread is
**`cache-writeback`** — the single write-back-cache flusher — spending
**64.86% of CPU in `Vec::from_iter`** (the `b.data.clone()` in
`src/cache.rs::flush_all_dirty`, ~line 457) plus serial O_DIRECT `pwrite64`
(ext4 dio, ~13%). i.e. **one thread, every `writeback_interval` tick, for each
shard: scans ALL cached blocks, CLONES every dirty block's 4 KiB into a Vec,
then writes them one-by-one.** With a 4 GiB cache under heavy writes that clones
~GiB/cycle on a single core → this IS the ~1-core server ceiling.

```
64.86% cache-writeback  [.] Vec::...::from_iter         <- b.data.clone() of all dirty blocks
18.92% cache-writeback  [k] ...pwrite64 / ext4_dio_rw   <- serial per-block O_DIRECT writes
```

## The fix plan (server-side writeback is the #1 lever; client is #2)

0. **Parallelize the write-back flush + kill the full-data clone**
   (`src/cache.rs`): the cache is already sharded (cores*2 shards) but ONE
   thread drains them serially and clones every dirty block each tick.
   (a) flush shards **in parallel** (a pool of writeback threads, one per shard
   group) so writeback uses many cores; (b) make `Block.data` an `Arc<[u8]>`
   (copy-on-write on mutation) so the per-tick snapshot is a refcount bump, not
   a 4 KiB memcpy — removes the 65% `from_iter` cost; (c) batch/iovec the device
   writes instead of one `pwrite` per block. This is the change that should
   break the 1-core ceiling.

1. **Client connection/pipelining model** (`client/go/pool.go`, `conn.go`): drive
   many concurrent in-flight requests **per connection** (the server already
   supports `pipeline_depth`), bounding total connections well under the per-IP
   cap, instead of one-conn-per-concurrent-caller + dial storm. This is the main
   lever — "parallelize in all aspects, lots of parallel batched calls."
2. **Coalesce SetLocked + Delete in the adapter** (`stores/utxo/teraslab`) like
   the spend batcher already does — the recipe currently issues them effectively
   single-item; Teranode always batches (size + 2–5 ms window).
3. Eliminate the **dial-storm failures** at ramp (pre-warm the pool; cap dial
   concurrency).
4. Re-confirm there is no residual **server-side per-op wall-clock** (lock
   handoff) once the client feeds it — profile with the server actually busy.
5. Then re-run the chain head-to-head fresh-per-round and apply the strict 4/4.

## PROGRESS — writeback fix landed (commit 6c97a37)

Implemented fix-plan step 0 (`src/cache.rs`): parallel per-shard flush on a
dedicated rayon pool + `Arc<[u8]>` CoW block data (snapshot = refcount bump, not
a memcpy). Measured on EC2 (10k chains):

| metric | before | after fix | reference |
|---|---|---|---|
| server CPU under load | ~1.0–1.8 cores | **3.2 cores** | — |
| create p50 | 84–128 ms | **28 ms** | 76 ms |
| create p99 | 449–4600 ms | **193 ms** | 130 ms |
| throughput (links/s) | ~7.5–9.4k | ~6.8–8.7k | ~40.7k |

**The 1-core writeback ceiling is broken** (3.2 cores, latency now *better than
the reference's p50*). But **throughput did NOT rise** — it is now capped
~8.7k links/s (~35k store-ops/s) **independent of server CPU**, so the
bottleneck has MOVED OFF the server to the **client**:

- Low pool (128): 0 failures but server starved (0.6 core), high latency →
  client offers too little concurrency.
- High pool (512–1024): server busy (3.2 cores), great latency, but the client
  **dial-storms** (opens one conn per concurrent caller, 2.7k–4.4k op failures)
  → each failure kills a chain → throughput stays capped.

**Next bottleneck = the Go client connection/pipelining model** (fix-plan step 1
below) + a ~35k store-ops/s client-side ceiling (go-batcher single-worker per
batcher / adapter — profile the CLIENT next, the server now has headroom).

### Update 2 — client connection fix landed (commit 3381397)

Fix-plan step 1 (`client/go/pool.go`,`conn.go`): bounded pre-warmed pool +
per-conn pipelining (reuse least-loaded conn while inflight < PipelineDepth=16;
dial only when all saturated & below MaxConns; capped non-fatal dials).
**Eliminated the dial-storm failures: 2,777 → 0** across pool 64–1024.

BUT throughput stayed ~8.7–9.8k links/s (~35k store-ops/s) and the server fell
back to ~0.6 core. Scaling chains 10k → 30k → 50k did **NOT** raise throughput —
it only raised latency (p50 250 → 450 → 705 ms). **So the ~35k store-ops/s cap is
a HARD client-side serialization, not under-offered concurrency** (server idle
throughout). The reference's client stack does 40.7k links/s (~163k store-ops/s);
TeraSlab's adapter+client caps ~4.5× lower.

### REMAINING bottleneck (next session) — the Go client/adapter throughput cap

~35k store-ops/s hard cap in the **client stack** (server idle).

### PROFILED (commit pending) — `client-cpu.prof` / `client-pprof-cum.txt`

CPU profile of the Go harness under load: the client process uses **~1 core**
(108% total) and is **not CPU-bound** — it is drowning in Go runtime
scheduler/lock contention (futex 7.9%, lfstack.pop 6.6%, lock2 6.2%, selectgo
6.2%, findRunnable/schedule ~18/21% cum). Cumulative app paths reveal WHY:

- create → storeBatcher, spend → spendBatcher: **coalesced** into batches,
  dispatched concurrently, pipelined per conn (after the pool fix). ✅
- **unlock → `SetLocked → SetLockedBatch → sendTxIDBatch` (9.8%)** and
  **get → `BatchDecorate → GetRecordBatch` (13.6%)**: sent as **single-item
  RPCs — NOT batched at all.** 2 of every 4 ops/link are 1-item round-trips,
  each taking the per-conn write mutex (`conn.mu` in roundTrip/writeRequest).

So ~17k single-item RPCs/s + 10k goroutines funneling through one
channel+worker per go-batcher = the coordination storm that caps throughput at
~35k store-ops/s with both client and server at ~1 core.

### Update 3 — coalescing landed + MEASURED (teranode-bench-wt 8031f0bc8)

Coalesced SetLocked + BatchDecorate in the adapter (all 4 ops now batched, matches
production). Measured on a fresh box (10k chains, pool 256, bigconn caps):

| metric | pre-coalesce | post-coalesce |
|---|---|---|
| throughput (links/s) | ~8.7k | **~11.6k (+33%)** |
| create p50 | 250 ms | **46 ms** |
| create p99 | ~2,000 ms | **78 ms** (25× tighter) |
| op failures | 0 | 0 |

Big latency win + 33% throughput. BUT still ~46k store-ops/s with the **server
idle (0.6 core)** — the client transport is STILL the ceiling. Throughput is far
below what the tight per-op latency implies (10k workers × ~184ms/link ⇒ ~54k
links/s expected, only 11.6k seen), i.e. chain goroutines spend most wall-clock
NOT in op latency → **Go scheduler/coordination overhead in the client process**
(too many goroutines + central go-batcher funnel + pool lock), exactly the
contention the reference client avoids. Next = the client transport redesign
(see REFERENCE_CLIENT_ANALYSIS.md): shard the conn pool by hint + de-funnel the
batcher.

### Update 4 — client conn-pool sharding landed (UNMEASURED-WIN: ~0)

Sharded the teraslab Go client conn pool by round-robin hint (global atomic
MaxConns cap), removing the single global pool mutex + O(conns) least-loaded scan.
Measured: 11.9k links/s — **no gain over coalescing's 11.6k**. So the global pool
lock was NOT the bottleneck (ruled out). Race/vet/gofmt clean, tests pass.

### Update 5 — re-profile with ALL fixes: client is GC + contention bound

`cli2.prof` (coalescing + pool-sharding): server now at **2.8 cores** (up from
0.6 — the client finally feeds it), but throughput still ~12k links/s. The CLIENT
is the wall, dominated by **mallocgc 25.9% cum** (allocation/GC pressure) +
channel/lock contention (selectgo 12%, lock2 8.9%, futex 9.2%, lfstack 9.2%) +
sha256 8% (tx build, shared with the reference). The reference's client avoids all
this central coordination/alloc (sharded conn-per-command, fewer goroutines).

**NET so far: 8.7k → ~12k links/s (+38%), create p99 ~2000ms → ~70ms (28×).
Still ~3.4× behind the reference (40.7k links/s). Both client & server now have
idle cores → the cap is client coordination+allocation, not compute.**

### NEXT hypotheses (local work; measure on a fresh box)
1. **Cut client allocations (mallocgc 25%)**: reuse/pool the wire frame buffers
   and batcher item structs in the teraslab client + adapter hot path; avoid
   per-request allocations. (sha256/tx-build is shared with the reference, leave
   it.)
2. **De-funnel the adapter go-batcher**: it is one channel + one worker goroutine
   per op-type (selectgo/lock contention). Shard it into M lanes, or replace with
   a lock-free per-shard accumulator, so 10k callers don't serialize on one
   channel+worker.
3. **If 1+2 don't close it, adopt the reference transport model**: synchronous
   conn-per-command over the (now sharded) bounded pool — far fewer goroutines
   (no readLoop-per-conn, no done-channel-per-request), which kills the scheduler
   thrash. Biggest change, highest potential.
Then re-run the chain h2h fresh-per-round and apply strict 4/4.

### (done) coalesce SetLocked + BatchDecorate in the adapter

Add coalescing batchers for `SetLocked` and the get/decorate path in
`stores/utxo/teraslab` exactly like the existing `spendBatcher` (merge concurrent
single-item calls into one wire batch, size + 2–5 ms window, ItemIndex re-map).
This makes ALL four ops real batches ("never single items, always batches"),
~halves RPC count, and breaks the single-item write-lock storm. Consider also
multiple go-batcher workers (sharded channel) to cut the single-worker
contention. Then re-run the chain h2h fresh-per-round and apply strict 4/4.

## Reproduce

Box: EC2 i3en.6xlarge spot, AMI `ami-08f44e8eca9095668` (us-east-1). Build
`teraslab-server`; rsync `teranode-bench-wt` (go.mod replace → local client/go);
`go test -c -tags utxobench`. Runner: `/tmp/ts_run.sh` (TeraSlab-only sweep),
`/tmp/h2h_recipe.sh` (interleaved h2h). NVMe ext4 at `/data`; config
`bench/configs/teraslab-async.toml` (+ raised conn caps).
