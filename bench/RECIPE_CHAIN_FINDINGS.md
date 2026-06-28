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

## Reproduce

Box: EC2 i3en.6xlarge spot, AMI `ami-08f44e8eca9095668` (us-east-1). Build
`teraslab-server`; rsync `teranode-bench-wt` (go.mod replace → local client/go);
`go test -c -tags utxobench`. Runner: `/tmp/ts_run.sh` (TeraSlab-only sweep),
`/tmp/h2h_recipe.sh` (interleaved h2h). NVMe ext4 at `/data`; config
`bench/configs/teraslab-async.toml` (+ raised conn caps).
