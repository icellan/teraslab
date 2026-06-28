# Why the reference datastore's client sustains high throughput over few connections

Read-only inspiration (no opponent code/names copied). The reference's Go client
sustains ~163k store-ops/s over a *limited* connection pool, while TeraSlab's Go
client caps ~35k store-ops/s with the server idle. The difference is the
transport's coordination model.

## The reference client model (observed in its Go driver)

1. **Sharded connection pool per node.** The pool is an ARRAY of N sub-heaps
   (each a small ring buffer). Acquire/return index by a `hint % N`, so a
   `Poll`/`Offer` contends on only 1/N of the pool — never a single global
   pool lock. Pool is bounded (default ~100 conns/node — "limited connections").
2. **Connection-per-command, synchronous, exclusive.** Each op (a batch command)
   checks out ONE connection, does a blocking write-request then read-response on
   it, returns it to the pool. NO pipelining, NO central command queue/worker, NO
   per-request response-demux map, NO done-channel-per-request. The op's goroutine
   does its own syscalls and owns the connection for the op's duration.
3. **Non-blocking acquire + async grow + retry.** On an empty pool it returns
   immediately, async-spawns one new connection (capped at the queue size), and
   the command layer retries with backoff. Callers never block holding goroutines
   on a central lock.

Net: independent goroutines each touch a *sharded* pool and do plain syscalls —
almost no shared mutable coordination state on the hot path.

## Why TeraSlab's current client is slower here (profiled)

CPU profile of the harness under load: client ~1 core, NOT CPU-bound, dominated
by Go scheduler/lock contention (futex, lfstack.pop, lock2, selectgo,
findRunnable). The contention comes from central coordination the reference
doesn't have:

- **Adapter go-batcher = one channel + one worker goroutine per op-type.** All
  concurrent callers `Put` onto a single channel drained by a single worker that
  forms batches serially. 10k goroutines hammering 3-4 such channels = the storm.
- **Client transport per request:** per-conn write mutex (`conn.mu`), a
  `pending sync.Map`, a response channel per request, and a `readLoop` demuxing
  by request-id. Pipelining adds matchmaking overhead on every op.
- **Global pool mutex:** the current pool `get()` scans for the least-loaded conn
  under ONE `p.mu` — a global serialization point per acquire (regression vs a
  sharded pool).

## Redesign plan for the TeraSlab Go client (inspired, generic)

Goal: remove central coordination so independent goroutines reach the wire with
minimal shared state — keep batching at the adapter (production does), but make
the transport cheap and sharded.

1. **Shard the connection pool by a hint** (goroutine-id / CPU / round-robin
   counter) into N sub-pools, each with its own lock + small ring — eliminate the
   global pool mutex and the least-loaded scan. (Directly mirrors the reference's
   sub-heaps.)
2. **Cut the central go-batcher funnel.** Either (a) shard the adapter batcher
   into M independent worker+channel lanes (so callers spread across lanes), or
   (b) move coalescing to a per-shard/lock-free accumulator so there is no single
   worker goroutine per op-type. Batches still go out (production coalesces), but
   forming them is no longer a single-goroutine bottleneck.
3. **Bounded pool + non-blocking acquire + retry** instead of dial-per-caller or
   blocking — pre-warm to the cap, async-grow, retry on transient empty.
4. **Reconsider pipelining vs connection-per-request.** The reference proves a
   *simple synchronous connection-per-command over a sharded bounded pool* can
   beat a pipelined-but-centrally-coordinated client for this access pattern. If
   sharding the pool + batcher doesn't close the gap, try the simpler model:
   each batch RPC grabs a sharded conn, writes, reads, returns it — dropping the
   pending-map/readLoop/per-request-channel machinery.

## Status / next

- Adapter coalescing of ALL ops landed (SetLocked + BatchDecorate now batched,
  matching production) — `teranode-bench-wt` commit `8031f0bc8`.
- The spot EC2 box was reclaimed before re-measuring the coalescing fix; a new
  spot box is needed to measure (a) the coalescing fix and (b) the client
  pool/batcher sharding above. Server-side writeback fix already measured
  (1.8->3.2 cores, p50 84->28ms).
