# High-concurrency head-to-head — TeraSlab beats the reference on throughput (2026-06-29)

**Breakthrough:** at the production-scale concurrency the real node runs (100k+
chains, per the user), **TeraSlab now beats the reference datastore on Spend AND
Create throughput, with ZERO failures, while the reference plateaus at ~40.7k and
sheds tens of thousands of ops.**

## Setup
EC2 i3en.6xlarge spot (24 vCPU, NVMe), both backends on the same NVMe, both async,
**fair 32 GiB device each** (the earlier 4 GiB device filled at high concurrency for
BOTH — bumped equally). Recipe chain workload (txblaster2 model) driven through
Teranode `utxo.Store`, interleaved fresh-per-round. TeraSlab: device_split=4,
fallocate, write-back cache + dirty-index, buffered redo. Raw:
`bench/results/20260629-highconc-h2h/`.

## Results (per-op throughput / failures), worker sweep — single clean round each

| chains | TS spend/s (fail) | REF spend/s (fail) | TS create/s (fail) | REF create/s (fail) |
|---|---|---|---|---|
| 10k  | 25.8k (0) | **40.7k (0)** | 25.8k (0) | 40.7k (0) |
| 30k  | 37.5k (0) | 40.8k (722–13.9k) | 37.1k (0) | 40.7k (722) |
| 50k  | 39.4k (0) | 40.7k (8.8k–19k) | 38.5k (0) | 40.8k (14k) |
| 100k | **41.0k (0)** | 40.6k (23k) | **41.3k (0)** | 40.6k (29k) |
| 150k | **43.3k (0)** | 40.4k (24.7k) | **44.1k (0)** | 40.5k (69k) |

(100k reproduced 3× independently: TS spend 41.0/41.9/41.9k f0, REF 40.3/40.3/40.6k
with 15–60k failures each round.)

**TeraSlab throughput keeps CLIMBING with concurrency (25.8k→44.1k) and stays f0;
the reference is SATURATED at ~40.7k and starts shedding load above ~30k chains.**
TeraSlab's max sustainable failure-free throughput (~44k) exceeds the reference's
(~40.7k, beyond which it fails).

## The honest caveat — p99.9 (the other half of the pass condition)
TS p99.9 at 100k is ~1.3 s vs the reference ~0.18 s. BUT this is a **closed-loop
overload artifact, not a fair tail comparison**: TeraSlab QUEUES and COMPLETES all
100k chains (f0), so its requests wait; the reference FAIL-FAST SHEDS ~30% of the
load, so only its fast survivors are measured (a dropped op is reported as a
failure, not as latency). At equal *successful* throughput the reference is
dropping a third of the work.

**Therefore the pass condition (Spend throughput AND p99.9) is:**
- **throughput: MET** (TS > reference, f0, reproduced).
- **p99.9: not cleanly met under closed-loop** — and the closed-loop tail is not a
  fair metric here. A defensible p99.9 comparison needs an **OPEN-LOOP fixed-rate**
  test: offer both backends a fixed ~38k/s (below saturation) and compare p99.9 +
  failures. That is the remaining step to a clean FINAL_REPORT.

## Remaining for a clean, committed FINAL_REPORT win
1. **Open-loop fixed-rate latency test** (rate-controlled recipe, ~35–38k/s) →
   compare p99.9 + failures fairly (closed-loop tail is confounded by the
   reference's load-shedding).
2. **Multi-round stability**: the harness round-2+ fails (`f100000`) because the
   24M `expected_records` index pre-allocation makes per-round server init exceed
   the 40 s health timeout under rising load — raise the health timeout and/or
   lower per-round re-init; pure tooling fix. (Round 1 is clean + reproduced.)
3. Account for the reference's failures in any throughput claim (a failed op needs
   retry → real offered load is higher than its successful rate).
4. Then re-run interleaved fresh-per-round, apply METHODOLOGY pass condition,
   write FINAL_REPORT.

## ⭐ OPEN-LOOP p99.9 TEST RUN — settles it: reference wins the tail
Added a rate limiter to the recipe (`RECIPE_RATE_LINKS_SEC`, token-bucket gate per
link) and ran BOTH backends OPEN-LOOP at a fixed **8,500 links/s (~34k store-ops/s,
below both saturation)**, 20k workers, both **f0**:

| op | TeraSlab p99.9 | reference p99.9 |
|---|---|---|
| spend | **113.9 ms** | **38.3 ms** |
| create | 104.1 ms | 31.3 ms |
| get | 133.3 ms | 7.8 ms |

(raw: `teraslab_openloop_8500.json` / `reference_openloop_8500.json`.)

**With the closed-loop confound removed, the reference has ~3× LOWER p99.9 at a
given throughput.** So TeraSlab's higher per-op latency is real (more per-op work:
write-back cache + redo + index + several locks vs the reference's tighter path),
not a load-shedding artifact.

## FINAL HONEST VERDICT
- **Throughput CEILING / high-concurrency goodput: TeraSlab WINS** — 41–44k links/s
  f0 at 100k–150k chains vs the reference's ~40.7k where it sheds 15–69k ops.
- **p99.9 (per-op tail latency at a fair fixed rate): reference WINS** (~3× lower).
- **Pass condition = "beats on Spend throughput AND p99.9": NOT met** — TeraSlab
  loses the p99.9 half. **TeraSlab does NOT win the suite. FINAL_REPORT not written.**

### p99.9 gap localized + hypotheses ruled out (open-loop @8.5k links/s, all f0)
| path | spend p50 | spend p99.9 |
|---|---|---|
| TS via **mock server** (same client, instant ack) | **3.8 ms** | 18 ms |
| TS via **real server**, default intervals | 65 ms | 108 ms |
| TS via real server, **flush+writeback=5ms** | ~65 ms | 103 ms (no change) |
| **reference** | 4.5 ms | 38–49 ms |

- The client/harness is fast (mock 3.8 ms) → **the ~61 ms is the REAL SERVER's
  per-op latency**, not the client/batcher/rate-limiter.
- Ruled OUT as the cause: redo flush + writeback interval (fastiv 5ms → no change);
  TCP Nagle (server DOES `set_nodelay` on the accept path, src/server/mod.rs:602;
  client sets it too).
- Little's law: 34k ops/s × 61 ms ≈ **~2000 requests in flight** on the real server
  vs ~130 via the mock → the server QUEUES ~1900 requests even at this low offered
  rate. Its effective per-op concurrency/latency is the limit — the distributed
  per-op overhead (write-back cache + redo + index + several brief locks + dispatch
  ~0.35 ms CPU each but ~60 ms wall-clock in queue/handoffs). This is the SAME
  per-op-overhead conclusion, now localized to the server and quantified.

The remaining latency gap is the distributed per-op overhead (the same finding all
along: each op takes the write-back-cache + redo + index + multiple brief locks).
Closing it to also win p99.9 needs the per-op-overhead reduction = a ground-up
lower-overhead path (lock-free / fewer per-op locks / lighter cache path) —
a deliberate architecture project, the documented deep next step. The campaign got
TeraSlab from ~4.7× behind to a throughput-ceiling win + a fairly-measured ~3×
p99.9 deficit; the win condition is not satisfied.
