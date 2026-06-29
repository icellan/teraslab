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

## Status
TeraSlab **beats the reference on throughput at production concurrency** (the
headline win the campaign was chasing) — but the full pass condition (throughput +
p99.9, proven + multi-round reproducible in FINAL_REPORT) is **not yet complete**:
the p99.9 half needs the open-loop measurement above. Not declaring the suite won.
