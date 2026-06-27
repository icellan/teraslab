# Head-to-Head Benchmark Methodology

TeraSlab vs. "the reference datastore" (a general-purpose, in-production
key/value store that BSV Teranode currently uses as its UTXO backend) on the
**production UTXO workload**.

> The opponent is used as a read-only benchmark opponent only. It is never named,
> imported, or otherwise embedded anywhere in the TeraSlab repository. All
> reference-side harness code and config live **outside** this repo (see paths
> below) and are referenced here by path.

## Why this comparison is fair (and what would make it unfair)

Both stores are driven through **the exact same production driver**: Teranode's
own `utxo.Store` Go interface (`stores/utxo/Interface.go`). Teranode ships a
production adapter for each backend:

- reference backend → Teranode's in-production reference-store adapter (in the Teranode tree)
- TeraSlab backend  → `stores/utxo/teraslab` (the in-production adapter, Teranode tree)

A single Go load generator constructs one `utxo.Store` for the chosen backend and
issues **identical logical operations** (same record shapes, same op mix, same
key distribution, same concurrency) against it. This eliminates the most common
cheating vectors:

- **No "different/faster client" asymmetry.** Both backends are spoken to by the
  same Go process through the same interface, each via *its own* production
  network adapter and wire protocol — i.e. exactly how Teranode uses each store.
- **Both run as real servers over the real wire** (Docker containers), not
  in-process and not TeraSlab-in-RAM vs. reference-on-disk.
- **Matched durability** (see below) — the single most important fairness knob.
- **Matched replication**: single copy on each side (reference
  `replication-factor 1`; TeraSlab single node). No quorum on either side.
- **Same host, same time, interleaved** so transient OS load hits both equally.

### Durability match — the key fairness decision

Teranode's *production* reference-datastore config (in the Teranode tree) uses:

```
storage-engine device { file ...; filesize 4G; flush-size 128K; post-write-cache 128M }
replication-factor 1
# NO commit-to-device   -> writes are NOT fsync'd per transaction
```

i.e. the reference, as Teranode runs it, uses **asynchronous / buffered
durability**: writes land in the post-write-cache and are flushed to the device
in 128K chunks in the background; it does **not** `fsync` on the commit path.

Therefore the honest matched-durability TeraSlab configuration is **buffered
durability + writeback cache** (background flusher), *not* strict
fsync-per-batch. Comparing strict-fsync TeraSlab against a non-fsync reference
would be the reverse of cheating — it would handicap TeraSlab against a weaker
durability guarantee. Both sides are configured to the same async-durability
posture and that choice is documented here so it can be challenged.

(If/when the reference is run with `commit-to-device` enabled, TeraSlab is to be
re-run in `strict` durability for that comparison. The default suite below is the
production posture: async on both sides.)

## Workload

Mirrors `teraslab/client/rust/src/bin/loadgen.rs` (the existing TeraSlab load
generator), so the Go driver and the Rust driver describe the *same* workload:

- **Record on Create**: tx_version=2, fee, `size_in_bytes=250`, `N` UTXO hashes
  where `N = 2 + (rng % 4)` (2–5 outputs), random 256-bit txid, random 32-byte
  utxo hashes. No cold/extended data.
- **Op mix** (uniform draw, matching loadgen's `rng % 10`):
  - Create  40%  (op 4)
  - Spend   30%  (op 1)   ← the op to win
  - Get     20%  (op 20, full-metadata field mask)
  - SetMined 10% (op 3)
  - (Freeze/Unfreeze measured separately as micro-latency, not in the churn mix.)
- **Key distribution**: each worker spends/reads/mines records *it* created
  (per-worker local queue), random txids — no cross-worker contention by
  construction, matching loadgen.
- **Spend payload**: 36-byte spending data per input.

Per op we report **p50 / p99 / p99.9 latency and sustained ops/sec**, for:
1. single-item latency (batch=1),
2. batched throughput (batch sweep: 1, 16, 64, 256),
3. the mixed create→spend→get→setMined churn at the ratios above.

### Load model: closed-loop vs open-loop

The driver supports two load models, because they expose different limits:

- **Closed-loop** (`WORKERS=N`): N synchronous workers, each blocking per op, so
  exactly N requests are in flight. Throughput = N ÷ latency. Models a fixed
  caller-concurrency.
- **Open-loop** (`OPEN_LOOP=1`, `IN_FLIGHT=M`, `DISPATCHERS=D`): a dispatcher pool
  fires ops as goroutines bounded by an `IN_FLIGHT` semaphore, decoupling offered
  load from completion latency and letting the adapter's batchers fill. This is
  the faithful Teranode block-validation pattern (bursty, thousands of concurrent
  txs). Sweep `IN_FLIGHT` to find each backend's **saturation throughput** and
  compare peaks — this is the measurement that matters for the 10M-ops/s target.

The open-loop sweep is the decisive comparison (see `PERF_LEDGER.md` E6).

## Explicit pass condition (defined before any tuning)

Under the matched fair configuration above, measured **interleaved** over **≥ 3
runs** per backend, TeraSlab **wins** iff ALL hold on the per-op median:

1. **PRIMARY (must win):** TeraSlab **Spend throughput ≥ reference** AND
   TeraSlab **Spend p99.9 ≤ reference**.
2. **GUARDRAIL (must not regress):** TeraSlab is **not more than 10% worse** than
   the reference on Create, Get, and SetMined — for both throughput and p99.9.
3. **MARGIN:** the Spend win must exceed run-to-run noise — the median delta must
   be **> 2× the larger backend's run-to-run standard deviation**. A win inside
   the noise band does not count.
4. **CORRECTNESS FLOOR:** `cargo test --all`, `cargo clippy --all -- -D warnings`,
   and the Docker e2e suite (cluster, failover, recovery) stay green for the
   committed TeraSlab build that produced the win.

## Environment

- Host: Apple M3, 8 cores, 24 GB RAM, macOS 26.3 (Darwin 25.x).
- Docker: Docker Desktop 29.4.0, VM allocated 8 CPU / ~12.6 GB.
- Reference image: the reference datastore's official server container image
  (exact tag recorded out-of-repo, in the harness).
- TeraSlab image: built from this repo (`teraslab-tests/docker/Dockerfile`),
  commit SHA recorded per run.

### Honesty caveat on this host (must be read with every number)

This is a shared workstation; observed 1-min load average during setup was **~34
on 8 cores** (heavily oversubscribed). **Absolute** ops/sec and tail latencies on
this box are scheduler-noise-dominated and are NOT the README's 10M-ops/sec
target conditions. To keep the *relative* comparison meaningful despite this:

- both backends are measured **interleaved in short alternating windows** within
  the same wall-clock period, so a load spike perturbs both;
- we report the **relative delta** and its run-to-run spread, not just absolutes;
- a win is only certified when criterion 3 (margin > 2σ) holds.

Final certification of the headline numbers (`FINAL_REPORT.md`) additionally
requires a re-run on a quiet host (load < 1 per core) following the reproduction
recipe. Until then, ledger rows are explicitly marked `[loaded-host]`.

## Reference-side harness location (outside this repo)

- Teranode dual-backend driver worktree: `/Users/siggioskarsson/gitcheckout/teranode-bench-wt`
  (branch `teraslab/integration-wip`; carries both production adapters).
- Unified Go load generator: `<that worktree>/cmd/utxobench/` (new, lives in the
  Teranode tree — never in TeraSlab).
- Reference server config: Teranode's production reference-datastore config (Teranode tree).

## Reproduction recipe

See `bench/results/` for the exact commands captured per run, and
`FINAL_REPORT.md` (written once the pass condition is met) for the clean-checkout
recipe.
