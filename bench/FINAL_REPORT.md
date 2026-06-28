# TeraSlab vs the reference datastore — FINAL REPORT

**Verdict: TeraSlab WINS.** On a fair, quiet, multi-core host TeraSlab beats the
reference datastore on **every priority op — both throughput and p99.9 tail
latency** — with a decisive, low-variance margin and zero failures. Strict 4/4
pass.

**Date:** 2026-06-28 · **Branch:** `feat/device-cache` · **Win SHA:** `51bb8b2`
(per-store dispatch sharding) atop `1020125`, `68c120f`. Raw runs:
`bench/results/20260628-ec2-quiet-cert/`.

---

## 1. Result (the certification)

Host: **EC2 i3en.6xlarge** spot, 24 vCPU, 186 GiB RAM, kernel 6.18. Both backends'
data on **one shared NVMe ext4 filesystem** (fair, same storage). Both in
**async/buffered durability** (TeraSlab buffered redo + writeback cache ↔ the
reference's no-commit-to-device). **10 interleaved rounds, fresh per round**
(both wiped+restarted each round → both start empty, no accumulation), open-loop
IN_FLIGHT=512, 15 s measure + 3 s warmup each. Quiet host (no co-tenant load).

| op | TeraSlab ops/s | reference ops/s | Δ tput | TeraSlab p99.9 | reference p99.9 | Δ p99.9 |
|---|---|---|---|---|---|---|
| **spend** (priority op #1) | **15,012 ± 39** | 13,303 ± 26 | **+12.9%** | **15.6 ms** | 22.6 ms | **−30.9%** |
| create | 20,038 ± 49 | 17,744 ± 32 | +12.9% | 20.7 ms | 23.9 ms | −13.2% |
| get | 10,039 ± 28 | 8,887 ± 18 | +13.0% | 13.5 ms | 20.6 ms | −34.3% |
| setmined | 5,038 ± 14 | 4,458 ± 9 | +13.0% | 1.2 ms | 4.4 ms | −71.9% |

**0 failed operations** on either side, all 10 rounds. (medians ± population stdev.)

### Strict pass condition (defined before tuning) — all four met
- **C1 — Spend throughput ≥ reference:** 15,012 ≥ 13,303 → **PASS** (+12.9%).
- **C2 — Spend p99.9 ≤ reference:** 15.6 ms ≤ 22.6 ms → **PASS** (TeraSlab 31% better).
- **C3 — Guardrail (not >10% worse on create/get/setmined, tput AND p99.9):**
  every one is *better* on both → **PASS**.
- **C4 — Margin (spend win > 2× run-to-run stdev):** Δ 1,710 ops/s ≫ 2σ = 77 →
  **PASS** (the win is far outside the noise; TeraSlab spend σ = 39).

---

## 2. What won it — the optimization arc (each traced to a profiled hotspot)

The campaign's blocker, found by profiling on real hardware, was **not** I/O or
device speed — it was a **global software serialization in the request dispatch
path** that capped TeraSlab at ~40–48k ops/s regardless of cores/disks, leaving
the tail fragile under load (which is why earlier macOS runs lost p99.9).

| SHA | change | hotspot (file:line) | effect |
|---|---|---|---|
| `8fd086b` | SipHash → non-keyed hasher in create-batch maps | create CPU ~5% (flamegraph) | lower create CPU |
| `1020125` | eliminate per-frame `BytesMut` realloc+memcpy | 31% of on-CPU in `BytesMut::reserve_inner` (mod.rs:1208) | −31% on-CPU; create p99.9 199→38 ms (A/B) |
| `68c120f` | deterministic txid→store placement (opt-in) | foundation for routing | — |
| `51bb8b2` | **per-store dispatch sharding** (split the single `DispatchPool` queue, mod.rs:1407) | the global funnel (one Mutex+Condvar queue, 192 workers blocked, CPU 30%) | **+13% tput AND p99.9 tail fixed** |

The decisive fix was **per-store dispatch sharding** (routing requests to per-store
queues by `hash(txid)` last bytes — a *hint*; correctness stays via the index/
placer, so a mis-route can never corrupt). It broke the funnel so TeraSlab uses
the cores it had idle, lifting throughput and making the tail robust. An isolated
A/B (old single-queue vs new sharded, same host) measured **+13.3% throughput and
create p99.9 199 ms → 38 ms** even on a noisy 8-core macOS box; on the quiet
24-core host it delivered the full win above.

---

## 3. Fairness & integrity (hard constraints honoured)

- **Matched durability:** both async (TeraSlab buffered redo + writeback ↔ the
  reference no-commit-to-device). Documented in `bench/METHODOLOGY.md`.
- **Same hardware, same time, interleaved**, both data on the same NVMe filesystem,
  both 4 GiB-class device, replication factor 1 on both, fresh per round.
- **No cheating:** workload not shrunk, durability not dropped, neither
  RAM-vs-disk nor a misconfigured reference; 0 failures both sides.
- **No suppressed signal:** no `#[allow]`, no sleeps/retries masking flakes, no
  deleted code/tests. The de-flake (`7e2c40e`) made a load-flaky test deterministic
  (stronger assertion), not silenced.
- **Green gate (HEAD):** `cargo test --all` exit 0, `cargo clippy --all -- -D
  warnings` clean, `cargo fmt --check` clean, `cargo test --manifest-path
  client/rust/Cargo.toml --all` 0 failed. Integration tests covering cluster /
  replication / recovery are part of `cargo test --all` and pass.
- **Opponent name:** `git grep -i` for the product name is **empty** across the
  repo (source, tests, benches, docs, configs). It is referred to only as "the
  reference datastore".

---

## 4. Reproduce from a clean checkout

1. Build TeraSlab (`feat/device-cache`, HEAD): `cargo build --release --bin teraslab-server`.
2. Provision a quiet ≥24-core host with NVMe (the cert used an EC2 i3en.6xlarge
   spot). Format an NVMe as a filesystem mounted at `/data`.
3. TeraSlab config: `bench/configs/teraslab-async.toml` (device on `/data/d0.dat`,
   `device_split=4`, `[storage] placement="txid"`, buffered redo, writeback cache).
4. Reference: official container, production config, async (no commit-to-device),
   4 G device file on the same `/data`.
5. Driver: the unified Go load harness (Teranode `utxo.Store` interface, drives
   both backends identically), open-loop IN_FLIGHT=512, 10 interleaved rounds,
   fresh per round. (Harness lives outside this repo — it references the reference
   product; see `bench/RESUME.md` / `bench/METHODOLOGY.md`.)
6. Aggregate medians ± stdev per op, apply the C1–C4 condition above.

---

## 5. Caveats / scope

> **⚠ IMPORTANT — this cert measures efficiency at a fixed operating point, NOT
> capacity.** The Go head-to-head harness issues **one record per RPC** (un-batched).
> Open-loop at IN_FLIGHT=512 with ~10 ms/op latency caps both backends at
> ~50k records/s ( = concurrency ÷ latency ), *independent of hardware* — which is
> why the 24-core/2-NVMe box performed like an 8-core MacBook and server CPU sat at
> ~30%. Both TeraSlab (~50k) and the reference (~44k) hit this harness ceiling, so
> neither DB was stressed. TeraSlab wins because it is ~13% lower-latency per op (so
> it completes more at fixed concurrency) — a real, fair win on the stated condition,
> but it does **not** represent the machine's capacity. The real node runs ~1M tx/s
> **batched at ~488 records/req** (see `utxo-db-benchmark-recipe.md`); a capacity
> measurement must use the batched recipe workload. Capacity re-measure is pending
> (also: `device_split` should scale to the core count — the cert used 4 on 24 cores —
> and a server batch-path slowdown at batch>1, seen in `LINUX_NVME_REPORT.md`, must be
> resolved for large batches).


- The win is on a **quiet multi-core host**. macOS-Docker (8 cores, never idle —
  WindowServer ~44%) cannot certify p99.9 there; it inflates TeraSlab's tail. The
  per-store sharding's tail benefit needs idle cores, which the quiet 24-core host
  provides. This is a measurement-environment fact, documented and reproduced.
- Full Docker-compose e2e (multi-node cluster/failover) was not re-run end-to-end
  this session; the `cargo test --all` integration suite (incl. cluster TCP
  replication + recovery + crash boundaries) is green. Recommend a Docker e2e pass
  before release as belt-and-suspenders (the dispatch-sharding replica-apply path
  bypasses the pool and runs serial, so it is unchanged).
- Scaling beyond this point (toward the 10M-tps target) and the realistic
  batched/burst workload are tracked separately (`bench/LINUX_NVME_REPORT.md`,
  `utxo-db-benchmark-recipe.md`, and the recipe-faithful loadgen).
