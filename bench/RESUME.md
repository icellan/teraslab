# RESUME — TeraSlab vs the reference datastore (perf campaign)

**Start here after a context reset.** Full chronological detail: `PERF_LEDGER.md`
(B0 → E26). Fairness/setup + pass condition: `METHODOLOGY.md`. The proven win:
`FINAL_REPORT.md`. Raw cert runs: `bench/results/`. Branch `feat/device-cache`,
worktree `.worktrees/device-cache`. Constraint: never name the reference product
in-repo (`git grep -i` stays empty — verified); call it "the reference datastore".

## CURRENT STATE (2026-06-28, updated PM) — REALISTIC WORKLOAD: TeraSlab LOSES ~4-5×

**⚠ The un-batched 4/4 "win" (FINAL_REPORT.md) is SUPERSEDED.** On the realistic
chain workload (the one the real node runs) TeraSlab loses badly. Full diagnosis:
**`bench/RECIPE_CHAIN_FINDINGS.md`** (read this first). Raw:
`bench/results/20260628-recipe-chain/`.

- Rebuilt the head-to-head on the real **`bitcoin-sv/teranode-coinbase` txblaster2**
  chain model (N goroutine chains, Create-LOCKED→Spend→unlock→decorate, wait per op,
  advance; SetMined burst + prune overlays). 0 failures both backends. Harness:
  teranode-bench-wt `cmd/utxobench/bench_test.go` RECIPE=1 (committed b0162e2a0).
- **Result (10k chains, i3en.6xlarge, NVMe, async):** TeraSlab ~9.4k links/s,
  create p99 4,600ms · Reference ~40.7k links/s, p99 130ms. **~4-5× slower.**
- **Root cause: NOT hardware.** Server is CPU-IDLE under load (<1 of 24 cores, all
  dispatch threads ~0%) → starved/latency-bound. The limiter is the
  request/connection-parallelism path: the Go client (client/go/pool.go) opens
  one-conn-per-concurrent-caller (231 conns at pool_size=64), storms past the
  server per-IP cap (1024) at higher pools → failures, and underuses the server's
  pipelined dispatch pool (192 workers, pipeline_depth=16).
- **Levers tested:** raising conn caps + pool_size→512-1024 fixed the TAIL
  (p99 4600→450ms, p50 128→50ms, srvCPU→1.8 cores) but throughput stayed ~7.5k +
  introduced ~2k dial-storm failures. TCP_NODELAY on client (committed abc092b,
  correct but not dominant). redo/writeback interval + batch window = minor.

**PROGRESS (2 fixes landed + measured, both committed on feat/device-cache):**
- ✅ **Server writeback** (6c97a37, src/cache.rs): single-thread flusher cloning
  every dirty block each tick was the 1-core ceiling → parallel per-shard flush
  (rayon pool) + Arc/CoW block data. Measured: server 1.8→3.2 cores, create p50
  84→28ms, p99 449→193ms (better than ref p50). 3008 tests green.
- ✅ **Client dial-storm** (3381397, client/go/pool.go+conn.go): one-conn-per-caller
  → bounded pre-warmed pool + per-conn pipelining (depth 16), capped non-fatal
  dials. Measured: op failures 2,777→0 across pool 64–1024.
- ❌ **Throughput STILL ~9k links/s** (~35k store-ops/s) — UNCHANGED. The cap kept
  moving (server→failures→client). Scaling chains 10k→30k→50k only raised latency
  (p50 250→705ms), throughput flat, server idle (0.6 core): **a HARD client-stack
  serialization at ~35k store-ops/s**. Reference does 40.7k links/s (~163k
  store-ops/s) through ITS client — TeraSlab's adapter+client is ~4.5× slower.

**PROFILED the client + a 3rd fix landed (committed, UNMEASURED — box reclaimed):**
- pprof of the harness: client ~1 core, NOT CPU-bound, drowning in Go
  scheduler/lock contention. create/spend WERE coalesced; **unlock(SetLocked) +
  get(BatchDecorate) went to the wire as SINGLE-ITEM RPCs** (2 of 4 ops/link) →
  the contention storm. (artifacts: bench/results/20260628-recipe-chain/client-*.prof/txt)
- ✅ Fix #3 (teranode-bench-wt commit 8031f0bc8): **coalesce SetLocked +
  BatchDecorate** into wire batches (modeled on spendBatcher; group SetLocked by
  value, union decorate txids+masks). Matches production (user: the Teranode
  adapter coalesces every op into a batch). 8 tests, builds clean. **NOT yet
  measured** — the spot box was reclaimed by AWS before re-deploy.

**REFERENCE CLIENT ANALYSIS (user directive, read-only inspiration) →
bench/REFERENCE_CLIENT_ANALYSIS.md:** the reference sustains ~163k store-ops/s over
a LIMITED pool because its client has NO central coordination: **sharded conn
pool (sub-heaps, Poll/Offer by hint%N, ~100 conns), connection-per-command
synchronous (no pipelining/no central batcher/no per-req demux), non-blocking
acquire+async-grow+retry.** TeraSlab's client funnels everything through the
adapter go-batcher's single channel+worker + a global pool mutex + per-req
machinery → the ~1-core contention. **Redesign plan (in that doc): shard the
teraslab client conn pool by hint; de-funnel the batcher (M lanes or lock-free
accumulate); bounded pool + non-blocking acquire; consider simpler
conn-per-request over a sharded pool.**

**MEASURED 3 fixes (2026-06-28 PM, fresh box, all committed):**
- coalescing (8031f0bc8): 8.7k→11.6k links/s (+33%), create p99 ~2000→78ms (25×), f=0.
- client pool sharding (de52542): ~0 gain → **pool lock ruled out**.
- re-profile (cli2.prof): server now 2.8 cores (fed at last); **client is GC +
  contention bound** — mallocgc 25.9%, selectgo/lock2/futex/lfstack.
- **NET: 8.7k → ~12k links/s (+38%), p99 28× tighter. STILL ~3.4× behind the
  reference (40.7k links/s).** Both have idle cores → cap = client
  coordination+allocation, not compute.

**⚠ GREEN-GATE TODO before any FINAL_REPORT:** the latest server changes
(src/device.rs fallocate, src/cache.rs shard count) passed module tests on Linux
(device 69/0, cache 13/0) but the FULL gate was NOT re-run with them — run
`cargo test --all` + `cargo clippy --all -- -D warnings` + `cargo fmt --check` +
`cargo test --manifest-path client/rust/Cargo.toml --all` (per memory
feedback_rust_prepush_checks) before declaring a win.

**ALSO DONE (commit d9d1c65): cache dirty-index** — flush_shard was ~45% on-CPU
(Vec::from_iter scanning all blocks/tick); now O(dirty) via a per-shard dirty
HashSet (lockstep w/ Block.dirty, invariant-tested). Server flush CPU 6.5→3.4
cores; throughput NEUTRAL (~28k, cap is locks not flush-CPU). cargo test --all 0
failed. Kept for efficiency/headroom.

**⭐⭐⭐ BREAKTHROUGH (2026-06-29 latest) — TeraSlab BEATS the reference on THROUGHPUT
at production concurrency.** The whole campaign benchmarked at 10k chains; the user
said the real workload is 100k+ threads. At 100k+ chains (fair 32GiB device both —
4GiB filled at high conc for both): **TS spend 41k/s f0 > REF 40.6k/s (23k fails);
150k → TS 43-44k f0 > REF 40.4k (24-69k fails).** TS throughput CLIMBS with
concurrency + stays f0; REF saturates ~40.7k and sheds load above ~30k chains.
Reproduced 3× at 100k. Full detail: **bench/HIGH_CONCURRENCY_RESULT.md**; raw:
bench/results/20260629-highconc-h2h/.
- **Pass condition: THROUGHPUT half MET. p99.9 half NOT yet** — TS p99.9 ~1.3s vs
  REF ~0.18s, but that's a CLOSED-LOOP overload artifact (TS queues+completes all;
  REF fail-fast sheds ~30% so its survivors look fast). Not a fair tail metric.
- **DECISIVE REMAINING STEP for a clean FINAL_REPORT win: OPEN-LOOP fixed-rate
  test** — rate-limit the recipe to ~35-38k/s offered (below saturation) for BOTH
  backends, compare p99.9 + failures fairly. If TS p99.9 ≤ REF at that rate with
  f0 both → throughput-ceiling win + tail parity = defensible win → FINAL_REPORT.
  Needs a rate-controlled variant of runRecipe (token bucket per worker / global
  ticker) — focused Go harness change. ALSO fix multi-round flakiness (24M
  expected_records → per-round init exceeds 40s health timeout under load; raise
  timeout or lower re-init) and the redb-wipe-between-rounds (now patched in
  /tmp/h2h_recipe.sh but verify).

**p99.9 INVESTIGATION — state at 2026-06-29 PM (the open blocker to the suite win):**
Open-loop fixed-rate tests settled the tail question: with the closed-loop confound
removed the **reference wins p99.9 by ~3×** (TS spend p99.9 114ms vs REF 38ms @8.5k
links/s; both f0). Localized to a **uniform ~71ms TeraSlab SERVER per-op latency
floor even at idle** (1500 links/s ≈ 10% util): mock-server-via-same-client = 3.4ms,
real server = 75ms ⇒ the floor is server-side, NOT the Go client/batcher/rate-limiter.
**GET (read-only, no redo/no writeback/no fsync) is also ~80ms** — so the floor is in
the COMMON request path, not the write path.
- **REFUTED levers (each tested, no p99.9 change):** redo flush + writeback interval
  (5ms → no change); TCP Nagle (server set_nodelay mod.rs:602, client too); CPU
  saturation (~10% util at 1500 links/s); the Go create-batcher (3ms yet create
  75ms); replication-fanout permit wait (replication-only); **mimalloc global
  allocator (STASHED stash@{0}, same-box NO improvement — the count-based futex
  profile that flagged glibc malloc-arena was misleading: it counts frequent-short
  malloc locks + idle-worker condvar parks, not the one long ~71ms per-op wait).**
- **RULED OUT BY CODE-READ:** the connection read loop (mod.rs:940), the sharded
  `DispatchPool` (submit→notify_one→recv, mod.rs:1588/1598), `ConnInFlight`
  backpressure, `write_response` (direct `write_all`), and `group_commit_window`
  (= `Duration::ZERO`, dispatch.rs:2208) are ALL clean. The floor is NOT in the
  server plumbing layer.

**★ ROOT CAUSE PINNED LOCALLY (2026-06-29, box-free, native macOS) ★**
Reproduced the floor on a native macOS server with the Go RECIPE=1 harness (no EC2):
`/tmp/ts-local/` (config + `run_probe.sh`). Server build runs on darwin.
- **The floor is NOT uniform — it is the DELETE op.** Closed-loop, NO rate limiter,
  256 workers: create/spend/get/unlock **p50 = 3–4 ms** (= mock-server ceiling), but
  **delete p50 = 447 ms**, and the other ops' **p99 ≈ 450 ms** (queued behind a
  delete stall). In the rate-limited 1:1:1:1 recipe the constant deletes SMEAR the
  stall across every op → on EC2 it *looked* like a uniform ~71 ms floor. macOS
  447 ms / NVMe ~71 ms is the slow-fsync ratio.
- **Why delete is slow: the DELETE path is synchronous + uncached while create/spend
  are async (write-back cache).** Three distinct per-delete costs, confirmed by
  experiment (gating each under buffered) + `sample`(1) of the server under load:
  1. **data-device `device.sync()`** — `delete_inner` step 3, engine.rs:7007. A full
     fsync per delete. Gating it under `redo_buffered()` dropped 447→~340 ms.
     *Safe to defer:* the checkpoint barrier already fsyncs the data device before
     redo reclaim (checkpoint.rs:637), exactly like create/spend.
  2. **tombstone `append_synced`** — `append_delete_tombstone`, engine.rs:7346. A
     second fsync (tombstone-log device). Gating to the unsynced `append` dropped to
     ~245 ms. *NOT safe to defer alone:* the checkpoint barrier does NOT currently
     fsync the tombstone log — deferring needs a matching tombstone-log sync added to
     the checkpoint barrier (else a crash loses tombstones the reclaimed redo no
     longer carries).
  3. **`append_deleted_child` (parent-prune) direct device I/O** — engine.rs:5965,
     the remaining ~245 ms. `sample` showed it dominates: per deleted child it does
     `allocate_deleted_children_block` → **direct O_DIRECT/F_NOCACHE `pwrite`** of a
     fresh children block + `write_metadata_fast` + a per-child redo
     `append_and_flush` (engine.rs:6018) — none routed through the write-back cache.
     Skipping redb (step 6, already `Durability::Eventual`) changed nothing → redb is
     NOT a factor.
- **THE FIX (durability-critical, multi-part — needs the careful TDD pass):** make
  the delete path async like create/spend: (1) defer data-device sync under buffered
  [safe today]; (2) defer tombstone fsync under buffered AND add a tombstone-log sync
  to the checkpoint barrier; (3) route `append_deleted_child`'s block + metadata
  writes through the write-back cache and use a buffered (no-flush) redo append under
  buffered mode. Expected result: delete → ~4 ms like create/spend → the p99.9 floor
  disappears → likely wins the tail. Experiments reverted (incomplete/unsafe as-is);
  tree clean.
- **EC2 ACCESS IS BROKEN (action needed):** the `teraslab-test-user` IAM key in
  `.aws/ec-credentials` lacks ec2:DescribeInstances / ec2:TerminateInstances, and the
  SSH key `~/.ssh/teraslab-perftest-key.pem` is now REJECTED by 44.201.221.186 (port
  22 open but publickey denied — likely the box hit its 120-min shutdown backstop and
  AWS recycled the public IP to another tenant). **Last launched perftest box:
  i-07116114bc43684bd (i3en.3xlarge, us-east-1).** Cannot verify/terminate it from
  here — manual check via the AWS console recommended. core-m-demo
  (i-0dd1b439a6b470c4f) must stay untouched.

**⚠⚠ [superseded by the breakthrough above] DIAGNOSIS REOPENED (2026-06-29): the cap is probably NOT lock
contention.** Buffered redo group-commit batching (coalesce concurrent commits →
one log.lock() instead of per-commit; fully implemented + tested, STASHED) gave
**0% same-box** (27.7k vs 27.5k HEAD). If cutting ~512 redo lock acquisitions/batch
does nothing, lock-acquisition COUNT is not the cap → the `__futex_wait` samples are
likely PARKED-IDLE workers (closed-loop, few requests in flight because each op's
response is slow), not contention. True cap = per-op SERVER latency limiting offered
concurrency (server uses ~5-6 of 24 cores; NOT cpu/device/lock-count/client bound).
**NEXT = off-CPU WAKEUP profiling** (offcputime / perf sched / `perf record -e
sched:sched_switch --call-graph dwarf`) to learn what the parked workers wait for:
the next REQUEST (limiter upstream: conn read / dispatch enqueue / client in-flight
depth) vs a server-side dependency (condvar/flush/barrier). Do NOT make more blind
lock changes (redo-batching 0% proves it's the wrong tree). See
RECIPE_CHAIN_FINDINGS.md §DIAGNOSIS REOPENED. (Older lock-contention next-levers
below are superseded.)

**CORRECTION (read the code): the "encode-under-lock" hypothesis is WRONG — redo
encode is ALREADY outside the lock (E7 optimization in `redo_group.rs::commit`:
`pre_encode` runs lock-free, only an O(1) finalize/memcpy runs under the per-store
log lock).** So the futex wait is on the per-store redo log lock ITSELF (only 4
locks for ~115k writes/s; device_split>4 tested WORSE) + distributed index/cache
locks on the read path (`handle_get_batch` waits on futex with NO redo involved →
index/cache lock, not redo). The remaining ~1.4× is **distributed lock contention
across a write/read path whose individual locks are ALREADY optimized** (redo E7,
index lock_stripes=65536, cache sharded cores*16 + dirty-index). No single cheap
fix remains; closing it likely needs deeper architecture — lock-free / per-thread
redo append buffers merged at flush, or reducing the number of distinct locks each
op takes (index+redo+cache+allocator per create). Treat as a DEEP change: prototype
+ profile + measure carefully; the gap is not yet proven fundamental but the easy
levers are exhausted.

**[earlier, now corrected] NEXT LEVER — the ~1.4× gap is LOCK CONTENTION (futex), profiled.** futex-caller
profile (bench/results/20260628-recipe-chain/futex-callers.txt) ranks the contended
sites: dispatch `handle_request`/`handle_create_batch`/`handle_get_batch`/
`handle_spend_batch` + **redo** (`RedoOp::serialize_data`, `RedoEntry::pre_encode`,
`RedoEntry::serialize`, `append_preencoded_atomic`, `redo_group::GroupCommit::commit`)
+ cache. Strong hypothesis: the **per-store redo append Mutex** serializes writes —
and redo ENCODE (serialize/pre_encode) may be happening UNDER that lock; moving the
encode OUTSIDE the lock (lock only covers the ring memcpy) should cut hold time.
Confirmed NOT the cap: device I/O (fallocate fixed it), cache flush-scan CPU
(dirty-index removed it, no throughput change), cache shard count (+2%), device_split
(4 optimal), the whole client pipeline (303k ceiling). DO NOT change redo blind —
re-provision, perf the futex callers at high res, confirm the encode-under-lock
hypothesis (read src/redo.rs append_preencoded_atomic + engine.rs journal path +
redo_group GroupCommit), make the smallest durability-safe change (encode outside
lock / stripe append), re-measure. If it wins: write FINAL_REPORT + run the green
gate (cargo test --all + clippy + fmt + client/rust tests).

**⭐⭐ 2026-06-29 BIGGEST WIN — fallocate device file → +110% (2×), gap 3×→1.4×:**
Off-CPU+perf profiling showed the CPU-idle server's writes hit `ext4_mb_new_blocks`
(ext4 block allocation) because the data file was SPARSE (`set_len`). Fix: fallocate
the file (commit 5b82b2e, src/device.rs, Linux best-effort). **Measured: 13,666 →
28,742 links/s (+110%); server CPU 0.6 → 5.8 cores; 0 fail; device tests 69/0.**
Now 28.7k vs reference 40.7k (~1.4×, was ~3×). device_split=4 optimal (8/12 worse).
Remaining cap = cache-shard mutex + redo lock contention (`__futex_wait`; perf:
cache::flush_block vs CachingDevice::pread + redo::prepare_flush). NEXT LEVER:
reduce cache/redo lock contention (separate flush vs serving lock domains; finer
buckets; redo append batching). See RECIPE_CHAIN_FINDINGS.md §BIGGEST WIN.

**[earlier] 2026-06-29 ISOLATION TEST CORRECTED THE DIAGNOSIS — bottleneck is the SERVER:**
A mock-server test (teranode-bench-wt `TestRecipeMockServer`, commit 7f27ecf80) runs
the recipe loadgen through the REAL client+adapter into an instant-success mock.
**Client pipeline ceiling = ~303k links/s (1.2M store-ops/s), p50 8ms** (mac 8-core)
vs ~13.7k against the real teraslab server (24-core). **The client has ~22× headroom
— it is NOT the bottleneck.** A CPU-idle (~0.6 core) teraslab server servicing only
~55k store-ops/s is **SERVER-side latency/serialization-bound**. Reference server =
~163k store-ops/s (~3×). So the client work (pool sharding/transport rewrite/batcher)
was marginal because the client was never the cap; the writeback fix was right-kind.
**REFRAMED NEXT LEVER: profile the SERVER's per-request latency (off-CPU/wakeup, NOT
on-CPU) under the chain workload — why idle-but-slow? dispatch pickup, redo
group-commit/per-op wait (cf 200µs redo-sleep), writeback/checkpoint, lock handoff.**
See RECIPE_CHAIN_FINDINGS.md §DECISIVE ISOLATION. (Superhseded below kept for history.)

**[SUPERSEDED] earlier (wrong) lever (cum profile reprioritized this):** the client CPU is
dominated by SHARED harness tx-build (`makeSpendTx`) + GC-assist, which both
backends pay equally — so cutting allocs / de-funneling won't close the gap (pool
sharding already = 0). The differentiator is TeraSlab's transport runs far more
goroutines/coordination (per-conn readLoop ×N + go-batcher workers +
done-channel+pending-map per request). **Redesign the teraslab Go client transport
to synchronous conn-per-command over the sharded bounded pool** (drop
readLoop-per-conn / pending-map / per-request channel → goroutines collapse toward
just the callers; keep adapter coalescing). See RECIPE_CHAIN_FINDINGS.md §NEXT +
REFERENCE_CLIENT_ANALYSIS.md. Then re-run chain h2h fresh-per-round, strict 4/4.

**EC2: torn down after this round** (measurements captured). Re-provision via the
recipe above for the next measure. Server writeback fix + client coalescing/
sharding are committed, so a fresh box only needs deploy+measure of the NEXT
client allocation/batcher changes.

**EC2:** all perftest boxes TERMINATED (none running). off-limits core-m-demo
i-0dd1b439a6b470c4f untouched. To re-provision: key `teraslab-perftest-key`
(/tmp/teraslab-perftest-key.pem) + SG sg-001ae8932446f7499 still exist in
us-east-1; AMI ami-08f44e8eca9095668; bootstrap=/tmp/ts_bootstrap.sh; runners
/tmp/ts_run.sh (TS-only) + /tmp/h2h_recipe.sh (h2h). Deploy = git archive HEAD →
build server; rsync teranode-bench-wt (fix go.mod replace → /home/ec2-user/
teraslab/client/go); `go test -c -tags utxobench`; configs: derive ts-bigconn.toml
(max_connections[_per_ip]=16384) from bench/configs/teraslab-async.toml.

## ⚠ UNCOMMITTED WORK — survives only if noted (lives OUTSIDE this repo)

In `/Users/siggioskarsson/gitcheckout/teranode-bench-wt` (integration worktree,
branch teraslab/integration-wip), working-tree changes, NOT committed:
1. **`cmd/utxobench/bench_test.go`** (untracked, 1112 lines) — the **Go head-to-head
   harness rebuilt to the recipe** (`RECIPE=1` mode, `runRecipe`): independent
   per-op goroutine streams (create/unlock/spend/read/delete) + a setMined-burst
   goroutine, causal UTXO graph driven through Teranode `utxo.Store` so BOTH backends
   run it. Compiles + vets + gofmt clean (`GOTOOLCHAIN=auto go test -c -tags utxobench`).
   Env: `RECIPE=1 RECIPE_WORKERS RECIPE_BURST_INTERVAL_SECS RECIPE_SETMINED_CHUNK`
   (+ DURATION_SEC/WARMUP_SEC/BACKEND/TARGET). Emits UTXOBENCH_RESULT JSON
   (create/unlock/spend/get/delete + burst section).
2. **`stores/utxo/teraslab/convert.go`** — Go locked-on-create now uses named
   `teraslab.FlagLocked/FlagConflicting/FlagFrozen` (was magic 0x01/0x02/0x04).
   Builds clean.
3. Pre-existing (predate this session): go.mod `replace github.com/icellan/teraslab/client/go`
   → the teraslab worktree's client/go; adapter perf bits (batcher/spend/pruner).

## Commits this campaign (feat/device-cache, all green)

8fd086b SipHash→fast-hasher (create CPU) · 7e2c40e de-flake redo test ·
1020125 BytesMut per-frame realloc fix (-31% on-CPU) · 68c120f opt-in txid→store
placement · **51bb8b2 per-store dispatch sharding (the cap-breaker)** · 5359ef3 E23
ledger + bench configs `placement="txid"` · 641972f Linux/NVMe report · 680e3a6
FINAL_REPORT (the win) · 6ef6e94 FINAL_REPORT loadgen-bound caveat · 8851997→**d1e64fa
recipe loadgen (causal UTXO streams)** · 86a6257 named CREATE-wire flag constants ·
e100363 setMined RAM-index O(1) (Vec→HashSet height buckets) · f5cc761 E26 ledger.

## TO CLOSE THE GOAL — realistic NVMe head-to-head (needs a spot box; user's go)

1. Spin a spot ≥24-core NVMe box (ALWAYS spot — see memory feedback_always_spot…).
   i3en.6xlarge worked. AMI `ami-08f44e8eca9095668` (AL2023 k6.18). Recreate key
   `teraslab-perftest-key` + SG (ssh from your IP). Tag Project=teraslab-perftest;
   record the instance ID; **terminate ONLY that ID** (off-limits: `core-m-demo`
   i-0dd1b439a6b470c4f — never touch). 6h shutdown backstop. AWS: default profile
   `core-m-deployer` (has EC2; lacks ssm/servicequotas/iam). Spot needs the
   AWSServiceRoleForEC2Spot SLR (user granted core-m-deployer iam:CreateServiceLinkedRole).
2. Provision: dnf gcc/git/docker; Go 1.26 via official tarball (GOSUMDB=off blocks
   toolchain auto-download — install /usr/local/go directly); rust via rustup.
   git archive HEAD → build teraslab-server. rsync teranode-bench-wt EXCLUDING
   .git/data/bin (NOT test/ — util/test is needed!); fix go.mod replace →
   /home/ec2-user/teraslab/client/go; `go test -c -tags utxobench` → utxobench.test.
   pull the reference datastore's official container; its config lives in the
   (out-of-repo) harness tree with `memory-size` stripped, mapped port 13000:3000.
3. Format one NVMe ext4 → /data; both backends' data on /data (fair). TeraSlab
   native: bench/configs/teraslab-async.toml (device /data/d0.dat, device_split=4,
   placement=txid, buffered redo + writeback cache).
4. Run BOTH with `RECIPE=1` (the new harness), interleaved fresh-per-round (random
   txids → no cross-run collision, but fresh keeps it bounded), capture per-round
   JSON + uptime; analyze medians±stdev → strict 4/4 (METHODOLOGY §pass-condition;
   /tmp/h2h_stats.py supported --keep for clean-round filtering).
5. Also confirm setMined-burst is fast on NVMe (it's a macOS host artifact, see below).
   If TeraSlab wins → update FINAL_REPORT (realistic-workload win) + ledger + commit.

## Key findings (don't re-derive)

- **The cap was a software funnel**, not hardware: one shared `DispatchPool` queue
  (mod.rs:1407) capped ~40-48k ops/s with CPU 30% idle. Per-store dispatch sharding
  (51bb8b2, routes by hash(txid) last bytes) broke it → the win. device_split alone
  did NOT help (funnel is upstream of stores).
- **setMined is lazy-by-design and correct**: buffered mode updates the RAM index
  (now O(1), e100363) + appends the buffered redo (no ack-path fsync,
  `journal_secondary_ops` append_atomic) + writes the record via the writeback cache
  (lazy). The macOS-Docker setMined-BURST multi-second stall is the **device-writeback
  HOST artifact** (cache flusher can't keep up on the slow Docker VM), NOT the ack
  path — confirm fast on NVMe.
- **macOS-Docker cannot certify tails** (never idle; O_DIRECT fsync freezes the VM).
  All p99.9/burst conclusions need NVMe. The 4/4 win required the quiet EC2 host.
- **Flag footgun (fixed)**: CREATE wire bits (locked=0x01/conflicting=0x02/frozen=0x04)
  differ from persisted TxFlags (LOCKED=0x04); now named constants everywhere.

## Recipe workload model (utxo-db-benchmark-recipe.md)

Causal UTXO graph, 1-in/1-out: create tx LOCKED w/ 1 output → unlock just-created →
spend a prior tx's output (check OK) → setMined all txids created since last burst
(every ~6min, 1024-record bursts, 1 client) → delete spent+mined. Steady streams
~1:1:1:1 (create/spend/read/delete), per-op batch sizes create=488/spend=329/
read(decorate parent)=291/delete=488/setmined=1024; pre-load a working set; the burst
stacks on the steady baseline. The Rust loadgen (`teraslab-loadgen --recipe`) and the
Go harness (RECIPE=1) both implement this.

## Deliverables status
METHODOLOGY.md ✓ (incl. explicit pass condition, defined before tuning).
PERF_LEDGER.md ✓ (B0→E26). FINAL_REPORT.md ✓ (un-batched 4/4 win, caveated).
bench/results/ ✓. cargo test --all + clippy -D warnings + fmt + client tests ✓.
opponent-name grep empty ✓. Docker e2e: cargo --all integration (cluster/repl/recovery)
green; full docker-compose e2e not re-run this session (recommend before release).
