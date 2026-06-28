# RESUME — TeraSlab vs the reference datastore (perf campaign)

**Start here after a context reset.** Full chronological detail: `PERF_LEDGER.md`
(B0 → E26). Fairness/setup + pass condition: `METHODOLOGY.md`. The proven win:
`FINAL_REPORT.md`. Raw cert runs: `bench/results/`. Branch `feat/device-cache`,
worktree `.worktrees/device-cache`. Constraint: never name the reference product
in-repo (`git grep -i` stays empty — verified); call it "the reference datastore".

## CURRENT STATE (2026-06-28)

**WON the un-batched head-to-head (strict 4/4), committed + reproducible.** On a
quiet 24-core EC2 NVMe host TeraSlab beats the reference on EVERY priority op,
throughput AND p99.9, 0 failures (10 interleaved fresh-per-round; FINAL_REPORT.md,
raw in bench/results/20260628-ec2-quiet-cert/):
spend 15,012 vs 13,303/s (+12.9%), p99.9 15.6 vs 22.6ms; create +12.9%; get +13%;
setmined +13% / p99.9 1.2 vs 4.4ms. The decisive fix was **per-store dispatch
sharding** (51bb8b2) breaking the global DispatchPool funnel (~40-48k cap, CPU 30%
idle).

**THE OPEN GAP (what the Stop hook wants):** that win is on the harness's
**un-batched** workload (~50k, concurrency/latency-bound — both backends hit it, so
it under-tests capacity). The goal also wants the **batched / production-churn
(realistic recipe)** workload. That head-to-head has NOT been run. Everything for it
is built; only the NVMe run remains (a cost decision the user is steering).

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
   docker pull aerospike/aerospike-server:latest; conf = teranode-bench-wt/test/
   aerospike/aerospike.conf with `memory-size` stripped, port 13000:3000.
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
