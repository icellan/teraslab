# Cluster E2E Flakiness Audit

Diagnostic audit of the Docker-based cluster/failover/recovery scenario tests
(`teraslab-tests/`) against the cluster implementation (`src/cluster/`,
`src/replication/`, `src/server/dispatch.rs`). **Diagnosis only — no production
or test code was changed.** All line numbers at `main` HEAD (`0a0d538`).

Evidence base: source reading of both sides, plus the `docker-e2e-pr-logs`
artifacts (scenario logs, per-node server logs, SUMMARY.md) downloaded from the
last 10 CI runs of the `Docker cluster E2E (PR tier)` job.

---

## 0. Observed failure data (last 10 CI runs)

| Run | Commit | sc01 | sc02 | sc03 | Notes |
|---|---|---|---|---|---|
| 26519140885 (May 27) | pre-fix | FAIL | FAIL | FAIL | |
| 26520052275 (May 27) | pre-fix | FAIL | FAIL | FAIL | |
| 26544911140 (May 27) | pre-fix | FAIL | FAIL | FAIL | |
| 26562931156 (May 28) | pre-fix | PASS | FAIL | PASS | |
| 27331523660 (Jun 11) | pre-fix | FAIL | FAIL | FAIL | |
| 27334138308 (Jun 11) | pre-fix | FAIL | FAIL | PASS | |
| 27343736454 (Jun 11) | `5f47b2a` | PASS | FAIL | PASS | |
| 27406259248 (Jun 12) | `65fcde8` | PASS | PASS | PASS | **first run containing C-1/D-6 fix `e6e65f0`** |
| 27407128516 (Jun 12) | `0a0d538` | FAIL | PASS | PASS | sc02 29 s, sc03 36 s (3-4× faster than pre-fix passes) |

Failure signatures, from the artifacts:

- **Scenario 01 failures: always duration 123 s with an empty panic column** —
  the harness `timeout 120` (run_all.sh:53) killed it. One PASSING run took
  112 s. The log shows ~60 s of `rustc` output *inside* the timeout window.
- **Scenario 02/03 failures (pre-fix only): `server error 20: replication: 0/1
  replicas ACKed, need 1: recv_ack: replica timeout after 3s`**, repeating
  every ~4 s for up to 16 retries (~100 s). Node logs show the replica
  *receiving* the op-240 batches (per-frame accept WARN) and applying some
  ("reconciled 44 ambiguous existing record(s)"), while the master's outbound
  port churns each cycle (reconnect per attempt). In run 27343736454 the wedge
  began 3 s after a `cluster: migration plan masters=2732 replicas=1365
  backfill=0 outbound=2049` fired mid-test on a settled 3-node cluster.

The error string `replication: {n}/{m} replicas ACKed, need {k}` was deleted by
commit `e6e65f0` (C-1/D-6); every run that produced it was built from pre-fix
code. Both post-fix runs pass scenarios 02/03. **The dominant historical
product bug is identified and already fixed; what remains flaky at HEAD is
scenario 01's timing budget plus the residual product bugs in §5.**

---

## 1. Scenario inventory

PR tier = 01–03; nightly adds 04–11, 17; weekly adds 12–15; release adds 16
(run_all.sh:24-29). All scenarios: RF=2, `swim_probe_interval_ms=150`,
`swim_suspicion_timeout_ms=1000` (5× more aggressive than the shipped defaults
200/5000, src/config.rs:804-805), no `ack_policy` key → `auto` → **WriteAll**
for RF=2, `replication_timeout_ms` not set → default 3000 ms,
`replication_degraded_mode` default `reject`.

Fault injection is all via Rust `DockerHelpers`, not the shell scripts:
`kill_node` = `docker kill --signal=SIGKILL` (client/src/helpers.rs:429);
`stop_node` = `docker stop --time=1` — doc comment claims 10 s but code gives
SIGTERM only **1 s** before SIGKILL (helpers.rs:442-452); partitions =
in-container `iptables -A INPUT/OUTPUT -j DROP` (helpers.rs:506-583); `tc
netem` for latency/loss. Toxiproxy compose file and
`scripts/{kill_node,partition_network,heal_network,slow_network}.sh` are
**dead code** — they reference the legacy `teraslab-node${N}`/`172.30.0.x`
naming that no scenario uses.

| # | Name | Topology | Fault | Key waits | Key asserts | Budget (internal) |
|---|---|---|---|---|---|---|
| 01 | Cluster formation | 3n ×4 bring-ups | none | `start_3node_cluster` (≤5×45 s ready + 120 s migrations); staggered/late-join variants; 30 s rebalance polls that **warn-and-proceed** (:335, :452); fixed `sleep(500ms)` for rogue-node check (:524, comment says 3 s) | 12 HTTP /status asserts; wire-level partition-map equality; master≠replica per shard; balance ±50 | **120 s (120 s)** |
| 02 | Basic operations | 3n | none | seed 1000×10 (16 retries, ~55 s budget); ~1100 sequential single-key reads | all 16 op types verified field-by-field; `verify_consistency`==0 | 300 s (300 s) |
| 03 | Replication correctness | 3n | none | seed 2000; `wait_replication_settled` **5 s fatal** ×5 (:72,:149,:195,:287,:320,:426) | exactly-2-holders + byte-equality on every record; 500 spends; 300 setMined | 300 s (300 s) |
| 04 | Node hard kill | 3n | SIGKILL ×2 | ready 30 s; migrations 60 s ×2; fixed sleeps 500 ms–3 s | reads after 1 retry; **<5 % failure rate over ~6 s kill window** (:536); 4.7 consistency check **masked to warn** (:386-393) | 300 s (300 s) |
| 05 | Recovery catch-up | 3n | SIGKILL + restart | ready 10 s; migrations 120 s; reads-ready 60 s; 30 s rebalance poll warn-and-proceed | **hard SLAs: membership ≤10 s, caught-up ≤60 s** (:311,:316); balance ±10 % | 600 s (600 s) |
| 06 | Scale up | 3→4n under load | join | ready 30 s; migrations 120 s; retry pass skipped if >50 stale reads (:309) | error rate <1 %; balance **±5 %**; no dupes/loss | 300 s (300 s) |
| 07 | Scale down | 4→3n | quiesce + `rm -f` | drain poll 120 s warn-and-proceed; migrations 120 s ×3 | masters 4096..4128; zero read failures | 300 s (300 s) — wait-sum can reach ~700 s |
| 08 | Network partition | 3n ×4 lifecycles | iptables ×3 variants + netem | ready 60–120 s ×3 attempts; migrations 180–300 s, 8b **"proceeding despite incomplete migrations"** (:486) | minority write-reject; zero post-heal read failures; RF2-exact | 900 s (900 s) |
| 09 | Rolling restart | 3n | quiesce + stop ×3 | per node: drain 60 s + ready 120 s + migrations 120 s + settle 60 s | `verify_consistency` with **spent_utxos mismatches filtered out** (:418-427) | **300 s (600 s) INVERTED** |
| 10 | Sustained load | 3n | none | 60 s load, checkpoints | RSS <20 % growth; p99 ratio ≤5 | 900 s (180 s) |
| 11 | Large transactions | 3n→4n | SIGKILL holder | migrations **300 s** wait (:712) | atomicity, large-payload integrity | **300 s (600 s) INVERTED** |
| 12 | Concurrent failures | 3n(+4th) ×5 lifecycles | dual kills, kill+partition, kill-during-join | `sleep(1s)` then assert write-fail in 3×500 ms tries; most 12.4 waits swallowed | quorum-loss rejection; consistency==0 ×5; RF2-exact | 900 s (600 s) |
| 13 | Migration under load | 3→4n @500 ops/s | join | seed **20 000**; ready 30 s; migrations 120 s | mismatches ≤max(3, 0.1 %); misroutes ≤5; balance ±100 | **300 s (600 s) INVERTED** |
| 14 | Split-brain prevention | 3n | partitions + flapping + pause | 14.3 hard 600 s drain (:501); 14.4 swallowed 30 s wait then hard assert | zero isolated-node writes; single-writer per shard; 14.3 tolerates **≤90 % NotFound** (:528-546) | 1200 s (900 s) |
| 15 | Crash recovery | 3n ×16 lifecycles | SIGKILL mid-batch (5 ms race), kill-all, cascade | per lifecycle: ready 30-45 s + migrations 120 s; **15.8:761 polls dead node1 instead of [2,3]** | consistency==0 each round; batch atomicity; RF2-exact | 1200 s (1200 s) |
| 16 | Chaos | 5n | randomized everything | checkpoint each 60 s: heal + ready 120 s ×3 + migrations 180 s | final consistency hard; checkpoints warn-only | 900 s (900 s) |
| 17 | Recovery hardening | 3n ×5 lifecycles | kill-during-migration races (500 ms sleeps) | ready 30 s; migrations 120 s; reads-ready 60 s — mostly hard-error (good) | consistency==0; RF2-exact; zero spend errors in degraded window (:545) | 1200 s (1200 s) |

Readiness surfaces used: `wait_cluster_ready` polls `/status` for
`cluster_size==n` + agreeing nonzero `shard_table_version` +
`master_shard_count>0` per node, every 50 ms (common/mod.rs:198-266);
`wait_migrations_complete` polls `/admin/migration_status` until
active==inbound==handoffs==0 and Σmasters==4096 for 3 consecutive polls
(:542-624); `wait_replication_settled` is a **quiescence heuristic** — redo
`current_sequence` unchanged for 2 polls / 100 ms (:802-841), not replica
equality. `/health/ready` itself only asserts "≥1 committed topology ever"
(src/cluster/coordinator.rs:5510-5514) plus recovery/lag gates
(src/server/http.rs:1271-1299) — the harness correctly does not rely on it for
convergence.

---

## 2. Timing-budget reconciliation

### The compile tax (root cause of scenario 01 failures)

`run_all.sh:132-134` runs `timeout $TIMEOUT cargo test --release --test
scenario_NN -- --nocapture` — **the test-binary compile happens inside the
timeout window**. The "Build test client" steps (run_all.sh:103,
ci.yml:162-164) run `cargo build --release`, which does **not** build test
targets or dev-deps. There is no `cargo test --no-run` prebuild anywhere.
Additionally `Swatinem/rust-cache@v2` with default config (ci.yml:157) caches
the root workspace's target dir, not `teraslab-tests/client/target` — so the
scenario-01 invocation pays dev-deps + the 2668-line `tests/common` + its own
binary + release link cold, every CI run: **~60 s observed** of the 120 s
budget. The remaining ~60 s must cover **four full cluster lifecycles**
(scenario 01 brings the cluster up four times). A passing run measured 112 s
total — an 8 s margin. Every observed sc01 failure is this budget, not
formation logic: killed at 123 s, no panic, formation never even got to fail.

Derived formation bound (5-node, from code): seed discovery ≤6.5 s
(swim.rs:579-581 retry backoff 100 ms→5 s cap) + up to 4 sequential topology
terms, each = proposal retries up to ~10 s (coordinator.rs:2552-2562,
2808-2811) + **hardcoded 2 s exchange phase** (coordinator.rs:1130) +
activation/migration work ⇒ 30–60 s is legitimate on a 2-core runner, before
counting CPU contention from the concurrently-running compile.

### Budget verdicts

| Scenario | Budget | Worst case derived | Verdict |
|---|---|---|---|
| 01 | 120 s | 60 s compile + 4 lifecycles (each ≤5×45 s ready + 120 s migrations) | **Provably too tight — primary live flake at HEAD** |
| 02 | 300 s | bring-up (worst 225 s w/ retries) + seed worst ~550 s | too tight under degradation; fine post-C-1 (passes in 29 s) |
| 03 | 300 s | seed 2000 worst ~1100 s of retry budget + 5× fatal 5 s settles | too tight under degradation; fine post-C-1 |
| 07 | 300 s | ~700 s of bounded waits | too tight |
| 09 | **300 s** | internal timeout 600 s; 3×(60+120+120+60) s waits | **inverted: harness kills before in-test timeout/teardown** |
| 11 | **300 s** | internal 600 s; single 300 s migration wait at :712 | **inverted** |
| 13 | **300 s** | internal 600 s; 20 000-record seed + migration under load | **inverted** |
| 15 | 1200 s | 16 cluster lifecycles × 45–165 s | tight worst-case |
| 04/05/06/08/12/14/16/17 | =internal | see §1 | zero headroom: internal==external means SIGKILL mid-teardown, no panic line, orphaned containers |

Where internal tokio timeout == run_all timeout (01, 02, 03, 04, 05, 06, 07,
08, 15, 16, 17), the in-test teardown/diagnostic path can never win the race —
all timeout failures appear as silent SIGKILLs with empty panic columns,
exactly as observed.

---

## 3. Race / ordering inventory

### R1 — Visibility-barrier deadlock across replication RTT (FIXED at HEAD)

**Class: REAL PRODUCT BUG (was CRITICAL; fixed by `e6e65f0` C-1/D-6).** The
dominant cause of the historical sc02/sc03 failures. Pre-fix, the engine-wide
exclusive `dispatch_visibility_barrier` was held across the synchronous
replication round-trip. Interleaving on the 3-node RF=2 cluster:

1. node A handles a create: takes its exclusive barrier, applies locally,
   sends `OP_REPLICA_BATCH` to B, blocks in `recv_ack` (3 s).
2. node B concurrently does the same toward A.
3. B's receiver reads A's frame off the socket (the op-240 accept WARN in the
   logs proves arrival) then blocks waiting for B's exclusive barrier — held
   by step 2. Symmetric on A. Deadlock broken only by both 3 s timeouts.
4. Timeout → connection dropped, NOT pooled (dispatch.rs:3025-3031 → the
   observed master-side port churn) → sequence positions burned → synchronous
   **compensation fan-out to the same wedged replica**
   (dispatch.rs:2872-2926) → second op-240 frame and second 3 s window per
   cycle.
5. Replica's blocked handler eventually applies and ACKs into the abandoned
   socket → write durable on the replica, error 20 at the client → the
   harness's `reconcile_existing_seed_records` finds it via read-back —
   the observed "reconciled N ambiguous existing record(s)".
6. Client backoff 500 ms·2^min(n,3) (common/mod.rs:1219) → the observed ~4 s
   cycle; 16 retries ≈ 100 s = `MAX_SEED_RETRIES` (:1143).

Post-fix evidence: both Jun 12 runs pass sc02/sc03 3-4× faster. The fix
releases the barrier before fan-out (dispatch.rs:1692-1694) and evaluates
quorum per key (dispatch.rs:1804-1835).

### R2 — Spontaneous full-cluster rebalance on a settled cluster (OPEN at HEAD)

**Class: REAL PRODUCT BUG.** Found independently by two reviewers. The
periodic reactivation loop (15 s/30 s cooldowns, coordinator.rs:1007-1082)
compares the active table against **pure round-robin**
`ShardTable::compute_with_epoch(committed_members, rf, 0)`
(coordinator.rs:277-290) — but Phase-F `apply_master_election`
(coordinator.rs:5309-5393, invoked at :1873) deliberately moves masters off
round-robin toward data holders. Every election-refined shard counts as
"mismatched" forever, so once migrations go idle the loop re-activates with an
**empty partition view** (coordinator.rs:1060-1079), recomputes raw
round-robin (silently reverting the election), and launches a migration plan
equal to the full diff — ~2/3 of 4096 ≈ 2731 masters for 3 nodes. This matches
the observed mid-scenario `migration plan masters=2732` on stable membership,
which then ignited R1 (pre-fix) and R3/R4 (still). Even at HEAD this causes a
multi-minute unprovoked migration storm in the middle of any scenario.

### R3 — Migration batches monopolize the receiver's exclusive barrier (OPEN)

**Class: REAL PRODUCT BUG.** Every `OP_REPLICA_BATCH` — including
migration-flagged ones — takes the engine-wide exclusive barrier for its whole
dispatch (dispatch.rs:408-417, 3260-3275); a migration batch holds it across a
1000-op apply + device fsync + redo flush (receiver.rs:952-988). With 128
migration connections funneling at one target (pool_size=128,
coordinator.rs:3367-3368), effective parallelism at the target is 1 and the
barrier queue stays ~100 deep: normal master→replica ACKs queue behind it and
blow the sender's 3 s timeout. Client reads on the target starve on the same
write-preferring RwLock. Non-migrating shards are collateral damage whenever
their replica lives on the migration target.

### R4 — Migration-pressure timeout escalation checks the wrong node (OPEN)

**Class: REAL PRODUCT BUG.** `replication_ack_timeout_for`
(dispatch.rs:1873-1883) escalates 3 s → `replication_timeout_during_migration_ms`
(30 s, config.rs:828) only when `migration_pressure_active()` is true **on the
sending master** (coordinator.rs:6246-6261 — local counts only). A master with
no local migration whose *replica* is the migration target keeps the 3 s
timeout precisely when the replica is slowest. The captured logs show 3 s (not
30 s) timeouts seconds after a cluster-wide plan — consistent with this gap.

### R5 — Fan-out permit exhaustion blocks writes to healthy replicas (OPEN)

**Class: REAL PRODUCT BUG (amplifier).** `MAX_REPLICATION_FANOUTS_IN_FLIGHT
= 128` with an **untimed condvar wait** (dispatch.rs:115-130). When 128
fan-outs are parked in 3 s timeouts against one wedged replica, writes to
*healthy* shards block in `acquire_replication_fanout_permit`. Also: the
per-replica-address slot mutex (dispatch.rs:3090-3092) serializes all batches
to one replica, so queued writes each wait multiples of 3 s; replica catch-up
shares the same slot mutex with 5 s-per-chunk holds (bin/server.rs:202-212).

### R6 — False death → double migration storm (OPEN; config-amplified)

**Class: PRODUCT BUG + test-config mismatch.** With the test configs'
probe=150 ms/suspicion=1000 ms, a node stalled ≈1.3–1.45 s is declared dead
(direct timeout 150 ms → suspect at 2×probe — swim.rs:194-203, 615 — then the
1 s suspicion clock, membership.rs:284-302). On a 2-core runner running 3-5
containers plus rustc, 1.3 s stalls are routine. Three product gaps make this
worse than it should be:
- **No SWIM refutation**: a node skips piggybacked updates about itself
  (swim.rs:899-901) and never bumps its incarnation to refute suspicion; no
  Lifeguard-style local-health multiplier exists.
- **Suspicion backoff is dead code**: `indirect_attempts` is incremented only
  in `send_indirect_probes`, which fires once per pending probe
  (swim.rs:1137-1139), so the documented exponential suspect backoff
  (swim.rs:194-203) never engages — effective suspect delay is always
  2×probe_interval.
- **Zero debounce**: `MembershipChanged` proposes a new topology immediately
  (coordinator.rs:1420-1430, topology.rs:907-992), and round-robin
  `members[shard % n]` (shards.rs:120-133) remaps ≈(1−1/n) of all 4096 shards
  on ANY size change. A flap costs **two** full migration storms (death leg +
  rejoin leg), each disrupting writes via R3/R4 for up to
  `MIGRATION_PRESSURE_GRACE` = 120 s (coordinator.rs:26).

### R7 — Error-20 ambiguity vs compensation (OPEN; correctness question)

**Class: spec mismatch with a possible REAL BUG underneath.** A write that
fails replication returns error 20 *after* the master has applied it locally
and durably; compensation then reverses it locally and replicates the
compensating op (dispatch.rs:2872-2926). Two issues:
- The harness's `reconcile_existing_seed_records` (common/mod.rs:1046-1084)
  treats "errored but found on read-back" as success — racing the pending
  compensation delete: a record counted as seeded can be compensation-deleted
  moments later. Test-side false-positive.
- After re-mastering, a pending compensation intent re-drives against the
  **new** topology; if the divergent replica became the shard's new master it
  is excluded from the replica fan-out (dispatch.rs:1565 iterates
  `assignment.replicas`) — the comp delete may never reach the node now
  serving the record. Unverified; needs a targeted test (flagged in §7).

### R8 — Harness asserts on transient state after warn-and-proceed waits

**Class: test-harness flake.** Pattern occurs in: scenario 01 rebalance polls
(:335-337, :452-454 — 30 s, then hard ±50 balance asserts), scenario 05
(:134-151 → hard ±10 % assert), scenario 08b ("proceeding despite incomplete
migrations" :486 → hard zero-read-failure assert, conflating product slow-drain
with data loss), scenario 12.4 (every stabilization wait swallowed before the
hard consistency gate), scenario 14.4 (swallowed 30 s wait → hard
`cluster_size≤2` assert).

### R9 — Retryable-error classification inconsistency

**Class: test-harness flake (amplifier) + contract ambiguity.** The client
retries only `ERR_MIGRATION_IN_PROGRESS` (19) and `ERR_STALE_EPOCH` (24)
(client/rust/src/lib.rs:2185-2187) plus one `ERR_NO_QUORUM` retry
(lib.rs:934-937). `ERR_REPLICATION_FAILED` (20) is non-retryable everywhere in
the client; `seed_records` retries it 16× at the harness layer, but every
post-seed mutation treats the first error 20 as instantly fatal
(scenario_02:174, scenario_03:140/:188/:313, scenario_17:545). Either error 20
is a retryable ambiguous outcome (then scenarios must retry it uniformly) or
it is a hard durability-contract violation (then `seed_records` masking 15
failures is wrong). The current half-and-half maximizes flakiness. See §7 Q3.

---

## 4. Determinism gaps in the harness

1. Compile inside scenario timeout; no `cargo test --no-run` prebuild
   (run_all.sh:132-134); rust-cache misses `teraslab-tests/client/target`
   (ci.yml:157).
2. Internal tokio timeouts equal to (01–08, 15–17) or exceeding (09, 11, 13)
   the external `timeout` — teardown/diagnostics suppressed on every timeout
   failure.
3. Fixed sleeps standing in for conditions: scenario_01:524 (500 ms rogue-node
   discovery; comment claims 3 s), scenario_04:512-517 (3 s windows),
   scenario_12:141/:356/:619 (`sleep(1s)` at the lower edge of the ~1.45 s
   death-declaration bound), scenario_15 kill-race 5 ms sleeps, scenario_17
   500 ms "mid-migration" timing — race-by-luck.
4. `wait_replication_settled` = 100 ms of redo-sequence quiet
   (common/mod.rs:802-841): can false-settle under CPU contention and its 5 s
   fatal deadline recurs ×5 in scenario 03.
5. Scenario 15.8:761 polls migrations on the **dead** node (`[1,3]`, node1 is
   the killed one; survivors are `[2,3]`) — guaranteed 30 s burn, masks
   whether survivors drained.
6. Masked oracles: scenario_04 4.7 prints "OK — zero mismatches"
   unconditionally (:386-393); scenario_09 filters `spent_utxos` mismatches
   out of `verify_consistency` (:418-427); scenario_14.3 tolerates ≤90 %
   NotFound (:528-546). Each documents a suspected product bug instead of
   tracking it.
7. Teardown: run_all cleanup + `wait_docker_ready` warn-and-proceed at 30 s
   (run_all.sh:84); `force_cleanup`/`wait_ports_free` all bounded
   best-effort — next scenario can start on a draining daemon.
8. Tracked-but-regenerated artifacts: `docker-compose.ts*.yml` +
   `docker/config/ts*.toml` are overwritten at runtime by
   `DockerHelpers::ensure_compose_file` (client/src/helpers.rs:346-368); the
   tracked copies embed a dev-machine absolute path.
9. Stale comments that misstate timing: seed retries "8 attempts ~30s" vs
   actual 16/~55 s (common/mod.rs:1139-1140); `stop_node` "10 s" vs `--time=1`
   (helpers.rs:442-452); rogue-node "3 s" vs 500 ms (scenario_01:522-524).
10. Shared-runner contention as a config blind spot: the test configs override
    SWIM timing (5× aggressive) and auth, but leave `replication_timeout_ms`
    at the production 3000 ms and ack_policy at WriteAll — the exact pair that
    converts a 3 s scheduler stall into a client-visible error 20.

---

## 5. Severity-ranked findings

| ID | Finding | Severity | Status |
|---|---|---|---|
| P1 | Visibility barrier held across replication RTT → bidirectional deadlock, error-20 wedge (R1) | **CRITICAL** (availability collapse; ambiguous-outcome writes) | **FIXED** at HEAD (`e6e65f0`); validated by 2/2 post-fix CI runs |
| P2 | Reactivation loop vs master election → spontaneous ~2700-shard migration storms on settled clusters (R2) | **HIGH** | OPEN |
| P3 | Migration batches hold the engine-wide exclusive barrier per 1000-op apply+fsync; foreground ACKs starve (R3) | **HIGH** | OPEN |
| P4 | Migration-pressure timeout escalation is sender-local; 3 s kept exactly when the replica is slowest (R4) | **HIGH** | OPEN |
| P5 | Compensation delete may never reach a re-mastered divergent replica (R7b) | **HIGH if confirmed** (durable divergence) | OPEN — needs targeted test |
| P6 | No SWIM incarnation refutation + dead suspicion-backoff code + zero proposal debounce + (1−1/n) round-robin reshuffle: one flap = two cluster-wide storms (R6) | **HIGH** | OPEN |
| P7 | Untimed fan-out permit condvar; healthy-shard writes block behind a wedged replica (R5) | MEDIUM | OPEN |
| P8 | Scenario 01 budget = 120 s including ~60 s compile + 4 cluster lifecycles | **HIGH (test)** — the only live PR-tier flake at HEAD | OPEN |
| P9 | Budget inversions 09/11/13 (external 300 s < internal 600 s); internal==external everywhere else | MEDIUM (test) | OPEN |
| P10 | Masked oracles (sc04 4.7 unconditional OK; sc09 spent_utxos filter; sc14.3 ≤90 % NotFound) — flaky tests hiding suspected real replication bugs | **HIGH (test integrity)** | OPEN |
| P11 | Error-20 retry asymmetry (seed 16× vs spend 0×) + harness reconciliation racing compensation (R7a, R9) | MEDIUM | OPEN |
| P12 | scenario_15.8:761 waits on the dead node | LOW (test bug) | OPEN |
| P13 | Test SWIM config (150/1000 ms) 5× more aggressive than defaults on the slowest plausible environment | MEDIUM (config) | OPEN |
| P14 | `pipeline_batch = chunk.len()` contradicts documented 32-shard sub-batching; fences held across whole chunk (coordinator.rs:3476-3484) | MEDIUM | OPEN |
| P15 | Dead harness scripts (`scripts/partition_network.sh` etc.), tracked generated artifacts, stale timing comments | LOW | OPEN |

---

## 6. Reproduction notes (do not commit as test changes)

1. **P8 (sc01 timeout):** `cd teraslab-tests/client && cargo clean && cd .. &&
   ./run_all.sh --tier pr --scenario 01` on a 2-4 core VM. Compile eats
   45-90 s of the 120 s window; SUMMARY.md shows ~123 s, empty panic column.
   Control: pre-run `cargo test --release --no-run --test
   scenario_01_cluster_formation` — scenario then passes with ~60 s headroom.
2. **P1 regression check (should NOT reproduce at HEAD):** checkout
   `e6e65f0^`, 2 nodes RF=2, two clients concurrently looping `create_batch`
   against node1 and node2 for keys each node masters. Wedges immediately into
   mutual 3 s timeout cycles. Same on HEAD must stay clean.
3. **P2:** 3-node cluster; load ≥100k records into node1 *before* nodes 2/3
   join; let migrations drain; wait 30-45 s while writing steadily. Expect
   `cluster: re-activating topology` then `cluster: migration plan` with
   masters ≈ 2·4096/3 and an error-20/latency spike, with zero membership
   changes in the log. Unit-level: build a table via `apply_master_election`
   with a one-node-holds-all view, call
   `committed_topology_reactivation_metrics` (coordinator.rs:277) — returns
   mismatched ≈ 2731.
4. **P3/P4:** 3 nodes A/B/C; trigger a large migration B→C (kill/rejoin C
   after loading B); while C ingests, write to A-keys whose RF=2 replica is C.
   A has no local pressure → 3 s timeouts → error 20, while C's logs show
   op-240 frames arriving. Log `migration_pressure_active()` on A (false)
   versus C's barrier queue depth. For P7: simultaneously write A-keys whose
   replica is B (healthy) and watch latency climb once ≥128 fan-outs park.
5. **P6 (false death):** `docker pause ts02-node3 && sleep 2 && docker unpause
   ts02-node3` → suspect at +300 ms, dead at ≈+1.3 s, full migration plan,
   rejoin, second plan, then ≥120 s of migration-pressure aftermath. Bisect
   the threshold with 0.4 s (survives via direct-probe refutation) vs 1.2 s
   (dies).
6. **P5:** seed a key; induce a replication failure for it (pause replica
   during the write) so a compensation intent is pending against replica R;
   force a topology change promoting R to master for that shard; resume and
   verify whether the compensating delete ever reaches R.

---

## 7. Open questions / spec ambiguities

1. **Is the reactivation loop supposed to converge placement back to
   round-robin, or is master election authoritative?** (P2). If round-robin is
   the goal, the loop must reuse a real partition view and pace the plan; if
   election is authoritative, the mismatch metric must compare against the
   election-refined expectation. The current code silently undoes the election.
2. **Should migration-flagged `OP_REPLICA_BATCH` take the engine-wide
   exclusive barrier at all** (P3), given migration applies already route
   through stripe-locked engine paths (receiver.rs:1711/1792, C-3/C-4)? The
   barrier appears needed only for torn-read protection of client-visible
   batches.
3. **What is the intended client contract for `ERR_REPLICATION_FAILED` (20)?**
   It is documented nowhere as retryable, the client treats it as fatal, the
   master may or may not have the write durable, and compensation may reverse
   it later. Scenarios encode two contradictory answers (seed: retry+reconcile;
   spend: fatal). A decision here dictates both client retry policy and the
   test oracles.
4. **Are the masked oracles (P10) tracking known bugs anywhere?** Scenario 04's
   4.7 cites "known replication bug" while printing OK; scenario 09 filters
   spent_utxos; scenario 14.3 accepts 90 % NotFound after flapping. Each needs
   either a tracked issue + explicit known-failure annotation, or un-masking.
5. **Was 1000 ms suspicion chosen to keep kill scenarios fast?** If so, the
   right shape is per-scenario SWIM configs (aggressive only in 04/05/12/15/17,
   ≥5000 ms in 01/02/03/06/07/10/13), not one global aggressive setting.
6. The hardcoded 2 s exchange-phase budget (coordinator.rs:1130): on a loaded
   runner a timeout yields a partial/empty partition view, which silently
   degrades plans to raw round-robin (larger storms). Should a partial view
   abort/retry the activation instead, and should this emit a metric?

---

## 8. Top three dominant flake causes (prioritized)

1. **[FIXED — verify it stays fixed] The C-1 visibility-barrier deadlock
   (P1/R1)** caused essentially all sc02/sc03 failures through Jun 11
   (replication wedge, error 20, ~75 %/50 % failure rates). Fix lives in
   cluster code and is already on main (`e6e65f0`); both subsequent CI runs
   pass those scenarios 3-4× faster. Action: none beyond keeping the
   regression test; re-baseline the flake statistics from Jun 12 onward.
2. **[Test harness] Scenario 01's 120 s budget swallowing ~60 s of compile +
   four cluster lifecycles (P8)** is the only PR-tier failure observed at
   HEAD. Fix belongs in the harness/CI: `cargo test --no-run` prebuild outside
   the timeout (or include `teraslab-tests/client` in rust-cache), plus a
   realistic per-scenario budget with internal < external headroom (also
   resolves P9's 09/11/13 inversions).
3. **[Cluster code] The migration-storm cluster of P2+P3+P4+P6** — spontaneous
   reactivation rebalances, barrier monopolization by migration batches,
   sender-local pressure detection, and flap-amplified churn — is what turns
   any perturbation (or nothing at all, per P2) into minutes of error-20s.
   These are real product defects that will bite production under node churn,
   not just CI; they are the next remediation priority after the harness
   budget fix, and they currently account for the residual nightly/weekly-tier
   flakiness (scenarios 04-17 all sit downstream of them).
