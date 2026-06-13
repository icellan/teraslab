# Cluster Bulletproofing — Remediation Plan

Source: `CLUSTER_E2E_FLAKINESS.md` (audit of 2026-06-12). Goal: every finding
fixed or explicitly decided, with the cluster correct under churn, partitions,
crashes, and migration — then fast. Correctness always wins ties.

Ordering rationale: Wave 0 makes CI signal trustworthy (cannot validate product
fixes against a harness that kills tests mid-compile). Waves 1–4 fix product
bugs in dependency/severity order. Wave 5 un-masks the weakened test oracles
*after* the bugs they were papering over are fixed, then soaks.

Standing execution rules (from CLAUDE.md + session history):
- Test-first within each item: write the failing test, then the fix.
- Parallel worktree agents within a wave; every agent runs `git merge main`
  before working; before merging any branch: `git merge-base --is-ancestor
  <main> <branch>` + grep-diff for dropped critical symbols.
- After each wave merge: `cargo test --all`, `cargo test --features
  fault-injection`, `cargo clippy --all-targets --all -- -D warnings` (on
  latest stable), zero failures/warnings. `scripts/cleanup-worktrees.sh` at
  wave end.
- No sleeps to hide races: any added wait must encode a derived timing bound
  with a comment citing the constant it is derived from.

---

## Wave 0 — Restore trustworthy CI signal (harness/CI only, no product code)

### W0.1 — Pre-build test binaries outside the timeout (P8)
- `run_all.sh`: after the existing client build step, add
  `cargo test --manifest-path client/Cargo.toml --release --no-run` (untimed).
- `ci.yml`: configure `Swatinem/rust-cache` `workspaces` to include
  `teraslab-tests/client` so the dep tree is cached across runs.
- **Success:** scenario logs show <5 s between invocation and `running N
  tests`; scenario 01 wall time ≤60 s on CI.

### W0.2 — Rational timeout budgets (P8, P9)
- Single source of truth: the scenario's internal tokio timeout is the
  authority; `run_all.sh scenario_timeout()` = internal + 60 s headroom so the
  in-test teardown/diagnostics always win the race. Fixes the 09/11/13
  inversions (300 s external vs 600 s internal) and the zero-headroom pairs.
- Scenario 01: raise internal to a derived budget (4 cluster lifecycles ×
  worst-case formation ≈ 240 s internal, 300 s external) — the current 120 s
  was never enough even without compile.
- **Success:** no failure in any scenario log ends with an empty panic column
  (every failure produces a panic/teardown trace).

### W0.3 — Targeted harness bug fixes (P12, P15, parts of §4)
- `scenario_15:761`: survivors `[1,3]` → `[2,3]`.
- Replace `scenario_01:524`'s 500 ms rogue-node sleep with a condition poll
  (cluster_size stable across N probe intervals; bound derived from
  probe_interval).
- Delete dead scripts (`scripts/{kill_node,partition_network,heal_network,
  slow_network}.sh`), untrack the regenerated compose/config artifacts
  (gitignore; runtime generation in `helpers.rs` is already authoritative).
- Fix lying comments: seed retries "8/~30s"→16/~55s, `stop_node` "10s"
  →`--time=1`, rogue-node "3s".
- **Success:** `grep` finds no references to the deleted scripts; fresh clone
  + run produces identical configs.

### W0.4 — Per-scenario SWIM configs (P13)
- Config generation in `helpers.rs`: suspicion 5000 ms / probe 200 ms (shipped
  defaults) for non-kill scenarios (01, 02, 03, 06, 07, 10, 11, 13);
  keep the aggressive 1000/150 only where fast failure detection is the thing
  under test (04, 05, 08, 09, 12, 14, 15, 16, 17).
- Deliberately do NOT raise `replication_timeout_ms` in test configs — the 3 s
  default is the product contract and post-C-1 runs pass with it; inflating it
  would mask regressions of the exact class just fixed.
- **Success:** PR tier green 10 consecutive CI runs (the wave-0 exit gate).

---

## Wave 1 — Stop self-inflicted migration storms (product)

### W1.1 — Reactivation loop vs master election (P2, §7 Q1)
- Step 1 (investigation, blocking): determine the reactivation loop's intended
  purpose from history (`git log` on coordinator.rs:1007-1082) — repair of
  partial activation vs placement convergence. Decision rule: **master
  election is authoritative placement**; reactivation may only repair
  *divergence from the activation's own intent*, never revert it.
- Step 2 (fix): make the mismatch metric compare the active table against the
  election-refined expectation (persist the refined table or the view that
  produced it at activation time), so a settled cluster yields mismatched == 0
  and the loop is a no-op.
- Tests: unit — repro recipe 3 (one-node-holds-all view through
  `apply_master_election`, then `committed_topology_reactivation_metrics` == 0);
  integration — settled 3-node cluster, 60 s idle under steady writes, assert
  zero `re-activating topology` and zero migration plans with stable
  membership.

### W1.2 — Receiver-aware replication timeout escalation (P4)
- The sending master must escalate 3 s → 30 s when the **target replica** is
  under migration pressure. Preferred mechanism: derive from state the master
  already has — the committed shard table knows every pending handoff and
  inbound assignment per node; `replication_ack_timeout_for` escalates when
  the replica node appears as a migration source/target in the active plan
  (plus the existing local check). No new wire traffic.
- Tests: unit on the timeout selector with a synthetic table; integration —
  P3/P4 repro (write to master A whose replica C ingests a migration): zero
  error-20s, log shows 30 s timeout selected.

### W1.3 — Bound the fan-out permit wait; isolate per replica (P7)
- Replace the untimed condvar wait in `acquire_replication_fanout_permit` with
  a deadline (ack-timeout + margin) returning a distinct error, AND split the
  global 128-permit pool into per-replica-address pools so one wedged replica
  cannot starve writes to healthy replicas.
- Tests: wedge replica C (paused), write to keys replicated to healthy B —
  assert p99 unaffected; writes to C-replicated keys fail fast with the
  distinct error once the pool drains.

### W1.4 — Migration fence window per sub-batch (P14)
- Implement the documented 32-shard sub-batching in the migration pipeline
  (`pipeline_batch = chunk.len()` at coordinator.rs:3484 currently fences the
  whole chunk across delta+manifest+handshake) so the per-shard write-blocked
  window shrinks ~30×. If investigation shows sub-batching was deliberately
  abandoned, fix the doc and justify the fence width instead — but the default
  assumption is the doc is right and the code regressed.
- Tests: integration — during a forced migration, measure max continuous
  err-19 window per shard; assert it is bounded by the sub-batch transfer
  time, not the chunk transfer time.

Files: W1.1 coordinator.rs; W1.2 dispatch.rs+coordinator.rs; W1.3 dispatch.rs;
W1.4 coordinator.rs. W1.2/W1.3 touch dispatch.rs — same agent or sequenced
merge. **Gate:** P2 unit+integration tests green; PR tier stays green; repro
recipes 3-4 from the audit no longer reproduce.

---

## Wave 2 — Migration applies off the engine-wide barrier (P3, §7 Q2)

The biggest single change; isolated wave on purpose.

- Step 1 (design, blocking): document exactly what the exclusive
  `dispatch_visibility_barrier` guarantees for `OP_REPLICA_BATCH` (atomic
  multi-key visibility of client-visible batches). Migration-flagged batches
  copy baseline data that is not client-visible on the target until unfence —
  they should need stripe locks only (already taken per C-3/C-4).
- Step 2 (fix): migration-flagged `OP_REPLICA_BATCH` bypasses the exclusive
  barrier; normal replica batches keep it but the **fsync moves outside the
  barrier** if WAL-ordering analysis permits (visibility ≠ durability; ACK
  still only after fsync). If analysis forbids it, keep fsync inside and
  document why — correctness first.
- Tests: fault-injection crash mid-migration (existing
  `tests/migration_crash.rs` must stay green); new concurrency test — inbound
  migration at full pool (128 conns) while foreground replication ACKs from a
  second master stay under 500 ms p99; torn-visibility property test for
  client batches concurrent with migration applies.
- **Gate:** full suite + fault-injection green; audit repro recipe 4 shows no
  error-20s during migration storms.

---

## Wave 3 — SWIM hardening (P6) — parallel with Wave 2 (disjoint files)

### W3.1 — Incarnation refutation
- Process piggybacked updates about self (currently skipped, swim.rs:899-901):
  on seeing self as Suspect/Dead with incarnation ≥ own, bump incarnation and
  gossip an Alive refutation with priority.
- Tests: unit — suspect-self message → next gossip carries higher-incarnation
  Alive; integration — pause a node for suspicion×0.8, assert it refutes and
  is never declared dead.

### W3.2 — Make the suspicion confirm-rounds real
- The exponential suspect backoff is dead code (`indirect_attempts` capped at
  1 by construction, swim.rs:1137-1139). Either wire multi-round re-probing of
  a Suspect before death declaration, or delete the dead path and document the
  single-round behavior. Default: wire it — a second probe round before death
  costs one probe interval and kills most false deaths.
- Tests: unit on the round counter; integration — node stalled just past one
  probe round but responsive in round two is not declared dead.

### W3.3 — Topology proposal debounce
- Coalesce `MembershipChanged` events for `topology_debounce_ms` (new config,
  default ≈ 2× probe interval for joins, ≥ suspicion_timeout/2 for leaves)
  before proposing, so flaps and staggered boots produce one term, not N.
- Tests: 5-node simultaneous boot converges in ≤2 terms (currently up to 4);
  a flap (dead→alive within debounce) produces zero topology changes.

(Placement reshuffle bound — formerly W3.4 — is approved as **Wave 6** below.)

**Gate:** audit repro recipe 5 (docker pause 2 s) produces suspicion +
refutation, zero migration storms.

---

## Wave 4 — Replication failure contract (P5, P11, R7, §7 Q3)

### W4.1 — Compensation vs re-mastering (P5 — confirm, then fix)
- First write the targeted test (audit repro recipe 6): pending compensation
  intent against replica R; promote R to master for that shard; assert the
  compensating delete still reaches R. If it fails (expected per
  dispatch.rs:1565 reading `assignment.replicas` only):
- Fix: compensation intents target the record's full current holder set
  (master + replicas) resolved at re-drive time, not the replica list frozen
  at intent creation.
- This is the one potential silent-divergence bug in the audit — it ships
  before anything else in this wave.

### W4.2 — Define and implement the error-20 contract
- Decision (recommended): `ERR_REPLICATION_FAILED` = **ambiguous outcome,
  idempotent-retry-safe**. Spec §3 gets a subsection: the write may be durable
  on master, replicas, both, or neither; compensation will converge state; a
  client retry of the identical op is safe and is the prescribed recovery.
- Client: add 20 to the retryable set with bounded retries + backoff
  (client/rust/src/lib.rs:2185); `spend_batch_cluster` gains the same retry
  loop create already effectively has via the harness.
- Harness: delete the seed-vs-spend asymmetry — all mutation helpers share one
  retry policy; `reconcile_existing_seed_records` then becomes a cross-check,
  not a crutch, and must re-verify after `wait_replication_settled` to avoid
  racing compensation (R7a).

**Gate:** scenario 02/03 pass with the unified retry policy; new P5 test green;
spec/spec-tests updated.

---

## Wave 5 — Un-mask the oracles (P10)

- Re-enable hard assertions: scenario 04 §4.7 consistency check (currently
  prints OK unconditionally), scenario 09 `spent_utxos` filter removed,
  scenario 14.3 NotFound tolerance 90 % → a small derived bound with a bounded
  convergence wait. Done after waves 1-4 deliberately: those fix the bugs
  these were masking; anything still red here is a fresh finding — triage each
  as product bug (fix in-wave) or file with evidence.
- **Gate:** nightly tier green with all oracles hard.

---

## Wave 6 — Bounded-movement placement (approved; replaces round-robin)

Round-robin `members[shard % n]` (shards.rs:120-133) reshuffles (1−1/n) of all
4096 shards on any membership change. Replace with rendezvous (HRW) hashing so
a single join/leave moves ≈1/n of shards.

- Step 1 (design doc, blocking): rendezvous hashing recommended over
  ring-based — deterministic, no virtual-node tuning, trivial compute at
  4096 shards × small n; replica set = next-highest weights, preserving the
  master ∉ replicas and spread invariants. The doc must cover the two
  correctness traps:
  - **Placement versioning.** Placement is computed independently on every
    node; two nodes computing with different placement functions = placement
    split-brain. The committed topology gains a `placement_version`; a node
    refuses to activate a table whose placement_version it does not implement.
    Mixed-version clusters keep round-robin until every member supports v2.
  - **Upgrade path.** First commit with placement v2 triggers one final
    full-reshuffle migration (planned, paced by the W1.4 sub-batching);
    thereafter movement is bounded.
  - Interaction with master election: unchanged — election refines on top,
    and the W1.1 reactivation fix already compares against the refined
    expectation, not the raw placement.
- Step 2 (implement + tests):
  - Property tests: determinism across nodes; add/remove one node moves
    ≤ ⌈4096/n⌉ + small slack masters (vs ~2731 today); RF invariants hold for
    all n ∈ 1..=16.
  - Upgrade test: mixed placement_version cluster does not activate v2 tables;
    homogeneous cluster migrates once and subsequent changes are ~1/n.
  - Re-run the Wave-1 P2 tests unchanged (election/reactivation must be
    placement-agnostic).
- **Gate:** full suite + fault-injection green; scenario 06/07 (scale up/down)
  migration plan sizes drop ~n-fold and budgets hold with margin.

---

## Wave 7 — Validation campaign (plan exit criteria)

- PR tier: 10 consecutive green CI runs.
- Release tier (scenarios 01-17): 5 consecutive green local runs on a
  CPU-constrained VM (2 cores, to match CI).
- `scripts/loop_scenario_17.sh` ×20 green (the suite's best product-bug
  detector).
- Chaos (16) ×3 green.
- Audit repro recipes 1-6: none reproduce on HEAD.

---

## Decisions (resolved 2026-06-12)

1. Consistent-hash placement: **approved — Wave 6** (rendezvous hashing,
   placement-versioned).
2. Error-20 contract: **ambiguous + idempotent-retry-safe** (W4.2 as written).
3. Scope: **all waves in order.**

## Dependency graph

```
W0 (harness)──────────────► gate: 10× green PR tier
W1 (storms: P2,P4,P7,P14)─► gate: repro 3-4 dead          [after W0]
W2 (barrier: P3)──────────► gate: repro 4 dead, FI green  [after W1]
W3 (SWIM: P6)─────────────► gate: repro 5 dead            [after W1, ∥ W2]
W4 (contract: P5,P11)─────► gate: P5 test, spec updated   [after W2]
W5 (oracles hard)─────────► gate: nightly green           [after W1-W4]
W6 (placement: HRW)───────► gate: ~1/n movement proven    [after W5]
W7 (soak)─────────────────► gate: exit criteria           [after W6]
```
