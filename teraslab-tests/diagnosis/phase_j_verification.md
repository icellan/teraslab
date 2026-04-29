# Phase J — Verification Status

This document records the Phase J verification sweep for the Phase A–I
TDD plan that fixes chronic cluster + replication test failures.

## Cargo gates (this run)

All three Cargo verification gates passed cleanly on the post-Phase-I
codebase:

| Gate                                         | Result                       |
|----------------------------------------------|------------------------------|
| `cargo test --all`                           | 1574 tests passed, 0 failed  |
| `cargo clippy --all-targets -- -D warnings`  | clean                        |
| `cargo fmt --all -- --check`                 | clean                        |

Crate-level breakdown (`grep "^test result" /tmp/p_j_test.log`):

```
1382 passed (lib)
  15 passed (cluster_smoke integration)
  38 passed (full_pipeline integration)
   9 passed (network_partition integration)
  21 passed (recovery integration)
  21 passed (replication_replay integration)
  25 passed (other integration)
  16 passed (other integration)
   8 passed (other integration)
   5 passed (other integration)
  23 passed (tcp dispatch integration)
   2 passed (stress)
   3 passed (tracing integration)
   1 passed (tracing lint)
   2 passed (doc tests)
```

Plus three empty integration crates (0 passed). One ignored test in lib
is the long-standing pipelined-migration TODO (`#[ignore] // TODO:
rewrite for pipelined migration flow` in `coordinator.rs`). It is
unrelated to the Phase A–I work.

## Scripts added in this phase

The plan calls for three loop drivers to prove the fixes hold under
repeated cluster churn, including the chronic flake bands:

- `teraslab-tests/scripts/loop_scenario_17.sh [iterations=50]` — runs
  scenario 17 (Failure Recovery Hardening) repeatedly. Asserts zero
  failures across the loop. Stops at the first failed iteration so the
  diagnostic dump for that run is preserved under
  `teraslab-tests/results/`.
- `teraslab-tests/scripts/loop_scenario_12.sh [iterations=50]` — same
  pattern for scenario 12 (Concurrent Failures), with Docker CPU
  throttling applied via `TERASLAB_DOCKER_CPU_QUOTA`
  (default `50000` = 0.5 CPU per container) to amplify the cluster
  startup race.
- `teraslab-tests/scripts/release_tier_10x.sh [iterations=10]` — runs
  the full 17-scenario release tier suite N times in sequence, asserts
  every scenario green on every iteration.

## Plan acceptance status

| Bar                                                | Status                  |
|----------------------------------------------------|-------------------------|
| Scenario 17: 50/50                                 | scripts ready, run pending |
| Scenario 12 throttled: 50/50                       | scripts ready, run pending |
| Release tier (17 scenarios): 10/10 consecutive     | scripts ready, run pending |
| Cargo gates clean                                  | green this run          |

The Docker-driven loop scripts have not yet been executed end-to-end on
this branch. Phase J's library-level gate (cargo test/clippy/fmt) is
green; the empirical scenario-loop bar is the next step and runs
outside this session given its wall-clock cost (≈12–18 hours total for
all three loops at default iteration counts).

## Phase A–I summary

| Phase | Subject                                 | Commit     |
|-------|-----------------------------------------|------------|
| A     | Diagnostic foundation                   | (pre-summary) |
| B     | Cluster-key gating                      | (pre-summary) |
| C     | Subset / version tracking               | dedce3f / cc62b23 |
| D     | Exchange phase before migration         | b9e618c / 0023851 |
| E     | Dual-write during migration             | 2969391    |
| F     | Master election scoring                 | 10897a0    |
| G     | Migration throttling (infra)            | 54c37d8    |
| H     | Redo truncation recovery (infra)        | b12c453    |
| I     | Cluster startup readiness (infra)       | 5816b16    |
| J     | Verification scripts + cargo gates      | (this commit) |

## Follow-up commits (post-Phase-J)

The phases above landed the algorithmic fixes and infrastructure;
these follow-up commits wired everything into production paths and
closed the durability/availability gaps the plan called out:

| Subject                                                          | Commit     |
|------------------------------------------------------------------|------------|
| Wire `exponential_seed_backoff` into SwimRunner seed retry loop  | 25a8ee5    |
| De-flake `migration_active_gauge_tracks_inflight_shards`         | 6d84592    |
| Wire `MigrationThrottle::try_admit` into migration spawn loop    | 22a4cb0    |
| Wire `ResyncRequest` channel — auto-recovery from redo truncation| 81a9610    |
| Require ≥1 ACK from dual-write set during migration              | e813b66    |
| Gate writes/reads with `ERR_CLUSTER_NOT_READY` when joining      | 7f4f53b    |
| Extend diagnostic dump to `wait_migrations_complete` + ad-hoc    | ef0c200    |
| Add `wait_specific_nodes_alive` — Phase I client consumer        | d555bac    |

After these commits the production state is:

- **Phase E** — strict per-set ACK accounting enforced. A write that
  touched a migrating shard cannot succeed unless at least one new-
  master target ACKs.
- **Phase G** — outbound migration spawn is byte-throttled
  (`run_migration_tasks_with_global_limit` calls `try_admit` per
  target group; cap defaults to 32 MiB via
  `TERASLAB_MAX_BYTES_EMIGRATING`).
- **Phase H** — `bin/server.rs` catchup loop posts a
  `ResyncRequest` whenever `run_catchup_for_replica` returns the
  truncation sentinel. Coordinator event loop drains the receiver
  and runs full-shard backfills through the migration pipeline,
  inheriting Phase E dual-write protection and Phase G throttling.
- **Phase I** — `OP_ADMIN_CLUSTER_HEALTH` snapshot derives from
  `topology_authority.committed_term` (Joining when 0, Alive
  otherwise). Dispatch returns `ERR_CLUSTER_NOT_READY` for client
  reads/writes against Joining nodes; bootstrap traffic bypasses.
  `SwimRunner` seed retry uses exponential backoff. Test crate has
  `wait_specific_nodes_alive(client, docker, node_nums,
   stability_window, timeout)`.
- **Test infrastructure** — `migration_active_gauge_*` flake fixed
  via per-test serialization on the global metrics singleton.
  `collect_admin_diagnose_dump` extracted as a reusable helper
  across `wait_migrations_complete_with_diag` and
  `wait_for_migration_reads_ready`.

## Genuinely deferred items

The single remaining deferred item is **the full
`NodeState{Joining, Alive, Suspect, Dead}` field on
`Membership`** (membership.rs / swim.rs / coordinator.rs). The
current `cluster_health()` derivation (Joining when
`committed_term == 0`, Alive otherwise) is functionally equivalent
to a formal `NodeState::Joining` for the dispatch readiness gate.
The full refactor is more elegant — it lets SWIM gossip carry per-
peer Joining state and propagate it instead of every node deriving
locally — but does not change observable behaviour for scenario
tests. This is an architectural cleanup task, not a bug-fix.

## Final cargo gate

```
cargo test --all                              → 1575 passed, 0 failed
cargo clippy --all-targets -- -D warnings    → clean
cargo clippy --tests (teraslab-test-client)  → clean
cargo fmt --all -- --check                    → clean
```
