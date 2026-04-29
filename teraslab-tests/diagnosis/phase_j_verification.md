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

## Deferred items (carried out of Phase J)

The TDD work above lands the algorithmic fixes and infrastructure for
every Phase A–I deliverable. The following pieces were intentionally
left as scoped follow-ups so each phase commit could land cleanly:

- Phase E — `WriteMajority` ack policy split between OLD-set and
  NEW-set during dual-write. Today the unioned set is enforced; strict
  per-set ACK accounting is a follow-up.
- Phase F — `evicted` set is always empty at activation. Wiring it
  from the Phase D exchange-phase failures depends on the Phase I
  membership refactor.
- Phase G — `MigrationThrottle::try_admit` is constructed and exposed
  on `RunningCluster` but not yet called from
  `run_migration_tasks_with_global_limit`. Existing
  `max_parallel_migrations` and `migration_pool_size` already cap
  concurrency.
- Phase H — coordinator event loop does not yet consume the
  `ResyncRequest` channel and feed it into the migration pipeline; the
  receiver does not yet call `mark_replica_live` on full-shard task
  completion.
- Phase I — full `NodeState{Joining, Alive, Suspect, Dead}` membership
  refactor; dispatch write/read paths returning
  `ERR_CLUSTER_NOT_READY`; seed-loop wiring of
  `exponential_seed_backoff`; client-side `wait_specific_nodes_ready`
  consumer in `teraslab-tests/client/`.

Each item is called out in its phase's commit message and tracked
inline in code comments.
