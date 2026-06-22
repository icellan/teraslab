# TeraSlab v1.0 (target 0.7.0) — Release Recommendation

## Verdict: **NO-GO** — clearable with 2 small fixes

The engine is v1-grade. Two narrow defects on durability/correctness-critical paths must land first; both have small fixes using primitives already in the tree. This is a "fix two things and re-gate," not a "not ready" verdict.

Date: 2026-06-23. HEAD `920ac32` (version bumped to 0.7.0 this session). Baseline: build clean (0 warnings); `cargo test --all` clean isolated **2710 / 0 / 0**; clippy clean.

## Must clear before tagging (blockers)

| ID | What | Fix size |
|----|------|----------|
| **REL-001** | Delete-tombstone write is a non-atomic data race (UB) vs the lock-free read path (`engine.rs:1518-1525`). Route it through `io_locks().write` + `atomic_store_from` like every other direct writer; add an aarch64-release/loom torn-header test. | small |
| **REL-002** | Cluster peak/topology persist doesn't fsync the parent dir after `rename` (`coordinator.rs:7454-7522`) → crash can roll the split-brain peak back → minority self-activation. Call the existing `fsync_parent_dir` post-rename; add a rename-durability crash test (REL-111). | small |

## Strongly recommended before v1 (majors — gate owner's shortlist)

These don't strictly corrupt data on the happy path, but they break documented cluster behavior and leave the cardinal contract ungated. At least the CI-gating and client-redirect ones should land for a credible v1:

- **REL-015** — promote `scenario_14` (split-brain) + `scenario_15` (crash recovery) to nightly + gate `release.yml` on them. The no-double-spend contract must be a release gate, not weekly.
- **REL-016** — run the Go client `integration` tests against a real server in CI.
- **REL-012 / REL-013** — Go client must follow per-item `ERR_REDIRECT` for batch mutations (the form the server actually emits), and its redirect tests must use the real wire shape.
- **REL-010 / REL-011** — Rust client: support `cluster_secret` (bootstrap a default secure cluster) and shard-fan-out/redirect for `unspend`/`get_spend` — or document the limitations loudly. (Lower priority if Teranode uses the Go client; confirm which client ships.)
- **REL-014** — both clients should send the 8-byte `ProcessExpiredPreservations` so the expiry phase actually runs (currently a silent no-op).
- **REL-017** — make the index snapshot/export round-trip tests assert full `TxIndexEntry` equality with non-zero fields (cheap; guards a silent-corruption path).
- **REL-018 / REL-019** — document the on-by-default tombstone subsystem; fix the `DEPLOYMENT_ASSUMPTIONS.md` `strict_auth` default (it says the opposite of shipped).

## Can ship as v1 with follow-ups (minors)

45 minors, overwhelmingly docs drift (stale sizes/defaults, io_uring still referenced + still a declared dep, README test count `2234`→`2710`, undocumented config keys) plus dead-code cleanup and small test gaps. None block. A docs-accuracy sweep before tagging is worthwhile since v1 docs are a contract — see FINDINGS REL-100..REL-144.

## What was NOT reviewed / not verifiable on this host (per the rules, stated explicitly)

- **NVMe/Linux performance** — throughput, redb throughput, SSD wear, replication bandwidth, sustained-load **p99.9 tail**, and live RSS-vs-records. Host is macOS, no `O_DIRECT`/NVMe. Marked `unverified-on-this-host`. The README's quantitative perf table remains **unvalidated** (though already hedged) until a Linux+NVMe run exists. Not a code blocker.
- **`cargo clippy --features fault-injection`** — not re-run this session (README claims clean; base clippy is clean).
- **Embedded `/ui/` dashboard behavior** — only route registration confirmed.
- **Raw `/dev/nvme` `BLKGETSIZE64` test** and **deep `cargo-fuzz` nightly** — pre-existing known residuals (README documents both).

These are deferred with justification (§8 of REVIEW.md), not silently skipped. No source subsystem was left unreviewed.

## Bottom line

Fix REL-001 and REL-002 → re-run the full suite + the cluster e2e tier → **GO**. Land the major shortlist (especially REL-015/016 CI-gating and the client redirect fixes) for a v1 that holds up under cluster rebalance and crash, and do a docs-accuracy sweep. The perf table stays labeled "design targets / MemoryDevice ceiling" until measured on Linux+NVMe.
