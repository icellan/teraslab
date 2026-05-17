# tests/cluster_edge_cases.rs — pre-existing failure triage

Branch: `worktree-agent-aaa9933b289ae9a5a` (base `dba8fcd merge: all 10 group-fix branches via G3 integration`).

All four failures pre-date the recent fix campaign and reproduce on `8b4306e`.
Common root cause: F-G8-001 added a `committed_voter_ever_seen` split-brain
fallback in `src/cluster/topology.rs` that rejects any topology proposal
introducing a NodeId never previously observed as a committed voter on
this node. The four tests were written before that layer existed and were
not updated in commit `3f18e6f` (which fixed the inline-mod topology
tests). Same remediation pattern as `3f18e6f` is applied here:
pre-seed the ever-seen set so the test's intended invariant is exercised
without the F-G8-001 layer short-circuiting first.

No production-code bugs uncovered; no flakes; no sleep bumps; no `#[ignore]`.

---

### topology_fallback_proposer_superseded_by_second_timeout — STALE-ASSERTION
- Symptom: `auth.check_timeout(&mems).expect("first timeout should propose")` panics — `check_timeout` returns `None`.
- Root cause: prior `on_membership_changed(&[1,2,3])` is rejected by F-G8-001 (NodeId(3) never a committed voter, ever-seen set = {1,2} from the term-1 commit). `observed_membership` therefore stays at `[1,2]`, and `check_timeout` short-circuits because `committed == target`.
- Fix: `tests/cluster_edge_cases.rs` — `auth.set_committed_voter_ever_seen(&[NodeId(1), NodeId(2), NodeId(3)])` immediately after the term-1 commit.
- Commit: `5f39785`
- Verified: 10 sequential runs PASS.

### topology_cluster_formation_three_simultaneous_starts — STALE-ASSERTION
- Symptom: `a1.on_membership_changed(&[1,2,3]).expect("node 1 should propose")` panics.
- Root cause: each authority's single-node term-1 commit only stamps itself into the ever-seen set; a1's set is `{1}`. The [1,2,3] proposal fails `ever_seen_check` on a1 (and would also fail on a2/a3 if it reached them).
- Fix: `tests/cluster_edge_cases.rs` — pre-seed each of `[a1, a2, a3]` with `[NodeId(1), NodeId(2), NodeId(3)]` so the formation-recovery branch is the only path under test.
- Commit: `a65a75e`
- Verified: 10 sequential runs PASS.

### topology_formation_recovery_blocked_by_outstanding_vote — STALE-ASSERTION
- Symptom: `assert!(v.accepted)` for the outstanding-vote proposal panics — `handle_propose` rejects.
- Root cause: F-G8-001 layer is wired into `handle_propose` (line 887 of `src/cluster/topology.rs`). Authority's committed_voters from term 1 = `{NodeId(2)}`, ever-seen = `{2}`. The outstanding-vote proposal includes NodeId(1) which is unseen, so the ever-seen check fails before the outstanding-vote-blocks-formation-recovery path runs.
- Fix: `tests/cluster_edge_cases.rs` — `auth.set_committed_voter_ever_seen(&[NodeId(1), NodeId(2), NodeId(3)])` after the term-1 commit.
- Commit: `0a5effe`
- Verified: 10 sequential runs PASS.

### split_brain_heal_detects_independent_clusters — STALE-ASSERTION
- Symptom: `assert!(proposal_super.is_some(), "perfect-superset merge is NOT caught by R-042 ...")` panics. F-G8-001 actually catches this case now.
- Root cause: test was written for R-042 alone and documented the perfect-superset merge case as an unhandled limitation. F-G8-001's later `ever_seen_check` does catch it because nodes 4/5/6 were never committed voters on A. The test's documentation comment and assertion are stale.
- Fix: `tests/cluster_edge_cases.rs` — replace the single is_some() assertion with TWO assertions:
  1. New, stronger invariant: without seeding, perfect-superset merge IS caught by F-G8-001 (`proposal_super_blocked.is_none()`).
  2. R-042-only contrast: after pre-seeding 4/5/6 as ever-seen voters, the R-042 monotonicity check alone is insufficient — proposal goes through. This pins the residual gap that cluster_id (future work) is meant to close.
- Commit: `1d2cd7a`
- Verified: 10 sequential runs PASS.

---

## End-of-group verification

- `cargo test --test cluster_edge_cases` → 39 passed, 0 failed.
- `cargo fmt --all -- --check` on `tests/cluster_edge_cases.rs` → clean (pre-existing fmt drift in other files predates this work).
- `cargo clippy --tests --test cluster_edge_cases` → no warnings or errors in `tests/cluster_edge_cases.rs` (pre-existing clippy errors in `src/replication/*` and `src/device_io/*` predate this work and are unrelated).
