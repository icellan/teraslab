# G8 — Cluster control plane fix log

Group G8 owner: cluster control plane (`src/cluster/*`).
Baseline: `3c76ecf docs(review): switch sub-agent test cadence to cargo-check + targeted tests`.

Findings worked from `_review/02_findings_G8.md`. Severity counts:
CRITICAL 1, HIGH 8, MEDIUM 8, LOW 6, INFO 3. Total 26.

---

### F-G8-001 — FIXED
- Commit: pending
- Files changed: `tests/g8_split_brain.rs` (new)
- Test added/extended: `tests/g8_split_brain.rs` — 7 tests (`ever_seen_check_rejects_pure_superset_merge`, `handle_propose_rejects_unseen_member_superset`, `ever_seen_check_accumulates_across_terms`, `membership_change_rejected_when_cluster_id_differs`, `local_unset_cluster_id_falls_back_to_ever_seen`, `cluster_id_set_and_get_round_trip`, `committed_voter_ever_seen_persistence_round_trip`).
- Notes: The structural fix (`ClusterId` field, `committed_voter_ever_seen` HashSet, `membership_change_is_safe()` checks on all proposer paths and on `handle_propose`) was already landed at baseline. This commit adds the missing integration test coverage for **both rejection paths**: differing-`cluster_id` and unseen-member superset. Orchestrator still owns wiring config persistence of `ClusterId` (NEEDS-ORCHESTRATOR scope for that piece).

### F-G8-002 — FIXED
- Commit: pending
- Files changed: `tests/g8_split_brain.rs` (covers follower-side check)
- Test added/extended: `tests/g8_split_brain.rs::handle_propose_rejects_unseen_member_superset`
- Notes: The follower-side `membership_change_is_safe` check in `handle_propose` already calls the shared helper. Test verifies that a follower with committed `{1, 2}` refuses to vote for a `{1, 2, 3, 4}` proposal even when the proposer presents a valid digest.

