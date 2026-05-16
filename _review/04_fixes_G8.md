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

### F-G8-003 — FIXED
- Commit: pending
- Files changed: `src/cluster/swim.rs` (wire format bump + per-peer replay window), `tests/g8_swim_replay.rs` (new)
- Test added/extended: `tests/g8_swim_replay.rs` — 3 tests covering verbatim replay, forward slide, and out-of-order. `tests/cluster_swim.rs` (10 existing tests) still passes.
- Notes: Added an 8-byte monotonic `sender_seq` to the SWIM header (now `[type:1][id:8][inc:8][seq:8][addr_len:2]...`), inside the HMAC envelope. `SwimRunner` carries `next_outbound_seq` + per-peer `seen_seq: HashMap<NodeId, ReplayWindow>`. Window is 256 bits per peer, accepts in-window unseen positions and rejects exact duplicates / left-of-window seqs. Wire format bumped per FIX_POLICY.md item 3.

### F-G8-004 — FIXED
- Commit: pending
- Files changed: `src/cluster/swim.rs` (bounded map + FIFO eviction + counter), `tests/g8_ping_req_cap.rs` (new)
- Test added/extended: `tests/g8_ping_req_cap.rs::ping_req_forwarding_evicts_oldest_under_flood` (4200 inserts → ≥104 evictions).
- Notes: `ping_req_forwarding` now capped at `PING_REQ_FORWARDING_MAX = 4096`. New parallel `VecDeque<NodeId>` preserves insertion order; when at cap we evict the oldest and increment a process-wide `SWIM_PING_REQ_DROPPED_TOTAL` counter exposed via `cluster::swim::ping_req_dropped_total()`. Counter lives in `swim.rs` rather than `metrics.rs` (owned by G6) — orchestrator can later wire it through the registry.

### F-G8-005 — FIXED
- Commit: pending
- Files changed: `src/cluster/coordinator.rs`
- Test added/extended: `tests/cluster_edge_cases.rs` (existing — same 4 pre-existing failures, 35 pass on baseline and after fix; no new failures).
- Notes: Refactored `activate_topology_with_view` to release the migration Mutex during the plan-rebuild. Phase 1 takes a single snapshot of `active_migrations()` + counters under lock; phase 2 builds preserved/stale sets with the lock released; phase 3 re-acquires only long enough to call `mark_failed`, `clear_inbound`, `cleanup_completed`. Lock hold time drops from O(NUM_SHARDS) + multiple scans to a single Vec copy + the mutation phase. Dispatch hot path (`dual_write_targets_for_shard`) no longer blocked.

### F-G8-002 — FIXED
- Commit: pending
- Files changed: `tests/g8_split_brain.rs` (covers follower-side check)
- Test added/extended: `tests/g8_split_brain.rs::handle_propose_rejects_unseen_member_superset`
- Notes: The follower-side `membership_change_is_safe` check in `handle_propose` already calls the shared helper. Test verifies that a follower with committed `{1, 2}` refuses to vote for a `{1, 2, 3, 4}` proposal even when the proposer presents a valid digest.

