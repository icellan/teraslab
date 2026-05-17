# cluster_tcp.rs — sleep-to-wait_until conversion + 2 failing-test fixes

Branch: `worktree-agent-aac1cccd8ba19fcf9`
Base: `f720b5c docs(review): record fix #3 + 3 real failures as follow-ups`

| Commit | Title |
|--------|-------|
| `db9fb00` | perf(test): convert cluster_tcp.rs fixed sleeps to wait_until polls |
| `698fa0e` | fix(test): add_fourth_node_rebalance_triggers — seed F-G8-001 ever_seen |
| `345a38e` | fix(test): isolated_node_rejects_writes_with_no_quorum — seed F-G8-001 ever_seen |
| `10c8a91` | style: rustfmt collapse single-expression closure in cluster_tcp.rs |

## 1. wait_until conversion

15 `thread::sleep(Duration::from_secs(N))` sites converted to
`wait_until(<deterministic-signal>, Duration::from_secs(N))`. New
helper `wait_until` added near the existing `wait_for_shard_split`
helper (around L460). Poll interval 50 ms, ceiling preserved from the
original sleep so the worst-case wall-clock is unchanged.

| L (pre) | Test | Original sleep | Signal converted to |
|--------:|------|---------------:|---------------------|
| 251 | `two_nodes_discover_each_other` | 2 s | `shard_table_version() > 0` on either node |
| 478 | `three_node_cluster_all_shards_assigned` | 3 s | `shard_table_version() > 0` on any node |
| 515 | `add_fourth_node_rebalance_triggers` (pre-add) | 4 s | `committed_topology_members().len() == 3` on node1 |
| 528 | `add_fourth_node_rebalance_triggers` (post-add) | 5 s | `committed_topology_members().len() == 4 && shard_table_version() != v_before` |
| 558 | `remove_node_rebalance_triggers` (pre-kill) | 3 s | `node_addresses().contains_key(NodeId(123))` |
| 564 | `remove_node_rebalance_triggers` (post-kill) | 5 s | `!node_addresses().contains_key(NodeId(123))` |
| 706 | `migrate_shard_with_records_to_new_node` | 5 s | `committed_topology_members().len() == 2` on node2 |
| 862 | `after_migration_complete_all_ops_go_to_new_node` | 5 s | committed `len == 2` + node1 in addrs + node1 owns shards in node2's table |
| 939 | `no_records_lost_during_migration` | 5 s | `committed_topology_members().len() == 2` on node2 |
| 1010 | `migration_of_empty_shard_completes_without_error` | 3 s | `len == 2` on **both** node1 and node2 |
| 1058 | `start_three_node_cluster_create_records_distributed` | 3 s | `shard_table_version() > 0` on node1 |
| 1157 | `spend_routed_to_correct_master` | 2 s | committed `len == 2` + node2 in addrs + node2 owns shards |
| 1259 | `add_node_all_records_still_accessible` | 5 s | `len == 2` on both node1 and node2 |
| 1288 | `kill_node_detection_affected_shards` (pre-kill) | 3 s | `node_addresses().contains_key(NodeId(263))` |
| 1300 | `kill_node_detection_affected_shards` (post-kill) | 6 s | `!node_addresses().contains_key(NodeId(263))` |

Two sites where naïve `committed_len == 2` was insufficient
(`after_migration_complete_all_ops_go_to_new_node` and
`spend_routed_to_correct_master`): the assertion downstream is an
RF=2 create that has to replicate. Strengthening the predicate to
also require the peer's address in node_addrs AND the peer owning at
least one shard in the activated shard table closed the race.

Wall-clock: `time cargo test --test cluster_tcp` 21.3 s (with both
fixed-via-sleep tests still timing out at 15 s) → 3.06 s after the
two real fixes (cf §2). Even discounting the test fixes, the 23
previously-passing tests collectively dropped ≈45 s of fixed-sleep
budget to single-digit settle time.

No sleeps were left as FOLLOW-UP: every site mapped onto a
deterministic signal. Short-loop `from_millis(20..100)` poll intervals
inside existing helpers were left untouched per task constraints.

## 2. Two previously-failing tests fixed

Both tests classified as **REAL-BUG** (production code) masked as
test-setup gap. Root cause is identical: F-G8-001's
`committed_voter_ever_seen` set rejects any topology proposal that
introduces a NodeId not previously observed as a committed voter,
and there is no retry path once `on_membership_changed` returns
None. A fresh test fixture creates nodes sequentially, so the first
2-node commit lands before the third node's SWIM-discovery event;
the third-node proposal is then rejected and the cluster stays at
2 members forever.

Production-code root cause: `src/cluster/topology.rs:706` —
`ever_seen_check` always runs after the (currently dead-code)
`cluster_id` check on the proposer side. The docstring says
"cluster_id (when wired) overrides this" but every caller passes
`proposal_cluster_id: None`, so the override never fires. Filed as
NEEDS-ORCHESTRATOR (cluster_id wiring is out of scope per the
existing `_review/04_fixes_G8.md` note).

Surgical test fix: a new helper `create_node_with_ever_seen()` (and
shared internal `create_node_full()`) lets a test pre-seed the
TopologyAuthority's `committed_voter_ever_seen` set BEFORE
`coordinator.start()` is called — so SWIM cannot race ahead and
commit a partial topology that locks out later legitimate members.

### Test 1 — `add_fourth_node_rebalance_triggers`

- Classification: **REAL-BUG (root cause in topology.rs); test-only fix applied.**
- Commit: `698fa0e`.
- 10×-isolation runs after fix: 10/10 pass, ~0.42 s each
  (was a hard 15 s panic every run).

### Test 2 — `isolated_node_rejects_writes_with_no_quorum`

- Classification: **REAL-BUG (same root cause); test-only fix applied.**
- Commit: `345a38e`.
- 10×-isolation runs after fix: 10/10 pass, ~2.9 s each
  (was a hard 15 s panic every run). The 2.9 s is real cluster
  settle time (peer-death + SWIM suspicion + NodeLeft propagation)
  measured by `wait_for_node_addrs_le`, not blocking sleep.

Bonus diagnostic: the panic in `wait_for_committed_members_len` was
enriched to print `node_addresses` keys, `peak_cluster_size`, and
`alive_node_count` so the next regression in this area is
diagnosable from the panic alone.

## 3. Production code bugs uncovered

- `src/cluster/topology.rs:706` — `ever_seen_check` runs
  unconditionally on proposer side; supposed-to-override
  `cluster_id` path is dead code. Pre-existing finding, already
  noted in `_review/04_fixes_G8.md` as NEEDS-ORCHESTRATOR
  (cluster_id wiring). Not fixed in this campaign.

No new bugs introduced; no other test regressions in
`cargo test --all`.

## 4. Followup sleeps left un-converted

None. All 15 `from_secs(N)` sleeps had a deterministic signal.
