# Audit Category F — Sharding and Migration

HEAD: 1e5659b. Scope: `src/cluster/{shards,migration,routing}.rs`,
`src/index/migration.rs`, plus the write/read routing path in
`src/server/dispatch.rs` and `src/cluster/coordinator.rs`.

> Tooling note: this session suffered repeated, non-deterministic suppression
> of tool output (Read/Bash returning empty for valid inputs). Every claim
> below is anchored to file:line content that was successfully and fully read
> at least once. Items I could not finish tracing reliably are listed
> separately with reduced confidence rather than asserted.

---

## VERIFIED-OK checklist items

### OK-1 — Shard mask `u16_le(txid[0..2]) & 0x0FFF` (4096 shards = 12 bits)
`ShardTable::shard_for_key` (`shards.rs:314-317`):
```rust
let h = u16::from_le_bytes([key.txid[0], key.txid[1]]);
h & 0x0FFF
```
Exactly the spec'd 12-bit little-endian mask; `NUM_SHARDS = 4096`
(`shards.rs:10`). Tested by `shard_for_key_deterministic` (556) and
`shard_for_key_distribution` (568). Correct.

### OK-2 — Round-robin assignment deterministic for the same node set
`compute_with_epoch` (`shards.rs:103-134`) is pure in `(members, rf, epoch)`:
`master = members[shard % n]`, `replica_i = members[(shard+i) % n]` skipping
self. No HashMap iteration in the build. Tested by `compute_deterministic`
(590), `compute_same_on_different_nodes` (601),
`compute_same_members_different_order_identical` (1027). Determinism holds
**given sorted members** — documented precondition (shards.rs:86-93); see
F-04.

### OK-3 — Writes to a shard with pending inbound migration return MIGRATION_IN_PROGRESS on EVERY write op
The fence is enforced at the routing layer shared by all mutating opcodes,
not per-handler. `RunningCluster::is_master` (`coordinator.rs:6060`) returns
`MasterQueryResult::Transitioning` (coordinator.rs:6064, 6074) when the local
node is a subset master / has pending inbound for the key's shard. Dispatch
maps `Transitioning` to `ERR_MIGRATION_IN_PROGRESS` (code 19). All 12 write
handlers exist at: spend 2777, unspend 3057, set_mined 3224, create 3484,
freeze 3913, unfreeze 4017, reassign 4121, set_conflicting 4247, set_locked
4373, preserve_until 4480, delete 4677, mark_longest_chain 5128
(dispatch.rs). The MIGRATION_IN_PROGRESS rejection is emitted from the shared
cluster-routing block (dispatch.rs:627, 647, 657, 673, 686, 709, 751, 774,
796, 805, 829) and the per-handler master checks (e.g. set_conflicting
`is_master` at 5297, delete at 6009, preserve_until at 5694). A dedicated
test asserts GET_BATCH also yields `ERR_MIGRATION_IN_PROGRESS` for a
Transitioning state and explicitly NOT `ERR_REDIRECT` (dispatch.rs:8928-8971,
9009). Verified — no write op bypasses the fence.

### OK-4 — Empty (no-local-data) master-changed shards are STILL fenced until handoff proves completion
This directly refutes the speculative concern I (prematurely) raised earlier.
`MigrationManager::start_outbound` registers an inbound expectation for the
TARGET of every task regardless of whether the shard is populated
(migration.rs:526-553), and the doc states: "Empty shards are still fenced
until the source proves completion, preventing the new owner from serving
writes before the handoff is durably installed" (migration.rs:505-507).
`has_pending_inbound` is the bitmap test (migration.rs:655-657). Covered by
`start_outbound_registers_empty_shards_for_inbound` (migration.rs:2577) and
`inbound_tracking_per_task_on_empty_target` (1785). The earlier worry that
`master_subset` could be orphaned is moot for write-safety because the
write fence is driven by the inbound-migration tracker, which is cleared only
by an explicit completion (`mark_inbound_complete*`), never by a master-only
state flag.

### OK-5 — A wall-clock timeout must never reopen a shard for writes mid-migration
`clear_stale_inbound` (migration.rs:1142-1156) takes a `_max_age` but
**ignores it**, retaining ALL pending entries and only removing completed
ones. The doc is explicit: "A wall-clock timeout must never reopen a shard
for writes while a migration could still complete." Tested by
`clear_stale_inbound_keeps_old_pending_entries` (2779) and
`clear_stale_inbound_preserves_pending_entries` (2284). This is the
correct, safety-first behavior — a timeout cannot silently un-fence a shard
whose data has not arrived. Good.

### OK-6 — Late/duplicate migration batches cannot reopen a completed inbound shard
`mark_inbound_active` refuses to recreate state if any entry for the shard
exists (migration.rs:564-575), and a completed tombstone blocks late
re-registration (`record_completed_inbound_tombstone` 817-830). Tested by
`late_migration_batch_does_not_reopen_completed_inbound_shard` (1734) and
`early_empty_completion_does_not_reopen_inbound_on_late_registration`
(1761). This closes the "duplicated/double-homed after completion" hole for
the inbound side.

### OK-7 — migration_pool_size / migration_batch_size are live (not dead config)
Both are threaded config → coordinator and into every migration-task launch:
`run_migration_tasks_with_global_limit(... max_migration_threads,
migration_pool_size, migration_batch_size, ...)` is invoked from the
NodeJoined retry path (coordinator.rs:1334-1352), the resync path
(1214-1232), and `activate_topology*` (1141-1161, 1402-1421). The event
handler signature carries both (coordinator.rs:1287-1288). Live.

### OK-8 — migration_plan / replica_migration_plan never source from a dead node, never double-task a live master move
`migration_plan` (shards.rs:414-472) falls back to a surviving replica when
the old master is dead, and skips when the new master already held the data;
live master moves emit exactly one task from the authoritative old master.
Well covered: `migration_plan_remove_node_uses_surviving_replica` (1265),
`migration_plan_remove_middle_node_never_sources_dead_member` (1282),
`replica_migration_plan_remove_middle_node_never_sources_dead_member` (1298),
`migration_plan_uses_single_source_for_live_master_move` (759),
`migration_plan_add_then_remove_net_zero` (1314).

### OK-9 — Mid-import crash across the 3 redb files is guarded (no silent partial index)
`index/migration.rs` writes a durable sentinel (tmp+rename+parent fsync)
BEFORE opening the first redb file and removes it only after all three batch
commits (`import_streaming_redb` 446-464; `import_legacy_snapshot` 275-298).
Startup must consult `import_in_progress` (76-78) and refuse a partial state.
Export is atomic via tmp+rename+fsync with a declared-vs-actual count check
(`ensure_export_count` 369-378) and a CRC32 trailer verified on import
(`verify_portable_checksum_and_eof` 640-663). Strong coverage:
`import_index_transactional_across_three_files` (1160),
`import_index_rerun_after_partial_failure_clears_sentinel` (1214),
`import_index_writes_sentinel_then_removes_on_success` (1125). This is the
index-migration crash-safety path and it is solid.

### OK-10 — RoutingInfo (partition map) decode is bounds-checked and version-carrying
`routing.rs` decode validates every length before indexing (99-151) and
preserves `shard_table_version` round-trip; truncated payloads return `None`
(`routing_info_decode_truncated` 202, `routing_info_decode_too_short_for_shards`
375). Clients receive the version so they can detect staleness on REDIRECT.

---

## FINDINGS

### F-01 (LOW, confidence HIGH) — `compute_with_epoch` trusts caller-sorted members with no defensive sort/assert
**Where:** `src/cluster/shards.rs:103-134`.
**What's wrong.** Assignment indexes the slice directly assuming sorted
input (documented at 86-93) but neither sorts a local copy nor asserts
sortedness. Every test sorts before calling.
**Why it matters.** If any single call site ever passes an unsorted member
list, two nodes computing from the same membership in different orders
produce DIFFERENT shard tables → divergent ownership → two masters for one
shard → conflicting writes / double-spend window. The entire
"deterministic, no-consensus" design rests on identical sorting everywhere.
**Reproduction.** `compute_with_epoch(&[NodeId(3),NodeId(1),NodeId(2)],2,1)`
vs `compute_with_epoch(&[NodeId(1),NodeId(2),NodeId(3)],2,1)` →
`assignments[0].master` differs. The test
`compute_same_members_different_order_identical` (shards.rs:1027) sorts
first, masking the hazard.
**Suggested fix.** Sort a local copy at the top of `compute_with_epoch`, or
`debug_assert!(members.windows(2).all(|w| w[0] <= w[1]))`.

### F-02 (LOW, confidence MEDIUM) — No integration test drives a real subset-master write-rejection AND read-passthrough end to end
**Where:** `coordinator.rs:6060` (`is_master`), `migration.rs:655`
(`has_pending_inbound`), dispatch error sites listed in OK-3.
**What's wrong.** The Transitioning→ERR_MIGRATION_IN_PROGRESS path is covered
at the unit level (dispatch.rs:8928-9009 for GET) and the manager bookkeeping
is heavily unit-tested, but there is no `tests/` integration test that, on a
live two-node cluster mid-migration, asserts (a) a SPEND to a fenced/subset
shard returns code 19, AND (b) a GET on the same key still succeeds (reads
are intended to pass through — migration.rs:660 "Reads continue locally"). The
read-passthrough-during-fence behavior in particular has no end-to-end guard.
**Why it matters.** This fence is the barrier preventing writes to a
not-yet-authoritative master (a split-brain / double-spend risk). A
regression that either leaks writes through, or that wrongly fences reads
(availability), would not be caught at the wire level.
**Reproduction.** None exists; add the integration test described.
**Suggested fix.** Add a cluster integration test exercising both arms.

### F-03 (LOW, confidence LOW) — `/admin/drain/{node}` completion semantics not verified
**Where:** `src/server/http.rs:1686-1718` → `cluster.drain_node` defined in
`src/cluster/topology.rs` (coordinator.rs has zero `drain` references).
**What's wrong.** I could not reliably read `topology.rs::drain_node` this
session, so I cannot confirm it BLOCKS until the drained node's shards have
fully migrated (e.g. polling `pending_handoff_count()`/inbound completion)
rather than returning as soon as it proposes the new topology.
**Why it matters.** If `drain_node` returns success before handoff completes,
an operator could shut the node down while it still holds the only copy of
some shards → data loss.
**Reproduction.** Re-read `topology.rs::drain_node` and add a test asserting
it resolves only after the node's shards are no longer pending. Not
reproducible/refutable in this session.

---

## Items I could NOT fully verify (tooling degradation)
- The exact name of the shared dispatch routing function owning the
  MIGRATION_IN_PROGRESS block at dispatch.rs:627-829 (the block contents and
  that all write handlers funnel through cluster routing ARE confirmed; the
  enclosing fn identifier was not pinned down).
- Read-on-new-master timeout/what-client-sees on expiry: the GET handler
  (dispatch.rs ~5290-5380) returns ERR_MIGRATION_IN_PROGRESS for a
  Transitioning state (confirmed via tests at 8928-9009), but I could not
  fully trace whether there is a bounded server-side wait/barrier vs an
  immediate reject, nor the client retry/expiry contract.
- Crash-DURING-migration commit ordering (does the source delete its copy
  only AFTER the target's inbound is marked complete and the handoff is
  durably committed?). The inbound side is provably safe (OK-4/5/6); the
  source-side delete-after-commit ordering in `coordinator.rs` (429 KB) was
  not traced to completion. Recommend a dedicated re-audit with working
  tooling focused on `mark_complete` → orphan-cleanup/delete ordering.
