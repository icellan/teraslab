# Audit F — Sharding and Migration

Scope: `src/cluster/{shards,migration,routing,coordinator}.rs`, `src/server/dispatch.rs`
(shard ownership/fence enforcement), `src/server/http.rs` (drain/rebalance),
`src/config.rs` (migration knobs), `tests/migration_fence.rs`, `tests/cluster_tcp.rs`,
README sharding section, `phases/09_clustering.md`.

Method: static trace of the production migration path (`run_migration_batch`, pipelined)
— note that `migrate_single_shard` (coordinator.rs:4042) is `#[allow(dead_code)]`
reference code; findings below are against the live pipelined flow.

---

### [CRITICAL] Acked writes can be lost in the unfence-before-commit window at migration completion

**Location:** `src/cluster/coordinator.rs:180-227` (`complete_migration_task_current_epoch`), enforcement at `src/server/dispatch.rs:2682-2735` (`check_shard_ownership`), deletion at `src/cluster/coordinator.rs:3970-4025` (`cleanup_orphaned_shard_if_settled`).

**What's wrong:** On successful migration the source executes, in order:
1. `mgr.mark_complete(task)` under the migration lock — this **lifts the write fence**
   (`unfence_shard`) and **closes the dual-write window** (`dual_write_remove`)
   (`src/cluster/migration.rs:736-761`), and clears `fenced_bm`;
2. *then*, under a separate shard-table lock, `shard_table.write().commit_shard(shard)`;
3. *then* `cleanup_orphaned_shard_if_settled` deletes the shard's records from the source.

Between (1) and (2) the source is still the **effective master** for the shard
(`effective_assignment` returns the old assignment until `commit_shard`), the fence
bitmap is clear, and dual-write fan-out to the new master is off. A client write
(Create/Spend/etc.) arriving in that window passes `check_shard_ownership`
(`MasterQueryResult::Yes`, `is_shard_write_fenced == false`), is applied to the source
engine, and is **ACKed to the client**. The target already verified the manifest
(collected before this write), so the completion stands. After (2)+(3) the record
(or mutation) exists nowhere: orphan cleanup deletes the source copy and the new
master never received it.

A second window of the same class: the fence is checked **once at dispatch entry**,
not revalidated at engine-apply time. A request thread that passed
`check_shard_ownership` *before* Phase 2 fencing (`coordinator.rs:3479-3498`) can stall
and apply its mutation *after* `collect_manifest_entries` ran. The manifest backstop
only catches mutations applied **before** manifest collection (generation mismatch →
target rejects → retry); a mutation applied after manifest collection but while the
shard is still fenced matches neither the delta (`sequence >= fence_seq` is excluded,
`coordinator.rs:5285-5311`) nor the manifest, and is silently lost at orphan cleanup.
The dead reference path even documents an attempted mitigation ("Drain in-flight
writes: acquire and release the redo lock", coordinator.rs:4209-4214) — which only
drains writers currently *holding* the redo lock, not threads between the dispatch
check and the redo append — and the live pipelined path doesn't even do that.

**Why it matters:** Rubric CRITICAL — a UTXO created or a spend applied in these
windows is acknowledged to Teranode and then destroyed. The windows are
microsecond-to-millisecond scale, but at the stated 10M ops/sec design target a
window of even 100 µs per migrated shard across 1024 shards per topology change is a
realistic loss event, and there is no synchronization preventing it — only timing.

**Reproduction:** Deterministic interleaving test: pause a write thread (fault-injection
hook) after `check_shard_ownership` returns `None` for a fenced-to-be shard; run a full
migration of that shard to completion; resume the write; assert STATUS_OK is returned;
then assert the key exists on the new master — it won't, and after
`cleanup_orphaned_shard_if_settled` it's gone from the source too. Second variant:
inject a delay between `mark_complete` and `commit_shard` and fire a Create at the
source during the delay.

**Suggested fix:** Invert the completion order: `commit_shard` (routing flips, source
starts returning REDIRECT) **before** lifting the fence; the fence then drains
straggler requests that already passed the routing check. For the TOCTOU window,
revalidate the fence (or a per-shard epoch) inside the engine critical section before
the redo append, or run a dispatch visibility barrier (the machinery in
`needs_dispatch_visibility_barrier` exists) between setting the fence and capturing
`fence_seq` / collecting the manifest.

---

### [HIGH] Migration restores primary metadata but not secondary-index state (DAH/unmined) or primary-index cached fields on the target

**Location:** `src/replication/receiver.rs:1036-1067` (`apply_create_lifecycle_and_blob`), `src/cluster/coordinator.rs:4563-4581` (baseline `meta_buf`, 70 bytes), `src/ops/engine.rs:2126-2138, 2175-2181` (create-time index/secondary population), read path `src/server/dispatch.rs:5556-5605` (fully-cached GET).

**What's wrong:** Baseline migration serializes lifecycle state (`generation`,
`updated_at`, `unmined_since`, `delete_at_height`, `preserve_until`) in bytes 46..70 of
`metadata_bytes`. On the target, `engine.create()` runs first with `block_height = 0`
(the optional extended block fields start at offset 70, and migration `meta_buf` is
exactly 70 bytes), so `meta.unmined_since = 0` at create time → **no unmined-index
entry** is created and the `TxIndexEntry` caches `unmined_since = 0`,
`dah_or_preserve = 0`. Then `apply_create_lifecycle_and_blob` patches the master's real
`unmined_since` / `delete_at_height` / `preserve_until` **directly into device metadata
via `io::write_metadata`**, bypassing:
- the **unmined secondary index** (never inserted for unmined migrated records),
- the **DAH secondary index** (never inserted for migrated records with a pending
  delete-at-height),
- the **primary-index cached fields** (`TxIndexEntry.unmined_since`,
  `dah_or_preserve` stay 0).

Mined state is replayed via proper `SetMined` ReplicaOps (indexes maintained), but
unmined/DAH/preserve state is metadata-only. The coordinator comment
(coordinator.rs:5249-5253) — "replicas rebuild their secondaries from their own
metadata replay" — is only true at **process startup**; a live migration target never
rebuilds.

Consequences on the new master, post-migration, until its next restart:
1. `OP_PROCESS_EXPIRED_PRESERVATIONS` (DAH sweep) never sees migrated records whose
   `delete_at_height` was set on the old master → records are never deleted
   (unbounded space leak; defeats the DAH lifecycle Teranode relies on).
2. `OP_QUERY_OLD_UNMINED` misses every migrated unmined record → unmined-transaction
   cleanup silently skips them.
3. The fully-cached GET fast path (`handle_get_batch`, `field_mask.fully_cached()`)
   serves `unmined_since = 0` / `delete_at_height = 0` from the stale `TxIndexEntry`
   while a slow-path (metadata) read of the same record returns the real values —
   the same key answers differently depending on the field mask.

**Why it matters:** Realistic correctness bug visible to clients (inconsistent GET
results) plus a lifecycle-management failure (DAH deletions and unmined cleanup
silently stop working for migrated records). Not data loss, hence HIGH not CRITICAL.

**Reproduction:** Two-node cluster; on node A create a record, set DAH (e.g. spend with
retention) and another record left unmined at height H. Add node B, wait for the shard
to migrate. On B: (a) `OP_GET_BATCH` with a fully-cached field mask asking
DELETE_AT_HEIGHT/UNMINED_SINCE → returns 0s; same GET with RAW_METADATA → returns real
values. (b) `OP_QUERY_OLD_UNMINED` for cutoff > H → migrated record absent.
(c) advance height past DAH and run the sweep → record not deleted. Restart B → all
three behave correctly (startup rebuild masks the bug).

**Suggested fix:** In `apply_create_lifecycle_and_blob`, route the lifecycle patch
through engine APIs that maintain the indexes (e.g. an
`engine.restore_lifecycle(key, unmined_since, dah, preserve_until)` that updates
`TxIndexEntry` and calls `update_unmined_index` / `update_dah_index`), or update the
index entry + secondaries explicitly after the metadata write.

---

### [MEDIUM] No crash-mid-migration integration test

**Location:** `tests/cluster_tcp.rs` (clean-path tests at 770, 851, 947, 1036, 1104, 1141, 1807), `tests/migration_fence.rs` (fence semantics via test hooks), persistence machinery `src/cluster/migration.rs:1250-1303`, restore wiring `src/bin/server.rs:908-909`.

**What's wrong:** The crash-safety design is present and correctly wired: inbound
migration state is persisted (atomic temp+rename, fsync) on **every** change
(`RunningCluster::mark_inbound_active/complete*`, coordinator.rs:6193-6264) and
restored at startup, so a target that crashes mid-inbound refuses writes for those
shards after restart until a source proves completion (manifest evidence is mandatory,
including for empty shards — `ERR_MIGRATION_MANIFEST_REQUIRED`,
dispatch.rs:718-735). Source failure rolls the shard back to the old master
(`fail_migration_task_*` → `rollback_shard`). Deletes during migration are forwarded
(`RedoOp::Delete` → `ReplicaOp::Delete`, coordinator.rs:5217-5225) and the
exact-entry manifest **prunes stale keys on the target**
(dispatch.rs:739-765) — so resurrection is covered. But **no integration test
kills a node while a migration is actively streaming** and verifies, after recovery:
no record lost, none duplicated, none served by two masters. All existing
loss/duplication tests (`no_records_lost_during_migration`,
`no_duplicate_records_after_migration`) cover clean migrations only; the
restore-inbound test (coordinator.rs:7432) is a unit-level serialize/restore check.

**Why it matters:** The crash-recovery path is exactly the code that never runs in CI.
The state machine has at least four distinct crash points (source pre-fence,
source post-fence/pre-complete, target pre-verify, target post-verify/pre-commit)
and only static reasoning currently backs them.

**Reproduction:** Add a fault-injection test: 3-node cluster with data, add node 4,
SIGKILL the source process while `migration_status` shows `Streaming`, restart it,
wait for re-plan, then assert the union of all nodes' records equals the original set
with no key counted as master on two nodes (`OP_ADMIN_DIAGNOSE_KEY` exists for this).

**Suggested fix:** Add the above as a `fault-injection`-gated integration test; cover
both source-kill and target-kill at the Fenced stage.

---

### [MEDIUM] Redirect loop protection exists only as an unused helper; no in-repo client follows redirects; empty-address redirects possible

**Location:** `src/protocol/codec.rs:1479-1574` (`encode_redirect_with_version`, `classify_redirect`, `RedirectFollowDecision`), `src/server/dispatch.rs:2756-2785` and `5500-5540` (emit sites), `src/bin/cli.rs` (no redirect handling).

**What's wrong:** The server-side design is sound: every REDIRECT carries the target
address **and** the issuing node's `shard_table_version`, and `classify_redirect`
implements the documented rule (server version ≤ client version → `Stale`, stop
following). That defeats A→B→C→A loops without a hop counter. However:
1. **No client in this repo follows redirects at all** — `cli.rs` and the test
   harnesses surface the error; `classify_redirect`'s only callers are its own unit
   tests and one dispatch test. The hop-limit / loop-stop behavior is therefore
   delegated entirely to the external Go adapter and is untested end-to-end here.
   A legacy client that parses only the address (which the codec explicitly supports,
   codec.rs:1493-1508) gets **no** loop protection.
2. When the routing table names a master whose address is unknown,
   `check_shard_ownership` emits a redirect with an **empty address**
   (`dispatch.rs:2766-2769`; GetBatch path uses `String::new()`,
   dispatch.rs:5516-5519). A client following that gets an unparseable target; a
   distinct retryable error (or `ERR_MIGRATION_IN_PROGRESS`) would be honest.

**Why it matters:** The checklist question "client following redirect to a node that
also migrated → no infinite loop" can only be answered "the mechanism exists, nothing
in this repo exercises it." Coverage gap, not a server bug.

**Reproduction:** Grep: `classify_redirect` has zero non-test callers. For (2): remove
a node's addr entry from `node_addrs` while it is still a shard master in the table,
issue a write for that shard at another node, observe `ERR_REDIRECT` with
`addr_len == 0`.

**Suggested fix:** (1) add a redirect-following test client (or test against the Go
adapter) asserting loop termination via the version rule plus a belt-and-braces hop
cap; (2) return a non-redirect retryable error when `node_addr` is `None`.

---

### [MEDIUM] Spec/code divergence: phases/09 mandates Redirect-on-write during migration; implementation (and README) do dual-write + fence instead

**Location:** `phases/09_clustering.md:270, 357-358` vs `src/server/dispatch.rs:2695-2720`, `src/cluster/migration.rs:453-458` (dual-write), `README.md:556-561`.

**What's wrong:** Phase 09 says: "Writes arriving at node A [old master during
migration]: node A returns a **Redirect** pointing the client to node B" and its
checklist requires "During migration: writes return Redirect to new node". The
implementation instead: old master keeps **accepting** writes during
Preparing/Streaming (captured by delta streaming + dual-write fan-out), rejects with
`ERR_MIGRATION_IN_PROGRESS` only during the brief Fenced window, and redirects only
**after** commit. README.md:556-561 documents the implemented behavior. Per repo Rule 6
this is a pick-one situation: the implemented protocol is the safer one (redirecting
to a target that lacks the data would either block on pending-inbound or serve
not-found), so the phase doc is the stale artifact. Additionally a stale comment in
`check_shard_ownership` (dispatch.rs:2697: "Reads are handled separately with a wait
loop") contradicts the actual GetBatch behavior, which returns
`ERR_MIGRATION_IN_PROGRESS` immediately "instead of parking a request thread"
(dispatch.rs:5541-5556) — there is no wait loop and no server-side timeout; the
client is expected to poll/retry with backoff (README.md:561).

**Why it matters:** Anyone implementing a client against phases/09 will build the
wrong retry behavior (expect REDIRECT, get MIGRATION_IN_PROGRESS). The "wait briefly"
behavior the audit checklist asks about does not exist in code or current docs — the
documented contract is immediate retryable error, unbounded from the client's view.

**Reproduction:** Read the two documents; run `tests/cluster_tcp.rs::tcp_write_to_pending_inbound_shard_returns_migration_in_progress`.

**Suggested fix:** Update phases/09 §"Write proxying during migration" and its
checklist to describe the implemented fence/dual-write/manifest protocol; fix the
dispatch.rs:2697 comment.

---

### [LOW] Legacy `ShardTable::compute` derives an order-dependent version from an order-independent table

**Location:** `src/cluster/shards.rs:305-315`.

**What's wrong:** `compute()` folds `version_hash += m.0 * (i+1)` over the member slice
in **caller order**, then calls `compute_with_epoch` (which sorts internally). Two
nodes passing the same member *set* in different orders derive identical assignments
but **different versions**. Version inequality is used for staleness/loop detection
(`shard_table_version` in redirects and the partition map). Production paths all use
`compute_with_epoch` with the topology term (coordinator.rs:572, 1799, 1821; only
tests and http.rs test code call `compute`), so this is latent, but the function is
`pub` and documented as a "bootstrap path".

**Why it matters:** A future caller wiring `compute` into a bootstrap/recovery path
reintroduces version divergence — the exact class of bug the F-01 internal sort was
added to kill for assignments.

**Reproduction:** `ShardTable::compute(&[NodeId(1),NodeId(2)],2).version != ShardTable::compute(&[NodeId(2),NodeId(1)],2).version`.

**Suggested fix:** Sort before hashing (or hash the sorted copy), or demote `compute`
to `#[cfg(test)]`.

---

## Checklist disposition

1. **`shard = u16_le(txid[0..2]) & 0x0FFF` masking + consistency** — ✅
   Single canonical implementation `ShardTable::shard_for_key`
   (`src/cluster/shards.rs:323-326`): `u16::from_le_bytes([txid[0], txid[1]]) & 0x0FFF`.
   0x0FFF = 12 bits = 4096 = `NUM_SHARDS`; matches README.md:546 and phases/09:81-84.
   `rg` over the whole tree: every production shard computation (ops/engine.rs:639,
   689, 740, 981, 999, 1017; dispatch.rs; coordinator.rs:2242, 3111; bin/server.rs:1049)
   delegates to `shard_for_key`. The only two independent `& 0x0FFF` sites
   (dispatch.rs:9883, coordinator.rs:7141) are inside `#[cfg(test)]` modules
   (test mods start at 7041 / 7121) and compute the *inverse* (shard → txid) with the
   same mask. No divergence.

2. **Round-robin assignment deterministic across nodes** — ✅
   `compute_with_epoch` sorts a local copy of the member list (F-01,
   shards.rs:112-116) so caller order is irrelevant; pure function of
   (member set, RF, epoch); epoch comes from the quorum-committed topology term.
   Tests `compute_unsorted_members_identical_to_sorted`,
   `compute_same_members_different_order_identical` cover it.
   `set_master_for_shard` (partition-view election) refuses candidates outside the
   shard's existing assignment (shards.rs:369-390), so a stale view can't fork the
   table. ⚠️ residual: legacy `compute()` version hash is order-dependent (LOW finding).

3. **Writes to pending-inbound shard return MIGRATION_IN_PROGRESS on EVERY write op** — ✅
   `check_shard_ownership(..., allow_if_migrating=false)` rejects on
   `has_pending_inbound` **and** `is_shard_write_fenced` (dispatch.rs:2695-2720) and is
   called by all 15 mutating handlers: spend (2835), unspend (3163), set_mined (3325),
   create (3644), freeze (4038), unfreeze (4152), reassign (4265), set_conflicting
   (4416), set_locked (4537), preserve_until (4659), delete (4873 + parent keys 4997),
   mark_longest_chain (5334), preserve_transactions (5933), process_expired (6066),
   stream_chunk (6336). This matches the `is_mutation_opcode` list (dispatch.rs:2513-2531)
   plus the two streaming ops. `Transitioning` topology also yields
   ERR_MIGRATION_IN_PROGRESS rather than a possibly-wrong redirect. Integration
   coverage: `tcp_write_to_pending_inbound_shard_returns_migration_in_progress`,
   `migration_fence.rs`. ⚠️ the fence check is dispatch-entry-only (TOCTOU — see
   CRITICAL finding).

4. **Reads on new master before migration completes** — ⚠️
   There is **no** "wait briefly" behavior: GetBatch on the new master with pending
   inbound and no local data returns `ERR_MIGRATION_IN_PROGRESS` immediately
   (dispatch.rs:5541-5556), explicitly "instead of parking a request thread". README:561
   documents exactly this ("return MIGRATION_IN_PROGRESS quickly; clients should
   poll/retry with backoff") — so code and current docs agree; no timeout exists, the
   client owns retry policy. The stale "wait loop" comment at dispatch.rs:2697 and the
   phases/09 Redirect-on-write text are doc drift (MEDIUM finding).

5. **Migration interrupted by crash: no loss/dup/dual-live** — ⚠️
   Design verified statically: inbound state fsync-persisted on every change and
   restored at startup (target refuses writes after crash); completion requires
   cryptographic manifest evidence even for empty shards; source failure →
   `rollback_shard` to old master; exact-entry manifest prunes stale target keys;
   post-commit `cleanup_orphaned_shard_if_settled`/`run_orphan_cleanup` removes
   source copies only after the epoch settles and nothing is active/failed. No
   concrete crash-induced loss/dup scenario found in the protocol itself, **but**
   (a) the unfence-before-commit window loses acked writes without any crash
   (CRITICAL finding), and (b) there is no crash-mid-migration integration test
   (MEDIUM finding).

6. **REDIRECT correct target + no infinite loop** — ⚠️
   Target address comes from `cluster.node_addr(route.node)` of the routed master and
   is version-stamped (`encode_redirect_with_version`); `classify_redirect` implements
   loop-stop (server version ≤ client version → stop). But no in-repo client follows
   redirects, there is no hop counter anywhere, and an unknown node address yields a
   redirect with an empty address (MEDIUM finding).

7. **migration_pool_size / migration_batch_size are live** — ✅
   Both flow `config.rs:694-699` → `bin/server.rs:871-872` → `ClusterCoordinator`
   (clamped `.max(1)`, coordinator.rs:633-634) → `run_migration_batch`.
   `pool_size` splits the per-target task list into `total.div_ceil(pool_size)` chunks,
   one TCP connection + worker thread each (coordinator.rs:3296-3335);
   `batch_size` chunks baseline streaming (`shard_keys.chunks(batch_size)`,
   coordinator.rs:4525) and scales TCP timeouts (`migration_stream_timeout`).
   TOML round-trip covered by config.rs tests. Not dead config. README descriptions
   accurate.

8. **Drain actually drains** — ✅
   `PUT /admin/drain/{node_id}` (self-only, mismatched id → 400) triggers `quiesce()`
   which commits a topology term that excludes the node (no reliance on SWIM death
   detection), and the normal activation path migrates all master shards away.
   With `?wait_seconds=N` it polls `cluster_drain_complete` = zero current master
   shards **and** zero target master shards **and** zero pending handoffs **and** zero
   active migrations (http.rs:1372-1400), returning 200 only when true, 202 otherwise.
   Outbound migrations count as active until the target verifies the manifest, so
   "done" implies the data really left. Unit test
   `drain_wait_helper_reports_completion_only_after_self_has_no_master_shards` covers
   the predicate. Without `wait_seconds` it returns 202 "initiated" — explicitly not a
   completion claim.

9. **In-flight ops at fence moment** — ⚠️ Not drained deterministically; manifest
   verification is the backstop and it has a hole after manifest collection
   (folded into the CRITICAL finding).
   **Secondary-index state migration** — ❌ Only primary record state is properly
   reconstructed; unmined/DAH secondaries and primary-index cached lifecycle fields
   are not (HIGH finding).
   **Tombstones for deletes during migration** — ✅ `RedoOp::Delete` in the
   snapshot→fence window is forwarded as `ReplicaOp::Delete`
   (coordinator.rs:5217-5225), and the exact-entry manifest in
   `OP_MIGRATION_COMPLETE` deletes any target key not in the source's final set
   (dispatch.rs:739-765), covering deletes that raced the baseline.

**Tally:** ✅ 5 (shard mask/consistency, determinism, write-fence coverage,
config knobs, drain, delete tombstones — counting 9's tombstone sub-item under its ✅)
· ⚠️ 4 (read behavior docs, crash-recovery verification, redirect loop coverage,
in-flight-op drain) · ❌ 1 (secondary-index migration).
Findings: 1 CRITICAL, 1 HIGH, 3 MEDIUM, 1 LOW.
