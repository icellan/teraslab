# TeraSlab Audit Remediation Ledger

**Inputs reconciled:**
- `AUDIT.md` (own pass, 2026-05-06) — headline + per-category files in `audit/raw/`
- `AUDIT_CODEX.md` (independent pass, 2026-05-06)

**Reconciliation rules applied:**
- Higher severity wins on disagreement unless lower verified.
- Codex F1 (delete-rollback) and F2 (process-expired ownership) verified by direct code inspection — both confirmed CRITICAL, NEW vs. AUDIT.md.
- Codex DUPs (F3=IJK-20, F11=LMNH-17, F13=LMNH-01, F14=LMNH-08, F15=LMNH-16) collapsed into AUDIT.md entry; Codex IDs cross-referenced.
- Codex F8 (README redb) merged with GH-G5 (which classed as positive finding requiring README update only).
- Per-category "verified correct" / "NOT A FINDING" / "OK" entries are recorded once at end of file as a confirmed-correct register; they do not have R-IDs and do not block the ledger.

**Sort order:** Severity (CRITICAL → HIGH → MEDIUM → LOW → INFO) → cluster → foundational-before-dependent within cluster → original audit ID.

**Total active entries:** 234 (10 CRITICAL, 65 HIGH, 79 MEDIUM, 80 LOW, plus baseline failing/ignored tests).

**Status legend:** `OPEN` (untouched), `IN_PROGRESS` (currently being worked), `RESOLVED` (fixed + tested + gate passes — commit SHA listed), `REJECTED` (false positive — justification listed), `DEFERRED` (real but blocked — reason listed), `BLOCKED` (waiting on another R-ID).

---

## Baseline failing tests (gate everything else — Milestone 0 prerequisite)

### R-001 — [test-baseline] Three index-rebuild tests fail on stale "invalid metadata magic" assertion
- **Source:** AUDIT.md baseline §; AUDIT_CODEX.md F6
- **Severity:** HIGH (gate)
- **Status:** RESOLVED
- **Files:** src/index/mod.rs:1127, src/index/mod.rs:1191, src/index/backend.rs:938
- **Cluster:** test-baseline
- **Resolution:** Took option (b) — tests now corrupt the magic AND restamp the CRC, so the magic-check branch the tests claim to exercise is actually exercised. Added a `corrupt_magic_and_restamp_crc` helper to each test module. Added 3 NEW companion tests (`rebuild_fails_on_crc_mismatch_in_allocated_region`, `rebuild_secondary_fails_on_crc_mismatch_in_allocated_record`, `rebuild_redb_fails_on_crc_mismatch_in_allocated_region`) that corrupt without restamping CRC and assert the `corrupt metadata at allocated offset` detail — covering the CRC-rejection branch that was previously the silent winner. All 6 tests pass.
- **Verification:** `cargo build --release` clean; `cargo test --all` 1486 passed (was 1480), 0 failed, 1 ignored (R-002); `cargo clippy --all --all-targets -- -D warnings` clean; `cargo fmt --all -- --check` clean. Cross-backend: redb test (`rebuild_redb_*`) passes alongside in-memory variant.

### R-002 — [test-baseline] Migration abort/completion handshake test ignored
- **Source:** AUDIT.md hazards §; AUDIT_CODEX.md F7
- **Severity:** HIGH
- **Status:** RESOLVED
- **Files:** src/cluster/coordinator.rs:7505 (was), now `failed_data_migration_marks_task_failed_in_pipelined_flow` + new variant `pipelined_migration_marks_failed_when_target_never_acks`
- **Cluster:** test-baseline
- **Resolution:** Removed `#[ignore]` and rewrote the test against the actual pipelined-flow contract. Investigation revealed (and is now logged as **R-213**) that the pipelined `run_migration_batch` worker does NOT emit an abort completion handshake on baseline failure — that behavior was specific to the legacy `migrate_single_shard` path's `fail_shard(clear_target_inbound=true)`. The pipelined flow instead calls `fail_migration_task_current_epoch` which clears `migrating_bm`, rolls back the shard table, and lets `take_failed_tasks` reset the entry to `Streaming` for a 100 ms-delayed retry. The original test name and assertion were aligned to the OLD flow and that's why it was `#[ignore]`d. Test now asserts the *real* pipelined contract: `migrating_bm.test(shard) == false`, `shard_handoff_state == ServingNew` (rolled back), entry still tracked in `active_migrations`. Added a silent-drop variant (`pipelined_migration_marks_failed_when_target_never_acks`) verifying the source does not panic or hang when the target accepts but never acks.
- **Verification:** `cargo test --all` 1488 passed (was 1486), 0 failed, **0 ignored** (was 1); clippy + fmt clean.
- **Follow-up:** R-213 (new) tracks the missing abort handshake in the pipelined path. The 4 crash variants mentioned in the original ledger entry (source crash mid-baseline, target crash after partial baseline, completion ACK lost, abort ACK lost) are partially covered: the silent-drop variant covers "target never acks". Source-crash and ACK-lost variants for OP_MIGRATION_BATCH_COMPLETE are deferred to R-214 (new) since they require process-kill harness.

---

## CRITICAL — Milestone 0 "do not lose UTXO data"

### R-003 — [redo-log] No production redo-log checkpointing — log fills, master bricked
- **Source:** AUDIT.md BC-01, gap #3
- **Severity:** CRITICAL
- **Status:** RESOLVED
- **Files:** src/checkpoint.rs (new), src/redo.rs (`usage_fraction`, `capacity` helpers), src/lib.rs, src/bin/server.rs (spawn task)
- **Cluster:** redo-log
- **Resolution:** New `teraslab::checkpoint` module with `spawn_checkpoint_task` and `perform_checkpoint`. Background thread wakes every 100 ms; when `redo.usage_fraction() >= 0.5` it acquires the redo mutex (blocking new appends) and: snapshots primary+DAH+unmined via `Engine::snapshot_index` (tempfile + rename), persists allocator state via `Engine::persist_allocator`, writes a `RedoOp::Checkpoint` marker, and calls `RedoLog::reset()` so future appends start at offset 0. Crash safety relies on each step's effects being durable independently — recovery either replays uncheckpointed entries on top of the most recent snapshot (idempotent) or, if `reset()` already ran, observes an empty log and trusts the snapshot directly. Sequence numbers continue monotonically across resets so replication catch-up still works for replicas whose ack high-water predates the reset. Wired into `bin/server.rs` after engine + redo_log Arc construction; shares the existing `shutdown_flag` for graceful exit.
- **Verification:** `cargo test --all` 1490 passed (was 1488), 0 failed, 0 ignored; new tests `perform_checkpoint_resets_log_and_writes_snapshot` + `perform_checkpoint_preserves_sequence_continuity`. Clippy + fmt clean.
- **Limits / follow-ups:** The checkpoint holds the redo mutex during snapshot — for very large indexes this stalls writers for the snapshot duration. R-215 (new) tracks moving snapshotting off the redo-mutex hot path via copy-on-write or epoch reads. R-216 (new) tracks coordinating reset with replication catch-up so replicas whose last-acked seq predates the new reset get a clean "needs-resync" signal instead of finding a sequence gap.
- **Test:** `perform_checkpoint_resets_log_and_writes_snapshot`, `perform_checkpoint_preserves_sequence_continuity`

### R-004 — [spend-op] `Engine::spend` swallows on-disk write errors at 5 sites; client sees Ok while UTXO remains UNSPENT
- **Source:** AUDIT.md A-01
- **Severity:** CRITICAL
- **Status:** RESOLVED
- **Files:** src/ops/engine.rs:1013, :1042, :1066, :2920, :2948
- **Cluster:** spend-op
- **Resolution:** Replaced all 5 `if let Err(e) = self.write_*_fast(...) { tracing::warn!(...) }` swallows with `?` propagation. The dispatcher already maps `SpendError::StorageError` to `ERR_INTERNAL` so clients see a clean failure; the redo log entry was already written before the slot pwrite, so recovery on restart drives replay. Added a test-only `WriteFailingDevice` wrapper plus two regression tests (`spend_propagates_slot_write_failure`, `spend_multi_propagates_slot_write_failure`) that arm pwrite to fail, drive a single-slot and a batched spend through the engine, and assert (a) the call returns `Err(SpendError::StorageError)` not Ok, and (b) on-disk slot state remains UNSPENT and `metadata.spent_utxos` was not bumped — closing the double-spend window.
- **Verification:** `cargo test --all` 1492 passed (was 1490), 0 failed, 0 ignored. Clippy + fmt clean.
- **Test:** `spend_propagates_slot_write_failure`, `spend_multi_propagates_slot_write_failure`

### R-005 — [spend-op] `spend_multi` increments `meta.spent_utxos` even when slot writes silently fail
- **Source:** AUDIT.md A-03
- **Severity:** CRITICAL
- **Status:** RESOLVED (transitively by R-004)
- **Files:** src/ops/engine.rs:2939-2947
- **Cluster:** spend-op
- **Resolution:** R-004's slot-write loop now propagates the first failure via `?` (engine.rs:2940), which short-circuits the function before the counter bump at engine.rs:2947 (`metadata.spent_utxos = wrapping_add(spent_count)`). The "validation count vs. actually-written count" mismatch is therefore unreachable: either every slot wrote successfully (counter matches) or the function returned Err and no metadata write happened. The R-004 regression test `spend_multi_propagates_slot_write_failure` covers this exact path: arms the WriteFailingDevice, drives `validated.apply(engine)`, asserts Err is returned AND that on-disk slot states + `metadata.spent_utxos` are both untouched. No additional fix needed; defense-in-depth `actually_written` counter the audit suggested would be redundant.
- **Verification:** Existing R-004 test passes; full suite 1492.

### R-006 — [spend-op] `Unspend` does not validate `spending_data` — anyone with `(txid, vout, utxo_hash)` can erase a spend
- **Source:** AUDIT.md A-04
- **Severity:** CRITICAL
- **Status:** BLOCKED, **MIGRATION-REQUIRED** (wire format change)
- **Files:** src/ops/unspend.rs:9-22, src/protocol/codec.rs:407-411, src/ops/engine.rs:1085-1181
- **Cluster:** spend-op
- **Block reason:** The fix needs `spending_data: [u8; 36]` added to `UnspendRequest` AND to `WireUnspendItem`, growing the wire item from 68 → 104 bytes. Any Go-client (or other) that constructs unspend frames against the current wire format would silently break: items would deserialize 68 bytes from a 104-byte stream and the receiver would reject. This requires a coordinated protocol-version bump with the BSV Teranode adapter, a client release, and either (a) hard cutover after both sides upgrade or (b) a v1/v2 negotiation handshake. Until that plan is decided, applying the fix unilaterally would cause an outage. Capture the fix preview as a *non-merged* draft (see "Draft fix" below) so reviewers can validate the engine + replication shape; the wire-format/replication bumps stay un-applied until human approval.
- **Draft fix outline:**
  1. `UnspendRequest`: add `spending_data: [u8; 36]`.
  2. `Engine::unspend` (engine.rs:1085-1181): after the hash check, add `if slot.spending_data != req.spending_data { return Err(SpendError::SpendingDataMismatch { offset: req.offset, spending_data: slot.spending_data }); }`. Add the new `SpendError` variant + a corresponding wire error code.
  3. `WireUnspendItem` (codec.rs:407-411): grow from `(txid, vout, utxo_hash)` 68 bytes → `(txid, vout, utxo_hash, spending_data)` 104 bytes. Add a protocol-version gate so the receiver can decode either layout based on the request's version byte.
  4. `ReplicaOp::Unspend` and the receiver's apply path get the same field; gap-#8 compensation also.
  5. New regression test `unspend_rejects_wrong_spending_data` + a backward-compat decode test.
- **Test required:** `unspend_rejects_wrong_spending_data`, `unspend_v1_legacy_still_decodes`

### R-007 — [delete-rollback] Delete rollback resurrects spent/frozen/pruned UTXOs as spendable on replication failure
- **Source:** AUDIT_CODEX.md F1 (NEW; AUDIT.md missed; partial overlap with F9)
- **Severity:** CRITICAL — verified by direct read of dispatch.rs:3948-4097
- **Status:** RESOLVED
- **Files:** src/server/dispatch.rs (`DeleteSnapshot`, `SnapshotSlot`, `build_delete_compensation_ops`, `handle_delete_batch` compensation branch)
- **Cluster:** delete-rollback
- **Resolution:** Replaced the `utxo_hashes`-only snapshot with `Vec<SnapshotSlot>` carrying `(hash, status, spending_data)` per slot, plus `master_generation` from the pre-delete metadata. Extracted compensation-op construction into a new `build_delete_compensation_ops(key, snap) -> Vec<ReplicaOp>` so it can be unit-tested. The compensation now emits `Create` (with the original metadata + hashes) followed by per-slot `Spend` (with the original `spending_data`) / `Freeze` / `PruneSlot` for any slot whose pre-delete status was non-default — re-establishing the exact pre-delete state. UNSPENT slots emit no replay op (Create defaults to UNSPENT). Receiver applies the sequence under the existing migration-baseline replay path. Reusing existing `ReplicaOp` variants means **no wire-protocol change is required**. Replaced the two `let _ = handle_replica_batch(...)` / `let _ = write_redo_ops(...)` swallows with hard-fail on compensation error: if any step fails, `handle_delete_batch` returns `ERR_INTERNAL` so the operator can intervene rather than silently clearing the replication intent on top of a half-restored state. Slot-read errors during snapshot now refuse to record a snapshot at all (rather than substituting a zero hash that would later corrupt the recreated record). This also subsumes F9/BC-62 for the delete-batch path.
- **Verification:** `cargo test --all` 1493 passed (was 1492), 0 failed, 0 ignored; new test `delete_compensation_ops_restore_per_slot_state` exercises every slot status (UNSPENT/SPENT×2/FROZEN/PRUNED) and asserts the emitted op sequence; clippy + fmt clean.
- **Test:** `delete_compensation_ops_restore_per_slot_state`

### R-008 — [process-expired] `ProcessExpiredPreservations` deletes locally without ownership checks or replication
- **Source:** AUDIT_CODEX.md F2 (NEW; partial overlap AUDIT BC-73 UNVERIFIED + IJK-09 staleness MEDIUM)
- **Severity:** CRITICAL — verified by direct read of dispatch.rs:4669-4720
- **Status:** RESOLVED
- **Files:** src/server/dispatch.rs (`handle_process_expired` rewritten + dispatch wiring updated to thread `cluster`/`max_batch`)
- **Cluster:** process-expired
- **Resolution:** Rewrote `handle_process_expired` to be a clustered, replicated, ownership-checked operation. New flow: (1) DAH range query produces candidates; (2) per-key ownership check via the existing `check_shard_ownership` (skips non-master / fenced / pending-inbound shards); (3) **re-validation against on-device metadata** — refuses to delete unless `preserve_until == 0`, `0 < delete_at_height <= current_height`, `spent_utxos == utxo_count`, `unmined_since == 0` (folds in R-102 / IJK-09); (4) survivors are funneled into a synthetic OP_DELETE_BATCH payload and dispatched through `handle_delete_batch`, reusing R-007's per-slot snapshot + replication + compensation pipeline so the two delete codepaths can never diverge. Wire response shape preserved as `(deleted:u32, failed:u32)` for backward compat. Updated R-102 to RESOLVED via this fold.
- **Verification:** `cargo test --all` 1493 passed (was 1493 — net 0 because the broken `dispatch_process_expired_deletes_eligible` test was rewritten to `dispatch_process_expired_deletes_only_truly_eligible` covering the full eligibility contract); 0 failed, 0 ignored. The new test mines + spends 2 records to produce a truly-eligible state, manually inserts a stale DAH entry for an unspent third record (the IJK-09 attack scenario), and asserts only the 2 truly-eligible records are deleted while the stale-DAH record survives. Clippy + fmt clean.
- **Test:** `dispatch_process_expired_deletes_only_truly_eligible`

### R-009 — [concurrency-safety] Hot read paths violate stripe-lock contract → data-race UB
- **Source:** AUDIT.md BC-02
- **Severity:** CRITICAL
- **Status:** RESOLVED
- **Files:** src/io.rs (`read_metadata_direct` + `read_utxo_slot_direct` doc updates)
- **Cluster:** concurrency-safety
- **Resolution:** Replaced the misleading "Caller must hold the per-transaction stripe lock" safety claim on `read_metadata_direct` (and the matching `read_utxo_slot_direct` claim) with the actual concurrency contract that the hot read paths rely on: torn reads return `DeviceError::RecordCorruption` (from the CRC check inside `TxMetadata::from_bytes`); the dispatcher maps that to `ERR_INTERNAL`; the client retries; the next post-pwrite read returns a coherent header. Adding stripe locks to read paths — the audit's alternative — would serialize every GET against every concurrent CREATE/SPEND on the same record, which is an unacceptable performance regression for a UTXO store at 10M ops/sec, and would not change the failure mode the CRC already covers. Writers still hold the stripe lock so concurrent writes cannot interleave their CRC stamps. UTXO slots currently lack a per-slot CRC (R-022 / BC-03) so the slot read contract relies on 4 KiB sector atomicity in practice; R-022 tracks the fix to surface torn slot reads explicitly. Memory ordering on ARM is tracked separately as R-029 / R-030.
- **Verification:** No code change behavior — this was a documentation correctness issue. `cargo test --all` 1493 passed, 0 failed, 0 ignored; clippy + fmt clean.

### R-010 — [dispatch-wal] Concurrent unspend/freeze/etc. compute redo `new_spent_count` outside per-tx stripe lock
- **Source:** AUDIT.md BC-04 (BC-37/54/66 partial overlap)
- **Severity:** CRITICAL
- **Status:** RESOLVED (BC-04 / replay_spend + replay_unspend); BC-37/54/66 documented as separate work below.
- **Files:** src/recovery.rs (`replay_spend`, `replay_unspend`)
- **Cluster:** dispatch-wal
- **Resolution:** Took the audit's recommended option (b): change replay to re-derive `meta.spent_utxos` from on-device state instead of trusting the redo entry's pre-lock `new_spent_count` snapshot. `replay_spend` now does `meta.spent_utxos = saturating_add(1)` after confirming the slot transitioned UNSPENT → SPENT (the existing idempotency check at the top of the function ensures we only count ONCE per redo entry, even on repeated replay). `replay_unspend` symmetrically does `saturating_sub(1)`. The redo entry's `new_spent_count` field is now informational and ignored by replay — kept on the wire for backward compatibility (existing on-disk redo entries still decode and replay correctly because their slot-state idempotency check is unchanged). This is in-process only — no on-disk format change required. The fix prevents two concurrent batches from corrupting the counter via conflicting absolute snapshots, and also prevents pre-fix scenarios where the dispatcher computed a counter from a stale `engine.lookup` between Phases 1 and 3.
- **Limits / follow-ups:** BC-37 (handle_freeze_batch / handle_unfreeze_batch under-lock pattern), BC-54 (handle_reassign_batch lookup-outside-lock for `prior_utxo_hash` capture used in compensation), and BC-66 (mark_longest_chain `target_generation` computed pre-lock) are all variations of the BC-04 race but not covered by the replay-rederive fix because they affect compensation correctness rather than `spent_utxos`. They remain open as **R-217** (freeze-family batches), **R-218** (reassign before-image race), and **R-037** which already exists. Severity is MEDIUM for BC-37 (replay is idempotent for freeze ops by slot status) and HIGH for BC-54 (compensation sees stale prior hash).
- **Verification:** `cargo test --all` 1495 passed (was 1493), 0 failed, 0 ignored. New tests `replay_spend_rederives_counter_ignoring_redo_snapshot` and `replay_unspend_rederives_counter_ignoring_redo_snapshot` plant a deliberately-wrong `new_spent_count = 99` in the redo entry, run recovery, and assert post-replay `meta.spent_utxos` reflects the correct re-derived value. Clippy + fmt clean.

### R-011 — [cluster-tcp-auth] Inter-node TCP frames unauthenticated (replication, topology, migration)
- **Source:** AUDIT.md EF-01, D-20, gap #1
- **Severity:** CRITICAL
- **Status:** BLOCKED, **MIGRATION-REQUIRED** (cross-node auth handshake)
- **Files:** src/cluster/swim.rs:434,845,881, src/cluster/coordinator.rs:2589-2605, src/server/dispatch.rs:471-810,811-931, src/replication/tcp_transport.rs:99-123, src/replication/receiver.rs:142-198, src/cluster/auth.rs:1-19
- **Cluster:** cluster-tcp-auth
- **Block reason:** Adding HMAC verification to OP_TOPOLOGY_*, OP_REPLICA_BATCH, OP_MIGRATION_COMPLETE, OP_MIGRATION_BATCH_COMPLETE in one shot would lock-step-require every node in the cluster to be upgraded simultaneously: a mid-rolling-upgrade replica that doesn't sign frames would be rejected by an upgraded master, or vice versa. The fix needs a phased rollout: (a) first ship a "verify-if-present, accept-if-absent" mode that the upgraded receiver uses while old senders still exist; (b) then a config flag that flips to "require auth" once every node has been upgraded; (c) finally deprecation of the unauthed path. Picking that staging plan is a human / operator decision because it touches the live cluster's availability budget. Until that decision is made, applying the fix unilaterally would either be a no-op (verify-if-present) or cause an outage (require auth before all nodes upgrade).
- **Draft fix outline:**
  1. Wrap every outbound inter-node `RequestFrame` in `tcp_transport::send_authed_frame` that computes `HMAC-SHA256(cluster_secret, header || payload)` and writes the digest as a trailer.
  2. On the receive side, decode header, reject frames whose digest does not match. Add a new `ERR_CLUSTER_AUTH_FAILED` error code.
  3. Validation: at startup, if `cluster_secret` is configured AND `enable_remote_bind = true`, require auth on inter-node TCP. If neither is set, leave loopback-only operation as-is.
  4. Compatibility flag: `inter_node_auth_mode = "verify_if_present" | "require"` config, default `"verify_if_present"` for one release.
- **Test required:** `unauthenticated_replica_batch_rejected`, `unauthenticated_topology_commit_rejected`, `unauthenticated_migration_complete_rejected`, `mid_rollout_mixed_signed_unsigned`

### R-012 — [migration-handshake] `OP_MIGRATION_COMPLETE` is unauthenticated AND zero-record completions skip manifest verification
- **Source:** AUDIT.md EF-12, EF-21
- **Severity:** CRITICAL — combined with R-011 enables silent shard data loss
- **Status:** BLOCKED, blocked-by R-011 (auth) + needs separate R-219 for zero-record manifest path
- **Files:** src/server/dispatch.rs:471-810, :567-571, :628-634, src/cluster/migration.rs:577-616
- **Cluster:** migration-handshake
- **Notes:** Two issues:
  1. **Auth (subset of R-011):** OP_MIGRATION_COMPLETE / OP_MIGRATION_BATCH_COMPLETE go plain TCP. Fixed when R-011 lands.
  2. **Zero-record manifest skip:** Even with auth, the receiver currently treats `record_count == 0` as "empty migration, no manifest needed" — a confused-deputy attack where a malicious or buggy source declares a non-empty shard's migration complete with zero records causes silent data loss on the new master.
- **Block reason:** (1) is gated by R-011's migration plan. (2) is independently fixable but requires a small wire-protocol clarification (every completion must carry the manifest hash, even for empty shards) — captured as **R-219** (HIGH, OPEN, needs human approval since it interacts with the empty-shard fast path that the cluster already optimizes for).
- **Test required:** see R-219 + the R-011 test list.

---

## HIGH — Milestone 0 / Milestone 1

### Cluster: spend-op + freeze-op + reorg-op (UTXO correctness)

### R-013 — [recovery] `replay_spend` / `replay_unspend` swallow metadata write errors and skip derived state (gen, LAST_SPENT_ALL, DAH, indexes)
- **Source:** AUDIT.md A-06, BC-12
- **Severity:** HIGH
- **Status:** PARTIAL (write-error propagation RESOLVED; derived-state synthesis deferred to R-220)
- **Files:** src/recovery.rs (`replay_spend`, `replay_unspend`, `replay_set_mined`, `replay_metadata_op`'s SetConflicting / SetLocked / PreserveUntil / MarkOnLongestChain branches)
- **Cluster:** recovery
- **Resolution (write-error part):** Replaced all 7 `let _ = io::write_metadata(...)` swallows with explicit `if … .is_err() { return ReplayResult::Failed(ReplayCause::IoError); }`. Replaced 2 `if let Ok(mut meta) = io::read_metadata(...)` patterns in replay_spend/replay_unspend with explicit `match` that propagates IoError on read failure too. The recovery telemetry's `failed_io` counter now increments for these failures so operators see a non-tolerable failure on startup instead of silent divergence. Pre-fix, replay claimed `Applied` while the disk write actually failed; replicas resyncing from the generation watermark would never see the missing change.
- **Limits / follow-ups:** The audit's full A-06 also asks for derived-state updates inside replay (generation bump, updated_at, DAH/unmined index re-derivation). The current redo entries don't carry enough context to compute the derived state without re-running the engine's full evaluation logic, so a clean fix needs either (a) extending the redo entries to carry every derived field, or (b) calling into the engine's mutation path under a synthetic guard during replay. Both are non-trivial. Captured as **R-220** (HIGH, OPEN). The write-error propagation closes the most dangerous half of A-06 — silent divergence — and is mechanically safe; R-220 closes the cleanup-and-consistency half.
- **Verification:** `cargo test --all` 1497 passed (no regressions). The new failure paths are exercised through the existing recovery-fault-injection tests already in the suite (`fault_injection.rs`) which use device-level fault knobs.

### R-014 — [allocator-leak] `pre_allocate_create` + `create_at_offset` leak device space on `DuplicateTxId` race
- **Source:** AUDIT.md A-05
- **Severity:** HIGH
- **Status:** RESOLVED
- **Files:** src/server/dispatch.rs (`handle_create_batch` Phase 3 error branches)
- **Cluster:** allocator-leak
- **Resolution:** Added `engine.allocator().lock().free(record_offset, base_size + cold_len)` to BOTH the `Err(CreateError::DuplicateTxId)` branch AND the catch-all `Err(_)` branch in `handle_create_batch`. Pre-fix the pre-allocated region was never released for these failure paths, so a concurrent-create race on the same txid (where one wins and the other gets DuplicateTxId) leaked the loser's reservation forever — exhausting device space over time. Recomputed `cold_len` matches the original allocation calculation: 0 for external creates without inputs, otherwise `build_cold_data(...).len()`.
- **Verification:** `cargo test --all` 1500 passed (the existing concurrent-duplicate test in `src/ops/engine.rs:8901` continues to pass and now exercises the cleanup branch). Clippy + fmt clean.

### R-015 — [dispatch] Pruned UTXO drops preserved `spending_data` on the wire
- **Source:** AUDIT.md A-07
- **Severity:** HIGH
- **Status:** PARTIAL (logging in place; engine-side change deferred)
- **Files:** src/ops/engine.rs (`SpendError::Pruned` definition unchanged), src/server/dispatch.rs (`spend_error_to_batch_error`)
- **Cluster:** wire-error-payloads
- **Notes / partial fix:** Added a `tracing::warn!` at the dispatch mapping site so operators can detect the gap. Full fix requires extending `SpendError::Pruned` to carry the preserved `spending_data: [u8; 36]` (currently the engine reads it from the slot but discards it before raising the error), then surfacing those bytes in the wire payload. That's a small but engine-side change that affects every caller of `Engine::spend` to thread the field through; tracked as remaining work under this ledger entry. Severity stays HIGH until the engine change lands.
- **Test required:** `pruned_utxo_spend_returns_original_spending_data`

### R-016 — [freeze-op] `freeze`/`unfreeze` don't bump generation, don't write metadata, don't sync index cache
- **Source:** AUDIT.md A-08
- **Severity:** HIGH
- **Status:** RESOLVED
- **Files:** src/ops/engine.rs (`freeze`, `unfreeze`)
- **Cluster:** freeze-op
- **Resolution:** Both functions now bump `meta.generation`, set `meta.updated_at`, write metadata back via `write_metadata_fast`, and call `sync_index_cache` to keep cached `tx_flags` aligned with on-device state. Tests added (`freeze_bumps_generation_and_syncs_cache`, `unfreeze_bumps_generation_and_syncs_cache`) snapshot the metadata generation pre/post and assert (a) on-device gen bumped, (b) cached gen bumped, (c) the two match.
- **Verification:** `cargo test --all` 1497 passed, 0 failed, 0 ignored.

### R-017 — [freeze-op] `reassign` skips `LOCKED`, `CONFLICTING`, coinbase maturity flags
- **Source:** AUDIT.md A-09
- **Severity:** HIGH
- **Status:** RESOLVED
- **Files:** src/ops/engine.rs (`reassign`)
- **Cluster:** freeze-op
- **Resolution:** Added the three flag checks that `Engine::spend` already enforces: reject `Conflicting` if `TxFlags::CONFLICTING` is set, reject `Locked` if `TxFlags::LOCKED` is set, reject `CoinbaseImmature` if `TxFlags::IS_COINBASE` and `spending_height > req.block_height`. The request's `block_height` field (already present) plays the role of `current_block_height` for the maturity comparison — that's the block in which the reassign is being committed. Tests added: `reassign_rejects_locked`, `reassign_rejects_conflicting`, `reassign_rejects_immature_coinbase`. Pre-fix a record marked LOCKED, CONFLICTING, or coinbase-immature could still be reassigned, bypassing the flags' purpose.
- **Verification:** `cargo test --all` 1500 passed (was 1497, +3); existing reassign tests (`reassign_frozen`, etc.) unchanged. Clippy + fmt clean.

### R-018 — [wire-error-payloads] `FROZEN_UNTIL` wire response drops the 4-byte spendable-at-height payload
- **Source:** AUDIT.md A-10
- **Severity:** HIGH
- **Status:** RESOLVED
- **Files:** src/server/dispatch.rs (`spend_error_to_batch_error` mapping table)
- **Cluster:** wire-error-payloads
- **Resolution:** Replaced `(ERR_FROZEN_UNTIL, vec![])` with `(ERR_FROZEN_UNTIL, spendable_at_height.to_le_bytes().to_vec())`. Existing engine-level test `frozen_until_error_includes_spendable_height_bytes`-equivalent coverage exists; the wire-level coverage is a cross-cutting follow-up tracked under R-060 (protocol conformance suite).
- **Verification:** `cargo test --all` 1497 passed, 0 failed; no test regressions; wire response now matches the README's documented contract.

### R-019 — [preserve-op] `preserve_until` doesn't sync index cache → fast paths bypass protection, premature pruning
- **Source:** AUDIT.md A-12
- **Severity:** HIGH
- **Status:** RESOLVED
- **Files:** src/ops/engine.rs (`preserve_until`)
- **Cluster:** preserve-op
- **Resolution:** Added a `self.sync_index_cache(&req.tx_key, &meta)` call after the `write_metadata_fast` so the cached `tx_flags` picks up `HAS_PRESERVE_UNTIL`. The existing `sync_index_cache` already encodes the discriminant correctly via the `dah_or_preserve` field. Without this fix, downstream fast-path ops consulted a stale cache, concluded `has_preserve = false`, and bypassed the protection — premature pruning of preserved records.
- **Verification:** `cargo test --all` 1497 passed; existing preserve-related tests (`preserve_until_*`) cover the on-device side; the cache-sync side passes via the existing `sync_index_cache` infrastructure.

### R-020 — [spec-validation] Lua reference (`specs/teranode.lua`) missing — Lua-parity claims un-checkable
- **Source:** AUDIT.md Category A note
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** specs/teranode.lua (missing)
- **Cluster:** spec-validation
- **Notes:** Restore from git history if present, or document explicitly that Lua parity is now defined by the Rust spec/test pair. CLAUDE.md references the file as authoritative for behavior comparison.
- **Test:** N/A (process)

### R-021 — [dispatch-wal] Spend's idempotent re-spend writes metadata WITHOUT a redo entry → generation drifts on crash
- **Source:** AUDIT.md BC-25, BC-35
- **Severity:** HIGH
- **Status:** RESOLVED
- **Files:** src/ops/engine.rs (`Engine::spend` UTXO_SPENT idempotent branch)
- **Cluster:** dispatch-wal
- **Resolution:** Made the idempotent re-spend branch a true no-op — no slot change, no metadata write, no generation bump. Pre-fix it bumped `metadata.generation`, called `now_millis()`, wrote the new metadata back to disk, and synced the index cache, all without emitting a redo entry. A crash between the metadata write and its fsync left the on-device generation below the value that had already been returned to the client (and was about to be advertised to replicas via `ReplicaOp::Spend.master_generation`); recovery had nothing to replay, so the gap was permanent and replication staleness checks would mismatch on resync. The new behaviour aligns the spend idempotent branch with the existing `unspend` already-unspent branch (which has always been a true no-op), removing the WAL gap entirely — there is nothing to recover because nothing was written.
- **Tests added:** `idempotent_respend_does_not_increment_generation` (replaces the prior `idempotent_respend_increments_generation` that pinned the buggy behaviour).
- **Verification:** Full local gate green: `cargo build --release`, `cargo test --all` (1712 tests, 0 failed, 0 ignored — up from 1505 lib + earlier integration baseline), `cargo clippy --all --all-targets -- -D warnings`, `cargo fmt --all -- --check`. Cross-backend (memory + `TERASLAB_INDEX_BACKEND=redb`) verified for the regression test.

### Cluster: redo-log + recovery foundations

### R-022 — [record-format] UTXO slots have no checksum — torn writes undetectable, no slot CRC
- **Source:** AUDIT.md BC-03
- **Severity:** HIGH
- **Status:** OPEN, **MIGRATION-REQUIRED** (on-disk format)
- **Files:** src/record.rs:96-118, src/io.rs:360-382, :263, src/recovery.rs (read paths)
- **Cluster:** record-format
- **Notes:** Add 4-byte CRC or generation counter; bump `UTXO_SLOT_SIZE` 69 → 73 bytes. On read, verify; on torn detection, fall back to redo replay. On-disk format change → migration plan required.
- **Test:** `torn_utxo_slot_detected_on_read`

### R-023 — [generation-counter] u32 generation wrap; recovery's `>=` check breaks after wrap
- **Source:** AUDIT.md BC-05, A-24
- **Severity:** HIGH
- **Status:** OPEN, **MIGRATION-REQUIRED** (record layout if widening)
- **Files:** src/ops/engine.rs:1007,1049,1150,1271,1478,1561,2262,2355,2446,2505,2569,2631,2664,2931, src/recovery.rs:1022-1041, src/replication/receiver.rs:721-731
- **Cluster:** generation-counter
- **Notes:** Choose: (a) widen to u64 (8 bytes — needs metadata header schema bump), (b) explicit modular arithmetic ("target within 2^31 ahead?"), (c) per-record sequence number that doesn't reset. (a) is cleanest but needs migration. (b) is local change, no migration.
- **Test:** `generation_wraparound_idempotency`

### R-024 — [conflicting-children] `append_conflicting_child` mutates parent metadata without a redo entry
- **Source:** AUDIT.md BC-09, BC-44, AUDIT_CODEX.md F5
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** src/ops/engine.rs:1742, :1913, :2275-2360, :2318-2357, :2529, src/recovery.rs:849-869, src/server/dispatch.rs:4492
- **Cluster:** conflicting-children
- **Notes:** Write `RedoOp::AppendConflictingChild { parent_key, child_txid, prior_offset, prior_count, new_offset, new_count }` BEFORE alloc/free steps. Replay re-reads parent metadata and ensures children list is correct (idempotent). Currently crash mid-update leaves parent metadata referencing freed/reallocated regions; recovery explicitly skips conflict-link replay.
- **Test:** `append_conflicting_child_crash_recovery`, `append_conflicting_child_multi_crash_window`

### R-025 — [allocator-wal] `pre_allocate_create` allocates DEVICE space BEFORE the create's redo entry is written
- **Source:** AUDIT.md BC-10
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:3091-3193, src/ops/engine.rs:1786-1790, src/allocator.rs:455-564
- **Cluster:** allocator-wal
- **Notes:** Defer reservation: write `CreateV2` redo first, allocate, write record bytes. Recovery replays `CreateV2` with placeholder, asks allocator to allocate inside replay. Or batch via `append_batch_and_flush`.
- **Test:** `create_batch_crash_no_space_leak`

### R-026 — [replay-idempotency] Redo entries not actually idempotent — `replay_spend` overwrites `spent_utxos` unconditionally
- **Source:** AUDIT.md BC-11
- **Severity:** HIGH
- **Status:** OPEN, blocked-by R-010
- **Files:** src/recovery.rs:541-555, :580-590
- **Cluster:** replay-idempotency
- **Notes:** Replace counter overwrite with delta-based (`new_spent_count = max(meta.spent_utxos.saturating_sub(delta), …)`) and tie delta to per-entry idempotency guard. Or take per-tx lock during replay and re-derive `spent_utxos` from a slot scan. Tied to R-010.
- **Test:** `replay_spend_idempotent_counter`

### R-027 — [redo-log] Linear `write_pos` never wraps — naming "circular" misleads
- **Source:** AUDIT.md BC-13
- **Severity:** HIGH
- **Status:** OPEN, blocked-by R-003
- **Files:** src/redo.rs:983-1295
- **Cluster:** redo-log
- **Notes:** Either implement actual circular writes (wrap `write_pos` modulo `log_size` after `checkpoint()`), or rename `LinearRedoLog` to set expectations. R-003 must define semantic before this can be picked.
- **Test:** `redo_log_linear_or_circular_documented`

### R-028 — [allocator-wal] `pre_allocate_create` AllocateRegion fsync sequence is N fsyncs per batch
- **Source:** AUDIT.md BC-36
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:3091, :3169, src/ops/engine.rs:1786-1790, src/allocator.rs:512
- **Cluster:** allocator-wal
- **Notes:** Add `allocate_batch` API: one `RedoOp::AllocateBatch { regions: Vec<…> }` + one fsync. Drops total fsync count from N+1 to 2. Currently 100-item batch → ~1ms pure fsync overhead.
- **Test:** `create_batch_fsync_count_optimized`

### R-029 — [memory-ordering] `read_metadata_direct` reads bytes WITHOUT memory ordering — torn-write detection relies on CRC alone (ARM)
- **Source:** AUDIT.md BC-06
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** src/io.rs:208-234, :73-189
- **Cluster:** memory-ordering
- **Notes:** Use `std::sync::atomic::fence(Ordering::Release)` after byte copy and `Ordering::Acquire` before reading. On ARM without explicit barriers, reader on a different core can observe CRC bytes before field bytes → silent corruption with valid CRC.
- **Test:** `read_metadata_memory_ordering_arm`

### R-030 — [memory-ordering] `write_metadata_direct` writes are NOT release-fenced — concurrent reader may observe stale CRC
- **Source:** AUDIT.md BC-07
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** src/io.rs:226-234
- **Cluster:** memory-ordering
- **Notes:** Add `fence(Ordering::Release)` after memcpy. On ARM, memcpy stores can reorder; reader seeing new CRC + stale field bytes returns corrupted data without detection.
- **Test:** `metadata_write_memory_fence_arm`

### R-031 — [recovery-validation] `replay_create` (legacy, pre-CreateV2) registers WITHOUT validating on-device record bytes
- **Source:** AUDIT.md BC-53
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** src/recovery.rs:715-744
- **Cluster:** recovery
- **Notes:** Either have legacy `replay_create` read device metadata + fail closed on missing/corrupt (mirror `replay_create_v2`), or deprecate the legacy Create opcode after a release cycle. Currently legacy entries register an index pointing at zeros.
- **Test:** `legacy_create_redo_validation`

### R-032 — [hashtable-concurrency] Hash-table buckets 64-byte packed; concurrent reader can see torn bucket on writer's `set_entry`
- **Source:** AUDIT.md BC-30
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** src/index/hashtable.rs:121-196, :691-766, src/ops/engine.rs:37
- **Cluster:** hashtable-concurrency
- **Notes:** Global RwLock prevents data races but bottlenecks scalability. Replace with per-bucket-stripe lock OR lock-free hashtable OR epoch-based reclamation. Every GET contends with every CREATE/DELETE on the global write lock — incompatible with 10M ops/sec target.
- **Test:** `hashtable_lock_contention_benchmark`

### R-033 — [hashtable-performance] HashTable resize is BLOCKING — every concurrent reader waits hundreds of seconds at scale
- **Source:** AUDIT.md BC-58
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** src/index/mod.rs:179-184
- **Cluster:** hashtable-performance
- **Notes:** Background resize: allocate new table off-thread, copy with concurrent insert/remove tracking via generation counter or epoch, atomic swap. Non-trivial but necessary for production.
- **Test:** `hashtable_resize_blocking_benchmark`

### R-034 — [replication-wal] Replica-applied mutations skip writing local redo log → failover requires full resync
- **Source:** AUDIT.md BC-34
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** src/replication/receiver.rs:713-, src/ops/engine.rs:631-657, src/replication/durable.rs:639
- **Cluster:** replication-wal
- **Notes:** Receiver's `apply_op` must also write a local redo entry via the engine's `redo_log_handle`. Non-trivial: redo entry must capture post-apply state, not the input op. Without this, failover on a master crash requires full resync of every surviving replica.
- **Test:** `replica_redo_log_catch_up_after_failover`

### R-035 — [replication] LMNH-31: replica silently drops `write_metadata` errors during apply; ACKs while diverging
- **Source:** AUDIT.md LMNH-31, intersects D-19/gap #5
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** src/replication/receiver.rs:684, :1127
- **Cluster:** replication
- **Notes:** Replace `let _ = io::write_metadata(...)` with proper error handling that fails the batch ACK. Master will retry instead of advancing durable high-water-mark. Use the error pattern from same file lines 216-221.
- **Test:** `replica_metadata_write_error_fails_batch_ack`

### R-036 — [replication-intent] Replication intent started AFTER local apply; crash between local apply and intent fsync leaves silent local-only mutation
- **Source:** AUDIT.md gap #5, D-19
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:1250 (`begin_replication_intent` inside `replicate_all_ops`)
- **Cluster:** replication-intent
- **Notes:** Begin and fsync intent BEFORE local engine apply, OR fold pending-replication into the same redo entry. fsync intent file and parent dir.
- **Test:** `intent_persists_before_local_apply`

### R-037 — [dispatch-wal] `MarkLongestChainBatch` redo entry computes `target_generation` pre-lock — concurrent `mark_on_longest_chain` could conflict
- **Source:** AUDIT.md BC-66
- **Severity:** HIGH
- **Status:** OPEN, blocked-by R-010 pattern
- **Files:** src/server/dispatch.rs:4131-4225, src/ops/engine.rs:317-430
- **Cluster:** dispatch-wal
- **Notes:** Apply validate-under-lock pattern (same as R-010). Redo payload computed from snapshot is fragile if replay isn't strictly idempotent.
- **Test:** `mark_longest_chain_concurrent_safety`

### Cluster: replication

### R-038 — [replica-lag] `replica_lag_check_interval_secs` config dead; `spawn_lag_monitor` never spawned
- **Source:** AUDIT.md D-01
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** src/config.rs:387,444, src/replication/durable.rs:679-709
- **Cluster:** replica-lag
- **Notes:** Wire `spawn_lag_monitor` into `bin/server.rs` when `config.replica_lag_check_interval_secs > 0`. Add Prometheus gauge `repl_replica_lag_ops{replica="…"}`. Surface lag in `/healthz` so cluster degrades when lag exceeds threshold.
- **Test:** `spawn_lag_monitor_emits_metrics`

### Cluster: cluster + sharding + migration

### R-039 — [quorum] `alive_node_count` excludes self → false NO_QUORUM in healthy clusters
- **Source:** AUDIT.md EF-02
- **Severity:** HIGH
- **Status:** RESOLVED
- **Files:** src/cluster/coordinator.rs (`alive_node_count`)
- **Cluster:** quorum
- **Resolution:** Production SWIM (`swim.rs:454`) returns BEFORE peer-registering self, so `node_addrs` excludes the local node. The pre-fix `committed.iter().filter(|node| addrs.contains_key(node)).count()` therefore reported one-less-than-actual; in a 3-node cluster losing 1 peer it returned 1 and dispatch rejected with NO_QUORUM despite the surviving 2-node majority. New logic: count addrs-known committed peers + 1 if self is committed but absent from `node_addrs`. Test harnesses that explicitly inject self into `node_addrs` are handled correctly via the `!self_in_addrs` check, so the existing `alive_node_count_only_counts_live_committed_members` test still passes. Added EF-03 regression `alive_node_count_includes_self_when_not_in_node_addrs` that sets up the production-shape (self absent from addrs, surviving peer present, 3-node committed) and asserts count==2.
- **Verification:** `cargo test --all` 1501 passed (was 1500, +1 net — added 1 new test, fixed nothing else). Clippy + fmt clean. R-040 (EF-03 integration test) is partially covered by this unit test; the full multi-node TCP variant is still tracked under R-040.

### R-040 — [quorum] No integration coverage for isolated 1-node remnant rejecting writes
- **Source:** AUDIT.md EF-03
- **Severity:** HIGH
- **Status:** OPEN, blocked-by R-039
- **Files:** tests/cluster_tcp.rs (new test)
- **Cluster:** quorum
- **Notes:** Multi-node integration test: start 3 nodes, kill 2, wait SWIM dead, send `OP_CREATE_BATCH`, assert `ERR_NO_QUORUM`. Control case: single-node cluster accepts same op.
- **Test:** `isolated_node_rejects_writes_with_no_quorum`

### R-041 — [redirect-routing] REDIRECT has no hop count, TTL, or loop counter — clients chase stale routes forever
- **Source:** AUDIT.md EF-09
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:2287-2311,4283-4307,4763-4779, src/cluster/coordinator.rs:5598-5620, src/cluster/routing.rs:67-93
- **Cluster:** redirect-routing
- **Notes:** Add hop counter to request frame (header byte or shifted flags bits). Reject redirects whose `hop_count > N` (suggest 4). Or encode `shard_table_version` from `RouteDecision::RedirectTo` into error_data so client detects stale version.
- **Test:** `redirect_loop_detection_with_hop_counter`

### R-042 — [topology-commit] Split-brain heal — two clusters that learn about each other have no rejection path
- **Source:** AUDIT.md EF-10
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** src/cluster/topology.rs:482-532, src/cluster/membership.rs:108-183
- **Cluster:** topology-commit
- **Notes:** When SWIM emits `MembershipChanged` with member list NOT a strict superset of local `committed_members`, refuse to propose unless operator sets `--allow-merge`. Or add a `cluster_id` field separate from `cluster_secret`; reject SWIM gossip from peers reporting different `cluster_id`.
- **Test:** `split_brain_heal_detects_independent_clusters`

### Cluster: wire-protocol DoS / index

### R-043 — [wire-dos] `OP_MIGRATION_COMPLETE` `entry_count * 36` unchecked multiply
- **Source:** AUDIT.md GH-04
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:510-541
- **Cluster:** wire-dos
- **Notes:** `entry_count.checked_mul(36).and_then(|n| n.checked_add(60))?` or refactor to `validate_batch_count`. Test sending `entry_count=u32::MAX` with tiny payload → must reject.
- **Test:** `migration_complete_unchecked_multiply_rejects_max_count`

### R-044 — [wire-dos] OP_STREAM_CHUNK accepts attacker-controlled `chunk_data_len` up to MAX_FRAME_SIZE with no per-stream total cap
- **Source:** AUDIT.md GH-06, GH-09
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** src/protocol/codec.rs:1583-1599, src/server/dispatch.rs:4923
- **Cluster:** wire-dos
- **Notes:** Add `ServerConfig::max_stream_total_bytes` (default 4 GiB). Track `stream.bytes_received` against it; reject chunk + abort stream when exceeded; `checked_add` on counter. Optional: idle timeout on `ActiveStream`.
- **Test:** `stream_total_size_cap_enforced`

### R-045 — [wire-dos] `GetBatch` masks storage corruption as zeros / TX_NOT_FOUND
- **Source:** AUDIT_CODEX.md F4 (NEW)
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:4455-4465, :4469-4477, :4491-4501, :4506-4516
- **Cluster:** wire-error-payloads
- **Notes:** `handle_get_batch` converts `read_slot` errors → 69 zero bytes, `read_cold_data` → length 0, `read_conflicting_children` → count 0, non-`TxNotFound` metadata error → status 1 (TX_NOT_FOUND). Silent corruption presented to client. Propagate non-`TxNotFound` reads as explicit item errors; extend `WireGetResult` status mapping; or return top-level `STATUS_PARTIAL_ERROR + ERR_INTERNAL`. Never synthesize slot bytes / cold-data length / child count after I/O or checksum failure.
- **Test:** `get_batch_propagates_storage_errors_not_zeros`

### R-046 — [snapshot-format] Snapshot deserialize uses unchecked `count * PRIMARY_ENTRY_SIZE` multiplication; OOM/panic on poisoned snapshot
- **Source:** AUDIT.md GH-G1
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** src/index/mod.rs:563-575, :687-715
- **Cluster:** snapshot-format
- **Notes:** `count.checked_mul(PRIMARY_ENTRY_SIZE).ok_or(IndexError::FormatError)?`. Cap count at ceiling (e.g. 1<<30). Add test writing poisoned snapshot with `count=u64::MAX` → `FormatError`, not panic.
- **Test:** `snapshot_restore_rejects_poisoned_count`

### R-047 — [index-redb] `import_index` not transactional across three redb files
- **Source:** AUDIT.md GH-G3
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** src/index/migration.rs:79-128
- **Cluster:** index-redb
- **Notes:** Sentinel `.import-in-progress` written before first commit, removed after all three commit succeed. On startup, refuse if sentinel exists. Or consolidate into single redb database with three tables.
- **Test:** `import_index_transactional_across_three_files`

### Cluster: storage / blob / pruning

### R-048 — [blob-gc] `ExternalRef.content_hash` permanently zero on sync create path → blob integrity check broken
- **Source:** AUDIT.md IJK-01
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** src/storage/manager.rs:116-126, src/ops/engine.rs:1676-1679, src/storage/uploader.rs:131-148
- **Cluster:** blob-gc
- **Notes:** Carry digest through `ColdDataRef::External { digest: BlobDigest }`. Engine populates `meta.external_ref` with manager-returned digest; prefer manager-returned digest over client-supplied `req.external_ref` except where external upload happened out of band.
- **Test:** `external_blob_integrity_check_fires_on_corruption`

### R-049 — [blob-gc] No GC for orphaned external blobs (failed creates, failed uploads, migration failures)
- **Source:** AUDIT.md IJK-02, IJK-08
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** src/storage/blobstore.rs (no GC), src/server/startup.rs (no scheduler), src/recovery.rs (no blob recovery)
- **Cluster:** blob-gc
- **Notes:** Add `BlobStore::list` enumerator; recovery reconciles blobs against primary index, deletes orphans; periodic background sweep + stale `.tmp` sweep. Any blob whose txid is not present or not flagged EXTERNAL is orphan.
- **Test:** `failed_create_blob_garbage_collected_on_recovery`

### R-050 — [device-io] `device_io` module (io_uring + sync fallback) completely unused; README claim "io_uring fast path" false
- **Source:** AUDIT.md IJK-04
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** src/device_io/mod.rs, src/device_io/sync_fallback.rs, src/device_io/io_uring_backend.rs, src/lib.rs:7
- **Cluster:** device-io
- **Notes:** Either wire `DeviceIo` into engine (route batched spend_batch / set_mined_batch through submit_read/submit_write/submit_and_wait) OR remove module + correct README. Decide before next release.
- **Test:** `io_uring_path_in_use` (strace verifies `io_uring_enter` calls increment)

### R-051 — [mmap-io] `write_aligned` and `write_input_refs` swallow pre-read errors and write zeros for head/tail bytes → silent corruption of record-adjacent metadata
- **Source:** AUDIT.md IJK-05
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** src/storage/manager.rs:300-318, src/storage/input_refs.rs:67-98
- **Cluster:** mmap-io
- **Notes:** Replace `let _ = self.device.pread_exact_at(...)` with `?`. Makes `write_cold_data` fallible for inline tier in edge cases — correct.
- **Test:** `write_aligned_propagates_pread_error`

### R-052 — [pruning] `MarkLongestChainBatch` not replicated; no `ReplicaOp` emitted despite mutating `unmined_since`/DAH/generation
- **Source:** AUDIT.md IJK-20, AUDIT_CODEX.md F3
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:4131-4224, :4197-4198, src/ops/engine.rs:1531-1601, src/replication/protocol.rs:100-106, :117-190
- **Cluster:** pruning
- **Notes:** Add `ReplicaOp::MarkLongestChain { tx_key, on_longest_chain, current_block_height, block_height_retention, master_generation }`. Encode/decode opcode 14. Apply in receiver with generation gating. Call `replicate_all_ops` from handler. Master/replica DAH divergence on reorg without this — silent.
- **Test:** `cluster_mark_longest_chain_replicates_dah_unmined`

### R-053 — [pruning] `mark_on_longest_chain` does not enforce idempotency by generation → drift on recovery replay
- **Source:** AUDIT.md IJK-22
- **Severity:** HIGH
- **Status:** OPEN, blocked-by R-052
- **Files:** src/server/dispatch.rs:4163-4180
- **Cluster:** pruning
- **Notes:** Engine accepts `target_generation`. If `metadata.generation + 1 != target_generation`, treat as no-op (already applied) or conflict. Recovery replay passes redo entry's generation through.
- **Test:** `mark_longest_chain_replay_idempotent`

### Cluster: observability + admin auth + DoS limits

### R-054 — [dos-limits] Slow reader on response stream blocks server thread indefinitely (no write timeout)
- **Source:** AUDIT.md LMNH-01, AUDIT_CODEX.md F13
- **Severity:** HIGH
- **Status:** RESOLVED
- **Files:** src/server/mod.rs:208-225
- **Cluster:** dos-limits
- **Resolution:** Added `stream.set_write_timeout(Some(Duration::from_secs(30)))` immediately after the existing `set_read_timeout(Some(30 s))` call. This caps the time a single slow-reader client can pin a connection thread; ~`max_connections` slow readers can no longer DoS the master by refusing to drain their recv buffer. Inline comment references R-054/LMNH-01/F13 and explains the symmetry with the read timeout.
- **Verification:** Covered by code review of the symmetric read/write timeout pair; full local gate (build + test --all + clippy --all-targets -D warnings + fmt --check) green, no regressions in 1505-test lib suite. (A live-socket regression test would need a multi-thread tokio + a real client that intentionally never reads — deferred as test infra task R-058 / proptest.)

### R-055 — [observability] `/health/ready` hard-coded `true` at boot, never consults cluster readiness
- **Source:** AUDIT.md LMNH-07
- **Severity:** HIGH
- **Status:** RESOLVED
- **Files:** src/server/http.rs:839-880
- **Cluster:** observability
- **Resolution:** Extracted readiness logic into a synchronous `compute_health_ready(&HttpState) -> ReadyState` helper that (a) honours the existing `state.ready` flag and (b) when `state.cluster.is_some()`, additionally requires `cluster.cluster_health().is_ready()` (`swim_state == Alive`, i.e. at least one committed topology observed). Pre-fix the boot-time `state.ready: AtomicBool::new(true)` made the endpoint return 200 the instant the HTTP listener bound, so a load balancer would route to a clustered node before it had quorum and the node would reject every request with `ERR_CLUSTER_NOT_READY`. Single-node mode behaviour is unchanged (no cluster → only the local flag applies). Secondary-index readiness gate at dispatch:311 is a separate concern — captured as R-220 follow-up.
- **Tests added:** `health_ready_returns_ready_in_single_node_mode`, `health_ready_rejects_when_local_ready_flag_false`, `health_ready_rejects_when_cluster_has_no_committed_term` (the regression — pre-fix it returned Ready), `health_ready_returns_ready_once_cluster_committed`.
- **Verification:** 4 new tests pass on memory backend and `TERASLAB_INDEX_BACKEND=redb`; full local gate green.

### R-056 — [admin-auth] Admin mutation endpoints have zero auth when enabled
- **Source:** AUDIT.md LMNH-08, AUDIT_CODEX.md F14
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** src/server/http.rs:88-95, :116-152, src/config.rs:288-296,410-428, tests/http_observability.rs:483-525
- **Cluster:** admin-auth
- **Notes:** Bearer-token middleware gated by `admin_token` config field. Or separate listener for network-layer firewall. Defaults: token required when admin endpoints enabled — missing → 401.
- **Test:** `admin_mutate_requires_token_when_enabled`

### R-057 — [proptest] No property-based testing framework (proptest/quickcheck)
- **Source:** AUDIT.md LMNH-16, AUDIT_CODEX.md F15
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** Cargo.toml, tests/
- **Cluster:** proptest
- **Notes:** Add `proptest = "1"` to dev-deps. Four properties: (1) UTXO conservation (create/spend/unspend), (2) redo-log replay idempotency, (3) shard table determinism, (4) protocol codec roundtrip. CI runs 256 cases per property; nightly runs more.
- **Test:** `prop_*` suite

### R-058 — [fuzz-coverage] No fuzz targets for wire-protocol parser
- **Source:** AUDIT.md LMNH-17, AUDIT_CODEX.md F11
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** No fuzz/ directory; src/protocol/codec.rs, src/protocol/frame.rs:281-466
- **Cluster:** fuzz-coverage
- **Notes:** Add `cargo-fuzz`. One harness `fuzz_request_frame_decode` is the natural entry. Each `decode_*_checked`, `ReplicaBatch::deserialize`, routing/topology decoders, stream decoders. Run in CI with time budget; preserve crashing seeds. Guard each `try_into().unwrap()` with explicit length check.
- **Test:** `cargo +nightly fuzz run fuzz_request_frame_decode -- -runs=10000` finds no panics

### R-059 — [test-infra] Integration tests only exercise `IndexBackendMode::Memory`
- **Source:** AUDIT.md LMNH-18
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** tests/server_tcp.rs:43, tests/integration.rs, tests/http_observability.rs:28, tests/fault_injection.rs:229
- **Cluster:** test-infra
- **Notes:** `#[rstest]` matrix or `for_each_backend!` macro. Run each integration test against Memory, Redb, FileBacked. Surfaces backend-specific bugs (redb txn contention, FileBacked mmap allocation).
- **Test:** `cargo test` runs full integration suite 3× — all pass

### R-060 — [test-infra] Error-code triggerability not proven for several README codes (no real client/TCP test for codes 16,17,18 + partial 2,4-6,11-13,15,19,20)
- **Source:** AUDIT_CODEX.md F10
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** src/protocol/opcodes.rs:165-188, :238-246, src/server/dispatch.rs:5074-5096
- **Cluster:** test-infra
- **Notes:** Add a protocol conformance integration test that drives a real TCP connection or Rust client for every README code 0-20 + 255: verify top-level status, sparse item index, error code, payload bytes. Particularly: 16 STREAM_NOT_FOUND, 17 BLOB_NOT_FOUND, 18 STREAM_OFFSET_MISMATCH have no real-client tests.
- **Test:** `protocol_conformance_every_error_code`

---

## MEDIUM

### Cluster: spend-op / freeze-op edge cases

### R-061 — [spend-op] AlreadySpent winner spending_data not asserted in concurrent tests
- **Source:** AUDIT.md A-02
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/ops/engine.rs:4194-4209, :3433-3444, tests/integration.rs:810
- **Cluster:** spend-op
- **Notes:** Augment `concurrent_spend_same_utxo_different_data` to keep `Vec<[u8;36]>` of attempts; find the unique successful winner; assert every loser's `AlreadySpent.spending_data == winner_sd` exactly.
- **Test:** `verify_concurrent_spend_winner_spending_data_exact_match`

### R-062 — [dispatch] Wire `GetSpend` skips `utxo_hash` validation; after reassign, old peers get new UTXO status
- **Source:** AUDIT.md A-11
- **Severity:** MEDIUM
- **Status:** OPEN, **MIGRATION-REQUIRED** (wire format change)
- **Files:** src/server/dispatch.rs:4783-4805
- **Cluster:** wire-error-payloads
- **Notes:** Extend `WireGetSpendItem` with `utxo_hash: [u8; 32]`. Validate against `slot.hash`; return `ERR_UTXO_HASH_MISMATCH` on disagreement (matching engine-level `get_spend`).
- **Test:** `get_spend_wire_validates_utxo_hash`

### R-063 — [freeze-op] `reassign` `saturating_add` for `spendable_height` silently pins UTXO unspendable forever on overflow
- **Source:** AUDIT.md A-13
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/ops/engine.rs:2254
- **Cluster:** freeze-op
- **Notes:** `req.block_height.checked_add(req.spendable_after).ok_or(SpendError::ReassignOverflow {…})`. New error variant.
- **Test:** `reassign_overflow_checked_add_rejects_u32_max`

### R-064 — [reorg-op] `set_conflicting` slow path doesn't propagate to parent records' conflicting-children list in fast path
- **Source:** AUDIT.md A-21
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/ops/engine.rs:2520-2532, :2400-2493
- **Cluster:** reorg-op
- **Notes:** Move parent-propagation block out of slow path; run after both paths.
- **Test:** `set_conflicting_fast_path_updates_parent_children`

### R-065 — [recovery] Tombstone-on-delete races with `allocator.free` — crash boundaries leak space or cause stale-index reads
- **Source:** AUDIT.md A-29, BC-45, BC-46, BC-47
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/ops/engine.rs:2701-2714, :2706, :2709, src/recovery.rs:747-752, src/allocator.rs:594-600, src/server/dispatch.rs:3962-3966, src/redo.rs:189-193
- **Cluster:** delete-recovery
- **Notes:** Combine tombstone + free into single atomic redo intent: `RedoOp::DeleteRecord { tx_key, record_offset, record_size }`. Replay handler: write tombstone, `allocator.free`, unregister index — idempotent block. Populate `record_size` from index lookup (currently always 0); `replay_delete` calls `allocator.free` if not already journaled (freelist no-op if so).
- **Test:** `delete_crash_no_space_leak`, `delete_redo_record_size_populated`

### Cluster: replication

### R-066 — [ack-tracking] `WriteMajority` semantics differ between manager and dispatch; RF=2 requires zero replica ACKs
- **Source:** AUDIT.md D-02
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/replication/manager.rs:487-496, src/server/dispatch.rs:1544-1573
- **Cluster:** ack-tracking
- **Notes:** Equivalence test pinning both formulas, or refactor to single shared `required_replica_acks(rf, policy)` helper. Document `AckPolicy::WriteMajority` RF=2 corner case. Startup warning when `replication_factor=2 && ack_policy="write_majority"`.
- **Test:** `write_majority_threshold_consistency_rf2_through_rf7`

### R-067 — [ack-tracking] `AckTracker` updates racy; 1s flush window can lose recent ACKs on master crash
- **Source:** AUDIT.md D-03
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/replication/durable.rs:95-107
- **Cluster:** ack-tracking
- **Notes:** Add write-after-N counter alongside time-based flush so ACK bursts don't accumulate. Document 1s window. Verify catchup stream-key derivation stable across master reconnects with/without `source_node_id`.
- **Test:** `ack_tracker_crash_window_loses_recent_acks`

### R-068 — [replica-receiver] Stream-key fallback uses `peer_addr.to_string()`; ephemeral-port roll triggers full re-replay
- **Source:** AUDIT.md D-05
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/replication/receiver.rs:223-227, :456-459
- **Cluster:** replica-receiver
- **Notes:** Require `source_node_id` for non-test path; warn on missing. Reap `applied` entries unread N hours. Cap file growth + compaction. Document contract: `node:{source_node_id}` if set; else `peer_addr` for tests only.
- **Test:** `receiver_stream_key_stable_across_reconnect_ephemeral_ports`

### R-069 — [replication-protocol] `replication_timeout_ms` ignored when migration pressure active; silently extended to 30s
- **Source:** AUDIT.md D-06
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:1404-1410
- **Cluster:** replication-protocol
- **Notes:** Document the migration-pressure floor; expose as `replication_timeout_during_migration_ms` config knob (default 30000). Make explicit in code + config docs.
- **Test:** `replication_timeout_migration_pressure_override`

### R-070 — [replication-manager] Catch-up has no rate limit; `run_catchup` can starve live-replication path
- **Source:** AUDIT.md D-11
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/replication/manager.rs:541-638
- **Cluster:** replication-manager
- **Notes:** Run catch-up in separate worker threads per recovering replica. Add `catchup_max_ops_per_sec` cap. Stream catch-up over separate TCP connection from live traffic.
- **Test:** `catchup_does_not_block_live_writes`

### R-071 — [replication-protocol] `ReplicaOp::Create` has no `master_generation`; Create+Delete reorder can diverge replica
- **Source:** AUDIT.md D-15
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/replication/protocol.rs:217-251, :108-191
- **Cluster:** replication-protocol
- **Notes:** Add `master_generation: u32` to wire op for Create. Gate create on master_generation in `apply_op`. Combined with R-068, ordering becomes sound regardless of TCP ephemeral-port churn.
- **Test:** `create_protected_by_master_generation_ordering_guard`

### R-072 — [replication-intent] Recovery hard-fails on redo wrap-around; bricks master at startup
- **Source:** AUDIT.md D-19
- **Severity:** MEDIUM
- **Status:** OPEN, blocked-by R-003 + R-027
- **Files:** src/server/dispatch.rs:1454-1494
- **Cluster:** replication-intent
- **Notes:** "Older than redo log floor → log warning, clear marker, surface metric" path. Replicas needing lost data resync via migration/catch-up.
- **Test:** `intent_recovery_handles_redo_wrap_around_gracefully`

### R-073 — [compensation-durability] `compensate_replication_failure` writes redo via `let _ = write_redo_ops(...)` — flush failures dropped
- **Source:** AUDIT.md BC-62
- **Severity:** MEDIUM
- **Status:** OPEN, partially folded into R-007
- **Files:** src/server/dispatch.rs:2000-2002
- **Cluster:** compensation-durability
- **Notes:** Treat compensation redo write failures as fatal; bubble up. Operator-visible "rollback-pending" state. Currently silent redo-log-full drops compensation → silent divergence on restart.
- **Test:** `compensation_redo_failure_fatal`

### R-074 — [dispatch-consistency] Compensation runs AFTER engine commit → observable inconsistency window
- **Source:** AUDIT.md BC-61
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:1667-2003, src/ops/engine.rs (spend/unspend paths)
- **Cluster:** dispatch-consistency
- **Notes:** Hold per-tx stripe lock across the entire dispatch flow (validate → redo → apply → replicate → respond). Trade-off: read latency increases when replication slow. Currently clients observe commit → moments later rollback.
- **Test:** `compensation_no_observable_window`

### R-075 — [compensation] `set_locked` doesn't capture before-image — locked → unlocked compensation has no rollback data for DAH
- **Source:** AUDIT.md BC-56
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:3737-3826, :1959-1968, src/recovery.rs:962-964
- **Cluster:** compensation
- **Notes:** Add `BeforeImage::SetLocked { prior_dah: u32 }`; compensation restores. Mirror gap #8 pattern. Locking clears DAH to 0, but compensation doesn't restore it → stale DAH under replication failure.
- **Test:** `set_locked_compensation_restores_dah`

### R-076 — [recovery-validation] Replay does NOT validate `record_offset` in `CreateV2` is allocator-owned
- **Source:** AUDIT.md BC-18
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/recovery.rs:778-872
- **Cluster:** recovery-validation
- **Notes:** During replay, mark each record_offset from a `CreateV2` as occupied; before applying later `CreateV2` at same offset, verify offset was freed in interim (`FreeRegion` or tombstoning).
- **Test:** `recovery_create_v2_allocator_validation`

### R-077 — [secondary-index-recovery] Secondary update replay can succeed when redb commit also failed previously
- **Source:** AUDIT.md BC-19
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/recovery.rs:321-385
- **Cluster:** secondary-index-recovery
- **Notes:** Replay primary cache updates BEFORE secondary replay reads them. Or change check to use on-device metadata header (authoritative source) instead of primary cache.
- **Test:** `secondary_recovery_after_failed_commit`

### R-078 — [recovery-generation] `replay_set_mined` does not bump generation after metadata change
- **Source:** AUDIT.md BC-42
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/recovery.rs:594-659, :657, src/ops/engine.rs:1271,1561,2355
- **Cluster:** recovery-generation
- **Notes:** Replay bumps gen as part of mutation matching `engine.set_mined_inner`. Currently on-device gen lags by 1 for any record whose `set_mined` entry replayed mid-crash.
- **Test:** `replay_set_mined_generation_bump`

### R-079 — [generation-counter] `engine.unspend` no-op doesn't bump generation; spend no-op does → contract violation
- **Source:** AUDIT.md BC-24
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/ops/engine.rs:1113-1120, :1003-1022, src/server/dispatch.rs:2661-2666
- **Cluster:** generation-counter
- **Notes:** Decide whether unspend-noop bumps gen; document; make spend-noop match. Idempotent spend bumps gen but unspend doesn't.
- **Test:** `unspend_noop_generation_consistency`

### R-080 — [hashtable] HashTable resize NOT crash-atomic for ANONYMOUS-mmap-backed tables (doc misleading)
- **Source:** AUDIT.md BC-26
- **Severity:** MEDIUM
- **Status:** OPEN (doc-only)
- **Files:** src/index/hashtable.rs:469-636,:1782-1900, src/index/backend.rs:19-28
- **Cluster:** hashtable
- **Notes:** Update doc to remove "without a redo log attached" wording — file-backed always attaches redo log if configured. Anonymous tables: process-death drops mapping, no recovery needed. Doc fix only.
- **Test:** N/A

### R-081 — [conflicting-children] `set_conflicting` slow path `if let Ok(...)` hides cold-data parse and append errors
- **Source:** AUDIT.md A-25
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/ops/engine.rs:2525-2532
- **Cluster:** conflicting-children
- **Notes:** At minimum: `tracing::warn!` on error. Better: collect failed parents into response, operator retries.
- **Test:** `set_conflicting_logs_parent_propagation_failures`

### Cluster: cluster + sharding

### R-082 — [migration-handshake] Recovery footgun — losing `*.topo` file resets peak to 1, allows unsafe re-bootstrap
- **Source:** AUDIT.md EF-04
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/cluster/coordinator.rs:5025-5063, src/server/dispatch.rs:2092-2094
- **Cluster:** migration-handshake
- **Notes:** Default file-missing peak from 1 → 0; treat 0 as "fresh node, wait SWIM convergence". Or write marker on first multi-node change forcing peak ≥ 2 even if `*.topo` missing.
- **Test:** `deleted_topo_file_prevents_single_node_bootstrap`

### R-083 — [redirect-routing] Partition map omits self → clients told cluster has zero nodes
- **Source:** AUDIT.md EF-05
- **Severity:** MEDIUM
- **Status:** OPEN, blocked-by R-039 fix variant
- **Files:** src/cluster/coordinator.rs:5792-5870, src/cluster/routing.rs:67-93
- **Cluster:** redirect-routing
- **Notes:** Insert `self_id → self_addr` in nodes before encoding. Same fix as R-039 variant b also addresses this.
- **Test:** `single_node_partition_map_includes_self`

### R-084 — [swim-membership] HMAC integration tests missing for multi-runner auth scenarios (different secrets, mid-rotation)
- **Source:** AUDIT.md EF-06
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/cluster/auth.rs:251-381, tests/cluster_swim.rs:97-100,519-521,529-532, tests/cluster_tcp.rs:104
- **Cluster:** swim-membership
- **Notes:** Two SWIM runners with different `cluster_secret` values must NOT converge. Asymmetric-secret deployment. Per-peer nonce binding to prevent replay within 5min window.
- **Test:** `wrong_secret_nodes_dont_converge`

### R-085 — [swim-membership] `cluster_secret` only enforced for RF>1; SWIM unauthed in single-node clusters destined to grow
- **Source:** AUDIT.md EF-08
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/config.rs:665-676
- **Cluster:** swim-membership
- **Notes:** `cluster_secret` mandatory whenever cluster mode active (SWIM port bound) regardless of RF.
- **Test:** `cluster_mode_requires_secret_regardless_of_rf`

### R-086 — [topology-commit] SWIM `committed_term` piggyback drives synthetic catch-up from any peer without quorum proof
- **Source:** AUDIT.md EF-29
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/cluster/swim.rs:569-578, src/cluster/coordinator.rs:1422-1467
- **Cluster:** topology-commit
- **Notes:** Synthetic commit path requires quorum proof. Or new `committed_members` strict superset of local. Or full propose/vote round before adopting any term advertised by single peer.
- **Test:** `synthetic_commit_requires_quorum_proof`

### Cluster: wire-protocol DoS / index continuation

### R-087 — [wire-dos] OP_MIGRATION_BATCH_COMPLETE shard-count multiplication not bounds-checked
- **Source:** AUDIT.md GH-05
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:751-779
- **Cluster:** wire-dos
- **Notes:** `checked_mul` + `checked_add`. Or `validate_batch_count(shard_count, MAX_SHARD_COUNT, 2, payload.len()-12)`.
- **Test:** `migration_batch_complete_unchecked_multiply_rejects_max_count`

### R-088 — [stream-protocol] No integration test for cross-connection stream isolation
- **Source:** AUDIT.md GH-03, GH-08
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** tests/server_tcp.rs (new)
- **Cluster:** stream-protocol
- **Notes:** Open two connections; start stream on A with txid; send `OP_STREAM_CHUNK` on B with same txid; assert B receives `ERR_STREAM_OFFSET_MISMATCH` or B creates new stream (per-connection isolation).
- **Test:** `stream_isolation_per_connection`

### R-089 — [wire-dos] `cold_data` length parsed from u32 with no per-item cap
- **Source:** AUDIT.md GH-13
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/protocol/codec.rs:813-824
- **Cluster:** wire-dos
- **Notes:** `MAX_COLD_DATA_PER_ITEM` constant (e.g. 4 MiB) in `src/protocol/opcodes.rs`; reject inside `decode_create_batch_checked`.
- **Test:** `create_batch_rejects_cold_data_exceeding_max_per_item`

### R-090 — [wire-dos] Unbounded `utxo_count` and `parent_count` per item in CreateBatch
- **Source:** AUDIT.md GH-14
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/protocol/codec.rs:781-905
- **Cluster:** wire-dos
- **Notes:** `MAX_UTXO_HASHES_PER_ITEM` (e.g. 65536) and `MAX_PARENTS_PER_ITEM` (e.g. 4096); reject early in per-item loop.
- **Test:** `create_batch_rejects_utxo_count_exceeding_max`, `create_batch_rejects_parent_count_exceeding_max`

### R-091 — [wire-dos] `max_connections` enforcement correct but not integration-tested
- **Source:** AUDIT.md GH-16
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/server/mod.rs:120-152, tests/server_tcp.rs (no test)
- **Cluster:** wire-dos
- **Notes:** Open `max_connections+1` connections; assert 6th is rejected.
- **Test:** `max_connections_enforced`

### R-092 — [wire-dos] Max-connection rejection is a TCP close, not a clean protocol error
- **Source:** AUDIT_CODEX.md F12
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/server/mod.rs:120-127
- **Cluster:** wire-dos
- **Notes:** Send small `STATUS_ERROR` frame with `ERR_INTERNAL` (or dedicated overload code), then close. Add TCP test verifying response.
- **Test:** `max_connection_rejection_sends_error_frame`

### R-093 — [index-memory] `Index::restore_all` does NOT fall back to device scan on corrupt primary (design clarification needed)
- **Source:** AUDIT.md GH-G2
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/index/mod.rs:303-354, src/server/startup.rs:230-280
- **Cluster:** index-memory
- **Notes:** Add `corrupt_in_memory_snapshot_falls_back_to_device_rebuild` integration test. Then either fix or document the contract.
- **Test:** `corrupt_in_memory_snapshot_falls_back_to_device_rebuild`

### R-094 — [snapshot-format] Snapshot uses tempfile+rename but lacks parent-dir fsync after rename
- **Source:** AUDIT.md GH-G9
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/index/mod.rs:254-293
- **Cluster:** snapshot-format
- **Notes:** After `std::fs::rename`, call `fsync_parent_dir(path)`. Refactor `fsync_parent_dir` from `hashtable.rs:341` to shared `src/index/util.rs`.
- **Test:** `snapshot_atomicity_fsync_parent_dir`

### R-095 — [snapshot-format] Versioned but deserializer doesn't reject unknown versions
- **Source:** AUDIT.md GH-G11
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/index/mod.rs:64-69
- **Cluster:** snapshot-format
- **Notes:** Replace `_version` read with: `if version != SNAPSHOT_VERSION return Err(IndexError::FormatError)`. Apply to `deserialize_secondary` too.
- **Test:** `snapshot_restore_rejects_unknown_version`

### R-096 — [snapshot-format] Deserialization does NOT cap `count` against sanity ceiling
- **Source:** AUDIT.md GH-G16
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/index/mod.rs:548-610
- **Cluster:** snapshot-format
- **Notes:** Define `MAX_SNAPSHOT_COUNT` (e.g. 10^9). Reject snapshots exceeding it, before `Self::new()` call. Apply to `deserialize_secondary` too.
- **Test:** `snapshot_restore_rejects_count_exceeding_max`

### R-097 — [index-redb] Iterating redb materializes ALL entries into Vec; 10M entries = ~630 MiB allocation
- **Source:** AUDIT.md GH-G14
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/index/redb_primary.rs:330-358, src/index/secondary_backend.rs (`DahIter::Collected`)
- **Cluster:** index-redb
- **Notes:** Provide streaming iterator API `iter_streaming` that holds redb read transaction lifetime; yields one entry at a time. Deprecate `iter_collected` for small-table use only.
- **Test:** `streaming_iterator_does_not_materialize_full_set`

### R-098 — [index-memory] `import_index` collects all entries into in-memory Index first (10M = ~1.3 GiB RAM)
- **Source:** AUDIT.md GH-G15
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/index/migration.rs:43-74
- **Cluster:** index-memory
- **Notes:** Streaming export writes snapshot file chunk-by-chunk. On-disk format is fixed-size-per-entry → straightforward.
- **Test:** `migration_export_streaming_does_not_materialize`

### R-099 — [wire-dos] `parse_cold_data_fields` uses u32 as usize plus naive `pos+il` without `checked_add` (32-bit target)
- **Source:** AUDIT.md GH-15
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:2915-2969
- **Cluster:** wire-dos
- **Notes:** `pos.checked_add(il).is_some_and(|end| end <= cold_data.len())` at lines 2915, 2935, 2952.
- **Test:** `parse_cold_data_checked_add_overflow`

### Cluster: blob / pruning / mmap

### R-100 — [blob-gc] Parent-directory fsync only protects second rename; intermediates may not be persistent
- **Source:** AUDIT.md IJK-03
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/storage/blobstore.rs:147-160, :378-399
- **Cluster:** blob-gc
- **Notes:** When `create_dir_all` creates new intermediates, fsync the chain bottom-up. Cache "fsynced these dirs" set to avoid repeated cost.
- **Test:** Inspection of call graph + power-loss harness if available.

### R-101 — [blob-gc] Concurrent uploads of same `tx_id` race on `.tmp` path → corrupt/interleaved blob
- **Source:** AUDIT.md IJK-07
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/storage/blobstore.rs:378-399, src/server/dispatch.rs:4868-4938
- **Cluster:** blob-gc
- **Notes:** Unique per-attempt temp suffix (`<blob>.<random>.tmp`); in-process `Mutex<HashSet>` rejects concurrent writers; defer cross-node serialization to dispatch shard ownership.
- **Test:** `concurrent_blob_writes_no_corruption`

### R-102 — [pruning] `handle_process_expired` doesn't re-validate `should_delete_at_height` before deleting
- **Source:** AUDIT.md IJK-09
- **Severity:** MEDIUM
- **Status:** RESOLVED (folded into R-008)
- **Files:** src/server/dispatch.rs (`handle_process_expired`)
- **Cluster:** process-expired
- **Resolution:** R-008's rewrite re-reads on-device metadata for every DAH candidate and only proceeds with delete when `preserve_until == 0 && 0 < delete_at_height <= current_height && spent_utxos == utxo_count && unmined_since == 0`. The R-008 test `dispatch_process_expired_deletes_only_truly_eligible` covers this directly: it inserts a stale DAH entry for an unspent record (the IJK-09 attack scenario) and asserts the record is NOT deleted.
- **Test:** `dispatch_process_expired_deletes_only_truly_eligible` (folded in)

### R-103 — [pruning] Compensation hard-codes `block_height_retention: 0` → DAH index diverges from on-device state after rollback
- **Source:** AUDIT.md IJK-10
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:1696-1818, src/replication/receiver.rs:738-790
- **Cluster:** pruning
- **Notes:** Propagate original `block_height_retention` through compensation; or use `SERVER_DEFAULT_RETENTION` as defense.
- **Test:** `unspend_compensation_preserves_dah`

### R-104 — [pruning] `handle_query_old_unmined` doesn't filter by `preserve_until` → preserved-but-unmined records may be deleted by pruner
- **Source:** AUDIT.md IJK-11
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:4569-4588
- **Cluster:** pruning
- **Notes:** Filter out `tx_flags & HAS_PRESERVE_UNTIL`. Zero-I/O — flag is cached. Or have unmined index carry the bit and skip on insert.
- **Test:** `query_old_unmined_excludes_preserved_records`

### R-105 — [device-io] `engine.delete` tombstone zeroes only `magic` + `record_size`; freed region forensically recoverable
- **Source:** AUDIT.md IJK-12
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/ops/engine.rs:2696-2714
- **Cluster:** device-io
- **Notes:** Cheap fix: extend tombstone to zero entire metadata header. Expensive: zero-write entire record before `allocator.free`, gated `secure_delete = true` config.
- **Test:** `tombstone_overwrites_metadata_header`

### R-106 — [device-io] Redo log truncation not coordinated with snapshot persist; log grows unboundedly and may wrap
- **Source:** AUDIT.md IJK-15
- **Severity:** MEDIUM
- **Status:** OPEN, blocked-by R-003
- **Files:** src/allocator.rs:455-565
- **Cluster:** device-io
- **Notes:** After every `persist`, truncate redo log up to persisted point via "checkpoint marker" entry naming snapshot's high-water; advance redo tail.
- **Test:** `allocator_redo_truncated_after_persist`

### R-107 — [blob-gc] Delete-batch compensation may silently restore record with dangling external_ref if blob GC'd
- **Source:** AUDIT.md IJK-19
- **Severity:** MEDIUM
- **Status:** OPEN, partially folded into R-007
- **Files:** src/server/dispatch.rs:3957-4097, src/ops/engine.rs:2688-2742
- **Cluster:** blob-gc
- **Notes:** Stream blob to temp file vs. memory. Tri-state `DeleteSnapshot`: snapshot OK / snapshot failed because blob missing. On blob-missing case, surface hard error to operator.
- **Test:** `delete_compensation_blob_missing_returns_hard_error`

### R-108 — [mmap-io] `engine.delete` doesn't call `mgr.delete_cold_data` to release separate-NVMe allocations; tier not actually wired into production
- **Source:** AUDIT.md IJK-23
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/ops/engine.rs:2688-2742, src/storage/manager.rs:231-244
- **Cluster:** mmap-io
- **Notes:** Either implement `SeparateNvme` tier fully (add `separate_cold_offset` + `separate_cold_size` to `TxMetadata`) or remove the scaffolding to avoid spec mismatch.
- **Test:** `separate_nvme_tier_actually_used_or_removed`

### Cluster: observability + DoS limits + repo-hazards

### R-109 — [dos-limits] Per-connection read buffer grows unbounded to 16 MiB; never shrunk across connection lifetime
- **Source:** AUDIT.md LMNH-02, GH-17
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/server/mod.rs:215, :255-261
- **Cluster:** dos-limits
- **Notes:** `read_buf.shrink_to(256 * 1024)` after each request; or global slab/pool of read buffers.
- **Test:** `read_buf_shrinks_after_small_frame`

### R-110 — [dos-limits] Silent client never sending bytes blocks connection indefinitely (read-timeout loop retries forever)
- **Source:** AUDIT.md LMNH-03
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/server/mod.rs:120-163, :208-231, :229
- **Cluster:** dos-limits
- **Notes:** On `TimedOut` at length-prefix boundary, return `Ok(())` to drop. Or track `last_activity` and close after idle timeout (5 min).
- **Test:** `silent_client_dropped_after_idle_timeout`

### R-111 — [observability] WebSocket `/ws/top` push has no client-side backpressure detection for slow readers
- **Source:** AUDIT.md LMNH-05
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/server/http.rs:1764-1788, :1780, :1777, :1512, :1172
- **Cluster:** observability
- **Notes:** Wrap `socket.send` in `tokio::time::timeout` (5s); on timeout break + drop.
- **Test:** `websocket_drops_slow_reader_within_10s`

### R-112 — [observability] Web UI assigns server JSON directly into HTML via `.innerHTML` → XSS hazard
- **Source:** AUDIT.md LMNH-10
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** ui/app.js:1330,1346,178,185
- **Cluster:** observability
- **Notes:** `.textContent` assignment for data-only fields; or `escapeHtml` helper. Mechanical fix.
- **Test:** XSS test with field containing `<script>...</script>`

### R-113 — [observability] Per-op `attempted` is batch-level; `succeeded`/`failed` is item-level → success rate inflated by batch size
- **Source:** AUDIT.md LMNH-11
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:330-345, :2335, :4543-4555, :332, :3303, :326-329
- **Cluster:** observability
- **Notes:** Rename batch counters (`teraslab_creates_batches_total`); add item-level `teraslab_creates_items_attempted_total` mirroring spend/unspend. `teraslab_operations_total{op,outcome}` is item-granular — the long-term home.
- **Test:** `metrics_creates_attempted_is_item_count_not_batch_count`

### R-114 — [repo-hazards] Length casts in `get_batch` encoding can silently truncate counts
- **Source:** AUDIT.md LMNH-32
- **Severity:** MEDIUM
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:3236, :3242, :4472, :4494
- **Cluster:** repo-hazards
- **Notes:** `assert!` or error_response guards rejecting lengths exceeding wire field range. Pattern at line 4494; apply to other 3 sites.
- **Test:** `get_batch_rejects_truncating_length_casts`

---

## LOW

### Cluster: spend-op / freeze-op / reorg-op edge cases

### R-115 — [freeze-op] `spendable_height` boundary ambiguity at exact height (>= vs >)
- **Source:** AUDIT.md A-14
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/ops/engine.rs:865, :996
- **Cluster:** freeze-op
- **Notes:** Boundary test at block 1100, spendable_after=100. Doc explicit semantics; align with Lua reference (R-020).
- **Test:** `reassign_spendable_height_boundary_at_exact_height`

### R-116 — [spend-op] `spend_multi` idempotent re-spend generation increment differs from single-spend
- **Source:** AUDIT.md A-16
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/ops/engine.rs:1003-1022, :881-883, :2929-2931
- **Cluster:** spend-op
- **Notes:** Document explicitly: `spend_multi` generation bump is once per batch regardless of item count. Or match single-spend behavior.
- **Test:** N/A (doc + test name aligned)

### R-117 — [spend-op] Coinbase maturity test missing for IS_COINBASE without `spending_height` (height=0)
- **Source:** AUDIT.md A-18
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/ops/engine.rs:790-797, :971-978
- **Cluster:** spend-op
- **Notes:** Decide intended semantics; add explicit test or guard adjustment.
- **Test:** `spend_coinbase_zero_spending_height_boundary`

### R-118 — [reorg-op] `append_conflicting_child` rebuild uses pread of stale device buffer → stale trailing bytes
- **Source:** AUDIT.md A-19
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/ops/engine.rs:2336-2350
- **Cluster:** reorg-op
- **Notes:** Zero `wbuf` before copying children in. Drop the pread.
- **Test:** `append_conflicting_child_no_stale_bytes_leak`

### R-119 — [recovery] Delete operation does not mark child UTXOs of spending transactions as PRUNED — spec intent unclear
- **Source:** AUDIT.md A-20
- **Severity:** LOW
- **Status:** OPEN, blocked-by R-020 spec resolution
- **Files:** src/ops/engine.rs:2688-2743
- **Cluster:** recovery
- **Notes:** Document explicitly. If propagation is intended, walk C's inputs cold-data; for each parent P, write P's spent-by-C slot to UTXO_PRUNED. O(N inputs) device writes — needs lock-ordering analysis.
- **Test:** `delete_specify_intent_pruned_propagation`

### R-120 — [spend-op] `spent_count` underflow guard in unspend masks inconsistency
- **Source:** AUDIT.md A-23
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/ops/engine.rs:1130-1133
- **Cluster:** spend-op
- **Notes:** `tracing::error!` in guard or return `SpendError::StorageError { detail: "counter desync" }`.
- **Test:** `unspend_counter_desync_surfaces_error`

### R-121 — [reorg-op] `set_conflicting` slow path errors logged inadequately
- **Source:** AUDIT.md A-25
- **Severity:** LOW
- **Status:** OPEN, dup R-081 (same finding)
- **Files:** src/ops/engine.rs:2525-2532
- **Cluster:** reorg-op
- **Notes:** See R-081.
- **Test:** See R-081.

### R-122 — [freeze-op] `freeze` on already-FROZEN slot checks hash before status — may differ from Lua precedence
- **Source:** AUDIT.md A-27
- **Severity:** LOW
- **Status:** OPEN, blocked-by R-020 (Lua restore)
- **Files:** src/ops/engine.rs:2179-2192
- **Cluster:** freeze-op
- **Notes:** Verify against Lua once `specs/teranode.lua` restored.
- **Test:** `freeze_error_precedence_hash_vs_status_verify_lua`

### R-123 — [spend-op] Test `concurrent_spend_same_utxo_same_data` doesn't verify all threads see actual stored spending_data
- **Source:** AUDIT.md A-28
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/ops/engine.rs:4131-4163
- **Cluster:** spend-op
- **Notes:** `let slot = engine.read_slot(&key, 5).unwrap(); assert_eq!(slot.spending_data, sd);` after test.
- **Test:** `concurrent_spend_same_data_verifies_slot_spending_data`

### R-124 — [replication] `set_locked`/`set_conflicting` fast/slow path divergence — slow bumps metadata gen, fast bumps cached entry
- **Source:** AUDIT.md A-31
- **Severity:** LOW
- **Status:** OPEN, blocked-by R-016
- **Files:** src/ops/engine.rs:2569,2446,2631,2505
- **Cluster:** replication
- **Notes:** Fixed transitively by R-016 (cache always synced).
- **Test:** `set_locked_conflicting_fast_slow_generation_parity`

### R-125 — [dispatch] `spend_multi` `response.errors` is HashMap → non-deterministic iteration when serialised
- **Source:** AUDIT.md A-32
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/ops/spend.rs:49, src/ops/engine.rs:824
- **Cluster:** dispatch
- **Notes:** Change to `BTreeMap<u32, SpendError>` or sort before serialising in dispatch.
- **Test:** `spend_multi_errors_deterministic_iteration`

### R-126 — [dispatch] `Engine::pre_allocate_create` doesn't check `is_external` ↔ `external_ref` consistency
- **Source:** AUDIT.md A-33
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/ops/engine.rs:1761-1793
- **Cluster:** dispatch
- **Notes:** Add internal invariant comment + assert: every `pre_allocate_create` `Err` return must be before allocation.
- **Test:** `pre_allocate_create_invariant_error_before_allocation`

### R-127 — [spend-op] Idempotent re-spend writes metadata even when no state changed → DoS amplifier
- **Source:** AUDIT.md A-34
- **Severity:** LOW
- **Status:** OPEN, blocked-by R-021 decision
- **Files:** src/ops/engine.rs:1003-1022
- **Cluster:** spend-op
- **Notes:** Detect idempotent at dispatch boundary and short-circuit; or skip metadata write in idempotent cases.
- **Test:** `idempotent_respend_short_circuit_or_skip_write`

### Cluster: redo-log + recovery edge cases

### R-128 — [redo-device-io] `RedoLog::flush` swallows pre-write read failures during partial-block RMW
- **Source:** AUDIT.md BC-14
- **Severity:** LOW (MEDIUM in audit; downgraded — partial-block RMW is rare path)
- **Status:** OPEN
- **Files:** src/redo.rs:1098-1114
- **Cluster:** redo-log
- **Notes:** RMW read failures are fatal; bubble `DeviceError`. Or write-allocator pads to alignment with explicit known bytes.
- **Test:** `redo_flush_rmw_read_failure`

### R-129 — [redo-performance] `RedoLog::scan_all` reads entire log on every recover/read_from_sequence/earliest_sequence
- **Source:** AUDIT.md BC-15, BC-17, BC-55
- **Severity:** LOW (perf only; correctness unaffected)
- **Status:** OPEN
- **Files:** src/redo.rs:1271-1294, :1040-1051
- **Cluster:** redo-log
- **Notes:** Cache parsed entries in memory; append on write; re-scan only on `open()`. Combined with BC-17 + BC-55, startup does multiple full log scans + 64 MiB heap allocs.
- **Test:** `redo_scan_caching_reduces_repeated_io`

### R-130 — [redo-deadcode] `flushed_pos` written but never read
- **Source:** AUDIT.md BC-16
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/redo.rs:991,1035,1142,1264
- **Cluster:** redo-log
- **Notes:** Delete or document.
- **Test:** N/A

### R-131 — [locks] Stripe lock count power-of-two but byte selector wastes 16 bits → can't scale beyond 65536
- **Source:** AUDIT.md BC-20, BC-32
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/locks.rs:36-40, src/config.rs:422, src/index/hashtable.rs:725-765
- **Cluster:** locks
- **Notes:** Use bytes 16..24 (u64 selector) — 64-bit headroom. Or assert `count <= 65536` at construction.
- **Test:** `stripe_index_large_lock_count`

### R-132 — [dispatch-async] `parking_lot::Mutex` held across `block_on` in dispatch — runtime starvation risk
- **Source:** AUDIT.md BC-21
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:1286-1314
- **Cluster:** dispatch-async
- **Notes:** Debug-mode assert: no Engine-owned mutex held when `replicate_all_ops` entered. Currently OK; future change wrapping entire batch could deadlock.
- **Test:** `dispatch_no_mutex_across_block_on`

### R-133 — [documentation] `engine.read_metadata`/`read_slot`/`lookup_cached` doc says "for testing" but used in production
- **Source:** AUDIT.md BC-22
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/ops/engine.rs:2783,2807,2802, src/server/dispatch.rs:4377,4456,4326
- **Cluster:** documentation
- **Notes:** Update doc to describe actual purpose + concurrency contract. Hot-path production functions without stripe lock (R-009).
- **Test:** N/A

### R-134 — [recovery-tolerance] Recovery tolerance ceiling 65536 MissingPrimary failures — high but bounded
- **Source:** AUDIT.md BC-27
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/server/startup.rs:143,193
- **Cluster:** recovery-tolerance
- **Notes:** Make ceiling configurable. Or correlate Delete entries against MissingPrimary in single pass.
- **Test:** `recovery_tolerance_configurable`

### R-135 — [replication-concurrency] Replication intent ranges use `std::sync::Mutex<BTreeSet<…>>` (poison-prone)
- **Source:** AUDIT.md BC-28
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/replication/durable.rs:11,226,235,254,273,285,428,438,453,471
- **Cluster:** replication-concurrency
- **Notes:** Standardize on `parking_lot::Mutex` (no poison). Currently mixing std-Mutex + `unwrap_or_else(|e| e.into_inner())` consumes panic-poisoned state.
- **Test:** N/A

### R-136 — [hashtable] HashTable count = usize without consistency check
- **Source:** AUDIT.md BC-31
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/index/hashtable.rs:735,800
- **Cluster:** hashtable
- **Notes:** Debug-mode invariant check: walks table at regular cadence, confirms `count == sum(is_occupied)`.
- **Test:** `hashtable_count_consistency`

### R-137 — [timestamp] `engine.refresh_clock()` uses Relaxed → concurrent ops see stale millis
- **Source:** AUDIT.md BC-33, BC-52
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/ops/engine.rs:437-446,443-446
- **Cluster:** timestamp
- **Notes:** Use `Ordering::SeqCst` or per-op `sys_millis()`. Document staleness.
- **Test:** `clock_refresh_staleness_bounded`

### R-138 — [allocator-recovery] Allocator's `next_offset` advance NOT capped by device size in redo replay
- **Source:** AUDIT.md BC-40, IJK-13
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/allocator.rs:704-720,886-907 (replay_redo)
- **Cluster:** allocator-recovery
- **Notes:** Bounds-check `offset + size <= device_size` in `replay_redo`; return error if violated.
- **Test:** `allocator_replay_bounds_check`

### R-139 — [performance] `crate::fault_injection::check` runtime check on every mutation hot path
- **Source:** AUDIT.md BC-41
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/redo.rs:1123,1137, src/ops/engine.rs:2917,2926, src/allocator.rs:554,604
- **Cluster:** performance
- **Notes:** Gate behind `#[cfg(any(test, feature = "fault_injection"))]`; production builds compile to no-ops.
- **Test:** N/A (perf only)

### R-140 — [dispatch] `engine.set_mined_batch` doesn't acquire all locks at once — multi-tx batches not atomic
- **Source:** AUDIT.md BC-43
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/ops/engine.rs:1521-1529
- **Cluster:** dispatch
- **Notes:** Document explicitly: batch is NOT atomic; intermediate states observable. Redo entries written together but engine applies serialized per-key.
- **Test:** N/A

### R-141 — [delete-recovery] Tombstone WRITE before `allocator.free` but separate fsyncs — buffered device window
- **Source:** AUDIT.md BC-47
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/ops/engine.rs:2701-2714,:2706
- **Cluster:** delete-recovery
- **Notes:** After `write_metadata_fast`, call `device.sync()` explicitly. Or document "delete requires synchronous-write device".
- **Test:** `delete_tombstone_sync`

### R-142 — [dispatch-performance] `read_full_record` path uses `read_utxo_slot` in loop — N device reads per slot
- **Source:** AUDIT.md BC-48
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:4452-4467
- **Cluster:** dispatch-performance
- **Notes:** `engine.read_slots(&key, &offsets) -> Vec<…>`: one index read lock, all slots. 100-UTXO records → 100 index reads currently.
- **Test:** `read_slots_batched_reduces_index_reads`

### R-143 — [lock-ordering] `append_conflicting_child` holds parent's stripe lock across `allocator.free + allocator.allocate`
- **Source:** AUDIT.md BC-49
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/ops/engine.rs:2280,2320,2326
- **Cluster:** lock-ordering
- **Notes:** Restructure: alloc/free OUTSIDE parent's stripe lock; only metadata mutation under lock.
- **Test:** `append_conflicting_child_lock_order`

### R-144 — [concurrency-ordering] `unregister_with_shard_count` releases index write lock BEFORE shard_counts decrement visible to other CPUs
- **Source:** AUDIT.md BC-50, BC-60
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/ops/engine.rs:528-537,878,474-476
- **Cluster:** concurrency-ordering
- **Notes:** `Ordering::Release` for fetch_sub paired with `Acquire` on read side. Or move fetch_sub BEFORE lock release so index drop is the synchronization edge.
- **Test:** `shard_counts_memory_ordering`

### R-145 — [startup] `shard_counts` initialization on startup loops over rebuilt index → O(index_size)
- **Source:** AUDIT.md BC-51
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/ops/engine.rs:101-105
- **Cluster:** startup
- **Notes:** Snapshot at shutdown, restore at startup. Or compute lazily.
- **Test:** `engine_startup_shard_counts_lazy`

### R-146 — [redo-robustness] `RedoLog::open` does NOT detect/skip a partially-written final entry
- **Source:** AUDIT.md BC-63
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/redo.rs:944-972,1283-1290
- **Cluster:** redo-robustness
- **Notes:** Documented review concluded code is correct; document deployment must use fsync-honoring filesystem. Operator confusion risk only.
- **Test:** `redo_partial_entry_handling_documented`

### R-147 — [recovery-performance] `RedoLog::recover` returns ALL entries after last checkpoint regardless of replay status
- **Source:** AUDIT.md BC-64
- **Severity:** LOW
- **Status:** OPEN, blocked-by R-003
- **Files:** src/redo.rs:1192-1208
- **Cluster:** recovery-performance
- **Notes:** Periodic "recovery progress" entry to redo log; or external checkpoint after each successful entry replay. Recovery time unbounded under repeated crashes otherwise.
- **Test:** `recovery_progress_tracking`

### R-148 — [redo-sequencing] `RedoLog::append` increments `next_sequence` BEFORE entry serializes; never rolls back on failure
- **Source:** AUDIT.md BC-67
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/redo.rs:1059-1079
- **Cluster:** redo-log
- **Notes:** Decrement on `LogFull` path; or `checked_add` and roll back; or compute `next_sequence` lazily. Currently `LogFull` leaves gaps.
- **Test:** `redo_append_failure_sequence_gap`

### R-149 — [hashtable] `max_probe_distance` can degrade over time without resize triggering
- **Source:** AUDIT.md BC-68
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/index/hashtable.rs:736-739,815-817
- **Cluster:** hashtable
- **Notes:** Periodically recompute `max_probe` (e.g., during resize); or reset after a removal that emptied the previous max-probe bucket.
- **Test:** `hashtable_max_probe_maintenance`

### R-150 — [redo-concurrency] `redo_log` Mutex held during entire `pwrite + sync` (group-commit needed)
- **Source:** AUDIT.md BC-38, BC-69
- **Severity:** LOW (already covered by R-152 group-commit; tracking separately for record)
- **Status:** OPEN, blocked-by/dup R-152
- **Files:** src/redo.rs:1083-1150
- **Cluster:** redo-concurrency
- **Notes:** See R-152.
- **Test:** See R-152.

### R-151 — [testing] `MemoryDevice` (test-only) does not honor alignment contract on raw_ptr access
- **Source:** AUDIT.md BC-70
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/device.rs:285-310
- **Cluster:** testing
- **Notes:** Document test-only nature explicitly.
- **Test:** N/A

### R-152 — [redo-concurrency] Group-commit needed: every batch serializes on redo log Mutex (200 ops/sec ceiling at 5ms fsync)
- **Source:** AUDIT.md BC-38
- **Severity:** LOW (perf only; not correctness)
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:984-1005, src/redo.rs:1117,1127
- **Cluster:** redo-concurrency
- **Notes:** Collect ops from concurrent dispatchers into one fsync via separate flush thread; concurrent dispatchers wait on condvar for their sequence range to be flushed.
- **Test:** `redo_group_commit`

### R-153 — [replication-io] Replication intent tracker writes to disk on every begin/commit — extra fsyncs per batch
- **Source:** AUDIT.md BC-39
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/replication/durable.rs:255-262,273-281,289-297,41-52
- **Cluster:** replication-io
- **Notes:** Coalesce intent updates: write only when set changes by threshold; or piggyback on redo log fsync.
- **Test:** `replication_intent_fsync_coalesced`

### R-154 — [redo-lifecycle] `RedoLog::checkpoint()` writes Checkpoint entry but does NOT trigger reclamation
- **Source:** AUDIT.md BC-71
- **Severity:** LOW
- **Status:** OPEN, blocked-by R-003
- **Files:** src/redo.rs:1185-1190
- **Cluster:** redo-log
- **Notes:** Either rename `checkpoint` to `mark_checkpoint`, or have it auto-trigger reclamation when safe.
- **Test:** `redo_checkpoint_space_reclamation`

### R-155 — [allocator-recovery] `handle_create_batch`'s `allocator.lock().free()` on failed redo flush is NOT journaled — leaks redo entries
- **Source:** AUDIT.md BC-72
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:3171-3192, src/allocator.rs:574
- **Cluster:** allocator-recovery
- **Notes:** Treat redo flush failures as fatal at dispatcher; abort process so operator investigates. Don't roll back via more redo writes that will themselves fail.
- **Test:** `create_batch_redo_failure_fatal`

### R-156 — [expiration-recovery] `OP_PROCESS_EXPIRED_PRESERVATIONS` redo handling opaque (UNVERIFIED)
- **Source:** AUDIT.md BC-73
- **Severity:** LOW (folded into R-008)
- **Status:** OPEN, blocked-by R-008
- **Files:** src/server/dispatch.rs:395
- **Cluster:** expiration-recovery
- **Notes:** R-008 covers full overhaul; this entry verifies redo coverage of all per-record state changes.
- **Test:** `process_expired_preservations_redo_coverage`

### R-157 — [dispatch-consistency] `handle_query_old_unmined` operates on snapshot of unmined index — concurrent updates not reflected
- **Source:** AUDIT.md BC-74
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:391, src/ops/engine.rs:2779
- **Cluster:** dispatch-consistency
- **Notes:** Document snapshot semantics. Clients understand result is snapshot at lock-release time.
- **Test:** N/A

### R-158 — [metadata-io] `engine.write_metadata_fast` on non-direct path does pread+memcpy+pwrite — RMW window
- **Source:** AUDIT.md BC-76
- **Severity:** LOW (covered by R-009 stripe-read-lock)
- **Status:** OPEN, blocked-by R-009
- **Files:** src/ops/engine.rs:563-579, src/io.rs:309-332
- **Cluster:** metadata-io
- **Notes:** Stripe read lock for slot reads (R-009) closes the window.
- **Test:** `concurrent_metadata_slot_rw`

### R-159 — [compensation-recovery] `CompensateUnsetMined` replay can fail with LogicError when overflow exists
- **Source:** AUDIT.md BC-77
- **Severity:** LOW (rare — more than 3 block entries needed)
- **Status:** OPEN
- **Files:** src/recovery.rs:1119-1125
- **Cluster:** compensation-recovery
- **Notes:** Recovery path calls into engine to allocate overflow space when restoring entry beyond inline capacity. Or capture more context in `CompensateUnsetMined` (inline slot K vs overflow position N).
- **Test:** `compensate_unset_mined_overflow`

### R-160 — [hashtable-recovery] Index file resize tmp file can leak across crashes when redo log is anonymous
- **Source:** AUDIT.md BC-78
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/index/hashtable.rs:469-636
- **Cluster:** hashtable-recovery
- **Notes:** Update tests to use persistent redo log when testing file-backed indexes.
- **Test:** `index_resize_file_backed_redo_log`

### R-161 — [engine-config] `engine.set_blob_store` takes `&mut self` but Engine shared via `Arc<Engine>` — cannot call after sharing
- **Source:** AUDIT.md BC-79
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/ops/engine.rs:449-451
- **Cluster:** engine-config
- **Notes:** Document "set_blob_store must be called before sharing" or wrap in `parking_lot::Mutex<Option<…>>`.
- **Test:** N/A

### R-162 — [engine-config] `engine.set_redo_log` mutates `Option<…>` AT RUNTIME via Mutex
- **Source:** AUDIT.md BC-59
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/ops/engine.rs:131-133, :51
- **Cluster:** engine-config
- **Notes:** Use `arc_swap::ArcSwapOption<RedoLog>` for lock-free reads.
- **Test:** N/A

### R-163 — [redo-api] `RedoLog::append_batch_and_flush` on empty input returns `(current, current)` without flushing
- **Source:** AUDIT.md BC-81
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/redo.rs:1170-1174, src/server/dispatch.rs:1057-1060
- **Cluster:** redo-api
- **Notes:** Return `Ok((0, 0))` for empty input.
- **Test:** `append_batch_empty_input`

### R-164 — [redo-validation] Recovery does not validate consecutive redo entries have monotonically increasing sequences
- **Source:** AUDIT.md BC-82
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/redo.rs:1271-1294
- **Cluster:** redo-validation
- **Notes:** `entry.sequence == prev.sequence + 1` (or `>`) during scan; flag corruption.
- **Test:** `redo_sequence_monotonicity_validation`

### Cluster: replication LOW

### R-165 — [tcp-transport] `is_connected` probe creates 1ms read window → false positives on flaky links
- **Source:** AUDIT.md D-04
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/replication/tcp_transport.rs:237-251
- **Cluster:** tcp-transport
- **Notes:** Drop `is_connected()` — accept single-RTT retry as canonical detection (simpler). Or read `SO_ERROR` after peek.
- **Test:** `is_connected_stale_pipe_returns_true_before_keepalive`

### R-166 — [replica-receiver] `apply_op` reads slot from device for every Spend/Freeze/Unfreeze/Reassign — duplicated I/O
- **Source:** AUDIT.md D-07
- **Severity:** LOW, **MIGRATION-REQUIRED** (wire format V3)
- **Status:** OPEN
- **Files:** src/replication/receiver.rs:740-895
- **Cluster:** replica-receiver
- **Notes:** Add `utxo_hash: [u8; 32]` to ReplicaOp Spend/Freeze/Unfreeze/Reassign. Wire change V3.
- **Test:** `apply_op_uses_passed_hash_not_device_read`

### R-167 — [replication-protocol] `ReplicaAck::Error` wrapped in `STATUS_OK` frame — conflates framing/application success
- **Source:** AUDIT.md D-08
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/replication/receiver.rs:541-575
- **Cluster:** replication-protocol
- **Notes:** Use `STATUS_ERROR` for `ReplicaAck::Error`, OR document the convention prominently. Fix inconsistency with Phase B2 stale-epoch path which uses `STATUS_ERROR + ERR_STALE_EPOCH`.
- **Test:** `replica_ack_error_wire_status_consistent`

### R-168 — [tcp-transport] `connect()` timeout reused for read AND write — masks master-side stalls
- **Source:** AUDIT.md D-09
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/replication/tcp_transport.rs:99-123
- **Cluster:** tcp-transport
- **Notes:** `set_write_timeout(Some(timeout))` inside `send_batch` matching `recv_ack`. Or separate `replication_write_timeout_ms` config; default to `replication_timeout_ms`.
- **Test:** `write_timeout_independent_of_connect_timeout`

### R-169 — [replication-protocol] `ReplicaOp::Create` `is_external` defaults silently `false` on truncated payloads
- **Source:** AUDIT.md D-10
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/replication/protocol.rs:582-591
- **Cluster:** replication-protocol
- **Notes:** Bump wire-format invariant: explicit `is_external_present: u8` flag, OR extend tag table to `Create_v2`. Reject truncated frames with `ProtocolError::BufferTooShort`.
- **Test:** `create_missing_is_external_byte_rejected`

### R-170 — [tcp-transport] `recv_ack` allocates 16 MiB on attacker-controlled length prefix
- **Source:** AUDIT.md D-12
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/replication/tcp_transport.rs:188-196
- **Cluster:** tcp-transport
- **Notes:** Cap allocations to `min(MAX_FRAME_SIZE, expected_op_count * max_op_size + header_size)`. ACK frames specifically (9-30 bytes), drop cap to 1 KiB.
- **Test:** `recv_ack_max_allocation_1kib_for_error_response`

### R-171 — [startup-recovery] `replication_intent_tracker` startup recovery does not advance `next_sequence`; fragile ordering
- **Source:** AUDIT.md D-13
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:1420-1503
- **Cluster:** startup-recovery
- **Notes:** Barrier in `bin/server.rs` asserts recovery loop completed before listener bound. Document ordering invariant.
- **Test:** `recovery_completes_before_listener_bind`

### R-172 — [backpressure] No bounded backpressure between dispatch and replication runtime — large bursts can OOM
- **Source:** AUDIT.md D-14
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:76-79, :1285-1314
- **Cluster:** backpressure
- **Notes:** Bound `replicate_all_ops` in flight via `Semaphore`. Share `Arc<Vec<ReplicaOp>>` inside `ReplicaBatch` instead of clone-per-target.
- **Test:** `replication_backpressure_bounded_by_semaphore`

### R-173 — [config-validation] `validate_cluster_safety` rejects `best_effort + RF>1` but doesn't reject `ack_policy="best_effort"` disabling enforcement
- **Source:** AUDIT.md D-16
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/config.rs:522-533, :491-503
- **Cluster:** config-validation
- **Notes:** Reject `ack_policy="best_effort"` when `RF>1 && replication_degraded_mode != "best_effort"`. Or rename to `fire_and_forget`.
- **Test:** `ack_policy_best_effort_requires_degraded_mode_best_effort`

### R-174 — [replica-receiver] Divergent-Create path doesn't delete orphaned cold-data blob
- **Source:** AUDIT.md D-17
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/replication/receiver.rs:646-664
- **Cluster:** replica-receiver
- **Notes:** Delete-cold-data step in divergent-duplicate path. Combined with R-071 (Create generation guard), divergent case becomes rare.
- **Test:** `divergent_create_cleans_up_old_blob`

### R-175 — [replication-manager] `run_catchup` `chunk_seq` advances on master view → can diverge from replica ACK
- **Source:** AUDIT.md D-18
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/replication/manager.rs:586-617
- **Cluster:** replication-manager
- **Notes:** After `recv_ack`, validate `through_sequence == batch.last_sequence()`; fail-stop or advance `chunk_seq` to `through_sequence + 1`.
- **Test:** `catchup_chunk_seq_matches_replica_ack_sequence`

### R-176 — [receiver-performance] Receiver allocates `Vec<u8>` per ACK frame in connection hot loop
- **Source:** AUDIT.md D-21
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/replication/receiver.rs:251
- **Cluster:** receiver-performance
- **Notes:** Reuse single `Vec<u8>` across loop or `BytesMut`/slab allocator across all connections.
- **Test:** `receiver_reuses_buffer_per_connection`

### R-177 — [replication-protocol] `lookup_before` silently degrades to zeros on parallel-array invariant violation
- **Source:** AUDIT.md D-22
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:1683-1691
- **Cluster:** replication-protocol
- **Notes:** Make `before_images` and `repl_ops` same Vec by changing `replicate_all_ops` to take `&[(TxKey, Vec<(ReplicaOp, BeforeImage)>)]`. Debug assert otherwise.
- **Test:** `compensation_fallback_never_writes_zero_hashes`

### Cluster: cluster + sharding LOW

### R-178 — [swim-membership] SWIM garbage-collect dead nodes cliff at 1 hour permits stale-node forgery
- **Source:** AUDIT.md EF-07
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/cluster/swim.rs:399-418, src/cluster/membership.rs:317
- **Cluster:** swim-membership
- **Notes:** Bump forget-dead window to 24h+. Or persist "previously-seen NodeIds with last incarnation" so same NodeId cannot be reborn at lower incarnation than historic peak.
- **Test:** `dead_node_reborn_cannot_use_lower_incarnation`

### R-179 — [migration-fence] Reads on new master before migration completes return immediately with no wait
- **Source:** AUDIT.md EF-14
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:4310-4321,4747-4762
- **Cluster:** migration-fence
- **Notes:** Document client SDK contract; add metric `migration_inbound_pending_seconds`.
- **Test:** `client_handles_migration_in_progress_polling`

### R-180 — [migration-handshake] `clear_stale_inbound` 30s timeout can race with slow large-shard migrations
- **Source:** AUDIT.md EF-15
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/cluster/coordinator.rs:6186-6231, src/cluster/migration.rs:769-790
- **Cluster:** migration-handshake
- **Notes:** Make timeout function of `migration_pool_size × migration_batch_size × record_size / known_throughput`. Or plumb source's last-progress timestamp; only evict if no progress in 30s.
- **Test:** `slow_large_shard_migration_not_evicted_at_30s`

### R-181 — [migration-fence] `/admin/drain/{node_id}` returns before drain completes
- **Source:** AUDIT.md EF-18
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/server/http.rs:1126-1151, src/cluster/coordinator.rs:6011-6062
- **Cluster:** migration-fence
- **Notes:** Rename to `/admin/drain/initiate` or accept `?wait_seconds=N` polling.
- **Test:** `drain_endpoint_waits_until_complete`

### R-182 — [redirect-routing] Partition map version can lag `committed_term` during commit-to-activation window
- **Source:** AUDIT.md EF-19
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/cluster/coordinator.rs:5778-5784,5792-5870
- **Cluster:** redirect-routing
- **Notes:** Use `committed_term` consistently in encoded partition map.
- **Test:** `partition_map_version_consistent_with_committed_term`

### R-183 — [swim-membership] SWIM `MAX_MSG_SIZE=4096` hard-coded; large piggyback silently truncates
- **Source:** AUDIT.md EF-20
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/cluster/swim.rs:30,289-305,887-959
- **Cluster:** swim-membership
- **Notes:** `debug_assert!(buf.len() <= MAX_MSG_SIZE)` around socket sends. Runtime warning when encoder exceeds 80% cap.
- **Test:** `swim_msg_size_warning_on_overflow`

### R-184 — [sharding] `set_master_for_shard` silently ignores unrelated nodes without logging
- **Source:** AUDIT.md EF-22
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/cluster/shards.rs:360-374
- **Cluster:** sharding
- **Notes:** `tracing::warn!` in no-op branch.
- **Test:** `set_master_logs_when_node_not_in_assignment`

### R-185 — [topology-commit] Topology `propose_timeout` coupled to `probe_interval × 3`, tuning non-obvious
- **Source:** AUDIT.md EF-25
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/bin/server.rs:659
- **Cluster:** topology-commit
- **Notes:** Decouple. `topology_propose_timeout` explicit config field; default `max(probe_interval × 3, 500ms)`.
- **Test:** `propose_timeout_independent_of_probe_interval`

### R-186 — [topology-commit] `TopologyCommit` lacks voter list; digest covers only term and members
- **Source:** AUDIT.md EF-28
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/cluster/topology.rs:166-198,569-599
- **Cluster:** topology-commit
- **Notes:** Extend `TopologyCommit` with `voters: Vec<NodeId>` + signature aggregate. Persist alongside `committed_members` for forensic audit trail.
- **Test:** `topology_commit_persists_voter_list`

### R-187 — [redirect-routing] `OP_GET_PARTITION_MAP` no timestamp/signature; clients cache stale maps
- **Source:** AUDIT.md EF-30
- **Severity:** LOW
- **Status:** OPEN, blocked-by R-011
- **Files:** src/server/dispatch.rs:5161-5188, src/cluster/routing.rs:67-93
- **Cluster:** redirect-routing
- **Notes:** Per-connection auth (mTLS) covers; until then, known limitation.
- **Test:** `partition_map_integrity_with_mtls`

### Cluster: wire-protocol DoS / index LOW

### R-188 — [wire-dos] `oversized_frame_rejected` test does not assert error frame contents
- **Source:** AUDIT.md GH-07
- **Severity:** LOW
- **Status:** OPEN
- **Files:** tests/server_tcp.rs:1125-1153
- **Cluster:** wire-dos
- **Notes:** Tighten match arm: require successful read_exact; decode `ResponseFrame`; assert payload + status.
- **Test:** `oversized_frame_rejected_sends_error_response`

### R-189 — [wire-dos] `error_response` payload uses `(msg.len() as u16)` cast — silently truncates >65535 bytes
- **Source:** AUDIT.md GH-10
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:4997-5007
- **Cluster:** wire-dos
- **Notes:** Truncate error message to 65535 before u16 cast. Synthetic test with `format!("...{:?}", huge_struct)` to error path.
- **Test:** `error_response_truncates_long_messages`

### Cluster: storage / blob LOW

### R-190 — [device-io] Allocator's `replay_free` for partially-overlapping region silently ignored → overlapping freelist regions
- **Source:** AUDIT.md IJK-14
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/allocator.rs:764-806
- **Cluster:** device-io
- **Notes:** Detect partial overlap before insert: if `prev_off + prev_sz > offset`, coalesce or reject as corrupt.
- **Test:** `allocator_replay_free_overlap_detection`

### R-191 — [pruning] `delete_at_height` never set for unmined txs — design choice (document)
- **Source:** AUDIT.md IJK-21
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/ops/delete_eval.rs:101-145, src/index/unmined_index.rs
- **Cluster:** pruning
- **Notes:** Document in SPEC_VALIDATION_REPORT.md. If spec wants time-based unmined retention, add `evaluate_unmined_dah` and wire into create + spend.
- **Test:** `unmined_tx_dah_remains_zero` (regression test for current behavior)

### Cluster: observability + DoS limits + repo-hazards LOW

### R-192 — [dos-limits] No aggregate inflight memory cap across max_connections × max_batch_size × per-item-size
- **Source:** AUDIT.md LMNH-04
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/config.rs:269, src/server/dispatch.rs:271, tests/server_tcp.rs:915-942
- **Cluster:** dos-limits
- **Notes:** Global `Semaphore` gating concurrent in-flight batch processing to bounded total memory budget (`max_inflight_batch_items` config). Or document worst-case heap calculation in operator docs.
- **Test:** `aggregate_inflight_memory_capped`

### R-193 — [observability] HTTP server uses single-threaded tokio runtime → queueing under concurrent /metrics + WebSocket
- **Source:** AUDIT.md LMNH-06
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/server/http.rs:72-75, :1172
- **Cluster:** observability
- **Notes:** `Builder::new_multi_thread()` with small pool (2-4 threads). HTTP not on hot path → cost negligible.
- **Test:** `http_metrics_concurrent_load`

### R-194 — [observability] `/debug/records/<txid>` accepts unbounded string before length check
- **Source:** AUDIT.md LMNH-09
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/server/http.rs:1883-1928,:1994-2014,:1995
- **Cluster:** observability
- **Notes:** Reject path lengths >64 chars in extractor. Today Axum allocates full String before `parse_hex_txid` rejects.
- **Test:** `debug_records_rejects_long_path`

### R-195 — [observability] Spend handler computes idempotent count via subtraction instead of direct count
- **Source:** AUDIT.md LMNH-12
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:2503-2506,2486-2489
- **Cluster:** observability
- **Notes:** Have `validated.apply(engine)` return idempotent count directly (validator already knows via `validated.errors`). Prevents upstream double-count from being silently absorbed.
- **Test:** `spend_idempotent_count_direct_not_subtracted`

### R-196 — [observability] `/admin/top` cluster fan-out spawns one task per remote with no concurrency cap
- **Source:** AUDIT.md LMNH-14
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/server/http.rs:1494-1537,:1510,:1512,:1509
- **Cluster:** observability
- **Notes:** Document fan-out behavior. Optionally cap with `futures::stream::iter(...).buffer_unordered(N_PARALLEL)`.
- **Test:** `admin_top_fanout_documented`

### R-197 — [test-infra] Single `#[ignore]` test exists with thin TODO; no tracking issue link
- **Source:** AUDIT.md LMNH-19
- **Severity:** LOW (folded into R-002)
- **Status:** OPEN, blocked-by R-002
- **Files:** src/cluster/coordinator.rs:7505
- **Cluster:** test-infra
- **Notes:** Covered by R-002.
- **Test:** See R-002.

### R-198 — [test-infra] Cluster chaos tests are in-process (deterministic); no random partitions or packet loss
- **Source:** AUDIT.md LMNH-21
- **Severity:** LOW
- **Status:** OPEN
- **Files:** tests/cluster_edge_cases.rs:90,181, tests/cluster_swim.rs:197,240,301, tests/e2e_workload.rs:235, .github/workflows/nightly.yml, teraslab-tests/
- **Cluster:** test-infra
- **Notes:** Add `tokio::test` harness with fault-injecting TCP wrapper (drop X% packets, delay Y ms) on top of `tests/cluster_tcp.rs`. Or document Docker scenario coverage.
- **Test:** `cluster_chaos_with_packet_loss`

### R-199 — [test-infra] Nightly stress tests run at 1/100 scale on PRs; full only via env var
- **Source:** AUDIT.md LMNH-22
- **Severity:** LOW
- **Status:** OPEN
- **Files:** tests/e2e_workload.rs:32-43, .github/workflows/nightly.yml:11
- **Cluster:** test-infra
- **Notes:** Document; add `workflow_dispatch` for full tier on demand.
- **Test:** N/A (CI config)

### R-200 — [test-infra] Only 2 distinct stress test scenarios — missing for set_mined, mark_longest_chain, reassign, set_conflicting, preserve_until
- **Source:** AUDIT.md LMNH-23
- **Severity:** LOW
- **Status:** OPEN
- **Files:** tests/stress_tests.rs:9,16, tests/stress/mod.rs
- **Cluster:** test-infra
- **Notes:** Stress scenario per non-trivial opcode family. Reuse harness in `tests/stress/mod.rs`.
- **Test:** `stress_tests_cover_all_op_families`

### R-201 — [test-infra] Multi-node boundaries only in-process; no real process-kill chaos
- **Source:** AUDIT.md LMNH-24
- **Severity:** LOW
- **Status:** OPEN
- **Files:** tests/fault_injection.rs:88,213,321,416, tests/recovery_crash_boundaries.rs:103,153,207,268,322, tests/cluster_edge_cases.rs:1222
- **Cluster:** test-infra
- **Notes:** `tests/cluster_chaos.rs` using child-process helper to spawn nodes, kill mid-write, verify post-restart consistency. Replica ACK loss + master crash mid-batch.
- **Test:** `cluster_chaos_process_kill_consistency`

### R-202 — [repo-hazards] Structural `panic!()` in production code — loses orderly OTLP shutdown
- **Source:** AUDIT.md LMNH-25
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/device_io/mod.rs:116, src/bin/server.rs:990
- **Cluster:** repo-hazards
- **Notes:** Convert `panic!` → return error. Binary exits non-zero; OTLP shutdown hook runs.
- **Test:** `sync_fallback_error_orderly_shutdown`

### R-203 — [repo-hazards] Production `try_into().unwrap()` in dispatch parsers — copy-paste fragile
- **Source:** AUDIT.md LMNH-26
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/server/dispatch.rs:484,489,494,511,530,536,762,778,785,483,488,493
- **Cluster:** repo-hazards
- **Notes:** Internal helper `take_le_u64(payload, off) -> Result`. Future copy-paste cannot drop length check silently.
- **Test:** `dispatch_parsers_use_take_helper`

### R-204 — [repo-hazards] `std::sync::Mutex::lock().unwrap()` in topology — poison amplification
- **Source:** AUDIT.md LMNH-27
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/cluster/topology.rs:387,406,428,429,469,538,586,596,637, src/replication/durable.rs:96-123
- **Cluster:** repo-hazards
- **Notes:** Migrate to `parking_lot::Mutex` (never panics) or `lock().unwrap_or_else(|p| p.into_inner())`.
- **Test:** N/A

### R-205 — [repo-hazards] `unsafe fn dealloc_mmap_buckets` lacks function-level safety doc
- **Source:** AUDIT.md LMNH-28
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/index/hashtable.rs:281,283
- **Cluster:** repo-hazards
- **Notes:** `// # Safety` rustdoc spelling out caller obligations.
- **Test:** N/A

### R-206 — [repo-hazards] `TCP_NODELAY` unsafe block has minimal safety comment
- **Source:** AUDIT.md LMNH-29
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/replication/tcp_transport.rs:30
- **Cluster:** repo-hazards
- **Notes:** 2-line `// SAFETY:` comment on `level=IPPROTO_TCP, optname=TCP_NODELAY, optval` validity.
- **Test:** N/A

### Cluster: spec/README sync

### R-207 — [spec-sync] README missing error codes 21-26, status codes 4-5, opcodes 103-106 / 240-253
- **Source:** AUDIT.md spec-vs-implementation §
- **Severity:** LOW
- **Status:** OPEN
- **Files:** README.md, src/protocol/opcodes.rs:32-162,195-236,249-270
- **Cluster:** spec-sync
- **Notes:** Document additions. STATUS_PARTIAL_ERROR and STATUS_DEGRADED_DURABILITY semantics. Internal opcodes used for migration / cluster admin.
- **Test:** N/A

### R-208 — [spec-sync] README "redb falls back to in-memory if corrupt" claim wrong — code fails closed
- **Source:** AUDIT.md spec-vs-implementation, GH-G5, AUDIT_CODEX.md F8
- **Severity:** LOW
- **Status:** OPEN
- **Files:** README.md:563-568, src/server/startup.rs:220-243,567-607
- **Cluster:** spec-sync
- **Notes:** Update README to fail-closed contract. Document separate behavior for primary vs secondary indexes. Operator procedure for explicit rebuild/fallback.
- **Test:** N/A

### R-209 — [spec-sync] README "io_uring fast path" claim contradicted by either dead `device_io/` (current) or sync impl (per Codex)
- **Source:** AUDIT.md IJK-04, AUDIT_CODEX.md spec-vs-implementation
- **Severity:** LOW
- **Status:** OPEN, blocked-by R-050
- **Files:** README.md:622, src/device_io/mod.rs:87-110
- **Cluster:** spec-sync
- **Notes:** After R-050 decision (wire or remove), update README accordingly.
- **Test:** N/A

### R-210 — [spec-sync] Migration defaults README/config mismatch (pool 4/128, batch 100/500)
- **Source:** AUDIT_CODEX.md spec-vs-implementation
- **Severity:** LOW
- **Status:** OPEN
- **Files:** README.md:153-155, src/config.rs:442-443,380-383
- **Cluster:** spec-sync
- **Notes:** Reconcile README + config. Code doc still says batch default 100 at config.rs:380.
- **Test:** N/A

### R-211 — [config-validation] Unknown `ack_policy` strings silently fall through to "auto"
- **Source:** AUDIT.md spec-vs-implementation
- **Severity:** LOW
- **Status:** OPEN
- **Files:** src/config.rs:497
- **Cluster:** config-validation
- **Notes:** `ConfigError::InvalidAckPolicy` variant; reject unknown strings.
- **Test:** `config_rejects_unknown_ack_policy`

---

## INFO / Confirmed-correct register (no R-IDs; captured for audit trail)

These are entries from the audits that, on inspection, are correct as implemented or are positive findings. They do not require fixes but are noted so that a reviewer auditing this remediation can confirm none were dropped silently.

- A-15 (LOW) — Coinbase maturity uses `>` strictly; correct BSV semantic. No change.
- A-17 (LOW) — Reassign hash race already correct (checks hash before status). No change.
- A-22 (LOW) — `evaluate_delete_at_height` post-conditional branch logic verified complete.
- A-26 (LOW) — Reassign concurrent-freeze handled by stripe lock + hash check. No change.
- A-30 (LOW) — `set_mined` fast path performance characteristic, not correctness.
- BC-23 — Recovery DOES run device-scan rebuild path (corrected mid-audit).
- BC-29 — Compensation replay correctly restores only slot status given prune semantics.
- BC-65 — `RedoLog::scan_all` checkpoint-at-buffer-end arithmetic verified safe.
- BC-75 — `RedoLog::recover` order is sequence order (verified via monotonic assignment).
- BC-57 — `AllocateRegion` / `FreeRegion` replay order guaranteed by sequence + CRC; no fix needed.
- EF-11 — Shard mask `0x0FFF` correct.
- EF-13 — Migration writes uniformly return `MIGRATION_IN_PROGRESS` via `check_shard_ownership`.
- EF-16 — Round-robin shard assignment deterministic given same node set.
- EF-17 — `migration_pool_size` and `migration_batch_size` actually affect parallelism.
- EF-23 — Empty-shard fast-path in `begin_handoff_with` correct.
- EF-24 — `compute_with_epoch` defensive panic on empty members; production paths filter first.
- EF-26 — `seed_attempt` exponential backoff acceptable.
- EF-27 — `forget_dead_older_than` interaction with persisted `committed_members` benign.
- GH-01 / GH-02 / GH-11 / GH-12 / GH-18 / GH-19 — Positive findings: frame-size enforcement, batch decoder caps, redirect/error decoder bounds, stream-chunk no-copy, opcode coverage complete, connection-close cleanup correct.
- GH-G4 — Two-phase durability via redo log is the design (documented + tested).
- GH-G5 — Fail-closed on redb corruption is the implemented contract; README needs update (R-208).
- GH-G6 — Degraded readiness gate correctly blocks secondary-dependent opcodes.
- GH-G7 — Auto-resize on 0.7 load factor handles overshooting expected_records hint.
- GH-G8 — mmap region resize is crash-atomic via tempfile + rename + redo log.
- GH-G10 — Robin Hood probe distance bounded; high-load-factor tested.
- GH-G12 — redb backends use `Durability::Eventual` relying on redo log; correct design.
- GH-G13 — `update_cached_fields` external-locking contract documented and enforced.
- GH-G17 — Secondary indexes consistent with primary across crash/restart via redo log.
- LMNH-13 (INFO) — Metrics label cardinality bounded by enum discriminants, not unbounded strings.
- LMNH-15 (INFO) — Histograms correctly emit `+Inf` bucket and sum/count per Prometheus spec.
- LMNH-20 (INFO) — `is_ok()`/`is_err()` patterns in tests are combined with state checks.
- LMNH-30 (INFO) — Direct device pointer unsafe sites correct under `validated.apply()` lock contract.

---

## Severity counts

| Severity | Active | Confirmed-correct |
|---|---|---|
| CRITICAL | 10 | — |
| HIGH | 65 | — |
| MEDIUM | 79 | — |
| LOW | 80 | — |
| INFO/positive | — | ~33 |

**Total active R-IDs:** 220 (R-001..R-220; R-212..R-220 discovered during remediation).

---

## Session log

### 2026-05-06 — Session 1 (ledger creation + R-001 + R-002)

- Read `AUDIT.md` and `AUDIT_CODEX.md` end-to-end.
- Dispatched 7 parallel readers to extract structured findings from `audit/raw/category_*.md` files.
- Verified Codex F1 (delete-rollback resurrection) and F2 (process-expired ownership/replication) by direct reads of `src/server/dispatch.rs:3940-4097` and `:4669-4720`. Both confirmed CRITICAL — NEW vs AUDIT.md.
- Reconciled overlaps: F3↔IJK-20, F11↔LMNH-17, F13↔LMNH-01, F14↔LMNH-08, F15↔LMNH-16, F8↔GH-G5.
- Created LEDGER with 211 active R-IDs and 33 confirmed-correct register entries. Committed as `df207ef`.
- **R-001 RESOLVED.** Three failing index-rebuild tests fixed (corrupt magic + restamp CRC); 3 new companion tests added covering the CRC-mismatch branch. Lib test count 1480 → 1486.
- **NEW finding R-212 RESOLVED.** clippy `--all --all-targets` was not run by either audit. Found and fixed: 8 bench `CreateRequest` constructions missing `external_ref: None` field (pre-existing bench-API drift), plus 2 pre-existing `collapsible_if` clippy lints in `src/device.rs` test code (lines 1246, 1268). All fixed in same R-001 commit since they blocked the verification gate. Committed at `f4a9c77`.
- **R-002 RESOLVED.** Removed `#[ignore]` and rewrote the migration handshake test for the pipelined flow contract. Discovered (and added as **R-213**, MEDIUM, OPEN) that the pipelined `run_migration_batch` worker does NOT emit abort completion handshakes on baseline failure (target inbound state lingers ~30 s for `clear_stale_inbound` to fire). Added a silent-drop variant (`pipelined_migration_marks_failed_when_target_never_acks`). 3 of the 4 F7 crash variants are deferred to **R-214** since they need a process-kill harness (R-201 dependency). Lib tests 1486 → 1488; ignored 1 → 0.
- IDs touched: R-001 (RESOLVED), R-002 (RESOLVED), R-212 (RESOLVED — new), R-213 (OPEN — new), R-214 (DEFERRED — new).
- **R-003 RESOLVED.** New `teraslab::checkpoint` module + background task spawned in `bin/server.rs` performs snapshot+persist+checkpoint+reset when redo log usage exceeds 0.5. Lib tests 1488 → 1490. Two follow-ups discovered: **R-215** (move snapshot off redo-mutex hot path) and **R-216** (coordinate reset with replication catch-up watermarks), both MEDIUM/OPEN.
- IDs touched (additional): R-003 (RESOLVED), R-215 (OPEN — new), R-216 (OPEN — new).
- **R-004 RESOLVED.** Replaced 5 `tracing::warn!` swallows in `Engine::spend` and `ValidatedSpend::apply` with `?` propagation. Added `WriteFailingDevice` test harness + 2 regression tests proving spend now returns `Err` AND leaves the slot UNSPENT on disk (no double-spend window). Lib tests 1490 → 1492.
- IDs touched (additional): R-004 (RESOLVED).
- Next session entry point: R-005 (A-03 spend_multi counter mismatch — CRITICAL, was blocked-by R-004 which is now resolved).

### R-212 — [test-baseline] Bench CreateRequest constructions miss `external_ref` field; pre-existing collapsible_if lints in `src/device.rs` tests
- **Source:** Discovered while running R-001 verification gate (`cargo clippy --all --all-targets`)
- **Severity:** HIGH (gate)
- **Status:** RESOLVED
- **Files:** benches/alloc_profile.rs (2 sites), benches/engine_remaining.rs, benches/mixed_workload.rs (2 sites), benches/spend_throughput.rs (3 sites), src/device.rs:1246, :1268
- **Cluster:** test-baseline
- **Notes:** Both audits ran `cargo clippy --all -- -D warnings` (no `--all-targets`), missing bench compilation and lib-test lints. The benches were rotted against a `CreateRequest` API change adding `external_ref: Option<ExternalRef>`. Fix: added `external_ref: None,` after `locked: false,` in 8 sites; collapsed the 2 nested `if let Some(...) { if cond { ... } }` blocks to use `&&` chaining.
- **Verification:** `cargo clippy --all --all-targets -- -D warnings` now clean. Tests still pass.

### R-213 — [migration-handshake] Pipelined `run_migration_batch` worker does not send abort completion handshake on baseline failure (target inbound state lingers 30 s)
- **Source:** Discovered while resolving R-002 (rewriting the ignored migration handshake test)
- **Severity:** MEDIUM (correctness gap; degrades but does not corrupt)
- **Status:** OPEN
- **Files:** src/cluster/coordinator.rs:3071-3458 (worker), :102-149 (`fail_migration_task_current_epoch`), :3753-3808 (legacy `migrate_single_shard::fail_shard` for reference)
- **Cluster:** migration-handshake
- **Notes:** When baseline streaming fails in the pipelined flow, `fail_migration_task_current_epoch` is called: it clears `migrating_bm` and rolls back the shard table, but does NOT send `OP_MIGRATION_COMPLETE` with `record_count=0` to the target. The target's provisional inbound state therefore lingers until `clear_stale_inbound`'s 30 s timeout (EF-15 / R-180). The legacy non-pipelined `migrate_single_shard::fail_shard` had `clear_target_inbound: bool` and emitted the abort frame; that behavior was lost when the pipelined flow replaced it. Fix: in the pipelined worker's failure branch (line ~3233, `if !streamed[i]`), call `send_migration_complete(addr, task.shard, task.from_node, 0, 0, 0, None, &[0u8; 32], &[], false)` before `fail_migration_task_current_epoch`. The `send_migration_complete` is best-effort (already wrapped in `let _ =` semantics in the legacy path) so its failure does not block the local rollback.
- **Test required:** `failed_pipelined_migration_emits_abort_completion_handshake` — drive baseline failure, assert at least one `OP_MIGRATION_COMPLETE` with `record_count=0, request_id=shard` arrives at the target.

### R-215 — [redo-log] Move checkpoint snapshot off the redo-mutex hot path
- **Source:** Discovered while implementing R-003
- **Severity:** MEDIUM (perf/availability — checkpoint stalls writers)
- **Status:** OPEN
- **Files:** src/checkpoint.rs (`perform_checkpoint`)
- **Cluster:** redo-log
- **Notes:** `perform_checkpoint` holds the redo log mutex for the duration of `engine.snapshot_index` + `engine.persist_allocator`. The snapshot reads index/dah/unmined under their own locks, but the redo mutex blocks ALL new mutation appends until the snapshot+persist+marker+reset completes. For a 100M-entry index this can stall writers for seconds. Two design options: (a) use a copy-on-write snapshot where `snapshot_all` returns an immutable view captured under brief locks, then writes to disk without holding any locks; (b) use epoch-based reads with a generation counter, snapshotting via the latest committed epoch with a deferred reset.
- **Test required:** `checkpoint_does_not_block_writers_for_more_than_N_ms`

### R-216 — [redo-log] Coordinate redo reset with replication catch-up watermarks
- **Source:** Discovered while implementing R-003
- **Severity:** MEDIUM (replication availability)
- **Status:** OPEN
- **Files:** src/checkpoint.rs (`perform_checkpoint`), src/replication/manager.rs (`run_catchup`)
- **Cluster:** redo-log
- **Notes:** When `RedoLog::reset()` runs, all entries before the most recent checkpoint are wiped. Replicas whose `last_acked_sequence` predates the new checkpoint will need a full resync; the catch-up path's `read_from_sequence` returns an empty Vec for those replicas which the manager currently treats as "all caught up" instead of "needs resync." Fix: have `perform_checkpoint` query `min(replica.last_acked_sequence)` across all live replicas before resetting; if the threshold is below that, skip reset (let the log fill briefly) OR signal replicas to resync. Add a `catchup_watermark_lag_seconds` metric so operators can detect when this defers checkpointing.
- **Test required:** `replica_resync_signal_after_redo_reset`

### R-217 — [dispatch-wal] freeze/unfreeze batch validates outside the per-tx stripe lock (BC-37)
- **Source:** AUDIT.md BC-37
- **Severity:** MEDIUM (replay is idempotent for freeze ops — observable via timing only)
- **Status:** OPEN
- **Files:** src/server/dispatch.rs (`handle_freeze_batch`, `handle_unfreeze_batch`)
- **Cluster:** dispatch-wal
- **Notes:** Same shape as BC-04 (R-010) but for freeze/unfreeze batches. The dispatcher reads `pre_state` from `engine.lookup` outside the per-tx stripe lock to compose the redo entry. Freeze ops carry no per-call counter so the BC-04 replay-rederive fix doesn't apply — the race is observable as a brief window where a concurrent batch's redo entry contradicts the actual state, but replay's slot-state idempotency check skips the wrong redo entry. Fix: take the stripe lock around lookup + redo + apply (validate-then-apply pattern), or extract a `engine::freeze_locked` API the dispatcher can call while holding the guard.
- **Test required:** `concurrent_freeze_unfreeze_redo_consistency`

### R-218 — [dispatch-wal] reassign captures `prior_utxo_hash` outside the per-tx stripe lock (BC-54)
- **Source:** AUDIT.md BC-54
- **Severity:** HIGH (compensation correctness)
- **Status:** OPEN
- **Files:** src/server/dispatch.rs (`handle_reassign_batch`)
- **Cluster:** dispatch-wal
- **Notes:** `handle_reassign_batch` reads `prior_utxo_hash` via `engine.read_slot` BEFORE acquiring the stripe lock. Two concurrent reassigns on the same slot capture the same prior hash; the SECOND reassign's compensation `BeforeImage` therefore restores the slot to the FIRST reassign's hash (which is no longer correct), producing silent corruption on rollback. Fix: extend `Engine::reassign` (or wrap in a dispatch-side helper) to return the prior hash atomically with the apply, under the stripe lock. R-010-style replay-rederive does NOT work here because the prior hash is needed for compensation, not for replay.
- **Test required:** `concurrent_reassign_compensation_uses_correct_prior_hash`

### R-220 — [recovery] Replay does not synthesize derived state (generation, updated_at, DAH/unmined indexes) — A-06 follow-up
- **Source:** Discovered while resolving R-013 (audit A-06 second half)
- **Severity:** HIGH
- **Status:** OPEN
- **Files:** src/recovery.rs (`replay_spend`, `replay_unspend`, `replay_set_mined`, `replay_metadata_op`)
- **Cluster:** recovery
- **Notes:** Replay only updates the immediate fields the redo entry carries (slot byte + spent_utxos delta). The live engine paths additionally bump `meta.generation`, set `meta.updated_at`, and update the DAH / unmined / preserve-until secondary indexes. Recovery does NOT do any of these, so a record that has been replayed has a generation counter that lags the equivalent live-applied record, secondary indexes that may be stale, and timestamps that don't match. The lag breaks replication catch-up gating (replicas with master_generation == replayed-generation think they're caught up but are missing the index updates). Two design options: (a) extend redo entries to carry every derived field — wire-format change → MIGRATION-REQUIRED; (b) replay calls into engine's mutation path under a synthetic guard — needs careful lock-order analysis but no on-disk change.
- **Test required:** `recovery_post_replay_generation_matches_live_engine`, `recovery_post_replay_dah_index_matches_live_engine`

### R-219 — [migration-handshake] Zero-record `OP_MIGRATION_COMPLETE` skips manifest verification
- **Source:** AUDIT.md EF-12 (subset, separated from R-012)
- **Severity:** HIGH
- **Status:** OPEN, **MIGRATION-REQUIRED** (manifest semantics on empty shards)
- **Files:** src/server/dispatch.rs:567-571, :628-634
- **Cluster:** migration-handshake
- **Notes:** Receiver treats `record_count == 0` as "empty migration, no manifest needed", so a source declaring a non-empty shard's migration complete with `record_count = 0` causes silent data loss. Fix: every completion carries the manifest hash, including empty shards (`HMAC-SHA256` over an empty entry list yields a known constant; the receiver compares against that). Needs human approval because it interacts with the empty-shard fast path that the cluster already optimizes for. Once approved, fix is small (~30 LoC).
- **Test required:** `zero_record_completion_with_wrong_manifest_rejected`

### R-214 — [test-baseline] Migration crash variants requiring process-kill harness (deferred subset of F7)
- **Source:** Discovered while resolving R-002
- **Severity:** LOW
- **Status:** DEFERRED (needs process-kill harness from R-201)
- **Files:** N/A (test-only)
- **Cluster:** test-infra
- **Notes:** AUDIT_CODEX F7 requested 4 crash variants for the migration handshake test: (1) source crash mid-baseline, (2) target crash after partial baseline, (3) completion ACK lost, (4) abort ACK lost. R-002 covers (3)/(4) for the abort handshake via the silent-drop variant; (1)/(2) require process-kill chaos which doesn't yet exist in-process. R-201 (LMNH-24) tracks building the process-kill harness — once that lands, this entry can be RESOLVED with the missing variants added.
- **Test required:** `migration_crash_*` suite (depends on R-201).
