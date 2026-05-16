# G3 fix log — indexes

Worktree: `agent-a6229594befd0ba42`
Branch: `worktree-agent-a6229594befd0ba42`
Baseline: `aeed289 merge(G8 partial): worktree progress — F-G8-001..005 + 4 commits`
Owner: `src/index/*.rs` (G3 per ownership matrix).

---

### F-G3-001 — FIXED
- Commit: `b1b4dfc fix(index): F-G3-001 + F-G3-007 single-row unregister/lookup propagate redb errors`
- Files changed: `src/index/redb_primary.rs`
- Test added: `index::redb_primary::tests::unregister_propagates_commit_failure`
- Notes: combined with F-G3-007 because the WIP baseline already changed `unregister`'s signature to `Result<Option<…>>` but left ~30 in-file test callsites on the old `Option<…>` shape — the library tests did not compile. Routed `unregister` through the existing `check_fail_injection()` hook so the test-only `arm_fail_next_write` actually triggers the single-row error path.

### F-G3-002 — FIXED
- Commit: `ef13eb7 fix(index): F-G3-002 RedbDah/RedbUnmined::clear propagate redb errors`
- Files changed: `src/index/redb_dah.rs`, `src/index/redb_unmined.rs`, `src/index/secondary_backend.rs`, `src/recovery.rs`
- Test added: `index::redb_dah::tests::clear_returns_ok_and_zeroes_count_only_on_success`, `index::redb_unmined::tests::clear_returns_ok_and_zeroes_count_only_on_success`
- Notes: lifted both `clear()` methods to `Result<(), IndexError>`. `DahBackend::clear` and `UnminedBackend::clear` propagate. `recovery::reconcile_secondary_indexes_from_metadata` (G4-owned but a caller) propagates the error as `RecoveryError::Index`. Cached `count` is now reset only after commit succeeds.

### F-G3-003 — FIXED
- Commit: `ae927a4 fix(index): F-G3-003 restrict insert_batch to pub(crate) with migration-only docs`
- Files changed: `src/index/redb_dah.rs`, `src/index/redb_unmined.rs`
- Test: covered by existing migration tests in `src/index/migration.rs` (the only valid caller).
- Notes: restricted `RedbDahIndex::insert_batch` and `RedbUnminedIndex::insert_batch` to `pub(crate)` and added "MIGRATION ONLY — does NOT use two-phase durability" doc banner. Hot-path callers now see a compile-time barrier.

### F-G3-004 — FIXED
- Commit: `9ff2c69 fix(index): F-G3-004 document UnminedBackend in-memory recovery contract + pin test`
- Files changed: `src/index/secondary_backend.rs`
- Test added: `index::secondary_backend::tests::unmined_in_memory_insert_no_redo_dependency`
- Notes: rewrote doc comments on `UnminedBackend::insert` / `::remove` to name `recovery::reconcile_secondary_indexes_from_metadata` as the load-bearing reconcile path. The pin test asserts insert/remove succeed with `None` redo log and that the surviving entry is queryable.

### F-G3-005 — FIXED
- Commit: `374dea4 fix(index): F-G3-005 bound HashTable::remove backward-shift to one table pass`
- Files changed: `src/index/hashtable.rs`
- Test added: `index::hashtable::tests::remove_backward_shift_terminates_under_corruption`
- Notes: added step counter capped at `self.capacity`. Pre-fix this regression test would never return. Emits `tracing::error!` if the cap fires.

### F-G3-006 — FIXED
- Commit: `96d6191 fix(index): F-G3-006 document HashTable Send/Sync safety contract`
- Files changed: `src/index/hashtable.rs`
- Test: documentation-only change; no test needed.
- Notes: replaced one-line Safety comment with multi-line `# Safety` block documenting the engine's `RwLock` contract. Also expanded `bucket()` / `bucket_mut()` safety comments to reference the module-level rule.

### F-G3-007 — FIXED
- Commit: `b1b4dfc` (combined with F-G3-001)
- Files changed: `src/index/redb_primary.rs`
- Test added: `index::redb_primary::tests::lookup_propagates_read_failure`
- Notes: added `fail_next_read: Cell<bool>` + `arm_fail_next_read()` + `check_fail_injection_read()` so the lookup propagation can be exercised through `&self`.

### F-G3-008 — FIXED
- Commit: `8bc0966 fix(index): F-G3-008 range_query emits tracing::error on every redb error path`
- Files changed: `src/index/redb_dah.rs`, `src/index/redb_unmined.rs`
- Test: existing `range_query_*` tests pin the happy path; the error log signal is observable via `tracing-test` but is not exercised here because the redb API does not expose a clean fault injection point. The signature is preserved (`Vec<TxKey>`) to avoid churning every caller; the operator-visible log line is the load-bearing change.
- Notes: wider `Result<…>` migration is deferred to the orchestrator's follow-up list — would touch `ops::*` and `recovery::*`.

### F-G3-009 — NOT-APPLICABLE
- Commit: `bbaf1d6 docs(audit): F-G3-009 mark stale rebuild_* failing-test entries as RESOLVED`
- Files changed: `AUDIT.md`
- Notes: AUDIT.md (root, 2026-05-06) listed three rebuild tests as "failing in main". The cited fix landed before the audit was committed (the `corrupt_magic_and_restamp_crc` helper). Marked the entries RESOLVED.

### F-G3-010 — FIXED
- Commit: `42e3ddb fix(index): F-G3-010 cap iter_collected prealloc at 1M entries`
- Files changed: `src/index/redb_primary.rs`
- Test: covered by existing `iter_collected` test (signature/behaviour unchanged).
- Notes: added `ITER_COLLECTED_PREALLOC_CAP = 1_000_000` to bound the up-front `Vec::with_capacity` allocation. Vector still grows on demand past the cap.

### F-G3-011 — FIXED
- Commit: `1f8856a fix(index): F-G3-011 stream serialize_secondary directly into buf`
- Files changed: `src/index/mod.rs`
- Test: covered by the existing 14 snapshot tests in `src/index/mod.rs::tests` (all still pass; byte layout unchanged).
- Notes: streamed iterator entries directly into `buf` with a count-placeholder patched after the loop. Eliminates the intermediate `Vec<(u32, TxKey)>`.

### F-G3-012 — FIXED
- Commit: `6ae95c6 fix(index): F-G3-012 verify CRC in locate_unmined_section to reject forged magic`
- Files changed: `src/index/mod.rs`
- Test added: `index::tests::locate_unmined_section_skips_forged_magic_when_real_follows`
- Notes: inlined the CRC check so a planted `UNMI` magic burst with a garbage CRC gets stepped over. Also reject candidates whose declared `count` exceeds `MAX_SNAPSHOT_COUNT`.

### F-G3-013 — FIXED
- Commit: `8d2f491 fix(index): F-G3-013 read old_height inside the write transaction (TOCTOU)`
- Files changed: `src/index/redb_dah.rs`, `src/index/redb_unmined.rs`
- Test: existing `insert` / `remove` / `redo_flush_failure_blocks_redb_commit` tests cover the new ordering.
- Notes: opens the write txn first, reads existing height under the same lock, then builds the redo entry. Also fixed a pre-existing test bug in `redo_flush_failure_blocks_redb_commit` (both DAH and Unmined) where `MemoryDevice::new(4096, 4096)` + `RedoLog::open(dev, 0, 256)` panicked with `LogRegionTooSmall { required_for_header: 8192 }`; resized to a 64 KiB device + 16 KiB log.

### F-G3-014 — FIXED
- Commit: `b02f211 fix(index): F-G3-014 cross-check record_size against utxo_count during rebuild`
- Files changed: `src/index/mod.rs`, `src/index/backend.rs`
- Test added: `index::tests::rebuild_fails_on_record_size_inconsistent_with_utxo_count`
- Notes: added `record_size == TxMetadata::record_size_for(utxo_count)` check to all four rebuild paths (`Index::rebuild`, `Index::rebuild_secondary`, `PrimaryBackend::rebuild_redb`, `PrimaryBackend::rebuild_file_backed`).

### F-G3-015 — FIXED
- Commit: `0cfc5be fix(index): F-G3-015 lift RedbPrimary concurrency contract to struct-level doc`
- Files changed: `src/index/redb_primary.rs`
- Test: documentation-only.
- Notes: moved the "MUST hold an exclusive lock" contract to the struct doc-comment so it covers every `&mut self` method, and called out the `self.count` cache hazard explicitly.

### F-G3-016 — FIXED
- Commit: `f9e8062 fix(index): F-G3-016 sidecar clean-shutdown sentinel for file-backed HashTable`
- Files changed: `src/index/hashtable.rs`
- Test added: `index::hashtable::tests::open_file_backed_writes_and_consumes_shutdown_sentinel`, `index::hashtable::tests::open_file_backed_succeeds_when_sentinel_missing`
- Notes: lighter-weight than the full magic+version+CRC header the finding recommends. Drop writes `<path>.shutdown_clean` after `msync`; open consumes it. Missing sentinel → `tracing::warn!` with operator advice to consider `PrimaryBackend::rebuild_file_backed`. The data plane is unchanged (redo log remains the canonical safety net); the fix is observational.

### F-G3-017 — FIXED
- Commit: `7f8b3bd fix(index): F-G3-017 stop recomputing max_probe on every remove`
- Files changed: `src/index/hashtable.rs`
- Test: existing `max_probe_distance_recomputed_after_remove` still passes (the public accessor now recomputes on read).
- Notes: dropped the O(capacity) recompute from the hot delete path. `max_probe_distance()` recomputes lazily on read. Stale-but-larger cache is safe because `get_entry` compares per-bucket `probe_distance` directly.

### F-G3-018 — NOT-APPLICABLE
- Notes: positive verification only ("Looks fine — `.max(16)` enforces the floor regardless of `initial_capacity`. Just noting for the coverage ledger."). No code change required.

### F-G3-019 — FIXED
- Commit: `e6ea108 fix(index): F-G3-019 debug_assert by_height/by_txid invariant on no-op insert`
- Files changed: `src/index/dah_index.rs`
- Test: covered by existing `dah_index` test suite (debug_assert fires loudly if invariant breaks in any test build).
- Notes: added `debug_assert!` that the `by_height` vec contains the key on the no-op short-circuit. Release builds are unchanged.

### F-G3-020 — NOT-APPLICABLE
- Notes: positive verification of `secondary_backend.rs` enum dispatch + `with_both_*_backends` test parameterization. The sharp edge (in-memory unmined drops redo entry) is captured separately at F-G3-004.

---

## Cross-cutting notes

- The WIP baseline (`aeed289`) contained a partial signature refactor: `RedbPrimary::unregister`, `RedbPrimary::lookup`, and the `PrimaryBackend::*_checked` siblings were already in place, but ~30 in-file test callsites used the pre-refactor `Option<…>` shape and the library tests did not compile. F-G3-001 fixes the callsites + adds propagation tests.
- `src/index/backend.rs` retains `lookup` and `unregister` wrappers that swallow redb errors and log via `tracing::error!`. The new `lookup_checked` / `unregister_checked` siblings propagate. Migrating the dozens of callers in `recovery.rs` / `ops::*` is captured as a follow-up; the cross-cutting refactor is out of G3 scope.
- `src/index/redb_dah.rs::redo_flush_failure_blocks_redb_commit` and `src/index/redb_unmined.rs::redo_flush_failure_blocks_redb_commit` had pre-existing test setup bugs (`RedoLog::open(dev, 0, 256)` against 4 KiB devices); fixed alongside F-G3-013 per the project-level instruction "always fix pre-existing bugs even if unrelated to current changes".
- `src/index/hashtable.rs::resize_journals_begin_and_commit` is a pre-existing failing test (`expected exactly Begin + Commit in redo log, got 1`). Confirmed it fails on the unmodified `aeed289` baseline too. The bug is in `src/redo.rs::recover` (recovery sees only the first `HashtableResize*` entry), which is G4-owned. Captured as NEEDS-ORCHESTRATOR.
