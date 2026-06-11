# Audit G — Index backends

Scope: `src/index/` (hashtable, mod, backend, secondary_backend, redb_primary, redb_dah, redb_unmined, migration), `src/recovery.rs`, `src/checkpoint.rs`, startup wiring in `src/server/startup.rs` + `src/bin/server.rs`. Tests: `tests/secondary_two_phase_durability.rs`, `tests/integration.rs`, `tests/fault_injection.rs`, `tests/g1_review.rs`, in-crate test modules.

Note on the audit brief: the brief claims the README documents a redb corruption fallback of "delete, recreate, fall back to memory." It does not. README.md:644-646 explicitly documents the opposite — fail-closed, file preserved, no automatic in-memory fallback for the primary — and the code matches the README. Findings below judge the code against the README as written.

---

### [CRITICAL] redb backend: checkpoint fences/compacts the redo log without making redb commits durable — power-loss silently loses index mutations

**Location:** `src/index/redb_primary.rs:92-96` (`Durability::Eventual`, same in `redb_dah.rs:42-46`, `redb_unmined.rs`), `src/checkpoint.rs:363-394` (`perform_checkpoint_with_reset_guard`), `src/index/backend.rs:445-446` (`snapshot_all` is a no-op for `OnDisk`), `src/redo.rs:1971-2005` (`recover()` skips entries ≤ `RecoveryProgress.through_sequence`), `src/bin/server.rs:1193-1236` (checkpoint task spawned unconditionally for every backend).

**What's wrong:** Every redb write transaction uses `Durability::Eventual` — no fsync per commit. The stated justification (redb_primary.rs:88-91) is "TeraSlab's redo log (WAL) provides crash recovery." That argument is only sound while the redo entries covering un-fsynced redb commits remain replayable. The background checkpoint breaks it:

1. `perform_checkpoint_with_reset_guard` calls `engine.snapshot_index()` → `PrimaryBackend::snapshot_all`, which for `OnDisk` is `Ok(())` — a no-op, "redb is already durable" (backend.rs:445).
2. It then writes a `RecoveryProgress` fence through `snapshot_fence_sequence` and (guard permitting) `compact_prefix_through` discards the prefix.
3. `RedoLog::recover()` filters out all entries with `sequence <= through_sequence` — even un-compacted ones (pinned by the test at checkpoint.rs:555-558).

Nothing flushes redb (no `Durability::Immediate` commit, no flush call — `grep Durability` finds only `Eventual`) before the fence is written. On power loss / host crash after a checkpoint, redb rolls back to its last internally-fsynced commit; the redo entries that would replay the gap are now unrecoverable (fenced and/or compacted). `load_primary_index_redb` then opens the stale redb file successfully — no error, no rebuild trigger.

**Why it matters:** Registered transactions vanish from the primary index (spends return `TX_NOT_FOUND` for records that exist on the device — UTXOs silently lost); deletes resurrect as stale entries pointing at freed/reallocated regions (the engine's `meta.tx_id == key.txid` re-check at ops/engine.rs:800 converts those into loud errors, but absences stay silent); `dah_or_preserve`/`unmined_since`/`generation` cached fields lag, and `reconcile_secondary_indexes_from_metadata` then faithfully rebuilds both secondaries from the stale primary, so all three redb files are consistently wrong together. README.md:639/644 sells this backend as "crash-durable by default — no snapshot needed," which is false under power loss. The window is up to a whole checkpoint period (default trigger 75% of a 64 MiB log ≈ hundreds of thousands of mutations).

**Reproduction:** (1) Code-level: configure `backend = "redb"`, run mutations, call `perform_checkpoint`, then simulate power loss by copying the three redb files from before redb's internal fsync (or run under a write-blocking harness like `dm-flakey`/CrashMonkey); restart and observe the registered txid absent from the primary while `read_metadata` at its device offset succeeds. (2) Cheaper unit demonstration: assert that `perform_checkpoint` on an engine whose primary is `PrimaryBackend::OnDisk` performs zero fsyncs on the redb path between `begin` and `mark_recovery_progress` (strace/fault-injection sync point).

**Suggested fix:** Before `mark_recovery_progress` in `perform_checkpoint_with_reset_guard`, force durability on all three redb databases when the OnDisk backend is active — e.g. add a `PrimaryBackend::make_durable()` that runs an empty `Durability::Immediate` commit (and the same for `DahBackend`/`UnminedBackend`), or have `snapshot_all` for `OnDisk` perform that flush instead of returning `Ok(())`. Alternatively keep per-op `Eventual` but tie the fence sequence to the last redb-fsynced sequence. Add a fault-injection test that crashes between checkpoint fence and redb fsync.

---

### [HIGH] File-backed backend: auto-resize writes the clean-shutdown sentinel mid-run, silently disabling torn-write detection for the rest of uptime

**Location:** `src/index/hashtable.rs:1420-1440` (Drop writes sentinel for any `FileBacked` table), `src/index/hashtable.rs:1289-1291` (`resize`: `*self = new_table` drops the old table), `src/ops/engine.rs:724-727` (`*write_guard = resized` drops the old backend), `src/index/hashtable.rs:680-706` (sentinel consumed only at open).

**What's wrong:** The G-01/F-G3-016 design is: sentinel removed at `open_file_backed`, recreated only by `Drop` on clean shutdown; a crash leaves no sentinel, so the next open fails closed (`UncleanShutdown`) and the index is rebuilt from a device scan. But `Drop` runs whenever a `FileBacked` `HashTable` value is dropped — including the *old* table being replaced during a load-factor resize (`*self = new_table` in `HashTable::resize`, and `*write_guard = resized` in the engine's non-blocking resize). After `build_resized` renames the tmp file over `old_path` and updates the new table's path, the old table's Drop executes `File::create(sentinel_path_for(old_path))` — creating `<path>.shutdown_clean` while the server is running and continuing to mutate the (new) mmap.

From the first auto-resize until the next clean shutdown, the sentinel sits on disk. If the process crashes in that window, the next `open_file_backed` finds the sentinel, concludes "clean shutdown," and maps the bucket bytes as-is — exactly the torn-write acceptance the sentinel exists to prevent. Note `tests/fault_injection.rs:313` (`kill_between_rename_and_dir_fsync_recovers_hashtable`) reopens successfully after a simulated crash *because* of this very artifact (the in-process `drop(ht)` wrote the sentinel), so the existing test inadvertently depends on the bug.

**Why it matters:** A growing UTXO store resizes regularly, so in practice the crash-detection sentinel is disabled for most of the process lifetime. A torn 64-byte bucket write accepted as clean can break a probe chain (existing entries become unreachable → UTXO silently "lost") or fabricate an occupied bucket (count drift, ghost entries). The engine's tx_id re-check catches wrong-pointer reads but not absences.

**Reproduction:** `let mut ht = HashTable::open_file_backed(&p, 16)` (fresh — no sentinel); insert until `resize` fires (or call `ht.resize(64)` directly); assert `p.with_extension("")…shutdown_clean` — i.e. `sentinel_path_for(&p)` — **exists while the table is still live**. Then `std::mem::forget(ht)` (simulate crash) and confirm `open_file_backed(&p, …)` succeeds instead of returning `UncleanShutdown`.

**Suggested fix:** In `Drop`, only write the sentinel if this table is the *current* owner of the path — simplest: after the rename in `build_resized`, clear the old table's `Backing::FileBacked` path (e.g. set a `defunct: bool` or swap backing to `Anonymous` semantics for Drop purposes) so the displaced table never writes a sentinel. Alternatively, remove the sentinel immediately after the swap completes. Add a regression test asserting no sentinel exists after a resize while the table is live.

---

### [HIGH] File-backed backend: existing index file with invalid size is silently wiped and opened as an empty index — bypasses the device-scan rebuild

**Location:** `src/index/hashtable.rs:661-678` (invalid size → "treat as new"), `src/index/hashtable.rs:701-706` (fresh-creation branch skips the sentinel check and deletes the stale sentinel), `src/index/hashtable.rs:456-459` (`set_len` truncates the file), `src/index/backend.rs:73-83` (`restore_file_backed` only checks `path.exists()`), `src/server/startup.rs:325-347` (`load_primary_index_file_backed` falls back to rebuild only when restore returns `Err`).

**What's wrong:** `open_file_backed` validates that an existing file's length is a power-of-two multiple of `BUCKET_SIZE`. If it isn't (truncated copy, disk-full partial write, any corruption that changed the length), the code takes the `else` branch: `(initial_capacity…, false)` — the file is **treated as freshly created**. Consequences: the unclean-shutdown sentinel check is skipped, the stale sentinel is deleted, `set_len` resizes the damaged file (destroying the evidence), and every bucket is overwritten with the empty sentinel. `restore_file_backed` therefore returns `Ok` with a 0-entry index, so `load_primary_index_file_backed` never reaches the device-scan rebuild that would have recovered every record. Contrast with the redb path, where a garbage file makes open fail and the fail-closed contract kicks in.

**Why it matters:** The server boots cleanly with an empty primary index over a fully populated device (only a `entries=0` log line hints at it). Every lookup returns `TX_NOT_FOUND`; subsequent creates can double-allocate logical state. This is precisely the failure the fail-closed startup policy (Gap #5) was built to prevent, and the in-file size check currently converts "detected corruption" into "silent data loss."

**Reproduction:** Create a file-backed index, insert entries, drop cleanly (sentinel written). `truncate -s -100 primary.idx` (any non-power-of-two-bucket length). Call `PrimaryBackend::restore_file_backed(&path, 100)` — observe `Ok` with `len() == 0` instead of an error; `load_primary_index_file_backed` returns the empty index without scanning the device.

**Suggested fix:** For an existing file with invalid size, fail closed: return a new `HashTableError::InvalidFileSize { path, len }` (do not `set_len`, do not delete the sentinel). `load_primary_index_file_backed` already falls back to `rebuild_file_backed` on restore error, which deletes and rebuilds the file from the device — the correct recovery. Add tests for truncated and extended files.

---

### [HIGH] Engine exclusively uses the lossy `lookup`/`unregister` shims — redb I/O errors are collapsed into "key absent" on every hot path

**Location:** `src/index/backend.rs:99-111` (`lookup` shim), `backend.rs:188-200` (`unregister` shim); call sites: `src/ops/engine.rs:742` (`guard.unregister(key)`), `engine.rs:960` and ~40 further `self.index.read().lookup(…)` sites (1061, 1308, 1496, 1635, 1990, 2067, 2603, 3963, 4041, …). `grep -rn lookup_checked\|unregister_checked` outside `src/index/` returns **zero** call sites.

**What's wrong:** `RedbPrimary::lookup`/`unregister` were made fallible (F-G3-007) precisely because a transient redb failure is indistinguishable from a key miss. The `PrimaryBackend` shims exist "so existing callers compile while the migration is performed" — but the migration never happened: every engine path (spend validation, create-duplicate check at engine.rs:2067, delete at 742, set_mined, freeze, conflict-child resolution) goes through the lossy shim. With the redb backend, a `begin_read`/`open_table`/`get` failure (transient I/O error, fd exhaustion, cache pressure) returns `None`: a spend of an existing UTXO reports `TX_NOT_FOUND`; a create of an existing txid passes the "not present" check and writes a duplicate record; `unregister` collapse means delete paths skip downstream cleanup (blob deletion, secondary removal, shard counts) while the row remains in redb. Only a `tracing::error!` records the truth.

**Why it matters:** Wrong client-visible answers under recoverable error conditions, redb backend only. The code's own doc comments label this a known migration debt ("this can mask a real entry as missing").

**Reproduction:** Unit-level: build an engine over `PrimaryBackend::OnDisk`, call `RedbPrimary::arm_fail_next_read()` (test-only hook already exists), then issue a spend for a registered key — observe `SpendError::TxNotFound` instead of a storage error. There is currently no engine-level test doing this.

**Suggested fix:** Migrate engine call sites to `lookup_checked`/`unregister_checked` and map `Err` to `ERR_INTERNAL`/storage error (never to not-found). Then delete the shims so the compiler enforces it.

---

### [MEDIUM] `tests/secondary_two_phase_durability.rs` never restarts anything — the "crash" keeps the live primary object, so the startup pipeline is untested

**Location:** `tests/secondary_two_phase_durability.rs:60-131` (and the other four tests in the file).

**What's wrong:** The tests simulate "crash after redo fsync, before redb commit" by simply not committing, then call `recover_all` — legitimate for the redo-vs-redb ordering window. But: (a) the **same in-memory `primary` object** built before the "crash" is passed into recovery — a real crash destroys it; the snapshot-restore/device-rebuild path that would actually reconstruct the primary is bypassed; (b) the data device is created *after* the crash with metadata hand-written at the expected offsets — there is no end-to-end statement that the pre-crash pipeline produced that state; (c) no process kill, ever. The checklist requirement "mutate primary, kill process, restart, verify secondaries" is only approximated: `tests/integration.rs::backend_modes_secondary_indexes_survive_reopen` (lines ~486-560) does real engine mutations + reopen across all three backends, but that is a **clean** restart (no crash); `tests/fault_injection.rs` panics at `BeforeSecondaryRedbCommit` (line ~444) but again recovers within the same process. Migration crash-consistency is covered only at the sentinel-refusal level (`startup_refuses_when_import_sentinel_present`), not with a partially-committed import.

**Why it matters:** The composed startup path (load primary from snapshot/redb/rebuild → recover → reconcile secondaries) is the thing that actually runs after a crash; none of these tests execute it as a whole, so regressions in the wiring (e.g. finding #1, #3) are invisible to the suite.

**Reproduction:** N/A (coverage gap). Suggested experiment: a `std::process::Command`-based test that runs a child binary doing engine mutations, `kill -9`s it at a fault-injection sync point, then boots the real startup path (`load_primary_index_*` + `recover_all_with_allocator`) and asserts primary+DAH+unmined agree with device metadata.

**Suggested fix:** Add at least one kill-based restart test per backend; at minimum, rebuild the primary through `load_primary_index_*` inside the existing two-phase tests instead of reusing the live object.

---

### [MEDIUM] Backend coverage asymmetry: engine/dispatch suites run only the in-memory backend; corrupt-snapshot→rebuild and corrupt-secondary-redb fallbacks are not tested end-to-end

**Location:** `src/index/backend.rs:780-796` (`with_all_backends` — unit level, all three backends), `tests/integration.rs:438-560` (two backend-matrix tests), `src/bin/server.rs:433-531` (corrupt-snapshot fallback wiring lives in untestable `main()`), `src/server/startup.rs:913-931` (`fallback_dah_index` tested only with a synthetic `IndexError`).

**What's wrong:** Default `cargo test` does exercise redb and file_backed — but only in the index unit tests (`with_all_backends`, `with_both_dah_backends`, etc.) and exactly two integration tests (`backend_modes_create_spend_and_reopen`, `backend_modes_secondary_indexes_survive_reopen`). The thousands of assertions in `src/ops/engine.rs` and `src/server/dispatch.rs` test modules construct engines exclusively over in-memory indexes. Specific untested wirings: (a) in-memory backend, corrupt/truncated snapshot → `restore_all` Err → device rebuild (bin/server.rs:507-518) — components are tested separately (`snapshot_checksum_verified`, `snapshot_truncated`, `rebuild_*`), the composition is not; (b) secondary redb open failure → `fallback_dah_index` → degraded readiness is tested with a synthetic error, never with an actual corrupt `dah.redb` file on disk; (c) engine behavior under injected redb failures (the `arm_fail_next_*` hooks are only used inside `redb_primary.rs`'s own tests).

**Why it matters:** Finding #4's failure mode and finding #3's silent-empty boot would both be caught by modest cross-backend engine tests; today they cannot be.

**Reproduction:** N/A (coverage gap).

**Suggested fix:** Extract the snapshot-load decision tree from `main()` into a testable `load_primary_index_in_memory_with_snapshot(path, dev, alloc)` in `startup.rs` and test it with a deliberately corrupted snapshot; add a startup test that writes garbage into `dah.redb` and asserts `SecondaryStatus { dah_ok: false, .. }`; run a slim engine smoke suite over all three backends.

---

### [MEDIUM] Corrupt-redb-primary test never asserts the outcome; fail-closed is pinned only indirectly

**Location:** `src/server/startup.rs:656-699` (`redb_primary_rebuild_failure_preserves_file`).

**What's wrong:** The only test that feeds a deliberately corrupt redb file into startup does `let _ = load_primary_index_redb(…)` and asserts the file bytes are preserved. The comment block is self-contradictory ("rebuild succeeds against an empty device… this case actually returns Ok") — in reality `rebuild_redb` re-opens the same garbage file via `redb::Builder::create` and fails, so the function returns `Err`; the byte-equality assertion passes only because of that. The test would keep passing if the error variant or message regressed, and would *also* pass under some hypothetical "rebuild silently returns empty in-memory index" regression as long as the file wasn't touched. Note also the structural quirk it hides: `rebuild_redb` writes into the *same corrupt path*, so for a corrupt-but-present file the rebuild can never succeed — startup always exits 1 and the operator must delete the file manually. That matches README.md:644's fail-closed contract, but no test asserts the `RebuildError::RedbPrimary` outcome.

**Why it matters:** The README's central redb failure-handling promise is enforced by a test that tolerates both success and failure.

**Reproduction:** Change `let _ =` to `assert!(matches!(result, Err(RebuildError::RedbPrimary{..})))` — verify it actually holds (it should), then keep it.

**Suggested fix:** Assert the `Err(RebuildError::RedbPrimary { .. })` variant and that both `restore_err` and `rebuild_err` are populated. Consider documenting (or changing) that device-rebuild into a corrupt existing redb file is impossible without deleting it first.

---

### [LOW] u16 probe-distance counters make the capacity-bound loop guards dead for tables > 65 536 buckets

**Location:** `src/index/hashtable.rs:865, 898, 927, 1028, 1182` (`let mut dist: u16` in `get_entry`, `insert` (both phases), `remove`, `update_cached_fields`).

**What's wrong:** Each probe loop guards against pathological state with `if dist as usize >= self.capacity { … }`. `dist` is `u16`: for any production-sized table (capacity > 2^16 — e.g. 2^28 buckets for 100M records) the guard can never fire; `dist += 1` overflows first (panic in debug, wrap in release). After a release-mode wrap, the Robin Hood early-termination comparison `dist > bucket.probe_distance` operates on a small wrapped value and can falsely return `None` for a present key, or `insert` can loop indefinitely on a corrupt full table. Unreachable under the 0.7 load factor + keyed hash (probe chains stay < 100, tests pin < 100 at 88% load), but the guard exists precisely for corrupted-state defense (cf. the F-G3-005 remove-loop cap, which correctly uses `usize`).

**Why it matters:** The safety net is dead exactly at production scale.

**Reproduction:** Code inspection; or build a 2^17-bucket table, corrupt all buckets to occupied via test hooks, call `get_entry` on a missing key in release mode and observe the wrap.

**Suggested fix:** Make `dist` a `usize` (it is only compared and capped via `cap_probe`, which already takes `u16` — adjust the signature).

---

### [LOW] Checkpoint snapshot materializes the entire index in one heap Vec while dispatch is quiesced

**Location:** `src/index/mod.rs:599-628` (`serialize_primary`), `mod.rs:332-349` (`snapshot_all` appends secondaries to the same Vec), `src/checkpoint.rs:347-366` (runs under the engine visibility guard).

**What's wrong:** `serialize_primary` builds the full snapshot (~63 B/entry, plus 36 B/entry per secondary) in memory before a single `std::fs::write`. At the README's design point (100M records ≈ 7.2 GB table) that is a ~6.3 GB transient allocation per checkpoint, on top of the table itself, while `acquire_checkpoint_visibility_guard` stalls dispatch (the latency side is acknowledged in F-G4-016; the memory spike is not).

**Why it matters:** Checkpoint OOM on a memory-tight host kills the server at exactly the moment the redo log is near-full; the failure mode of the mitigation becomes the outage.

**Reproduction:** Heap-profile `perform_checkpoint` with 10M entries; observe peak RSS ≈ table + serialized Vec.

**Suggested fix:** Stream the snapshot through a `BufWriter` with an incremental CRC instead of one Vec.

---

### [LOW] Minor hygiene: stale doc, asymmetric TOCTOU fix, no device-identity binding for index files

**Location:** `src/index/redb_dah.rs:50` ("The database is shared with the unmined index (same file, different tables)" — false: config uses separate `redb_dah_path` / `redb_unmined_path` and `bin/server.rs:388-401` opens two files); `src/index/redb_dah.rs:174` (`remove` reads `old_height` via `get_height`'s separate read txn — the F-G3-013 TOCTOU fix was applied to `insert` only; harmless today since replay uses `new_height` only, but the asymmetry invites the same future hazard the insert fix warns about); snapshot files (`TSIX`) and redb files carry no device-identity binding, unlike the allocator's `device_id` verification — pointing a config at an index file from a different device opens cleanly and serves wrong offsets until the per-record `tx_id` re-check errors out.

**Why it matters:** Low individually; the device-binding gap turns an ops mistake into a flood of corruption errors rather than one clear startup refusal.

**Reproduction:** Code inspection; for device binding: snapshot on device A, boot against device B, observe successful index load followed by `tx_id` mismatch errors.

**Suggested fix:** Fix the doc comment; move `remove`'s height read inside the write txn; stamp the allocator's `device_id` into the snapshot header and redb metadata table and verify at open.

---

## Checklist disposition

1. **Robin Hood probe distance bounded; high-load behavior** — ✅ with caveat. Stored probe capped at 254 (`MAX_STORED_PROBE`, hashtable.rs:115-126) with early-termination correctly disabled for capped entries; all probe loops bounded by capacity; backward-shift capped at capacity (F-G3-005). At >0.7 load `Index::register` auto-resizes (mod.rs:199-205; engine non-blocking variant engine.rs:706-728; stress test `concurrent_register_produces_one_resize_per_threshold_crossing` mod.rs:1836); raw `HashTable` returns `HashTableError::Full` rather than degrading (hashtable.rs:962-967). Tested at 70/88/90/100% load (`fill_70_percent`, `max_probe_distance_reasonable`, `fill_90_percent`, `fill_to_100_percent`) and 1000-collision adversarial. Caveat: the capacity bound is dead for tables > 2^16 buckets (LOW finding, u16 `dist`).
2. **Snapshot versioned; truncated/corrupt → device-scan fallback (code + test)** — ✅ format / ⚠️ fallback test. `TSIX` magic + version 1, unknown version rejected (test `snapshot_restore_rejects_unknown_version`); CRC verified (`snapshot_checksum_verified` flips a byte; `snapshot_truncated`); poisoned-count overflow tests. Fallback code: bin/server.rs:507-518 (`restore_all` Err → warn → `load_primary_index_in_memory` device rebuild). No test exercises that composition (it lives in `main()`) — MEDIUM finding #6.
3. **redb: every txn commits or aborts cleanly; cross-file sync** — ✅ mechanics / ❌ durability. All write paths either `commit()` or drop the txn (redb Drop aborts): not-found `update_cached_fields` (redb_primary.rs:351-354), all-miss `unregister_batch` (289-297), no-op DAH insert (redb_dah.rs:119-123). Cross-file consistency: runtime sync via two-phase redo intents + `reconcile_secondary_indexes_from_metadata` (recovery.rs:541-576, rebuilds both secondaries from primary on every recovery); bulk import via the R-047 sentinel (write-before-first-commit, remove-after-all-three, startup refuses while present — tested at startup.rs:701-747). ❌ The `Durability::Eventual`-vs-checkpoint-fence interaction is the CRITICAL finding #1.
4. **redb corruption fallback tested with a deliberately corrupt file** — ⚠️. Brief's premise is stale: README documents fail-closed (644-646), not delete/recreate/memory-fallback, and code matches. A deliberately-corrupt-file test exists (`redb_primary_rebuild_failure_preserves_file`, garbage bytes at the redb path) and pins file preservation, but never asserts the load result — MEDIUM finding #7. Corrupt *secondary* redb → degraded readiness is tested only with a synthetic error, never an actual corrupt file (MEDIUM finding #6). Dispatch `ERR_INDEX_DEGRADED` gating is tested (dispatch.rs:12984-13023).
5. **Secondary consistency across crash/restart/migration; judge secondary_two_phase_durability.rs** — ⚠️. The two-phase tests verify the redo-fsync-before-redb-commit window and stale-intent skipping (incl. HAS_PRESERVE_UNTIL), but reuse the live primary object across the "crash," hand-write device metadata post-hoc, and never restart a process — MEDIUM finding #5. Real-mutation clean-restart coverage exists for all three backends (`backend_modes_secondary_indexes_survive_reopen`). Migration crash covered only via sentinel refusal. No kill-based test anywhere.
6. **expected_records hint** — ✅. Sizing: `ceil(expected/0.7)` → next pow2, min 16 (mod.rs:140-150); exceeding it auto-resizes at 0.7 threshold (tests `resize_preserves_entries`, `new_inserts_after_resize`, M9 concurrent stress, `resize_to_smaller_or_equal_capacity_is_noop` defensive re-check); `with_all_backends` registers past small hints without failure. `expected_records=0` → capacity 16.
7. **Both backends in `cargo test`** — ⚠️. Yes at index-unit level (`with_all_backends` covers memory/redb/file_backed; `with_both_dah/unmined_backends`) and two integration matrix tests; engine/dispatch suites are memory-only; no engine test under injected redb failure — MEDIUM finding #6, HIGH finding #4.
8. **Hash DoS / keyed hash** — ✅. Per-process 64-bit seed from `getrandom` mixed via SplitMix64 finalizer (hashtable.rs:250-278); seed never persisted; file-backed reopen rehashes under a fresh seed (`rehash_to_seed`); resize preserves the seed. Tests: `bucket_seed_differs_across_instances`, determinism, `fresh_seed_is_random`, plus adversarial same-bucket tests. Probe blowup from attacker-chosen txids requires knowing the seed.
9. **mmap snapshot safety** — ⚠️. Snapshot write is atomic (tmp + fsync + rename + parent-dir fsync, pinned by `snapshot_atomicity_fsync_parent_dir`); restore reads into a heap Vec (no live-mmap deserialization); file-backed torn-write defense is the sentinel + device rebuild — but findings #2 (resize re-arms the sentinel mid-run) and #3 (invalid-size file silently wiped to empty) both undermine it. Memory spike during snapshot is LOW finding.
10. **Stale index entry → reallocated slot** — ✅ runtime / ⚠️ redb-crash. The engine re-validates `meta.tx_id == key.txid` on reads (ops/engine.rs:789-800, F-G2-001) so a stale pointer at a reallocated slot surfaces as an error, not the wrong UTXO; delete replay re-frees regions idempotently. Residual: redb power-loss staleness (finding #1) produces silent *absences* (lost entries) that no re-check can catch, plus resurrected entries that fail loudly.

Severity tally: 1 CRITICAL, 3 HIGH, 3 MEDIUM, 3 LOW.
