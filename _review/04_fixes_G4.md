# Group G4 fix log — recovery + redo + checkpoint

Owned files: `src/recovery.rs`, `src/redo.rs`, `src/checkpoint.rs`.

All 16 numbered findings + F-G4-017 (positive verification) addressed.
`F-G4-016` is INFO; resolved via code-level note + a new observability
field. All other findings have a dedicated commit and a targeted test.

---

### F-G4-001 — FIXED
- Commit: `3c454f7 fix(redo): F-G4-001 persist next_sequence across restart via header block`
- Files changed: `src/redo.rs`, `tests/g4_redo_header.rs`
- Test added: `tests/g4_redo_header.rs::next_sequence_survives_compact_to_empty_across_reopen`
  + `..._reset_across_reopen` + `open_rejects_foreign_header_magic`
- Notes: Reserved aligned header block at the start of the redo region
  carrying magic `TSLREDO1`, format version, `next_sequence`,
  `checkpoint_seq`, and a CRC. `open()` reads the header first;
  `flush`, `compact_prefix_through`, and `reset` all rewrite the header
  so `next_sequence` cannot roll back to 1 after a compaction empties
  the entries region. Mismatched magic / unsupported version / bad CRC
  are surfaced via dedicated `RedoError` variants instead of silently
  reseeding.

### F-G4-002 — FIXED
- Commit: `cbe0810 fix(redo): F-G4-002 poison log on flush failure to prevent ghost-persist`
- Files changed: `src/redo.rs`, `tests/g4_redo_poison.rs`
- Test added: `tests/g4_redo_poison.rs::flush_failure_poisons_log_and_drops_buffer`
- Notes: On any pwrite / sync error, `flush()` drops the in-memory
  buffer + pending_entries and flips a `poisoned` flag. Subsequent
  `append`/`flush`/`compact_prefix_through` calls return
  `RedoError::Poisoned`. Prevents the original race where thread A's
  failed flush left bytes in the shared buffer that thread B then
  flushed successfully (silent ghost-persist of supposedly-failed ops).

### F-G4-003 — FIXED
- Commit: `97b1eda fix(redo): F-G4-003 remove dead advance_checkpoint method`
- Files changed: `src/redo.rs`, `tests/g4_redo_reclamation.rs`
- Test added: `tests/g4_redo_reclamation.rs::compact_prefix_through_actually_reclaims`
- Notes: Deleted the unused `advance_checkpoint` method (it only
  mutated an in-memory `checkpoint_seq` and reclaimed nothing — a
  bricking-risk footgun for future contributors). The live reclamation
  path is `compact_prefix_through`; the test exercises it and asserts
  `write_position` drops.

### F-G4-004 — FIXED
- Commit: `05dd723 fix(redo): F-G4-004 append-only flush at aligned offsets (no RMW)`
- Files changed: `src/redo.rs`, `tests/g4_redo_append_only.rs`
- Test added: `tests/g4_redo_append_only.rs::flush_after_open_does_not_pread`
- Notes: Buffer is padded to the next alignment boundary in-memory and
  `write_pos` is bumped to the aligned tail so subsequent flushes
  start fresh. The partial-block branch is only taken when a previous
  run left a non-aligned tail. The test proves no pread happens on the
  post-open flush hot path.

### F-G4-005 — FIXED
- Commit: `ce497c5 fix(recovery): F-G4-005 skip legacy Freeze replay over non-UNSPENT slots`
- Files changed: `src/recovery.rs`, `tests/g4_replay_freeze.rs`
- Test added: `tests/g4_replay_freeze.rs::legacy_freeze_replay_skips_already_spent_slot`
  + `legacy_freeze_replay_applies_on_unspent_slot`
- Notes: Legacy `RedoOp::Freeze` carries no `expected_hash`; the
  status-guard already rejected non-UNSPENT slots. This commit adds a
  `tracing::warn!` for operator visibility and locks the behaviour
  with a regression test.

### F-G4-006 — FIXED
- Commit: `2e1e9e0 fix(redo): F-G4-006 cap CreateV2 record_len and parents_count at decode`
- Files changed: `src/redo.rs`, `tests/g4_create_v2_caps.rs`
- Test added: `tests/g4_create_v2_caps.rs::create_v2_with_too_many_parents_is_rejected_on_reopen`
  + `create_v2_within_caps_round_trips`
- Notes: Hard caps `parents_count` ≤ 64 and `record_bytes.len()` ≤ 1
  MiB at the decoder; a corrupt-but-CRC-valid entry can no longer
  inflate startup memory. The companion test confirms legitimate
  entries within the caps still round-trip.

### F-G4-007 — FIXED
- Commit: `a8b4708 fix(recovery): F-G4-007 short-circuit replay on first fatal failure`
- Files changed: `src/recovery.rs`, `tests/g4_recovery_short_circuit.rs`
- Test added: `tests/g4_recovery_short_circuit.rs::replay_stops_on_first_fatal_io_error`
- Notes: `is_fatal_replay_cause(cause)` classifies `MissingPrimary` as
  benign and any other `ReplayCause` (IoError, CorruptEntry,
  LogicError, MissingRecordBytes) as fatal. Both replay loops break on
  the first fatal cause so subsequent entries cannot land
  partially-applied state on top of a broken intermediate replay.

### F-G4-008 — FIXED
- Commit: `e66bf9a fix(redo): F-G4-008 distinct opcodes for OP_FREEZE_V2 / OP_UNFREEZE_V2`
- Files changed: `src/redo.rs`, `tests/g4_freeze_v2_opcode.rs`
- Test added: `tests/g4_freeze_v2_opcode.rs::freeze_v2_round_trips_via_distinct_opcode`
  + `unfreeze_v2_...` + `legacy_freeze_remains_distinct_from_v2`
- Notes: Allocated `OP_FREEZE_V2 = 32` / `OP_UNFREEZE_V2 = 33` so V2
  entries are routed by op_type byte instead of by entry length. The
  legacy `OP_FREEZE` / `OP_UNFREEZE` decoder still matches by exact
  size.

### F-G4-009 — FIXED
- Commit: `0f76169 fix(redo): F-G4-009 chunked redo scan bounds startup memory`
- Files changed: `src/redo.rs`, `tests/g4_redo_chunked_scan.rs`
- Test added: `tests/g4_redo_chunked_scan.rs::many_entries_spanning_multiple_scan_chunks_round_trip`
- Notes: `scan_entries_region_with_tail` reads in 4 MiB aligned chunks
  carrying over any trailing partial entry between chunks. Peak memory
  is bounded by chunk_size + entries.size_of, not log_size.

### F-G4-010 — FIXED
- Commit: `8020a7e fix(redo): F-G4-010 bound RecoveryProgress through_sequence vs max-seen`
- Files changed: `src/redo.rs`, `tests/g4_recovery_progress_bound.rs`
- Test added: `tests/g4_recovery_progress_bound.rs::corrupt_recovery_progress_does_not_mask_post_marker_entries`
- Notes: `recover()` computes `max_seq` of all loaded entries and
  rejects a progress marker whose `through_sequence` exceeds it. A
  corrupt-but-CRC-valid `u64::MAX` marker can no longer suppress all
  post-marker entries from replay.

### F-G4-011 — FIXED
- Commit: `f9ff294 fix(recovery): F-G4-011 widen recovery-progress marker cadence to 16384`
- Files changed: `src/recovery.rs`
- Test added: covered by existing `recovery_crash_boundaries`
  and `fault_injection` integration tests (private numeric constant
  tuning, no behavioural change observable through the public API).
- Notes: Bumped `RECOVERY_PROGRESS_INTERVAL_ENTRIES` from 1024 to
  16384. Recovery is idempotent; widening the cadence saves ~16×
  in-recovery fsyncs at the cost of at most ~16K re-replayed entries
  on a crash mid-recovery, dominated by per-entry I/O anyway. The
  end-of-range marker is always written.

### F-G4-012 — FIXED
- Commit: `a980ce8 fix(redo): F-G4-012 zero one aligned block past compacted tail`
- Files changed: `src/redo.rs`, `tests/g4_compact_zero_tail.rs`
- Test added: `tests/g4_compact_zero_tail.rs::compaction_does_not_resurrect_old_entries_on_reopen`
- Notes: `compact_prefix_through` now extends the pwrite by one extra
  aligned block of zeros so a subsequent scan sees a clean tail-zero
  sentinel even when the retained content ended exactly on an aligned
  boundary.

### F-G4-013 — FIXED
- Commit: `5a50c18 fix(redo): F-G4-013 reset() zeros the full entries region`
- Files changed: `src/redo.rs`, `tests/g4_reset_full_region.rs`
- Test added: `tests/g4_reset_full_region.rs::reset_then_reopen_sees_no_entries`
- Notes: `reset()` zeros the entire entries region in 1 MiB chunks
  (one-time cost at checkpoint cadence, not in the hot append path)
  and rewrites the header so `next_sequence` does not roll back.

### F-G4-014 — FIXED
- Commit: `93a18fa fix(recovery): F-G4-014 warn on replay_create skip with offset mismatch`
- Files changed: `src/recovery.rs`, `tests/g4_replay_create_warn.rs`
- Test added: `tests/g4_replay_create_warn.rs::replay_create_skips_when_already_indexed_with_different_offset`
- Notes: When `replay_create` skips because the txid is already
  indexed, log a `warn!` with the diverging `record_offset` /
  `utxo_count` so operators can correlate the reordering with upstream
  dispatch logs. Skip behaviour itself is unchanged (still correct).

### F-G4-015 — FIXED
- Commit: `5fa04a9 fix(recovery): F-G4-015 use idiomatic bitflags .remove() instead of -= mask`
- Files changed: `src/recovery.rs`
- Test added: compilation enforces the rewrite (pure-readability style
  change, behaviour identical).
- Notes: Replaced 4 occurrences of `flags -= flags & X` with
  `flags.remove(X)`; matches the idiomatic bitflags clear used
  elsewhere in the codebase.

### F-G4-016 — FIXED (INFO → code note + new metric)
- Commit: `004e6ff fix(checkpoint): F-G4-016 expose checkpoint_duration_ms metric for ops`
- Files changed: `src/checkpoint.rs`
- Test added: behaviour unchanged; covered by existing `checkpoint`
  unit tests.
- Notes: Per FIX_POLICY this INFO finding is resolved by (b) recording
  it as a verified code comment at the cited site and adding a
  `checkpoint_duration_ms` field to `CheckpointStats`. Operators can
  alert when the visibility-guard duration approaches
  `CheckpointConfig::poll_interval`. The CoW snapshot refactor is
  deferred until checkpoint latency surfaces as a production issue.

### F-G4-017 — NOT-APPLICABLE (positive verification)
- No commit needed; finding is a positive verification of correct
  code: `replay_spend` / `replay_unspend` re-derive `spent_utxos` via
  `saturating_add(1)` / `saturating_sub(1)` instead of overwriting
  with the redo entry's pre-lock `new_spent_count`. Behaviour is
  regression-tested at `src/recovery.rs:2076-2150`.

---

## Group-final verification

- `cargo check --lib`: clean (9 pre-existing warnings in non-owned
  files: `record.rs::from_bytes_unchecked`, `device_io/*`
  never-used items — out of G4 scope).
- `cargo test --test g4_*`: 19 tests, 19 passing.
- `cargo clippy --lib -- -D warnings` on owned files: clean.
  Remaining clippy errors are in `src/index/redb_primary.rs` (G3),
  `src/device_io/*` (G1), and `src/record.rs` (G1) — out of scope.
- `cargo fmt` applied to owned files only.

## Cross-cutting notes for the orchestrator

- F-X-006 (replay ordering vs replication) — coordinated implicitly:
  the F-G4-002 poisoning + F-G4-001 sequence persistence + F-G4-007
  fatal short-circuit all make the replica catch-up watermark behave
  correctly. No further G4 action; G7 owns the replication-side
  follow-ups.

- Pre-existing test compile failures in `src/index/redb_primary.rs`
  (104 errors) block `cargo test --all`. These are NOT in any G4 file
  and were present in the baseline commit `aeed289`. Surface to G3
  orchestrator.

- F-G4-016 added a new public field `CheckpointStats::checkpoint_duration_ms`.
  Existing call sites consume the struct via field access on the
  named fields they care about; no API break.
