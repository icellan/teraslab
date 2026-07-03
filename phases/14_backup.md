# Phase 14: Online backup and restore

**Status:** planned — design in `docs/ONLINE_BACKUP_DESIGN.md` (read it first;
this phase file is the build order and test list, the design doc is the
correctness argument).

## Goal

A backup taken from a running node with minimal performance impact, and an
offline restore that boots the node in the captured state. Correctness model:
the backup is a crash-legal device image at instant `T` plus the complete redo
tail `(F, T]`, so **restore is the existing crash-recovery path** — no new
recovery code.

## Dependencies

Phases 1-13 complete. v1 scope: segment engine, single node, Memory index
backend only (refusal matrix in the design doc's Non-goals section).

## Reference

- `docs/ONLINE_BACKUP_DESIGN.md` — authoritative design
- `docs/DURABILITY_CONTRACT.md` — WAL-first ordering the tail replay relies on
- `src/checkpoint.rs` — fence sampling pattern
  (`acquire_checkpoint_visibility_guard`)

## Requirements

1. **R14-1 Pins.** A RAII `BackupPinGuard` that (a) makes
   `reclaim_fully_dead_segments` and `defrag_victims`/`defrag_compact`
   no-ops, (b) disables the `free_segments` pop in
   `advance_to_next_segment` (queue preserved; allocation advances the
   high-water mark only), (c) pauses the blob-GC sweep. Pins are process-RAM
   only and are released on drop, abort, error, and panic.
2. **R14-2 Instance lock.** The server takes an exclusive `flock` on
   `device_paths[0]` at startup; `restore` takes a non-blocking flock and
   refuses if held.
3. **R14-3 Redo tee.** `attach_tee`/`detach_tee` on `RedoLog`, invoked inside
   `buffer_entry`/`buffer_preencoded` under the log mutex (order-preserving),
   filtering `RecoveryProgress`/`Checkpoint` markers, buffering into a
   bounded buffer whose overflow aborts the backup and never blocks an
   appender.
4. **R14-4 Fabricated redo.** Per-store linear v2 file: header
   `checkpoint_seq=F, next_sequence=T+1`, one `RecoveryProgress{F}` frame at
   sequence `F`, then teed frames in `(F, T]` in commit order. `recover()` on
   the fabricated file yields exactly the entries in `(F, T]`. Verify
   `RedoLog::open` under `redo_segment_ring = true` adopts a linear v2 file;
   if not, fabricate v3 ring format instead.
5. **R14-5 Copier.** Throttled (token bucket) chunked copy of every used
   segment through the server's device handle via a new
   `BlockDevice::pread_nocache` (CachingDevice: no insert on miss), each
   chunk read under sorted, deduplicated read-side `io_locks` guards
   (`read_span_blocks`, read mirror of `lock_span_blocks`); per-range
   SHA-256; sparse image per physical device at geometry-derived offsets.
6. **R14-6 Job.** State machine `Idle → Pinning → Fencing → Snapshotting →
   Copying → CatchUp → Finalizing → Done|Failed`; single-flight lease; fence
   `F` and finalize `T` sampled under the visibility guard; backup-owned
   index snapshot via `engine.snapshot_index(<backup_dir>/…)`; allocator
   header serialized from memory (`serialize_header_bytes()`); catch-up loop
   bounded by `stall_copy_max_segments`/`max_catchup_rounds`; final stall
   does no backup-dir I/O under the guard.
7. **R14-7 Headroom.** Pre-flight refuse below `min_headroom_segments`;
   mid-run abort below `abort_headroom_segments`. A backup may fail; client
   writes must never fail because of a backup.
8. **R14-8 Blob store.** Tree copied after `T`, skipping `*.tmp`, tolerating
   ENOENT; post-`T` extras are reconciled by `reconcile_blobs_after_recovery`
   on restore boot.
9. **R14-9 Manifest.** `MANIFEST.json` written last (fsync + parent fsync),
   versioned, carrying fence/tail_end, engine/format versions, full device
   geometry, and per-file SHA-256. Manifest presence = complete backup;
   manifest-less dirs are cleaned on the next run and refused by restore.
10. **R14-10 Restore.** Offline CLI: validates manifest version, checksums,
    geometry vs target config, and the instance flock before touching any
    target file; places artifacts; the node then boots through normal
    recovery.
11. **R14-11 Surface.** `BackupConfig` (default-off via
    `backup_dir: Option<PathBuf>`) validated in `Config::validate()`;
    `POST/GET/DELETE /admin/backup*` endpoints (bearer `admin_token`, target
    dir confined under `backup_dir`); `teraslab-cli backup run|status|abort`
    and `teraslab-cli restore`; `teraslab_backup_*` metrics passing
    `prometheus_conformance.rs`.
12. **R14-12 Refusals.** POST refused (typed 400/409/507) for: non-segment
    engine, non-memory index backend, cluster mode, replication configured,
    unset `backup_dir`, running job, insufficient headroom.

## Build order (PR-sized stages)

1. **backup-1 — Pins + instance lock.** `RecordAllocator` trait additions,
   segment-allocator gating, engine pin plumbing + RAII guard, blob-GC
   pause, server flock. Tests U1, U2, I11a.
2. **backup-2 — Redo tee + fabricated tail.** Tee hook with marker filter +
   bounded buffer; `redo_tail` writer; ring-adoption verification (R14-4).
   Tests U3, U4.
3. **backup-3 — Copier + job core.** `pread_nocache`, `read_span_blocks`,
   chunked guarded copy, sparse images, throttle, headroom monitor, catch-up
   loop, final stall, `serialize_header_bytes`, backup snapshot, manifest.
   Tests U5-U9, I1-I5, I7-I8, I12.
4. **backup-4 — HTTP + config + metrics.** `BackupConfig` + validate,
   `BackupManager` in `HttpState`, three endpoints, metrics. Tests I6, I10,
   I11b, I14.
5. **backup-5 — CLI + restore.** `teraslab-cli backup …` / `restore …`,
   restore validation matrix, blob tree handling. Tests I9, I11c, I13, I15.
6. **backup-6 — Docs.** Cross-reference the restore-equals-crash-recovery
   argument from `docs/DURABILITY_CONTRACT.md`; update `docs/observability.md`
   metric list if applicable.

## Tests

Test-first within each stage. No stubs, no `#[ignore]`, every assertion
checks real values.

### Unit

- **U1 `segment_allocator`**:
  `pinned_advance_skips_free_list_pop_and_advances_high_water`;
  `pinned_reclaim_and_defrag_victims_are_noops`; `unpin_restores_reuse`;
  `serialize_header_bytes_matches_persisted_header`.
- **U2 `engine`**: `defrag_reclaim_and_compact_noop_while_pinned`;
  `backup_pin_guard_unpins_on_drop_and_panic`.
- **U3 `redo`**: `tee_receives_every_committed_frame_in_commit_order`
  (concurrent appenders);
  `tee_filters_recovery_progress_and_checkpoint_markers`;
  `tee_bounded_buffer_overflow_signals_abort_without_blocking_append`.
- **U4 `redo_tail`**:
  `fabricated_linear_file_recovers_exactly_entries_above_fence`;
  `fabricated_file_opens_under_segment_ring_config`;
  `empty_tail_file_replays_nothing`.
- **U5 `cache`**: `pread_nocache_serves_dirty_block_without_inserting_miss`.
- **U6 `io`**: `read_span_blocks_sorted_deduped_and_excludes_concurrent_writer`.
- **U7 `copier`**: `chunk_copy_is_block_atomic_under_concurrent_inplace_rmw`
  (hammer freeze/setMined/delete-tombstone during copy; every copied 4 KiB
  block byte-equals a committed pre- or post-state);
  `throttle_token_bucket_rate` (mock clock); `sha256_stream_matches_file`.
- **U8 `job`**: `single_flight_lease_second_start_rejected`;
  `state_transitions_and_abort`;
  `headroom_preflight_reject_and_mid_run_abort`.
- **U9 `manifest`**: `round_trip`; `checksum_mismatch_detected`;
  `missing_manifest_means_incomplete`.

### Integration (`tests/backup_online.rs`, `tests/backup_restore.rs`)

- **I1** `backup_idle_store_restore_verifies_full_record_set` (fast).
- **I2** `backup_under_concurrent_mixed_load_then_restore_matches_reference`
  — flagship: workload generator drives
  create/spend/setMined/freeze/unfreeze/delete throughout the window;
  restore into a fresh dir; `tests/workload/verifier.rs`
  `StateVerifier::verify_against` at `T` (slow).
- **I3** `inplace_rmw_after_segment_copied_is_repaired_by_tail_replay`
  (deterministic; zero CRC errors post-restore).
- **I4** `delete_during_backup_window_restores_as_deleted` (tombstone case).
- **I5** `relocations_during_backup_resolve_via_relocate_replay`.
- **I6** `live_checkpoints_run_during_backup` (small redo forces the 0.75
  trigger mid-copy; live reclaim proceeds; backup consistent).
- **I7** `abort_releases_pins_and_partial_dir_cleaned_on_next_run`.
- **I8** `headroom_exhaustion_aborts_backup_never_client_writes` (tiny
  device, heavy writes).
- **I9** `blobstore_backup_with_gc_pinned_orphans_reconciled_on_restore`.
- **I10** `multi_store_backup_restore_device_paths_x_device_split` +
  `restore_refuses_geometry_mismatch` (slow).
- **I11** refusal matrix: (a)
  `backup_refused_for_in_place_engine_redb_index_cluster_mode`, (b)
  `http_backup_endpoints_status_progression_409_delete_abort_401_unauthenticated`,
  (c) `restore_refuses_bad_checksum` /
  `restore_refuses_running_server_flock`.
- **I12** `crash_mid_backup_leaves_no_manifest_restore_refuses`.
- **I13** `cli_backup_run_status_abort_and_restore_end_to_end` (slow).
- **I14** extend `prometheus_conformance.rs` for the new metrics.
- **I15** `hashtable_resize_during_window_replays_safely` (tail contains
  `HashtableResizeBegin{path}` referencing a source-host path — verify
  replay tolerance).

## Checkpoint protocol

Before starting:

```bash
cargo test --all 2>&1 | tail -30
```

After each stage and at phase end:

```bash
cargo test --all 2>&1 | tail -30
cargo test --all 2>&1 | grep -E "test result|FAILED"
cargo clippy --all -- -D warnings
cargo fmt --check
```

## Done criteria

- All unit + integration tests above green; zero ignored tests; clippy and
  fmt clean.
- `prometheus_conformance.rs` passes with the new metrics.
- I2 (flagship) passes repeatedly under `--release` with a sustained
  concurrent workload.
- `docs/DURABILITY_CONTRACT.md` cross-references the
  restore-equals-crash-recovery argument.
- A backup can never make a client write fail (I8 demonstrates the abort
  path).
