# Category B — Crash recovery and durability (FINAL)

HEAD branch: main. This supersedes all earlier drafts of this file. The tool
channel was intermittent this session; every claim below is backed by source I
read AND re-verified with verbatim `grep`/`sed` (the Read tool produced one
hallucinated snippet — see the note under B-04 — so I cross-checked load-bearing
lines with grep).

Process note / WITHDRAWN finding: an early draft (and two early structured
outputs) claimed "recovery swallows all apply errors." That is FALSE and is
WITHDRAWN. It was written before I had read `replay_entry`/`replay_spend`/…; the
full replay path classifies every error and the startup glue fails closed
(verified-OK items 1–3 below).

Files read + cross-checked:
- `src/redo.rs` 1–2372 (full prod body) — FULL
- `src/checkpoint.rs` 1–401 (full prod body) — FULL
- `src/recovery.rs` 1–1624 (full prod body incl. every replay handler) — FULL
- `src/server/startup.rs` `check_replay_tolerance*` 174–240 + module doc 1–28 — FULL
- `src/bin/server.rs` recovery wiring 608–696 + checkpoint wiring 1180–1226 — FULL
- `src/record.rs` CRC/from_bytes 80–106,185–197,576–625,655–673 (via grep) — VERIFIED
- `src/io.rs` 1–238 + read_metadata 907–925 — FULL/VERIFIED

---

## FINDINGS

### B-02 (LOW, high confidence) — checkpoint.rs module doc: wrong default (0.5) + stale "circular"
Locations: `src/checkpoint.rs:3` ("fixed-size circular-by-checkpoint write-ahead
log"), `:11` ("the configured threshold (default 0.5)") vs the real default
`high_water: 0.75` (`:63` doc, `:86` value) and the linear-with-reset design
(`redo.rs:6-15`). Doc/code drift only, no behavioral bug. Fix: ":11"→"default
0.75", ":3"→"linear-with-reset". Confidence HIGH.

### B-03 (LOW, medium confidence) — checkpoint JoinHandle not joined on shutdown
Locations: `src/bin/server.rs:1183-1226` (handle captured into
`checkpoint_handle` but, unlike `blob_gc_handle` at `:1235`, not observed being
`join()`ed on shutdown). Each checkpoint step is independently durable
(`checkpoint.rs:24-30`) so a torn checkpoint at exit is recoverable — no data
loss, just no clean bounded shutdown of the checkpointer. Fix: bind + `join()`
on shutdown. Confidence MEDIUM (the join-on-shutdown is what I could not finish
reading).

### B-04 (LOW→MEDIUM, medium confidence) — Missing test coverage for the snapshot-deletion device-scan rebuild path
Locations: `src/server/startup.rs` (the device-scan primary-index rebuild path,
documented at `startup.rs:255-263`: "rebuild from a full device scan … CRC-checked
… never fall back to an empty in-memory index"); `tests/integration.rs:435,483`
(reopen tests exist but I could not confirm any deletes the index snapshot to
force the device-scan rebuild). The brief explicitly asks for a test that deletes
the snapshot and exercises rebuild-from-device-scan. The rebuild path is
documented and fails-closed-by-design, and `read_metadata` CRC-validates every
header during the scan (verified-OK item 5), so this is a COVERAGE gap, not a
known bug. Fix: add a recovery test that deletes the index snapshot and asserts
the device-scan rebuild reproduces the correct index (and fails closed on a
corrupt header). Confidence MEDIUM.

NOTE on a hallucinated read: one Read of `startup.rs:255-272` returned a body
`unreachable_placeholder()` with a corrupted line number. A verbatim `grep` for
`unreachable_placeholder|unreachable!|todo!|unimplemented!` in startup.rs returns
NOTHING — that snippet was a tool-channel artifact, not real code. There is no
stub there. I mention it only so a future reader does not chase a phantom.

---

## VERIFIED-OK (each backed by verbatim source lines)

1. **Replay classifies device errors; does NOT swallow them.** Every handler
   maps device read/write failures to `ReplayResult::Failed(ReplayCause::IoError)`
   and short/corrupt records to `CorruptEntry`/`MissingRecordBytes`:
   `replay_spend` (recovery.rs:954,965,987,1005), `replay_unspend` (1028,1045,1054,1072),
   `replay_set_mined` (1099,1145), `replay_freeze`/`replay_unfreeze` (1165,1197/1217,1234),
   `replay_create` (1298 MissingRecordBytes,1306 CorruptEntry,1326 LogicError),
   `replay_create_v2` (1424 CorruptEntry,1438/1449 MissingRecordBytes,1456 CorruptEntry,1470 LogicError),
   `replay_delete`/`write_zeroed_metadata_header` (1340,1344,1347),
   `replay_metadata_op` Reassign/PruneSlot/PruneSlotIfSpentBy/SetConflicting/SetLocked
   (1507,1517 / 1528,1535 / 1551,1561,1566,1571 / 1583,1597 / 1608,1624),
   secondary replay (637,655 / 673,687). The R-013 comment (recovery.rs:979-984)
   records the prior `let _ = io::write_metadata(...)` silent-drop bug was FIXED.

2. **The loop fails closed on the first non-benign cause.** `is_fatal_replay_cause`
   is fatal for everything except `MissingPrimary` (recovery.rs:176-178). Both
   `recover()` (196-204) and `recover_*_collecting_pending_conflicts` (438-464)
   `break` on the first fatal outcome — no later entry lands on a broken state.

3. **Fail-closed is ENFORCED in production startup.** `src/bin/server.rs:649-662`
   calls `check_replay_tolerance_with_cap(&stats, config.recovery_missing_primary_tolerance)`
   and `std::process::exit(1)` when it returns Err; a top-level recovery `Err` also
   `exit(1)`s (server.rs:664-671). `check_replay_tolerance_with_cap`
   (startup.rs:180-240) returns Err when ANY of `failed_io`, `failed_corrupt`,
   `failed_logic`, `failed_missing_record_bytes` is `> 0`, or `failed_missing_primary`
   exceeds the cap. So a single device I/O error during replay refuses to boot —
   the strongest possible response for a UTXO store. (Tested at startup.rs:578-648.)

4. **Replay recomputes generation + DAH on apply (spec idempotency token), and
   re-derives the spent counter.** `replay_spend`/`replay_unspend` set
   `meta.generation = ctx.target_generation` + re-evaluate delete-at-height via
   `evaluate_delete_at_height` + `apply_replay_dah_patch` (990-1004, 1057-1071);
   `replay_set_mined` bumps generation (1142); `PruneSlotIfSpentBy` bumps
   generation + adjusts spent/pruned counts (1568-1570). The spent counter is
   RE-DERIVED ±1 from the slot transition, not trusted from the redo entry
   (989,1056; R-010/BC-04 comment 968-977). `replay_mark_longest_chain` uses a
   wrapping-safe generation idempotency token.

5. **Torn 4 KiB DATA-block writes are detected via CRC and refused on read.**
   `TxMetadata::to_bytes` stamps a CRC32 over the full METADATA_SIZE header
   (record.rs:576-581); `TxMetadata::from_bytes` recomputes and returns
   `RecordError::CrcMismatch` on mismatch (record.rs:584-605). `UtxoSlot` does the
   same over its 69-byte payload (record.rs:185-197). `io::read_metadata`
   (io.rs:907-925) calls `TxMetadata::from_bytes` and maps a CRC mismatch to
   `DeviceError::Io("metadata CRC mismatch")` (io.rs:922-923); `read_utxo_slot`
   goes through `UtxoSlot::from_bytes`. So a half-written record after power loss
   fails CRC → read returns Err → replay classifies IoError/CorruptEntry → fatal →
   startup refuses to boot. The module doc states "every write recomputes the
   relevant CRC and every read validates it" (record.rs:10-11). (A separate
   torn-READ concurrency window on the direct/mmap path is additionally closed by a
   record-level striped RwLock, io.rs:23-67, regression-tested at io.rs:1409.)

6. **Append-before-data-write durability ordering.** `append()` only buffers
   (redo.rs:1776-1801). `flush()` pwrites the aligned buffer, `device.sync()`s
   (redo.rs:1866; fault points BeforeRedoFsync 1862 / AfterRedoFsync 1877), and
   ONLY after the fsync succeeds advances `write_pos`, clears `buffer`, and moves
   `pending_entries`→`entries_cache` (1879-1886). Architecture: validate→append+
   fsync→pwrite data→replicate (recovery.rs:9-15).

7. **Per-entry + header CRC; torn tail dropped not misapplied.** Entry CRC32
   stamped (redo.rs:1515) + re-checked (1543-1546); header magic/version/CRC
   (188-231). A CRC-failing/truncated final entry with nonzero length is treated as
   end-of-log (2304-2319); mid-block length=0 must be all-zero flush pad else scan
   stops (2327-2350).

8. **No in-place wrap; redo-full rejects cleanly, never silently drops.**
   `append()` checks capacity BEFORE buffering, returns `RedoError::LogFull{used,
   capacity}` (redo.rs:1785-1791). Linear log; reclamation only after a durable
   snapshot+marker (checkpoint.rs:360-390).

9. **Flush I/O error poisons the log + drops the buffer**, forcing restart→
   recovery and preventing a re-flush of bytes a client was told failed
   (redo.rs:1855-1913, 1778-1780, 1817-1819).

10. **Compaction/reset cannot reseed seq=1; reset zeroes whole entries region.**
    `compact_prefix_through` retains `> through_sequence` + zero sentinel + persists
    high-water next_sequence (redo.rs:2123-2193); `reset()` zeroes the entire
    entries region then persists the header (2080-2108).

11. **RecoveryProgress cannot hide all entries** — `progress_through` clamped to
    max observed sequence (redo.rs:1974-1991).

12. **checkpoint()/reset()/compaction IS driven in production** (brief's CRITICAL
    question — RESOLVED). `spawn_checkpoint_task_with_reset_guard` (server.rs:1211)
    / `spawn_checkpoint_task` (server.rs:1219) on the live startup path, configured
    from `checkpoint_high_water/low_water/poll_interval` (server.rs:1191-1194). The
    clustered reset-guard defers reset until `min_acked >= floor_sequence`
    (server.rs:1196-1210) so reset never erases redo bytes a lagging replica needs.
    Loop: usage sample → snapshot → persist allocator → mark_recovery_progress
    fence → compact_prefix_through, with hysteresis + exp backoff
    (checkpoint.rs:164-262, 355-390). Dead advance_checkpoint removed
    (redo.rs:2033-2036). The log does NOT fill and brick the master.

13. **CreateV2 decode bounded** (record_len ≤ 1 MiB, parents ≤ 64, checked
    offsets; redo.rs:1263-1301); `replay_create_v2` also requires the allocator
    range to be allocated (recovery.rs:406-409) and rejects record_bytes <
    METADATA_SIZE as CorruptEntry (1423-1424).

14. **Recovery is O(n).** Startup scan reads in 4 MiB chunks with bounded carry
    (redo.rs:2200-2365); `scan_all` clones the in-memory cache (2196-2198);
    mid-replay progress markers every 16384 entries bound re-replay after a
    crash-during-recovery (recovery.rs:57, 450-457).

15. **Secondary reconcile uses on-device metadata as ground truth, idempotent,
    fail-closed.** replay_secondary_unmined/dah skip stale redo, IoError on read
    failure, LogicError on backend failure (recovery.rs:619-689);
    reconcile_secondary_indexes_from_metadata clears+rebuilds both and propagates
    read failures as hard error (522-557).

---

## Net assessment
No CRITICAL/HIGH money-loss bug found in the redo/checkpoint/recovery paths read.
The subsystem is well-defended (WAL-first ordering, per-record + per-entry CRC,
classified fail-closed replay enforced by a production `process::exit(1)`, bounded
recovery, in-production checkpoint reclamation with replica-aware reset guard).
Open items are minor: doc drift (B-02), unjoined checkpoint handle (B-03), and a
missing snapshot-deletion rebuild test (B-04).
