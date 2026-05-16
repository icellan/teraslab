# Group G4: recovery + redo + checkpoint — findings

Files in scope:
- `src/recovery.rs` (4008 LOC)
- `src/redo.rs` (3302 LOC)
- `src/checkpoint.rs` (366 LOC)

Severity counts: **CRITICAL 1 · HIGH 3 · MEDIUM 5 · LOW 4 · INFO 3**

---

### F-G4-001: `next_sequence` rolls back to 1 after a process restart when the redo log was compacted to empty
- **Severity**: CRITICAL
- **Category**: Correctness
- **Location**: `src/redo.rs:1367-1389` and `src/redo.rs:1660-1712`
- **Code**:
  ```rust
  // open()
  let mut log = Self { ..., next_sequence: 1, ... };
  let (entries, tail_pos) = log.scan_all_with_tail()?;
  log.write_pos = tail_pos;
  log.entries_cache = entries.clone();
  if let Some(last) = entries.last() {
      log.next_sequence = last.sequence + 1;
  }
  // compact_prefix_through() retains `RecoveryProgress`-only as empty:
  if retained.iter().all(|entry| matches!(&entry.op, RedoOp::RecoveryProgress {..})) {
      retained.clear();
  }
  ```
- **Issue**: `perform_checkpoint_with_reset_guard` (checkpoint.rs:175) calls `mark_recovery_progress(snapshot_fence_sequence)` then `compact_prefix_through(snapshot_fence_sequence)`. The progress marker is the only entry with sequence > fence, and the special-case at line 1669 drops it, leaving the on-disk log empty. `compact_prefix_through` does NOT persist `next_sequence` anywhere — the in-memory value is correct while the process keeps running, but **a subsequent process restart re-opens the log, finds zero entries, and falls back to `next_sequence = 1`**. The master then starts re-using sequence numbers it already issued before the crash.
- **Impact**: Replica catch-up uses redo sequence as its watermark (`read_from_sequence(from_seq)`, replication intent tracker). After a restart in this state the master will hand out sequence 1, 2, 3 … which replicas (and the durable replication intent tracker) think they already ACKed. The intent-tracker code at dispatch.rs:9537 explicitly relies on "reclaimed redo range should clear stale intent instead of bricking startup" — but with rollback, *new* mutations look like old intents and may be silently dropped as "already replicated". Also breaks `earliest_sequence`-based truncation detection: a replica disconnected at sequence 1000 reconnecting after this rollback will believe the master has truncated past it.
- **Recommendation**: Persist `next_sequence` (and `checkpoint_seq`) in a small header block at the start of the redo region, updated by `flush`/`compact_prefix_through`/`reset`. On `open()`, prefer that header over the empty-scan default. Alternatively, when `compact_prefix_through` would leave the on-disk log empty, write a single zero-payload marker carrying the sequence high-water mark (a new RedoOp variant) so the rebuilt cache lifts `next_sequence` correctly.
- **Confidence**: High (verified by reading `open`, `compact_prefix_through`, and `perform_checkpoint_with_reset_guard` end-to-end).

---

### F-G4-002: Concurrent appenders share `buffer`; a failed flush leaves another thread's entries in place and a subsequent successful flush persists ops the originating client was told failed
- **Severity**: HIGH
- **Category**: Correctness · Concurrency
- **Location**: `src/redo.rs:1405-1488`, `src/server/dispatch.rs:1095-1127`
- **Code**:
  ```rust
  // dispatch.rs:1095
  let (first_seq, last_seq) = { let mut log = redo.lock();
      for op in ops { last_seq = log.append(op.clone())?; }
      (first_seq, last_seq)
  };
  std::thread::sleep(group_window);
  let mut log = redo.lock();
  log.flush().map_err(...)?;
  // redo.rs flush() returns Err without clearing self.buffer on pwrite/sync error.
  ```
- **Issue**: `write_redo_ops_with_group_window` acquires the lock, appends into the shared in-memory `buffer`, drops the lock, sleeps, re-acquires, flushes. Other dispatcher threads can interleave appends into the same `buffer`. If thread A's `flush()` returns an I/O error, `flush()` does NOT clear `self.buffer` (lines 1455-1487 — `buffer.clear()` runs only on the success path). Thread A propagates the error to its client. Thread B then re-takes the lock, flushes the same buffer (now containing both A's and B's ops); if the transient I/O issue is gone B's flush succeeds and **A's redo entries become durable**. After a crash recovery replays A's ops, materializing a mutation A's client was told failed.
- **Impact**: Client may have already issued compensating actions on the "failed" op, or treated the error as authoritative. Cluster-wide divergence: A's reply was an error, so cluster never replicated A's ops; the master alone has them, then replicates on next catch-up. Replicas accept the "new" ops; clients see ghost mutations.
- **Recommendation**: On flush error, atomically truncate `self.buffer` and `self.pending_entries` back to the size each thread expects (track per-call offsets), or serialise the whole "append + flush" critical section so only one batch is in the buffer at a time. The simplest fix: on `flush` error, drop the buffer entirely and treat all in-flight callers as failed (poison the log), forcing a recover.
- **Confidence**: High.

---

### F-G4-003: `RedoLog::advance_checkpoint` is still dead code in production — only updates an in-memory `checkpoint_seq` and reclaims nothing
- **Severity**: HIGH
- **Category**: Correctness
- **Location**: `src/redo.rs:1601-1606`
- **Code**:
  ```rust
  pub fn advance_checkpoint(&mut self, up_to_sequence: u64) -> Result<()> {
      if up_to_sequence > self.checkpoint_seq {
          self.checkpoint_seq = up_to_sequence;
      }
      Ok(())
  }
  ```
- **Issue**: The function has zero non-test callers (`rg "advance_checkpoint" src/` shows only the definition itself and tests inside `mod tests`). It also writes nothing to disk and reclaims nothing. The prior audit BC-01 noted this method along with `RedoLog::checkpoint`/`reset` as bricking risks; `mark_checkpoint` and `reset` now have one (test-only) call site each, and reclamation now happens via `compact_prefix_through` (production caller at `checkpoint.rs:209`). But `advance_checkpoint` was never repurposed — it is unreferenced API surface that misleadingly implies advancing the recovery boundary does something durable.
- **Impact**: Operators / future contributors reading this code reasonably assume calling `advance_checkpoint` reclaims log space. They will not, and the bug will reproduce the original BC-01 symptom (log full, server bricks).
- **Recommendation**: Delete `advance_checkpoint` and `checkpoint_seq` if unused, or wire `advance_checkpoint` to actually call `compact_prefix_through` and make `checkpoint_seq` durable.
- **Confidence**: High.

---

### F-G4-004: `flush()` performs a read-modify-write of the trailing aligned block on every flush — doubles I/O and creates a torn-write window
- **Severity**: HIGH
- **Category**: Correctness · Performance
- **Location**: `src/redo.rs:1443-1454`
- **Code**:
  ```rust
  if intra > 0 || !total.is_multiple_of(align) {
      let read_len = aligned_total.min(...);
      let read_aligned = read_len.div_ceil(align) * align;
      if read_aligned <= buf.len() {
          self.device.pread_exact_at(&mut buf[..read_aligned], aligned_offset)?;
      }
  }
  buf[intra..intra + self.buffer.len()].copy_from_slice(&self.buffer);
  if let Err(e) = self.device.pwrite_all_at(&buf, aligned_offset) { ... }
  ```
- **Issue**: When the buffer doesn't end on a block boundary (the common case — entries are tens-of-bytes, blocks are 4 KiB), every flush reads the trailing aligned-blocks back, splices the new bytes in, and rewrites the whole thing. Two implications:
  1. **Performance**: each fsync flush now requires a synchronous pread before pwrite, doubling redo-path latency vs. an append-only pattern.
  2. **Torn-write hazard**: the device may write the *aligned* block atomically but the pre-existing tail bytes that the read pulled in could be stale w.r.t. another in-flight writer (there shouldn't be one — but during recovery this region was just scanned, so any partial old write that confused the scanner could be re-amplified here).
- **Impact**: Direct hit on the latency-critical WAL hot path; on a 4 KiB-aligned `DirectDevice` every append/flush is RMW. At design throughput (10M ops/sec, group-commit batches of ~100), the redo log becomes the system bottleneck. Combined with finding F-G4-002, transient I/O errors during the pread phase corrupt the buffer state.
- **Recommendation**: Round each entry up to alignment (zero-pad), so flushes are pure appends starting at an aligned offset. Or maintain a separate "trailing partial block" buffer that's logically owned by the in-memory state, and only pwrite full blocks until the partial block fills.
- **Confidence**: High.

---

### F-G4-005: `replay_freeze` (legacy form, without `expected_hash`) will freeze a slot whose hash has been reassigned since the redo entry was written
- **Severity**: MEDIUM
- **Category**: Correctness
- **Location**: `src/recovery.rs:1080-1115`
- **Code**:
  ```rust
  fn replay_freeze(device: &dyn BlockDevice, index: &PrimaryBackend,
                   tx_key: &TxKey, offset: u32, expected_hash: Option<&[u8; 32]>) -> ReplayResult {
      let slot = io::read_utxo_slot(device, ie.record_offset, offset)?;
      if let Some(expected_hash) = expected_hash
          && slot.hash != *expected_hash { return ReplayResult::Skipped; }
      if slot.status == UTXO_FROZEN { return ReplayResult::Skipped; }
      if slot.status != UTXO_UNSPENT { return ReplayResult::Skipped; }
      let frozen = UtxoSlot::new_frozen(slot.hash);
      io::write_utxo_slot(...)?;
  ```
- **Issue**: For legacy `RedoOp::Freeze` entries the `expected_hash` is `None`, so the slot's *current* hash is frozen unconditionally — there is no validation that this is the same UTXO the original freeze was for. If between the original freeze and recovery a `RedoOp::Reassign` re-stamped the slot with a new hash AND that reassign somehow didn't make it into the post-checkpoint redo (e.g. a snapshot crossed both), recovery will re-freeze the wrong UTXO. Likewise the slot's `status != UTXO_UNSPENT` guard treats SPENT and PRUNED as "skip" but the original intent was to freeze regardless of state — for a slot that became SPENT after the original Freeze decision, replay silently drops the operation.
- **Impact**: Possible silent loss of a freeze operation during recovery in rare reordering scenarios. Real-world risk is mitigated by V2 entries carrying `utxo_hash`, but the legacy decoder remains live.
- **Recommendation**: Either (a) deprecate the legacy `Freeze`/`Unfreeze` redo opcode at the encoder side, refusing to ever emit it, and treat any legacy entry found on disk as a soft-fail with `LogicError`; or (b) document explicitly that legacy entries cannot safely be replayed past the next-after-them metadata mutation and require a full snapshot before any old log is replayed.
- **Confidence**: Medium.

---

### F-G4-006: `Vec::with_capacity(parents_count)` in `CreateV2` decode can pre-allocate up to 2 MiB per entry — DoS amplifier via crafted log
- **Severity**: MEDIUM
- **Category**: Security · Performance
- **Location**: `src/redo.rs:1044-1058`
- **Code**:
  ```rust
  let parents_count = u16::from_le_bytes(data[record_end..record_end + 2].try_into().unwrap()) as usize;
  let parents_start = record_end + 2;
  let parents_end = parents_start.checked_add(parents_count.checked_mul(32)?)?;
  if data.len() < parents_end { return None; }
  let mut parent_txids: Vec<[u8; 32]> = Vec::with_capacity(parents_count);
  ```
- **Issue**: `parents_count` is a `u16` (max 65535), so the allocation is bounded at ~2 MiB per entry. The bounds check `data.len() < parents_end` runs BEFORE `Vec::with_capacity`, so an over-large value is rejected before allocation actually happens in the wild. But a legitimate-sized entry with `parents_count = 65535` reads ~2 MiB into `data` (from the redo region) and then allocates another 2 MiB — easily inflates startup memory if many such entries exist. The `record_bytes.to_vec()` at line 1043 is similarly bounded only by the redo region size, so a single CreateV2 entry could span ~64 MiB of log (default). Recovery's `Vec<RedoEntry>` then holds all entries in memory simultaneously.
- **Impact**: Memory amplification during recovery on a master with a corrupt-but-checksum-valid redo log. Less of a DoS than a fragility — on a healthy master this never fires.
- **Recommendation**: Cap `record_bytes.len()` to a sane max (e.g. 1 MiB — record size is bounded by `TxMetadata::record_size_for(utxo_count)` plus cold data). Cap `parents_count` to a small constant (e.g. 64 — Bitcoin transactions in practice rarely have more conflicting parents).
- **Confidence**: Medium.

---

### F-G4-007: Recovery replay continues past fatal I/O / corruption errors instead of stopping — risks producing partially-consistent on-disk state
- **Severity**: MEDIUM
- **Category**: Correctness
- **Location**: `src/recovery.rs:291-420`
- **Code**:
  ```rust
  for entry in &entries {
      let outcome = match &entry.op { ... };
      ...
      match outcome {
          ReplayResult::Applied => stats.entries_replayed += 1,
          ReplayResult::Skipped => stats.entries_skipped += 1,
          ReplayResult::Failed(cause) => stats.record_failure(cause),
      }
      ...
  }
  ```
- **Issue**: The replay loop never bails on an `IoError`/`CorruptEntry`/`LogicError`/`MissingRecordBytes` failure — it just records the failure and proceeds. Subsequent entries may apply successfully, but if a later entry depends on the failed entry's effect, the on-disk state is now partially-applied. Startup eventually exits via `check_replay_tolerance_with_cap`, but only AFTER the entire log has been touched — there is no rollback, and successful Applied ops have already mutated the device. A subsequent restart replays the same log; if the device returns the IoError consistently, the system never makes progress. If the device flakily returns errors only for entry N during the first run but not the second, the second run leaves a different on-disk state than the first attempted, both potentially inconsistent.
- **Impact**: Failure-mode is poorly bounded. Operators get an error log and a startup-abort but the on-device state is half-mutated by the first run's partial replay.
- **Recommendation**: On the first non-tolerable failure (any cause other than `MissingPrimary`), short-circuit the loop and return the recovery stats so far. The startup check then aborts before any further mutations land on disk. Add a regression test driving an IoError at entry N and asserting that entries N+1.. are not touched.
- **Confidence**: High.

---

### F-G4-008: `OP_FREEZE | OP_UNFREEZE if data.len() >= 68` decoder branch can mis-decode legacy 36-byte entries when the entry happens to carry 68 trailing bytes
- **Severity**: MEDIUM
- **Category**: Correctness
- **Location**: `src/redo.rs:952-993`
- **Code**:
  ```rust
  OP_FREEZE | OP_UNFREEZE if data.len() >= 68 => { /* V2 with utxo_hash */ }
  OP_FREEZE | OP_UNFREEZE | OP_PRUNE_SLOT if data.len() >= 36 => { /* legacy */ }
  ```
- **Issue**: The op_type byte alone does not distinguish legacy `Freeze` (36 bytes payload) from `FreezeV2` (68 bytes). Both versions use the same `OP_FREEZE = 4` tag. The decoder picks V2 first when `data.len() >= 68`. An entry with `length` field = full 68 bytes of payload is always V2; an entry with 36 bytes is legacy. But because deserialize is fed the exact op_data slice produced by the length-prefixed framing, the disambiguation works in practice. However, `data.len()` here is `content_len - 9` (everything after sequence + op_type), which for the legacy form is exactly 36. The V2 branch's `>= 68` guard excludes it. **OK in practice**, but the design is fragile: adding any future V3 with 68+ bytes would silently route to the V2 decoder. Worse, if someone ever adds inline trailing padding to entries, the disambiguation breaks.
- **Impact**: Latent fragility. No active bug today but every code review of redo opcodes has to re-prove this.
- **Recommendation**: Allocate distinct op-type bytes for legacy vs V2 (already done for `OP_CREATE`/`OP_CREATE_V2`, `OP_SPEND`/`OP_SPEND_V2`, `OP_UNSPEND`/`OP_UNSPEND_V2`). Add explicit `OP_FREEZE_V2` / `OP_UNFREEZE_V2` tags and update `op_type()` and decoders accordingly.
- **Confidence**: Medium.

---

### F-G4-009: `scan_all_with_tail` reads the whole log into memory on `open()` — 64 MiB allocation at startup, scales linearly with log size
- **Severity**: MEDIUM
- **Category**: Performance
- **Location**: `src/redo.rs:1719-1753`
- **Code**:
  ```rust
  fn scan_all_with_tail(&self) -> Result<(Vec<RedoEntry>, u64)> {
      let read_size = self.log_size as usize;
      let aligned_read = read_size.div_ceil(align) * align;
      let mut buf = AlignedBuf::new(aligned_read, align);
      self.device.pread_exact_at(&mut buf, self.log_offset)?;
      ...
  }
  ```
- **Issue**: `scan_all_with_tail` reads `log_size` bytes (default 64 MiB; configurable up to gigabytes) into a single contiguous aligned allocation, then iterates it linearly. The entries are then `.clone()`d into `entries_cache`. On startup of a server with a 1 GiB redo log, this pre-allocates 1 GiB + N×size_of(RedoEntry) before the server can serve traffic.
- **Impact**: Slow startup; large peak memory at recovery time; container OOM risk on memory-constrained replicas.
- **Recommendation**: Stream the scan in aligned chunks (e.g. 4 MiB at a time) and append `RedoEntry` to `entries_cache` incrementally. The per-entry parsing is already linear; chunking is a mechanical refactor.
- **Confidence**: High.

---

### F-G4-010: `RecoveryProgress` filter in `recover()` can be defeated by a corrupt `through_sequence` value that exceeds real entries
- **Severity**: LOW
- **Category**: Correctness · Security
- **Location**: `src/redo.rs:1545-1571`
- **Code**:
  ```rust
  } else if let RedoOp::RecoveryProgress { through_sequence } = e.op
      && i >= start_idx
      && through_sequence > progress_through
  {
      progress_through = through_sequence;
  }
  ...
  .filter(|entry| !matches!(entry.op, RedoOp::RecoveryProgress { .. })
                  && entry.sequence > progress_through)
  ```
- **Issue**: `through_sequence` is validated against the prior progress marker (`through_sequence > progress_through`) but NOT against the real maximum entry sequence. A corrupt-but-CRC-valid RecoveryProgress entry with `through_sequence = u64::MAX` (or any large value) hides every subsequent entry from replay. The CRC catches random corruption, but anyone who can write to the redo device can suppress replay.
- **Impact**: Limited to attackers with write access to the device (game over already). But also: a software bug elsewhere that writes a wildly wrong `through_sequence` silently drops data.
- **Recommendation**: Bound `through_sequence` against `self.next_sequence` (or the max entry sequence seen so far in the scan). Reject the progress marker if it exceeds that.
- **Confidence**: Medium.

---

### F-G4-011: `mark_recovery_progress` writes a separate fsync per call — at 1024-entry intervals this is 1 extra fsync per ~1024 replayed entries, but no batching with other recovery I/O
- **Severity**: LOW
- **Category**: Performance
- **Location**: `src/recovery.rs:45`, `src/redo.rs:1539-1542`, `src/recovery.rs:415`
- **Code**:
  ```rust
  // redo.rs
  pub fn mark_recovery_progress(&mut self, through_sequence: u64) -> Result<()> {
      self.append(RedoOp::RecoveryProgress { through_sequence })?;
      self.flush()
  }
  // recovery.rs
  const RECOVERY_PROGRESS_INTERVAL_ENTRIES: u64 = 1024;
  ...
  log.mark_recovery_progress(entry.sequence)?;
  ```
- **Issue**: Each progress checkpoint is a separate append + fsync. Replay applies many metadata writes (also synced inside `io::write_metadata`/`write_utxo_slot`), each of which is already a separate fsync. The progress fsync adds 0.1% overhead at the 1024-entry interval but locks the redo region in the middle of replay; on a flaky device this is one more opportunity to fail.
- **Impact**: Modest slowdown; one more failure surface during recovery.
- **Recommendation**: Defer the progress fsync to a coarser cadence (every 16384 entries, or every 1s of wall time during recovery) and ensure the final marker is always written. If `mark_recovery_progress` itself errors, log loudly but continue — the recovery is still correct without it, just slower on a re-crash.
- **Confidence**: Medium.

---

### F-G4-012: `compact_prefix_through` overwrites the entire log region without first clearing trailing bytes — stale entry headers past the new write tail remain
- **Severity**: LOW
- **Category**: Correctness
- **Location**: `src/redo.rs:1660-1712`
- **Code**:
  ```rust
  let aligned_total = required.div_ceil(align) * align;
  let mut buf = AlignedBuf::new(aligned_total, align);
  buf[..bytes.len()].copy_from_slice(&bytes);
  self.device.pwrite_all_at(&buf, self.log_offset)?;
  self.device.sync()?;
  self.write_pos = bytes.len() as u64;
  ```
- **Issue**: `compact_prefix_through` writes only `aligned_total` bytes starting at `log_offset`. The region BEYOND `aligned_total` (up to `log_size`) is left untouched — so old, possibly-still-parseable entries with sequence > `through_sequence` may sit there. The next `scan_all_with_tail` calls `RedoEntry::deserialize` linearly: as long as the first byte after the compacted region is `0` (the length prefix), parsing stops. But because `AlignedBuf::new` zero-initialises only the *newly allocated* buffer, and the compact only writes `aligned_total` bytes, the tail bytes on the device are whatever was there before (possibly entries from before compaction). After a crash mid-compaction, the next open might re-discover OLD entries past the new tail.
- **Impact**: Tested via `mark_checkpoint_clears_recovery_entries` and `truncated_entry_stops_recovery` — the tests rely on a zero-length sentinel at the new tail. The pwrite of `aligned_total` bytes writes the new entries plus zero padding inside the aligned block. As long as the new content ends BEFORE the next aligned block boundary, the tail-zero stop works. But for new content that ends exactly at an aligned boundary, the next aligned block on device is whatever was there before — old entries. `scan_all_with_tail` would then find them.
- **Recommendation**: After pwriting the new content, also pwrite a single zero-block at `log_offset + aligned_total` (or as part of the same write extend `buf` by one alignment unit of zeros).
- **Confidence**: Medium.

---

### F-G4-013: `reset()` zeros only the first alignment-block and trusts the scan to stop on first zero — same trailing-stale-bytes hazard
- **Severity**: LOW
- **Category**: Correctness
- **Location**: `src/redo.rs:1641-1652`
- **Code**:
  ```rust
  pub fn reset(&mut self) -> Result<()> {
      let align = self.device.alignment();
      let buf = AlignedBuf::new(align, align);
      self.device.pwrite_all_at(&buf, self.log_offset)?;
      self.write_pos = 0;
      ...
  }
  ```
- **Issue**: `reset()` writes a single zeroed block at the start, relying on `RedoEntry::deserialize` returning `None` for `length == 0` (line 1278). That works for a contiguous append path. But if a previous run left valid entries beyond the first block, and a future `flush()` overwrites bytes that don't span past those stale entries, the in-memory `entries_cache` is empty but the on-disk parse path could still find those old entries. The sequence-number monotonicity check (`SequenceOutOfOrder`, line 1738) would catch the discontinuity, but only by failing recovery — a noisy, hard-to-debug startup error.
- **Impact**: Operator confusion after a crash following reset.
- **Recommendation**: Track sequence in a header and validate against it. Or simply zero the full log region on `reset` (cost: one large write at checkpoint cadence, not the hot path).
- **Confidence**: Medium.

---

### F-G4-014: `replay_create` (legacy) skips when the index already has an entry — but never verifies that the existing entry points at the same `record_offset`
- **Severity**: LOW
- **Category**: Correctness
- **Location**: `src/recovery.rs:1183-1187`
- **Code**:
  ```rust
  fn replay_create(...) -> ReplayResult {
      if index.lookup(tx_key).is_some() {
          return ReplayResult::Skipped;
      }
  ```
- **Issue**: For an idempotent skip, the existing index entry's `record_offset` and `utxo_count` should be cross-checked against the redo entry's. If they differ, the index entry came from a different create (e.g. a delete + re-create after the original Create entry was logged but before this replay). Skipping is still correct in that case, but emitting no warning hides a real reordering scenario worth surfacing.
- **Impact**: Recovery is silent about reorderings that may indicate upstream bugs.
- **Recommendation**: When the existing index entry's `record_offset` doesn't match the redo entry, log at `warn!` and account it under a new counter. Continue to skip.
- **Confidence**: Medium.

---

### F-G4-015: `apply_replay_dah_patch` uses `metadata.flags -= metadata.flags & TxFlags::LAST_SPENT_ALL` — surprising bitflags pattern, equivalent to `&= !LAST_SPENT_ALL` but rederives
- **Severity**: INFO
- **Category**: Code Quality
- **Location**: `src/recovery.rs:655-662`
- **Code**:
  ```rust
  fn apply_replay_dah_patch(metadata: &mut TxMetadata, patch: &DahPatch) {
      metadata.delete_at_height = patch.new_delete_at_height;
      if patch.last_spent_all {
          metadata.flags |= TxFlags::LAST_SPENT_ALL;
      } else {
          metadata.flags -= metadata.flags & TxFlags::LAST_SPENT_ALL;
      }
  }
  ```
- **Issue**: `flags -= flags & X` clears bit X — fine — but `.remove(X)` / `&= !X` is the idiomatic bitflags form. The same pattern appears at lines 1490, 1517 (and several elsewhere). Pure style nit but each occurrence forces the reader to mentally evaluate `flags & X` before deciding what's being cleared.
- **Impact**: Readability only.
- **Recommendation**: Switch all such patterns to `flags.remove(TxFlags::X)`.
- **Confidence**: High.

---

### F-G4-016: `perform_checkpoint_with_reset_guard` quiesces dispatch via `acquire_dispatch_visibility_guard` for the entire snapshot — large indexes block writes for the full snapshot duration
- **Severity**: INFO
- **Category**: Performance · Concurrency
- **Location**: `src/checkpoint.rs:175-225`
- **Code**:
  ```rust
  pub fn perform_checkpoint_with_reset_guard<F>(...) -> Result<...> {
      let _visibility_guard = engine.acquire_dispatch_visibility_guard();
      let entries_before = redo_log.lock().current_sequence();
      ...
      engine.snapshot_index(&config.snapshot_path)?;  // potentially long
      engine.persist_allocator()?;
      let mut log = redo_log.lock();
      log.mark_recovery_progress(snapshot_fence_sequence)?;
      ...
  }
  ```
- **Issue**: The visibility guard is held across `snapshot_index` and `persist_allocator`. For a primary index with millions of entries, `snapshot_index` (and any future incremental-vs-full snapshot logic) can take seconds. Dispatch blocks for the whole duration. This is documented as the design ("Dispatch is still quiesced while the in-memory snapshot is collected"), but the redo-mutex is correctly released between the fence sample and the marker. Worth re-evaluating whether the snapshot can use a copy-on-write of the index instead of a stop-the-world.
- **Impact**: Periodic tail-latency spikes correlated with checkpoint cadence.
- **Recommendation**: Investigate snapshotting via the index's CoW shadow / generation. Track snapshot duration as a metric and alert when it crosses the configured `poll_interval`.
- **Confidence**: Medium.

---

### F-G4-017: Positive verification — `replay_spend` / `replay_unspend` correctly re-derive `spent_utxos` via `saturating_add(1)` / `saturating_sub(1)` and ignore the redo entry's `new_spent_count`, addressing prior audit BC-04
- **Severity**: INFO
- **Category**: Correctness
- **Location**: `src/recovery.rs:918, 985`
- **Code**:
  ```rust
  meta.spent_utxos = { meta.spent_utxos }.saturating_add(1);  // replay_spend
  meta.spent_utxos = { meta.spent_utxos }.saturating_sub(1);  // replay_unspend
  ```
- **Issue**: Verifies BC-04 is closed. Both replay paths re-derive the counter from on-device state plus the slot transition, instead of overwriting with the redo entry's pre-lock `new_spent_count` snapshot. The behavior is regression-tested at `recovery.rs:2076-2150`.
- **Confidence**: High.

---

## Coverage notes

- `src/redo.rs`: read end-to-end including all 30 `RedoOp` variants, `serialize_data`/`deserialize`, `RedoEntry::serialize`/`deserialize`, `RedoLog::open`/`append`/`flush`/`compact_prefix_through`/`reset`/`recover`/`scan_all_with_tail`/`mark_recovery_progress`. Findings F-G4-001, F-G4-002, F-G4-003, F-G4-004, F-G4-006, F-G4-008, F-G4-009, F-G4-010, F-G4-011, F-G4-012, F-G4-013 covered.
- `src/recovery.rs`: read replay dispatcher, every per-op replay function (Spend/Unspend/SetMined/Freeze/Unfreeze/Reassign/PruneSlot/PruneSlotIfSpentBy/Create/CreateV2/Delete/SetConflicting/SetLocked/PreserveUntil/MarkOnLongestChain/AppendConflictingChild/SecondaryUnminedUpdate/SecondaryDahUpdate/AllocateRegion/FreeRegion/HashtableResize{Begin,Commit}/Compensate{UnsetMined,Reassign,Prune,SetLocked}), the recovery orchestrator, blob/secondary reconciliation, and the recovery-progress writer. Findings F-G4-005, F-G4-007, F-G4-014, F-G4-015, F-G4-017 covered. BC-04 verified closed.
- `src/checkpoint.rs`: read end-to-end. Single production reclamation site confirmed; concurrency model documented. Finding F-G4-016 covered.
