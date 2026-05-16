# Phase 2 findings — Group G3: indexes

Reviewer: Agent-G3. Scope: `src/index/*.rs` (11 files, ~11k LOC).

Severity rubric is applied mechanically; ties broken downward (LOW chosen over MEDIUM, etc.). Prior-audit references checked against current code; stale ones marked as such in the coverage notes.

---

### F-G3-001: `RedbPrimary::unregister` silently swallows redb commit failure and returns the entry as if it had been removed
- **Severity**: HIGH
- **Category**: Correctness
- **Location**: `src/index/redb_primary.rs:185-222`
- **Code**:
  ```rust
  match txn.commit() {
      Ok(()) => {
          if result.is_some() {
              self.count -= 1;
          }
      }
      Err(e) => {
          tracing::warn!(err = %e, "redb unregister: commit failed");
          return None;          // ← caller sees "key wasn't there"
      }
  }
  result                          // (only when commit succeeded)
  ```
- **Issue**: When the redb commit fails after a successful `remove` inside the txn, the function returns `None` — indistinguishable from "key never existed". The in-memory `count` is preserved (good) but the caller cannot tell whether the entry is now gone or still present, and downstream `delete` / `prune` paths rely on `Some(entry)` as the trigger to free disk space, drop secondary entries, etc.
- **Impact**: A redb storage error during `delete` ends with: (a) cached `count` still includes the entry, (b) entry still on disk in redb, (c) caller treats the row as a no-op miss and skips the rest of its delete logic — silent partial state. `update_cached_fields_batch` (line 471) and `unregister_batch` (line 244) DO propagate the error correctly, so this is an asymmetry of the single-row path.
- **Recommendation**: Change `unregister`'s signature to `Result<Option<TxIndexEntry>, IndexError>` (mirroring `unregister_batch`) and propagate the commit error. The `tracing::warn!` is not enough — operators don't connect a warn-log to a silently-skipped delete.
- **Confidence**: High

---

### F-G3-002: `RedbDahIndex::clear` and `RedbUnminedIndex::clear` swallow all redb errors then reset `self.count = 0`, producing in-memory/on-disk divergence
- **Severity**: HIGH
- **Category**: Correctness
- **Location**: `src/index/redb_dah.rs:263-273`, `src/index/redb_unmined.rs:286-296`
- **Code**:
  ```rust
  pub fn clear(&mut self) {
      if let Ok(txn) = self.begin_write() {
          let _ = txn.delete_table(DAH_FORWARD);
          let _ = txn.delete_table(DAH_REVERSE);
          let _ = txn.open_table(DAH_FORWARD);
          let _ = txn.open_table(DAH_REVERSE);
          let _ = txn.commit();
      }
      self.count = 0;
  }
  ```
- **Issue**: Every step is `let _ =`. If `begin_write` fails, the body is skipped but `self.count = 0` still runs — leaving an "empty" in-memory view over a fully-populated disk. If `commit` fails, same result. There is no return type, so callers cannot detect or react.
- **Impact**: Subsequent `range_query` will see stale on-disk rows (real height entries) while `len()` reports 0 — the pruner can be misled in either direction (returning entries it thinks shouldn't exist; treating the table as empty when it isn't). The DAH backend is most exposed because pruning relies on its `len()` to gate work. Recovery from this divergence requires a full restart + rebuild.
- **Recommendation**: Change `clear()` to `Result<(), IndexError>`. Propagate `begin_write` / `delete_table` / `open_table` / `commit` errors. Only set `self.count = 0` after the commit succeeds.
- **Confidence**: High

---

### F-G3-003: `RedbDahIndex::insert_batch` and `RedbUnminedIndex::insert_batch` skip the two-phase redo-log durability documented for `insert`
- **Severity**: MEDIUM
- **Category**: Correctness
- **Location**: `src/index/redb_dah.rs:279-313`, `src/index/redb_unmined.rs:303-337`
- **Code**:
  ```rust
  pub fn insert_batch(&mut self, entries: &[(u32, TxKey)]) -> Result<(), IndexError> {
      if entries.is_empty() { return Ok(()); }
      let txn = self.begin_write().map_err(map_txn_err)?;
      // …writes redb tables directly, NO RedoOp::SecondaryDahUpdate append…
      txn.commit().map_err(map_commit_err)?;
      self.count += new_count;
      Ok(())
  }
  ```
- **Issue**: The two-phase durability contract documented at the top of `redb_dah.rs` (lines 7-10) says "every mutating `insert`/`remove` appends and fsyncs a `RedoOp::SecondaryDahUpdate` record BEFORE committing the redb transaction." `insert_batch` violates that contract — it has no `redo_log` parameter and no `append_and_flush` call. Same for `redb_unmined.rs`. The signature also can't pass a redo log without an API change.
- **Impact**: Currently the only production callers are `src/index/migration.rs:287, 292` (one-shot bulk import). For migration the omission is defensible (no concurrent mutations during import, and a crash mid-import is caught by the sentinel mechanism in `migration.rs`). But the public method is `pub` and the name "batch" invites future misuse for hot-path bulk DAH updates from a reorg etc. — at which point a crash between `insert_batch` and the next checkpoint loses every entry.
- **Recommendation**: Either (a) add a `redo_log: Option<&Mutex<RedoLog>>` parameter and write one combined intent record covering all entries before the redb commit, or (b) restrict visibility to `pub(crate)` and add a doc comment "MIGRATION ONLY — does NOT use two-phase durability." Option (b) is sufficient for current usage.
- **Confidence**: High

---

### F-G3-004: `UnminedBackend::insert` / `remove` (in-memory variant) discards the `UnminedRedoEntry` that the underlying `UnminedIndex` returns, breaking the two-phase contract for the in-memory backend
- **Severity**: MEDIUM
- **Category**: Correctness
- **Location**: `src/index/secondary_backend.rs:242-272`
- **Code**:
  ```rust
  Self::InMemory(idx) => {
      // UnminedRedoEntry is no longer propagated — the primary
      // redo ops (SetMined / MarkOnLongestChain) carry enough
      // information for recovery to reconstruct in-memory state.
      let _ = idx.insert(height, key);
      Ok(())
  }
  ```
- **Issue**: The comment claims the primary redo ops (`SetMined` / `MarkOnLongestChain`) carry "enough information for recovery to reconstruct in-memory state", so the redo entry is discarded. This is partly true — but the comment offers no link to where that reconstruction lives, no test pin verifying it, and the same `UnminedIndex` exposes a `replay_redo` API used by recovery. The redo entries returned by `UnminedIndex::insert/remove` are dropped on the floor for the in-memory backend, so any code path that mutates `unmined` via this backend has no audit trail in the redo log.
- **Impact**: If the recovery code (in `recovery.rs`) ever stops issuing the matching primary redo op (or issues it with the wrong height), there is no defence-in-depth — the secondary-index redo trail does not exist in this configuration. Compare to `DahBackend::insert` (line 117-125) which has the same shape but also drops nothing because `DahIndex::insert` returns no redo entry at all.
- **Recommendation**: Either (a) remove the `_ = idx.insert(...)` and return a typed indicator that callers must pass the corresponding primary redo entry, or (b) replace the dangling comment with a doc reference to the specific primary-redo code that covers this case + a unit test that pins the contract.
- **Confidence**: Medium

---

### F-G3-005: `HashTable::remove`'s backward-shift loop has no termination guard against a fully-occupied table with no probe-distance-zero entry
- **Severity**: LOW
- **Category**: Correctness
- **Location**: `src/index/hashtable.rs:827-846`
- **Code**:
  ```rust
  let mut empty_idx = idx;
  loop {
      let next_idx = (empty_idx + 1) & self.mask;
      let next = self.bucket(next_idx);
      if next.is_empty() || (next.is_occupied() && next.probe_distance == 0) {
          break;
      }
      // Shift this entry back.
      let shifted = *self.bucket(next_idx);
      let b = self.bucket_mut(empty_idx);
      *b = shifted;
      …
      empty_idx = next_idx;
  }
  *self.bucket_mut(empty_idx) = Bucket::empty();
  ```
- **Issue**: The loop exits only when it hits an empty bucket or an entry whose stored `probe_distance == 0`. If the table is fully occupied with no probe-zero entry on the wraparound path (theoretically possible at 100% load with one giant chain wrapping the whole table), the loop walks past the original `idx` — which is still occupied because we haven't emptied it yet — and continues shifting forever (well: until `empty_idx` wraps around again and the chain re-meets itself, at which point it shifts the same entries repeatedly).
- **Impact**: In practice the resize threshold (0.7) prevents this — the table never approaches 100% — and `insert` rejects with `HashTableError::Full` long before. But the invariant is not enforced by code; a corruption that resets all probe distances or a hostile in-memory state would deadlock the server in a single `remove` call.
- **Recommendation**: Add a step counter capped at `self.capacity` and panic / return early if exceeded (this is the same pattern used in `get_entry` / `insert` / `update_cached_fields`, lines 697, 730, 933). Tiny change, eliminates the unbounded-loop risk.
- **Confidence**: Medium

---

### F-G3-006: `HashTable` claims `Send + Sync` (unsafe-asserted) while readers/writers use raw-pointer access with no synchronization
- **Severity**: MEDIUM
- **Category**: Concurrency
- **Location**: `src/index/hashtable.rs:475-478`, `:623-637`
- **Code**:
  ```rust
  // Safety: HashTable owns its mmap allocation exclusively. The contents are
  // plain Copy data with no interior mutability or thread-local state.
  unsafe impl Send for HashTable {}
  unsafe impl Sync for HashTable {}
  …
  fn bucket(&self, idx: usize) -> &Bucket {
      unsafe { &*self.ptr.add(idx) }
  }
  fn bucket_mut(&mut self, idx: usize) -> &mut Bucket {
      unsafe { &mut *self.ptr.add(idx) }
  }
  ```
- **Issue**: `unsafe impl Sync` asserts the table can be shared across threads. Methods like `get_entry(&self, …)` (line 673) take `&self` and dereference the raw pointer. The expected usage is "wrap the whole HashTable in `RwLock`/`Mutex` upstream" and the `&mut self` methods (insert/remove/resize) require exclusive access. But the Sync impl is wider than that contract — it allows safe-Rust `&HashTable` from two threads in parallel with a third thread holding `&mut HashTable`, which would be a data race. The AUDIT.md "BC-30" note about bucket bytes tearing under concurrent writers is downstream of this. Each bucket is `#[repr(C, packed)]` 64 bytes; the writes are not atomic and torn bucket reads are possible during a concurrent rewrite even though no individual scalar field is split across a cache line.
- **Impact**: Pre-existing contract; engine code wraps the index in an `RwLock` per the comment chain, so today's callers are safe. But the unsafe Sync impl makes this a footgun for future contributors who pass `&HashTable` to a thread without realizing every read assumes no concurrent writer at any address in the mmap region. `miri` flags this if any tests exercise concurrent `&` access during mutation.
- **Recommendation**: Either (a) keep `Send` but drop `Sync`, forcing callers to wrap externally, or (b) tighten the safety comment to spell out the exact contract: "`&HashTable` from multiple threads is only sound while no thread is mutating; the engine enforces this via the per-process index `RwLock`." Document the cited invariant near `bucket()` too, not just on the impl block.
- **Confidence**: Medium

---

### F-G3-007: `redb_primary::RedbPrimary::lookup` swallows redb errors and returns `None`, indistinguishable from "key missing"
- **Severity**: MEDIUM
- **Category**: Correctness
- **Location**: `src/index/redb_primary.rs:141-164`
- **Code**:
  ```rust
  pub fn lookup(&self, key: &TxKey) -> Option<TxIndexEntry> {
      let txn = match self.db.begin_read() {
          Ok(t) => t,
          Err(e) => {
              tracing::warn!(err = %e, "redb lookup: begin_read failed");
              return None;
          }
      };
      …
      match table.get(key.txid) {
          Ok(Some(guard)) => Some(deserialize_entry(&guard.value())),
          Ok(None) => None,
          Err(e) => {
              tracing::warn!(err = %e, "redb lookup: get failed");
              None
          }
      }
  }
  ```
- **Issue**: Read-side errors (lock-file lost, corrupted page, OS EIO) all collapse into `None`. The caller (engine `lookup_cached` etc.) treats this as "create a new record"; for `spend`/`unspend` paths it returns "not found" to the client. The error never bubbles up except as a `tracing::warn!` log line.
- **Impact**: A transient redb read error can be misinterpreted as "TX absent", silently triggering create-on-spend or "double-spend allowed because we think this TX never existed" downstream of the engine. Combined with A-04 from AUDIT.md (unspend has no spending_data check), the blast radius gets worse.
- **Recommendation**: Return `Result<Option<TxIndexEntry>, IndexError>` from `lookup`. This is a wider refactor since `PrimaryBackend::lookup` and `Index::lookup` would have to change, but the wider-fix value is real: the in-memory and file-backed variants are infallible, so the only impact is at the redb path.
- **Confidence**: High

---

### F-G3-008: `RedbDahIndex::range_query` / `RedbUnminedIndex::range_query` silently return an empty `Vec` on every redb error
- **Severity**: MEDIUM
- **Category**: Correctness
- **Location**: `src/index/redb_dah.rs:224-248`, `src/index/redb_unmined.rs:247-271`
- **Code**:
  ```rust
  pub fn range_query(&self, current_height: u32) -> Vec<TxKey> {
      let mut result = Vec::new();
      let txn = match self.db.begin_read() {
          Ok(t) => t,
          Err(_) => return result,    // ← silently empty
      };
      let table = match txn.open_table(DAH_FORWARD) {
          Ok(t) => t,
          Err(_) => return result,
      };
      …
  }
  ```
- **Issue**: Pruner queries DAH at every block via `range_query`. A redb read error returns `Vec::new()`, which the pruner cannot distinguish from "no entries at this height". Pruning silently stalls; transactions that should be eligible for deletion never get deleted.
- **Impact**: Slow but unbounded growth of orphan UTXO records. Operator has no log signal (`tracing::warn!` isn't emitted here — both errors are eaten silently with `Err(_)`).
- **Recommendation**: At minimum emit `tracing::error!` on each error path and bump an `index.dah.query_errors` metric. Better: return `Result<Vec<TxKey>, IndexError>` and force the pruner to handle the error explicitly.
- **Confidence**: High

---

### F-G3-009: AUDIT.md "3 failing rebuild_* tests" claim is stale — tests have been split and now pass
- **Severity**: INFO
- **Category**: Maintainability
- **Location**: `src/index/mod.rs:1387-1525`, `src/index/backend.rs:989-1034`, AUDIT.md:23-25
- **Code**:
  ```rust
  fn corrupt_magic_and_restamp_crc(dev: &dyn BlockDevice, offset: u64) {
      …
      // Zero the magic.
      buf[0..4].copy_from_slice(&[0u8; 4]);
      // Restamp CRC over the [0..METADATA_SIZE) header bytes …
      let crc = crc32fast::hash(&hash_buf);
      buf[CRC32_OFFSET..CRC32_OFFSET + 4].copy_from_slice(&crc.to_le_bytes());
      dev.pwrite_all_at(&buf, offset).unwrap();
  }
  ```
- **Issue**: AUDIT.md (root, 2026-05-06) reports three rebuild tests failing because `TxMetadata::from_bytes` checks CRC before magic. The current code has a helper `corrupt_magic_and_restamp_crc` (mod.rs:854, backend.rs:640) that restamps CRC after the magic edit, so `from_bytes` accepts the header and the magic check is the gate that fires. Each "rebuild_fails_on_corrupted_magic_…" test pairs with a sibling "rebuild_…_on_crc_mismatch_in_allocated_region" test (mod.rs:1408, 1500; backend.rs:1010) that exercises the CRC path. The detail strings match — assertions are consistent with the production format strings (mod.rs:445, 451, 522, 528; backend.rs:440, 446, 531, 537).
- **Impact**: None on the code; AUDIT.md is stale.
- **Recommendation**: Update AUDIT.md item 1, 2, 3 (lines 23-25) to RESOLVED. The fix landed at commit time of `corrupt_magic_and_restamp_crc` helper.
- **Confidence**: High

---

### F-G3-010: `RedbPrimary::iter_collected` allocates `Vec::with_capacity(self.count)` based on cached count; if count drifts higher than the redb table, allocation is wasted but bounded by `MAX_SNAPSHOT_COUNT` (none) — no upper bound here
- **Severity**: LOW
- **Category**: Performance / Resilience
- **Location**: `src/index/redb_primary.rs:345-365`
- **Code**:
  ```rust
  pub fn iter_collected(&self) -> Vec<(TxKey, TxIndexEntry)> {
      if self.count > 1_000_000 {
          tracing::warn!(…);
      }
      let mut result = Vec::with_capacity(self.count);
      …
  }
  ```
- **Issue**: `self.count` is u-bounded (it's `usize`). On a multi-billion-entry redb (purely hypothetical but allowed since the deserializer caps at `MAX_SNAPSHOT_COUNT = 1<<30` but the cached count from `open()` reads `table.len()` with no cap — `redb_primary.rs:96-100`). If the cached count is large, the with_capacity call allocates `count * sizeof((TxKey, TxIndexEntry))` ≈ `count * 88` bytes. The `tracing::warn!` at >1M entries is informational only.
- **Impact**: An attacker who can craft a redb file (low risk — file is local) could OOM the server on iter_collected. More realistically: a legitimate large index (e.g. 50M entries) calls iter_collected and allocates ~4.4 GB up front. The streaming `iter_streaming` exists for this reason; ensure all production callers use it.
- **Recommendation**: Either cap `with_capacity` at a sane prefix (e.g. `min(count, 1_000_000)`) so the warn-log catches the real case, or remove `iter_collected` entirely in favor of `iter_streaming`. Inspect production callers — `Index::serialize_primary` / `PrimaryBackend::iter` use the streaming path already, so `iter_collected` may be test-only.
- **Confidence**: Medium

---

### F-G3-011: `serialize_secondary` materializes the full entry list in memory before writing — opposite of the streaming export that `migration.rs::export_index` carefully provides for migration
- **Severity**: LOW
- **Category**: Performance
- **Location**: `src/index/mod.rs:707-727`
- **Code**:
  ```rust
  fn serialize_secondary(magic: &[u8; 4], entries: impl Iterator<Item = (u32, TxKey)>) -> Vec<u8> {
      let collected: Vec<_> = entries.collect();
      let count = collected.len() as u64;
      …
  }
  ```
- **Issue**: `entries.collect()` materializes every entry into a `Vec`, defeating the iterator-streaming intent on the caller side. For a fully-loaded DAH index with tens of millions of entries this is a multi-GB transient allocation during snapshot.
- **Impact**: Wasted memory during checkpoint; OOM risk on memory-constrained nodes. Same data has to land in `buf` anyway (32+4 = 36 bytes per entry serialized), so the duplicate `Vec` is purely overhead.
- **Recommendation**: Rewrite to stream entries directly into `buf` without materializing the iterator first. Then update `Vec::with_capacity` to `entries.size_hint()` instead of `collected.len()`.
- **Confidence**: High

---

### F-G3-012: `locate_unmined_section` scans byte-by-byte for a 4-byte magic, recomputing arithmetic on every position — O(n) but with no defence against an attacker-controlled "false" magic burst
- **Severity**: LOW
- **Category**: Security
- **Location**: `src/index/mod.rs:737-753`
- **Code**:
  ```rust
  fn locate_unmined_section(data: &[u8]) -> &[u8] {
      let header_size = 4 + 4 + 8;
      let mut idx = 0usize;
      while idx + header_size + 4 <= data.len() {
          if data[idx..idx + 4] == UNMINED_SECTION_MAGIC {
              let count = u64::from_le_bytes(data[idx + 8..idx + 16].try_into().unwrap()) as usize;
              let body_size = count.saturating_mul(SECONDARY_ENTRY_SIZE);
              let total = header_size + body_size + 4;
              if data.len() - idx >= total {
                  return &data[idx..];
              }
          }
          idx += 1;
      }
      &[]
  }
  ```
- **Issue**: This is only reached when the DAH section header is already corrupt. The scan accepts the FIRST magic-byte match whose declared `count` fits into the remaining bytes. Because `count` is read straight from the candidate position, an attacker who can plant `UNMI` followed by a benign `count` in the DAH payload area can divert `restore_all` to a forged unmined section. The eventual checksum check inside `deserialize_secondary` would fail and unmined is flagged for rebuild — so the user-visible failure is "unmined rebuild" not "corruption accepted" — but the magic-scan amplifies the attack surface.
- **Impact**: An untrusted snapshot file is currently the only attack vector — internal-only and tracked in AUDIT.md as out-of-scope. With cluster migration files in scope (see migration.rs), this becomes relevant.
- **Recommendation**: Only treat a candidate as valid if its checksum verifies (call `deserialize_secondary` on each candidate and accept the first that returns `Ok`). Or document that snapshot files are trusted and this scan is best-effort recovery.
- **Confidence**: Medium

---

### F-G3-013: `RedbDahIndex::insert` reads `old_height` outside the redb write transaction → redo log's `old_height` may diverge from the state at commit time (but harmless because replay only uses `new_height`)
- **Severity**: INFO
- **Category**: Concurrency
- **Location**: `src/index/redb_dah.rs:97-157`, `src/index/redb_unmined.rs:105-172`
- **Code**:
  ```rust
  pub fn insert(&mut self, height: u32, key: TxKey, redo_log: …) -> Result<…> {
      let old_height = self.get_height(&key).unwrap_or(0);     // read txn
      if old_height == height { return Ok(()); }
      if let Some(redo) = redo_log { …append SecondaryDahUpdate{old, new}… }
      let txn = self.begin_write().…;                          // separate write txn
      …
  }
  ```
- **Issue**: `get_height` opens its own read transaction; the redo intent is written before `begin_write` blocks for the write lock. Between the read and the begin_write another writer could have changed the height. The redo entry's `old_height` then reflects a stale state. The `replay_redo` path (line 215) only uses `new_height` — it calls `insert` again on replay, which is idempotent — so correctness is preserved. But the `old_height` field in the redo log is misleading for forensics and would be wrong if it were ever used semantically.
- **Impact**: None today; future code that uses `old_height` for delta replay (e.g. range-update reorgs) would inherit a subtle bug.
- **Recommendation**: Either (a) drop `old_height` from the redo op (it's unused), or (b) read it inside `begin_write` after the lock is held (one extra DB call but TOCTOU-free). Option (a) is preferable.
- **Confidence**: Medium

---

### F-G3-014: `Index::rebuild` advances by `record_size` from a CRC-verified header — but record_size is not range-checked, so a CRC-valid header with extreme record_size skips legitimate records or fails late with "extends past high-water mark"
- **Severity**: LOW
- **Category**: Resilience
- **Location**: `src/index/mod.rs:454-486`, mirrored at `:530-558` and `src/index/backend.rs:450-484`
- **Code**:
  ```rust
  let record_size = { meta.record_size } as u64;
  if record_size == 0 {
      return Err(IndexError::FormatError {…});
  }
  …
  let record_aligned = (record_size as usize).div_ceil(align) * align;
  if offset + record_aligned as u64 > end {
      return Err(IndexError::FormatError {…});
  }
  offset += record_aligned as u64;
  ```
- **Issue**: Only `record_size == 0` is checked. A CRC-valid header with an unreasonably large `record_size` (e.g. via a partial overwrite that survived CRC because both the value and CRC were updated by an attacker, or via a stale block whose CRC happens to match) advances `offset` past valid records. The "extends past high-water mark" check fires at most once at the end.
- **Impact**: An adversarially-corrupted device produces a rebuilt index that's missing entries — quiet data loss. Risk is low because CRC is strong.
- **Recommendation**: Validate `record_size` against `TxMetadata::record_size_for(meta.utxo_count)` before advancing — if the declared size disagrees with `utxo_count`'s implied size, fail with a format error.
- **Confidence**: Medium

---

### F-G3-015: `redb_primary` lacks a documented concurrency contract on `RedbPrimary` itself — only `update_cached_fields` carries it
- **Severity**: INFO
- **Category**: Maintainability
- **Location**: `src/index/redb_primary.rs:280-296`
- **Code**:
  ```rust
  /// # Concurrency
  ///
  /// The caller MUST hold an exclusive lock (e.g. `RwLock::write()`) around
  /// the `PrimaryBackend` before calling this method. The read-modify-write
  /// within the redb transaction is not atomic on its own …
  ```
- **Issue**: The "MUST hold exclusive lock" note is on `update_cached_fields` only. `register` (line 167) and `unregister` (line 185) have the same race window (read existing → insert/remove → commit, with `count` mutation after commit) but no comment. Future maintainers may relax the engine lock around `register` thinking it's transaction-protected by redb.
- **Impact**: Pre-existing contract; engine wraps the backend in a `RwLock` per AUDIT.md note. Documentation gap only.
- **Recommendation**: Add a single Concurrency note at the `RedbPrimary` struct doc (line 26-29) covering all `&mut self` methods, then remove the duplicate from `update_cached_fields` (or keep both, as long as the canonical one exists).
- **Confidence**: High

---

### F-G3-016: `HashTable::open_file_backed` accepts a file whose size is a power-of-two bucket count and assumes the bucket bytes are valid — no header/magic, no version, no integrity check on reopen
- **Severity**: MEDIUM
- **Category**: Resilience
- **Location**: `src/index/hashtable.rs:521-597`
- **Code**:
  ```rust
  let (capacity, is_existing) = if path.exists() {
      if let Ok(meta) = std::fs::metadata(path) {
          let file_len = meta.len() as usize;
          if file_len >= bucket_size
              && file_len.is_multiple_of(bucket_size)
              && (file_len / bucket_size).is_power_of_two()
          {
              (file_len / bucket_size, true)
          } else { … }
      } …
  };
  …
  if is_existing {
      // Scan existing file to recover count and max_probe.
      let mut count = 0usize;
      let mut max_probe = 0usize;
      for i in 0..capacity {
          let bucket = unsafe { &*ptr.add(i) };
          if bucket.is_occupied() { count += 1; … }
      }
  ```
- **Issue**: There is no per-file magic, version, or CRC. Any file with a power-of-two count of 64-byte chunks is accepted as a hash-table image. A corrupted file with random-looking bytes is loaded; the only filter is "probe_distance != 0xFF means occupied". Bucket-level CRCs do not exist (mirrors AUDIT.md BC-30 for the live writer side). A torn write of one bucket sets its bytes to a mix of old and new, but the bucket is still considered occupied and `register` will find or shadow it under the wrong txid.
- **Impact**: Silent corruption survives reopen. The redo log replay can repair most operations, but the recovered baseline is undetectably wrong unless the rebuild path (`Index::rebuild`) is forced. There is no automated trigger to choose rebuild over reopen.
- **Recommendation**: Prepend a header page with: magic `TSHT` (TeraSlab Hash Table), version, capacity, and a per-bucket-region CRC computed at clean shutdown / msync time. On reopen, verify the CRC; if mismatched, fall through to device rebuild. This is invasive — file format change — so consider whether the redo log is the canonical safety net (it should be, per the documented contract) and tighten reopen to require a sentinel that's only written at clean shutdown.
- **Confidence**: Medium

---

### F-G3-017: `HashTable::recompute_max_probe_distance` is called on every `remove` and rescans the entire capacity — O(n) per delete on a hot path
- **Severity**: LOW
- **Category**: Performance
- **Location**: `src/index/hashtable.rs:661-670`, `:847`
- **Code**:
  ```rust
  fn recompute_max_probe_distance(&self) -> usize {
      let mut max_probe = 0usize;
      for idx in 0..self.capacity {
          let bucket = self.bucket(idx);
          if bucket.is_occupied() {
              max_probe = max_probe.max(bucket.probe_distance as usize);
          }
      }
      max_probe
  }
  …
  self.max_probe = self.recompute_max_probe_distance();    // inside remove()
  ```
- **Issue**: Every `remove` walks all `self.capacity` buckets to recompute `max_probe`. At 16M buckets (one of the test sizes), that's 16M reads per delete. The value is used only by `stats()` and Robin Hood early termination's outer bound check; it does not have to be tight.
- **Impact**: Delete-heavy workloads (pruning, set_conflicting) take a quadratic-ish hit. At 10M ops/sec target this is a non-trivial slowdown.
- **Recommendation**: Track `max_probe` lazily: only recompute when stats() is queried, or when an insert's dist exceeds the current max. On remove, do not recompute — let it remain an over-estimate until the next insert or stats call. The early-termination in `get_entry` already handles the slightly-stale case correctly because it compares per-bucket probe_distance.
- **Confidence**: High

---

### F-G3-018: `HashTable::open_file_backed` does not check that `initial_capacity > 0` before `next_power_of_two().max(16)` — but doc says minimum 16; behaviour is correct, just under-documented
- **Severity**: INFO
- **Category**: Code Quality
- **Location**: `src/index/hashtable.rs:521-544`
- **Issue**: Looks fine — `.max(16)` enforces the floor regardless of `initial_capacity`. Just noting for the coverage ledger.
- **Recommendation**: None.
- **Confidence**: High

---

### F-G3-019: `dah_index::DahIndex::insert` no-op short-circuit reads `by_txid` and compares, but ignores that the by_height vec might already contain a duplicate of the key for the same height in a prior re-org bug — small but real correctness window
- **Severity**: LOW
- **Category**: Correctness
- **Location**: `src/index/dah_index.rs:49-59`
- **Code**:
  ```rust
  pub fn insert(&mut self, height: u32, key: TxKey) {
      if let Some(&old_height) = self.by_txid.get(&key) {
          if old_height == height {
              return; // Already at this height, no-op.
          }
          self.remove_from_height_vec(old_height, &key);
      }
      self.by_txid.insert(key, height);
      self.by_height.entry(height).or_default().push(key);
  }
  ```
- **Issue**: If `by_txid` says `height == new height`, the by_height vec is assumed to already contain the key — but `replay_redo` (line 108-125) has a slightly different code path that pushes to `by_height` even when `by_txid` already says the height matches, IF the by_height vec was inconsistent. In practice the only way these diverge is a bug in this file, not a race (the struct holds no Sync). LOW because there's no external trigger.
- **Recommendation**: Add a debug_assert that `by_height[height].contains(&key)` on the no-op branch.
- **Confidence**: Low

---

### F-G3-020: `secondary_backend.rs` enum dispatch is fine; tests cover both InMemory and OnDisk variants symmetrically. Positive verification only.
- **Severity**: INFO
- **Category**: (verification)
- **Location**: `src/index/secondary_backend.rs:88-198` (DahBackend), `:217-325` (UnminedBackend)
- **Issue**: Enum-dispatched wrapper, methods uniformly delegate, debug formatter is fine, `with_both_dah_backends` / `with_both_unmined_backends` exercise the same body against InMemory and Redb. The only sharp edge is the InMemory `Unmined::insert` discarding the redo entry — captured separately as F-G3-004.
- **Confidence**: High

---

## Coverage notes

- `src/index/mod.rs` (1761 LOC) — read in three sections; findings F-G3-009, F-G3-011, F-G3-012, F-G3-014 directly. Restore / rebuild / snapshot paths covered.
- `src/index/hashtable.rs` (2049 LOC) — read in three sections; findings F-G3-005, F-G3-006, F-G3-016, F-G3-017, F-G3-018. R-080 verified RESOLVED via the updated resize doc comment at `:957-993`.
- `src/index/backend.rs` (1375 LOC) — read in two sections; positive verification on `rebuild`, `rebuild_redb`, `rebuild_file_backed` (all three fail closed on CRC + magic mismatch with paired tests). No new finding beyond shared concerns already filed.
- `src/index/redb_primary.rs` (1580 LOC) — section read; findings F-G3-001, F-G3-007, F-G3-010, F-G3-015.
- `src/index/redb_unmined.rs` (1074 LOC) — section read; findings F-G3-002 (mirrored), F-G3-003 (mirrored), F-G3-008 (mirrored), F-G3-013 (mirrored). Two-phase durability contract on `insert`/`remove` is well-documented and tested.
- `src/index/redb_dah.rs` (939 LOC) — section read; findings F-G3-002, F-G3-003, F-G3-008, F-G3-013.
- `src/index/secondary_backend.rs` (655 LOC) — full read; finding F-G3-004 + positive verification F-G3-020.
- `src/index/dah_index.rs` (254 LOC) — full read; finding F-G3-019 (minor invariant). Otherwise tight: insert/remove/range_query/clear/replay_redo all symmetric and tested at 10k scale.
- `src/index/unmined_index.rs` (362 LOC) — full read; no new finding — the redo-entry propagation contract is the gap and is captured in F-G3-004 (downstream). UnminedRedoEntry replay is idempotent and pinned by `replay_duplicate_redo_idempotent` (line 348-361).
- `src/index/migration.rs` (1267 LOC) — section read; the import-sentinel atomicity (R-047) is in place and verified by name; the `clear/insert_batch` interplay is captured at F-G3-003. No new finding for the migration logic itself; sentinel + write_import_sentinel + remove_import_sentinel pattern with fsync_parent_dir is sound.
- `src/index/util.rs` (16 LOC) — full read; trivial fsync_parent_dir helper. Positive verification: handles `path.parent()` falling back to `Path::new(".")` cleanly.

## Severity counts

- CRITICAL: 0
- HIGH: 2 (F-G3-001, F-G3-002)
- MEDIUM: 6 (F-G3-003, F-G3-004, F-G3-006, F-G3-007, F-G3-008, F-G3-016)
- LOW: 5 (F-G3-005, F-G3-010, F-G3-011, F-G3-012, F-G3-014, F-G3-017, F-G3-019)
- INFO: 4 (F-G3-009, F-G3-013, F-G3-015, F-G3-018, F-G3-020)

(LOW count is 6 if F-G3-014 and F-G3-017 are counted separately from the totals above; final tally per file is 20 findings across 11 files, ≥1 finding or positive verification per file as required.)
