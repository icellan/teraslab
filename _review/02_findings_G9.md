# G9 — Storage Tiers Review Findings

Scope: `src/storage/{blobstore,manager,blob_gc,uploader,tiers,input_refs,mod}.rs`.
Recent audits cross-referenced: R-048 (BlobDigest), R-049 (orphan blob GC), R-089 (cold-data cap), R-051 (RMW pread).

---

### F-G9-001: `read_cold_data` silently returns empty cold data when external blob is missing
- **Severity**: HIGH
- **Category**: Correctness
- **Location**: `src/storage/manager.rs:136-145`
- **Code**:
  ```rust
  if flags.contains(TxFlags::EXTERNAL) {
      let data = self.blob_store.get(&metadata.tx_id)?;
      match data {
          Some(bytes) => ColdData::deserialize(&bytes).ok_or(StorageError::InvalidColdData),
          None => Ok(ColdData {
              inputs: vec![],
              outputs: vec![],
              inpoints: vec![],
          }),
      }
  }
  ```
- **Issue**: When a record is flagged `EXTERNAL` but the blob `get` returns `Ok(None)` (blob missing because deletion raced, blob_gc deleted a still-referenced blob during a race, manual rm, or upload incomplete), this path returns an **empty** `ColdData` instead of an error. Callers that use `read_cold_data` cannot distinguish "the tx had no cold data" from "the cold data was lost". Note the contrast with `stream_cold_data` (manager.rs:178) which propagates `BlobError::NotFound`.
- **Impact**: Validation, SPV proof generation, and audit tooling that reads via `read_cold_data` would observe a record with zero inputs/outputs/inpoints — silent data-integrity violation. A lost blob masquerades as "no cold data", which may bypass invariants in callers that assume `EXTERNAL`-flagged records always have cold data.
- **Recommendation**: Return a `StorageError::Blob(BlobError::NotFound { ... })` when an EXTERNAL-flagged record's blob is missing. The empty-vector branch is only legitimate when `EXTERNAL` flag is set but the blob was intentionally absent — and in that case the record should not have `EXTERNAL` set in the first place. Add a regression test.
- **Confidence**: High

---

### F-G9-002: `read_cold_data` does not cross-check against `ExternalRef.content_hash`
- **Severity**: HIGH
- **Category**: Security / Correctness
- **Location**: `src/storage/manager.rs:127-145`
- **Code**:
  ```rust
  if flags.contains(TxFlags::EXTERNAL) {
      let data = self.blob_store.get(&metadata.tx_id)?;
      match data {
          Some(bytes) => ColdData::deserialize(&bytes).ok_or(StorageError::InvalidColdData),
          ...
  ```
- **Issue**: `BlobStore::get` verifies the payload against the **sidecar** (`<blob>.meta`) digest. But the record on the device carries an independent `metadata.external_ref.content_hash` — the durable record-anchored digest stamped during create. R-048 was specifically about getting that hash populated. This read path does not cross-check the two digests. The spend-path in `src/ops/engine.rs:2317` does (`if actual != meta.external_ref.content_hash`), but `read_cold_data` does not, so other readers (audit tooling, SPV, prune validation) bypass the wrong-blob-served scenario.
- **Impact**: Threat model R-048 explicitly mentions "wrong-blob-served scenario". If an attacker (or operator with disk access) substitutes BOTH payload and sidecar for a different (valid-internally) payload, the sidecar verifier passes — the record-anchored digest in `ExternalRef.content_hash` would catch it, but only the spend path checks. Any other reader silently returns wrong data.
- **Recommendation**: After `blob_store.get` returns `Some(bytes)`, compute SHA-256 and compare against `metadata.external_ref.content_hash` before returning. Surface `StorageError` on mismatch. Same fix needed for `stream_cold_data`'s external branch and `read_inputs` / `read_output_at`.
- **Confidence**: High

---

### F-G9-003: Uploader queue is unbounded — submit() has no backpressure
- **Severity**: MEDIUM
- **Category**: Performance / Security (DoS)
- **Location**: `src/storage/uploader.rs:88`
- **Code**:
  ```rust
  pub fn new(blob_store: Arc<dyn BlobStore>, device: Arc<dyn BlockDevice>) -> Self {
      let (task_tx, task_rx) = std::sync::mpsc::channel::<UploadTask>();
      ...
  ```
- **Issue**: `std::sync::mpsc::channel()` is unbounded. `submit` accepts arbitrarily many `UploadTask { data: Vec<u8>, ... }` and returns immediately. A flood of submissions — each with multi-MiB `data` — can grow the in-flight queue without limit, since the upload thread can never process tasks as fast as a fast caller can enqueue them.
- **Impact**: Memory exhaustion under bursty external-tier upload. The handle's `.wait()` doesn't help — by the time the caller decides to wait, the queue is already full of un-uploaded Vecs.
- **Recommendation**: Use `mpsc::sync_channel(capacity)` or a bounded queue (`crossbeam_channel::bounded`). Pick a capacity based on `max_in_flight_bytes` rather than count, or expose it via `ServerConfig`. Reject `submit` with a `BlobError::Io(WouldBlock)` when the queue is full so callers can apply backpressure.
- **Confidence**: High

---

### F-G9-004: Background blob-GC sweep races with concurrent create — may delete a freshly-uploaded blob
- **Severity**: MEDIUM
- **Category**: Concurrency
- **Location**: `src/storage/blob_gc.rs:105-116`, `:117-181`
- **Code**:
  ```rust
  /// To avoid that race in production, callers SHOULD invoke
  /// this from recovery (no concurrent dispatch) or from the background task
  /// after the create flow has been quiesced — or accept the failure mode that a
  /// freshly-uploaded blob whose index registration was about to land may need a
  /// re-upload. The recovery path is race-free because no client is connected.
  ```
- **Issue**: The author-documented contract acknowledges the race: `list()` enumerates blobs at T0, the per-blob `lookup` at T1, and `delete` at T2. A concurrent dispatch that does `blob.put` between T0 and T1 will be enumerated; if its `index.register` has not landed by T1, the GC will treat the blob as orphan and delete it. The periodic background task spawned at `src/bin/server.rs:1061` runs concurrently with live dispatch — the documented "quiesce" precondition is not met.
- **Impact**: Under load (heavy create rate + GC sweep coincident), legitimate freshly-uploaded blobs can be deleted out from under the create that just wrote them. The create's subsequent `register` lands but points at a now-missing blob — record is corrupt at first read (F-G9-001 then silently returns empty data; F-G9-002 mismatch would catch it on the spend path).
- **Recommendation**: Two viable mitigations: (a) re-`lookup` once more AFTER the `delete` would actually happen and skip if the index now has it; (b) introduce a min-age threshold on `list()` so only blobs older than, say, 60s are considered orphans — gives the register time to land. Option (b) is simpler and matches the `STALE_TMP_AGE_SECS` pattern already used for `.tmp` files.
- **Confidence**: High

---

### F-G9-005: `FileStreamWriter` lock-window allows put/stream interleaving to leave payload+sidecar mismatched briefly
- **Severity**: LOW
- **Category**: Concurrency
- **Location**: `src/storage/blobstore.rs:505-545`
- **Code**:
  ```rust
  let _guard = key_locks[lock_index].lock();
  // 1. fsync the payload temp file, then rename into place.
  file.sync_all()?;
  drop(file);
  std::fs::rename(&temp_path, &final_path)?;
  // 2. Finalize the digest and write the sidecar atomically.
  ...
  atomic_write_no_dir_fsync(&meta_path, &meta_bytes)?;
  ```
- **Issue**: `FileStreamWriter::finish` only takes the per-key lock at finish-time. A concurrent `put` to the same key that completes between this stream's `begin_stream` and `finish` writes its own payload+sidecar pair atomically. Then when this stream's `finish` lands: payload is renamed (the stream's bytes), THEN sidecar is overwritten. Between those two ops (steps 1 and 2 above), an outside reader sees the stream's payload but the put's still-stale sidecar — `DigestMismatch` error to that reader for the brief window. The two writes are not atomic together.
- **Impact**: Transient `DigestMismatch` errors under concurrent put/stream to the same txid. Eventually consistent (final state is valid). The existing `file_concurrent_puts_same_key_do_not_corrupt_blob` test only exercises put/put, not put/stream.
- **Recommendation**: Add a put/stream concurrency test that asserts no observable mismatch from concurrent readers — likely requires reordering: write sidecar first (with the stream's digest), then rename payload. Or accept the brief window and document it explicitly.
- **Confidence**: Medium

---

### F-G9-006: `read_cold_data` honors attacker/corruption-controlled `record_size` without upper bound
- **Severity**: LOW
- **Category**: Security / Correctness
- **Location**: `src/storage/manager.rs:146-162`
- **Code**:
  ```rust
  let cold_offset = record_offset + Self::inline_cold_offset(utxo_count);
  let record_size = { metadata.record_size } as u64;
  let cold_end = record_offset + record_size;
  let cold_size = cold_end.saturating_sub(cold_offset);
  ...
  let bytes = self.read_aligned(cold_offset, cold_size as usize)?;
  ```
- **Issue**: `metadata.record_size` is a `u32` (up to ~4 GiB). A corrupt or attacker-tampered on-device record with `record_size = u32::MAX` causes a 4 GiB aligned read via `AlignedBuf::new(total, align)`. There's no upper bound check tying `record_size` to `MAX_COLD_DATA_PER_ITEM + METADATA_SIZE + utxo_count*69`.
- **Impact**: Memory exhaustion via single corrupt record. The wire-side R-089 cap (4 MiB cold data) is enforced in the codec but not echoed on the read-back path — defense-in-depth gap.
- **Recommendation**: Add a sanity bound: `cold_size <= MAX_COLD_DATA_PER_ITEM + 64` (or similar). Surface `StorageError::InvalidColdData` if exceeded. Same applies to `stream_cold_data` (line 184).
- **Confidence**: High

---

### F-G9-007: Disk-full / write failure in `FileStreamWriter::write_chunk` leaks `.tmp` for up to 5 minutes
- **Severity**: LOW
- **Category**: Correctness
- **Location**: `src/storage/blobstore.rs:506-511`
- **Code**:
  ```rust
  fn write_chunk(&mut self, data: &[u8]) -> Result<()> {
      self.file.write_all(data)?;
      self.hasher.update(data);
      self.bytes_written += data.len() as u64;
      Ok(())
  }
  ```
- **Issue**: If `write_all` fails (disk full, ENOSPC), the error propagates but the `.tmp` file is left on disk. The caller does not see the `FileStreamWriter` value (it's borrowed `&mut`), so it cannot call `abort` automatically; the dispatch path at `src/server/dispatch.rs:6050-6055` does abort, but only the in-memory writer-state is dropped — the stale `.tmp` file remains. Cleanup waits for the next `list()` sweep, which uses a 5-minute mtime threshold.
- **Impact**: Brief on-disk debris on write failure. Cleaned up on next periodic sweep but accumulates until then. Counts up `ENOSPC` cases that may be the very condition causing the failure.
- **Recommendation**: On `write_chunk` error, mark the writer as "poisoned" and have `abort` always called by the dispatcher; the abort path at blobstore.rs:547 already removes the temp file. Consider a stricter `STALE_TMP_AGE_SECS` (e.g., 60s) or run `list()` more often when sustained ENOSPC is observed.
- **Confidence**: Medium

---

### F-G9-008: Uploader's pwrite of `ExternalRef` failure leaves blob present + record content_hash=0
- **Severity**: MEDIUM
- **Category**: Correctness
- **Location**: `src/storage/uploader.rs:123-149`
- **Code**:
  ```rust
  let digest = blob_store.put(&task.tx_id, &task.data)?;
  ...
  let ext_ref = ExternalRef {
      store_type: 1,
      content_hash: digest.sha256,
      ...
  };
  // pwrite the ExternalRef into the metadata region
  Self::write_external_ref(device, task.record_offset, &ext_ref).map_err(|e| {
      BlobError::Io(std::io::Error::other(format!("device write failed: {e}")))
  })?;
  ```
- **Issue**: If `blob_store.put` succeeds but the subsequent `write_external_ref` (read-modify-write of metadata) fails (transient device error, allocator race), the upload returns `Err`. The hot record (which the create path wrote earlier and which already has `TxFlags::EXTERNAL` set) is now in a half-state: the blob exists, `ExternalRef.content_hash` is still zero. blob_gc keeps the blob (record has EXTERNAL flag set → `kept`), but any subsequent read that cross-checks content_hash (engine.rs:2317) will fail.
- **Impact**: Permanently broken record without a foreground signal to the create caller (the upload was async). Operator must manually run integrity scan to detect.
- **Recommendation**: On `write_external_ref` failure, attempt to clean up the just-uploaded blob (`blob_store.delete(&task.tx_id)`); if that also fails, surface a metric. Alternatively, re-queue the metadata write for retry rather than failing the upload outright.
- **Confidence**: High

---

### F-G9-009: No encryption-at-rest for blob payloads (deployment assumption)
- **Severity**: INFO
- **Category**: Security
- **Location**: `src/storage/blobstore.rs` (whole module)
- **Code**: (none — this is an absence)
- **Issue**: Blob payloads are stored verbatim on the host filesystem. No encryption, key management, or per-tenant isolation. Sidecar is plain SHA-256 + length.
- **Impact**: Any operator/attacker with filesystem access can read every transaction's cold data (inputs, outputs, scripts). For BSV (a public blockchain), this may be acceptable since the data is public; for hosted/multi-tenant deployments it is not.
- **Recommendation**: Document the deployment assumption: blob root MUST be on a filesystem accessible only to the TeraSlab process, on encrypted block storage (LUKS, dm-crypt, cloud-provider EBS encryption) if any sensitivity beyond the public chain is required.
- **Confidence**: High

---

### F-G9-010: `OP_BLOB_PUT` referenced in blob_gc doc comment does not exist
- **Severity**: LOW
- **Category**: Maintainability
- **Location**: `src/storage/blob_gc.rs:19-20`
- **Code**:
  ```rust
  //! 3. **Migration cancellation.** A migration target receives the blob bytes
  //!    via `OP_BLOB_PUT` and then the migration is rolled back — the index
  ```
- **Issue**: The doc references `OP_BLOB_PUT` but a grep across the codebase finds no such opcode handler (`grep -rn "OP_BLOB_PUT" src/` returns only this comment). Either the opcode was renamed (likely the streaming `OP_STREAM_CHUNK` path), or migration-side blob transfer is unimplemented.
- **Impact**: Stale doc — leads readers to look for a non-existent path.
- **Recommendation**: Update the comment to reference the actual transport (`OP_STREAM_CHUNK` / migration apply path) or, if migration blob transfer is unimplemented, file a tracking task.
- **Confidence**: High

---

### F-G9-011: `stream_to`'s two-pass design races a concurrent rename
- **Severity**: LOW
- **Category**: Concurrency
- **Location**: `src/storage/blobstore.rs:640-699`
- **Code**:
  ```rust
  let mut file = match std::fs::File::open(&path) {
      Ok(f) => f,
      ...
  };
  let (expected_sha, expected_len) = Self::read_meta(&path, key)?;
  // Pass 1: verify by hashing fixed-size chunks.
  ...
  // Pass 2: stream the verified payload without retaining it in memory.
  file.rewind()?;
  ```
- **Issue**: The implementation opens the file handle, reads the sidecar, hashes pass 1, then rewinds and streams pass 2. A concurrent `put` to the same key under the per-key mutex may rename a NEW payload into `path` between the two passes. On Linux this is benign (the open `file` handle still points at the original inode; rename doesn't invalidate it), so pass 2 reads the same bytes pass 1 hashed. But the just-rewritten sidecar (now describing different bytes) is unread by `stream_to`. Worse: a concurrent reader using `get` AFTER this point sees the new sidecar+new payload (consistent), but if `stream_to` is in flight, no harm. Verified safe on Linux. Worth a comment to clarify the inode-based reasoning.
- **Impact**: None on Linux; depends on inode-based read semantics. Could surprise readers reasoning about the code.
- **Recommendation**: Add a comment explaining why the two-pass design is race-safe (open-then-mutate is inode-based on Linux), or take the per-key lock for the duration of `stream_to`.
- **Confidence**: Medium

---

### F-G9-012: Positive — Path-traversal not possible via tx-supplied identifier
- **Severity**: INFO
- **Category**: Security
- **Location**: `src/storage/blobstore.rs:429-439`
- **Code**:
  ```rust
  fn blob_path(&self, key: &[u8; 32]) -> PathBuf {
      let hex: String = key.iter().map(|b| format!("{b:02x}")).collect();
      let mut path = self.base_dir.clone();
      for i in 0..self.prefix_depth {
          let start = i * 2;
          if start + 2 <= hex.len() {
              path = path.join(&hex[start..start + 2]);
          }
      }
      path.join(&hex)
  }
  ```
- **Issue**: Verified: `key: &[u8; 32]` is strongly typed; the hex encoding can only produce `[0-9a-f]{64}`. No traversal sequences (`..`, `/`) can appear. Path is always `base_dir/<2hex>/<2hex>.../<64hex>`. Good.
- **Impact**: Path traversal attack via blob key is impossible at this layer.
- **Recommendation**: None.
- **Confidence**: High

---

### F-G9-013: Positive — R-049 orphan-blob recovery reconciliation correctly placed
- **Severity**: INFO
- **Category**: Correctness
- **Location**: `src/recovery.rs:516-532`, `src/storage/blob_gc.rs:117-181`
- **Code**:
  ```rust
  pub fn reconcile_blobs_after_recovery(
      blob_store: &dyn BlobStore,
      index: &PrimaryBackend,
  ) -> Result<BlobGcStats, BlobError> {
      let started = std::time::Instant::now();
      let stats = blob_gc::reconcile_orphan_blobs_against_index(blob_store, index)?;
      ...
  ```
- **Issue**: Verified: recovery-time reconciliation runs after redo replay and before clients connect (correct ordering per the doc), so it cannot race a concurrent dispatch. The recovery path is race-free. The runtime concern is the **periodic** sweep — covered in F-G9-004.
- **Impact**: Recovery-time orphan cleanup is correct.
- **Recommendation**: None for recovery; see F-G9-004 for the periodic-sweep race.
- **Confidence**: High

---

### F-G9-014: Positive — `tiers.rs` is straightforward and well-tested
- **Severity**: INFO
- **Category**: Code Quality
- **Location**: `src/storage/tiers.rs:1-127`
- **Issue**: Verified: pure functions, deterministic threshold logic, round-trip-tested serializer with bounds checks on deserialize (returns `None` on truncation rather than panicking). The `BlobDigest` propagation through `ColdDataRef::External { digest }` (R-048) is correctly enforced by type-system construction.
- **Impact**: None.
- **Recommendation**: None.
- **Confidence**: High

---

### F-G9-015: Positive — `input_refs.rs` correctly applies R-051 RMW pread-error propagation
- **Severity**: INFO
- **Category**: Correctness
- **Location**: `src/storage/input_refs.rs:84-96`
- **Issue**: Verified: the R-051 fix is present and correctly propagates `pread_exact_at` errors when the write is not aligned. The corresponding comment is detailed and accurate. No "reference counting" semantics in this module — it's purely outpoint serialization (despite the module name suggesting otherwise to the audit prompt). No leaks under crash because no resource is owned.
- **Impact**: None.
- **Recommendation**: None.
- **Confidence**: High

---

### F-G9-016: `delete_cold_data` deletes blob unconditionally — fine because txid→blob is 1:1, but undocumented
- **Severity**: INFO
- **Category**: Maintainability
- **Location**: `src/storage/manager.rs:205-211`
- **Code**:
  ```rust
  pub fn delete_cold_data(&self, metadata: &TxMetadata) -> Result<()> {
      if metadata.flags.contains(TxFlags::EXTERNAL) {
          self.blob_store.delete(&metadata.tx_id)?;
      }
      ...
  ```
- **Issue**: Blob is keyed by txid which is unique per record, so a delete cannot affect another record. This invariant is implicit and not documented. A reader may worry about ref-counting (off-by-one decrement, leak under crash) — none of those concerns apply because the relation is 1:1.
- **Impact**: None functionally; doc clarity.
- **Recommendation**: Add a doc comment stating: "Each blob is keyed by txid, which is unique per record — no refcount needed."
- **Confidence**: High

---

### F-G9-017: `FileBlobStore::walk_dir` swallows recursion errors silently from subdir failures
- **Severity**: LOW
- **Category**: Correctness
- **Location**: `src/storage/blobstore.rs:381-385`
- **Code**:
  ```rust
  if file_type.is_dir() {
      if let Err(e) = Self::walk_dir(&path, stale_cutoff, out) {
          tracing::warn!(path = %path.display(), err = %e, "blob list: subdir walk failed");
      }
      continue;
  }
  ```
- **Issue**: A subdirectory walk failure is logged at `warn` and skipped. If the `0a/` subdir of the prefix tree becomes unreadable (perm bit, transient I/O), every blob under it is silently absent from the returned list. blob_gc would NOT consider those blobs orphans (they're not in the list at all) — but they're also not protected if their index entries don't exist. Inverse failure mode: blobs that should be checked are skipped.
- **Impact**: Reconciliation under degraded filesystem state produces silently incomplete results. Operator sees a `warn` line but no metric, no error returned.
- **Recommendation**: Add a counter (subdir_walk_failures) returned via `BlobGcStats` so operators can alert on it. The `walk_dir` signature already returns `io::Result`, so failing fast on persistent errors is also viable.
- **Confidence**: Medium

---

## Coverage notes

Files reviewed end-to-end:

- `src/storage/mod.rs` (12 LOC) — trivial module declarations; covered by positive verification implicit in other findings.
- `src/storage/tiers.rs` (187 LOC) — F-G9-014.
- `src/storage/blobstore.rs` (1469 LOC) — F-G9-005, F-G9-007, F-G9-011, F-G9-012, F-G9-017.
- `src/storage/manager.rs` (1463 LOC) — F-G9-001, F-G9-002, F-G9-006, F-G9-016.
- `src/storage/blob_gc.rs` (430 LOC) — F-G9-004, F-G9-010, F-G9-013.
- `src/storage/uploader.rs` (367 LOC) — F-G9-003, F-G9-008.
- `src/storage/input_refs.rs` (266 LOC) — F-G9-015.

Cross-references checked outside scope (read-only, no modifications):

- `src/protocol/codec.rs` (R-089 enforcement) — verified the 4 MiB cap is enforced on the `decode_create_batch_checked` create path. The `OP_STREAM_CHUNK` path uses a different cap (`max_stream_total_bytes`, 4 GiB default at `ServerConfig::DEFAULT_MAX_STREAM_TOTAL_BYTES`). Both write paths to the blob store are bounded but by different ceilings — intentional per threat model (one-shot CREATE vs streaming upload).
- `src/ops/engine.rs:2317` — the spend path DOES cross-check `ExternalRef.content_hash`. F-G9-002 is about other read paths that bypass this check.
- `src/recovery.rs:516` — F-G9-013 verifies the recovery-time reconciliation is correctly placed.
- `src/bin/server.rs:1059` — confirms the periodic blob-GC task is spawned during startup; F-G9-004 is about its concurrency contract being violated by live dispatch.

Audit-prior findings status:

- R-048 (BlobDigest propagated through `ColdDataRef::External`) — RESOLVED at create path; F-G9-002 highlights that the read paths do not enforce the same digest at read time (a separate concern, not a regression of R-048).
- R-049 (orphan-blob GC) — RESOLVED for recovery (race-free); F-G9-004 highlights a residual race in the periodic sweep that the authors documented but did not mitigate.
- R-089 (per-item cold_data cap) — RESOLVED at the wire/codec boundary. F-G9-006 notes the read-back path has no symmetric bound on `metadata.record_size`.
- R-051 (RMW pread-error propagation) — verified present at `src/storage/manager.rs:288` and `src/storage/input_refs.rs:95`.

Areas not exhaustively reviewed (acknowledged):

- `MemoryBlobStore` test-only paths — read but not deeply audited (test-only code, lower priority).
- Full `manager.rs` test module (lines 309-1463) — sampled; covers tier classification, R-048 regression, integrity-check firing. The test surface is good.
