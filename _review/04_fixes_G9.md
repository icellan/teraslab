# G9 — Storage tiers fix log

Scope: `src/storage/{blobstore,manager,blob_gc,uploader,tiers,input_refs,mod}.rs`.
Source: `_review/02_findings_G9.md` (17 findings).

Notes on the baseline (`3c76ecf`):

- The pre-review WIP snapshot at `8920447` already carried in-tree code changes
  for **F-G9-001**, **F-G9-002**, **F-G9-006**, and **F-G9-016** (the new
  `StorageError::ColdDataNotFound` / `ContentHashMismatch` / `ColdDataTooLarge`
  variants, `verify_content_hash` helper, the deterministic `MAX_COLD_DATA_READ_BYTES`
  bound, and the doc clarifying the 1:1 txid:blob invariant). Those edits were
  never committed as individual review fixes — they live inside the snapshot.
  We deliver the regression tests as the per-finding commit and reference the
  baseline code edits in the commit body.
- Pre-existing test-target compilation errors in `src/index/redb_primary.rs`
  (owned by G3) prevent `cargo build --tests` for the lib's internal `#[cfg(test)]`
  modules. Integration tests under `tests/` build and run fine and are how we
  gate every fix in this group. The end-of-group `cargo test --all` will be
  noted accordingly.

---

### F-G9-001 — FIXED
- Commit: 6d8986d
- Files changed: `tests/g9_001_cold_data_not_found.rs` (new test); code in
  `src/storage/manager.rs:229-253` (new `StorageError::ColdDataNotFound` and
  read-path branch) already present in the baseline snapshot.
- Test added: `tests/g9_001_cold_data_not_found.rs::external_record_with_missing_blob_returns_cold_data_not_found`.
- Notes: stamp a record with `TxFlags::EXTERNAL` but never put the blob; assert
  `read_cold_data` returns `StorageError::ColdDataNotFound { key: "dead..." }`
  instead of an empty `ColdData`. Matches the recommendation in F-G9-001
  exactly.

### F-G9-002 — FIXED
- Commit: pending
- Files changed: `tests/g9_002_content_hash_mismatch.rs` (new test); code
  (`verify_content_hash` helper + cross-check in `read_cold_data`'s EXTERNAL
  branch at `src/storage/manager.rs:80-112,251-252`) already present in the
  baseline snapshot.
- Tests added:
  - `tests/g9_002_content_hash_mismatch.rs::blob_payload_disagreeing_with_record_anchored_digest_fails`
  - `tests/g9_002_content_hash_mismatch.rs::zero_record_anchored_digest_tolerated_for_legacy_records`
- Notes: the first test simulates a coordinated payload+sidecar swap by
  stamping the record with the SHA-256 of payload A and then putting payload
  B in the (Memory) blob store — the in-store digest passes the sidecar
  check but the record-anchored cross-check catches the swap. The second test
  confirms the legacy `[0; 32]` placeholder is tolerated with a warn so we do
  not break upgrades.

### F-G9-003 — FIXED
- Commit: pending
- Files changed: `src/storage/uploader.rs`, `src/storage/blobstore.rs`,
  `tests/g9_003_uploader_bounded_queue.rs`.
- Tests added:
  - `tests/g9_003_uploader_bounded_queue.rs::submit_returns_uploader_queue_full_when_saturated`
  - `tests/g9_003_uploader_bounded_queue.rs::default_capacity_matches_documented_constant`
- Notes: replaced `std::sync::mpsc::channel` with `std::sync::mpsc::sync_channel`
  bounded by `DEFAULT_UPLOADER_QUEUE_CAPACITY = 1024`. `submit` uses
  `try_send` and returns the new `BlobError::UploaderQueueFull` variant on a
  full queue, also incrementing a `queue_full_count` atomic counter (exposed
  via `BlobUploader::queue_full_count()`) and emitting a `tracing::warn!` at
  `target = "teraslab::storage::uploader"` so operators can wire it to an
  alert. The test wedges the background thread with a custom `BlobStore`
  whose `put` spins on a release flag, then asserts saturation produces the
  documented error.
