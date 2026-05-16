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
- Commit: pending
- Files changed: `tests/g9_001_cold_data_not_found.rs` (new test); code in
  `src/storage/manager.rs:229-253` (new `StorageError::ColdDataNotFound` and
  read-path branch) already present in the baseline snapshot.
- Test added: `tests/g9_001_cold_data_not_found.rs::external_record_with_missing_blob_returns_cold_data_not_found`.
- Notes: stamp a record with `TxFlags::EXTERNAL` but never put the blob; assert
  `read_cold_data` returns `StorageError::ColdDataNotFound { key: "dead..." }`
  instead of an empty `ColdData`. Matches the recommendation in F-G9-001
  exactly.
