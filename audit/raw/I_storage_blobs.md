# Audit Category I — Storage tiers and blobs

HEAD: `1e5659b` | scope: `src/storage/{manager,tiers,blobstore,uploader,blob_gc,input_refs}.rs`

Note on method: the primary files were read in full (tiers.rs, blobstore.rs, blob_gc.rs,
uploader.rs, input_refs.rs, mod.rs) and manager.rs lines 1–1303 (core logic + most of the
test module; the tail 1304–1607 is the remainder of `#[cfg(test)] mod tests`). The harness
Bash channel degraded mid-session, so cross-file grep into the dispatch/create caller could
not be completed. Findings that depend on the create-path caller are flagged with reduced
confidence accordingly. Findings that are fully contained within the storage module are
high/medium confidence.

---

## Findings

### I-01 (MEDIUM) — Tier classification is inclusive at the boundary but the module doc / inline-threshold semantics differ from the documented "< 8 KiB" contract; threshold is fixed at construction and `inline_threshold` is never wired to config

Location: `src/storage/tiers.rs:25-31`, `src/storage/manager.rs:146-152`, `src/storage/manager.rs:126-142`.

What's wrong:
- `mod.rs:4` and `tiers.rs:5` document the inline tier as "**< 8 KiB**", but both
  `tier_for_size` implementations use `data_size <= INLINE_THRESHOLD` (i.e. `<= 8192`),
  so exactly 8192 bytes is Inline, not External. The boundary is `<=`, not `<`. This is a
  doc/code mismatch, not a correctness bug per se — but the threshold semantics matter
  because the serialized cold payload (with its 12-byte length-prefix header from
  `ColdData::serialize`) is what is measured (`manager.rs:180-182`), so the effective
  user-data inline ceiling is 8192 − 12 = 8180 bytes, which neither the doc nor any test
  documents.
- `StorageManager.inline_threshold` (manager.rs:127) is initialized only to the compile-time
  `INLINE_THRESHOLD` constant in `new()` (manager.rs:141). There is no constructor or setter
  that takes a configurable threshold, despite the field doc saying "Configurable inline
  threshold (defaults to `INLINE_THRESHOLD`)." The field is effectively a constant. Not a
  money bug; flagged as dead-configurability / doc drift.

Why it matters: The transition points the audit asks about (7 KiB → Inline, 9 KiB → External)
are correct under the 8 KiB `<=` rule. There is no off-by-one that misroutes a payload to the
wrong tier in the tested range. The risk is purely operator confusion about the exact ceiling.

Reproduction / coverage: `manager.rs:497-525` tests 8000→Inline and 8193→External, but there
is NO test at exactly 8192 (the inclusive boundary) and none at 8191/8193 framed against the
"< 8 KiB" doc. The `tier_for_size` boundary IS exercised at INLINE_THRESHOLD in
`tiers.rs:137` (`tier_for_size(INLINE_THRESHOLD) == Inline`), so the inclusive behavior is
locked in by a passing test — meaning the doc is what is wrong, not the code.

Suggested fix: change the doc strings in `mod.rs:4` and `tiers.rs:5,10` to say "≤ 8 KiB"
(or "≤ 8192 bytes of serialized cold data"), or remove the unused `inline_threshold` field
and add a real config wire-through if a configurable threshold is intended.

Confidence: high (fully verified within module).

---

### I-02 (HIGH) — Growth-after-create into the External tier is structurally impossible to express durably, and the create-path tier decision is the ONLY place the tier is chosen; there is no guard preventing an inline record from later exceeding its allocated inline span

Location: `src/storage/manager.rs:162-209` (`write_cold_data`), `src/storage/tiers.rs:11-15`
(design note), `src/storage/manager.rs:154-160` (`inline_cold_offset`).

What's wrong: `write_cold_data` re-derives the tier from the *current* serialized size every
time it is called (manager.rs:180-182). For the Inline path it writes at
`record_offset + inline_cold_offset(utxo_count)` (manager.rs:186-187) using a read-modify-write
(`write_aligned`, manager.rs:412-437). The function does NOT take or check the originally
allocated record span. If a caller ever re-writes cold data for an existing inline record with
a larger payload (the "growth after create" the audit asks about), `write_aligned` will write
past the originally allocated record extent into whatever neighbouring record occupies the next
allocation slot — silent cross-record corruption — because nothing here bounds the write to the
allocated `record_size`. The tier may also flip Inline→External on the second call, but the
inline bytes from the first call are never reclaimed and the device offset is reused by the
allocator.

The design note at tiers.rs:11-15 confirms the architecture deliberately removed the
separate-NVMe middle tier "because record metadata has no durable fields for a separate
cold-data offset/length." That means an External record's cold-data location is implied solely
by `TxFlags::EXTERNAL` + the txid key; there is no durable in-record pointer that could be
updated to migrate an inline record to external in place. So growth-after-create is only safe
if the caller (create path) rejects it. Whether it does cannot be verified from the storage
module alone.

Why it matters: BSV records are immutable post-create for cold data (inputs/outputs are fixed
at tx creation), so in the normal pipeline this should never fire. But `write_cold_data` is a
public API with no guard. A second create on the same txid (idempotent retry, migration
re-apply, replica replay) that re-serializes a slightly different payload — or any future code
path that grows cold data — would corrupt the adjacent record with no error. The safe design
would be a debug_assert or hard check that `inline_cold_offset + serialized.len() <=
record_size` for the Inline branch.

Reproduction: no test exercises a second `write_cold_data` with a larger payload against an
already-written inline record. `inline_cold_data_offset_matches_formula` (manager.rs:572-607)
and `e2e_*` only ever write once per record. A reproducer: allocate `record_size_for(1) + 50`,
write 50-byte inline cold data, then call `write_cold_data` again with 5000 bytes — it will
silently `write_aligned` 5000 bytes starting at the inline offset, overrunning the 50-byte
inline span into the next allocation. Assert the neighbouring record's metadata is unchanged
(it will not be).

Suggested fix: add the original allocated cold span as a parameter (or recompute from
`record_size`) and return an error if `serialized.len()` exceeds the inline capacity for an
Inline-tier write; require the caller to choose the tier at allocation time, not at write time.

UPDATE after reading the create path (engine.rs:2044-2090): `Engine::create_record` chooses
the tier from `cold_size` at allocation time (engine.rs:2061-2066), allocates a record sized
exactly for that tier (Inline = METADATA_SIZE + slots + cold_size; External = METADATA_SIZE +
slots, engine.rs:2063-2066, 2078-2080), and rejects oversized inline cold data up front
(engine.rs:2070-2074). Inline cold data is then written into the allocated tail via
`write_cold_data_inline` (engine.rs:2087). Because BSV cold data is fixed at create and the
allocation is sized to the payload, the standard pipeline never re-writes a larger inline
payload, so the overrun does NOT occur on the production create path. The residual risk is
confined to the `StorageManager::write_cold_data` public API itself being un-guarded against
a future/erroneous second-or-larger write. Downgraded accordingly.

Confidence: medium (the un-guarded overrun is real in the public API; no production caller
currently triggers it — create sizes the allocation to the payload).

---

### I-03 (LOW) — `blobstore.rs:712-729` (`FileBlobStore::get_range`) computes `end = (offset + length) as usize` without overflow guard; a crafted `offset+length` near `u64::MAX` panics in debug / wraps in release before `.min(data.len())`

Location: `src/storage/blobstore.rs:726` (`let end = (offset + length) as usize;`), and the
equivalent `MemoryBlobStore::get_range` at `blobstore.rs:971`.

What's wrong: `offset + length` is an unchecked `u64` add. In release builds it wraps; the
subsequent `end.min(data.len())` then clamps to a small value and `data[start..actual_end]`
could panic if `actual_end < start` (start already validated `< data.len()`, but a wrapped
`end` could be `< start`). In the file backend `start >= data.len()` is checked first
(blobstore.rs:723) so `start < data.len()`, but a wrapped `end` smaller than `start` makes
`data[start..actual_end]` a reverse range → panic.

Why it matters: `get_range` is reachable from the streaming/SPV read path with caller-supplied
`offset`/`length`. A malformed range request could panic the worker thread. Low severity
because the full-payload digest verification runs first (reads whole blob), and the wire codec
likely caps these values upstream — but the storage layer should not rely on that.

Reproduction: `store.get_range(&key, data_len_minus_1, u64::MAX)` → `offset+length` wraps to a
small value `< start` → slice panic. No test covers a `length` that overflows when added to
`offset`; existing `file_get_range` (blobstore.rs:1135) uses tiny values.

Suggested fix: use `offset.saturating_add(length)` (or `checked_add` → return empty/clamped),
matching the defensive `saturating_sub` already used in manager.rs:259.

Confidence: medium-high (arithmetic is plainly unchecked; exact panic path verified by reading
the slicing logic).

---

## Verified-OK checklist items

1. **Threshold transitions 7 KiB → 9 KiB → 1.1 MiB land in the right tier.** `tier_for_size`
   (`manager.rs:146-152`, `tiers.rs:25-31`) is `<= 8192 → Inline`, else External. 7 KiB →
   Inline, 9 KiB → External, 1.1 MiB → External. Covered by `tier_8000_bytes_inline`,
   `tier_8193_bytes_external`, `tier_500k_external`, `tier_1m_plus_1_external`,
   `tier_320m_external` (manager.rs:497-525) and the e2e small/medium/large test
   (manager.rs:1145-1223) which asserts the actual `ColdDataRef` variant per tier, not just
   `Ok`. (Doc nit noted in I-01.)

2. **Missing blob → BLOB_NOT_FOUND.** `read_cold_data` for an EXTERNAL record maps a
   `get()==None` to `StorageError::ColdDataNotFound` (manager.rs:241-245) rather than silently
   returning empty cold data (the F-G9-001 fix). `stream_to` on the file backend returns
   `BlobError::NotFound` for a missing payload (blobstore.rs:783-788), covered by
   `file_stream_to_nonexistent` asserting the `NotFound` variant (blobstore.rs:1195-1202).

3. **Orphaned blobs (create rolled back) eventually cleaned.** `reconcile_orphan_blobs`
   (blob_gc.rs:243-254) deletes blobs whose index entry is absent (`deleted_no_index`) or
   present without `EXTERNAL` (`deleted_not_external`). Covered by `reconcile_mixed_set`,
   `reconcile_deletes_blob_with_no_index_entry`,
   `reconcile_deletes_blob_when_index_entry_lacks_external_flag` (blob_gc.rs:415-470), each
   asserting the specific stat counter AND `!exists` post-sweep.

4. **Blob-GC race / grace window correct.** The periodic sweep uses
   `list_for_gc(PERIODIC_GC_MIN_BLOB_AGE = 60s)` (blob_gc.rs:63, 253) which the file backend
   implements by excluding blobs whose payload OR sidecar mtime is newer than the cutoff
   (blobstore.rs:879-897, 494-512). It correctly takes the *later* of payload/sidecar mtime
   (blobstore.rs:502-505) so a recent sidecar rewrite still protects an older payload. Recovery
   uses the un-aged `list()` (race-free, no clients) (blob_gc.rs:224-234). The dispatch ordering
   (put-before-register) and the race it creates are documented in the concurrency contract
   (blob_gc.rs:122-134). The mtime grace window logic is sound. (Note: the in-memory backend's
   `list_for_gc` falls back to un-aged `list()` per the default trait method blobstore.rs:314-316,
   which is fine for tests where the race cannot manifest.)

5. **Concurrent uploads to same txid handled.** `FileBlobStore` uses 256 per-key mutexes keyed
   by `key[0]` (blobstore.rs:356, 376-378). `put`, `get`, `get_range`, `stream_to` (snapshot),
   and `FileStreamWriter::finish` all take the per-key lock (blobstore.rs:677, 707, 717,
   778-792, 618). The atomic tmp+rename pattern (blobstore.rs:195-206, 676-698) ensures the
   final blob always matches exactly one writer's complete payload. Covered by
   `file_concurrent_puts_same_key_do_not_corrupt_blob` (blobstore.rs:1231-1264) which barriers
   16 threads on one key and asserts the final blob equals one complete payload and the sidecar
   is readable. The F-G9-005 reader-lock comments (blobstore.rs:700-707) correctly explain the
   payload/sidecar consistency window the lock closes.

6. **blobstore_path not-writable / full surfaces an error rather than panicking.**
   `atomic_write_no_dir_fsync` propagates `File::create`/`write_all`/`sync_all` errors via `?`
   (blobstore.rs:195-206); `put` returns them (blobstore.rs:676-698). ENOSPC on `write_chunk`
   propagates as `BlobError::Io` (blobstore.rs:602-615) and the `Drop`/`abort` backstop removes
   the partial `.tmp` (blobstore.rs:652-672). Covered by `file_put_non_writable_dir`
   (blobstore.rs:1267-1271) asserting `is_err()` on a non-existent path. No `unwrap`/`expect`
   on the I/O paths; all return typed `BlobError`.

7. **Crash-atomicity & integrity.** Payload and sidecar each written tmp→fsync→rename, then
   parent dir fsync'd (blobstore.rs:676-698, 617-650). `exists` requires both payload and
   sidecar (blobstore.rs:748-755); `list` returns only complete pairs (blobstore.rs:483-489).
   Read paths verify length then SHA-256 against the sidecar (blobstore.rs:554-577), and the
   manager additionally cross-checks the record-anchored `ExternalRef.content_hash`
   (manager.rs:246-252, F-G9-002). Tampering/truncation/missing-sidecar all covered with
   variant-specific assertions (blobstore.rs:1275-1368).

8. **Async uploader rollback & backpressure.** On `write_external_ref` failure the uploader
   deletes the just-uploaded blob and surfaces the device error (uploader.rs:235-255,
   F-G9-008); the bounded `sync_channel` rejects with `UploaderQueueFull` via `try_send`
   instead of blocking or growing unbounded (uploader.rs:286-327, F-G9-003). `content_hash` is
   the real payload SHA-256, not the txid, asserted by `submit_and_wait` (uploader.rs:401-402).

9. **input_refs read-modify-write correctness.** Head/tail bytes of the aligned RMW block are
   preserved by propagating the pread (input_refs.rs:94-96, R-051), preventing zero-fill
   corruption of neighbouring records. Round-trip and independence covered
   (input_refs.rs:178-265).

## Could not fully verify (harness Bash degraded mid-session)
- The create-path caller in `src/server/dispatch.rs` / `src/ops/*` that chooses the tier,
  orders blob-put vs index-register, and whether it rejects growth-after-create (relevant to
  I-02). Recommend a follow-up grep of `write_cold_data`/`begin_stream`/`TxFlags::EXTERNAL`
  call sites in the dispatch and ops modules.
