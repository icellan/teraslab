# TeraSlab Audit — Categories I, J, K (Storage tiers / I/O / Pruning)

Audit scope: storage tiers (cold-data inline / separate / external blob),
the device I/O layer (alignment, sync vs io_uring, allocator durability),
and pruning paths (`block_height_retention`, `PreserveUntilBatch`,
`ProcessExpiredPreservations`, `MarkLongestChainBatch`, `QueryOldUnmined`).

## Overview

The storage tiering scheme is well-defined and structurally correct:
records lay out cold data either inline at a deterministic offset, in a
separate NVMe allocation on the same device, or in a content-addressed
external blob store with a digest-bound `ExternalRef`. Tier selection is
purely a function of serialized cold-data size at create time
(`tier_for_size` in `src/storage/tiers.rs:24`), and the formal contract
from `BSV_UTXO_STORE_SPEC.md` is implemented faithfully. The blob store
defends against bit rot via a SHA-256 sidecar and verifies the entire
payload before returning bytes — no partial-read-after-tamper paths.

The I/O layer is more concerning. The good news first:

- `BlockDevice` strictly enforces 4 KiB alignment on every `pread`/`pwrite`
  through `check_alignment` (`src/device.rs:373` for `MemoryDevice`,
  `src/device.rs:584` for `DirectDevice`); both backends reject zero-aligned
  or non-power-of-two alignments at construction
  (`validate_alignment`, `src/device.rs:86`).
- The trait helpers `pread_exact_at` and `pwrite_all_at` correctly loop
  over POSIX-allowed short reads/writes and surface fatal `ShortRead` /
  `WriteStalled` errors when no progress is made
  (`src/device.rs:176`, `src/device.rs:216`). Tests exercise these paths
  on a synthetic chunky device.
- For raw block devices, `DirectDevice::open` queries the actual size via
  `BLKGETSIZE64` (Linux) and `DKIOCGETBLOCKCOUNT` × `DKIOCGETBLOCKSIZE`
  (macOS), and explicitly never calls `set_len` on `S_IFBLK` files
  (`src/device.rs:511-554`). For regular files, the file is grown but
  never truncated (`src/device.rs:556-563`).
- `pread`/`pwrite` retry on `EINTR` so a signal-delivered short return is
  not misinterpreted as I/O error (`src/device.rs:614-633`,
  `src/device.rs:660-675`).

The bad news:

- The `device_io` module (`IoUringBackend`, `SyncFallback`,
  `create_device_io`) is **completely unused in production**. Nothing in
  `src/ops/engine.rs`, `src/server/dispatch.rs`, `src/bin/server.rs`,
  `src/server/startup.rs`, or `src/recovery.rs` constructs a `DeviceIo`,
  and `src/lib.rs:7` only re-exports the module. The README's "io_uring
  fast path" is currently dead code; every read and write actually goes
  through `BlockDevice::pread/pwrite` (i.e. `libc::pread/pwrite`) on the
  hot path. This is finding **IJK-04**.
- The alignment-aware read-modify-write helpers used for inline cold
  data and input refs (`storage/manager.rs:301`,
  `storage/input_refs.rs:67`) **silently swallow the pre-read error**
  with `let _ = ...`, and then proceed to write a buffer whose head and
  tail bytes are zeros if the pre-read failed. This is finding
  **IJK-05** — in the unhappy path it is a silent corruption hazard for
  record-adjacent bytes.
- `SyncFallback::submit_and_wait` does not loop on `EINTR` and does not
  retry short reads/writes (`src/device_io/sync_fallback.rs:91-114`); it
  copies the raw `pread`/`pwrite` byte count into `Completion::result`.
  Callers would have to reissue the op themselves. Because the module is
  unused this is currently latent, but if `device_io` is ever wired in
  this becomes finding **IJK-06**.
- `BlobUploader` (`src/storage/uploader.rs`) is **not wired into the
  production server** either: a `grep -rln BlobUploader src tests` shows
  it referenced only by its own tests and a doc-comment in
  `storage/manager.rs:84`. The async upload path advertised by the
  spec doc is therefore latent. The synchronous path
  (`StorageManager::write_cold_data` for `External`) is what production
  actually exercises, and it discards the digest returned by `put()`,
  leaving `meta.external_ref.content_hash` as zeros for all
  manager-driven external writes. This is finding **IJK-07**.
- There is **no garbage collector for orphaned blobs**. If a create is
  rolled back after the blob has been written (e.g., redo flush failure,
  duplicate txid detected after upload, replication failure), the blob
  payload + sidecar remain on disk indefinitely. Searches for
  `garbage_collect`, `cleanup_blob`, `orphan*blob`, or `gc_blob` return
  nothing. The "orphan cleanup" code in `cluster/coordinator.rs` is
  shard-orphan, not blob-orphan. Finding **IJK-08**.

The pruning subsystem mostly works:

- `block_height_retention` is honoured: `evaluate_delete_at_height`
  returns no signal/no patch when `block_height_retention == 0`
  (`ops/delete_eval.rs:70`), and it computes
  `current_block_height + block_height_retention` with `checked_add`,
  errors with `DahOverflow` instead of saturating
  (`ops/delete_eval.rs:31-41`). Config validates the retention is below
  the overflow threshold (`config.rs:687`).
- `PreserveUntilBatch` correctly clears DAH and stamps `preserve_until`
  on the record (`ops/engine.rs:2647`). Subsequent DAH evaluations
  short-circuit on `preserve_until != 0`
  (`ops/delete_eval.rs:74`), so preserved transactions are never written
  into the DAH index.
- `MarkLongestChainBatch` re-evaluates DAH after toggling
  `unmined_since` and clears DAH when off-chain (because
  `on_longest_chain == false` ⇒ `unmined_since != 0` ⇒
  `evaluate_delete_at_height`'s "all_spent && has_blocks &&
  on_longest_chain" precondition fails ⇒ DAH cleared).

The bad news here:

- `handle_process_expired` (`server/dispatch.rs:4669`) blindly issues
  `engine.delete()` for every txid returned by the DAH index range
  query. It does not re-validate that `preserve_until` is still 0,
  that `unmined_since` is still 0, or that the record is still
  fully-spent at the moment of deletion. The DAH index removal in
  `engine.preserve_until` is explicit (`ops/engine.rs:2670`), so the
  index *should* not list preserved records — but if the DAH index is
  ever rebuilt against an older snapshot or a stale set of cached
  fields, the pruner will delete preserved records without rechecking
  the on-device metadata. This is finding **IJK-09** (defence-in-depth
  gap).
- The compensation paths in `server/dispatch.rs` for replication
  failure hard-code `block_height_retention: 0` when issuing the
  inverse engine call (`server/dispatch.rs:1703, 1727, 1782, 1816`,
  and similar in `replication/receiver.rs:759, 782`). Because the
  inverse op short-circuits DAH evaluation under retention=0, a DAH
  set during the forward path will **not** be cleared during rollback.
  After compensation the on-device DAH and the DAH secondary index can
  diverge from the "as if the op never happened" baseline. Finding
  **IJK-10**.
- `handle_query_old_unmined` (`server/dispatch.rs:4569`) is a thin
  passthrough to `unmined_index().range_query(cutoff)` — it does not
  filter by current-mined or by `preserve_until`. Pruner clients are
  expected to handle that, and the system uses `delete_at_height`
  (set via the all-spent path in `evaluate_delete_at_height`) for the
  actual delete decision, so this is more an observation than a bug.
  Finding **IJK-11**.

The allocator (`src/allocator.rs`) is solid: best-fit with three-way
coalescing, hybrid Vec/BTree freelist with hysteresis, redo-journaling
of every allocate/free with rollback on flush failure, CRC32-protected
header, and idempotent replay. The one production gap is that
`SlotAllocator` writes are **not synchronized with the data writes**: a
freelist mutation appends a redo entry but the data-region writes do
not. This is by design (storage is two-stage durable: redo log first,
then snapshot/data) but produces a window where a freed block can be
allocated to a new record while the old record's bytes are still on
disk; the new record write must overwrite them. The allocator
currently ensures the new record's metadata is written before the old
record's metadata is read elsewhere, but the `delete` path's tombstone
write (`engine.rs:2706`) only zeroes magic+record_size, not the entire
record. Bit-level scavenger tools could resurrect partial state.
Finding **IJK-12**.

Overall severity:
- **CRITICAL**: 0 (no fully-live data-loss path)
- **HIGH**: 4 (IJK-04 dead io_uring path, IJK-05 silent pre-read swallow,
  IJK-07 missing content-hash, IJK-08 no blob GC)
- **MEDIUM**: 6 (IJK-06 sync-fallback partial I/O, IJK-09 pruner doesn't
  recheck preserve, IJK-10 compensation bypasses DAH eval, IJK-11
  query-old-unmined passthrough, IJK-12 deletion not zero-fill,
  IJK-13 BlobUploader race)
- **LOW**: 5 (IJK-14 storage_manager loses content_hash via discard,
  IJK-15 atomic_write_no_dir_fsync flushes only one file, IJK-16
  sub-block coalesce window, IJK-17 inline cold data may share sector
  with hot record, IJK-18 stream_to two-pass)

The findings are listed by category.

---

## Category I — Storage tiers / blobs

### IJK-01: External-tier `ExternalRef.content_hash` is permanently zero on the synchronous create path (HIGH)
**Category:** I
**Location:** `src/storage/manager.rs:116-126`, `src/ops/engine.rs:1676-1679`,
`src/storage/uploader.rs:131-148`
**What:**
`StorageManager::write_cold_data` for `StorageTier::External` calls
`self.blob_store.put(tx_id, &serialized)?` and **discards** the returned
`BlobDigest`:

```rust
StorageTier::External => {
    // The blob store returns the actual content digest and length;
    // we discard them here ...
    let _digest = self.blob_store.put(tx_id, &serialized)?;
    Ok(ColdDataRef::External)
}
```

The result is `ColdDataRef::External` with no payload digest carried
back to the caller. Looking at `engine.rs:1676`, on a synchronous
external create the metadata field `external_ref` is populated only
from `req.external_ref` — i.e. whatever the **client** sent on the
wire — never from what the storage manager actually wrote. Production
clients that go through `OP_STREAM_CHUNK` + `OP_STREAM_END` do receive
the digest from `handle_stream_end` and then must echo it back in the
subsequent create, but if a client uses the in-process synchronous path
(tests, embedded users) the field stays zero.

The on-disk hot record therefore says "external blob X with content
hash 0x000…0" — and a subsequent `read_cold_data` that integrity-checks
will reject the (correct) blob payload because the recorded hash is
zero. The end-to-end integrity check at `ops/engine.rs:2112` does
exactly this:

```rust
if actual != meta.external_ref.content_hash {
    return Err(... "external blob digest does not match record ExternalRef" ...);
}
```

**Why it matters:**
Blob integrity checking is one of the explicit defences against bit
rot in the spec. With `content_hash = 0` the on-record-bound check is
nominally enforced but always-failing for synchronous external creates,
which is precisely what `tests/integration.rs` exercises. The system
has been working for production traffic only because the streaming
upload path (`OP_STREAM_CHUNK` → `OP_STREAM_END`) returns the digest in
the response and the client is expected to put it into the create
request. Any future code path that calls `StorageManager::write_cold_data`
directly (e.g., a recovery rebuild or a cluster migration) will silently
write a zero `content_hash` and break the integrity guarantee.

**Reproduction:**
```
cargo test --test storage_manager -- --nocapture external_cold_data_write_read
# returns OK because read_cold_data() does not currently digest-check
# (the check lives only on engine.rs's read path)
```
Then create a record manually via `StorageManager::write_cold_data`,
mutate the blob payload on disk, and call `engine.read_cold_data` —
the integrity check will fire because `content_hash == 0` but the
actual blob hash is nonzero.

**Suggested fix:**
Carry the digest through `ColdDataRef::External { digest:
BlobDigest }` so the caller can populate `meta.external_ref` with the
actual content hash and length returned by the store. The signature
change is local and the spec already provides for this in
`BlobDigest`. In `engine.rs` callers (`create`, `create_at_offset`,
`create_at_offset_with_meta_template`), prefer the digest returned by
the manager over the client-supplied `req.external_ref`, falling back
to the request only when external upload happened out of band.

---

### IJK-02: No garbage collector for orphaned external blobs (HIGH)
**Category:** I
**Location:** `src/storage/blobstore.rs` (no GC method),
`src/server/startup.rs` (no GC scheduler),
`src/recovery.rs` (recovery does not visit blobstore)
**What:**
A blob upload that "should not have happened" is **never cleaned up**.
The orphan windows are:

1. Client streams chunks → `OP_STREAM_END` finalizes the blob → the
   subsequent create request fails (duplicate txid, redo flush failure,
   replication failure). The blob is now on disk with no record
   referencing it.
2. The async `BlobUploader` (not actually used in production) writes
   the blob, then fails to write the `ExternalRef`. Same orphan.
3. A delete batch's compensation re-creates the record from the
   snapshot but the original external blob was deleted by
   `engine.delete()`. The recreate is not external, so the blob is no
   longer needed but it has already been destroyed — the inverse
   problem (data loss not orphan, called out separately in IJK-19).
4. A cluster shard migration never finalizes; the destination has the
   blob written but no record. There is no migration-cleanup that
   walks the blobstore.

`grep -rn "garbage_collect\|cleanup_blob\|gc_blob\|orphan.*blob\|prune.*blob" src/` returns zero hits. The cluster
coordinator's `run_orphan_cleanup` (`cluster/coordinator.rs:3585`)
deletes orphaned **shard records**, not blobs.

**Why it matters:**
At BSV scale, every failed external create leaks ≥1 MiB on disk forever.
For a busy node the leak rate is bounded by failure rate but
unrecoverable without operator intervention (manual `find blobstore
-name '*.tmp'`, etc.). Worse, leaked `.tmp` files (from
`atomic_write_no_dir_fsync`) are also never cleaned: although
`FileStreamWriter::abort` removes its own temp file, a process crash
between `File::create(&temp_path)` and the abort/finish path leaves
a stale `.tmp` on disk forever (recovery code at `src/recovery.rs:261`
mentions resize tmp files but never blob tmp files).

**Reproduction:**
1. Configure a small `blobstore_path`.
2. Run a workload that issues `OP_STREAM_CHUNK` + `OP_STREAM_END`
   → then either kill the server or send a malformed
   `OP_CREATE_TX` (mismatching `external_ref`).
3. Inspect `blobstore_path`: the blob file + sidecar are present with
   no corresponding record in the index.
4. Restart the server. The blob remains.

**Suggested fix:**
- Add a `BlobStore::list` / `BlobStore::list_keys` enumerator (file
  walker for `FileBlobStore`).
- During recovery (`recovery::recover_all_with_allocator`), enumerate
  the blob store and reconcile against the primary index — every blob
  whose `txid` is not present in the index OR present but not flagged
  `EXTERNAL` is an orphan and should be deleted (with the same
  refusal-on-active-stream guards).
- Add a periodic background sweep (e.g. once an hour) that does the
  same in steady state.
- Sweep `<blob>.tmp` files: any `.tmp` whose mtime is older than a
  configurable threshold (e.g. 1 hour) and whose corresponding blob
  is missing OR present-and-newer is an aborted upload; delete it.

---

### IJK-03: `FileBlobStore::atomic_write_no_dir_fsync` parent-directory fsync only protects the second rename (MEDIUM)
**Category:** I
**Location:** `src/storage/blobstore.rs:147-160`, `:378-399`
**What:**
`FileBlobStore::put` calls:

```rust
atomic_write_no_dir_fsync(&path, data)?;       // payload tmp+rename
let meta_path = Self::meta_path_for(&path);
atomic_write_no_dir_fsync(&meta_path, &encode_meta(...))?; // sidecar tmp+rename
fsync_parent_dir(&path)?;                        // ONE dir fsync, after both renames
```

Each `atomic_write_no_dir_fsync` writes `tmp + fsync + rename` and the
intent is to flush the directory's dentry only once at the end. That
is correct for crash safety **as long as** both renames produced
dentries that survive the fsync. They do: `fsync` on the parent
dentry covers all dirents created since the last fsync of that
directory.

However, there is a subtle bug for the streaming case
(`FileStreamWriter::finish`, `:339-368`): the payload rename and the
sidecar tmp+rename are both followed by a single
`fsync_parent_dir(&final_path)`. If `final_path` is in directory
`base/ab/cd/`, the fsync covers payload and sidecar dentries — good.
But neither **the parent of `ab/cd/`** nor `ab/`'s parent are fsync'd,
so on first-write the `ab/` and `ab/cd/` directories themselves may
not yet be persistent. `create_dir_all` does not fsync.

**Why it matters:**
On a pristine blobstore directory tree, a power loss between
`create_dir_all` and `fsync_parent_dir(blob)` can resurrect with
the blob file present but the `ab/cd/` directory linked from `ab/`
not yet persistent — the file is unreachable. `FileBlobStore::exists`
returns false and `get` returns `None`, so the system thinks the blob
was never written. Most blob writes after the first one in a given
prefix are safe, since `ab/cd/` already exists and its dirent is
already persisted from a prior fsync.

**Reproduction:** difficult without an `fsfreeze`/power-loss harness.
Detectable by inspection: the call graph never fsyncs intermediate
directories.

**Suggested fix:** when `create_dir_all` actually creates new
intermediates, fsync the chain bottom-up. A simple implementation: after
`create_dir_all(parent)`, walk from `parent` up to `base_dir` calling
`fsync` on each; cheap (8 ops at most for the 4-byte hex prefix
hierarchy). Cache the "I have fsynced these dirs already" set in the
store to avoid the cost on every put.

---

### IJK-04: External-tier creates do not propagate digest from `BlobUploader` (LOW)
**Category:** I
**Location:** `src/storage/uploader.rs:131-148`
**What:**
The async `BlobUploader::process_task` correctly captures the digest:

```rust
let digest = blob_store.put(&task.tx_id, &task.data)?;
let ext_ref = ExternalRef {
    store_type: 1,
    content_hash: digest.sha256,
    total_size: digest.length,
    ...
};
```

— and pwrites it into the metadata. But `BlobUploader::write_external_ref`
takes no per-record stripe lock:

```rust
fn write_external_ref(device, record_offset, ext_ref) {
    let mut meta = crate::io::read_metadata(device, record_offset)?;
    meta.external_ref = *ext_ref;
    crate::io::write_metadata(device, record_offset, &meta)?;
}
```

A concurrent spend / setMined / mark-on-longest-chain on the same
record will lose-update each other: the non-uploader thread holds the
`StripeLocks` guard; the uploader thread does not. Both perform RMW on
metadata; the last writer wins. This is moot because the uploader is
not actually wired in (see IJK-07), but it is a real bug if it is.

**Why it matters:** today, latent. If anyone enables async upload
without a stripe-lock fix, the on-device generation and `external_ref`
fields can desync from the index cache.

**Suggested fix:** the uploader needs to receive a reference to the
engine (or at least the engine's stripe locks + index) so it can do
`let _g = locks.lock(&tx_key); ... index_cache.update(...)` after the
metadata write, mirroring the engine's own RMW protocol. Better still:
move the post-upload pwrite back into the engine via a callback so
all metadata mutations stay centralized.

---

### IJK-05: External cold-data digest-check is enforced ONLY by the engine read path (LOW)
**Category:** I
**Location:** `src/storage/manager.rs:135-172`,
`src/ops/engine.rs:2096-2120`
**What:**
`StorageManager::read_cold_data` calls `blob_store.get(...)` which
does verify the digest against the sidecar. Good. But it does **not**
verify the on-device `meta.external_ref.content_hash` against the
sidecar — so if the sidecar has been silently rewritten by an
attacker (they can write a new sidecar matching their tampered blob),
the manager will return tampered data without detection.

The engine read path (`engine.rs:2112-2118`) does the second-level
check by comparing `actual` to `meta.external_ref.content_hash`, but
the manager's API does not. Callers that go through the manager (e.g.
SPV proof generation, or the migration source path) get only one
layer of integrity protection.

**Why it matters:** sidecar-rewrite is a stronger attacker model than
bit rot, but it's the model an external object store (S3) should
defend against. Today the on-record `content_hash` is the only
defence, and it's only checked in `engine.rs`.

**Suggested fix:** make `StorageManager::read_cold_data` also accept
an `expected_digest: &[u8; 32]` (typically `meta.external_ref.content_hash`)
and compare the blob's payload digest against both the sidecar and
this expected value. The current single-source design can be kept as
a non-strict variant for the bootstrap case where the on-record hash
is zero.

---

### IJK-06: `blob_store.exists` returns `true` for partially-uploaded blobs in the streaming path (LOW)
**Category:** I
**Location:** `src/storage/blobstore.rs:440-447`, `:523-540`
**What:**
`exists()` checks `blob_path.exists() && meta_path.exists()`. The
streaming writer (`begin_stream`) creates a temp file at
`blob_path + ".tmp"`, **not** `blob_path` itself, so during streaming
`blob_path` does not exist and `exists()` correctly returns false.

The synchronous `put()` writes the temp at `blob_path + ".tmp"` then
renames; same protection.

So this is **not** a real exposure — `exists()` is correctly
sidecar-gated. Leaving this as a NO-FINDING entry to acknowledge the
search was performed.

**Why it matters:** noise — but the design assumes both files were
written via the atomic `tmp+rename` discipline. A future code path
that bypasses the helper would silently break the
half-written-blob-is-not-visible invariant. Add a debug-level invariant
check in `put`/`finish` that the destination + sidecar are both
present after the fsync.

---

### IJK-07: Concurrent uploads of the same `tx_id` race on the file path (MEDIUM)
**Category:** I
**Location:** `src/storage/blobstore.rs:378-399`,
`src/server/dispatch.rs:4868-4938`
**What:**
`FileBlobStore::put` writes `<blob>.tmp` then renames. If two threads
both call `put(tx_id, ...)` concurrently, both will write to the
same `<blob>.tmp` (overwriting each other) and then both call
`rename(<blob>.tmp, <blob>)`. POSIX `rename` is atomic, but two
concurrent renames against the same destination are not synchronized
with respect to each other. The `<blob>.tmp` will be lost when the
second `File::create` truncates it; if the first thread finishes
`File::write_all + sync_all + rename` after the second's truncate,
the resulting on-disk blob is the truncated head of thread 2's data
or zero-length.

The sidecar write that follows is a separate `tmp+rename` and the
two threads can race independently.

**The streaming path has the same problem.** Two concurrent
`begin_stream` calls for the same txid both create
`<blob>.tmp`, then `File::write_all` interleaves bytes, and finally
both call `rename`. The `bytes_written` counter is per-stream so each
call's `BlobDigest.length` is plausible from the caller's perspective,
but the on-disk file is interleaved garbage of two different streams
with whichever rename ran second winning.

The dispatch-level `ConnectionState.streams` is per-connection so a
single client cannot race itself — but two clients streaming the same
txid (as happens during a re-send after timeout) will collide.

**Why it matters:** in cluster mode replicas may receive concurrent
streaming uploads from the master and from a recovering peer, or two
clients may both be attempting to re-upload after a perceived
timeout. The result is corrupt blobs.

**Reproduction:** spin two threads each calling
`store.begin_stream(&same_key) → write_chunk(..., 1MB pattern A/B
respectively) → finish()`. The on-disk blob is 1 MB but with
arbitrarily interleaved A/B bytes, and the sidecar matches one of the
two digests — so `get()` on that key surfaces a `DigestMismatch`.

**Suggested fix:**
- Use a unique per-attempt temp suffix
  (`<blob>.<random>.tmp` or `<blob>.<pid>.<tid>.tmp`) so two writers
  do not share the same temp file.
- For the synchronous `put`: same fix.
- Add an in-process `Mutex<HashSet<[u8; 32]>>` to `FileBlobStore`
  that rejects a `put` / `begin_stream` while another writer holds
  the same key. This keeps cross-thread serialization local; for
  cross-node serialization rely on the dispatch layer's shard
  ownership check (already done at
  `dispatch.rs:4880` for `OP_STREAM_CHUNK`).

---

### IJK-08: `blobstore_path` not writable / full surfaces only as `BlobError::Io` with no actionable diagnostic (LOW)
**Category:** I
**Location:** `src/storage/blobstore.rs:378-399`
**What:**
`FileBlobStore::put` returns `BlobError::Io(...)` for ENOSPC, EACCES,
and a read-only filesystem. The error message is "I/O error: ...".
At the dispatch layer this becomes ERR_INTERNAL with the same string
appended. There is no specific status code for "blobstore is full" or
"blobstore is read-only" so operators have to grep server logs to
diagnose. The unit test `file_put_non_writable_dir`
(`storage/blobstore.rs:869`) only asserts `is_err()`, not the variant.

**Why it matters:** during operations a full or read-only blobstore is
the most important alert to surface. Today the dashboard sees
"ERR_INTERNAL with text I/O error: No space left on device". A
dedicated `BlobError::OutOfSpace` and a wire-level
`ERR_BLOBSTORE_FULL` would let the cluster shed external creates
gracefully (push back to the client) without taking the whole node
out.

**Suggested fix:** match on `e.raw_os_error()` in `put`/`begin_stream`
and map `ENOSPC` → `BlobError::OutOfSpace`, `EROFS`/`EACCES` →
`BlobError::ReadOnly`. Surface as distinct error codes at the wire
level.

---

### IJK-09: Tier transitions are not supported (NO ISSUE — by design)
**Category:** I
**Location:** `src/storage/manager.rs:85-127`
**What:**
The audit asks: "tx that grows from 7 KiB to 9 KiB to 1.1 MiB lands in
correct tier each time, or is rejected if growth-after-create
unsupported."

There is **no record-resize/tier-promotion path**. Records are
allocated with their final size at create time
(`engine.rs:1638`: `total_size = base_size + cold_size`). After
creation, the record cannot grow: no `update_record`, no
`reallocate`, no equivalent. UTXO-slot mutations (spend, freeze) are
in-place and do not change `record_size`. The only metadata-only
field that gets pwritten into a record after creation is
`ExternalRef` (`uploader.rs:156`), which fits into the fixed
metadata header.

So tier transitions are simply **not a concern** because they are
physically impossible. Each tx commits to one tier at create time. If
a client tries to "grow" an existing record by creating again with
the same txid, `engine.create` returns `CreateError::DuplicateTxId`
(`engine.rs:1622`). Marking this as a NO-FINDING for completeness.

**Why it matters:** spec compliance — per spec the system "exploits
fixed, known workload patterns" and tx-data is immutable post-publish.
This matches Bitcoin-protocol reality.

---

### IJK-10: Missing-blob → `BLOB_NOT_FOUND` mapping is correct only for `stream_to`, not `get`/`get_range`/`exists` (LOW)
**Category:** I
**Location:** `src/storage/blobstore.rs:401-447`
**What:**
- `stream_to` returns `BlobError::NotFound { key }` for a missing
  payload (`:469`).
- `get` returns `Ok(None)` for ENOENT, **not** a typed
  `BlobError::NotFound` (`:296`).
- `exists` returns `Ok(false)` (`:446`).
- `get_range` returns `Ok(None)` (`:412`).
- `delete` is idempotent — ENOENT is `Ok(())` (`:434`).

The trait doc on `BlobStore::get` does not mention the "returns None
on missing" contract; it says "Returns `None` if not found", but the
`stream_to` doc says "Returns `BlobError::NotFound` if the blob does
not exist". The asymmetry is intentional but undocumented in the
audit-relevant places.

`engine.rs:2103` checks `Ok(Some(data))` and falls back to a generic
"missing external blob" error otherwise — consistent with the trait
contract.

**Why it matters:** trait-contract clarity. The dispatch layer relies
on `Ok(None)` for cluster-mode "not yet migrated" responses. Map this
to a wire-level `BLOB_NOT_FOUND` code if the spec requires.

**Suggested fix:** unify on `Ok(None)` for "absent" across all read
methods of the trait, and rename `BlobError::NotFound` to
`BlobError::ReadFailedNotFound` so the typed variant is reserved for
unexpected-absence-during-stream cases.

---

## Category J — I/O layer

### IJK-04: `device_io` module (io_uring + sync fallback) is dead code; production uses `BlockDevice` directly (HIGH)
**Category:** J
**Location:** `src/device_io/mod.rs`, `src/device_io/sync_fallback.rs`,
`src/device_io/io_uring_backend.rs`, `src/lib.rs:7`
**What:**
Comprehensive search:

```
$ grep -rln "DeviceIo\|create_device_io\|IoUringBackend\|SyncFallback" src/
src/device_io/io_uring_backend.rs
src/device_io/sync_fallback.rs
src/device_io/mod.rs
```

The only references are within the `device_io` module itself. None of
the engine, dispatch, server, or recovery code constructs a
`DeviceIo`. The lib.rs only re-exports the module
(`pub mod device_io;`).

The actual hot-path I/O on production `DirectDevice` flows through
`BlockDevice::pread` → `libc::pread` (`device.rs:619`) and
`BlockDevice::pwrite` → `libc::pwrite` (`device.rs:665`), one syscall
per op. There is no batching, no async, no kernel-side polling.

**Why it matters:**
The README's headline claim of "kernel async I/O via io_uring on
Linux >= 5.6" is currently false. The 10M+ ops/sec goal cannot be
hit with a sync per-record syscall path; on a sustained 4 KiB random
write workload the per-syscall overhead alone (~1 µs) caps throughput
at ~1M ops/sec/thread, well below the spec target.

The unused `IoUringBackend` is a real implementation (574 lines
including tests with timestamp ring for completion latency, etc.) —
this is not a stub, but it is not connected to any caller.

**Reproduction:**
```
cargo run --release --bin server -- --config conf.toml
# strace -p $pid -c -e pread,pwrite,io_uring_enter
# observe: pread/pwrite syscalls increment, io_uring_enter does not
```

**Suggested fix (large, but the gap is real):**
- Either wire `DeviceIo` into the engine (route batched
  `spend_batch`/`set_mined_batch` through `submit_read` /
  `submit_write` /`submit_and_wait`), or remove the unused module from
  the codebase. Either resolves the dead-code surface; only the first
  resolves the README-claim mismatch.
- If wired, the engine spend path would batch all RMW reads for a
  single replication batch, then issue all writes in a single
  submit_and_wait. The current code path that does this batching at
  the trait level is `pread_exact_at`/`pwrite_all_at` only — those
  don't batch across records.

---

### IJK-05: `write_aligned` and `write_input_refs` silently swallow pre-read errors and write zeros for the head/tail bytes (HIGH)
**Category:** J
**Location:** `src/storage/manager.rs:300-318`,
`src/storage/input_refs.rs:67-98`
**What:**
Both helpers compute `aligned_base` and `intra` for the partial-block
RMW pattern. They allocate a zeroed `AlignedBuf` and conditionally
pre-read the existing on-disk bytes:

```rust
if intra > 0 || !data.len().is_multiple_of(align) {
    // Pre-read for read-modify-write. A failure here is non-fatal
    // because the bytes we read are immediately overwritten by
    // `data` below; on success we are guaranteed an exact buffer
    // (no partial blocks).
    let _ = self.device.pread_exact_at(&mut buf, aligned_base);
}
buf[intra..intra + data.len()].copy_from_slice(data);
self.device.pwrite_all_at(&buf, aligned_base)?;
```

The comment "A failure here is non-fatal because the bytes we read
are immediately overwritten by `data` below" is **wrong**. If
`pread_exact_at` fails:

1. `buf` is left zeroed on the bytes outside `[intra, intra +
   data.len())`.
2. The subsequent `copy_from_slice` fills only the **middle** portion.
3. The `pwrite_all_at` flushes the entire aligned buffer, **zeroing
   out the head bytes `[0, intra)` and the tail bytes after
   `intra+data.len()`**.

For inline cold data, `intra` can be up to 4095 bytes (for a 1-UTXO
record at offset 256+69 = 325 within the leading 4 KiB block; the
preceding 325 bytes contain the metadata + the first UTXO slot
hash/status/spending_data). If the pre-read fails, the metadata header
is silently overwritten with zeros — including the magic, CRC, and
record_size. Subsequent reads will see a corrupt record.

Same logic applies to `input_refs.rs:84-89`.

**Why it matters:**
- A pre-read failure is rare on a healthy device, but possible:
  out-of-bounds (after a manual file shrink), an EIO from a flaky
  sector, a hot-swap of the underlying block device, etc.
- The failure is logged as `let _ = ...` — completely silent. There
  is no metric, no warning, no diagnostic. The operator first sees a
  cascade of `RecordCorruption: bad CRC` errors on subsequent reads.
- The code is reachable on every `StorageManager::write_cold_data`
  for the inline tier (which is the most common tier for typical BSV
  txs at < 8 KiB cold size).

**Reproduction:**
1. `MemoryDevice` of 64 MiB. Allocate a record at offset 1 MiB.
2. Write the full record (metadata + slots + cold data).
3. Mock the device to return `Err(EIO)` on the next `pread_exact_at`.
4. Call `mgr.write_cold_data(...)` for an inline-tier write.
5. Read the record back. The metadata header is now zeros. The
   `read_metadata` call returns `RecordCorruption`.

Practically reproducible by injecting via a shim around `device.pread`.

**Suggested fix:** propagate the error.

```rust
if intra > 0 || !data.len().is_multiple_of(align) {
    self.device.pread_exact_at(&mut buf, aligned_base)
        .map_err(StorageError::Device)?;
}
```

The original comment's reasoning is invalid. The downside of
propagating is that `write_cold_data` becomes a fallible op for the
inline tier in cases that today silently corrupt — that is the right
behaviour.

---

### IJK-06: `SyncFallback::submit_and_wait` does not loop on EINTR and treats short reads/writes as completions (MEDIUM, latent — module unused)
**Category:** J
**Location:** `src/device_io/sync_fallback.rs:89-115`
**What:**
The submit_and_wait loop calls `libc::pread`/`libc::pwrite` once per
queued op:

```rust
let result = match op.kind {
    OpKind::Read => unsafe { libc::pread(op.fd, op.buf_ptr as *mut _, op.len, op.offset as _) },
    OpKind::Write => unsafe { libc::pwrite(op.fd, ..., op.len, op.offset as _) },
};
completions.push(Completion { user_data: op.user_data, result: result as i32 });
```

A `-1` return is captured as `result: -1` (with `errno` lost). A
short return — for example if `pread` is interrupted by a signal
before any bytes are read — is captured verbatim. The caller is
expected to inspect `completion.result` and decide what to do, but
there is no documented contract for "this is a partial; reissue the
remainder at offset+result". Current callers of `DeviceIo` (none, see
IJK-04) would have to implement that logic themselves.

`BlockDevice::pread_exact_at` does loop on partial reads but is
implemented at the `BlockDevice` layer, not `DeviceIo` — so the two
abstractions are not coherent.

**Why it matters:** today no production code path is on this path; if
the device_io module is wired in (recommended in IJK-04), the loop
must be added or the sync fallback will be subtly different from the
io_uring path, which the kernel auto-resumes on EINTR.

**Suggested fix:** drop the per-op result-as-completion model; replace
with a `completion.result` that always reflects the **total bytes
written** for the op, looping internally on EINTR and on partial
returns until either the buffer is fully drained or the kernel
returns 0/error. This matches what callers expect of an
"async I/O abstraction".

---

### IJK-12: `engine.delete()` tombstone zeroes only `magic` + `record_size`, not the full record (MEDIUM)
**Category:** J (storage layout)
**Location:** `src/ops/engine.rs:2696-2714`
**What:**
The delete path:

```rust
let mut tombstone = self.read_metadata_fast(entry.record_offset)?;
tombstone.magic = 0;
tombstone.record_size = 0;
self.write_metadata_fast(entry.record_offset, &tombstone)?;
self.allocator.lock().free(entry.record_offset, record_size)?;
```

The tombstone is metadata-only: `tx_id`, `utxo_count`, the UTXO slots
themselves, and any inline cold data, remain on the device with their
original bytes. The freelist returns this region for re-use; the next
allocate will overwrite the bytes during the next create's
`write_full_record_with_cold` call (`engine.rs:2074`), which writes
the exact aligned buffer. So in steady state the bytes are overwritten
within seconds.

But:
1. A crash between `free` and the next allocate-and-write leaves
   stale bytes in the freed region. Recovery rebuilds the index from
   redo entries and skips the (now non-magic) record — correct. The
   stale bytes are not visible through the index, but tools that
   walk the device by offset (audit, forensics) will see them.
2. If the next allocator request is **smaller** than the freed
   region, the trailing bytes are not overwritten. For example, free
   a 100-slot record (record_size = 256 + 100*69 + cold = ~7 KiB),
   then allocate a 1-slot record (256 + 69 = 325 → aligned to 4 KiB).
   The trailing 3 KiB still hold the old record's UTXO bytes. With a
   debug tool walking from 4 KiB onward, an attacker can recover
   spending data from supposedly-deleted records.

**Why it matters:** the spec promises "delete frees the space" but in
practice the bytes are recoverable until they are overwritten by the
allocator's next user. For a UTXO store with per-output spending data
and frozen-asset reassignment history, this is a low-grade information
leak. Not a corruption issue.

**Suggested fix:** Two options:
- Cheap: extend the tombstone to zero the entire metadata header,
  not just `magic + record_size`. UTXO slots and cold data still
  leak, but the metadata identifying the tx (txid, fee,
  external_ref) is gone.
- Expensive: zero-write the entire record before calling
  `allocator.free`. This matches "secure delete" semantics but adds
  a full record write per delete (~4-8 KiB per tx). Gate behind a
  config flag `secure_delete = true`.

The current state is acceptable for non-adversarial workloads but
should be documented.

---

### IJK-13: Allocator high-water `next_offset` recovery does not bound-check against `device_size` (LOW)
**Category:** J
**Location:** `src/allocator.rs:704-720`, `:886-907`
**What:**
`replay_allocate` bumps `next_offset` to `offset + aligned_size` if
the redo entry implies it. There is no check that
`offset + aligned_size <= self.device_size`. A corrupt redo entry
with a bogus offset could push `next_offset` past the device, after
which all subsequent allocations would fail with `DeviceFull`.

`recover` reads the persisted `next_offset` from the header
(`:886`) and stores it without validation. If the on-disk header is
corrupted in a way that survives the CRC check (collision is 1 in
2^32), `next_offset` could exceed `device_size`.

**Why it matters:** post-recovery the allocator is unusable until
operator intervention. CRC32 collision is ~0; on-disk corruption that
the CRC catches returns `HeaderCorruption` (an actionable error). The
real risk is a future bug that produces a malformed redo entry and
slips past validation.

**Suggested fix:** in both `replay_allocate` and `recover`,
clamp/reject:

```rust
if offset >= self.device_size || end > self.device_size {
    return false; // ignore corrupt entry
}
```

at recovery time, or fail with `AllocatorError::CorruptedHeader` for
the header path.

---

### IJK-14: Allocator's `replay_redo` for a `FreeRegion` with overlap is silently ignored (LOW)
**Category:** J
**Location:** `src/allocator.rs:764-806`
**What:**
`replay_free` first checks "is this region entirely inside an
existing free region? then no-op." It then attempts to merge with
adjacent regions and insert. If the redo entry partially overlaps an
existing free region (e.g. `[1MB, 2MB)` is free, and the redo says
"free `[1.5MB, 2.5MB)`"), the merge logic does:

```rust
let next = next_from(2.5MB)  // None, no merge
let prev = prev_before(1.5MB) // (1MB, 1MB)
   if 1MB+1MB == 1.5MB? — no.   // no merge
self.freelist.insert(1.5MB, 1MB);
```

Now the freelist has `[1MB, 2MB)` AND `[1.5MB, 2.5MB)` — overlapping!
Subsequent allocations could double-allocate the overlap region.

This is a corner case that should be impossible in a well-formed
redo log (the engine only frees regions it knows are allocated), but
a malformed/forged redo entry could trigger it.

**Why it matters:** double-allocation of the overlap region. A new
record's metadata + slot data is then written into a region that the
freelist thinks is free and may hand to a second new record.

**Reproduction:** craft a redo log with two overlapping FreeRegion
entries. Replay. Allocate 1 GB twice. Both allocations could hit the
overlap.

**Suggested fix:** in `replay_free`, detect partial overlap before
inserting:

```rust
if let Some((prev_off, prev_sz)) = freelist.prev_before(end + 1) {
    if prev_off + prev_sz > offset {
        // overlap; coalesce instead of insert
        return self.replay_free_overlap(...);
    }
}
```

and either coalesce or reject as corrupt. Today the bug is reachable
only through a corrupted redo log, so it's LOW.

---

### IJK-15: Allocator does not journal `next_offset` independently — only the freelist (MEDIUM)
**Category:** J
**Location:** `src/allocator.rs:455-565`
**What:**
The redo journaling captures `AllocateRegion { offset, size, device_id }`
and `FreeRegion { ... }`. The on-disk header carries `next_offset` but
that is only updated on `persist`. Between two `persist` calls, if
the allocator does an `allocate` from the high-water (not from the
freelist), the redo entry is `AllocateRegion`. On replay, the
`replay_allocate` bumps `next_offset` past the allocated region — good.

But if a `persist` happens **between** two consecutive
`AllocateRegion` redo entries that came from the high-water, the new
high-water is captured in the snapshot. After the snapshot, only the
`AllocateRegion`s past the snapshot would be replayed.

This works correctly. **HOWEVER**: there is no mechanism to truncate
old redo entries after a snapshot. The redo log grows unboundedly
across many snapshots, and recovery replays them all. Replays of
`AllocateRegion` entries below the snapshot's `next_offset` are
idempotent no-ops (`replay_allocate` returns `false` if `end <=
next_offset` and the region is not in the freelist), so correctness
is preserved.

**Why it matters:** disk usage of the redo log grows. The "snapshot"
in `SlotAllocator::persist` writes the current freelist + next_offset
into a 1 MB header region, but the redo log itself is independently
sized (`config.rs: redo_log_size = 64 * 1024 * 1024`). The redo log
is a ring buffer (per `redo.rs`) so it does eventually wrap, but if
the wrap discards entries before the next persist, the post-persist
allocator state is **unrecoverable** — recovery replays the partial
redo log against the snapshot and may miss intermediate operations.

This is finding **IJK-15**: the snapshot/redo coordination is
fragile and is not validated end-to-end. It is closely related to
allocator-redo correctness in the broader durability gap.

**Suggested fix:** after every `persist`, truncate the redo log up
to the persisted point (i.e., commit a "checkpoint marker" entry
naming the snapshot's high-water, then advance the redo tail). The
existing `redo.rs` has the machinery; the wiring needs to happen.

---

## Category K — Pruning

### IJK-09: `handle_process_expired` does not re-validate `preserve_until` or staleness before deleting (MEDIUM)
**Category:** K
**Location:** `src/server/dispatch.rs:4669-4720`
**What:**
The handler:

```rust
let keys = engine.dah_index().range_query(current_height);
// Phase 1: Lookup record offsets, build redo ops
for key in &keys {
    let record_offset = engine.lookup(key).map(|e| e.record_offset).unwrap_or(0);
    redo_ops.push(RedoOp::Delete { tx_key: *key, record_offset, ... });
    valid_keys.push(*key);
}
// Phase 3: Apply
for key in &valid_keys {
    engine.delete(&DeleteRequest { tx_key: *key }) ...
}
```

It does not re-read the metadata to confirm:
- `preserve_until == 0`,
- `delete_at_height <= current_height`,
- `unmined_since == 0`,
- `spent_utxos == utxo_count`.

The DAH index is supposed to be the source of truth for "this record
is past its DAH". And `engine.preserve_until` correctly removes from
the DAH index when called. So in steady state the index is correct.

But:
1. Recovery rebuilds the DAH index from a snapshot + redo replay
   (`recovery.rs:354 replay_secondary_dah`). If a `PreserveUntil`
   redo entry was journaled but the DAH-secondary-redo for the
   removal was lost (e.g. ring-buffer wrap), the rebuilt DAH index
   could still contain the preserved txid.
2. A misbehaving client could send a `PreserveUntil` and a
   `ProcessExpiredPreservations` in rapid succession. The `preserve_until`
   handler's DAH index update happens after the metadata write
   (`engine.rs:2670`); a tiny window exists where the DAH index still
   carries the old DAH but the metadata has `preserve_until` set.
   Reading the index races with the metadata write.

**Why it matters:** preserved records are *the* most important
records to not delete. The whole point of `PreserveUntil` is to
guarantee retention through audit windows, regulatory holds, etc.
A defence-in-depth check on the actual on-device state would prevent
a stale-DAH-index bug from causing data loss.

**Reproduction:** simulate a recovery with a corrupted DAH redo log
(truncate the last entry). The rebuilt index has stale entries.
`ProcessExpiredPreservations` deletes them.

**Suggested fix:** in `handle_process_expired`, after fetching the
candidate keys but before issuing `engine.delete`, read each candidate's
metadata and re-evaluate `should_delete_at_height(metadata,
current_height)`:

```rust
let meta = engine.read_metadata(key).unwrap_or_continue;
if meta.preserve_until != 0 || meta.delete_at_height > current_height {
    // skip — the index is stale
    continue;
}
if meta.spent_utxos != meta.utxo_count { continue; }
if meta.unmined_since != 0 { continue; }
// safe to delete
```

The double-check costs one read per candidate; still fast.

---

### IJK-10: Compensation-on-replication-failure short-circuits DAH evaluation by hard-coding `block_height_retention: 0` (MEDIUM)
**Category:** K
**Location:** `src/server/dispatch.rs:1696-1818`,
`src/replication/receiver.rs:738-790`
**What:**
The compensation paths invoke the inverse engine op with
`block_height_retention: 0`, which causes
`evaluate_delete_at_height` to short-circuit (line 70 of
`ops/delete_eval.rs`) and return `(Signal::None, None)`.

So if the forward op was a "spend that triggered DAH set", the
compensation "unspend" will not reset DAH. The DAH index thinks
the record is at `delete_at_height = X`; the metadata still has
`delete_at_height = X`; but the spent_utxos count is now 1 less than
utxo_count (because we unspent), so `evaluate_delete_at_height`
under correct retention would have returned `Signal::DeleteAtHeightUnset`
and cleared the DAH. After compensation it does not.

**Why it matters:**
- The DAH index now contains a stale entry for a
  no-longer-fully-spent record. `ProcessExpiredPreservations` will
  pick it up at `current_height >= X` and delete the record even
  though it's no longer fully-spent. This is exactly the data-loss
  scenario the audit asks about for K.5 ("MarkLongestChainBatch
  interaction with pruning is correct").
- The same applies to `set_mined` ↔ `unset_mined` compensation,
  `freeze` ↔ `unfreeze`, and any other pair that affects DAH.

**Reproduction:**
1. Create a transaction with utxo_count=2 and one of the UTXOs
   already spent.
2. SetMined to mark the record as on-longest-chain.
3. Spend the second UTXO. This triggers
   `evaluate_delete_at_height` → all_spent && has_blocks &&
   on_longest_chain → set DAH to current_height + retention. The
   DAH index gets the entry.
4. The replication of this Spend fails. Compensation runs Unspend
   with `block_height_retention: 0`.
5. The metadata's DAH stays at the old value; the DAH index still
   has the entry. `meta.spent_utxos = 1 != 2 = utxo_count`.
6. At `current_height = old_DAH_target`, `ProcessExpiredPreservations`
   queries the DAH index, finds this record, calls `engine.delete`.
   The record is destroyed despite having one unspent UTXO.

**Suggested fix:** propagate the original `block_height_retention`
through the compensation path. The `BeforeImage` capture
infrastructure (which already exists for `UnsetMined`) should also
carry the retention value, or alternatively the compensation
should call `engine.evaluate_dah(...)` explicitly to recompute and
re-update DAH index for the inverse-applied state.

A simpler fix: in `replication/receiver.rs:759, 782` and the
dispatch compensation paths, change `block_height_retention: 0` to
`block_height_retention: SERVER_DEFAULT_RETENTION` (288 by default)
as a defence — the receiver/compensation does not have the
per-request value but a sensible default is better than 0.

---

### IJK-11: `handle_query_old_unmined` does not filter by `preserve_until` (MEDIUM)
**Category:** K
**Location:** `src/server/dispatch.rs:4569-4588`
**What:**
The pruner's discovery query is:

```rust
let cutoff = u32::from_le_bytes(req.payload[0..4].try_into().unwrap());
let keys = engine.unmined_index().range_query(cutoff);
```

The unmined secondary index is keyed on `unmined_since`, indexing
records that have been unmined for more than `cutoff` blocks. The
caller (presumably the pruner / orphan-cleaner in Teranode) then
issues `delete` for each key. There is no filter on `preserve_until`.

`engine.delete` itself does not check `preserve_until` either. A
preserved-but-unmined-for-long-time record will be returned by this
query and the caller will delete it.

**Why it matters:** a preserved record that is unmined for
`cutoff_height` blocks will be deleted on the next pruner sweep,
even though the operator explicitly preserved it. This breaks the
preservation contract.

**Reproduction:**
1. Create an unmined tx at block_height = 1000.
2. PreserveUntil to block_height = 5000.
3. Wait — assume current block_height advances to 2000.
4. Pruner queries `OP_QUERY_OLD_UNMINED` with cutoff = 1500
   (records unmined since before block 1500).
5. The preserved record is returned because `unmined_since = 1000
   < 1500`.
6. Pruner issues `delete`. Record gone.

**Suggested fix:** in `handle_query_old_unmined`, after fetching
candidate keys, filter out any whose `preserve_until > 0`:

```rust
let keys = engine.unmined_index().range_query(cutoff);
let filtered: Vec<TxKey> = keys.into_iter().filter(|k| {
    engine.lookup_cached(k).map(|e| {
        let has_preserve = e.tx_flags & TxFlags::HAS_PRESERVE_UNTIL.bits() != 0;
        !has_preserve  // skip preserved records
    }).unwrap_or(true)
}).collect();
```

The HAS_PRESERVE_UNTIL flag is already cached in
`TxIndexEntry.tx_flags` so this is a zero-I/O filter.

Alternatively: have the unmined index itself carry the
"preserve_until is set" bit and skip on insert/update. This avoids
the filter at query time.

---

### IJK-19: Delete-batch compensation re-creates a record but the original blob payload was already deleted (MEDIUM)
**Category:** K
**Location:** `src/server/dispatch.rs:3957-4097`,
`src/ops/engine.rs:2688-2742`
**What:**
The delete-batch compensation snapshots the metadata + UTXO hashes +
external blob payload **before** calling `engine.delete()` (`:3974`).
The blob payload is captured via `engine.blob_store().get(&key.txid)`.
Then `engine.delete` is called, which for an external record calls
`mgr.delete_cold_data(&meta, None)` (`storage/manager.rs:236`):

```rust
if metadata.flags.contains(TxFlags::EXTERNAL) {
    self.blob_store.delete(&metadata.tx_id)?;
}
```

The blob is gone from disk. If replication then fails, compensation
re-creates the record and **also** re-uploads the blob via the
`Create` `ReplicaOp` which carries `cold_data` (`:4055-4061`):

```rust
let create_op = ReplicaOp::Create {
    tx_key: *key,
    metadata_bytes: snap.metadata_bytes.clone(),
    utxo_hashes: snap.utxo_hashes.clone(),
    cold_data: snap.cold_data.clone(),
    is_external: snap.is_external,
};
```

So the blob is restored — **good**. But:

1. The snapshot captures the blob in memory (`Vec<u8>`). For a
   1 MiB+ blob across N records in the batch, this is memory-bound:
   `N * blob_size` bytes resident during the batch. For pathological
   batches this can OOM.
2. The blob's content_hash is recomputed on re-upload — different
   `BlobDigest` (well, same — SHA-256 is deterministic). The
   `meta.external_ref.content_hash` written by the compensation
   path uses the **original** value, so the digest still matches.
3. If the snapshot read failed (`engine.blob_store().get(...)`
   returned None — say, the blob had already been GC'd by another
   process or was missing in cluster mode), `snap.cold_data` is
   `None` and the Create receives `cold_data: None`. The receive
   path's `replica_create_external` handler currently expects the
   cold_data to either be provided or for the blob to already exist
   in the local blobstore — if neither is true, the record's
   external_ref points at a non-existent blob.

**Why it matters:** delete is the most destructive op; its
compensation must work for cluster availability. The current
implementation works for the common case but has gaps for the
"blob missing" sub-case.

**Suggested fix:**
- For point 1: stream the blob to a temp file rather than holding
  it in memory. Use `engine.blob_store().stream_to(&key, &mut
  tempfile)` to capture, and re-stream during compensation.
- For point 3: distinguish "snapshot failed because external blob
  missing" from "snapshot succeeded with cold_data=None" by giving
  `DeleteSnapshot` a tri-state (snapshot OK / snapshot
  blob-missing / no-snapshot). On compensation for the
  blob-missing case, surface a hard error to the operator rather
  than silently re-creating a record with a dangling external_ref.

---

### IJK-20: `MarkLongestChainBatch` rollback on error does not restore the DAH index (HIGH)
**Category:** K
**Location:** `src/server/dispatch.rs:4131-4224`
**What:**
The handler:

```rust
// Phase 1: build redo
// Phase 2: WAL-first
// Phase 3: Apply engine mutations
for v in &valid_items {
    match engine.mark_on_longest_chain(...) {
        Ok(_) => {},
        Err(err) => errors.push(spend_error_to_batch_error(...)),
    }
}
```

There is **no replication phase** for MarkLongestChainBatch. Comment:

```
// MarkOnLongestChain is metadata-only; no dedicated ReplicaOp
// needed — the SetMined replication already covers block tracking.
```

This is a correctness gap. `mark_on_longest_chain` toggles
`unmined_since` (and triggers DAH re-evaluation), which both:
- Updates the on-device metadata (replicated by hot path? no, see below).
- Updates the DAH secondary index AND the unmined secondary index
  (`engine.rs:1588-1595` `sync_primary_and_both_secondary_atomic`).

But the comment claims "SetMined replication already covers block
tracking". That's wrong: SetMined adds/removes block entries;
MarkOnLongestChain toggles `unmined_since` independently. The two are
distinct mutations — there is no automatic SetMined/MarkLongestChain
correspondence.

After a MarkLongestChainBatch:
- The master's metadata and indexes are updated.
- No `ReplicaOp` is emitted.
- The replicas still see `unmined_since = old_value`,
  `delete_at_height = old_value`.

A subsequent `ProcessExpiredPreservations` on the master deletes
records the master thinks are pruneable. The replica still has them.
On a master failover the new master sees the old un-pruned state and
either re-creates the records (if it has them in its index) or treats
them as missing.

**Why it matters:**
- DAH index divergence between master and replicas → split-brain on
  pruning decisions.
- During reorgs, a node that was previously a replica becomes
  master and sees a stale DAH index. Records that should have been
  pruned are not pruned.
- The cited rationale ("SetMined covers it") is incorrect.

**Reproduction:**
1. Cluster of 2 nodes (RF=2). Node A is master.
2. Create tx, mine it (SetMined), wait for replication ACK.
3. Issue MarkLongestChainBatch on Node A flipping the tx
   off-longest-chain. Node A's `evaluate_delete_at_height`
   re-evaluates: `unmined_since != 0` ⇒ DAH cleared.
4. On Node A: meta.delete_at_height = 0, dah_index does not
   contain the tx.
5. On Node B: meta.delete_at_height = old_DAH (from the original
   SetMined-driven set), dah_index still contains the tx.
6. Failover to Node B. Node B's pruner runs at
   `current_height = old_DAH`. It deletes the tx.
7. Node A (now replica) does not have the deletion — divergence.

**Suggested fix:** add a `ReplicaOp::MarkLongestChain { tx_key,
on_longest_chain, current_block_height, block_height_retention,
master_generation }` and emit it from the dispatch handler.
Implement the receiver in `replication/receiver.rs` to call
`engine.mark_on_longest_chain` with the same params. The
`master_generation` field is the idempotency token — replicas skip
the op if their cached generation already matches.

This is a **HIGH** finding because it's a real, current
divergence-on-reorg path.

---

### IJK-21: `delete_at_height` is set on creation only via the all-spent path; there is no time-based retention for unmined txs (LOW — design choice, but worth noting)
**Category:** K
**Location:** `src/ops/delete_eval.rs:101-145`,
`src/index/unmined_index.rs`
**What:**
The audit asks: "delete_at_height is set and respected for unmined
txs older than block_height_retention".

The current logic only sets `delete_at_height` when:
- `block_height_retention != 0`, AND
- `preserve_until == 0`, AND
- (`CONFLICTING && existing_dah == 0`) OR
  (`all_spent && has_blocks && on_longest_chain`)

**Unmined txs are never on the longest chain by definition**
(`unmined_since != 0` ⇒ `on_longest_chain = false` in the eval). So
unmined txs never get a `delete_at_height` from this path.

The orphan/old-unmined cleanup is handled separately via
`OP_QUERY_OLD_UNMINED` — a query-and-delete cycle driven by an
external pruner. The pruner is responsible for the time-based
retention; the database doesn't auto-prune unmined txs.

**Why it matters:** the spec asks the audit question literally —
"DAH is respected for unmined txs older than retention" — and the
answer is "DAH is never set for unmined txs; orphan cleanup is
out-of-band via the unmined index". This is a **design choice**, not
a bug, but the deviation from the audit's working assumption is
worth recording.

**Reproduction:** create an unmined tx at block_height = 1000.
Inspect `meta.delete_at_height` after spending all UTXOs. Result: 0.

**Suggested fix:** none required; document the design in
`SPEC_VALIDATION_REPORT.md`. If the spec actually wants
time-based unmined retention, add it as a separate
`evaluate_unmined_dah` function and wire it into `create` and
`spend`.

---

### IJK-22: `MarkLongestChainBatch` does not write a generation idempotency token usable by replicas (HIGH, sub-finding of IJK-20)
**Category:** K
**Location:** `src/server/dispatch.rs:4163-4180`
**What:**
The redo entry has a `generation` field for idempotency:

```rust
let target_generation = engine
    .lookup(&key)
    .map(|e| e.generation.wrapping_add(1))
    .unwrap_or(1);
redo_ops.push(RedoOp::MarkOnLongestChain {
    tx_key: key,
    on_longest_chain,
    ...,
    generation: target_generation,
});
```

But the engine's actual `mark_on_longest_chain` increments
`metadata.generation` once (`engine.rs:1561`) without checking
against `target_generation`. So the redo entry's `generation`
field is informational only; recovery replay does not enforce
idempotency by generation.

This means a redo replay that re-applies a MarkLongestChain after
the engine has already advanced the generation will further bump
the generation, drifting the master's and replicas' values out of
sync.

**Why it matters:** generation drift breaks read-your-writes
semantics for clients tracking generation numbers across mutations.
Combined with IJK-20 (no replication of MarkLongestChain), the
generation diverges on reorgs.

**Suggested fix:** in the engine's mark_on_longest_chain, accept a
`target_generation` parameter (the dispatch already computes it).
If `metadata.generation + 1 != target_generation`, this is either a
no-op (already applied) or a conflict (someone else mutated). The
recovery replay can then pass the redo entry's generation through
and the engine compares.

---

### IJK-23: `engine.delete` doesn't journal the freed cold_size for separate-NVMe records (MEDIUM)
**Category:** K
**Location:** `src/ops/engine.rs:2688-2742`,
`src/storage/manager.rs:231-244`
**What:**
The delete path frees the hot record's allocation
(`engine.rs:2711`) but **does not** call
`mgr.delete_cold_data(&metadata, separate_cold)` to release the
separate-NVMe allocation for tier-2 cold data. Reading the code:

```rust
let record_size = ({ meta.record_size }) as u64;
// tombstone, write meta with magic=0, free record region
self.allocator.lock().free(entry.record_offset, record_size)?;
```

For a SeparateNVMe-tier record, `record_size` is the hot record only
(METADATA_SIZE + utxo_count * 69) — the cold-data allocation was a
**separate** allocator call at create time and lives at a different
offset stored in `meta.external_ref` (well, no — `external_ref` is
for blob-tier; SeparateNVMe doesn't use external_ref, it stores the
device offset in the record metadata layout differently).

Looking at `storage/manager.rs:108-114`:

```rust
StorageTier::SeparateNvme => {
    let device_offset = self.allocator.lock().allocate(data_size as u64)?;
    self.write_aligned(&serialized, device_offset)?;
    Ok(ColdDataRef::SeparateNvme {
        device_offset,
        cold_size: data_size as u32,
    })
}
```

The `device_offset` and `cold_size` are returned as `ColdDataRef`,
but they are NOT stored in the on-device metadata anywhere — only in
the test code's local variable. There is no field in `TxMetadata`
for "separate NVMe cold data location".

Looking at engine.rs's external creation path: `is_external` is set
from `req.is_external`, and `external_ref` is populated. No
`is_separate_nvme` flag exists, no `separate_cold_offset` or
`separate_cold_size` field exists.

So **records that exceed 8 KiB cold but stay below 1 MiB never
actually use the SeparateNVMe tier in production** — `engine.create`
calls `write_full_record_with_cold` which writes the cold data
inline regardless of size. The `StorageManager::write_cold_data`
function with its three-tier dispatch is only exercised by tests; the
actual create path does not call it.

**Why it matters:** the SeparateNVMe tier is **not actually wired
into production**, just like `BlobUploader`. Looking at the engine's
`build_cold_data` calls (`engine.rs:1633, 1779, 1822, 1962`): they
all go directly to `write_full_record_with_cold` which inlines the
cold data into the same allocation.

This is a major spec deviation. The spec says cold data 8KiB-1MiB
goes to separate NVMe; in reality everything below 1 MiB is inline,
and everything above goes to external blob via the streaming upload
path.

**Reproduction:** create a tx with 50 KiB of input data via
`OP_CREATE_TX`. The record allocation is `record_size_for(N) +
50KiB`, written as one buffer. No separate allocation.

**Suggested fix:** either implement the SeparateNVMe tier (add
`separate_cold_offset` and `separate_cold_size` to `TxMetadata`,
route `write_cold_data` through engine.create), or remove
`StorageTier::SeparateNvme` and its scaffolding to avoid the spec
mismatch.

---

## Verification status

The following audit checkpoints were verified by code inspection
and the cited tests:

| Check | Status | Notes |
|---|---|---|
| I-1 tier transitions (7→9→1.1MB) | NO ISSUE | Records are immutable post-create; no tier transition path exists. See IJK-09 (NO-FINDING). |
| I-2 missing blob → BLOB_NOT_FOUND | PARTIAL | `stream_to` returns NotFound; `get`/`exists` return Ok(None). Trait contract is asymmetric but consistent. See IJK-10 (LOW). |
| I-3 orphaned blob GC | **HIGH FINDING** | No GC implemented. See IJK-08. |
| I-4 concurrent uploads same txid | **MEDIUM FINDING** | File-blob race on shared `.tmp`. See IJK-07. |
| I-5 blobstore_path full / not writable | LOW | Surfaces as ERR_INTERNAL with stringified errno; no dedicated code. See IJK-08-LOW (alias). |
| I-6 blob hash/integrity on read | OK | SHA-256 verified against sidecar before returning bytes. See IJK-05 for caveat. |
| J-1 4 KiB alignment on every read/write | OK | `check_alignment` enforces. **One exception:** `write_aligned` masks pre-read errors → IJK-05. |
| J-2 block-device size from kernel ioctl | OK | `BLKGETSIZE64` (Linux), `DKIOCGETBLOCKCOUNT × DKIOCGETBLOCKSIZE` (macOS). |
| J-3 file grown but never truncated | OK | `:556-563`. Test `direct_device_no_truncate_existing` asserts. |
| J-4 partial pread/pwrite retried | OK | `pread_exact_at` / `pwrite_all_at` loop with EINTR retry and ShortRead/WriteStalled fatal errors. **Caveat:** `SyncFallback` does not loop → IJK-06 (latent). |
| J-5 io_uring stub vs real | **HIGH FINDING** | Real implementation but **not wired in**; production uses sync `pread/pwrite`. See IJK-04. |
| J-6 allocator freelist correctness | OK | Best-fit, three-way coalesce, hybrid Vec/BTree, stress test with 100 ops. |
| J-7 allocator power-loss leak | OK | Redo journals every allocate/free with rollback on flush failure. **Caveat:** redo log truncation is not coordinated with persist → IJK-15. |
| K-1 block_height_retention honored | OK | Short-circuits when 0; checked_add for overflow. |
| K-2 PreserveUntilBatch prevents pruning | OK | `evaluate_delete_at_height` short-circuits on `preserve_until != 0`. **Caveat:** stale DAH index → IJK-09. |
| K-3 ProcessExpiredPreservations does not delete preserved | OK in steady state. **Caveat:** stale DAH index after recovery → IJK-09 + IJK-11. |
| K-4 pruning during reorg | **HIGH FINDING** | MarkLongestChain not replicated → DAH index divergence → IJK-20. |
| K-5 MarkLongestChainBatch with pruning correct | **HIGH FINDING** | See IJK-20, IJK-22. |
| K-6 delete_at_height set for unmined > retention | **NO** | Unmined txs never get DAH; relies on external pruner via OP_QUERY_OLD_UNMINED → IJK-21 (design observation). |

## Files cited

- `src/storage/mod.rs` (12 lines)
- `src/storage/manager.rs` (1283 lines)
- `src/storage/tiers.rs` (185 lines)
- `src/storage/blobstore.rs` (1120 lines)
- `src/storage/uploader.rs` (367 lines)
- `src/storage/input_refs.rs` (259 lines)
- `src/device.rs` (1421 lines)
- `src/device_io/mod.rs` (119 lines)
- `src/device_io/sync_fallback.rs` (326 lines)
- `src/device_io/io_uring_backend.rs` (574 lines)
- `src/io.rs` (710 lines)
- `src/allocator.rs` (2241 lines)
- `src/ops/delete_eval.rs` (522 lines)
- `src/ops/mark_longest_chain.rs` (29 lines)
- `src/ops/remaining.rs` (116 lines)
- `src/ops/engine.rs` (sampled 1500..2800 lines)
- `src/server/dispatch.rs` (sampled 3800..4720, 5500..5800 lines)

## Tests reviewed (not exhaustive)

- `tests/integration.rs` — `preserve_until_blocks_pruning`,
  `simulate_block_reorg`, `concurrent_mixed_workload`
- `tests/e2e_workload.rs` — `realistic_block_reorg`,
  cold-data tests (mixed inline/external workload)
- `tests/recovery_crash_boundaries.rs` — alignment-aware crash
  recovery
- `tests/stress/mod.rs` — `stress_device_fill_and_churn`
- `src/storage/blobstore.rs` unit tests — integrity, atomicity, race
- `src/allocator.rs` unit tests — fragment, persist+recover,
  redo replay, header CRC

## Unverified

- I/O performance under load: README claims 10M+ ops/sec but with
  `device_io` unused (IJK-04), the achievable throughput on real
  hardware is unknown. Benchmark on real NVMe to confirm.
- Cluster-wide DAH/index recovery after a partition: code paths exist
  but the audit did not trace a full partition + heal cycle.
- macOS `F_NOCACHE` actually disables caching: the spec says it
  approximates O_DIRECT but on macOS `pread`/`pwrite` may still hit
  the page cache for some workloads. Behaviour observed only in
  tests against a tempfile, not against an SSD.
- `BlobStore::stream_to`'s two-pass design (verify, then stream): a
  malicious peer could exploit the second pass's lack of digest
  re-check by tampering between passes. Not a finding because the
  blobstore is local and write-protected per the storage stack
  (single FS-writer model). Document the assumption.
- `DirectDevice` on a hugepages-backed file: alignment and direct
  I/O semantics differ; not exercised by tests.
- Allocator redo log full / wrap behavior: the redo log is a ring
  buffer per `redo.rs`; when it wraps without an intervening
  `persist`, replay loses entries. Recovery's robustness here was
  not exercised end-to-end in the test suite.
- Concurrent BlobStore puts vs deletes for the same key: race
  semantics not specified by the trait. Today both paths use
  `tmp+rename` and `unlink`; rename + unlink concurrently has
  POSIX-defined "rename wins or unlink wins" semantics, but the
  resulting state is racy and not tested.
