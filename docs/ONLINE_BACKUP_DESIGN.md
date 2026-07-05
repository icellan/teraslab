# TeraSlab Online Backup Design

**Status:** DESIGN — implementation planned as `phases/14_backup.md`. Nothing in
this document is built yet; every `file:line` reference describes the code this
design was verified against (commit `26aaf1e`).

## Motivation

TeraSlab has no backup capability. The existing `teraslab-cli
export-index` / `import-index` / `repair` commands are offline-only and cover
the index, not record data. Operators need:

- a backup taken **from a running node**, with minimal impact on the serving
  workload (no quiesce, no fsync storms, no lock convoys);
- a **verified, offline restore** that brings up a node in the exact state the
  backup captured;
- clear failure semantics: a backup may fail, client writes must never fail
  because of a backup.

## Non-goals (v1)

The backup job **refuses to start** (typed error at `POST /admin/backup`) for
any configuration outside this v1 envelope:

| Rejected configuration | Reason |
|---|---|
| `storage.engine != "segment"` | In-place engine mutates record bytes at rest; the segment-lifecycle pin below does not bound its write set |
| `index.backend != "memory"` (primary or secondaries) | With redb / file-backed backends, durable index state lives outside the `.snap` file and outside this design's copy set |
| Cluster mode configured | Shard ownership + cluster state out of scope |
| Replication configured | Interaction between the redo tee and replica catch-up untested in v1 |
| `backup.backup_dir` unset | Feature is default-off |
| A backup job already running | Single-flight lease (409) |

Also out of scope for v1 (listed as follow-ups at the end): incremental
backups, streaming the backup over the wire to a remote sink, point-in-time
recovery beyond the captured instant.

## What state must a backup capture?

| Artifact | Path (defaults) | In backup? |
|---|---|---|
| Data device(s) | `device_paths` (default `teraslab-data.dat`); first 1 MiB per store = allocator header (`TERASEGL`, `segment_allocator.rs:52-58`), data region beyond `DATA_REGION_OFFSET` (`segment_allocator.rs:42`) | **Yes** — header + used segments |
| Redo log(s) | `<device>.redo` (+ `.{i}` per extra store, `bin/server.rs:1030-1041`) | **Fabricated** — see "The redo tail" |
| Index snapshot | `teraslab-index.snap` | **Yes** — backup writes its own, directly into the backup dir |
| Durable height | `<snapshot>.height` | Yes (tiny, CRC-protected) |
| Blob store | `blobstore_path` directory tree | **Yes** — immutable files, GC pinned during copy |
| Index (Memory backend) | anonymous mmap | No — derived; rebuilt from `.snap` + replay |

## Why "copy the files after a checkpoint" is not enough

The obvious design — force a checkpoint (fence `F`), then copy the device
files, shipping a redo log truncated at `F` — is **unsound**. Two verified
properties of the segment engine break it:

1. **In-place RMWs exist even in the log-structured engine.** Spends relocate
   (`PreparedSpend::apply_locked` → `relocate_record`, `engine.rs:8211-8283`,
   `engine.rs:4812`) and never touch old bytes, but a family of operations
   mutates already-allocated record bytes at their current offset, with no
   `log_structured` branch:
   - `setMined` — block entry + metadata written in place
     (`engine.rs:3285-3316`);
   - `unspend` (`engine.rs:3436-3482`), `freeze`/`unfreeze`
     (`engine.rs:5402-5411`, `5460-5465`), `reassign` (`5541-5557`),
     `prune_slot` (`5163-5215`), `set_conflicting` / `set_locked` /
     `preserve_until` / `mark_longest_chain` (all via `write_metadata_fast` /
     `write_slot_fast`);
   - **delete tombstone** — `write_zeroed_metadata_header`
     (`engine.rs:7187` → `2555-2600`) zeroes the metadata header of the
     record at its current offset;
   - **children-block RMW** — `write_children_block` (`engine.rs:6266-6283`)
     appends child ids into a parent record's extent in place.

   Consequence: a post-`F` `delete` zeroes the header of a record that is
   live in the `.snap@F`. A copier that captures the zeroed block produces a
   backup whose restore loads `.snap@F`, replays nothing, and then fails CRC
   on every read of that key. That is corruption, not fuzziness.

2. **In-place ops are multi-write sequences.** Slot write and metadata write
   take `io_locks` separately (e.g. freeze at `engine.rs:5402` then `5411`).
   A copier can capture slot-new + metadata-old. With no replay to reconcile,
   the restored store violates engine invariants
   (`spent_utxos == count(SPENT slots)`, generation/flag divergence).

## The correctness model: restore ≡ crash recovery

Instead of trying to capture a perfectly frozen image, the backup is
constructed to be a **crash-legal device state at an instant `T`**, paired
with a complete redo tail `(F, T]`:

- Every copied 4 KiB block is an **atomic** snapshot of its content at some
  `t ∈ [F, T]` (the copier holds the same striped `io_locks` read-side that
  every writer holds write-side across its pwrite/memcpy —
  `lock_span_blocks`, `io.rs:1337-1365`; `StripedRwLocks`, `locks.rs:74-117`,
  65,536 stripes keyed `(offset >> 12) & mask`). This block-mixed image is a
  *strict subset* of what a write-back cache plus power loss can already
  produce.
- The backup ships the full redo tail `(F, T]`. Restore boots the node
  normally; recovery loads the `.snap`, replays the tail, and converges —
  the engine's existing, most-tested code path. **No new recovery code.**

Replay-per-op argument (all verified against `src/recovery.rs`):

- `Spend`/`Unspend`/`SetMined`/`Freeze`/etc. are **self-contained**: replay
  writes absolute slot state and recomputes `spent_utxos` from actual slots
  (`recovery.rs:1823-1927`; contract note at `recovery.rs:30`) — idempotent
  whether or not the copied block already reflects the op.
- `Delete` is self-contained (removes the index entry; tombstone bytes are
  irrelevant afterwards).
- `CreateV2`/`Relocate` read payloads back from the device, and tolerate
  missing/garbage target bytes as buffered-tail loss → `Skipped`
  (`recovery.rs:2800-2853`, `2869-2903`). The copy set therefore includes
  segments appended-to during the window (catch-up loop below).
- `replay_spend` writes the spent slot at the index-current offset without a
  tx-identity check at that offset (`recovery.rs:1833-1875`). The
  segment-lifecycle pin (below) guarantees no tail entry ever resolves to a
  reclaimed-and-reused extent during the window, closing that interleaving
  class in the restored image.

WAL-first ordering is what makes the tail sufficient: every acknowledged
mutation has a redo entry carrying every byte recovery needs
(`docs/DURABILITY_CONTRACT.md`, "Write Path Ordering") — `CreateV2` carries
the full record bytes, so even a record whose device blocks were missed
entirely is reconstructed from the tail.

## End-to-end flow

Job state machine: `Idle → Pinning → Fencing → Snapshotting → Copying →
CatchUp → Finalizing → Done | Failed`.

1. **Pin** (RAII `BackupPinGuard`; pins are process-RAM only — a server crash
   clears them by construction):
   - *Segment lifecycle pin*: `reclaim_fully_dead_segments`
     (`segment_allocator.rs:492`) and `defrag_victims`/`defrag_compact`
     (`segment_allocator.rs:520`, engine wrappers `engine.rs:7964`, `7992`)
     become no-ops; `advance_to_next_segment` (`segment_allocator.rs:452`)
     skips the `free_segments` pop (queue preserved) so allocation advances
     the high-water mark only. Sealed segment bytes are now stable for the
     whole window.
   - *Blob-GC pin*: the sweep loop (`blob_gc.rs:172-260`, spawned at
     `bin/server.rs:1936-1952`) pauses; no blob unlinks during the window.
   - Record per store: the used-segment set snapshot (under the allocator
     mutex) and the open-segment cursor.
2. **Fence**: acquire `engine.acquire_checkpoint_visibility_guard()`
   (`engine.rs:1447` — drains in-flight mutation applies, exactly as
   `perform_checkpoint_inner` does at `checkpoint.rs:648-653`);
   `F = current_sequence() - 1`; **install the redo tee** on every store's
   log under the log mutex (inside `buffer_entry`/`buffer_preencoded`,
   `redo.rs:3226/3245` — order-preserving); release the guard.
   - The tee filters out live `RecoveryProgress`/`Checkpoint` markers: a
     mid-window marker at `F₂ > F` would falsely fence the backup replay at
     `F₂` against a `.snap` taken at `F`.
   - Teed entries `≤ F` are harmless (replay skips at/below the fence).
   - The tee buffer is bounded (`tee_buffer_max_bytes`); overflow **aborts
     the backup**, never blocks an appender.
3. **Snapshot**: `engine.snapshot_index(<backup_dir>/teraslab-index.snap)` —
   the API takes an arbitrary path (`engine.rs:7913-7916`, atomic
   tmp+rename+fsync inside), so the backup owns its snapshot and never races
   the live checkpoint's `.snap` renames. The snapshot is fuzzy (may contain
   post-`F` effects), identical to live checkpoints, reconciled by tail
   replay (`checkpoint.rs:663-671`).
4. **Main copy pass** (throttled): per store, copy every used segment in
   128 KiB chunks. Each chunk is read via `pread_nocache` under sorted,
   deduplicated read-side `io_locks` guards; SHA-256 is computed while
   streaming; bytes land in one sparse image per **physical device** at
   physical offsets (`store_base + store_offset`). The headroom monitor runs
   on every throttle tick.
5. **Catch-up loop**: with reuse pinned, allocation is monotone, so the hot
   range per store is contiguous: `[last_copied_frontier ..
   open_segment_now]`. Re-copy it; repeat until it is
   `≤ stall_copy_max_segments` or `max_catchup_rounds` is exceeded (job
   fails: write rate exceeds backup throttle).
6. **Final bounded stall** (~tens of ms): acquire the visibility guard;
   `T = current_sequence() - 1`; copy the residual hot segments **into RAM**
   (device reads only — no backup-dir I/O under the guard); read the
   `.height` bytes; serialize each store's allocator header **from memory**
   (`serialize_header_bytes()`, factored from `persist_header_no_sync`,
   `segment_allocator.rs:860-928` — untorn by construction, covers all
   segments used through `T`); detach the tees; release the guard.
7. **Finalize**: write the RAM buffers; write the fabricated per-store redo
   files (format below) draining teed frames `≤ T`; copy the blobstore tree
   (skip `*.tmp`, tolerate ENOENT); fsync everything.
8. **Unpin** (guard drop), then write `MANIFEST.json` **last**
   (fsync + `fsutil::fsync_parent_dir`). Manifest presence = complete
   backup; a manifest-less directory is garbage, cleaned on the next run and
   refused by restore.

### The redo tail: teed, not read back

Mid-backup checkpoints **keep running** — they must, or a long backup window
would fill the redo log and trip the 0.90 write-backpressure gate
(`checkpoint.rs:176-191`, `redo.rs:4208`). Checkpoint redo-prefix reclaim
only touches the redo files, never data segments
(`redo.rs:4009-4029`), so it is safe alongside the pin — but it legitimately
destroys `(F, F₂]` from the live log. That is why the tail is captured by a
**tee at append time**, not read back at finalize (`read_from_sequence`,
`redo.rs:4083`, would return a truncated range).

Checkpoints tolerate the pin: `perform_checkpoint_inner` treats
defrag/reclaim as best-effort counters — zero-return is the normal no-op
path (`checkpoint.rs:674-712`).

**Fabricated redo file** (one per store; store 0 at
`resolved_redo_log_path()`, store *i* with `.{i}` suffix; one global
sequence across stores, `redo.rs:3204-3222`):

```
RedoHeader:  magic | version=2 (linear) | next_sequence=T+1 |
             checkpoint_seq=F | logical_start=0 | crc32       (redo.rs:208-305)
frame 0:     RecoveryProgress { through_sequence: F }  at sequence F
frames 1..:  teed frames of this store, commit order, sequences in (F, T]
```

Linear `recover()` honors the marker: the F-G4-010 bound
`through_sequence <= max_seq` (`redo.rs:4040-4076`) is satisfied because the
marker's own sequence `F` counts toward `max_seq`. Implementation must verify
that `RedoLog::open` under `redo_segment_ring = true` adopts an existing
linear v2 file per its header (`redo.rs:200-207` states a linear log stays
byte-identical to v2 until the ring is adopted); if not, fabricate the v3
ring format instead — this is a named task in `phases/14_backup.md`.

### Copy path: through the server's device handle

The store device stack is
`StreamingWriteDevice(CachingDevice(SubDevice(DirectDevice)))`
(`bin/server.rs:490-575`). A second raw fd on the file is rejected: with the
write-back cache or streaming buffer, acknowledged data lives in RAM, and
mixing O_DIRECT writes with buffered second-fd reads has stale-page-cache
hazards. Reading through the handle is coherent in every mode
(`streaming.rs:225`, `cache.rs:424-466` merge buffered/dirty state).

One amendment is required: `CachingDevice::pread` inserts every miss into the
cache (`cache.rs:446-463`) — streaming a device-sized backup through it would
evict the hot working set. Add `BlockDevice::pread_nocache(buf, offset)`
(default impl = `pread`); `CachingDevice` overrides it (hit → serve from
cache including dirty blocks; miss → `load_from_inner` **without insert**);
`SubDevice`/`StreamingWriteDevice` delegate.

### Headroom: backups fail, writes never do

With reuse pinned, allocation only advances the high-water mark; at
`next >= segment_count` allocation returns `DeviceFull` — client-visible
write errors. Worked example: 100k creates+relocates/s × ~1 KiB ≈ 100 MB/s ≈
12.5 segments/s ≈ **45 GB of virgin headroom per hour of backup window**.
Controls:

- **Pre-flight**: per store, virgin headroom
  `segment_count - 1 - max(open_segment, highest_used)` must be
  `≥ min_headroom_segments` (default 64 = 512 MiB/store) or the POST is
  refused (507).
- **Monitor**: re-checked every throttle tick; below
  `abort_headroom_segments` (default 16) the job aborts and unpins.
- **RAII**: every pin lives in `BackupPinGuard`; `Drop` unpins on abort,
  error, or panic.

### Blob store

Delete does not unlink blobs inline — reclamation is the periodic GC sweep
only (`engine.rs:7064-7066`). With the sweep pinned, blob files are immutable
and written tmp+rename (`blobstore.rs:17-20`, suffixes at `56-59`). The tree
is copied **after** `T` is fixed, skipping `*.tmp` and tolerating ENOENT:
every blob referenced by an entry `≤ T` exists (the blob rename precedes the
create's journal append), and post-`T` blobs copied as extras are exactly the
orphans `reconcile_blobs_after_recovery` (`recovery.rs:1358-1392`, run before
serving at `bin/server.rs:1184`) deletes on restore boot.

### Multi-device / `device_split` geometry

Stores = `device_paths × device_split`; `split_device`
(`subdevice.rs:243-260`) carves `region = (total / k) / align * align`,
subdevice *i* at physical base `i * region`; each store's allocator header is
at store-relative 0, data region at `base + 1 MiB`. The backup emits one
sparse image per physical device, placing each store's header + used segments
at physical offsets computed from config-derived geometry (the same formula
as `split_device`). The manifest records the full geometry; restore validates
it against the target config and refuses on mismatch.

## Failure matrix

| Failure | Behavior |
|---|---|
| Pre-flight (unsupported config, low headroom, no `backup_dir`, job running) | POST rejected 400/409/507; nothing pinned |
| Headroom hits abort threshold mid-copy | Job → Failed, pins dropped (RAII), partial dir manifest-less |
| Backup-dir I/O or checksum-stream error | Job → Failed, unpin, partial dir cleaned on next run |
| Tee buffer overflow (slow sink) | Job → Failed, tees detached, writers never blocked |
| Catch-up non-convergence | Job → Failed after `max_catchup_rounds` |
| Server crash mid-backup | Pins vanish with the process; dir has no manifest → ignored/cleaned |
| `DELETE /admin/backup` | Cooperative cancel at next chunk boundary; unpin; Failed(aborted) |
| Checkpoint error during window | Independent — backup has its own snapshot/fence/tail |
| Restore onto live server / bad checksum / wrong geometry | Refused before touching any target file |

## Restore (offline)

`teraslab-cli restore <backup-dir> --config <config>`:

1. Validate manifest version, per-file SHA-256, geometry vs target config.
2. Refuse if a live server holds the instance lock. **New requirement**: the
   server takes an exclusive `flock` on `device_paths[0]` at startup (none
   exists today); restore attempts a non-blocking flock and refuses if held.
3. Write each store's header + listed segment ranges into the device paths
   (virgin regions left sparse/zero); install the fabricated redo files at
   `resolved_redo_log_path()` (+ `.{i}`); place `.snap`, `.height`, and the
   blobstore tree at their configured paths.
4. Boot normally. Recovery loads `.snap`, replays `(F, T]` merged by global
   sequence (`recover_all_multi_store`, `recovery.rs:606`), recomputes
   cursors/free lists from the rebuilt index (`recovery.rs:3585-3637`,
   `segment_allocator.rs:756-771`), reconciles orphan blobs.

## Operator surface

### Config (`[backup]`, default-off)

```toml
[backup]
backup_dir = "/mnt/backups/teraslab"    # Option<PathBuf>; None (default) = disabled
throttle_bytes_per_sec = 268435456       # 256 MiB/s
min_headroom_segments = 64
abort_headroom_segments = 16
stall_copy_max_segments = 4
max_catchup_rounds = 10
tee_buffer_max_bytes = 268435456
```

`Config::validate()`: `backup_dir` set ⇒ `enable_admin_endpoints = true`
required; `abort_headroom_segments < min_headroom_segments`;
`stall_copy_max_segments ≥ 1`; warn if `backup_dir` lies inside a device
path's directory.

### HTTP (bearer `admin_token`)

- `POST /admin/backup` body `{"dir": "sub/name"}` (optional, resolved under
  `backup_dir` — arbitrary target paths are refused so an admin-token holder
  cannot exfiltrate elsewhere) → 202 `{job_id}` | 400 | 409 | 507.
- `GET /admin/backup/status` → `{state, phase, fence, tail_end, bytes_copied,
  bytes_total_estimate, segments_total, segments_copied, catchup_round,
  started_at, finished_at, error, manifest_path}`.
- `DELETE /admin/backup` → 200 | 404.

### CLI

- `teraslab-cli backup run [--dir NAME] [--wait]` / `backup status` /
  `backup abort` — HTTP client wrappers.
- `teraslab-cli restore <backup-dir> --config <cfg> [--force]` — offline, no
  HTTP.

### Manifest (`MANIFEST.json`, written last)

```json
{ "manifest_version": 1, "teraslab_version": "…", "created_at": "…",
  "fence": F, "tail_end": T,
  "engine": {"kind":"segment","seg_header_version":2,"redo_header_version":2,"packed":true},
  "geometry": {"device_count":N,"device_size":…,"device_split":K,"alignment":4096,
               "segment_size":8388608,
               "stores":[{"store":0,"device":0,"base":0,"device_id_hex":"…",
                          "segment_count":…,
                          "used_segments":[[id,"sha256",len],…],
                          "header_sha256":"…"}]},
  "files": {"index_snapshot":{"name":"teraslab-index.snap","sha256":"…","len":…},
            "height":{…},
            "redo":[{"store":0,"name":"redo.0","sha256":"…","len":…}],
            "device_images":[{"device":0,"name":"device.0.img"}],
            "blobs":[{"rel":"ab/cd/…","sha256":"…","len":…}]} }
```

### Metrics (`/metrics`, must pass `prometheus_conformance.rs`)

`teraslab_backup_state` (enum-labeled gauge),
`teraslab_backup_bytes_copied_total`,
`teraslab_backup_throttle_bytes_per_sec`,
`teraslab_backup_last_success_timestamp_seconds`,
`teraslab_backup_runs_total{result}`, `teraslab_backup_pinned`,
`teraslab_backup_headroom_segments_min`.

## Performance impact analysis

- **Reads**: throttled (token bucket, default 256 MiB/s) sequential
  `pread_nocache` — no page-cache pollution of the hot set, no fsync of the
  live files, no forced checkpoint.
- **Locks**: the copier acquires 32 read guards per 128 KiB chunk (one per
  4 KiB block, out of 65,536 stripes), held only across the chunk's pread
  (~50-500 µs); throttle sleeps happen outside guards. A concurrent writer
  collides only if its `(offset >> 12)` hits one of those 32 stripes during
  the hold — ~0.05% per write, microsecond stalls.
- **Stalls**: exactly two visibility-guard acquisitions (fence + finalize),
  each bounded (the finalize one copies at most `stall_copy_max_segments`
  segments into RAM). Same guard the checkpoint already takes on every cycle.
- **Write amplification**: none on the data path; the tee buffers frames in
  RAM (bounded) and writes them to the backup dir only.
- **Space**: pinned reuse grows the device high-water for the window
  (headroom math above); pinned blob GC defers unlinks.

## Module layout (implementation)

`src/backup/` — `mod.rs`, `job.rs` (state machine, single-flight lease, RAII
pins, headroom monitor), `copier.rs` (chunked guarded reads, sparse image
writer, SHA-256, token-bucket throttle), `redo_tail.rs` (tee sink +
fabricated-file writer), `manifest.rs` (versioned serde), `restore.rs`
(shared with CLI). Hooks:

- `src/allocator.rs`: `RecordAllocator` gains `set_lifecycle_pinned(bool)`
  (default no-op) and `backup_view()` (default `None` → refuses backup for
  the in-place allocator).
- `src/segment_allocator.rs`: `pinned` flag gating reclaim/defrag/free-list
  pop; `serialize_header_bytes()` factored from `persist_header_no_sync`.
- `src/ops/engine.rs`: engine-level pin (`AtomicBool`) short-circuiting
  `defrag_reclaim_fully_dead` / `defrag_compact`.
- `src/storage/blob_gc.rs`: pause flag consulted by the sweep loop.
- `src/redo.rs`: `attach_tee` / `detach_tee` invoked in
  `buffer_entry`/`buffer_preencoded` under the log mutex.
- `src/io.rs`: `read_span_blocks` — read-side mirror of `lock_span_blocks`.
- `src/device.rs` + `src/bin/server.rs`: startup instance `flock`.
- `src/server/http.rs`: `BackupManager` in `HttpState`, three endpoints.
- `src/bin/cli.rs`: `backup` / `restore` commands.

## Follow-ups (explicitly deferred)

- **Incremental backup**: diff the per-segment `used`/`dead` table across
  manifests; sealed segments are byte-stable until reclaimed, so unchanged
  used segments can be skipped. Requires a per-segment generation stamp or
  table diffing to detect reclaim+reuse between runs.
- **Wire-streaming sink**: new opcodes (gated behind the `OP_HELLO` protocol
  version) streaming the same artifact set to a remote client, with explicit
  backpressure.
- **In-place engine / redb / cluster support**: each needs its own pin and
  copy-set analysis; refused in v1.
