# Dead Code & Dangerous-Pattern Inventory — TeraSlab `src/`

Scope: all of `src/` (69 files, ~117k LOC). Tests excluded from FINDING
classification but used to establish reachability. `cargo` was not run; all
results from `rg` + `Read`. Test-region filtering done by a brace-matching
`#[cfg(test)]` mapper, so counts are "non-test source" unless noted.

---

## Category 1 — `unwrap()` / `expect()` on fallible paths

**Total non-test hits: 216.** Breakdown:

- **166** are `<int>::from_le_bytes(slice[a..b].try_into().unwrap())` on
  **fixed, constant-width sub-slices** whose bounds were already validated
  by a preceding `need(...)` / `data.len() >= N` / `if data.len() < N`
  guard. `try_into()` on a `&[u8]` of statically-correct length is
  infallible; the `unwrap()` cannot fire. JUSTIFIED.
  - Representative: `src/redo.rs:209,217-219`, `src/replication/protocol.rs:829,885-886`,
    `src/replication/receiver.rs:1057-1061`, `src/index/migration.rs:693-700`,
    `src/cluster/topology.rs` decoders (paired with `checked_add`/`MAX_*` guards).
- **~30** are `RwLock::read()/write().unwrap()` in `src/cluster/topology.rs`
  (lines 682-1311). Poison-only panics; the codebase treats lock poisoning
  as a fatal invariant violation everywhere. JUSTIFIED (consistent policy),
  but see note below.
- **~16** are `.expect("…")` with a proven-invariant message:
  `src/cluster/auth.rs:73,94,133,397` (HMAC accepts any key length — true
  for `Hmac<Sha256>`), `src/locks.rs:128` (32-byte txid sub-slice),
  `src/device.rs:417,423` (alignment is a checked power-of-two),
  `src/protocol/codec.rs:1164` (length pre-checked by `checked_u32_len`),
  `src/server/mod.rs:865,967` ("checked above" — `auth_required` implies
  `cluster_secret.is_some()`). JUSTIFIED.
- Thread-spawn `.expect("spawn … thread")` at `src/checkpoint.rs:140`,
  `src/storage/blob_gc.rs:345`, `src/storage/uploader.rs:158`,
  `src/replication/manager.rs:602`, `src/server/dispatch.rs:98`. Spawn
  failure at startup = OOM/rlimit; panicking is acceptable boot-time
  behavior. JUSTIFIED.

The remaining hits are FINDINGs below.

### [LOW] `getrandom().expect()` can abort the process on entropy failure
- **Location:** `src/index/hashtable.rs:276`
- **What's wrong:** `getrandom::getrandom(&mut buf).expect("getrandom failed to produce a bucket seed")` panics if the OS RNG is unavailable.
- **Why it matters:** Project rule bans `expect()` in library code. A hash-table reseed during a resize would abort the whole server rather than return an error. In practice `getrandom` only fails on broken/sandboxed kernels, so impact is low, but it violates the no-panic-in-lib rule.
- **Reproduction:** Run on a platform where `getrandom` returns `Err` (seccomp without `getrandom` syscall).
- **Suggested fix:** Propagate via `HashTableError` or fall back to a time/address-seeded value with a logged warning.

### [LOW] `BlobUploadHandle::wait()` unwrap on completed result
- **Location:** `src/storage/uploader.rs:57` (`guard.take().unwrap()`)
- **What's wrong:** Unwraps the `Option<Result>` after the condvar loop. The loop only exits when `guard.is_some()`, so the unwrap is logically safe — but it relies on no other consumer having `take()`n the slot first. `wait(self)` consumes the handle, so there is exactly one consumer; the invariant holds.
- **Why it matters:** Library `unwrap()`; safe today but fragile if a second waiter is ever added. Borderline JUSTIFIED — flagged for visibility.
- **Suggested fix:** Replace with `unwrap_or(Err(BlobError::…))` or document the single-consumer invariant in code.

### [LOW] `cli.rs` HTTP client expect
- **Location:** `src/bin/cli.rs:187` (`.expect("failed to create HTTP client")`)
- **What's wrong:** Binary, not library. Panic on reqwest client build failure.
- **Why it matters:** It's a CLI binary `main` path, so panic-to-exit is acceptable. JUSTIFIED-by-context; listed only because it is a literal `expect`.

> Note on topology RwLock unwraps: these are not new findings — they are the
> established lock-poisoning policy. If the audit wants lock-poison resilience
> it is a project-wide decision, not a `topology.rs`-local bug. Flagged as a
> JUSTIFIED group, not a FINDING.

---

## Category 2 — `panic!` / `todo!` / `unimplemented!` / `unreachable!`

**Total non-test hits: 1.**

### JUSTIFIED — fault-injection panic (intentional)
- `src/fault_injection.rs:183` — `panic!("teraslab fault-injection: panic at {point:?}")`. This is the deliberate crash-simulation primitive; it is compiled only under `#[cfg(any(test, feature = "fault-injection"))]` (the production `mod inner` at line 194+ is a no-op). JUSTIFIED.

The known `src/replication/manager.rs:1911 unreachable!("send_batch panicked")` is **inside `#[cfg(test)] mod tests`** (a `PanickingTransport` test double, mod starts at line 1186). Test-only — correctly excluded. No production `unreachable!`/`todo!`/`unimplemented!` exist.

---

## Category 3 — swallowed errors on durability paths

Counts (non-test): `let _ =` 131, `.ok()` 76, `Err(_) =>` 72. The vast
majority are benign (hex-formatting `write!` into a `String`, metrics
`writeln!` into a buffer, UDP `send_to` best-effort gossip, `try_into().ok()?`
length checks, parse fallbacks). Durability-relevant swallows examined
individually below.

### [HIGH] Rollback slot-restore writes ignore device write failure
- **Location:** `src/server/dispatch.rs:2198` and `:2248` (`let _ = crate::io::write_utxo_slot(...)`)
- **What's wrong:** During replica-batch compensation (reverse Reassign / reverse PruneSlot), the slot-restore `write_utxo_slot` result is discarded. If the device write fails, the slot is left in the post-apply (wrong) state but the compensation reports success.
- **Why it matters:** This is the rollback path for a failed replicated mutation. A silently-dropped restore write diverges the replica's on-device UTXO state from the master while the batch is treated as cleanly rolled back — exactly the double-spend/divergence window the surrounding R-007/Gap-#8 comments warn about. The forward `comp_redo` entry is pushed regardless, so recovery *may* re-apply it, but the in-line write failure is invisible to the caller.
- **Reproduction:** Inject an `io::write_utxo_slot` error during a `compensate_replication_failure` reverse-Reassign and observe the batch still completes.
- **Suggested fix:** Capture the `Result`; on `Err`, propagate as `ERR_INTERNAL` so the master retries (mirrors the R-035 hard-fail discipline already applied to metadata writes at `receiver.rs:1064`).

### [LOW] Topology-state persistence discarded at 7 event-loop sites
- **Location:** `src/cluster/coordinator.rs:823, 1399, 1444, 1612, 1648, 2695` (`let _ = persist_topology_state(...)`)
- **What's wrong:** `persist_topology_state` returns `io::Result` and its doc explicitly says "Safety-critical callers MUST fail the request rather than reply when this returns `Err`." These six event-loop call sites discard it with `let _ =`.
- **Why it matters:** The doc-documented H10 invariant (a voter must never advertise a vote it could lose across a crash) is enforced only at the *vote handler* sites (`dispatch.rs:998,1071` via `persist_topology()`, which DO check). The discarded sites are proposer-bookkeeping / fallback / catch-up persists, not the safety-critical vote ack — and every failure is still counted in `PERSIST_FAILURES` and logged inside the function. So this is defense-in-depth, not a correctness hole. Downgraded to LOW; flagged because the `let _ =` visually contradicts the function's own MUST-check doc and a future refactor could move a vote-critical persist to one of these sites.
- **Reproduction:** N/A (no live correctness bug); inspect the contradiction.
- **Suggested fix:** Either route these through a `persist_topology_best_effort` wrapper named to signal intent, or assert at each site that it is not a vote-ack path.

### [LOW] `applied.flush()` failure logged-and-continued in receiver
- **Location:** `src/replication/receiver.rs:278` and `:930` (`if let Err(e) = self.applied.flush() { … }` — logs, does not abort)
- **What's wrong:** The replica-applied dedup tracker flush failure is logged but the batch still ACKs.
- **Why it matters:** Inspected the surrounding code — the data device fsync and redo flush happen *before* this and DO hard-fail; the `applied` tracker is the idempotency journal, and the comment block (lines 885-895) documents that recovery re-derives from the redo log, so a lost `applied` flush replays at-most-once-extra (idempotent ops). Acceptable. Borderline JUSTIFIED; listed for the durability-path completeness requirement.

### JUSTIFIED group — best-effort error response writes
- `src/replication/receiver.rs:472`, `src/server/mod.rs:496,721,742,804,897`, `src/server/dispatch.rs:6391,6403,6418,6462` (`let _ = stream.write_all(&resp.encode())` / `writer.abort()`). These are error-reply / connection-teardown writes on a socket that is already being dropped; a write failure there is unrecoverable and irrelevant. JUSTIFIED.

### JUSTIFIED group — `recovery.rs` `Err(_) => ReplayResult::Failed(...)` (≈30 sites)
- The `Err(_)` discards the *error value* but maps to a typed `ReplayCause` (`IoError`/`LogicError`/`MissingRecordBytes`) that halts replay. Information loss (no `{e}` in the cause) is a minor observability gap, not a swallow — control flow stops correctly. JUSTIFIED with a note: consider threading the source error into `ReplayCause` for post-mortem.

### JUSTIFIED group — gossip / UDP / metrics swallows
- `src/cluster/swim.rs` `let _ = socket.send_to(...)` (×11), `src/server/http.rs` `let _ = writeln!(...)` Prometheus rendering (×many), hex `let _ = write!(s, ...)` into `String` (`storage/manager.rs:86`, `blob_gc.rs:261`, `allocator.rs:1344`). UDP gossip is best-effort by protocol design; `writeln!`/`write!` into a `String`/`Vec` is infallible. JUSTIFIED.

### JUSTIFIED — blob `get().ok().flatten()` on migration baseline (3 sites)
- `src/cluster/coordinator.rs:4583, 5200`, `src/server/dispatch.rs:4945`. A blob-store read error during migration-baseline `Create` op construction collapses to `cold_data = None`. The receiver side (`receiver.rs:1072`) DOES hard-fail on blob *write*, and a missing cold blob on the source is treated as "no external data" — the manifest/generation handshake later detects divergence. This is a real but low-severity observability gap (a transient blob read error silently ships an incomplete Create); noting as borderline rather than a clear FINDING because the downstream manifest compare catches a persistent mismatch.

---

## Category 4 — `unsafe` blocks & safety comments

**Total non-test `unsafe` occurrences: ~111** across 10 files
(io.rs 41, hashtable.rs 25, device.rs 20, ops/engine.rs 13, record.rs 5,
bin/server.rs 4, plus singletons). Of these, ~65 have a nearby
Safety/Invariant/Soundness comment; the heuristic flagged 43 as "missing,"
but manual inspection shows most are covered by a **function-level `# Safety`
doc** or a shared module-level contract above the heuristic's 10-line window.

Manually verified — adequately documented despite heuristic miss:
- `src/io.rs` `*_direct` fns (lines 306, 384, 763, 802, 845, 879): each has a `# Safety` / `# F-X-007` doc section several lines above the `unsafe` body (e.g. lines 288-304 for `write_mutation_footer_direct`, 738-768 for `read_metadata_direct`). JUSTIFIED.
- `src/index/hashtable.rs` `unsafe impl Send/Sync` (579-580): preceded by a 19-line concurrency-contract comment (560-578). JUSTIFIED.
- `src/replication/tcp_transport.rs:43`: inline `// SAFETY:` comment present at 50-54 (heuristic counted the outer `unsafe {` line). JUSTIFIED.
- `src/bin/server.rs:71` getifaddrs: `// SAFETY:` at 66-70. JUSTIFIED.
- `src/cluster/swim.rs:486` setsockopt: no per-block SAFETY comment but the FFI is a trivial `setsockopt(SO_RCVBUF)` with a stack `c_int`. See finding below.

### [LOW] `ops/engine.rs` direct-I/O `unsafe` call sites lack local safety comments
- **Location:** `src/ops/engine.rs:765, 814, 872, 894, 1469, 1585, 1705, 2022, 3584, 3769, 4397` (`unsafe { io::read/write_*_direct(self.device_ptr, ...) }`)
- **What's wrong:** Each calls an `unsafe fn` whose contract is "`device_ptr` non-null, within bounds, caller holds stripe lock." The call sites guard `!self.device_ptr.is_null()` but carry no `// SAFETY:` comment asserting the bounds/lock invariant the callee requires.
- **Why it matters:** Project rule: every `unsafe` needs a comment justifying invariants. The invariant *is* upheld (offset comes from a validated index entry; record-level lock taken inside the callee), but it is undocumented at the call boundary, so a future edit could pass an unchecked offset without tripping a reviewer.
- **Reproduction:** Code review only.
- **Suggested fix:** Add a one-line `// SAFETY: device_ptr non-null (checked); record_offset from validated index entry; callee takes record lock.` at each site, or wrap in a safe `impl` method.

### [LOW] `index/hashtable.rs` raw libc mmap/msync/madvise blocks lack per-block SAFETY comments
- **Location:** `src/index/hashtable.rs:309, 326, 464, 476, 483, 599, 715, 747, 750, 1009, 1223, 1424, 1425, 1426, 1436` (mmap/msync/madvise/close/write_bytes/`&*ptr.add(i)`)
- **What's wrong:** The mmap lifecycle `unsafe` blocks rely on `byte_len` being a checked `capacity * size_of::<Bucket>()` and `ptr`/`mmap_len` being live for the table's lifetime, but the individual blocks have no `// SAFETY:` annotation (the module-level contract at 560-578 covers Send/Sync, not the mmap pointer arithmetic).
- **Why it matters:** Same rule violation. `byte_len` is `checked_mul`-guarded (line 298) and `ptr` validity is structurally maintained, so these are sound — but undocumented. `:715` (`&*ptr.add(i)`) in particular dereferences a raw bucket pointer with only the loop bound as the guarantee.
- **Suggested fix:** Annotate each block; the invariants already exist, they just need to be written down.

### [LOW] `cluster/swim.rs:486` setsockopt unsafe without SAFETY comment
- **Location:** `src/cluster/swim.rs:486`
- **What's wrong:** `setsockopt(SO_RCVBUF)` FFI block, no `// SAFETY:` (compare `tcp_transport.rs:43` which has one).
- **Suggested fix:** Add the same comment used in `tcp_transport.rs`.

> `src/record.rs:571,669` (`from_raw_parts` over `repr(C, packed)` metadata) and `src/device.rs:445,456,632,661` DO carry `// Safety:` comments. JUSTIFIED.

---

## Category 5 — narrowing `as` casts on network/disk-derived values

**Total `as u32/u16/u8` non-test hits: 224.** Almost all are **encode-side**
length casts (`(x.len() as u32).to_le_bytes()`) where the length is bounded
by `MAX_FRAME_SIZE`/`MAX_DECODE_BATCH` or a small in-memory collection.
Decode-side narrowing is the risk; inspected those.

Decode-side `as usize` casts from `u32`/`u64` headers
(`src/replication/protocol.rs:886,912`, `durable.rs:223,432`,
`topology.rs:152` etc.) are all on a 64-bit target where `u32 as usize` is
**widening, not narrowing** — no truncation — and each is bounds-checked by a
following `need(...)`/`checked_add`/`MAX_*` guard before allocation.
JUSTIFIED. The encode-side `len() as u32` casts (`frame.rs:90,226`,
`redo.rs:1518`, etc.) can only truncate if a payload exceeds 4 GiB, which the
`MAX_FRAME_SIZE` (16 MiB) ceiling and `checked_u32_len` guards prevent.
JUSTIFIED.

No FINDINGs. One note:

### JUSTIFIED-with-note — encode-side `inner_len as u32` not pre-checked at frame.rs:90/226
- `src/protocol/frame.rs:90` (`RequestFrame::encode`) and `:226` (`ResponseFrame::encode`) compute `total_length = inner_len as u32` from `payload.len()`. There is no assertion that `inner_len <= u32::MAX` at the encode site. In practice payloads are bounded by the decoders' `MAX_FRAME_SIZE`, and these are outbound frames the server itself built, so a >4 GiB payload is not reachable. Acceptable, but a `debug_assert!(inner_len <= u32::MAX as usize)` would make the invariant explicit. Not a FINDING.

---

## Category 6 — unchecked integer arithmetic on network/disk values

Reviewed shifts/multiplies on decoded values. All allocation-sizing
multiplies in decoders are **guarded**:
- `src/replication/protocol.rs:658` `need(rest, pos + hash_count * 32)?` precedes `Vec::with_capacity(hash_count)` — but `hash_count * 32` itself is unchecked (see finding).
- `src/cluster/topology.rs:156,312` use `count.checked_mul(8)?` + `checked_add`. JUSTIFIED.
- `src/redo.rs:1291` caps `parents_count_raw > MAX_CREATE_V2_PARENTS` before `with_capacity`. JUSTIFIED.
- `src/protocol/codec.rs` decoders all run the two-stage `MAX_FRAME_SIZE/per_item_min` + `max_batch_size` guard (documented at opcodes.rs:467, codec.rs:9). JUSTIFIED.

### [LOW] `hash_count * 32` and `meta_len` arithmetic in OP_CREATE decoder unchecked before `need`
- **Location:** `src/replication/protocol.rs:658` (`need(rest, pos + hash_count * 32)?`), `:660` (`Vec::with_capacity(hash_count)`)
- **What's wrong:** `hash_count` is a `r_u32(...) as usize` from the wire. `hash_count * 32` and `pos + hash_count*32` are computed with normal (panicking-in-debug, wrapping-in-release) arithmetic. On 64-bit, `u32::MAX * 32 ≈ 1.3e11` fits in `usize` so it cannot wrap, and `need` then rejects it because the frame is `MAX_FRAME_SIZE`-bounded. So no live overflow — but `Vec::with_capacity(hash_count)` runs only *after* `need` confirms the bytes exist, which caps `hash_count` at ~`MAX_FRAME_SIZE/32`. Safe on 64-bit; would be a truncation/overallocation risk on 32-bit.
- **Why it matters:** Defense-in-depth only on the supported (64-bit) target. The `protocol.rs` decoder lacks the explicit `checked_mul` the sibling `topology.rs`/`codec.rs` decoders use — an inconsistency (Rule 6 territory) more than a bug.
- **Suggested fix:** Use `checked_mul`/`checked_add` for parity with the other decoders, or add a `hash_count` ceiling like the `MAX_CREATE_V2_PARENTS` cap.

---

## Category 7 — fire-and-forget `thread::spawn` / `tokio::spawn`

**Total non-test spawn sites: ~24** (`redb::Builder::new`/`thread::Builder`
filtered for false positives). Lifecycle assessment:

JUSTIFIED — return a `JoinHandle` and/or are driven by an `Arc<AtomicBool>`
shutdown flag:
- `src/checkpoint.rs:135`, `src/storage/blob_gc.rs:303` — return `JoinHandle`, cooperative `shutdown` flag. JUSTIFIED.
- `src/replication/durable.rs:864` (`spawn_lag_monitor`) — returns `JoinHandle`, `shutdown` flag. JUSTIFIED.
- `src/storage/uploader.rs:153`, `src/cluster/swim.rs:464` — handle retained in struct. JUSTIFIED.
- `src/bin/server.rs:1474` — joiner thread with `recv_timeout`. JUSTIFIED.

### [LOW] Detached HTTP server thread, no join/shutdown handle
- **Location:** `src/bin/server.rs:988` (`std::thread::spawn(move || { start_http_server(...) })`)
- **What's wrong:** The admin/metrics HTTP server is spawned detached; no handle stored, no shutdown signal. On server shutdown it is simply abandoned (process exit reaps it).
- **Why it matters:** Acceptable for a metrics endpoint at process teardown, but means in-flight admin requests are cut without drain, and in a test/embedded-host context the thread leaks. Low severity (binary, process-lifetime thread).
- **Suggested fix:** Store the handle / pass a shutdown flag if graceful HTTP drain is ever wanted. Otherwise document the detach as intentional.

### [LOW] OTLP shutdown helper thread detached
- **Location:** `src/observability/mod.rs:280`
- **What's wrong:** Spawns a thread to run the blocking `provider.shutdown()` and signals completion over a channel with `recv_timeout`. If it times out, the thread is leaked (continues draining after the timeout returns).
- **Why it matters:** This is the documented design (enforce a wall-clock bound on a synchronous SDK call). Leak is bounded to one thread at process shutdown. Borderline JUSTIFIED — listed for completeness.

### [LOW] Coordinator per-migration / per-exchange worker threads detached
- **Location:** `src/cluster/coordinator.rs:738, 886, 1074, 1232, 1352, 1459, 1496, 2103, 2183, 3288, 3821`
- **What's wrong:** Migration-worker, exchange-phase, and orphan-cleanup threads are `thread::spawn`ed detached. Most do their own epoch-fencing checks and exit when the topology epoch advances (`migration_epoch_current`), so they self-terminate — but there is no central join/tracking on coordinator shutdown.
- **Why it matters:** On a fast restart or rapid topology churn, detached migration workers can still be running against the old epoch; they are fenced (checks at e.g. coordinator.rs:3431 region) so they cannot corrupt state, but they are not awaited. This is the existing clustering design; flagged as LOW because correctness rests entirely on the epoch-fence checks, not on lifecycle management.
- **Suggested fix:** Track handles in the coordinator for deterministic shutdown, or document that epoch-fencing is the sole termination mechanism.

---

## Category 8 — dead code

`#[allow(dead_code)]` items: 6 (excluding the 3 doc-comment mentions). Each
inspected:

### JUSTIFIED — genuinely reachable / intentionally retained
- `src/server/dispatch.rs:1891` `BeforeImage::Prune` variant — constructed by `tests/replication_rollback.rs`; kept for a future prune-client rollback API. Doc-explained. JUSTIFIED-retained.
- `src/server/dispatch.rs:6638` `classify_spend_error` — used by tests. JUSTIFIED-retained.
- `src/record.rs:637` `from_bytes_unchecked` — `pub(crate)`, zero in-crate callers today, kept for future diagnostics. Honest dead code, correctly marked. JUSTIFIED-marked.

### [LOW] `migrate_single_shard` + `send_delta_ops` — partly-dead reference path
- **Location:** `src/cluster/coordinator.rs:4042` (`migrate_single_shard`, `#[allow(dead_code)]`) and `:5318` (`send_delta_ops`)
- **What's wrong:** `migrate_single_shard` is the "reference" per-shard migration flow; the comment says production uses `run_migration_batch`. It is `#[allow(dead_code)]` and **has no non-test caller** (only referenced in a test doc-comment at 8243). `send_delta_ops`, however, IS called by the production batch path (`coordinator.rs:3595, 4307`), so its `#[allow(dead_code)]` is now **stale/wrong** — it is not dead.
- **Why it matters:** `migrate_single_shard` is ~250 lines of unmaintained parallel migration logic kept "as reference" — a divergence hazard (Rule 6): two migration implementations, only one tested in production. `send_delta_ops`'s incorrect `#[allow(dead_code)]` masks the fact that it's live, so clippy won't warn if it later genuinely becomes dead.
- **Suggested fix:** Delete `migrate_single_shard` (git history is the reference), and remove the now-false `#[allow(dead_code)]` from `send_delta_ops`.

### [LOW] `persist_peak_cluster_size` dead except tests
- **Location:** `src/cluster/coordinator.rs:5524` (`#[allow(dead_code)]`)
- **What's wrong:** "Backward-compatible alias" `persist_peak_cluster_size` — only callers are `coordinator.rs:9081,9085`, both inside `#[cfg(test)] mod tests`. No production caller.
- **Why it matters:** A back-compat shim with no remaining back-compat consumer. Test-only helper masquerading as production API.
- **Suggested fix:** Move it into the test module or delete and inline `persist_cluster_state(path, peak, 0)` in the two tests.

### [LOW] `StorageManager.allocator` field dead; `read_output_at` "simplified"
- **Location:** `src/storage/manager.rs:137` (`#[allow(dead_code)] allocator`), `:402` (doc: "simplified implementation — for production…")
- **What's wrong:** The `allocator` field is retained "for tests and constructor compatibility" but production cold data no longer allocates a device region. `StorageManager` itself is reachable only via the static `inline_cold_offset` associated fn (`ops/engine.rs:2643`) and via tests (`g9_001`, `g9_002`); the instance methods (`read_cold_data`/`read_output_at`/`stream_cold_data`) have **no production instance caller** — production reads cold data through `Engine::read_cold_data` (`ops/engine.rs:2599`), which does NOT go through `StorageManager`.
- **Why it matters:** `StorageManager` is effectively a test-and-one-static-fn type carrying a dead `parking_lot::Mutex<SlotAllocator>` field. The `read_output_at` "simplified implementation" doc-comment is a documented incomplete-feature marker on a method nothing in production calls. This is a whole-struct reachability question the suspect list flagged — confirmed: instance methods are test-only.
- **Reproduction:** `rg 'StorageManager' src` → only `inline_cold_offset` (static) is hit in production; instance methods only in `tests/`.
- **Suggested fix:** Either delete the instance side of `StorageManager` (keep `inline_cold_offset` as a free fn) or wire `Engine::read_cold_data` through it. The dead `allocator` field should go regardless.

### Reachability confirmations for other suspects (NOT dead)
- **`src/replication/manager.rs` (`ReplicationManager`)** — re-exported via `src/replication/mod.rs:19` and the type/`AckPolicy`/`ResyncRequest`/`ReplicaTransport` are used throughout `server/dispatch.rs` and `cluster/coordinator.rs` (config.rs:912, dispatch.rs:26/1831/1835, coordinator.rs:507/693/5641/5944/6543). **REACHABLE — not dead.** (The earlier audit-thread "test-only?" suspicion is wrong: `ReplicaTransport` is the production replication interface.)
- **`src/storage/uploader.rs` (`BlobUploader`)** — used by `tests/g9_003`, `g9_008`, and referenced from `storage/blobstore.rs:47`/`manager.rs:184`. Whether it is wired into the production write path could not be confirmed from `src/` alone (no `BlobUploader::with_capacity`/`::new` call found outside tests in `src/`). **Likely test-only / not yet wired into `bin/server.rs`.** Flag for the storage-I/O audit thread to confirm; if production cold-blob writes go straight through `BlobStore::put` (as `receiver.rs:1074` does), then `BlobUploader` is an unwired async-upload subsystem. Treating as a LOW dead-code candidate pending that confirmation.

### [LOW] `BlobUploader` may be unwired in production
- **Location:** `src/storage/uploader.rs` (whole module, 489 LOC)
- **What's wrong:** No `BlobUploader` constructor call exists in `src/` outside `#[cfg(test)]` / `tests/`. Production cold-blob persistence on the replica path calls `BlobStore::put` directly (`receiver.rs:1074`).
- **Why it matters:** A 489-line bounded-queue async uploader subsystem with its own metrics that may never run in production. If intentional (future wiring), fine; if it was meant to be the write path and isn't connected, cold-blob writes are synchronous on the hot path instead.
- **Suggested fix:** Confirm with the storage-I/O thread whether `bin/server.rs` is supposed to construct it. If not yet, add a `// TODO: wire into server startup` or remove until needed.

---

## Category 9 — `TODO` / `FIXME` / `HACK` / `XXX` / "for now" / "simplified"

**Total non-test hits: 3.** (No `TODO`/`FIXME`/`HACK`/`XXX` literals remain
in non-test src — the codebase has been scrubbed.)

### [LOW] "simplified implementation" on `StorageManager::read_output_at`
- **Location:** `src/storage/manager.rs:402` — `// This is a simplified implementation — for production, the cold data format would need per-output indexing for O(1) access.`
- **What's wrong:** Documented incomplete implementation: `read_output_at` reads the *entire* cold blob and linearly extracts one output (O(n) per access).
- **Why it matters:** It's on a (per Category 8) test-only method, so production isn't paying the cost — but the project's "no simplified-for-now" rule is technically violated by this marker. If `StorageManager` is ever wired into production, this becomes an O(n)-per-output read.
- **Suggested fix:** Either implement per-output indexing or remove the method along with the dead instance API.

### JUSTIFIED — descriptive "for now" comments (not deferred work)
- `src/lib.rs:3` — `// …internal modules stay `pub` for now because they have…` — a deliberate visibility decision with rationale, not a stub. JUSTIFIED.
- `src/server/dispatch.rs:5182` — `// …for now the synthesised-frame approach is wired so the rollback semantics stay correct.` — documents an *intentional* current design (with the structural-fix path noted for later), and the rollback logic is fully implemented. Not a stub. JUSTIFIED.

---

## Summary of FINDINGs (severity-ordered)

| Sev | Location | One-line |
|-----|----------|----------|
| HIGH | server/dispatch.rs:2198,2248 | Rollback slot-restore `write_utxo_slot` failure silently ignored → replica divergence |
| LOW | index/hashtable.rs:276 | `getrandom().expect()` aborts process on entropy failure (lib-panic rule) |
| LOW | storage/uploader.rs:57 | `wait()` unwrap relies on single-consumer invariant (undocumented) |
| LOW | cluster/coordinator.rs:823,1399,1444,1612,1648,2695 | `persist_topology_state` result discarded at 6 sites vs its own MUST-check doc |
| LOW | replication/receiver.rs:278,930 | `applied.flush()` failure logged-and-ACK'd (idempotent, borderline) |
| LOW | ops/engine.rs (11 sites) | direct-I/O `unsafe` calls lack local SAFETY comments |
| LOW | index/hashtable.rs (15 sites) | mmap/msync `unsafe` blocks lack per-block SAFETY comments |
| LOW | cluster/swim.rs:486 | setsockopt `unsafe` lacks SAFETY comment |
| LOW | replication/protocol.rs:658 | `hash_count * 32` unchecked mul in OP_CREATE decoder (safe on 64-bit, inconsistent) |
| LOW | bin/server.rs:988 | HTTP server thread detached, no join/shutdown handle |
| LOW | observability/mod.rs:280 | OTLP shutdown helper thread leaks on timeout (by design) |
| LOW | cluster/coordinator.rs (11 sites) | migration/exchange worker threads detached, rely on epoch-fencing |
| LOW | cluster/coordinator.rs:4042,5318 | `migrate_single_shard` dead reference path; `send_delta_ops` stale `#[allow(dead_code)]` (it IS live) |
| LOW | cluster/coordinator.rs:5524 | `persist_peak_cluster_size` dead except tests |
| LOW | storage/manager.rs:137,402 | `StorageManager` instance API + `allocator` field test-only; `read_output_at` "simplified" |
| LOW | storage/uploader.rs (module) | `BlobUploader` likely unwired in production (confirm with storage thread) |

Only one finding rises above LOW. The codebase is unusually disciplined:
`try_into().unwrap()` on validated slices, lock-poison `unwrap()` as policy,
`# Safety` docs on the FFI-heavy modules, and decoder allocation guards are
consistently present. The genuine risk is the HIGH rollback-write swallow.
