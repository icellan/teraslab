# Phase 2 — G1 (core data plane) findings

Files reviewed:
- `src/device.rs` (1485 LOC)
- `src/io.rs` (974 LOC)
- `src/record.rs` (1451 LOC)
- `src/allocator.rs` (2475 LOC)
- `src/device_io/mod.rs` (111 LOC)
- `src/device_io/sync_fallback.rs` (326 LOC)
- `src/device_io/io_uring_backend.rs` (574 LOC)
- `src/locks.rs` (140 LOC)
- `src/fault_injection.rs` (310 LOC)

Prior audits (`AUDIT.md`, `AUDIT_CODEX.md`) referenced for orientation only. Prior `BC-02` (read paths violate stripe-lock contract) has been documented away in code — the safety doc on `io::read_metadata_direct` now explicitly states reads do not hold the lock and the contract is "torn read → CRC fail → retry". Prior `IJK-04` (device_io dead code) is partly stale: `device_io/mod.rs::create_device_io` exists, but the orchestrator wiring is out of G1 scope.

---

### F-G1-001: SyncFallback loses errno on I/O failure — `Completion::result` violates documented contract
- **Severity**: HIGH
- **Category**: Correctness
- **Location**: `src/device_io/sync_fallback.rs:89-113`
- **Code**:
  ```rust
  let result = match op.kind {
      OpKind::Read  => unsafe { libc::pread(op.fd, op.buf_ptr as *mut _, op.len, op.offset as libc::off_t) },
      OpKind::Write => unsafe { libc::pwrite(op.fd, op.buf_ptr as *const _, op.len, op.offset as libc::off_t) },
  };
  completions.push(Completion {
      user_data: op.user_data,
      result: result as i32,
  });
  ```
- **Issue**: `libc::pread`/`pwrite` returns `-1` on error and sets `errno`. The trait's `Completion::result` is documented at `src/device_io/mod.rs:36-37` as "Bytes transferred, or negative errno on failure" — meaning callers expect `result == -EIO`, `-ENOSPC`, etc. `SyncFallback` ignores `errno` entirely and writes a bare `-1`, so every distinct failure (disk full, bad fd, EIO, EAGAIN) is collapsed to the same indistinguishable code. Metrics in `io_uring_backend::IoUringBackend::drain_completions` (`src/device_io/io_uring_backend.rs:154-161`) feed `record_completion_error(result)` from this value; on the sync backend that telemetry is permanently wrong.
- **Impact**: On non-Linux production (or on Linux when io_uring init falls back) the operator cannot distinguish a transient ENOSPC from a fatal EIO from the metrics or from any caller branching on `result`. A future caller that translates `result` to an `io::Error::from_raw_os_error(-result)` will produce a meaningless "unknown error" entry. This also masks short reads/writes: a partial pread that returns e.g. `100` would be reported faithfully, but the sync fallback's batched API never surfaces "short transfer" as a distinct condition either.
- **Recommendation**: When `result < 0`, read `std::io::Error::last_os_error().raw_os_error().unwrap_or(libc::EIO)` and store `-(errno as i32)` so the contract matches io_uring's CQE encoding. Add a unit test that injects a bad fd and asserts `result == -libc::EBADF`.
- **Confidence**: High

---

### F-G1-002: Targeted-write helpers in `io::write_*_footer_direct` leave header in CRC-invalid state if caller forgets the `write_crc_direct` finalizer
- **Severity**: MEDIUM
- **Category**: Correctness
- **Location**: `src/io.rs:72-196` (esp. lines 101-105, 165-168)
- **Code**:
  ```rust
  // Callers MUST follow with [`write_crc_direct`] using a meta snapshot
  // that reflects the final disk state of ALL fields (including those
  // written by preceding targeted helpers). Without the CRC restamp a
  // subsequent read will return `DeviceError::RecordCorruption`.
  ```
- **Issue**: The contract is documented in prose only. Every footer-writer mutates header bytes in place WITHOUT updating the CRC slot, and relies entirely on the caller to chase the write with `write_crc_direct`. A missing finalizer leaves the on-disk header CRC stale relative to the freshly written field bytes — every subsequent `read_metadata_direct` then surfaces `RecordCorruption` until something else rewrites the header. There is no debug-only sentinel, no typestate, no test that proves every call site in `src/ops/` follows the contract.
- **Impact**: A bug in any caller path (or a future op added without reading this doc) silently bricks a record. The error is fail-loud (CRC mismatch → `ERR_INTERNAL`) so a UTXO doesn't go quiet, but every read of that record fails until the next full write. Hypothesis: combined with a recovery path that does `read_metadata` → `mutate` → `write_metadata`, a CRC-invalid record will be skipped or rewritten — but a `read_metadata_direct` from a hot operation will see `RecordCorruption` and retry forever if the rewriter is gated by the same read.
- **Recommendation**: Either (a) make the footer writers private and expose a single `write_footer_and_crc_direct(...)` wrapper that always restamps the CRC, or (b) add a `#[must_use = "must be sealed with write_crc_direct"]` guard struct returned by each footer helper that panics on drop in debug builds if not consumed.
- **Confidence**: Medium

---

### F-G1-003: `read_metadata_direct` Acquire fence does not establish happens-before in Rust's memory model
- **Severity**: MEDIUM
- **Category**: Concurrency
- **Location**: `src/io.rs:237-266`
- **Code**:
  ```rust
  // Note: Rust's strict memory model says fences
  // alone don't establish happens-before without a paired
  // atomic load/store; in practice the AArch64 hardware
  // barrier prevents the reorderings we care about, and
  // the CRC remains the true safety net.
  std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);
  let src = base_ptr.add(record_offset as usize);
  let bytes = std::slice::from_raw_parts(src, METADATA_SIZE);
  Ok(TxMetadata::from_bytes(bytes)?)
  ```
- **Issue**: The author's own comment admits the fix is hardware-shaped, not language-shaped: a Rust `fence(Acquire)` paired with `fence(Release)` does NOT establish happens-before unless one side is an actual atomic load and the other an atomic store on the same location (C++20 [atomics.fences] / Rust follows the C++ model). The 320-byte `from_raw_parts` read is a non-atomic memcpy of plain bytes — UB under strict Rust memory model when racing with the non-atomic memcpy in `write_metadata_direct`. The CRC is the only real correctness gate. Calling this safe — even with the fence — is misleading.
- **Impact**: `cargo miri` and any future borrow-aware concurrency checker will flag every direct read/write race on the data region as UB. The CRC catches torn results but the underlying read is still defined to be UB in the language. The hardware reality on x86/AArch64 happens to make the right thing occur, but a future LTO/optimizer that knows the read is racing data may legally fold the load or assume non-aliasing.
- **Recommendation**: Either (a) convert the metadata reads/writes to a sequence of `AtomicU64::load(Relaxed)` / `AtomicU64::store(Relaxed)` ops on 8-byte chunks (legal racing access), or (b) document the dependency on volatile/Atomic semantics and treat the current shape as "rely on hardware + CRC, accept the UB-on-paper risk" with a runbook entry. The current state is somewhere in between: the doc says the fence works "in practice on AArch64" while disclaiming the model.
- **Confidence**: Medium

---

### F-G1-004: `MemoryDevice` exposes both `data: RwLock<Vec<u8>>` and `raw_ptr` aliasing the same heap allocation — concurrent use is UB
- **Severity**: MEDIUM
- **Category**: Concurrency
- **Location**: `src/device.rs:329-372`, used at `src/device.rs:434-436`
- **Code**:
  ```rust
  pub struct MemoryDevice {
      data: parking_lot::RwLock<Vec<u8>>,
      raw_ptr: *mut u8,
      raw_len: usize,
      alignment: usize,
  }
  // ...
  let mut data = vec![0u8; size as usize];
  let raw_ptr = data.as_mut_ptr();
  // ...
  Self { data: parking_lot::RwLock::new(data), raw_ptr, ... }
  ```
- **Issue**: `data.write()` returns a `&mut Vec<u8>` (an exclusive borrow), and `pwrite` writes bytes through that guard. Concurrently, `as_raw_ptr()` callers in the hot direct-path can read/write those same bytes via `raw_ptr`. From Rust's aliasing-model perspective the writer holds an exclusive `&mut [u8]` while the reader/writer through `raw_ptr` overlaps it — undefined behaviour under stacked-borrows/tree-borrows. This is the same UB shape as F-G1-003 but at a different layer (Vec borrow vs raw deref). Stacked Borrows will reject every concurrent read-during-pwrite.
- **Impact**: Production code never mixes the paths (the engine picks one based on `as_raw_ptr().is_some()`), so the bug is latent — but `recovery.rs` uses the RwLock path while the data region is otherwise idle, and tests freely mix them. Anyone running `cargo miri` against the test suite hits the UB. Long-term, if someone adds a hot path that uses `device.pread(...)` while another thread is doing direct-ptr writes, the optimizer is free to reorder or fold loads in ways that the CRC will eventually catch but the language model permits arbitrary behaviour before the catch.
- **Recommendation**: Replace `data: RwLock<Vec<u8>>` with a `data: UnsafeCell<Vec<u8>>` and document that ALL access — including `pread`/`pwrite` — must go through `raw_ptr` (after the construction phase). Or: drop `as_raw_ptr` for `MemoryDevice` and route every read/write through the lock guard. The split contract is the bug.
- **Confidence**: Medium

---

### F-G1-005: `record_offset as usize` truncation in every `*_direct` helper — silent corruption on 32-bit targets
- **Severity**: LOW
- **Category**: Correctness
- **Location**: `src/io.rs:79`, `:115`, `:133`, `:157`, `:184`, `:262`, `:280`, `:334`, `:355`
- **Code**:
  ```rust
  let p = base_ptr.add(record_offset as usize);
  ```
- **Issue**: `record_offset: u64` is unconditionally cast to `usize`. On a 32-bit target (or wasm32) any record offset > 4 GiB silently truncates to the low 32 bits and the helper writes/reads at the wrong place. There is no debug-assert pinning `record_offset <= usize::MAX as u64`.
- **Impact**: Not reachable on x86_64/aarch64 (current targets), so this is a latent correctness bug for any future 32-bit/wasm port. A truncated offset can land back inside the data region of another transaction — silent UTXO corruption, not even a CRC mismatch (the wrong record's CRC will validate fine for that wrong record).
- **Recommendation**: Add `debug_assert!(record_offset <= usize::MAX as u64, "record_offset exceeds platform pointer width")` at the top of each direct helper, or a single helper `fn record_ptr(base: *mut u8, off: u64) -> *mut u8`. Even better: gate the entire `as_raw_ptr` API behind `#[cfg(target_pointer_width = "64")]`.
- **Confidence**: High

---

### F-G1-006: `TxMetadata::from_bytes_unchecked` is `pub` but skips CRC — easy footgun if grepped on by future code
- **Severity**: LOW
- **Category**: Maintainability
- **Location**: `src/record.rs:605-622`
- **Code**:
  ```rust
  pub fn from_bytes_unchecked(src: &[u8]) -> Self {
      debug_assert!(src.len() >= METADATA_SIZE);
      let mut meta = std::mem::MaybeUninit::<Self>::uninit();
      unsafe {
          std::ptr::copy_nonoverlapping(src.as_ptr(), meta.as_mut_ptr().cast::<u8>(), METADATA_SIZE);
          meta.assume_init()
      }
  }
  ```
- **Issue**: Function is `pub` (crate-wide visible) and signature is identical to `from_bytes` except it skips CRC. Doc says "Intended for diagnostics / recovery tooling" but nothing prevents a hot-path caller from using it (no `unsafe fn`, no feature flag, no name-prefix convention). The function name itself is the only guard.
- **Impact**: A future maintainer searching for "fast metadata read" will find this and call it from the hot path, defeating the entire CRC integrity story. Recovery code in `src/recovery.rs` already uses the checked `read_metadata` everywhere, so the diagnostic-only API has no legitimate caller inside the crate (need to verify in G4 — out of scope here).
- **Recommendation**: Either mark `pub(crate)` and add a check that confirms no production caller exists, or rename to `from_bytes_for_diagnostics_unchecked` and add a `#[deprecated(note = "internal — use from_bytes")]` so external use is gated.
- **Confidence**: Medium

---

### F-G1-007: `MemoryDevice::pwrite` / `pread` `off + buf.len()` can overflow `usize` on theoretical huge configs
- **Severity**: LOW
- **Category**: Correctness
- **Location**: `src/device.rs:392-420`
- **Code**:
  ```rust
  let off = offset as usize;
  if off + buf.len() > data.len() {
      return Err(DeviceError::OutOfBounds { ... });
  }
  ```
- **Issue**: `off + buf.len()` is plain `usize` addition. If `offset` is near `usize::MAX` and `buf.len()` is non-trivial, the sum wraps to a small number and the bounds check passes incorrectly. The same shape appears in `DirectDevice::pread`/`pwrite` at lines 666 and 712 but uses `u64` arithmetic with `self.size` (still wrap-vulnerable in principle).
- **Impact**: Not exploitable in current production — `MemoryDevice` is tests-only and bounded by available RAM. `DirectDevice` is bounded by `self.size`. But the bounds check is wrong by construction and a debug build with a synthetic huge size would not detect the off-by-wrap.
- **Recommendation**: Use `offset.checked_add(buf.len() as u64).map_or(true, |end| end > data.len() as u64)` pattern. Or take a leaf from `pwrite_all_at`: `data.len().checked_sub(off).filter(|rem| *rem >= buf.len())`.
- **Confidence**: High

---

### F-G1-008: `read_metadata` (block-I/O path) allocates a redundant 4 KiB `AlignedBuf` on every call
- **Severity**: LOW
- **Category**: Performance
- **Location**: `src/io.rs:376-392`
- **Code**:
  ```rust
  let read_size = align_up(METADATA_SIZE, align);
  let mut buf = AlignedBuf::new(read_size, align);          // 4 KiB
  // ...
  let mut read_buf = AlignedBuf::new(total_read, align);     // 4 KiB
  device.pread_exact_at(&mut read_buf, aligned_base)?;
  buf[..METADATA_SIZE].copy_from_slice(&read_buf[intra_offset..intra_offset + METADATA_SIZE]);
  Ok(TxMetadata::from_bytes(&buf[..METADATA_SIZE])?)
  ```
- **Issue**: `buf` is allocated, written to once (line 389), then read by `from_bytes` on line 391 — it serves no purpose; the function could call `TxMetadata::from_bytes(&read_buf[intra_offset..intra_offset+METADATA_SIZE])` directly. Hot-path callers (recovery.rs at line 474, 575, 611, …) pay one extra heap-allocation + copy per metadata read.
- **Impact**: Recovery and any non-direct-ptr path takes a measurable perf hit at boot (recovery scans every record). On the direct-ptr device this code is bypassed entirely, so production hot reads are unaffected.
- **Recommendation**: Delete the redundant `buf` allocation; pass `&read_buf[intra_offset..intra_offset+METADATA_SIZE]` straight into `from_bytes`.
- **Confidence**: High

---

### F-G1-009: `MAX_PERSISTED_FREE_REGIONS` not enforced on `persist()` — freelist beyond the cap silently truncates
- **Severity**: MEDIUM
- **Category**: Correctness
- **Location**: `src/allocator.rs:956-973` (vs `:39`)
- **Code**:
  ```rust
  const MAX_PERSISTED_FREE_REGIONS: usize = (DATA_REGION_OFFSET as usize - FREELIST_OFFSET) / 16;
  // ...
  pub fn persist(&self) -> Result<()> {
      let count = self.freelist.len().min(MAX_PERSISTED_FREE_REGIONS);
      // ...
      for (i, (offset, size)) in self.freelist.iter_offset_order().take(count).enumerate() { ... }
  ```
- **Issue**: When `freelist.len() > MAX_PERSISTED_FREE_REGIONS` (~65 488 entries), `persist()` silently writes only the first `MAX_PERSISTED_FREE_REGIONS` regions in offset-order and drops the rest. `recover()` then re-reads only those entries and the dropped tail regions are LOST — they look "allocated" to the recovered allocator, and a later allocate will not find them. Worse, if the redo log was truncated before the next persist captured the drop, the original `FreeRegion` redo entries are gone too. No log, no error, no metric.
- **Impact**: Highly-fragmented servers (hot e2e load with many one-shot creates+deletes) can hit this cap. The capacity is large (65 KiB / 16 = 4 096 entries on a 1 MiB header? — wait, MAX = (1 MiB - 48) / 16 ≈ 65 533, so actually ~65k entries, which is a lot, but the BTree promotion already triggers above 64 entries). Still, a pathological fragmentation pattern hits the cap silently. Recovery returns a smaller freelist than the in-memory state — data isn't lost but free space is leaked permanently.
- **Recommendation**: In `persist()`, when `self.freelist.len() > MAX_PERSISTED_FREE_REGIONS`, return an explicit error (a new `AllocatorError::FreelistOverflow { entries, max }` variant) or — better — write an overflow pointer into the header that chains to a follow-on aligned block of freelist entries. Add a regression test that fragments past the cap and asserts persist→recover round-trips exactly.
- **Confidence**: High

---

### F-G1-010: `AlignedBuf` `len == 0` returns a `dangling().as_ptr()` but `as_ptr()` callers (in `DeviceIo::submit_*`) hand that pointer to the kernel
- **Severity**: LOW
- **Category**: Correctness
- **Location**: `src/device.rs:258-276`, used at `src/device_io/io_uring_backend.rs:217-225`
- **Code**:
  ```rust
  pub fn new(len: usize, alignment: usize) -> Self {
      if len == 0 {
          return Self { ptr: std::ptr::NonNull::dangling().as_ptr(), len: 0, layout: ... };
      }
      // ...
  }
  ```
- **Issue**: A zero-length `AlignedBuf` carries a dangling-but-aligned pointer. If anyone calls `submit_read`/`submit_write` with a zero-length buf, the io_uring path checks `len > u32::MAX as usize` but does NOT check `len == 0`, and passes the dangling pointer + `len=0` to the kernel. POSIX permits a zero-length pread/pwrite (it's a no-op returning 0), so the kernel won't deref the pointer, but the contract is undocumented and a future caller might assume the buf carries a real allocation.
- **Impact**: No reachable bug today (the engine never submits zero-length I/O). Latent footgun if someone adds a length-0 fast path.
- **Recommendation**: Add `debug_assert!(buf.len() > 0)` at the top of `submit_read`/`submit_write` in both backends, or short-circuit zero-length submits with a synthesized `Completion { result: 0 }` so the kernel is never touched.
- **Confidence**: High

---

### F-G1-011: `device_io::create_device_io` ignores `queue_depth` parameter when falling back to `SyncFallback`
- **Severity**: INFO
- **Category**: Code Quality
- **Location**: `src/device_io/mod.rs:93-111`, `src/device_io/sync_fallback.rs:45-49`
- **Code**:
  ```rust
  let _ = queue_depth;
  SyncFallback::new(queue_depth).map(|backend| Box::new(backend) as Box<dyn DeviceIo>)
  ```
  ```rust
  pub fn new(_queue_depth: u32) -> Result<Self, std::io::Error> {
      Ok(Self { pending: Vec::new() })
  }
  ```
- **Issue**: `SyncFallback::new` ignores `queue_depth` entirely. Real backends (io_uring) use it to size the SQ/CQ. The sync backend could pre-size `pending: Vec::with_capacity(queue_depth as usize)` to avoid the first reallocations. Not a bug, but an inconsistency that obscures the contract: callers think they're sizing the queue when they're not.
- **Impact**: Tiny: a few extra Vec reallocations on first batch.
- **Recommendation**: `Self { pending: Vec::with_capacity(queue_depth.min(4096) as usize) }`.
- **Confidence**: High

---

### F-G1-012: Hard-coded ioctl numbers `0x80081272` (BLKGETSIZE64) and macOS `DKIOCGETBLOCKCOUNT` are not portable across libc evolutions
- **Severity**: LOW
- **Category**: Maintainability
- **Location**: `src/device.rs:583-611`
- **Code**:
  ```rust
  // BLKGETSIZE64 = 0x80081272 — returns byte count as u64.
  let rc = unsafe { libc::ioctl(fd, 0x8008_1272 as libc::c_ulong, &mut dev_size) };
  ```
- **Issue**: The Linux `BLKGETSIZE64` constant is defined in `<linux/fs.h>` but `libc` doesn't re-export it; the code hard-codes the architecture-specific value. The value `0x80081272` is correct for x86_64/aarch64 but is computed from `_IOR('B', 114, size_t)` which depends on the `_IOC_*` macros and on `sizeof(size_t) == 8`. On a 32-bit kernel header (or future arch with different `_IOC_` encoding) this value is wrong and `ioctl` returns ENOTTY.
- **Impact**: Not reachable on current targets, but a port to a 32-bit Linux variant or a kernel with re-tooled IOC macros will silently fail to query device size and return zero — every subsequent I/O fails `OutOfBounds`. A real test running on such a host would catch it; absent such a test, the failure mode is silent.
- **Recommendation**: Use the `nix` crate's `nix::ioctl_read!(blkgetsize64, 0x12, 114, u64)` macro (or `rustix::fs::ioctl_blkgetsize64`) which encodes the value via the correct platform macros. Same for the macOS pair.
- **Confidence**: Medium

---

### F-G1-013: `fault_injection::FaultMode::NoOpAt` is documented as "functionally equivalent to None"
- **Severity**: INFO
- **Category**: Code Quality
- **Location**: `src/fault_injection.rs:140-148`
- **Code**:
  ```rust
  /// Currently [`check`] does not gate any real work itself — the
  /// production side-effect (fsync / pwrite / redb commit) is still
  /// performed by the caller after [`check`] returns. `NoOpAt` is
  /// therefore functionally equivalent to `None` unless a future
  /// extension adds guarded actions.
  NoOpAt(SyncPoint),
  ```
- **Issue**: A `FaultMode` variant that does nothing is dead intent: tests cannot meaningfully arm it because no production behaviour changes. The doc admits this and reserves the variant for a future extension. The `noop_at_does_not_panic` test (line 290) does not actually verify any behavioural difference vs `None`.
- **Impact**: None today; future maintenance risk: someone arms `NoOpAt` expecting it to suppress a side-effect, gets confused.
- **Recommendation**: Either remove the variant (and re-add when the extension lands) or wire `check` to return a `bool` so callers can branch on the result and short-circuit a side-effect. Current state is API-future-proofing without enforcement.
- **Confidence**: High

---

### F-G1-014: `IoUringBackend` timestamp ring race — `record_submit_ts` is unconditionally Relaxed, but completion consumer is on a different thread
- **Severity**: LOW
- **Category**: Concurrency
- **Location**: `src/device_io/io_uring_backend.rs:115-137`
- **Code**:
  ```rust
  fn record_submit_ts(&self, user_data: u64) {
      let idx = (user_data & RING_MASK) as usize;
      self.ts_ring[idx].store(self.now_ns(), Ordering::Relaxed);
  }
  fn consume_submit_ts_from(...) -> Option<u64> {
      let stored = ts_ring[idx].swap(0, Ordering::Relaxed);
      // ...
  }
  ```
- **Issue**: The doc on the type (lines 65-70) says collisions are harmless ("the latency for the colliding CQE is measured from the more recent SQE's timestamp"). True from a measurement-tolerance perspective. But `submit_and_wait` and `submit` are `&mut self` so a single thread owns the ring; the ts_ring sees no actual concurrency. Reasonable. The Relaxed ordering is fine BECAUSE this is single-threaded by `&mut self`. The doc never says so — it implies concurrent safety where there isn't any.
- **Impact**: Future maintainer who shares an `IoUringBackend` across threads via interior mutability (say wrapping it in a `Mutex`) will be surprised by the ts_ring's lack of synchronization to other state.
- **Recommendation**: Document that the type is single-owner (matched by `&mut self` on every mutating fn) and the ts_ring is a within-thread cache. Or, if cross-thread support is intended, use AcqRel ordering.
- **Confidence**: Medium

---

### F-G1-015: `replay_free` partial-overlap rejection is silent — corrupt redo entry returns `false`, no telemetry
- **Severity**: LOW
- **Category**: Observability
- **Location**: `src/allocator.rs:875-936` (esp. lines 900-912)
- **Code**:
  ```rust
  // Reject partial overlaps. Idempotent contained frees were handled
  // above; any remaining overlap would create intersecting freelist
  // regions and allow a later allocation to hand out live space.
  if let Some((prev_off, prev_sz)) = self.freelist.prev_before(offset + 1)
      && prev_off.saturating_add(prev_sz) > offset
  {
      return false;
  }
  if let Some((next_off, _)) = self.freelist.next_from(offset)
      && next_off < end
  {
      return false;
  }
  ```
- **Issue**: A corrupt redo `FreeRegion` entry that partially overlaps an existing free region is silently dropped (`return false`). The caller (`recovery.rs`) can't tell whether `false` meant "idempotent no-op" (safe) or "corrupt entry rejected" (alarming — the redo log is broken). Same shape applies to `replay_allocate` bounds rejection at line 815-816.
- **Impact**: An operator running recovery against a partially-corrupted redo log gets a clean log line saying recovery succeeded, when in fact entries were silently discarded and the live state may differ from the intended state. No metric counts dropped entries.
- **Recommendation**: Promote partial-overlap rejection to a `tracing::error!` (and increment a counter) rather than silent return-false. Distinguish "idempotent no-op" (`return false`) from "rejected as corrupt" (return some sentinel or tracing-warn) at minimum.
- **Confidence**: High

---

### F-G1-016: `Reservation::FromFreelist` rollback re-inserts the original region but does not coalesce with newly-adjacent free regions
- **Severity**: LOW
- **Category**: Correctness
- **Location**: `src/allocator.rs:710-733`
- **Code**:
  ```rust
  Reservation::FromFreelist { alloc_offset, region_size } => {
      if region_size > aligned_size {
          let remainder_offset = alloc_offset + aligned_size;
          self.freelist.remove(remainder_offset);
      }
      self.freelist.insert(alloc_offset, region_size);
      self.freelist.maybe_promote();
  }
  ```
- **Issue**: After a redo-flush failure, the freelist is restored by re-inserting the original region. But between the `reserve_aligned` call and the rollback, another concurrent allocation could have created a free region adjacent to `[alloc_offset, alloc_offset + region_size)` (impossible in practice — the allocator is single-threaded under its own `Mutex` in the engine), and the rollback would leave two adjacent free regions that should have coalesced.
- **Impact**: The allocator is single-threaded (every mutating method takes `&mut self`), so this can't happen in practice. The concern is theoretical until a future refactor introduces interior-mutability. Hypothesis: if a redo flush call hands control back to an inner caller (it doesn't today — `RedoLog::append_and_flush` is synchronous), and that inner caller mutates the freelist, rollback would not coalesce. This is hypothesis-only; no current code path triggers it.
- **Recommendation**: After `freelist.insert(alloc_offset, region_size)`, call the same coalesce logic used in `free()` (next_from + prev_before merges). Cheap, defensive.
- **Confidence**: Low

---

### F-G1-017: `MemoryDevice::raw_len` shadows `data.read().len()` — drift opportunity if `Vec` ever resizes
- **Severity**: LOW
- **Category**: Code Quality
- **Location**: `src/device.rs:363-372`, used at `:427`
- **Code**:
  ```rust
  let mut data = vec![0u8; size as usize];
  let raw_ptr = data.as_mut_ptr();
  let raw_len = data.len();
  Self { data: parking_lot::RwLock::new(data), raw_ptr, raw_len, alignment }
  // ...
  fn size(&self) -> u64 { self.raw_len as u64 }
  ```
- **Issue**: `raw_len` is captured at construction and never updated. `size()` returns `raw_len`; `pread`/`pwrite` use `data.len()` from the lock guard. Today both are identical because the Vec is never resized — but the dual source of truth is bait for a future bug. If someone adds a `MemoryDevice::resize`, the `raw_len` and `raw_ptr` go stale.
- **Impact**: None today.
- **Recommendation**: Derive `size()` from `data.read().len()` and drop the `raw_len` field, OR make the type explicitly immutable-by-construction (which it already de facto is) and document that any future resize must update both fields atomically.
- **Confidence**: High

---

### F-G1-018: `StripedLocks::lock` clones key bytes 16..24 every call — minor hot-path overhead
- **Severity**: INFO
- **Category**: Performance
- **Location**: `src/locks.rs:36-43`
- **Code**:
  ```rust
  pub fn stripe_index(&self, key: &TxKey) -> usize {
      let mut bytes = [0u8; 8];
      bytes.copy_from_slice(&key.txid[16..24]);
      let h = u64::from_le_bytes(bytes) as usize;
      h & self.mask
  }
  ```
- **Issue**: `copy_from_slice` + `from_le_bytes` is two memcpys. `u64::from_le_bytes(key.txid[16..24].try_into().unwrap())` is exactly the same code path semantically but produces tighter codegen via the slice→array conversion. Trivial.
- **Impact**: A nanosecond on the hot path — meaningful only at 10 M ops/s.
- **Recommendation**: `let h = u64::from_le_bytes(key.txid[16..24].try_into().expect("txid is 32 bytes")) as usize;`
- **Confidence**: High

---

### F-G1-019: `record::generation_target_ahead` correctly handles wraparound but is missing a `delta == GENERATION_ORDER_WINDOW` ambiguity test
- **Severity**: INFO
- **Category**: Correctness
- **Location**: `src/record.rs:710-717`
- **Code**:
  ```rust
  pub const GENERATION_ORDER_WINDOW: u32 = 1u32 << 31;
  pub fn generation_target_ahead(local: u32, target: u32) -> bool {
      let delta = target.wrapping_sub(local);
      delta != 0 && delta < GENERATION_ORDER_WINDOW
  }
  ```
- **Issue**: When `target.wrapping_sub(local) == GENERATION_ORDER_WINDOW` (exactly 2^31 apart), `delta < WINDOW` is false → not-ahead. The doc states "the exact half-range distance is ambiguous, so it is classified as not-ahead". Consistent. Existing test asserts `!generation_target_ahead(0, 1<<31)` — good. BUT the converse (`generation_target_ahead(1<<31, 0)`) is also `false` (delta == 2^31, not < WINDOW) — neither direction wins. If both sides exist simultaneously, both replicas will treat themselves as the freshest. The 2^31-outstanding-mutations bound is documented in the constant doc but not enforced anywhere — no metric, no warning when a record's generation jumps by more than `2^30`.
- **Impact**: Only triggers on pathological replicas drifting by ~2 billion generations on a single record — implausible but not impossible if a record sees ~10⁹ updates and a partition isolates it for a long time. No active correctness bug.
- **Recommendation**: Emit a warn-level log + metric if `(target.wrapping_sub(local)).abs() > 2^30`. Add a unit test for the symmetric ambiguity.
- **Confidence**: Medium

---

## Coverage notes

- `src/device.rs` (1485) — 4 findings (F-G1-004, F-G1-007, F-G1-012, F-G1-017). Verified: BlockDevice trait, AlignedBuf alloc/dealloc, MemoryDevice ALL methods, DirectDevice open/pread/pwrite/sync, ReadFailingDevice test wrapper, ChunkyDevice test wrapper. AlignedBuf drop guards `len > 0` correctly. DirectDevice EINTR-retry loop on pread/pwrite is correct.
- `src/io.rs` (974) — 4 findings (F-G1-002, F-G1-003, F-G1-005, F-G1-008). Verified: all META_OFF_* constants pin against `offset_of!` at compile time, all `*_direct` helpers honor the documented safety contract, BC-02 stripe-lock contract is now documented away rather than enforced.
- `src/record.rs` (1451) — 3 findings (F-G1-006, F-G1-019, F-G1-005 also covers `as usize` casts here). Verified: CRC32 over header AND over each UTXO slot, compile-time size assertions, `TxFlags` repr-transparent makes raw-byte transmute safe, generation wraparound logic correct. `from_bytes`/`from_bytes_unchecked` MaybeUninit path is correct (full 320 bytes always written).
- `src/allocator.rs` (2475) — 3 findings (F-G1-009, F-G1-015, F-G1-016). Verified: hybrid Vec/BTree freelist with hysteresis is correct, header CRC32 over `[0..FREELIST_OFFSET + count*16]` with CRC slot zeroed, redo journaling rollback paths cover both freelist and high-water reservations, `is_allocated_range` overlap check is correct.
- `src/device_io/mod.rs` (111) — 1 finding (F-G1-011). Verified: trait surface is consistent, Linux/non-Linux dispatch is correct, `create_device_io` falls back with a warn log.
- `src/device_io/sync_fallback.rs` (326) — 1 finding (F-G1-001 — HIGH). Verified: pwrite/pread submission ordering, `Send`/`Sync` impls on `PendingOp` (raw pointers — caller-guaranteed lifetime).
- `src/device_io/io_uring_backend.rs` (574) — 2 findings (F-G1-010, F-G1-014). Verified: SQE push + CQE drain semantics, timestamp ring power-of-two indexing, `WouldBlock` back-pressure surface, metrics hookup. The `Box<[AtomicU64; RING_SIZE]>` construction via `Vec → Box<[T]> → *mut [T; N] → Box<[T; N]>` is sound (length is exactly RING_SIZE by construction).
- `src/locks.rs` (140) — 1 finding (F-G1-018). Verified: stripe count rounded up to power-of-two with min 16; bucket bytes 16-23 chosen to avoid collision with hashtable index (bytes 0-7) and fingerprint (bytes 8-15); RAII guard semantics.
- `src/fault_injection.rs` (310) — 1 finding (F-G1-013). Verified: thread-local `FAULT_MODE` is properly isolated, every `SyncPoint` matches a documented crash window, `check` is provably zero-cost when feature flag is off (inline empty body).
