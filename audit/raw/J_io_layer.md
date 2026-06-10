# Audit Category J — I/O Layer

Scope: `src/device.rs` (full, 1623 lines), `src/io.rs` (production code lines
1–1110 read in full; 1111–1799 are `#[cfg(test)] mod tests`). HEAD branch `main`.
Caller wiring confirmed against `src/lib.rs`, `src/storage/manager.rs`,
`src/bin/server.rs`, `src/server/http.rs`, `src/metrics.rs`.

---

## Device-construction wiring (confirmed)

`src/lib.rs:8-16` documents that the previous pluggable `device_io`
(`DeviceIo` trait + io_uring + sync-fallback backends) was **deleted on
2026-05-28** as dead scaffolding. There is now a single code path: the engine
holds an `Arc<dyn BlockDevice>`. Production opens `DirectDevice` (O_DIRECT) at
`src/bin/server.rs:280-281`
(`DirectDevice::open(device_path, config.device_size, config.device_alignment)`);
`MemoryDevice` is tests/benches only. `StorageManager` holds the
`Arc<dyn BlockDevice>` (`src/storage/manager.rs:119-120`).

---

## VERIFIED-OK checklist items

1. **Direct I/O alignment — offset + length enforced.**
   `DirectDevice::check_alignment` (device.rs:772-784) rejects any `offset` or
   `len` not a multiple of `self.alignment`, on both `pread` (device.rs:789) and
   `pwrite` (device.rs:845), before any libc call. `MemoryDevice` mirrors it
   (device.rs:454-466). The alignment value itself is validated power-of-two
   ≥ 512 at construction (`validate_alignment`, device.rs:111-115, called from
   device.rs:415 and 663). Tests cover offset, length, and the
   power-of-two/min-512 rejections (device.rs:970-1011, 1199-1262).

2. **Block-device size queried from kernel; config `size` ignored for raw
   devices.** `DirectDevice::open` detects `S_IFBLK` via `fstat`
   (device.rs:679-691) and queries the kernel (`blkgetsize64` Linux
   device.rs:709; `DKIOCGETBLOCKCOUNT * DKIOCGETBLOCKSIZE` macOS
   device.rs:724-734) into `actual_size`; `set_len` is never called on a block
   device. ioctl request numbers are computed by `nix::ioctl_read!`
   (device.rs:27-34), not hand-encoded. The config `size` is used only on the
   regular-file branch (device.rs:743-752). Coverage gap: see J-03.

3. **Regular file grown but never truncated.** `.truncate(false)`
   (device.rs:667); the regular-file branch only `set_len(size)` when
   `existing < size` (device.rs:746-748), else keeps the existing length.
   Tested against on-disk `metadata().len()` in
   `direct_device_no_truncate_existing` (device.rs:1140-1162) and
   `direct_device_grows_existing_file` (device.rs:1164-1187).

4. **Partial `pread`/`pwrite` retried until complete or clean failure.** Trait
   helpers `pread_exact_at` (device.rs:201-222) and `pwrite_all_at`
   (device.rs:241-256) loop and convert a zero-progress short return into typed
   fatal `ShortRead`/`WriteStalled` carrying byte counts. EINTR retried in the
   raw libc loops (device.rs:814-832, 868-881). Tested against a short-count
   `ChunkyDevice` including mid-buffer EOF and zero-progress-mid-buffer with
   exact `got`/`remaining` assertions (device.rs:1506-1594). All io.rs
   block-path callers use the `_exact_at` / `_all_at` helpers, never bare
   `pread`/`pwrite` (io.rs:923, 948, 955, 974, 1010, 1038, 1044, 1077).

5. **Out-of-bounds + integer-overflow on offset+len.** Both backends use
   `checked_add` for `offset + len` mapping overflow to `OutOfBounds`
   (device.rs:478-489 / 516-527; 793-806 / 847-860). Tested (device.rs:1050-1060,
   1110-1118). `read_all_utxo_slots` guards `slot_count * UTXO_SLOT_SIZE` with
   `checked_mul` (io.rs:999-1006).

6. **io_uring backend: deliberately DELETED, not a stub.** `src/lib.rs:9-16`
   confirms the io_uring + sync-fallback backends were removed 2026-05-28.
   `device.rs`/`io.rs` contain zero io_uring references. The only data path is
   synchronous `libc::pread`/`pwrite` (DirectDevice, device.rs:807-891) plus the
   raw-pointer atomic-chunked path (MemoryDevice/direct, io.rs:112-237). The
   "sync fallback" IS the implementation; nothing to fall back from. The
   synchronous path is sound (see #4/#5). Residue: vestigial `IoUringMetrics`
   surface still exported — see J-04.

---

## FINDINGS

### J-01 — MEDIUM (high) — O_DIRECT buffer-address alignment is NOT checked (only offset and length)
**Files:** `src/device.rs:772-784`, `:816-823`, `:870-872`, `:673`.

Linux `O_DIRECT` (set at device.rs:673) requires the user buffer's **memory
address** to be block-aligned in addition to offset and length.
`check_alignment` validates only offset and length. Nothing verifies
`buf.as_ptr()` before the `libc::pread`/`pwrite` calls. On a real O_DIRECT NVMe
device, a non-block-aligned buffer yields `EINVAL`, surfaced as an opaque
`DeviceError::Io`. Production is saved only by convention (all io.rs callers
allocate `AlignedBuf`: io.rs:922, 944, 973, 1009, 1036, 1062), but the methods
are `pub` and the trait docs (device.rs:138-147) never state the requirement.
CI never catches it: tests use `MemoryDevice` (no address requirement) or
regular-file/macOS `DirectDevice` (F_NOCACHE tolerates misalignment). The
failure first appears in production on the first real NVMe write.

**Fix:** add `if (buf.as_ptr() as usize) % self.alignment != 0 { return
Err(AlignmentViolation {..}) }` in `DirectDevice::pread`/`pwrite`; document the
buffer-address requirement on the trait.

### J-03 — MEDIUM (high) — Raw block-device size-query branch has zero test coverage
**Files:** `src/device.rs:695-742`, `:709`, `:724-734`, `:800`, `:854`.

Every `DirectDevice` test opens a regular file (`is_block == false`), so the
ioctl size-query branch (device.rs:695-742) is never executed. That branch
computes `actual_size`, the value every subsequent OOB bounds check depends on
(device.rs:800/854). A bug — wrong ioctl, unit confusion (bytes vs sectors),
silently-zero size — would let the allocator hand out offsets past the real end
of the device (corruption) or refuse the whole device. On a fresh Teranode
deployment against `/dev/nvme0n1` this number is trusted immediately, and a
regression would ship undetected.

**Fix:** Linux-gated integration test using a `losetup` loop device (and macOS
`hdiutil` RAM disk) asserting `DirectDevice::open(..).size()`; extract the macOS
`sectors * sector_size` math into a pure, unit-tested helper.

### J-04 — LOW (high) — Vestigial `IoUringMetrics` still exported after the backend was deleted
**Files:** `src/metrics.rs:1046-1162`, `:1505-1511`; `src/bin/server.rs:110-111,
233`; `src/server/http.rs:965-992`, `:1877-1898`; `src/lib.rs:9-16`.

The io_uring backend was removed (lib.rs:9-16) but `IoUringMetrics` is still
constructed as a static (`server.rs:110-111`), installed at startup
(`server.rs:233`), and exported as `teraslab_uring_submit_latency_ns`,
`teraslab_uring_pending`, `teraslab_uring_*_errors_total` via `/metrics`
(http.rs:965-992) and the `/admin/top` JSON (http.rs:1877-1898). No live I/O
path records into these counters, so every series is permanently zero.
Operators alerting on `teraslab_uring_*` will see flat-zero submit/completion
latency and zero submit errors and may believe the device path is healthy when
the metrics are simply disconnected — worse than absent.

**Fix:** remove `IoUringMetrics` + `init_io_uring_metrics` + the server install
+ the `/metrics`/`/admin/top` exporters (or feature-gate for a future backend).
Update CLAUDE.md, which still names a `uring` module.

### J-02 — LOW (high) — Loop helpers use unchecked `offset + done` arithmetic
**Files:** `src/device.rs:207`, `:245`.

`pread_exact_at`/`pwrite_all_at` compute the per-iteration offset with an
unchecked `+`, inconsistent with the module-wide `checked_add` hardening
(device.rs:478-489, 516-527, 793-806, 847-860). Non-exploitable: the inner
`pread`/`pwrite` `checked_add` rejects any out-of-range offset with `OutOfBounds`
before any I/O. Debug would panic on overflow; release wraps then is caught.
Hardening/consistency only.

**Fix:** `offset.checked_add(done as u64).ok_or(OutOfBounds{..})?` in both.

### J-05 — LOW (medium) — Dead `#[cfg(not(unix))]` path uses non-atomic seek+read_exact, mis-types short I/O
**Files:** `src/device.rs:834-841`, `:883-890`.

The non-unix branches do `seek + read_exact/write_all` on a shared `&File` and
return `Ok(buf.len())` unconditionally. `seek+read` is not atomic (concurrent
seek corrupts the offset) and `read_exact` returns `UnexpectedEof` instead of
the typed `ShortRead`. Dead on all supported targets (Linux + macOS are unix);
flagged so a future Windows/WASM port does not inherit it.

**Fix:** for a future non-unix target, use positional I/O (`seek_read`/
`seek_write`) and map short results to `ShortRead`/`WriteStalled`.

---

## Adjacent correctness (in scope, verified OK, not findings)
- Direct-pointer torn-read defense (F-X-007/BC-02): process-wide
  `StripedRwLocks` keyed by `record_offset` (io.rs:64-67, 760-764, 805) +
  atomic-chunked transfer (io.rs:112-237); footer→CRC window held under one
  write guard via the `FooterPendingCrc` typestate (io.rs:468-607). Sound on
  inspection; the aarch64 stress claim is taken from the in-tree test
  (not re-run here).
- `read_metadata`/`write_metadata` RMW only when the write does not cover a full
  aligned block (io.rs:947-949) and always finalizes via `pwrite_all_at`
  (io.rs:955). Single-slot RMW reads the aligned block first (io.rs:1038).
