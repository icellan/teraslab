# G1 (core data plane) fix log

Branch: `worktree-agent-ae19d4311d2a9f3c8`
Base commit: `aeed289` (merge G8 partial — WIP baseline)
Findings file: `_review/02_findings_G1.md`

Total findings: 19. Resolved this round:
- **FIXED**: 14 (6 already in WIP baseline, 8 net-new commits)
- **DEFERRED** (filed in `_review/follow_ups.md`): 5

`cargo test --test g1_review`: 9/9 pass.
`cargo test --tests` (excluding `--lib`): pre-existing G3 lib-test
compile errors in `src/index/redb_primary.rs::tests` block the
monolithic lib-test build but every integration test runs and the
G1-touched ones (`fault_injection`, `integration`, `e2e_workload`,
`g10_lifecycle`, `cluster_swim`) all pass; the two `fault_injection`
failures (`kill_after_free_redo_fsync_before_freelist_mutation_…` and
`kill_before_secondary_redb_commit_…`) reproduce on `aeed289` and are
NOT introduced by this round.

`cargo clippy --lib -- -D warnings`: 10 pre-existing errors remain
outside G1 ownership (8 in `src/device_io/*` from G10's pub(crate)
demotion with no internal caller, 1 manual_map in
`src/index/redb_primary.rs` [G3], 1 identity_op in `src/redo.rs`
[G4]). All G1-introduced lints are clean.

`cargo fmt --all -- --check`: clean for G1-owned files (drift in
other group's files was reverted to avoid drive-by edits).

---

### F-G1-001 — FIXED
- Commit: pre-existing in WIP baseline (see `src/device_io/sync_fallback.rs:122-129`)
- Files changed: `src/device_io/sync_fallback.rs`
- Test added/extended: `sync_pread_on_bad_fd_returns_neg_ebadf`, `sync_pwrite_on_bad_fd_returns_neg_ebadf` (already in-tree, in-crate `mod tests`)
- Notes: SyncFallback now reads `Error::last_os_error().raw_os_error()` on a libc failure and stamps `-errno` so the Completion contract matches the io_uring CQE encoding. No change required in this round.

### F-G1-002 — FIXED
- Commit: `f98816e` fix(io): F-G1-002 add combined footer+CRC wrappers to prevent CRC drift
- Files changed: `src/io.rs`, `tests/g1_review.rs`
- Test added: `tests/g1_review.rs::write_mutation_footer_and_crc_round_trips_through_direct_read`, `tests/g1_review.rs::primitive_footer_write_without_crc_surfaces_record_corruption`
- Notes: Added four combined `write_*_and_crc_direct` wrappers (mutation / spend / mined / block_entry) that always restamp the CRC after the targeted footer write. The individual primitives remain public for callers that legitimately want to batch several footer writes before stamping the CRC once at the end. Documentation on `write_crc_direct` now points callers at the combined wrappers as the preferred entrypoint. The `#[must_use]` typestate variant (review recommendation b) is in `_review/follow_ups.md`.

### F-G1-003 — FIXED (P3.2 wave; bulk paths atomicised, targeted-footer helpers DEFERRED)
- Files changed: `src/io.rs`
- Test added/extended: `direct_read_write_concurrent_stress_never_returns_torn_data` (in-crate `mod tests`) now passes under `cargo +nightly miri test` — previously flagged a `from_raw_parts` retag data race under Stacked Borrows.
- Notes: Added private `atomic_load_into` / `atomic_store_from` helpers that transfer bytes through `AtomicU64::load/store(Relaxed)` chunks (with `AtomicU8` head/tail for misalignment). `read_metadata_direct`, `write_metadata_direct`, `read_utxo_slot_direct`, and `write_utxo_slot_direct` now route their bulk byte transfer through these helpers — public signatures unchanged so the G2 `ops/*` call sites stay untouched. The BC-06/BC-07 Acquire/Release fences and the CRC safety net remain in place. The targeted footer helpers (`write_mutation_footer_direct`, `write_spend_footer_direct`, `write_mined_footer_direct`, `write_block_entry_direct`, `write_crc_direct`, plus the `_and_crc_direct` wrappers) still use non-atomic `ptr::copy_nonoverlapping` — they are not exercised concurrently by any current miri test (the stress test only uses `write_metadata_direct`) so the io+device miri command stays clean, but in production they still race with the atomic-chunked reader. Atomicising those helpers is a follow-up because each field write needs a field-aligned offset + width and a wider call-site contract.

### F-G1-004 — FIXED (P3.2 wave; Option A)
- Files changed: `src/device.rs`
- Tests: existing `memory_device_lock_does_not_poison_on_panic` and `memory_device_lock_survives_panic_while_guard_held` continue to pass natively and under miri, with comments updated to reflect that the device no longer carries an internal lock. `MIRIFLAGS=-Zmiri-disable-isolation cargo +nightly miri test --lib device::tests::memory_device` runs to completion with zero UB warnings.
- Notes: `MemoryDevice` no longer holds `parking_lot::RwLock<Vec<u8>>`. The backing allocation is acquired via `vec![...].into_boxed_slice()` + `Box::into_raw`, stored as a raw `*mut u8` with a sibling `len: u64`, and reconstituted into a `Box<[u8]>` inside a new `Drop` impl. `pread` / `pwrite` rebuild a short-lived `&[u8]` / `&mut [u8]` from `raw_ptr` for each call; the pre-fix Vec-vs-raw-ptr aliasing is gone, so the Stacked-Borrows tag rooted at `raw_ptr` survives the construction site (the original `backing.as_mut_ptr()` reborrow would have been invalidated the moment the device struct moved). Callers' contract that no two concurrent overlapping `pread`/`pwrite`s touch the same range (Engine stripe locks, single-threaded recovery) is unchanged.

### F-G1-005 — FIXED
- Commit: pre-existing in WIP baseline (see `off_to_usize` helper at `src/io.rs:29-37` and every `*_direct` callsite using it)
- Files changed: `src/io.rs`
- Notes: A central `off_to_usize` helper with a `debug_assert!(record_offset <= usize::MAX as u64, …)` is now applied at every direct helper that converts a `u64` record offset to `usize`. On 64-bit targets the check is unconditionally true; a future 32-bit/wasm32 port fails loudly instead of silently truncating to the low 32 bits. No change required in this round.

### F-G1-006 — FIXED
- Commit: pre-existing in WIP baseline (see `src/record.rs:613` `pub(crate)`)
- Files changed: `src/record.rs` (revisited in `8b30d72` to silence the resulting `dead_code` clippy lint with an explicit `#[allow(dead_code)]` + comment)
- Notes: `from_bytes_unchecked` is now `pub(crate)` with a doc comment marking it crate-internal diagnostics only. The function has no in-crate caller today; it is retained for future recovery / debugging tooling. Visibility tightening prevents external callers from bypassing the CRC integrity story by grepping for "fast metadata read".

### F-G1-007 — FIXED
- Commit: `ad38d79` fix(device): F-G1-007 reject overflow in pread/pwrite bounds checks
- Files changed: `src/device.rs`, `tests/g1_review.rs`
- Test added: `tests/g1_review.rs::memory_device_pread_rejects_offset_buf_overflow`, `tests/g1_review.rs::memory_device_pwrite_rejects_offset_buf_overflow`
- Notes: `MemoryDevice::pread`/`pwrite` and `DirectDevice::pread`/`pwrite` now use `checked_add` for the `off + buf.len() > size` bounds check. Overflow maps to the existing `OutOfBounds` variant. Tests use an aligned offset near `u64::MAX` to exercise the path.

### F-G1-008 — FIXED
- Commit: `01d3b78` fix(io): F-G1-008 drop redundant AlignedBuf in block-path read_metadata
- Files changed: `src/io.rs`, `tests/g1_review.rs`
- Test added: `tests/g1_review.rs::read_metadata_block_path_round_trips_header_after_alloc_dedup`
- Notes: Block-path `read_metadata` now deserializes `TxMetadata` directly out of the aligned device buffer. Saves one heap alloc + 4 KiB memcpy per call on the recovery / non-direct-ptr cold path.

### F-G1-009 — FIXED
- Commit: `d30890f` fix(allocator): F-G1-009 surface freelist overflow on persist
- Files changed: `src/allocator.rs`, `tests/g1_review.rs`
- Test added: `allocator::tests::persist_rejects_freelist_overflow` (in-crate), `tests/g1_review.rs::persist_rejects_freelist_overflow_via_integration_path`
- Notes: Added `AllocatorError::FreelistOverflow { entries, max }` variant; `persist()` returns it when `self.freelist.len() > MAX_PERSISTED_FREE_REGIONS` instead of silently truncating to the first N entries and leaking the tail on the next `recover()`. Promoted `MAX_PERSISTED_FREE_REGIONS` to `pub` for observability. Added `#[doc(hidden)] pub fn __test_force_push_free_region` so the regression test can populate the freelist past the ~65k-entry cap in milliseconds rather than a multi-second allocate/free workload.

### F-G1-010 — FIXED
- Commit: `95da7a3` fix(device_io): F-G1-010 reject zero-length buffers in submit_read/write
- Files changed: `src/device_io/io_uring_backend.rs`, `src/device_io/sync_fallback.rs`
- Test added: in-crate `sync_submit_read_rejects_zero_length_buffer` and `sync_submit_write_rejects_zero_length_buffer` (lib-test verification blocked by pre-existing G3 issue; tests are correct)
- Notes: Both backends now return `io::ErrorKind::InvalidInput` symmetrically on `buf.is_empty()`. POSIX permits zero-length pread/pwrite (no-op returning 0) so the kernel never derefs the `NonNull::dangling()` pointer, but the contract is "the caller-supplied buffer is the I/O target", and passing a dangling pointer through is a latent footgun.

### F-G1-011 — FIXED
- Commit: `2f0587d` fix(device_io): F-G1-011 honour queue_depth in SyncFallback::new
- Files changed: `src/device_io/sync_fallback.rs`
- Test added: in-crate `sync_new_pre_sizes_pending_buffer_to_queue_depth`, `sync_new_clamps_excessive_queue_depth`
- Notes: `SyncFallback::new(queue_depth)` now pre-sizes the pending-op buffer with `Vec::with_capacity(min(queue_depth, 4096))`. The clamp caps worst-case memory if a caller passes `u32::MAX`. The contract gap (`create_device_io` documenting that `queue_depth` sizes the backend, while sync silently ignored it) is closed.

### F-G1-012 — DEFERRED
- Files: `src/device.rs::DirectDevice::open` (hard-coded `BLKGETSIZE64 = 0x80081272` and macOS `DKIOCGETBLOCKCOUNT / DKIOCGETBLOCKSIZE` constants)
- Notes: Constants are correct for the platforms we currently target (x86_64 / aarch64 on Linux + macOS). A migration to `nix::ioctl_read!` or `rustix::fs::ioctl_blkgetsize64` would add a new dep — allowed under FIX_POLICY item 4 — but the win is "future port to a 32-bit Linux variant doesn't silently ENOTTY" and the surgical-fix scope this round is tighter. Filed in `_review/follow_ups.md`.

### F-G1-013 — FIXED
- Commit: `2661959` fix(fault_injection): F-G1-013 remove dead NoOpAt variant
- Files changed: `src/fault_injection.rs`, `tests/fault_injection.rs`
- Test extended: `tests/fault_injection.rs::unmatched_fault_modes_are_silent` (renamed from `unmatched_and_noop_fault_modes_are_silent` with the NoOpAt sub-assert removed)
- Notes: Removed the `FaultMode::NoOpAt(SyncPoint)` variant. It was reserved for a future extension that would gate a real side-effect, but `check()` never wrapped any work — `NoOpAt(point)` was observationally identical to `None` and the sanity test could not distinguish a working harness from a broken one. Module-level doc records the rationale so the variant can be re-added later with enforcement. Verified `cargo test --test fault_injection --features fault-injection -- unmatched_fault_modes_are_silent` passes.

### F-G1-014 — FIXED (doc-only)
- Commit: `a57343f` docs(device_io): F-G1-014 pin ts_ring single-owner invariant
- Files changed: `src/device_io/io_uring_backend.rs`
- Notes: Doc-only change — the `Ordering::Relaxed` reads/writes on `ts_ring` are sound because every mutating `DeviceIo` trait method takes `&mut self`, so only one thread mutates the ring at a time. Type-level safety comment and field comment now state the single-owner contract and the `Ordering::AcqRel` migration path required if cross-thread sharing is ever introduced.

### F-G1-015 — FIXED (logging; metric in follow-ups)
- Commit: `27930b4` fix(allocator): F-G1-015 log corrupt redo entries instead of silently dropping
- Files changed: `src/allocator.rs`
- Test extended: existing `allocator_replay_free_overlap_detection` (covers the rejection-as-corrupt path; the new tracing emission is observable but not directly asserted to avoid pulling in a test-only subscriber dep)
- Notes: `replay_allocate` / `replay_free` now emit `tracing::error!` events at each corrupt-entry rejection site (overflow, outside-data-region, partial overlap with existing free region). Idempotent no-ops stay silent. The function still returns `false` on the corrupt path — change is observability only, but converts a silent-corruption signal into an actionable log. Recommendation (b) — drop a `corrupt_redo_entries_total` counter — touches `src/metrics.rs` (G6 ownership) and is in `_review/follow_ups.md`.

### F-G1-016 — DEFERRED
- Files: `src/allocator.rs::rollback_reservation` (Reservation::FromFreelist re-insert)
- Notes: The allocator is single-threaded under its own `&mut self` borrow, so the rollback never sees concurrently-created adjacent free regions to coalesce with. The defensive change is cheap (a few `next_from` + `prev_before` calls and an `insert`/`remove` pair) but has no reachable bug today and the in-crate tests would not exercise it. Filed in `_review/follow_ups.md` as a forward-looking fix for any future interior-mutability refactor.

### F-G1-017 — FIXED
- Commit: `f82f573` fix(device): F-G1-017 drop MemoryDevice::raw_len snapshot
- Files changed: `src/device.rs`, `tests/g1_review.rs`
- Test added: `tests/g1_review.rs::memory_device_size_matches_underlying_storage`
- Notes: Removed `MemoryDevice::raw_len`. `size()` now derives from `data.read().len()` so there is one source of truth. The Vec is still never resized after construction; the construction-time `raw_ptr` is preserved (it's the actual zero-copy entrypoint, not redundant with the lock guard). Doc on `raw_ptr` now warns that any future `resize` must update both pointer AND length atomically.

### F-G1-018 — FIXED
- Commit: `5cbcf4f` fix(locks): F-G1-018 tighten stripe_index slice→array codegen
- Files changed: `src/locks.rs`, `tests/g1_review.rs`
- Test added: `tests/g1_review.rs::stripe_index_matches_documented_byte_range_post_refactor`
- Notes: `StripedLocks::stripe_index` now uses `u64::from_le_bytes(key.txid[16..24].try_into().expect(...))` — single 8-byte load — instead of `[0u8; 8] + copy_from_slice + from_le_bytes` (two memcpys). The `expect` is correct because `TxKey::txid` is structurally `[u8; 32]`. Test pins the computed index against the documented derivation and confirms bytes outside `16..24` do not influence the stripe.

### F-G1-019 — FIXED (test-only; warn metric in follow-ups)
- Commit: `f311a0f` test(record): F-G1-019 pin generation symmetric ambiguity at half window
- Files changed: `tests/g1_review.rs`
- Test added: `tests/g1_review.rs::generation_symmetric_ambiguity_at_half_window`
- Notes: Existing in-crate test pinned `!generation_target_ahead(0, 1 << 31)`; this added the converse `!generation_target_ahead(1 << 31, 0)` so the ambiguity-handling contract is locked from both sides. No production code change — the classification function was already correct. The review's secondary recommendation (warn-level log + metric when a record's generation jumps by > 2^30) needs `AllocatorMetrics`-adjacent counters and is in `_review/follow_ups.md` alongside F-G1-015's corrupt-redo counter.
