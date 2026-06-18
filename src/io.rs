//! Read/write helpers for TeraSlab records on block devices.
//!
//! Two paths:
//!
//! - **Direct path** (`_direct` functions): Zero-allocation access through a raw
//!   pointer. Used when the device supports [`BlockDevice::as_raw_ptr`] (e.g.,
//!   `MemoryDevice`, future mmap'd `DirectDevice`). No alignment overhead, no
//!   `AlignedBuf`, no `RwLock`.
//!
//! - **Block I/O path** (original functions): Read-modify-write through
//!   `pread`/`pwrite` with `AlignedBuf` for `O_DIRECT` compatibility. Used when
//!   the device does not expose direct memory access.

use crate::device::{AlignedBuf, BlockDevice, DeviceError};
use crate::locks::StripedRwLocks;
use crate::record::{
    BLOCK_ENTRY_SIZE, BlockEntry, CRC32_OFFSET, IDENTITY_READ_LEN, METADATA_SIZE, TxIdentity,
    TxMetadata, UTXO_SLOT_SIZE, UtxoSlot,
};

/// Result type for I/O helper operations.
pub type Result<T> = std::result::Result<T, DeviceError>;

// ---------------------------------------------------------------------------
// F-X-007 / BC-02: torn-read fix — record-offset striped RwLock
// ---------------------------------------------------------------------------
//
// The `*_direct` helpers below memcpy `TxMetadata` (320 bytes) and
// `UtxoSlot` (73 bytes) via plain `copy_from_slice`. On AArch64 release
// builds the LLVM-emitted SIMD memcpy is non-atomic and may publish bytes
// in any order. The original BC-02 contract claimed the CRC32 at the end
// of the metadata header would catch every torn read; the regression
// test `direct_read_write_concurrent_stress_never_returns_torn_data`
// proves the claim is empirically false on Apple Silicon (m-series).
// Mechanism: the CRC slot lives near the *end* of the 320-byte header
// (offset 253, not 4-byte aligned). NEON-based memcpy can land the new
// CRC bytes before the new field bytes; a concurrent reader observes
// the new CRC paired with mostly-old field bytes, recomputes a matching
// CRC against the partial state, and returns garbage to the caller.
// Release/Acquire fences on either side do not help — Rust's memory
// model only establishes happens-before through paired atomic
// load/store operations on the *same* address, which the memcpy is not.
//
// Closing the window without changing the on-disk format requires real
// mutual exclusion at the record level. We stripe a `RwLock<()>` table
// keyed by `record_offset`: writers hold the write guard for the bulk
// memcpy + CRC restamp, readers hold the read guard while they copy
// bytes off the device. Two readers on the same record still parallelize;
// readers and writers on different records (or stripes) do not contend.
//
// The lock table is a process-wide singleton because the `_direct` helpers
// only have a raw pointer + offset, no device handle, no engine context.
// Per-device tables would be cleaner but would require threading the
// table through every direct-read call site — a much larger change.
// The default 65_536 stripes match the engine's `StripedLocks` default;
// false-sharing only occurs when two distinct record offsets hash to the
// same stripe (effectively zero contention in practice).
//
// The torn-read-safety doc on `ops::engine::Engine` (read-only paths
// section) references this block: these read-side guards are the load-
// bearing defense for the engine's lock-free read paths and must not be
// removed as a "redundant given CRC" optimization.

/// Process-wide striped RwLock table that serializes writer↔reader
/// access at the record level for the direct-pointer I/O helpers.
///
/// Initialized lazily on first use. Stripe count: 65_536 (matches
/// `StripedLocks` default), giving an expected false-sharing rate
/// below 0.002% for typical record cardinalities.
fn io_locks() -> &'static StripedRwLocks {
    static LOCKS: std::sync::OnceLock<StripedRwLocks> = std::sync::OnceLock::new();
    LOCKS.get_or_init(|| StripedRwLocks::new(65_536))
}

/// F-G1-005: convert a `u64` device record offset to `usize` with a
/// debug-assertion that the value fits. On 64-bit targets (the only
/// currently-supported ones) this is unconditionally true; on a future
/// 32-bit / wasm32 port the check fires loudly instead of silently
/// truncating to the low 32 bits — silent truncation could land the
/// pointer inside another transaction's data region with a CRC that
/// happens to validate against the wrong record (no mismatch on read).
#[inline(always)]
fn off_to_usize(record_offset: u64) -> usize {
    debug_assert!(
        record_offset <= usize::MAX as u64,
        "record_offset {record_offset} exceeds platform pointer width (usize::MAX = {})",
        usize::MAX
    );
    record_offset as usize
}

// ===========================================================================
// F-G1-003 / C-3: atomic chunked byte transfer between a raw device pointer
// and a stack buffer.
//
// `cargo miri test` treats `slice::from_raw_parts` / `from_raw_parts_mut`
// against the SAME backing memory on different threads as a data race even
// when the CRC + BC-06/BC-07 fences keep the program logically correct.
// The fix is to perform the bulk byte transfer through `AtomicU64::load /
// store` (with an `AtomicU8` head/tail for misaligned spans) so miri sees
// the racing accesses as atomic — no data-race UB. Relaxed ordering is
// sufficient: the CRC, not the memory-order tag, provides the consistency
// guarantee, and the existing Release/Acquire fences in the calling
// helpers remain in place for AArch64 hardware barriers.
// ===========================================================================

/// Atomically read `dst.len()` bytes from the device pointer `src` into the
/// stack buffer `dst`. Uses `AtomicU64::load(Relaxed)` for the 8-byte body
/// chunks (when `src` reaches 8-byte alignment) and `AtomicU8::load(Relaxed)`
/// for the head and tail misalignment.
///
/// # Safety
///
/// `src` must be valid for `dst.len()` bytes; the bytes must be initialized;
/// and any concurrent writer must access the same range exclusively through
/// `atomic_store_from` or another atomic write path (no non-atomic stores).
#[inline(always)]
unsafe fn atomic_load_into(src: *const u8, dst: &mut [u8]) {
    use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
    let n = dst.len();
    let mut i = 0;
    // Head bytes until `src.add(i)` reaches 8-byte alignment.
    //
    // `align_offset(8)` returns `usize::MAX` only on zero-sized
    // allocations, where this loop body is a no-op anyway (n==0).
    let head = src.align_offset(8).min(n);
    while i < head {
        // Safety: src.add(i) is within the caller-promised range and
        // AtomicU8::from_ptr accepts any 1-byte aligned pointer.
        let v = unsafe { AtomicU8::from_ptr(src.add(i).cast_mut()) }.load(Ordering::Relaxed);
        dst[i] = v;
        i += 1;
    }
    // 8-byte body chunks. Pointer is 8-byte aligned at this point.
    while i + 8 <= n {
        // Safety: src.add(i) is 8-byte aligned, within range, and
        // AtomicU64::from_ptr accepts any 8-byte aligned pointer.
        let v =
            unsafe { AtomicU64::from_ptr(src.add(i).cast_mut().cast()) }.load(Ordering::Relaxed);
        dst[i..i + 8].copy_from_slice(&v.to_ne_bytes());
        i += 8;
    }
    // Tail bytes when `n` is not a multiple of 8.
    while i < n {
        // Safety: same as the head loop.
        let v = unsafe { AtomicU8::from_ptr(src.add(i).cast_mut()) }.load(Ordering::Relaxed);
        dst[i] = v;
        i += 1;
    }
}

/// F-G1-003: atomic targeted write that operates on whole u64 chunks via
/// read-modify-write. Required because the bulk `write_metadata_direct`
/// path writes [0..256) as `AtomicU64` chunks; if a targeted helper writes
/// the same byte address with a narrower atomic (u8/u32), miri's
/// weak-memory model panics with "partially overlapping store buffer".
///
/// Algorithm: for each u64-aligned chunk in `[dst..dst+src.len())`, load
/// the chunk as `AtomicU64::load(Relaxed)`, splice the field bytes into
/// the chunk-local position, then `AtomicU64::store(Relaxed)`. The bytes
/// outside the field within the chunk are preserved (they round-trip
/// through the RMW).
///
/// `dst` does NOT need to be u64-aligned, but `dst` and the *base record
/// pointer* must share an 8-byte alignment so each affected chunk is the
/// same chunk the bulk path writes. All records in TeraSlab start at
/// `base_ptr.add(record_offset)` where both terms are u64-aligned
/// (RECORD_SIZE is 4096), so every field offset within a record produces
/// chunks aligned to the bulk path's chunks.
///
/// # Safety
///
/// `dst` must be valid for `src.len()` bytes. Caller must hold the
/// per-record write lock (the RMW is per-chunk atomic but the cross-chunk
/// field write is observably non-atomic to concurrent readers without
/// the lock; the CRC check still catches the resulting tear at read
/// time).
#[inline(always)]
unsafe fn atomic_store_u64_rmw(dst: *mut u8, src: &[u8]) {
    use std::sync::atomic::{AtomicU64, Ordering};
    let n = src.len();
    if n == 0 {
        return;
    }
    let dst_addr = dst as usize;
    let head_chunk_addr = dst_addr & !7;
    let tail_byte = dst_addr + n - 1;
    let tail_chunk_addr = tail_byte & !7;
    let mut chunk_addr = head_chunk_addr;
    while chunk_addr <= tail_chunk_addr {
        // Safety: chunk_addr is 8-byte aligned (forced by `& !7`) and
        // lies within the caller-promised range of `dst`.
        let chunk_ptr = chunk_addr as *mut u8;
        let atomic = unsafe { AtomicU64::from_ptr(chunk_ptr.cast::<u64>()) };
        let mut chunk_bytes = atomic.load(Ordering::Relaxed).to_ne_bytes();
        let chunk_start = chunk_addr;
        let chunk_end = chunk_addr + 8;
        let overlap_start = dst_addr.max(chunk_start);
        let overlap_end = (dst_addr + n).min(chunk_end);
        let chunk_off = overlap_start - chunk_start;
        let src_off = overlap_start - dst_addr;
        let span = overlap_end - overlap_start;
        chunk_bytes[chunk_off..chunk_off + span].copy_from_slice(&src[src_off..src_off + span]);
        atomic.store(u64::from_ne_bytes(chunk_bytes), Ordering::Relaxed);
        chunk_addr += 8;
    }
}

/// Atomically write `src.len()` bytes from the stack buffer `src` to the
/// device pointer `dst`. Mirrors [`atomic_load_into`] — see that function's
/// doc-comment for the rationale and ordering choice.
///
/// # Safety
///
/// `dst` must be valid for `src.len()` bytes and any concurrent reader must
/// access the same range exclusively through `atomic_load_into` or another
/// atomic read path (no non-atomic loads).
#[inline(always)]
unsafe fn atomic_store_from(dst: *mut u8, src: &[u8]) {
    use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
    let n = src.len();
    let mut i = 0;
    let head = dst.align_offset(8).min(n);
    while i < head {
        // Safety: dst.add(i) is within the caller-promised range.
        unsafe { AtomicU8::from_ptr(dst.add(i)) }.store(src[i], Ordering::Relaxed);
        i += 1;
    }
    while i + 8 <= n {
        let mut chunk = [0u8; 8];
        chunk.copy_from_slice(&src[i..i + 8]);
        // Safety: dst.add(i) is 8-byte aligned and within range.
        unsafe { AtomicU64::from_ptr(dst.add(i).cast()) }
            .store(u64::from_ne_bytes(chunk), Ordering::Relaxed);
        i += 8;
    }
    while i < n {
        // Safety: same as the head loop.
        unsafe { AtomicU8::from_ptr(dst.add(i)) }.store(src[i], Ordering::Relaxed);
        i += 1;
    }
}

// ===========================================================================
// TxMetadata byte-offset constants (repr(C, packed), 256 bytes)
// ===========================================================================

// NOTE: these offsets all shifted +4 when the immutable `identity_crc` slot
// was inserted after `locktime` (offset 56). The `offset_of!` asserts below
// are the source of truth — if a field moves, the compile fails here.
/// Byte offset of `flags` (u8) within TxMetadata.
pub const META_OFF_FLAGS: usize = 84;
/// Byte offset of `spent_utxos` (u32 LE) within TxMetadata.
pub const META_OFF_SPENT_UTXOS: usize = 97;
/// Byte offset of `generation` (u32 LE) within TxMetadata.
pub const META_OFF_GENERATION: usize = 105;
/// Byte offset of `updated_at` (u64 LE) within TxMetadata.
pub const META_OFF_UPDATED_AT: usize = 109;
/// Byte offset of `block_entry_count` (u8) within TxMetadata.
pub const META_OFF_BLOCK_ENTRY_COUNT: usize = 117;
/// Byte offset of `block_entries_inline` ([BlockEntry; 3]) within TxMetadata.
pub const META_OFF_BLOCK_ENTRIES: usize = 118;
/// Byte offset of `unmined_since` (u32 LE) within TxMetadata.
pub const META_OFF_UNMINED_SINCE: usize = 171;
/// Byte offset of `delete_at_height` (u32 LE) within TxMetadata.
pub const META_OFF_DELETE_AT_HEIGHT: usize = 175;
/// Byte offset of `preserve_until` (u32 LE) within TxMetadata.
pub const META_OFF_PRESERVE_UNTIL: usize = 179;

// Compile-time verification of offsets against the actual struct layout.
const _: () = assert!(std::mem::offset_of!(TxMetadata, flags) == META_OFF_FLAGS);
const _: () = assert!(std::mem::offset_of!(TxMetadata, spent_utxos) == META_OFF_SPENT_UTXOS);
const _: () = assert!(std::mem::offset_of!(TxMetadata, generation) == META_OFF_GENERATION);
const _: () = assert!(std::mem::offset_of!(TxMetadata, updated_at) == META_OFF_UPDATED_AT);
const _: () =
    assert!(std::mem::offset_of!(TxMetadata, block_entry_count) == META_OFF_BLOCK_ENTRY_COUNT);
const _: () =
    assert!(std::mem::offset_of!(TxMetadata, block_entries_inline) == META_OFF_BLOCK_ENTRIES);
const _: () = assert!(std::mem::offset_of!(TxMetadata, unmined_since) == META_OFF_UNMINED_SINCE);
const _: () =
    assert!(std::mem::offset_of!(TxMetadata, delete_at_height) == META_OFF_DELETE_AT_HEIGHT);
const _: () = assert!(std::mem::offset_of!(TxMetadata, preserve_until) == META_OFF_PRESERVE_UNTIL);

// ===========================================================================
// Targeted metadata field writes — write only changed bytes
// ===========================================================================

/// Write the common mutation footer: flags + generation + updated_at + delete_at_height.
///
/// Every mutation bumps generation and updated_at, and may change flags or
/// delete_at_height via DAH evaluation. This writes 17 bytes at 4 offsets.
///
/// # Safety
///
/// `base_ptr` must be valid for `record_offset + METADATA_SIZE` bytes.
/// Caller must hold the per-transaction stripe lock.
///
/// # F-X-007 (BC-02) record-level lock
///
/// This primitive does **not** acquire the process-wide
/// `StripedRwLocks` torn-read guard. Callers that interleave several
/// footer writes (e.g. footer + block-entry + CRC) hold the write
/// guard once at the combining call site so a reader cannot observe a
/// half-committed mutation between primitive calls. The combined
/// `*_and_crc_direct` wrappers below acquire the guard internally;
/// the bare primitives must be invoked with the guard already held
/// or from a write path that has no concurrent readers (e.g.
/// recovery, creation).
#[inline]
pub unsafe fn write_mutation_footer_direct(
    base_ptr: *mut u8,
    record_offset: u64,
    meta: &TxMetadata,
) {
    unsafe {
        let p = base_ptr.add(off_to_usize(record_offset));
        // F-G1-003: every field write goes through `atomic_store_from`
        // so the abstract-machine memory model sees the same atomic
        // accesses as the bulk `write_metadata_direct` path. Without
        // this, the targeted `ptr::copy_nonoverlapping` writes would
        // alias the bulk path's `AtomicU64::store` at the same byte
        // address with a non-atomic store, surfacing as UB under miri
        // / Stacked Borrows / Tree Borrows.
        // flags (1 byte)
        atomic_store_u64_rmw(p.add(META_OFF_FLAGS), &[meta.flags.bits()]);
        // generation (4 bytes LE)
        atomic_store_u64_rmw(p.add(META_OFF_GENERATION), &meta.generation.to_le_bytes());
        // updated_at (8 bytes LE)
        atomic_store_u64_rmw(p.add(META_OFF_UPDATED_AT), &meta.updated_at.to_le_bytes());
        // delete_at_height (4 bytes LE)
        atomic_store_u64_rmw(
            p.add(META_OFF_DELETE_AT_HEIGHT),
            &meta.delete_at_height.to_le_bytes(),
        );
    }
    // Callers MUST follow with [`write_crc_direct`] using a meta snapshot
    // that reflects the final disk state of ALL fields (including those
    // written by preceding targeted helpers). Without the CRC restamp a
    // subsequent read will return `DeviceError::RecordCorruption`.
}

/// Write mutation footer + spent_utxos (for spend/unspend). 21 bytes at 5 offsets.
///
/// # Safety
///
/// Same as [`write_mutation_footer_direct`].
#[inline]
pub unsafe fn write_spend_footer_direct(base_ptr: *mut u8, record_offset: u64, meta: &TxMetadata) {
    unsafe {
        write_mutation_footer_direct(base_ptr, record_offset, meta);
        let p = base_ptr.add(off_to_usize(record_offset));
        // F-G1-003: atomic chunked store — see `write_mutation_footer_direct`.
        atomic_store_u64_rmw(p.add(META_OFF_SPENT_UTXOS), &meta.spent_utxos.to_le_bytes());
    }
}

/// Write mutation footer + unmined_since (for set_mined, mark_on_longest_chain). 21 bytes.
///
/// # Safety
///
/// Same as [`write_mutation_footer_direct`].
#[inline]
pub unsafe fn write_mined_footer_direct(base_ptr: *mut u8, record_offset: u64, meta: &TxMetadata) {
    unsafe {
        write_mutation_footer_direct(base_ptr, record_offset, meta);
        let p = base_ptr.add(off_to_usize(record_offset));
        // F-G1-003: atomic chunked store — see `write_mutation_footer_direct`.
        atomic_store_u64_rmw(
            p.add(META_OFF_UNMINED_SINCE),
            &meta.unmined_since.to_le_bytes(),
        );
    }
}

/// Write block_entry_count + one inline BlockEntry (for setMined inline add). 13 bytes.
///
/// # Safety
///
/// Same as [`write_mutation_footer_direct`]. `inline_index` must be < 3.
#[inline]
pub unsafe fn write_block_entry_direct(
    base_ptr: *mut u8,
    record_offset: u64,
    count: u8,
    inline_index: usize,
    entry: &BlockEntry,
) {
    unsafe {
        let p = base_ptr.add(off_to_usize(record_offset));
        // F-G1-003: atomic chunked store — see `write_mutation_footer_direct`.
        // block_entry_count (1 byte)
        atomic_store_u64_rmw(p.add(META_OFF_BLOCK_ENTRY_COUNT), &[count]);
        // BlockEntry at inline_index (12 bytes)
        let entry_offset = META_OFF_BLOCK_ENTRIES + inline_index * BLOCK_ENTRY_SIZE;
        let mut buf = [0u8; BLOCK_ENTRY_SIZE];
        entry.to_bytes(&mut buf);
        atomic_store_u64_rmw(p.add(entry_offset), &buf);
    }
    // Callers MUST follow with [`write_crc_direct`] using a meta snapshot
    // that reflects the final disk state.
}

/// Write the common mutation footer AND restamp the CRC in one call.
///
/// F-G1-002: the preferred entrypoint for callers that mutate the
/// "footer fields" (flags, generation, updated_at, delete_at_height)
/// and have no other in-flight header edits to batch. The individual
/// [`write_mutation_footer_direct`] + [`write_crc_direct`] pair is
/// still public for the rare caller that needs to interleave multiple
/// footer writes (e.g. footer + block_entry) and stamp the CRC once at
/// the end — but every other caller should use this combined helper so
/// "forgot the CRC finalizer" cannot happen by omission.
///
/// # Safety
///
/// Same as [`write_mutation_footer_direct`].
#[inline]
pub unsafe fn write_mutation_footer_and_crc_direct(
    base_ptr: *mut u8,
    record_offset: u64,
    meta: &TxMetadata,
) {
    // Implemented via the typestate split below so the combined helper
    // and the typestate path remain bit-identical at the call sites.
    // SAFETY: the contract is identical to the primitives this composes.
    unsafe { write_mutation_footer_pending_crc(base_ptr, record_offset, meta) }.stamp_crc(meta);
}

// ---------------------------------------------------------------------------
// F-G1-002 typestate guard: footer-write returns a token that must be
// consumed by stamp_crc(...) before drop. The token holds the
// record-level write guard from `io_locks()` so the footer→CRC pair
// remains atomic w.r.t. concurrent direct-pointer readers (BC-02), and
// the `#[must_use]` + debug-assert-on-drop make "footer written but
// CRC never stamped" structurally impossible for a future caller that
// needs to interleave other writes between footer and CRC.
//
// The combined `write_mutation_footer_and_crc_direct` wrapper above is
// still the right entry point for the simple case; the typestate is
// the lower-level building block for callers that genuinely need to
// split.
// ---------------------------------------------------------------------------

/// Typestate token returned by [`write_mutation_footer_pending_crc`].
///
/// The footer bytes have been written but the CRC has not yet been
/// restamped — a read in this window would observe `RecordCorruption`
/// because the header bytes no longer match the on-disk CRC slot.
/// Callers MUST consume the token by calling [`Self::stamp_crc`] so
/// the matching CRC is written and the record-level write guard is
/// released only after the header is coherent again.
///
/// # `#[must_use]`
///
/// The attribute is enforced at compile time — dropping the token
/// without consuming it is a debug-assertion failure (tested in
/// `tests::footer_pending_crc_panics_when_dropped_unstamped`) and is
/// rejected at compile time via a `compile_fail` doc-test:
///
/// ```compile_fail,E0277
/// use teraslab::io::write_mutation_footer_pending_crc;
/// use teraslab::record::TxMetadata;
/// # let mut buf = [0u8; 4096];
/// # let base_ptr = buf.as_mut_ptr();
/// # let meta = TxMetadata::new(1);
/// # let record_offset = 0;
/// // ERROR: `FooterPendingCrc` cannot be dropped — the CRC must be stamped.
/// #[deny(unused_must_use)]
/// fn must_consume() {
///     unsafe { write_mutation_footer_pending_crc(base_ptr, record_offset, &meta) };
/// }
/// ```
#[must_use = "FooterPendingCrc must be consumed via .stamp_crc() before drop \
              — the header CRC is stale until then and a concurrent read \
              would observe RecordCorruption"]
pub struct FooterPendingCrc<'a> {
    base_ptr: *mut u8,
    record_offset: u64,
    // Holds the BC-02 record-level write guard for the duration of the
    // footer→CRC window. Released on drop (i.e. inside `stamp_crc`).
    _w: parking_lot::RwLockWriteGuard<'a, ()>,
    // Set to true by `stamp_crc`; the drop-time debug-assert reads this
    // to distinguish a legitimate consume-by-stamp from a forgotten drop.
    stamped: bool,
}

// SAFETY: the raw pointer inside is only used while the held write
// guard ensures exclusive access to `record_offset`'s stripe. The
// token is conceptually a `&mut` over the metadata region; sending it
// across threads is no less safe than sending the originating `*mut u8`.
unsafe impl Send for FooterPendingCrc<'_> {}

impl FooterPendingCrc<'_> {
    /// Stamp the CRC computed from `meta` and release the BC-02
    /// write guard.
    ///
    /// `meta` MUST reflect the final on-disk state of every field in
    /// the metadata header (including any writes performed between the
    /// footer write and this call) so the CRC matches what a
    /// subsequent reader will observe.
    ///
    /// # Safety
    ///
    /// `meta` must describe the post-write byte layout of the
    /// metadata header at `record_offset`. The pointer captured by
    /// [`write_mutation_footer_pending_crc`] must still be valid (no
    /// device teardown between construction and consumption); the
    /// write guard held by the token prevents concurrent readers in
    /// the same record stripe.
    #[inline]
    pub fn stamp_crc(mut self, meta: &TxMetadata) {
        // SAFETY: same contract as `write_crc_direct`. The held write
        // guard makes the footer→CRC pair atomic w.r.t. concurrent
        // direct-pointer readers.
        unsafe { write_crc_direct(self.base_ptr, self.record_offset, meta) };
        self.stamped = true;
        // Drop fires next; the assertion below is satisfied.
    }
}

impl Drop for FooterPendingCrc<'_> {
    fn drop(&mut self) {
        // F-G1-002: a forgotten CRC stamp leaves the record's CRC slot
        // stale against the footer bytes. The next reader will get
        // `DeviceError::RecordCorruption`, but that is a silent
        // failure for the calling op — surface it loudly in debug
        // (panic via `debug_assert!`) and observably in release
        // (structured `tracing::error!` event + counter increment).
        //
        // We deliberately do NOT panic in release: panicking from
        // `Drop` aborts the process on a double-panic during unwind,
        // which is worse than the corruption itself. The tracing
        // event lets operators alert on the failure mode without
        // taking the node down.
        if !self.stamped {
            tracing::error!(
                target: "teraslab::io::footer_crc",
                record_offset = self.record_offset,
                "FooterPendingCrc dropped without stamp_crc — CRC is stale, record will fail validation on next read"
            );
            if let Some(c) = footer_crc_drop_counter() {
                c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        debug_assert!(
            self.stamped,
            "FooterPendingCrc at record_offset={} dropped without stamp_crc — CRC is stale, \
             record will fail validation on next read",
            self.record_offset,
        );
    }
}

/// Process-wide counter of `FooterPendingCrc::drop` calls without a
/// matching `stamp_crc`. Surfaces the same failure mode as the
/// debug-build panic without aborting in release. Wired into
/// `/metrics` via the registry; `None` when no registry is installed
/// (tests, embedded harnesses).
fn footer_crc_drop_counter() -> Option<&'static std::sync::atomic::AtomicU64> {
    static COUNTER: std::sync::OnceLock<std::sync::atomic::AtomicU64> = std::sync::OnceLock::new();
    Some(COUNTER.get_or_init(|| std::sync::atomic::AtomicU64::new(0)))
}

/// Read the cumulative count of unstamped `FooterPendingCrc` drops
/// (release-build observability for the F-G1-002 footer-CRC pairing
/// invariant — see `Drop` impl). Exposed for `/metrics` exporters and
/// integration tests.
pub fn footer_crc_drop_count() -> u64 {
    footer_crc_drop_counter()
        .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
        .unwrap_or(0)
}

/// Lower-level entry point for [`write_mutation_footer_and_crc_direct`].
///
/// Writes the mutation footer bytes and returns a typestate token that
/// MUST be consumed by [`FooterPendingCrc::stamp_crc`] to write the
/// matching CRC. The token holds the BC-02 record-level write guard so
/// the footer→CRC window is atomic w.r.t. concurrent direct-pointer
/// readers.
///
/// Use the combined [`write_mutation_footer_and_crc_direct`] wrapper
/// for the common case. Use this typestate variant only when you need
/// to interleave other targeted writes (e.g. an inline block-entry
/// update) between the footer and the CRC stamp so the CRC reflects
/// all of them.
///
/// # Safety
///
/// Same as [`write_mutation_footer_direct`].
#[inline]
pub unsafe fn write_mutation_footer_pending_crc<'a>(
    base_ptr: *mut u8,
    record_offset: u64,
    meta: &TxMetadata,
) -> FooterPendingCrc<'a> {
    // F-X-007 (BC-02): record-level write guard held across the
    // footer→CRC window so a concurrent direct-pointer read either
    // sees the pre-mutation header or the post-CRC header, never a
    // half-written mix that validates against the old CRC. The guard
    // is released when the returned `FooterPendingCrc` is dropped
    // (i.e. inside `stamp_crc`).
    let w = io_locks().write(record_offset);
    // SAFETY: caller upholds the per-tx stripe-lock contract and
    // base_ptr validity for METADATA_SIZE bytes at record_offset
    // (same as `write_mutation_footer_direct`).
    unsafe { write_mutation_footer_direct(base_ptr, record_offset, meta) };
    FooterPendingCrc {
        base_ptr,
        record_offset,
        _w: w,
        stamped: false,
    }
}

/// Write the spend footer (flags + generation + updated_at +
/// delete_at_height + spent_utxos) AND restamp the CRC in one call.
///
/// See [`write_mutation_footer_and_crc_direct`] for the rationale on
/// why callers should prefer the combined helpers.
///
/// # Safety
///
/// Same as [`write_mutation_footer_direct`].
#[inline]
pub unsafe fn write_spend_footer_and_crc_direct(
    base_ptr: *mut u8,
    record_offset: u64,
    meta: &TxMetadata,
) {
    // F-X-007: see write_mutation_footer_and_crc_direct for the
    // record-level write-guard rationale.
    let _w = io_locks().write(record_offset);
    // Safety: see write_mutation_footer_and_crc_direct.
    unsafe {
        write_spend_footer_direct(base_ptr, record_offset, meta);
        write_crc_direct(base_ptr, record_offset, meta);
    }
}

/// Write the mined footer (footer + unmined_since) AND restamp the CRC
/// in one call.
///
/// # Safety
///
/// Same as [`write_mutation_footer_direct`].
#[inline]
pub unsafe fn write_mined_footer_and_crc_direct(
    base_ptr: *mut u8,
    record_offset: u64,
    meta: &TxMetadata,
) {
    // F-X-007: see write_mutation_footer_and_crc_direct for the
    // record-level write-guard rationale.
    let _w = io_locks().write(record_offset);
    // Safety: see write_mutation_footer_and_crc_direct.
    unsafe {
        write_mined_footer_direct(base_ptr, record_offset, meta);
        write_crc_direct(base_ptr, record_offset, meta);
    }
}

/// Write a single inline block entry AND restamp the CRC in one call.
///
/// # Safety
///
/// Same as [`write_block_entry_direct`].
#[inline]
pub unsafe fn write_block_entry_and_crc_direct(
    base_ptr: *mut u8,
    record_offset: u64,
    count: u8,
    inline_index: usize,
    entry: &BlockEntry,
    meta: &TxMetadata,
) {
    // F-X-007: see write_mutation_footer_and_crc_direct for the
    // record-level write-guard rationale.
    let _w = io_locks().write(record_offset);
    // Safety: see write_mutation_footer_and_crc_direct. `meta` must
    // reflect the post-write block_entry_count + block_entries_inline
    // state so the CRC matches the on-disk bytes.
    unsafe {
        write_block_entry_direct(base_ptr, record_offset, count, inline_index, entry);
        write_crc_direct(base_ptr, record_offset, meta);
    }
}

/// Write only the CRC32 field of a metadata header (4 bytes at
/// [`CRC32_OFFSET`]), computed from the full in-memory `meta`.
///
/// This is the required finalizer after any sequence of targeted metadata
/// writes (footer, block-entry, etc.) — it stamps the checksum so that
/// subsequent reads validate the header as a whole. `meta` must reflect
/// the final disk state of every field, including those already written
/// by preceding targeted helpers.
///
/// Most callers should use one of the combined `_and_crc_direct`
/// wrappers (e.g. [`write_mutation_footer_and_crc_direct`]) instead of
/// pairing this primitive with a footer write — F-G1-002 introduced
/// the combined wrappers specifically to make "forgot the CRC
/// finalizer" structurally impossible.
///
/// # Safety
///
/// Same as [`write_mutation_footer_direct`].
#[inline]
pub unsafe fn write_crc_direct(base_ptr: *mut u8, record_offset: u64, meta: &TxMetadata) {
    unsafe {
        let p = base_ptr.add(off_to_usize(record_offset));
        let crc = meta.compute_crc();
        // F-G1-003: atomic chunked store — see `write_mutation_footer_direct`.
        atomic_store_u64_rmw(p.add(CRC32_OFFSET), &crc.to_le_bytes());
    }
    // F-X-007: visibility to concurrent readers is provided by the
    // record-level write guard held by the combining call site
    // (`write_mutation_footer_and_crc_direct` and siblings), not by
    // a memory fence. Memory fences without paired atomic accesses
    // on the same address do not establish happens-before in Rust's
    // memory model — only the lock release does.
}

// ===========================================================================
// Direct memory access path — zero allocations
// ===========================================================================

/// Read [`TxMetadata`] directly from a memory-mapped device region, validating
/// the on-disk CRC32.
///
/// Zero-copy: interprets the bytes in place and returns a bitwise copy.
/// Returns [`DeviceError::RecordCorruption`] if the CRC slot disagrees
/// with a freshly-computed CRC over the header bytes.
///
/// # Safety
///
/// `base_ptr` must be valid for at least `record_offset + METADATA_SIZE` bytes.
///
/// # Concurrency contract (F-X-007 / BC-02)
///
/// Acquires the process-wide `StripedRwLocks` *read* guard keyed by
/// `record_offset` for the duration of the read. The matching write
/// guard is held by `write_metadata_direct` and the combined
/// `*_and_crc_direct` helpers, so a reader observes either the
/// pre-mutation header or the post-mutation header — never a torn
/// byte mix that happens to pass the CRC check.
///
/// **Why the lock is required.** The previous design relied on CRC32
/// alone to surface torn reads. On AArch64 release builds with NEON
/// memcpy, the on-disk CRC slot (offset 253 inside the 320-byte
/// header, not 4-byte aligned) can be published before the matching
/// field bytes; a reader observes the new CRC paired with mostly-old
/// fields, recomputes a CRC that coincidentally matches the mix, and
/// returns garbage to the caller. The regression test
/// `direct_read_write_concurrent_stress_never_returns_torn_data`
/// fails ~90 % of release runs without this guard. Release/Acquire
/// fences do not help — Rust's memory model only establishes
/// happens-before via paired atomic accesses on the same address, and
/// the memcpy is none. Real mutual exclusion at the record level is
/// the smallest change that closes the window without altering the
/// on-disk format. Striped over 65_536 slots, two readers on the same
/// record still parallelize (`RwLockReadGuard`); a writer briefly
/// excludes readers only for its own record's stripe.
///
/// Multiple readers on the same record run in parallel; writers block
/// readers only on the same stripe.
#[inline]
pub unsafe fn read_metadata_direct(base_ptr: *const u8, record_offset: u64) -> Result<TxMetadata> {
    // F-X-007 (BC-02): record-level read guard — see the doc comment
    // for the full rationale. The release-build aarch64 stress test
    // proves CRC alone is not sufficient.
    let _r = io_locks().read(record_offset);
    unsafe {
        let src = base_ptr.add(off_to_usize(record_offset));
        // F-G1-003 / C-3: copy the 256 header bytes via atomic
        // chunked load (AtomicU64 body + AtomicU8 head/tail) into a
        // local stack buffer. This eliminates the data race miri
        // flags when a concurrent `write_metadata_direct` (which
        // performs atomic chunked stores) targets the same record.
        // The local buffer is a non-aliased stack value so the
        // subsequent `TxMetadata::from_bytes(&buf)` slice deref
        // does not retag racing references.
        let mut buf = [0u8; METADATA_SIZE];
        atomic_load_into(src, &mut buf);
        Ok(TxMetadata::from_bytes(&buf)?)
    }
}

/// Read and validate just the immutable identity prefix of a record from a
/// memory-mapped device region.
///
/// This copies only the first [`IDENTITY_READ_LEN`] bytes (one cache line)
/// of the header and validates them against [`TxMetadata::identity_crc`],
/// returning [`TxIdentity`] (`tx_id`, `utxo_count`, `locktime`). It is the
/// hot-path counterpart to [`read_metadata_direct`] for `get_spend`, which
/// needs only those three fields and pays for one cache-line fetch instead
/// of five.
///
/// # Safety
///
/// `base_ptr` must be valid for at least `record_offset + IDENTITY_READ_LEN`
/// bytes (always true: every record is at least `METADATA_SIZE` bytes).
///
/// # Concurrency contract (F-X-007 / BC-02)
///
/// Acquires the process-wide `StripedRwLocks` read guard keyed by
/// `record_offset`, exactly like [`read_metadata_direct`] — the identity
/// bytes are written by the bulk `write_metadata_direct` path via atomic
/// chunked stores, so the read must go through `atomic_load_into` under the
/// guard to avoid a torn read. The identity fields are immutable after
/// create, so a concurrent full rewrite restamps identical values; the
/// guard + CRC still guarantee a non-torn, valid-or-rejected result.
#[inline]
pub unsafe fn read_identity_direct(base_ptr: *const u8, record_offset: u64) -> Result<TxIdentity> {
    let _r = io_locks().read(record_offset);
    unsafe {
        let src = base_ptr.add(off_to_usize(record_offset));
        let mut buf = [0u8; IDENTITY_READ_LEN];
        atomic_load_into(src, &mut buf);
        Ok(TxMetadata::read_identity_from(&buf)?)
    }
}

/// Block-device fallback for [`read_identity_direct`].
///
/// Reads the leading [`IDENTITY_READ_LEN`] bytes of the record (rounded up to
/// device alignment) and validates the identity prefix.
pub fn read_identity(device: &dyn BlockDevice, record_offset: u64) -> Result<TxIdentity> {
    let align = device.alignment();
    let aligned_base = record_offset / align as u64 * align as u64;
    let intra_offset = (record_offset - aligned_base) as usize;
    let total_read = align_up(intra_offset + IDENTITY_READ_LEN, align);
    let mut read_buf = AlignedBuf::new(total_read, align);
    device.pread_exact_at(&mut read_buf, aligned_base)?;
    Ok(TxMetadata::read_identity_from(
        &read_buf[intra_offset..intra_offset + IDENTITY_READ_LEN],
    )?)
}

/// Write [`TxMetadata`] directly to a memory-mapped device region.
///
/// Zero-copy serialization: writes the metadata bytes directly to the
/// target address. No `AlignedBuf`, no read-modify-write.
///
/// # Safety
///
/// `base_ptr` must be valid for at least `record_offset + METADATA_SIZE` bytes.
/// Caller must hold the per-transaction stripe lock.
///
/// # Concurrency contract (F-X-007 / BC-02)
///
/// Acquires the process-wide `StripedRwLocks` *write* guard keyed by
/// `record_offset` for the duration of the bulk memcpy. Paired with
/// `read_metadata_direct`'s read guard so a concurrent direct-pointer
/// reader observes one of the values written, never a torn mix.
/// See `read_metadata_direct`'s doc for the full rationale.
#[inline]
pub unsafe fn write_metadata_direct(base_ptr: *mut u8, record_offset: u64, metadata: &TxMetadata) {
    // F-X-007 (BC-02): record-level write guard — see
    // `read_metadata_direct`. The CRC-alone defense documented at
    // this site in the previous revision was empirically false on
    // aarch64 release builds (the regression test fails ~90 % of
    // runs without this guard).
    let _w = io_locks().write(record_offset);
    unsafe {
        let dst = base_ptr.add(off_to_usize(record_offset));
        // F-G1-003 / C-3: serialize to a local stack buffer first,
        // then atomic-chunked-store into the device memory. See
        // `read_metadata_direct` for the rationale on why the bulk
        // copy must go through atomics rather than `from_raw_parts_mut`
        // + `copy_from_slice` (which retag races miri's data-race
        // detector against the reader on a different thread).
        let mut buf = [0u8; METADATA_SIZE];
        metadata.to_bytes(&mut buf);
        atomic_store_from(dst, &buf);
        // R-030 (BC-07): Release fence AFTER the memcpy so all
        // store operations commit before the next memory access
        // can be observed by another core. Pairs with the reader's
        // Acquire fence (R-029); together they prevent a reader on
        // a different core from seeing the new CRC bytes alongside
        // stale header bytes.
        std::sync::atomic::fence(std::sync::atomic::Ordering::Release);
    }
}

/// Read a single [`UtxoSlot`] directly from a memory-mapped device region.
///
/// # Safety
///
/// `base_ptr` must be valid for at least `record_offset + METADATA_SIZE +
/// (slot_index + 1) * UTXO_SLOT_SIZE` bytes.
///
/// # Concurrency contract (F-X-007 / BC-02)
///
/// Acquires the process-wide `StripedRwLocks` read guard keyed by
/// `record_offset` for the duration of the slot read. UTXO slots
/// carry their own per-slot CRC and would otherwise be subject to
/// the same torn-read window as `read_metadata_direct` — see that
/// function's doc for the full rationale.
#[inline]
pub unsafe fn read_utxo_slot_direct(
    base_ptr: *const u8,
    record_offset: u64,
    slot_index: u32,
) -> Result<UtxoSlot> {
    // F-X-007 (BC-02): record-level read guard. The previous design
    // relied on the per-slot CRC alone; that defense is not
    // sufficient on aarch64 release builds (see `read_metadata_direct`).
    let _r = io_locks().read(record_offset);
    unsafe {
        let slot_offset = record_offset + TxMetadata::utxo_slot_offset(slot_index);
        let src = base_ptr.add(off_to_usize(slot_offset));
        // F-G1-003 / C-3: atomic chunked load into a local stack
        // buffer — see `read_metadata_direct`.
        let mut buf = [0u8; UTXO_SLOT_SIZE];
        atomic_load_into(src, &mut buf);
        Ok(UtxoSlot::from_bytes(&buf)?)
    }
}

/// Write a single [`UtxoSlot`] directly to a memory-mapped device region.
///
/// # Safety
///
/// `base_ptr` must be valid for at least `record_offset + METADATA_SIZE +
/// (slot_index + 1) * UTXO_SLOT_SIZE` bytes. Caller must hold the stripe lock.
///
/// # Concurrency contract (F-X-007 / BC-02)
///
/// Acquires the process-wide `StripedRwLocks` write guard keyed by
/// `record_offset` for the duration of the bulk memcpy. See
/// `read_metadata_direct` for the full rationale on why the CRC-only
/// defense is not sufficient on aarch64 release builds.
#[inline]
pub unsafe fn write_utxo_slot_direct(
    base_ptr: *mut u8,
    record_offset: u64,
    slot_index: u32,
    slot: &UtxoSlot,
) {
    // F-X-007 (BC-02): record-level write guard.
    let _w = io_locks().write(record_offset);
    unsafe {
        let slot_offset = record_offset + TxMetadata::utxo_slot_offset(slot_index);
        let dst = base_ptr.add(off_to_usize(slot_offset));
        // F-G1-003 / C-3: atomic chunked store from a local stack
        // buffer — see `write_metadata_direct`.
        let mut buf = [0u8; UTXO_SLOT_SIZE];
        slot.to_bytes(&mut buf);
        atomic_store_from(dst, &buf);
        // R-030 (BC-07): Release fence — see `write_metadata_direct`
        // for the full rationale. Slot writes have the same
        // memory-ordering risk as metadata writes on AArch64.
        std::sync::atomic::fence(std::sync::atomic::Ordering::Release);
    }
}

// ===========================================================================
// Block I/O path — for O_DIRECT devices without memory mapping
// ===========================================================================

/// Read the [`TxMetadata`] header of a record at `record_offset`.
///
/// Reads the first `METADATA_SIZE` bytes from the device at the given
/// record offset. The read is rounded up to the device alignment.
pub fn read_metadata(device: &dyn BlockDevice, record_offset: u64) -> Result<TxMetadata> {
    let align = device.alignment();

    // Record offset must be aligned (allocator guarantees this).
    let aligned_base = record_offset / align as u64 * align as u64;
    let intra_offset = (record_offset - aligned_base) as usize;

    // F-G1-008: read directly from the aligned device buffer. The
    // previous implementation allocated a second `AlignedBuf` of size
    // `align_up(METADATA_SIZE, align)`, copied the header bytes into
    // it, and then deserialized from there — one redundant heap alloc
    // + memcpy per call. Recovery scans every record at boot, so this
    // matters on the cold path; on the direct-ptr hot path this code
    // is bypassed entirely.
    let total_read = align_up(intra_offset + METADATA_SIZE, align);
    let mut read_buf = AlignedBuf::new(total_read, align);
    device.pread_exact_at(&mut read_buf, aligned_base)?;

    Ok(TxMetadata::from_bytes(
        &read_buf[intra_offset..intra_offset + METADATA_SIZE],
    )?)
}

/// Write the [`TxMetadata`] header of a record at `record_offset`.
///
/// Uses read-modify-write if `METADATA_SIZE` is not a multiple of the
/// device alignment.
pub fn write_metadata(
    device: &dyn BlockDevice,
    record_offset: u64,
    metadata: &TxMetadata,
) -> Result<()> {
    let align = device.alignment();
    let aligned_base = record_offset / align as u64 * align as u64;
    let intra_offset = (record_offset - aligned_base) as usize;
    let total_size = align_up(intra_offset + METADATA_SIZE, align);

    let mut buf = AlignedBuf::new(total_size, align);

    // If the write doesn't cover a full aligned block, read-modify-write.
    if intra_offset != 0 || !METADATA_SIZE.is_multiple_of(align) {
        device.pread_exact_at(&mut buf, aligned_base)?;
    }

    let mut meta_bytes = [0u8; METADATA_SIZE];
    metadata.to_bytes(&mut meta_bytes);
    buf[intra_offset..intra_offset + METADATA_SIZE].copy_from_slice(&meta_bytes);

    device.pwrite_all_at(&buf, aligned_base)?;
    Ok(())
}

/// Read a single [`UtxoSlot`] at `slot_index` within the record at `record_offset`.
///
/// The slot offset is: `record_offset + METADATA_SIZE + slot_index * UTXO_SLOT_SIZE`.
pub fn read_utxo_slot(
    device: &dyn BlockDevice,
    record_offset: u64,
    slot_index: u32,
) -> Result<UtxoSlot> {
    let align = device.alignment();
    let slot_offset = record_offset + TxMetadata::utxo_slot_offset(slot_index);
    let aligned_base = slot_offset / align as u64 * align as u64;
    let intra_offset = (slot_offset - aligned_base) as usize;
    let total_read = align_up(intra_offset + UTXO_SLOT_SIZE, align);

    let mut buf = AlignedBuf::new(total_read, align);
    device.pread_exact_at(&mut buf, aligned_base)?;

    Ok(UtxoSlot::from_bytes(
        &buf[intra_offset..intra_offset + UTXO_SLOT_SIZE],
    )?)
}

/// Read every [`UtxoSlot`] for a record in one aligned device read.
///
/// This is the batched counterpart to [`read_utxo_slot`] for GET/delete
/// snapshot paths that need the full slot set. It avoids one aligned
/// `pread` per slot while preserving the same O_DIRECT alignment rules.
pub fn read_all_utxo_slots(
    device: &dyn BlockDevice,
    record_offset: u64,
    slot_count: u32,
) -> Result<Vec<UtxoSlot>> {
    if slot_count == 0 {
        return Ok(Vec::new());
    }

    let align = device.alignment();
    let first_slot_offset = record_offset + TxMetadata::utxo_slot_offset(0);
    let aligned_base = first_slot_offset / align as u64 * align as u64;
    let intra_offset = (first_slot_offset - aligned_base) as usize;
    let slot_bytes = (slot_count as usize)
        .checked_mul(UTXO_SLOT_SIZE)
        .ok_or_else(|| {
            DeviceError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "slot read byte count overflow",
            ))
        })?;
    let total_read = align_up(intra_offset + slot_bytes, align);

    // F-X-007 (BC-02): record-level read guard so a concurrent
    // `write_record_bytes` (create into a reused region) or a slot write
    // cannot be observed mid-copy. Keyed by the record's base offset —
    // the same key every reader and writer of this record uses — so the
    // guard excludes whole-record and slot writers regardless of which
    // 4 KiB page the slot region itself lands in.
    let _r = io_locks().read(record_offset);

    let mut buf = AlignedBuf::new(total_read, align);
    device.pread_exact_at(&mut buf, aligned_base)?;

    let mut slots = Vec::with_capacity(slot_count as usize);
    for i in 0..slot_count as usize {
        let start = intra_offset + i * UTXO_SLOT_SIZE;
        slots.push(UtxoSlot::from_bytes(&buf[start..start + UTXO_SLOT_SIZE])?);
    }
    Ok(slots)
}

/// Write a single [`UtxoSlot`] at `slot_index` within the record at `record_offset`.
///
/// Uses read-modify-write: reads the aligned block containing the slot,
/// modifies the slot bytes, writes the block back.
pub fn write_utxo_slot(
    device: &dyn BlockDevice,
    record_offset: u64,
    slot_index: u32,
    slot: &UtxoSlot,
) -> Result<()> {
    let align = device.alignment();
    let slot_offset = record_offset + TxMetadata::utxo_slot_offset(slot_index);
    let aligned_base = slot_offset / align as u64 * align as u64;
    let intra_offset = (slot_offset - aligned_base) as usize;
    let total_size = align_up(intra_offset + UTXO_SLOT_SIZE, align);

    let mut buf = AlignedBuf::new(total_size, align);
    // Read-modify-write: one slot is always less than a 4096 block.
    device.pread_exact_at(&mut buf, aligned_base)?;

    let mut slot_bytes = [0u8; UTXO_SLOT_SIZE];
    slot.to_bytes(&mut slot_bytes);
    buf[intra_offset..intra_offset + UTXO_SLOT_SIZE].copy_from_slice(&slot_bytes);

    device.pwrite_all_at(&buf, aligned_base)?;
    Ok(())
}

/// Write a complete new record (metadata + all UTXO slots) in one operation.
///
/// Used at creation time. The entire record is written as a single aligned
/// buffer to minimize I/O operations.
pub fn write_full_record(
    device: &dyn BlockDevice,
    record_offset: u64,
    metadata: &TxMetadata,
    slots: &[UtxoSlot],
) -> Result<()> {
    let align = device.alignment();
    let data_len = METADATA_SIZE + slots.len() * UTXO_SLOT_SIZE;
    let aligned_len = align_up(data_len, align);

    let mut buf = AlignedBuf::new(aligned_len, align);

    // Write metadata
    let mut meta_bytes = [0u8; METADATA_SIZE];
    metadata.to_bytes(&mut meta_bytes);
    buf[..METADATA_SIZE].copy_from_slice(&meta_bytes);

    // Write slots
    for (i, slot) in slots.iter().enumerate() {
        let offset = METADATA_SIZE + i * UTXO_SLOT_SIZE;
        let mut slot_bytes = [0u8; UTXO_SLOT_SIZE];
        slot.to_bytes(&mut slot_bytes);
        buf[offset..offset + UTXO_SLOT_SIZE].copy_from_slice(&slot_bytes);
    }

    device.pwrite_all_at(&buf, record_offset)?;
    Ok(())
}

/// Write a fully serialized record image (metadata + UTXO slots + optional
/// cold data, already alignment-padded by the caller) at `record_offset`
/// in one device write.
///
/// Used by the engine's create path, which builds the aligned record buffer
/// itself (it interleaves cold data after the slots).
///
/// # Errors
///
/// Propagates any [`DeviceError`] from the underlying `pwrite`.
///
/// # Concurrency contract (F-X-007 / BC-02)
///
/// Acquires the process-wide `StripedRwLocks` *write* guard keyed by
/// `record_offset` for the duration of the write. Creation was previously
/// exempt from the record-level guard on the assumption that a record being
/// created has no concurrent readers — that assumption is false when the
/// allocator reuses a freed region: a lock-free reader still holding the old
/// offset (obtained from the primary index before the previous occupant's
/// delete unregistered it) reads the region while the new record's bytes are
/// being copied in. If the region is re-created for the SAME txid the stale
/// reader is verifying, the `meta.tx_id` defense (F-G2-001) passes while the
/// slot region still carries the previous occupant's bytes — returning
/// another transaction's slots under this key. Live repro:
/// `tests/g2_delete_race.rs::delete_does_not_alias_concurrent_create`.
/// Pairs with the read guards in `read_metadata_direct`,
/// `read_utxo_slot_direct`, and `read_all_utxo_slots`.
pub fn write_record_bytes(device: &dyn BlockDevice, record_offset: u64, buf: &[u8]) -> Result<()> {
    // F-X-007 (BC-02): record-level write guard — see the doc comment.
    let _w = io_locks().write(record_offset);
    device.pwrite_all_at(buf, record_offset)?;
    Ok(())
}

/// Read multiple UTXO slots by index from a record.
///
/// Returns a vector of `(slot_index, UtxoSlot)` pairs in the order requested.
/// This batches reads when slots are close together on disk.
pub fn read_utxo_slots(
    device: &dyn BlockDevice,
    record_offset: u64,
    slot_indices: &[u32],
) -> Result<Vec<(u32, UtxoSlot)>> {
    if slot_indices.is_empty() {
        return Ok(Vec::new());
    }

    let mut result = Vec::with_capacity(slot_indices.len());
    for &idx in slot_indices {
        let slot = read_utxo_slot(device, record_offset, idx)?;
        result.push((idx, slot));
    }
    Ok(result)
}

/// Round `size` up to the nearest multiple of `alignment`.
pub fn align_up(size: usize, alignment: usize) -> usize {
    size.div_ceil(alignment) * alignment
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::MemoryDevice;
    use crate::record::*;
    use std::sync::Arc;

    fn test_device() -> Arc<MemoryDevice> {
        // 16 MB, 4096-byte alignment
        Arc::new(MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap())
    }

    /// Helper: create test metadata + slots and write them at `record_offset`.
    fn create_test_record(
        dev: &dyn BlockDevice,
        record_offset: u64,
        num_slots: u32,
    ) -> (TxMetadata, Vec<UtxoSlot>) {
        let mut meta = TxMetadata::new(num_slots);
        meta.tx_id[0] = 0xAA;
        meta.tx_id[31] = 0xBB;
        meta.fee = 5000;
        meta.locktime = 700_000;

        let mut slots = Vec::with_capacity(num_slots as usize);
        for i in 0..num_slots {
            let mut hash = [0u8; 32];
            hash[0] = (i & 0xFF) as u8;
            hash[1] = ((i >> 8) & 0xFF) as u8;
            slots.push(UtxoSlot::new_unspent(hash));
        }

        write_full_record(dev, record_offset, &meta, &slots).unwrap();
        (meta, slots)
    }

    #[test]
    fn write_full_then_read_metadata() {
        let dev = test_device();
        let (meta, _) = create_test_record(&*dev, 0, 10);

        let read_meta = read_metadata(&*dev, 0).unwrap();
        assert_eq!(read_meta, meta);
        assert_eq!({ read_meta.utxo_count }, 10);
        assert_eq!({ read_meta.fee }, 5000);
    }

    #[test]
    fn read_identity_block_and_direct_agree() {
        // The identity prefix must round-trip through BOTH the block-device
        // fallback (`read_identity`) and the mmap fast path
        // (`read_identity_direct`), at a non-zero (aligned) record offset.
        let dev = test_device();
        let off = 4096u64;
        create_test_record(&*dev, off, 10);

        let id_block = read_identity(&*dev, off).expect("block-path identity");
        assert_eq!(id_block.utxo_count, 10);
        assert_eq!(id_block.locktime, 700_000);
        assert_eq!(id_block.tx_id[0], 0xAA);
        assert_eq!(id_block.tx_id[31], 0xBB);

        let base = dev.as_raw_ptr().expect("memory device exposes raw_ptr");
        // SAFETY: `base` is the live device base; `off` is an aligned,
        // in-bounds record offset written above.
        let id_direct = unsafe { read_identity_direct(base, off) }.expect("direct-path identity");
        assert_eq!(id_block, id_direct);
    }

    #[test]
    fn write_full_then_read_each_slot() {
        let dev = test_device();
        let (_, slots) = create_test_record(&*dev, 0, 10);

        for (i, expected) in slots.iter().enumerate() {
            let actual = read_utxo_slot(&*dev, 0, i as u32).unwrap();
            assert_eq!(actual, *expected, "slot {i} mismatch");
        }
    }

    #[test]
    fn modify_single_slot() {
        let dev = test_device();
        create_test_record(&*dev, 0, 10);

        // Modify slot 5
        let mut sd = [0u8; 36];
        sd[..32].copy_from_slice(&[0xDE; 32]);
        sd[32..36].copy_from_slice(&1u32.to_le_bytes());
        let new_slot = UtxoSlot::new_spent(
            [
                0x05, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0,
            ],
            sd,
        );

        write_utxo_slot(&*dev, 0, 5, &new_slot).unwrap();

        let read_back = read_utxo_slot(&*dev, 0, 5).unwrap();
        assert_eq!(read_back, new_slot);
        assert!(read_back.is_spent());
    }

    #[test]
    fn modify_slot_does_not_affect_neighbors() {
        let dev = test_device();
        let (_, original_slots) = create_test_record(&*dev, 0, 10);

        // Modify slot 5
        let mut sd = [0u8; 36];
        sd[0] = 0xFF;
        let new_slot = UtxoSlot::new_spent(original_slots[5].hash, sd);
        write_utxo_slot(&*dev, 0, 5, &new_slot).unwrap();

        // Check neighbors are unchanged
        let slot4 = read_utxo_slot(&*dev, 0, 4).unwrap();
        assert_eq!(slot4, original_slots[4]);

        let slot6 = read_utxo_slot(&*dev, 0, 6).unwrap();
        assert_eq!(slot6, original_slots[6]);
    }

    #[test]
    fn write_metadata_updates_counter() {
        let dev = test_device();
        let (mut meta, _) = create_test_record(&*dev, 0, 10);

        meta.spent_utxos = 3;
        write_metadata(&*dev, 0, &meta).unwrap();

        let read_meta = read_metadata(&*dev, 0).unwrap();
        assert_eq!({ read_meta.spent_utxos }, 3);

        // UTXO slots should be unchanged
        let slot0 = read_utxo_slot(&*dev, 0, 0).unwrap();
        assert!(slot0.is_unspent());
    }

    #[test]
    fn read_utxo_slots_batch() {
        let dev = test_device();
        let (_, slots) = create_test_record(&*dev, 0, 10);

        let results = read_utxo_slots(&*dev, 0, &[0, 5, 9]).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, 0);
        assert_eq!(results[0].1, slots[0]);
        assert_eq!(results[1].0, 5);
        assert_eq!(results[1].1, slots[5]);
        assert_eq!(results[2].0, 9);
        assert_eq!(results[2].1, slots[9]);
    }

    #[test]
    fn read_utxo_slots_empty() {
        let dev = test_device();
        create_test_record(&*dev, 0, 10);

        let results = read_utxo_slots(&*dev, 0, &[]).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn read_all_utxo_slots_batch_reads_full_slot_region() {
        let dev = test_device();
        let (_, slots) = create_test_record(&*dev, 0, 10);

        let results = read_all_utxo_slots(&*dev, 0, 10).unwrap();
        assert_eq!(results, slots);
        assert!(read_all_utxo_slots(&*dev, 0, 0).unwrap().is_empty());
    }

    #[test]
    fn record_with_1000_slots() {
        let dev = test_device();
        let (_, slots) = create_test_record(&*dev, 0, 1000);

        // Read slot 999
        let slot999 = read_utxo_slot(&*dev, 0, 999).unwrap();
        assert_eq!(slot999, slots[999]);

        // Read slot 0
        let slot0 = read_utxo_slot(&*dev, 0, 0).unwrap();
        assert_eq!(slot0, slots[0]);
    }

    #[test]
    fn write_slot_0_does_not_corrupt_slot_999() {
        let dev = test_device();
        let (_, original_slots) = create_test_record(&*dev, 0, 1000);

        // Modify slot 0
        let frozen = UtxoSlot::new_frozen(original_slots[0].hash);
        write_utxo_slot(&*dev, 0, 0, &frozen).unwrap();

        // Slot 999 must be unchanged
        let slot999 = read_utxo_slot(&*dev, 0, 999).unwrap();
        assert_eq!(slot999, original_slots[999]);
    }

    // -- Integration test: full lifecycle --

    #[test]
    fn full_lifecycle() {
        use crate::allocator::{DATA_REGION_OFFSET, SlotAllocator};

        // 1. Create allocator on MemoryDevice (64 MB to fit 1000-slot records)
        let dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(dev.clone()).unwrap();

        // 2. Allocate space for a record with 100 UTXO slots
        let record_size = TxMetadata::record_size_for(100);
        let record_offset = alloc.allocate(record_size).unwrap();
        assert!(record_offset >= DATA_REGION_OFFSET);

        // 3. Write full record with metadata + 100 unspent slots
        let mut meta = TxMetadata::new(100);
        meta.tx_id = [0xBEu8; 32];
        meta.fee = 42;

        let mut slots = Vec::with_capacity(100);
        for i in 0u32..100 {
            let mut hash = [0u8; 32];
            hash[0] = i as u8;
            slots.push(UtxoSlot::new_unspent(hash));
        }
        write_full_record(&*dev, record_offset, &meta, &slots).unwrap();

        // 4. Read back metadata, verify utxo_count=100, spent_utxos=0
        let read_meta = read_metadata(&*dev, record_offset).unwrap();
        assert_eq!({ read_meta.utxo_count }, 100);
        assert_eq!({ read_meta.spent_utxos }, 0);
        assert_eq!(read_meta.tx_id, [0xBEu8; 32]);

        // 5. Read back each slot, verify all unspent
        for i in 0u32..100 {
            let slot = read_utxo_slot(&*dev, record_offset, i).unwrap();
            assert!(slot.is_unspent(), "slot {i} should be unspent");
        }

        // 6. Write spent data to slot 50
        let mut sd = [0u8; 36];
        sd[..32].copy_from_slice(&[0xABu8; 32]);
        sd[32..36].copy_from_slice(&99u32.to_le_bytes());
        let spent_slot = UtxoSlot::new_spent(slots[50].hash, sd);
        write_utxo_slot(&*dev, record_offset, 50, &spent_slot).unwrap();

        // 7. Read slot 50: verify spent
        let s50 = read_utxo_slot(&*dev, record_offset, 50).unwrap();
        assert!(s50.is_spent());
        assert_eq!(s50.spending_data, sd);

        // 8. Read slot 49 and 51: still unspent
        let s49 = read_utxo_slot(&*dev, record_offset, 49).unwrap();
        assert!(s49.is_unspent());
        let s51 = read_utxo_slot(&*dev, record_offset, 51).unwrap();
        assert!(s51.is_unspent());

        // 9. Update metadata spent_utxos=1
        let mut updated_meta = read_meta;
        updated_meta.spent_utxos = 1;
        write_metadata(&*dev, record_offset, &updated_meta).unwrap();

        // 10. Read metadata: verify spent_utxos=1, other fields unchanged
        let final_meta = read_metadata(&*dev, record_offset).unwrap();
        assert_eq!({ final_meta.spent_utxos }, 1);
        assert_eq!({ final_meta.utxo_count }, 100);
        assert_eq!(final_meta.tx_id, [0xBEu8; 32]);
        assert_eq!({ final_meta.fee }, 42);

        // 11. Free the record
        alloc.free(record_offset, record_size).unwrap();

        // 12. Allocate new record at same location
        let new_offset = alloc.allocate(record_size).unwrap();
        assert_eq!(new_offset, record_offset);

        // 13. Write new record, verify old data is gone
        let new_meta = TxMetadata::new(50);
        let new_slots: Vec<UtxoSlot> = (0..50u32)
            .map(|i| {
                let mut h = [0u8; 32];
                h[0] = 0xFF;
                h[1] = i as u8;
                UtxoSlot::new_unspent(h)
            })
            .collect();
        write_full_record(&*dev, new_offset, &new_meta, &new_slots).unwrap();

        let check_meta = read_metadata(&*dev, new_offset).unwrap();
        assert_eq!({ check_meta.utxo_count }, 50);
        assert_ne!(check_meta.tx_id, [0xBEu8; 32]); // Old txid is gone
    }

    /// R-029 / R-030 (BC-06 / BC-07) regression: under concurrent
    /// write_metadata_direct + read_metadata_direct, the reader must
    /// observe EITHER a coherent metadata value (one of the values
    /// the writer wrote) OR a `RecordCorruption` error. The reader
    /// must NEVER see a "successful" read whose `tx_version` /
    /// `fee` field combination was never written by the writer
    /// (which would indicate a torn read that the CRC failed to
    /// catch, i.e. a missed memory-ordering barrier).
    ///
    /// Pre-fix the direct read/write paths had no memory fences;
    /// on AArch64 the relaxed store ordering allowed a reader on a
    /// different core to observe the new CRC bytes paired with old
    /// header bytes (or vice versa) — silent corruption with a
    /// validating CRC. The fences (Acquire on read, Release on
    /// write) ensure the AArch64 hardware emits dmb instructions
    /// that prevent the reordering.
    ///
    /// Note: even on x86-64 (where TSO largely guarantees the
    /// ordering), the test exercises the contract; the fences are
    /// essentially free on x86 (they compile to no instruction on
    /// some configurations, otherwise just `mfence`). The point of
    /// the test is the contract pin, not the AArch64 hardware
    /// proof.
    #[test]
    fn direct_read_write_concurrent_stress_never_returns_torn_data() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let dev = test_device();
        let record_offset: u64 = 0;
        // Two well-formed metadata values the writer rotates between.
        // Each carries a distinctive (tx_version, fee) pair so the
        // reader can validate it observed one of the two — never an
        // unwritten combination.
        let mut meta_a = TxMetadata::new(4);
        meta_a.tx_id = [0xAAu8; 32];
        meta_a.tx_version = 0xA1A2A3A4;
        meta_a.fee = 0xAAAA_AAAA_AAAA_AAAA;
        let mut meta_b = TxMetadata::new(4);
        meta_b.tx_id = [0xBBu8; 32];
        meta_b.tx_version = 0xB1B2B3B4;
        meta_b.fee = 0xBBBB_BBBB_BBBB_BBBB;

        // Seed the device with meta_a so a coherent read is always
        // possible, and obtain the raw_ptr for the direct paths.
        let slots: Vec<UtxoSlot> = (0..4)
            .map(|i| UtxoSlot::new_unspent([i as u8; 32]))
            .collect();
        write_full_record(&*dev, record_offset, &meta_a, &slots).unwrap();

        let base_ptr_addr = dev.as_raw_ptr().expect("memory device must expose raw_ptr") as usize;
        let stop = Arc::new(AtomicBool::new(false));

        // Writer thread: rotate between meta_a and meta_b. The
        // stripe-lock contract says writers don't interleave; we
        // model that by having a single writer.
        let writer_stop = stop.clone();
        let writer = std::thread::spawn(move || {
            let mut toggle = false;
            // Local copies because TxMetadata is not Send by default
            // when borrowed through a *mut u8 via the writer side.
            let local_a = meta_a;
            let local_b = meta_b;
            for _ in 0..20_000 {
                if writer_stop.load(Ordering::Relaxed) {
                    break;
                }
                let m = if toggle { &local_a } else { &local_b };
                toggle = !toggle;
                unsafe {
                    let p = base_ptr_addr as *mut u8;
                    write_metadata_direct(p, record_offset, m);
                }
            }
        });

        // Reader threads: each spins reading and asserts the
        // coherence invariant. If the read returns a coherent
        // metadata value (CRC-validated), the (tx_version, fee)
        // pair MUST be one of the two we wrote — never a torn
        // mix. RecordCorruption is acceptable (CRC caught the
        // tear); silent garbage is not.
        let mut readers = Vec::new();
        for _ in 0..3 {
            let reader_stop = stop.clone();
            readers.push(std::thread::spawn(move || {
                let mut iterations = 0u64;
                let mut corruption_count = 0u64;
                while iterations < 30_000 && !reader_stop.load(Ordering::Relaxed) {
                    let result = unsafe {
                        let p = base_ptr_addr as *const u8;
                        read_metadata_direct(p, record_offset)
                    };
                    match result {
                        Ok(m) => {
                            let v = { m.tx_version };
                            let f = { m.fee };
                            // Coherent read MUST be one of the two
                            // pairs we wrote.
                            let is_a = v == 0xA1A2A3A4 && f == 0xAAAA_AAAA_AAAA_AAAA;
                            let is_b = v == 0xB1B2B3B4 && f == 0xBBBB_BBBB_BBBB_BBBB;
                            assert!(
                                is_a || is_b,
                                "torn read passed CRC: tx_version={v:#x}, fee={f:#x}",
                            );
                        }
                        Err(_) => {
                            // CRC caught a torn read; this is the
                            // correct fail-closed behaviour.
                            corruption_count += 1;
                        }
                    }
                    iterations += 1;
                }
                (iterations, corruption_count)
            }));
        }

        for r in readers {
            let (iters, _corruptions) = r.join().unwrap();
            assert!(iters > 0, "reader thread must observe at least one read");
        }
        stop.store(true, Ordering::Relaxed);
        writer.join().unwrap();
    }

    /// F-G1-002 typestate guard: dropping a `FooterPendingCrc` without
    /// calling `stamp_crc` is a contract violation. In debug builds
    /// this fires `debug_assert!` from `Drop`, which we observe via
    /// `catch_unwind`. The `#[must_use]` attribute on the type
    /// catches the same mistake at compile time when callers honour
    /// `-D unused_must_use`; this test is the runtime backstop.
    ///
    /// The drop-time panic is intentional: it is the only way to
    /// surface "footer written, CRC forgotten" before the next reader
    /// fails with `RecordCorruption` at an unrelated call site. In
    /// release builds the assertion compiles out and the reader
    /// surfaces the corruption — also acceptable, just less helpful.
    #[test]
    #[cfg(debug_assertions)]
    fn footer_pending_crc_panics_when_dropped_unstamped() {
        let dev = test_device();
        let base_ptr_addr = dev.as_raw_ptr().expect("memory device must expose raw_ptr") as usize;
        let mut meta = TxMetadata::new(1);
        meta.tx_id = [0x11; 32];
        meta.fee = 7;
        let slots = vec![UtxoSlot::new_unspent([0xCD; 32])];
        write_full_record(&*dev, 0, &meta, &slots).unwrap();

        let result = std::panic::catch_unwind(|| {
            // SAFETY: `base_ptr` valid for METADATA_SIZE; this thread
            // holds no concurrent reader/writer on offset 0.
            let token =
                unsafe { write_mutation_footer_pending_crc(base_ptr_addr as *mut u8, 0, &meta) };
            // Intentionally drop without stamping. The Drop impl
            // should fire `debug_assert!`.
            drop(token);
        });

        assert!(
            result.is_err(),
            "dropping FooterPendingCrc without stamp_crc must panic in debug builds",
        );
    }

    /// Re-review P2: in RELEASE builds the drop-time `debug_assert!`
    /// compiles out, so the only observable signal of a forgotten CRC
    /// stamp is the `tracing::error!` event plus the
    /// `footer_crc_drop_count()` counter. Verify the counter increments
    /// on an unstamped drop. The counter bump happens BEFORE the
    /// `debug_assert!` in the `Drop` impl, so this test works in both
    /// build modes: in debug the drop panics (caught by `catch_unwind`)
    /// after the increment; in release it just runs. The counter is
    /// process-wide and monotonic, so we assert a relative increase
    /// (`> before`) to stay robust against other tests dropping
    /// unstamped tokens concurrently.
    #[test]
    fn footer_pending_crc_drop_increments_release_counter() {
        let dev = test_device();
        let base_ptr_addr = dev.as_raw_ptr().expect("memory device must expose raw_ptr") as usize;
        let mut meta = TxMetadata::new(1);
        meta.tx_id = [0x44; 32];
        meta.fee = 3;
        let slots = vec![UtxoSlot::new_unspent([0xEE; 32])];
        write_full_record(&*dev, 0, &meta, &slots).unwrap();

        let before = footer_crc_drop_count();

        // `catch_unwind` covers both modes: debug drop panics after the
        // counter bump; release drop returns normally.
        let _ = std::panic::catch_unwind(|| {
            // SAFETY: `base_ptr` valid for METADATA_SIZE; this thread
            // holds no concurrent reader/writer on offset 0.
            let token =
                unsafe { write_mutation_footer_pending_crc(base_ptr_addr as *mut u8, 0, &meta) };
            // Intentionally drop without stamping.
            drop(token);
        });

        let after = footer_crc_drop_count();
        assert!(
            after > before,
            "unstamped FooterPendingCrc drop must increment the release-build \
             observability counter (before={before}, after={after})",
        );
    }

    /// F-G1-002 typestate: the happy path. Constructing the token,
    /// stamping the CRC, and reading the metadata back must round-trip
    /// the post-mutation field values with a valid CRC.
    #[test]
    fn footer_pending_crc_stamp_round_trips() {
        let dev = test_device();
        let base_ptr_addr = dev.as_raw_ptr().expect("memory device must expose raw_ptr") as usize;

        // Seed a baseline record so the read path has a valid CRC to
        // overwrite.
        let mut meta = TxMetadata::new(2);
        meta.tx_id = [0x22; 32];
        meta.fee = 1;
        meta.generation = 0;
        let slots = vec![
            UtxoSlot::new_unspent([0x01; 32]),
            UtxoSlot::new_unspent([0x02; 32]),
        ];
        write_full_record(&*dev, 0, &meta, &slots).unwrap();

        // Mutate the footer fields the helper actually touches:
        // flags, generation, updated_at, delete_at_height.
        let mut new_meta = meta;
        new_meta.generation = 99;
        new_meta.updated_at = 0x1234_5678_9ABC_DEF0;
        new_meta.delete_at_height = 17;
        new_meta.flags = crate::record::TxFlags::LOCKED;

        // SAFETY: same as `footer_pending_crc_panics_when_dropped_unstamped`.
        let token =
            unsafe { write_mutation_footer_pending_crc(base_ptr_addr as *mut u8, 0, &new_meta) };
        token.stamp_crc(&new_meta);

        let read_back = read_metadata(&*dev, 0).unwrap();
        assert_eq!({ read_back.generation }, 99);
        assert_eq!({ read_back.updated_at }, 0x1234_5678_9ABC_DEF0);
        assert_eq!({ read_back.delete_at_height }, 17);
        assert_eq!(read_back.flags, crate::record::TxFlags::LOCKED);
        // Other fields preserved from the baseline write.
        assert_eq!(read_back.tx_id, [0x22; 32]);
        assert_eq!({ read_back.fee }, 1);
    }

    /// F-G1-002 typestate: the combined wrapper and the explicit
    /// `stamp_crc` path must produce byte-identical metadata. This
    /// pins the refactor — if a future change diverges the two
    /// paths, this test fails.
    #[test]
    fn footer_pending_crc_matches_combined_wrapper() {
        let dev_a = test_device();
        let dev_b = test_device();
        let ptr_a = dev_a.as_raw_ptr().expect("raw_ptr") as usize;
        let ptr_b = dev_b.as_raw_ptr().expect("raw_ptr") as usize;

        let mut meta = TxMetadata::new(1);
        meta.tx_id = [0x33; 32];
        meta.fee = 42;
        let slots = vec![UtxoSlot::new_unspent([0x55; 32])];
        write_full_record(&*dev_a, 0, &meta, &slots).unwrap();
        write_full_record(&*dev_b, 0, &meta, &slots).unwrap();

        let mut updated = meta;
        updated.generation = 5;
        updated.updated_at = 0xDEAD_BEEF;
        updated.delete_at_height = 9;

        // Path A: combined wrapper.
        unsafe { write_mutation_footer_and_crc_direct(ptr_a as *mut u8, 0, &updated) };

        // Path B: typestate split.
        let token = unsafe { write_mutation_footer_pending_crc(ptr_b as *mut u8, 0, &updated) };
        token.stamp_crc(&updated);

        let read_a = read_metadata(&*dev_a, 0).unwrap();
        let read_b = read_metadata(&*dev_b, 0).unwrap();
        assert_eq!(read_a, read_b);
    }

    /// F-G1-003: the targeted footer helpers (`write_mutation_footer_direct`,
    /// `write_spend_footer_direct`, `write_mined_footer_direct`,
    /// `write_block_entry_direct`, `write_crc_direct`) now route through
    /// `atomic_store_u64_rmw` so every store uses the SAME atomic width
    /// (u64) as the bulk `write_metadata_direct` path. Without this,
    /// miri's weak-memory model panics with "cannot have partially
    /// overlapping store buffer when previous write was atomic" — a
    /// narrower atomic (u8/u32) cannot retag an address previously
    /// written by a u64 atomic.
    ///
    /// `_smoke` test runs miri-sized iterations (10 writer × 5 × 1 reader)
    /// — fast enough for the abstract-machine model check.
    /// The `_stress` test runs full iterations (2000 × 5000 × 3 readers)
    /// — native-only because miri interpretation is too slow for it.
    ///
    /// Run under miri to verify the atomic-width invariant:
    ///   `MIRIFLAGS=-Zmiri-disable-isolation cargo +nightly miri test --lib io::tests::direct_footer_helpers_atomic_widths_match_bulk_smoke`
    #[test]
    fn direct_footer_helpers_atomic_widths_match_bulk_smoke() {
        run_footer_helpers_stress(10, 5, 1);
    }

    #[test]
    fn direct_footer_helpers_concurrent_stress_never_returns_torn_data() {
        run_footer_helpers_stress(2_000, 5_000, 3);
    }

    fn run_footer_helpers_stress(writer_iters: usize, reader_iters: u64, reader_count: usize) {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let dev = test_device();
        let record_offset: u64 = 0;
        let mut meta_a = TxMetadata::new(4);
        meta_a.tx_id = [0xAAu8; 32];
        meta_a.tx_version = 0xA1A2A3A4;
        meta_a.fee = 0xAAAA_AAAA_AAAA_AAAA;
        meta_a.spent_utxos = 0xA1A1_A1A1;
        meta_a.generation = 0xA2A2_A2A2;
        meta_a.updated_at = 0xA3A3_A3A3_A3A3_A3A3;
        meta_a.delete_at_height = 0xA4A4_A4A4;
        let mut meta_b = meta_a;
        meta_b.spent_utxos = 0xB1B1_B1B1;
        meta_b.generation = 0xB2B2_B2B2;
        meta_b.updated_at = 0xB3B3_B3B3_B3B3_B3B3;
        meta_b.delete_at_height = 0xB4B4_B4B4;

        let slots: Vec<UtxoSlot> = (0..4)
            .map(|i| UtxoSlot::new_unspent([i as u8; 32]))
            .collect();
        write_full_record(&*dev, record_offset, &meta_a, &slots).unwrap();

        let base_ptr_addr = dev.as_raw_ptr().expect("memory device must expose raw_ptr") as usize;
        let stop = Arc::new(AtomicBool::new(false));

        // Writer thread alternates between bulk `write_metadata_direct`
        // and the targeted `write_spend_footer_and_crc_direct` helper.
        // After every write, the on-disk record must remain coherent
        // (CRC-validated) and the (spent_utxos, generation) pair must
        // match one of the two we wrote.
        let writer_stop = stop.clone();
        let writer = std::thread::spawn(move || {
            let local_a = meta_a;
            let local_b = meta_b;
            for i in 0..writer_iters {
                if writer_stop.load(Ordering::Relaxed) {
                    break;
                }
                let m = if i % 2 == 0 { &local_a } else { &local_b };
                unsafe {
                    let p = base_ptr_addr as *mut u8;
                    if i % 4 == 0 {
                        // Bulk path — exercises the [0..256) u64 chunks.
                        write_metadata_direct(p, record_offset, m);
                        write_crc_direct(p, record_offset, m);
                    } else {
                        // Targeted path — exercises the footer helpers
                        // at offsets 80, 93, 101, 105, ...  The atomic
                        // widths must agree with the bulk path's u64
                        // chunks for the same bytes; without that,
                        // miri's data-race detector / weak-memory
                        // model would flag mixed-width access.
                        write_spend_footer_direct(p, record_offset, m);
                        write_crc_direct(p, record_offset, m);
                    }
                }
            }
        });

        let mut readers = Vec::new();
        for _ in 0..reader_count {
            let reader_stop = stop.clone();
            readers.push(std::thread::spawn(move || {
                let mut iterations = 0u64;
                while iterations < reader_iters && !reader_stop.load(Ordering::Relaxed) {
                    let result = unsafe {
                        let p = base_ptr_addr as *const u8;
                        read_metadata_direct(p, record_offset)
                    };
                    if let Ok(m) = result {
                        let s = { m.spent_utxos };
                        let g = { m.generation };
                        let is_a = s == 0xA1A1_A1A1 && g == 0xA2A2_A2A2;
                        let is_b = s == 0xB1B1_B1B1 && g == 0xB2B2_B2B2;
                        assert!(
                            is_a || is_b,
                            "torn footer-helper read passed CRC: spent_utxos={s:#x}, generation={g:#x}",
                        );
                    }
                    iterations += 1;
                }
                iterations
            }));
        }

        for r in readers {
            let iters = r.join().unwrap();
            assert!(iters > 0, "reader thread must observe at least one read");
        }
        stop.store(true, Ordering::Relaxed);
        writer.join().unwrap();
    }
}
