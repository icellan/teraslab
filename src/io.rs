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
use crate::record::{
    BLOCK_ENTRY_SIZE, BlockEntry, CRC32_OFFSET, METADATA_SIZE, TxMetadata, UTXO_SLOT_SIZE, UtxoSlot,
};

/// Result type for I/O helper operations.
pub type Result<T> = std::result::Result<T, DeviceError>;

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
        let v = unsafe { AtomicU64::from_ptr(src.add(i).cast_mut().cast()) }
            .load(Ordering::Relaxed);
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

/// Byte offset of `flags` (u8) within TxMetadata.
pub const META_OFF_FLAGS: usize = 80;
/// Byte offset of `spent_utxos` (u32 LE) within TxMetadata.
pub const META_OFF_SPENT_UTXOS: usize = 93;
/// Byte offset of `generation` (u32 LE) within TxMetadata.
pub const META_OFF_GENERATION: usize = 101;
/// Byte offset of `updated_at` (u64 LE) within TxMetadata.
pub const META_OFF_UPDATED_AT: usize = 105;
/// Byte offset of `block_entry_count` (u8) within TxMetadata.
pub const META_OFF_BLOCK_ENTRY_COUNT: usize = 113;
/// Byte offset of `block_entries_inline` ([BlockEntry; 3]) within TxMetadata.
pub const META_OFF_BLOCK_ENTRIES: usize = 114;
/// Byte offset of `unmined_since` (u32 LE) within TxMetadata.
pub const META_OFF_UNMINED_SINCE: usize = 167;
/// Byte offset of `delete_at_height` (u32 LE) within TxMetadata.
pub const META_OFF_DELETE_AT_HEIGHT: usize = 171;
/// Byte offset of `preserve_until` (u32 LE) within TxMetadata.
pub const META_OFF_PRESERVE_UNTIL: usize = 175;

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
#[inline]
pub unsafe fn write_mutation_footer_direct(
    base_ptr: *mut u8,
    record_offset: u64,
    meta: &TxMetadata,
) {
    unsafe {
        let p = base_ptr.add(off_to_usize(record_offset));
        // flags (1 byte)
        p.add(META_OFF_FLAGS).write(meta.flags.bits());
        // generation (4 bytes LE)
        std::ptr::copy_nonoverlapping(
            meta.generation.to_le_bytes().as_ptr(),
            p.add(META_OFF_GENERATION),
            4,
        );
        // updated_at (8 bytes LE)
        std::ptr::copy_nonoverlapping(
            meta.updated_at.to_le_bytes().as_ptr(),
            p.add(META_OFF_UPDATED_AT),
            8,
        );
        // delete_at_height (4 bytes LE)
        std::ptr::copy_nonoverlapping(
            meta.delete_at_height.to_le_bytes().as_ptr(),
            p.add(META_OFF_DELETE_AT_HEIGHT),
            4,
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
        std::ptr::copy_nonoverlapping(
            meta.spent_utxos.to_le_bytes().as_ptr(),
            p.add(META_OFF_SPENT_UTXOS),
            4,
        );
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
        std::ptr::copy_nonoverlapping(
            meta.unmined_since.to_le_bytes().as_ptr(),
            p.add(META_OFF_UNMINED_SINCE),
            4,
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
        // block_entry_count (1 byte)
        p.add(META_OFF_BLOCK_ENTRY_COUNT).write(count);
        // BlockEntry at inline_index (12 bytes)
        let entry_offset = META_OFF_BLOCK_ENTRIES + inline_index * BLOCK_ENTRY_SIZE;
        let mut buf = [0u8; BLOCK_ENTRY_SIZE];
        entry.to_bytes(&mut buf);
        std::ptr::copy_nonoverlapping(buf.as_ptr(), p.add(entry_offset), BLOCK_ENTRY_SIZE);
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
    // Safety: same contract as the primitives — caller holds the
    // stripe lock and the base_ptr is valid for METADATA_SIZE bytes
    // at the record offset.
    unsafe {
        write_mutation_footer_direct(base_ptr, record_offset, meta);
        write_crc_direct(base_ptr, record_offset, meta);
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
        std::ptr::copy_nonoverlapping(crc.to_le_bytes().as_ptr(), p.add(CRC32_OFFSET), 4);
        // R-030 (BC-07): Release fence after the CRC stamp — this
        // is the LAST write of any direct-mutation sequence (the
        // contract on the targeted footer helpers requires callers
        // to follow with `write_crc_direct`), so the fence here
        // covers the entire mutation. See `write_metadata_direct`
        // for the full rationale.
        std::sync::atomic::fence(std::sync::atomic::Ordering::Release);
    }
}

// ===========================================================================
// Direct memory access path — zero allocations
// ===========================================================================

/// Read [`TxMetadata`] directly from a memory-mapped device region, validating
/// the on-disk CRC32.
///
/// Zero-copy: interprets the bytes in place and returns a bitwise copy.
/// No `AlignedBuf` allocation, no `RwLock`, no syscalls. Returns
/// [`DeviceError::RecordCorruption`] if the CRC slot disagrees with a
/// freshly-computed CRC over the header bytes.
///
/// # Safety
///
/// `base_ptr` must be valid for at least `record_offset + METADATA_SIZE` bytes.
///
/// # Concurrency contract (R-009 / BC-02)
///
/// **Read-paths do NOT need the per-transaction stripe lock.** A reader
/// that races with a concurrent `write_metadata_direct` on the same
/// record can observe a torn header — which the CRC32 check at the end
/// of `TxMetadata::from_bytes` detects and surfaces as
/// `DeviceError::RecordCorruption`. The dispatcher maps that to
/// `ERR_INTERNAL` so the client retries; the next read after the
/// writer's pwrite/memcpy completes returns a coherent header.
///
/// Earlier comments on this function asserted "Caller must hold the
/// per-transaction stripe lock" — that contract was never actually
/// honored by `Engine::lookup` / `read_metadata` / `read_slot` /
/// `lookup_cached`, which are hot-path read entries. Adding the lock
/// would serialize all reads against all writes on the same record
/// (an unacceptable performance regression for a UTXO store) without
/// changing the failure mode the CRC already covers. The actual
/// contract is what callers rely on: torn reads → `RecordCorruption`
/// → retry; CRC-clean reads → consistent header.
///
/// `write_metadata_direct` MUST hold the stripe lock so concurrent
/// writes do not interleave (each writer ends with a CRC over its own
/// snapshot of the header).
#[inline]
pub unsafe fn read_metadata_direct(base_ptr: *const u8, record_offset: u64) -> Result<TxMetadata> {
    unsafe {
        // R-029 (BC-06): Acquire fence BEFORE the read so the
        // CPU's load buffer is drained. On AArch64 this emits a
        // `dmb ishld` (data memory barrier, load-only, inner
        // shareable). Combined with the writer's Release fence
        // (R-030), it prevents the CPU from observing the new
        // CRC bytes paired with old field bytes (or vice versa)
        // — the torn read the CRC check is supposed to catch.
        // Without the fence, ARM's relaxed ordering can let
        // independent loads complete out of program order, so a
        // reader on a different core may see the four CRC bytes
        // from the writer's most recent memcpy paired with header
        // bytes from a previous one. The CRC validates correctly
        // against the new bytes' CRC slot but the actual header
        // payload is stale — silent corruption with a passing
        // checksum. The fence is hardware-cheap (a single dmb)
        // and the extra barrier is dwarfed by the metadata read
        // itself. Note: Rust's strict memory model says fences
        // alone don't establish happens-before without a paired
        // atomic load/store; in practice the AArch64 hardware
        // barrier prevents the reorderings we care about, and
        // the CRC remains the true safety net.
        std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);
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

/// Write [`TxMetadata`] directly to a memory-mapped device region.
///
/// Zero-copy serialization: writes the metadata bytes directly to the
/// target address. No `AlignedBuf`, no read-modify-write.
///
/// # Safety
///
/// `base_ptr` must be valid for at least `record_offset + METADATA_SIZE` bytes.
/// Caller must hold the per-transaction stripe lock.
#[inline]
pub unsafe fn write_metadata_direct(base_ptr: *mut u8, record_offset: u64, metadata: &TxMetadata) {
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
        // can be observed by another core. On AArch64 this emits
        // a `dmb ishst` (data memory barrier, store-only, inner
        // shareable). Pairs with the reader's Acquire fence
        // (R-029); together they prevent a reader on a different
        // core from seeing the new CRC bytes alongside stale
        // header bytes. Without this fence, ARM's relaxed store
        // ordering allows the four CRC bytes (which `to_bytes`
        // computes and writes inside `buf`, then the bulk
        // copy_from_slice transfers to the destination) to land
        // visibly before the rest of the buffer — a concurrent
        // reader would compute a CRC over old header bytes plus
        // the new CRC slot, see a mismatch, and surface
        // RecordCorruption (which is the protective behaviour we
        // already rely on); but in the WORST case for a same-CRC
        // collision the reader sees a stale header that validates
        // against the stale CRC. The fence eliminates the
        // reordering window entirely. The stripe-lock contract on
        // writes (held by callers) prevents two writers from
        // interleaving; the fence covers the writer-vs-reader case.
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
/// # Concurrency contract (R-009 / BC-02)
///
/// Like [`read_metadata_direct`], read-paths do not hold the stripe lock.
/// UTXO slots carry a per-slot CRC, so a torn read returns
/// `DeviceError::RecordCorruption` instead of exposing unchecked slot
/// bytes to spend/recovery logic.
#[inline]
pub unsafe fn read_utxo_slot_direct(
    base_ptr: *const u8,
    record_offset: u64,
    slot_index: u32,
) -> Result<UtxoSlot> {
    unsafe {
        // R-029 (BC-06): Acquire fence — see `read_metadata_direct`
        // for the full rationale. Slot reads have the same
        // memory-ordering risk as metadata reads on AArch64.
        std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);
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
#[inline]
pub unsafe fn write_utxo_slot_direct(
    base_ptr: *mut u8,
    record_offset: u64,
    slot_index: u32,
    slot: &UtxoSlot,
) {
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
}
