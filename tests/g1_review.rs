//! Regression tests for G1 (core data plane) review findings.
//!
//! Each test names the finding it pins (F-G1-NNN). When a fix lands,
//! the corresponding test should already be failing on the unfixed
//! baseline (where applicable — some are forward-looking tests of
//! contract invariants that the fix makes provable).
//!
//! Kept as an integration test (rather than inline `#[cfg(test)] mod
//! tests`) because the in-crate `src/index/redb_primary.rs` test module
//! has pre-existing compile errors that block the lib-test build; G1's
//! ownership scope does not include that file.

use teraslab::device::{AlignedBuf, BlockDevice, DeviceError, MemoryDevice};
use teraslab::index::TxKey;
use teraslab::io::{
    read_metadata, read_metadata_direct, write_full_record, write_metadata,
    write_mutation_footer_and_crc_direct, write_mutation_footer_direct,
};
use teraslab::locks::StripedLocks;
use teraslab::record::{GENERATION_ORDER_WINDOW, TxMetadata, UtxoSlot, generation_target_ahead};

/// F-G1-007 regression: `MemoryDevice::pread` with an `offset` near
/// `usize::MAX` plus a non-trivial buffer length must return
/// `OutOfBounds` rather than silently wrapping and passing the
/// bounds check. Before the fix the bare `off + buf.len() > data.len()`
/// expression could overflow and produce a small `end` that satisfied
/// the comparison.
#[test]
fn memory_device_pread_rejects_offset_buf_overflow() {
    let dev = MemoryDevice::new(8192, 4096).unwrap();
    let mut buf = AlignedBuf::new(4096, 4096);
    // Largest aligned offset is `u64::MAX & !4095` — the alignment
    // check passes, so the bounds-check path is the one we exercise.
    let near_max = !4095u64;
    match dev.pread(&mut buf, near_max) {
        Err(DeviceError::OutOfBounds { offset, .. }) => {
            assert_eq!(offset, near_max);
        }
        other => panic!("expected OutOfBounds, got {other:?}"),
    }
}

/// F-G1-002 regression: the combined
/// `write_mutation_footer_and_crc_direct` wrapper must leave the
/// header in a CRC-validating state — a subsequent
/// `read_metadata_direct` succeeds rather than returning
/// `RecordCorruption`. The "primitive footer write WITHOUT CRC
/// restamp" path leaves the header in a CRC-invalid state if the
/// caller forgets to follow up; pinning the combined helper's
/// behaviour ensures the new safe entrypoint cannot regress.
#[test]
fn write_mutation_footer_and_crc_round_trips_through_direct_read() {
    let dev = MemoryDevice::new(64 * 1024, 4096).unwrap();

    // Seed the device with a valid record so the header has a
    // baseline CRC the read path can validate against later.
    let mut meta = TxMetadata::new(4);
    meta.tx_id = [0xA0u8; 32];
    meta.fee = 1;
    meta.generation = 1;
    let slots: Vec<UtxoSlot> = (0..4).map(|i| UtxoSlot::new_unspent([i; 32])).collect();
    write_full_record(&dev, 0, &meta, &slots).unwrap();

    let raw_ptr = dev.as_raw_ptr().expect("MemoryDevice exposes raw_ptr");

    // Bump generation + updated_at + clear delete_at_height and use the
    // combined wrapper to write the footer + restamp the CRC.
    meta.generation = 2;
    meta.updated_at = 42;
    meta.delete_at_height = 0;
    unsafe {
        write_mutation_footer_and_crc_direct(raw_ptr, 0, &meta);
    }

    // Direct read must succeed: the CRC-validating read path returns
    // the new metadata, not RecordCorruption.
    let read_back = unsafe { read_metadata_direct(raw_ptr as *const u8, 0) }
        .expect("CRC must validate after combined wrapper");
    assert_eq!({ read_back.generation }, 2);
    assert_eq!({ read_back.updated_at }, 42);
}

/// F-G1-002 regression: the *primitive* footer write WITHOUT the CRC
/// finalizer leaves the header in a CRC-invalid state — a subsequent
/// `read_metadata_direct` MUST return `RecordCorruption`. This pins
/// the failure mode the combined wrapper was introduced to prevent
/// by omission, and documents why callers must use the
/// `_and_crc_direct` variant unless they explicitly know they want to
/// batch several footer writes before stamping the CRC.
#[test]
fn primitive_footer_write_without_crc_surfaces_record_corruption() {
    let dev = MemoryDevice::new(64 * 1024, 4096).unwrap();

    let mut meta = TxMetadata::new(4);
    meta.tx_id = [0xB0u8; 32];
    meta.fee = 1;
    meta.generation = 1;
    let slots: Vec<UtxoSlot> = (0..4).map(|i| UtxoSlot::new_unspent([i; 32])).collect();
    write_full_record(&dev, 0, &meta, &slots).unwrap();

    let raw_ptr = dev.as_raw_ptr().expect("MemoryDevice exposes raw_ptr");

    // Mutate generation/updated_at via the primitive — DO NOT stamp
    // the CRC. The on-disk header bytes change but the CRC slot still
    // matches the OLD bytes, so the next read must fail loudly.
    meta.generation = 2;
    meta.updated_at = 99;
    unsafe {
        write_mutation_footer_direct(raw_ptr, 0, &meta);
    }

    match unsafe { read_metadata_direct(raw_ptr as *const u8, 0) } {
        Err(DeviceError::RecordCorruption { .. }) => {}
        other => {
            panic!("primitive footer write without CRC must yield RecordCorruption, got {other:?}")
        }
    }
}

// F-G1-009 regression: the duplicate `persist_rejects_freelist_overflow_via_integration_path`
// test that lived here was a near-exact copy of the lib test at
// `src/allocator.rs::tests::persist_rejects_freelist_overflow`. Both used the same
// `__test_force_push_free_region` helper to seed 65 537 regions, so both paid the
// same ~17 s of O(n²) `Vec::insert` + `debug_assert_sorted` cost in debug. The lib
// test has the closer assertion shape and exercises the same overflow branch, so
// the duplicate is removed here. See `_review/08_test_perf_audit.md` (smell #1)
// and `_review/09_perf_fixes.md` for context.

/// F-G1-019 regression: `generation_target_ahead` must be symmetrically
/// classified as not-ahead when the two generations are exactly
/// `GENERATION_ORDER_WINDOW` (2^31) apart in either direction. Existing
/// in-crate tests pin one direction (`generation_target_ahead(0, 1<<31)`);
/// this fills in the converse (`generation_target_ahead(1<<31, 0)`) so the
/// ambiguity-handling contract is locked from both sides.
///
/// The constant doc warns that retaining more than `2^31 - 1` outstanding
/// mutations on a single record makes wrap classification ambiguous. The
/// test does not assert anything about the warning path (no metric is
/// wired yet — flagged as a future observability follow-up in the review);
/// it pins the function's classification only.
#[test]
fn generation_symmetric_ambiguity_at_half_window() {
    let half = GENERATION_ORDER_WINDOW;
    assert!(
        !generation_target_ahead(0, half),
        "delta == 2^31 must be classified as not-ahead (target after local)"
    );
    assert!(
        !generation_target_ahead(half, 0),
        "delta == 2^31 must be classified as not-ahead in the converse direction too"
    );
}

/// F-G1-017 regression: `MemoryDevice::size()` must agree with the
/// underlying Vec's length. Pre-fix the device cached a `raw_len`
/// snapshot at construction; this test exercises the single-
/// source-of-truth path that survives any future `resize` because
/// the field was removed.
#[test]
fn memory_device_size_matches_underlying_storage() {
    let dev = MemoryDevice::new(16 * 1024, 4096).unwrap();
    assert_eq!(dev.size(), 16 * 1024);
}

/// F-G1-007 regression: same for `MemoryDevice::pwrite`.
#[test]
fn memory_device_pwrite_rejects_offset_buf_overflow() {
    let dev = MemoryDevice::new(8192, 4096).unwrap();
    let buf = AlignedBuf::new(4096, 4096);
    let near_max = !4095u64;
    match dev.pwrite(&buf, near_max) {
        Err(DeviceError::OutOfBounds { offset, .. }) => {
            assert_eq!(offset, near_max);
        }
        other => panic!("expected OutOfBounds, got {other:?}"),
    }
}

/// F-G1-018 regression: `StripedLocks::stripe_index` must hash off
/// bytes 16..24 of the txid; replacing the `[0u8; 8] +
/// copy_from_slice` shape with `try_into().expect(...)` is a pure
/// codegen change and must not alter the computed stripe for any
/// txid. Pin a few known inputs against the documented derivation.
#[test]
fn stripe_index_matches_documented_byte_range_post_refactor() {
    let locks = StripedLocks::new(65536);

    // txid with a known little-endian u64 at bytes 16..24.
    let mut txid = [0u8; 32];
    txid[16..24].copy_from_slice(&0x0123_4567_89AB_CDEFu64.to_le_bytes());
    let key = TxKey { txid };
    let idx = locks.stripe_index(&key);
    let expected = (0x0123_4567_89AB_CDEFu64 as usize) & (locks.stripe_count() - 1);
    assert_eq!(
        idx, expected,
        "stripe_index must hash bytes 16..24 as u64-LE & mask"
    );

    // Distinct bytes 0..15 must not affect the stripe (bytes 16..24 are
    // the only input).
    let mut txid2 = [0xFFu8; 32];
    txid2[16..24].copy_from_slice(&0x0123_4567_89AB_CDEFu64.to_le_bytes());
    let key2 = TxKey { txid: txid2 };
    assert_eq!(
        locks.stripe_index(&key2),
        idx,
        "stripe_index must ignore bytes outside 16..24"
    );
}

/// F-G1-008 regression: `read_metadata` must round-trip a header
/// written by `write_metadata` exactly. The block-I/O path previously
/// allocated a second `AlignedBuf` of size `align_up(METADATA_SIZE,
/// align)`, copied bytes into it, and deserialized from there — one
/// redundant heap alloc + memcpy. Post-fix the deserialize reads
/// directly out of the aligned device buffer.
#[test]
fn read_metadata_block_path_round_trips_header_after_alloc_dedup() {
    let dev = MemoryDevice::new(64 * 1024, 4096).unwrap();
    let mut meta = TxMetadata::new(8);
    meta.tx_id = [0x5Au8; 32];
    meta.fee = 12345;
    meta.locktime = 700_001;
    write_metadata(&dev, 0, &meta).unwrap();

    let read_back = read_metadata(&dev, 0).unwrap();
    assert_eq!(read_back, meta);
    assert_eq!({ read_back.utxo_count }, 8);
    assert_eq!({ read_back.fee }, 12345);
}
