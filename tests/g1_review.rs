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
use teraslab::io::{read_metadata, write_metadata};
use teraslab::locks::StripedLocks;
use teraslab::record::TxMetadata;

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
    let near_max = u64::MAX & !4095u64;
    match dev.pread(&mut buf, near_max) {
        Err(DeviceError::OutOfBounds { offset, .. }) => {
            assert_eq!(offset, near_max);
        }
        other => panic!("expected OutOfBounds, got {other:?}"),
    }
}

/// F-G1-007 regression: same for `MemoryDevice::pwrite`.
#[test]
fn memory_device_pwrite_rejects_offset_buf_overflow() {
    let dev = MemoryDevice::new(8192, 4096).unwrap();
    let buf = AlignedBuf::new(4096, 4096);
    let near_max = u64::MAX & !4095u64;
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
