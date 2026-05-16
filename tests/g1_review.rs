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
