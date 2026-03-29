//! Input reference storage for large transactions.
//!
//! Stores compact outpoint references (prev_txid + prev_vout) on NVMe
//! for fast spend validation without reading the full cold data blob.

use crate::device::{AlignedBuf, BlockDevice, DeviceError};

/// Size of a single input reference in bytes.
pub const INPUT_REF_SIZE: usize = 36;

/// A compact outpoint reference for fast spend validation.
///
/// Contains only the previous transaction ID and output index — the
/// minimum needed to verify that a spend references a valid UTXO.
#[repr(C, packed)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct InputRef {
    /// Transaction ID of the referenced output.
    pub prev_txid: [u8; 32],
    /// Output index within the referenced transaction.
    pub prev_vout: u32,
}

const _: () = assert!(std::mem::size_of::<InputRef>() == INPUT_REF_SIZE);

impl InputRef {
    /// Serialize this input ref to a byte slice.
    ///
    /// The destination must be at least `INPUT_REF_SIZE` bytes.
    pub fn to_bytes(&self, dst: &mut [u8]) {
        debug_assert!(dst.len() >= INPUT_REF_SIZE);
        dst[..32].copy_from_slice(&self.prev_txid);
        dst[32..36].copy_from_slice(&{ self.prev_vout }.to_le_bytes());
    }

    /// Deserialize an input ref from a byte slice.
    ///
    /// The source must be at least `INPUT_REF_SIZE` bytes.
    pub fn from_bytes(src: &[u8]) -> Self {
        debug_assert!(src.len() >= INPUT_REF_SIZE);
        let mut prev_txid = [0u8; 32];
        prev_txid.copy_from_slice(&src[..32]);
        let prev_vout = u32::from_le_bytes(src[32..36].try_into().unwrap());
        Self { prev_txid, prev_vout }
    }
}

impl std::fmt::Debug for InputRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InputRef")
            .field("prev_txid", &format_args!("{:02x}{:02x}...", self.prev_txid[0], self.prev_txid[1]))
            .field("prev_vout", &{ self.prev_vout })
            .finish()
    }
}

/// Write input references to a device at the given offset.
///
/// Uses alignment-aware read-modify-write when the data doesn't
/// fall on device alignment boundaries.
pub fn write_input_refs(
    device: &dyn BlockDevice,
    offset: u64,
    refs: &[InputRef],
) -> Result<(), DeviceError> {
    if refs.is_empty() {
        return Ok(());
    }

    let align = device.alignment();
    let data_size = refs.len() * INPUT_REF_SIZE;
    let aligned_base = offset / align as u64 * align as u64;
    let intra = (offset - aligned_base) as usize;
    let total = (intra + data_size).div_ceil(align) * align;

    let mut buf = AlignedBuf::new(total, align);

    // Read-modify-write if not aligned
    if intra > 0 || !data_size.is_multiple_of(align) {
        let _ = device.pread(&mut buf, aligned_base);
    }

    for (i, r) in refs.iter().enumerate() {
        let pos = intra + i * INPUT_REF_SIZE;
        r.to_bytes(&mut buf[pos..pos + INPUT_REF_SIZE]);
    }

    device.pwrite(&buf, aligned_base)?;
    Ok(())
}

/// Read input references from a device at the given offset.
///
/// Returns a vector of `count` input references.
pub fn read_input_refs(
    device: &dyn BlockDevice,
    offset: u64,
    count: u32,
) -> Result<Vec<InputRef>, DeviceError> {
    if count == 0 {
        return Ok(Vec::new());
    }

    let align = device.alignment();
    let data_size = count as usize * INPUT_REF_SIZE;
    let aligned_base = offset / align as u64 * align as u64;
    let intra = (offset - aligned_base) as usize;
    let total = (intra + data_size).div_ceil(align) * align;

    let mut buf = AlignedBuf::new(total, align);
    device.pread(&mut buf, aligned_base)?;

    let mut result = Vec::with_capacity(count as usize);
    for i in 0..count as usize {
        let pos = intra + i * INPUT_REF_SIZE;
        result.push(InputRef::from_bytes(&buf[pos..pos + INPUT_REF_SIZE]));
    }

    Ok(result)
}

/// Read a single input reference by index.
///
/// More efficient than reading all refs when only one is needed.
pub fn read_input_ref_at(
    device: &dyn BlockDevice,
    base_offset: u64,
    index: u32,
) -> Result<InputRef, DeviceError> {
    let offset = base_offset + index as u64 * INPUT_REF_SIZE as u64;
    let align = device.alignment();
    let aligned_base = offset / align as u64 * align as u64;
    let intra = (offset - aligned_base) as usize;
    let total = (intra + INPUT_REF_SIZE).div_ceil(align) * align;

    let mut buf = AlignedBuf::new(total, align);
    device.pread(&mut buf, aligned_base)?;

    Ok(InputRef::from_bytes(&buf[intra..intra + INPUT_REF_SIZE]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::{SlotAllocator, DATA_REGION_OFFSET};
    use crate::device::MemoryDevice;
    use std::sync::Arc;

    fn test_device() -> Arc<MemoryDevice> {
        Arc::new(MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap())
    }

    fn make_ref(n: u8, vout: u32) -> InputRef {
        let mut prev_txid = [0u8; 32];
        prev_txid[0] = n;
        prev_txid[31] = n.wrapping_mul(7);
        InputRef { prev_txid, prev_vout: vout }
    }

    #[test]
    fn input_ref_size() {
        assert_eq!(std::mem::size_of::<InputRef>(), 36);
    }

    #[test]
    fn input_ref_round_trip() {
        let r = make_ref(0xAB, 42);
        let mut buf = [0u8; INPUT_REF_SIZE];
        r.to_bytes(&mut buf);
        let restored = InputRef::from_bytes(&buf);
        assert_eq!(restored, r);
    }

    #[test]
    fn write_100_read_all() {
        let dev = test_device();
        let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
        let size = 100 * INPUT_REF_SIZE as u64;
        let offset = alloc.allocate(size).unwrap();

        let refs: Vec<InputRef> = (0..100u8)
            .map(|i| make_ref(i, i as u32 * 10))
            .collect();

        write_input_refs(&*dev, offset, &refs).unwrap();
        let read = read_input_refs(&*dev, offset, 100).unwrap();
        assert_eq!(read.len(), 100);
        for (i, (original, restored)) in refs.iter().zip(read.iter()).enumerate() {
            assert_eq!(original, restored, "mismatch at index {i}");
        }
    }

    #[test]
    fn read_individual_by_index() {
        let dev = test_device();
        let offset = DATA_REGION_OFFSET;

        let refs: Vec<InputRef> = (0..10u8)
            .map(|i| make_ref(i, i as u32 + 100))
            .collect();

        write_input_refs(&*dev, offset, &refs).unwrap();

        // Read each individually
        for (i, expected) in refs.iter().enumerate() {
            let actual = read_input_ref_at(&*dev, offset, i as u32).unwrap();
            assert_eq!(actual, *expected, "mismatch at index {i}");
        }
    }

    #[test]
    fn input_refs_independent_of_cold_data() {
        let dev = test_device();
        let mut alloc = SlotAllocator::new(dev.clone()).unwrap();

        // Allocate space for cold data (simulating a record region)
        let cold_offset = alloc.allocate(4096).unwrap();
        // Allocate separate space for input refs
        let refs_offset = alloc.allocate(10 * INPUT_REF_SIZE as u64).unwrap();

        // Write cold data (simulated)
        let mut cold_buf = AlignedBuf::new(4096, 4096);
        cold_buf[..4].copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        dev.pwrite(&cold_buf, cold_offset).unwrap();

        // Write input refs
        let refs: Vec<InputRef> = (0..10u8).map(|i| make_ref(i, i as u32)).collect();
        write_input_refs(&*dev, refs_offset, &refs).unwrap();

        // Overwrite cold data region
        cold_buf[..4].copy_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]);
        dev.pwrite(&cold_buf, cold_offset).unwrap();

        // Input refs should be unaffected
        let read = read_input_refs(&*dev, refs_offset, 10).unwrap();
        for (i, (original, restored)) in refs.iter().zip(read.iter()).enumerate() {
            assert_eq!(original, restored, "input ref {i} corrupted after cold data overwrite");
        }
    }

    #[test]
    fn empty_refs() {
        let dev = test_device();
        let result = write_input_refs(&*dev, DATA_REGION_OFFSET, &[]);
        assert!(result.is_ok());
        let read = read_input_refs(&*dev, DATA_REGION_OFFSET, 0).unwrap();
        assert!(read.is_empty());
    }
}
