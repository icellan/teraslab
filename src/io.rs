//! Read/write helpers for TeraSlab records on block devices.
//!
//! These functions handle the read-modify-write pattern needed for sub-block
//! writes with `O_DIRECT`. When the data to write is smaller than the device's
//! minimum I/O size, the containing aligned block is read, modified in memory,
//! and written back.

use crate::device::{AlignedBuf, BlockDevice, DeviceError};
use crate::record::{TxMetadata, UtxoSlot, METADATA_SIZE, UTXO_SLOT_SIZE};

/// Result type for I/O helper operations.
pub type Result<T> = std::result::Result<T, DeviceError>;

/// Read the [`TxMetadata`] header of a record at `record_offset`.
///
/// Reads the first `METADATA_SIZE` bytes from the device at the given
/// record offset. The read is rounded up to the device alignment.
pub fn read_metadata(
    device: &dyn BlockDevice,
    record_offset: u64,
) -> Result<TxMetadata> {
    let align = device.alignment();
    let read_size = align_up(METADATA_SIZE, align);
    let mut buf = AlignedBuf::new(read_size, align);

    // Record offset must be aligned (allocator guarantees this).
    let aligned_base = record_offset / align as u64 * align as u64;
    let intra_offset = (record_offset - aligned_base) as usize;

    let total_read = align_up(intra_offset + METADATA_SIZE, align);
    let mut read_buf = AlignedBuf::new(total_read, align);
    device.pread(&mut read_buf, aligned_base)?;

    buf[..METADATA_SIZE]
        .copy_from_slice(&read_buf[intra_offset..intra_offset + METADATA_SIZE]);

    Ok(TxMetadata::from_bytes(&buf[..METADATA_SIZE]))
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
        device.pread(&mut buf, aligned_base)?;
    }

    let mut meta_bytes = [0u8; METADATA_SIZE];
    metadata.to_bytes(&mut meta_bytes);
    buf[intra_offset..intra_offset + METADATA_SIZE]
        .copy_from_slice(&meta_bytes);

    device.pwrite(&buf, aligned_base)?;
    Ok(())
}

/// Read a single [`UtxoSlot`] at `slot_index` within the record at `record_offset`.
///
/// The slot offset is: `record_offset + METADATA_SIZE + slot_index * 69`.
pub fn read_utxo_slot(
    device: &dyn BlockDevice,
    record_offset: u64,
    slot_index: u32,
) -> Result<UtxoSlot> {
    let align = device.alignment();
    let slot_offset =
        record_offset + TxMetadata::utxo_slot_offset(slot_index);
    let aligned_base = slot_offset / align as u64 * align as u64;
    let intra_offset = (slot_offset - aligned_base) as usize;
    let total_read = align_up(intra_offset + UTXO_SLOT_SIZE, align);

    let mut buf = AlignedBuf::new(total_read, align);
    device.pread(&mut buf, aligned_base)?;

    Ok(UtxoSlot::from_bytes(
        &buf[intra_offset..intra_offset + UTXO_SLOT_SIZE],
    ))
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
    let slot_offset =
        record_offset + TxMetadata::utxo_slot_offset(slot_index);
    let aligned_base = slot_offset / align as u64 * align as u64;
    let intra_offset = (slot_offset - aligned_base) as usize;
    let total_size = align_up(intra_offset + UTXO_SLOT_SIZE, align);

    let mut buf = AlignedBuf::new(total_size, align);
    // Read-modify-write: 69 bytes is always less than a 4096 block.
    device.pread(&mut buf, aligned_base)?;

    let mut slot_bytes = [0u8; UTXO_SLOT_SIZE];
    slot.to_bytes(&mut slot_bytes);
    buf[intra_offset..intra_offset + UTXO_SLOT_SIZE]
        .copy_from_slice(&slot_bytes);

    device.pwrite(&buf, aligned_base)?;
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

    device.pwrite(&buf, record_offset)?;
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
            assert_eq!(
                actual, *expected,
                "slot {i} mismatch"
            );
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
        let new_slot = UtxoSlot::new_spent([0x05, 0x00, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0], sd);

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
        use crate::allocator::{SlotAllocator, DATA_REGION_OFFSET};

        // 1. Create allocator on MemoryDevice (64 MB to fit 1000-slot records)
        let dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(dev.clone());

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
        write_full_record(&*dev, new_offset, &new_meta, &new_slots)
            .unwrap();

        let check_meta = read_metadata(&*dev, new_offset).unwrap();
        assert_eq!({ check_meta.utxo_count }, 50);
        assert_ne!(check_meta.tx_id, [0xBEu8; 32]); // Old txid is gone
    }
}
