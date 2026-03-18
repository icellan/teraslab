//! Tiered storage manager — coordinates inline, separate NVMe, and
//! external blob store tiers.

use crate::device::{AlignedBuf, BlockDevice};
use crate::record::{METADATA_SIZE, TxMetadata, UTXO_SLOT_SIZE};
use crate::storage::blobstore::BlobStore;
use crate::storage::tiers::*;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("device error: {0}")]
    Device(#[from] crate::device::DeviceError),
    #[error("blob error: {0}")]
    Blob(#[from] crate::storage::blobstore::BlobError),
    #[error("allocator error: {0}")]
    Allocator(#[from] crate::allocator::AllocatorError),
    #[error("invalid cold data")]
    InvalidColdData,
}

pub type Result<T> = std::result::Result<T, StorageError>;

/// Manages tiered storage for cold data (inputs, outputs, inpoints).
pub struct StorageManager {
    device: Arc<dyn BlockDevice>,
    allocator: parking_lot::Mutex<crate::allocator::SlotAllocator>,
    blob_store: Arc<dyn BlobStore>,
    /// Configurable in future; currently uses module-level constants.
    #[expect(dead_code)]
    inline_threshold: usize,
    #[expect(dead_code)]
    separate_threshold: usize,
}

impl StorageManager {
    /// Create a new storage manager.
    pub fn new(
        device: Arc<dyn BlockDevice>,
        allocator: crate::allocator::SlotAllocator,
        blob_store: Arc<dyn BlobStore>,
    ) -> Self {
        Self {
            device,
            allocator: parking_lot::Mutex::new(allocator),
            blob_store,
            inline_threshold: INLINE_THRESHOLD,
            separate_threshold: SEPARATE_THRESHOLD,
        }
    }

    /// Determine which tier to use for the given cold data size.
    pub fn tier_for_size(&self, data_size: usize) -> StorageTier {
        tier_for_size(data_size)
    }

    /// Compute the deterministic inline cold data offset for a record.
    pub fn inline_cold_offset(utxo_count: u32) -> u64 {
        METADATA_SIZE as u64 + utxo_count as u64 * UTXO_SLOT_SIZE as u64
    }

    /// Write cold data for a record. Returns where the data was placed.
    ///
    /// For inline tier: the caller must have already allocated space for
    /// the cold data at the end of the record. This writes it there.
    ///
    /// For separate NVMe: allocates a new device block and writes there.
    ///
    /// For external: writes to the blob store synchronously (for simplicity;
    /// async upload is a future optimization).
    pub fn write_cold_data(
        &self,
        tx_id: &[u8; 32],
        cold: &ColdData,
        utxo_count: u32,
        record_offset: u64,
    ) -> Result<ColdDataRef> {
        if cold.is_empty() {
            return Ok(ColdDataRef::None);
        }

        let serialized = cold.serialize();
        let data_size = serialized.len();
        let tier = self.tier_for_size(data_size);

        match tier {
            StorageTier::Inline => {
                let cold_offset = record_offset + Self::inline_cold_offset(utxo_count);
                self.write_aligned(&serialized, cold_offset)?;
                Ok(ColdDataRef::Inline { cold_size: data_size as u32 })
            }
            StorageTier::SeparateNvme => {
                let device_offset = self.allocator.lock().allocate(data_size as u64)?;
                self.write_aligned(&serialized, device_offset)?;
                Ok(ColdDataRef::SeparateNvme {
                    device_offset,
                    cold_size: data_size as u32,
                })
            }
            StorageTier::External => {
                self.blob_store.put(tx_id, &serialized)?;
                Ok(ColdDataRef::External)
            }
        }
    }

    /// Read cold data for a record.
    pub fn read_cold_data(
        &self,
        record_offset: u64,
        utxo_count: u32,
        metadata: &TxMetadata,
    ) -> Result<ColdData> {
        let flags = metadata.flags;

        // External tier
        if flags.contains(crate::record::TxFlags::EXTERNAL) {
            let data = self.blob_store.get(&metadata.tx_id)?;
            match data {
                Some(bytes) => ColdData::deserialize(&bytes)
                    .ok_or(StorageError::InvalidColdData),
                None => Ok(ColdData {
                    inputs: vec![],
                    outputs: vec![],
                    inpoints: vec![],
                }),
            }
        } else {
            // Inline tier: cold data at deterministic offset
            let cold_offset = record_offset + Self::inline_cold_offset(utxo_count);
            let record_size = { metadata.record_size } as u64;
            let cold_end = record_offset + record_size;
            let cold_size = cold_end.saturating_sub(cold_offset);

            if cold_size == 0 {
                return Ok(ColdData { inputs: vec![], outputs: vec![], inpoints: vec![] });
            }

            let bytes = self.read_aligned(cold_offset, cold_size as usize)?;
            ColdData::deserialize(&bytes).ok_or(StorageError::InvalidColdData)
        }
    }

    /// Delete cold data when a record is pruned.
    pub fn delete_cold_data(
        &self,
        metadata: &TxMetadata,
    ) -> Result<()> {
        if metadata.flags.contains(crate::record::TxFlags::EXTERNAL) {
            self.blob_store.delete(&metadata.tx_id)?;
        }
        // Inline and separate NVMe: freed with the record allocation.
        Ok(())
    }

    fn write_aligned(&self, data: &[u8], offset: u64) -> Result<()> {
        let align = self.device.alignment();
        let aligned_base = offset / align as u64 * align as u64;
        let intra = (offset - aligned_base) as usize;
        let total = (intra + data.len()).div_ceil(align) * align;

        let mut buf = AlignedBuf::new(total, align);
        if intra > 0 || !data.len().is_multiple_of(align) {
            let _ = self.device.pread(&mut buf, aligned_base);
        }
        buf[intra..intra + data.len()].copy_from_slice(data);
        self.device.pwrite(&buf, aligned_base)?;
        Ok(())
    }

    fn read_aligned(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let align = self.device.alignment();
        let aligned_base = offset / align as u64 * align as u64;
        let intra = (offset - aligned_base) as usize;
        let total = (intra + len).div_ceil(align) * align;

        let mut buf = AlignedBuf::new(total, align);
        self.device.pread(&mut buf, aligned_base)?;
        Ok(buf[intra..intra + len].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::SlotAllocator;
    use crate::device::MemoryDevice;
    use crate::io;
    use crate::record::{TxFlags, TxMetadata, UtxoSlot};
    use crate::storage::blobstore::MemoryBlobStore;

    fn setup() -> (Arc<MemoryDevice>, StorageManager) {
        let dev = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone());
        let blob = Arc::new(MemoryBlobStore::new());
        let mgr = StorageManager::new(dev.clone(), alloc, blob);
        (dev, mgr)
    }

    fn write_test_record(dev: &dyn BlockDevice, offset: u64, utxo_count: u32, flags: TxFlags) -> TxMetadata {
        let mut meta = TxMetadata::new(utxo_count);
        meta.tx_id[0] = 0xAA;
        meta.flags = flags;
        let slots: Vec<UtxoSlot> = (0..utxo_count)
            .map(|i| { let mut h = [0u8; 32]; h[0] = i as u8; UtxoSlot::new_unspent(h) })
            .collect();
        io::write_full_record(dev, offset, &meta, &slots).unwrap();
        meta
    }

    #[test]
    fn inline_cold_data_write_read() {
        let (dev, mgr) = setup();

        let utxo_count = 5u32;
        let cold = ColdData {
            inputs: vec![1, 2, 3, 4],
            outputs: vec![0xA, 0xB, 0xC],
            inpoints: vec![0xD],
        };
        let cold_size = cold.serialized_size();

        // Allocate record + cold data together
        let total = TxMetadata::record_size_for(utxo_count) + cold_size as u64;
        let offset = mgr.allocator.lock().allocate(total).unwrap();

        let mut meta = TxMetadata::new(utxo_count);
        meta.record_size = total as u32;
        meta.tx_id[0] = 0x01;
        let slots: Vec<UtxoSlot> = (0..utxo_count)
            .map(|_| UtxoSlot::new_unspent([0; 32]))
            .collect();
        io::write_full_record(&*dev, offset, &meta, &slots).unwrap();

        // Write cold data
        let result = mgr.write_cold_data(&meta.tx_id, &cold, utxo_count, offset).unwrap();
        assert!(matches!(result, ColdDataRef::Inline { .. }));

        // Read back
        let read = mgr.read_cold_data(offset, utxo_count, &meta).unwrap();
        assert_eq!(read, cold);
    }

    #[test]
    fn inline_cold_offset_deterministic() {
        assert_eq!(
            StorageManager::inline_cold_offset(10),
            METADATA_SIZE as u64 + 10 * UTXO_SLOT_SIZE as u64,
        );
    }

    #[test]
    fn spend_does_not_corrupt_cold_data() {
        let (dev, mgr) = setup();
        let utxo_count = 5u32;
        let cold = ColdData {
            inputs: vec![0xDE, 0xAD],
            outputs: vec![0xBE, 0xEF],
            inpoints: vec![],
        };
        let total = TxMetadata::record_size_for(utxo_count) + cold.serialized_size() as u64;
        let offset = mgr.allocator.lock().allocate(total).unwrap();

        let mut meta = TxMetadata::new(utxo_count);
        meta.record_size = total as u32;
        meta.tx_id[0] = 0x02;
        let slots: Vec<UtxoSlot> = (0..utxo_count)
            .map(|i| { let mut h = [0u8; 32]; h[0] = i as u8; UtxoSlot::new_unspent(h) })
            .collect();
        io::write_full_record(&*dev, offset, &meta, &slots).unwrap();
        mgr.write_cold_data(&meta.tx_id, &cold, utxo_count, offset).unwrap();

        // Simulate a spend (write to slot 2)
        let mut sd = [0u8; 36]; sd[0] = 0xFF;
        let spent = UtxoSlot::new_spent(slots[2].hash, sd);
        io::write_utxo_slot(&*dev, offset, 2, &spent).unwrap();

        // Cold data unchanged
        let read = mgr.read_cold_data(offset, utxo_count, &meta).unwrap();
        assert_eq!(read, cold);
    }

    #[test]
    fn external_cold_data_write_read() {
        let (dev, mgr) = setup();
        let utxo_count = 3u32;
        let record_size = TxMetadata::record_size_for(utxo_count);
        let offset = mgr.allocator.lock().allocate(record_size).unwrap();

        let mut meta = write_test_record(&*dev, offset, utxo_count, TxFlags::EXTERNAL);
        meta.record_size = record_size as u32;

        let cold = ColdData {
            inputs: vec![0x42; 2 * 1024 * 1024], // 2 MB → external
            outputs: vec![0x43; 100],
            inpoints: vec![],
        };

        let result = mgr.write_cold_data(&meta.tx_id, &cold, utxo_count, offset).unwrap();
        assert_eq!(result, ColdDataRef::External);

        // Read back via blob store
        let read = mgr.read_cold_data(offset, utxo_count, &meta).unwrap();
        assert_eq!(read, cold);
    }

    #[test]
    fn external_delete() {
        let (_dev, mgr) = setup();
        let mut meta = TxMetadata::new(1);
        meta.flags = TxFlags::EXTERNAL;
        meta.tx_id[0] = 0x77;

        let cold = ColdData {
            inputs: vec![0; 2 * 1024 * 1024],
            outputs: vec![],
            inpoints: vec![],
        };
        mgr.write_cold_data(&meta.tx_id, &cold, 1, 0).unwrap();

        mgr.delete_cold_data(&meta).unwrap();
        assert!(!mgr.blob_store.exists(&meta.tx_id).unwrap());
    }

    #[test]
    fn no_cold_data_returns_none() {
        let (dev, mgr) = setup();
        let utxo_count = 3u32;
        let record_size = TxMetadata::record_size_for(utxo_count);
        let offset = mgr.allocator.lock().allocate(record_size).unwrap();

        let meta = write_test_record(&*dev, offset, utxo_count, TxFlags::empty());
        // record_size exactly fits metadata + slots, no cold data
        let read = mgr.read_cold_data(offset, utxo_count, &meta).unwrap();
        assert!(read.is_empty());
    }

    #[test]
    fn empty_cold_data() {
        let (_dev, mgr) = setup();
        let cold = ColdData { inputs: vec![], outputs: vec![], inpoints: vec![] };
        let result = mgr.write_cold_data(&[0; 32], &cold, 1, 0).unwrap();
        assert_eq!(result, ColdDataRef::None);
    }
}
