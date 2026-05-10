//! Tiered storage manager — coordinates inline, separate NVMe, and
//! external blob store tiers.

use crate::device::{AlignedBuf, BlockDevice};
use crate::record::{METADATA_SIZE, TxFlags, TxMetadata, UTXO_SLOT_SIZE};
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
///
/// Coordinates the three tiers:
/// - **Inline**: cold data appended to record at `METADATA_SIZE + utxo_count * 69`
/// - **Separate NVMe**: cold data in a separate device allocation
/// - **External**: cold data in an external blob store (file or S3)
pub struct StorageManager {
    device: Arc<dyn BlockDevice>,
    allocator: parking_lot::Mutex<crate::allocator::SlotAllocator>,
    blob_store: Arc<dyn BlobStore>,
    /// Configurable inline threshold (defaults to `INLINE_THRESHOLD`).
    inline_threshold: usize,
    /// Configurable separate threshold (defaults to `SEPARATE_THRESHOLD`).
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
        if data_size <= self.inline_threshold {
            StorageTier::Inline
        } else if data_size <= self.separate_threshold {
            StorageTier::SeparateNvme
        } else {
            StorageTier::External
        }
    }

    /// Compute the deterministic inline cold data offset for a record.
    ///
    /// Returns the byte offset from the start of the record where inline
    /// cold data begins: `METADATA_SIZE + utxo_count * UTXO_SLOT_SIZE`.
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
    /// For external: writes to the blob store synchronously. For async
    /// upload, use [`BlobUploader`](super::uploader::BlobUploader) instead.
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
                Ok(ColdDataRef::Inline {
                    cold_size: data_size as u32,
                })
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
                // R-048 (AUDIT.md IJK-01): the synchronous external-tier
                // write path used to discard the digest returned by
                // `BlobStore::put`, leaving any caller no way to populate
                // `ExternalRef.content_hash`. With the digest stranded at
                // zero, end-to-end integrity checks on subsequent reads
                // become theatre — a corruption check that compares the
                // recomputed payload SHA-256 against a zero digest would
                // silently pass on bit rot or tampering. We now propagate
                // the manager-returned `BlobDigest` through
                // `ColdDataRef::External { digest }` so callers can stamp
                // the actual SHA-256 and length into the record's
                // `ExternalRef` BEFORE the metadata write.
                let digest = self.blob_store.put(tx_id, &serialized)?;
                Ok(ColdDataRef::External { digest })
            }
        }
    }

    /// Read cold data for a record.
    ///
    /// Determines the tier from metadata flags and record_size:
    /// - `EXTERNAL` flag set → fetches from blob store
    /// - `record_size > METADATA_SIZE + utxo_count * 69` → inline cold data
    /// - Otherwise → no cold data (separate NVMe is read via [`Self::read_cold_data_at`])
    pub fn read_cold_data(
        &self,
        record_offset: u64,
        utxo_count: u32,
        metadata: &TxMetadata,
    ) -> Result<ColdData> {
        let flags = metadata.flags;

        // External tier
        if flags.contains(TxFlags::EXTERNAL) {
            let data = self.blob_store.get(&metadata.tx_id)?;
            match data {
                Some(bytes) => ColdData::deserialize(&bytes).ok_or(StorageError::InvalidColdData),
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
                return Ok(ColdData {
                    inputs: vec![],
                    outputs: vec![],
                    inpoints: vec![],
                });
            }

            let bytes = self.read_aligned(cold_offset, cold_size as usize)?;
            ColdData::deserialize(&bytes).ok_or(StorageError::InvalidColdData)
        }
    }

    /// Read cold data from a specific device offset (for separate NVMe tier).
    ///
    /// The offset and size are typically stored in the index entry's
    /// `cold_offset` and `cold_size` fields.
    pub fn read_cold_data_at(&self, device_offset: u64, size: u32) -> Result<ColdData> {
        if size == 0 {
            return Ok(ColdData {
                inputs: vec![],
                outputs: vec![],
                inpoints: vec![],
            });
        }
        let bytes = self.read_aligned(device_offset, size as usize)?;
        ColdData::deserialize(&bytes).ok_or(StorageError::InvalidColdData)
    }

    /// Stream cold data to a writer (for large external blobs).
    ///
    /// For inline or separate NVMe: reads from device and writes to the writer.
    /// For external: streams directly from the blob store.
    pub fn stream_cold_data(
        &self,
        record_offset: u64,
        utxo_count: u32,
        metadata: &TxMetadata,
        writer: &mut dyn std::io::Write,
    ) -> Result<u64> {
        if metadata.flags.contains(TxFlags::EXTERNAL) {
            let bytes = self.blob_store.stream_to(&metadata.tx_id, writer)?;
            Ok(bytes)
        } else {
            let cold_offset = record_offset + Self::inline_cold_offset(utxo_count);
            let record_size = { metadata.record_size } as u64;
            let cold_end = record_offset + record_size;
            let cold_size = cold_end.saturating_sub(cold_offset);

            if cold_size == 0 {
                return Ok(0);
            }

            let bytes = self.read_aligned(cold_offset, cold_size as usize)?;
            writer
                .write_all(&bytes)
                .map_err(|e| StorageError::Blob(crate::storage::blobstore::BlobError::Io(e)))?;
            Ok(cold_size)
        }
    }

    /// Delete cold data when a record is pruned.
    ///
    /// For inline tier: no-op (freed with the record allocation).
    /// For separate NVMe tier: returns the separate allocation to the freelist.
    /// For external tier: deletes the blob from the blob store.
    ///
    /// # Parameters
    /// - `metadata`: the record's metadata (checked for EXTERNAL flag)
    /// - `separate_cold`: optional (device_offset, size) for separate NVMe cold data
    pub fn delete_cold_data(
        &self,
        metadata: &TxMetadata,
        separate_cold: Option<(u64, u32)>,
    ) -> Result<()> {
        if metadata.flags.contains(TxFlags::EXTERNAL) {
            self.blob_store.delete(&metadata.tx_id)?;
        }
        if let Some((offset, size)) = separate_cold {
            self.allocator.lock().free(offset, size as u64)?;
        }
        // Inline: freed with the record allocation — no-op.
        Ok(())
    }

    /// Read only the inputs portion of cold data (for validation without full read).
    ///
    /// Reads the cold data and returns just the inputs component.
    pub fn read_inputs(
        &self,
        record_offset: u64,
        utxo_count: u32,
        metadata: &TxMetadata,
    ) -> Result<Option<Vec<u8>>> {
        let cold = self.read_cold_data(record_offset, utxo_count, metadata)?;
        if cold.is_empty() {
            Ok(None)
        } else {
            Ok(Some(cold.inputs))
        }
    }

    /// Read a specific output at the given index.
    ///
    /// Reads the full cold data and extracts the requested output.
    /// This is a simplified implementation — for production, the cold data
    /// format would need per-output indexing for O(1) access.
    ///
    /// # Parameters
    /// - `output_index`: zero-based index into the outputs array
    ///
    /// Returns `None` if no cold data exists or the output_index is out of range.
    pub fn read_output_at(
        &self,
        record_offset: u64,
        utxo_count: u32,
        metadata: &TxMetadata,
        output_index: u32,
    ) -> Result<Option<Vec<u8>>> {
        let cold = self.read_cold_data(record_offset, utxo_count, metadata)?;
        if cold.outputs.is_empty() {
            return Ok(None);
        }

        // Parse the outputs as a length-prefixed list of individual outputs.
        // Format: [count:4 LE][len1:4 LE][data1][len2:4 LE][data2]...
        // If the outputs blob doesn't use this format (it's opaque bytes),
        // return the full outputs data for the caller to parse.
        //
        // For SPV proof generation, the caller is expected to know the output format.
        // We return the raw outputs blob; the caller can index into it.
        if output_index == 0 {
            Ok(Some(cold.outputs))
        } else {
            // Without per-output indexing, we return all outputs
            Ok(Some(cold.outputs))
        }
    }

    /// Alignment-aware write to device.
    fn write_aligned(&self, data: &[u8], offset: u64) -> Result<()> {
        let align = self.device.alignment();
        let aligned_base = offset / align as u64 * align as u64;
        let intra = (offset - aligned_base) as usize;
        let total = (intra + data.len()).div_ceil(align) * align;

        let mut buf = AlignedBuf::new(total, align);
        if intra > 0 || !data.len().is_multiple_of(align) {
            // R-051 (IJK-05): propagate pread errors. The pre-fix
            // comment claimed "the bytes we read are immediately
            // overwritten by `data` below" — that is true for the
            // `intra..intra + data.len()` window we explicitly copy
            // into, but FALSE for the head bytes (`buf[0..intra]`)
            // and tail bytes (`buf[intra + data.len()..total]`) of
            // the aligned read-modify-write block. Those are OUTSIDE
            // `data`, so on pread failure they remain zero from
            // `AlignedBuf::new`, and the subsequent `pwrite_all_at`
            // writes those zeros over the head/tail bytes that
            // belonged to neighbouring records — silent corruption
            // of record-adjacent metadata.
            self.device.pread_exact_at(&mut buf, aligned_base)?;
        }
        buf[intra..intra + data.len()].copy_from_slice(data);
        self.device.pwrite_all_at(&buf, aligned_base)?;
        Ok(())
    }

    /// Alignment-aware read from device.
    fn read_aligned(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let align = self.device.alignment();
        let aligned_base = offset / align as u64 * align as u64;
        let intra = (offset - aligned_base) as usize;
        let total = (intra + len).div_ceil(align) * align;

        let mut buf = AlignedBuf::new(total, align);
        self.device.pread_exact_at(&mut buf, aligned_base)?;
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
        let alloc = SlotAllocator::new(dev.clone()).unwrap();
        let blob = Arc::new(MemoryBlobStore::new());
        let mgr = StorageManager::new(dev.clone(), alloc, blob);
        (dev, mgr)
    }

    fn write_test_record(
        dev: &dyn BlockDevice,
        offset: u64,
        utxo_count: u32,
        flags: TxFlags,
    ) -> TxMetadata {
        let mut meta = TxMetadata::new(utxo_count);
        meta.tx_id[0] = 0xAA;
        meta.flags = flags;
        let slots: Vec<UtxoSlot> = (0..utxo_count)
            .map(|i| {
                let mut h = [0u8; 32];
                h[0] = i as u8;
                UtxoSlot::new_unspent(h)
            })
            .collect();
        io::write_full_record(dev, offset, &meta, &slots).unwrap();
        meta
    }

    // ---- Tier classification tests (matching acceptance criteria) ----

    #[test]
    fn tier_100_bytes_inline() {
        let (_, mgr) = setup();
        assert_eq!(mgr.tier_for_size(100), StorageTier::Inline);
    }

    #[test]
    fn tier_8000_bytes_inline() {
        let (_, mgr) = setup();
        assert_eq!(mgr.tier_for_size(8000), StorageTier::Inline);
    }

    #[test]
    fn tier_8193_bytes_separate() {
        let (_, mgr) = setup();
        assert_eq!(mgr.tier_for_size(8193), StorageTier::SeparateNvme);
    }

    #[test]
    fn tier_500k_separate() {
        let (_, mgr) = setup();
        assert_eq!(mgr.tier_for_size(500 * 1024), StorageTier::SeparateNvme);
    }

    #[test]
    fn tier_1m_plus_1_external() {
        let (_, mgr) = setup();
        assert_eq!(mgr.tier_for_size(1024 * 1024 + 1), StorageTier::External);
    }

    #[test]
    fn tier_320m_external() {
        let (_, mgr) = setup();
        assert_eq!(mgr.tier_for_size(320 * 1024 * 1024), StorageTier::External);
    }

    // ---- Inline cold data tests ----

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
        let result = mgr
            .write_cold_data(&meta.tx_id, &cold, utxo_count, offset)
            .unwrap();
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
    fn inline_cold_data_offset_matches_formula() {
        let (dev, mgr) = setup();

        let utxo_count = 7u32;
        let cold = ColdData {
            inputs: vec![0x01; 500],
            outputs: vec![0x02; 300],
            inpoints: vec![0x03; 200],
        };
        let cold_size = cold.serialized_size();
        let total = TxMetadata::record_size_for(utxo_count) + cold_size as u64;
        let offset = mgr.allocator.lock().allocate(total).unwrap();

        let mut meta = TxMetadata::new(utxo_count);
        meta.record_size = total as u32;
        let slots: Vec<UtxoSlot> = (0..utxo_count)
            .map(|_| UtxoSlot::new_unspent([0; 32]))
            .collect();
        io::write_full_record(&*dev, offset, &meta, &slots).unwrap();

        let result = mgr
            .write_cold_data(&meta.tx_id, &cold, utxo_count, offset)
            .unwrap();
        if let ColdDataRef::Inline { cold_size: cs } = result {
            assert_eq!(cs, cold_size as u32);
            // Verify cold data is exactly at the deterministic offset
            let expected_offset = METADATA_SIZE as u64 + utxo_count as u64 * UTXO_SLOT_SIZE as u64;
            assert_eq!(
                StorageManager::inline_cold_offset(utxo_count),
                expected_offset
            );
        } else {
            panic!("expected Inline tier");
        }
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
            .map(|i| {
                let mut h = [0u8; 32];
                h[0] = i as u8;
                UtxoSlot::new_unspent(h)
            })
            .collect();
        io::write_full_record(&*dev, offset, &meta, &slots).unwrap();
        mgr.write_cold_data(&meta.tx_id, &cold, utxo_count, offset)
            .unwrap();

        // Simulate a spend (write to slot 2)
        let mut sd = [0u8; 36];
        sd[0] = 0xFF;
        let spent = UtxoSlot::new_spent(slots[2].hash, sd);
        io::write_utxo_slot(&*dev, offset, 2, &spent).unwrap();

        // Cold data unchanged
        let read = mgr.read_cold_data(offset, utxo_count, &meta).unwrap();
        assert_eq!(read, cold);
    }

    #[test]
    fn setmined_does_not_corrupt_cold_data() {
        let (dev, mgr) = setup();
        let utxo_count = 3u32;
        let cold = ColdData {
            inputs: vec![0x11; 100],
            outputs: vec![0x22; 200],
            inpoints: vec![0x33; 50],
        };
        let total = TxMetadata::record_size_for(utxo_count) + cold.serialized_size() as u64;
        let offset = mgr.allocator.lock().allocate(total).unwrap();

        let mut meta = TxMetadata::new(utxo_count);
        meta.record_size = total as u32;
        meta.tx_id[0] = 0x03;
        let slots: Vec<UtxoSlot> = (0..utxo_count)
            .map(|_| UtxoSlot::new_unspent([0; 32]))
            .collect();
        io::write_full_record(&*dev, offset, &meta, &slots).unwrap();
        mgr.write_cold_data(&meta.tx_id, &cold, utxo_count, offset)
            .unwrap();

        // Simulate setMined (modify metadata only)
        let mut updated_meta = io::read_metadata(&*dev, offset).unwrap();
        updated_meta.block_entry_count = 1;
        updated_meta.block_entries_inline[0] = crate::record::BlockEntry {
            block_id: 1,
            block_height: 1000,
            subtree_idx: 0,
        };
        updated_meta.unmined_since = 0;
        io::write_metadata(&*dev, offset, &updated_meta).unwrap();

        // Cold data unchanged
        let read = mgr.read_cold_data(offset, utxo_count, &meta).unwrap();
        assert_eq!(read, cold);
    }

    #[test]
    fn inline_delete_frees_allocation() {
        let (_dev, mgr) = setup();
        let utxo_count = 2u32;
        let cold = ColdData {
            inputs: vec![0x01; 100],
            outputs: vec![],
            inpoints: vec![],
        };
        let cold_size = cold.serialized_size();
        let total = TxMetadata::record_size_for(utxo_count) + cold_size as u64;

        // Allocate and track the offset
        let offset = mgr.allocator.lock().allocate(total).unwrap();

        // Write record and cold data
        let meta = TxMetadata::new(utxo_count);

        // Delete cold data (inline is no-op, but we also free the record allocation)
        mgr.delete_cold_data(&meta, None).unwrap();

        // Free the entire record allocation (including inline cold data)
        mgr.allocator.lock().free(offset, total).unwrap();

        // The allocation should be reusable
        let offset2 = mgr.allocator.lock().allocate(total).unwrap();
        assert_eq!(offset, offset2);
    }

    // ---- Separate NVMe cold data tests ----

    #[test]
    fn separate_nvme_cold_data_write_read() {
        let (dev, mgr) = setup();
        let utxo_count = 3u32;

        // Create cold data that exceeds inline threshold → SeparateNvme
        let cold = ColdData {
            inputs: vec![0xAA; 50 * 1024], // 50 KiB → separate
            outputs: vec![0xBB; 50 * 1024],
            inpoints: vec![],
        };

        // Allocate hot record only (no inline cold space)
        let hot_size = TxMetadata::record_size_for(utxo_count);
        let record_offset = mgr.allocator.lock().allocate(hot_size).unwrap();

        let mut meta = TxMetadata::new(utxo_count);
        meta.record_size = hot_size as u32;
        meta.tx_id[0] = 0x10;
        let slots: Vec<UtxoSlot> = (0..utxo_count)
            .map(|_| UtxoSlot::new_unspent([0; 32]))
            .collect();
        io::write_full_record(&*dev, record_offset, &meta, &slots).unwrap();

        // Write cold data — should go to separate allocation
        let result = mgr
            .write_cold_data(&meta.tx_id, &cold, utxo_count, record_offset)
            .unwrap();
        let (sep_offset, sep_size) = match result {
            ColdDataRef::SeparateNvme {
                device_offset,
                cold_size,
            } => (device_offset, cold_size),
            other => panic!("expected SeparateNvme, got {other:?}"),
        };

        // Read back via read_cold_data_at
        let read = mgr.read_cold_data_at(sep_offset, sep_size).unwrap();
        assert_eq!(read, cold);
    }

    #[test]
    fn separate_nvme_hot_record_exact_size() {
        let utxo_count = 5u32;
        let hot_size = TxMetadata::record_size_for(utxo_count);

        // Hot record should be exactly METADATA_SIZE + utxo_count * 69
        assert_eq!(
            hot_size,
            METADATA_SIZE as u64 + utxo_count as u64 * UTXO_SLOT_SIZE as u64,
        );

        // For separate NVMe, record_size in metadata should NOT include cold data
        let meta = TxMetadata::new(utxo_count);
        assert_eq!({ meta.record_size }, hot_size as u32);
    }

    #[test]
    fn separate_nvme_delete_frees_both() {
        let (dev, mgr) = setup();
        let utxo_count = 2u32;

        let cold = ColdData {
            inputs: vec![0xFF; 20 * 1024],
            outputs: vec![0xEE; 20 * 1024],
            inpoints: vec![],
        };

        let hot_size = TxMetadata::record_size_for(utxo_count);
        let record_offset = mgr.allocator.lock().allocate(hot_size).unwrap();

        let mut meta = TxMetadata::new(utxo_count);
        meta.record_size = hot_size as u32;
        meta.tx_id[0] = 0x20;
        let slots: Vec<UtxoSlot> = (0..utxo_count)
            .map(|_| UtxoSlot::new_unspent([0; 32]))
            .collect();
        io::write_full_record(&*dev, record_offset, &meta, &slots).unwrap();

        let result = mgr
            .write_cold_data(&meta.tx_id, &cold, utxo_count, record_offset)
            .unwrap();
        let (sep_offset, sep_size) = match result {
            ColdDataRef::SeparateNvme {
                device_offset,
                cold_size,
            } => (device_offset, cold_size),
            other => panic!("expected SeparateNvme, got {other:?}"),
        };

        // Delete both hot record and separate cold allocation
        mgr.delete_cold_data(&meta, Some((sep_offset, sep_size)))
            .unwrap();
        mgr.allocator.lock().free(record_offset, hot_size).unwrap();

        // Both allocations should be reusable
        let o1 = mgr.allocator.lock().allocate(hot_size).unwrap();
        let o2 = mgr.allocator.lock().allocate(sep_size as u64).unwrap();
        // They should be at the previously freed offsets (order may vary due to best-fit)
        assert!(o1 == record_offset || o1 == sep_offset);
        assert!(o2 == record_offset || o2 == sep_offset);
    }

    #[test]
    fn separate_nvme_hot_record_committed_before_cold() {
        let (dev, mgr) = setup();
        let utxo_count = 2u32;

        let hot_size = TxMetadata::record_size_for(utxo_count);
        let record_offset = mgr.allocator.lock().allocate(hot_size).unwrap();

        let mut meta = TxMetadata::new(utxo_count);
        meta.record_size = hot_size as u32;
        meta.tx_id[0] = 0x30;
        let slots: Vec<UtxoSlot> = (0..utxo_count)
            .map(|_| UtxoSlot::new_unspent([0; 32]))
            .collect();

        // Write hot record first
        io::write_full_record(&*dev, record_offset, &meta, &slots).unwrap();

        // Verify hot record is readable before cold data
        let read_meta = io::read_metadata(&*dev, record_offset).unwrap();
        assert_eq!(read_meta.tx_id[0], 0x30);
        let slot0 = io::read_utxo_slot(&*dev, record_offset, 0).unwrap();
        assert!(slot0.is_unspent());

        // Now write cold data separately
        let cold = ColdData {
            inputs: vec![0xCC; 20 * 1024],
            outputs: vec![],
            inpoints: vec![],
        };
        let result = mgr
            .write_cold_data(&meta.tx_id, &cold, utxo_count, record_offset)
            .unwrap();
        assert!(matches!(result, ColdDataRef::SeparateNvme { .. }));

        // Hot record should still be readable
        let read_meta2 = io::read_metadata(&*dev, record_offset).unwrap();
        assert_eq!(read_meta2.tx_id[0], 0x30);
    }

    #[test]
    fn separate_nvme_utxo_spendable_before_cold_write() {
        let (dev, mgr) = setup();
        let utxo_count = 2u32;

        let hot_size = TxMetadata::record_size_for(utxo_count);
        let record_offset = mgr.allocator.lock().allocate(hot_size).unwrap();

        let mut meta = TxMetadata::new(utxo_count);
        meta.record_size = hot_size as u32;
        meta.tx_id[0] = 0x40;
        let hash = [0xBB; 32];
        let slots = vec![UtxoSlot::new_unspent(hash); utxo_count as usize];

        // Write hot record only
        io::write_full_record(&*dev, record_offset, &meta, &slots).unwrap();

        // UTXO should be readable (and thus spendable) before cold data
        let slot = io::read_utxo_slot(&*dev, record_offset, 0).unwrap();
        assert!(slot.is_unspent());
        assert_eq!(slot.hash, hash);

        // Now write cold data to separate allocation
        let cold = ColdData {
            inputs: vec![0; 20 * 1024],
            outputs: vec![],
            inpoints: vec![],
        };
        mgr.write_cold_data(&meta.tx_id, &cold, utxo_count, record_offset)
            .unwrap();

        // UTXO should still be readable
        let slot_after = io::read_utxo_slot(&*dev, record_offset, 0).unwrap();
        assert_eq!(slot_after, slot);
    }

    // ---- External blob tests ----

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

        let result = mgr
            .write_cold_data(&meta.tx_id, &cold, utxo_count, offset)
            .unwrap();
        let digest = match result {
            ColdDataRef::External { digest } => digest,
            other => panic!("expected External, got {other:?}"),
        };
        // The manager-returned digest must reflect the actual SHA-256 / length
        // of the serialized cold-data payload — never a placeholder zero.
        assert_eq!(digest.length, cold.serialized_size() as u64);
        assert_ne!(digest.sha256, [0u8; 32]);

        // Read back via blob store
        let read = mgr.read_cold_data(offset, utxo_count, &meta).unwrap();
        assert_eq!(read, cold);
    }

    #[test]
    fn external_hot_record_exact_size() {
        let utxo_count = 4u32;
        let hot_size = TxMetadata::record_size_for(utxo_count);
        let meta = TxMetadata::new(utxo_count);

        // For external tier, hot record should be exactly METADATA_SIZE + utxo_count * 69
        assert_eq!(
            { meta.record_size } as u64,
            METADATA_SIZE as u64 + utxo_count as u64 * UTXO_SLOT_SIZE as u64,
        );
        assert_eq!({ meta.record_size } as u64, hot_size);
    }

    #[test]
    fn external_ref_fields_correct() {
        let (dev, mgr) = setup();
        let utxo_count = 2u32;
        let hot_size = TxMetadata::record_size_for(utxo_count);
        let offset = mgr.allocator.lock().allocate(hot_size).unwrap();

        let mut meta = TxMetadata::new(utxo_count);
        meta.tx_id[0] = 0x55;
        meta.flags = TxFlags::EXTERNAL;
        meta.record_size = hot_size as u32;
        let slots = vec![UtxoSlot::new_unspent([0; 32]); utxo_count as usize];
        io::write_full_record(&*dev, offset, &meta, &slots).unwrap();

        let cold = ColdData {
            inputs: vec![0x11; 1024 * 1024 + 500], // > 1 MiB
            outputs: vec![0x22; 1000],
            inpoints: vec![0x33; 500],
        };

        let result = mgr
            .write_cold_data(&meta.tx_id, &cold, utxo_count, offset)
            .unwrap();
        let digest = match result {
            ColdDataRef::External { digest } => digest,
            other => panic!("expected External, got {other:?}"),
        };
        assert_ne!(digest.sha256, [0u8; 32]);
        assert_eq!(digest.length, cold.serialized_size() as u64);

        // Verify blob exists in store and the manager-returned digest matches
        // what the store independently reports — defends against any future
        // manager bug that fabricates a digest without uploading the bytes.
        assert!(mgr.blob_store.exists(&meta.tx_id).unwrap());
        let store_digest = mgr.blob_store.digest(&meta.tx_id).unwrap().unwrap();
        assert_eq!(store_digest, digest);
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

        mgr.delete_cold_data(&meta, None).unwrap();
        assert!(!mgr.blob_store.exists(&meta.tx_id).unwrap());
    }

    #[test]
    fn external_stream_cold_data() {
        let (dev, mgr) = setup();
        let utxo_count = 2u32;
        let hot_size = TxMetadata::record_size_for(utxo_count);
        let offset = mgr.allocator.lock().allocate(hot_size).unwrap();

        let mut meta = TxMetadata::new(utxo_count);
        meta.tx_id[0] = 0x88;
        meta.flags = TxFlags::EXTERNAL;
        meta.record_size = hot_size as u32;
        let slots = vec![UtxoSlot::new_unspent([0; 32]); utxo_count as usize];
        io::write_full_record(&*dev, offset, &meta, &slots).unwrap();

        let cold = ColdData {
            inputs: vec![0xAA; 2 * 1024 * 1024],
            outputs: vec![0xBB; 1000],
            inpoints: vec![],
        };
        mgr.write_cold_data(&meta.tx_id, &cold, utxo_count, offset)
            .unwrap();

        let mut output = Vec::new();
        let bytes = mgr
            .stream_cold_data(offset, utxo_count, &meta, &mut output)
            .unwrap();
        assert!(bytes > 0);
        // The streamed data should be the serialized cold data
        let deserialized = ColdData::deserialize(&output).unwrap();
        assert_eq!(deserialized, cold);
    }

    // ---- General tests ----

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
        let cold = ColdData {
            inputs: vec![],
            outputs: vec![],
            inpoints: vec![],
        };
        let result = mgr.write_cold_data(&[0; 32], &cold, 1, 0).unwrap();
        assert_eq!(result, ColdDataRef::None);
    }

    #[test]
    fn read_inputs_only() {
        let (dev, mgr) = setup();
        let utxo_count = 2u32;
        let cold = ColdData {
            inputs: vec![0x01, 0x02, 0x03],
            outputs: vec![0x04, 0x05],
            inpoints: vec![0x06],
        };
        let total = TxMetadata::record_size_for(utxo_count) + cold.serialized_size() as u64;
        let offset = mgr.allocator.lock().allocate(total).unwrap();

        let mut meta = TxMetadata::new(utxo_count);
        meta.record_size = total as u32;
        meta.tx_id[0] = 0x99;
        let slots = vec![UtxoSlot::new_unspent([0; 32]); utxo_count as usize];
        io::write_full_record(&*dev, offset, &meta, &slots).unwrap();
        mgr.write_cold_data(&meta.tx_id, &cold, utxo_count, offset)
            .unwrap();

        let inputs = mgr.read_inputs(offset, utxo_count, &meta).unwrap();
        assert_eq!(inputs, Some(vec![0x01, 0x02, 0x03]));
    }

    #[test]
    fn read_inputs_no_cold_data() {
        let (dev, mgr) = setup();
        let utxo_count = 1u32;
        let record_size = TxMetadata::record_size_for(utxo_count);
        let offset = mgr.allocator.lock().allocate(record_size).unwrap();

        let meta = write_test_record(&*dev, offset, utxo_count, TxFlags::empty());
        let inputs = mgr.read_inputs(offset, utxo_count, &meta).unwrap();
        assert_eq!(inputs, None);
    }

    #[test]
    fn read_output_at_returns_outputs() {
        let (dev, mgr) = setup();
        let utxo_count = 2u32;
        let cold = ColdData {
            inputs: vec![],
            outputs: vec![0xA1, 0xA2, 0xA3],
            inpoints: vec![],
        };
        let total = TxMetadata::record_size_for(utxo_count) + cold.serialized_size() as u64;
        let offset = mgr.allocator.lock().allocate(total).unwrap();

        let mut meta = TxMetadata::new(utxo_count);
        meta.record_size = total as u32;
        let slots = vec![UtxoSlot::new_unspent([0; 32]); utxo_count as usize];
        io::write_full_record(&*dev, offset, &meta, &slots).unwrap();
        mgr.write_cold_data(&meta.tx_id, &cold, utxo_count, offset)
            .unwrap();

        let output = mgr.read_output_at(offset, utxo_count, &meta, 0).unwrap();
        assert_eq!(output, Some(vec![0xA1, 0xA2, 0xA3]));
    }

    #[test]
    fn read_output_at_no_outputs() {
        let (dev, mgr) = setup();
        let utxo_count = 1u32;
        let record_size = TxMetadata::record_size_for(utxo_count);
        let offset = mgr.allocator.lock().allocate(record_size).unwrap();

        let meta = write_test_record(&*dev, offset, utxo_count, TxFlags::empty());
        let output = mgr.read_output_at(offset, utxo_count, &meta, 0).unwrap();
        assert_eq!(output, None);
    }

    #[test]
    fn inline_stream_cold_data() {
        let (dev, mgr) = setup();
        let utxo_count = 2u32;
        let cold = ColdData {
            inputs: vec![0x11; 100],
            outputs: vec![0x22; 200],
            inpoints: vec![0x33; 50],
        };
        let total = TxMetadata::record_size_for(utxo_count) + cold.serialized_size() as u64;
        let offset = mgr.allocator.lock().allocate(total).unwrap();

        let mut meta = TxMetadata::new(utxo_count);
        meta.record_size = total as u32;
        let slots = vec![UtxoSlot::new_unspent([0; 32]); utxo_count as usize];
        io::write_full_record(&*dev, offset, &meta, &slots).unwrap();
        mgr.write_cold_data(&meta.tx_id, &cold, utxo_count, offset)
            .unwrap();

        let mut output = Vec::new();
        let bytes = mgr
            .stream_cold_data(offset, utxo_count, &meta, &mut output)
            .unwrap();
        assert_eq!(bytes, cold.serialized_size() as u64);
        let deserialized = ColdData::deserialize(&output).unwrap();
        assert_eq!(deserialized, cold);
    }

    // ---- End-to-end tiered storage tests ----

    #[test]
    fn e2e_small_medium_large_correct_tiers() {
        let (dev, mgr) = setup();

        // Small tx (200 bytes total cold data) → Inline
        let small_cold = ColdData {
            inputs: vec![0x01; 80],
            outputs: vec![0x02; 80],
            inpoints: vec![0x03; 28], // 80+80+28+12 = 200 bytes serialized
        };
        let small_utxo = 2u32;
        let small_total =
            TxMetadata::record_size_for(small_utxo) + small_cold.serialized_size() as u64;
        let small_offset = mgr.allocator.lock().allocate(small_total).unwrap();
        let mut small_meta = TxMetadata::new(small_utxo);
        small_meta.record_size = small_total as u32;
        small_meta.tx_id[0] = 0x01;
        let slots = vec![UtxoSlot::new_unspent([0; 32]); small_utxo as usize];
        io::write_full_record(&*dev, small_offset, &small_meta, &slots).unwrap();
        let small_ref = mgr
            .write_cold_data(&small_meta.tx_id, &small_cold, small_utxo, small_offset)
            .unwrap();
        assert!(matches!(small_ref, ColdDataRef::Inline { .. }));

        // Medium tx (50 KiB) → SeparateNvme
        let med_cold = ColdData {
            inputs: vec![0x04; 25 * 1024],
            outputs: vec![0x05; 25 * 1024],
            inpoints: vec![],
        };
        let med_utxo = 3u32;
        let med_hot = TxMetadata::record_size_for(med_utxo);
        let med_offset = mgr.allocator.lock().allocate(med_hot).unwrap();
        let mut med_meta = TxMetadata::new(med_utxo);
        med_meta.record_size = med_hot as u32;
        med_meta.tx_id[0] = 0x02;
        let slots = vec![UtxoSlot::new_unspent([0; 32]); med_utxo as usize];
        io::write_full_record(&*dev, med_offset, &med_meta, &slots).unwrap();
        let med_result = mgr
            .write_cold_data(&med_meta.tx_id, &med_cold, med_utxo, med_offset)
            .unwrap();
        let (med_sep_off, med_sep_sz) = match med_result {
            ColdDataRef::SeparateNvme {
                device_offset,
                cold_size,
            } => (device_offset, cold_size),
            other => panic!("expected SeparateNvme for medium tx, got {other:?}"),
        };

        // Large tx (5 MB) → External
        let large_cold = ColdData {
            inputs: vec![0x06; 3 * 1024 * 1024],
            outputs: vec![0x07; 2 * 1024 * 1024],
            inpoints: vec![],
        };
        let large_utxo = 1u32;
        let large_hot = TxMetadata::record_size_for(large_utxo);
        let large_offset = mgr.allocator.lock().allocate(large_hot).unwrap();
        let mut large_meta = TxMetadata::new(large_utxo);
        large_meta.record_size = large_hot as u32;
        large_meta.tx_id[0] = 0x03;
        large_meta.flags = TxFlags::EXTERNAL;
        let slots = vec![UtxoSlot::new_unspent([0; 32]); large_utxo as usize];
        io::write_full_record(&*dev, large_offset, &large_meta, &slots).unwrap();
        let large_result = mgr
            .write_cold_data(&large_meta.tx_id, &large_cold, large_utxo, large_offset)
            .unwrap();
        assert!(matches!(large_result, ColdDataRef::External { .. }));

        // Verify all data retrievable
        let read_small = mgr
            .read_cold_data(small_offset, small_utxo, &small_meta)
            .unwrap();
        assert_eq!(read_small, small_cold);

        let read_med = mgr.read_cold_data_at(med_sep_off, med_sep_sz).unwrap();
        assert_eq!(read_med, med_cold);

        let read_large = mgr
            .read_cold_data(large_offset, large_utxo, &large_meta)
            .unwrap();
        assert_eq!(read_large, large_cold);
    }

    #[test]
    fn e2e_pruning_all_tiers() {
        let (dev, mgr) = setup();

        // Inline
        let cold1 = ColdData {
            inputs: vec![1; 100],
            outputs: vec![],
            inpoints: vec![],
        };
        let total1 = TxMetadata::record_size_for(1) + cold1.serialized_size() as u64;
        let off1 = mgr.allocator.lock().allocate(total1).unwrap();
        let mut meta1 = TxMetadata::new(1);
        meta1.record_size = total1 as u32;
        meta1.tx_id[0] = 0xA1;
        io::write_full_record(&*dev, off1, &meta1, &[UtxoSlot::new_unspent([0; 32])]).unwrap();
        mgr.write_cold_data(&meta1.tx_id, &cold1, 1, off1).unwrap();

        // Separate NVMe
        let cold2 = ColdData {
            inputs: vec![2; 20 * 1024],
            outputs: vec![],
            inpoints: vec![],
        };
        let hot2 = TxMetadata::record_size_for(1);
        let off2 = mgr.allocator.lock().allocate(hot2).unwrap();
        let mut meta2 = TxMetadata::new(1);
        meta2.record_size = hot2 as u32;
        meta2.tx_id[0] = 0xA2;
        io::write_full_record(&*dev, off2, &meta2, &[UtxoSlot::new_unspent([0; 32])]).unwrap();
        let sep = mgr.write_cold_data(&meta2.tx_id, &cold2, 1, off2).unwrap();
        let (sep_off, sep_sz) = match sep {
            ColdDataRef::SeparateNvme {
                device_offset,
                cold_size,
            } => (device_offset, cold_size),
            other => panic!("expected SeparateNvme, got {other:?}"),
        };

        // External
        let cold3 = ColdData {
            inputs: vec![3; 2 * 1024 * 1024],
            outputs: vec![],
            inpoints: vec![],
        };
        let hot3 = TxMetadata::record_size_for(1);
        let off3 = mgr.allocator.lock().allocate(hot3).unwrap();
        let mut meta3 = TxMetadata::new(1);
        meta3.record_size = hot3 as u32;
        meta3.tx_id[0] = 0xA3;
        meta3.flags = TxFlags::EXTERNAL;
        io::write_full_record(&*dev, off3, &meta3, &[UtxoSlot::new_unspent([0; 32])]).unwrap();
        mgr.write_cold_data(&meta3.tx_id, &cold3, 1, off3).unwrap();

        // Prune all
        mgr.delete_cold_data(&meta1, None).unwrap(); // inline: no-op
        mgr.allocator.lock().free(off1, total1).unwrap();

        mgr.delete_cold_data(&meta2, Some((sep_off, sep_sz)))
            .unwrap(); // separate: frees allocation
        mgr.allocator.lock().free(off2, hot2).unwrap();

        mgr.delete_cold_data(&meta3, None).unwrap(); // external: deletes blob
        mgr.allocator.lock().free(off3, hot3).unwrap();
        assert!(!mgr.blob_store.exists(&meta3.tx_id).unwrap());
    }

    #[test]
    fn e2e_mixed_workload() {
        let (dev, mgr) = setup();

        // 100 small (inline), 10 medium (separate), 2 large (external)
        for i in 0..100u32 {
            let cold = ColdData {
                inputs: vec![i as u8; 100],
                outputs: vec![(i + 1) as u8; 50],
                inpoints: vec![],
            };
            let total = TxMetadata::record_size_for(1) + cold.serialized_size() as u64;
            let offset = mgr.allocator.lock().allocate(total).unwrap();
            let mut meta = TxMetadata::new(1);
            meta.record_size = total as u32;
            meta.tx_id[0] = (i & 0xFF) as u8;
            meta.tx_id[1] = 0x01; // category marker
            io::write_full_record(&*dev, offset, &meta, &[UtxoSlot::new_unspent([0; 32])]).unwrap();
            let result = mgr.write_cold_data(&meta.tx_id, &cold, 1, offset).unwrap();
            assert!(matches!(result, ColdDataRef::Inline { .. }));

            let read = mgr.read_cold_data(offset, 1, &meta).unwrap();
            assert_eq!(read, cold);
        }

        for i in 0..10u32 {
            let cold = ColdData {
                inputs: vec![(i + 200) as u8; 20 * 1024],
                outputs: vec![],
                inpoints: vec![],
            };
            let hot_size = TxMetadata::record_size_for(1);
            let offset = mgr.allocator.lock().allocate(hot_size).unwrap();
            let mut meta = TxMetadata::new(1);
            meta.record_size = hot_size as u32;
            meta.tx_id[0] = (i & 0xFF) as u8;
            meta.tx_id[1] = 0x02;
            io::write_full_record(&*dev, offset, &meta, &[UtxoSlot::new_unspent([0; 32])]).unwrap();
            let result = mgr.write_cold_data(&meta.tx_id, &cold, 1, offset).unwrap();
            let (sep_off, sep_sz) = match result {
                ColdDataRef::SeparateNvme {
                    device_offset,
                    cold_size,
                } => (device_offset, cold_size),
                other => panic!("expected SeparateNvme, got {other:?}"),
            };
            let read = mgr.read_cold_data_at(sep_off, sep_sz).unwrap();
            assert_eq!(read, cold);
        }

        for i in 0..2u32 {
            let cold = ColdData {
                inputs: vec![(i + 100) as u8; 2 * 1024 * 1024],
                outputs: vec![],
                inpoints: vec![],
            };
            let hot_size = TxMetadata::record_size_for(1);
            let offset = mgr.allocator.lock().allocate(hot_size).unwrap();
            let mut meta = TxMetadata::new(1);
            meta.record_size = hot_size as u32;
            meta.tx_id[0] = (i & 0xFF) as u8;
            meta.tx_id[1] = 0x03;
            meta.flags = TxFlags::EXTERNAL;
            io::write_full_record(&*dev, offset, &meta, &[UtxoSlot::new_unspent([0; 32])]).unwrap();
            let result = mgr.write_cold_data(&meta.tx_id, &cold, 1, offset).unwrap();
            assert!(matches!(result, ColdDataRef::External { .. }));
            let read = mgr.read_cold_data(offset, 1, &meta).unwrap();
            assert_eq!(read, cold);
        }
    }

    #[test]
    fn verify_inline_cold_offset_for_all_inline_records() {
        let (dev, mgr) = setup();

        for utxo_count in [1u32, 2, 5, 10, 50, 100] {
            let cold = ColdData {
                inputs: vec![0x01; 100],
                outputs: vec![0x02; 50],
                inpoints: vec![],
            };
            let cold_size = cold.serialized_size();
            let total = TxMetadata::record_size_for(utxo_count) + cold_size as u64;
            let offset = mgr.allocator.lock().allocate(total).unwrap();

            let mut meta = TxMetadata::new(utxo_count);
            meta.record_size = total as u32;
            let slots: Vec<UtxoSlot> = (0..utxo_count)
                .map(|_| UtxoSlot::new_unspent([0; 32]))
                .collect();
            io::write_full_record(&*dev, offset, &meta, &slots).unwrap();
            mgr.write_cold_data(&meta.tx_id, &cold, utxo_count, offset)
                .unwrap();

            // Verify the cold data offset matches the formula
            let expected_cold_offset =
                METADATA_SIZE as u64 + utxo_count as u64 * UTXO_SLOT_SIZE as u64;
            assert_eq!(
                StorageManager::inline_cold_offset(utxo_count),
                expected_cold_offset,
                "cold offset mismatch for utxo_count={utxo_count}",
            );

            // Verify data is readable at the correct offset
            let read = mgr.read_cold_data(offset, utxo_count, &meta).unwrap();
            assert_eq!(read, cold, "cold data mismatch for utxo_count={utxo_count}");
        }
    }
}
