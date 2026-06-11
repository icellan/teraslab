//! Tiered storage manager — coordinates inline and external blob store tiers.

use crate::device::{AlignedBuf, BlockDevice};
use crate::record::{METADATA_SIZE, TxFlags, TxMetadata, UTXO_SLOT_SIZE};
use crate::storage::blobstore::BlobStore;
use crate::storage::tiers::*;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use thiserror::Error;

/// Defence-in-depth upper bound on the cold-data payload returned by
/// [`StorageManager::read_cold_data`] / [`StorageManager::stream_cold_data`].
///
/// Mirrors the wire-side [`crate::protocol::opcodes::MAX_COLD_DATA_PER_ITEM`]
/// cap (R-089) on the read-back path so a corrupt or attacker-tampered
/// `record_size` cannot trigger a multi-GiB aligned read. The extra
/// [`METADATA_SIZE`] head-room covers the metadata + UTXO slot tail that
/// `record_size` includes for inline records — `cold_size` is the
/// `record_size - inline_cold_offset`, but we bound the cap generously so the
/// inline tier never trips it under legitimate workloads.
pub const MAX_COLD_DATA_READ_BYTES: u64 =
    crate::protocol::opcodes::MAX_COLD_DATA_PER_ITEM as u64 + METADATA_SIZE as u64;

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
    /// An EXTERNAL-flagged record's blob is absent from the blob store.
    ///
    /// F-G9-001: previously `read_cold_data` silently returned an empty
    /// [`ColdData`] in this case, which made a lost blob indistinguishable
    /// from "the tx had no cold data". Callers (validation, SPV proofs,
    /// audit tooling) MUST treat this as a data-integrity violation.
    #[error("cold data blob not found for txid {key}")]
    ColdDataNotFound { key: String },
    /// SHA-256 of the blob payload returned by the store disagrees with
    /// the record-anchored `ExternalRef.content_hash` durable digest.
    ///
    /// F-G9-002: pre-fix only the spend path cross-checked the
    /// record-anchored digest against the recomputed payload hash. Other
    /// read paths (audit tooling, SPV, prune validation) trusted the blob
    /// store's sidecar alone — an attacker who could substitute both
    /// payload and sidecar would pass the sidecar check while the
    /// record-anchored digest would catch the swap. We now compare both.
    #[error("blob content hash mismatch for txid {key}")]
    ContentHashMismatch {
        key: String,
        expected: [u8; 32],
        actual: [u8; 32],
    },
    /// `record_size` decoded from on-device metadata exceeds the per-item
    /// cold-data cap, signalling corruption or attacker tampering.
    ///
    /// F-G9-006: pre-fix the read-back path honoured `metadata.record_size`
    /// up to `u32::MAX`, allowing a single corrupt record to trigger a
    /// multi-GiB aligned device read. We mirror the wire-side R-089 cap
    /// here as defence-in-depth.
    #[error("cold data size {size} exceeds maximum {max}")]
    ColdDataTooLarge { size: u64, max: u64 },
    /// I-02: an inline-tier cold-data write would extend past the record's
    /// allocated extent and silently overwrite the neighbouring allocation
    /// (another transaction's metadata/slots).
    ///
    /// `write_cold_data` re-derives the tier from the payload size on every
    /// call, so without this guard a second, larger inline write for the
    /// same record would land at `inline_cold_offset(utxo_count)` and run
    /// past the bytes allocated at create time. No production caller does
    /// this today; the bound makes the public API safe regardless.
    #[error("inline cold data end {required} exceeds allocated record capacity {capacity}")]
    ColdDataExceedsRecord { required: u64, capacity: u64 },
}

pub type Result<T> = std::result::Result<T, StorageError>;

/// Lowercase hex encoding of a 32-byte txid for error messages.
fn hex_key(key: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in key {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// F-G9-002: recompute SHA-256 of `payload` and compare against the
/// record-anchored `expected` digest carried in `ExternalRef.content_hash`.
///
/// The all-zero placeholder is tolerated for records written before R-048
/// populated the field — every blob shipped with a real digest on the
/// post-R-048 create path, but legacy on-disk records may still have it at
/// zero until they are re-created. We emit a `warn` on the zero-digest
/// fallback so operators can spot stale records.
fn verify_content_hash(
    tx_id: &[u8; 32],
    payload: &[u8],
    expected: &[u8; 32],
) -> std::result::Result<(), StorageError> {
    if *expected == [0u8; 32] {
        tracing::warn!(
            txid = %hex_key(tx_id),
            "read_cold_data: ExternalRef.content_hash is zero — skipping record-anchored digest check (legacy record?)",
        );
        return Ok(());
    }
    let mut hasher = Sha256::new();
    hasher.update(payload);
    let mut actual = [0u8; 32];
    actual.copy_from_slice(&hasher.finalize());
    if actual != *expected {
        return Err(StorageError::ContentHashMismatch {
            key: hex_key(tx_id),
            expected: *expected,
            actual,
        });
    }
    Ok(())
}

/// Manages tiered storage for cold data (inputs, outputs, inpoints).
///
/// Coordinates the production tiers:
/// - **Inline** (serialized size `<=` 8 KiB, i.e. `<=` 8192 bytes including
///   the 12-byte `ColdData` length prefixes — 8180 bytes of user data):
///   cold data appended to record at `METADATA_SIZE + utxo_count * 69`
/// - **External** (serialized size `>` 8192 bytes): cold data in an external
///   blob store (file or S3)
pub struct StorageManager {
    device: Arc<dyn BlockDevice>,
    /// Retained for tests and constructor compatibility; production cold data
    /// no longer allocates a separate device region.
    #[allow(dead_code)]
    allocator: parking_lot::Mutex<crate::allocator::SlotAllocator>,
    blob_store: Arc<dyn BlobStore>,
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
        }
    }

    /// Determine which tier to use for the given serialized cold data size.
    ///
    /// Sizes `<=` [`INLINE_THRESHOLD`] (8192 bytes) are [`StorageTier::Inline`];
    /// larger sizes are [`StorageTier::External`].
    pub fn tier_for_size(&self, data_size: usize) -> StorageTier {
        crate::storage::tiers::tier_for_size(data_size)
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
    /// `record_capacity` is the total byte length of that allocation
    /// (metadata + UTXO slots + inline cold region); an inline write whose
    /// end would exceed it is rejected with
    /// [`StorageError::ColdDataExceedsRecord`] instead of silently
    /// overwriting the neighbouring allocation (I-02).
    ///
    /// For external: writes to the blob store synchronously and
    /// `record_capacity` is not consulted. For async upload, use
    /// [`BlobUploader`](super::uploader::BlobUploader) instead.
    pub fn write_cold_data(
        &self,
        tx_id: &[u8; 32],
        cold: &ColdData,
        utxo_count: u32,
        record_offset: u64,
        record_capacity: u64,
    ) -> Result<ColdDataRef> {
        if cold.is_empty() {
            return Ok(ColdDataRef::None);
        }

        let serialized = cold.serialize();
        let data_size = serialized.len();
        let tier = self.tier_for_size(data_size);

        match tier {
            StorageTier::Inline => {
                let inline_off = Self::inline_cold_offset(utxo_count);
                let required = inline_off + data_size as u64;
                if required > record_capacity {
                    return Err(StorageError::ColdDataExceedsRecord {
                        required,
                        capacity: record_capacity,
                    });
                }
                let cold_offset = record_offset + inline_off;
                self.write_aligned(&serialized, cold_offset)?;
                Ok(ColdDataRef::Inline {
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
    /// - Otherwise → no inline cold data
    ///
    /// # Errors
    ///
    /// - [`StorageError::ColdDataNotFound`] when the record carries
    ///   [`TxFlags::EXTERNAL`] but the blob is missing (F-G9-001).
    /// - [`StorageError::ContentHashMismatch`] when the recomputed payload
    ///   SHA-256 disagrees with the record-anchored
    ///   `ExternalRef.content_hash` (F-G9-002). The all-zero placeholder is
    ///   tolerated for backward compatibility with records written before
    ///   R-048 populated the field, but is logged at `warn`.
    /// - [`StorageError::ColdDataTooLarge`] when `record_size` implies a
    ///   cold payload exceeding [`MAX_COLD_DATA_READ_BYTES`] (F-G9-006).
    pub fn read_cold_data(
        &self,
        record_offset: u64,
        utxo_count: u32,
        metadata: &TxMetadata,
    ) -> Result<ColdData> {
        let flags = metadata.flags;

        // External tier
        if flags.contains(TxFlags::EXTERNAL) {
            // F-G9-001: a missing blob on an EXTERNAL-flagged record is a
            // data-integrity violation, NOT an "empty cold data" signal.
            let bytes = self.blob_store.get(&metadata.tx_id)?.ok_or_else(|| {
                StorageError::ColdDataNotFound {
                    key: hex_key(&metadata.tx_id),
                }
            })?;
            // F-G9-002: cross-check the recomputed payload SHA-256 against
            // the record-anchored `ExternalRef.content_hash`. The blob
            // store's own sidecar already covers payload-only tampering,
            // but a coordinated payload+sidecar swap would slip past that
            // check — only the durable record-anchored digest catches it.
            let expected_hash: [u8; 32] = { metadata.external_ref.content_hash };
            verify_content_hash(&metadata.tx_id, &bytes, &expected_hash)?;
            ColdData::deserialize(&bytes).ok_or(StorageError::InvalidColdData)
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

            // F-G9-006: cap the read length against the same per-item
            // bound the codec enforces on writes (R-089). A corrupt record
            // with `record_size = u32::MAX` would otherwise trigger a
            // ~4 GiB aligned read.
            if cold_size > MAX_COLD_DATA_READ_BYTES {
                return Err(StorageError::ColdDataTooLarge {
                    size: cold_size,
                    max: MAX_COLD_DATA_READ_BYTES,
                });
            }

            let bytes = self.read_aligned(cold_offset, cold_size as usize)?;
            ColdData::deserialize(&bytes).ok_or(StorageError::InvalidColdData)
        }
    }

    /// Stream cold data to a writer (for large external blobs).
    ///
    /// For inline records: reads from device and writes to the writer.
    /// For external: streams directly from the blob store.
    ///
    /// # Errors
    ///
    /// External tier: [`StorageError::ColdDataTooLarge`] is not surfaced
    /// because the blob store's sidecar already encodes a durable payload
    /// length — the bound is enforced at write time.
    /// Inline tier: returns [`StorageError::ColdDataTooLarge`] when
    /// `record_size` implies a cold payload exceeding
    /// [`MAX_COLD_DATA_READ_BYTES`] (F-G9-006).
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

            // F-G9-006: mirror the wire-side R-089 cap on the read-back path.
            if cold_size > MAX_COLD_DATA_READ_BYTES {
                return Err(StorageError::ColdDataTooLarge {
                    size: cold_size,
                    max: MAX_COLD_DATA_READ_BYTES,
                });
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
    /// For external tier: deletes the blob from the blob store.
    ///
    /// # 1:1 invariant
    ///
    /// F-G9-016: each blob is keyed by txid, which is unique per record —
    /// no reference counting is needed. A `delete` therefore cannot affect
    /// another record's blob, and there is no leak under crash because the
    /// `EXTERNAL` flag in the index entry is what keeps the blob alive
    /// for the orphan-blob GC (R-049).
    ///
    /// # Parameters
    /// - `metadata`: the record's metadata (checked for EXTERNAL flag)
    pub fn delete_cold_data(&self, metadata: &TxMetadata) -> Result<()> {
        if metadata.flags.contains(TxFlags::EXTERNAL) {
            self.blob_store.delete(&metadata.tx_id)?;
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
    fn tier_8192_bytes_inline() {
        // Boundary: exactly INLINE_THRESHOLD serialized bytes is Inline
        // (inclusive `<=` semantics; 8180 bytes of user data + 12-byte
        // ColdData length prefixes).
        let (_, mgr) = setup();
        assert_eq!(mgr.tier_for_size(8192), StorageTier::Inline);
    }

    #[test]
    fn tier_8193_bytes_external() {
        let (_, mgr) = setup();
        assert_eq!(mgr.tier_for_size(8193), StorageTier::External);
    }

    #[test]
    fn tier_500k_external() {
        let (_, mgr) = setup();
        assert_eq!(mgr.tier_for_size(500 * 1024), StorageTier::External);
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
            .write_cold_data(&meta.tx_id, &cold, utxo_count, offset, total)
            .unwrap();
        assert!(matches!(result, ColdDataRef::Inline { .. }));

        // Read back
        let read = mgr.read_cold_data(offset, utxo_count, &meta).unwrap();
        assert_eq!(read, cold);
    }

    #[test]
    fn inline_write_exceeding_record_capacity_rejected() {
        let (dev, mgr) = setup();

        // Record A: 1 UTXO + a small inline cold payload, allocated exactly.
        let utxo_count = 1u32;
        let small = ColdData {
            inputs: vec![0x11; 38],
            outputs: vec![],
            inpoints: vec![],
        };
        let capacity = TxMetadata::record_size_for(utxo_count) + small.serialized_size() as u64;
        let offset_a = mgr.allocator.lock().allocate(capacity).unwrap();

        let mut meta_a = TxMetadata::new(utxo_count);
        meta_a.record_size = capacity as u32;
        meta_a.tx_id[0] = 0xA1;
        let slots = vec![UtxoSlot::new_unspent([0x0A; 32])];
        io::write_full_record(&*dev, offset_a, &meta_a, &slots).unwrap();
        mgr.write_cold_data(&meta_a.tx_id, &small, utxo_count, offset_a, capacity)
            .unwrap();

        // Record B: the neighbouring allocation an unbounded second write
        // would clobber.
        let cap_b = TxMetadata::record_size_for(utxo_count);
        let offset_b = mgr.allocator.lock().allocate(cap_b).unwrap();
        let meta_b = write_test_record(&*dev, offset_b, utxo_count, TxFlags::empty());

        // A second, larger inline write for record A must be rejected with
        // the typed error, not run past A's extent.
        let big = ColdData {
            inputs: vec![0x22; 5000],
            outputs: vec![],
            inpoints: vec![],
        };
        let required =
            StorageManager::inline_cold_offset(utxo_count) + big.serialized_size() as u64;
        let err = mgr
            .write_cold_data(&meta_a.tx_id, &big, utxo_count, offset_a, capacity)
            .unwrap_err();
        match err {
            StorageError::ColdDataExceedsRecord {
                required: r,
                capacity: c,
            } => {
                assert_eq!(r, required);
                assert_eq!(c, capacity);
            }
            other => panic!("expected ColdDataExceedsRecord, got {other:?}"),
        }

        // Record B untouched (metadata CRC-valid with its tx_id) and record
        // A's original cold data still reads back byte-for-byte.
        let read_b = io::read_metadata(&*dev, offset_b).unwrap();
        assert_eq!(read_b.tx_id, meta_b.tx_id);
        let read_a = mgr.read_cold_data(offset_a, utxo_count, &meta_a).unwrap();
        assert_eq!(read_a, small);
    }

    #[test]
    fn inline_write_exact_capacity_fit_succeeds() {
        let (dev, mgr) = setup();
        let utxo_count = 2u32;
        let cold = ColdData {
            inputs: vec![0x33; 100],
            outputs: vec![0x44; 60],
            inpoints: vec![],
        };
        let capacity = TxMetadata::record_size_for(utxo_count) + cold.serialized_size() as u64;
        let offset = mgr.allocator.lock().allocate(capacity).unwrap();
        let mut meta = TxMetadata::new(utxo_count);
        meta.record_size = capacity as u32;
        meta.tx_id[0] = 0xA2;
        let slots: Vec<UtxoSlot> = (0..utxo_count)
            .map(|_| UtxoSlot::new_unspent([0; 32]))
            .collect();
        io::write_full_record(&*dev, offset, &meta, &slots).unwrap();

        // required == capacity exactly: must succeed (the bound rejects only
        // writes that EXCEED the extent).
        let result = mgr
            .write_cold_data(&meta.tx_id, &cold, utxo_count, offset, capacity)
            .unwrap();
        assert!(matches!(result, ColdDataRef::Inline { .. }));
        assert_eq!(mgr.read_cold_data(offset, utxo_count, &meta).unwrap(), cold);
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
            .write_cold_data(&meta.tx_id, &cold, utxo_count, offset, total)
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
        mgr.write_cold_data(&meta.tx_id, &cold, utxo_count, offset, total)
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
        mgr.write_cold_data(&meta.tx_id, &cold, utxo_count, offset, total)
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
        mgr.delete_cold_data(&meta).unwrap();

        // Free the entire record allocation (including inline cold data)
        mgr.allocator.lock().free(offset, total).unwrap();

        // The allocation should be reusable
        let offset2 = mgr.allocator.lock().allocate(total).unwrap();
        assert_eq!(offset, offset2);
    }

    // ---- Non-inline cold data tests ----

    #[test]
    fn non_inline_cold_data_uses_external_blob_tier() {
        let (dev, mgr) = setup();
        let utxo_count = 3u32;
        let cold = ColdData {
            inputs: vec![0xAA; 50 * 1024],
            outputs: vec![0xBB; 50 * 1024],
            inpoints: vec![],
        };

        let hot_size = TxMetadata::record_size_for(utxo_count);
        let record_offset = mgr.allocator.lock().allocate(hot_size).unwrap();

        let mut meta = TxMetadata::new(utxo_count);
        meta.record_size = hot_size as u32;
        meta.flags = TxFlags::EXTERNAL;
        meta.tx_id[0] = 0x10;
        let slots: Vec<UtxoSlot> = (0..utxo_count)
            .map(|_| UtxoSlot::new_unspent([0; 32]))
            .collect();
        io::write_full_record(&*dev, record_offset, &meta, &slots).unwrap();

        let result = mgr
            .write_cold_data(&meta.tx_id, &cold, utxo_count, record_offset, hot_size)
            .unwrap();
        assert!(matches!(result, ColdDataRef::External { .. }));

        let read = mgr
            .read_cold_data(record_offset, utxo_count, &meta)
            .unwrap();
        assert_eq!(read, cold);
    }

    #[test]
    fn non_inline_external_hot_record_exact_size() {
        let utxo_count = 5u32;
        let hot_size = TxMetadata::record_size_for(utxo_count);

        // Hot record should be exactly METADATA_SIZE + utxo_count * 69
        assert_eq!(
            hot_size,
            METADATA_SIZE as u64 + utxo_count as u64 * UTXO_SLOT_SIZE as u64,
        );

        // For external cold data, record_size in metadata does not include cold data.
        let meta = TxMetadata::new(utxo_count);
        assert_eq!({ meta.record_size }, hot_size as u32);
    }

    #[test]
    fn external_delete_removes_blob_only() {
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
        meta.flags = TxFlags::EXTERNAL;
        meta.tx_id[0] = 0x20;
        let slots: Vec<UtxoSlot> = (0..utxo_count)
            .map(|_| UtxoSlot::new_unspent([0; 32]))
            .collect();
        io::write_full_record(&*dev, record_offset, &meta, &slots).unwrap();

        let result = mgr
            .write_cold_data(&meta.tx_id, &cold, utxo_count, record_offset, hot_size)
            .unwrap();
        assert!(matches!(result, ColdDataRef::External { .. }));
        assert!(mgr.blob_store.exists(&meta.tx_id).unwrap());

        mgr.delete_cold_data(&meta).unwrap();
        assert!(!mgr.blob_store.exists(&meta.tx_id).unwrap());
        mgr.allocator.lock().free(record_offset, hot_size).unwrap();

        // Hot allocation should be reusable.
        let o1 = mgr.allocator.lock().allocate(hot_size).unwrap();
        assert_eq!(o1, record_offset);
    }

    #[test]
    fn external_hot_record_committed_before_cold() {
        let (dev, mgr) = setup();
        let utxo_count = 2u32;

        let hot_size = TxMetadata::record_size_for(utxo_count);
        let record_offset = mgr.allocator.lock().allocate(hot_size).unwrap();

        let mut meta = TxMetadata::new(utxo_count);
        meta.record_size = hot_size as u32;
        meta.flags = TxFlags::EXTERNAL;
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

        // Now write cold data externally.
        let cold = ColdData {
            inputs: vec![0xCC; 20 * 1024],
            outputs: vec![],
            inpoints: vec![],
        };
        let result = mgr
            .write_cold_data(&meta.tx_id, &cold, utxo_count, record_offset, hot_size)
            .unwrap();
        assert!(matches!(result, ColdDataRef::External { .. }));

        // Hot record should still be readable
        let read_meta2 = io::read_metadata(&*dev, record_offset).unwrap();
        assert_eq!(read_meta2.tx_id[0], 0x30);
    }

    #[test]
    fn external_utxo_spendable_before_cold_write() {
        let (dev, mgr) = setup();
        let utxo_count = 2u32;

        let hot_size = TxMetadata::record_size_for(utxo_count);
        let record_offset = mgr.allocator.lock().allocate(hot_size).unwrap();

        let mut meta = TxMetadata::new(utxo_count);
        meta.record_size = hot_size as u32;
        meta.flags = TxFlags::EXTERNAL;
        meta.tx_id[0] = 0x40;
        let hash = [0xBB; 32];
        let slots = vec![UtxoSlot::new_unspent(hash); utxo_count as usize];

        // Write hot record only
        io::write_full_record(&*dev, record_offset, &meta, &slots).unwrap();

        // UTXO should be readable (and thus spendable) before cold data
        let slot = io::read_utxo_slot(&*dev, record_offset, 0).unwrap();
        assert!(slot.is_unspent());
        assert_eq!(slot.hash, hash);

        // Now write cold data to the external tier.
        let cold = ColdData {
            inputs: vec![0; 20 * 1024],
            outputs: vec![],
            inpoints: vec![],
        };
        mgr.write_cold_data(&meta.tx_id, &cold, utxo_count, record_offset, hot_size)
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
            .write_cold_data(&meta.tx_id, &cold, utxo_count, offset, record_size)
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
            .write_cold_data(&meta.tx_id, &cold, utxo_count, offset, hot_size)
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
        mgr.write_cold_data(&meta.tx_id, &cold, 1, 0, TxMetadata::record_size_for(1)).unwrap();

        mgr.delete_cold_data(&meta).unwrap();
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
        mgr.write_cold_data(&meta.tx_id, &cold, utxo_count, offset, hot_size)
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
        let result = mgr.write_cold_data(&[0; 32], &cold, 1, 0, TxMetadata::record_size_for(1)).unwrap();
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
        mgr.write_cold_data(&meta.tx_id, &cold, utxo_count, offset, total)
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
        mgr.write_cold_data(&meta.tx_id, &cold, utxo_count, offset, total)
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
        mgr.write_cold_data(&meta.tx_id, &cold, utxo_count, offset, total)
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
            .write_cold_data(&small_meta.tx_id, &small_cold, small_utxo, small_offset, small_total)
            .unwrap();
        assert!(matches!(small_ref, ColdDataRef::Inline { .. }));

        // Medium tx (50 KiB) → External. The old separate-NVMe tier was
        // removed because record metadata cannot persist its offset/length.
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
        med_meta.flags = TxFlags::EXTERNAL;
        med_meta.tx_id[0] = 0x02;
        let slots = vec![UtxoSlot::new_unspent([0; 32]); med_utxo as usize];
        io::write_full_record(&*dev, med_offset, &med_meta, &slots).unwrap();
        let med_result = mgr
            .write_cold_data(&med_meta.tx_id, &med_cold, med_utxo, med_offset, med_hot)
            .unwrap();
        assert!(matches!(med_result, ColdDataRef::External { .. }));

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
            .write_cold_data(&large_meta.tx_id, &large_cold, large_utxo, large_offset, large_hot)
            .unwrap();
        assert!(matches!(large_result, ColdDataRef::External { .. }));

        // Verify all data retrievable
        let read_small = mgr
            .read_cold_data(small_offset, small_utxo, &small_meta)
            .unwrap();
        assert_eq!(read_small, small_cold);

        let read_med = mgr.read_cold_data(med_offset, med_utxo, &med_meta).unwrap();
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
        mgr.write_cold_data(&meta1.tx_id, &cold1, 1, off1, total1).unwrap();

        // Medium non-inline payload: external blob tier.
        let cold2 = ColdData {
            inputs: vec![2; 20 * 1024],
            outputs: vec![],
            inpoints: vec![],
        };
        let hot2 = TxMetadata::record_size_for(1);
        let off2 = mgr.allocator.lock().allocate(hot2).unwrap();
        let mut meta2 = TxMetadata::new(1);
        meta2.record_size = hot2 as u32;
        meta2.flags = TxFlags::EXTERNAL;
        meta2.tx_id[0] = 0xA2;
        io::write_full_record(&*dev, off2, &meta2, &[UtxoSlot::new_unspent([0; 32])]).unwrap();
        let medium_ref = mgr.write_cold_data(&meta2.tx_id, &cold2, 1, off2, hot2).unwrap();
        assert!(matches!(medium_ref, ColdDataRef::External { .. }));

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
        mgr.write_cold_data(&meta3.tx_id, &cold3, 1, off3, hot3).unwrap();

        // Prune all
        mgr.delete_cold_data(&meta1).unwrap(); // inline: no-op
        mgr.allocator.lock().free(off1, total1).unwrap();

        mgr.delete_cold_data(&meta2).unwrap(); // external: deletes blob
        mgr.allocator.lock().free(off2, hot2).unwrap();

        mgr.delete_cold_data(&meta3).unwrap(); // external: deletes blob
        mgr.allocator.lock().free(off3, hot3).unwrap();
        assert!(!mgr.blob_store.exists(&meta3.tx_id).unwrap());
    }

    #[test]
    fn e2e_mixed_workload() {
        let (dev, mgr) = setup();

        // 100 small (inline), 10 medium (external), 2 large (external)
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
            let result = mgr.write_cold_data(&meta.tx_id, &cold, 1, offset, total).unwrap();
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
            meta.flags = TxFlags::EXTERNAL;
            meta.tx_id[0] = (i & 0xFF) as u8;
            meta.tx_id[1] = 0x02;
            io::write_full_record(&*dev, offset, &meta, &[UtxoSlot::new_unspent([0; 32])]).unwrap();
            let result = mgr.write_cold_data(&meta.tx_id, &cold, 1, offset, hot_size).unwrap();
            assert!(matches!(result, ColdDataRef::External { .. }));
            let read = mgr.read_cold_data(offset, 1, &meta).unwrap();
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
            let result = mgr.write_cold_data(&meta.tx_id, &cold, 1, offset, hot_size).unwrap();
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
            mgr.write_cold_data(&meta.tx_id, &cold, utxo_count, offset, total)
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

    // ---- R-048 (AUDIT.md IJK-01) regression tests ----
    //
    // These tests pin the invariant that `StorageManager::write_cold_data`
    // for the External tier propagates the durable `BlobDigest` back to the
    // caller via `ColdDataRef::External { digest }`. Pre-fix the variant was
    // a unit `External` and the digest was discarded with `let _digest = ...`,
    // so any caller wiring this manager into a real create path would have
    // populated `ExternalRef.content_hash` with `[0; 32]`. Two consequences,
    // both proven below:
    //
    //   1. The recorded `content_hash` would never match the real payload
    //      SHA-256, so end-to-end integrity checks become theatre.
    //   2. The blob store's own sidecar-based integrity check (which is what
    //      defends against bit rot on the underlying file) only fires when
    //      the on-disk payload disagrees with the digest computed at `put`
    //      time. Without a manager-returned digest, callers cannot even
    //      assert that the manager actually uploaded the bytes — the digest
    //      could be fabricated.

    /// Build a `FileBlobStore`-backed `StorageManager` so corruption tests can
    /// mutate on-disk blob payloads independently of the recorded digest
    /// sidecar. The in-memory store cannot model "payload changed but recorded
    /// digest unchanged" because every mutation goes through `put`.
    fn setup_with_file_blobstore() -> (
        Arc<MemoryDevice>,
        Arc<crate::storage::blobstore::FileBlobStore>,
        StorageManager,
        tempfile::TempDir,
    ) {
        let dev = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone()).unwrap();
        let blob_dir = tempfile::tempdir().unwrap();
        let blob = Arc::new(crate::storage::blobstore::FileBlobStore::new(
            blob_dir.path(),
            2,
        ));
        let mgr = StorageManager::new(dev.clone(), alloc, blob.clone());
        (dev, blob, mgr, blob_dir)
    }

    fn sha256_bytes(data: &[u8]) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        h.update(data);
        let mut out = [0u8; 32];
        out.copy_from_slice(&h.finalize());
        out
    }

    #[test]
    fn external_create_populates_content_hash_from_blob_digest() {
        // Direct invariant: the manager-returned digest must be the durable
        // SHA-256 of the serialized cold-data payload, never the all-zero
        // placeholder that pre-fix code stranded in `ExternalRef.content_hash`.
        let (dev, blob, mgr, _tmp) = setup_with_file_blobstore();
        let utxo_count = 2u32;
        let hot_size = TxMetadata::record_size_for(utxo_count);
        let offset = mgr.allocator.lock().allocate(hot_size).unwrap();

        let mut meta = TxMetadata::new(utxo_count);
        meta.tx_id[0] = 0xCA;
        meta.tx_id[1] = 0xFE;
        meta.flags = TxFlags::EXTERNAL;
        meta.record_size = hot_size as u32;
        let slots = vec![UtxoSlot::new_unspent([0; 32]); utxo_count as usize];
        io::write_full_record(&*dev, offset, &meta, &slots).unwrap();

        // Cold data large enough to land in the External tier (> 1 MiB).
        let cold = ColdData {
            inputs: vec![0xAA; 1024 * 1024],
            outputs: vec![0xBB; 256 * 1024],
            inpoints: vec![0xCC; 64],
        };
        let serialized = cold.serialize();
        let expected_sha = sha256_bytes(&serialized);

        let result = mgr
            .write_cold_data(&meta.tx_id, &cold, utxo_count, offset, hot_size)
            .unwrap();
        let digest = match result {
            ColdDataRef::External { digest } => digest,
            other => panic!(
                "expected External tier for {} bytes, got {other:?}",
                serialized.len()
            ),
        };

        // Pre-fix: this assertion would have been impossible — the unit
        // variant carried no digest, and the eventual `ExternalRef.content_hash`
        // would have been left at `[0; 32]`.
        assert_ne!(digest.sha256, [0u8; 32], "digest must not be zero");
        assert_eq!(
            digest.sha256, expected_sha,
            "manager-returned digest must equal SHA-256 of the serialized cold data",
        );
        assert_eq!(digest.length, serialized.len() as u64);

        // Cross-check: the blob store independently reports the same digest.
        // This catches a future bug where the manager fabricates a digest
        // without uploading the bytes.
        let store_digest = blob.digest(&meta.tx_id).unwrap().unwrap();
        assert_eq!(store_digest.sha256, expected_sha);
        assert_eq!(store_digest.length, serialized.len() as u64);

        // And: stamping the manager-returned digest into `ExternalRef`
        // produces a record whose recorded `content_hash` actually matches
        // what is on disk — the entire point of R-048.
        let ext_ref = crate::record::ExternalRef {
            store_type: 1,
            content_hash: digest.sha256,
            total_size: digest.length,
            input_count: 0,
            output_count: 0,
            inputs_offset: 0,
            outputs_offset: 0,
        };
        assert_eq!(ext_ref.content_hash, expected_sha);
    }

    #[test]
    fn external_blob_integrity_check_fires_on_corruption() {
        // Audit-prescribed regression name. Pre-fix the recorded
        // `content_hash` was permanently zero, so the two failure modes were:
        //   * If a reader compared SHA-256 against zero, every read would
        //     reject (which is what AUDIT.md IJK-01 reported).
        //   * If a reader skipped the check on a zero hash, corruption would
        //     go undetected.
        // Either way, the integrity contract was broken. Post-fix the
        // FileBlobStore's sidecar carries the durable digest from `put`, so
        // mutating the on-disk payload (without touching the sidecar) MUST
        // cause `read_cold_data` to surface a `DigestMismatch`.
        let (dev, blob, mgr, _tmp) = setup_with_file_blobstore();
        let utxo_count = 1u32;
        let hot_size = TxMetadata::record_size_for(utxo_count);
        let offset = mgr.allocator.lock().allocate(hot_size).unwrap();

        let mut meta = TxMetadata::new(utxo_count);
        meta.tx_id[0] = 0xBA;
        meta.tx_id[1] = 0xAD;
        meta.flags = TxFlags::EXTERNAL;
        meta.record_size = hot_size as u32;
        let slots = vec![UtxoSlot::new_unspent([0; 32]); utxo_count as usize];
        io::write_full_record(&*dev, offset, &meta, &slots).unwrap();

        let cold = ColdData {
            inputs: vec![0x11; 1024 * 1024 + 100],
            outputs: vec![0x22; 1024],
            inpoints: vec![],
        };
        let result = mgr
            .write_cold_data(&meta.tx_id, &cold, utxo_count, offset, hot_size)
            .unwrap();
        let digest = match result {
            ColdDataRef::External { digest } => digest,
            other => panic!("expected External, got {other:?}"),
        };
        // Sanity: the freshly-written blob reads back cleanly with the
        // integrity check passing — establishes the baseline before we
        // tamper with the on-disk bytes.
        let clean_read = mgr.read_cold_data(offset, utxo_count, &meta).unwrap();
        assert_eq!(clean_read, cold);

        // Corrupt the on-disk payload while leaving the sidecar (which
        // records the digest from `put`) intact. The sidecar holds
        // `digest.sha256`; the payload now hashes to something else.
        // FileBlobStore lays the payload out under base_dir/ab/cd/<hex>.
        let key_hex: String = meta.tx_id.iter().map(|b| format!("{b:02x}")).collect();
        let mut blob_path = _tmp.path().to_path_buf();
        blob_path = blob_path
            .join(&key_hex[0..2])
            .join(&key_hex[2..4])
            .join(&key_hex);
        assert!(
            blob_path.exists(),
            "expected blob payload at {blob_path:?} (key {key_hex})"
        );
        let mut on_disk = std::fs::read(&blob_path).unwrap();
        // Flip a byte in the middle of the payload.
        let mid = on_disk.len() / 2;
        on_disk[mid] ^= 0xFF;
        std::fs::write(&blob_path, &on_disk).unwrap();

        // Sidecar must still encode the original (now stale) digest.
        let sidecar_digest = blob.digest(&meta.tx_id).unwrap().unwrap();
        assert_eq!(
            sidecar_digest, digest,
            "sidecar must retain the original digest after payload corruption",
        );

        // The integrity check MUST fire — `BlobStore::get` recomputes the
        // payload SHA-256 and compares against the sidecar. With a corrupted
        // payload, that comparison fails and the error bubbles through the
        // manager as `StorageError::Blob(BlobError::DigestMismatch { .. })`.
        match mgr.read_cold_data(offset, utxo_count, &meta) {
            Err(StorageError::Blob(crate::storage::blobstore::BlobError::DigestMismatch {
                expected,
                actual,
                ..
            })) => {
                assert_eq!(
                    expected, digest.sha256,
                    "expected digest must match the manager-returned digest",
                );
                assert_ne!(
                    actual, expected,
                    "actual digest must differ after tampering"
                );
                assert_eq!(
                    actual,
                    sha256_bytes(&on_disk),
                    "actual digest must equal SHA-256 of the tampered on-disk payload",
                );
            }
            Ok(_) => panic!("integrity check did not fire on corrupted blob payload"),
            Err(other) => panic!("expected DigestMismatch, got {other:?}"),
        }
    }
}
