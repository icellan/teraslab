//! Storage tier definitions for cold data placement.

use crate::record::{METADATA_SIZE, UTXO_SLOT_SIZE};
use crate::storage::blobstore::BlobDigest;

/// Byte offset, from the start of a record, where inline cold data begins:
/// `METADATA_SIZE + utxo_count * UTXO_SLOT_SIZE`.
///
/// This is the production read path's anchor for inline cold data (used by
/// [`crate::ops::engine::Engine::read_cold_data`]). The on-disk layout places
/// the metadata header first, then the fixed-size UTXO slots, then any inline
/// cold data — so the cold-data region starts immediately after the slots.
pub fn inline_cold_offset(utxo_count: u32) -> u64 {
    METADATA_SIZE as u64 + utxo_count as u64 * UTXO_SLOT_SIZE as u64
}

/// Cold data up to and including this serialized size (`<=` 8 KiB = 8192
/// bytes, i.e. 8180 bytes of user data plus the 12-byte `ColdData` length
/// prefixes) is stored inline in the same NVMe allocation as the hot record
/// (metadata + UTXO slots + cold data in one write).
pub const INLINE_THRESHOLD: usize = 8 * 1024; // 8 KiB, inclusive

/// Which storage tier to use for the given serialized cold data size.
/// Exactly [`INLINE_THRESHOLD`] bytes is still [`StorageTier::Inline`];
/// above it, cold data goes to an external blob store.
///
/// Earlier phase documents described a middle "separate NVMe" tier. The
/// production record metadata has no durable fields for a separate cold-data
/// offset/length, so the middle tier is deliberately removed instead of
/// returning an unreferencable allocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageTier {
    /// Cold data inline at `record_offset + METADATA_SIZE + utxo_count * 69`.
    Inline,
    /// Cold data in an external blob store (file, S3, MinIO).
    External,
}

/// Determine the storage tier for a given serialized cold data size
/// (inclusive boundary: `data_size <= INLINE_THRESHOLD` is Inline).
pub fn tier_for_size(data_size: usize) -> StorageTier {
    if data_size <= INLINE_THRESHOLD {
        StorageTier::Inline
    } else {
        StorageTier::External
    }
}

/// Result of a cold data write, indicating where the data was placed.
#[derive(Debug, Clone, PartialEq)]
pub enum ColdDataRef {
    /// Written inline at deterministic offset. No extra state needed.
    Inline { cold_size: u32 },
    /// Uploaded to the external blob store.
    ///
    /// Carries the [`BlobDigest`] returned by [`crate::storage::blobstore::BlobStore::put`]
    /// so the caller can stamp the actual content SHA-256 and length into the
    /// record's [`crate::record::ExternalRef`]. Without this digest the
    /// `content_hash` on the record would remain zero and any subsequent
    /// integrity check on the blob payload would either trivially pass (if the
    /// reader compares against zero) or trivially fail (if the reader compares
    /// against the real digest). See R-048 for the
    /// regression this prevents.
    External { digest: BlobDigest },
    /// No cold data.
    None,
}

/// Parsed cold data from a record.
#[derive(Debug, Clone, PartialEq)]
pub struct ColdData {
    pub inputs: Vec<u8>,
    pub outputs: Vec<u8>,
    pub inpoints: Vec<u8>,
}

impl ColdData {
    /// Serialize cold data with length prefixes.
    ///
    /// Format: `[inputs_len:4 LE][inputs][outputs_len:4 LE][outputs][inpoints_len:4 LE][inpoints]`
    pub fn serialize(&self) -> Vec<u8> {
        let total = 12 + self.inputs.len() + self.outputs.len() + self.inpoints.len();
        let mut buf = Vec::with_capacity(total);
        buf.extend_from_slice(&(self.inputs.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.inputs);
        buf.extend_from_slice(&(self.outputs.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.outputs);
        buf.extend_from_slice(&(self.inpoints.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.inpoints);
        buf
    }

    /// Deserialize cold data from length-prefixed bytes.
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 12 {
            return None;
        }
        let inputs_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let mut pos = 4;
        if pos + inputs_len > data.len() {
            return None;
        }
        let inputs = data[pos..pos + inputs_len].to_vec();
        pos += inputs_len;

        if pos + 4 > data.len() {
            return None;
        }
        let outputs_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + outputs_len > data.len() {
            return None;
        }
        let outputs = data[pos..pos + outputs_len].to_vec();
        pos += outputs_len;

        if pos + 4 > data.len() {
            return None;
        }
        let inpoints_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + inpoints_len > data.len() {
            return None;
        }
        let inpoints = data[pos..pos + inpoints_len].to_vec();

        Some(Self {
            inputs,
            outputs,
            inpoints,
        })
    }

    /// Total serialized size including length prefixes.
    pub fn serialized_size(&self) -> usize {
        12 + self.inputs.len() + self.outputs.len() + self.inpoints.len()
    }

    /// Whether all components are empty.
    pub fn is_empty(&self) -> bool {
        self.inputs.is_empty() && self.outputs.is_empty() && self.inpoints.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{METADATA_SIZE, UTXO_SLOT_SIZE};

    #[test]
    fn inline_cold_offset_matches_layout_formula() {
        // The inline cold-data region begins immediately after the metadata
        // header and the fixed-size UTXO slots.
        for utxo_count in [0u32, 1, 7, 10, 1000] {
            assert_eq!(
                inline_cold_offset(utxo_count),
                METADATA_SIZE as u64 + utxo_count as u64 * UTXO_SLOT_SIZE as u64,
            );
        }
    }

    #[test]
    fn tier_inline() {
        assert_eq!(tier_for_size(100), StorageTier::Inline);
        assert_eq!(tier_for_size(8000), StorageTier::Inline);
        assert_eq!(tier_for_size(INLINE_THRESHOLD), StorageTier::Inline);
    }

    #[test]
    fn tier_separate() {
        assert_eq!(tier_for_size(INLINE_THRESHOLD + 1), StorageTier::External);
        assert_eq!(tier_for_size(500 * 1024), StorageTier::External);
    }

    #[test]
    fn tier_external() {
        assert_eq!(tier_for_size(INLINE_THRESHOLD + 1), StorageTier::External);
        assert_eq!(tier_for_size(320 * 1024 * 1024), StorageTier::External);
    }

    #[test]
    fn cold_data_round_trip() {
        let cd = ColdData {
            inputs: vec![1, 2, 3, 4],
            outputs: vec![0xA, 0xB, 0xC],
            inpoints: vec![0xD, 0xE],
        };
        let bytes = cd.serialize();
        let decoded = ColdData::deserialize(&bytes).unwrap();
        assert_eq!(decoded, cd);
    }

    #[test]
    fn cold_data_empty() {
        let cd = ColdData {
            inputs: vec![],
            outputs: vec![],
            inpoints: vec![],
        };
        assert!(cd.is_empty());
        let bytes = cd.serialize();
        assert_eq!(bytes.len(), 12);
        let decoded = ColdData::deserialize(&bytes).unwrap();
        assert_eq!(decoded, cd);
    }

    #[test]
    fn cold_data_serialized_size() {
        let cd = ColdData {
            inputs: vec![0; 100],
            outputs: vec![0; 200],
            inpoints: vec![0; 50],
        };
        assert_eq!(cd.serialized_size(), 12 + 100 + 200 + 50);
    }
}
