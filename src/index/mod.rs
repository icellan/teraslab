//! Primary and secondary in-memory indexes for TeraSlab.
//!
//! - [`Index`]: primary hash index mapping `TxKey` → `TxIndexEntry` (device location)
//! - [`DahIndex`]: secondary index for `delete_at_height` pruner queries
//! - [`UnminedIndex`]: secondary index for `unmined_since` pruner queries

pub mod dah_index;
pub mod hashtable;
pub mod unmined_index;

pub use dah_index::DahIndex;
pub use hashtable::{TxIndexEntry, TxKey};
pub use unmined_index::UnminedIndex;

use crate::allocator::SlotAllocator;
use crate::device::{AlignedBuf, BlockDevice};
use crate::record::{METADATA_MAGIC, METADATA_SIZE, TxMetadata};
use hashtable::HashTable;
use thiserror::Error;

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors from index operations.
#[derive(Error, Debug)]
pub enum IndexError {
    /// Hash table error.
    #[error("hash table error: {0}")]
    HashTable(#[from] hashtable::HashTableError),

    /// Snapshot I/O error.
    #[error("snapshot I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Snapshot checksum mismatch.
    #[error("snapshot checksum mismatch: expected {expected:#010x}, got {actual:#010x}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    /// Snapshot file is truncated or has an invalid header.
    #[error("snapshot format error: {detail}")]
    FormatError { detail: String },

    /// Device I/O error during rebuild.
    #[error("device error during rebuild: {0}")]
    Device(#[from] crate::device::DeviceError),
}

pub type Result<T> = std::result::Result<T, IndexError>;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SNAPSHOT_MAGIC: [u8; 4] = *b"TSIX"; // TeraSlab IndeX
const SNAPSHOT_VERSION: u32 = 1;

const DAH_SECTION_MAGIC: [u8; 4] = *b"DAHI";
const UNMINED_SECTION_MAGIC: [u8; 4] = *b"UNMI";
const SECONDARY_VERSION: u32 = 1;

// Per-entry sizes in the snapshot file
const PRIMARY_ENTRY_SIZE: usize = 32 + 1 + 8 + 4 + 1 + 1 + 4 + 4 + 4 + 4; // TxKey + TxIndexEntry = 63
const SECONDARY_ENTRY_SIZE: usize = 4 + 32; // height + txid = 36

// ---------------------------------------------------------------------------
// IndexStats
// ---------------------------------------------------------------------------

/// Statistics for monitoring the primary index.
#[derive(Debug, Clone)]
pub struct IndexStats {
    /// Number of entries in the primary index.
    pub entry_count: usize,
    /// Total bucket capacity.
    pub capacity: usize,
    /// Load factor (0.0 – 1.0).
    pub load_factor: f64,
    /// Whether 2 MB hugepages are backing the hash table.
    pub hugepage_enabled: bool,
    /// Maximum observed probe distance.
    pub max_probe_distance: usize,
    /// Approximate memory usage in bytes.
    pub memory_bytes: usize,
}

/// Flags indicating which secondary indexes need rebuilding after restore.
#[derive(Debug, Clone, Default)]
pub struct RestoreFlags {
    /// The DAH section was missing or corrupt — rebuild from device scan.
    pub dah_needs_rebuild: bool,
    /// The unmined section was missing or corrupt — replay redo log or scan.
    pub unmined_needs_rebuild: bool,
}

// ---------------------------------------------------------------------------
// Index
// ---------------------------------------------------------------------------

/// Primary hash index managing `TxKey` → `TxIndexEntry` lookups.
///
/// Wraps a [`HashTable`] with auto-resize and snapshot/restore capabilities.
#[derive(Debug)]
pub struct Index {
    table: HashTable,
    resize_threshold: f64,
}

impl Index {
    /// Create a new index sized for `expected_records` entries.
    ///
    /// The hash table is pre-allocated to `expected_records / 0.7` buckets
    /// (rounded to the next power of two) to keep the load factor below 70%.
    pub fn new(expected_records: usize) -> Result<Self> {
        let capacity = (expected_records as f64 / 0.7).ceil() as usize;
        let table = HashTable::new(capacity.max(16))?;
        Ok(Self {
            table,
            resize_threshold: 0.7,
        })
    }

    /// Look up where a transaction's record lives on disk.
    pub fn lookup(&self, key: &TxKey) -> Option<TxIndexEntry> {
        self.table.get_entry(key)
    }

    /// Register a newly created transaction record in the index.
    ///
    /// Automatically resizes the hash table if the load factor exceeds
    /// the threshold (default 0.7).
    pub fn register(&mut self, key: TxKey, entry: TxIndexEntry) -> Result<()> {
        self.table.insert(key, entry)?;
        if self.table.load_factor() > self.resize_threshold {
            self.table.resize(self.table.capacity() * 2)?;
        }
        Ok(())
    }

    /// Remove a transaction from the index (on deletion/pruning).
    pub fn unregister(&mut self, key: &TxKey) -> Option<TxIndexEntry> {
        self.table.remove(key)
    }

    /// Update the cached fields in the bucket for `key`.
    /// Returns `true` if the key was found and updated.
    #[allow(clippy::too_many_arguments)]
    pub fn update_cached_fields(
        &mut self,
        key: &TxKey,
        tx_flags: u8,
        block_entry_count: u8,
        spent_utxos: u32,
        dah_or_preserve: u32,
        unmined_since: u32,
        generation: u32,
    ) -> bool {
        self.table
            .update_cached_fields(key, tx_flags, block_entry_count, spent_utxos, dah_or_preserve, unmined_since, generation)
    }

    /// Number of entries in the primary index.
    pub fn len(&self) -> usize {
        self.table.len()
    }

    /// Whether the primary index is empty.
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// Iterate over all `(TxKey, TxIndexEntry)` pairs in the primary index.
    ///
    /// Used for migration scanning and index snapshots.
    pub fn iter(&self) -> impl Iterator<Item = (TxKey, TxIndexEntry)> + '_ {
        self.table.iter()
    }

    /// Statistics for monitoring.
    pub fn stats(&self) -> IndexStats {
        IndexStats {
            entry_count: self.table.len(),
            capacity: self.table.capacity(),
            load_factor: self.table.load_factor(),
            hugepage_enabled: self.table.hugepage_enabled(),
            max_probe_distance: self.table.max_probe_distance(),
            memory_bytes: self.table.memory_bytes(),
        }
    }

    // -----------------------------------------------------------------------
    // Snapshot (primary index only)
    // -----------------------------------------------------------------------

    /// Snapshot the primary index to a file.
    ///
    /// Format: `[magic(4)][version(4)][entry_count(8)][capacity(8)]`
    /// followed by `entry_count` serialized entries, followed by a CRC32.
    /// Written atomically via temp file + rename.
    pub fn snapshot(&self, path: &std::path::Path) -> Result<()> {
        let tmp_path = path.with_extension("tmp");
        let data = self.serialize_primary();
        std::fs::write(&tmp_path, &data)?;
        // fsync the file
        let f = std::fs::File::open(&tmp_path)?;
        f.sync_all()?;
        drop(f);
        std::fs::rename(&tmp_path, path)?;
        Ok(())
    }

    /// Restore the primary index from a snapshot file.
    pub fn restore(path: &std::path::Path) -> Result<Self> {
        let data = std::fs::read(path)?;
        Self::deserialize_primary(&data)
    }

    // -----------------------------------------------------------------------
    // Snapshot all (primary + secondary indexes)
    // -----------------------------------------------------------------------

    /// Snapshot primary index + both secondary indexes to a single file.
    pub fn snapshot_all(
        &self,
        dah: &DahIndex,
        unmined: &UnminedIndex,
        path: &std::path::Path,
    ) -> Result<()> {
        let tmp_path = path.with_extension("tmp");
        let mut data = self.serialize_primary();
        data.extend_from_slice(&serialize_secondary(&DAH_SECTION_MAGIC, dah.iter()));
        data.extend_from_slice(&serialize_secondary(
            &UNMINED_SECTION_MAGIC,
            unmined.iter(),
        ));
        std::fs::write(&tmp_path, &data)?;
        let f = std::fs::File::open(&tmp_path)?;
        f.sync_all()?;
        drop(f);
        std::fs::rename(&tmp_path, path)?;
        Ok(())
    }

    /// Restore all indexes from a snapshot file.
    ///
    /// If a secondary index section is corrupt, the corresponding index is
    /// returned empty and the rebuild flag is set.
    pub fn restore_all(
        path: &std::path::Path,
    ) -> Result<(Self, DahIndex, UnminedIndex, RestoreFlags)> {
        let data = std::fs::read(path)?;
        let (index, primary_end) = Self::deserialize_primary_with_offset(&data)?;

        let mut flags = RestoreFlags::default();
        let mut dah = DahIndex::new();
        let mut unmined = UnminedIndex::new();

        let remaining = &data[primary_end..];

        // Try to parse DAH section
        let dah_end = match deserialize_secondary(remaining, &DAH_SECTION_MAGIC) {
            Ok((entries, consumed)) => {
                for (h, k) in entries {
                    dah.insert(h, k);
                }
                consumed
            }
            Err(_) => {
                flags.dah_needs_rebuild = true;
                // Can't determine where DAH section ends — unmined section
                // may also be unreadable.
                flags.unmined_needs_rebuild = true;
                return Ok((index, dah, unmined, flags));
            }
        };

        // Try to parse unmined section
        let unmined_remaining = &remaining[dah_end..];
        match deserialize_secondary(unmined_remaining, &UNMINED_SECTION_MAGIC) {
            Ok((entries, _)) => {
                for (h, k) in entries {
                    unmined.insert(h, k);
                }
            }
            Err(_) => {
                flags.unmined_needs_rebuild = true;
            }
        }

        Ok((index, dah, unmined, flags))
    }

    // -----------------------------------------------------------------------
    // Rebuild from device scan
    // -----------------------------------------------------------------------

    /// Rebuild the primary index by scanning all records on the device.
    ///
    /// This is the cold-start path when no snapshot exists. Reads every
    /// record header between `allocator.data_region_start()` and
    /// `allocator.next_offset()`, checking for valid magic numbers.
    pub fn rebuild(
        device: &dyn BlockDevice,
        allocator: &SlotAllocator,
    ) -> Result<Self> {
        let mut index = Self::new(1024)?;
        let align = allocator.device_alignment();
        let start = allocator.data_region_start();
        let end = allocator.next_offset();

        let read_size = align.max(METADATA_SIZE);
        let aligned_read = read_size.div_ceil(align) * align;

        let mut offset = start;
        while offset + aligned_read as u64 <= end {
            let mut buf = AlignedBuf::new(aligned_read, align);
            if device.pread(&mut buf, offset).is_err() {
                offset += align as u64;
                continue;
            }

            let meta = TxMetadata::from_bytes(&buf[..METADATA_SIZE]);
            if { meta.magic } != METADATA_MAGIC {
                offset += align as u64;
                continue;
            }

            let record_size = { meta.record_size } as u64;
            if record_size == 0 {
                offset += align as u64;
                continue;
            }

            let key = TxKey { txid: meta.tx_id };
            let entry = TxIndexEntry {
                device_id: 0,
                record_offset: offset,
                utxo_count: { meta.utxo_count },
                block_entry_count: meta.block_entry_count,
                tx_flags: meta.flags.bits(),
                spent_utxos: meta.spent_utxos,
                dah_or_preserve: 0,
                unmined_since: 0,
                generation: 0,
            };
            index.register(key, entry)?;

            // Advance past this record (aligned)
            let record_aligned = (record_size as usize).div_ceil(align) * align;
            offset += record_aligned as u64;
        }

        Ok(index)
    }

    /// Rebuild secondary indexes by scanning all records on the device.
    ///
    /// Returns `(DahIndex, UnminedIndex)` populated from record metadata.
    pub fn rebuild_secondary(
        device: &dyn BlockDevice,
        allocator: &SlotAllocator,
    ) -> Result<(DahIndex, UnminedIndex)> {
        let mut dah = DahIndex::new();
        let mut unmined = UnminedIndex::new();
        let align = allocator.device_alignment();
        let start = allocator.data_region_start();
        let end = allocator.next_offset();

        let read_size = align.max(METADATA_SIZE);
        let aligned_read = read_size.div_ceil(align) * align;

        let mut offset = start;
        while offset + aligned_read as u64 <= end {
            let mut buf = AlignedBuf::new(aligned_read, align);
            if device.pread(&mut buf, offset).is_err() {
                offset += align as u64;
                continue;
            }

            let meta = TxMetadata::from_bytes(&buf[..METADATA_SIZE]);
            if { meta.magic } != METADATA_MAGIC {
                offset += align as u64;
                continue;
            }

            let record_size = { meta.record_size } as u64;
            if record_size == 0 {
                offset += align as u64;
                continue;
            }

            let key = TxKey { txid: meta.tx_id };
            let dah_val = { meta.delete_at_height };
            let unmined_val = { meta.unmined_since };

            if dah_val != 0 {
                dah.insert(dah_val, key);
            }
            if unmined_val != 0 {
                unmined.insert(unmined_val, key);
            }

            let record_aligned = (record_size as usize).div_ceil(align) * align;
            offset += record_aligned as u64;
        }

        Ok((dah, unmined))
    }

    // -----------------------------------------------------------------------
    // Serialization helpers
    // -----------------------------------------------------------------------

    fn serialize_primary(&self) -> Vec<u8> {
        let count = self.table.len() as u64;
        let capacity = self.table.capacity() as u64;
        let header_size = 4 + 4 + 8 + 8; // magic + version + count + capacity
        let body_size = self.table.len() * PRIMARY_ENTRY_SIZE;
        let total = header_size + body_size + 4; // + checksum

        let mut buf = Vec::with_capacity(total);
        buf.extend_from_slice(&SNAPSHOT_MAGIC);
        buf.extend_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
        buf.extend_from_slice(&count.to_le_bytes());
        buf.extend_from_slice(&capacity.to_le_bytes());

        for (key, entry) in self.table.iter() {
            buf.extend_from_slice(&key.txid);
            buf.push(entry.device_id);
            buf.extend_from_slice(&entry.record_offset.to_le_bytes());
            buf.extend_from_slice(&entry.utxo_count.to_le_bytes());
            buf.push(entry.block_entry_count);
            buf.push(entry.tx_flags);
            buf.extend_from_slice(&entry.spent_utxos.to_le_bytes());
            buf.extend_from_slice(&entry.dah_or_preserve.to_le_bytes());
            buf.extend_from_slice(&entry.unmined_since.to_le_bytes());
            buf.extend_from_slice(&entry.generation.to_le_bytes());
        }

        let checksum = crc32fast::hash(&buf);
        buf.extend_from_slice(&checksum.to_le_bytes());
        buf
    }

    fn deserialize_primary(data: &[u8]) -> Result<Self> {
        let (index, _) = Self::deserialize_primary_with_offset(data)?;
        Ok(index)
    }

    fn deserialize_primary_with_offset(data: &[u8]) -> Result<(Self, usize)> {
        let header_size = 4 + 4 + 8 + 8;
        if data.len() < header_size + 4 {
            return Err(IndexError::FormatError {
                detail: "file too small for header".into(),
            });
        }

        if data[0..4] != SNAPSHOT_MAGIC {
            return Err(IndexError::FormatError {
                detail: "invalid magic".into(),
            });
        }

        let _version = u32::from_le_bytes(data[4..8].try_into().unwrap());
        let count = u64::from_le_bytes(data[8..16].try_into().unwrap()) as usize;
        let capacity = u64::from_le_bytes(data[16..24].try_into().unwrap()) as usize;

        let body_size = count * PRIMARY_ENTRY_SIZE;
        let total = header_size + body_size + 4;
        if data.len() < total {
            return Err(IndexError::FormatError {
                detail: format!(
                    "file too small: need {total} bytes for {count} entries, have {}",
                    data.len()
                ),
            });
        }

        // Verify checksum
        let stored_checksum =
            u32::from_le_bytes(data[total - 4..total].try_into().unwrap());
        let computed_checksum = crc32fast::hash(&data[..total - 4]);
        if stored_checksum != computed_checksum {
            return Err(IndexError::ChecksumMismatch {
                expected: stored_checksum,
                actual: computed_checksum,
            });
        }

        let mut index = Self::new(capacity.max(count))?;
        let entries_start = header_size;
        for i in 0..count {
            let base = entries_start + i * PRIMARY_ENTRY_SIZE;
            let mut txid = [0u8; 32];
            txid.copy_from_slice(&data[base..base + 32]);
            let key = TxKey { txid };

            let entry = TxIndexEntry {
                device_id: data[base + 32],
                record_offset: u64::from_le_bytes(
                    data[base + 33..base + 41].try_into().unwrap(),
                ),
                utxo_count: u32::from_le_bytes(
                    data[base + 41..base + 45].try_into().unwrap(),
                ),
                block_entry_count: data[base + 45],
                tx_flags: data[base + 46],
                spent_utxos: u32::from_le_bytes(
                    data[base + 47..base + 51].try_into().unwrap(),
                ),
                dah_or_preserve: u32::from_le_bytes(
                    data[base + 51..base + 55].try_into().unwrap(),
                ),
                unmined_since: u32::from_le_bytes(
                    data[base + 55..base + 59].try_into().unwrap(),
                ),
                generation: u32::from_le_bytes(
                    data[base + 59..base + 63].try_into().unwrap(),
                ),
            };
            index.register(key, entry)?;
        }

        Ok((index, total))
    }
}

// ---------------------------------------------------------------------------
// Secondary index serialization helpers
// ---------------------------------------------------------------------------

fn serialize_secondary(
    magic: &[u8; 4],
    entries: impl Iterator<Item = (u32, TxKey)>,
) -> Vec<u8> {
    let collected: Vec<_> = entries.collect();
    let count = collected.len() as u64;
    let header_size = 4 + 4 + 8; // magic + version + count
    let body_size = collected.len() * SECONDARY_ENTRY_SIZE;
    let total = header_size + body_size + 4;

    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(magic);
    buf.extend_from_slice(&SECONDARY_VERSION.to_le_bytes());
    buf.extend_from_slice(&count.to_le_bytes());

    for (height, key) in &collected {
        buf.extend_from_slice(&height.to_le_bytes());
        buf.extend_from_slice(&key.txid);
    }

    let checksum = crc32fast::hash(&buf);
    buf.extend_from_slice(&checksum.to_le_bytes());
    buf
}

fn deserialize_secondary(
    data: &[u8],
    expected_magic: &[u8; 4],
) -> Result<(Vec<(u32, TxKey)>, usize)> {
    let header_size = 4 + 4 + 8;
    if data.len() < header_size + 4 {
        return Err(IndexError::FormatError {
            detail: "secondary section too small".into(),
        });
    }

    if &data[0..4] != expected_magic {
        return Err(IndexError::FormatError {
            detail: format!(
                "secondary magic mismatch: expected {:?}, got {:?}",
                expected_magic,
                &data[0..4]
            ),
        });
    }

    let _version = u32::from_le_bytes(data[4..8].try_into().unwrap());
    let count = u64::from_le_bytes(data[8..16].try_into().unwrap()) as usize;
    let body_size = count * SECONDARY_ENTRY_SIZE;
    let total = header_size + body_size + 4;

    if data.len() < total {
        return Err(IndexError::FormatError {
            detail: "secondary section truncated".into(),
        });
    }

    let stored_checksum =
        u32::from_le_bytes(data[total - 4..total].try_into().unwrap());
    let computed_checksum = crc32fast::hash(&data[..total - 4]);
    if stored_checksum != computed_checksum {
        return Err(IndexError::ChecksumMismatch {
            expected: stored_checksum,
            actual: computed_checksum,
        });
    }

    let mut entries = Vec::with_capacity(count);
    for i in 0..count {
        let base = header_size + i * SECONDARY_ENTRY_SIZE;
        let height = u32::from_le_bytes(data[base..base + 4].try_into().unwrap());
        let mut txid = [0u8; 32];
        txid.copy_from_slice(&data[base + 4..base + 36]);
        entries.push((height, TxKey { txid }));
    }

    Ok((entries, total))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::MemoryDevice;
    use crate::io::write_full_record;
    use crate::record::{TxMetadata, UtxoSlot};
    use std::sync::Arc;

    fn make_key(n: u64) -> TxKey {
        let mut txid = [0u8; 32];
        txid[0..8].copy_from_slice(&n.to_le_bytes());
        txid[8..16].copy_from_slice(&(n.wrapping_mul(0x9E3779B97F4A7C15)).to_le_bytes());
        TxKey { txid }
    }

    fn make_entry(offset: u64) -> TxIndexEntry {
        TxIndexEntry {
            device_id: 0,
            record_offset: offset,
            utxo_count: 10,
            block_entry_count: 0,
            tx_flags: 0,
            spent_utxos: 0,
            dah_or_preserve: 0,
            unmined_since: 0,
            generation: 0,
        }
    }

    // -- Snapshot and restore tests --

    #[test]
    fn snapshot_restore_1000() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("index.snap");

        let mut idx = Index::new(2000).unwrap();
        for i in 0..1000u64 {
            idx.register(make_key(i), make_entry(i * 100)).unwrap();
        }

        idx.snapshot(&snap_path).unwrap();
        let restored = Index::restore(&snap_path).unwrap();

        assert_eq!(restored.len(), 1000);
        for i in 0..1000u64 {
            let e = restored.lookup(&make_key(i)).expect("entry should exist");
            assert_eq!(e.record_offset, i * 100);
        }
    }

    #[test]
    fn snapshot_checksum_verified() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("index.snap");

        let mut idx = Index::new(100).unwrap();
        idx.register(make_key(1), make_entry(100)).unwrap();
        idx.snapshot(&snap_path).unwrap();

        // Corrupt one byte
        let mut data = std::fs::read(&snap_path).unwrap();
        let mid = data.len() / 2;
        data[mid] ^= 0xFF;
        std::fs::write(&snap_path, &data).unwrap();

        let result = Index::restore(&snap_path);
        assert!(result.is_err());
        match result.unwrap_err() {
            IndexError::ChecksumMismatch { .. } => {}
            other => panic!("expected ChecksumMismatch, got {other:?}"),
        }
    }

    #[test]
    fn snapshot_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("index.snap");

        let mut idx = Index::new(100).unwrap();
        idx.register(make_key(1), make_entry(100)).unwrap();
        idx.snapshot(&snap_path).unwrap();

        // Truncate
        let data = std::fs::read(&snap_path).unwrap();
        std::fs::write(&snap_path, &data[..data.len() / 2]).unwrap();

        let result = Index::restore(&snap_path);
        assert!(result.is_err());
    }

    #[test]
    fn snapshot_empty_table() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("index.snap");

        let idx = Index::new(16).unwrap();
        idx.snapshot(&snap_path).unwrap();

        let restored = Index::restore(&snap_path).unwrap();
        assert_eq!(restored.len(), 0);
    }

    #[test]
    fn snapshot_nonwritable_path() {
        let idx = Index::new(16).unwrap();
        let result = idx.snapshot(std::path::Path::new("/nonexistent/dir/snap"));
        assert!(result.is_err());
    }

    // -- Snapshot all with secondary indexes --

    #[test]
    fn snapshot_all_restore_all() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("all.snap");

        let mut idx = Index::new(100).unwrap();
        for i in 0..50u64 {
            idx.register(make_key(i), make_entry(i * 100)).unwrap();
        }

        let mut dah = DahIndex::new();
        dah.insert(100, make_key(1));
        dah.insert(200, make_key(2));

        let mut unmined = UnminedIndex::new();
        unmined.insert(500, make_key(3));
        unmined.insert(600, make_key(4));

        idx.snapshot_all(&dah, &unmined, &snap_path).unwrap();

        let (restored_idx, restored_dah, restored_unmined, flags) =
            Index::restore_all(&snap_path).unwrap();

        assert!(!flags.dah_needs_rebuild);
        assert!(!flags.unmined_needs_rebuild);
        assert_eq!(restored_idx.len(), 50);
        assert_eq!(restored_dah.len(), 2);
        assert_eq!(restored_unmined.len(), 2);
        assert_eq!(restored_dah.range_query(200).len(), 2);
        assert_eq!(restored_unmined.range_query(600).len(), 2);
    }

    #[test]
    fn snapshot_all_corrupt_dah_section() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("all.snap");

        let mut idx = Index::new(100).unwrap();
        idx.register(make_key(1), make_entry(100)).unwrap();

        let mut dah = DahIndex::new();
        dah.insert(100, make_key(1));

        let unmined = UnminedIndex::new();

        idx.snapshot_all(&dah, &unmined, &snap_path).unwrap();

        // Corrupt the DAH section (after primary index data)
        let mut data = std::fs::read(&snap_path).unwrap();
        // Find DAHI magic and corrupt it
        if let Some(pos) = data
            .windows(4)
            .position(|w| w == b"DAHI")
        {
            data[pos + 10] ^= 0xFF; // Corrupt a data byte
        }
        std::fs::write(&snap_path, &data).unwrap();

        let (restored_idx, restored_dah, _restored_unmined, flags) =
            Index::restore_all(&snap_path).unwrap();

        assert_eq!(restored_idx.len(), 1); // Primary should be fine
        assert!(flags.dah_needs_rebuild);
        assert!(restored_dah.is_empty());
    }

    #[test]
    fn snapshot_all_corrupt_unmined_section() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("all.snap");

        let mut idx = Index::new(100).unwrap();
        idx.register(make_key(1), make_entry(100)).unwrap();

        let dah = DahIndex::new();
        let mut unmined = UnminedIndex::new();
        unmined.insert(500, make_key(1));

        idx.snapshot_all(&dah, &unmined, &snap_path).unwrap();

        // Corrupt the UNMI section
        let mut data = std::fs::read(&snap_path).unwrap();
        if let Some(pos) = data
            .windows(4)
            .position(|w| w == b"UNMI")
        {
            data[pos + 10] ^= 0xFF;
        }
        std::fs::write(&snap_path, &data).unwrap();

        let (restored_idx, restored_dah, restored_unmined, flags) =
            Index::restore_all(&snap_path).unwrap();

        assert_eq!(restored_idx.len(), 1);
        assert!(!flags.dah_needs_rebuild);
        assert!(flags.unmined_needs_rebuild);
        assert!(restored_dah.is_empty()); // empty, not corrupt
        assert!(restored_unmined.is_empty());
    }

    #[test]
    fn snapshot_all_empty_secondary() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("all.snap");

        let idx = Index::new(16).unwrap();
        let dah = DahIndex::new();
        let unmined = UnminedIndex::new();

        idx.snapshot_all(&dah, &unmined, &snap_path).unwrap();

        let (_, restored_dah, restored_unmined, flags) =
            Index::restore_all(&snap_path).unwrap();

        assert!(!flags.dah_needs_rebuild);
        assert!(!flags.unmined_needs_rebuild);
        assert!(restored_dah.is_empty());
        assert!(restored_unmined.is_empty());
    }

    #[test]
    fn snapshot_all_no_leakage() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("all.snap");

        let mut idx = Index::new(100).unwrap();
        idx.register(make_key(1), make_entry(100)).unwrap();

        let mut dah = DahIndex::new();
        dah.insert(100, make_key(1));

        let unmined = UnminedIndex::new();

        idx.snapshot_all(&dah, &unmined, &snap_path).unwrap();

        // Add more entries AFTER snapshot
        idx.register(make_key(2), make_entry(200)).unwrap();
        dah.insert(200, make_key(2));

        // Restore — should only have entries from snapshot time
        let (restored_idx, restored_dah, _, _) =
            Index::restore_all(&snap_path).unwrap();

        assert_eq!(restored_idx.len(), 1);
        assert!(restored_idx.lookup(&make_key(2)).is_none());
        assert_eq!(restored_dah.len(), 1);
    }

    // -- Rebuild from device tests --

    fn setup_device_with_records(
        record_count: usize,
    ) -> (Arc<MemoryDevice>, SlotAllocator, Vec<(TxKey, u64)>) {
        let dev = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(dev.clone());
        let mut records = Vec::new();

        for i in 0..record_count {
            let mut meta = TxMetadata::new(5);
            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&(i as u64).to_le_bytes());
            txid[8..16].copy_from_slice(
                &((i as u64).wrapping_mul(0x9E3779B97F4A7C15)).to_le_bytes(),
            );
            meta.tx_id = txid;
            meta.delete_at_height = if i % 2 == 0 { (i as u32 + 1) * 100 } else { 0 };
            meta.unmined_since = if i % 4 == 0 { (i as u32 + 1) * 50 } else { 0 };

            let record_size = TxMetadata::record_size_for(5);
            let offset = alloc.allocate(record_size).unwrap();

            let slots: Vec<UtxoSlot> = (0..5)
                .map(|s| {
                    let mut h = [0u8; 32];
                    h[0] = s;
                    UtxoSlot::new_unspent(h)
                })
                .collect();

            write_full_record(&*dev, offset, &meta, &slots).unwrap();
            records.push((TxKey { txid }, offset));
        }

        (dev, alloc, records)
    }

    #[test]
    fn rebuild_10_records() {
        let (dev, alloc, records) = setup_device_with_records(10);

        let rebuilt = Index::rebuild(&*dev, &alloc).unwrap();
        assert_eq!(rebuilt.len(), 10);

        for (key, offset) in &records {
            let e = rebuilt.lookup(key).expect("record should be indexed");
            assert_eq!(e.record_offset, *offset);
        }
    }

    #[test]
    fn rebuild_skips_corrupted_magic() {
        let (dev, alloc, records) = setup_device_with_records(10);

        // Corrupt the magic number of record at index 3
        let offset = records[3].1;
        let align = dev.alignment();
        let mut buf = crate::device::AlignedBuf::new(align, align);
        dev.pread(&mut buf, offset).unwrap();
        buf[0] = 0x00; // Corrupt first byte of magic
        buf[1] = 0x00;
        buf[2] = 0x00;
        buf[3] = 0x00;
        dev.pwrite(&buf, offset).unwrap();

        let rebuilt = Index::rebuild(&*dev, &alloc).unwrap();
        assert_eq!(rebuilt.len(), 9); // One less
        assert!(rebuilt.lookup(&records[3].0).is_none());
    }

    #[test]
    fn rebuild_empty_device() {
        let dev = Arc::new(MemoryDevice::new(4 * 1024 * 1024, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone());
        let rebuilt = Index::rebuild(&*dev, &alloc).unwrap();
        assert_eq!(rebuilt.len(), 0);
    }

    #[test]
    fn rebuild_secondary_from_device() {
        let (dev, alloc, _) = setup_device_with_records(20);

        let (dah, unmined) = Index::rebuild_secondary(&*dev, &alloc).unwrap();

        // 10 of 20 records have delete_at_height != 0 (even indices)
        assert_eq!(dah.len(), 10);
        // 5 of 20 records have unmined_since != 0 (indices divisible by 4)
        assert_eq!(unmined.len(), 5);
    }

    #[test]
    fn rebuild_secondary_skips_corrupted() {
        let (dev, alloc, records) = setup_device_with_records(20);

        // Corrupt record 0 (which has both dah and unmined set)
        let offset = records[0].1;
        let align = dev.alignment();
        let mut buf = crate::device::AlignedBuf::new(align, align);
        dev.pread(&mut buf, offset).unwrap();
        buf[0..4].copy_from_slice(&[0u8; 4]); // Zero magic
        dev.pwrite(&buf, offset).unwrap();

        let (dah, unmined) = Index::rebuild_secondary(&*dev, &alloc).unwrap();
        assert_eq!(dah.len(), 9); // Lost one DAH entry
        assert_eq!(unmined.len(), 4); // Lost one unmined entry
    }

    #[test]
    fn rebuild_secondary_empty_device() {
        let dev = Arc::new(MemoryDevice::new(4 * 1024 * 1024, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone());
        let (dah, unmined) = Index::rebuild_secondary(&*dev, &alloc).unwrap();
        assert!(dah.is_empty());
        assert!(unmined.is_empty());
    }

    #[test]
    fn rebuild_secondary_dah_range_query_correct() {
        let (dev, alloc, _) = setup_device_with_records(20);
        let (dah, _) = Index::rebuild_secondary(&*dev, &alloc).unwrap();

        // Record 0: dah = 100, record 2: dah = 300, record 4: dah = 500...
        let result = dah.range_query(300);
        // Heights 100, 300 — records 0 and 2
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn rebuild_secondary_unmined_range_query_correct() {
        let (dev, alloc, _) = setup_device_with_records(20);
        let (_, unmined) = Index::rebuild_secondary(&*dev, &alloc).unwrap();

        // Record 0: unmined = 50, record 4: unmined = 250, record 8: unmined = 450...
        let result = unmined.range_query(250);
        assert_eq!(result.len(), 2); // Records 0 and 4
    }

    // -- Index manager integration test --

    #[test]
    fn full_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("index.snap");

        // 1. Create index for 1000 expected records
        let mut idx = Index::new(1000).unwrap();

        // 2. Register 500 records
        for i in 0..500u64 {
            idx.register(make_key(i), make_entry(i * 100)).unwrap();
        }

        // 3. Lookup each: all found
        for i in 0..500u64 {
            assert!(idx.lookup(&make_key(i)).is_some(), "key {i} not found");
        }

        // 4. Unregister 100 records (keys 0..100)
        for i in 0..100u64 {
            let removed = idx.unregister(&make_key(i));
            assert!(removed.is_some());
        }

        // 5. Lookup unregistered: None
        for i in 0..100u64 {
            assert!(idx.lookup(&make_key(i)).is_none());
        }

        // 6. Lookup remaining 400: all found
        for i in 100..500u64 {
            assert!(idx.lookup(&make_key(i)).is_some(), "key {i} not found");
        }

        // 7. Stats show count=400
        let stats = idx.stats();
        assert_eq!(stats.entry_count, 400);

        // 8. Snapshot to temp file
        idx.snapshot(&snap_path).unwrap();

        // 9. Drop index, restore from snapshot
        drop(idx);
        let restored = Index::restore(&snap_path).unwrap();

        // 10. All 400 still found, 100 still absent
        assert_eq!(restored.len(), 400);
        for i in 0..100u64 {
            assert!(restored.lookup(&make_key(i)).is_none());
        }
        for i in 100..500u64 {
            let e = restored.lookup(&make_key(i)).expect("entry should exist");
            assert_eq!(e.record_offset, i * 100);
        }
    }
}
