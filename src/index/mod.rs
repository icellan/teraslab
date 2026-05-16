//! Primary and secondary in-memory indexes for TeraSlab.
//!
//! - [`Index`]: primary hash index mapping `TxKey` → `TxIndexEntry` (device location)
//! - [`DahIndex`]: secondary index for `delete_at_height` pruner queries
//! - [`UnminedIndex`]: secondary index for `unmined_since` pruner queries

pub mod backend;
pub mod dah_index;
pub mod hashtable;
pub mod migration;
pub mod redb_dah;
pub mod redb_primary;
pub mod redb_unmined;
pub mod secondary_backend;
pub mod unmined_index;
mod util;

pub use backend::PrimaryBackend;
pub use dah_index::{DahIndex, DahRedoEntry};
pub use hashtable::{TxIndexEntry, TxKey};
pub use redb_primary::CachedFieldsUpdate;
pub use secondary_backend::{DahBackend, UnminedBackend};
pub use unmined_index::{UnminedIndex, UnminedRedoEntry};

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
const MAX_SNAPSHOT_COUNT: usize = 1 << 30;

// Per-entry sizes in the snapshot file
const PRIMARY_ENTRY_SIZE: usize = 32 + 1 + 8 + 4 + 1 + 1 + 4 + 4 + 4 + 4; // TxKey + TxIndexEntry = 63
const SECONDARY_ENTRY_SIZE: usize = 4 + 32; // height + txid = 36

#[cfg(test)]
thread_local! {
    static INDEX_NEW_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_index_new_call_count() {
    INDEX_NEW_CALLS.with(|calls| calls.set(0));
}

#[cfg(test)]
pub(crate) fn index_new_call_count() -> usize {
    INDEX_NEW_CALLS.with(std::cell::Cell::get)
}

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
        #[cfg(test)]
        INDEX_NEW_CALLS.with(|calls| calls.set(calls.get() + 1));

        let capacity = (expected_records as f64 / 0.7).ceil() as usize;
        let table = HashTable::new(capacity.max(16))?;
        Ok(Self {
            table,
            resize_threshold: 0.7,
        })
    }

    /// Open or create a file-backed index at `path`.
    ///
    /// The hash table is pre-allocated to `expected_records / 0.7` buckets
    /// (rounded to the next power of two) to keep the load factor below 70%.
    /// If the file already exists with the correct size, entries are recovered
    /// from the mapped file. Otherwise a fresh empty index is created.
    pub fn open_file_backed(path: &std::path::Path, expected_records: usize) -> Result<Self> {
        let capacity = (expected_records as f64 / 0.7).ceil() as usize;
        let table = HashTable::open_file_backed(path, capacity.max(16))?;
        Ok(Self {
            table,
            resize_threshold: 0.7,
        })
    }

    /// Flush dirty pages to the backing file (async).
    ///
    /// No-op for anonymous-mmap-backed indexes. For file-backed indexes,
    /// schedules an asynchronous writeback of dirty pages.
    pub fn sync(&self) {
        self.table.sync();
    }

    /// Whether this index is backed by a persistent file.
    pub fn is_file_backed(&self) -> bool {
        self.table.is_file_backed()
    }

    /// Attach a redo log for journaling crash-atomic file-backed resizes.
    ///
    /// See [`hashtable::HashTable::set_redo_log`] for the full contract.
    pub fn set_redo_log(
        &mut self,
        redo_log: std::sync::Arc<parking_lot::Mutex<crate::redo::RedoLog>>,
    ) {
        self.table.set_redo_log(redo_log);
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
        self.register_without_resize(key, entry)?;
        if let Some(target_capacity) = self.resize_target_capacity() {
            self.resize_to_capacity(target_capacity)?;
        }
        Ok(())
    }

    /// Register or update an entry without performing an automatic resize.
    ///
    /// Production engine inserts use this to keep the primary-index write lock
    /// short, then perform the O(entries) resize copy under an upgradable read
    /// lock so concurrent readers are not blocked by the rehash.
    pub(crate) fn register_without_resize(
        &mut self,
        key: TxKey,
        entry: TxIndexEntry,
    ) -> Result<()> {
        self.table.insert(key, entry)?;
        Ok(())
    }

    pub(crate) fn resize_target_capacity(&self) -> Option<usize> {
        if self.table.load_factor() > self.resize_threshold {
            Some(self.table.capacity() * 2)
        } else {
            None
        }
    }

    pub(crate) fn resized_copy(&self, target_capacity: usize) -> Result<Self> {
        Ok(Self {
            table: self.table.build_resized(target_capacity)?,
            resize_threshold: self.resize_threshold,
        })
    }

    fn resize_to_capacity(&mut self, target_capacity: usize) -> Result<()> {
        self.table.resize(target_capacity)?;
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
        self.table.update_cached_fields(
            key,
            tx_flags,
            block_entry_count,
            spent_utxos,
            dah_or_preserve,
            unmined_since,
            generation,
        )
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
    pub fn iter(&self) -> hashtable::HashTableIter<'_> {
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
        util::fsync_parent_dir(path)?;
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
        data.extend_from_slice(&serialize_secondary(&UNMINED_SECTION_MAGIC, unmined.iter()));
        std::fs::write(&tmp_path, &data)?;
        let f = std::fs::File::open(&tmp_path)?;
        f.sync_all()?;
        drop(f);
        std::fs::rename(&tmp_path, path)?;
        util::fsync_parent_dir(path)?;
        Ok(())
    }

    /// Restore all indexes from a snapshot file.
    ///
    /// Each secondary index section is parsed independently (H6): if the
    /// DAH section is corrupt, only `dah_needs_rebuild` is set — the
    /// unmined section is still searched for and parsed. Symmetrically, if
    /// unmined is corrupt the DAH section is retained. Recovery then only
    /// rebuilds the sections that actually failed, avoiding a full device
    /// rescan that would throw away still-valid unmined data.
    pub fn restore_all(
        path: &std::path::Path,
    ) -> Result<(Self, DahIndex, UnminedIndex, RestoreFlags)> {
        let data = std::fs::read(path)?;
        let (index, primary_end) = Self::deserialize_primary_with_offset(&data)?;

        let mut flags = RestoreFlags::default();
        let mut dah = DahIndex::new();
        let mut unmined = UnminedIndex::new();

        let remaining = &data[primary_end..];

        // Attempt DAH section parse at the expected offset (right after
        // primary). On success we know where unmined begins. On failure we
        // fall back to a targeted scan for the unmined section magic.
        let (dah_ok, unmined_slice): (bool, &[u8]) =
            match deserialize_secondary(remaining, &DAH_SECTION_MAGIC) {
                Ok((entries, consumed)) => {
                    for (h, k) in entries {
                        dah.insert(h, k);
                    }
                    (true, &remaining[consumed..])
                }
                Err(_) => {
                    flags.dah_needs_rebuild = true;
                    // DAH offset is unknown; locate unmined by scanning for
                    // its magic marker. Because magic bytes can in theory
                    // appear inside DAH payload data, the first match with
                    // a successfully-parsable header is preferred; if no
                    // candidate parses cleanly, unmined is also flagged.
                    (false, locate_unmined_section(remaining))
                }
            };

        // Parse unmined section from the located slice (or continue after
        // DAH in the happy path).
        match deserialize_secondary(unmined_slice, &UNMINED_SECTION_MAGIC) {
            Ok((entries, _)) => {
                for (h, k) in entries {
                    unmined.insert(h, k);
                }
            }
            Err(_) => {
                flags.unmined_needs_rebuild = true;
            }
        }

        // Suppress unused-variable lint when both succeed.
        let _ = dah_ok;

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
    pub fn rebuild(device: &dyn BlockDevice, allocator: &SlotAllocator) -> Result<Self> {
        let mut index = Self::new(1024)?;
        let align = allocator.device_alignment();
        let start = allocator.data_region_start();
        let end = allocator.next_offset();

        let read_size = align.max(METADATA_SIZE);
        let aligned_read = read_size.div_ceil(align) * align;

        let mut offset = start;
        while offset + aligned_read as u64 <= end {
            if let Some((free_offset, free_size)) = allocator.free_region_containing(offset) {
                let free_end = free_offset.saturating_add(free_size).min(end);
                offset = free_end.max(offset + align as u64);
                continue;
            }

            let mut buf = AlignedBuf::new(aligned_read, align);
            device.pread_exact_at(&mut buf, offset)?;

            let meta = match TxMetadata::from_bytes(&buf[..METADATA_SIZE]) {
                Ok(m) => m,
                Err(e) => {
                    return Err(IndexError::FormatError {
                        detail: format!("corrupt metadata at allocated offset {offset}: {e}"),
                    });
                }
            };
            if { meta.magic } != METADATA_MAGIC {
                return Err(IndexError::FormatError {
                    detail: format!("invalid metadata magic at allocated offset {offset}"),
                });
            }

            let record_size = { meta.record_size } as u64;
            if record_size == 0 {
                return Err(IndexError::FormatError {
                    detail: format!("zero record_size at allocated offset {offset}"),
                });
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
            if offset + record_aligned as u64 > end {
                return Err(IndexError::FormatError {
                    detail: format!(
                        "record at allocated offset {offset} extends past allocator high-water mark"
                    ),
                });
            }
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
            if let Some((free_offset, free_size)) = allocator.free_region_containing(offset) {
                let free_end = free_offset.saturating_add(free_size).min(end);
                offset = free_end.max(offset + align as u64);
                continue;
            }

            let mut buf = AlignedBuf::new(aligned_read, align);
            device.pread_exact_at(&mut buf, offset)?;

            let meta = match TxMetadata::from_bytes(&buf[..METADATA_SIZE]) {
                Ok(m) => m,
                Err(e) => {
                    return Err(IndexError::FormatError {
                        detail: format!("corrupt metadata at allocated offset {offset}: {e}"),
                    });
                }
            };
            if { meta.magic } != METADATA_MAGIC {
                return Err(IndexError::FormatError {
                    detail: format!("invalid metadata magic at allocated offset {offset}"),
                });
            }

            let record_size = { meta.record_size } as u64;
            if record_size == 0 {
                return Err(IndexError::FormatError {
                    detail: format!("zero record_size at allocated offset {offset}"),
                });
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
            if offset + record_aligned as u64 > end {
                return Err(IndexError::FormatError {
                    detail: format!(
                        "record at allocated offset {offset} extends past allocator high-water mark"
                    ),
                });
            }
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

        let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
        if version != SNAPSHOT_VERSION {
            return Err(IndexError::FormatError {
                detail: format!(
                    "unsupported snapshot version {version}; expected {SNAPSHOT_VERSION}"
                ),
            });
        }
        let count = u64::from_le_bytes(data[8..16].try_into().unwrap()) as usize;
        let capacity = u64::from_le_bytes(data[16..24].try_into().unwrap()) as usize;

        // R-046 (GH-G1): use `checked_mul` + `checked_add` so a poisoned
        // snapshot whose declared `count` would make
        // `count * PRIMARY_ENTRY_SIZE` overflow `usize` (matters on
        // 32-bit; defensive on 64-bit) cannot bypass the size check
        // below — the wrapped tiny `total` could otherwise pass and
        // the loop would index `data[base..base + …]` and panic.
        // Cap `count` at a sane ceiling so a hostile snapshot cannot
        // request a multi-gigabyte `Vec` allocation via the
        // index-rebuild fast path. 2^30 is well above any realistic
        // working-set size for a UTXO store.
        if count > MAX_SNAPSHOT_COUNT {
            return Err(IndexError::FormatError {
                detail: format!("snapshot count {count} exceeds maximum {MAX_SNAPSHOT_COUNT}",),
            });
        }
        let body_size =
            count
                .checked_mul(PRIMARY_ENTRY_SIZE)
                .ok_or_else(|| IndexError::FormatError {
                    detail: format!(
                        "snapshot count {count} * entry_size {PRIMARY_ENTRY_SIZE} overflows usize",
                    ),
                })?;
        let total = header_size
            .checked_add(body_size)
            .and_then(|n| n.checked_add(4))
            .ok_or_else(|| IndexError::FormatError {
                detail: "snapshot total size overflows usize".into(),
            })?;
        if data.len() < total {
            return Err(IndexError::FormatError {
                detail: format!(
                    "file too small: need {total} bytes for {count} entries, have {}",
                    data.len()
                ),
            });
        }

        // Verify checksum
        let stored_checksum = u32::from_le_bytes(data[total - 4..total].try_into().unwrap());
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
                record_offset: u64::from_le_bytes(data[base + 33..base + 41].try_into().unwrap()),
                utxo_count: u32::from_le_bytes(data[base + 41..base + 45].try_into().unwrap()),
                block_entry_count: data[base + 45],
                tx_flags: data[base + 46],
                spent_utxos: u32::from_le_bytes(data[base + 47..base + 51].try_into().unwrap()),
                dah_or_preserve: u32::from_le_bytes(data[base + 51..base + 55].try_into().unwrap()),
                unmined_since: u32::from_le_bytes(data[base + 55..base + 59].try_into().unwrap()),
                generation: u32::from_le_bytes(data[base + 59..base + 63].try_into().unwrap()),
            };
            index.register(key, entry)?;
        }

        Ok((index, total))
    }
}

// ---------------------------------------------------------------------------
// Secondary index serialization helpers
// ---------------------------------------------------------------------------

fn serialize_secondary(magic: &[u8; 4], entries: impl Iterator<Item = (u32, TxKey)>) -> Vec<u8> {
    // F-G3-011: stream `entries` straight into `buf` instead of `.collect()`-ing
    // into an intermediate Vec. A fully-loaded DAH backend with tens of
    // millions of rows previously paid for the same data twice — once as
    // the `Vec<(u32, TxKey)>` and once as the serialized bytes.
    //
    // The serialized header carries a u64 `count` that has to be written
    // before the entries. We use `entries.size_hint().0` as a best-effort
    // capacity hint, reserve the header (with a placeholder count), append
    // entries one at a time updating a running counter, then patch the
    // count back into the header bytes at the known offset.
    let (size_hint_lo, _) = entries.size_hint();
    let header_size = 4 + 4 + 8; // magic + version + count
    let estimated_body = size_hint_lo * SECONDARY_ENTRY_SIZE;
    let mut buf = Vec::with_capacity(header_size + estimated_body + 4);
    buf.extend_from_slice(magic);
    buf.extend_from_slice(&SECONDARY_VERSION.to_le_bytes());
    let count_offset = buf.len();
    buf.extend_from_slice(&0u64.to_le_bytes()); // placeholder

    let mut count = 0u64;
    for (height, key) in entries {
        buf.extend_from_slice(&height.to_le_bytes());
        buf.extend_from_slice(&key.txid);
        count += 1;
    }
    // Patch the actual count into the header.
    buf[count_offset..count_offset + 8].copy_from_slice(&count.to_le_bytes());

    let checksum = crc32fast::hash(&buf);
    buf.extend_from_slice(&checksum.to_le_bytes());
    buf
}

/// Scan the provided slice for a byte window that begins with the unmined
/// section magic (`UNMI`) AND whose declared `count` + body fits inside the
/// remaining bytes AND whose stored CRC verifies. Returns the subslice
/// starting at the first candidate that passes all three checks, or an
/// empty slice if no candidate is found.
///
/// Used by [`Index::restore_all`] when the DAH section header is corrupt and
/// the offset of the unmined section is unknown.
///
/// F-G3-012: the pre-fix scan accepted the first candidate that passed the
/// size check, leaving `deserialize_secondary` to catch a forged section
/// via the CRC. That worked, but an attacker who could plant `UNMI`
/// followed by a benign `count` inside the corrupt DAH payload could
/// divert the scan to a chosen offset before the genuine unmined section
/// was even considered. Validating the CRC inline here removes that
/// amplification: we now skip past any candidate whose stored CRC does
/// not match, so a planted false-magic burst gets stepped over and the
/// real section (if present) is still found.
fn locate_unmined_section(data: &[u8]) -> &[u8] {
    let header_size = 4 + 4 + 8;
    let mut idx = 0usize;
    while idx + header_size + 4 <= data.len() {
        if data[idx..idx + 4] == UNMINED_SECTION_MAGIC {
            // Check declared count fits in remaining bytes.
            let count = u64::from_le_bytes(data[idx + 8..idx + 16].try_into().unwrap()) as usize;
            // Reject ludicrous counts up front so a poisoned u64 cannot
            // produce a `total` that is large but still within `data.len()`.
            if count <= MAX_SNAPSHOT_COUNT {
                let body_size = count.saturating_mul(SECONDARY_ENTRY_SIZE);
                let total = header_size + body_size + 4;
                if data.len() - idx >= total {
                    // Verify the CRC before declaring the match. Pre-fix this
                    // step happened inside `deserialize_secondary` AFTER the
                    // scan had already accepted the candidate; doing it here
                    // means a forged magic burst no longer hides the real
                    // section behind it.
                    let stored_checksum =
                        u32::from_le_bytes(data[idx + total - 4..idx + total].try_into().unwrap());
                    let computed_checksum = crc32fast::hash(&data[idx..idx + total - 4]);
                    if stored_checksum == computed_checksum {
                        return &data[idx..];
                    }
                }
            }
        }
        idx += 1;
    }
    &[]
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

    let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
    if version != SECONDARY_VERSION {
        return Err(IndexError::FormatError {
            detail: format!(
                "unsupported secondary version {version}; expected {SECONDARY_VERSION}"
            ),
        });
    }
    let count = u64::from_le_bytes(data[8..16].try_into().unwrap()) as usize;
    // R-046 (GH-G1): use checked arithmetic for the same reasons as
    // the primary `Index::restore` path. A poisoned secondary section
    // could otherwise wrap `count * SECONDARY_ENTRY_SIZE` and bypass
    // the size sanity check below.
    if count > MAX_SNAPSHOT_COUNT {
        return Err(IndexError::FormatError {
            detail: format!("secondary count {count} exceeds maximum {MAX_SNAPSHOT_COUNT}",),
        });
    }
    let body_size =
        count
            .checked_mul(SECONDARY_ENTRY_SIZE)
            .ok_or_else(|| IndexError::FormatError {
                detail: format!(
                    "secondary count {count} * entry_size {SECONDARY_ENTRY_SIZE} overflows usize",
                ),
            })?;
    let total = header_size
        .checked_add(body_size)
        .and_then(|n| n.checked_add(4))
        .ok_or_else(|| IndexError::FormatError {
            detail: "secondary total size overflows usize".into(),
        })?;

    if data.len() < total {
        return Err(IndexError::FormatError {
            detail: "secondary section truncated".into(),
        });
    }

    let stored_checksum = u32::from_le_bytes(data[total - 4..total].try_into().unwrap());
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
    use crate::record::{CRC32_OFFSET, TxMetadata, UtxoSlot};
    use std::sync::Arc;

    /// Corrupt the first 4 bytes of an allocated record's metadata header
    /// (the magic field) AND restamp the CRC over the corrupted bytes so
    /// `TxMetadata::from_bytes` accepts the header and the magic check is
    /// the gate that fails. Without restamping the CRC, the CRC check
    /// short-circuits before the magic check and the test exercises a
    /// different code path than its name implies.
    fn corrupt_magic_and_restamp_crc(dev: &dyn BlockDevice, offset: u64) {
        let align = dev.alignment();
        let mut buf = AlignedBuf::new(align, align);
        dev.pread_exact_at(&mut buf, offset).unwrap();
        // Zero the magic.
        buf[0..4].copy_from_slice(&[0u8; 4]);
        // Restamp CRC over the [0..METADATA_SIZE) header bytes (with the
        // CRC slot temporarily zeroed during the hash, matching
        // `TxMetadata::stamp_crc`'s semantics).
        let mut hash_buf = [0u8; METADATA_SIZE];
        hash_buf.copy_from_slice(&buf[..METADATA_SIZE]);
        hash_buf[CRC32_OFFSET..CRC32_OFFSET + 4].copy_from_slice(&[0u8; 4]);
        let crc = crc32fast::hash(&hash_buf);
        buf[CRC32_OFFSET..CRC32_OFFSET + 4].copy_from_slice(&crc.to_le_bytes());
        dev.pwrite_all_at(&buf, offset).unwrap();
    }

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
    fn snapshot_restore_rejects_unknown_version() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("index.snap");

        let mut idx = Index::new(100).unwrap();
        idx.register(make_key(1), make_entry(100)).unwrap();
        idx.snapshot(&snap_path).unwrap();

        let mut data = std::fs::read(&snap_path).unwrap();
        data[4..8].copy_from_slice(&(SNAPSHOT_VERSION + 1).to_le_bytes());
        let checksum = crc32fast::hash(&data[..data.len() - 4]);
        let checksum_offset = data.len() - 4;
        data[checksum_offset..].copy_from_slice(&checksum.to_le_bytes());
        std::fs::write(&snap_path, &data).unwrap();

        match Index::restore(&snap_path) {
            Err(IndexError::FormatError { detail }) => {
                assert!(
                    detail.contains("unsupported snapshot version"),
                    "unexpected detail: {detail}",
                );
            }
            Err(other) => panic!("expected unknown-version FormatError, got {other:?}"),
            Ok(_) => panic!("unknown snapshot version must be rejected"),
        }
    }

    #[test]
    fn secondary_restore_rejects_unknown_version() {
        let entries = vec![(42u32, make_key(1))];
        let mut data = serialize_secondary(&DAH_SECTION_MAGIC, entries.into_iter());
        data[4..8].copy_from_slice(&(SECONDARY_VERSION + 1).to_le_bytes());
        let checksum = crc32fast::hash(&data[..data.len() - 4]);
        let checksum_offset = data.len() - 4;
        data[checksum_offset..].copy_from_slice(&checksum.to_le_bytes());

        match deserialize_secondary(&data, &DAH_SECTION_MAGIC) {
            Err(IndexError::FormatError { detail }) => {
                assert!(
                    detail.contains("unsupported secondary version"),
                    "unexpected detail: {detail}",
                );
            }
            Err(other) => panic!("expected unknown-version FormatError, got {other:?}"),
            Ok(_) => panic!("unknown secondary version must be rejected"),
        }
    }

    #[test]
    fn snapshot_atomicity_fsync_parent_dir() {
        let source = include_str!("mod.rs");
        let calls = source.matches("util::fsync_parent_dir(path)?").count();
        assert!(
            calls >= 2,
            "both snapshot() and snapshot_all() must fsync the parent directory after rename",
        );
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
        if let Some(pos) = data.windows(4).position(|w| w == b"DAHI") {
            data[pos + 10] ^= 0xFF; // Corrupt a data byte
        }
        std::fs::write(&snap_path, &data).unwrap();

        let (restored_idx, restored_dah, _restored_unmined, flags) =
            Index::restore_all(&snap_path).unwrap();

        assert_eq!(restored_idx.len(), 1); // Primary should be fine
        assert!(flags.dah_needs_rebuild);
        assert!(restored_dah.is_empty());
    }

    /// R-046 (GH-G1) regression: a poisoned snapshot whose declared
    /// primary `count` is `u64::MAX` MUST be rejected with a
    /// `FormatError` instead of panicking on `count * PRIMARY_ENTRY_SIZE`
    /// (32-bit overflow) or attempting a multi-gigabyte
    /// `Vec::with_capacity(count)` (64-bit). Pre-fix the deserializer
    /// performed unchecked `count * PRIMARY_ENTRY_SIZE`; on a 32-bit
    /// build the wrap could even bypass the size sanity check and
    /// reach the for-loop, where slice indexing would panic.
    #[test]
    fn snapshot_restore_rejects_poisoned_primary_count() {
        // Build the minimal valid header for a primary section, then
        // overwrite `count` with `u64::MAX`. The deserializer reads
        // `count` from offset 8.
        let mut data = Vec::new();
        data.extend_from_slice(&SNAPSHOT_MAGIC); // 4 bytes
        data.extend_from_slice(&1u32.to_le_bytes()); // version
        data.extend_from_slice(&u64::MAX.to_le_bytes()); // POISONED count
        data.extend_from_slice(&0u64.to_le_bytes()); // capacity
        // 4-byte trailing checksum so the header alone passes the
        // initial `data.len() < header_size + 4` gate.
        data.extend_from_slice(&0u32.to_le_bytes());

        let result = Index::deserialize_primary(&data);
        match result {
            Err(IndexError::FormatError { detail }) => {
                assert!(
                    detail.contains("exceeds maximum") || detail.contains("overflow"),
                    "expected count-cap or overflow rejection, got: {detail}",
                );
            }
            Err(other) => panic!("expected FormatError for poisoned count, got: {other:?}",),
            Ok(_) => {
                panic!("deserialize_primary must reject u64::MAX count, not silently succeed",)
            }
        }
    }

    /// R-046 regression for the secondary-section deserializer: same
    /// pattern, same rejection contract.
    #[test]
    fn snapshot_restore_rejects_poisoned_secondary_count() {
        let mut data = Vec::new();
        data.extend_from_slice(&DAH_SECTION_MAGIC); // 4
        data.extend_from_slice(&SECONDARY_VERSION.to_le_bytes()); // 4
        data.extend_from_slice(&u64::MAX.to_le_bytes()); // POISONED count
        data.extend_from_slice(&0u32.to_le_bytes()); // checksum slot

        match deserialize_secondary(&data, &DAH_SECTION_MAGIC) {
            Err(IndexError::FormatError { detail }) => {
                assert!(
                    detail.contains("exceeds maximum") || detail.contains("overflow"),
                    "expected count-cap or overflow rejection, got: {detail}",
                );
            }
            Err(other) => {
                panic!("expected FormatError for poisoned secondary count, got: {other:?}",)
            }
            Ok(_) => panic!("deserialize_secondary must reject u64::MAX count",),
        }
    }

    #[test]
    fn restore_all_dah_corrupt_but_unmined_intact() {
        // H6: DAH section is corrupted, but unmined is intact. Recovery
        // must flag ONLY dah_needs_rebuild and retain the unmined entries.
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("all.snap");

        let mut idx = Index::new(100).unwrap();
        idx.register(make_key(1), make_entry(100)).unwrap();
        idx.register(make_key(2), make_entry(200)).unwrap();
        idx.register(make_key(3), make_entry(300)).unwrap();

        let mut dah = DahIndex::new();
        dah.insert(100, make_key(1));
        dah.insert(200, make_key(2));

        let mut unmined = UnminedIndex::new();
        unmined.insert(500, make_key(1));
        unmined.insert(600, make_key(2));
        unmined.insert(700, make_key(3));

        idx.snapshot_all(&dah, &unmined, &snap_path).unwrap();

        // Corrupt ONLY the DAH section header by flipping a byte inside its
        // declared count field. Leave UNMI section untouched.
        let mut data = std::fs::read(&snap_path).unwrap();
        let dah_pos = data
            .windows(4)
            .position(|w| w == b"DAHI")
            .expect("DAH magic should be present in snapshot");
        // Flip a bit in the count word (offset 8 after magic+version)
        data[dah_pos + 8] ^= 0xFF;
        std::fs::write(&snap_path, &data).unwrap();

        let (restored_idx, restored_dah, restored_unmined, flags) =
            Index::restore_all(&snap_path).unwrap();

        // Primary index is still good.
        assert_eq!(restored_idx.len(), 3);
        assert!(restored_idx.lookup(&make_key(1)).is_some());
        assert!(restored_idx.lookup(&make_key(2)).is_some());
        assert!(restored_idx.lookup(&make_key(3)).is_some());

        // DAH is empty and flagged for rebuild.
        assert!(flags.dah_needs_rebuild);
        assert!(restored_dah.is_empty());

        // Unmined is intact — NOT flagged for rebuild and entries preserved.
        assert!(
            !flags.unmined_needs_rebuild,
            "unmined should not be flagged when only DAH is corrupt"
        );
        assert_eq!(restored_unmined.len(), 3);
        let up_to_700 = restored_unmined.range_query(700);
        assert_eq!(up_to_700.len(), 3);
        let up_to_600 = restored_unmined.range_query(600);
        assert_eq!(up_to_600.len(), 2);
    }

    // F-G3-012: `locate_unmined_section` must skip over a planted `UNMI`
    // magic burst whose stored CRC does not verify, and continue scanning
    // for the genuine unmined section that follows. Pre-fix, the first
    // candidate that passed the size sanity-check was accepted and the
    // CRC check happened inside `deserialize_secondary` — by then the
    // scan had already locked onto the wrong offset.
    #[test]
    fn locate_unmined_section_skips_forged_magic_when_real_follows() {
        // Build a valid serialized unmined section (the "real" one).
        let real_entries = vec![
            (500u32, make_key(1)),
            (600u32, make_key(2)),
        ];
        let real_bytes =
            serialize_secondary(&UNMINED_SECTION_MAGIC, real_entries.iter().copied());

        // Build a poisoned prefix: `UNMI` magic + arbitrary version + count
        // that fits in `data.len()` after the prefix, plus garbage CRC.
        let mut blob = Vec::new();
        blob.extend_from_slice(&UNMINED_SECTION_MAGIC); // 4
        blob.extend_from_slice(&SECONDARY_VERSION.to_le_bytes()); // 4
        // count = 1 — small enough that the entire forged "section"
        // (header + 1 entry + crc) fits inside the prefix block before
        // the real section.
        blob.extend_from_slice(&1u64.to_le_bytes()); // 8
        // Fake entry (height + txid)
        blob.extend_from_slice(&[0xAA; SECONDARY_ENTRY_SIZE]);
        // Wrong CRC — deliberately not the real hash of the bytes above.
        blob.extend_from_slice(&0xDEADBEEFu32.to_le_bytes());

        // Now append the real section right after the forged one.
        blob.extend_from_slice(&real_bytes);

        // The scan must return the REAL section's start, not the forged
        // prefix at offset 0.
        let located = locate_unmined_section(&blob);
        assert!(!located.is_empty(), "expected the real section to be located");

        // Confirm by deserializing — if we got the forged prefix, the CRC
        // check would fail; if we got the real one, it should succeed.
        let (entries, _) =
            deserialize_secondary(located, &UNMINED_SECTION_MAGIC).expect(
                "locate must hand back the real, CRC-valid section, not the forged prefix",
            );
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (500, make_key(1)));
        assert_eq!(entries[1], (600, make_key(2)));
    }

    #[test]
    fn restore_all_unmined_corrupt_but_dah_intact() {
        // H6 symmetric case: unmined corrupt, DAH intact.
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("all.snap");

        let mut idx = Index::new(100).unwrap();
        idx.register(make_key(1), make_entry(100)).unwrap();
        idx.register(make_key(2), make_entry(200)).unwrap();

        let mut dah = DahIndex::new();
        dah.insert(111, make_key(1));
        dah.insert(222, make_key(2));

        let mut unmined = UnminedIndex::new();
        unmined.insert(333, make_key(1));

        idx.snapshot_all(&dah, &unmined, &snap_path).unwrap();

        // Corrupt the UNMI section's declared count.
        let mut data = std::fs::read(&snap_path).unwrap();
        let pos = data
            .windows(4)
            .position(|w| w == b"UNMI")
            .expect("UNMI magic should be present");
        data[pos + 8] ^= 0xFF;
        std::fs::write(&snap_path, &data).unwrap();

        let (restored_idx, restored_dah, restored_unmined, flags) =
            Index::restore_all(&snap_path).unwrap();

        assert_eq!(restored_idx.len(), 2);
        assert!(
            !flags.dah_needs_rebuild,
            "DAH should not be flagged when only unmined is corrupt"
        );
        assert_eq!(restored_dah.len(), 2);
        assert_eq!(restored_dah.range_query(222).len(), 2);

        assert!(flags.unmined_needs_rebuild);
        assert!(restored_unmined.is_empty());
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
        if let Some(pos) = data.windows(4).position(|w| w == b"UNMI") {
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

        let (_, restored_dah, restored_unmined, flags) = Index::restore_all(&snap_path).unwrap();

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
        let (restored_idx, restored_dah, _, _) = Index::restore_all(&snap_path).unwrap();

        assert_eq!(restored_idx.len(), 1);
        assert!(restored_idx.lookup(&make_key(2)).is_none());
        assert_eq!(restored_dah.len(), 1);
    }

    // -- Rebuild from device tests --

    fn setup_device_with_records(
        record_count: usize,
    ) -> (Arc<MemoryDevice>, SlotAllocator, Vec<(TxKey, u64)>) {
        let dev = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
        let mut records = Vec::new();

        for i in 0..record_count {
            let mut meta = TxMetadata::new(5);
            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&(i as u64).to_le_bytes());
            txid[8..16]
                .copy_from_slice(&((i as u64).wrapping_mul(0x9E3779B97F4A7C15)).to_le_bytes());
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
    fn rebuild_fails_on_corrupted_magic_in_allocated_region() {
        let (dev, alloc, records) = setup_device_with_records(10);

        let offset = records[3].1;
        corrupt_magic_and_restamp_crc(&*dev, offset);

        let err = Index::rebuild(&*dev, &alloc).unwrap_err();
        match err {
            IndexError::FormatError { detail } => {
                assert!(
                    detail.contains("invalid metadata magic"),
                    "expected magic-mismatch detail, got: {detail}"
                );
                assert!(detail.contains(&offset.to_string()));
            }
            other => panic!("expected FormatError, got {other:?}"),
        }
    }

    #[test]
    fn rebuild_fails_on_crc_mismatch_in_allocated_region() {
        // Companion to the magic-mismatch test: when the magic bytes are
        // zeroed WITHOUT restamping the CRC, `TxMetadata::from_bytes`
        // rejects the header on CRC before the magic check is reached.
        // The rebuild path must surface the CRC error in its detail
        // string so operators can distinguish torn-write corruption
        // from "valid header pointing at the wrong record type".
        let (dev, alloc, records) = setup_device_with_records(10);

        let offset = records[3].1;
        let align = dev.alignment();
        let mut buf = AlignedBuf::new(align, align);
        dev.pread_exact_at(&mut buf, offset).unwrap();
        buf[0..4].copy_from_slice(&[0u8; 4]);
        dev.pwrite_all_at(&buf, offset).unwrap();

        let err = Index::rebuild(&*dev, &alloc).unwrap_err();
        match err {
            IndexError::FormatError { detail } => {
                assert!(
                    detail.contains("corrupt metadata at allocated offset"),
                    "expected CRC-error detail, got: {detail}"
                );
                assert!(detail.contains(&offset.to_string()));
            }
            other => panic!("expected FormatError, got {other:?}"),
        }
    }

    #[test]
    fn rebuild_skips_corruption_inside_freelist_hole() {
        let (dev, mut alloc, records) = setup_device_with_records(10);

        let offset = records[3].1;
        let record_size = TxMetadata::record_size_for(5);
        alloc.free(offset, record_size).unwrap();

        let align = dev.alignment();
        let mut buf = crate::device::AlignedBuf::new(align, align);
        dev.pread(&mut buf, offset).unwrap();
        buf[0..4].copy_from_slice(&[0u8; 4]);
        dev.pwrite(&buf, offset).unwrap();

        let rebuilt = Index::rebuild(&*dev, &alloc).unwrap();
        assert_eq!(rebuilt.len(), 9);
        assert!(rebuilt.lookup(&records[3].0).is_none());
    }

    #[test]
    fn rebuild_empty_device() {
        let dev = Arc::new(MemoryDevice::new(4 * 1024 * 1024, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone()).unwrap();
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
    fn rebuild_secondary_fails_on_corrupted_allocated_record() {
        let (dev, alloc, records) = setup_device_with_records(20);

        // Corrupt record 0 (which has both dah and unmined set)
        let offset = records[0].1;
        corrupt_magic_and_restamp_crc(&*dev, offset);

        let err = match Index::rebuild_secondary(&*dev, &alloc) {
            Ok(_) => panic!("corrupt allocated secondary record must fail rebuild"),
            Err(err) => err,
        };
        match err {
            IndexError::FormatError { detail } => {
                assert!(
                    detail.contains("invalid metadata magic"),
                    "expected magic-mismatch detail, got: {detail}"
                );
                assert!(detail.contains(&offset.to_string()));
            }
            other => panic!("expected FormatError, got {other:?}"),
        }
    }

    #[test]
    fn rebuild_secondary_fails_on_crc_mismatch_in_allocated_record() {
        let (dev, alloc, records) = setup_device_with_records(20);

        let offset = records[0].1;
        let align = dev.alignment();
        let mut buf = AlignedBuf::new(align, align);
        dev.pread_exact_at(&mut buf, offset).unwrap();
        buf[0..4].copy_from_slice(&[0u8; 4]);
        dev.pwrite_all_at(&buf, offset).unwrap();

        let err = match Index::rebuild_secondary(&*dev, &alloc) {
            Ok(_) => panic!("corrupt CRC must fail secondary rebuild"),
            Err(err) => err,
        };
        match err {
            IndexError::FormatError { detail } => {
                assert!(
                    detail.contains("corrupt metadata at allocated offset"),
                    "expected CRC-error detail, got: {detail}"
                );
                assert!(detail.contains(&offset.to_string()));
            }
            other => panic!("expected FormatError, got {other:?}"),
        }
    }

    #[test]
    fn rebuild_secondary_empty_device() {
        let dev = Arc::new(MemoryDevice::new(4 * 1024 * 1024, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone()).unwrap();
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

    // -- File-backed index tests --

    #[test]
    fn file_backed_index_create_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("primary.idx");

        {
            let mut idx = Index::open_file_backed(&path, 100).unwrap();
            assert!(idx.is_file_backed());
            for i in 0..50u64 {
                let key = TxKey::from_bytes({
                    let mut txid = [0u8; 32];
                    txid[0..8].copy_from_slice(&i.to_le_bytes());
                    txid[8..16]
                        .copy_from_slice(&(i.wrapping_mul(0x9E3779B97F4A7C15)).to_le_bytes());
                    txid
                });
                let entry = TxIndexEntry {
                    device_id: 0,
                    record_offset: i * 100,
                    utxo_count: 10,
                    block_entry_count: 0,
                    tx_flags: 0,
                    spent_utxos: 0,
                    dah_or_preserve: 0,
                    unmined_since: 0,
                    generation: 0,
                };
                idx.register(key, entry).unwrap();
            }
            assert_eq!(idx.len(), 50);
            idx.sync();
        }

        let idx = Index::open_file_backed(&path, 100).unwrap();
        assert_eq!(idx.len(), 50);
    }

    #[test]
    fn file_backed_index_stats() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("primary.idx");
        let idx = Index::open_file_backed(&path, 100).unwrap();
        let stats = idx.stats();
        assert_eq!(stats.entry_count, 0);
        assert!(!stats.hugepage_enabled);
    }

    #[test]
    fn anonymous_index_is_not_file_backed() {
        let idx = Index::new(16).unwrap();
        assert!(!idx.is_file_backed());
    }

    #[test]
    fn concurrent_register_produces_one_resize_per_threshold_crossing() {
        // M9: stress test for the register→resize path.
        //
        // N threads each register M keys through a single shared Index
        // (wrapped in a Mutex since register takes &mut self). The
        // capacity must monotonically grow and never resize when the
        // load factor is below the threshold. Because the underlying
        // HashTable::resize now defensively re-checks `new_cap > capacity`,
        // a racing caller that observes a stale load factor can't
        // accidentally grow past the target on the same generation.
        use std::sync::{Arc, Mutex};
        use std::thread;

        let idx = Arc::new(Mutex::new(Index::new(16).unwrap()));
        let start_capacity = idx.lock().unwrap().stats().capacity;

        const THREADS: usize = 8;
        const PER_THREAD: usize = 200;

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let idx = Arc::clone(&idx);
                thread::spawn(move || {
                    for i in 0..PER_THREAD {
                        let mut txid = [0u8; 32];
                        txid[0] = t as u8;
                        txid[1..5].copy_from_slice(&(i as u32).to_le_bytes());
                        let key = TxKey { txid };
                        let entry = TxIndexEntry {
                            device_id: 0,
                            record_offset: ((t * PER_THREAD + i) * 4096) as u64,
                            utxo_count: 1,
                            block_entry_count: 0,
                            tx_flags: 0,
                            spent_utxos: 0,
                            dah_or_preserve: 0,
                            unmined_since: 0,
                            generation: 0,
                        };
                        idx.lock().unwrap().register(key, entry).unwrap();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        let final_stats = idx.lock().unwrap().stats();
        assert_eq!(
            final_stats.entry_count,
            THREADS * PER_THREAD,
            "all registered entries must be present",
        );
        // Final capacity must have grown by at least one doubling and
        // must be a power of two (the invariant enforced by resize).
        assert!(
            final_stats.capacity > start_capacity,
            "expected capacity > {start_capacity}, got {}",
            final_stats.capacity,
        );
        assert!(
            final_stats.capacity.is_power_of_two(),
            "capacity must be power of two, got {}",
            final_stats.capacity,
        );
        // Load factor must satisfy the invariant (<= threshold) after
        // the last register call.
        assert!(
            final_stats.load_factor <= 0.7,
            "final load factor {} must respect resize threshold 0.7",
            final_stats.load_factor,
        );
    }

    #[test]
    fn resize_to_smaller_or_equal_capacity_is_noop() {
        // M9 defensive re-check: resize() with a `new_capacity` that
        // rounds to the current or smaller capacity must NOT mutate.
        let mut idx = Index::new(64).unwrap();
        let start_capacity = idx.stats().capacity;
        // Directly reach into the table to request a "bogus" resize.
        idx.table.resize(16).unwrap(); // rounds to 16 < start_capacity
        idx.table.resize(start_capacity).unwrap(); // equal
        assert_eq!(
            idx.stats().capacity,
            start_capacity,
            "no-op resize must not change capacity"
        );
    }
}
