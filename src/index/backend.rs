//! Primary index backend abstraction.
//!
//! Uses enum dispatch (not trait objects) so the in-memory variant has zero
//! overhead — the compiler inlines through match arms.

use crate::allocator::SlotAllocator;
use crate::config::IndexConfig;
use crate::device::BlockDevice;
use crate::index::hashtable::{TxIndexEntry, TxKey};
use crate::index::redb_primary::RedbPrimary;
use crate::index::{DahIndex, Index, IndexError, IndexStats, RestoreFlags, UnminedIndex};

/// Primary index backend selection.
///
/// Uses enum dispatch so the in-memory variant has zero overhead: the compiler
/// can inline through match arms. The single branch prediction site per call
/// is negligible compared to the redb I/O cost.
pub enum PrimaryBackend {
    /// In-memory Robin Hood hash table (mmap-backed). Default and fastest.
    InMemory(Index),
    /// On-disk B+ tree via redb. Low RAM, crash-durable.
    OnDisk(RedbPrimary),
}

impl std::fmt::Debug for PrimaryBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InMemory(_) => f.write_str("PrimaryBackend::InMemory"),
            Self::OnDisk(_) => f.write_str("PrimaryBackend::OnDisk(redb)"),
        }
    }
}

impl PrimaryBackend {
    /// Create a new in-memory backend sized for `expected_records` entries.
    pub fn new_in_memory(expected_records: usize) -> Result<Self, IndexError> {
        Ok(Self::InMemory(Index::new(expected_records)?))
    }

    /// Open or create a redb on-disk backend at the configured path.
    pub fn new_on_disk(config: &IndexConfig) -> Result<Self, IndexError> {
        Ok(Self::OnDisk(RedbPrimary::open(
            &config.redb_path,
            config.redb_cache_size,
        )?))
    }

    /// Look up where a transaction's record lives on disk.
    pub fn lookup(&self, key: &TxKey) -> Option<TxIndexEntry> {
        match self {
            Self::InMemory(idx) => idx.lookup(key),
            Self::OnDisk(redb) => redb.lookup(key),
        }
    }

    /// Register a newly created transaction record in the index.
    pub fn register(&mut self, key: TxKey, entry: TxIndexEntry) -> Result<(), IndexError> {
        match self {
            Self::InMemory(idx) => idx.register(key, entry),
            Self::OnDisk(redb) => redb.register(key, entry),
        }
    }

    /// Remove a transaction from the index (on deletion/pruning).
    pub fn unregister(&mut self, key: &TxKey) -> Option<TxIndexEntry> {
        match self {
            Self::InMemory(idx) => idx.unregister(key),
            Self::OnDisk(redb) => redb.unregister(key),
        }
    }

    /// Update the cached fields in the index entry for `key`.
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
        match self {
            Self::InMemory(idx) => idx.update_cached_fields(
                key,
                tx_flags,
                block_entry_count,
                spent_utxos,
                dah_or_preserve,
                unmined_since,
                generation,
            ),
            Self::OnDisk(redb) => redb.update_cached_fields(
                key,
                tx_flags,
                block_entry_count,
                spent_utxos,
                dah_or_preserve,
                unmined_since,
                generation,
            ),
        }
    }

    /// Number of entries in the primary index.
    pub fn len(&self) -> usize {
        match self {
            Self::InMemory(idx) => idx.len(),
            Self::OnDisk(redb) => redb.len(),
        }
    }

    /// Whether the primary index is empty.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::InMemory(idx) => idx.is_empty(),
            Self::OnDisk(redb) => redb.is_empty(),
        }
    }

    /// Iterate over all `(TxKey, TxIndexEntry)` pairs in the primary index.
    ///
    /// **Warning (redb backend):** The on-disk variant materializes all entries
    /// into memory (~63 bytes/entry). At 10M entries this is ~630 MB. Use
    /// batched processing for large on-disk indexes in memory-constrained
    /// environments.
    pub fn iter(&self) -> PrimaryIter<'_> {
        match self {
            Self::InMemory(idx) => PrimaryIter::InMemory(idx.iter()),
            Self::OnDisk(redb) => PrimaryIter::Collected(redb.iter_collected().into_iter()),
        }
    }

    /// Statistics for monitoring.
    pub fn stats(&self) -> IndexStats {
        match self {
            Self::InMemory(idx) => idx.stats(),
            Self::OnDisk(redb) => redb.stats(),
        }
    }

    /// The name of the active backend (for monitoring/logging).
    pub fn backend_name(&self) -> &'static str {
        match self {
            Self::InMemory(_) => "memory",
            Self::OnDisk(_) => "redb",
        }
    }

    // -----------------------------------------------------------------------
    // Snapshot / restore
    // -----------------------------------------------------------------------

    /// Snapshot the primary index to a file.
    ///
    /// For the redb backend, this is a no-op (redb is already crash-durable).
    pub fn snapshot(&self, path: &std::path::Path) -> Result<(), IndexError> {
        match self {
            Self::InMemory(idx) => idx.snapshot(path),
            Self::OnDisk(redb) => redb.snapshot(path),
        }
    }

    /// Restore the primary index from a snapshot file (in-memory backend).
    pub fn restore(path: &std::path::Path) -> Result<Self, IndexError> {
        Ok(Self::InMemory(Index::restore(path)?))
    }

    /// Restore the primary index from an existing redb database.
    pub fn restore_redb(config: &IndexConfig) -> Result<Self, IndexError> {
        if !config.redb_path.exists() {
            return Err(IndexError::FormatError {
                detail: format!("redb file not found: {}", config.redb_path.display()),
            });
        }
        Self::new_on_disk(config)
    }

    /// Snapshot primary index + both secondary indexes to a single file.
    pub fn snapshot_all(
        &self,
        dah: &DahIndex,
        unmined: &UnminedIndex,
        path: &std::path::Path,
    ) -> Result<(), IndexError> {
        match self {
            Self::InMemory(idx) => idx.snapshot_all(dah, unmined, path),
            Self::OnDisk(_) => Ok(()), // No-op: redb is already durable
        }
    }

    /// Restore all indexes from a snapshot file (in-memory backend).
    pub fn restore_all(
        path: &std::path::Path,
    ) -> Result<(Self, DahIndex, UnminedIndex, RestoreFlags), IndexError> {
        let (idx, dah, unmined, flags) = Index::restore_all(path)?;
        Ok((Self::InMemory(idx), dah, unmined, flags))
    }

    // -----------------------------------------------------------------------
    // Rebuild from device scan
    // -----------------------------------------------------------------------

    /// Rebuild the primary index by scanning all records on the device.
    pub fn rebuild(
        device: &dyn BlockDevice,
        allocator: &SlotAllocator,
    ) -> Result<Self, IndexError> {
        Ok(Self::InMemory(Index::rebuild(device, allocator)?))
    }

    /// Rebuild the primary index into a redb database by scanning all records.
    ///
    /// Records with I/O errors or invalid magic are skipped with a warning.
    /// The total number of skipped offsets is logged at the end so operators
    /// can detect partial rebuilds from device corruption.
    pub fn rebuild_redb(
        config: &IndexConfig,
        device: &dyn BlockDevice,
        allocator: &SlotAllocator,
    ) -> Result<Self, IndexError> {
        let mut primary = RedbPrimary::open(&config.redb_path, config.redb_cache_size)?;

        let align = allocator.device_alignment();
        let start = allocator.data_region_start();
        let end = allocator.next_offset();

        let read_size = align.max(crate::record::METADATA_SIZE);
        let aligned_read = read_size.div_ceil(align) * align;

        const BATCH_SIZE: usize = 10_000;
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        let mut skipped: u64 = 0;
        let mut indexed: u64 = 0;

        let mut offset = start;
        while offset + aligned_read as u64 <= end {
            let mut buf = crate::device::AlignedBuf::new(aligned_read, align);
            if device.pread(&mut buf, offset).is_err() {
                skipped += 1;
                offset += align as u64;
                continue;
            }

            let meta = crate::record::TxMetadata::from_bytes(
                &buf[..crate::record::METADATA_SIZE],
            );
            if { meta.magic } != crate::record::METADATA_MAGIC {
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
            batch.push((key, entry));
            indexed += 1;

            if batch.len() >= BATCH_SIZE {
                primary.register_batch(&batch)?;
                batch.clear();
            }

            let record_aligned = (record_size as usize).div_ceil(align) * align;
            offset += record_aligned as u64;
        }

        // Flush remaining entries
        if !batch.is_empty() {
            primary.register_batch(&batch)?;
        }

        if skipped > 0 {
            eprintln!(
                "  rebuild_redb: WARNING: skipped {skipped} offsets due to I/O errors \
                 ({indexed} records indexed successfully)"
            );
        }

        Ok(Self::OnDisk(primary))
    }

    /// Rebuild secondary indexes by scanning all records on the device.
    pub fn rebuild_secondary(
        device: &dyn BlockDevice,
        allocator: &SlotAllocator,
    ) -> Result<(DahIndex, UnminedIndex), IndexError> {
        Index::rebuild_secondary(device, allocator)
    }
}

impl From<Index> for PrimaryBackend {
    fn from(idx: Index) -> Self {
        Self::InMemory(idx)
    }
}

// ---------------------------------------------------------------------------
// Iterator
// ---------------------------------------------------------------------------

/// Iterator over all `(TxKey, TxIndexEntry)` pairs, dispatching to the
/// active backend.
pub enum PrimaryIter<'a> {
    /// In-memory hash table iterator.
    InMemory(crate::index::hashtable::HashTableIter<'a>),
    /// Collected entries from on-disk backend (owned Vec iterator).
    Collected(std::vec::IntoIter<(TxKey, TxIndexEntry)>),
}

impl Iterator for PrimaryIter<'_> {
    type Item = (TxKey, TxIndexEntry);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::InMemory(it) => it.next(),
            Self::Collected(it) => it.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::InMemory(it) => it.size_hint(),
            Self::Collected(it) => it.size_hint(),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::SlotAllocator;
    use crate::config::{IndexBackendMode, IndexConfig};
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
            block_entry_count: 2,
            tx_flags: 0x05,
            spent_utxos: 3,
            dah_or_preserve: 100,
            unmined_since: 500,
            generation: 7,
        }
    }

    fn redb_config(dir: &std::path::Path) -> IndexConfig {
        IndexConfig {
            backend: IndexBackendMode::Redb,
            redb_path: dir.join("primary.redb"),
            redb_dah_path: dir.join("dah.redb"),
            redb_unmined_path: dir.join("unmined.redb"),
            redb_cache_size: 64 * 1024 * 1024,
        }
    }

    /// Helper that runs the same test body against both InMemory and OnDisk backends.
    fn with_both_backends(f: impl Fn(&mut PrimaryBackend)) {
        // In-memory
        let mut mem = PrimaryBackend::new_in_memory(1000).unwrap();
        f(&mut mem);

        // On-disk (redb)
        let dir = tempfile::tempdir().unwrap();
        let config = redb_config(dir.path());
        let mut disk = PrimaryBackend::new_on_disk(&config).unwrap();
        f(&mut disk);
    }

    // -----------------------------------------------------------------------
    // Parameterized tests: both backends produce identical results
    // -----------------------------------------------------------------------

    #[test]
    fn both_backends_lookup_register_unregister() {
        with_both_backends(|backend| {
            // Empty lookup
            assert!(backend.lookup(&make_key(1)).is_none());
            assert!(backend.is_empty());
            assert_eq!(backend.len(), 0);

            // Register
            backend.register(make_key(1), make_entry(4096)).unwrap();
            assert_eq!(backend.len(), 1);
            assert!(!backend.is_empty());

            let e = backend.lookup(&make_key(1)).expect("should find entry");
            assert_eq!(e.record_offset, 4096);
            assert_eq!(e.utxo_count, 10);
            assert_eq!(e.tx_flags, 0x05);
            assert_eq!(e.generation, 7);

            // Missing key
            assert!(backend.lookup(&make_key(999)).is_none());

            // Unregister
            let removed = backend.unregister(&make_key(1)).expect("should return entry");
            assert_eq!(removed.record_offset, 4096);
            assert!(backend.is_empty());

            // Unregister missing
            assert!(backend.unregister(&make_key(1)).is_none());
        });
    }

    #[test]
    fn both_backends_register_many_and_iterate() {
        with_both_backends(|backend| {
            for i in 0..100u64 {
                backend.register(make_key(i), make_entry(i * 100)).unwrap();
            }
            assert_eq!(backend.len(), 100);

            // Iterate and verify all entries present
            let entries: Vec<_> = backend.iter().collect();
            assert_eq!(entries.len(), 100);
            for i in 0..100u64 {
                let found = entries
                    .iter()
                    .any(|(k, e)| *k == make_key(i) && e.record_offset == i * 100);
                assert!(found, "entry {i} not found in iter");
            }

            // size_hint: Collected variant gives exact, InMemory may not
            let iter = backend.iter();
            let (_lower, _upper) = iter.size_hint();
        });
    }

    #[test]
    fn both_backends_update_cached_fields() {
        with_both_backends(|backend| {
            backend.register(make_key(1), make_entry(4096)).unwrap();

            let updated = backend.update_cached_fields(
                &make_key(1), 0xFF, 5, 8, 200, 600, 99,
            );
            assert!(updated);

            let e = backend.lookup(&make_key(1)).unwrap();
            assert_eq!(e.tx_flags, 0xFF);
            assert_eq!(e.block_entry_count, 5);
            assert_eq!(e.spent_utxos, 8);
            assert_eq!(e.dah_or_preserve, 200);
            assert_eq!(e.unmined_since, 600);
            assert_eq!(e.generation, 99);
            // Unchanged fields
            assert_eq!(e.record_offset, 4096);
            assert_eq!(e.utxo_count, 10);

            // Update missing key
            assert!(!backend.update_cached_fields(
                &make_key(999), 0, 0, 0, 0, 0, 0,
            ));
        });
    }

    #[test]
    fn both_backends_stats() {
        with_both_backends(|backend| {
            let stats = backend.stats();
            assert_eq!(stats.entry_count, 0);

            for i in 0..10u64 {
                backend.register(make_key(i), make_entry(i)).unwrap();
            }
            let stats = backend.stats();
            assert_eq!(stats.entry_count, 10);
        });
    }

    // -----------------------------------------------------------------------
    // OnDisk-specific tests
    // -----------------------------------------------------------------------

    #[test]
    fn on_disk_new_on_disk_creates_empty() {
        let dir = tempfile::tempdir().unwrap();
        let config = redb_config(dir.path());
        let backend = PrimaryBackend::new_on_disk(&config).unwrap();
        assert!(backend.is_empty());
        assert_eq!(backend.backend_name(), "redb");
    }

    #[test]
    fn in_memory_backend_name() {
        let backend = PrimaryBackend::new_in_memory(16).unwrap();
        assert_eq!(backend.backend_name(), "memory");
    }

    #[test]
    fn on_disk_restore_redb_opens_existing() {
        let dir = tempfile::tempdir().unwrap();
        let config = redb_config(dir.path());

        // Create and populate
        {
            let mut backend = PrimaryBackend::new_on_disk(&config).unwrap();
            for i in 0..50u64 {
                backend.register(make_key(i), make_entry(i * 100)).unwrap();
            }
        }

        // Restore from existing file
        let restored = PrimaryBackend::restore_redb(&config).unwrap();
        assert_eq!(restored.len(), 50);
        assert_eq!(restored.backend_name(), "redb");
        for i in 0..50u64 {
            let e = restored.lookup(&make_key(i)).expect("entry should survive reopen");
            assert_eq!(e.record_offset, i * 100);
        }
    }

    #[test]
    fn on_disk_restore_redb_missing_file_errors() {
        let dir = tempfile::tempdir().unwrap();
        let config = IndexConfig {
            backend: IndexBackendMode::Redb,
            redb_path: dir.path().join("nonexistent.redb"),
            redb_dah_path: dir.path().join("dah.redb"),
            redb_unmined_path: dir.path().join("unmined.redb"),
            redb_cache_size: 64 * 1024 * 1024,
        };
        let result = PrimaryBackend::restore_redb(&config);
        assert!(result.is_err());
        match result.unwrap_err() {
            IndexError::FormatError { detail } => {
                assert!(detail.contains("not found"), "error was: {detail}");
            }
            other => panic!("expected FormatError, got {other:?}"),
        }
    }

    #[test]
    fn on_disk_snapshot_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let config = redb_config(dir.path());
        let mut backend = PrimaryBackend::new_on_disk(&config).unwrap();
        backend.register(make_key(1), make_entry(100)).unwrap();

        // Snapshot should succeed (no-op)
        let snap_path = dir.path().join("noop.snap");
        backend.snapshot(&snap_path).unwrap();
        // File should NOT be created (it's a no-op)
        assert!(!snap_path.exists());
    }

    #[test]
    fn on_disk_snapshot_all_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let config = redb_config(dir.path());
        let mut backend = PrimaryBackend::new_on_disk(&config).unwrap();
        backend.register(make_key(1), make_entry(100)).unwrap();

        let dah = DahIndex::new();
        let unmined = UnminedIndex::new();
        let snap_path = dir.path().join("all.snap");
        backend.snapshot_all(&dah, &unmined, &snap_path).unwrap();
        // No-op for OnDisk
        assert!(!snap_path.exists());
    }

    #[test]
    fn on_disk_debug_format() {
        let dir = tempfile::tempdir().unwrap();
        let config = redb_config(dir.path());
        let backend = PrimaryBackend::new_on_disk(&config).unwrap();
        let debug = format!("{backend:?}");
        assert!(debug.contains("OnDisk"), "debug was: {debug}");

        let mem = PrimaryBackend::new_in_memory(16).unwrap();
        let debug = format!("{mem:?}");
        assert!(debug.contains("InMemory"), "debug was: {debug}");
    }

    #[test]
    fn from_index_produces_in_memory() {
        let idx = Index::new(16).unwrap();
        let backend: PrimaryBackend = idx.into();
        assert_eq!(backend.backend_name(), "memory");
    }

    // -----------------------------------------------------------------------
    // rebuild_redb tests
    // -----------------------------------------------------------------------

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
            txid[8..16].copy_from_slice(
                &((i as u64).wrapping_mul(0x9E3779B97F4A7C15)).to_le_bytes(),
            );
            meta.tx_id = txid;

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
    fn rebuild_redb_from_device() {
        let dir = tempfile::tempdir().unwrap();
        let config = redb_config(dir.path());
        let (dev, alloc, records) = setup_device_with_records(10);

        let rebuilt = PrimaryBackend::rebuild_redb(&config, &*dev, &alloc).unwrap();
        assert_eq!(rebuilt.len(), 10);
        assert_eq!(rebuilt.backend_name(), "redb");

        for (key, offset) in &records {
            let e = rebuilt.lookup(key).expect("record should be indexed");
            assert_eq!(e.record_offset, *offset);
            assert_eq!(e.utxo_count, 5);
        }
    }

    #[test]
    fn rebuild_redb_empty_device() {
        let dir = tempfile::tempdir().unwrap();
        let config = redb_config(dir.path());
        let dev = Arc::new(MemoryDevice::new(4 * 1024 * 1024, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone()).unwrap();

        let rebuilt = PrimaryBackend::rebuild_redb(&config, &*dev, &alloc).unwrap();
        assert_eq!(rebuilt.len(), 0);
    }

    #[test]
    fn rebuild_redb_skips_corrupted_magic() {
        let dir = tempfile::tempdir().unwrap();
        let config = redb_config(dir.path());
        let (dev, alloc, records) = setup_device_with_records(10);

        // Corrupt record 3's magic
        let offset = records[3].1;
        let align = dev.alignment();
        let mut buf = crate::device::AlignedBuf::new(align, align);
        dev.pread(&mut buf, offset).unwrap();
        buf[0..4].copy_from_slice(&[0u8; 4]);
        dev.pwrite(&buf, offset).unwrap();

        let rebuilt = PrimaryBackend::rebuild_redb(&config, &*dev, &alloc).unwrap();
        assert_eq!(rebuilt.len(), 9); // One less
        assert!(rebuilt.lookup(&records[3].0).is_none());
    }

    #[test]
    fn rebuild_redb_matches_in_memory_rebuild() {
        let (dev, alloc, records) = setup_device_with_records(20);

        // Rebuild in-memory
        let mem = PrimaryBackend::rebuild(&*dev, &alloc).unwrap();

        // Rebuild redb
        let dir = tempfile::tempdir().unwrap();
        let config = redb_config(dir.path());
        let redb = PrimaryBackend::rebuild_redb(&config, &*dev, &alloc).unwrap();

        // Both should have the same entries
        assert_eq!(mem.len(), redb.len());
        for (key, offset) in &records {
            let mem_entry = mem.lookup(key).expect("mem should have key");
            let redb_entry = redb.lookup(key).expect("redb should have key");
            assert_eq!(mem_entry.record_offset, redb_entry.record_offset);
            assert_eq!(mem_entry.utxo_count, redb_entry.utxo_count);
            assert_eq!(mem_entry.record_offset, *offset);
        }
    }

    // -----------------------------------------------------------------------
    // In-memory snapshot/restore tests
    // -----------------------------------------------------------------------

    #[test]
    fn in_memory_snapshot_and_restore() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("test.snap");

        // Create and populate an in-memory backend
        let mut backend = PrimaryBackend::new_in_memory(1000).unwrap();
        for i in 0..50u64 {
            backend.register(make_key(i), make_entry(i * 100)).unwrap();
        }

        // Snapshot to disk
        backend.snapshot(&snap_path).unwrap();
        assert!(snap_path.exists());

        // Restore from snapshot
        let restored = PrimaryBackend::restore(&snap_path).unwrap();
        assert_eq!(restored.len(), 50);
        assert_eq!(restored.backend_name(), "memory");

        for i in 0..50u64 {
            let e = restored.lookup(&make_key(i)).expect("entry should survive snapshot");
            assert_eq!(e.record_offset, i * 100);
        }
    }

    #[test]
    fn in_memory_snapshot_all_and_restore_all() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("all.snap");

        // Create populated in-memory backend with secondary indexes
        let mut backend = PrimaryBackend::new_in_memory(1000).unwrap();
        for i in 0..20u64 {
            backend.register(make_key(i), make_entry(i * 100)).unwrap();
        }

        let mut dah = DahIndex::new();
        dah.insert(100, make_key(1));
        dah.insert(200, make_key(2));

        let mut unmined = UnminedIndex::new();
        unmined.insert(300, make_key(3));

        // Snapshot all
        backend.snapshot_all(&dah, &unmined, &snap_path).unwrap();
        assert!(snap_path.exists());

        // Restore all
        let (restored, restored_dah, restored_unmined, _flags) =
            PrimaryBackend::restore_all(&snap_path).unwrap();

        assert_eq!(restored.len(), 20);
        assert_eq!(restored.backend_name(), "memory");
        assert_eq!(restored_dah.len(), 2);
        assert_eq!(restored_unmined.len(), 1);

        // Verify primary data
        for i in 0..20u64 {
            let e = restored.lookup(&make_key(i)).expect("entry should survive snapshot_all");
            assert_eq!(e.record_offset, i * 100);
        }

        // Verify secondary data
        let dah_result = restored_dah.range_query(200);
        assert_eq!(dah_result.len(), 2);
        let unmined_result = restored_unmined.range_query(300);
        assert_eq!(unmined_result.len(), 1);
    }

    #[test]
    fn restore_from_nonexistent_file_errors() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("does_not_exist.snap");

        let result = PrimaryBackend::restore(&snap_path);
        assert!(result.is_err());
    }

    #[test]
    fn restore_all_from_nonexistent_file_errors() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("does_not_exist.snap");

        let result = PrimaryBackend::restore_all(&snap_path);
        assert!(result.is_err());
    }

    #[test]
    fn both_backends_overwrite_same_key() {
        with_both_backends(|backend| {
            let key = make_key(1);
            backend.register(key, make_entry(100)).unwrap();
            backend.register(key, make_entry(200)).unwrap();

            assert_eq!(backend.len(), 1);
            let e = backend.lookup(&key).unwrap();
            assert_eq!(e.record_offset, 200);
        });
    }

    #[test]
    fn both_backends_unregister_then_reregister() {
        with_both_backends(|backend| {
            let key = make_key(1);
            backend.register(key, make_entry(100)).unwrap();
            backend.unregister(&key);
            assert!(backend.is_empty());

            backend.register(key, make_entry(999)).unwrap();
            assert_eq!(backend.len(), 1);
            let e = backend.lookup(&key).unwrap();
            assert_eq!(e.record_offset, 999);
        });
    }

    #[test]
    fn rebuild_secondary_from_device() {
        let (dev, alloc, _records) = setup_device_with_records(5);
        let (dah, unmined) = PrimaryBackend::rebuild_secondary(&*dev, &alloc).unwrap();

        // rebuild_secondary scans device for DAH/unmined metadata flags.
        // Our test records have no DAH/unmined flags set, so both should be empty.
        assert!(dah.is_empty());
        assert!(unmined.is_empty());
    }
}
