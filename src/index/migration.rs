//! Migration tooling for exporting/importing index data between backends.
//!
//! Uses the existing snapshot binary format as the portable intermediate
//! representation, making it backend-agnostic.

use crate::config::IndexConfig;
use crate::index::backend::PrimaryBackend;
use crate::index::dah_index::DahIndex;

use crate::index::redb_dah::RedbDahIndex;
use crate::index::redb_primary::RedbPrimary;
use crate::index::redb_unmined::RedbUnminedIndex;
use crate::index::secondary_backend::{DahBackend, UnminedBackend};
use crate::index::unmined_index::UnminedIndex;
use crate::index::{Index, IndexError};

/// Statistics from an export operation.
#[derive(Debug, Clone)]
pub struct ExportStats {
    /// Number of primary index entries exported.
    pub primary_entries: usize,
    /// Number of DAH entries exported.
    pub dah_entries: usize,
    /// Number of unmined entries exported.
    pub unmined_entries: usize,
}

/// Statistics from an import operation.
#[derive(Debug, Clone)]
pub struct ImportStats {
    /// Number of primary index entries imported.
    pub primary_entries: usize,
    /// Number of DAH entries imported.
    pub dah_entries: usize,
    /// Number of unmined entries imported.
    pub unmined_entries: usize,
}

/// Export all index data to a portable snapshot file.
///
/// Works with any backend combination. The output file uses the existing
/// snapshot binary format (magic "TSIX" + entries + CRC32).
pub fn export_index(
    primary: &PrimaryBackend,
    dah: &DahBackend,
    unmined: &UnminedBackend,
    path: &std::path::Path,
) -> Result<ExportStats, IndexError> {
    // Collect all data into in-memory indexes for serialization.
    let mut mem_primary = Index::new(primary.len().max(16))?;
    for (key, entry) in primary.iter() {
        mem_primary.register(key, entry)?;
    }

    let mut mem_dah = DahIndex::new();
    for (height, key) in dah.iter() {
        mem_dah.insert(height, key);
    }

    let mut mem_unmined = UnminedIndex::new();
    for (height, key) in unmined.iter() {
        mem_unmined.insert(height, key);
    }

    let stats = ExportStats {
        primary_entries: mem_primary.len(),
        dah_entries: mem_dah.len(),
        unmined_entries: mem_unmined.len(),
    };

    mem_primary.snapshot_all(&mem_dah, &mem_unmined, path)?;

    Ok(stats)
}

/// Import index data from a portable snapshot file into the configured backend.
///
/// Creates fresh backend instances and bulk-loads all entries.
pub fn import_index(
    config: &IndexConfig,
    path: &std::path::Path,
) -> Result<(PrimaryBackend, DahBackend, UnminedBackend, ImportStats), IndexError> {
    // Read the portable snapshot.
    let (mem_idx, mem_dah, mem_unmined, _flags) = Index::restore_all(path)?;

    let primary_count = mem_idx.len();
    let dah_count = mem_dah.len();
    let unmined_count = mem_unmined.len();

    if config.is_redb() {
        // Import into redb backends using batch methods for performance.
        let mut redb_primary = RedbPrimary::open(&config.redb_path, config.redb_cache_size)?;
        let primary_entries: Vec<_> = mem_idx.iter().collect();
        redb_primary.register_batch(&primary_entries)?;

        let mut redb_dah =
            RedbDahIndex::open(&config.redb_dah_path, config.redb_cache_size)?;
        let dah_entries: Vec<_> = mem_dah.iter().collect();
        redb_dah.insert_batch(&dah_entries);

        let mut redb_unmined =
            RedbUnminedIndex::open(&config.redb_unmined_path, config.redb_cache_size)?;
        let unmined_entries: Vec<_> = mem_unmined.iter().collect();
        redb_unmined.insert_batch(&unmined_entries);

        Ok((
            PrimaryBackend::OnDisk(redb_primary),
            DahBackend::OnDisk(redb_dah),
            UnminedBackend::OnDisk(redb_unmined),
            ImportStats {
                primary_entries: primary_count,
                dah_entries: dah_count,
                unmined_entries: unmined_count,
            },
        ))
    } else {
        // Import into in-memory backends (effectively just restore).
        Ok((
            PrimaryBackend::InMemory(mem_idx),
            DahBackend::InMemory(mem_dah),
            UnminedBackend::InMemory(mem_unmined),
            ImportStats {
                primary_entries: primary_count,
                dah_entries: dah_count,
                unmined_entries: unmined_count,
            },
        ))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::IndexBackendMode;
    use crate::index::hashtable::{TxIndexEntry, TxKey};

    fn make_key(n: u64) -> TxKey {
        let mut txid = [0u8; 32];
        txid[0..8].copy_from_slice(&n.to_le_bytes());
        TxKey { txid }
    }

    fn make_entry(offset: u64) -> TxIndexEntry {
        TxIndexEntry {
            device_id: 0,
            record_offset: offset,
            utxo_count: 5,
            block_entry_count: 0,
            tx_flags: 0,
            spent_utxos: 0,
            dah_or_preserve: 0,
            unmined_since: 0,
            generation: 0,
        }
    }

    #[test]
    fn export_import_memory_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("export.snap");

        // Create populated backends
        let mut primary = PrimaryBackend::new_in_memory(100).unwrap();
        for i in 0..50u64 {
            primary.register(make_key(i), make_entry(i * 100)).unwrap();
        }
        let mut dah = DahBackend::new_in_memory();
        dah.insert(100, make_key(1));
        dah.insert(200, make_key(2));

        let mut unmined = UnminedBackend::new_in_memory();
        unmined.insert(500, make_key(3));

        // Export
        let export_stats = export_index(&primary, &dah, &unmined, &snap_path).unwrap();
        assert_eq!(export_stats.primary_entries, 50);
        assert_eq!(export_stats.dah_entries, 2);
        assert_eq!(export_stats.unmined_entries, 1);

        // Import into memory backend
        let config = IndexConfig::default();
        let (restored_primary, restored_dah, restored_unmined, import_stats) =
            import_index(&config, &snap_path).unwrap();

        assert_eq!(import_stats.primary_entries, 50);
        assert_eq!(restored_primary.len(), 50);
        assert_eq!(restored_dah.len(), 2);
        assert_eq!(restored_unmined.len(), 1);

        // Verify data
        for i in 0..50u64 {
            let e = restored_primary.lookup(&make_key(i)).expect("entry should exist");
            assert_eq!(e.record_offset, i * 100);
        }
    }

    #[test]
    fn export_import_to_redb() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("export.snap");

        // Create populated in-memory backends
        let mut primary = PrimaryBackend::new_in_memory(100).unwrap();
        for i in 0..20u64 {
            primary.register(make_key(i), make_entry(i * 100)).unwrap();
        }
        let mut dah = DahBackend::new_in_memory();
        dah.insert(100, make_key(1));

        let mut unmined = UnminedBackend::new_in_memory();
        unmined.insert(500, make_key(3));

        // Export
        export_index(&primary, &dah, &unmined, &snap_path).unwrap();

        // Import into redb backend
        let config = IndexConfig {
            backend: IndexBackendMode::Redb,
            redb_path: dir.path().join("primary.redb"),
            redb_dah_path: dir.path().join("dah.redb"),
            redb_unmined_path: dir.path().join("unmined.redb"),
            redb_cache_size: 64 * 1024 * 1024,
            ..IndexConfig::default()
        };
        let (restored_primary, restored_dah, restored_unmined, import_stats) =
            import_index(&config, &snap_path).unwrap();

        assert_eq!(import_stats.primary_entries, 20);
        assert_eq!(restored_primary.len(), 20);
        assert_eq!(restored_dah.len(), 1);
        assert_eq!(restored_unmined.len(), 1);

        for i in 0..20u64 {
            let e = restored_primary.lookup(&make_key(i)).expect("entry should exist");
            assert_eq!(e.record_offset, i * 100);
        }
    }

    #[test]
    fn export_empty_index() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("empty.snap");

        let primary = PrimaryBackend::new_in_memory(16).unwrap();
        let dah = DahBackend::new_in_memory();
        let unmined = UnminedBackend::new_in_memory();

        let stats = export_index(&primary, &dah, &unmined, &snap_path).unwrap();
        assert_eq!(stats.primary_entries, 0);

        let config = IndexConfig::default();
        let (p, d, u, _) = import_index(&config, &snap_path).unwrap();
        assert!(p.is_empty());
        assert!(d.is_empty());
        assert!(u.is_empty());
    }

    #[test]
    fn export_from_redb_import_to_memory() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("export.snap");

        // Create redb backend
        let config = IndexConfig {
            backend: IndexBackendMode::Redb,
            redb_path: dir.path().join("primary.redb"),
            redb_dah_path: dir.path().join("dah.redb"),
            redb_unmined_path: dir.path().join("unmined.redb"),
            redb_cache_size: 64 * 1024 * 1024,
            ..IndexConfig::default()
        };
        let mut primary = PrimaryBackend::new_on_disk(&config).unwrap();
        for i in 0..10u64 {
            primary.register(make_key(i), make_entry(i * 100)).unwrap();
        }
        let dah = DahBackend::new_in_memory(); // Use in-memory for simplicity
        let unmined = UnminedBackend::new_in_memory();

        // Export from redb
        export_index(&primary, &dah, &unmined, &snap_path).unwrap();

        // Import into memory
        let mem_config = IndexConfig::default();
        let (restored, _, _, stats) = import_index(&mem_config, &snap_path).unwrap();
        assert_eq!(stats.primary_entries, 10);
        assert_eq!(restored.len(), 10);
    }

    #[test]
    fn redb_to_redb_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("export.snap");

        // Create redb source
        let src_config = IndexConfig {
            backend: IndexBackendMode::Redb,
            redb_path: dir.path().join("src_primary.redb"),
            redb_dah_path: dir.path().join("src_dah.redb"),
            redb_unmined_path: dir.path().join("src_unmined.redb"),
            redb_cache_size: 64 * 1024 * 1024,
            ..IndexConfig::default()
        };

        let mut primary = PrimaryBackend::new_on_disk(&src_config).unwrap();
        for i in 0..30u64 {
            primary.register(make_key(i), make_entry(i * 100)).unwrap();
        }
        let mut dah = DahBackend::OnDisk(
            crate::index::redb_dah::RedbDahIndex::open(
                &src_config.redb_dah_path,
                src_config.redb_cache_size,
            )
            .unwrap(),
        );
        dah.insert(100, make_key(1));
        dah.insert(200, make_key(2));

        let mut unmined = UnminedBackend::OnDisk(
            crate::index::redb_unmined::RedbUnminedIndex::open(
                &src_config.redb_unmined_path,
                src_config.redb_cache_size,
            )
            .unwrap(),
        );
        unmined.insert(500, make_key(3));

        // Export from redb
        let export_stats = export_index(&primary, &dah, &unmined, &snap_path).unwrap();
        assert_eq!(export_stats.primary_entries, 30);
        assert_eq!(export_stats.dah_entries, 2);
        assert_eq!(export_stats.unmined_entries, 1);

        // Import into a different redb
        let dst_config = IndexConfig {
            backend: IndexBackendMode::Redb,
            redb_path: dir.path().join("dst_primary.redb"),
            redb_dah_path: dir.path().join("dst_dah.redb"),
            redb_unmined_path: dir.path().join("dst_unmined.redb"),
            redb_cache_size: 64 * 1024 * 1024,
            ..IndexConfig::default()
        };
        let (restored_primary, restored_dah, restored_unmined, import_stats) =
            import_index(&dst_config, &snap_path).unwrap();

        assert_eq!(import_stats.primary_entries, 30);
        assert_eq!(import_stats.dah_entries, 2);
        assert_eq!(import_stats.unmined_entries, 1);

        assert_eq!(restored_primary.len(), 30);
        assert_eq!(restored_dah.len(), 2);
        assert_eq!(restored_unmined.len(), 1);

        for i in 0..30u64 {
            let e = restored_primary.lookup(&make_key(i)).expect("entry should exist");
            assert_eq!(e.record_offset, i * 100);
        }
    }

    #[test]
    fn import_corrupt_snapshot_errors() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("corrupt.snap");
        std::fs::write(&snap_path, b"not a valid snapshot").unwrap();

        let config = IndexConfig::default();
        let result = import_index(&config, &snap_path);
        assert!(result.is_err());
    }

    #[test]
    fn import_missing_snapshot_errors() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("nonexistent.snap");

        let config = IndexConfig::default();
        let result = import_index(&config, &snap_path);
        assert!(result.is_err());
    }
}
