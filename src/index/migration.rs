//! Migration tooling for exporting/importing index data between backends.
//!
//! Uses the existing snapshot binary format as the portable intermediate
//! representation, making it backend-agnostic.
//!
//! ## Import atomicity (R-047)
//!
//! The redb backend stores its three indexes (primary, DAH, unmined) in
//! three independent files at [`IndexConfig::redb_path`],
//! [`IndexConfig::redb_dah_path`], and [`IndexConfig::redb_unmined_path`].
//! redb has no cross-file transaction support, so a crash midway through
//! [`import_index`] could otherwise leave the on-disk state in a partial
//! shape — primary fully populated, DAH empty, unmined empty (or any
//! permutation). On the next startup the partial state would be opened
//! as if it were complete, producing silent index inconsistency.
//!
//! To prevent this, [`import_index`] writes a sentinel file at
//! [`import_sentinel_path`] BEFORE opening the first redb backend and
//! removes it ONLY after all three batch inserts have committed. Startup
//! consults [`import_in_progress`] before opening the redb files and
//! refuses to start while the sentinel exists, so the operator must
//! either re-run the import (which overwrites the partial state) or
//! delete the sentinel after manually verifying the on-disk state.

use std::path::{Path, PathBuf};

use crate::config::IndexConfig;
use crate::index::backend::PrimaryBackend;
use crate::index::dah_index::DahIndex;

use crate::index::redb_dah::RedbDahIndex;
use crate::index::redb_primary::RedbPrimary;
use crate::index::redb_unmined::RedbUnminedIndex;
use crate::index::secondary_backend::{DahBackend, UnminedBackend};
use crate::index::unmined_index::UnminedIndex;
use crate::index::{Index, IndexError};

/// Suffix appended to [`IndexConfig::redb_path`] to derive the
/// in-progress sentinel file. The sentinel is written by
/// [`import_index`] before the first redb commit and removed only after
/// all three commits succeed.
const IMPORT_SENTINEL_SUFFIX: &str = ".import-in-progress";

/// Compute the import-sentinel path derived from the redb primary path.
///
/// The sentinel lives next to the primary redb file with the
/// [`IMPORT_SENTINEL_SUFFIX`] suffix. Storing it in the same directory
/// keeps the rename / fsync atomic with respect to the redb files
/// themselves on the typical single-mount deployment, and means an
/// operator who relocates the redb files automatically relocates the
/// sentinel too.
pub fn import_sentinel_path(redb_path: &Path) -> PathBuf {
    let mut s = redb_path.as_os_str().to_os_string();
    s.push(IMPORT_SENTINEL_SUFFIX);
    PathBuf::from(s)
}

/// Whether an [`import_index`] call is currently in progress (or
/// crashed mid-way) for the redb files referenced by `config`.
///
/// Returns `true` iff the sentinel file at [`import_sentinel_path`]
/// exists. Startup MUST consult this and refuse to open the redb
/// backends when it returns `true`.
pub fn import_in_progress(config: &IndexConfig) -> bool {
    import_sentinel_path(&config.redb_path).exists()
}

/// Atomically write the import-in-progress sentinel for `redb_path`.
///
/// Uses a tempfile + rename + parent-dir fsync sequence so the sentinel
/// is durably observable before the first redb commit begins. The
/// sentinel content is informational only — its presence is the signal
/// that `import_index` did not run to completion.
fn write_import_sentinel(redb_path: &Path) -> std::io::Result<()> {
    let path = import_sentinel_path(redb_path);
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    let tmp_path = PathBuf::from(tmp);
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp_path)?;
        f.write_all(
            b"teraslab redb import in progress; do not start the server while this \
             file exists. Re-run the import to recover.\n",
        )?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp_path, &path)?;
    fsync_parent_dir(&path)?;
    Ok(())
}

/// Remove the import-in-progress sentinel for `redb_path`. Returns
/// `Ok(())` when the file was removed or already absent.
fn remove_import_sentinel(redb_path: &Path) -> std::io::Result<()> {
    let path = import_sentinel_path(redb_path);
    match std::fs::remove_file(&path) {
        Ok(()) => fsync_parent_dir(&path),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

#[cfg(unix)]
fn fsync_parent_dir(path: &Path) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let dir = std::fs::File::open(parent)?;
    dir.sync_all()
}

#[cfg(not(unix))]
fn fsync_parent_dir(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

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
///
/// # Atomicity (R-047)
///
/// For the redb backend the three on-disk files (`redb_path`,
/// `redb_dah_path`, `redb_unmined_path`) are written under separate
/// transactions because redb has no cross-file commit. To prevent a
/// crash mid-import from leaving a partially populated set of files
/// that the next startup would silently treat as complete, this
/// function writes a sentinel file via [`import_sentinel_path`] BEFORE
/// opening the first redb backend and removes it ONLY after all three
/// batch inserts have committed. Startup consults [`import_in_progress`]
/// and refuses to open the redb backends while the sentinel exists.
///
/// On any error during the redb branch the sentinel is intentionally
/// left in place so the operator notices the partial state on the next
/// startup. The sentinel write itself is best-effort: if it fails, the
/// import aborts before any redb file is opened so the on-disk state is
/// untouched.
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
        // Write the in-progress sentinel BEFORE opening any redb file so
        // a crash at any point during the import is observable on the
        // next startup. (R-047 / AUDIT GH-G3.)
        write_import_sentinel(&config.redb_path).map_err(IndexError::Io)?;

        // Import into redb backends using batch methods for performance.
        // `?` propagation here intentionally leaves the sentinel in
        // place so startup refuses until the operator re-runs the
        // import.
        let mut redb_primary = RedbPrimary::open(&config.redb_path, config.redb_cache_size)?;
        let primary_entries: Vec<_> = mem_idx.iter().collect();
        redb_primary.register_batch(&primary_entries)?;

        let mut redb_dah = RedbDahIndex::open(&config.redb_dah_path, config.redb_cache_size)?;
        let dah_entries: Vec<_> = mem_dah.iter().collect();
        redb_dah.insert_batch(&dah_entries);

        let mut redb_unmined =
            RedbUnminedIndex::open(&config.redb_unmined_path, config.redb_cache_size)?;
        let unmined_entries: Vec<_> = mem_unmined.iter().collect();
        redb_unmined.insert_batch(&unmined_entries);

        // All three backends committed successfully — clear the sentinel.
        // Each `*_batch` call above commits its own write transaction
        // synchronously, so the on-disk state is durable by the time we
        // reach this point.
        remove_import_sentinel(&config.redb_path).map_err(IndexError::Io)?;

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
        dah.insert(100, make_key(1), None).unwrap();
        dah.insert(200, make_key(2), None).unwrap();

        let mut unmined = UnminedBackend::new_in_memory();
        unmined.insert(500, make_key(3), None).unwrap();

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
            let e = restored_primary
                .lookup(&make_key(i))
                .expect("entry should exist");
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
        dah.insert(100, make_key(1), None).unwrap();

        let mut unmined = UnminedBackend::new_in_memory();
        unmined.insert(500, make_key(3), None).unwrap();

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
            let e = restored_primary
                .lookup(&make_key(i))
                .expect("entry should exist");
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
        dah.insert(100, make_key(1), None).unwrap();
        dah.insert(200, make_key(2), None).unwrap();

        let mut unmined = UnminedBackend::OnDisk(
            crate::index::redb_unmined::RedbUnminedIndex::open(
                &src_config.redb_unmined_path,
                src_config.redb_cache_size,
            )
            .unwrap(),
        );
        unmined.insert(500, make_key(3), None).unwrap();

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
            let e = restored_primary
                .lookup(&make_key(i))
                .expect("entry should exist");
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
        match result {
            Err(IndexError::FormatError { .. }) => {}
            other => panic!(
                "expected IndexError::FormatError for invalid snapshot magic, got {:?}",
                other
            ),
        }
    }

    #[test]
    fn import_missing_snapshot_errors() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("nonexistent.snap");

        let config = IndexConfig::default();
        let result = import_index(&config, &snap_path);
        match result {
            Err(IndexError::Io(_)) => {}
            other => panic!(
                "expected IndexError::Io for missing snapshot, got {:?}",
                other
            ),
        }
    }

    // -----------------------------------------------------------------------
    // R-047 — sentinel atomicity across the three redb files.
    // -----------------------------------------------------------------------

    fn write_minimal_snapshot(snap_path: &std::path::Path) {
        // Build a populated in-memory snapshot to exercise the redb
        // batch-insert paths during import.
        let mut primary = PrimaryBackend::new_in_memory(100).unwrap();
        for i in 0..5u64 {
            primary.register(make_key(i), make_entry(i * 100)).unwrap();
        }
        let mut dah = DahBackend::new_in_memory();
        dah.insert(100, make_key(1), None).unwrap();
        let mut unmined = UnminedBackend::new_in_memory();
        unmined.insert(500, make_key(2), None).unwrap();
        export_index(&primary, &dah, &unmined, snap_path).unwrap();
    }

    #[test]
    fn import_index_writes_sentinel_then_removes_on_success() {
        // Successful redb import: sentinel must NOT remain after the
        // call returns. Pre-fix this test trivially passes (no
        // sentinel was ever written); after the fix it verifies that
        // the cleanup step removed it.
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("export.snap");
        write_minimal_snapshot(&snap_path);

        let config = IndexConfig {
            backend: IndexBackendMode::Redb,
            redb_path: dir.path().join("primary.redb"),
            redb_dah_path: dir.path().join("dah.redb"),
            redb_unmined_path: dir.path().join("unmined.redb"),
            redb_cache_size: 64 * 1024 * 1024,
            ..IndexConfig::default()
        };

        let sentinel = import_sentinel_path(&config.redb_path);
        assert!(!sentinel.exists(), "sentinel must not exist before import");
        assert!(!import_in_progress(&config));

        let (_p, _d, _u, stats) = import_index(&config, &snap_path).unwrap();
        assert_eq!(stats.primary_entries, 5);
        assert_eq!(stats.dah_entries, 1);
        assert_eq!(stats.unmined_entries, 1);

        assert!(
            !sentinel.exists(),
            "sentinel must be removed after a successful redb import"
        );
        assert!(!import_in_progress(&config));
    }

    #[test]
    fn import_index_transactional_across_three_files() {
        // Simulate a crash mid-import by making the DAH path
        // un-openable: pre-create a *directory* at the dah path so
        // `RedbDahIndex::open` (which opens a file) fails. Pre-fix the
        // primary redb file is left populated and no sentinel exists,
        // so a follow-up startup would silently load a partial index.
        // Post-fix: the sentinel is left in place and a follow-up
        // `load_primary_index_redb` refuses startup.
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("export.snap");
        write_minimal_snapshot(&snap_path);

        let dah_path = dir.path().join("dah-as-directory");
        std::fs::create_dir_all(&dah_path).unwrap();

        let config = IndexConfig {
            backend: IndexBackendMode::Redb,
            redb_path: dir.path().join("primary.redb"),
            redb_dah_path: dah_path.clone(),
            redb_unmined_path: dir.path().join("unmined.redb"),
            redb_cache_size: 64 * 1024 * 1024,
            ..IndexConfig::default()
        };

        let result = import_index(&config, &snap_path);
        match result {
            Err(IndexError::FormatError { detail }) => {
                assert!(
                    detail.contains("redb open error (dah)"),
                    "expected dah-open error, got: {detail}"
                );
            }
            other => panic!("expected dah-open FormatError, got {:?}", other),
        }

        // The primary redb file was created and committed before the
        // failure; this is exactly the partial state the sentinel
        // protects against.
        assert!(
            config.redb_path.exists(),
            "primary redb file should have been created before the dah failure"
        );

        // The sentinel MUST still be present so the next startup
        // refuses to open the partial state.
        let sentinel = import_sentinel_path(&config.redb_path);
        assert!(
            sentinel.exists(),
            "sentinel must remain after a mid-import failure (partial-state guard)"
        );
        assert!(import_in_progress(&config));
    }

    #[test]
    fn import_index_rerun_after_partial_failure_clears_sentinel() {
        // Operator workflow: after a partial-import crash the operator
        // re-runs `import_index` against the same paths once the
        // underlying problem is fixed. The successful re-run MUST
        // remove the sentinel so the next startup proceeds.
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("export.snap");
        write_minimal_snapshot(&snap_path);

        // First attempt — fails partway through (dah path is a dir).
        let dah_dir = dir.path().join("dah-as-directory");
        std::fs::create_dir_all(&dah_dir).unwrap();
        let bad_config = IndexConfig {
            backend: IndexBackendMode::Redb,
            redb_path: dir.path().join("primary.redb"),
            redb_dah_path: dah_dir.clone(),
            redb_unmined_path: dir.path().join("unmined.redb"),
            redb_cache_size: 64 * 1024 * 1024,
            ..IndexConfig::default()
        };
        match import_index(&bad_config, &snap_path) {
            Err(IndexError::FormatError { .. }) => {}
            other => panic!("expected FormatError on first attempt, got {:?}", other),
        }
        assert!(import_in_progress(&bad_config));

        // Operator removes the offending directory and retries with a
        // valid dah path next to the primary.
        std::fs::remove_dir_all(&dah_dir).unwrap();
        let good_config = IndexConfig {
            redb_dah_path: dir.path().join("dah.redb"),
            ..bad_config
        };
        let (_p, _d, _u, stats) = import_index(&good_config, &snap_path).unwrap();
        assert_eq!(stats.primary_entries, 5);
        assert!(
            !import_in_progress(&good_config),
            "successful re-run must clear the sentinel"
        );
    }

    #[test]
    fn import_sentinel_path_is_derived_from_primary_path() {
        // Locks in the path-derivation contract that startup depends
        // on. Changing the suffix is a breaking change for operators
        // who may be looking for the file via `find` or monitoring.
        let primary = std::path::Path::new("/data/primary.redb");
        let sentinel = import_sentinel_path(primary);
        assert_eq!(
            sentinel,
            std::path::PathBuf::from("/data/primary.redb.import-in-progress")
        );
    }
}
