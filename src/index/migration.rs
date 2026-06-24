//! Migration tooling for exporting/importing index data between backends.
//!
//! Exports use a streaming migration format with fixed-size records so large
//! redb-backed indexes can be copied without first rebuilding a full in-memory
//! [`Index`]. Import keeps a legacy `TSIX` snapshot fallback for older files.
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

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::config::IndexConfig;
use crate::index::backend::PrimaryBackend;
use crate::index::dah_index::DahIndex;
use crate::index::hashtable::{TxIndexEntry, TxKey};

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

/// Streaming migration-file magic (`TeraSlab Migration Index`).
const PORTABLE_MAGIC: [u8; 4] = *b"TSMI";
const PORTABLE_VERSION: u32 = 1;
const PORTABLE_MAX_COUNT: u64 = 1 << 30;
const PORTABLE_PRIMARY_ENTRY_SIZE: usize = 63;
const PORTABLE_SECONDARY_ENTRY_SIZE: usize = 36;
const IMPORT_BATCH_SIZE: usize = 4096;

/// Compute the import-sentinel path derived from the redb primary path.
///
/// The sentinel lives next to the primary redb file with the
/// `IMPORT_SENTINEL_SUFFIX` suffix. Storing it in the same directory
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
    // `parent()` is `Some("")` for a bare relative name, not `None` (issue #13).
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
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

/// Export all index data to a portable migration file.
///
/// Works with any backend combination. The output file uses a streaming
/// backend-agnostic format:
///
/// `[magic "TSMI"][version u32][primary_count u64][dah_count u64]`
/// `[unmined_count u64][primary entries][dah entries][unmined entries][crc32]`.
///
/// The fixed-size entries intentionally mirror the existing snapshot payload
/// layout, but this writer emits each entry directly from the backend iterator
/// instead of materializing a temporary in-memory [`Index`].
pub fn export_index(
    primary: &PrimaryBackend,
    dah: &DahBackend,
    unmined: &UnminedBackend,
    path: &std::path::Path,
) -> Result<ExportStats, IndexError> {
    let declared =
        PortableCounts::new(primary.len() as u64, dah.len() as u64, unmined.len() as u64)?;
    let tmp_path = path.with_extension("tmp");
    let file = File::create(&tmp_path)?;
    let mut writer = BufWriter::new(file);
    let mut hasher = crc32fast::Hasher::new();

    write_portable_header(&mut writer, &mut hasher, declared)?;

    let mut primary_entries = 0usize;
    for (key, entry) in primary.iter() {
        write_tracked(&mut writer, &mut hasher, &encode_primary_entry(key, entry))?;
        primary_entries += 1;
    }
    ensure_export_count("primary", declared.primary_usize()?, primary_entries)?;

    let mut dah_entries = 0usize;
    for (height, key) in dah.iter() {
        write_tracked(
            &mut writer,
            &mut hasher,
            &encode_secondary_entry(height, key),
        )?;
        dah_entries += 1;
    }
    ensure_export_count("dah", declared.dah_usize()?, dah_entries)?;

    let mut unmined_entries = 0usize;
    for (height, key) in unmined.iter() {
        write_tracked(
            &mut writer,
            &mut hasher,
            &encode_secondary_entry(height, key),
        )?;
        unmined_entries += 1;
    }
    ensure_export_count("unmined", declared.unmined_usize()?, unmined_entries)?;

    writer.write_all(&hasher.finalize().to_le_bytes())?;
    writer.flush()?;
    let file = writer
        .into_inner()
        .map_err(|e| IndexError::Io(e.into_error()))?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp_path, path)?;
    fsync_parent_dir(path)?;

    Ok(ExportStats {
        primary_entries,
        dah_entries,
        unmined_entries,
    })
}

/// Export all index data to a portable migration file, reading from a sharded index.
///
/// Identical to [`export_index`] but iterates the primary data via
/// [`crate::index::ShardedIndex::for_each`] instead of `PrimaryBackend::iter`.
/// Used by the CLI export path when the snapshot is v2 (TSX2), which yields a
/// [`crate::index::ShardedIndex`] from
/// [`crate::index::ShardedIndex::restore_all`].
pub fn export_index_sharded(
    primary: &crate::index::ShardedIndex,
    dah: &DahBackend,
    unmined: &UnminedBackend,
    path: &std::path::Path,
) -> Result<ExportStats, IndexError> {
    let primary_count = primary.len() as u64;
    let declared = PortableCounts::new(primary_count, dah.len() as u64, unmined.len() as u64)?;
    let tmp_path = path.with_extension("tmp");
    let file = File::create(&tmp_path)?;
    let mut writer = BufWriter::new(file);
    let mut hasher = crc32fast::Hasher::new();

    write_portable_header(&mut writer, &mut hasher, declared)?;

    let mut primary_entries = 0usize;
    let mut io_err: Option<IndexError> = None;
    primary.for_each(|key, entry| {
        if io_err.is_none()
            && let Err(e) =
                write_tracked(&mut writer, &mut hasher, &encode_primary_entry(key, *entry))
        {
            io_err = Some(e);
        }
        primary_entries += 1;
    });
    if let Some(e) = io_err {
        return Err(e);
    }
    ensure_export_count("primary", declared.primary_usize()?, primary_entries)?;

    let mut dah_entries = 0usize;
    for (height, key) in dah.iter() {
        write_tracked(
            &mut writer,
            &mut hasher,
            &encode_secondary_entry(height, key),
        )?;
        dah_entries += 1;
    }
    ensure_export_count("dah", declared.dah_usize()?, dah_entries)?;

    let mut unmined_entries = 0usize;
    for (height, key) in unmined.iter() {
        write_tracked(
            &mut writer,
            &mut hasher,
            &encode_secondary_entry(height, key),
        )?;
        unmined_entries += 1;
    }
    ensure_export_count("unmined", declared.unmined_usize()?, unmined_entries)?;

    writer.write_all(&hasher.finalize().to_le_bytes())?;
    writer.flush()?;
    let file = writer
        .into_inner()
        .map_err(|e| IndexError::Io(e.into_error()))?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp_path, path)?;
    fsync_parent_dir(path)?;

    Ok(ExportStats {
        primary_entries,
        dah_entries,
        unmined_entries,
    })
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
    if is_streaming_migration_file(path)? {
        import_streaming_index(config, path)
    } else {
        import_legacy_snapshot(config, path)
    }
}

fn import_legacy_snapshot(
    config: &IndexConfig,
    path: &std::path::Path,
) -> Result<(PrimaryBackend, DahBackend, UnminedBackend, ImportStats), IndexError> {
    // Compatibility path for pre-streaming `TSIX` snapshots. This still
    // materializes the legacy snapshot by design; new exports use `TSMI`.
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
        redb_dah.insert_batch(&dah_entries)?;

        let mut redb_unmined =
            RedbUnminedIndex::open(&config.redb_unmined_path, config.redb_cache_size)?;
        let unmined_entries: Vec<_> = mem_unmined.iter().collect();
        redb_unmined.insert_batch(&unmined_entries)?;

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

#[derive(Debug, Clone, Copy)]
struct PortableCounts {
    primary: u64,
    dah: u64,
    unmined: u64,
}

impl PortableCounts {
    fn new(primary: u64, dah: u64, unmined: u64) -> Result<Self, IndexError> {
        for (name, count) in [("primary", primary), ("dah", dah), ("unmined", unmined)] {
            if count > PORTABLE_MAX_COUNT {
                return Err(IndexError::FormatError {
                    detail: format!(
                        "portable migration {name} count {count} exceeds maximum {PORTABLE_MAX_COUNT}"
                    ),
                });
            }
        }
        Ok(Self {
            primary,
            dah,
            unmined,
        })
    }

    fn primary_usize(self) -> Result<usize, IndexError> {
        portable_count_to_usize(self.primary, "primary")
    }

    fn dah_usize(self) -> Result<usize, IndexError> {
        portable_count_to_usize(self.dah, "dah")
    }

    fn unmined_usize(self) -> Result<usize, IndexError> {
        portable_count_to_usize(self.unmined, "unmined")
    }
}

fn portable_count_to_usize(count: u64, name: &str) -> Result<usize, IndexError> {
    usize::try_from(count).map_err(|_| IndexError::FormatError {
        detail: format!("portable migration {name} count {count} does not fit usize"),
    })
}

fn ensure_export_count(name: &str, declared: usize, actual: usize) -> Result<(), IndexError> {
    if declared == actual {
        return Ok(());
    }
    Err(IndexError::FormatError {
        detail: format!(
            "portable migration {name} count changed during export: declared {declared}, wrote {actual}"
        ),
    })
}

fn is_streaming_migration_file(path: &std::path::Path) -> Result<bool, IndexError> {
    let mut file = File::open(path)?;
    let mut magic = [0u8; 4];
    read_exact_or_format(&mut file, &mut magic, "portable migration magic")?;
    Ok(magic == PORTABLE_MAGIC)
}

fn import_streaming_index(
    config: &IndexConfig,
    path: &std::path::Path,
) -> Result<(PrimaryBackend, DahBackend, UnminedBackend, ImportStats), IndexError> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut hasher = crc32fast::Hasher::new();
    let counts = read_portable_header(&mut reader, &mut hasher)?;

    if config.is_redb() {
        import_streaming_redb(config, reader, hasher, counts)
    } else {
        import_streaming_memory(reader, hasher, counts)
    }
}

fn import_streaming_memory<R: Read>(
    mut reader: R,
    mut hasher: crc32fast::Hasher,
    counts: PortableCounts,
) -> Result<(PrimaryBackend, DahBackend, UnminedBackend, ImportStats), IndexError> {
    let mut primary = Index::new(counts.primary_usize()?.max(16))?;
    for _ in 0..counts.primary {
        let (key, entry) = read_primary_entry(&mut reader, &mut hasher)?;
        primary.register(key, entry)?;
    }

    let mut dah = DahIndex::new();
    for _ in 0..counts.dah {
        let (height, key) = read_secondary_entry(&mut reader, &mut hasher)?;
        dah.insert(height, key);
    }

    let mut unmined = UnminedIndex::new();
    for _ in 0..counts.unmined {
        let (height, key) = read_secondary_entry(&mut reader, &mut hasher)?;
        unmined.insert(height, key);
    }

    verify_portable_checksum_and_eof(&mut reader, hasher)?;

    Ok((
        PrimaryBackend::InMemory(primary),
        DahBackend::InMemory(dah),
        UnminedBackend::InMemory(unmined),
        ImportStats {
            primary_entries: counts.primary_usize()?,
            dah_entries: counts.dah_usize()?,
            unmined_entries: counts.unmined_usize()?,
        },
    ))
}

fn import_streaming_redb<R: Read>(
    config: &IndexConfig,
    mut reader: R,
    mut hasher: crc32fast::Hasher,
    counts: PortableCounts,
) -> Result<(PrimaryBackend, DahBackend, UnminedBackend, ImportStats), IndexError> {
    write_import_sentinel(&config.redb_path).map_err(IndexError::Io)?;

    let mut redb_primary = RedbPrimary::open(&config.redb_path, config.redb_cache_size)?;
    import_primary_entries_to_redb(&mut reader, &mut hasher, counts.primary, &mut redb_primary)?;

    let mut redb_dah = RedbDahIndex::open(&config.redb_dah_path, config.redb_cache_size)?;
    import_secondary_entries_to_dah(&mut reader, &mut hasher, counts.dah, &mut redb_dah)?;

    let mut redb_unmined =
        RedbUnminedIndex::open(&config.redb_unmined_path, config.redb_cache_size)?;
    import_secondary_entries_to_unmined(
        &mut reader,
        &mut hasher,
        counts.unmined,
        &mut redb_unmined,
    )?;

    verify_portable_checksum_and_eof(&mut reader, hasher)?;
    remove_import_sentinel(&config.redb_path).map_err(IndexError::Io)?;

    Ok((
        PrimaryBackend::OnDisk(redb_primary),
        DahBackend::OnDisk(redb_dah),
        UnminedBackend::OnDisk(redb_unmined),
        ImportStats {
            primary_entries: counts.primary_usize()?,
            dah_entries: counts.dah_usize()?,
            unmined_entries: counts.unmined_usize()?,
        },
    ))
}

fn import_primary_entries_to_redb<R: Read>(
    reader: &mut R,
    hasher: &mut crc32fast::Hasher,
    count: u64,
    redb: &mut RedbPrimary,
) -> Result<(), IndexError> {
    let mut batch = Vec::with_capacity(batch_capacity(count));
    for _ in 0..count {
        batch.push(read_primary_entry(reader, hasher)?);
        if batch.len() == IMPORT_BATCH_SIZE {
            redb.register_batch(&batch)?;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        redb.register_batch(&batch)?;
    }
    Ok(())
}

fn import_secondary_entries_to_dah<R: Read>(
    reader: &mut R,
    hasher: &mut crc32fast::Hasher,
    count: u64,
    redb: &mut RedbDahIndex,
) -> Result<(), IndexError> {
    let mut batch = Vec::with_capacity(batch_capacity(count));
    for _ in 0..count {
        batch.push(read_secondary_entry(reader, hasher)?);
        if batch.len() == IMPORT_BATCH_SIZE {
            redb.insert_batch(&batch)?;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        redb.insert_batch(&batch)?;
    }
    Ok(())
}

fn import_secondary_entries_to_unmined<R: Read>(
    reader: &mut R,
    hasher: &mut crc32fast::Hasher,
    count: u64,
    redb: &mut RedbUnminedIndex,
) -> Result<(), IndexError> {
    let mut batch = Vec::with_capacity(batch_capacity(count));
    for _ in 0..count {
        batch.push(read_secondary_entry(reader, hasher)?);
        if batch.len() == IMPORT_BATCH_SIZE {
            redb.insert_batch(&batch)?;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        redb.insert_batch(&batch)?;
    }
    Ok(())
}

fn batch_capacity(count: u64) -> usize {
    std::cmp::min(count, IMPORT_BATCH_SIZE as u64) as usize
}

fn write_portable_header<W: Write>(
    writer: &mut W,
    hasher: &mut crc32fast::Hasher,
    counts: PortableCounts,
) -> Result<(), IndexError> {
    write_tracked(writer, hasher, &PORTABLE_MAGIC)?;
    write_tracked(writer, hasher, &PORTABLE_VERSION.to_le_bytes())?;
    write_tracked(writer, hasher, &counts.primary.to_le_bytes())?;
    write_tracked(writer, hasher, &counts.dah.to_le_bytes())?;
    write_tracked(writer, hasher, &counts.unmined.to_le_bytes())?;
    Ok(())
}

fn read_portable_header<R: Read>(
    reader: &mut R,
    hasher: &mut crc32fast::Hasher,
) -> Result<PortableCounts, IndexError> {
    let mut magic = [0u8; 4];
    read_tracked(reader, hasher, &mut magic, "portable migration magic")?;
    if magic != PORTABLE_MAGIC {
        return Err(IndexError::FormatError {
            detail: "invalid portable migration magic".into(),
        });
    }

    let version = read_u32(reader, hasher, "portable migration version")?;
    if version != PORTABLE_VERSION {
        return Err(IndexError::FormatError {
            detail: format!(
                "unsupported portable migration version {version}; expected {PORTABLE_VERSION}"
            ),
        });
    }

    PortableCounts::new(
        read_u64(reader, hasher, "portable primary count")?,
        read_u64(reader, hasher, "portable dah count")?,
        read_u64(reader, hasher, "portable unmined count")?,
    )
}

fn read_u32<R: Read>(
    reader: &mut R,
    hasher: &mut crc32fast::Hasher,
    what: &str,
) -> Result<u32, IndexError> {
    let mut buf = [0u8; 4];
    read_tracked(reader, hasher, &mut buf, what)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u64<R: Read>(
    reader: &mut R,
    hasher: &mut crc32fast::Hasher,
    what: &str,
) -> Result<u64, IndexError> {
    let mut buf = [0u8; 8];
    read_tracked(reader, hasher, &mut buf, what)?;
    Ok(u64::from_le_bytes(buf))
}

fn write_tracked<W: Write>(
    writer: &mut W,
    hasher: &mut crc32fast::Hasher,
    bytes: &[u8],
) -> Result<(), IndexError> {
    writer.write_all(bytes)?;
    hasher.update(bytes);
    Ok(())
}

fn read_tracked<R: Read>(
    reader: &mut R,
    hasher: &mut crc32fast::Hasher,
    buf: &mut [u8],
    what: &str,
) -> Result<(), IndexError> {
    read_exact_or_format(reader, buf, what)?;
    hasher.update(buf);
    Ok(())
}

fn read_exact_or_format<R: Read>(
    reader: &mut R,
    buf: &mut [u8],
    what: &str,
) -> Result<(), IndexError> {
    reader.read_exact(buf).map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            IndexError::FormatError {
                detail: format!("portable migration truncated while reading {what}"),
            }
        } else {
            IndexError::Io(e)
        }
    })
}

fn verify_portable_checksum_and_eof<R: Read>(
    reader: &mut R,
    hasher: crc32fast::Hasher,
) -> Result<(), IndexError> {
    let mut checksum_buf = [0u8; 4];
    read_exact_or_format(reader, &mut checksum_buf, "portable migration checksum")?;
    let stored = u32::from_le_bytes(checksum_buf);
    let actual = hasher.finalize();
    if stored != actual {
        return Err(IndexError::ChecksumMismatch {
            expected: stored,
            actual,
        });
    }

    let mut trailing = [0u8; 1];
    match reader.read(&mut trailing) {
        Ok(0) => Ok(()),
        Ok(_) => Err(IndexError::FormatError {
            detail: "portable migration has trailing bytes after checksum".into(),
        }),
        Err(e) => Err(IndexError::Io(e)),
    }
}

fn encode_primary_entry(key: TxKey, entry: TxIndexEntry) -> [u8; PORTABLE_PRIMARY_ENTRY_SIZE] {
    let mut buf = [0u8; PORTABLE_PRIMARY_ENTRY_SIZE];
    buf[0..32].copy_from_slice(&key.txid);
    buf[32] = entry.device_id;
    buf[33..41].copy_from_slice(&entry.record_offset.to_le_bytes());
    buf[41..45].copy_from_slice(&entry.utxo_count.to_le_bytes());
    buf[45] = entry.block_entry_count;
    buf[46] = entry.tx_flags;
    buf[47..51].copy_from_slice(&entry.spent_utxos.to_le_bytes());
    buf[51..55].copy_from_slice(&entry.dah_or_preserve.to_le_bytes());
    buf[55..59].copy_from_slice(&entry.unmined_since.to_le_bytes());
    buf[59..63].copy_from_slice(&entry.generation.to_le_bytes());
    buf
}

fn read_primary_entry<R: Read>(
    reader: &mut R,
    hasher: &mut crc32fast::Hasher,
) -> Result<(TxKey, TxIndexEntry), IndexError> {
    let mut buf = [0u8; PORTABLE_PRIMARY_ENTRY_SIZE];
    read_tracked(reader, hasher, &mut buf, "portable primary entry")?;

    let mut txid = [0u8; 32];
    txid.copy_from_slice(&buf[0..32]);
    Ok((
        TxKey { txid },
        TxIndexEntry {
            device_id: buf[32],
            record_offset: u64::from_le_bytes(buf[33..41].try_into().unwrap()),
            utxo_count: u32::from_le_bytes(buf[41..45].try_into().unwrap()),
            block_entry_count: buf[45],
            tx_flags: buf[46],
            spent_utxos: u32::from_le_bytes(buf[47..51].try_into().unwrap()),
            dah_or_preserve: u32::from_le_bytes(buf[51..55].try_into().unwrap()),
            unmined_since: u32::from_le_bytes(buf[55..59].try_into().unwrap()),
            generation: u32::from_le_bytes(buf[59..63].try_into().unwrap()),
        },
    ))
}

fn encode_secondary_entry(height: u32, key: TxKey) -> [u8; PORTABLE_SECONDARY_ENTRY_SIZE] {
    let mut buf = [0u8; PORTABLE_SECONDARY_ENTRY_SIZE];
    buf[0..4].copy_from_slice(&height.to_le_bytes());
    buf[4..36].copy_from_slice(&key.txid);
    buf
}

fn read_secondary_entry<R: Read>(
    reader: &mut R,
    hasher: &mut crc32fast::Hasher,
) -> Result<(u32, TxKey), IndexError> {
    let mut buf = [0u8; PORTABLE_SECONDARY_ENTRY_SIZE];
    read_tracked(reader, hasher, &mut buf, "portable secondary entry")?;
    let height = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    let mut txid = [0u8; 32];
    txid.copy_from_slice(&buf[4..36]);
    Ok((height, TxKey { txid }))
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

    /// REL-017: populate EVERY field with a distinct, non-zero value derived
    /// from `offset` so an offset-swap, width-truncation, or field-zeroing
    /// regression on the portable export/import serialization path is caught by
    /// full-entry equality assertions (rather than passing against an all-zeros
    /// entry that only checks `record_offset`). Each field uses a different
    /// arithmetic shape and stays within its declared width.
    fn make_entry(offset: u64) -> TxIndexEntry {
        TxIndexEntry {
            device_id: (offset.wrapping_add(1) & 0xFF) as u8,
            record_offset: offset,
            utxo_count: (offset as u32).wrapping_mul(7).wrapping_add(3),
            block_entry_count: ((offset.wrapping_add(17)) & 0xFF) as u8,
            tx_flags: ((offset.wrapping_mul(3).wrapping_add(5)) & 0xFF) as u8,
            spent_utxos: (offset as u32).wrapping_mul(11).wrapping_add(13),
            dah_or_preserve: (offset as u32).wrapping_mul(101).wrapping_add(29),
            unmined_since: (offset as u32).wrapping_add(0x4000_0001),
            generation: (offset as u32).wrapping_mul(5).wrapping_add(1),
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

        // Verify data — REL-017: full entry equality across the
        // memory→memory round trip, not just `record_offset`.
        for i in 0..50u64 {
            let e = restored_primary
                .lookup(&make_key(i))
                .expect("entry should exist");
            assert_eq!(e, make_entry(i * 100), "entry {i} must survive round trip");
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

        // REL-017: full entry equality across the memory→redb round trip.
        for i in 0..20u64 {
            let e = restored_primary
                .lookup(&make_key(i))
                .expect("entry should exist");
            assert_eq!(e, make_entry(i * 100), "entry {i} must survive round trip");
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

        // REL-017: full entry equality across the redb→redb round trip.
        for i in 0..30u64 {
            let e = restored_primary
                .lookup(&make_key(i))
                .expect("entry should exist");
            assert_eq!(e, make_entry(i * 100), "entry {i} must survive round trip");
        }
    }

    #[test]
    fn migration_export_streaming_does_not_materialize() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("streaming-export.tsm");

        let src_config = IndexConfig {
            backend: IndexBackendMode::Redb,
            redb_path: dir.path().join("stream_src_primary.redb"),
            redb_dah_path: dir.path().join("stream_src_dah.redb"),
            redb_unmined_path: dir.path().join("stream_src_unmined.redb"),
            redb_cache_size: 64 * 1024 * 1024,
            ..IndexConfig::default()
        };

        let mut primary = PrimaryBackend::new_on_disk(&src_config).unwrap();
        for i in 0..25u64 {
            primary.register(make_key(i), make_entry(i * 100)).unwrap();
        }

        let mut dah = DahBackend::OnDisk(
            RedbDahIndex::open(&src_config.redb_dah_path, src_config.redb_cache_size).unwrap(),
        );
        for i in 0..9u64 {
            dah.insert(1_000 + i as u32, make_key(i), None).unwrap();
        }

        let mut unmined = UnminedBackend::OnDisk(
            RedbUnminedIndex::open(&src_config.redb_unmined_path, src_config.redb_cache_size)
                .unwrap(),
        );
        for i in 9..14u64 {
            unmined.insert(2_000 + i as u32, make_key(i), None).unwrap();
        }

        crate::index::reset_index_new_call_count();
        let export_stats = export_index(&primary, &dah, &unmined, &snap_path).unwrap();
        assert_eq!(export_stats.primary_entries, 25);
        assert_eq!(export_stats.dah_entries, 9);
        assert_eq!(export_stats.unmined_entries, 5);
        assert_eq!(
            crate::index::index_new_call_count(),
            0,
            "export_index must stream redb entries without constructing an in-memory Index"
        );

        let file_bytes = std::fs::read(&snap_path).unwrap();
        assert_eq!(&file_bytes[0..4], &PORTABLE_MAGIC);

        let dst_config = IndexConfig {
            backend: IndexBackendMode::Redb,
            redb_path: dir.path().join("stream_dst_primary.redb"),
            redb_dah_path: dir.path().join("stream_dst_dah.redb"),
            redb_unmined_path: dir.path().join("stream_dst_unmined.redb"),
            redb_cache_size: 64 * 1024 * 1024,
            ..IndexConfig::default()
        };

        let (restored_primary, restored_dah, restored_unmined, import_stats) =
            import_index(&dst_config, &snap_path).unwrap();
        assert_eq!(
            crate::index::index_new_call_count(),
            0,
            "streaming redb import must not restore the file into an in-memory Index first"
        );

        assert_eq!(import_stats.primary_entries, 25);
        assert_eq!(import_stats.dah_entries, 9);
        assert_eq!(import_stats.unmined_entries, 5);
        assert_eq!(restored_primary.len(), 25);
        assert_eq!(restored_dah.len(), 9);
        assert_eq!(restored_unmined.len(), 5);
        // REL-017: full entry equality across the streaming redb→redb round
        // trip.
        for i in 0..25u64 {
            let entry = restored_primary.lookup(&make_key(i)).unwrap();
            assert_eq!(
                entry,
                make_entry(i * 100),
                "entry {i} must survive streaming round trip",
            );
        }
    }

    #[test]
    fn import_legacy_tsix_snapshot_still_works() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("legacy.snap");

        let mut primary = Index::new(16).unwrap();
        for i in 0..6u64 {
            primary.register(make_key(i), make_entry(i * 100)).unwrap();
        }
        let mut dah = DahIndex::new();
        dah.insert(100, make_key(1));
        let mut unmined = UnminedIndex::new();
        unmined.insert(200, make_key(2));
        primary.snapshot_all(&dah, &unmined, &snap_path).unwrap();

        let config = IndexConfig {
            backend: IndexBackendMode::Redb,
            redb_path: dir.path().join("legacy_primary.redb"),
            redb_dah_path: dir.path().join("legacy_dah.redb"),
            redb_unmined_path: dir.path().join("legacy_unmined.redb"),
            redb_cache_size: 64 * 1024 * 1024,
            ..IndexConfig::default()
        };

        let (restored_primary, restored_dah, restored_unmined, stats) =
            import_index(&config, &snap_path).unwrap();
        assert_eq!(stats.primary_entries, 6);
        assert_eq!(stats.dah_entries, 1);
        assert_eq!(stats.unmined_entries, 1);
        assert_eq!(restored_primary.len(), 6);
        assert_eq!(restored_dah.len(), 1);
        assert_eq!(restored_unmined.len(), 1);
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
