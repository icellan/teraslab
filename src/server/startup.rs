//! Startup helpers for index loading, secondary rebuild, and replay tolerance.
//!
//! Gap #5 (TERANODE_PRODUCTION_READINESS_GAPS.md) — recovery and index
//! rebuild were previously fail-open: a corrupt redb primary file was
//! deleted and replaced with an empty index, secondary rebuild errors fell
//! through to empty indexes, and replay applied a blanket
//! `MAX_TOLERATED_FAILURES = 32` across all replay error causes.
//!
//! This module centralizes the production policy:
//!
//! * Primary rebuild failure is **fatal** at startup. The on-disk file is
//!   NOT deleted — operators must investigate or trigger an explicit
//!   rescan. Returning [`RebuildError`] propagates through `main()` to a
//!   non-zero exit so deployment automation can detect the failure.
//!
//! * Secondary rebuild failure is **degraded readiness, not empty start**.
//!   The node returns a [`SecondaryLoadOutcome`] carrying empty indexes
//!   plus a [`SecondaryStatus`] that flips the corresponding `dah_ok` /
//!   `unmined_ok` flag to `false`. The dispatch path consults
//!   [`crate::server::dispatch::secondary_status`] and rejects handlers
//!   that depend on the missing index with `ERR_INDEX_DEGRADED`.
//!
//! * Replay failure tolerance is **per-cause**:
//!   [`ReplayCause::MissingPrimary`] is benign during idempotent replay
//!   (the record was deleted between the redo append and recovery) so it
//!   is tolerated up to a high cap; any other cause
//!   ([`ReplayCause::IoError`], [`ReplayCause::CorruptEntry`],
//!   [`ReplayCause::LogicError`]) fails closed regardless of count.

use std::path::Path;
use std::sync::Arc;

use thiserror::Error;

use crate::allocator::{AllocatorError, BoxedAllocator, RecordAllocator, SlotAllocator};
use crate::config::{IndexConfig, StorageEngine};
use crate::device::BlockDevice;
use crate::index::{
    DahBackend, DahIndex, IndexError, PrimaryBackend, ShardedIndex, UnminedBackend, UnminedIndex,
};
use crate::recovery::{RecoveryStats, ReplayCause};

use super::dispatch::SecondaryStatus;

/// Errors raised by `load_primary_index_*` when neither restore nor
/// rebuild can produce a valid primary index.
///
/// Each variant carries the operator-facing context (file path,
/// underlying error) needed to investigate the failure. The
/// `Display` impl includes a remediation hint so log-level dashboards
/// can surface actionable text directly.
#[derive(Error, Debug)]
pub enum RebuildError {
    /// The redb primary index file existed but could not be opened, AND
    /// rebuilding from the device also failed. The file is preserved
    /// untouched so the operator can capture diagnostics; deletion is
    /// the operator's call.
    #[error(
        "redb primary index unavailable: restore failed ({restore_err}) and \
         rebuild from device failed ({rebuild_err}); file preserved at {path}; \
         investigate the underlying device or redb error and run an explicit \
         rescan before restarting"
    )]
    RedbPrimary {
        /// Path of the redb primary index that was preserved.
        path: String,
        /// `Display`-formatted error from the restore attempt.
        restore_err: String,
        /// `Display`-formatted error from the device-rebuild attempt.
        rebuild_err: String,
    },

    /// File-backed mmap primary index is unavailable. Both restore (when
    /// the file exists) and rebuild from device returned errors. The file
    /// is preserved untouched.
    #[error(
        "file-backed primary index unavailable: rebuild from device failed \
         ({rebuild_err}){restore_suffix}; file preserved at {path}; \
         investigate the underlying device or filesystem error before restarting"
    )]
    FileBackedPrimary {
        /// Path of the file-backed primary index that was preserved.
        path: String,
        /// `Display`-formatted rebuild error.
        rebuild_err: String,
        /// Optional suffix describing the prior restore error, e.g.
        /// `; restore failed (truncated)` or empty when the file did
        /// not exist before.
        restore_suffix: String,
    },

    /// In-memory primary index could not be rebuilt from the device.
    /// The in-memory variant has no persistent file to preserve.
    #[error(
        "in-memory primary index unavailable: rebuild from device failed \
         ({rebuild_err}); investigate the underlying device or allocator state"
    )]
    InMemoryPrimary {
        /// `Display`-formatted rebuild error.
        rebuild_err: String,
    },

    /// A redb [`crate::index::migration::import_index`] call was
    /// observed to have started but never completed — the sentinel
    /// file at [`crate::index::migration::import_sentinel_path`] is
    /// still present. Opening the redb files in this state would
    /// silently load whichever subset of the three indexes was
    /// committed before the crash, producing inconsistent on-disk
    /// state. R-047 / AUDIT GH-G3.
    #[error(
        "redb import was interrupted: in-progress sentinel present at \
         {sentinel_path}; redb files may contain a partial import. \
         Re-run `teraslab-cli import-index` to overwrite the partial state, \
         or remove the sentinel manually after verifying the on-disk \
         redb files are consistent"
    )]
    RedbImportInProgress {
        /// Path of the sentinel file that triggered the refusal.
        sentinel_path: String,
    },
}

/// Outcome of a secondary-index load attempt.
///
/// On success both backends carry their populated state and `status`
/// reports both flags as `true`. On failure the corresponding
/// `dah_ok` / `unmined_ok` flag flips to `false` and an empty backend
/// is returned in its slot. The caller is expected to call
/// [`crate::server::dispatch::set_secondary_status`] with `status` so
/// the dispatch readiness gate can reject endpoints that depend on the
/// missing index.
#[derive(Debug)]
pub struct SecondaryLoadOutcome {
    /// DAH secondary index — populated on success, empty on failure.
    pub dah: DahBackend,
    /// Unmined secondary index — populated on success, empty on failure.
    pub unmined: UnminedBackend,
    /// Per-secondary readiness flags. Returned to the binary so it can
    /// install the global flags via
    /// [`crate::server::dispatch::set_secondary_status`].
    pub status: SecondaryStatus,
}

/// Cap on the number of tolerable [`ReplayCause::MissingPrimary`]
/// failures during startup replay.
///
/// `MissingPrimary` is benign: the redo entry references a `tx_key`
/// that is no longer in the primary index, which can happen when a
/// later entry in the same log deleted the record (idempotent replay)
/// or when the primary index snapshot already captured the post-delete
/// state. We tolerate a generous fixed cap rather than no cap at all
/// because an unbounded `MissingPrimary` count is a strong signal that
/// either the redo entries are referencing the wrong primary database
/// (mismatched device / wrong path) or the primary index is missing
/// far more state than the redo log can plausibly explain. Either case
/// warrants operator attention rather than silent recovery.
///
/// The cap is intentionally much higher than the previous blanket
/// `MAX_TOLERATED_FAILURES = 32` — that limit was picked when the
/// recovery path could not distinguish causes, and routine hot-shutdown
/// recovery sometimes tripped it. Now that other causes fail closed
/// immediately, a high cap on the benign class is safe.
pub const MAX_TOLERATED_MISSING_PRIMARY: u64 = 65_536;

/// Cap on the number of tolerable [`ReplayCause::ReplicaRecordAbsent`]
/// failures during startup replay.
///
/// `ReplicaRecordAbsent` is a legacy (payload-less) `RedoOp::ReplicaCreate` for a
/// replica / migration-received SECONDARY copy whose on-device record bytes
/// were never durable on this node — the receiver's documented contract
/// (fsync data device, then flush redo, then ACK) allows a stop between the
/// two flushes to leave a redo `Create` ahead of its record bytes. The
/// authoritative record lives on the master and is re-replicated on rejoin,
/// so aborting startup would only strand the whole node (scenario_09:
/// cluster wedged at 0/N ready). Tolerated up to the same generous cap as
/// `MissingPrimary`; an unbounded count still aborts because it would
/// signal a mismatched device / wrong redo log rather than a routine
/// stop-between-flushes window.
pub const MAX_TOLERATED_REPLICA_RECORD_ABSENT: u64 = 65_536;

/// Apply the per-cause replay tolerance policy and produce a
/// human-readable error string when startup must abort.
///
/// Returns `Ok(())` when:
/// * Every replay outcome was [`ReplayCause::MissingPrimary`] (benign),
///   AND the count is at or below [`MAX_TOLERATED_MISSING_PRIMARY`].
/// * No replay failures occurred.
///
/// Returns `Err(message)` when any non-`MissingPrimary` cause appears
/// at all, OR when the `MissingPrimary` count exceeds the cap.
pub fn check_replay_tolerance(stats: &RecoveryStats) -> Result<(), String> {
    check_replay_tolerance_with_cap(stats, MAX_TOLERATED_MISSING_PRIMARY)
}

/// Apply the replay tolerance policy with an operator-configured
/// MissingPrimary cap.
pub fn check_replay_tolerance_with_cap(
    stats: &RecoveryStats,
    max_missing_primary: u64,
) -> Result<(), String> {
    // F-G6-018: tag each error with the structured `cause=<label>` field
    // sourced from `replay_cause_label` so log scrapers and dashboards
    // see the exact same wording the per-cause classifier emits.
    if stats.failed_io > 0 {
        return Err(format!(
            "recovery: {n} replay failure(s) caused by device I/O errors — \
             non-tolerable, the device is unreachable or returning corrupt \
             blocks; investigate before restarting [cause={cause}]",
            n = stats.failed_io,
            cause = replay_cause_label(ReplayCause::IoError),
        ));
    }
    if stats.failed_corrupt > 0 {
        return Err(format!(
            "recovery: {n} replay failure(s) caused by corrupt redo or \
             metadata records — non-tolerable, on-device data is unreadable; \
             investigate before restarting [cause={cause}]",
            n = stats.failed_corrupt,
            cause = replay_cause_label(ReplayCause::CorruptEntry),
        ));
    }
    if stats.failed_logic > 0 {
        return Err(format!(
            "recovery: {n} replay failure(s) caused by logic-level \
             inconsistency — non-tolerable; investigate before restarting \
             [cause={cause}]",
            n = stats.failed_logic,
            cause = replay_cause_label(ReplayCause::LogicError),
        ));
    }
    if stats.failed_missing_record_bytes > 0 {
        // Gap #2 (TERANODE_PRODUCTION_READINESS_GAPS.md): a Create
        // replay could not write the full record bytes captured in the
        // redo entry. Short I/O on the record area means the device is
        // misbehaving — continuing would silently register an index
        // entry pointing at incomplete bytes (the exact failure mode
        // that motivated the full-payload redesign).
        return Err(format!(
            "recovery: {n} create-replay failure(s) — full record bytes \
             could not be written to device; non-tolerable, the device \
             returned short I/O; investigate before restarting [cause={cause}]",
            n = stats.failed_missing_record_bytes,
            cause = replay_cause_label(ReplayCause::MissingRecordBytes),
        ));
    }
    if stats.failed_missing_primary > max_missing_primary {
        return Err(format!(
            "recovery: {n} missing-primary replay failure(s) exceed cap \
             ({cap}) — the redo log references far more deleted records than \
             the primary index can plausibly explain; verify device / path \
             and investigate before restarting [cause={cause}]",
            n = stats.failed_missing_primary,
            cap = max_missing_primary,
            cause = replay_cause_label(ReplayCause::MissingPrimary),
        ));
    }
    if stats.failed_replica_record_absent > MAX_TOLERATED_REPLICA_RECORD_ABSENT {
        return Err(format!(
            "recovery: {n} replica-record-absent replay failure(s) exceed cap \
             ({cap}) — far more legacy replica `Create` entries reference \
             record bytes missing from this device than a stop-between-flushes \
             window can plausibly explain; verify device / path and \
             investigate before restarting [cause={cause}]",
            n = stats.failed_replica_record_absent,
            cap = MAX_TOLERATED_REPLICA_RECORD_ABSENT,
            cause = replay_cause_label(ReplayCause::ReplicaRecordAbsent),
        ));
    }
    Ok(())
}

/// Convert a [`ReplayCause`] into the human label used in tolerance
/// error messages. Kept `pub(crate)` so other diagnostic surfaces can
/// reuse the same wording.
///
/// F-G6-018: this function used to be `#[allow(dead_code)]` because no
/// caller existed. It now backs the cause-label suffix that
/// [`check_replay_tolerance_with_cap`] appends to its error messages,
/// so the label strings can never drift from the per-cause classifier.
pub(crate) fn replay_cause_label(cause: ReplayCause) -> &'static str {
    match cause {
        ReplayCause::MissingPrimary => "missing-primary",
        ReplayCause::IoError => "io-error",
        ReplayCause::CorruptEntry => "corrupt-entry",
        ReplayCause::LogicError => "logic-error",
        ReplayCause::MissingRecordBytes => "missing-record-bytes",
        ReplayCause::ReplicaRecordAbsent => "replica-record-absent",
    }
}

/// How the allocator returned by [`recover_or_create_allocator`] was
/// obtained, so the caller can log and apply device-identity policy
/// accordingly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllocatorOrigin {
    /// A valid persisted header was found and recovered.
    Recovered,
    /// The header region was all zeros (genuinely fresh device); a new
    /// allocator was created.
    Fresh,
}

/// Recover the allocator from the device header, creating a fresh one
/// ONLY when the device has never had a header persisted.
///
/// Audit B-2: a torn/corrupt allocator header must never silently fall
/// back to a fresh allocator — a fresh allocator restarts allocation at
/// the data-region start and its next creates overwrite live records.
/// The only error that may fall through to [`SlotAllocator::new`] is
/// [`AllocatorError::NoPersistedState`] (all-zero header region, i.e. a
/// genuinely fresh device); that fallthrough is logged explicitly.
///
/// # Errors
///
/// Propagates every other [`SlotAllocator::recover`] error unchanged —
/// [`AllocatorError::HeaderCorruption`] (CRC mismatch),
/// [`AllocatorError::CorruptedHeader`] (non-zero garbage header),
/// [`AllocatorError::UnsupportedVersion`], and device I/O errors — so
/// the caller fails closed at startup. Also propagates
/// [`SlotAllocator::new`] errors on the fresh path.
pub fn recover_or_create_allocator(
    device: Arc<dyn BlockDevice>,
) -> Result<(SlotAllocator, AllocatorOrigin), AllocatorError> {
    match SlotAllocator::recover(device.clone()) {
        Ok(alloc) => Ok((alloc, AllocatorOrigin::Recovered)),
        Err(AllocatorError::NoPersistedState) => {
            tracing::info!(
                "allocator header region is all zeros — fresh device, \
                 creating a new allocator"
            );
            SlotAllocator::new(device).map(|alloc| (alloc, AllocatorOrigin::Fresh))
        }
        Err(e) => Err(e),
    }
}

/// Recover-or-create the per-store allocator for the configured storage engine,
/// returning it boxed as a [`BoxedAllocator`] so the engine can hold either the
/// in-place [`SlotAllocator`] or the log-structured
/// [`crate::segment_allocator::SegmentAllocator`].
///
/// Each engine stamps a distinct on-disk header magic, so opening a device with
/// the wrong engine fails closed ([`AllocatorError::CorruptedHeader`]) rather
/// than misreading it. `NoPersistedState` (all-zero header) is the only "fresh"
/// signal; every other recover error propagates so a corrupt/foreign header
/// refuses to start.
pub fn recover_or_create_boxed_allocator(
    device: Arc<dyn BlockDevice>,
    engine: StorageEngine,
    segment_size: u64,
) -> Result<(BoxedAllocator, AllocatorOrigin), AllocatorError> {
    match engine {
        StorageEngine::InPlace => {
            let (alloc, origin) = recover_or_create_allocator(device)?;
            Ok((Box::new(alloc), origin))
        }
        StorageEngine::Segment => {
            use crate::segment_allocator::SegmentAllocator;
            match SegmentAllocator::recover(device.clone()) {
                Ok(alloc) => Ok((Box::new(alloc), AllocatorOrigin::Recovered)),
                Err(crate::segment_allocator::SegmentAllocatorError::NoPersistedState) => {
                    tracing::info!(
                        "segment allocator header region is all zeros — fresh device, \
                         creating a new segment allocator"
                    );
                    let alloc = SegmentAllocator::new(device, segment_size)
                        .map_err(crate::allocator::AllocatorError::from)?;
                    Ok((Box::new(alloc), AllocatorOrigin::Fresh))
                }
                Err(e) => {
                    tracing::error!(detail = %e, "segment allocator recover failed");
                    Err(e.into())
                }
            }
        }
    }
}

/// Reconcile a store's allocator packed-ness with the configured `packed`
/// flag, honoring the rule that the DEVICE's on-disk format always wins.
///
/// - **Fresh device** ([`AllocatorOrigin::Fresh`]): the device has no format
///   yet, so config decides — `set_packed(config_packed)` BEFORE any
///   allocation, so the first reservations pack and the first `persist`
///   stamps the packed header version.
/// - **Recovered device** ([`AllocatorOrigin::Recovered`]):
///   [`SlotAllocator::recover`] already set packed-ness from the header. The
///   config is NOT allowed to override it: reopening a packed device
///   non-packed (or vice versa) would corrupt it via `free()`'s block-rounding.
///   If config disagrees with the device, the device wins and a clear warning
///   is logged (packing a non-packed device, or un-packing a packed one,
///   requires a fresh device / migration — out of scope).
///
/// `store` is the store index, used only for log context.
pub fn apply_packed_mode(
    alloc: &mut dyn RecordAllocator,
    origin: AllocatorOrigin,
    config_packed: bool,
    store: usize,
) {
    match origin {
        AllocatorOrigin::Fresh => {
            // Fresh device: config decides the on-disk format.
            alloc.set_packed(config_packed);
            if config_packed {
                tracing::info!(
                    store,
                    "storage.packed = true: fresh device will use the packed record layout"
                );
            }
        }
        AllocatorOrigin::Recovered => {
            // Device format wins. Warn on any config/device mismatch but never
            // override the recovered packed-ness.
            let device_packed = alloc.is_packed();
            if config_packed && !device_packed {
                tracing::warn!(
                    store,
                    "storage.packed = true but this device recovered as NON-packed \
                     (existing data uses the block-per-record layout); honoring the \
                     device and staying NON-packed. Packing requires a fresh device / \
                     migration (no in-place migration exists)."
                );
            } else if !config_packed && device_packed {
                tracing::warn!(
                    store,
                    "storage.packed = false but this device recovered as PACKED; \
                     honoring the device and staying PACKED. Opening a packed device \
                     non-packed would corrupt it via free()'s block-rounding."
                );
            }
        }
    }
}

/// Load the redb primary index. Restore first, fall back to a
/// device-rebuild on a clean restore-error, fail closed otherwise.
///
/// On rebuild failure the redb file at [`IndexConfig::redb_path`] is
/// **not** removed — the operator must inspect it before deciding to
/// rescan. This is the gap #5 fail-closed contract.
///
/// Before any restore/rebuild attempt this function consults the
/// import-in-progress sentinel written by
/// [`crate::index::migration::import_index`]. If the sentinel exists
/// the redb files may be in a partial state from an interrupted
/// import; we refuse to proceed (R-047 / AUDIT GH-G3) and return
/// [`RebuildError::RedbImportInProgress`] so the operator can re-run
/// the import or manually clear the sentinel.
pub fn load_primary_index_redb(
    config: &IndexConfig,
    device: &dyn BlockDevice,
    allocator: &dyn RecordAllocator,
) -> Result<PrimaryBackend, RebuildError> {
    if crate::index::migration::import_in_progress(config) {
        let sentinel_path = crate::index::migration::import_sentinel_path(&config.redb_path);
        // F-G6-028: log the sentinel's mtime + wall-clock age so the
        // operator can distinguish "fresh sentinel from a crashed
        // import" from "stale sentinel restored from backup". The
        // refusal is unconditional (we don't auto-clear), but the log
        // line gives operators the context to decide quickly.
        if let Ok(meta) = std::fs::metadata(&sentinel_path)
            && let Ok(mtime) = meta.modified()
            && let Ok(age) = std::time::SystemTime::now().duration_since(mtime)
        {
            tracing::warn!(
                sentinel_path = %sentinel_path.display(),
                mtime_unix_secs = mtime
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
                age_secs = age.as_secs(),
                "redb import-in-progress sentinel detected — startup will refuse \
                 until the sentinel is cleared. Age hints at whether this is a \
                 fresh crash or a stale leftover.",
            );
        }
        return Err(RebuildError::RedbImportInProgress {
            sentinel_path: sentinel_path.display().to_string(),
        });
    }
    let restore_err = match PrimaryBackend::restore_redb(config) {
        Ok(idx) => return Ok(idx),
        Err(e) => e,
    };
    match PrimaryBackend::rebuild_redb(config, device, allocator) {
        Ok(idx) => Ok(idx),
        Err(rebuild_err) => Err(RebuildError::RedbPrimary {
            path: config.redb_path.display().to_string(),
            restore_err: format!("{restore_err}"),
            rebuild_err: format!("{rebuild_err}"),
        }),
    }
}

/// Load the file-backed mmap primary index. Restore first if the file
/// exists, fall back to device-rebuild, fail closed otherwise.
///
/// On rebuild failure the file at `path` is **not** removed.
pub fn load_primary_index_file_backed(
    path: &Path,
    expected_records: usize,
    device: &dyn BlockDevice,
    allocator: &dyn RecordAllocator,
) -> Result<PrimaryBackend, RebuildError> {
    let restore_suffix = if path.exists() {
        match PrimaryBackend::restore_file_backed(path, expected_records) {
            Ok(idx) => return Ok(idx),
            Err(e) => {
                // Explicit, logged decision (G-3): the existing file is
                // unusable (unclean shutdown, invalid size, mapping
                // failure) — fall back to the device-scan rebuild rather
                // than booting an empty index.
                tracing::warn!(
                    path = %path.display(),
                    err = %e,
                    "file-backed primary index restore failed — rebuilding \
                     from device scan",
                );
                format!("; restore failed ({e})")
            }
        }
    } else {
        String::new()
    };
    match PrimaryBackend::rebuild_file_backed(path, device, allocator) {
        Ok(idx) => Ok(idx),
        Err(rebuild_err) => Err(RebuildError::FileBackedPrimary {
            path: path.display().to_string(),
            rebuild_err: format!("{rebuild_err}"),
            restore_suffix,
        }),
    }
}

/// Rebuild the in-memory primary from the device. Fail closed on rebuild
/// error rather than starting with an empty index.
pub fn load_primary_index_in_memory(
    device: &dyn BlockDevice,
    allocator: &dyn RecordAllocator,
) -> Result<PrimaryBackend, RebuildError> {
    PrimaryBackend::rebuild(device, allocator).map_err(|e| RebuildError::InMemoryPrimary {
        rebuild_err: format!("{e}"),
    })
}

/// Rebuild the in-memory sharded primary index from the device.
///
/// Delegates to [`ShardedIndex::rebuild_in_memory`], which internally calls
/// the proven [`PrimaryBackend::rebuild`] device scan and re-routes every
/// entry into the correct shard. Fail closed on any rebuild error — starting
/// with an empty or partial sharded index is not an option.
///
/// # Parameters
///
/// - `device`: block device to scan.
/// - `allocator`: slot allocator whose freelist is used to skip free holes.
/// - `shard_count`: number of index shards to create (rounded up to the next
///   power of two, clamped to `[1, 256]`).
/// - `expected_records`: configured steady-state record count. Each shard's
///   hash table is pre-sized to `max(scanned_count, expected_records)` so a
///   fresh/empty device still allocates steady-state capacity and avoids a
///   rehash-under-write-guard resize storm on the create path.
///
/// # Errors
///
/// Returns [`RebuildError::InMemoryPrimary`] if the device scan or shard
/// routing fails, carrying a `Display`-formatted description of the
/// underlying [`IndexError`].
pub fn load_sharded_index_in_memory(
    device: &dyn BlockDevice,
    allocator: &dyn RecordAllocator,
    shard_count: usize,
    expected_records: usize,
) -> Result<ShardedIndex, RebuildError> {
    ShardedIndex::rebuild_in_memory(device, allocator, shard_count, expected_records).map_err(|e| {
        RebuildError::InMemoryPrimary {
            rebuild_err: format!("{e}"),
        }
    })
}

/// Multi-store variant of [`load_sharded_index_in_memory`]: scan EVERY store's
/// device, stamping each discovered record's `device_id` from the store it was
/// found on, so a device-scan rebuild (snapshot lost/corrupt) recovers records
/// placed across all stores — not just store 0.
///
/// Records are round-robin placed across the configured stores and routed by the
/// index entry's `device_id`; a single-device scan would index only the records
/// that happened to land on store 0 and silently lose the rest.
///
/// `expected_records` pre-sizes each shard's hash table to
/// `max(total_scanned_count, expected_records)`, so a fresh/empty cluster of
/// stores still allocates steady-state capacity and avoids a resize storm.
///
/// # Errors
///
/// Returns [`RebuildError::InMemoryPrimary`] if any store's device scan or shard
/// routing fails.
pub fn load_sharded_index_in_memory_multi(
    devices: &[std::sync::Arc<dyn BlockDevice>],
    allocators: &[BoxedAllocator],
    shard_count: usize,
    expected_records: usize,
) -> Result<ShardedIndex, RebuildError> {
    ShardedIndex::rebuild_in_memory_multi_store(devices, allocators, shard_count, expected_records)
        .map_err(|e| RebuildError::InMemoryPrimary {
            rebuild_err: format!("{e}"),
        })
}

/// Rebuild secondary indexes from the device, returning a
/// [`SecondaryLoadOutcome`] that includes per-secondary readiness flags.
///
/// On rebuild failure both secondaries fall through to empty in-memory
/// backends and both flags flip to `false`. The dispatch readiness gate
/// then rejects endpoints that depend on the missing data.
pub fn rebuild_in_memory_secondaries(
    device: &dyn BlockDevice,
    allocator: &dyn RecordAllocator,
) -> SecondaryLoadOutcome {
    match PrimaryBackend::rebuild_secondary(device, allocator) {
        Ok((dah, unmined)) => SecondaryLoadOutcome {
            dah: DahBackend::from(dah),
            unmined: UnminedBackend::from(unmined),
            status: SecondaryStatus {
                dah_ok: true,
                unmined_ok: true,
            },
        },
        Err(e) => {
            tracing::error!(
                err = %e,
                "secondary index rebuild failed — node will start with degraded \
                 readiness; pruner / unmined / DAH / conflict / mining endpoints \
                 will reject requests with ERR_INDEX_DEGRADED until the operator \
                 investigates and restarts (gap #5)",
            );
            SecondaryLoadOutcome {
                dah: DahBackend::from(DahIndex::new()),
                unmined: UnminedBackend::from(UnminedIndex::new()),
                status: SecondaryStatus {
                    dah_ok: false,
                    unmined_ok: false,
                },
            }
        }
    }
}

/// Wrap a successful pair of in-memory secondaries in a
/// [`SecondaryLoadOutcome`] with both flags set to `true`. Used by the
/// snapshot-restore path where the rebuild was unnecessary.
pub fn secondaries_from_pair(dah: DahIndex, unmined: UnminedIndex) -> SecondaryLoadOutcome {
    SecondaryLoadOutcome {
        dah: DahBackend::from(dah),
        unmined: UnminedBackend::from(unmined),
        status: SecondaryStatus {
            dah_ok: true,
            unmined_ok: true,
        },
    }
}

/// Translate an [`IndexError`] from a one-shot DAH or unmined open
/// attempt (redb backend) into the operator-facing degraded-readiness
/// log line. Returns the empty in-memory fallback the caller should use.
///
/// `which` should be `"DAH"` or `"unmined"` for the log message.
pub fn fallback_dah_index(which: &str, err: IndexError) -> DahBackend {
    tracing::error!(
        index = which,
        err = %err,
        "secondary {which} index unavailable — node will start with degraded \
         readiness; dependent endpoints will reject with ERR_INDEX_DEGRADED \
         until the operator investigates and restarts (gap #5)",
    );
    DahBackend::new_in_memory()
}

/// Sibling of [`fallback_dah_index`] for the unmined secondary index.
pub fn fallback_unmined_index(which: &str, err: IndexError) -> UnminedBackend {
    tracing::error!(
        index = which,
        err = %err,
        "secondary {which} index unavailable — node will start with degraded \
         readiness; dependent endpoints will reject with ERR_INDEX_DEGRADED \
         until the operator investigates and restarts (gap #5)",
    );
    UnminedBackend::new_in_memory()
}

// ---------------------------------------------------------------------------
// Mandatory redo log open (gap #2)
// ---------------------------------------------------------------------------

/// Errors raised by [`open_mandatory_redo_log`] when the redo log cannot
/// be made available at startup.
///
/// The redo log is mandatory under the WAL-first durability contract
/// (gap #2 — TERANODE_PRODUCTION_READINESS_GAPS.md). A missing or
/// unwritable log means every WAL fsync ack would be a lie, so we fail
/// closed at startup and let the operator fix the underlying device or
/// path before retrying.
#[derive(Error, Debug)]
pub enum RedoOpenError {
    /// The configured redo-log path could not be opened or created as a
    /// `DirectDevice`. The variant carries the path and a `Display`-
    /// formatted reason so the operator can pinpoint the underlying
    /// permission / disk / path issue.
    #[error(
        "redo log device unavailable at {path}: {reason}; mandatory WAL \
         requires a writable path — fix permissions / disk / config and retry"
    )]
    Device {
        /// Path the operator configured.
        path: String,
        /// `Display`-formatted [`crate::device::DeviceError`] from the open call.
        /// Stored as a string (not `#[source]`) so the variant remains
        /// constructible from any `Display` cause without coupling to the
        /// device error type.
        reason: String,
    },

    /// `DirectDevice::open` succeeded but [`crate::redo::RedoLog::open`]
    /// returned an error (e.g. the configured size does not fit on the
    /// device, or the existing log is corrupt). Both cases are fail-closed
    /// because continuing without a working redo log silently downgrades
    /// the durability contract.
    #[error(
        "redo log open failed at {path}: {reason}; mandatory WAL requires a \
         valid log — investigate the underlying redo error and retry"
    )]
    Log {
        /// Path the operator configured.
        path: String,
        /// `Display`-formatted [`crate::redo::RedoError`] from `RedoLog::open`.
        reason: String,
    },
}

/// Open or create the redo log at `path` and prepare a [`crate::redo::RedoLog`].
///
/// This is the gap #2 mandatory-redo entry point: any failure surfaces
/// as a [`RedoOpenError`] that the binary turns into a non-zero exit.
/// There is **no in-memory fallback** in production — that path was
/// removed because it broke the WAL-first durability promise.
///
/// On success the caller receives the device handle (kept alive for the
/// lifetime of the server so future replay/extension paths can share the
/// same fd) and the open `RedoLog`. The caller is expected to wrap the
/// log in `Arc<Mutex<RedoLog>>` for shared dispatch access.
///
/// # Errors
///
/// * [`RedoOpenError::Device`] — `DirectDevice::open` failed (path
///   missing, permissions denied, parent dir not writable, alignment
///   mismatch, etc.).
/// * [`RedoOpenError::Log`] — the device opened but `RedoLog::open`
///   could not establish a valid log (bounds, corrupt history, etc.).
///
/// `buffered_io` selects the device open mode: `false` (default / strict)
/// opens the redo with `O_DIRECT` (Linux) / `F_NOCACHE` (macOS) exactly as
/// before; `true` opens it through the OS page cache
/// ([`crate::device::DirectDevice::open_buffered`]) for the relaxed
/// `redo_buffered_io` mode. It affects ONLY the redo device — the data
/// device(s) are opened elsewhere and always stay `O_DIRECT`.
pub fn open_mandatory_redo_log(
    path: &Path,
    size: u64,
    alignment: usize,
    segment_ring: Option<u64>,
    buffered_io: bool,
) -> Result<
    (
        std::sync::Arc<dyn crate::device::BlockDevice>,
        crate::redo::RedoLog,
    ),
    RedoOpenError,
> {
    let open_res = if buffered_io {
        crate::device::DirectDevice::open_buffered(path, size, alignment)
    } else {
        crate::device::DirectDevice::open(path, size, alignment)
    };
    let device = open_res.map_err(|e| RedoOpenError::Device {
        path: path.display().to_string(),
        reason: format!("{e}"),
    })?;
    let device: std::sync::Arc<dyn crate::device::BlockDevice> = std::sync::Arc::new(device);
    let log =
        crate::redo::RedoLog::open(device.clone(), 0, size).map_err(|e| RedoOpenError::Log {
            path: path.display().to_string(),
            reason: format!("{e}"),
        })?;

    // Lever 7: adopt the segment-ring layout per config, with the same
    // "device-format-wins" discipline as `storage.packed`:
    //   * an existing on-disk ring is always used as a ring (handled above by
    //     `RedoLog::open`);
    //   * a FRESH region (never written: current_sequence == 1) adopts the ring;
    //   * an existing NON-EMPTY linear region stays linear this session (the
    //     ring is not read-compatible and we will not discard live redo) — the
    //     operator drains (clean shutdown) + resets the region to adopt it.
    if let Some(seg_cfg) = segment_ring {
        if log.is_segment_ring() {
            return Ok((device, log));
        }
        if log.current_sequence() == 1 {
            let seg_size = if seg_cfg == 0 {
                derive_ring_segment_size(size, alignment)
            } else {
                seg_cfg
            };
            let ring = crate::redo::RedoLog::format_ring(device.clone(), 0, size, seg_size)
                .map_err(|e| RedoOpenError::Log {
                    path: path.display().to_string(),
                    reason: format!("{e}"),
                })?;
            return Ok((device, ring));
        }
        tracing::warn!(
            path = %path.display(),
            "redo_segment_ring enabled but the redo region holds existing linear data — \
             staying on the linear layout this session; drain (clean shutdown) and reset \
             the redo region to adopt the segment ring",
        );
    }
    Ok((device, log))
}

/// Auto-derive a ring segment size (~8 segments) from the redo region size,
/// rounded down to the device alignment and floored at one alignment block so a
/// tiny region still yields whole segments.
fn derive_ring_segment_size(size: u64, alignment: usize) -> u64 {
    let align = alignment as u64;
    let entries = size.saturating_sub(align);
    let eighth = (entries / 8) / align * align;
    eighth.max(align)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::SlotAllocator;
    use crate::config::IndexConfig;
    use crate::device::MemoryDevice;
    use std::sync::Arc;
    use tempfile::TempDir;

    /// Build a dev + allocator pair backed by a fresh in-memory device.
    fn fresh_dev_alloc() -> (Arc<MemoryDevice>, SlotAllocator) {
        let dev = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone()).unwrap();
        (dev, alloc)
    }

    // -----------------------------------------------------------------------
    // Replay tolerance check
    // -----------------------------------------------------------------------

    #[test]
    fn replay_tolerance_accepts_zero_failures() {
        let stats = RecoveryStats::default();
        check_replay_tolerance(&stats).expect("zero failures must be tolerated");
    }

    #[test]
    fn replay_tolerance_accepts_high_missing_primary_below_cap() {
        let stats = RecoveryStats {
            failed_missing_primary: MAX_TOLERATED_MISSING_PRIMARY,
            entries_failed: MAX_TOLERATED_MISSING_PRIMARY,
            ..RecoveryStats::default()
        };
        check_replay_tolerance(&stats).expect("missing-primary at the cap must still be tolerated");
    }

    #[test]
    fn replay_tolerance_rejects_one_io_error() {
        let stats = RecoveryStats {
            failed_io: 1,
            entries_failed: 1,
            ..RecoveryStats::default()
        };
        let err = check_replay_tolerance(&stats).expect_err("io-error must fail closed");
        assert!(err.contains("device I/O"), "msg: {err}");
        assert!(err.contains("non-tolerable"), "msg: {err}");
    }

    #[test]
    fn replay_tolerance_rejects_one_corrupt_entry() {
        let stats = RecoveryStats {
            failed_corrupt: 1,
            entries_failed: 1,
            ..RecoveryStats::default()
        };
        let err = check_replay_tolerance(&stats).expect_err("corrupt-entry must fail closed");
        assert!(err.contains("corrupt"), "msg: {err}");
    }

    #[test]
    fn replay_tolerance_rejects_one_logic_error() {
        let stats = RecoveryStats {
            failed_logic: 1,
            entries_failed: 1,
            ..RecoveryStats::default()
        };
        let err = check_replay_tolerance(&stats).expect_err("logic-error must fail closed");
        assert!(err.contains("logic"), "msg: {err}");
    }

    #[test]
    fn replay_tolerance_rejects_missing_primary_above_cap() {
        let n = MAX_TOLERATED_MISSING_PRIMARY + 1;
        let stats = RecoveryStats {
            failed_missing_primary: n,
            entries_failed: n,
            ..RecoveryStats::default()
        };
        let err =
            check_replay_tolerance(&stats).expect_err("missing-primary over cap must fail closed");
        assert!(err.contains("missing-primary"), "msg: {err}");
        assert!(err.contains("cap"), "msg: {err}");
    }

    #[test]
    fn replay_tolerance_uses_configured_missing_primary_cap() {
        let stats = RecoveryStats {
            failed_missing_primary: 11,
            entries_failed: 11,
            ..RecoveryStats::default()
        };
        check_replay_tolerance_with_cap(&stats, 11)
            .expect("configured cap should accept at-threshold missing-primary count");
        let err = check_replay_tolerance_with_cap(&stats, 10)
            .expect_err("configured cap should reject over-threshold count");
        assert!(err.contains("(10)"), "msg: {err}");
    }

    #[test]
    fn replay_tolerance_accepts_replica_record_absent_below_cap() {
        // scenario_09: a legacy replica `Create` whose record bytes are not
        // durable on this node is tolerable (the master re-replicates) so
        // the node boots instead of crash-looping.
        let stats = RecoveryStats {
            failed_replica_record_absent: MAX_TOLERATED_REPLICA_RECORD_ABSENT,
            entries_failed: MAX_TOLERATED_REPLICA_RECORD_ABSENT,
            ..RecoveryStats::default()
        };
        check_replay_tolerance(&stats)
            .expect("replica-record-absent at the cap must still be tolerated");
    }

    #[test]
    fn replay_tolerance_rejects_replica_record_absent_above_cap() {
        let n = MAX_TOLERATED_REPLICA_RECORD_ABSENT + 1;
        let stats = RecoveryStats {
            failed_replica_record_absent: n,
            entries_failed: n,
            ..RecoveryStats::default()
        };
        let err = check_replay_tolerance(&stats)
            .expect_err("replica-record-absent over cap must fail closed");
        assert!(err.contains("replica-record-absent"), "msg: {err}");
        assert!(err.contains("cap"), "msg: {err}");
    }

    #[test]
    fn replay_tolerance_rejects_one_missing_record_bytes() {
        // The Create short-I/O class stays fatal regardless of count —
        // it signals a misbehaving device, distinct from the tolerable
        // legacy replica-record-absent class.
        let stats = RecoveryStats {
            failed_missing_record_bytes: 1,
            entries_failed: 1,
            ..RecoveryStats::default()
        };
        let err =
            check_replay_tolerance(&stats).expect_err("missing-record-bytes must fail closed");
        assert!(err.contains("record bytes"), "msg: {err}");
        assert!(err.contains("non-tolerable"), "msg: {err}");
    }

    #[test]
    fn replay_tolerance_io_takes_priority_over_missing_primary() {
        // If both classes appear in the same run, the non-tolerable cause
        // must dominate the verdict.
        let stats = RecoveryStats {
            failed_missing_primary: 5,
            failed_io: 1,
            entries_failed: 6,
            ..RecoveryStats::default()
        };
        let err = check_replay_tolerance(&stats)
            .expect_err("any io-error must fail closed even with benign cases present");
        assert!(err.contains("device I/O"), "msg: {err}");
    }

    // -----------------------------------------------------------------------
    // Primary rebuild — fail closed
    // -----------------------------------------------------------------------

    #[test]
    fn redb_primary_rebuild_failure_preserves_file() {
        // G-7: write garbage into the redb primary path so `restore_redb`
        // fails. `rebuild_redb` then re-opens the SAME corrupt path via
        // `redb::Builder::create`, which cannot turn the garbage bytes into
        // a valid redb database, so the rebuild also fails. The fail-closed
        // contract (README.md:644) is that this surfaces a typed
        // `RebuildError::RedbPrimary` with BOTH the restore and rebuild
        // causes populated, and the corrupt file is preserved untouched for
        // the operator to inspect — it is never silently deleted/recreated.
        let tmp = TempDir::new().unwrap();
        let redb_path = tmp.path().join("primary.redb");
        let dah_path = tmp.path().join("dah.redb");
        let unmined_path = tmp.path().join("unmined.redb");
        std::fs::write(&redb_path, b"this is not a redb file").unwrap();
        let original_bytes = std::fs::read(&redb_path).unwrap();

        let cfg = IndexConfig {
            redb_path: redb_path.clone(),
            redb_dah_path: dah_path,
            redb_unmined_path: unmined_path,
            ..IndexConfig::default()
        };

        let (dev, alloc) = fresh_dev_alloc();
        let result = load_primary_index_redb(&cfg, &*dev, &alloc);
        match result {
            Err(RebuildError::RedbPrimary {
                ref path,
                ref restore_err,
                ref rebuild_err,
            }) => {
                assert_eq!(path, &redb_path.display().to_string());
                assert!(
                    !restore_err.is_empty(),
                    "restore_err cause must be populated"
                );
                assert!(
                    !rebuild_err.is_empty(),
                    "rebuild_err cause must be populated"
                );
            }
            other => panic!("expected fail-closed RebuildError::RedbPrimary, got {other:?}"),
        }
        assert!(
            redb_path.exists(),
            "redb primary file must be preserved across load_primary_index_redb"
        );
        assert_eq!(
            std::fs::read(&redb_path).unwrap(),
            original_bytes,
            "redb primary file bytes must be preserved untouched on the fail-closed path"
        );
    }

    #[test]
    fn redb_primary_rebuild_succeeds_after_corrupt_file_removed() {
        // F-G1: proves the operator recovery path for a corrupt redb primary.
        // The fail-closed contract (see `redb_primary_rebuild_failure_preserves_file`)
        // intentionally preserves a corrupt file and refuses to auto-rescan —
        // a silent full device scan could mask a failing disk. Once the operator
        // moves the corrupt file aside, `load_primary_index_redb` must rebuild
        // the index from a device scan and recover every record.
        use crate::index::TxKey;
        use crate::io::write_full_record;
        use crate::record::{TxMetadata, UtxoSlot};

        // Ground truth: 5 real records on the device.
        let dev = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
        let mut records = Vec::new();
        for i in 0..5u64 {
            let mut meta = TxMetadata::new(5);
            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&i.to_le_bytes());
            txid[8..16].copy_from_slice(&i.wrapping_mul(0x9E3779B97F4A7C15).to_le_bytes());
            meta.tx_id = txid;
            let offset = alloc.allocate(TxMetadata::record_size_for(5)).unwrap();
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

        let tmp = TempDir::new().unwrap();
        let redb_path = tmp.path().join("primary.redb");
        let cfg = IndexConfig {
            redb_path: redb_path.clone(),
            redb_dah_path: tmp.path().join("dah.redb"),
            redb_unmined_path: tmp.path().join("unmined.redb"),
            ..IndexConfig::default()
        };

        // Corrupt primary → fail closed (preserves file, no silent auto-rescan).
        std::fs::write(&redb_path, b"this is not a redb file").unwrap();
        let err = load_primary_index_redb(&cfg, &*dev, &alloc)
            .expect_err("corrupt primary must fail closed");
        assert!(
            matches!(err, RebuildError::RedbPrimary { .. }),
            "expected fail-closed RedbPrimary, got {err:?}"
        );
        assert!(redb_path.exists(), "corrupt file preserved for inspection");

        // Operator action: move the corrupt file aside (per the runbook).
        std::fs::rename(&redb_path, tmp.path().join("primary.redb.corrupt")).unwrap();

        // Reboot: device-scan rebuild succeeds and recovers every record.
        let loaded = load_primary_index_redb(&cfg, &*dev, &alloc)
            .expect("rebuild from device scan must succeed once the corrupt file is removed");
        assert_eq!(
            loaded.len(),
            5,
            "rebuilt index must hold all device records"
        );
        for (key, offset) in &records {
            let e = loaded
                .lookup(key)
                .expect("device record must be present after rebuild");
            assert_eq!(e.record_offset, *offset);
        }
    }

    #[test]
    fn startup_refuses_when_import_sentinel_present() {
        // R-047: If `import_index` crashed mid-way it leaves the
        // sentinel file behind. `load_primary_index_redb` MUST refuse
        // to open the (possibly partial) redb files in that state.
        let tmp = TempDir::new().unwrap();
        let redb_path = tmp.path().join("primary.redb");
        let dah_path = tmp.path().join("dah.redb");
        let unmined_path = tmp.path().join("unmined.redb");

        // Pre-populate the redb file so restore_redb would otherwise
        // succeed; without the sentinel check the partial-state risk
        // would slip through silently.
        let _ = crate::index::redb_primary::RedbPrimary::open(&redb_path, 64 * 1024 * 1024)
            .expect("create primary redb for sentinel test");

        // Manually drop a sentinel file in the conventional location.
        let sentinel = crate::index::migration::import_sentinel_path(&redb_path);
        std::fs::write(&sentinel, b"in progress").unwrap();

        let cfg = IndexConfig {
            redb_path: redb_path.clone(),
            redb_dah_path: dah_path,
            redb_unmined_path: unmined_path,
            ..IndexConfig::default()
        };
        let (dev, alloc) = fresh_dev_alloc();
        let err = load_primary_index_redb(&cfg, &*dev, &alloc)
            .expect_err("startup must refuse while sentinel exists");
        match err {
            RebuildError::RedbImportInProgress { ref sentinel_path } => {
                assert_eq!(sentinel_path, &sentinel.display().to_string());
            }
            other => panic!("expected RedbImportInProgress, got {other:?}"),
        }

        // The redb file MUST be preserved untouched — the operator
        // must investigate and re-run the import.
        assert!(redb_path.exists(), "redb file must not be removed");
        assert!(sentinel.exists(), "sentinel must not be removed");

        // Operator workflow: removing the sentinel after manual
        // verification re-enables startup.
        std::fs::remove_file(&sentinel).unwrap();
        load_primary_index_redb(&cfg, &*dev, &alloc)
            .expect("startup must succeed once sentinel is cleared");
    }

    #[test]
    fn redb_import_in_progress_error_message_includes_remediation() {
        // Operator-facing display contract for the new RebuildError
        // variant. Log scrapers and dashboards depend on the wording
        // identifying the sentinel path and pointing to the import
        // CLI.
        let err = RebuildError::RedbImportInProgress {
            sentinel_path: "/data/primary.redb.import-in-progress".to_string(),
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("/data/primary.redb.import-in-progress"),
            "msg: {msg}"
        );
        assert!(msg.contains("import was interrupted"), "msg: {msg}");
        assert!(msg.contains("Re-run"), "msg: {msg}");
    }

    #[test]
    fn redb_primary_rebuild_error_message_contains_path_and_hint() {
        // Construct a RebuildError directly and verify the operator-facing
        // text. This is the contract the dashboard / log scrapers depend on.
        let err = RebuildError::RedbPrimary {
            path: "/data/primary.redb".to_string(),
            restore_err: "checksum mismatch".to_string(),
            rebuild_err: "device read returned EIO".to_string(),
        };
        let msg = format!("{err}");
        assert!(msg.contains("/data/primary.redb"), "msg: {msg}");
        assert!(msg.contains("checksum mismatch"), "msg: {msg}");
        assert!(msg.contains("device read returned EIO"), "msg: {msg}");
        assert!(msg.contains("explicit rescan"), "msg: {msg}");
    }

    #[test]
    fn file_backed_primary_rebuild_error_includes_restore_suffix() {
        let err = RebuildError::FileBackedPrimary {
            path: "/data/primary.dat".to_string(),
            rebuild_err: "device EIO".to_string(),
            restore_suffix: "; restore failed (truncated)".to_string(),
        };
        let msg = format!("{err}");
        assert!(msg.contains("/data/primary.dat"), "msg: {msg}");
        assert!(msg.contains("device EIO"), "msg: {msg}");
        assert!(msg.contains("restore failed (truncated)"), "msg: {msg}");
    }

    #[test]
    fn file_backed_unclean_shutdown_triggers_device_rebuild() {
        // G-01: an existing file-backed index whose clean-shutdown
        // sentinel is missing may contain torn bucket bytes. The restore
        // must fail closed and `load_primary_index_file_backed` must
        // auto-fall back to the device-scan rebuild, so the booted index
        // reflects the device — not the possibly-torn file.
        use crate::index::{TxIndexEntry, TxKey};
        use crate::io::write_full_record;
        use crate::record::{TxMetadata, UtxoSlot};

        // Ground truth: 5 real records on the device.
        let dev = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
        let mut records = Vec::new();
        for i in 0..5u64 {
            let mut meta = TxMetadata::new(5);
            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&i.to_le_bytes());
            txid[8..16].copy_from_slice(&i.wrapping_mul(0x9E3779B97F4A7C15).to_le_bytes());
            meta.tx_id = txid;
            let offset = alloc.allocate(TxMetadata::record_size_for(5)).unwrap();
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

        // Previous run: file-backed index holding a bogus entry that is
        // NOT on the device. Drop writes the sentinel; removing it
        // simulates the crash.
        let tmp = TempDir::new().unwrap();
        let fb_path = tmp.path().join("primary.idx");
        let bogus_key = TxKey { txid: [0xAB; 32] };
        {
            let mut backend = PrimaryBackend::new_file_backed(&fb_path, 100).unwrap();
            let entry = TxIndexEntry {
                device_id: 0,
                record_offset: 0xDEAD_0000,
                utxo_count: 1,
                block_entry_count: 0,
                tx_flags: 0,
                spent_utxos: 0,
                dah_or_preserve: 0,
                unmined_since: 0,
                generation: 0,
            };
            backend.register(bogus_key, entry).unwrap();
        }
        let mut sentinel_os = fb_path.as_os_str().to_owned();
        sentinel_os.push(crate::index::hashtable::HashTable::SHUTDOWN_CLEAN_SUFFIX);
        let sentinel = std::path::PathBuf::from(sentinel_os);
        assert!(sentinel.exists(), "clean drop must have written sentinel");
        std::fs::remove_file(&sentinel).unwrap();

        // Boot: restore fails closed (unclean shutdown), rebuild kicks in.
        let loaded = load_primary_index_file_backed(&fb_path, 100, &*dev, &alloc)
            .expect("unclean shutdown must fall back to device-scan rebuild");
        assert_eq!(loaded.backend_name(), "file_backed");
        assert_eq!(
            loaded.len(),
            5,
            "rebuilt index must hold the device records"
        );
        for (key, offset) in &records {
            let e = loaded
                .lookup(key)
                .expect("device record must be present after rebuild");
            assert_eq!(e.record_offset, *offset);
        }
        assert!(
            loaded.lookup(&bogus_key).is_none(),
            "stale pre-crash entry must not survive the device rebuild"
        );
    }

    #[test]
    fn file_backed_invalid_size_triggers_device_rebuild() {
        // G-3: an existing file-backed index with an invalid size
        // (truncated) must NOT be silently wiped and booted as an empty
        // index — restore must fail closed and
        // `load_primary_index_file_backed` must fall back to the
        // device-scan rebuild so the booted index reflects the device.
        use crate::index::{TxIndexEntry, TxKey};
        use crate::io::write_full_record;
        use crate::record::{TxMetadata, UtxoSlot};

        // Ground truth: 5 real records on the device.
        let dev = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
        let mut records = Vec::new();
        for i in 0..5u64 {
            let mut meta = TxMetadata::new(5);
            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&i.to_le_bytes());
            txid[8..16].copy_from_slice(&i.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes());
            meta.tx_id = txid;
            let offset = alloc.allocate(TxMetadata::record_size_for(5)).unwrap();
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

        // Previous run: file-backed index dropped cleanly (sentinel
        // written), then the file is truncated to an invalid size —
        // e.g. a disk-full partial copy or filesystem corruption.
        let tmp = TempDir::new().unwrap();
        let fb_path = tmp.path().join("primary.idx");
        {
            let mut backend = PrimaryBackend::new_file_backed(&fb_path, 100).unwrap();
            let entry = TxIndexEntry {
                device_id: 0,
                record_offset: 0xDEAD_0000,
                utxo_count: 1,
                block_entry_count: 0,
                tx_flags: 0,
                spent_utxos: 0,
                dah_or_preserve: 0,
                unmined_since: 0,
                generation: 0,
            };
            backend.register(TxKey { txid: [0xAB; 32] }, entry).unwrap();
        }
        let valid_len = std::fs::metadata(&fb_path).unwrap().len();
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&fb_path)
            .unwrap();
        f.set_len(valid_len - 100).unwrap();
        drop(f);

        // Boot: restore fails closed (invalid size), rebuild kicks in.
        let loaded = load_primary_index_file_backed(&fb_path, 100, &*dev, &alloc)
            .expect("invalid-size file must fall back to device-scan rebuild");
        assert_eq!(loaded.backend_name(), "file_backed");
        assert_eq!(
            loaded.len(),
            5,
            "rebuilt index must hold the device records, not boot empty"
        );
        for (key, offset) in &records {
            let e = loaded
                .lookup(key)
                .expect("device record must be present after rebuild");
            assert_eq!(e.record_offset, *offset);
        }
    }

    // -----------------------------------------------------------------------
    // Allocator recover-or-create (audit B-2)
    // -----------------------------------------------------------------------

    #[test]
    fn recover_or_create_allocator_fresh_device_starts_fresh() {
        // A genuinely blank device (all-zero header) is the ONLY case
        // that may produce a fresh allocator.
        let dev = Arc::new(MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap());
        let (mut alloc, origin) =
            recover_or_create_allocator(dev).expect("blank device must start fresh");
        assert_eq!(origin, AllocatorOrigin::Fresh);
        // The fresh allocator is fully usable.
        let offset = alloc.allocate(4096).expect("fresh allocator must allocate");
        assert_eq!(offset % 4096, 0);
    }

    #[test]
    fn recover_or_create_allocator_recovers_persisted_state() {
        let dev = Arc::new(MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap());
        let o1;
        {
            let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
            o1 = alloc.allocate(8192).unwrap();
            alloc.persist().unwrap();
        }
        let (mut alloc, origin) =
            recover_or_create_allocator(dev).expect("persisted header must recover");
        assert_eq!(origin, AllocatorOrigin::Recovered);
        // A recovered allocator must not re-allocate the live region.
        let o2 = alloc.allocate(4096).unwrap();
        assert!(
            o2 >= o1 + 8192 || o2 + 4096 <= o1,
            "recovered allocator must not overlap live region [{o1}, {})",
            o1 + 8192
        );
    }

    #[test]
    fn recover_or_create_allocator_corrupt_header_fails_closed() {
        // B-2: a torn/corrupt header must abort startup, never fall back
        // to a fresh allocator whose creates would overwrite live records.
        use crate::device::AlignedBuf;

        let dev = Arc::new(MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap());
        {
            let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
            alloc.allocate(8192).unwrap();
            alloc.persist().unwrap();
        }
        // Tear the header: flip the next_offset field (bytes 8..16),
        // magic and stored CRC left intact -> CRC verification fails.
        let mut buf = AlignedBuf::new(4096, 4096);
        dev.pread(&mut buf, 0).unwrap();
        for b in &mut buf[8..16] {
            *b ^= 0xFF;
        }
        dev.pwrite(&buf, 0).unwrap();

        match recover_or_create_allocator(dev) {
            Err(AllocatorError::HeaderCorruption { expected, actual }) => {
                assert_ne!(expected, actual, "CRC mismatch must be reported");
            }
            Err(other) => panic!("expected HeaderCorruption, got: {other}"),
            Ok((_, origin)) => {
                panic!("corrupt header must fail closed, got a {origin:?} allocator")
            }
        }
    }

    // -----------------------------------------------------------------------
    // Packed-mode startup wiring (apply_packed_mode): fresh adopts config,
    // recovered honors the device (device format wins).
    // -----------------------------------------------------------------------

    #[test]
    fn apply_packed_mode_fresh_device_adopts_config_on() {
        // Fresh device + config packed -> allocator becomes packed before use.
        let dev = Arc::new(MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap());
        let (mut alloc, origin) = recover_or_create_allocator(dev).expect("fresh");
        assert_eq!(origin, AllocatorOrigin::Fresh);
        assert!(!alloc.is_packed(), "fresh allocator starts non-packed");
        apply_packed_mode(&mut alloc, origin, true, 0);
        assert!(alloc.is_packed(), "fresh + config packed -> packed");
    }

    #[test]
    fn apply_packed_mode_fresh_device_adopts_config_off() {
        let dev = Arc::new(MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap());
        let (mut alloc, origin) = recover_or_create_allocator(dev).expect("fresh");
        apply_packed_mode(&mut alloc, origin, false, 0);
        assert!(!alloc.is_packed(), "fresh + config off -> non-packed");
    }

    #[test]
    fn apply_packed_mode_recovered_packed_device_wins_over_config_off() {
        // A v2 (packed) device must STAY packed even when config says off —
        // opening it non-packed would corrupt it via free()'s block-rounding.
        let dev = Arc::new(MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap());
        {
            let mut a = SlotAllocator::new(dev.clone()).unwrap();
            a.set_packed(true);
            a.allocate(600).unwrap();
            a.persist().unwrap();
        }
        let (mut alloc, origin) = recover_or_create_allocator(dev).expect("recover packed");
        assert_eq!(origin, AllocatorOrigin::Recovered);
        assert!(alloc.is_packed(), "recovered device is packed");
        // Config says OFF, but the device wins.
        apply_packed_mode(&mut alloc, origin, false, 0);
        assert!(
            alloc.is_packed(),
            "recovered packed device must stay packed regardless of config"
        );
    }

    #[test]
    fn apply_packed_mode_recovered_nonpacked_device_wins_over_config_on() {
        // A v1 (non-packed) device must STAY non-packed even when config says
        // packed — packing existing data needs a fresh device / migration.
        let dev = Arc::new(MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap());
        {
            let mut a = SlotAllocator::new(dev.clone()).unwrap();
            a.allocate(600).unwrap();
            a.persist().unwrap();
        }
        let (mut alloc, origin) = recover_or_create_allocator(dev).expect("recover non-packed");
        assert_eq!(origin, AllocatorOrigin::Recovered);
        assert!(!alloc.is_packed());
        apply_packed_mode(&mut alloc, origin, true, 0);
        assert!(
            !alloc.is_packed(),
            "recovered non-packed device must stay non-packed regardless of config"
        );
    }

    #[test]
    fn in_memory_primary_rebuild_error_includes_underlying_cause() {
        let err = RebuildError::InMemoryPrimary {
            rebuild_err: "device error: short read".to_string(),
        };
        let msg = format!("{err}");
        assert!(msg.contains("short read"), "msg: {msg}");
        assert!(msg.contains("in-memory"), "msg: {msg}");
    }

    // -----------------------------------------------------------------------
    // Secondary rebuild — degraded readiness, not empty start
    // -----------------------------------------------------------------------

    #[test]
    fn secondaries_from_pair_marks_both_ok() {
        let dah = DahIndex::new();
        let unmined = UnminedIndex::new();
        let outcome = secondaries_from_pair(dah, unmined);
        assert!(outcome.status.dah_ok);
        assert!(outcome.status.unmined_ok);
        assert!(outcome.status.fully_ok());
    }

    #[test]
    fn rebuild_in_memory_secondaries_succeeds_on_empty_device() {
        // An empty device has no records, so rebuild returns Ok(empty).
        // Both flags must be true and indexes must be empty.
        let (dev, alloc) = fresh_dev_alloc();
        let outcome = rebuild_in_memory_secondaries(&*dev, &alloc);
        assert!(outcome.status.dah_ok);
        assert!(outcome.status.unmined_ok);
        assert_eq!(outcome.dah.len(), 0);
        assert_eq!(outcome.unmined.len(), 0);
    }

    #[test]
    fn fallback_dah_index_returns_empty_in_memory() {
        // Construct a synthetic IndexError and verify the fallback path
        // produces an empty in-memory backend (no panic).
        let err = IndexError::FormatError {
            detail: "synthetic test error".to_string(),
        };
        let dah = fallback_dah_index("DAH", err);
        assert_eq!(dah.len(), 0);
    }

    #[test]
    fn fallback_unmined_index_returns_empty_in_memory() {
        let err = IndexError::FormatError {
            detail: "synthetic test error".to_string(),
        };
        let unmined = fallback_unmined_index("unmined", err);
        assert_eq!(unmined.len(), 0);
    }

    #[test]
    fn replay_cause_labels_are_distinct() {
        assert_eq!(
            replay_cause_label(ReplayCause::MissingPrimary),
            "missing-primary"
        );
        assert_eq!(replay_cause_label(ReplayCause::IoError), "io-error");
        assert_eq!(
            replay_cause_label(ReplayCause::CorruptEntry),
            "corrupt-entry"
        );
        assert_eq!(replay_cause_label(ReplayCause::LogicError), "logic-error");
    }

    // -----------------------------------------------------------------------
    // Mandatory redo log open (gap #2)
    // -----------------------------------------------------------------------

    #[test]
    fn mandatory_redo_open_succeeds_in_clean_dir() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("redo.log");
        let result = super::open_mandatory_redo_log(&path, 1 << 20, 4096, None, false);
        let (_dev, log) = match result {
            Ok(parts) => parts,
            Err(e) => panic!("clean tmp path must open: {e}"),
        };
        // Smoke check: a freshly opened log starts at sequence 1.
        assert_eq!(
            log.current_sequence(),
            1,
            "freshly opened redo log must start at seq 1"
        );
    }

    /// Phase 7: with the ring enabled, a FRESH redo region adopts the segment
    /// ring; reopening the same region keeps it a ring (device format wins).
    #[test]
    fn mandatory_redo_open_fresh_adopts_ring() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("ring.redo");
        // segment_ring = Some(0) → auto-derive segment size.
        let (_dev, log) =
            super::open_mandatory_redo_log(&path, 1 << 20, 4096, Some(0), false).unwrap();
        assert!(log.is_segment_ring(), "fresh region adopts the ring");
        drop(log);

        // Reopen WITHOUT requesting the ring: the on-disk ring is still used.
        let (_dev2, log2) =
            super::open_mandatory_redo_log(&path, 1 << 20, 4096, None, false).unwrap();
        assert!(log2.is_segment_ring(), "on-disk ring format wins on reopen");
    }

    /// Phase 7: an explicit segment size is honored when adopting the ring.
    #[test]
    fn mandatory_redo_open_honors_explicit_segment_size() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("ring_explicit.redo");
        let (_dev, log) =
            super::open_mandatory_redo_log(&path, 1 << 20, 4096, Some(64 * 1024), false).unwrap();
        assert!(log.is_segment_ring());
        // 1 MiB region − 4 KiB header = 1044480 B; / 64 KiB = 15 segments.
        assert_eq!(log.capacity(), 15 * 64 * 1024);
    }

    /// Phase 7: requesting the ring on a region that already holds linear data
    /// stays linear (does not discard live redo).
    #[test]
    fn mandatory_redo_open_keeps_existing_linear() {
        use crate::redo::RedoOp;
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("linear.redo");
        {
            let (_dev, mut log) =
                super::open_mandatory_redo_log(&path, 1 << 20, 4096, None, false).unwrap();
            log.append(RedoOp::Checkpoint).unwrap();
            log.flush().unwrap();
            assert!(!log.is_segment_ring());
        }
        // Now request the ring: existing linear data → stays linear this session.
        let (_dev, log) =
            super::open_mandatory_redo_log(&path, 1 << 20, 4096, Some(0), false).unwrap();
        assert!(
            !log.is_segment_ring(),
            "non-empty linear region must not be reformatted to a ring"
        );
    }

    #[test]
    fn mandatory_redo_open_fails_on_unwritable_path() {
        // Pointing at a parent directory that does not exist returns a
        // `DeviceError` from `DirectDevice::open` — the gap #2 contract
        // is that this propagates instead of falling back to memory.
        let path = std::path::Path::new("/this/path/does/not/exist/redo.log");
        let result = super::open_mandatory_redo_log(path, 1 << 20, 4096, None, false);
        let err = match result {
            Ok(_) => panic!("missing parent dir must fail closed (no in-memory fallback)"),
            Err(e) => e,
        };
        match err {
            super::RedoOpenError::Device { path: p, reason } => {
                assert!(p.contains("does/not/exist"), "path in error: {p}");
                assert!(
                    !reason.is_empty(),
                    "reason must carry underlying error: {reason}"
                );
            }
            super::RedoOpenError::Log { .. } => {
                panic!("missing parent dir should surface as Device error, not Log error");
            }
        }
    }

    #[test]
    fn mandatory_redo_open_fails_on_path_pointing_at_existing_directory() {
        // Pointing the redo log at a directory (not a file) is a config
        // error that `DirectDevice::open` rejects. Verify that the gap #2
        // contract — fail closed, no in-memory fallback — is honored.
        let tmp = TempDir::new().unwrap();
        // tmp.path() itself exists as a directory.
        let dir_path = tmp.path().to_path_buf();
        let result = super::open_mandatory_redo_log(&dir_path, 1 << 20, 4096, None, false);
        let err = match result {
            Ok(_) => panic!("a directory path must fail closed (no in-memory fallback)"),
            Err(e) => e,
        };
        match err {
            super::RedoOpenError::Device { path: p, reason } => {
                assert_eq!(p, dir_path.display().to_string(), "path in error: {p}");
                assert!(
                    !reason.is_empty(),
                    "reason must carry underlying error: {reason}"
                );
            }
            super::RedoOpenError::Log { .. } => {
                panic!("a directory path should surface as Device error, not Log error");
            }
        }
    }

    // -----------------------------------------------------------------------
    // Task 6 test: load_sharded_index_in_memory
    // -----------------------------------------------------------------------

    /// Test 3 (Task 6): `load_sharded_index_in_memory` scans the device and
    /// returns a populated N=16 `ShardedIndex`. All written records must be
    /// findable and their `record_offset` values must match.
    #[test]
    fn load_sharded_index_in_memory_returns_populated_n16_index() {
        use crate::index::TxKey;
        use crate::io::write_full_record;
        use crate::record::{TxMetadata, UtxoSlot};

        let dev = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(dev.clone()).unwrap();

        // Write 30 records with keys that spread across shards ([24..32] varied)
        let mut expected: Vec<(TxKey, u64)> = Vec::new();
        for i in 0u64..30 {
            let mut meta = TxMetadata::new(5);
            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&i.to_le_bytes());
            txid[8..16].copy_from_slice(&i.wrapping_mul(0x9E3779B97F4A7C15).to_le_bytes());
            txid[24..32].copy_from_slice(&i.wrapping_mul(0x517C_C1B7_2722_0A95).to_le_bytes());
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
            expected.push((TxKey { txid }, offset));
        }

        let sharded = super::load_sharded_index_in_memory(&*dev, &alloc, 16, 0)
            .expect("load_sharded_index_in_memory must succeed with populated device");

        assert_eq!(sharded.shard_count(), 16, "must produce 16 shards");
        assert_eq!(
            sharded.len(),
            expected.len(),
            "rebuilt index must contain all written records"
        );

        for (key, expected_offset) in &expected {
            let entry = sharded.lookup(key).unwrap_or_else(|| {
                panic!(
                    "key {:?} not found after load_sharded_index_in_memory",
                    key.txid
                )
            });
            assert_eq!(
                entry.record_offset, *expected_offset,
                "record_offset mismatch for key {:?}",
                key.txid
            );
        }
    }
}
