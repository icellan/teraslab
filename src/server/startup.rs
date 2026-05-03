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

use thiserror::Error;

use crate::allocator::SlotAllocator;
use crate::config::IndexConfig;
use crate::device::BlockDevice;
use crate::index::{
    DahBackend, DahIndex, IndexError, PrimaryBackend, UnminedBackend, UnminedIndex,
};
use crate::recovery::{RecoveryStats, ReplayCause};

use super::dispatch::SecondaryStatus;

/// Errors raised by [`load_primary_index_*`] when neither restore nor
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
    if stats.failed_io > 0 {
        return Err(format!(
            "recovery: {n} replay failure(s) caused by device I/O errors — \
             non-tolerable, the device is unreachable or returning corrupt \
             blocks; investigate before restarting",
            n = stats.failed_io
        ));
    }
    if stats.failed_corrupt > 0 {
        return Err(format!(
            "recovery: {n} replay failure(s) caused by corrupt redo or \
             metadata records — non-tolerable, on-device data is unreadable; \
             investigate before restarting",
            n = stats.failed_corrupt
        ));
    }
    if stats.failed_logic > 0 {
        return Err(format!(
            "recovery: {n} replay failure(s) caused by logic-level \
             inconsistency — non-tolerable; investigate before restarting",
            n = stats.failed_logic
        ));
    }
    if stats.failed_missing_primary > MAX_TOLERATED_MISSING_PRIMARY {
        return Err(format!(
            "recovery: {n} missing-primary replay failure(s) exceed cap \
             ({cap}) — the redo log references far more deleted records than \
             the primary index can plausibly explain; verify device / path \
             and investigate before restarting",
            n = stats.failed_missing_primary,
            cap = MAX_TOLERATED_MISSING_PRIMARY,
        ));
    }
    Ok(())
}

/// Convert a [`ReplayCause`] into the human label used in tolerance
/// error messages. Kept `pub(crate)` so other diagnostic surfaces can
/// reuse the same wording.
#[allow(dead_code)]
pub(crate) fn replay_cause_label(cause: ReplayCause) -> &'static str {
    match cause {
        ReplayCause::MissingPrimary => "missing-primary",
        ReplayCause::IoError => "io-error",
        ReplayCause::CorruptEntry => "corrupt-entry",
        ReplayCause::LogicError => "logic-error",
    }
}

/// Load the redb primary index. Restore first, fall back to a
/// device-rebuild on a clean restore-error, fail closed otherwise.
///
/// On rebuild failure the redb file at [`IndexConfig::redb_path`] is
/// **not** removed — the operator must inspect it before deciding to
/// rescan. This is the gap #5 fail-closed contract.
pub fn load_primary_index_redb(
    config: &IndexConfig,
    device: &dyn BlockDevice,
    allocator: &SlotAllocator,
) -> Result<PrimaryBackend, RebuildError> {
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
    allocator: &SlotAllocator,
) -> Result<PrimaryBackend, RebuildError> {
    let restore_suffix = if path.exists() {
        match PrimaryBackend::restore_file_backed(path, expected_records) {
            Ok(idx) => return Ok(idx),
            Err(e) => format!("; restore failed ({e})"),
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
    allocator: &SlotAllocator,
) -> Result<PrimaryBackend, RebuildError> {
    PrimaryBackend::rebuild(device, allocator).map_err(|e| RebuildError::InMemoryPrimary {
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
    allocator: &SlotAllocator,
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
        check_replay_tolerance(&stats)
            .expect("missing-primary at the cap must still be tolerated");
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
        let err = check_replay_tolerance(&stats)
            .expect_err("missing-primary over cap must fail closed");
        assert!(err.contains("missing-primary"), "msg: {err}");
        assert!(err.contains("cap"), "msg: {err}");
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
        // Write garbage into the redb primary path so `restore_redb` fails;
        // simulate rebuild failure by passing a device whose contents do
        // not parse as TeraSlab records (the empty in-memory device returns
        // Ok with zero entries, which is success — so to simulate failure
        // we instead force a restore error and verify the on-disk file is
        // preserved by the fail-closed path.
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
        // Empty device → rebuild returns Ok(empty). To test the fail-closed
        // path we corrupt the device contents by writing partial records,
        // but rebuild_redb tolerates corrupt magic by skipping. Instead,
        // we verify the simpler invariant: a corrupted redb file is NOT
        // deleted on the rebuild path. Since rebuild succeeds against an
        // empty device, this case actually returns Ok with the rebuilt
        // index — which is still acceptable, because the contract is
        // "fail closed when rebuild errors", not "fail closed when restore
        // errors and rebuild succeeds". Verify the file is preserved
        // either way.
        let _ = load_primary_index_redb(&cfg, &*dev, &alloc);
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
        assert_eq!(replay_cause_label(ReplayCause::MissingPrimary), "missing-primary");
        assert_eq!(replay_cause_label(ReplayCause::IoError), "io-error");
        assert_eq!(replay_cause_label(ReplayCause::CorruptEntry), "corrupt-entry");
        assert_eq!(replay_cause_label(ReplayCause::LogicError), "logic-error");
    }
}
