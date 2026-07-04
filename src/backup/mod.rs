//! Online backup and offline restore.
//!
//! A backup is a crash-legal device image at instant `T` (the finalize tail)
//! plus the complete redo tail `(F, T]`, so restore is the existing
//! crash-recovery path — no new recovery code. This module owns the pieces:
//!
//! * [`manifest`] — the versioned, checksummed index written last (its presence
//!   marks a backup complete).
//! * [`copier`] — the throttled, torn-read-safe device range copier.
//! * [`job`] — [`job::run_backup`], the online state machine that pins the
//!   segment lifecycle, fences the redo, snapshots the index, copies the used
//!   segments, catches up, and writes the manifest.
//! * [`restore`] — [`restore::restore`], the offline placer that validates the
//!   manifest and lays every artifact back down for a normal boot to recover.
//!
//! [`BackupManager`] is the process-lifetime handle: a single-flight lease over
//! a background [`job::run_backup`] thread, exposing [`BackupManager::start`],
//! [`BackupManager::status`], and [`BackupManager::abort`]. The deep correctness
//! logic lives in [`job::run_backup`]; the manager is a thin coordinator.

pub mod copier;
pub mod job;
pub mod manifest;
pub mod restore;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;

use parking_lot::Mutex;

pub use manifest::{MANIFEST_FILE, MANIFEST_VERSION, Manifest};

use crate::config::ServerConfig;
use crate::ops::engine::Engine;

/// Errors from the online-backup / restore subsystem.
#[derive(Debug, thiserror::Error)]
pub enum BackupError {
    /// A device read or write failed.
    #[error("backup device I/O error: {0}")]
    Device(#[from] crate::device::DeviceError),
    /// A filesystem read or write (backup directory, redo file, snapshot) failed.
    #[error("backup filesystem I/O error: {0}")]
    Io(std::io::Error),
    /// The manifest could not be read, written, or verified.
    #[error("backup manifest error: {0}")]
    Manifest(#[from] crate::backup::manifest::ManifestError),
    /// The engine's index snapshot could not be written or read.
    #[error("backup index snapshot error: {0}")]
    Index(String),
    /// The engine or config cannot support a v1 backup (non-segment store,
    /// non-memory index, held instance lock, …).
    #[error("configuration unsupported for backup: {0}")]
    UnsupportedConfig(String),
    /// A store has fewer virgin segments than the pre-flight floor requires.
    #[error(
        "store {store} has insufficient virgin headroom for a backup: \
         {have} segment(s) free, need at least {need}"
    )]
    InsufficientHeadroom {
        /// The store that failed the check.
        store: u8,
        /// Virgin segments currently available.
        have: u32,
        /// Virgin segments required.
        need: u32,
    },
    /// The append frontier kept outrunning the copier past the round budget.
    #[error("backup catch-up did not converge after {rounds} round(s)")]
    CatchupExceeded {
        /// The number of catch-up rounds attempted before giving up.
        rounds: u32,
    },
    /// The bounded redo-tail buffer overflowed; the backup is aborted so a
    /// client write is never blocked behind it.
    #[error("backup redo-tail buffer overflowed; aborting")]
    TeeOverflow,
    /// The caller set the cancel flag.
    #[error("backup aborted by caller")]
    Aborted,
    /// Restore refused because the manifest geometry does not match the target
    /// node's configuration.
    #[error("restore geometry mismatch: {0}")]
    GeometryMismatch(String),
    /// A backup is already in flight under this manager's single-flight lease.
    #[error("a backup is already running")]
    AlreadyRunning,
}

/// Tunables for a backup run. See [`Default`] for production defaults.
#[derive(Debug, Clone)]
pub struct BackupParams {
    /// Copier throttle in bytes/sec (0 = unthrottled).
    pub throttle_bytes_per_sec: u64,
    /// Pre-flight: refuse a store below this many virgin segments.
    pub min_headroom_segments: u32,
    /// Mid-run: abort a store that falls below this many virgin segments.
    pub abort_headroom_segments: u32,
    /// Catch-up: a store within this many segments of the copier is "caught up".
    pub stall_copy_max_segments: u32,
    /// Catch-up: maximum rounds before declaring the frontier diverged.
    pub max_catchup_rounds: u32,
    /// The bounded redo-tail buffer size per store (bytes); overflow aborts.
    pub tee_buffer_max_bytes: usize,
}

impl Default for BackupParams {
    fn default() -> Self {
        Self {
            throttle_bytes_per_sec: 268_435_456, // 256 MiB/s
            min_headroom_segments: 64,
            abort_headroom_segments: 16,
            stall_copy_max_segments: 4,
            max_catchup_rounds: 10,
            tee_buffer_max_bytes: 268_435_456, // 256 MiB
        }
    }
}

/// The online-backup state machine phases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize)]
pub enum BackupState {
    /// No backup has started (fresh progress).
    #[default]
    Idle,
    /// Pre-flight passed; the segment lifecycle is being pinned.
    Pinning,
    /// Sampling the fence `F` and attaching the redo tees.
    Fencing,
    /// Writing the backup-owned index snapshot.
    Snapshotting,
    /// Copying sealed + growing segments.
    Copying,
    /// Bounded catch-up as the frontier advances.
    CatchUp,
    /// Final stall: sampling `T`, capturing headers, assembling artifacts.
    Finalizing,
    /// The manifest is durable; the backup is complete.
    Done,
    /// The backup failed; see [`BackupProgress::error`].
    Failed,
}

/// A snapshot of a backup's progress, cloneable for status reporting.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct BackupProgress {
    /// Current phase.
    pub state: BackupState,
    /// The fence `F`, once sampled.
    pub fence: Option<u64>,
    /// The tail end `T`, once sampled at finalize.
    pub tail_end: Option<u64>,
    /// Total bytes copied so far.
    pub bytes_copied: u64,
    /// Segments copied so far.
    pub segments_copied: u32,
    /// Estimated total segments to copy (segments + one header per store).
    pub segments_total: u32,
    /// The current catch-up round.
    pub catchup_round: u32,
    /// The failure string when `state == Failed`.
    pub error: Option<String>,
    /// The manifest path once the backup completes.
    pub manifest_path: Option<String>,
}

/// Mutable manager state behind a mutex.
struct ManagerInner {
    running: bool,
    progress: Arc<Mutex<BackupProgress>>,
    cancel: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

/// Process-lifetime backup coordinator: a single-flight lease over one
/// background [`job::run_backup`] thread.
pub struct BackupManager {
    engine: Arc<Engine>,
    blob_pause: Arc<AtomicBool>,
    params: BackupParams,
    /// The node config the job needs (device geometry, redo/snapshot paths,
    /// blob-store root). Stored here because [`Self::start`] takes `&self` and
    /// cannot otherwise thread it into the spawned job.
    config: ServerConfig,
    backup_root: Option<PathBuf>,
    inner: Mutex<ManagerInner>,
}

impl BackupManager {
    /// Build a manager over `engine`. `blob_pause` is the shared flag the blob
    /// GC observes (held true for a backup's duration). `backup_root` confines
    /// target directories; `None` disables `start` (rejects with
    /// [`BackupError::UnsupportedConfig`]).
    pub fn new(
        engine: Arc<Engine>,
        blob_pause: Arc<AtomicBool>,
        params: BackupParams,
        config: ServerConfig,
        backup_root: Option<PathBuf>,
    ) -> Arc<Self> {
        Arc::new(Self {
            engine,
            blob_pause,
            params,
            config,
            backup_root,
            inner: Mutex::new(ManagerInner {
                running: false,
                progress: Arc::new(Mutex::new(BackupProgress::default())),
                cancel: Arc::new(AtomicBool::new(false)),
                handle: None,
            }),
        })
    }

    /// Start a backup into `<backup_root>/<subdir>` (subdir defaults to
    /// `"backup"`). Returns immediately (the work runs on a background thread);
    /// poll [`Self::status`] for progress.
    ///
    /// # Errors
    /// * [`BackupError::AlreadyRunning`] if a backup is in flight.
    /// * [`BackupError::UnsupportedConfig`] if no `backup_root` is configured or
    ///   `subdir` is absolute or escapes the root via `..`.
    /// * [`BackupError::Io`] if the worker thread cannot be spawned.
    pub fn start(&self, subdir: Option<String>) -> Result<(), BackupError> {
        let mut inner = self.inner.lock();

        // A prior run that reached a terminal state clears the lease.
        {
            let p = inner.progress.lock();
            if matches!(p.state, BackupState::Done | BackupState::Failed) {
                drop(p);
                inner.running = false;
            }
        }
        if inner.running {
            return Err(BackupError::AlreadyRunning);
        }

        let root = self.backup_root.as_ref().ok_or_else(|| {
            BackupError::UnsupportedConfig("no backup_root configured; backups are disabled".into())
        })?;
        let name = subdir.unwrap_or_else(|| "backup".to_string());
        let sub = PathBuf::from(&name);
        if sub.is_absolute()
            || sub
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Err(BackupError::UnsupportedConfig(format!(
                "backup subdir {name:?} must be a relative path without `..`"
            )));
        }
        let target = root.join(sub);

        let progress = Arc::new(Mutex::new(BackupProgress::default()));
        let cancel = Arc::new(AtomicBool::new(false));

        let engine = self.engine.clone();
        let blob_pause = self.blob_pause.clone();
        let params = self.params.clone();
        let config = self.config.clone();
        let thread_progress = progress.clone();
        let thread_cancel = cancel.clone();

        let handle = std::thread::Builder::new()
            .name("teraslab-backup".to_string())
            .spawn(move || {
                let _ = job::run_backup(
                    &engine,
                    &blob_pause,
                    &config,
                    &params,
                    &target,
                    &thread_cancel,
                    &thread_progress,
                );
            })
            .map_err(BackupError::Io)?;

        inner.progress = progress;
        inner.cancel = cancel;
        inner.handle = Some(handle);
        inner.running = true;
        Ok(())
    }

    /// A clone of the current progress.
    pub fn status(&self) -> BackupProgress {
        let inner = self.inner.lock();
        inner.progress.lock().clone()
    }

    /// Request cancellation of the in-flight backup. Idempotent; setting the
    /// flag with no backup running is harmless.
    pub fn abort(&self) -> Result<(), BackupError> {
        let inner = self.inner.lock();
        inner.cancel.store(true, Ordering::Release);
        Ok(())
    }
}
