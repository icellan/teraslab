//! Integration coverage for the online-backup coordinator that backs the
//! `/admin/backup*` HTTP endpoints.
//!
//! # Sandbox note
//!
//! The endpoints themselves are driven end-to-end (through the axum router +
//! bearer gate) by in-crate unit tests in `src/server/http.rs`, using
//! `tower::ServiceExt::oneshot` — this test *process cannot bind a loopback
//! socket* (the CI/dev sandbox returns `os error 49, AddrNotAvailable`), and
//! the router / handlers are `pub(crate)`, unreachable from an external test
//! crate. So here we exercise the **public** [`BackupManager`] API that those
//! handlers delegate to: the same start/status/abort behaviour and status-JSON
//! shape, minus the HTTP framing. Every assertion checks real state, not just
//! `is_ok()`/`is_err()`.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use teraslab::allocator::SlotAllocator;
use teraslab::backup::{BackupError, BackupManager, BackupParams, BackupState};
use teraslab::config::ServerConfig;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index};
use teraslab::locks::StripedLocks;
use teraslab::ops::engine::Engine;

/// Build a manager over a fresh in-memory engine, rooted at `backup_root`
/// (`None` disables `start`).
fn build_manager(backup_root: Option<std::path::PathBuf>) -> Arc<BackupManager> {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
    let alloc = SlotAllocator::new(dev.clone()).unwrap();
    let index = Index::new(1024).unwrap();
    let engine = Arc::new(Engine::new(
        dev,
        index,
        alloc,
        StripedLocks::new(64),
        DahIndex::new(),
    ));
    BackupManager::new(
        engine,
        Arc::new(AtomicBool::new(false)),
        BackupParams::default(),
        ServerConfig::default(),
        backup_root,
    )
}

/// Poll `status()` until it reaches a terminal state (`Done`/`Failed`) or the
/// deadline elapses. Returns the last observed progress.
fn wait_for_terminal(
    manager: &BackupManager,
    timeout: Duration,
) -> teraslab::backup::BackupProgress {
    let deadline = Instant::now() + timeout;
    loop {
        let p = manager.status();
        if matches!(p.state, BackupState::Done | BackupState::Failed) {
            return p;
        }
        if Instant::now() >= deadline {
            return p;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// With no backup root (backups disabled), `start` rejects with
/// `UnsupportedConfig` — the exact case the HTTP handler maps to 400.
#[test]
fn start_rejects_when_backups_disabled() {
    let manager = build_manager(None);
    let err = manager
        .start(None)
        .expect_err("start must fail with no configured root");
    assert!(
        matches!(err, BackupError::UnsupportedConfig(_)),
        "expected UnsupportedConfig, got {err:?}"
    );
}

/// A fresh manager serializes an `Idle` progress with zeroed counters — the
/// exact payload `GET /admin/backup/status` returns as JSON.
#[test]
fn status_serializes_with_idle_state() {
    let manager = build_manager(None);
    let progress = manager.status();
    assert_eq!(progress.state, BackupState::Idle);

    let v = serde_json::to_value(&progress).expect("BackupProgress serializes");
    assert_eq!(v["state"], "Idle");
    assert_eq!(v["bytes_copied"], 0);
    assert_eq!(v["segments_copied"], 0);
    assert!(v["error"].is_null());
    assert!(v["manifest_path"].is_null());
}

/// `abort` is idempotent and infallible whether or not a backup is running.
#[test]
fn abort_is_idempotent_ok() {
    let manager = build_manager(None);
    manager.abort().expect("first abort ok");
    manager.abort().expect("second abort ok");
}

/// A relative subdir escaping the root via `..` is rejected before any thread
/// is spawned — `UnsupportedConfig` (→ 400 at the HTTP layer).
#[test]
fn start_rejects_parent_dir_traversal() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = build_manager(Some(tmp.path().to_path_buf()));
    let err = manager
        .start(Some("../escape".to_string()))
        .expect_err("`..` subdir must be rejected");
    assert!(
        matches!(err, BackupError::UnsupportedConfig(_)),
        "expected UnsupportedConfig, got {err:?}"
    );
}

/// An absolute subdir is rejected (it would escape the configured root).
#[test]
fn start_rejects_absolute_subdir() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = build_manager(Some(tmp.path().to_path_buf()));
    let err = manager
        .start(Some("/etc/teraslab".to_string()))
        .expect_err("absolute subdir must be rejected");
    assert!(
        matches!(err, BackupError::UnsupportedConfig(_)),
        "expected UnsupportedConfig, got {err:?}"
    );
}

/// With a valid root, `start` returns immediately (`Ok`, the 202 path) and the
/// background job runs to a terminal state. On this in-memory (non-segment)
/// engine the pre-flight fails, so the job lands in `Failed` with an error
/// string surfaced through `status()`.
#[test]
fn start_runs_job_to_terminal_failed_on_memory_engine() {
    let tmp = tempfile::TempDir::new().unwrap();
    let manager = build_manager(Some(tmp.path().to_path_buf()));

    manager
        .start(Some("snapshot-1".to_string()))
        .expect("start spawns the job and returns Ok");

    let progress = wait_for_terminal(&manager, Duration::from_secs(5));
    assert_eq!(
        progress.state,
        BackupState::Failed,
        "memory engine has no segment store; the job must fail pre-flight"
    );
    let err = progress
        .error
        .expect("Failed state must carry an error string");
    assert!(
        !err.is_empty(),
        "the failure error string must be populated"
    );
    assert!(
        progress.manifest_path.is_none(),
        "a failed backup must not report a manifest path"
    );

    // A terminal run clears the single-flight lease, so a subsequent start is
    // admitted again (rather than wedging on a stale AlreadyRunning).
    manager
        .start(Some("snapshot-2".to_string()))
        .expect("terminal run clears the lease; a new start is admitted");
    manager.abort().expect("abort infallible");
    let _ = wait_for_terminal(&manager, Duration::from_secs(5));
    drop(tmp);
}
