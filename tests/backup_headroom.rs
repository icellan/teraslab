//! I8 — headroom exhaustion aborts the BACKUP, never a client write.
//!
//! The online-backup design guarantee is asymmetric: a backup running while
//! client writes consume virgin segments must ABORT before an allocation would
//! fail. "Backups fail, client writes never do." These tests prove that
//! guarantee IN-PROCESS over a real `DirectDevice` (no sockets — the sandbox
//! denies loopback binding), driving the `Engine` and `run_backup` directly.
//!
//! ## Approach (per-test)
//!
//! The load-bearing property is *client allocations always succeed*. The
//! concurrent version of that property is timing-sensitive (the backup can
//! finish before the writer drops headroom), so the strict, race-free proofs
//! are two DETERMINISTIC tests:
//!
//!   1. [`backup_aborts_mid_run_on_headroom_exhaustion`] — consume virgin
//!      segments up front so pre-flight passes but the first live per-segment
//!      headroom sample is below the abort floor; assert `run_backup` returns
//!      `InsufficientHeadroom`, every consuming allocation returned `Ok`, and
//!      more allocations succeed AFTER the aborted backup (the pin released).
//!   2. [`client_allocations_succeed_while_backup_pin_held`] — hold the RAII
//!      `BackupPinGuard` (reuse frozen, allocation advances the high-water mark
//!      only) and assert whole-segment allocations keep succeeding while pinned.
//!
//! A third, genuinely CONCURRENT test
//! ([`concurrent_writer_never_fails_while_backup_runs`]) runs the writer and
//! the backup on separate threads with a *lenient* backup-outcome assertion
//! (Done, or Failed-with-`InsufficientHeadroom`) but a *strict* "every
//! allocation Ok" assertion — non-flaky, and it exercises the true race.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;
use tempfile::TempDir;

use teraslab::backup::job::{BackupPinGuard, run_backup};
use teraslab::backup::{BackupError, BackupParams, BackupProgress, BackupState};
use teraslab::config::ServerConfig;
use teraslab::device::{BlockDevice, DirectDevice};
use teraslab::index::{DahIndex, Index};
use teraslab::locks::StripedLocks;
use teraslab::ops::engine::Engine;
use teraslab::segment_allocator::SegmentAllocator;

const ALIGN: usize = 4096;
const SEG: u64 = 1024 * 1024; // 1 MiB segments
// 1 MiB reserved header + 11 MiB data → 11 segments → 10 virgin at boot.
const DEVICE_SIZE: u64 = 12 * 1024 * 1024;

#[allow(clippy::field_reassign_with_default)]
fn test_config(dir: &std::path::Path) -> ServerConfig {
    let mut cfg = ServerConfig::default();
    cfg.device_paths = vec![dir.join("data.dat")];
    cfg.device_size = DEVICE_SIZE;
    cfg.device_alignment = ALIGN;
    cfg.device_split = 1;
    cfg.blobstore_path = dir.join("no-blobstore");
    cfg
}

/// A segment engine over a file-backed `DirectDevice` of [`DEVICE_SIZE`] bytes.
fn seg_engine(dev: &Arc<dyn BlockDevice>) -> Engine {
    let seg = SegmentAllocator::new(dev.clone(), SEG).unwrap();
    Engine::new(
        dev.clone(),
        Index::new(256).unwrap(),
        seg,
        StripedLocks::new(64),
        DahIndex::new(),
    )
}

/// Current virgin headroom of store 0.
fn headroom(engine: &Engine) -> u32 {
    engine
        .backup_view_for(0)
        .expect("segment store")
        .virgin_headroom_segments()
}

/// Allocate one ~whole segment worth of bytes on store 0, asserting the
/// allocation succeeds (a client write must NEVER fail because of a backup).
fn allocate_one_segment(engine: &Engine) -> u64 {
    // `SEG - ALIGN` fills almost the whole open segment, so the next call
    // advances the high-water mark by one segment — a deterministic way to
    // burn a virgin segment per call.
    engine
        .allocator_for(0)
        .lock()
        .allocate(SEG - ALIGN as u64)
        .expect("client allocation must never fail")
}

#[test]
fn backup_aborts_mid_run_on_headroom_exhaustion() {
    let dir = TempDir::new().unwrap();
    let config = test_config(dir.path());
    let dev: Arc<dyn BlockDevice> =
        Arc::new(DirectDevice::open(&config.device_paths[0], DEVICE_SIZE, ALIGN).unwrap());
    let engine = seg_engine(&dev);

    let boot_headroom = headroom(&engine);
    assert!(
        boot_headroom >= 6,
        "device should start with generous headroom, got {boot_headroom}"
    );

    // Consume segments down to a known low headroom `H`, asserting every client
    // allocation succeeds along the way.
    let target = 3u32;
    let mut allocations = 0u32;
    while headroom(&engine) > target {
        allocate_one_segment(&engine);
        allocations += 1;
        assert!(allocations < 1000, "consumption loop must terminate");
    }
    let h = headroom(&engine);
    assert_eq!(
        h, target,
        "should have consumed down to the target headroom"
    );
    assert!(
        allocations > 0,
        "at least one segment must have been consumed"
    );

    // Pre-flight passes (H >= min), but the mid-run abort floor is set ABOVE
    // the current headroom so the FIRST per-segment live sample trips it. This
    // deterministically exercises the mid-run monitor rather than pre-flight.
    let params = BackupParams {
        throttle_bytes_per_sec: 0,
        min_headroom_segments: 1,       // 3 >= 1 → pre-flight passes
        abort_headroom_segments: h + 5, // 3 < 8 → mid-run aborts on first segment
        ..BackupParams::default()
    };
    let cancel = AtomicBool::new(false);
    let progress = Mutex::new(BackupProgress::default());
    let blob_pause = AtomicBool::new(false);

    match run_backup(
        &engine,
        &blob_pause,
        &config,
        &params,
        &dir.path().join("bk"),
        &cancel,
        &progress,
    ) {
        Err(BackupError::InsufficientHeadroom { store, have, need }) => {
            assert_eq!(store, 0);
            assert_eq!(need, h + 5);
            assert!(have <= h, "mid-run sample {have} must be <= {h}");
        }
        other => panic!("expected InsufficientHeadroom, got {other:?}"),
    }
    assert_eq!(progress.lock().state, BackupState::Failed);
    // The pin was released on the aborted backup.
    assert!(!engine.is_segment_lifecycle_pinned());
    // MANIFEST must not exist — an aborted backup leaves an incomplete dir.
    assert!(!dir.path().join("bk").join("MANIFEST.json").exists());

    // The client keeps allocating successfully AFTER the backup aborted: the
    // failure did not wedge the store, and the released pin restores normal
    // headroom accounting.
    for _ in 0..2 {
        if headroom(&engine) == 0 {
            break;
        }
        allocate_one_segment(&engine);
    }
}

#[test]
fn client_allocations_succeed_while_backup_pin_held() {
    let dir = TempDir::new().unwrap();
    let config = test_config(dir.path());
    let dev: Arc<dyn BlockDevice> =
        Arc::new(DirectDevice::open(&config.device_paths[0], DEVICE_SIZE, ALIGN).unwrap());
    let engine = seg_engine(&dev);
    let blob_pause = AtomicBool::new(false);

    let before = headroom(&engine);
    assert!(
        before >= 4,
        "need headroom to allocate under pin, got {before}"
    );

    // Hold the same RAII pin a running backup holds: reuse/defrag frozen,
    // allocation advances the high-water mark only — it must still succeed.
    let guard = BackupPinGuard::acquire(&engine, &blob_pause);
    assert!(engine.is_segment_lifecycle_pinned());

    let mut consumed = 0u32;
    while headroom(&engine) > 1 && consumed < before {
        // Each allocation succeeds even though the lifecycle is pinned.
        allocate_one_segment(&engine);
        consumed += 1;
    }
    assert!(
        consumed > 0,
        "at least one allocation must have succeeded under the pin"
    );
    assert!(
        headroom(&engine) < before,
        "allocations under the pin must consume virgin headroom (advance the \
         high-water mark), before={before} after={}",
        headroom(&engine)
    );

    drop(guard);
    assert!(!engine.is_segment_lifecycle_pinned());
}

#[test]
fn concurrent_writer_never_fails_while_backup_runs() {
    let dir = TempDir::new().unwrap();
    let config = test_config(dir.path());
    let dev: Arc<dyn BlockDevice> =
        Arc::new(DirectDevice::open(&config.device_paths[0], DEVICE_SIZE, ALIGN).unwrap());
    let engine = Arc::new(seg_engine(&dev));

    let boot_headroom = headroom(&engine);
    assert!(boot_headroom >= 8, "need room for a concurrent writer");

    // Pre-flight passes (boot headroom >> min); the writer then consumes
    // segments, dropping live headroom below the abort floor mid-run.
    let params = BackupParams {
        // Throttle the copier so the copy phase overlaps the writer instead of
        // finishing instantly — biases toward exercising the mid-run abort.
        throttle_bytes_per_sec: 4 * 1024 * 1024,
        min_headroom_segments: 4,
        abort_headroom_segments: 3,
        ..BackupParams::default()
    };

    let progress = Arc::new(Mutex::new(BackupProgress::default()));
    let cancel = Arc::new(AtomicBool::new(false));
    let blob_pause = Arc::new(AtomicBool::new(false));
    let go = Arc::new(AtomicBool::new(false));

    // Backup thread.
    let backup_engine = engine.clone();
    let backup_progress = progress.clone();
    let backup_cancel = cancel.clone();
    let backup_blob = blob_pause.clone();
    let backup_config = config.clone();
    let backup_go = go.clone();
    let target = dir.path().join("bk");
    let backup = std::thread::spawn(move || {
        while !backup_go.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        run_backup(
            &backup_engine,
            &backup_blob,
            &backup_config,
            &params,
            &target,
            &backup_cancel,
            &backup_progress,
        )
    });

    // Writer thread: consume a bounded number of whole segments, staying well
    // clear of device-full so any failure would be the backup's fault, not the
    // device's. EVERY allocation must succeed.
    let writer_engine = engine.clone();
    let writer_go = go.clone();
    let writer = std::thread::spawn(move || {
        while !writer_go.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        // Consume up to `boot_headroom - 2` segments — never exhausts the
        // device (so allocate can only fail if a backup blocked it, which it
        // must not).
        let mut ok = 0u32;
        for _ in 0..(boot_headroom - 2) {
            let r = writer_engine
                .allocator_for(0)
                .lock()
                .allocate(SEG - ALIGN as u64);
            assert!(
                r.is_ok(),
                "a client allocation failed while a backup was running: {r:?}"
            );
            ok += 1;
            // Give the copier a chance to interleave.
            std::thread::yield_now();
        }
        ok
    });

    go.store(true, Ordering::Release);

    let allocs_ok = writer.join().expect("writer thread panicked");
    let outcome = backup.join().expect("backup thread panicked");

    assert!(
        allocs_ok > 0,
        "the writer should have performed some allocations"
    );

    // Lenient backup outcome (timing decides which): either it finished, or it
    // aborted specifically on headroom — but it must NEVER have failed for any
    // other reason, and it must NEVER have caused a client write to fail (the
    // strict assertion is inside the writer thread above).
    match outcome {
        Ok(_manifest) => {}
        Err(BackupError::InsufficientHeadroom { store, .. }) => assert_eq!(store, 0),
        other => panic!("backup failed for a non-headroom reason: {other:?}"),
    }
    // Pin released regardless of outcome.
    assert!(!engine.is_segment_lifecycle_pinned());
}
