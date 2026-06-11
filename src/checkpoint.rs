//! Background redo-log checkpoint task.
//!
//! The redo log is a fixed-size linear-with-reset write-ahead log (see
//! [`crate::redo`] — there is no in-place wrap; `write_pos` advances
//! monotonically until a checkpoint resets it back to zero).
//! Without a reclamation mechanism it would fill (~750k mutations at a
//! 64 MiB default + ~85 B/entry) and the master would brick: `append` would
//! return [`crate::redo::RedoError::LogFull`] and every mutation would error
//! out.
//!
//! This module wires the missing reclamation cadence. A background thread
//! periodically samples the log's usage fraction; when it crosses the
//! configured `high_water` threshold (default 0.75), the thread takes a
//! checkpoint:
//!
//! 1. Quiesce dispatch long enough to establish a stable redo fence.
//! 2. Snapshot the in-memory primary, DAH, and unmined indexes to disk
//!    via [`crate::ops::engine::Engine::snapshot_index`] (atomic via
//!    tempfile + rename).
//! 3. Persist the allocator's freelist + high-water mark via
//!    [`crate::ops::engine::Engine::persist_allocator`].
//! 4. Append a [`crate::redo::RedoOp::RecoveryProgress`] fence through the
//!    snapshotted sequence.
//! 5. Compact redo entries through that fence when replica ACK watermarks
//!    allow reclamation; post-fence entries are preserved.
//!
//! Crash safety: each step's effects are durable independently of the
//! others. Snapshot is fsynced before the rename; allocator persist is
//! fsynced before returning; recovery-progress marker is fsynced; prefix
//! compaction fsyncs the rewritten log. After a crash at any point recovery
//! either replays all un-fenced entries on top of the most recent snapshot
//! (safe — recovery is idempotent) or, if compaction already ran, sees only
//! entries newer than the snapshot fence.
//!
//! Concurrency: the redo mutex is held only while sampling the fence and
//! while appending/compacting the marker. Snapshot file I/O no longer holds
//! the redo mutex, so replica catch-up and other redo readers are not pinned
//! behind filesystem work. Dispatch is still quiesced while the in-memory
//! snapshot is collected so the fence cannot race ahead of unapplied
//! mutations.

use parking_lot::Mutex;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::ops::engine::Engine;
use crate::redo::RedoLog;

type ResetGuard = Arc<dyn Fn(u64) -> bool + Send + Sync + 'static>;

/// Configuration for the background checkpoint task.
///
/// BC-01: the task uses hysteresis to avoid back-to-back checkpoints
/// during a sustained mutation burst. Once usage crosses `high_water`
/// and a checkpoint runs, the task will not arm a second checkpoint
/// until usage falls below `low_water`. With single-flight enforcement
/// in the loop body, this guarantees at most one in-flight checkpoint
/// per engine.
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// Usage fraction (0.0..1.0) at or above which the next tick triggers
    /// a checkpoint. Default: 0.75.
    pub high_water: f64,
    /// Usage fraction (0.0..1.0) at or below which the trigger re-arms
    /// after a previous checkpoint. Default: 0.25.
    pub low_water: f64,
    /// How often the task wakes to sample usage. Default: 1 second.
    pub poll_interval: Duration,
    /// Initial back-off after a failed checkpoint. Doubles each
    /// successive failure up to `max_backoff`. Reset to 0 on a
    /// successful checkpoint. Default: 1 second.
    pub initial_backoff: Duration,
    /// Cap on the exponential back-off interval. Default: 60 seconds.
    pub max_backoff: Duration,
    /// Where to write the index/dah/unmined snapshot. Must be on the same
    /// filesystem so the tempfile + rename is atomic.
    pub snapshot_path: PathBuf,
}

impl CheckpointConfig {
    /// Construct a checkpoint config with sensible production defaults
    /// (75 % high water / 25 % low water, 1 s poll, 1 s → 60 s
    /// exponential back-off on failure).
    pub fn new(snapshot_path: PathBuf) -> Self {
        Self {
            high_water: 0.75,
            low_water: 0.25,
            poll_interval: Duration::from_secs(1),
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            snapshot_path,
        }
    }

    /// Backwards-compat shim: the previous API exposed a single
    /// `trigger_usage` threshold. Treat that as the high water mark
    /// and derive a low water mark roughly one-third below it.
    pub fn with_trigger_usage(mut self, trigger_usage: f64) -> Self {
        self.high_water = trigger_usage;
        self.low_water = (trigger_usage / 3.0).clamp(0.0, trigger_usage);
        self
    }
}

/// Spawn the background checkpoint task. Returns a join handle and a
/// shutdown flag the caller can flip to ask the task to exit cleanly.
///
/// The task runs until `shutdown` is set to `true` and `poll_interval`
/// has elapsed; each wake checks usage and may perform a checkpoint.
pub fn spawn_checkpoint_task(
    config: CheckpointConfig,
    engine: Arc<Engine>,
    redo_log: Arc<Mutex<RedoLog>>,
    shutdown: Arc<AtomicBool>,
) -> JoinHandle<()> {
    spawn_checkpoint_task_with_reset_guard(config, engine, redo_log, shutdown, Arc::new(|_| true))
}

/// Spawn a checkpoint task with an explicit guard for redo reset.
///
/// The guard receives the highest pre-checkpoint redo sequence that would be
/// erased by `reset()`. Returning `false` still writes the snapshot and
/// checkpoint marker, but leaves redo bytes intact so lagging replicas can
/// catch up from the old log.
pub fn spawn_checkpoint_task_with_reset_guard(
    config: CheckpointConfig,
    engine: Arc<Engine>,
    redo_log: Arc<Mutex<RedoLog>>,
    shutdown: Arc<AtomicBool>,
    reset_guard: ResetGuard,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("teraslab-checkpoint".to_string())
        .spawn(move || {
            run_checkpoint_loop(config, engine, redo_log, shutdown, reset_guard)
        })
        .expect("spawn checkpoint thread")
}

/// Body of the checkpoint thread, factored out so unit tests can drive
/// the loop directly without spawning.
///
/// The loop implements three production-critical behaviours on top of
/// `perform_checkpoint_with_reset_guard`:
///
/// 1. **Hysteresis (debounce).** A sustained mutation burst pushes
///    `usage_fraction` past `high_water` for many polls in a row.
///    Without hysteresis we would launch a checkpoint per poll.
///    Instead we set `armed = false` after triggering and only
///    re-arm once usage drops below `low_water`. Combined with the
///    synchronous `perform_checkpoint_*` call (no concurrent
///    checkpoint can start while one is running), this gives the
///    required single-flight semantics.
/// 2. **Exponential back-off on error.** A persistently failing
///    checkpoint (e.g. snapshot directory missing) would otherwise
///    flood logs and metrics every `poll_interval`. We double the
///    wait after each failure up to `max_backoff`, and reset to zero
///    on the next success.
/// 3. **Observable.** Each checkpoint emits a `tracing::info!` at
///    start with the usage fraction and at completion with the
///    elapsed wall-clock time, plus updates to
///    `redo_checkpoint_{triggered,failed}_total` and the
///    `redo_checkpoint_duration_ns` histogram.
fn run_checkpoint_loop(
    config: CheckpointConfig,
    engine: Arc<Engine>,
    redo_log: Arc<Mutex<RedoLog>>,
    shutdown: Arc<AtomicBool>,
    reset_guard: ResetGuard,
) {
    tracing::info!(
        high_water = config.high_water,
        low_water = config.low_water,
        poll_interval_ms = config.poll_interval.as_millis() as u64,
        "checkpoint task started",
    );

    let mut armed = true;
    let mut backoff = Duration::ZERO;

    while !shutdown.load(Ordering::Relaxed) {
        // Wait at least poll_interval, plus any pending back-off, but
        // check the shutdown flag in small slices so the task stops
        // within ~poll_interval on shutdown rather than within
        // `backoff` (which can be up to `max_backoff` = 60 s by
        // default).
        let wait = config.poll_interval + backoff;
        if !sleep_with_shutdown(wait, &shutdown, config.poll_interval) {
            break;
        }

        let usage = redo_log.lock().usage_fraction();

        // Hysteresis: re-arm when usage drops below low water.
        if !armed && usage <= config.low_water {
            tracing::debug!(
                usage_fraction = usage,
                low_water = config.low_water,
                "checkpoint trigger re-armed",
            );
            armed = true;
        }

        if !armed || usage < config.high_water {
            continue;
        }

        // Trip the trigger.
        armed = false;

        if let Some(m) = crate::metrics::redo_metrics() {
            m.redo_checkpoint_triggered_total.inc();
        }
        tracing::info!(
            usage_fraction = usage,
            high_water = config.high_water,
            "redo log above high-water — checkpointing",
        );

        let started = std::time::Instant::now();
        let outcome = perform_checkpoint_with_reset_guard(
            &config,
            &engine,
            &redo_log,
            |floor_sequence| reset_guard(floor_sequence),
        );
        let elapsed = started.elapsed();
        if let Some(m) = crate::metrics::redo_metrics() {
            m.redo_checkpoint_duration_ns.record_ns(elapsed.as_nanos() as u64);
        }

        match outcome {
            Ok(stats) => {
                backoff = Duration::ZERO;
                // Latch the re-arm on the checkpoint's own measured
                // `usage_after` instead of waiting for a later poll to
                // observe `usage <= low_water`: under sustained fast
                // mutation bursts, usage can drop below low water (right
                // after the compaction) and climb back above it between
                // two polls. The crossing is then never sampled, the
                // trigger never re-arms, and the log eventually bricks at
                // 100 % usage with every append returning `LogFull` —
                // defeating the task's whole purpose (BC-01; this was the
                // intermittent failure of
                // `sustained_mutations_never_brick_when_task_is_running`).
                // Debounce is preserved: re-arming requires that this
                // checkpoint actually reclaimed to low water, so the next
                // trip still implies a full low→high climb.
                if stats.reset_performed && stats.usage_after <= config.low_water {
                    armed = true;
                }
                tracing::info!(
                    elapsed_ms = elapsed.as_millis() as u64,
                    entries_before = stats.entries_before,
                    usage_after = stats.usage_after,
                    reset_performed = stats.reset_performed,
                    checkpoint_duration_ms = stats.checkpoint_duration_ms,
                    "checkpoint complete",
                );
            }
            Err(e) => {
                if let Some(m) = crate::metrics::redo_metrics() {
                    m.redo_checkpoint_failed_total.inc();
                }
                backoff = next_backoff(backoff, &config);
                tracing::error!(
                    err = %e,
                    next_backoff_ms = backoff.as_millis() as u64,
                    "checkpoint failed",
                );
                // After a failure, leave the trigger armed so the next
                // tick retries — usage has not actually been reclaimed
                // and waiting for low_water would deadlock the loop.
                armed = true;
            }
        }
    }
    tracing::info!("checkpoint task exiting");
}

/// Compute the next exponential back-off, doubling up to `max_backoff`.
fn next_backoff(current: Duration, config: &CheckpointConfig) -> Duration {
    let next = if current.is_zero() {
        config.initial_backoff
    } else {
        current.saturating_mul(2)
    };
    if next > config.max_backoff {
        config.max_backoff
    } else {
        next
    }
}

/// Sleep up to `total` in `slice`-sized chunks, returning early if
/// `shutdown` is set. Returns `false` if shutdown was observed
/// (caller should exit), `true` if the full duration elapsed.
fn sleep_with_shutdown(
    total: Duration,
    shutdown: &Arc<AtomicBool>,
    slice: Duration,
) -> bool {
    if total.is_zero() {
        return !shutdown.load(Ordering::Relaxed);
    }
    let slice = if slice.is_zero() {
        total
    } else {
        slice.min(total)
    };
    let mut remaining = total;
    while remaining > Duration::ZERO {
        if shutdown.load(Ordering::Relaxed) {
            return false;
        }
        let step = remaining.min(slice);
        std::thread::sleep(step);
        remaining = remaining.saturating_sub(step);
    }
    !shutdown.load(Ordering::Relaxed)
}

/// Result of a successful checkpoint, returned for logging.
#[derive(Debug)]
pub struct CheckpointStats {
    pub entries_before: u64,
    pub usage_after: f64,
    pub reset_performed: bool,
    /// F-G4-016: wall-clock time the dispatch visibility guard was
    /// held during the snapshot. Operators should alert when this
    /// climbs into the same order of magnitude as
    /// `CheckpointConfig::poll_interval`.
    pub checkpoint_duration_ms: u64,
}

/// Perform a single checkpoint: snapshot, persist, fence, compact.
///
/// The redo log mutex is held only for the initial fence sample and final
/// marker/compaction step. Dispatch is quiesced across the snapshot so every
/// redo entry through the sampled fence has a corresponding applied engine
/// effect in the snapshot.
pub fn perform_checkpoint(
    config: &CheckpointConfig,
    engine: &Engine,
    redo_log: &Mutex<RedoLog>,
) -> Result<CheckpointStats, String> {
    perform_checkpoint_with_reset_guard(config, engine, redo_log, |_| true)
}

/// Perform a checkpoint using `can_reset` to decide whether it is safe to
/// reclaim redo bytes after the checkpoint marker is durable.
pub fn perform_checkpoint_with_reset_guard<F>(
    config: &CheckpointConfig,
    engine: &Engine,
    redo_log: &Mutex<RedoLog>,
    can_reset: F,
) -> Result<CheckpointStats, String>
where
    F: Fn(u64) -> bool,
{
    // F-G4-016: the visibility guard is held across `snapshot_index`
    // and `persist_allocator`, so dispatch is quiesced for the entire
    // snapshot duration. For a primary index with millions of entries
    // this can run into the hundreds of ms and surfaces as periodic
    // tail-latency spikes correlated with checkpoint cadence. This is
    // a known tradeoff vs. the simpler "stop-the-world snapshot"
    // design; a CoW / generation-tracking refactor is deferred until
    // checkpoint latency is observed as a production bottleneck. The
    // `checkpoint_duration_ms` field of the returned `CheckpointStats`
    // exposes this so operators can alert when it crosses
    // `poll_interval`.
    let _visibility_guard = engine.acquire_checkpoint_visibility_guard();
    let entries_before = redo_log.lock().current_sequence();
    let snapshot_fence_sequence = entries_before.saturating_sub(1);
    let started_at = std::time::Instant::now();

    // 1. Snapshot index + DAH + unmined to disk (tempfile + rename).
    engine
        .snapshot_index(&config.snapshot_path)
        .map_err(|e| format!("snapshot_index: {e}"))?;

    // 2. Persist allocator state to its on-disk header.
    engine
        .persist_allocator()
        .map_err(|e| format!("persist_allocator: {e}"))?;

    // 3. Fence recovery at the sequence covered by the snapshot. This is not
    //    a Checkpoint marker: recovery must still replay post-fence entries
    //    that can exist when non-dispatch redo producers append while the
    //    snapshot is being written.
    let mut log = redo_log.lock();
    log.mark_recovery_progress(snapshot_fence_sequence)
        .map_err(|e| format!("redo checkpoint fence: {e}"))?;

    // 4. Reclaim only the covered prefix. Sequence numbers continue
    //    monotonically, and entries after the fence remain available.
    let reset_performed = if can_reset(snapshot_fence_sequence) {
        log.compact_prefix_through(snapshot_fence_sequence)
            .map_err(|e| format!("redo compact: {e}"))?;
        true
    } else {
        tracing::warn!(
            snapshot_fence_sequence,
            "checkpoint reset skipped because redo entries are still needed",
        );
        false
    };

    let usage_after = log.usage_fraction();
    let checkpoint_duration_ms = started_at.elapsed().as_millis() as u64;
    Ok(CheckpointStats {
        entries_before,
        usage_after,
        reset_performed,
        checkpoint_duration_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::SlotAllocator;
    use crate::device::{BlockDevice, MemoryDevice};
    use crate::index::{DahIndex, Index, UnminedIndex};
    use crate::locks::StripedLocks;
    use crate::ops::engine::Engine;
    use crate::redo::{RedoLog, RedoOp};
    use std::sync::Arc;

    fn make_engine_and_redo() -> (Arc<Engine>, Arc<Mutex<RedoLog>>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(8 * 1024 * 1024, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone()).unwrap();
        let index = Index::new(128).unwrap();
        let engine = Arc::new(Engine::new(
            dev.clone(),
            index,
            alloc,
            StripedLocks::new(64),
            DahIndex::new(),
            UnminedIndex::new(),
        ));

        // Dedicated device region for the redo log so it does not collide
        // with the engine's record area. The engine's MemoryDevice is
        // independent — the redo log lives on its own MemoryDevice here
        // for test simplicity.
        let redo_dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(64 * 1024, 4096).unwrap());
        let log = RedoLog::open(redo_dev, 0, 64 * 1024).unwrap();
        let redo = Arc::new(Mutex::new(log));
        (engine, redo, dir)
    }

    #[test]
    fn perform_checkpoint_resets_log_and_writes_snapshot() {
        let (engine, redo, dir) = make_engine_and_redo();
        let snap_path = dir.path().join("ckpt.snap");

        // Append some redo entries to push usage > 0.
        {
            let mut log = redo.lock();
            for _ in 0..50 {
                log.append(RedoOp::Checkpoint).unwrap();
            }
            log.flush().unwrap();
            assert!(log.usage_fraction() > 0.0);
        }

        let cfg = CheckpointConfig::new(snap_path.clone());
        let stats = perform_checkpoint(&cfg, &engine, &redo).expect("checkpoint must succeed");

        // Snapshot file exists.
        assert!(
            snap_path.exists(),
            "checkpoint must write the snapshot file"
        );

        // Log was reset. Under F-G4-001/004 the on-disk layout
        // reserves the first alignment unit for a persisted header
        // (F-G4-001) and `compact_prefix_through` writes one aligned
        // block worth of zeros (F-G4-013) even when nothing is
        // retained, so `write_position` lands at one alignment unit
        // (4 KiB on the in-memory device) rather than 0. The
        // checkpoint's reclamation effect is observable via the
        // usage drop below.
        let log = redo.lock();
        let post_write_pos = log.write_position();
        assert!(
            post_write_pos <= 4096,
            "checkpoint must reduce write_pos to at most one alignment block, found {post_write_pos}"
        );
        assert!(
            stats.entries_before > 0,
            "should have observed some entries before checkpoint"
        );
        assert!(stats.reset_performed, "unguarded checkpoint should reset");
        // Pre-checkpoint we appended 50 entries (~830 bytes plus
        // alignment padding); the post-compact write_pos is one
        // 4 KiB aligned block at most, so for the 64 KiB test log
        // (entries capacity ≈ 60 KiB) usage drops well below 10%.
        assert!(
            stats.usage_after < 0.10,
            "usage_fraction must drop sharply after reset, found {}",
            stats.usage_after
        );
    }

    #[test]
    fn perform_checkpoint_preserves_sequence_continuity() {
        let (engine, redo, dir) = make_engine_and_redo();
        let snap_path = dir.path().join("seq.snap");

        let seq_before;
        {
            let mut log = redo.lock();
            log.append(RedoOp::Checkpoint).unwrap();
            log.flush().unwrap();
            seq_before = log.current_sequence();
        }

        let cfg = CheckpointConfig::new(snap_path);
        perform_checkpoint(&cfg, &engine, &redo).unwrap();

        let mut log = redo.lock();
        // The checkpoint marker itself bumps current_sequence by 1, so
        // current_sequence after must be > seq_before.
        assert!(log.current_sequence() > seq_before);

        // Appending after reset still produces monotonically-increasing
        // sequences — sequences are NOT reset, only the write_pos is.
        let next = log.append(RedoOp::Checkpoint).unwrap();
        assert!(next > seq_before);
    }

    #[test]
    fn perform_checkpoint_skips_reset_when_guard_rejects_floor() {
        let (engine, redo, dir) = make_engine_and_redo();
        let snap_path = dir.path().join("guarded.snap");

        let floor_before;
        {
            let mut log = redo.lock();
            log.append(RedoOp::Checkpoint).unwrap();
            log.append(RedoOp::Checkpoint).unwrap();
            log.flush().unwrap();
            floor_before = log.current_sequence().saturating_sub(1);
        }

        let cfg = CheckpointConfig::new(snap_path);
        let stats = perform_checkpoint_with_reset_guard(&cfg, &engine, &redo, |floor_sequence| {
            assert_eq!(floor_sequence, floor_before);
            false
        })
        .unwrap();

        assert!(!stats.reset_performed);
        assert!(stats.usage_after > 0.0);

        let log = redo.lock();
        assert!(
            log.write_position() > 0,
            "guarded checkpoint must leave redo bytes in place"
        );
        assert!(
            log.read_from_sequence(1).unwrap().len() >= 2,
            "lagging replica catch-up must still be able to read pre-checkpoint entries"
        );
        assert!(
            log.recover().unwrap().is_empty(),
            "startup recovery must skip entries covered by the durable snapshot fence"
        );
    }

    // -- BC-01 background-task tests --

    #[test]
    fn next_backoff_doubles_then_caps() {
        let cfg = CheckpointConfig {
            high_water: 0.75,
            low_water: 0.25,
            poll_interval: Duration::from_millis(10),
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(400),
            snapshot_path: PathBuf::from("/dev/null"),
        };
        let b0 = next_backoff(Duration::ZERO, &cfg);
        assert_eq!(b0, Duration::from_millis(100), "first failure → initial");
        let b1 = next_backoff(b0, &cfg);
        assert_eq!(b1, Duration::from_millis(200), "second → double");
        let b2 = next_backoff(b1, &cfg);
        assert_eq!(b2, Duration::from_millis(400), "third → at cap");
        let b3 = next_backoff(b2, &cfg);
        assert_eq!(b3, Duration::from_millis(400), "fourth → still capped");
    }

    #[test]
    fn sleep_with_shutdown_returns_early_on_flag() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let s2 = shutdown.clone();
        let handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            s2.store(true, Ordering::Relaxed);
        });
        let start = std::time::Instant::now();
        let finished_full =
            sleep_with_shutdown(Duration::from_secs(5), &shutdown, Duration::from_millis(5));
        let elapsed = start.elapsed();
        handle.join().unwrap();
        assert!(!finished_full, "must report shutdown observed");
        assert!(
            elapsed < Duration::from_millis(500),
            "must return within ~slice of shutdown, took {elapsed:?}"
        );
    }

    #[test]
    fn background_task_triggers_checkpoint_when_high_water_crossed() {
        // BC-01 acceptance test: install a tiny redo log, push usage
        // above the high-water mark, and confirm the background task
        // runs a checkpoint that drops usage well below low-water.
        let (engine, redo, dir) = make_engine_and_redo();
        let snap_path = dir.path().join("bg-trigger.snap");
        // The 64 KiB test redo log has a ~60 KiB entries region and
        // each `RedoOp::Checkpoint` serialises to ~21 bytes, so we
        // need enough appends to push usage well past the high
        // water mark. After a successful `compact_prefix_through`
        // the write_position lands at one alignment block (4 KiB ≈
        // 6.7 % of the entries region), so the low water mark must
        // be ABOVE that floor — otherwise no checkpoint outcome
        // could clear the trigger.
        let cfg = CheckpointConfig {
            high_water: 0.50,
            low_water: 0.20,
            poll_interval: Duration::from_millis(10),
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(40),
            snapshot_path: snap_path.clone(),
        };

        {
            let mut log = redo.lock();
            // 2000 appends × ~21 B ≈ 42 KB which is ~70 % of the
            // 60 KB entries region — well above the 50 % high water.
            for _ in 0..2000 {
                log.append(RedoOp::Checkpoint).unwrap();
            }
            log.flush().unwrap();
            assert!(
                log.usage_fraction() >= cfg.high_water,
                "test setup: usage must be above high water, got {}",
                log.usage_fraction()
            );
        }

        let shutdown = Arc::new(AtomicBool::new(false));
        let handle = spawn_checkpoint_task(cfg.clone(), engine, redo.clone(), shutdown.clone());

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let mut final_usage = 1.0;
        while std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
            final_usage = redo.lock().usage_fraction();
            if final_usage <= cfg.low_water {
                break;
            }
        }
        shutdown.store(true, Ordering::Relaxed);

        let join_start = std::time::Instant::now();
        handle.join().expect("checkpoint thread must not panic");
        let join_elapsed = join_start.elapsed();

        assert!(snap_path.exists(), "checkpoint must have written snapshot");
        assert!(
            final_usage <= cfg.low_water,
            "background checkpoint must reduce usage below low water, got {final_usage}"
        );
        assert!(
            join_elapsed < Duration::from_secs(1),
            "task must shut down within 1 s, took {join_elapsed:?}"
        );
    }

    #[test]
    fn background_task_does_not_re_trigger_below_low_water() {
        // Hysteresis (debounce) regression test: with usage far below
        // high water at all times, the task must NOT take a single
        // checkpoint over many polls.
        let (engine, redo, dir) = make_engine_and_redo();
        let snap_path = dir.path().join("no-trigger.snap");
        let cfg = CheckpointConfig {
            high_water: 0.95,
            low_water: 0.10,
            poll_interval: Duration::from_millis(10),
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(40),
            snapshot_path: snap_path.clone(),
        };

        {
            let mut log = redo.lock();
            for _ in 0..5 {
                log.append(RedoOp::Checkpoint).unwrap();
            }
            log.flush().unwrap();
            assert!(log.usage_fraction() < cfg.high_water);
        }

        let shutdown = Arc::new(AtomicBool::new(false));
        let handle = spawn_checkpoint_task(cfg, engine, redo, shutdown.clone());

        std::thread::sleep(Duration::from_millis(200));
        shutdown.store(true, Ordering::Relaxed);
        handle.join().expect("thread must not panic");

        assert!(
            !snap_path.exists(),
            "task must not have taken a checkpoint when usage stayed below high water"
        );
    }

    #[test]
    fn sustained_mutations_never_brick_when_task_is_running() {
        // BC-01 acceptance: with the background task running, a
        // mutation workload that would brick the master pre-fix
        // (every `append` after the redo log fills returns
        // `RedoError::LogFull`) must complete with zero `LogFull`
        // errors observed by the caller.
        //
        // Without the background task, the 64 KiB test redo log
        // (~60 KiB entries region, ~21 B/entry) bricks after about
        // 3000 appends — every subsequent append returns
        // `RedoError::LogFull`. We push 8000 here in paced bursts
        // (well past the brick threshold) and assert that the
        // checkpointer keeps space available so the caller never
        // observes `LogFull`.
        //
        // The pacing models the production reality: the dispatcher
        // doesn't try to write 64 MiB in 0 seconds — it produces
        // entries at finite rate while the checkpointer reclaims
        // them in the background. The test fails (pre-fix) with
        // thousands of `LogFull` errors if you delete the
        // `spawn_checkpoint_task` line below.
        let (engine, redo, dir) = make_engine_and_redo();
        let cfg = CheckpointConfig {
            high_water: 0.50,
            low_water: 0.20,
            poll_interval: Duration::from_millis(5),
            initial_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(40),
            snapshot_path: dir.path().join("sustained.snap"),
        };

        let high_water = cfg.high_water;
        let shutdown = Arc::new(AtomicBool::new(false));
        let handle = spawn_checkpoint_task(cfg, engine, redo.clone(), shutdown.clone());

        let mut log_full_errors = 0usize;
        // 16 bursts of 500 entries each. That's 8000 total entries —
        // comfortably past the 3000-entry bricking threshold for the
        // pre-fix code path. Each burst pushes usage past high_water;
        // between bursts we wait (bounded) until the checkpointer has
        // actually reclaimed below high water. A fixed 25 ms gap was
        // flaky under load — the checkpointer thread is not guaranteed
        // to be scheduled within an arbitrary wall-clock window on a
        // busy machine. Pre-fix (delete the `spawn_checkpoint_task`
        // line above) usage never drops, every wait times out at its
        // bound, and the workload still bricks the log with thousands
        // of `LogFull` errors — the test keeps its detection power.
        for _burst in 0..16 {
            for _ in 0..500 {
                let result = {
                    let mut log = redo.lock();
                    log.append(RedoOp::Checkpoint)
                };
                if let Err(crate::redo::RedoError::LogFull { .. }) = result {
                    log_full_errors += 1;
                }
            }
            let reclaim_deadline = std::time::Instant::now() + Duration::from_secs(2);
            while redo.lock().usage_fraction() >= high_water
                && std::time::Instant::now() < reclaim_deadline
            {
                std::thread::sleep(Duration::from_millis(5));
            }
        }

        shutdown.store(true, Ordering::Relaxed);
        handle.join().expect("checkpoint thread must not panic");

        assert_eq!(
            log_full_errors, 0,
            "with the background checkpoint task running, no mutation should observe LogFull",
        );
    }

    #[test]
    fn shutdown_joins_promptly_while_checkpoints_in_flight() {
        // B-03: the bin signals the checkpointer via the shared shutdown
        // flag and joins its handle (bounded by `join_with_timeout`'s
        // 5 s). The other two shutdown tests flip the flag while the
        // task is idle or after the trigger has settled; this one flips
        // it mid-activity — a writer thread keeps pushing redo usage
        // over high water so checkpoints are actively firing when the
        // stop signal lands. The join must still complete promptly.
        let (engine, redo, dir) = make_engine_and_redo();
        let snap_path = dir.path().join("inflight.snap");
        let cfg = CheckpointConfig {
            high_water: 0.30,
            low_water: 0.10,
            poll_interval: Duration::from_millis(2),
            initial_backoff: Duration::from_millis(2),
            max_backoff: Duration::from_millis(10),
            snapshot_path: snap_path.clone(),
        };

        let shutdown = Arc::new(AtomicBool::new(false));
        let handle = spawn_checkpoint_task(cfg, engine, redo.clone(), shutdown.clone());

        // Sustained pressure until told to stop. LogFull is acceptable
        // here — the point is keeping the checkpointer busy, not
        // lossless throughput.
        let writer_stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let redo = redo.clone();
            let stop = writer_stop.clone();
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let _ = redo.lock().append(RedoOp::Checkpoint);
                    std::thread::sleep(Duration::from_micros(50));
                }
            })
        };

        // Wait until at least one checkpoint has actually fired, so the
        // stop signal demonstrably lands while checkpoints are in flight.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !snap_path.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            snap_path.exists(),
            "a checkpoint must have fired before the stop signal"
        );

        shutdown.store(true, Ordering::Relaxed);
        let join_start = std::time::Instant::now();
        handle.join().expect("checkpoint thread must not panic");
        let join_elapsed = join_start.elapsed();

        writer_stop.store(true, Ordering::Relaxed);
        writer.join().expect("writer thread must not panic");

        assert!(
            join_elapsed < Duration::from_secs(1),
            "stop+join must be prompt while checkpoints are in flight, took {join_elapsed:?}"
        );
    }

    #[test]
    fn background_task_shuts_down_within_bounded_time() {
        // Even with no work to do, the task must stop quickly on
        // shutdown — verified separately so a regression in
        // sleep_with_shutdown wiring is caught even when the trigger
        // never fires.
        let (engine, redo, dir) = make_engine_and_redo();
        let cfg = CheckpointConfig {
            high_water: 0.99,
            low_water: 0.10,
            poll_interval: Duration::from_millis(50),
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(40),
            snapshot_path: dir.path().join("shutdown.snap"),
        };

        let shutdown = Arc::new(AtomicBool::new(false));
        let handle = spawn_checkpoint_task(cfg, engine, redo, shutdown.clone());
        std::thread::sleep(Duration::from_millis(20));

        let start = std::time::Instant::now();
        shutdown.store(true, Ordering::Relaxed);
        handle.join().expect("thread must not panic");
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(1),
            "task must shut down within 1 s, took {elapsed:?}"
        );
    }
}
