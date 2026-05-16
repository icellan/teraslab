//! Background redo-log checkpoint task.
//!
//! The redo log is a fixed-size circular-by-checkpoint write-ahead log.
//! Without a reclamation mechanism it would fill (~750k mutations at a
//! 64 MiB default + ~85 B/entry) and the master would brick: `append` would
//! return [`crate::redo::RedoError::LogFull`] and every mutation would error
//! out.
//!
//! This module wires the missing reclamation cadence. A background thread
//! periodically samples the log's usage fraction; when it crosses the
//! configured threshold (default 0.5), the thread takes a checkpoint:
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
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// Usage fraction (0.0..1.0) at or above which the next tick triggers
    /// a checkpoint. Default: 0.5.
    pub trigger_usage: f64,
    /// How often the task wakes to sample usage. Default: 100 ms.
    pub poll_interval: Duration,
    /// Where to write the index/dah/unmined snapshot. Must be on the same
    /// filesystem so the tempfile + rename is atomic.
    pub snapshot_path: PathBuf,
}

impl CheckpointConfig {
    pub fn new(snapshot_path: PathBuf) -> Self {
        Self {
            trigger_usage: 0.5,
            poll_interval: Duration::from_millis(100),
            snapshot_path,
        }
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
            tracing::info!(
                trigger_usage = config.trigger_usage,
                poll_interval_ms = config.poll_interval.as_millis() as u64,
                "checkpoint task started",
            );
            while !shutdown.load(Ordering::Relaxed) {
                std::thread::sleep(config.poll_interval);
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }

                let usage = redo_log.lock().usage_fraction();
                if usage < config.trigger_usage {
                    continue;
                }

                tracing::info!(
                    usage_fraction = usage,
                    threshold = config.trigger_usage,
                    "redo log above watermark — checkpointing",
                );
                let started = std::time::Instant::now();
                match perform_checkpoint_with_reset_guard(
                    &config,
                    &engine,
                    &redo_log,
                    |floor_sequence| reset_guard(floor_sequence),
                ) {
                    Ok(stats) => {
                        tracing::info!(
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            entries_before = stats.entries_before,
                            usage_after = stats.usage_after,
                            reset_performed = stats.reset_performed,
                            checkpoint_duration_ms = stats.checkpoint_duration_ms,
                            "checkpoint complete",
                        );
                    }
                    Err(e) => {
                        tracing::error!(err = %e, "checkpoint failed");
                    }
                }
            }
            tracing::info!("checkpoint task exiting");
        })
        .expect("spawn checkpoint thread")
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
    let _visibility_guard = engine.acquire_dispatch_visibility_guard();
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

        // Log was reset.
        let log = redo.lock();
        assert_eq!(log.write_position(), 0, "reset must zero write_pos");
        assert!(
            stats.entries_before > 0,
            "should have observed some entries before checkpoint"
        );
        assert!(stats.reset_performed, "unguarded checkpoint should reset");
        assert!(
            stats.usage_after < 0.01,
            "usage_fraction must drop near zero after reset"
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
}
