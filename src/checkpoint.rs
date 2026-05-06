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
//! 1. Acquire the redo log mutex (blocks new appends).
//! 2. Snapshot the in-memory primary, DAH, and unmined indexes to disk
//!    via [`crate::ops::engine::Engine::snapshot_index`] (atomic via
//!    tempfile + rename).
//! 3. Persist the allocator's freelist + high-water mark via
//!    [`crate::ops::engine::Engine::persist_allocator`].
//! 4. Append a [`crate::redo::RedoOp::Checkpoint`] marker to the log.
//! 5. Call [`crate::redo::RedoLog::reset`] to wipe entries and reset
//!    `write_pos` to 0.
//! 6. Release the mutex; appends resume from offset 0.
//!
//! Crash safety: each step's effects are durable independently of the
//! others. Snapshot is fsynced before the rename; allocator persist is
//! fsynced before returning; checkpoint marker is fsynced; reset wipes
//! the leading block atomically. After a crash at any point recovery
//! either replays the un-checkpointed entries on top of the most recent
//! snapshot (safe — recovery is idempotent) or, if reset already ran,
//! observes an empty log and trusts the snapshot directly.
//!
//! Concurrency: the redo mutex serializes the checkpoint with concurrent
//! mutation appends. The snapshot reads index/dah/unmined under their own
//! locks; those locks are held for the snapshot's duration which is
//! O(entries) — operators should size the redo log so the checkpoint
//! cadence keeps the snapshot small enough that the write-stall is
//! tolerable. (R-215 tracks moving snapshotting off the redo-mutex hot
//! path via copy-on-write or epoch-based reads.)

use parking_lot::Mutex;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::ops::engine::Engine;
use crate::redo::RedoLog;

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
                match perform_checkpoint(&config, &engine, &redo_log) {
                    Ok(stats) => {
                        tracing::info!(
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            entries_before = stats.entries_before,
                            usage_after = stats.usage_after,
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
}

/// Perform a single checkpoint: snapshot, persist, mark, reset.
///
/// Held lock order: redo log mutex (whole function), then index/dah/unmined
/// (acquired inside `engine.snapshot_index`), then allocator (acquired
/// inside `engine.persist_allocator`). New mutation appends block on the
/// redo mutex for the duration.
pub fn perform_checkpoint(
    config: &CheckpointConfig,
    engine: &Engine,
    redo_log: &Mutex<RedoLog>,
) -> Result<CheckpointStats, String> {
    // Hold the redo lock for the whole checkpoint so concurrent appends
    // cannot interleave with the snapshot/marker/reset sequence.
    let mut log = redo_log.lock();
    let entries_before = log.current_sequence();

    // 1. Snapshot index + DAH + unmined to disk (tempfile + rename).
    engine
        .snapshot_index(&config.snapshot_path)
        .map_err(|e| format!("snapshot_index: {e}"))?;

    // 2. Persist allocator state to its on-disk header.
    engine
        .persist_allocator()
        .map_err(|e| format!("persist_allocator: {e}"))?;

    // 3. Write a checkpoint marker so any post-snapshot entries that
    //    landed before the snapshot/persist returned (there should be
    //    none because we hold the lock, but the marker is also a
    //    forward-compatibility invariant) have an explicit commit point.
    log.checkpoint()
        .map_err(|e| format!("redo checkpoint: {e}"))?;

    // 4. Reset the log so future appends write from offset 0 again.
    //    Sequence numbers continue monotonically.
    log.reset().map_err(|e| format!("redo reset: {e}"))?;

    let usage_after = log.usage_fraction();
    Ok(CheckpointStats {
        entries_before,
        usage_after,
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
}
