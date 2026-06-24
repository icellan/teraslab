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
//! 4. Durability barrier: flush the on-disk (redb) index backends via
//!    [`crate::ops::engine::Engine::flush_index_durable`] and sync the
//!    data device. Redo reclamation is only legal after this barrier —
//!    per-op redb commits use `Durability::Eventual` and data pwrites may
//!    sit in the drive's volatile write cache, so the redo entries are the
//!    only durable copy of those mutations until the barrier completes.
//!    A barrier failure aborts the checkpoint with no fence written and
//!    no compaction performed.
//! 5. Append a [`crate::redo::RedoOp::RecoveryProgress`] fence through the
//!    snapshotted sequence.
//! 6. Compact redo entries through that fence when replica ACK watermarks
//!    allow reclamation; post-fence entries are preserved.
//!
//! Crash safety: each step's effects are durable independently of the
//! others. Snapshot is fsynced before the rename; allocator persist is
//! fsynced before returning; the barrier makes index backends and data
//! device durable before any redo entry is fenced or reclaimed;
//! recovery-progress marker is fsynced; prefix compaction fsyncs the
//! rewritten log. After a crash at any point recovery either replays all
//! un-fenced entries on top of the most recent snapshot (safe — recovery
//! is idempotent) or, if compaction already ran, sees only entries newer
//! than the snapshot fence — whose covered state the barrier made durable.
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
        .spawn(move || run_checkpoint_loop(config, engine, redo_log, shutdown, reset_guard))
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
        let outcome =
            perform_checkpoint_with_reset_guard(&config, &engine, &redo_log, |floor_sequence| {
                reset_guard(floor_sequence)
            });
        let elapsed = started.elapsed();
        if let Some(m) = crate::metrics::redo_metrics() {
            m.redo_checkpoint_duration_ns
                .record_ns(elapsed.as_nanos() as u64);
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
fn sleep_with_shutdown(total: Duration, shutdown: &Arc<AtomicBool>, slice: Duration) -> bool {
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

/// Perform a single checkpoint: snapshot, persist, durability barrier,
/// fence, compact.
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
    // Sample the recovery fence under a BRIEF exclusive quiesce — not across
    // the snapshot.
    //
    // The fence must be a sequence at which every covered redo entry has its
    // engine effect applied. Mutations hold the EXCLUSIVE visibility barrier
    // from before-apply through redo durability (see
    // `Engine::acquire_mutation_visibility_guard`), so acquiring it momentarily
    // drains every in-flight apply; the redo sequence sampled inside that
    // window is therefore fully applied. This is O(1) — it does NOT span the
    // O(index) snapshot.
    //
    // F-G4-016 (resolved): the barrier used to be held across the ENTIRE
    // `snapshot_index`, quiescing all reads and writes for the snapshot's
    // duration — hundreds of ms for millions of entries, growing into
    // multi-second serving stalls at the full UTXO set, every checkpoint. Now
    // serving stays live across the snapshot: the snapshot is "fuzzy" (it may
    // capture mutations that landed after the fence) and recovery reconciles
    // that post-fence tail by idempotent redo replay (see `crate::recovery`).
    // `checkpoint_duration_ms` now measures background snapshot time, not a
    // stall.
    let entries_before = {
        let _quiesce = engine.acquire_checkpoint_visibility_guard();
        redo_log.lock().current_sequence()
    };
    let snapshot_fence_sequence = entries_before.saturating_sub(1);
    let started_at = std::time::Instant::now();

    // 1. Snapshot index + DAH + unmined to disk (tempfile + rename). FUZZY /
    //    non-blocking: serving is NOT quiesced across this — `snapshot_index`
    //    serializes each shard under its own short-lived read lock in write-path
    //    order (shard before secondaries), so it cannot deadlock the write path
    //    and never blocks reads.
    engine
        .snapshot_index(&config.snapshot_path)
        .map_err(|e| format!("snapshot_index: {e}"))?;

    // 2. Persist allocator state to its on-disk header (fsynced before
    //    returning).
    engine
        .persist_allocator()
        .map_err(|e| format!("persist_allocator: {e}"))?;

    // 2b. Persist the node's last-durable height to its tiny durable file
    //     (deletion-tombstone design §4, height subsystem). Always-on and
    //     additive; a no-op when no height path is attached. A failure here
    //     does NOT abort the checkpoint: the height is a monotone hint
    //     recoverable from the record-derived floor, so unlike the index /
    //     allocator it is non-fatal — log and continue so a transient height
    //     write error never blocks redo reclamation.
    if let Err(e) = engine.persist_last_durable_height() {
        tracing::warn!(err = %e, "checkpoint: last-durable-height persist failed (non-fatal)");
    }

    // 3. Durability barrier (B-1/G-1 audit fixes). Redo reclamation is
    //    only legal once every store the fenced entries cover is durable:
    //
    //    * On-disk (redb) index backends commit with Durability::Eventual
    //      per op — crash-safe only while their redo entries are
    //      replayable. Flush them durably NOW; a failure aborts the
    //      checkpoint before any fence or compaction (the redo log is
    //      untouched, so nothing is lost and the next attempt retries).
    //    * Data-device pwrites (slots, metadata) can sit in the drive's
    //      volatile write cache; sync the device so a power loss after
    //      compaction cannot silently revert acked mutations whose only
    //      durable copy was the just-reclaimed redo prefix.
    crate::fault_injection::check(crate::fault_injection::SyncPoint::BeforeCheckpointDataSync);
    engine
        .flush_index_durable()
        .map_err(|e| format!("index durable flush: {e}"))?;
    engine
        .device()
        .sync()
        .map_err(|e| format!("data device sync: {e}"))?;

    // 4. Fence recovery at the sequence covered by the snapshot. This is not
    //    a Checkpoint marker: recovery must still replay post-fence entries
    //    that can exist when non-dispatch redo producers append while the
    //    snapshot is being written.
    let mut log = redo_log.lock();
    log.mark_recovery_progress(snapshot_fence_sequence)
        .map_err(|e| format!("redo checkpoint fence: {e}"))?;

    // Fault-injection point (test-only, inert without the
    // `fault-injection` feature): the snapshot is durable, the
    // data/index barrier has run, and the recovery-progress fence is
    // written, but the redo prefix has NOT yet been reclaimed. A crash
    // here must lose no acked write — the snapshot and the still-intact
    // redo prefix both cover them.
    crate::fault_injection::check(
        crate::fault_injection::SyncPoint::AfterSnapshotRenameBeforeReclaim,
    );

    // 5. Reclaim only the covered prefix. Sequence numbers continue
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

    /// Isolation contract: client reads (which take the SHARED
    /// `dispatch_visibility_barrier`) are NOT stalled by a running checkpoint.
    ///
    /// Pre-fix the checkpoint held the EXCLUSIVE barrier across the entire
    /// O(index) `snapshot_index`, so a reader's `.read()` blocked for the whole
    /// snapshot — at the full UTXO set a multi-second serving stall every
    /// ~75 s. With the fuzzy checkpoint the barrier is held only for an O(1)
    /// fence sample, so reads fly across the snapshot. We seed a multi-shard
    /// index large enough that the snapshot takes real wall time, run the
    /// checkpoint, and spin read-guard acquisitions on another thread: pre-fix
    /// that thread would complete ~1 acquisition (blocked the whole time);
    /// fuzzy lets thousands through.
    #[test]
    fn checkpoint_does_not_stall_reads() {
        use crate::index::{ShardedIndex, TxIndexEntry, TxKey};
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
        use std::time::{Duration, Instant};

        let dir = tempfile::tempdir().unwrap();
        let dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone()).unwrap();

        // Seed a 16-shard index directly (in-memory, no device I/O) with enough
        // entries that the snapshot takes meaningful wall time — that is the
        // window a stop-the-world checkpoint would block reads across.
        const N: u32 = 300_000;
        let index = ShardedIndex::new_in_memory(N as usize, 16).unwrap();
        for i in 0..N {
            let mut txid = [0u8; 32];
            txid[0..4].copy_from_slice(&i.to_le_bytes());
            // Spread keys across shards (index_shard_for_key hashes [24..32]).
            txid[24..32]
                .copy_from_slice(&(i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes());
            index
                .register(
                    TxKey { txid },
                    TxIndexEntry {
                        device_id: 0,
                        record_offset: i as u64 * 256,
                        utxo_count: 1,
                        block_entry_count: 0,
                        tx_flags: 0,
                        spent_utxos: 0,
                        dah_or_preserve: 0,
                        unmined_since: 0,
                        generation: 0,
                    },
                )
                .unwrap();
        }
        let engine = Arc::new(Engine::new_with_sharded_index(
            dev.clone(),
            index,
            alloc,
            StripedLocks::new(64),
            DahIndex::new(),
            UnminedIndex::new(),
        ));

        let redo_dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(64 * 1024, 4096).unwrap());
        let redo = Arc::new(Mutex::new(RedoLog::open(redo_dev, 0, 64 * 1024).unwrap()));
        {
            let mut log = redo.lock();
            for _ in 0..50 {
                log.append(RedoOp::Checkpoint).unwrap();
            }
            log.flush().unwrap();
        }

        let cfg = CheckpointConfig::new(dir.path().join("isolation.snap"));

        let done = Arc::new(AtomicBool::new(false));
        let reads_done = Arc::new(AtomicU64::new(0));
        let max_latency_us = Arc::new(AtomicU64::new(0));

        let reader = {
            let engine = engine.clone();
            let done = done.clone();
            let reads_done = reads_done.clone();
            let max_latency_us = max_latency_us.clone();
            std::thread::spawn(move || {
                while !done.load(Ordering::Relaxed) {
                    let t = Instant::now();
                    {
                        let _g = engine.acquire_dispatch_visibility_guard();
                    }
                    let us = t.elapsed().as_micros() as u64;
                    reads_done.fetch_add(1, Ordering::Relaxed);
                    max_latency_us.fetch_max(us, Ordering::Relaxed);
                    // Cooperative, not a tight busy-spin: still completes
                    // thousands of acquisitions across the snapshot, without
                    // pegging a core and starving jitter-sensitive tests that
                    // run concurrently in the same binary.
                    std::thread::yield_now();
                }
            })
        };

        // Let the reader warm up, then run the checkpoint to completion.
        std::thread::sleep(Duration::from_millis(5));
        let started = Instant::now();
        let _stats = perform_checkpoint(&cfg, &engine, &redo).expect("checkpoint must succeed");
        let ckpt_wall = started.elapsed();
        done.store(true, Ordering::Relaxed);
        reader.join().unwrap();

        let reads = reads_done.load(Ordering::Relaxed);
        let max_us = max_latency_us.load(Ordering::Relaxed);
        let ckpt_us = ckpt_wall.as_micros();

        // The snapshot must have taken real wall time, else the test proves
        // nothing about isolation.
        assert!(
            ckpt_us >= 20_000,
            "snapshot too fast ({ckpt_us}us) to be a meaningful isolation test; raise N"
        );
        // Reads kept flowing across the whole checkpoint. A stop-the-world
        // checkpoint would block the reader for the full snapshot, completing
        // ~1 acquisition; the fuzzy checkpoint lets thousands through.
        assert!(
            reads >= 1000,
            "reads stalled during checkpoint: only {reads} guard acquisitions across {ckpt_us}us"
        );
        // Sanity: no read waited for ~the whole checkpoint (a reader blocked on
        // a barrier held across the snapshot would show max ≈ checkpoint
        // duration). The `reads >= 1000` count above is the definitive
        // non-blocking signal; this bound stays loose so heavy parallel
        // test-runner scheduling jitter cannot flake it.
        assert!(
            (max_us as u128) < ckpt_us,
            "a read stalled ~the whole checkpoint ({max_us}us vs {ckpt_us}us) — \
             is the barrier still held across the snapshot?"
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

    // -- B-1 / G-1 durability-barrier tests --

    /// Seed a volatile data device + redo log with two acked, WAL-first
    /// mutations: a 2-output create followed by a spend of slot 0.
    /// Returns everything a restart needs to verify the mutations.
    ///
    /// The sequence mirrors the dispatch path exactly: redo append +
    /// fsync FIRST, then the data-device pwrite (which on the volatile
    /// device stays in the simulated drive cache until a sync).
    fn seed_acked_create_and_spend(
        data_dev: &MemoryDevice,
        alloc: &mut SlotAllocator,
        log: &mut RedoLog,
    ) -> (crate::index::TxKey, u64, [u8; 36]) {
        use crate::record::{TxMetadata, UtxoSlot};

        let mut txid = [0u8; 32];
        txid[0] = 0xB1;
        let key = crate::index::TxKey { txid };
        let utxo_count = 2u32;

        let mut meta = TxMetadata::new(utxo_count);
        meta.tx_id = txid;
        meta.record_size = TxMetadata::record_size_for(utxo_count) as u32;
        let slots: Vec<UtxoSlot> = (0..utxo_count)
            .map(|i| {
                let mut h = [0u8; 32];
                h[0] = i as u8 + 1;
                UtxoSlot::new_unspent(h)
            })
            .collect();
        let record_offset = alloc
            .allocate(TxMetadata::record_size_for(utxo_count))
            .unwrap();

        // Acked mutation 1: create (WAL-first).
        let mut record_bytes = Vec::with_capacity(
            crate::record::METADATA_SIZE + slots.len() * crate::record::UTXO_SLOT_SIZE,
        );
        let mut meta_bytes = [0u8; crate::record::METADATA_SIZE];
        meta.to_bytes(&mut meta_bytes);
        record_bytes.extend_from_slice(&meta_bytes);
        for slot in &slots {
            let mut sb = [0u8; crate::record::UTXO_SLOT_SIZE];
            slot.to_bytes(&mut sb);
            record_bytes.extend_from_slice(&sb);
        }
        log.append_and_flush(RedoOp::CreateV2 {
            tx_key: key,
            record_offset,
            utxo_count,
            is_conflicting: false,
            record_bytes,
            parent_txids: Vec::new(),
        })
        .unwrap();
        crate::io::write_full_record(data_dev, record_offset, &meta, &slots).unwrap();

        // Acked mutation 2: spend slot 0 (WAL-first).
        let mut spending_data = [0u8; 36];
        spending_data[..4].copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        log.append_and_flush(RedoOp::SpendV2 {
            tx_key: key,
            offset: 0,
            spending_data,
            new_spent_count: 1,
            current_block_height: 0,
            block_height_retention: 0,
            target_generation: 1,
            updated_at: 0,
            utxo_hash: None,
        })
        .unwrap();
        let spent = UtxoSlot::new_spent(slots[0].hash, spending_data);
        crate::io::write_utxo_slot(data_dev, record_offset, 0, &spent).unwrap();
        meta.spent_utxos = 1;
        crate::io::write_metadata(data_dev, record_offset, &meta).unwrap();

        (key, record_offset, spending_data)
    }

    /// B-1 (CRITICAL): the checkpoint must issue a data-device
    /// durability barrier BEFORE fencing/compacting the redo log.
    /// Pre-fix, the slot/metadata/allocator-header pwrites for every
    /// acked mutation sat in the (simulated) volatile drive cache while
    /// the only durable copy — the redo entries — was reclaimed; a power
    /// loss then silently reverted acked spends and creates.
    #[test]
    fn checkpoint_makes_acked_mutations_durable_before_redo_reclamation() {
        use crate::index::{DahBackend, PrimaryBackend, TxIndexEntry, UnminedBackend};
        use crate::recovery::recover_all_with_allocator;

        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("powerloss.snap");

        // Data device with a simulated volatile write cache.
        let data_dev = Arc::new(MemoryDevice::new_volatile(16 * 1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(data_dev.clone() as Arc<dyn BlockDevice>).unwrap();

        // Redo log on its own always-durable device (RedoLog::flush syncs it).
        let redo_dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
        let mut log = RedoLog::open(redo_dev.clone(), 0, 1024 * 1024).unwrap();

        let (key, record_offset, spending_data) =
            seed_acked_create_and_spend(&data_dev, &mut alloc, &mut log);

        // Engine over the same device/allocator with the record registered,
        // so the checkpoint snapshot covers it.
        let mut index = PrimaryBackend::new_in_memory(128).unwrap();
        index
            .register(
                key,
                TxIndexEntry {
                    device_id: 0,
                    record_offset,
                    utxo_count: 2,
                    block_entry_count: 0,
                    tx_flags: 0,
                    spent_utxos: 1,
                    dah_or_preserve: 0,
                    unmined_since: 0,
                    generation: 1,
                },
            )
            .unwrap();
        let engine = Engine::new(
            data_dev.clone() as Arc<dyn BlockDevice>,
            index,
            alloc,
            StripedLocks::new(16),
            DahIndex::new(),
            UnminedIndex::new(),
        );
        let redo = Mutex::new(log);

        let cfg = CheckpointConfig::new(snap_path.clone());
        let stats = perform_checkpoint(&cfg, &engine, &redo).expect("checkpoint must succeed");
        assert!(
            stats.reset_performed,
            "redo prefix must have been reclaimed"
        );

        // Power loss: every data-device write not covered by a sync is gone.
        assert!(data_dev.simulate_power_loss(), "device must be volatile");

        // Restart: allocator from its header, index from the snapshot,
        // then redo replay (empty — the fence covers everything).
        let mut alloc2 = SlotAllocator::recover(data_dev.clone() as Arc<dyn BlockDevice>)
            .expect("allocator header must be durable after checkpoint");
        let (primary2, dah2, unmined2, _flags) =
            PrimaryBackend::restore_all(&snap_path).expect("snapshot must restore");
        let index2 = crate::index::ShardedIndex::from_single(primary2);
        let mut dah_b = DahBackend::from(dah2);
        let mut unmined_b = UnminedBackend::from(unmined2);
        let log2 = RedoLog::open(redo_dev, 0, 1024 * 1024).unwrap();
        recover_all_with_allocator(
            &*data_dev,
            &log2,
            &index2,
            &mut dah_b,
            &mut unmined_b,
            Some(&mut alloc2),
        )
        .expect("recovery must succeed");

        // Both acked mutations must be reproduced.
        let entry = index2.lookup(&key).expect("record must still be indexed");
        assert_eq!(entry.record_offset, record_offset);
        let meta_after = crate::io::read_metadata(&*data_dev, record_offset)
            .expect("record metadata must be durable after checkpoint + power loss");
        assert_eq!(
            { meta_after.tx_id },
            key.txid,
            "metadata must belong to the record"
        );
        assert_eq!(
            { meta_after.spent_utxos },
            1,
            "acked spend count must survive"
        );
        let slot0 = crate::io::read_utxo_slot(&*data_dev, record_offset, 0).unwrap();
        assert!(slot0.is_spent(), "acked spend must not silently revert");
        assert_eq!(
            slot0.spending_data, spending_data,
            "spending data must survive"
        );
        let slot1 = crate::io::read_utxo_slot(&*data_dev, record_offset, 1).unwrap();
        assert!(slot1.is_unspent(), "untouched slot must stay unspent");
    }

    /// G-1 (CRITICAL): with the redb (`OnDisk`) primary backend, per-op
    /// commits use `Durability::Eventual` and rely on the redo log for
    /// crash recovery. The checkpoint must therefore make redb durable
    /// BEFORE fencing/compacting — and a flush failure must abort the
    /// checkpoint cleanly: no fence written, no redo compaction, error
    /// surfaced. A subsequent checkpoint (flush healthy again) succeeds.
    #[test]
    fn checkpoint_aborts_and_preserves_redo_when_redb_flush_fails() {
        use crate::index::PrimaryBackend;
        use crate::index::redb_primary::RedbPrimary;

        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("redb-gate.snap");

        let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(8 * 1024 * 1024, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone()).unwrap();
        let primary = RedbPrimary::open(&dir.path().join("primary.redb"), 1024 * 1024).unwrap();
        primary.arm_fail_next_flush();
        let engine = Engine::new(
            dev,
            PrimaryBackend::OnDisk(primary),
            alloc,
            StripedLocks::new(16),
            DahIndex::new(),
            UnminedIndex::new(),
        );

        let redo_dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(64 * 1024, 4096).unwrap());
        let log = RedoLog::open(redo_dev, 0, 64 * 1024).unwrap();
        let redo = Mutex::new(log);
        {
            let mut log = redo.lock();
            for i in 0..4u64 {
                log.append(RedoOp::AllocateRegion {
                    offset: 4096 * (i + 1),
                    size: 4096,
                    device_id: 0,
                })
                .unwrap();
            }
            log.flush().unwrap();
        }
        let write_pos_before = redo.lock().write_position();

        let cfg = CheckpointConfig::new(snap_path.clone());
        let err = perform_checkpoint(&cfg, &engine, &redo)
            .expect_err("checkpoint must abort when the redb durability flush fails");
        assert!(
            err.contains("index durable flush"),
            "error must name the failing step, got: {err}"
        );

        {
            let log = redo.lock();
            assert_eq!(
                log.write_position(),
                write_pos_before,
                "aborted checkpoint must append no fence and compact nothing"
            );
            let recovered = log.recover().unwrap();
            assert_eq!(
                recovered.len(),
                4,
                "every redo entry must remain replayable after the aborted checkpoint"
            );
        }

        // The fail flag auto-disarms: the next checkpoint must succeed,
        // flush redb durably, fence, and reclaim the prefix.
        let stats =
            perform_checkpoint(&cfg, &engine, &redo).expect("subsequent checkpoint must succeed");
        assert!(stats.reset_performed, "healthy checkpoint must compact");
        assert!(
            redo.lock().recover().unwrap().is_empty(),
            "the fence must now cover the previously appended entries"
        );
    }

    /// Crash inside the checkpoint at the new
    /// [`crate::fault_injection::SyncPoint::BeforeCheckpointDataSync`]
    /// boundary (after snapshot + allocator persist, before the
    /// durability barrier and the fence): no fence was written, so after
    /// power loss every redo entry must still be replayable and recovery
    /// must reproduce all acked mutations.
    #[test]
    fn crash_before_checkpoint_data_sync_keeps_all_mutations_replayable() {
        use crate::fault_injection::{self, FaultMode, SyncPoint};
        use crate::index::{DahBackend, PrimaryBackend, TxIndexEntry, UnminedBackend};
        use crate::recovery::recover_all_with_allocator;

        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("ckpt-crash.snap");

        let data_dev = Arc::new(MemoryDevice::new_volatile(16 * 1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(data_dev.clone() as Arc<dyn BlockDevice>).unwrap();
        let redo_dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
        let mut log = RedoLog::open(redo_dev.clone(), 0, 1024 * 1024).unwrap();

        let (key, record_offset, spending_data) =
            seed_acked_create_and_spend(&data_dev, &mut alloc, &mut log);

        let mut index = PrimaryBackend::new_in_memory(128).unwrap();
        index
            .register(
                key,
                TxIndexEntry {
                    device_id: 0,
                    record_offset,
                    utxo_count: 2,
                    block_entry_count: 0,
                    tx_flags: 0,
                    spent_utxos: 1,
                    dah_or_preserve: 0,
                    unmined_since: 0,
                    generation: 1,
                },
            )
            .unwrap();
        let engine = Engine::new(
            data_dev.clone() as Arc<dyn BlockDevice>,
            index,
            alloc,
            StripedLocks::new(16),
            DahIndex::new(),
            UnminedIndex::new(),
        );
        let redo = Mutex::new(log);
        let cfg = CheckpointConfig::new(snap_path);

        // Crash mid-checkpoint, before the barrier and the fence.
        fault_injection::arm(FaultMode::PanicAt(SyncPoint::BeforeCheckpointDataSync));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = perform_checkpoint(&cfg, &engine, &redo);
        }));
        fault_injection::disarm();
        assert!(
            result.is_err(),
            "checkpoint must have crashed at the sync point"
        );

        // Power loss on top of the crash.
        assert!(data_dev.simulate_power_loss(), "device must be volatile");

        // Restart. No fence was written, so the full redo log replays and
        // must reproduce both acked mutations regardless of which data
        // writes survived.
        let log2 = RedoLog::open(redo_dev, 0, 1024 * 1024).unwrap();
        let entries = log2.recover().unwrap();
        assert_eq!(
            entries.len(),
            2,
            "no fence may exist — both entries must still be replayable"
        );
        let mut alloc2 = SlotAllocator::recover(data_dev.clone() as Arc<dyn BlockDevice>)
            .expect("allocator persist (fsynced) preceded the crash point");
        let index2 =
            crate::index::ShardedIndex::from_single(PrimaryBackend::new_in_memory(128).unwrap());
        let mut dah_b = DahBackend::new_in_memory();
        let mut unmined_b = UnminedBackend::new_in_memory();
        recover_all_with_allocator(
            &*data_dev,
            &log2,
            &index2,
            &mut dah_b,
            &mut unmined_b,
            Some(&mut alloc2),
        )
        .expect("recovery must succeed");

        let entry = index2
            .lookup(&key)
            .expect("replay must re-register the record");
        assert_eq!(entry.record_offset, record_offset);
        let meta_after = crate::io::read_metadata(&*data_dev, record_offset).unwrap();
        assert_eq!({ meta_after.tx_id }, key.txid);
        assert_eq!({ meta_after.spent_utxos }, 1);
        let slot0 = crate::io::read_utxo_slot(&*data_dev, record_offset, 0).unwrap();
        assert!(slot0.is_spent(), "acked spend must be reproduced by replay");
        assert_eq!(slot0.spending_data, spending_data);
    }

    // -- Crash-injection regression tests (snapshot/reclaim ordering,
    //    allocator point-skew). These lock in the verified-correct
    //    durability ordering against future regression. --

    /// Restore the index + allocator from disk and replay the redo log
    /// after a (simulated) power loss, then return the recovered primary
    /// index so the caller can assert post-recovery state. Mirrors the
    /// restart sequence the other crash tests use: allocator from its
    /// header (or fresh on `NoPersistedState`), index from the snapshot
    /// (or fresh in-memory when no snapshot is durable), then redo replay.
    fn recover_after_crash(
        data_dev: &Arc<MemoryDevice>,
        redo_dev: &Arc<dyn BlockDevice>,
        snap_path: &std::path::Path,
        redo_capacity: u64,
    ) -> (
        crate::index::ShardedIndex,
        crate::allocator::SlotAllocator,
        usize,
    ) {
        use crate::index::{DahBackend, PrimaryBackend, ShardedIndex, UnminedBackend};
        use crate::recovery::recover_all_with_allocator;

        let mut alloc = match SlotAllocator::recover(data_dev.clone() as Arc<dyn BlockDevice>) {
            Ok(a) => a,
            Err(crate::allocator::AllocatorError::NoPersistedState) => {
                SlotAllocator::new(data_dev.clone() as Arc<dyn BlockDevice>).unwrap()
            }
            Err(e) => panic!("allocator recover failed unexpectedly: {e:?}"),
        };

        let (primary, dah, unmined) = if snap_path.exists() {
            let (idx, dah, unmined, _flags) =
                PrimaryBackend::restore_all(snap_path).expect("snapshot must restore");
            (idx, DahBackend::from(dah), UnminedBackend::from(unmined))
        } else {
            (
                PrimaryBackend::new_in_memory(128).unwrap(),
                DahBackend::new_in_memory(),
                UnminedBackend::new_in_memory(),
            )
        };
        let index = ShardedIndex::from_single(primary);
        let mut dah_b = dah;
        let mut unmined_b = unmined;

        let log = RedoLog::open(redo_dev.clone(), 0, redo_capacity).unwrap();
        let replayed = log.recover().unwrap().len();
        recover_all_with_allocator(
            &**data_dev,
            &log,
            &index,
            &mut dah_b,
            &mut unmined_b,
            Some(&mut alloc),
        )
        .expect("recovery must succeed");
        (index, alloc, replayed)
    }

    /// TEST 1 (regression lock — snapshot-durable-STRICTLY-before-reclaim).
    ///
    /// Crash inside the checkpoint at
    /// [`SyncPoint::AfterSnapshotRenameBeforeReclaim`]: the snapshot is
    /// renamed + parent-dir-fsynced, the durability barrier ran, and the
    /// recovery-progress fence is written, but `compact_prefix_through`
    /// (redo reclamation) NEVER runs. After power loss, EVERY acked
    /// mutation must survive: the durable snapshot carries them and the
    /// redo prefix is still intact too. The property: there is NO crash
    /// point between snapshot and reclaim that loses an acked write.
    #[test]
    fn snapshot_durable_strictly_before_redo_reclaim_loses_no_acked_write() {
        use crate::fault_injection::{self, FaultMode, SyncPoint};
        use crate::index::{PrimaryBackend, TxIndexEntry};

        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("snap-before-reclaim.snap");

        let data_dev = Arc::new(MemoryDevice::new_volatile(16 * 1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(data_dev.clone() as Arc<dyn BlockDevice>).unwrap();
        let redo_dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
        let mut log = RedoLog::open(redo_dev.clone(), 0, 1024 * 1024).unwrap();

        let (key, record_offset, spending_data) =
            seed_acked_create_and_spend(&data_dev, &mut alloc, &mut log);

        let mut index = PrimaryBackend::new_in_memory(128).unwrap();
        index
            .register(
                key,
                TxIndexEntry {
                    device_id: 0,
                    record_offset,
                    utxo_count: 2,
                    block_entry_count: 0,
                    tx_flags: 0,
                    spent_utxos: 1,
                    dah_or_preserve: 0,
                    unmined_since: 0,
                    generation: 1,
                },
            )
            .unwrap();
        let engine = Engine::new(
            data_dev.clone() as Arc<dyn BlockDevice>,
            index,
            alloc,
            StripedLocks::new(16),
            DahIndex::new(),
            UnminedIndex::new(),
        );
        let redo = Mutex::new(log);
        let cfg = CheckpointConfig::new(snap_path.clone());

        // Crash AFTER snapshot rename + barrier + fence, BEFORE reclaim.
        fault_injection::arm(FaultMode::PanicAt(
            SyncPoint::AfterSnapshotRenameBeforeReclaim,
        ));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = perform_checkpoint(&cfg, &engine, &redo);
        }));
        fault_injection::disarm();
        assert!(
            result.is_err(),
            "checkpoint must have crashed at the pre-reclaim sync point"
        );
        // The reclamation never ran, so the redo prefix is still on disk.
        // (We must drop the engine/redo handles to simulate the crash; the
        // Mutex<RedoLog> is consumed below by reopening from the device.)
        drop(redo);
        drop(engine);

        // Power loss on top of the crash: only synced data survives.
        assert!(data_dev.simulate_power_loss(), "device must be volatile");

        // Recover. The snapshot exists (rename completed pre-crash) AND the
        // redo prefix was never reclaimed — recovery from snapshot + replay
        // must reproduce both acked mutations regardless of which data
        // writes the volatile device dropped.
        let (index2, _alloc2, _replayed) =
            recover_after_crash(&data_dev, &redo_dev, &snap_path, 1024 * 1024);

        let entry = index2
            .lookup(&key)
            .expect("record must survive snapshot + replay");
        assert_eq!(entry.record_offset, record_offset);
        let meta_after = crate::io::read_metadata(&*data_dev, record_offset)
            .expect("record metadata must be durable");
        assert_eq!({ meta_after.tx_id }, key.txid);
        assert_eq!(
            { meta_after.spent_utxos },
            1,
            "acked spend count must survive a crash between snapshot and reclaim"
        );
        let slot0 = crate::io::read_utxo_slot(&*data_dev, record_offset, 0).unwrap();
        assert!(
            slot0.is_spent(),
            "acked spend must NOT silently revert when crashing before reclaim"
        );
        assert_eq!(slot0.spending_data, spending_data);
        let slot1 = crate::io::read_utxo_slot(&*data_dev, record_offset, 1).unwrap();
        assert!(slot1.is_unspent(), "untouched slot must stay unspent");
    }

    /// TEST 1 (variant — checkpoint COMPLETES, then power loss).
    ///
    /// The complementary half of the property: once the checkpoint runs to
    /// completion (snapshot durable AND redo prefix reclaimed), the
    /// snapshot ALONE must carry every acked mutation across a power loss —
    /// the redo prefix it relied on is now gone. Together with the crash
    /// variant above, this proves there is no crash point in the
    /// snapshot→reclaim window that loses an acked write.
    #[test]
    fn completed_checkpoint_snapshot_alone_carries_acked_writes_after_power_loss() {
        use crate::index::{PrimaryBackend, TxIndexEntry};

        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("snap-complete.snap");

        let data_dev = Arc::new(MemoryDevice::new_volatile(16 * 1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(data_dev.clone() as Arc<dyn BlockDevice>).unwrap();
        let redo_dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
        let mut log = RedoLog::open(redo_dev.clone(), 0, 1024 * 1024).unwrap();

        let (key, record_offset, spending_data) =
            seed_acked_create_and_spend(&data_dev, &mut alloc, &mut log);

        let mut index = PrimaryBackend::new_in_memory(128).unwrap();
        index
            .register(
                key,
                TxIndexEntry {
                    device_id: 0,
                    record_offset,
                    utxo_count: 2,
                    block_entry_count: 0,
                    tx_flags: 0,
                    spent_utxos: 1,
                    dah_or_preserve: 0,
                    unmined_since: 0,
                    generation: 1,
                },
            )
            .unwrap();
        let engine = Engine::new(
            data_dev.clone() as Arc<dyn BlockDevice>,
            index,
            alloc,
            StripedLocks::new(16),
            DahIndex::new(),
            UnminedIndex::new(),
        );
        let redo = Mutex::new(log);
        let cfg = CheckpointConfig::new(snap_path.clone());

        let stats = perform_checkpoint(&cfg, &engine, &redo).expect("checkpoint must succeed");
        assert!(stats.reset_performed, "redo prefix must be reclaimed");
        drop(redo);
        drop(engine);

        // Power loss: the reclaimed redo prefix is gone; only the durable
        // snapshot + barriered data writes remain.
        assert!(data_dev.simulate_power_loss(), "device must be volatile");

        let (index2, _alloc2, replayed) =
            recover_after_crash(&data_dev, &redo_dev, &snap_path, 1024 * 1024);
        assert_eq!(
            replayed, 0,
            "the fence + reclaim must leave zero redo entries to replay"
        );

        let entry = index2
            .lookup(&key)
            .expect("snapshot alone must carry the record");
        assert_eq!(entry.record_offset, record_offset);
        let slot0 = crate::io::read_utxo_slot(&*data_dev, record_offset, 0).unwrap();
        assert!(
            slot0.is_spent(),
            "snapshot alone must carry the acked spend after reclaim + power loss"
        );
        assert_eq!(slot0.spending_data, spending_data);
        let meta_after = crate::io::read_metadata(&*data_dev, record_offset).unwrap();
        assert_eq!({ meta_after.spent_utxos }, 1);
    }

    /// TEST 2 (snapshot/allocator point-skew — item #4).
    ///
    /// `persist_allocator` fails AFTER the snapshot has already been
    /// renamed but BEFORE the recovery-progress fence is written, so the
    /// checkpoint returns `Err` with no fence. After power loss, recovery
    /// runs the full redo replay (no fence ⇒ nothing is skipped), which
    /// must self-heal: re-derive the freelist from the `AllocateRegion`
    /// redo entry and reproduce the acked CREATE + SPEND. Assert: (a) no
    /// acked mutation is lost, and (b) the region the CREATE allocated is
    /// NEITHER double-allocatable NOR aliased — the index entry's offset
    /// and the recovered allocator's high-water mark agree, so a fresh
    /// allocation can never hand back the live record's region.
    #[test]
    fn allocator_persist_skew_after_snapshot_self_heals_via_redo_replay() {
        use crate::index::{PrimaryBackend, TxIndexEntry};

        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("alloc-skew.snap");

        let data_dev = Arc::new(MemoryDevice::new_volatile(16 * 1024 * 1024, 4096).unwrap());
        let redo_dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
        let redo = Arc::new(Mutex::new(
            RedoLog::open(redo_dev.clone(), 0, 1024 * 1024).unwrap(),
        ));

        let mut alloc = SlotAllocator::new(data_dev.clone() as Arc<dyn BlockDevice>).unwrap();

        // Seed an acked CREATE + SPEND. We journal the redo stream in the
        // SAME order production does — `AllocateRegion` FIRST (so recovery's
        // `is_allocated_range` gate on `CreateV2` passes during replay),
        // then `CreateV2`, then `SpendV2` — driving the log directly through
        // a `&mut RedoLog` (matching the recovery harness's allocate-region
        // tests). We deliberately do NOT attach the log to the allocator
        // here: `alloc.allocate` would re-lock the same `Arc<Mutex<RedoLog>>`
        // we already hold, deadlocking. The on-device record bytes are
        // written to the volatile cache (no sync) so power loss drops them,
        // forcing the redo replay to do the real reconstruction work.
        let (key, region_r, record_size, spending_data) = {
            use crate::record::{TxMetadata, UtxoSlot};
            let mut log = redo.lock();

            let mut txid = [0u8; 32];
            txid[0] = 0xA2;
            let key = crate::index::TxKey { txid };
            let utxo_count = 2u32;
            let record_size = TxMetadata::record_size_for(utxo_count);

            // Reserve region R via the allocator (no redo attached → no
            // journal here), then journal AllocateRegion ourselves first.
            // The journaled size is the allocator's alignment-rounded size
            // (4 KiB device alignment) so the replayed high-water mark
            // covers the full reserved extent.
            let region_r = alloc.allocate(record_size).unwrap();
            let aligned_size = record_size.div_ceil(4096) * 4096;
            log.append_and_flush(RedoOp::AllocateRegion {
                offset: region_r,
                size: aligned_size,
                device_id: 0,
            })
            .unwrap();

            let mut meta = TxMetadata::new(utxo_count);
            meta.tx_id = txid;
            meta.record_size = record_size as u32;
            let slots: Vec<UtxoSlot> = (0..utxo_count)
                .map(|i| {
                    let mut h = [0u8; 32];
                    h[0] = i as u8 + 1;
                    UtxoSlot::new_unspent(h)
                })
                .collect();

            let mut record_bytes = Vec::with_capacity(
                crate::record::METADATA_SIZE + slots.len() * crate::record::UTXO_SLOT_SIZE,
            );
            let mut meta_bytes = [0u8; crate::record::METADATA_SIZE];
            meta.to_bytes(&mut meta_bytes);
            record_bytes.extend_from_slice(&meta_bytes);
            for slot in &slots {
                let mut sb = [0u8; crate::record::UTXO_SLOT_SIZE];
                slot.to_bytes(&mut sb);
                record_bytes.extend_from_slice(&sb);
            }
            log.append_and_flush(RedoOp::CreateV2 {
                tx_key: key,
                record_offset: region_r,
                utxo_count,
                is_conflicting: false,
                record_bytes,
                parent_txids: Vec::new(),
            })
            .unwrap();
            crate::io::write_full_record(&*data_dev, region_r, &meta, &slots).unwrap();

            let mut spending_data = [0u8; 36];
            spending_data[..4].copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
            log.append_and_flush(RedoOp::SpendV2 {
                tx_key: key,
                offset: 0,
                spending_data,
                new_spent_count: 1,
                current_block_height: 0,
                block_height_retention: 0,
                target_generation: 1,
                updated_at: 0,
                utxo_hash: None,
            })
            .unwrap();
            let spent = UtxoSlot::new_spent(slots[0].hash, spending_data);
            crate::io::write_utxo_slot(&*data_dev, region_r, 0, &spent).unwrap();
            meta.spent_utxos = 1;
            crate::io::write_metadata(&*data_dev, region_r, &meta).unwrap();

            (key, region_r, record_size, spending_data)
        };
        let record_offset = region_r;
        assert!(region_r >= crate::allocator::DATA_REGION_OFFSET);

        // Make the record's data-device bytes durable, modelling the
        // realistic precondition that the acked CREATE+SPEND were already
        // flushed to the data device by an EARLIER successful checkpoint
        // (step-3 `device.sync()`). Without this the test would also be
        // exercising "create record bytes never synced" — a different,
        // WAL-replay concern — instead of isolating the allocator
        // point-skew. The allocator HEADER is deliberately left non-durable
        // (persist will fail below), so this sync isolates exactly the skew:
        // record bytes durable, allocator header stale/absent.
        data_dev.sync().unwrap();

        let mut index = PrimaryBackend::new_in_memory(128).unwrap();
        index
            .register(
                key,
                TxIndexEntry {
                    device_id: 0,
                    record_offset,
                    utxo_count: 2,
                    block_entry_count: 0,
                    tx_flags: 0,
                    spent_utxos: 1,
                    dah_or_preserve: 0,
                    unmined_since: 0,
                    generation: 1,
                },
            )
            .unwrap();
        let engine = Engine::new(
            data_dev.clone() as Arc<dyn BlockDevice>,
            index,
            alloc,
            StripedLocks::new(16),
            DahIndex::new(),
            UnminedIndex::new(),
        );

        // Arm the allocator to fail its next persist. The checkpoint will
        // have already renamed the snapshot (step 1) before reaching
        // persist_allocator (step 2), so the failure lands in the skew
        // window: snapshot durable, NO fence written.
        engine.allocator().lock().arm_fail_next_persist();
        let cfg = CheckpointConfig::new(snap_path.clone());
        let err = perform_checkpoint(&cfg, &engine, &redo)
            .expect_err("checkpoint must abort when persist_allocator fails");
        assert!(
            err.contains("persist_allocator"),
            "error must name the failing step, got: {err}"
        );
        // No fence was written: the full redo log must still be replayable.
        {
            let log = redo.lock();
            assert!(
                !log.recover().unwrap().is_empty(),
                "aborted checkpoint must leave the redo log fully replayable"
            );
        }
        // The snapshot was already renamed before the failure.
        assert!(
            snap_path.exists(),
            "snapshot rename precedes persist_allocator — file must exist"
        );

        drop(engine);
        drop(redo);

        // Power loss: drop volatile data writes + the (failed) header.
        assert!(data_dev.simulate_power_loss(), "device must be volatile");

        // Recover. No durable allocator header survived the power loss
        // (persist failed), so SlotAllocator::recover returns
        // NoPersistedState and we start fresh; the full redo replay then
        // re-derives the freelist/high-water from AllocateRegion AND
        // reproduces the CREATE + SPEND.
        let (index2, alloc2, replayed) =
            recover_after_crash(&data_dev, &redo_dev, &snap_path, 1024 * 1024);
        assert!(
            replayed > 0,
            "no fence ⇒ the full redo prefix must be replayed"
        );

        // (a) No acked mutation lost.
        let entry = index2
            .lookup(&key)
            .expect("replay must re-register the record");
        assert_eq!(entry.record_offset, region_r);
        let slot0 = crate::io::read_utxo_slot(&*data_dev, region_r, 0).unwrap();
        assert!(slot0.is_spent(), "acked spend must be reproduced by replay");
        assert_eq!(slot0.spending_data, spending_data);

        // (b) Region R is NEITHER double-allocatable NOR aliased: a fresh
        // allocation after recovery must hand back an offset STRICTLY
        // beyond R's extent (or carved from a freelist hole that does not
        // overlap R). The allocator's high-water mark must cover R, so the
        // next bump-allocation cannot return R again.
        let mut alloc2 = alloc2;
        let fresh = alloc2
            .allocate(record_size)
            .expect("post-recovery allocation must succeed");
        let r_end = region_r + record_size;
        let fresh_end = fresh + record_size;
        assert!(
            fresh + record_size <= region_r || fresh >= r_end,
            "fresh allocation {fresh}..{fresh_end} must NOT overlap live region R {region_r}..{r_end} (no double-alloc / aliasing)"
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
