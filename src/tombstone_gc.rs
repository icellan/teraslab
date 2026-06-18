//! Phase 5 — bounded-retention tombstone garbage-collection daemon
//! (deletion-tombstone design §4).
//!
//! A periodic background task (sibling to the redo-log checkpoint task in
//! [`crate::checkpoint`]) that, each tick and ONLY when `tombstone_gc_enabled`,
//! advances the GC horizon and reclaims tombstones whose `deletion_height` is
//! safely below it.
//!
//! # The horizon rule (§4.2)
//!
//! A tombstone with `deletion_height = h` is safe to GC once
//! `min_member_finalized_height − h ≥ rejoin_grace_blocks`, i.e. once
//! `h < safe_height` where `safe_height = min_member_finalized_height −
//! rejoin_grace_blocks`. The minimum finalized height is taken across ALL
//! current committed members (self included); a single unreachable member
//! makes it `None`, in which case this round is SKIPPED — the horizon never
//! advances on incomplete information (§4.6).
//!
//! # Coupling to the rejoin gate (§4.3)
//!
//! GC and the Phase 4 rejoin-eligibility gate
//! ([`crate::cluster::coordinator::rejoin_gate_decision`]) share the SAME
//! `rejoin_grace_blocks` bound. That shared bound is the entire load-bearing
//! coupling: any node stale enough to still need a tombstone this daemon would
//! drop is, by the §4.3 proof, too stale to be admitted incrementally and is
//! instead full-resynced (discarding its stale copy). Therefore GC can never
//! drop a tombstone a still-incrementally-admissible laggard needs.
//!
//! # Crash-safety / ordering (§4.6)
//!
//! Each round deletes the derived redb tombstone rows below `safe_height`
//! FIRST, then advances the on-device log's `compacted_through_height` and
//! reclaims the prefix. The on-device log is the durable source of truth and
//! its `compact_through` is itself crash-safe (it fsyncs the advanced header
//! before reclaiming the prefix). The redb index is DERIVED and rebuilt from
//! the log on recovery, so a crash BETWEEN the redb delete and the log compact
//! is harmless: recovery re-derives the index from the surviving log suffix,
//! and the dropped prefix was already proven safe by the horizon. Doing the
//! redb delete first (rather than after) means a crash in between leaves the
//! index momentarily ahead of the log — also harmless, since the next round
//! (or recovery's rebuild) reconciles it. Either order is sound; this order is
//! chosen so the durable watermark (the log header) advances LAST.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::cluster::coordinator::RunningCluster;
use crate::ops::engine::Engine;

/// Configuration for the tombstone-GC daemon.
#[derive(Debug, Clone, Copy)]
pub struct TombstoneGcConfig {
    /// Master switch (deletion-tombstone design §11.5). When `false` the
    /// daemon still ticks but performs NO work — no horizon query, no
    /// range-delete, no log compaction. Default-off is the byte-identical
    /// pre-Phase-5 behavior.
    pub enabled: bool,
    /// The shared staleness bound (§4.2/§4.5) used to derive the safe horizon.
    pub rejoin_grace_blocks: u32,
    /// Cadence at which the daemon evaluates the horizon.
    pub poll_interval: Duration,
}

/// What a single GC round did, returned by [`perform_gc_round`] for logging
/// and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcRoundOutcome {
    /// The feature is disabled — the round did nothing.
    Disabled,
    /// `min_member_finalized_height` was `None` (a committed member was
    /// unreachable): the round was SKIPPED to avoid GC on incomplete info
    /// (§4.6). Carries nothing because there is no safe height.
    SkippedIncompleteMembership,
    /// The tombstone log / index are not attached (feature inert this round).
    NoTombstoneStore,
    /// `safe_height` resolved to 0 — nothing is strictly below it, so there is
    /// nothing to reclaim. Carries the `min_height` that produced it.
    NothingToReclaim { min_height: u32 },
    /// A reclaim ran. Carries the derived `safe_height`, the number of redb
    /// rows removed, and whether the log compaction advanced the watermark.
    Reclaimed {
        safe_height: u32,
        rows_removed: usize,
    },
}

/// Compute the GC safe horizon from the minimum committed-member finalized
/// height and the shared grace bound (deletion-tombstone design §4.2).
///
/// `safe_height = min_height − rejoin_grace_blocks` (saturating). A tombstone
/// is reclaimable iff its `deletion_height < safe_height`. Returns `0` when
/// the grace bound meets or exceeds `min_height` — i.e. nothing is yet old
/// enough to reclaim. Pure and exhaustively unit-testable.
pub fn gc_safe_height(min_height: u32, rejoin_grace_blocks: u32) -> u32 {
    min_height.saturating_sub(rejoin_grace_blocks)
}

/// Run one GC round against `engine`'s tombstone store.
///
/// `min_height` is the result of
/// [`RunningCluster::min_member_finalized_height`] (passed in so this function
/// is testable without a live cluster): `None` means a committed member was
/// unreachable and the round must be skipped (§4.6).
///
/// Steps (when enabled, store attached, and `safe_height > 0`):
/// 1. `redb_tombstone_index.range_delete_below_height(safe_height)`.
/// 2. `tombstone_log.compact_through(safe_height)` — advances the durable
///    `compacted_through_height` watermark and reclaims the log prefix.
///
/// See the module docs for the redb-then-log ordering and crash-safety.
///
/// Returns a [`GcRoundOutcome`] describing what happened. A redb or log error
/// is logged and the round returns the best outcome it reached (it never
/// panics and never propagates the error: GC is best-effort background work,
/// and the durable watermark only advances after a successful redb delete +
/// log compact).
pub fn perform_gc_round(
    engine: &Engine,
    enabled: bool,
    min_height: Option<u32>,
    rejoin_grace_blocks: u32,
) -> GcRoundOutcome {
    if !enabled {
        return GcRoundOutcome::Disabled;
    }
    let Some(min_height) = min_height else {
        tracing::debug!(
            "tombstone GC: min member finalized height unavailable — skipping round \
             (conservative §4.6: never GC on incomplete membership info)",
        );
        return GcRoundOutcome::SkippedIncompleteMembership;
    };

    let (Some(log), Some(index)) = (engine.tombstone_log(), engine.tombstone_index()) else {
        // No tombstone store attached (tombstones disabled / unconfigured):
        // nothing to GC. Inert.
        return GcRoundOutcome::NoTombstoneStore;
    };

    let safe_height = gc_safe_height(min_height, rejoin_grace_blocks);
    if safe_height == 0 {
        return GcRoundOutcome::NothingToReclaim { min_height };
    }

    // Step 1: delete the DERIVED redb rows below the horizon first.
    let rows_removed = match index.lock().range_delete_below_height(safe_height) {
        Ok(n) => n,
        Err(e) => {
            tracing::warn!(
                err = %e,
                safe_height,
                "tombstone GC: redb range-delete failed; skipping log compaction this round",
            );
            // Do NOT advance the durable log watermark if the index delete
            // failed — keep the two consistent on the next round.
            return GcRoundOutcome::Reclaimed {
                safe_height,
                rows_removed: 0,
            };
        }
    };

    // Step 2: advance the durable log watermark and reclaim the prefix. This
    // is itself crash-safe (header fsynced before prefix reclamation).
    if let Err(e) = log.lock().compact_through(safe_height) {
        tracing::warn!(
            err = %e,
            safe_height,
            rows_removed,
            "tombstone GC: log compaction failed after redb range-delete; the redb \
             index is derived and will be reconciled next round / on recovery",
        );
    }

    GcRoundOutcome::Reclaimed {
        safe_height,
        rows_removed,
    }
}

/// Spawn the tombstone-GC daemon thread (deletion-tombstone design §4.6).
///
/// Mirrors [`crate::checkpoint::spawn_checkpoint_task`]: a named OS thread
/// running [`run_gc_loop`] until `shutdown` is set. The thread is cheap when
/// disabled (it wakes on cadence, checks the flag, and returns immediately),
/// so it is always spawned and gated internally on `config.enabled`.
pub fn spawn_tombstone_gc_task(
    config: TombstoneGcConfig,
    engine: Arc<Engine>,
    cluster: Arc<RunningCluster>,
    shutdown: Arc<AtomicBool>,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("teraslab-tombstone-gc".to_string())
        .spawn(move || run_gc_loop(config, engine, cluster, shutdown))
        .expect("spawn tombstone-gc thread")
}

/// Body of the GC thread, factored out for direct testing.
fn run_gc_loop(
    config: TombstoneGcConfig,
    engine: Arc<Engine>,
    cluster: Arc<RunningCluster>,
    shutdown: Arc<AtomicBool>,
) {
    tracing::info!(
        enabled = config.enabled,
        rejoin_grace_blocks = config.rejoin_grace_blocks,
        poll_interval_ms = config.poll_interval.as_millis() as u64,
        "tombstone GC daemon started",
    );

    while !shutdown.load(Ordering::Relaxed) {
        // Sleep the cadence in small slices so shutdown is observed promptly.
        if !sleep_with_shutdown(config.poll_interval, &shutdown, &config.poll_interval) {
            break;
        }

        if !config.enabled {
            continue;
        }

        // Read membership from the COMMITTED view (§4.6) via the coordinator;
        // self's height comes from the engine (no self-loopback RPC).
        let self_height = engine.last_durable_height();
        let min_height = cluster.min_member_finalized_height(self_height);

        match perform_gc_round(
            &engine,
            config.enabled,
            min_height,
            config.rejoin_grace_blocks,
        ) {
            GcRoundOutcome::Reclaimed {
                safe_height,
                rows_removed,
            } => {
                tracing::info!(
                    safe_height,
                    rows_removed,
                    self_height,
                    "tombstone GC: reclaimed tombstones below horizon",
                );
            }
            GcRoundOutcome::NothingToReclaim { min_height } => {
                tracing::debug!(
                    min_height,
                    self_height,
                    "tombstone GC: horizon below 0 — nothing to reclaim this round",
                );
            }
            GcRoundOutcome::SkippedIncompleteMembership => {
                // Already logged at debug inside perform_gc_round.
            }
            GcRoundOutcome::NoTombstoneStore | GcRoundOutcome::Disabled => {}
        }
    }
    tracing::info!("tombstone GC daemon exiting");
}

/// Sleep for `total`, polling `shutdown` every `slice`. Returns `false` if
/// shutdown was observed (caller should break), `true` if the full duration
/// elapsed. Mirrors [`crate::checkpoint`]'s helper of the same shape.
fn sleep_with_shutdown(total: Duration, shutdown: &AtomicBool, slice: &Duration) -> bool {
    let slice = (*slice)
        .min(Duration::from_millis(100))
        .max(Duration::from_millis(1));
    let mut waited = Duration::ZERO;
    while waited < total {
        if shutdown.load(Ordering::Relaxed) {
            return false;
        }
        let step = slice.min(total - waited);
        std::thread::sleep(step);
        waited += step;
    }
    !shutdown.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::redb_tombstone::RedbTombstoneIndex;
    use crate::index::{Index, TxKey};
    use crate::tombstone::{Tombstone, TombstoneCause, TombstoneLog};

    // -- pure horizon math ---------------------------------------------------

    #[test]
    fn gc_safe_height_subtracts_grace_saturating() {
        assert_eq!(gc_safe_height(800_000, 100_000), 700_000);
        // grace == min → 0 (nothing old enough).
        assert_eq!(gc_safe_height(100_000, 100_000), 0);
        // grace > min → saturates to 0.
        assert_eq!(gc_safe_height(50_000, 100_000), 0);
        // grace 0 → safe height == min (reclaim everything strictly below min).
        assert_eq!(gc_safe_height(800_000, 0), 800_000);
    }

    // -- perform_gc_round gating --------------------------------------------

    fn empty_engine() -> Arc<Engine> {
        let dev: Arc<dyn crate::device::BlockDevice> =
            Arc::new(crate::device::MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let alloc = crate::allocator::SlotAllocator::new(dev.clone()).unwrap();
        let index = Index::new(1000).unwrap();
        Arc::new(Engine::new(
            dev,
            index,
            alloc,
            crate::locks::StripedLocks::new(1024),
            crate::index::DahIndex::new(),
            crate::index::UnminedIndex::new(),
        ))
    }

    /// Build an engine with a real on-device tombstone log + redb index
    /// attached, seeded with tombstones at the given `(txid_byte, height)`.
    fn engine_with_tombstones(seed: &[(u8, u32)]) -> (Arc<Engine>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = empty_engine();

        // On-device tombstone log in its own memory device region.
        let log_dev: Arc<dyn crate::device::BlockDevice> =
            Arc::new(crate::device::MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap());
        let mut log = TombstoneLog::create(log_dev, 0, 16 * 1024 * 1024).unwrap();

        let mut index = RedbTombstoneIndex::open(&dir.path().join("tomb.redb"), 0).unwrap();

        for (b, height) in seed {
            let mut txid = [0u8; 32];
            txid[0] = *b;
            let key = TxKey { txid };
            let shard = crate::cluster::shards::ShardTable::shard_for_key(&key);
            let ts = Tombstone::new(txid, shard, *height, 1, TombstoneCause::SpentDah, 0);
            log.append_synced(&ts).unwrap();
            index
                .insert(key, *height, 1, shard, TombstoneCause::SpentDah.as_u8())
                .unwrap();
        }

        engine.set_tombstone_log(Arc::new(parking_lot::Mutex::new(log)));
        engine.set_tombstone_index(Arc::new(parking_lot::Mutex::new(index)));
        (engine, dir)
    }

    #[test]
    fn perform_gc_round_disabled_is_noop() {
        let (engine, _dir) = engine_with_tombstones(&[(1, 100), (2, 200)]);
        let out = perform_gc_round(&engine, false, Some(1_000_000), 0);
        assert_eq!(out, GcRoundOutcome::Disabled);
        // Nothing removed.
        assert_eq!(engine.tombstone_index().unwrap().lock().len(), 2);
    }

    #[test]
    fn perform_gc_round_skips_on_none_min_height() {
        let (engine, _dir) = engine_with_tombstones(&[(1, 100), (2, 200)]);
        let out = perform_gc_round(&engine, true, None, 0);
        assert_eq!(out, GcRoundOutcome::SkippedIncompleteMembership);
        assert_eq!(engine.tombstone_index().unwrap().lock().len(), 2);
    }

    #[test]
    fn perform_gc_round_nothing_to_reclaim_when_safe_height_zero() {
        let (engine, _dir) = engine_with_tombstones(&[(1, 100)]);
        // min_height (50_000) - grace (100_000) saturates to 0.
        let out = perform_gc_round(&engine, true, Some(50_000), 100_000);
        assert_eq!(out, GcRoundOutcome::NothingToReclaim { min_height: 50_000 });
        assert_eq!(engine.tombstone_index().unwrap().lock().len(), 1);
    }

    #[test]
    fn perform_gc_round_reclaims_only_below_safe_height() {
        // Tombstones at heights 100, 200, 300. min=1000, grace=750 →
        // safe_height = 250 → reclaim heights 100 and 200, keep 300.
        let (engine, _dir) = engine_with_tombstones(&[(1, 100), (2, 200), (3, 300)]);
        let out = perform_gc_round(&engine, true, Some(1000), 750);
        assert_eq!(
            out,
            GcRoundOutcome::Reclaimed {
                safe_height: 250,
                rows_removed: 2,
            }
        );
        let idx = engine.tombstone_index().unwrap();
        assert_eq!(idx.lock().len(), 1, "only height 300 should remain");

        // The log watermark advanced to the safe height (durable).
        assert_eq!(
            engine
                .tombstone_log()
                .unwrap()
                .lock()
                .compacted_through_height(),
            250
        );
    }

    #[test]
    fn perform_gc_round_no_store_is_inert() {
        // Engine with NO tombstone log/index attached.
        let engine = empty_engine();
        let out = perform_gc_round(&engine, true, Some(1_000_000), 0);
        assert_eq!(out, GcRoundOutcome::NoTombstoneStore);
    }

    #[test]
    fn sleep_with_shutdown_returns_early_on_flag() {
        let flag = AtomicBool::new(true);
        // Already-set flag → returns false immediately without sleeping long.
        let start = std::time::Instant::now();
        let completed =
            sleep_with_shutdown(Duration::from_secs(10), &flag, &Duration::from_millis(10));
        assert!(!completed);
        assert!(start.elapsed() < Duration::from_secs(1));
    }
}
