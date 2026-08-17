//! In-RAM per-shard recency cache backing the cluster partition-version
//! report (W10 FIX 1).
//!
//! The exchange-phase report used to recompute every participating shard's
//! reverse-heal recency fingerprint — a full index walk plus a PER-KEY
//! on-device `read_metadata` — inside every `OP_PARTITION_VERSION_REPORT`
//! dispatch and in the exchange's own self-report. On a seeded store that
//! scan takes multiple seconds, so every peer query hit the frame read
//! timeout and every post-seed exchange collected a starved partial view
//! (CI @ d3437e4: term-1 exchanges on empty engines complete in ~200 ms
//! with full views; EVERY post-seed exchange timed out its peers).
//!
//! This cache makes report building O(participating shards) over RAM:
//!
//! - Serving reads the last PUBLISHED snapshot — zero device reads, zero
//!   index walks. The per-shard live record count is NOT cached (the
//!   engine's O(1) `shard_record_count` counters stay authoritative).
//! - A snapshot is (re)computed OFF the serving path by
//!   [`crate::ops::engine::Engine::refresh_shard_recency_cache`], which
//!   runs the same one-filtered-index-scan + per-key footer read the old
//!   inline path ran — but on a refresh thread, never inside a dispatch
//!   handler or the exchange window, and paced by
//!   [`RECENCY_REFRESH_MIN_INTERVAL`] so a busy store never scans
//!   back-to-back.
//! - Staleness is tracked by a mutation stamp
//!   ([`ShardRecencyCache::note_mutation`]) bumped by every engine path
//!   that can change any record's `(txid, generation)` membership:
//!   primary-index register/unregister (create/delete) and every
//!   metadata persist (in-place footer writes, direct-memory footer
//!   writes, and log-structured relocation). Refresh captures the stamp
//!   BEFORE scanning, so a mutation racing the scan leaves the snapshot
//!   stale and the next trigger refreshes again.
//!
//! # Serving semantics — staleness vs. UNKNOWN
//!
//! A served entry is the shard's honest recency AS OF the last completed
//! refresh (plus the always-live record count). Consumers split in two:
//!
//! - The MISMATCH pre-filter (reverse-heal Tier-2 detection) and the
//!   authoritative Phase-2 per-record manifest exchange tolerate bounded
//!   staleness: a wrong digest costs a no-op confirm, never a wrong heal,
//!   and the value was already live-moving across responders.
//! - The Phase-3b online-reheal DIRECTION gate is DESTRUCTIVE: a served
//!   `max_generation` LOWER than reality makes
//!   `is_self_behind_any_replica_coarse` flag this node behind, and
//!   `trigger_online_reheal` then FENCES the shard (`heal_pending` →
//!   `Transitioning` — keyspace unavailable) and queues a baseline pull.
//!   Bounded staleness on a SCANNED shard is still tolerable there (the
//!   direction gate also reads the live count, and the fence resolves via
//!   the pull); what is NOT tolerable is FABRICATED emptiness — serving
//!   `digest-of-empty / max_generation 0` for a shard that demonstrably
//!   holds records. A node booting WITH data would report exactly that
//!   for every held shard until its first whole-store scan completes, and
//!   every mastered shard would be suspected, fenced, and pulled on each
//!   rolling restart.
//!
//! The cache therefore distinguishes NO EVIDENCE from evidence-of-empty:
//! a shard that has never been scanned since boot ([`CachedShardRecency::Unscanned`]),
//! or that was scanned while empty but now shows a non-zero live count,
//! resolves as RECENCY-UNKNOWN ([`CachedShardRecency::resolve`]). The
//! report builder surfaces that as the wire flag
//! `PARTITION_FLAG_RECENCY_UNKNOWN`, and the reverse-heal consumers treat
//! an UNKNOWN side as carrying NO recency evidence: no SUSPECT flag, no
//! direction verdict, no `max_generation` ranking — the same "no evidence
//! is not evidence of divergence" posture as the F1 honest-absence rule.
//!
//! A genuinely empty engine is exact from construction: every shard's
//! scan-of-empty fingerprint equals [`empty_shard_recency`], and the
//! zero live count keeps the empty-but-counted case from arising.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// W10 P1-1 — minimum interval between two COMPLETED recency scans.
///
/// The refresh gate used to be only `is_stale && claim_slot`; under any
/// write load `is_stale` is permanently true, so the whole-store scans
/// (one index walk + ~2 device reads per record) ran BACK-TO-BACK forever,
/// kicked from every exchange, every report dispatch, and the parked-fence
/// reactivation loop. This floor bounds steady-state refresh I/O to at
/// most one whole-store scan per interval regardless of trigger rate.
///
/// Sizing against the exchange cadences: commit-triggered exchanges are
/// bursty (two per term within ~2 s — both serve one snapshot), the
/// re-query cadence is 500 ms within a 2 s window (same snapshot is fine —
/// peers need ANSWERS, not per-query freshness), and the re-heal /
/// reactivation cooldown is 15-30 s. A 5 s floor therefore refreshes well
/// inside every re-heal round while collapsing per-term trigger bursts
/// into one scan.
pub const RECENCY_REFRESH_MIN_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// A shard's cached recency fingerprint: the `(manifest_digest,
/// max_generation)` pair of [`crate::cluster::coordinator::ShardRecency`],
/// minus the live record count (served from the engine's O(1) per-shard
/// counters instead — never cached, never stale).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShardRecencyValue {
    /// Order-independent 64-bit `(txid, generation)` set fingerprint, as
    /// produced by `Engine::recency_for_keys`.
    pub digest: u64,
    /// Maximum record generation under wrapping-serial ordering (0 when
    /// the shard held no readable record at snapshot time).
    pub max_generation: u32,
}

/// One shard's cached scan state (W10 P0-1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CachedShardRecency {
    /// The shard has never been scanned since boot — the cache holds NO
    /// evidence about its content. Resolves as recency-UNKNOWN.
    Unscanned,
    /// The shard was scanned by a completed refresh.
    Scanned {
        /// The fingerprint the scan produced.
        value: ShardRecencyValue,
        /// Whether the scan folded at least one READABLE record. `false`
        /// covers both a genuinely empty shard and one whose every record
        /// footer was unreadable — in either case, a non-zero live count
        /// at serve time means the fingerprint is not evidence about the
        /// records now present, and the shard resolves UNKNOWN.
        had_records: bool,
    },
}

impl CachedShardRecency {
    /// Resolve the cached state against the LIVE record count into the
    /// `(fingerprint, recency_unknown)` pair the report serves.
    ///
    /// UNKNOWN iff records exist NOW (`live_count > 0`) and no scan has
    /// evidenced any record: never scanned, or scanned with no readable
    /// records (the boot-with-data, empty→populated, and all-unreadable
    /// cases). Two deliberate KNOWN edges:
    ///
    /// - `live_count == 0` is always KNOWN-empty regardless of scan
    ///   history — the live count is exact (maintained under the shard
    ///   write locks), and an empty shard's true fingerprint IS
    ///   [`empty_shard_recency`]; nothing is fabricated. This keeps a
    ///   genuinely-emptied master detectable as behind a populated
    ///   replica even before its first scan.
    /// - A scanned shard whose count merely CHANGED since the scan stays
    ///   KNOWN — that is ordinary bounded staleness, resolved by the next
    ///   paced refresh and arbitrated by the authoritative confirm.
    pub fn resolve(self, live_count: u64) -> (ShardRecencyValue, bool) {
        match self {
            CachedShardRecency::Unscanned => (empty_shard_recency(), live_count > 0),
            CachedShardRecency::Scanned { value, had_records } => {
                (value, !had_records && live_count > 0)
            }
        }
    }
}

/// The recency fingerprint of an EMPTY shard: what
/// `Engine::recency_for_keys(&[])` returns — the `ManifestHasher` digest
/// of zero folded entries and generation 0. Every node computes this
/// identically, so an empty shard's cached entry always matches a peer's
/// scanned entry byte-for-byte.
pub fn empty_shard_recency() -> ShardRecencyValue {
    // ManifestHasher::finalize over zero entries = SHA-256 of the empty
    // buffer, truncated to the low 8 bytes — computed once and memoized.
    static EMPTY_DIGEST: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    let digest = *EMPTY_DIGEST.get_or_init(|| {
        let full = crate::cluster::coordinator::ManifestHasher::new().finalize();
        u64::from_le_bytes([
            full[0], full[1], full[2], full[3], full[4], full[5], full[6], full[7],
        ])
    });
    ShardRecencyValue {
        digest,
        max_generation: 0,
    }
}

/// The published snapshot: one [`CachedShardRecency`] per shard, plus the
/// mutation stamp observed BEFORE the scan that produced it began.
struct RecencySnapshot {
    /// [`ShardRecencyCache::mutation_stamp`] loaded before the producing
    /// scan started. `0` = the initial never-refreshed snapshot (the
    /// stamp starts at 1, so an initial snapshot is always stale).
    stamp: u64,
    /// Indexed by shard number; length [`crate::cluster::shards::NUM_SHARDS`].
    per_shard: Vec<CachedShardRecency>,
}

/// Read guard over the published snapshot (W10 P2-7): the report builder
/// iterates every shard, so it takes the snapshot lock ONCE through this
/// instead of once per shard.
pub struct SnapshotReader<'a> {
    guard: parking_lot::RwLockReadGuard<'a, RecencySnapshot>,
}

impl SnapshotReader<'_> {
    /// The cached state for `shard`. Out-of-range shard numbers resolve
    /// as [`CachedShardRecency::Unscanned`] (no evidence) rather than
    /// panicking.
    pub fn get(&self, shard: u16) -> CachedShardRecency {
        self.guard
            .per_shard
            .get(shard as usize)
            .copied()
            .unwrap_or(CachedShardRecency::Unscanned)
    }
}

/// Point-in-time refresh/scan statistics (W10 P1-1), for the metrics
/// endpoint and operator triage.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecencyScanStats {
    /// Completed refresh scans since boot.
    pub scans_completed: u64,
    /// Wall-clock duration of the most recent completed scan, in ms.
    pub last_scan_duration_ms: u64,
    /// Seconds since the most recent completed scan, or `None` when no
    /// scan has completed since boot.
    pub last_scan_age_secs: Option<u64>,
    /// Total keys the refresh scans SKIPPED because their footer was
    /// unreadable or raced a deletion (W10 P2-8) — aggregate across
    /// shards (the underlying enumeration reports one total, so per-shard
    /// attribution is not available; a shard whose EVERY record was
    /// skipped still resolves UNKNOWN via `had_records == false`).
    pub skipped_keys_total: u64,
    /// Refresh threads that FAILED to spawn (W10 P1-2) — each such
    /// failure released the single-flight slot so a later trigger retries.
    pub spawn_failures: u64,
}

/// See the module doc. Owned by the engine; one per engine.
pub struct ShardRecencyCache {
    /// Bumped (Relaxed) by every engine mutation that can change any
    /// record's `(txid, generation)` membership. Starts at 1 so the
    /// initial snapshot (stamp 0) reads as stale on a store that boots
    /// with pre-existing records; a genuinely empty store's initial
    /// snapshot is exact anyway (see [`empty_shard_recency`]).
    mutation_stamp: AtomicU64,
    /// Single-flight guard for the off-thread refresh: set by
    /// [`Self::claim_refresh_slot`], cleared by
    /// [`Self::release_refresh_slot`].
    refresh_in_flight: AtomicBool,
    snapshot: parking_lot::RwLock<RecencySnapshot>,
    /// When the most recent COMPLETED scan published, for the
    /// [`RECENCY_REFRESH_MIN_INTERVAL`] pacing gate and the age metric.
    last_refresh_completed: parking_lot::Mutex<Option<std::time::Instant>>,
    /// W10 P1-1 metrics — see [`RecencyScanStats`].
    scans_completed: AtomicU64,
    last_scan_duration_ms: AtomicU64,
    skipped_keys_total: AtomicU64,
    spawn_failures: AtomicU64,
}

impl ShardRecencyCache {
    /// A fresh cache with NO evidence for any shard
    /// ([`CachedShardRecency::Unscanned`]), stale by construction (see
    /// [`Self::mutation_stamp`]).
    pub fn new() -> Self {
        Self {
            mutation_stamp: AtomicU64::new(1),
            refresh_in_flight: AtomicBool::new(false),
            snapshot: parking_lot::RwLock::new(RecencySnapshot {
                stamp: 0,
                per_shard: vec![CachedShardRecency::Unscanned; crate::cluster::shards::NUM_SHARDS],
            }),
            last_refresh_completed: parking_lot::Mutex::new(None),
            scans_completed: AtomicU64::new(0),
            last_scan_duration_ms: AtomicU64::new(0),
            skipped_keys_total: AtomicU64::new(0),
            spawn_failures: AtomicU64::new(0),
        }
    }

    /// Record that a record-content mutation happened somewhere. Relaxed —
    /// the stamp only feeds the staleness comparison, never any ordering.
    #[inline(always)]
    pub fn note_mutation(&self) {
        self.mutation_stamp.fetch_add(1, Ordering::Relaxed);
    }

    /// Has any mutation landed since the published snapshot was scanned?
    pub fn is_stale(&self) -> bool {
        self.snapshot.read().stamp != self.mutation_stamp.load(Ordering::Relaxed)
    }

    /// W10 P1-1 — is a refresh DUE: stale AND at least
    /// [`RECENCY_REFRESH_MIN_INTERVAL`] since the last completed scan
    /// (always due when no scan has completed yet).
    pub fn refresh_due(&self) -> bool {
        if !self.is_stale() {
            return false;
        }
        match *self.last_refresh_completed.lock() {
            Some(completed) => completed.elapsed() >= RECENCY_REFRESH_MIN_INTERVAL,
            None => true,
        }
    }

    /// The cached state for `shard`. Takes the snapshot lock; a caller
    /// iterating many shards should use [`Self::read`] instead.
    pub fn get(&self, shard: u16) -> CachedShardRecency {
        self.read().get(shard)
    }

    /// One read guard over the whole snapshot (W10 P2-7).
    pub fn read(&self) -> SnapshotReader<'_> {
        SnapshotReader {
            guard: self.snapshot.read(),
        }
    }

    /// Load the mutation stamp to associate with a refresh scan that is
    /// about to start. Must be read BEFORE the scan touches the index so
    /// a mutation racing the scan leaves the published snapshot stale.
    pub fn stamp_for_refresh(&self) -> u64 {
        self.mutation_stamp.load(Ordering::Acquire)
    }

    /// Publish a completed refresh scan. `stamp` is the value
    /// [`Self::stamp_for_refresh`] returned before the scan began;
    /// `scan_duration` and `skipped_keys` feed the P1-1/P2-8 metrics.
    ///
    /// W10 P2-9 — the length contract is TOTAL: `per_shard` must be
    /// [`crate::cluster::shards::NUM_SHARDS`] long, and a short vector is
    /// padded with [`CachedShardRecency::Unscanned`] (no evidence — the
    /// affected high shards resolve UNKNOWN) rather than letting a
    /// release-mode caller bug silently degrade them to a wrong "known
    /// empty" fingerprint; an overlong vector is truncated. Either case
    /// still trips the debug assertion in tests.
    pub fn publish(
        &self,
        stamp: u64,
        mut per_shard: Vec<CachedShardRecency>,
        scan_duration: std::time::Duration,
        skipped_keys: u64,
    ) {
        debug_assert_eq!(per_shard.len(), crate::cluster::shards::NUM_SHARDS);
        per_shard.resize(
            crate::cluster::shards::NUM_SHARDS,
            CachedShardRecency::Unscanned,
        );
        *self.snapshot.write() = RecencySnapshot { stamp, per_shard };
        *self.last_refresh_completed.lock() = Some(std::time::Instant::now());
        self.scans_completed.fetch_add(1, Ordering::Relaxed);
        self.last_scan_duration_ms.store(
            u64::try_from(scan_duration.as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.skipped_keys_total
            .fetch_add(skipped_keys, Ordering::Relaxed);
    }

    /// Try to claim the single-flight refresh slot. Returns `false` when
    /// a refresh is already in flight. A successful claim MUST be paired
    /// with [`Self::release_refresh_slot`] once the refresh finishes (the
    /// engine's spawn path pairs them with an unwind-safe drop guard on
    /// the refresh thread, and releases inline if the spawn itself
    /// fails), or no further refresh can ever start.
    pub fn claim_refresh_slot(&self) -> bool {
        self.refresh_in_flight
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    /// Release the single-flight refresh slot claimed by
    /// [`Self::claim_refresh_slot`].
    pub fn release_refresh_slot(&self) {
        self.refresh_in_flight.store(false, Ordering::Release);
    }

    /// W10 P1-2 — count one failed refresh-thread spawn.
    pub fn note_spawn_failure(&self) {
        self.spawn_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Point-in-time scan statistics (W10 P1-1).
    pub fn scan_stats(&self) -> RecencyScanStats {
        RecencyScanStats {
            scans_completed: self.scans_completed.load(Ordering::Relaxed),
            last_scan_duration_ms: self.last_scan_duration_ms.load(Ordering::Relaxed),
            last_scan_age_secs: self
                .last_refresh_completed
                .lock()
                .map(|at| at.elapsed().as_secs()),
            skipped_keys_total: self.skipped_keys_total.load(Ordering::Relaxed),
            spawn_failures: self.spawn_failures.load(Ordering::Relaxed),
        }
    }
}

impl Default for ShardRecencyCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scanned(digest: u64, max_generation: u32, had_records: bool) -> CachedShardRecency {
        CachedShardRecency::Scanned {
            value: ShardRecencyValue {
                digest,
                max_generation,
            },
            had_records,
        }
    }

    fn full_table(fill: CachedShardRecency) -> Vec<CachedShardRecency> {
        vec![fill; crate::cluster::shards::NUM_SHARDS]
    }

    #[test]
    fn fresh_cache_is_stale_and_resolves_populated_shards_unknown() {
        let cache = ShardRecencyCache::new();
        assert!(
            cache.is_stale(),
            "a fresh cache must read stale so a store that boots with \
             pre-existing records refreshes before its fingerprints are trusted",
        );
        let empty = empty_shard_recency();
        assert_ne!(
            empty.digest, 0,
            "the empty-shard digest is the ManifestHasher empty fold, never 0",
        );
        // Records present + never scanned = the P0 fabricated-emptiness
        // shape — UNKNOWN.
        assert_eq!(
            cache
                .get(crate::cluster::shards::NUM_SHARDS as u16 - 1)
                .resolve(7),
            (empty, true),
        );
        // A live count of ZERO is exact evidence of emptiness — the empty
        // fingerprint is the truth, scan history or not.
        assert_eq!(cache.get(0).resolve(0), (empty, false));
        // Out-of-range resolves Unscanned instead of panicking.
        assert_eq!(cache.get(u16::MAX), CachedShardRecency::Unscanned);
    }

    #[test]
    fn resolve_distinguishes_evidence_of_empty_from_no_evidence() {
        let empty = empty_shard_recency();
        // Scanned-empty + still empty: honest KNOWN empty fingerprint.
        assert_eq!(scanned(empty.digest, 0, false).resolve(0), (empty, false));
        // Scanned-empty but records arrived since (or every record was
        // unreadable at scan time): the fingerprint is NOT evidence about
        // the records now present — UNKNOWN.
        assert_eq!(scanned(empty.digest, 0, false).resolve(3), (empty, true));
        // Scanned with records: KNOWN, even when the live count has moved
        // since the scan (ordinary bounded staleness).
        let v = ShardRecencyValue {
            digest: 0xF00D,
            max_generation: 9,
        };
        assert_eq!(scanned(0xF00D, 9, true).resolve(5), (v, false));
        assert_eq!(scanned(0xF00D, 9, true).resolve(0), (v, false));
    }

    #[test]
    fn publish_at_captured_stamp_clears_staleness_until_next_mutation() {
        let cache = ShardRecencyCache::new();
        let stamp = cache.stamp_for_refresh();
        let mut table = full_table(scanned(empty_shard_recency().digest, 0, false));
        table[7] = scanned(0xDEAD_BEEF, 3, true);
        cache.publish(stamp, table, std::time::Duration::from_millis(4), 0);
        assert!(
            !cache.is_stale(),
            "publishing at the captured stamp is fresh"
        );
        assert_eq!(cache.get(7), scanned(0xDEAD_BEEF, 3, true));
        cache.note_mutation();
        assert!(
            cache.is_stale(),
            "any mutation after the captured stamp must re-mark the cache stale",
        );
    }

    #[test]
    fn mutation_racing_the_scan_leaves_snapshot_stale() {
        let cache = ShardRecencyCache::new();
        let stamp = cache.stamp_for_refresh();
        // A mutation lands while the (conceptual) scan is running…
        cache.note_mutation();
        // …so publishing the scan's result at the PRE-scan stamp stays stale.
        cache.publish(
            stamp,
            full_table(CachedShardRecency::Unscanned),
            std::time::Duration::ZERO,
            0,
        );
        assert!(
            cache.is_stale(),
            "a snapshot whose scan raced a mutation must remain stale so the \
             next trigger refreshes again",
        );
    }

    #[test]
    fn refresh_slot_is_single_flight_until_released() {
        let cache = ShardRecencyCache::new();
        assert!(cache.claim_refresh_slot(), "first claim succeeds");
        assert!(
            !cache.claim_refresh_slot(),
            "a second claim while one refresh is in flight must be refused",
        );
        cache.release_refresh_slot();
        assert!(
            cache.claim_refresh_slot(),
            "releasing the slot must allow the next claim",
        );
    }

    /// W10 P1-1 — a stale cache is DUE before any scan has completed,
    /// NOT due again immediately after a completed scan re-stales, and
    /// never due while fresh.
    #[test]
    fn refresh_due_respects_min_interval_after_a_completed_scan() {
        let cache = ShardRecencyCache::new();
        assert!(
            cache.refresh_due(),
            "stale + never-scanned must be due immediately (boot convergence)",
        );
        let stamp = cache.stamp_for_refresh();
        cache.publish(
            stamp,
            full_table(CachedShardRecency::Unscanned),
            std::time::Duration::ZERO,
            0,
        );
        assert!(!cache.refresh_due(), "a fresh cache is never due");
        cache.note_mutation();
        assert!(cache.is_stale());
        assert!(
            !cache.refresh_due(),
            "stale within RECENCY_REFRESH_MIN_INTERVAL of the last completed \
             scan must NOT be due — back-to-back whole-store scans were the \
             P1-1 defect",
        );
    }

    /// W10 P2-9 — a short published vector must not degrade the missing
    /// high shards to a wrong "known" state in release builds: they pad
    /// to Unscanned (no evidence → UNKNOWN).
    #[test]
    #[cfg(not(debug_assertions))]
    fn publish_pads_short_vectors_to_unscanned() {
        let cache = ShardRecencyCache::new();
        let stamp = cache.stamp_for_refresh();
        cache.publish(
            stamp,
            vec![scanned(1, 1, true); 8],
            std::time::Duration::ZERO,
            0,
        );
        assert_eq!(cache.get(7), scanned(1, 1, true));
        assert_eq!(
            cache.get(crate::cluster::shards::NUM_SHARDS as u16 - 1),
            CachedShardRecency::Unscanned,
            "shards beyond a short publish must resolve UNKNOWN, not known-empty",
        );
    }

    #[test]
    fn scan_stats_track_completions_duration_and_skips() {
        let cache = ShardRecencyCache::new();
        assert_eq!(cache.scan_stats(), RecencyScanStats::default());
        cache.publish(
            cache.stamp_for_refresh(),
            full_table(CachedShardRecency::Unscanned),
            std::time::Duration::from_millis(12),
            3,
        );
        cache.note_spawn_failure();
        let stats = cache.scan_stats();
        assert_eq!(stats.scans_completed, 1);
        assert_eq!(stats.last_scan_duration_ms, 12);
        assert_eq!(stats.skipped_keys_total, 3);
        assert_eq!(stats.spawn_failures, 1);
        assert!(stats.last_scan_age_secs.is_some());
    }
}
