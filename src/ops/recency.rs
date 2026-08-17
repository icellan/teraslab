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
//!   handler or the exchange window.
//! - Staleness is tracked by a mutation stamp
//!   ([`ShardRecencyCache::note_mutation`]) bumped by every engine path
//!   that can change any record's `(txid, generation)` membership:
//!   primary-index register/unregister (create/delete) and every
//!   metadata persist (in-place footer writes, direct-memory footer
//!   writes, and log-structured relocation). Refresh captures the stamp
//!   BEFORE scanning, so a mutation racing the scan leaves the snapshot
//!   stale and the next trigger refreshes again.
//!
//! # Serving semantics
//!
//! A served entry is the shard's honest recency AS OF the last completed
//! refresh (plus the always-live record count). That staleness is safe for
//! every consumer by construction: the recency fields are a MISMATCH
//! pre-filter for reverse-heal detection (log + meter only — an
//! authoritative per-record manifest exchange arbitrates before any heal),
//! and the coarse direction pre-filter tolerates over/under-flagging (a
//! wrong nomination costs a no-op confirm, never a wrong heal). The value
//! was already live-moving — two nodes were never sampling at the same
//! instant — so bounded staleness changes the comparison's nature not at
//! all, while removing the device scan that starved the exchange.
//!
//! A NEVER-refreshed cache serves the fingerprint of an EMPTY shard for
//! every shard ([`empty_shard_recency`]) — exactly what the scan computes
//! for a shard with no records, so a fresh (empty) engine serves exact
//! values with no refresh at all. A store that boots with existing data
//! starts stale and converges at its first refresh (triggered by the first
//! report it builds or serves).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

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

/// The published snapshot: one [`ShardRecencyValue`] per shard, plus the
/// mutation stamp observed BEFORE the scan that produced it began.
struct RecencySnapshot {
    /// [`ShardRecencyCache::mutation_stamp`] loaded before the producing
    /// scan started. `0` = the initial never-refreshed snapshot (the
    /// stamp starts at 1, so an initial snapshot is always stale).
    stamp: u64,
    /// Indexed by shard number; length [`crate::cluster::shards::NUM_SHARDS`].
    per_shard: Vec<ShardRecencyValue>,
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
}

impl ShardRecencyCache {
    /// A fresh cache serving [`empty_shard_recency`] for every shard,
    /// stale by construction (see [`Self::mutation_stamp`]).
    pub fn new() -> Self {
        Self {
            mutation_stamp: AtomicU64::new(1),
            refresh_in_flight: AtomicBool::new(false),
            snapshot: parking_lot::RwLock::new(RecencySnapshot {
                stamp: 0,
                per_shard: vec![empty_shard_recency(); crate::cluster::shards::NUM_SHARDS],
            }),
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

    /// The published recency for `shard`. RAM only — never touches the
    /// index or the device. Out-of-range shard numbers (impossible from
    /// the report builder, which iterates `0..NUM_SHARDS`) serve the
    /// empty fingerprint rather than panicking.
    pub fn get(&self, shard: u16) -> ShardRecencyValue {
        self.snapshot
            .read()
            .per_shard
            .get(shard as usize)
            .copied()
            .unwrap_or_else(empty_shard_recency)
    }

    /// Load the mutation stamp to associate with a refresh scan that is
    /// about to start. Must be read BEFORE the scan touches the index so
    /// a mutation racing the scan leaves the published snapshot stale.
    pub fn stamp_for_refresh(&self) -> u64 {
        self.mutation_stamp.load(Ordering::Acquire)
    }

    /// Publish a completed refresh scan. `per_shard` must be
    /// [`crate::cluster::shards::NUM_SHARDS`] long; `stamp` is the value
    /// [`Self::stamp_for_refresh`] returned before the scan began.
    pub fn publish(&self, stamp: u64, per_shard: Vec<ShardRecencyValue>) {
        debug_assert_eq!(per_shard.len(), crate::cluster::shards::NUM_SHARDS);
        *self.snapshot.write() = RecencySnapshot { stamp, per_shard };
    }

    /// Try to claim the single-flight refresh slot. Returns `false` when
    /// a refresh is already in flight. A successful claim MUST be paired
    /// with [`Self::release_refresh_slot`] once the refresh finishes (the
    /// engine's spawn path pairs them with an unwind-safe drop guard on
    /// the refresh thread), or no further refresh can ever start.
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
}

impl Default for ShardRecencyCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_cache_is_stale_and_serves_empty_fingerprints() {
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
        assert_eq!(cache.get(0), empty);
        assert_eq!(
            cache.get(crate::cluster::shards::NUM_SHARDS as u16 - 1),
            empty
        );
        // Out-of-range serves the empty fingerprint instead of panicking.
        assert_eq!(cache.get(u16::MAX), empty);
    }

    #[test]
    fn publish_at_captured_stamp_clears_staleness_until_next_mutation() {
        let cache = ShardRecencyCache::new();
        let stamp = cache.stamp_for_refresh();
        let mut table = vec![empty_shard_recency(); crate::cluster::shards::NUM_SHARDS];
        table[7] = ShardRecencyValue {
            digest: 0xDEAD_BEEF,
            max_generation: 3,
        };
        cache.publish(stamp, table);
        assert!(
            !cache.is_stale(),
            "publishing at the captured stamp is fresh"
        );
        assert_eq!(
            cache.get(7),
            ShardRecencyValue {
                digest: 0xDEAD_BEEF,
                max_generation: 3,
            }
        );
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
            vec![empty_shard_recency(); crate::cluster::shards::NUM_SHARDS],
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
}
