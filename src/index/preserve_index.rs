//! Preserve (preserve-until) secondary index.
//!
//! Maps `preserve_until` values to sets of transaction keys. The pruner's
//! expired-preservation sweep (`OP_PROCESS_EXPIRED_PRESERVATIONS`, spec §3.18
//! Phase 3) queries `range_query(current_height)` each call to find records
//! whose preservation window has elapsed — `[1, current_height]` — instead of
//! walking the whole primary index (the O(index-size) sweep that
//! `ShardedIndex::scan_expired_preservations` used to perform, issue #25).
//!
//! Structurally identical to [`crate::index::dah_index::DahIndex`]; the only
//! semantic divergence is the lower bound: a `preserve_until` of 0 means "not
//! preserved", so the query range starts at 1 (never treats the 0 sentinel as
//! an expired preservation). Entries are only ever inserted with a non-zero
//! height, so the bound is also defensive.
//!
//! NOT critical for crash safety — a stale preserve index only delays the
//! Phase-3 transition (preserve → DAH), and the eventual delete still happens
//! one `BlockHeightRetention` window later, which is harmless. It is therefore
//! in the same crash-safety class as [`DahIndex`](crate::index::dah_index)
//! (NOT the unmined "critical" class): it is NOT journaled to the redo log and
//! carries NO `replay_redo` path. It is re-derived after a crash from each
//! record's authoritative on-device `preserve_until` via
//! [`crate::ops::engine::Engine::rebuild_preserve_index_from_device`] — which
//! reads the device footer, not the index cache (the `HAS_PRESERVE_UNTIL`
//! cache discriminant lags after a redo replay), in the same spirit as the
//! conflicting index being rebuilt from each record's flag at startup.

use crate::index::TxKey;
use std::collections::{BTreeMap, HashMap};

/// Secondary index mapping `preserve_until` to transactions.
pub struct PreserveIndex {
    /// Forward map: height -> txids whose preservation expires at that height.
    by_height: BTreeMap<u32, Vec<TxKey>>,
    /// Reverse map: txid -> current preserve_until (for O(1) removal).
    by_txid: HashMap<TxKey, u32>,
}

impl PreserveIndex {
    /// Create an empty preserve index.
    pub fn new() -> Self {
        Self {
            by_height: BTreeMap::new(),
            by_txid: HashMap::new(),
        }
    }

    /// Insert a transaction into the preserve index.
    ///
    /// If the txid already has a preserve entry at a different height, the old
    /// entry is removed first (handles a record being re-preserved at a new
    /// height).
    pub fn insert(&mut self, height: u32, key: TxKey) {
        // Remove old entry if it exists at a different height.
        if let Some(&old_height) = self.by_txid.get(&key) {
            if old_height == height {
                // Mirror of the DahIndex F-G3-019 invariant: the no-op branch
                // assumes the by_height bucket for this height already contains
                // `key`. If a prior bug left the two maps out of sync, this
                // fires in debug builds rather than silently masking the drift.
                debug_assert!(
                    self.by_height
                        .get(&height)
                        .is_some_and(|v| v.contains(&key)),
                    "preserve_index invariant violated: by_txid says height={height} but \
                     by_height[{height}] does not contain {:?}",
                    key.txid,
                );
                return; // Already at this height, no-op.
            }
            self.remove_from_height_vec(old_height, &key);
        }
        self.by_txid.insert(key, height);
        self.by_height.entry(height).or_default().push(key);
    }

    /// Remove a transaction from the preserve index.
    ///
    /// No-op if the key is not present.
    pub fn remove(&mut self, key: &TxKey) {
        if let Some(height) = self.by_txid.remove(key) {
            self.remove_from_height_vec(height, key);
        }
    }

    /// Return all txids whose `preserve_until` is in `[1, current_height]`.
    ///
    /// The lower bound is 1 (NOT 0): a `preserve_until` of 0 is the "not
    /// preserved" sentinel and must never be reported as expired. Returns an
    /// empty vec when `current_height == 0` (the `1..=0` range would otherwise
    /// be an invalid `start > end` range for `BTreeMap::range`).
    ///
    /// Results are returned in ascending height order.
    pub fn range_query(&self, current_height: u32) -> Vec<TxKey> {
        if current_height == 0 {
            return Vec::new();
        }
        let mut result = Vec::new();
        for (_, keys) in self.by_height.range(1..=current_height) {
            result.extend_from_slice(keys);
        }
        result
    }

    /// Like [`Self::range_query`] but stops after collecting `limit` keys
    /// (lowest-`preserve_until` first). Bounds the expiry sweep's per-call work
    /// the same way the DAH sweep is bounded — see
    /// [`crate::index::dah_index::DahIndex::range_query_limited`]. The preserve
    /// set is normally small, but a pathological backlog must not peg a core.
    pub fn range_query_limited(&self, current_height: u32, limit: usize) -> Vec<TxKey> {
        let mut result = Vec::new();
        if limit == 0 || current_height == 0 {
            return result;
        }
        for (_, keys) in self.by_height.range(1..=current_height) {
            for &key in keys {
                result.push(key);
                if result.len() >= limit {
                    return result;
                }
            }
        }
        result
    }

    /// Number of entries in the index.
    pub fn len(&self) -> usize {
        self.by_txid.len()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.by_txid.is_empty()
    }

    /// Remove all entries.
    pub fn clear(&mut self) {
        self.by_height.clear();
        self.by_txid.clear();
    }

    /// Iterate over all `(height, key)` pairs (for snapshot/export).
    pub fn iter(&self) -> impl Iterator<Item = (u32, TxKey)> + '_ {
        self.by_height
            .iter()
            .flat_map(|(&h, keys)| keys.iter().map(move |&k| (h, k)))
    }

    /// Remove a key from the Vec at the given height, cleaning up empty vecs.
    fn remove_from_height_vec(&mut self, height: u32, key: &TxKey) {
        if let Some(keys) = self.by_height.get_mut(&height) {
            keys.retain(|k| k != key);
            if keys.is_empty() {
                self.by_height.remove(&height);
            }
        }
    }
}

impl Default for PreserveIndex {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn key(n: u8) -> TxKey {
        let mut txid = [0u8; 32];
        txid[0] = n;
        TxKey { txid }
    }

    #[test]
    fn insert_single_range_query() {
        let mut idx = PreserveIndex::new();
        idx.insert(100, key(1));
        let result = idx.range_query(100);
        assert_eq!(result, vec![key(1)]);
    }

    /// The load-bearing divergence from `DahIndex`: the query range starts at
    /// 1, so a `current_height` of 0 returns nothing even with entries present,
    /// and the `1..=0` range never panics.
    #[test]
    fn range_query_excludes_height_zero() {
        let mut idx = PreserveIndex::new();
        idx.insert(1, key(1));
        idx.insert(50, key(2));
        // current_height == 0 -> empty (and must not panic on the 1..=0 range).
        assert!(idx.range_query(0).is_empty());
        // From height 1 the lowest entry is included.
        assert_eq!(idx.range_query(1), vec![key(1)]);
        let r50 = idx.range_query(50);
        assert_eq!(r50.len(), 2);
        assert!(r50.contains(&key(1)));
        assert!(r50.contains(&key(2)));
    }

    #[test]
    fn insert_multiple_heights_range_query() {
        let mut idx = PreserveIndex::new();
        idx.insert(100, key(1));
        idx.insert(100, key(2));
        idx.insert(200, key(3));

        let r100 = idx.range_query(100);
        assert_eq!(r100.len(), 2);
        assert!(r100.contains(&key(1)));
        assert!(r100.contains(&key(2)));

        let r200 = idx.range_query(200);
        assert_eq!(r200.len(), 3);

        let r99 = idx.range_query(99);
        assert!(r99.is_empty());
    }

    #[test]
    fn insert_then_remove() {
        let mut idx = PreserveIndex::new();
        idx.insert(100, key(1));
        idx.remove(&key(1));
        assert!(idx.range_query(u32::MAX).is_empty());
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn insert_updates_height() {
        let mut idx = PreserveIndex::new();
        idx.insert(100, key(1));
        idx.insert(200, key(1)); // Re-preserve at a later height.

        assert!(idx.range_query(100).is_empty());
        assert_eq!(idx.range_query(200), vec![key(1)]);
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn insert_same_height_is_noop() {
        let mut idx = PreserveIndex::new();
        idx.insert(100, key(1));
        idx.insert(100, key(1)); // Same height — no duplicate bucket entry.
        assert_eq!(idx.len(), 1);
        assert_eq!(idx.range_query(100), vec![key(1)]);
    }

    #[test]
    fn remove_nonexistent_is_noop() {
        let mut idx = PreserveIndex::new();
        idx.remove(&key(99)); // Should not panic.
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn clear_empties_index() {
        let mut idx = PreserveIndex::new();
        idx.insert(1, key(1));
        idx.insert(2, key(2));
        idx.clear();
        assert_eq!(idx.len(), 0);
        assert!(idx.is_empty());
        assert!(idx.range_query(u32::MAX).is_empty());
    }

    #[test]
    fn range_query_limited_caps_and_excludes_height_zero() {
        let mut idx = PreserveIndex::new();
        idx.insert(10, key(1));
        idx.insert(10, key(2));
        idx.insert(20, key(3));

        // limit 0 → empty; height-0 query → empty (and the 1..=0 range never
        // panics under the cap either).
        assert!(idx.range_query_limited(100, 0).is_empty());
        assert!(idx.range_query_limited(0, 10).is_empty());

        // capped to the lowest preserve heights first.
        assert_eq!(idx.range_query_limited(100, 2).len(), 2);
        // limit >= total → all.
        assert_eq!(idx.range_query_limited(100, 99).len(), 3);
    }

    #[test]
    fn len_tracks_inserts_and_removes() {
        let mut idx = PreserveIndex::new();
        idx.insert(1, key(1));
        idx.insert(2, key(2));
        idx.insert(3, key(3));
        assert_eq!(idx.len(), 3);

        idx.remove(&key(2));
        assert_eq!(idx.len(), 2);

        idx.remove(&key(2)); // Already removed.
        assert_eq!(idx.len(), 2);
    }

    #[test]
    fn iter_yields_all_pairs() {
        let mut idx = PreserveIndex::new();
        idx.insert(100, key(1));
        idx.insert(200, key(2));
        idx.insert(200, key(3));
        let mut pairs: Vec<(u32, TxKey)> = idx.iter().collect();
        pairs.sort_by_key(|(h, k)| (*h, k.txid));
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0], (100, key(1)));
    }

    #[test]
    fn ten_thousand_entries() {
        let mut idx = PreserveIndex::new();
        for i in 0u32..10_000 {
            let height = (i % 100) * 10 + 1; // 100 distinct heights, all >= 1.
            let mut txid = [0u8; 32];
            txid[0..4].copy_from_slice(&i.to_le_bytes());
            idx.insert(height, TxKey { txid });
        }
        assert_eq!(idx.len(), 10_000);

        // Range query for the first 50 heights (1..=491).
        let result = idx.range_query(491);
        assert_eq!(result.len(), 5_000); // 100 entries per height × 50 heights.
    }
}
