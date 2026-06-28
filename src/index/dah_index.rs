//! DAH (delete-at-height) secondary index.
//!
//! Maps `delete_at_height` values to sets of transaction keys. The pruner
//! queries `range_query(0..=current_height)` each block to find records
//! eligible for deletion.
//!
//! NOT critical for crash safety — a stale DAH index only delays pruning.
//! Rebuilt from a device scan on recovery.

use crate::index::TxKey;
use crate::server::fast_hash::FastTxHasher;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::BuildHasherDefault;

/// Per-height bucket of txids, keyed by the fast non-cryptographic txid hasher.
///
/// Insert and remove are O(1). A block's transactions can share a single
/// delete_at_height, collapsing into one bucket; with a `Vec` removal was
/// O(bucket) and draining that bucket was O(n^2). A set keeps each removal O(1).
/// txids are double-SHA256 digests (uniformly random), so the fast first-8-bytes
/// hasher distributes them well.
type HeightBucket = HashSet<TxKey, BuildHasherDefault<FastTxHasher>>;

/// Redo log entry for DAH secondary index mutations.
///
/// Parallel to [`crate::index::unmined_index::UnminedRedoEntry`]. Captures
/// the before/after state so the mutation can be replayed idempotently
/// on crash recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DahRedoEntry {
    /// Transaction ID.
    pub txid: [u8; 32],
    /// Previous delete_at_height value (0 = was not in the index).
    pub old_height: u32,
    /// New delete_at_height value (0 = removed from the index).
    pub new_height: u32,
}

/// Secondary index mapping delete_at_height to transactions.
pub struct DahIndex {
    /// Forward map: height -> txids scheduled for deletion at that height.
    by_height: BTreeMap<u32, HeightBucket>,
    /// Reverse map: txid -> current delete_at_height (for O(1) removal).
    by_txid: HashMap<TxKey, u32>,
}

impl DahIndex {
    /// Create an empty DAH index.
    pub fn new() -> Self {
        Self {
            by_height: BTreeMap::new(),
            by_txid: HashMap::new(),
        }
    }

    /// Insert a transaction into the DAH index.
    ///
    /// If the txid already has a DAH entry at a different height, the old
    /// entry is removed first (handles DAH updates on re-org).
    pub fn insert(&mut self, height: u32, key: TxKey) {
        // Remove old entry if it exists at a different height.
        if let Some(&old_height) = self.by_txid.get(&key) {
            if old_height == height {
                // F-G3-019: the no-op branch assumes the by_height bucket
                // for this height already contains `key`. If a previous
                // bug left the two maps out of sync, this assertion fires
                // in debug builds rather than silently masking the drift.
                debug_assert!(
                    self.by_height
                        .get(&height)
                        .is_some_and(|v| v.contains(&key)),
                    "dah_index invariant violated: by_txid says height={height} but \
                     by_height[{height}] does not contain {:?}",
                    key.txid,
                );
                return; // Already at this height, no-op.
            }
            self.remove_from_height_vec(old_height, &key);
        }
        self.by_txid.insert(key, height);
        self.by_height.entry(height).or_default().insert(key);
    }

    /// Remove a transaction from the DAH index.
    ///
    /// No-op if the key is not present.
    pub fn remove(&mut self, key: &TxKey) {
        if let Some(height) = self.by_txid.remove(key) {
            self.remove_from_height_vec(height, key);
        }
    }

    /// Return all txids with delete_at_height in `[0, current_height]`.
    ///
    /// Results are returned in ascending height order.
    pub fn range_query(&self, current_height: u32) -> Vec<TxKey> {
        let mut result = Vec::new();
        for (_, keys) in self.by_height.range(..=current_height) {
            result.extend(keys.iter().copied());
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

    /// Iterate over all `(height, key)` pairs (for snapshot).
    pub fn iter(&self) -> impl Iterator<Item = (u32, TxKey)> + '_ {
        self.by_height
            .iter()
            .flat_map(|(&h, keys)| keys.iter().map(move |&k| (h, k)))
    }

    /// Replay a DAH redo entry.
    ///
    /// Idempotent: replaying the same entry multiple times produces the
    /// same result.
    pub fn replay_redo(&mut self, entry: &DahRedoEntry) {
        let key = TxKey { txid: entry.txid };
        if entry.new_height == 0 {
            self.remove(&key);
        } else {
            // Insert-or-update idempotently.
            if let Some(&old_h) = self.by_txid.get(&key) {
                if old_h == entry.new_height {
                    return;
                }
                self.remove_from_height_vec(old_h, &key);
            }
            self.by_txid.insert(key, entry.new_height);
            self.by_height
                .entry(entry.new_height)
                .or_default()
                .insert(key);
        }
    }

    /// Remove a key from the bucket at the given height in O(1), dropping the
    /// bucket when it becomes empty.
    fn remove_from_height_vec(&mut self, height: u32, key: &TxKey) {
        if let Some(keys) = self.by_height.get_mut(&height) {
            keys.remove(key);
            if keys.is_empty() {
                self.by_height.remove(&height);
            }
        }
    }
}

impl Default for DahIndex {
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
        let mut idx = DahIndex::new();
        idx.insert(100, key(1));
        let result = idx.range_query(100);
        assert_eq!(result, vec![key(1)]);
    }

    #[test]
    fn insert_multiple_heights_range_query() {
        let mut idx = DahIndex::new();
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
        let mut idx = DahIndex::new();
        idx.insert(100, key(1));
        idx.remove(&key(1));
        assert!(idx.range_query(100).is_empty());
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn insert_updates_height() {
        let mut idx = DahIndex::new();
        idx.insert(100, key(1));
        idx.insert(200, key(1)); // Move to height 200

        assert!(idx.range_query(100).is_empty());
        assert_eq!(idx.range_query(200), vec![key(1)]);
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn remove_nonexistent_is_noop() {
        let mut idx = DahIndex::new();
        idx.remove(&key(99)); // Should not panic
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn ten_thousand_entries() {
        let mut idx = DahIndex::new();
        for i in 0u32..10_000 {
            let height = (i % 100) * 10; // 100 distinct heights
            let mut txid = [0u8; 32];
            txid[0..4].copy_from_slice(&i.to_le_bytes());
            idx.insert(height, TxKey { txid });
        }
        assert_eq!(idx.len(), 10_000);

        // Range query for first 50 heights (0..490)
        let result = idx.range_query(490);
        assert_eq!(result.len(), 5_000); // 100 entries per height × 50 heights
    }

    #[test]
    fn len_tracks_inserts_and_removes() {
        let mut idx = DahIndex::new();
        idx.insert(1, key(1));
        idx.insert(2, key(2));
        idx.insert(3, key(3));
        assert_eq!(idx.len(), 3);

        idx.remove(&key(2));
        assert_eq!(idx.len(), 2);

        idx.remove(&key(2)); // Already removed
        assert_eq!(idx.len(), 2);
    }

    #[test]
    fn clear_empties_index() {
        let mut idx = DahIndex::new();
        idx.insert(1, key(1));
        idx.insert(2, key(2));
        idx.clear();
        assert_eq!(idx.len(), 0);
        assert!(idx.is_empty());
        assert!(idx.range_query(u32::MAX).is_empty());
    }

    #[test]
    fn large_single_bucket_remove_all_one_by_one() {
        // All keys scheduled for deletion at the SAME height collapse into one
        // bucket. With the old Vec+retain this drain is O(n^2); with a set it is
        // O(1) per removal.
        const N: u32 = 5_000;
        let height = 900_000;
        let mut idx = DahIndex::new();

        let keys: Vec<TxKey> = (0..N)
            .map(|i| {
                let mut txid = [0u8; 32];
                txid[0..4].copy_from_slice(&i.to_le_bytes());
                TxKey { txid }
            })
            .collect();

        for &k in &keys {
            idx.insert(height, k);
        }
        assert_eq!(idx.len(), N as usize);
        assert_eq!(idx.range_query(height).len(), N as usize);

        for (removed, &k) in keys.iter().enumerate() {
            idx.remove(&k);
            let expected_remaining = N as usize - (removed + 1);
            assert_eq!(idx.len(), expected_remaining);
            assert_eq!(idx.range_query(height).len(), expected_remaining);
        }

        assert!(idx.is_empty());
        assert!(idx.range_query(height).is_empty());
    }

    #[test]
    fn mixed_insert_remove_query_correctness() {
        let mut idx = DahIndex::new();
        for i in 0u8..20 {
            let h = if i % 2 == 0 { 100 } else { 200 };
            idx.insert(h, key(i));
        }
        assert_eq!(idx.len(), 20);

        for i in (0u8..20).filter(|i| i % 3 == 0) {
            idx.remove(&key(i));
        }

        use std::collections::HashSet as StdHashSet;
        let mut expected_at_or_below_100: StdHashSet<[u8; 32]> = StdHashSet::new();
        let mut expected_all: StdHashSet<[u8; 32]> = StdHashSet::new();
        for i in 0u8..20 {
            if i % 3 == 0 {
                continue;
            }
            expected_all.insert(key(i).txid);
            if i % 2 == 0 {
                expected_at_or_below_100.insert(key(i).txid);
            }
        }

        // delete-at-height query (range_query) is order-independent now.
        let got_100: StdHashSet<[u8; 32]> = idx.range_query(100).iter().map(|k| k.txid).collect();
        assert_eq!(got_100, expected_at_or_below_100);

        let got_all: StdHashSet<[u8; 32]> = idx.range_query(200).iter().map(|k| k.txid).collect();
        assert_eq!(got_all, expected_all);

        let iter_pairs: StdHashSet<(u32, [u8; 32])> =
            idx.iter().map(|(h, k)| (h, k.txid)).collect();
        let mut expected_pairs: StdHashSet<(u32, [u8; 32])> = StdHashSet::new();
        for i in 0u8..20 {
            if i % 3 == 0 {
                continue;
            }
            let h = if i % 2 == 0 { 100 } else { 200 };
            expected_pairs.insert((h, key(i).txid));
        }
        assert_eq!(iter_pairs, expected_pairs);
    }
}
