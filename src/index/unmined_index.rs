//! Unmined secondary index.
//!
//! Maps `unmined_since` values to sets of transaction keys. The pruner
//! queries `range_query(0..=cutoff_height)` to find old unmined transactions
//! whose parents should be preserved.
//!
//! CRITICAL for crash safety — a stale unmined index would miss transactions
//! that need parent preservation, leading to data loss. Mutations to this
//! index are logged in the redo log and replayed on recovery (Phase 7).

use crate::index::TxKey;
use std::collections::{BTreeMap, HashMap};

/// Redo log entry for unmined index mutations.
///
/// Captures the before/after state so the mutation can be replayed
/// idempotently on crash recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnminedRedoEntry {
    /// Transaction ID.
    pub txid: [u8; 32],
    /// Previous unmined_since value (0 = was not in the index).
    pub old_height: u32,
    /// New unmined_since value (0 = removed from the index).
    pub new_height: u32,
}

/// Secondary index mapping unmined_since to transactions.
pub struct UnminedIndex {
    /// Forward map: unmined_since height -> txids.
    by_height: BTreeMap<u32, Vec<TxKey>>,
    /// Reverse map: txid -> current unmined_since value.
    by_txid: HashMap<TxKey, u32>,
}

impl UnminedIndex {
    /// Create an empty unmined index.
    pub fn new() -> Self {
        Self {
            by_height: BTreeMap::new(),
            by_txid: HashMap::new(),
        }
    }

    /// Insert a transaction into the unmined index.
    ///
    /// If the txid already has an entry at a different height, the old entry
    /// is removed first (handles re-org updates).
    ///
    /// Returns a [`UnminedRedoEntry`] that the caller MUST write to the redo
    /// log before acknowledging the client.
    pub fn insert(&mut self, height: u32, key: TxKey) -> UnminedRedoEntry {
        let old_height = if let Some(&old_h) = self.by_txid.get(&key) {
            if old_h == height {
                // Already at this height — still return a redo entry for idempotency.
                return UnminedRedoEntry {
                    txid: key.txid,
                    old_height: old_h,
                    new_height: height,
                };
            }
            self.remove_from_height_vec(old_h, &key);
            old_h
        } else {
            0
        };

        self.by_txid.insert(key, height);
        self.by_height.entry(height).or_default().push(key);

        UnminedRedoEntry {
            txid: key.txid,
            old_height,
            new_height: height,
        }
    }

    /// Remove a transaction from the unmined index.
    ///
    /// No-op if the key is not present (returns a redo entry with both heights 0).
    ///
    /// Returns a [`UnminedRedoEntry`] that the caller MUST write to the redo
    /// log before acknowledging the client.
    pub fn remove(&mut self, key: &TxKey) -> UnminedRedoEntry {
        let old_height = if let Some(h) = self.by_txid.remove(key) {
            self.remove_from_height_vec(h, key);
            h
        } else {
            0
        };

        UnminedRedoEntry {
            txid: key.txid,
            old_height,
            new_height: 0,
        }
    }

    /// Return all txids with unmined_since in `[0, cutoff_height]`.
    pub fn range_query(&self, cutoff_height: u32) -> Vec<TxKey> {
        let mut result = Vec::new();
        for (_, keys) in self.by_height.range(..=cutoff_height) {
            result.extend_from_slice(keys);
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

    /// Replay a redo entry to bring the index up to date.
    ///
    /// Idempotent: replaying the same entry multiple times produces the
    /// same result.
    pub fn replay_redo(&mut self, entry: &UnminedRedoEntry) {
        let key = TxKey {
            txid: entry.txid,
        };
        if entry.new_height == 0 {
            // Removal
            self.by_txid.remove(&key).inspect(|&h| {
                self.remove_from_height_vec(h, &key);
            });
        } else {
            // Insert or update
            if let Some(&old_h) = self.by_txid.get(&key) {
                if old_h != entry.new_height {
                    self.remove_from_height_vec(old_h, &key);
                } else {
                    return; // Already correct
                }
            }
            self.by_txid.insert(key, entry.new_height);
            self.by_height.entry(entry.new_height).or_default().push(key);
        }
    }

    fn remove_from_height_vec(&mut self, height: u32, key: &TxKey) {
        if let Some(keys) = self.by_height.get_mut(&height) {
            keys.retain(|k| k != key);
            if keys.is_empty() {
                self.by_height.remove(&height);
            }
        }
    }
}

impl Default for UnminedIndex {
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
        let mut idx = UnminedIndex::new();
        idx.insert(500, key(1));
        assert_eq!(idx.range_query(500), vec![key(1)]);
    }

    #[test]
    fn insert_multiple_heights_range_query() {
        let mut idx = UnminedIndex::new();
        idx.insert(500, key(1));
        idx.insert(500, key(2));
        idx.insert(600, key(3));

        let r500 = idx.range_query(500);
        assert_eq!(r500.len(), 2);
        assert!(r500.contains(&key(1)));
        assert!(r500.contains(&key(2)));

        let r600 = idx.range_query(600);
        assert_eq!(r600.len(), 3);

        let r499 = idx.range_query(499);
        assert!(r499.is_empty());
    }

    #[test]
    fn insert_then_remove() {
        let mut idx = UnminedIndex::new();
        idx.insert(500, key(1));
        idx.remove(&key(1));
        assert!(idx.range_query(500).is_empty());
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn insert_updates_height() {
        let mut idx = UnminedIndex::new();
        idx.insert(500, key(1));
        idx.insert(700, key(1));

        assert!(idx.range_query(500).is_empty());
        assert_eq!(idx.range_query(700), vec![key(1)]);
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn remove_nonexistent_noop() {
        let mut idx = UnminedIndex::new();
        let redo = idx.remove(&key(99));
        assert_eq!(redo.old_height, 0);
        assert_eq!(redo.new_height, 0);
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn ten_thousand_entries() {
        let mut idx = UnminedIndex::new();
        for i in 0u32..10_000 {
            let height = (i % 100) * 10;
            let mut txid = [0u8; 32];
            txid[0..4].copy_from_slice(&i.to_le_bytes());
            idx.insert(height, TxKey { txid });
        }
        assert_eq!(idx.len(), 10_000);

        let result = idx.range_query(490);
        assert_eq!(result.len(), 5_000);
    }

    #[test]
    fn len_tracks_mutations() {
        let mut idx = UnminedIndex::new();
        idx.insert(1, key(1));
        idx.insert(2, key(2));
        idx.insert(3, key(3));
        assert_eq!(idx.len(), 3);

        idx.remove(&key(2));
        assert_eq!(idx.len(), 2);

        idx.remove(&key(2));
        assert_eq!(idx.len(), 2);
    }

    #[test]
    fn clear_empties_index() {
        let mut idx = UnminedIndex::new();
        idx.insert(1, key(1));
        idx.insert(2, key(2));
        idx.clear();
        assert_eq!(idx.len(), 0);
        assert!(idx.is_empty());
    }

    // -- Redo entry tests --

    #[test]
    fn insert_redo_entry() {
        let mut idx = UnminedIndex::new();
        let redo = idx.insert(500, key(1));
        assert_eq!(redo.old_height, 0);
        assert_eq!(redo.new_height, 500);
        assert_eq!(redo.txid, key(1).txid);
    }

    #[test]
    fn remove_redo_entry() {
        let mut idx = UnminedIndex::new();
        idx.insert(500, key(1));
        let redo = idx.remove(&key(1));
        assert_eq!(redo.old_height, 500);
        assert_eq!(redo.new_height, 0);
    }

    #[test]
    fn update_redo_entry() {
        let mut idx = UnminedIndex::new();
        idx.insert(500, key(1));
        let redo = idx.insert(700, key(1));
        assert_eq!(redo.old_height, 500);
        assert_eq!(redo.new_height, 700);
    }

    #[test]
    fn replay_redo_entries() {
        let mut idx = UnminedIndex::new();
        idx.insert(100, key(1));
        idx.insert(200, key(2));

        // Capture redo entries
        let r1 = UnminedRedoEntry {
            txid: key(3).txid,
            old_height: 0,
            new_height: 300,
        };
        let r2 = UnminedRedoEntry {
            txid: key(1).txid,
            old_height: 100,
            new_height: 0,
        };
        let r3 = UnminedRedoEntry {
            txid: key(2).txid,
            old_height: 200,
            new_height: 400,
        };

        idx.replay_redo(&r1);
        idx.replay_redo(&r2);
        idx.replay_redo(&r3);

        assert!(idx.range_query(100).is_empty()); // key(1) removed
        assert!(idx.range_query(200).is_empty()); // key(2) moved
        assert_eq!(idx.range_query(300), vec![key(3)]);
        let r400 = idx.range_query(400);
        assert_eq!(r400.len(), 2); // key(2) at 400 + key(3) at 300
    }

    #[test]
    fn replay_duplicate_redo_idempotent() {
        let mut idx = UnminedIndex::new();
        let entry = UnminedRedoEntry {
            txid: key(1).txid,
            old_height: 0,
            new_height: 500,
        };
        idx.replay_redo(&entry);
        idx.replay_redo(&entry); // Replay again
        idx.replay_redo(&entry); // And again

        assert_eq!(idx.len(), 1);
        assert_eq!(idx.range_query(500), vec![key(1)]);
    }
}
