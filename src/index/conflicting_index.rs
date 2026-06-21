//! Conflicting secondary index.
//!
//! Tracks the set of transactions carrying the CONFLICTING metadata flag
//! ([`crate::record::TxFlags::CONFLICTING`], bit `0x02`). The conflict-query
//! op (`OP_QUERY_CONFLICTING`) iterates this set to return every conflicting
//! txid to a caller (Teranode's `GetConflictingTxIterator`).
//!
//! Unlike the unmined / DAH secondaries, this index carries **no** redo-log
//! durability and **no** redb backing — it is a plain in-memory set. It is
//! rebuilt at startup from the primary index
//! (see [`crate::ops::engine::Engine::rebuild_conflicting_index`]), so a crash simply
//! re-derives it from the authoritative on-device CONFLICTING flags. The
//! conflicting flag is only ever toggled by `create` (with the flag set) and
//! `set_conflicting`, and cleared by `delete`; those are the maintenance
//! points (see the engine hooks), so the set stays consistent with the
//! primary index without any two-phase write machinery.

use crate::index::TxKey;
use std::collections::HashSet;

/// In-memory set of transactions with the CONFLICTING flag set.
#[derive(Default)]
pub struct ConflictingIndex {
    keys: HashSet<TxKey>,
}

impl ConflictingIndex {
    /// Create an empty conflicting index.
    pub fn new() -> Self {
        Self {
            keys: HashSet::new(),
        }
    }

    /// Mark `key` as conflicting. Idempotent (a repeated insert is a no-op).
    pub fn insert(&mut self, key: TxKey) {
        self.keys.insert(key);
    }

    /// Clear the conflicting mark for `key`. Idempotent (a no-op if absent).
    pub fn remove(&mut self, key: &TxKey) {
        self.keys.remove(key);
    }

    /// Whether `key` is currently marked conflicting.
    pub fn contains(&self, key: &TxKey) -> bool {
        self.keys.contains(key)
    }

    /// Iterate all conflicting txids (unordered).
    pub fn iter(&self) -> impl Iterator<Item = TxKey> + '_ {
        self.keys.iter().copied()
    }

    /// Number of conflicting transactions tracked.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Remove all entries. Used before a full rebuild so re-running the
    /// rebuild is idempotent.
    pub fn clear(&mut self) {
        self.keys.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(b: u8) -> TxKey {
        TxKey { txid: [b; 32] }
    }

    #[test]
    fn insert_then_contains_and_len() {
        let mut idx = ConflictingIndex::new();
        assert!(idx.is_empty());
        idx.insert(key(1));
        idx.insert(key(2));
        assert_eq!(idx.len(), 2);
        assert!(idx.contains(&key(1)));
        assert!(idx.contains(&key(2)));
        assert!(!idx.contains(&key(3)));
    }

    #[test]
    fn insert_is_idempotent() {
        let mut idx = ConflictingIndex::new();
        idx.insert(key(7));
        idx.insert(key(7));
        idx.insert(key(7));
        assert_eq!(idx.len(), 1);
        assert!(idx.contains(&key(7)));
    }

    #[test]
    fn remove_clears_and_is_idempotent_when_absent() {
        let mut idx = ConflictingIndex::new();
        idx.insert(key(5));
        idx.remove(&key(5));
        assert!(!idx.contains(&key(5)));
        assert_eq!(idx.len(), 0);
        // Removing an absent key is a no-op, not an error.
        idx.remove(&key(5));
        idx.remove(&key(9));
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn iter_yields_exactly_the_inserted_set() {
        let mut idx = ConflictingIndex::new();
        for b in [10u8, 20, 30] {
            idx.insert(key(b));
        }
        let mut got: Vec<[u8; 32]> = idx.iter().map(|k| k.txid).collect();
        got.sort();
        let mut want = vec![[10u8; 32], [20u8; 32], [30u8; 32]];
        want.sort();
        assert_eq!(got, want);
    }

    #[test]
    fn clear_empties_for_rebuild() {
        let mut idx = ConflictingIndex::new();
        idx.insert(key(1));
        idx.insert(key(2));
        idx.clear();
        assert!(idx.is_empty());
        assert_eq!(idx.len(), 0);
        // Rebuild-style re-population works after clear.
        idx.insert(key(3));
        assert_eq!(idx.len(), 1);
        assert!(idx.contains(&key(3)));
    }
}
