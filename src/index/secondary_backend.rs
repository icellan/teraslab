//! Secondary index backend abstractions.
//!
//! Separate enums for DAH and unmined because they have different return types
//! (`UnminedIndex::insert` returns `UnminedRedoEntry`, `DahIndex::insert` does not).

use crate::index::dah_index::DahIndex;
use crate::index::hashtable::TxKey;
use crate::index::redb_dah::RedbDahIndex;
use crate::index::redb_unmined::RedbUnminedIndex;
use crate::index::unmined_index::{UnminedIndex, UnminedRedoEntry};

// ---------------------------------------------------------------------------
// Enum iterators (concrete dispatch, matching PrimaryIter pattern)
// ---------------------------------------------------------------------------

/// Iterator over all `(height, TxKey)` pairs from a DAH backend.
pub enum DahIter<'a> {
    /// In-memory index iterator (opaque `impl Iterator`).
    InMemory(Box<dyn Iterator<Item = (u32, TxKey)> + 'a>),
    /// Collected entries from on-disk backend.
    Collected(std::vec::IntoIter<(u32, TxKey)>),
}

impl Iterator for DahIter<'_> {
    type Item = (u32, TxKey);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::InMemory(it) => it.next(),
            Self::Collected(it) => it.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::InMemory(it) => it.size_hint(),
            Self::Collected(it) => it.size_hint(),
        }
    }
}

/// Iterator over all `(height, TxKey)` pairs from an unmined backend.
pub enum UnminedIter<'a> {
    /// In-memory index iterator (opaque `impl Iterator`).
    InMemory(Box<dyn Iterator<Item = (u32, TxKey)> + 'a>),
    /// Collected entries from on-disk backend.
    Collected(std::vec::IntoIter<(u32, TxKey)>),
}

impl Iterator for UnminedIter<'_> {
    type Item = (u32, TxKey);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::InMemory(it) => it.next(),
            Self::Collected(it) => it.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::InMemory(it) => it.size_hint(),
            Self::Collected(it) => it.size_hint(),
        }
    }
}

// ---------------------------------------------------------------------------
// DAH backend
// ---------------------------------------------------------------------------

/// DAH (delete-at-height) secondary index backend.
///
/// Uses enum dispatch for zero overhead on the in-memory path.
pub enum DahBackend {
    /// In-memory BTreeMap + HashMap (default).
    InMemory(DahIndex),
    /// On-disk via redb.
    OnDisk(RedbDahIndex),
}

impl std::fmt::Debug for DahBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InMemory(_) => f.write_str("DahBackend::InMemory"),
            Self::OnDisk(_) => f.write_str("DahBackend::OnDisk(redb)"),
        }
    }
}

impl DahBackend {
    /// Create a new empty in-memory DAH backend.
    pub fn new_in_memory() -> Self {
        Self::InMemory(DahIndex::new())
    }

    /// Insert a transaction into the DAH index.
    pub fn insert(&mut self, height: u32, key: TxKey) {
        match self {
            Self::InMemory(idx) => idx.insert(height, key),
            Self::OnDisk(redb) => redb.insert(height, key),
        }
    }

    /// Remove a transaction from the DAH index.
    pub fn remove(&mut self, key: &TxKey) {
        match self {
            Self::InMemory(idx) => idx.remove(key),
            Self::OnDisk(redb) => redb.remove(key),
        }
    }

    /// Return all txids with delete_at_height in `[0, current_height]`.
    pub fn range_query(&self, current_height: u32) -> Vec<TxKey> {
        match self {
            Self::InMemory(idx) => idx.range_query(current_height),
            Self::OnDisk(redb) => redb.range_query(current_height),
        }
    }

    /// Number of entries in the index.
    pub fn len(&self) -> usize {
        match self {
            Self::InMemory(idx) => idx.len(),
            Self::OnDisk(redb) => redb.len(),
        }
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::InMemory(idx) => idx.is_empty(),
            Self::OnDisk(redb) => redb.is_empty(),
        }
    }

    /// Remove all entries.
    pub fn clear(&mut self) {
        match self {
            Self::InMemory(idx) => idx.clear(),
            Self::OnDisk(redb) => redb.clear(),
        }
    }

    /// Iterate over all `(height, key)` pairs (for snapshot/export).
    pub fn iter(&self) -> DahIter<'_> {
        match self {
            Self::InMemory(idx) => DahIter::InMemory(Box::new(idx.iter())),
            Self::OnDisk(redb) => DahIter::Collected(redb.iter().into_iter()),
        }
    }
}

impl Default for DahBackend {
    fn default() -> Self {
        Self::new_in_memory()
    }
}

impl From<DahIndex> for DahBackend {
    fn from(idx: DahIndex) -> Self {
        Self::InMemory(idx)
    }
}

// ---------------------------------------------------------------------------
// Unmined backend
// ---------------------------------------------------------------------------

/// Unmined secondary index backend.
///
/// Uses enum dispatch for zero overhead on the in-memory path.
pub enum UnminedBackend {
    /// In-memory BTreeMap + HashMap (default).
    InMemory(UnminedIndex),
    /// On-disk via redb.
    OnDisk(RedbUnminedIndex),
}

impl std::fmt::Debug for UnminedBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InMemory(_) => f.write_str("UnminedBackend::InMemory"),
            Self::OnDisk(_) => f.write_str("UnminedBackend::OnDisk(redb)"),
        }
    }
}

impl UnminedBackend {
    /// Create a new empty in-memory unmined backend.
    pub fn new_in_memory() -> Self {
        Self::InMemory(UnminedIndex::new())
    }

    /// Insert a transaction into the unmined index.
    ///
    /// Returns an [`UnminedRedoEntry`] that MUST be written to the redo log.
    pub fn insert(&mut self, height: u32, key: TxKey) -> UnminedRedoEntry {
        match self {
            Self::InMemory(idx) => idx.insert(height, key),
            Self::OnDisk(redb) => redb.insert(height, key),
        }
    }

    /// Remove a transaction from the unmined index.
    ///
    /// Returns an [`UnminedRedoEntry`] that MUST be written to the redo log.
    pub fn remove(&mut self, key: &TxKey) -> UnminedRedoEntry {
        match self {
            Self::InMemory(idx) => idx.remove(key),
            Self::OnDisk(redb) => redb.remove(key),
        }
    }

    /// Return all txids with unmined_since in `[0, cutoff_height]`.
    pub fn range_query(&self, cutoff_height: u32) -> Vec<TxKey> {
        match self {
            Self::InMemory(idx) => idx.range_query(cutoff_height),
            Self::OnDisk(redb) => redb.range_query(cutoff_height),
        }
    }

    /// Number of entries in the index.
    pub fn len(&self) -> usize {
        match self {
            Self::InMemory(idx) => idx.len(),
            Self::OnDisk(redb) => redb.len(),
        }
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::InMemory(idx) => idx.is_empty(),
            Self::OnDisk(redb) => redb.is_empty(),
        }
    }

    /// Remove all entries.
    pub fn clear(&mut self) {
        match self {
            Self::InMemory(idx) => idx.clear(),
            Self::OnDisk(redb) => redb.clear(),
        }
    }

    /// Iterate over all `(height, key)` pairs (for snapshot/export).
    pub fn iter(&self) -> UnminedIter<'_> {
        match self {
            Self::InMemory(idx) => UnminedIter::InMemory(Box::new(idx.iter())),
            Self::OnDisk(redb) => UnminedIter::Collected(redb.iter().into_iter()),
        }
    }

    /// Replay a redo entry to bring the index up to date.
    pub fn replay_redo(&mut self, entry: &UnminedRedoEntry) {
        match self {
            Self::InMemory(idx) => idx.replay_redo(entry),
            Self::OnDisk(redb) => redb.replay_redo(entry),
        }
    }
}

impl Default for UnminedBackend {
    fn default() -> Self {
        Self::new_in_memory()
    }
}

impl From<UnminedIndex> for UnminedBackend {
    fn from(idx: UnminedIndex) -> Self {
        Self::InMemory(idx)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::redb_dah::RedbDahIndex;
    use crate::index::redb_unmined::RedbUnminedIndex;

    fn key(n: u8) -> TxKey {
        let mut txid = [0u8; 32];
        txid[0] = n;
        TxKey { txid }
    }

    /// Helper to run the same test body against both InMemory and OnDisk DAH backends.
    fn with_both_dah_backends(f: impl Fn(&mut DahBackend)) {
        // In-memory
        let mut mem = DahBackend::new_in_memory();
        f(&mut mem);

        // On-disk
        let dir = tempfile::tempdir().unwrap();
        let redb = RedbDahIndex::open(dir.path().join("dah.redb").as_path(), 16 * 1024 * 1024).unwrap();
        let mut disk = DahBackend::OnDisk(redb);
        f(&mut disk);
    }

    /// Helper to run the same test body against both InMemory and OnDisk unmined backends.
    fn with_both_unmined_backends(f: impl Fn(&mut UnminedBackend)) {
        // In-memory
        let mut mem = UnminedBackend::new_in_memory();
        f(&mut mem);

        // On-disk
        let dir = tempfile::tempdir().unwrap();
        let redb = RedbUnminedIndex::open(dir.path().join("unmined.redb").as_path(), 16 * 1024 * 1024).unwrap();
        let mut disk = UnminedBackend::OnDisk(redb);
        f(&mut disk);
    }

    // -----------------------------------------------------------------------
    // DahBackend: parameterized tests
    // -----------------------------------------------------------------------

    #[test]
    fn dah_both_insert_and_range_query() {
        with_both_dah_backends(|backend| {
            backend.insert(100, key(1));
            backend.insert(200, key(2));
            backend.insert(300, key(3));

            assert_eq!(backend.len(), 3);
            assert!(!backend.is_empty());

            let result = backend.range_query(200);
            assert_eq!(result.len(), 2);
            assert!(result.contains(&key(1)));
            assert!(result.contains(&key(2)));

            // Above all heights
            let result = backend.range_query(300);
            assert_eq!(result.len(), 3);

            // Below all heights
            let result = backend.range_query(99);
            assert!(result.is_empty());
        });
    }

    #[test]
    fn dah_both_insert_updates_height() {
        with_both_dah_backends(|backend| {
            backend.insert(100, key(1));
            backend.insert(200, key(1)); // Move to new height
            assert_eq!(backend.len(), 1);

            let result = backend.range_query(100);
            assert!(result.is_empty());
            let result = backend.range_query(200);
            assert_eq!(result.len(), 1);
        });
    }

    #[test]
    fn dah_both_remove() {
        with_both_dah_backends(|backend| {
            backend.insert(100, key(1));
            backend.insert(200, key(2));
            backend.remove(&key(1));

            assert_eq!(backend.len(), 1);
            let result = backend.range_query(300);
            assert_eq!(result.len(), 1);
            assert_eq!(result[0], key(2));

            // Remove missing is no-op
            backend.remove(&key(99));
            assert_eq!(backend.len(), 1);
        });
    }

    #[test]
    fn dah_both_clear() {
        with_both_dah_backends(|backend| {
            backend.insert(100, key(1));
            backend.insert(200, key(2));
            backend.clear();
            assert!(backend.is_empty());
            assert!(backend.range_query(1000).is_empty());
        });
    }

    #[test]
    fn dah_both_iter() {
        with_both_dah_backends(|backend| {
            backend.insert(100, key(1));
            backend.insert(200, key(2));
            backend.insert(300, key(3));

            let entries: Vec<_> = backend.iter().collect();
            assert_eq!(entries.len(), 3);
        });
    }

    #[test]
    fn dah_default_is_in_memory() {
        let backend = DahBackend::default();
        assert!(backend.is_empty());
    }

    #[test]
    fn dah_from_dah_index() {
        let mut idx = DahIndex::new();
        idx.insert(100, key(1));
        let backend: DahBackend = idx.into();
        assert_eq!(backend.len(), 1);
    }

    #[test]
    fn dah_debug_format() {
        let mem = DahBackend::new_in_memory();
        assert!(format!("{mem:?}").contains("InMemory"));

        let dir = tempfile::tempdir().unwrap();
        let redb = RedbDahIndex::open(dir.path().join("dah.redb").as_path(), 16 * 1024 * 1024).unwrap();
        let disk = DahBackend::OnDisk(redb);
        assert!(format!("{disk:?}").contains("OnDisk"));
    }

    // -----------------------------------------------------------------------
    // UnminedBackend: parameterized tests
    // -----------------------------------------------------------------------

    #[test]
    fn unmined_both_insert_returns_redo_entry() {
        with_both_unmined_backends(|backend| {
            let redo = backend.insert(100, key(1));
            assert_eq!(redo.old_height, 0);
            assert_eq!(redo.new_height, 100);
            assert_eq!(redo.txid[0], 1);

            assert_eq!(backend.len(), 1);
            assert!(!backend.is_empty());
        });
    }

    #[test]
    fn unmined_both_insert_update_returns_old_height() {
        with_both_unmined_backends(|backend| {
            backend.insert(100, key(1));
            let redo = backend.insert(200, key(1));
            assert_eq!(redo.old_height, 100);
            assert_eq!(redo.new_height, 200);
            assert_eq!(backend.len(), 1);
        });
    }

    #[test]
    fn unmined_both_remove_returns_redo_entry() {
        with_both_unmined_backends(|backend| {
            backend.insert(100, key(1));
            let redo = backend.remove(&key(1));
            assert_eq!(redo.old_height, 100);
            assert_eq!(redo.new_height, 0);
            assert!(backend.is_empty());

            // Remove missing
            let redo = backend.remove(&key(99));
            assert_eq!(redo.old_height, 0);
            assert_eq!(redo.new_height, 0);
        });
    }

    #[test]
    fn unmined_both_range_query() {
        with_both_unmined_backends(|backend| {
            backend.insert(100, key(1));
            backend.insert(200, key(2));
            backend.insert(300, key(3));

            let result = backend.range_query(200);
            assert_eq!(result.len(), 2);
            assert!(result.contains(&key(1)));
            assert!(result.contains(&key(2)));
        });
    }

    #[test]
    fn unmined_both_clear() {
        with_both_unmined_backends(|backend| {
            backend.insert(100, key(1));
            backend.insert(200, key(2));
            backend.clear();
            assert!(backend.is_empty());
        });
    }

    #[test]
    fn unmined_both_iter() {
        with_both_unmined_backends(|backend| {
            backend.insert(100, key(1));
            backend.insert(200, key(2));

            let entries: Vec<_> = backend.iter().collect();
            assert_eq!(entries.len(), 2);
        });
    }

    #[test]
    fn unmined_both_replay_redo() {
        with_both_unmined_backends(|backend| {
            // Replay insert
            let entry = UnminedRedoEntry {
                txid: key(1).txid,
                old_height: 0,
                new_height: 500,
            };
            backend.replay_redo(&entry);
            assert_eq!(backend.len(), 1);

            let result = backend.range_query(500);
            assert_eq!(result.len(), 1);
            assert_eq!(result[0], key(1));

            // Replay remove
            let entry = UnminedRedoEntry {
                txid: key(1).txid,
                old_height: 500,
                new_height: 0,
            };
            backend.replay_redo(&entry);
            assert!(backend.is_empty());
        });
    }

    #[test]
    fn unmined_default_is_in_memory() {
        let backend = UnminedBackend::default();
        assert!(backend.is_empty());
    }

    #[test]
    fn unmined_from_unmined_index() {
        let mut idx = UnminedIndex::new();
        idx.insert(100, key(1));
        let backend: UnminedBackend = idx.into();
        assert_eq!(backend.len(), 1);
    }

    #[test]
    fn dah_iter_size_hint() {
        // On-disk (Collected variant) gives exact bounds
        let dir = tempfile::tempdir().unwrap();
        let mut redb = RedbDahIndex::open(dir.path().join("dah.redb").as_path(), 16 * 1024 * 1024).unwrap();
        redb.insert(100, key(1));
        redb.insert(200, key(2));
        redb.insert(300, key(3));
        let disk = DahBackend::OnDisk(redb);
        let iter = disk.iter();
        let (lower, upper) = iter.size_hint();
        assert_eq!(lower, 3);
        assert_eq!(upper, Some(3));
    }

    #[test]
    fn unmined_iter_size_hint() {
        // On-disk (Collected variant) gives exact bounds
        let dir = tempfile::tempdir().unwrap();
        let mut redb = RedbUnminedIndex::open(dir.path().join("unmined.redb").as_path(), 16 * 1024 * 1024).unwrap();
        redb.insert(100, key(1));
        redb.insert(200, key(2));
        let disk = UnminedBackend::OnDisk(redb);
        let iter = disk.iter();
        let (lower, upper) = iter.size_hint();
        assert_eq!(lower, 2);
        assert_eq!(upper, Some(2));
    }

    #[test]
    fn unmined_debug_format() {
        let mem = UnminedBackend::new_in_memory();
        assert!(format!("{mem:?}").contains("InMemory"));

        let dir = tempfile::tempdir().unwrap();
        let redb = RedbUnminedIndex::open(dir.path().join("unmined.redb").as_path(), 16 * 1024 * 1024).unwrap();
        let disk = UnminedBackend::OnDisk(redb);
        assert!(format!("{disk:?}").contains("OnDisk"));
    }
}
