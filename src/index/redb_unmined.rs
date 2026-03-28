//! ReDB-backed unmined secondary index.
//!
//! Same dual-table structure as the DAH index but returns `UnminedRedoEntry`
//! from insert/remove for redo log persistence.

use crate::index::hashtable::TxKey;
use crate::index::unmined_index::UnminedRedoEntry;
use crate::index::IndexError;
use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use std::path::Path;

/// Forward table: `[height_be(4) || txid(32)]` -> `()`
const UNMINED_FORWARD: TableDefinition<[u8; 36], ()> =
    TableDefinition::new("unmined_forward");

/// Reverse table: `txid(32)` -> `height_le(4)`
const UNMINED_REVERSE: TableDefinition<[u8; 32], [u8; 4]> =
    TableDefinition::new("unmined_reverse");

/// ReDB-backed unmined secondary index.
pub struct RedbUnminedIndex {
    db: Database,
    count: usize,
}

impl RedbUnminedIndex {
    /// Start a write transaction with eventual durability.
    ///
    /// TeraSlab's redo log provides crash recovery, so per-operation fsync
    /// is unnecessary. See [`RedbPrimary::begin_write`] for rationale.
    #[allow(clippy::result_large_err)] // redb::TransactionError is external; we cannot shrink it
    fn begin_write(&self) -> Result<redb::WriteTransaction, redb::TransactionError> {
        let mut txn = self.db.begin_write()?;
        txn.set_durability(redb::Durability::Eventual);
        Ok(txn)
    }

    /// Open or create a redb unmined index.
    ///
    /// Shares the same database file as the DAH index (different tables).
    pub fn open(path: &Path, cache_size: usize) -> Result<Self, IndexError> {
        let db = redb::Builder::new()
            .set_cache_size(cache_size)
            .create(path)
            .map_err(|e| IndexError::FormatError {
                detail: format!("redb open error (unmined): {e}"),
            })?;

        {
            let mut txn = db.begin_write().map_err(map_txn_err)?;
            txn.set_durability(redb::Durability::Eventual);
            txn.open_table(UNMINED_FORWARD).map_err(map_table_err)?;
            txn.open_table(UNMINED_REVERSE).map_err(map_table_err)?;
            txn.commit().map_err(map_commit_err)?;
        }

        let count = {
            let txn = db.begin_read().map_err(map_txn_err)?;
            let table = txn.open_table(UNMINED_REVERSE).map_err(map_table_err)?;
            table.len().map_err(map_storage_err)? as usize
        };

        Ok(Self { db, count })
    }

    /// Look up the current height for a key using a cheap read transaction.
    ///
    /// Returns `None` if the key is not in the index.
    pub fn get_height(&self, key: &TxKey) -> Option<u32> {
        let txn = self.db.begin_read().ok()?;
        let table = txn.open_table(UNMINED_REVERSE).ok()?;
        let guard = table.get(key.txid).ok()??;
        Some(u32::from_le_bytes(guard.value()))
    }

    /// Insert a transaction into the unmined index.
    ///
    /// Returns an [`UnminedRedoEntry`] that MUST be written to the redo log.
    ///
    /// On I/O errors the write is not committed and a **no-op** redo entry
    /// (`old_height == new_height`) is returned so that replaying it is
    /// harmless and does not create index/data inconsistency.
    pub fn insert(&mut self, height: u32, key: TxKey) -> UnminedRedoEntry {
        // Read old_height before attempting the write so we have the correct
        // value even if the write transaction fails.
        let old_height = self.get_height(&key).unwrap_or(0);

        // No-op redo entry returned on any I/O failure — replaying it is
        // harmless because old_height == new_height means "nothing changed".
        let noop = UnminedRedoEntry {
            txid: key.txid,
            old_height,
            new_height: old_height,
        };

        let txn = match self.begin_write() {
            Ok(t) => t,
            Err(e) => {
                eprintln!("redb unmined insert: begin_write failed: {e}");
                return noop;
            }
        };
        let was_new;
        {
            let mut fwd = match txn.open_table(UNMINED_FORWARD) {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("redb unmined insert: open_table(forward) failed: {e}");
                    return noop;
                }
            };
            let mut rev = match txn.open_table(UNMINED_REVERSE) {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("redb unmined insert: open_table(reverse) failed: {e}");
                    return noop;
                }
            };

            match rev.get(key.txid) {
                Ok(Some(guard)) => {
                    let existing_height = u32::from_le_bytes(guard.value());
                    drop(guard);
                    if existing_height == height {
                        return UnminedRedoEntry {
                            txid: key.txid,
                            old_height,
                            new_height: height,
                        };
                    }
                    was_new = false;
                    let old_fwd_key = make_forward_key(existing_height, &key);
                    if let Err(e) = fwd.remove(old_fwd_key) {
                        eprintln!("redb unmined insert: remove old forward entry failed: {e}");
                        return noop;
                    }
                }
                Ok(None) => {
                    was_new = true;
                }
                Err(e) => {
                    eprintln!("redb unmined insert: reverse lookup failed: {e}");
                    return noop;
                }
            }

            if let Err(e) = rev.insert(key.txid, height.to_le_bytes()) {
                eprintln!("redb unmined insert: reverse insert failed: {e}");
                return noop;
            }
            if let Err(e) = fwd.insert(make_forward_key(height, &key), ()) {
                eprintln!("redb unmined insert: forward insert failed: {e}");
                return noop;
            }
        }
        match txn.commit() {
            Ok(()) => {
                if was_new {
                    self.count += 1;
                }
            }
            Err(e) => {
                eprintln!("redb unmined insert: commit failed: {e}");
                return noop;
            }
        }

        UnminedRedoEntry {
            txid: key.txid,
            old_height,
            new_height: height,
        }
    }

    /// Remove a transaction from the unmined index.
    ///
    /// Returns an [`UnminedRedoEntry`] that MUST be written to the redo log.
    ///
    /// On I/O errors the write is not committed and a **no-op** redo entry
    /// (`old_height == new_height`) is returned so that replaying it is
    /// harmless and does not create index/data inconsistency.
    pub fn remove(&mut self, key: &TxKey) -> UnminedRedoEntry {
        // Read old_height before attempting the write for correct redo entry.
        let old_height = self.get_height(key).unwrap_or(0);

        // No-op redo entry returned on any I/O failure.
        let noop = UnminedRedoEntry {
            txid: key.txid,
            old_height,
            new_height: old_height,
        };

        let txn = match self.begin_write() {
            Ok(t) => t,
            Err(e) => {
                eprintln!("redb unmined remove: begin_write failed: {e}");
                return noop;
            }
        };
        let had_entry;
        {
            let mut fwd = match txn.open_table(UNMINED_FORWARD) {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("redb unmined remove: open_table(forward) failed: {e}");
                    return noop;
                }
            };
            let mut rev = match txn.open_table(UNMINED_REVERSE) {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("redb unmined remove: open_table(reverse) failed: {e}");
                    return noop;
                }
            };

            had_entry = match rev.remove(key.txid) {
                Ok(Some(guard)) => {
                    let h = u32::from_le_bytes(guard.value());
                    if let Err(e) = fwd.remove(make_forward_key(h, key)) {
                        eprintln!("redb unmined remove: forward remove failed: {e}");
                        return noop;
                    }
                    true
                }
                Ok(None) => false,
                Err(e) => {
                    eprintln!("redb unmined remove: reverse remove failed: {e}");
                    return noop;
                }
            };
        }
        match txn.commit() {
            Ok(()) => {
                if had_entry {
                    self.count -= 1;
                }
            }
            Err(e) => {
                eprintln!("redb unmined remove: commit failed: {e}");
                return noop;
            }
        }

        UnminedRedoEntry {
            txid: key.txid,
            old_height,
            new_height: 0,
        }
    }

    /// Return all txids with unmined_since in `[0, cutoff_height]`.
    pub fn range_query(&self, cutoff_height: u32) -> Vec<TxKey> {
        let mut result = Vec::new();
        let txn = match self.db.begin_read() {
            Ok(t) => t,
            Err(_) => return result,
        };
        let table = match txn.open_table(UNMINED_FORWARD) {
            Ok(t) => t,
            Err(_) => return result,
        };

        let start = [0u8; 36];
        let end = make_forward_key(cutoff_height, &TxKey { txid: [0xFF; 32] });

        if let Ok(range) = table.range(start..=end) {
            for (k, _) in range.flatten() {
                let composite = k.value();
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&composite[4..36]);
                result.push(TxKey { txid });
            }
        }
        result
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.count
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Remove all entries.
    ///
    /// Uses table drop + recreate for O(1) memory instead of row-by-row deletion.
    pub fn clear(&mut self) {
        if let Ok(txn) = self.begin_write() {
            // Drop and recreate tables — O(1) memory regardless of entry count.
            let _ = txn.delete_table(UNMINED_FORWARD);
            let _ = txn.delete_table(UNMINED_REVERSE);
            let _ = txn.open_table(UNMINED_FORWARD);
            let _ = txn.open_table(UNMINED_REVERSE);
            let _ = txn.commit();
        }
        self.count = 0;
    }

    /// Insert multiple transactions in a single write transaction.
    ///
    /// Much faster than calling [`insert`](Self::insert) in a loop because
    /// only one fsync is performed for the entire batch. No redo entries are
    /// returned (bulk import does not need them).
    pub fn insert_batch(&mut self, entries: &[(u32, TxKey)]) {
        if entries.is_empty() {
            return;
        }
        let txn = match self.begin_write() {
            Ok(t) => t,
            Err(e) => {
                eprintln!("redb unmined insert_batch: begin_write failed: {e}");
                return;
            }
        };
        let mut new_count = 0usize;
        {
            let mut fwd = match txn.open_table(UNMINED_FORWARD) {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("redb unmined insert_batch: open_table(forward) failed: {e}");
                    return;
                }
            };
            let mut rev = match txn.open_table(UNMINED_REVERSE) {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("redb unmined insert_batch: open_table(reverse) failed: {e}");
                    return;
                }
            };

            for &(height, key) in entries {
                let mut already_exists = false;
                match rev.get(key.txid) {
                    Ok(Some(guard)) => {
                        let old_height = u32::from_le_bytes(guard.value());
                        drop(guard);
                        if old_height == height {
                            continue;
                        }
                        already_exists = true;
                        if let Err(e) = fwd.remove(make_forward_key(old_height, &key)) {
                            eprintln!("redb unmined insert_batch: remove old forward failed: {e}");
                            return;
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        eprintln!("redb unmined insert_batch: reverse lookup failed: {e}");
                        return;
                    }
                }
                if !already_exists {
                    new_count += 1;
                }
                if let Err(e) = rev.insert(key.txid, height.to_le_bytes()) {
                    eprintln!("redb unmined insert_batch: reverse insert failed: {e}");
                    return;
                }
                if let Err(e) = fwd.insert(make_forward_key(height, &key), ()) {
                    eprintln!("redb unmined insert_batch: forward insert failed: {e}");
                    return;
                }
            }
        }
        match txn.commit() {
            Ok(()) => {
                self.count += new_count;
            }
            Err(e) => {
                eprintln!("redb unmined insert_batch: commit failed: {e}");
            }
        }
    }

    /// Iterate over all `(height, key)` pairs.
    pub fn iter(&self) -> Vec<(u32, TxKey)> {
        let mut result = Vec::with_capacity(self.count);
        if let Ok(txn) = self.db.begin_read()
            && let Ok(table) = txn.open_table(UNMINED_REVERSE)
            && let Ok(range) = table.iter()
        {
            for (k, v) in range.flatten() {
                let key = TxKey { txid: k.value() };
                let height = u32::from_le_bytes(v.value());
                result.push((height, key));
            }
        }
        result
    }

    /// Replay a redo entry to bring the index up to date.
    pub fn replay_redo(&mut self, entry: &UnminedRedoEntry) {
        let key = TxKey {
            txid: entry.txid,
        };
        if entry.new_height == 0 {
            self.remove(&key);
        } else {
            self.insert(entry.new_height, key);
        }
    }

}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_forward_key(height: u32, key: &TxKey) -> [u8; 36] {
    let mut buf = [0u8; 36];
    buf[0..4].copy_from_slice(&height.to_be_bytes());
    buf[4..36].copy_from_slice(&key.txid);
    buf
}

fn map_txn_err(e: redb::TransactionError) -> IndexError {
    IndexError::FormatError { detail: format!("redb txn error (unmined): {e}") }
}

fn map_table_err(e: redb::TableError) -> IndexError {
    IndexError::FormatError { detail: format!("redb table error (unmined): {e}") }
}

fn map_commit_err(e: redb::CommitError) -> IndexError {
    IndexError::FormatError { detail: format!("redb commit error (unmined): {e}") }
}

fn map_storage_err(e: redb::StorageError) -> IndexError {
    IndexError::FormatError { detail: format!("redb storage error (unmined): {e}") }
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

    fn open_temp() -> (tempfile::TempDir, RedbUnminedIndex) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("unmined.redb");
        let idx = RedbUnminedIndex::open(&db_path, 16 * 1024 * 1024).unwrap();
        (dir, idx)
    }

    #[test]
    fn insert_returns_redo_entry() {
        let (_dir, mut idx) = open_temp();
        let redo = idx.insert(100, key(1));
        assert_eq!(redo.old_height, 0);
        assert_eq!(redo.new_height, 100);
        assert_eq!(redo.txid[0], 1);
    }

    #[test]
    fn insert_update_returns_old_height() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1));
        let redo = idx.insert(200, key(1));
        assert_eq!(redo.old_height, 100);
        assert_eq!(redo.new_height, 200);
    }

    #[test]
    fn insert_same_height_returns_same() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1));
        let redo = idx.insert(100, key(1));
        assert_eq!(redo.old_height, 100);
        assert_eq!(redo.new_height, 100);
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn remove_returns_redo_entry() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1));
        let redo = idx.remove(&key(1));
        assert_eq!(redo.old_height, 100);
        assert_eq!(redo.new_height, 0);
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn remove_missing_returns_zero() {
        let (_dir, mut idx) = open_temp();
        let redo = idx.remove(&key(99));
        assert_eq!(redo.old_height, 0);
        assert_eq!(redo.new_height, 0);
    }

    #[test]
    fn range_query() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1));
        idx.insert(200, key(2));
        idx.insert(300, key(3));

        let result = idx.range_query(200);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&key(1)));
        assert!(result.contains(&key(2)));
    }

    #[test]
    fn clear() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1));
        idx.insert(200, key(2));
        idx.clear();
        assert!(idx.is_empty());
    }

    #[test]
    fn replay_redo_insert() {
        let (_dir, mut idx) = open_temp();
        let entry = UnminedRedoEntry {
            txid: key(1).txid,
            old_height: 0,
            new_height: 500,
        };
        idx.replay_redo(&entry);
        assert_eq!(idx.len(), 1);
        let result = idx.range_query(500);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn replay_redo_remove() {
        let (_dir, mut idx) = open_temp();
        idx.insert(500, key(1));
        let entry = UnminedRedoEntry {
            txid: key(1).txid,
            old_height: 500,
            new_height: 0,
        };
        idx.replay_redo(&entry);
        assert!(idx.is_empty());
    }

    #[test]
    fn insert_count_incremental() {
        let (_dir, mut idx) = open_temp();
        assert_eq!(idx.len(), 0);
        idx.insert(100, key(1));
        assert_eq!(idx.len(), 1);
        idx.insert(200, key(2));
        assert_eq!(idx.len(), 2);
        idx.insert(300, key(3));
        assert_eq!(idx.len(), 3);
    }

    #[test]
    fn remove_count_incremental() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1));
        idx.insert(200, key(2));
        idx.insert(300, key(3));
        assert_eq!(idx.len(), 3);

        idx.remove(&key(2));
        assert_eq!(idx.len(), 2);
        idx.remove(&key(1));
        assert_eq!(idx.len(), 1);
        idx.remove(&key(3));
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn insert_update_does_not_change_count() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1));
        assert_eq!(idx.len(), 1);
        idx.insert(200, key(1)); // Update height, same key
        assert_eq!(idx.len(), 1);
        idx.insert(300, key(1)); // Another update
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn insert_batch_basic() {
        let (_dir, mut idx) = open_temp();
        let entries: Vec<_> = (1..=10u8).map(|n| (n as u32 * 100, key(n))).collect();
        idx.insert_batch(&entries);
        assert_eq!(idx.len(), 10);

        let result = idx.range_query(1000);
        assert_eq!(result.len(), 10);
    }

    #[test]
    fn insert_batch_with_duplicates() {
        let (_dir, mut idx) = open_temp();
        let entries = vec![
            (100, key(1)),
            (200, key(1)), // same key, different height
        ];
        idx.insert_batch(&entries);
        assert_eq!(idx.len(), 1);

        // Should be at height 200
        assert!(idx.range_query(100).is_empty());
        assert_eq!(idx.range_query(200).len(), 1);
    }

    #[test]
    fn insert_batch_empty() {
        let (_dir, mut idx) = open_temp();
        idx.insert_batch(&[]);
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn insert_batch_matches_individual() {
        // Individual inserts
        let dir1 = tempfile::tempdir().unwrap();
        let mut idx1 = RedbUnminedIndex::open(dir1.path().join("unmined.redb").as_path(), 16 * 1024 * 1024).unwrap();
        for n in 1..=20u8 {
            idx1.insert(n as u32 * 100, key(n));
        }

        // Batch insert
        let dir2 = tempfile::tempdir().unwrap();
        let mut idx2 = RedbUnminedIndex::open(dir2.path().join("unmined.redb").as_path(), 16 * 1024 * 1024).unwrap();
        let entries: Vec<_> = (1..=20u8).map(|n| (n as u32 * 100, key(n))).collect();
        idx2.insert_batch(&entries);

        assert_eq!(idx1.len(), idx2.len());
        let r1 = idx1.range_query(2000);
        let r2 = idx2.range_query(2000);
        assert_eq!(r1.len(), r2.len());
    }

    #[test]
    fn get_height_returns_correct_value() {
        let (_dir, mut idx) = open_temp();
        assert!(idx.get_height(&key(1)).is_none());

        idx.insert(100, key(1));
        assert_eq!(idx.get_height(&key(1)), Some(100));

        idx.insert(200, key(1)); // Update
        assert_eq!(idx.get_height(&key(1)), Some(200));

        idx.remove(&key(1));
        assert!(idx.get_height(&key(1)).is_none());
    }

    #[test]
    fn persistence() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("unmined.redb");

        {
            let mut idx = RedbUnminedIndex::open(&db_path, 16 * 1024 * 1024).unwrap();
            idx.insert(100, key(1));
            idx.insert(200, key(2));
        }

        {
            let idx = RedbUnminedIndex::open(&db_path, 16 * 1024 * 1024).unwrap();
            assert_eq!(idx.len(), 2);
            let result = idx.range_query(200);
            assert_eq!(result.len(), 2);
        }
    }

    #[test]
    fn iter_returns_correct_height_key_pairs() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1));
        idx.insert(200, key(2));
        idx.insert(300, key(3));

        let entries = idx.iter();
        assert_eq!(entries.len(), 3);

        // Verify each (height, key) pair matches what was inserted
        for (height, k) in &entries {
            match k.txid[0] {
                1 => assert_eq!(*height, 100),
                2 => assert_eq!(*height, 200),
                3 => assert_eq!(*height, 300),
                other => panic!("unexpected txid byte: {other}"),
            }
        }
    }

    #[test]
    fn range_query_empty_index() {
        let (_dir, idx) = open_temp();
        assert!(idx.range_query(1000).is_empty());
        assert!(idx.range_query(0).is_empty());
    }

    #[test]
    fn range_query_below_all_heights() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1));
        idx.insert(200, key(2));

        let result = idx.range_query(99);
        assert!(result.is_empty());
    }

    #[test]
    fn range_query_above_all_heights() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1));
        idx.insert(200, key(2));
        idx.insert(300, key(3));

        let result = idx.range_query(u32::MAX);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn clear_then_reinsert() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1));
        idx.insert(200, key(2));
        idx.clear();
        assert!(idx.is_empty());
        assert!(idx.range_query(1000).is_empty());

        // New inserts work on a clean slate
        idx.insert(500, key(10));
        assert_eq!(idx.len(), 1);
        let result = idx.range_query(500);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], key(10));
    }

    #[test]
    fn remove_then_reinsert_tracks_redo_correctly() {
        let (_dir, mut idx) = open_temp();
        let redo1 = idx.insert(100, key(1));
        assert_eq!(redo1.old_height, 0);
        assert_eq!(redo1.new_height, 100);

        let redo2 = idx.remove(&key(1));
        assert_eq!(redo2.old_height, 100);
        assert_eq!(redo2.new_height, 0);
        assert!(idx.is_empty());

        // Re-insert: old_height should be 0 since we removed it
        let redo3 = idx.insert(500, key(1));
        assert_eq!(redo3.old_height, 0);
        assert_eq!(redo3.new_height, 500);
        assert_eq!(idx.len(), 1);

        let result = idx.range_query(500);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], key(1));
    }

    #[test]
    fn len_tracks_through_operations() {
        let (_dir, mut idx) = open_temp();

        assert_eq!(idx.len(), 0);
        assert!(idx.is_empty());

        idx.insert(100, key(1));
        assert_eq!(idx.len(), 1);
        assert!(!idx.is_empty());

        idx.insert(200, key(2));
        assert_eq!(idx.len(), 2);

        // Update existing key (different height) — count stays the same
        idx.insert(300, key(1));
        assert_eq!(idx.len(), 2);

        idx.remove(&key(1));
        assert_eq!(idx.len(), 1);

        idx.remove(&key(2));
        assert_eq!(idx.len(), 0);
        assert!(idx.is_empty());
    }

    #[test]
    fn persistence_after_clear() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("unmined.redb");

        {
            let mut idx = RedbUnminedIndex::open(&db_path, 16 * 1024 * 1024).unwrap();
            idx.insert(100, key(1));
            idx.insert(200, key(2));
            idx.clear();
        }

        let idx = RedbUnminedIndex::open(&db_path, 16 * 1024 * 1024).unwrap();
        assert!(idx.is_empty());
        assert!(idx.range_query(1000).is_empty());
    }

    #[test]
    fn multiple_entries_same_height() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1));
        idx.insert(100, key(2));
        idx.insert(100, key(3));

        assert_eq!(idx.len(), 3);
        let result = idx.range_query(100);
        assert_eq!(result.len(), 3);
        assert!(result.contains(&key(1)));
        assert!(result.contains(&key(2)));
        assert!(result.contains(&key(3)));

        // Query below should find none
        assert!(idx.range_query(99).is_empty());
    }

    #[test]
    fn large_height_values() {
        let (_dir, mut idx) = open_temp();
        idx.insert(u32::MAX, key(1));
        idx.insert(u32::MAX - 1, key(2));
        idx.insert(1, key(3));

        assert_eq!(idx.len(), 3);

        let result = idx.range_query(1);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], key(3));

        let result = idx.range_query(u32::MAX);
        assert_eq!(result.len(), 3);
    }
}
