//! ReDB-backed DAH (delete-at-height) secondary index.
//!
//! Uses two tables for O(1) lookup in both directions:
//! - Forward: composite key `height_be(4) || txid(32)` -> `()` (big-endian for correct sort)
//! - Reverse: `txid(32)` -> `height(4 LE)`

use crate::index::hashtable::TxKey;
use crate::index::IndexError;
use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use std::path::Path;

/// Forward table: `[height_be(4) || txid(32)]` -> `()`
const DAH_FORWARD: TableDefinition<[u8; 36], ()> = TableDefinition::new("dah_forward");

/// Reverse table: `txid(32)` -> `height_le(4)`
const DAH_REVERSE: TableDefinition<[u8; 32], [u8; 4]> = TableDefinition::new("dah_reverse");

/// ReDB-backed DAH secondary index.
pub struct RedbDahIndex {
    db: Database,
    count: usize,
}

impl RedbDahIndex {
    /// Start a write transaction with eventual durability.
    ///
    /// TeraSlab's redo log provides crash recovery, so per-operation fsync
    /// is unnecessary. See [`RedbPrimary::begin_write`] for rationale.
    #[allow(clippy::result_large_err)]
    fn begin_write(&self) -> Result<redb::WriteTransaction, redb::TransactionError> {
        let mut txn = self.db.begin_write()?;
        txn.set_durability(redb::Durability::Eventual);
        Ok(txn)
    }

    /// Open or create a redb DAH index.
    ///
    /// The database is shared with the unmined index (same file, different tables).
    pub fn open(path: &Path, cache_size: usize) -> Result<Self, IndexError> {
        let db = redb::Builder::new()
            .set_cache_size(cache_size)
            .create(path)
            .map_err(|e| IndexError::FormatError {
                detail: format!("redb open error (dah): {e}"),
            })?;

        // Ensure tables exist.
        {
            let mut txn = db.begin_write().map_err(map_txn_err)?;
            txn.set_durability(redb::Durability::Eventual);
            txn.open_table(DAH_FORWARD).map_err(map_table_err)?;
            txn.open_table(DAH_REVERSE).map_err(map_table_err)?;
            txn.commit().map_err(map_commit_err)?;
        }

        let count = {
            let txn = db.begin_read().map_err(map_txn_err)?;
            let table = txn.open_table(DAH_REVERSE).map_err(map_table_err)?;
            table.len().map_err(map_storage_err)? as usize
        };

        Ok(Self { db, count })
    }

    /// Insert a transaction into the DAH index.
    ///
    /// If the txid already has a DAH entry at a different height, the old
    /// entry is removed first. Logs and returns early on I/O errors without
    /// updating the cached count, keeping it consistent with committed state.
    pub fn insert(&mut self, height: u32, key: TxKey) {
        let txn = match self.begin_write() {
            Ok(t) => t,
            Err(e) => {
                eprintln!("redb dah insert: begin_write failed: {e}");
                return;
            }
        };
        let was_new;
        {
            let mut fwd = match txn.open_table(DAH_FORWARD) {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("redb dah insert: open_table(forward) failed: {e}");
                    return;
                }
            };
            let mut rev = match txn.open_table(DAH_REVERSE) {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("redb dah insert: open_table(reverse) failed: {e}");
                    return;
                }
            };

            // Remove old entry if at a different height.
            let mut already_exists = false;
            match rev.get(key.txid) {
                Ok(Some(guard)) => {
                    let old_height = u32::from_le_bytes(guard.value());
                    drop(guard);
                    if old_height == height {
                        return; // Already at this height.
                    }
                    already_exists = true;
                    let old_fwd_key = make_forward_key(old_height, &key);
                    if let Err(e) = fwd.remove(old_fwd_key) {
                        eprintln!("redb dah insert: remove old forward entry failed: {e}");
                        return;
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    eprintln!("redb dah insert: reverse lookup failed: {e}");
                    return;
                }
            }
            was_new = !already_exists;

            if let Err(e) = rev.insert(key.txid, height.to_le_bytes()) {
                eprintln!("redb dah insert: reverse insert failed: {e}");
                return;
            }
            if let Err(e) = fwd.insert(make_forward_key(height, &key), ()) {
                eprintln!("redb dah insert: forward insert failed: {e}");
                return;
            }
        }
        match txn.commit() {
            Ok(()) => {
                if was_new {
                    self.count += 1;
                }
            }
            Err(e) => {
                eprintln!("redb dah insert: commit failed: {e}");
            }
        }
    }

    /// Remove a transaction from the DAH index.
    ///
    /// Logs and returns early on I/O errors without updating the cached
    /// count, keeping it consistent with committed state.
    pub fn remove(&mut self, key: &TxKey) {
        let txn = match self.begin_write() {
            Ok(t) => t,
            Err(e) => {
                eprintln!("redb dah remove: begin_write failed: {e}");
                return;
            }
        };
        let had_entry;
        {
            let mut fwd = match txn.open_table(DAH_FORWARD) {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("redb dah remove: open_table(forward) failed: {e}");
                    return;
                }
            };
            let mut rev = match txn.open_table(DAH_REVERSE) {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("redb dah remove: open_table(reverse) failed: {e}");
                    return;
                }
            };

            had_entry = match rev.remove(key.txid) {
                Ok(Some(guard)) => {
                    let height = u32::from_le_bytes(guard.value());
                    drop(guard);
                    if let Err(e) = fwd.remove(make_forward_key(height, key)) {
                        eprintln!("redb dah remove: forward remove failed: {e}");
                        return;
                    }
                    true
                }
                Ok(None) => false,
                Err(e) => {
                    eprintln!("redb dah remove: reverse remove failed: {e}");
                    return;
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
                eprintln!("redb dah remove: commit failed: {e}");
            }
        }
    }

    /// Return all txids with delete_at_height in `[0, current_height]`.
    pub fn range_query(&self, current_height: u32) -> Vec<TxKey> {
        let mut result = Vec::new();
        let txn = match self.db.begin_read() {
            Ok(t) => t,
            Err(_) => return result,
        };
        let table = match txn.open_table(DAH_FORWARD) {
            Ok(t) => t,
            Err(_) => return result,
        };

        let start = [0u8; 36];
        let end = make_forward_key(current_height, &TxKey { txid: [0xFF; 32] });

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
            let _ = txn.delete_table(DAH_FORWARD);
            let _ = txn.delete_table(DAH_REVERSE);
            let _ = txn.open_table(DAH_FORWARD);
            let _ = txn.open_table(DAH_REVERSE);
            let _ = txn.commit();
        }
        self.count = 0;
    }

    /// Insert multiple transactions in a single write transaction.
    ///
    /// Much faster than calling [`insert`](Self::insert) in a loop because
    /// only one fsync is performed for the entire batch.
    pub fn insert_batch(&mut self, entries: &[(u32, TxKey)]) {
        if entries.is_empty() {
            return;
        }
        let txn = match self.begin_write() {
            Ok(t) => t,
            Err(e) => {
                eprintln!("redb dah insert_batch: begin_write failed: {e}");
                return;
            }
        };
        let mut new_count = 0usize;
        {
            let mut fwd = match txn.open_table(DAH_FORWARD) {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("redb dah insert_batch: open_table(forward) failed: {e}");
                    return;
                }
            };
            let mut rev = match txn.open_table(DAH_REVERSE) {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("redb dah insert_batch: open_table(reverse) failed: {e}");
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
                            eprintln!("redb dah insert_batch: remove old forward failed: {e}");
                            return;
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        eprintln!("redb dah insert_batch: reverse lookup failed: {e}");
                        return;
                    }
                }
                if !already_exists {
                    new_count += 1;
                }
                if let Err(e) = rev.insert(key.txid, height.to_le_bytes()) {
                    eprintln!("redb dah insert_batch: reverse insert failed: {e}");
                    return;
                }
                if let Err(e) = fwd.insert(make_forward_key(height, &key), ()) {
                    eprintln!("redb dah insert_batch: forward insert failed: {e}");
                    return;
                }
            }
        }
        match txn.commit() {
            Ok(()) => {
                self.count += new_count;
            }
            Err(e) => {
                eprintln!("redb dah insert_batch: commit failed: {e}");
            }
        }
    }

    /// Iterate over all `(height, key)` pairs.
    pub fn iter(&self) -> Vec<(u32, TxKey)> {
        let mut result = Vec::with_capacity(self.count);
        if let Ok(txn) = self.db.begin_read()
            && let Ok(table) = txn.open_table(DAH_REVERSE)
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

}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create a composite forward key: `height_be(4) || txid(32)`.
/// Big-endian height ensures lexicographic ordering matches numeric ordering.
fn make_forward_key(height: u32, key: &TxKey) -> [u8; 36] {
    let mut buf = [0u8; 36];
    buf[0..4].copy_from_slice(&height.to_be_bytes());
    buf[4..36].copy_from_slice(&key.txid);
    buf
}

fn map_txn_err(e: redb::TransactionError) -> IndexError {
    IndexError::FormatError { detail: format!("redb txn error (dah): {e}") }
}

fn map_table_err(e: redb::TableError) -> IndexError {
    IndexError::FormatError { detail: format!("redb table error (dah): {e}") }
}

fn map_commit_err(e: redb::CommitError) -> IndexError {
    IndexError::FormatError { detail: format!("redb commit error (dah): {e}") }
}

fn map_storage_err(e: redb::StorageError) -> IndexError {
    IndexError::FormatError { detail: format!("redb storage error (dah): {e}") }
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

    fn open_temp() -> (tempfile::TempDir, RedbDahIndex) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("dah.redb");
        let idx = RedbDahIndex::open(&db_path, 16 * 1024 * 1024).unwrap();
        (dir, idx)
    }

    #[test]
    fn insert_and_range_query() {
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
    fn insert_updates_height() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1));
        idx.insert(200, key(1)); // Move to new height

        assert_eq!(idx.len(), 1);
        let result = idx.range_query(100);
        assert!(result.is_empty()); // No longer at 100
        let result = idx.range_query(200);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn insert_same_height_is_noop() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1));
        idx.insert(100, key(1));
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn remove() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1));
        idx.insert(200, key(2));
        idx.remove(&key(1));

        assert_eq!(idx.len(), 1);
        let result = idx.range_query(300);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], key(2));
    }

    #[test]
    fn remove_missing_is_noop() {
        let (_dir, mut idx) = open_temp();
        idx.remove(&key(99)); // Should not panic
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn clear() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1));
        idx.insert(200, key(2));
        idx.clear();
        assert!(idx.is_empty());
        assert!(idx.range_query(1000).is_empty());
    }

    #[test]
    fn iter_all() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1));
        idx.insert(200, key(2));
        idx.insert(300, key(3));

        let entries = idx.iter();
        assert_eq!(entries.len(), 3);
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
        let mut idx1 = RedbDahIndex::open(dir1.path().join("dah.redb").as_path(), 16 * 1024 * 1024).unwrap();
        for n in 1..=20u8 {
            idx1.insert(n as u32 * 100, key(n));
        }

        // Batch insert
        let dir2 = tempfile::tempdir().unwrap();
        let mut idx2 = RedbDahIndex::open(dir2.path().join("dah.redb").as_path(), 16 * 1024 * 1024).unwrap();
        let entries: Vec<_> = (1..=20u8).map(|n| (n as u32 * 100, key(n))).collect();
        idx2.insert_batch(&entries);

        assert_eq!(idx1.len(), idx2.len());
        // Both should return same range query results
        let r1 = idx1.range_query(2000);
        let r2 = idx2.range_query(2000);
        assert_eq!(r1.len(), r2.len());
    }

    #[test]
    fn persistence() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("dah.redb");

        {
            let mut idx = RedbDahIndex::open(&db_path, 16 * 1024 * 1024).unwrap();
            idx.insert(100, key(1));
            idx.insert(200, key(2));
        }

        {
            let idx = RedbDahIndex::open(&db_path, 16 * 1024 * 1024).unwrap();
            assert_eq!(idx.len(), 2);
            let result = idx.range_query(200);
            assert_eq!(result.len(), 2);
        }
    }

    #[test]
    fn range_query_empty() {
        let (_dir, idx) = open_temp();
        assert!(idx.range_query(1000).is_empty());
    }

    #[test]
    fn range_query_boundary() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1));
        idx.insert(100, key(2)); // Same height, different key

        let result = idx.range_query(100);
        assert_eq!(result.len(), 2);

        let result = idx.range_query(99);
        assert!(result.is_empty());
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
    fn clear_then_reinsert() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1));
        idx.insert(200, key(2));
        idx.clear();

        // After clear, old data is gone
        assert!(idx.range_query(1000).is_empty());

        // New inserts work on a clean slate
        idx.insert(500, key(10));
        assert_eq!(idx.len(), 1);
        let result = idx.range_query(500);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], key(10));

        // Old keys are not present
        assert!(idx.range_query(200).is_empty());
    }

    #[test]
    fn remove_then_reinsert_at_different_height() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1));
        idx.remove(&key(1));
        assert!(idx.is_empty());

        // Re-insert at a different height
        idx.insert(500, key(1));
        assert_eq!(idx.len(), 1);

        // Should only be found at the new height
        assert!(idx.range_query(100).is_empty());
        assert_eq!(idx.range_query(500).len(), 1);
    }

    #[test]
    fn large_height_values() {
        let (_dir, mut idx) = open_temp();
        // Test near u32::MAX to verify big-endian encoding handles high values
        idx.insert(u32::MAX - 1, key(1));
        idx.insert(u32::MAX, key(2));
        idx.insert(1, key(3));

        assert_eq!(idx.len(), 3);

        // Low query should only find the low entry
        let result = idx.range_query(1);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], key(3));

        // Max query should find all
        let result = idx.range_query(u32::MAX);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn persistence_after_clear() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("dah.redb");

        {
            let mut idx = RedbDahIndex::open(&db_path, 16 * 1024 * 1024).unwrap();
            idx.insert(100, key(1));
            idx.insert(200, key(2));
            idx.clear();
        }

        // Reopen and verify still empty
        let idx = RedbDahIndex::open(&db_path, 16 * 1024 * 1024).unwrap();
        assert!(idx.is_empty());
        assert!(idx.range_query(1000).is_empty());
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
}
