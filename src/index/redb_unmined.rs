//! ReDB-backed unmined secondary index.
//!
//! Two-phase durability: every mutating `insert`/`remove` call appends and
//! fsyncs a [`RedoOp::SecondaryUnminedUpdate`] entry BEFORE committing the
//! redb transaction. If the fsync fails the redb commit is skipped and an
//! error is returned — the on-disk secondary index never diverges from the
//! redo log without a matching redo entry.
//!
//! On crash recovery, the replay path is idempotent: it reads the current
//! `unmined_since` from the primary index and only reapplies the secondary
//! update when the current state is stale.

use crate::index::IndexError;
use crate::index::hashtable::TxKey;
use crate::index::unmined_index::UnminedRedoEntry;
use crate::redo::{RedoLog, RedoOp};
use parking_lot::Mutex;
use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use std::path::Path;

/// Forward table: `[height_be(4) || txid(32)]` -> `()`
const UNMINED_FORWARD: TableDefinition<[u8; 36], ()> = TableDefinition::new("unmined_forward");

/// Reverse table: `txid(32)` -> `height_le(4)`
const UNMINED_REVERSE: TableDefinition<[u8; 32], [u8; 4]> = TableDefinition::new("unmined_reverse");

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
    #[allow(clippy::result_large_err)]
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

    /// Insert a transaction into the unmined index with two-phase durability.
    ///
    /// Steps:
    ///   1. Read the current height (if any) from redb.
    ///   2. If a redo log is provided, append and fsync a
    ///      [`RedoOp::SecondaryUnminedUpdate`] record.
    ///   3. Commit the redb transaction.
    ///
    /// If the redo flush fails, the redb transaction is NOT committed — so
    /// the on-disk state cannot race ahead of the redo log. If the redb
    /// transaction fails, the error is returned to the caller, but a redo
    /// entry may have been written — recovery replay is idempotent and
    /// converges the secondary index to the primary's authoritative state.
    ///
    /// `redo_log` may be `None` in contexts where durability is not required
    /// (unit tests, in-memory-only fixtures). Production callers MUST provide
    /// the redo log — passing `None` skips step 2 and leaves the redb commit
    /// unprotected.
    pub fn insert(
        &mut self,
        height: u32,
        key: TxKey,
        redo_log: Option<&Mutex<RedoLog>>,
    ) -> Result<(), IndexError> {
        let old_height = self.get_height(&key).unwrap_or(0);
        if old_height == height {
            // No-op: redb state already matches. No redo entry needed
            // because the primary's current unmined_since equals `height`
            // and recovery will not try to "undo" a no-op.
            return Ok(());
        }

        // Phase 1: redo-first durability. If the fsync fails we MUST NOT
        // commit redb, or recovery cannot reconcile the divergence.
        if let Some(redo) = redo_log {
            let op = RedoOp::SecondaryUnminedUpdate {
                tx_key: key,
                old_height,
                new_height: height,
            };
            let mut log = redo.lock();
            log.append_and_flush(op)
                .map_err(|e| IndexError::FormatError {
                    detail: format!("redo append_and_flush (unmined insert): {e}"),
                })?;
        }

        // Phase 2: commit redb. At this point the redo log (if any) has a
        // durable record of the intent; recovery can re-apply if this fails.
        let txn = self.begin_write().map_err(map_txn_err)?;
        let was_new;
        {
            let mut fwd = txn.open_table(UNMINED_FORWARD).map_err(map_table_err)?;
            let mut rev = txn.open_table(UNMINED_REVERSE).map_err(map_table_err)?;

            match rev.get(key.txid).map_err(map_storage_err)? {
                Some(guard) => {
                    let existing_height = u32::from_le_bytes(guard.value());
                    drop(guard);
                    was_new = false;
                    let old_fwd_key = make_forward_key(existing_height, &key);
                    fwd.remove(old_fwd_key).map_err(map_storage_err)?;
                }
                None => {
                    was_new = true;
                }
            }

            rev.insert(key.txid, height.to_le_bytes())
                .map_err(map_storage_err)?;
            fwd.insert(make_forward_key(height, &key), ())
                .map_err(map_storage_err)?;
        }
        // Fault-injection: crash between durable redo intent and the redb
        // commit. Post-recovery, the secondary index MUST be reconciled
        // from the durable redo entry (C4 two-phase durability contract).
        crate::fault_injection::check(crate::fault_injection::SyncPoint::BeforeSecondaryRedbCommit);
        crate::fault_injection::check(crate::fault_injection::SyncPoint::BeforeIndexCommit);
        txn.commit().map_err(map_commit_err)?;
        crate::fault_injection::check(crate::fault_injection::SyncPoint::AfterSecondaryRedbCommit);
        crate::fault_injection::check(crate::fault_injection::SyncPoint::AfterIndexCommit);
        if was_new {
            self.count += 1;
        }
        Ok(())
    }

    /// Remove a transaction from the unmined index with two-phase durability.
    ///
    /// Steps are analogous to [`Self::insert`]: read old_height, redo-first
    /// fsync (if a log is provided), then commit the redb transaction.
    /// See [`Self::insert`] for the durability ordering rationale.
    pub fn remove(
        &mut self,
        key: &TxKey,
        redo_log: Option<&Mutex<RedoLog>>,
    ) -> Result<(), IndexError> {
        let old_height = match self.get_height(key) {
            Some(h) => h,
            None => return Ok(()), // Already absent — nothing to do, no redo.
        };

        if let Some(redo) = redo_log {
            let op = RedoOp::SecondaryUnminedUpdate {
                tx_key: *key,
                old_height,
                new_height: 0,
            };
            let mut log = redo.lock();
            log.append_and_flush(op)
                .map_err(|e| IndexError::FormatError {
                    detail: format!("redo append_and_flush (unmined remove): {e}"),
                })?;
        }

        let txn = self.begin_write().map_err(map_txn_err)?;
        let had_entry;
        {
            let mut fwd = txn.open_table(UNMINED_FORWARD).map_err(map_table_err)?;
            let mut rev = txn.open_table(UNMINED_REVERSE).map_err(map_table_err)?;

            had_entry = match rev.remove(key.txid).map_err(map_storage_err)? {
                Some(guard) => {
                    let h = u32::from_le_bytes(guard.value());
                    fwd.remove(make_forward_key(h, key))
                        .map_err(map_storage_err)?;
                    true
                }
                None => false,
            };
        }
        crate::fault_injection::check(crate::fault_injection::SyncPoint::BeforeSecondaryRedbCommit);
        txn.commit().map_err(map_commit_err)?;
        crate::fault_injection::check(crate::fault_injection::SyncPoint::AfterSecondaryRedbCommit);
        if had_entry {
            self.count -= 1;
        }
        Ok(())
    }

    /// Commit the redb transaction for an already-fsynced redo entry.
    ///
    /// Used when the caller has already appended a batched redo flush covering
    /// multiple secondary updates (e.g., a combined DAH + unmined change from
    /// `mark_on_longest_chain`). The caller is responsible for ensuring the
    /// matching [`RedoOp::SecondaryUnminedUpdate`] is already durable before
    /// calling this function. Recovery replay handles any inconsistency if
    /// this redb commit fails after the fsync.
    pub fn commit_insert_post_fsync(&mut self, height: u32, key: TxKey) -> Result<(), IndexError> {
        // Delegate to insert with no redo log: the caller has already done
        // redo-first durability, so this just performs the redb commit.
        self.insert(height, key, None)
    }

    /// Commit the redb remove for an already-fsynced redo entry.
    /// See [`Self::commit_insert_post_fsync`] for the caller contract.
    pub fn commit_remove_post_fsync(&mut self, key: &TxKey) -> Result<(), IndexError> {
        self.remove(key, None)
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
                tracing::warn!(err = %e, "redb unmined insert_batch: begin_write failed");
                return;
            }
        };
        let mut new_count = 0usize;
        {
            let mut fwd = match txn.open_table(UNMINED_FORWARD) {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!(err = %e, "redb unmined insert_batch: open_table(forward) failed");
                    return;
                }
            };
            let mut rev = match txn.open_table(UNMINED_REVERSE) {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!(err = %e, "redb unmined insert_batch: open_table(reverse) failed");
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
                            tracing::warn!(err = %e, "redb unmined insert_batch: remove old forward failed");
                            return;
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!(err = %e, "redb unmined insert_batch: reverse lookup failed");
                        return;
                    }
                }
                if !already_exists {
                    new_count += 1;
                }
                if let Err(e) = rev.insert(key.txid, height.to_le_bytes()) {
                    tracing::warn!(err = %e, "redb unmined insert_batch: reverse insert failed");
                    return;
                }
                if let Err(e) = fwd.insert(make_forward_key(height, &key), ()) {
                    tracing::warn!(err = %e, "redb unmined insert_batch: forward insert failed");
                    return;
                }
            }
        }
        match txn.commit() {
            Ok(()) => {
                self.count += new_count;
            }
            Err(e) => {
                tracing::warn!(err = %e, "redb unmined insert_batch: commit failed");
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
    ///
    /// Idempotent: if the redb state already matches the redo entry's
    /// `new_height`, the replay is a no-op. No additional redo entry is
    /// written during replay — the original intent record already covers
    /// the transition.
    pub fn replay_redo(&mut self, entry: &UnminedRedoEntry) -> Result<(), IndexError> {
        let key = TxKey { txid: entry.txid };
        if entry.new_height == 0 {
            self.remove(&key, None)
        } else {
            self.insert(entry.new_height, key, None)
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
    IndexError::FormatError {
        detail: format!("redb txn error (unmined): {e}"),
    }
}

fn map_table_err(e: redb::TableError) -> IndexError {
    IndexError::FormatError {
        detail: format!("redb table error (unmined): {e}"),
    }
}

fn map_commit_err(e: redb::CommitError) -> IndexError {
    IndexError::FormatError {
        detail: format!("redb commit error (unmined): {e}"),
    }
}

fn map_storage_err(e: redb::StorageError) -> IndexError {
    IndexError::FormatError {
        detail: format!("redb storage error (unmined): {e}"),
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

    fn open_temp() -> (tempfile::TempDir, RedbUnminedIndex) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("unmined.redb");
        let idx = RedbUnminedIndex::open(&db_path, 16 * 1024 * 1024).unwrap();
        (dir, idx)
    }

    #[test]
    fn insert_basic() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1), None).unwrap();
        assert_eq!(idx.get_height(&key(1)), Some(100));
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn insert_update_replaces_height() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1), None).unwrap();
        idx.insert(200, key(1), None).unwrap();
        assert_eq!(idx.get_height(&key(1)), Some(200));
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn insert_same_height_is_noop() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1), None).unwrap();
        idx.insert(100, key(1), None).unwrap();
        assert_eq!(idx.get_height(&key(1)), Some(100));
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn remove_basic() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1), None).unwrap();
        idx.remove(&key(1), None).unwrap();
        assert_eq!(idx.get_height(&key(1)), None);
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn remove_missing_is_noop() {
        let (_dir, mut idx) = open_temp();
        idx.remove(&key(99), None).unwrap();
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn range_query() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1), None).unwrap();
        idx.insert(200, key(2), None).unwrap();
        idx.insert(300, key(3), None).unwrap();

        let result = idx.range_query(200);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&key(1)));
        assert!(result.contains(&key(2)));
    }

    #[test]
    fn clear() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1), None).unwrap();
        idx.insert(200, key(2), None).unwrap();
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
        idx.replay_redo(&entry).unwrap();
        assert_eq!(idx.len(), 1);
        let result = idx.range_query(500);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn replay_redo_remove() {
        let (_dir, mut idx) = open_temp();
        idx.insert(500, key(1), None).unwrap();
        let entry = UnminedRedoEntry {
            txid: key(1).txid,
            old_height: 500,
            new_height: 0,
        };
        idx.replay_redo(&entry).unwrap();
        assert!(idx.is_empty());
    }

    #[test]
    fn insert_count_incremental() {
        let (_dir, mut idx) = open_temp();
        assert_eq!(idx.len(), 0);
        idx.insert(100, key(1), None).unwrap();
        assert_eq!(idx.len(), 1);
        idx.insert(200, key(2), None).unwrap();
        assert_eq!(idx.len(), 2);
        idx.insert(300, key(3), None).unwrap();
        assert_eq!(idx.len(), 3);
    }

    #[test]
    fn remove_count_incremental() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1), None).unwrap();
        idx.insert(200, key(2), None).unwrap();
        idx.insert(300, key(3), None).unwrap();
        assert_eq!(idx.len(), 3);

        idx.remove(&key(2), None).unwrap();
        assert_eq!(idx.len(), 2);
        idx.remove(&key(1), None).unwrap();
        assert_eq!(idx.len(), 1);
        idx.remove(&key(3), None).unwrap();
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn insert_update_does_not_change_count() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1), None).unwrap();
        assert_eq!(idx.len(), 1);
        idx.insert(200, key(1), None).unwrap(); // Update height, same key
        assert_eq!(idx.len(), 1);
        idx.insert(300, key(1), None).unwrap(); // Another update
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
        let mut idx1 =
            RedbUnminedIndex::open(dir1.path().join("unmined.redb").as_path(), 16 * 1024 * 1024)
                .unwrap();
        for n in 1..=20u8 {
            idx1.insert(n as u32 * 100, key(n), None).unwrap();
        }

        // Batch insert
        let dir2 = tempfile::tempdir().unwrap();
        let mut idx2 =
            RedbUnminedIndex::open(dir2.path().join("unmined.redb").as_path(), 16 * 1024 * 1024)
                .unwrap();
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

        idx.insert(100, key(1), None).unwrap();
        assert_eq!(idx.get_height(&key(1)), Some(100));

        idx.insert(200, key(1), None).unwrap(); // Update
        assert_eq!(idx.get_height(&key(1)), Some(200));

        idx.remove(&key(1), None).unwrap();
        assert!(idx.get_height(&key(1)).is_none());
    }

    #[test]
    fn persistence() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("unmined.redb");

        {
            let mut idx = RedbUnminedIndex::open(&db_path, 16 * 1024 * 1024).unwrap();
            idx.insert(100, key(1), None).unwrap();
            idx.insert(200, key(2), None).unwrap();
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
        idx.insert(100, key(1), None).unwrap();
        idx.insert(200, key(2), None).unwrap();
        idx.insert(300, key(3), None).unwrap();

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
        idx.insert(100, key(1), None).unwrap();
        idx.insert(200, key(2), None).unwrap();

        let result = idx.range_query(99);
        assert!(result.is_empty());
    }

    #[test]
    fn range_query_above_all_heights() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1), None).unwrap();
        idx.insert(200, key(2), None).unwrap();
        idx.insert(300, key(3), None).unwrap();

        let result = idx.range_query(u32::MAX);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn clear_then_reinsert() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1), None).unwrap();
        idx.insert(200, key(2), None).unwrap();
        idx.clear();
        assert!(idx.is_empty());
        assert!(idx.range_query(1000).is_empty());

        // New inserts work on a clean slate
        idx.insert(500, key(10), None).unwrap();
        assert_eq!(idx.len(), 1);
        let result = idx.range_query(500);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], key(10));
    }

    #[test]
    fn remove_then_reinsert_cycle() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1), None).unwrap();
        assert_eq!(idx.get_height(&key(1)), Some(100));

        idx.remove(&key(1), None).unwrap();
        assert!(idx.is_empty());

        // Re-insert
        idx.insert(500, key(1), None).unwrap();
        assert_eq!(idx.len(), 1);
        assert_eq!(idx.get_height(&key(1)), Some(500));

        let result = idx.range_query(500);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], key(1));
    }

    #[test]
    fn len_tracks_through_operations() {
        let (_dir, mut idx) = open_temp();

        assert_eq!(idx.len(), 0);
        assert!(idx.is_empty());

        idx.insert(100, key(1), None).unwrap();
        assert_eq!(idx.len(), 1);
        assert!(!idx.is_empty());

        idx.insert(200, key(2), None).unwrap();
        assert_eq!(idx.len(), 2);

        // Update existing key (different height) — count stays the same
        idx.insert(300, key(1), None).unwrap();
        assert_eq!(idx.len(), 2);

        idx.remove(&key(1), None).unwrap();
        assert_eq!(idx.len(), 1);

        idx.remove(&key(2), None).unwrap();
        assert_eq!(idx.len(), 0);
        assert!(idx.is_empty());
    }

    #[test]
    fn persistence_after_clear() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("unmined.redb");

        {
            let mut idx = RedbUnminedIndex::open(&db_path, 16 * 1024 * 1024).unwrap();
            idx.insert(100, key(1), None).unwrap();
            idx.insert(200, key(2), None).unwrap();
            idx.clear();
        }

        let idx = RedbUnminedIndex::open(&db_path, 16 * 1024 * 1024).unwrap();
        assert!(idx.is_empty());
        assert!(idx.range_query(1000).is_empty());
    }

    #[test]
    fn multiple_entries_same_height() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1), None).unwrap();
        idx.insert(100, key(2), None).unwrap();
        idx.insert(100, key(3), None).unwrap();

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
        idx.insert(u32::MAX, key(1), None).unwrap();
        idx.insert(u32::MAX - 1, key(2), None).unwrap();
        idx.insert(1, key(3), None).unwrap();

        assert_eq!(idx.len(), 3);

        let result = idx.range_query(1);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], key(3));

        let result = idx.range_query(u32::MAX);
        assert_eq!(result.len(), 3);
    }

    // -----------------------------------------------------------------------
    // Two-phase durability tests — these exercise the bug fix for C4.
    // -----------------------------------------------------------------------

    use crate::device::MemoryDevice;
    use crate::redo::RedoLog;
    use std::sync::Arc;

    fn make_redo_log(size: u64) -> (Arc<MemoryDevice>, Mutex<RedoLog>) {
        let dev = Arc::new(MemoryDevice::new(size, 4096).unwrap());
        let log = RedoLog::open(dev.clone(), 0, size).unwrap();
        (dev, Mutex::new(log))
    }

    #[test]
    fn insert_with_redo_log_appends_intent_before_commit() {
        let (_dir, mut idx) = open_temp();
        let (redo_dev, redo) = make_redo_log(1024 * 1024);

        idx.insert(500, key(1), Some(&redo)).unwrap();

        // Redb committed
        assert_eq!(idx.get_height(&key(1)), Some(500));

        // Redo log has the intent record
        let log = RedoLog::open(redo_dev, 0, 1024 * 1024).unwrap();
        let entries = log.recover().unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0].op {
            RedoOp::SecondaryUnminedUpdate {
                tx_key,
                old_height,
                new_height,
            } => {
                assert_eq!(tx_key.txid, key(1).txid);
                assert_eq!(*old_height, 0);
                assert_eq!(*new_height, 500);
            }
            other => panic!("expected SecondaryUnminedUpdate, got {other:?}"),
        }
    }

    #[test]
    fn insert_with_redo_log_captures_old_height() {
        let (_dir, mut idx) = open_temp();
        let (redo_dev, redo) = make_redo_log(1024 * 1024);

        // Seed without redo (bulk-import pattern)
        idx.insert(100, key(1), None).unwrap();

        // Update with redo
        idx.insert(200, key(1), Some(&redo)).unwrap();

        let log = RedoLog::open(redo_dev, 0, 1024 * 1024).unwrap();
        let entries = log.recover().unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0].op {
            RedoOp::SecondaryUnminedUpdate {
                old_height,
                new_height,
                ..
            } => {
                assert_eq!(*old_height, 100);
                assert_eq!(*new_height, 200);
            }
            other => panic!("expected SecondaryUnminedUpdate, got {other:?}"),
        }
    }

    #[test]
    fn remove_with_redo_log_emits_removal_intent() {
        let (_dir, mut idx) = open_temp();
        idx.insert(500, key(1), None).unwrap();
        let (redo_dev, redo) = make_redo_log(1024 * 1024);

        idx.remove(&key(1), Some(&redo)).unwrap();
        assert!(idx.is_empty());

        let log = RedoLog::open(redo_dev, 0, 1024 * 1024).unwrap();
        let entries = log.recover().unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0].op {
            RedoOp::SecondaryUnminedUpdate {
                old_height,
                new_height,
                ..
            } => {
                assert_eq!(*old_height, 500);
                assert_eq!(*new_height, 0);
            }
            other => panic!("expected SecondaryUnminedUpdate removal, got {other:?}"),
        }
    }

    #[test]
    fn insert_noop_with_same_height_skips_redo() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1), None).unwrap();
        let (redo_dev, redo) = make_redo_log(1024 * 1024);

        // Same-height insert is a no-op — no redo entry should be written.
        idx.insert(100, key(1), Some(&redo)).unwrap();

        let log = RedoLog::open(redo_dev, 0, 1024 * 1024).unwrap();
        let entries = log.recover().unwrap();
        assert!(
            entries.is_empty(),
            "no-op same-height insert must not append redo entries"
        );
    }

    #[test]
    fn redo_flush_failure_blocks_redb_commit() {
        // Use a redo log sized so small that a single append fills it,
        // triggering LogFull on the second call.
        let (_dir, mut idx) = open_temp();
        let dev = Arc::new(MemoryDevice::new(4096, 4096).unwrap());
        let log = RedoLog::open(dev.clone(), 0, 256).unwrap();
        let redo = Mutex::new(log);

        // First insert consumes space in the tiny log.
        idx.insert(100, key(1), Some(&redo)).unwrap();
        // Pack the log until it's near full so the next append fails.
        // Each SecondaryUnminedUpdate entry is small but append fails when
        // the buffer + fsynced offset exceed capacity. We force failure
        // by appending unique keys until the log refuses.
        let mut next: u8 = 2;
        loop {
            match idx.insert((next as u32) * 10, key(next), Some(&redo)) {
                Ok(_) => next += 1,
                Err(e) => {
                    // Expect a redo-related error
                    assert!(
                        format!("{e}").contains("redo append_and_flush"),
                        "expected redo error, got: {e}"
                    );
                    break;
                }
            }
            if next > 50 {
                panic!("expected redo log to fill up");
            }
        }

        // Assert that the failed insert did NOT commit to redb.
        let failed_key = key(next);
        assert!(
            idx.get_height(&failed_key).is_none(),
            "redb must NOT contain an entry whose redo flush failed"
        );
    }

    #[test]
    fn replay_redo_idempotent_against_existing_state() {
        let (_dir, mut idx) = open_temp();
        idx.insert(500, key(1), None).unwrap();

        // Replay the same insert — should be a no-op
        let entry = UnminedRedoEntry {
            txid: key(1).txid,
            old_height: 0,
            new_height: 500,
        };
        idx.replay_redo(&entry).unwrap();
        assert_eq!(idx.get_height(&key(1)), Some(500));
        assert_eq!(idx.len(), 1);

        // Replay again — still idempotent
        idx.replay_redo(&entry).unwrap();
        assert_eq!(idx.len(), 1);
    }
}
