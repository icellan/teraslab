//! ReDB-backed DAH (delete-at-height) secondary index.
//!
//! Uses two tables for O(1) lookup in both directions:
//! - Forward: composite key `height_be(4) || txid(32)` -> `()` (big-endian for correct sort)
//! - Reverse: `txid(32)` -> `height(4 LE)`
//!
//! Two-phase durability: every mutating `insert`/`remove` appends and
//! fsyncs a [`RedoOp::SecondaryDahUpdate`] record BEFORE committing the
//! redb transaction. If the fsync fails the redb commit is skipped so
//! on-disk state cannot race ahead of the redo log.

use crate::index::IndexError;
use crate::index::dah_index::DahRedoEntry;
use crate::index::hashtable::TxKey;
use crate::redo::{RedoLog, RedoOp};
use parking_lot::Mutex;
use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use std::collections::VecDeque;
use std::path::Path;

/// Forward table: `[height_be(4) || txid(32)]` -> `()`
const DAH_FORWARD: TableDefinition<[u8; 36], ()> = TableDefinition::new("dah_forward");

/// Reverse table: `txid(32)` -> `height_le(4)`
const DAH_REVERSE: TableDefinition<[u8; 32], [u8; 4]> = TableDefinition::new("dah_reverse");

/// Maximum rows materialized by the streaming iterator at once.
const ITER_BATCH_SIZE: usize = 4096;

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

    /// Make every previously committed (`Durability::Eventual`) write
    /// transaction durable by committing an empty `Durability::Immediate`
    /// transaction. See [`crate::index::redb_primary::RedbPrimary::flush_durable`]
    /// for the checkpoint contract this supports.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if the transaction cannot be started or
    /// committed; callers must treat that as "redb state is NOT durable".
    pub fn flush_durable(&self) -> Result<(), IndexError> {
        let mut txn = self.db.begin_write().map_err(map_txn_err)?;
        txn.set_durability(redb::Durability::Immediate);
        txn.commit().map_err(map_commit_err)?;
        Ok(())
    }

    /// Look up the current height for a key using a cheap read transaction.
    ///
    /// Returns `None` if the key is not present in the index.
    pub fn get_height(&self, key: &TxKey) -> Option<u32> {
        let txn = self.db.begin_read().ok()?;
        let table = txn.open_table(DAH_REVERSE).ok()?;
        let guard = table.get(key.txid).ok()??;
        Some(u32::from_le_bytes(guard.value()))
    }

    /// Insert a transaction into the DAH index with two-phase durability.
    ///
    /// Steps:
    ///   1. Read the current height (if any) from redb.
    ///   2. If a redo log is provided, append and fsync a
    ///      [`RedoOp::SecondaryDahUpdate`] record.
    ///   3. Commit the redb transaction.
    ///
    /// See [`crate::index::redb_unmined::RedbUnminedIndex::insert`] for the
    /// durability rationale.
    pub fn insert(
        &mut self,
        height: u32,
        key: TxKey,
        redo_log: Option<&Mutex<RedoLog>>,
    ) -> Result<(), IndexError> {
        // F-G3-013: open the write transaction BEFORE reading the existing
        // height. Pre-fix, `get_height` ran its own read txn and the value
        // used for the `RedoOp::SecondaryDahUpdate.old_height` field could
        // be stale by the time the write actually committed. The replay
        // path only uses `new_height` so today the redo entry's old_height
        // was misleading-but-harmless; reading it under the same write
        // lock makes it TOCTOU-free in case a future replay handler grows
        // a dependency on it.
        let txn = self.begin_write().map_err(map_txn_err)?;
        let (old_height, was_new) = {
            let rev = txn.open_table(DAH_REVERSE).map_err(map_table_err)?;
            match rev.get(key.txid).map_err(map_storage_err)? {
                Some(guard) => (u32::from_le_bytes(guard.value()), false),
                None => (0u32, true),
            }
        };
        if old_height == height {
            // No-op; abort the write txn (no redo entry needed).
            drop(txn);
            return Ok(());
        }

        if let Some(redo) = redo_log {
            let op = RedoOp::SecondaryDahUpdate {
                tx_key: key,
                old_height,
                new_height: height,
            };
            let mut log = redo.lock();
            log.append_and_flush(op)
                .map_err(|e| IndexError::FormatError {
                    detail: format!("redo append_and_flush (dah insert): {e}"),
                })?;
        }

        {
            let mut fwd = txn.open_table(DAH_FORWARD).map_err(map_table_err)?;
            let mut rev = txn.open_table(DAH_REVERSE).map_err(map_table_err)?;

            if !was_new {
                let old_fwd_key = make_forward_key(old_height, &key);
                fwd.remove(old_fwd_key).map_err(map_storage_err)?;
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

    /// Remove a transaction from the DAH index with two-phase durability.
    ///
    /// See [`Self::insert`] for the ordering rationale.
    pub fn remove(
        &mut self,
        key: &TxKey,
        redo_log: Option<&Mutex<RedoLog>>,
    ) -> Result<(), IndexError> {
        let old_height = match self.get_height(key) {
            Some(h) => h,
            None => return Ok(()), // Already absent — no redo entry needed.
        };

        if let Some(redo) = redo_log {
            let op = RedoOp::SecondaryDahUpdate {
                tx_key: *key,
                old_height,
                new_height: 0,
            };
            let mut log = redo.lock();
            log.append_and_flush(op)
                .map_err(|e| IndexError::FormatError {
                    detail: format!("redo append_and_flush (dah remove): {e}"),
                })?;
        }

        let txn = self.begin_write().map_err(map_txn_err)?;
        let had_entry;
        {
            let mut fwd = txn.open_table(DAH_FORWARD).map_err(map_table_err)?;
            let mut rev = txn.open_table(DAH_REVERSE).map_err(map_table_err)?;

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

    /// Replay a DAH redo entry.
    ///
    /// Idempotent: the underlying `insert`/`remove` will no-op if redb
    /// already matches the target state. No additional redo entry is
    /// written during replay.
    pub fn replay_redo(&mut self, entry: &DahRedoEntry) -> Result<(), IndexError> {
        let key = TxKey { txid: entry.txid };
        if entry.new_height == 0 {
            self.remove(&key, None)
        } else {
            self.insert(entry.new_height, key, None)
        }
    }

    /// Return all txids with delete_at_height in `[0, current_height]`.
    ///
    /// F-G3-008: every redb read error path now emits a `tracing::error!`
    /// at `target = "teraslab::index"` before falling back to an empty
    /// vector. Pre-fix every error was eaten silently — the pruner
    /// couldn't distinguish "no entries at this height" from "redb read
    /// failed" and the operator had no log signal of the silent stall.
    /// The return type stays `Vec<TxKey>` to avoid churning every caller;
    /// the operator-visible log line is the load-bearing change.
    pub fn range_query(&self, current_height: u32) -> Vec<TxKey> {
        self.range_query_limited(current_height, usize::MAX)
    }

    /// Like [`Self::range_query`] but stops after collecting `limit` keys
    /// (lowest-`delete_at_height` first). Bounds the DAH sweep's per-call work;
    /// see [`crate::index::dah_index::DahIndex::range_query_limited`].
    pub fn range_query_limited(&self, current_height: u32, limit: usize) -> Vec<TxKey> {
        let mut result = Vec::new();
        if limit == 0 {
            return result;
        }
        let txn = match self.db.begin_read() {
            Ok(t) => t,
            Err(e) => {
                tracing::error!(
                    target: "teraslab::index",
                    err = %e,
                    "RedbDahIndex::range_query: begin_read failed; returning empty vec",
                );
                return result;
            }
        };
        let table = match txn.open_table(DAH_FORWARD) {
            Ok(t) => t,
            Err(e) => {
                tracing::error!(
                    target: "teraslab::index",
                    err = %e,
                    "RedbDahIndex::range_query: open_table failed; returning empty vec",
                );
                return result;
            }
        };

        let start = [0u8; 36];
        let end = make_forward_key(current_height, &TxKey { txid: [0xFF; 32] });

        match table.range(start..=end) {
            Ok(range) => {
                for (k, _) in range.flatten() {
                    let composite = k.value();
                    let mut txid = [0u8; 32];
                    txid.copy_from_slice(&composite[4..36]);
                    result.push(TxKey { txid });
                    if result.len() >= limit {
                        break;
                    }
                }
            }
            Err(e) => {
                tracing::error!(
                    target: "teraslab::index",
                    err = %e,
                    "RedbDahIndex::range_query: table.range failed; returning partial/empty vec",
                );
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
    ///
    /// Returns an [`IndexError`] if the redb begin/delete/recreate/commit
    /// chain fails. The cached `count` is only reset to zero after the
    /// commit succeeds; previously every step silently swallowed errors
    /// then forced `count = 0`, leaving an "empty" in-memory view over
    /// a fully-populated table on disk and misleading the pruner.
    pub fn clear(&mut self) -> Result<(), IndexError> {
        let txn = self.begin_write().map_err(map_txn_err)?;
        // Drop and recreate tables — O(1) memory regardless of entry count.
        txn.delete_table(DAH_FORWARD).map_err(map_table_err)?;
        txn.delete_table(DAH_REVERSE).map_err(map_table_err)?;
        // Open the empty tables so they exist for subsequent transactions.
        txn.open_table(DAH_FORWARD).map_err(map_table_err)?;
        txn.open_table(DAH_REVERSE).map_err(map_table_err)?;
        txn.commit().map_err(map_commit_err)?;
        self.count = 0;
        Ok(())
    }

    /// Insert multiple transactions in a single write transaction.
    ///
    /// **MIGRATION ONLY — does NOT use two-phase durability.** Unlike
    /// [`insert`](Self::insert), this method commits the redb transaction
    /// without first appending and fsyncing a [`RedoOp::SecondaryDahUpdate`]
    /// record, so a crash between the redb commit and the next checkpoint
    /// can lose entries that were just written. The trade-off is acceptable
    /// for one-shot bulk import (see [`crate::index::migration`]) which
    /// detects partial imports via its own sentinel-file mechanism and is
    /// not interleaved with foreground mutations.
    ///
    /// Restricted to `pub(crate)` so hot-path callers cannot accidentally
    /// adopt the bulk path and lose the redo-log guarantee that
    /// [`insert`](Self::insert) provides.
    pub(crate) fn insert_batch(&mut self, entries: &[(u32, TxKey)]) -> Result<(), IndexError> {
        if entries.is_empty() {
            return Ok(());
        }
        let txn = self.begin_write().map_err(map_txn_err)?;
        let mut new_count = 0usize;
        {
            let mut fwd = txn.open_table(DAH_FORWARD).map_err(map_table_err)?;
            let mut rev = txn.open_table(DAH_REVERSE).map_err(map_table_err)?;

            for &(height, key) in entries {
                let mut already_exists = false;
                if let Some(guard) = rev.get(key.txid).map_err(map_storage_err)? {
                    let old_height = u32::from_le_bytes(guard.value());
                    drop(guard);
                    if old_height == height {
                        continue;
                    }
                    already_exists = true;
                    fwd.remove(make_forward_key(old_height, &key))
                        .map_err(map_storage_err)?;
                }
                if !already_exists {
                    new_count += 1;
                }
                rev.insert(key.txid, height.to_le_bytes())
                    .map_err(map_storage_err)?;
                fwd.insert(make_forward_key(height, &key), ())
                    .map_err(map_storage_err)?;
            }
        }
        txn.commit().map_err(map_commit_err)?;
        self.count += new_count;
        Ok(())
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

    /// Iterate over all `(height, key)` pairs using bounded batches.
    pub fn iter_streaming(&self) -> RedbDahIter<'_> {
        RedbDahIter {
            index: self,
            next_start: Some([0u8; 32]),
            buffer: VecDeque::new(),
            finished: false,
        }
    }

    fn load_iter_batch(&self, start: [u8; 32]) -> Vec<(u32, TxKey)> {
        let mut result = Vec::with_capacity(ITER_BATCH_SIZE);
        if let Ok(txn) = self.db.begin_read()
            && let Ok(table) = txn.open_table(DAH_REVERSE)
            && let Ok(range) = table.range(start..)
        {
            for row in range.take(ITER_BATCH_SIZE) {
                match row {
                    Ok((k, v)) => {
                        let key = TxKey { txid: k.value() };
                        let height = u32::from_le_bytes(v.value());
                        result.push((height, key));
                    }
                    Err(e) => {
                        tracing::warn!(err = %e, "redb dah iter_streaming: row read failed");
                        break;
                    }
                }
            }
        }
        result
    }
}

/// Bounded-memory iterator over all entries in a redb DAH index.
pub struct RedbDahIter<'a> {
    index: &'a RedbDahIndex,
    next_start: Option<[u8; 32]>,
    buffer: VecDeque<(u32, TxKey)>,
    finished: bool,
}

impl RedbDahIter<'_> {
    fn refill(&mut self) {
        if self.finished || !self.buffer.is_empty() {
            return;
        }
        let Some(start) = self.next_start else {
            self.finished = true;
            return;
        };

        let batch = self.index.load_iter_batch(start);
        if batch.is_empty() {
            self.finished = true;
            self.next_start = None;
            return;
        }

        let last_key = batch.last().map(|(_, key)| key.txid);
        self.buffer = batch.into();
        self.next_start = last_key.and_then(next_lexicographic_key);
        if self.next_start.is_none() {
            self.finished = true;
        }
    }
}

impl Iterator for RedbDahIter<'_> {
    type Item = (u32, TxKey);

    fn next(&mut self) -> Option<Self::Item> {
        self.refill();
        self.buffer.pop_front()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.buffer.len(), None)
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
    IndexError::FormatError {
        detail: format!("redb txn error (dah): {e}"),
    }
}

fn map_table_err(e: redb::TableError) -> IndexError {
    IndexError::FormatError {
        detail: format!("redb table error (dah): {e}"),
    }
}

fn map_commit_err(e: redb::CommitError) -> IndexError {
    IndexError::FormatError {
        detail: format!("redb commit error (dah): {e}"),
    }
}

fn map_storage_err(e: redb::StorageError) -> IndexError {
    IndexError::FormatError {
        detail: format!("redb storage error (dah): {e}"),
    }
}

fn next_lexicographic_key(mut key: [u8; 32]) -> Option<[u8; 32]> {
    for byte in key.iter_mut().rev() {
        if *byte != u8::MAX {
            *byte += 1;
            return Some(key);
        }
        *byte = 0;
    }
    None
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
        idx.insert(100, key(1), None).unwrap();
        idx.insert(200, key(2), None).unwrap();
        idx.insert(300, key(3), None).unwrap();

        let result = idx.range_query(200);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&key(1)));
        assert!(result.contains(&key(2)));
    }

    #[test]
    fn insert_updates_height() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1), None).unwrap();
        idx.insert(200, key(1), None).unwrap();

        assert_eq!(idx.len(), 1);
        let result = idx.range_query(100);
        assert!(result.is_empty());
        let result = idx.range_query(200);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn insert_same_height_is_noop() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1), None).unwrap();
        idx.insert(100, key(1), None).unwrap();
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn remove() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1), None).unwrap();
        idx.insert(200, key(2), None).unwrap();
        idx.remove(&key(1), None).unwrap();

        assert_eq!(idx.len(), 1);
        let result = idx.range_query(300);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], key(2));
    }

    #[test]
    fn remove_missing_is_noop() {
        let (_dir, mut idx) = open_temp();
        idx.remove(&key(99), None).unwrap();
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn clear() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1), None).unwrap();
        idx.insert(200, key(2), None).unwrap();
        idx.clear().unwrap();
        assert!(idx.is_empty());
        assert!(idx.range_query(1000).is_empty());
    }

    // F-G3-002: `clear` must propagate redb errors. Pre-fix every step of
    // the drop+recreate chain was `let _ = …` and `self.count = 0` was
    // unconditional — a fully-populated on-disk table could be paired with
    // a zero in-memory count, misleading the pruner indefinitely. This
    // test pins the success path: cached count drops only after the
    // commit returns `Ok`.
    #[test]
    fn clear_returns_ok_and_zeroes_count_only_on_success() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1), None).unwrap();
        idx.insert(200, key(2), None).unwrap();
        assert_eq!(idx.len(), 2);
        let result: Result<(), IndexError> = idx.clear();
        result.expect("redb clear should succeed on an open temp db");
        assert_eq!(idx.len(), 0);
        assert!(idx.is_empty());
    }

    #[test]
    fn iter_all() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1), None).unwrap();
        idx.insert(200, key(2), None).unwrap();
        idx.insert(300, key(3), None).unwrap();

        let entries = idx.iter();
        assert_eq!(entries.len(), 3);
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
        idx.insert(200, key(1), None).unwrap();
        assert_eq!(idx.len(), 1);
        idx.insert(300, key(1), None).unwrap();
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn insert_batch_basic() {
        let (_dir, mut idx) = open_temp();
        let entries: Vec<_> = (1..=10u8).map(|n| (n as u32 * 100, key(n))).collect();
        idx.insert_batch(&entries).unwrap();
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
        idx.insert_batch(&entries).unwrap();
        assert_eq!(idx.len(), 1);

        // Should be at height 200
        assert!(idx.range_query(100).is_empty());
        assert_eq!(idx.range_query(200).len(), 1);
    }

    #[test]
    fn insert_batch_empty() {
        let (_dir, mut idx) = open_temp();
        idx.insert_batch(&[]).unwrap();
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn insert_batch_matches_individual() {
        // Individual inserts
        let dir1 = tempfile::tempdir().unwrap();
        let mut idx1 =
            RedbDahIndex::open(dir1.path().join("dah.redb").as_path(), 16 * 1024 * 1024).unwrap();
        for n in 1..=20u8 {
            idx1.insert(n as u32 * 100, key(n), None).unwrap();
        }

        // Batch insert
        let dir2 = tempfile::tempdir().unwrap();
        let mut idx2 =
            RedbDahIndex::open(dir2.path().join("dah.redb").as_path(), 16 * 1024 * 1024).unwrap();
        let entries: Vec<_> = (1..=20u8).map(|n| (n as u32 * 100, key(n))).collect();
        idx2.insert_batch(&entries).unwrap();

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
            idx.insert(100, key(1), None).unwrap();
            idx.insert(200, key(2), None).unwrap();
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
        idx.insert(100, key(1), None).unwrap();
        idx.insert(100, key(2), None).unwrap();

        let result = idx.range_query(100);
        assert_eq!(result.len(), 2);

        let result = idx.range_query(99);
        assert!(result.is_empty());
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
    fn clear_then_reinsert() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1), None).unwrap();
        idx.insert(200, key(2), None).unwrap();
        idx.clear().unwrap();

        assert!(idx.range_query(1000).is_empty());

        idx.insert(500, key(10), None).unwrap();
        assert_eq!(idx.len(), 1);
        let result = idx.range_query(500);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], key(10));

        assert!(idx.range_query(200).is_empty());
    }

    #[test]
    fn remove_then_reinsert_at_different_height() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1), None).unwrap();
        idx.remove(&key(1), None).unwrap();
        assert!(idx.is_empty());

        idx.insert(500, key(1), None).unwrap();
        assert_eq!(idx.len(), 1);

        assert!(idx.range_query(100).is_empty());
        assert_eq!(idx.range_query(500).len(), 1);
    }

    #[test]
    fn large_height_values() {
        let (_dir, mut idx) = open_temp();
        idx.insert(u32::MAX - 1, key(1), None).unwrap();
        idx.insert(u32::MAX, key(2), None).unwrap();
        idx.insert(1, key(3), None).unwrap();

        assert_eq!(idx.len(), 3);

        let result = idx.range_query(1);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], key(3));

        let result = idx.range_query(u32::MAX);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn persistence_after_clear() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("dah.redb");

        {
            let mut idx = RedbDahIndex::open(&db_path, 16 * 1024 * 1024).unwrap();
            idx.insert(100, key(1), None).unwrap();
            idx.insert(200, key(2), None).unwrap();
            idx.clear().unwrap();
        }

        let idx = RedbDahIndex::open(&db_path, 16 * 1024 * 1024).unwrap();
        assert!(idx.is_empty());
        assert!(idx.range_query(1000).is_empty());
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

    // -----------------------------------------------------------------------
    // Two-phase durability tests
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

        idx.insert(900, key(1), Some(&redo)).unwrap();
        assert_eq!(idx.get_height(&key(1)), Some(900));

        let log = RedoLog::open(redo_dev, 0, 1024 * 1024).unwrap();
        let entries = log.recover().unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0].op {
            RedoOp::SecondaryDahUpdate {
                tx_key,
                old_height,
                new_height,
            } => {
                assert_eq!(tx_key.txid, key(1).txid);
                assert_eq!(*old_height, 0);
                assert_eq!(*new_height, 900);
            }
            other => panic!("expected SecondaryDahUpdate, got {other:?}"),
        }
    }

    #[test]
    fn insert_with_redo_log_captures_old_height() {
        let (_dir, mut idx) = open_temp();
        let (redo_dev, redo) = make_redo_log(1024 * 1024);

        idx.insert(500, key(1), None).unwrap();
        idx.insert(800, key(1), Some(&redo)).unwrap();

        let log = RedoLog::open(redo_dev, 0, 1024 * 1024).unwrap();
        let entries = log.recover().unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0].op {
            RedoOp::SecondaryDahUpdate {
                old_height,
                new_height,
                ..
            } => {
                assert_eq!(*old_height, 500);
                assert_eq!(*new_height, 800);
            }
            other => panic!("expected SecondaryDahUpdate, got {other:?}"),
        }
    }

    #[test]
    fn remove_with_redo_log_emits_removal_intent() {
        let (_dir, mut idx) = open_temp();
        idx.insert(900, key(1), None).unwrap();
        let (redo_dev, redo) = make_redo_log(1024 * 1024);

        idx.remove(&key(1), Some(&redo)).unwrap();
        assert!(idx.is_empty());

        let log = RedoLog::open(redo_dev, 0, 1024 * 1024).unwrap();
        let entries = log.recover().unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0].op {
            RedoOp::SecondaryDahUpdate {
                old_height,
                new_height,
                ..
            } => {
                assert_eq!(*old_height, 900);
                assert_eq!(*new_height, 0);
            }
            other => panic!("expected SecondaryDahUpdate removal, got {other:?}"),
        }
    }

    #[test]
    fn insert_noop_with_same_height_skips_redo() {
        let (_dir, mut idx) = open_temp();
        idx.insert(100, key(1), None).unwrap();
        let (redo_dev, redo) = make_redo_log(1024 * 1024);

        idx.insert(100, key(1), Some(&redo)).unwrap();

        let log = RedoLog::open(redo_dev, 0, 1024 * 1024).unwrap();
        let entries = log.recover().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn redo_flush_failure_blocks_redb_commit() {
        let (_dir, mut idx) = open_temp();
        // Device must be at least as large as the redo log's header
        // requirement (8192 bytes). Pick 16 KiB device + 8 KiB log so the
        // log fits its own header but fills quickly once we start writing
        // 64-byte SecondaryDahUpdate records.
        // Device: 64 KiB, 4 KiB alignment. Log: 16 KiB — fits its own
        // header (8 KiB) and leaves room for ~80 64-byte redo records
        // before the log fills.
        let dev = Arc::new(MemoryDevice::new(64 * 1024, 4096).unwrap());
        let log = RedoLog::open(dev.clone(), 0, 16 * 1024).unwrap();
        let redo = Mutex::new(log);

        idx.insert(100, key(1), Some(&redo)).unwrap();

        let mut next: u8 = 2;
        loop {
            match idx.insert((next as u32) * 10, key(next), Some(&redo)) {
                Ok(_) => next += 1,
                Err(e) => {
                    assert!(
                        format!("{e}").contains("redo append_and_flush"),
                        "expected redo error, got: {e}"
                    );
                    break;
                }
            }
            if next > 200 {
                panic!("expected redo log to fill up");
            }
        }

        let failed_key = key(next);
        assert!(
            idx.get_height(&failed_key).is_none(),
            "redb must NOT contain an entry whose redo flush failed"
        );
    }

    #[test]
    fn replay_redo_idempotent() {
        let (_dir, mut idx) = open_temp();
        idx.insert(500, key(1), None).unwrap();

        let entry = DahRedoEntry {
            txid: key(1).txid,
            old_height: 0,
            new_height: 500,
        };
        idx.replay_redo(&entry).unwrap();
        assert_eq!(idx.get_height(&key(1)), Some(500));

        // Replaying again is still a no-op (no-op insert path).
        idx.replay_redo(&entry).unwrap();
        assert_eq!(idx.len(), 1);
    }
}
