//! ReDB-backed deletion-tombstone lookup index (deletion-tombstone Phase 2).
//!
//! A single redb table `tombstones: TxKey -> (deletion_height, generation,
//! shard, cause)` derived from the on-device append-only tombstone log
//! ([`crate::tombstone::TombstoneLog`]). It gives O(1) `is_tombstoned(key)` /
//! `get(key)` for the future migration-reconciliation hot path and the
//! receiver's idempotency check, without scanning the log. Range-delete by
//! `deletion_height` supports the future GC daemon; `rebuild_from` supports
//! recovery reconstruction from the log.
//!
//! The on-device log is the durable source of truth; this redb table is a
//! derived index. Modeled on [`crate::index::redb_dah`] (same
//! `TableDefinition` + `Durability::Eventual` conventions, same `thiserror`
//! mapped-error style).
//!
//! ## Value encoding
//!
//! redb values are fixed-size `[u8; 11]`:
//! `deletion_height(4 LE) | generation(4 LE) | shard(2 LE) | cause(1)`.
//!
//! ## Phase scope
//!
//! PURE STORAGE — nothing reads or writes this yet. Additive; no behavior
//! change.

use crate::index::hashtable::TxKey;
use crate::tombstone::Tombstone;
use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use std::path::Path;
use thiserror::Error;

/// Tombstone table: `txid(32)` -> packed `(deletion_height, generation,
/// shard, cause)` value, 11 bytes.
const TOMBSTONES: TableDefinition<[u8; 32], [u8; VALUE_LEN]> = TableDefinition::new("tombstones");

/// Forward table for height-ordered range scans / GC range-delete:
/// composite key `deletion_height_be(4) || txid(32)` -> `()`. Big-endian
/// height makes the lexicographic redb order match numeric height order, so
/// `range_delete_below_height` is a single bounded range. Mirrors the
/// forward/reverse split in [`crate::index::redb_dah`].
const TOMBSTONES_BY_HEIGHT: TableDefinition<[u8; 36], ()> =
    TableDefinition::new("tombstones_by_height");

/// Encoded value length: `deletion_height(4) + generation(4) + shard(2) + cause(1)`.
const VALUE_LEN: usize = 4 + 4 + 2 + 1;

/// Decoded tombstone index value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TombstoneIndexValue {
    /// Block height at which the deletion became authoritative.
    pub deletion_height: u32,
    /// Record generation at deletion time.
    pub generation: u32,
    /// Shard id.
    pub shard: u16,
    /// [`crate::tombstone::TombstoneCause`] discriminant byte.
    pub cause: u8,
}

impl TombstoneIndexValue {
    fn encode(&self) -> [u8; VALUE_LEN] {
        let mut out = [0u8; VALUE_LEN];
        out[0..4].copy_from_slice(&self.deletion_height.to_le_bytes());
        out[4..8].copy_from_slice(&self.generation.to_le_bytes());
        out[8..10].copy_from_slice(&self.shard.to_le_bytes());
        out[10] = self.cause;
        out
    }

    fn decode(bytes: &[u8; VALUE_LEN]) -> Self {
        TombstoneIndexValue {
            deletion_height: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            generation: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            shard: u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
            cause: bytes[10],
        }
    }
}

/// Errors from the redb tombstone index.
#[derive(Error, Debug)]
pub enum TombstoneIndexError {
    /// A redb error (open / transaction / table / storage / commit), with a
    /// human-readable detail string. redb's own error types are not
    /// `thiserror`-friendly enough to wrap structurally, so the detail
    /// carries the original `Display`.
    #[error("redb tombstone index error: {detail}")]
    Redb { detail: String },
}

/// ReDB-backed tombstone lookup index.
pub struct RedbTombstoneIndex {
    db: Database,
    count: usize,
}

impl RedbTombstoneIndex {
    /// Open or create a redb tombstone index at `path`.
    ///
    /// # Errors
    /// [`TombstoneIndexError::Redb`] if the database cannot be opened or the
    /// initial tables cannot be created.
    pub fn open(path: &Path, cache_size: usize) -> Result<Self, TombstoneIndexError> {
        let db = redb::Builder::new()
            .set_cache_size(cache_size)
            .create(path)
            .map_err(|e| redb_err("open", e))?;

        {
            let mut txn = db.begin_write().map_err(|e| redb_err("begin", e))?;
            txn.set_durability(redb::Durability::Eventual);
            txn.open_table(TOMBSTONES)
                .map_err(|e| redb_err("table", e))?;
            txn.open_table(TOMBSTONES_BY_HEIGHT)
                .map_err(|e| redb_err("table", e))?;
            txn.commit().map_err(|e| redb_err("commit", e))?;
        }

        let count = {
            let txn = db.begin_read().map_err(|e| redb_err("begin", e))?;
            let table = txn
                .open_table(TOMBSTONES)
                .map_err(|e| redb_err("table", e))?;
            table.len().map_err(|e| redb_err("len", e))? as usize
        };

        Ok(Self { db, count })
    }

    /// Start a write transaction with eventual durability.
    ///
    /// The on-device tombstone log is the durable source of truth (it is
    /// fsynced on the delete path), so per-redb-transaction fsync is
    /// unnecessary — this index is rebuilt from the log on recovery. Mirrors
    /// [`crate::index::redb_dah`]'s rationale.
    #[allow(clippy::result_large_err)]
    fn begin_write(&self) -> Result<redb::WriteTransaction, redb::TransactionError> {
        let mut txn = self.db.begin_write()?;
        txn.set_durability(redb::Durability::Eventual);
        Ok(txn)
    }

    /// Insert (or overwrite) a tombstone row for `key`.
    ///
    /// Idempotent on `key`: re-inserting the same key replaces the prior
    /// value and adjusts the height-forward table accordingly. The cached
    /// count increments only when `key` was not already present.
    ///
    /// # Errors
    /// [`TombstoneIndexError::Redb`] on any redb failure.
    pub fn insert(
        &mut self,
        key: TxKey,
        deletion_height: u32,
        generation: u32,
        shard: u16,
        cause: u8,
    ) -> Result<(), TombstoneIndexError> {
        self.insert_many(&[(key, deletion_height, generation, shard, cause)])
    }

    /// Insert (or overwrite) many tombstone rows in a SINGLE write transaction.
    ///
    /// This is the batched form of [`Self::insert`]: it opens one redb write
    /// transaction, applies every `(key, deletion_height, generation, shard,
    /// cause)` row, and commits once — instead of one transaction+commit per
    /// row. The deletion hot path uses it to fold a whole delete batch's
    /// tombstone-index rows into one commit (≈N× fewer redb commits per batch),
    /// which `sample`-profiling showed was the dominant remaining delete cost
    /// once the per-delete fsyncs were removed. Per-row semantics are identical
    /// to [`Self::insert`]: idempotent on `key` (re-inserting replaces the prior
    /// value and fixes the height-forward entry), and the cached count
    /// increments only for keys not already present (reads-your-writes within
    /// the transaction makes a duplicate key inside `rows` a no-op for the
    /// count). A no-op for an empty slice.
    ///
    /// # Errors
    /// [`TombstoneIndexError::Redb`] on any redb failure; the transaction is
    /// dropped (rolled back) so the index is unchanged on error.
    pub fn insert_many(
        &mut self,
        rows: &[(TxKey, u32, u32, u16, u8)],
    ) -> Result<(), TombstoneIndexError> {
        if rows.is_empty() {
            return Ok(());
        }
        let txn = self.begin_write().map_err(|e| redb_err("begin", e))?;
        let mut added = 0usize;
        {
            let mut table = txn
                .open_table(TOMBSTONES)
                .map_err(|e| redb_err("table", e))?;
            let mut by_height = txn
                .open_table(TOMBSTONES_BY_HEIGHT)
                .map_err(|e| redb_err("table", e))?;

            for &(key, deletion_height, generation, shard, cause) in rows {
                let value = TombstoneIndexValue {
                    deletion_height,
                    generation,
                    shard,
                    cause,
                };
                // Remove any stale height-forward entry if the key already
                // exists at a different height (reads-your-writes within the
                // txn, so an earlier row in this same batch counts as present).
                if let Some(prev) = table.get(key.txid).map_err(|e| redb_err("get", e))? {
                    let prev_val = TombstoneIndexValue::decode(&prev.value());
                    drop(prev);
                    by_height
                        .remove(make_height_key(prev_val.deletion_height, &key))
                        .map_err(|e| redb_err("remove", e))?;
                } else {
                    added += 1;
                }

                table
                    .insert(key.txid, value.encode())
                    .map_err(|e| redb_err("insert", e))?;
                by_height
                    .insert(make_height_key(deletion_height, &key), ())
                    .map_err(|e| redb_err("insert", e))?;
            }
        }
        txn.commit().map_err(|e| redb_err("commit", e))?;
        self.count += added;
        Ok(())
    }

    /// Whether `key` has a tombstone.
    ///
    /// O(1) point lookup. A redb read failure is reported as `false` (the
    /// caller treats a missing/erroring index as "not tombstoned"); use
    /// [`get`](Self::get) if the distinction matters.
    pub fn is_tombstoned(&self, key: &TxKey) -> bool {
        self.get(key).is_some()
    }

    /// Look up the tombstone value for `key`, or `None` if absent.
    ///
    /// O(1) point lookup for the future reconciliation hot path. Returns
    /// `None` on a redb read error (logged at `warn`) as well as on a genuine
    /// absence.
    pub fn get(&self, key: &TxKey) -> Option<TombstoneIndexValue> {
        let txn = self.db.begin_read().ok()?;
        let table = txn.open_table(TOMBSTONES).ok()?;
        let guard = table.get(key.txid).ok()??;
        Some(TombstoneIndexValue::decode(&guard.value()))
    }

    /// Number of tombstone rows.
    pub fn len(&self) -> usize {
        self.count
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Range-delete all tombstone rows with `deletion_height < height`.
    ///
    /// For the future GC daemon: once a height falls below the safe-rejoin
    /// horizon its tombstones are reclaimable. Returns the number of rows
    /// removed. Idempotent — re-running with the same height removes nothing.
    ///
    /// # Errors
    /// [`TombstoneIndexError::Redb`] on any redb failure.
    pub fn range_delete_below_height(&mut self, height: u32) -> Result<usize, TombstoneIndexError> {
        if height == 0 {
            // Nothing can be strictly below height 0.
            return Ok(0);
        }
        // Collect the txids to delete from the height-forward table first
        // (bounded range below `height`), then delete from both tables.
        let to_delete: Vec<(u32, [u8; 32])> = {
            let txn = self.db.begin_read().map_err(|e| redb_err("begin", e))?;
            let by_height = txn
                .open_table(TOMBSTONES_BY_HEIGHT)
                .map_err(|e| redb_err("table", e))?;
            let start = [0u8; 36];
            // Exclusive upper bound: height_be || 0x00..00 is the first key at
            // `height`, so the range `[0, that)` covers strictly-below rows.
            let end = make_height_key(height, &TxKey { txid: [0u8; 32] });
            let mut out = Vec::new();
            let range = by_height
                .range(start..end)
                .map_err(|e| redb_err("range", e))?;
            for row in range {
                let (k, _) = row.map_err(|e| redb_err("row", e))?;
                let composite = k.value();
                let h = u32::from_be_bytes(composite[0..4].try_into().unwrap());
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&composite[4..36]);
                out.push((h, txid));
            }
            out
        };

        if to_delete.is_empty() {
            return Ok(0);
        }

        let txn = self.begin_write().map_err(|e| redb_err("begin", e))?;
        let mut removed = 0usize;
        {
            let mut table = txn
                .open_table(TOMBSTONES)
                .map_err(|e| redb_err("table", e))?;
            let mut by_height = txn
                .open_table(TOMBSTONES_BY_HEIGHT)
                .map_err(|e| redb_err("table", e))?;
            for (h, txid) in &to_delete {
                if table
                    .remove(*txid)
                    .map_err(|e| redb_err("remove", e))?
                    .is_some()
                {
                    removed += 1;
                }
                by_height
                    .remove(make_height_key(*h, &TxKey { txid: *txid }))
                    .map_err(|e| redb_err("remove", e))?;
            }
        }
        txn.commit().map_err(|e| redb_err("commit", e))?;
        self.count -= removed;
        Ok(removed)
    }

    /// The maximum `deletion_height` across all live tombstone rows, or `None`
    /// if the index is empty.
    ///
    /// Cheap: the `TOMBSTONES_BY_HEIGHT` table is keyed `height_be || txid`, so
    /// the last key in iteration order carries the max height — a single
    /// reverse range step, not a full scan.
    ///
    /// Used at recovery to derive a sound lower-bound floor for the node's
    /// last-durable height (deletion-tombstone design §4, height subsystem): a
    /// tombstone's `deletion_height` is, by construction, ≤ the
    /// `current_block_height` the node observed when it applied that delete, so
    /// it is a valid (free) floor that prevents the restored height from
    /// regressing below deletions the node has durably recorded.
    ///
    /// # Errors
    /// [`TombstoneIndexError::Redb`] on a redb read failure.
    pub fn max_deletion_height(&self) -> Result<Option<u32>, TombstoneIndexError> {
        let txn = self.db.begin_read().map_err(|e| redb_err("begin", e))?;
        let by_height = txn
            .open_table(TOMBSTONES_BY_HEIGHT)
            .map_err(|e| redb_err("table", e))?;
        let mut range = by_height
            .range::<[u8; 36]>(..)
            .map_err(|e| redb_err("range", e))?;
        match range.next_back() {
            Some(row) => {
                let (k, _) = row.map_err(|e| redb_err("row", e))?;
                let composite = k.value();
                Ok(Some(u32::from_be_bytes(
                    composite[0..4].try_into().unwrap(),
                )))
            }
            None => Ok(None),
        }
    }

    /// All tombstoned keys for `shard`, as `(TxKey, generation)` pairs.
    ///
    /// Scans the tombstone table and filters by the stored `shard` field (the
    /// shard recorded at insert time, computed from the txid via
    /// [`crate::cluster::shards::ShardTable::shard_for_key`] — so it matches the
    /// derived shard of each key). This is the source side of tombstone-driven
    /// migration reconciliation (deletion-tombstone Phase 8, design §7): the
    /// master builds the completion frame's tombstone section from this, mirroring
    /// the engine's `keys_for_shard` for live keys.
    ///
    /// O(total tombstones). The full tombstone set is bounded by the GC horizon
    /// (design §3.3), so this is acceptable for the per-shard handoff path.
    /// Returns `(TxKey, deletion-generation)` pairs; the generation is the
    /// record's generation at deletion time (what the §7 row-2/row-4 split
    /// compares against the rejoinee's local generation).
    ///
    /// On a redb read error the scan returns whatever it accumulated before the
    /// error and logs at `warn` — a partial tombstone section can only cause a
    /// key to be *transferred* instead of *dropped* (a tombstone the source
    /// failed to read is simply not presented), which is no-loss-safe; it can
    /// never cause a spurious drop.
    pub fn tombstones_for_shard(&self, shard: u16) -> Vec<(TxKey, u32)> {
        let mut out = Vec::new();
        let txn = match self.db.begin_read() {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(err = %e, shard, "tombstones_for_shard: begin_read failed");
                return out;
            }
        };
        let table = match txn.open_table(TOMBSTONES) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(err = %e, shard, "tombstones_for_shard: open_table failed");
                return out;
            }
        };
        let iter = match table.iter() {
            Ok(it) => it,
            Err(e) => {
                tracing::warn!(err = %e, shard, "tombstones_for_shard: iter failed");
                return out;
            }
        };
        for row in iter {
            let (k, v) = match row {
                Ok(kv) => kv,
                Err(e) => {
                    tracing::warn!(err = %e, shard, "tombstones_for_shard: row read failed");
                    return out;
                }
            };
            let value = TombstoneIndexValue::decode(&v.value());
            if value.shard == shard {
                out.push((TxKey { txid: k.value() }, value.generation));
            }
        }
        out
    }

    /// Clear the index and bulk-insert from an iterator of [`Tombstone`]s.
    ///
    /// For the future recovery path: rebuild the redb index from the durable
    /// on-device log. Clears both tables, then inserts every entry in a single
    /// write transaction. A later entry for the same key overwrites an earlier
    /// one (last-writer-wins, matching log append order where the newest
    /// tombstone for a key is authoritative).
    ///
    /// # Errors
    /// [`TombstoneIndexError::Redb`] on any redb failure.
    pub fn rebuild_from(
        &mut self,
        entries: impl Iterator<Item = Tombstone>,
    ) -> Result<(), TombstoneIndexError> {
        let txn = self.begin_write().map_err(|e| redb_err("begin", e))?;
        // Drop + recreate for O(1)-memory clear (mirrors redb_dah::clear).
        txn.delete_table(TOMBSTONES)
            .map_err(|e| redb_err("table", e))?;
        txn.delete_table(TOMBSTONES_BY_HEIGHT)
            .map_err(|e| redb_err("table", e))?;
        let mut new_count;
        {
            let mut table = txn
                .open_table(TOMBSTONES)
                .map_err(|e| redb_err("table", e))?;
            let mut by_height = txn
                .open_table(TOMBSTONES_BY_HEIGHT)
                .map_err(|e| redb_err("table", e))?;
            new_count = 0usize;
            for t in entries {
                let key = TxKey { txid: t.txid };
                let value = TombstoneIndexValue {
                    deletion_height: t.deletion_height,
                    generation: t.generation,
                    shard: t.shard,
                    cause: t.cause,
                };
                // If this key already has a row from an earlier log entry,
                // remove its stale height-forward key before overwriting.
                if let Some(prev) = table.get(key.txid).map_err(|e| redb_err("get", e))? {
                    let prev_val = TombstoneIndexValue::decode(&prev.value());
                    drop(prev);
                    by_height
                        .remove(make_height_key(prev_val.deletion_height, &key))
                        .map_err(|e| redb_err("remove", e))?;
                } else {
                    new_count += 1;
                }
                table
                    .insert(key.txid, value.encode())
                    .map_err(|e| redb_err("insert", e))?;
                by_height
                    .insert(make_height_key(t.deletion_height, &key), ())
                    .map_err(|e| redb_err("insert", e))?;
            }
        }
        txn.commit().map_err(|e| redb_err("commit", e))?;
        self.count = new_count;
        Ok(())
    }
}

/// Build the height-forward composite key: `deletion_height_be(4) || txid(32)`.
/// Big-endian height so lexicographic redb order matches numeric order.
fn make_height_key(height: u32, key: &TxKey) -> [u8; 36] {
    let mut buf = [0u8; 36];
    buf[0..4].copy_from_slice(&height.to_be_bytes());
    buf[4..36].copy_from_slice(&key.txid);
    buf
}

/// Wrap a redb error into [`TombstoneIndexError::Redb`] with an operation tag.
fn redb_err<E: std::fmt::Display>(op: &str, e: E) -> TombstoneIndexError {
    TombstoneIndexError::Redb {
        detail: format!("{op}: {e}"),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tombstone::TombstoneCause;

    fn key(n: u8) -> TxKey {
        let mut txid = [0u8; 32];
        txid[0] = n;
        TxKey { txid }
    }

    fn open_temp() -> (tempfile::TempDir, RedbTombstoneIndex) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("tombstones.redb");
        let idx = RedbTombstoneIndex::open(&db_path, 16 * 1024 * 1024).unwrap();
        (dir, idx)
    }

    #[test]
    fn insert_get_round_trip() {
        let (_dir, mut idx) = open_temp();
        idx.insert(key(1), 100, 7, 3, TombstoneCause::SpentDah.as_u8())
            .unwrap();

        assert!(idx.is_tombstoned(&key(1)));
        let v = idx.get(&key(1)).unwrap();
        assert_eq!(v.deletion_height, 100);
        assert_eq!(v.generation, 7);
        assert_eq!(v.shard, 3);
        assert_eq!(v.cause, TombstoneCause::SpentDah.as_u8());
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn insert_many_round_trips_counts_and_is_idempotent() {
        let (_dir, mut idx) = open_temp();
        // One batched transaction with three distinct keys.
        idx.insert_many(&[
            (key(1), 100, 1, 3, TombstoneCause::SpentDah.as_u8()),
            (key(2), 110, 2, 3, TombstoneCause::Admin.as_u8()),
            (key(3), 120, 3, 4, TombstoneCause::SpentDah.as_u8()),
        ])
        .unwrap();
        assert_eq!(idx.len(), 3, "three new keys counted once each");
        for (k, h, g) in [(key(1), 100u32, 1u32), (key(2), 110, 2), (key(3), 120, 3)] {
            let v = idx.get(&k).expect("row present after batch insert");
            assert_eq!(v.deletion_height, h);
            assert_eq!(v.generation, g);
        }
        // Equivalent to per-row inserts: max height reflects the batch, and a
        // height-forward GC below 115 removes exactly the two rows < 115.
        assert_eq!(idx.max_deletion_height().unwrap(), Some(120));

        // Re-inserting an existing key in a batch overwrites (no count change),
        // and a duplicate key WITHIN the batch is counted once (reads-your-
        // writes inside the transaction).
        idx.insert_many(&[
            (key(2), 999, 9, 3, TombstoneCause::Admin.as_u8()), // overwrite existing
            (key(4), 130, 4, 5, TombstoneCause::Admin.as_u8()), // new
            (key(4), 131, 5, 5, TombstoneCause::SpentDah.as_u8()), // dup-in-batch
        ])
        .unwrap();
        assert_eq!(
            idx.len(),
            4,
            "one new key (key4); key2 overwrite + dup not double-counted"
        );
        assert_eq!(
            idx.get(&key(2)).unwrap().deletion_height,
            999,
            "overwrite applied"
        );
        assert_eq!(
            idx.get(&key(4)).unwrap().deletion_height,
            131,
            "last dup wins"
        );
    }

    #[test]
    fn insert_many_empty_is_noop() {
        let (_dir, mut idx) = open_temp();
        idx.insert_many(&[]).unwrap();
        assert!(idx.is_empty());
    }

    #[test]
    fn absent_key_is_false_and_none() {
        let (_dir, idx) = open_temp();
        assert!(!idx.is_tombstoned(&key(42)));
        assert!(idx.get(&key(42)).is_none());
        assert!(idx.is_empty());
    }

    #[test]
    fn insert_same_key_overwrites_no_count_change() {
        let (_dir, mut idx) = open_temp();
        idx.insert(key(1), 100, 1, 0, TombstoneCause::SpentDah.as_u8())
            .unwrap();
        assert_eq!(idx.len(), 1);
        idx.insert(key(1), 200, 2, 0, TombstoneCause::Admin.as_u8())
            .unwrap();
        assert_eq!(idx.len(), 1);
        let v = idx.get(&key(1)).unwrap();
        assert_eq!(v.deletion_height, 200);
        assert_eq!(v.generation, 2);
        assert_eq!(v.cause, TombstoneCause::Admin.as_u8());
    }

    #[test]
    fn range_delete_below_height_removes_only_below() {
        let (_dir, mut idx) = open_temp();
        idx.insert(key(1), 100, 0, 0, 0).unwrap();
        idx.insert(key(2), 150, 0, 0, 0).unwrap();
        idx.insert(key(3), 200, 0, 0, 0).unwrap();
        idx.insert(key(4), 250, 0, 0, 0).unwrap();
        assert_eq!(idx.len(), 4);

        let removed = idx.range_delete_below_height(200).unwrap();
        assert_eq!(removed, 2, "heights 100 and 150 should be removed");
        assert_eq!(idx.len(), 2);
        assert!(!idx.is_tombstoned(&key(1)));
        assert!(!idx.is_tombstoned(&key(2)));
        // height == threshold is kept (strictly-below semantics).
        assert!(idx.is_tombstoned(&key(3)));
        assert!(idx.is_tombstoned(&key(4)));
    }

    #[test]
    fn max_deletion_height_returns_highest_or_none() {
        let (_dir, mut idx) = open_temp();
        // Empty index → None.
        assert_eq!(idx.max_deletion_height().unwrap(), None);

        idx.insert(key(1), 100, 0, 0, 0).unwrap();
        idx.insert(key(2), 250, 0, 0, 0).unwrap();
        idx.insert(key(3), 150, 0, 0, 0).unwrap();
        assert_eq!(idx.max_deletion_height().unwrap(), Some(250));

        // Removing the max below a threshold lowers the reported max.
        idx.range_delete_below_height(200).unwrap();
        assert_eq!(idx.max_deletion_height().unwrap(), Some(250));
        idx.range_delete_below_height(300).unwrap();
        assert_eq!(idx.max_deletion_height().unwrap(), None);
    }

    #[test]
    fn tombstones_for_shard_filters_by_stored_shard() {
        let (_dir, mut idx) = open_temp();
        // Keys 1 and 3 in shard 7; key 2 in shard 9. The generation is the
        // record's deletion-generation returned to the reconciliation path.
        idx.insert(key(1), 100, 11, 7, TombstoneCause::SpentDah.as_u8())
            .unwrap();
        idx.insert(key(2), 100, 22, 9, TombstoneCause::SpentDah.as_u8())
            .unwrap();
        idx.insert(key(3), 100, 33, 7, TombstoneCause::Admin.as_u8())
            .unwrap();

        let mut s7 = idx.tombstones_for_shard(7);
        s7.sort_by_key(|(k, _)| k.txid);
        assert_eq!(s7, vec![(key(1), 11u32), (key(3), 33u32)]);

        let s9 = idx.tombstones_for_shard(9);
        assert_eq!(s9, vec![(key(2), 22u32)]);

        // A shard with no tombstones returns empty.
        assert!(idx.tombstones_for_shard(123).is_empty());
    }

    #[test]
    fn range_delete_below_zero_is_noop() {
        let (_dir, mut idx) = open_temp();
        idx.insert(key(1), 0, 0, 0, 0).unwrap();
        idx.insert(key(2), 5, 0, 0, 0).unwrap();
        let removed = idx.range_delete_below_height(0).unwrap();
        assert_eq!(removed, 0);
        assert_eq!(idx.len(), 2);
    }

    #[test]
    fn range_delete_idempotent() {
        let (_dir, mut idx) = open_temp();
        idx.insert(key(1), 100, 0, 0, 0).unwrap();
        idx.insert(key(2), 300, 0, 0, 0).unwrap();
        assert_eq!(idx.range_delete_below_height(200).unwrap(), 1);
        assert_eq!(idx.range_delete_below_height(200).unwrap(), 0);
        assert_eq!(idx.len(), 1);
        assert!(idx.is_tombstoned(&key(2)));
    }

    #[test]
    fn rebuild_from_clears_and_repopulates() {
        let (_dir, mut idx) = open_temp();
        // Pre-existing rows that must be wiped by rebuild.
        idx.insert(key(99), 999, 0, 0, 0).unwrap();
        idx.insert(key(98), 888, 0, 0, 0).unwrap();
        assert_eq!(idx.len(), 2);

        let entries = vec![
            Tombstone::new(key(1).txid, 5, 100, 11, TombstoneCause::SpentDah, 0),
            Tombstone::new(key(2).txid, 6, 200, 22, TombstoneCause::Admin, 0),
            Tombstone::new(key(3).txid, 7, 300, 33, TombstoneCause::MigrationPrune, 0),
        ];
        idx.rebuild_from(entries.into_iter()).unwrap();

        assert_eq!(idx.len(), 3);
        assert!(!idx.is_tombstoned(&key(99)));
        assert!(!idx.is_tombstoned(&key(98)));

        let v1 = idx.get(&key(1)).unwrap();
        assert_eq!(v1.deletion_height, 100);
        assert_eq!(v1.generation, 11);
        assert_eq!(v1.shard, 5);
        assert_eq!(v1.cause, TombstoneCause::SpentDah.as_u8());

        let v3 = idx.get(&key(3)).unwrap();
        assert_eq!(v3.deletion_height, 300);
        assert_eq!(v3.cause, TombstoneCause::MigrationPrune.as_u8());

        // Range-delete still works on the rebuilt forward table.
        assert_eq!(idx.range_delete_below_height(250).unwrap(), 2);
        assert!(idx.is_tombstoned(&key(3)));
    }

    #[test]
    fn rebuild_from_dedups_repeated_key_last_wins() {
        let (_dir, mut idx) = open_temp();
        // Two log entries for the same key: the later (higher-gen) one wins.
        let entries = vec![
            Tombstone::new(key(1).txid, 0, 100, 5, TombstoneCause::SpentDah, 0),
            Tombstone::new(key(1).txid, 0, 180, 6, TombstoneCause::SpentDah, 0),
        ];
        idx.rebuild_from(entries.into_iter()).unwrap();
        assert_eq!(idx.len(), 1);
        let v = idx.get(&key(1)).unwrap();
        assert_eq!(v.deletion_height, 180);
        assert_eq!(v.generation, 6);

        // The stale height-forward key (100) must have been removed, so a
        // range-delete below 150 keeps the row (its live height is 180).
        assert_eq!(idx.range_delete_below_height(150).unwrap(), 0);
        assert!(idx.is_tombstoned(&key(1)));
    }

    #[test]
    fn persistence_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("tombstones.redb");
        {
            let mut idx = RedbTombstoneIndex::open(&db_path, 16 * 1024 * 1024).unwrap();
            idx.insert(key(1), 100, 1, 2, 0).unwrap();
            idx.insert(key(2), 200, 3, 4, 1).unwrap();
        }
        let idx = RedbTombstoneIndex::open(&db_path, 16 * 1024 * 1024).unwrap();
        assert_eq!(idx.len(), 2);
        assert_eq!(idx.get(&key(1)).unwrap().deletion_height, 100);
        assert_eq!(idx.get(&key(2)).unwrap().shard, 4);
    }
}
