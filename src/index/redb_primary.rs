//! ReDB-backed primary index implementation.
//!
//! Stores `TxKey -> TxIndexEntry` in a crash-durable B+ tree. Trades
//! throughput for dramatically lower RAM requirements compared to the
//! in-memory Robin Hood hash table.

use crate::index::hashtable::{TxIndexEntry, TxKey};
use crate::index::{IndexError, IndexStats};
use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use std::path::Path;

/// Entry value size: 1 + 8 + 4 + 1 + 1 + 4 + 4 + 4 + 4 = 31 bytes.
const ENTRY_VALUE_SIZE: usize = 31;

/// ReDB table definition: txid(32 bytes) -> serialized TxIndexEntry(31 bytes).
const PRIMARY_TABLE: TableDefinition<[u8; 32], [u8; ENTRY_VALUE_SIZE]> =
    TableDefinition::new("primary_index");

/// ReDB-backed primary index.
///
/// Each mutation is a separate write transaction committed to disk. Reads use
/// MVCC snapshots and do not block writes.
pub struct RedbPrimary {
    db: Database,
    /// Cached entry count, maintained on insert/remove to avoid table scans.
    count: usize,
}

/// Batch update parameters for [`RedbPrimary::update_cached_fields_batch`].
#[derive(Clone, Debug)]
pub struct CachedFieldsUpdate {
    /// The transaction key to update.
    pub key: TxKey,
    /// Transaction-level flags.
    pub tx_flags: u8,
    /// Number of entries in the block.
    pub block_entry_count: u8,
    /// Number of spent UTXOs.
    pub spent_utxos: u32,
    /// Delete-at-height or preserve-until value.
    pub dah_or_preserve: u32,
    /// Unmined-since timestamp.
    pub unmined_since: u32,
    /// Generation counter.
    pub generation: u32,
}

impl RedbPrimary {
    /// Start a write transaction with eventual durability.
    ///
    /// TeraSlab's redo log (WAL) provides crash recovery, so the redb index
    /// does not need per-operation fsync. `Durability::Eventual` lets redb
    /// batch fsyncs internally, which is 10-100x faster for small writes.
    #[allow(clippy::result_large_err)]
    fn begin_write(&self) -> Result<redb::WriteTransaction, redb::TransactionError> {
        let mut txn = self.db.begin_write()?;
        txn.set_durability(redb::Durability::Eventual);
        Ok(txn)
    }

    /// Open or create a redb primary index at the given path.
    ///
    /// If the database file exists, it is opened and the entry count is
    /// recovered from a table scan. If it does not exist, a fresh empty
    /// database is created.
    pub fn open(path: &Path, cache_size: usize) -> Result<Self, IndexError> {
        let db = redb::Builder::new()
            .set_cache_size(cache_size)
            .create(path)
            .map_err(|e| IndexError::FormatError {
                detail: format!("redb open error: {e}"),
            })?;

        // Ensure the table exists by opening a write transaction.
        {
            let mut txn = db.begin_write().map_err(map_redb_txn_err)?;
            txn.set_durability(redb::Durability::Eventual);
            txn.open_table(PRIMARY_TABLE).map_err(map_redb_table_err)?;
            txn.commit().map_err(map_redb_commit_err)?;
        }

        // Recover entry count from the table.
        let count = {
            let txn = db.begin_read().map_err(map_redb_txn_err)?;
            let table = txn.open_table(PRIMARY_TABLE).map_err(map_redb_table_err)?;
            table.len().map_err(map_redb_storage_err)? as usize
        };

        Ok(Self { db, count })
    }

    /// Look up a transaction's index entry.
    pub fn lookup(&self, key: &TxKey) -> Option<TxIndexEntry> {
        let txn = match self.db.begin_read() {
            Ok(t) => t,
            Err(e) => {
                eprintln!("redb lookup: begin_read failed: {e}");
                return None;
            }
        };
        let table = match txn.open_table(PRIMARY_TABLE) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("redb lookup: open_table failed: {e}");
                return None;
            }
        };
        match table.get(key.txid) {
            Ok(Some(guard)) => Some(deserialize_entry(&guard.value())),
            Ok(None) => None,
            Err(e) => {
                eprintln!("redb lookup: get failed: {e}");
                None
            }
        }
    }

    /// Register a new transaction in the index.
    pub fn register(&mut self, key: TxKey, entry: TxIndexEntry) -> Result<(), IndexError> {
        let value = serialize_entry(&entry);
        let txn = self.begin_write().map_err(map_redb_txn_err)?;
        let is_new = {
            let mut table = txn.open_table(PRIMARY_TABLE).map_err(map_redb_table_err)?;
            let existed = table.insert(key.txid, value).map_err(map_redb_storage_err)?;
            existed.is_none()
        };
        txn.commit().map_err(map_redb_commit_err)?;
        if is_new {
            self.count += 1;
        }
        Ok(())
    }

    /// Remove a transaction from the index.
    pub fn unregister(&mut self, key: &TxKey) -> Option<TxIndexEntry> {
        let txn = match self.begin_write() {
            Ok(t) => t,
            Err(e) => {
                eprintln!("redb unregister: begin_write failed: {e}");
                return None;
            }
        };
        let result = {
            let mut table = match txn.open_table(PRIMARY_TABLE) {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("redb unregister: open_table failed: {e}");
                    return None;
                }
            };
            match table.remove(key.txid) {
                Ok(Some(guard)) => Some(deserialize_entry(&guard.value())),
                Ok(None) => None,
                Err(e) => {
                    eprintln!("redb unregister: remove failed: {e}");
                    return None;
                }
            }
        };
        match txn.commit() {
            Ok(()) => {
                if result.is_some() {
                    self.count -= 1;
                }
            }
            Err(e) => {
                eprintln!("redb unregister: commit failed: {e}");
                return None;
            }
        }
        result
    }

    /// Remove multiple transactions from the index in a single write transaction.
    ///
    /// Returns a `Vec` parallel to the input: `Some(entry)` for keys that were
    /// found and removed, `None` for missing keys.
    ///
    /// All removals that succeed are committed atomically. If the commit fails,
    /// the entire batch is treated as failed and `vec![None; keys.len()]` is
    /// returned.
    pub fn unregister_batch(&mut self, keys: &[TxKey]) -> Vec<Option<TxIndexEntry>> {
        if keys.is_empty() {
            return Vec::new();
        }
        let txn = match self.begin_write() {
            Ok(t) => t,
            Err(e) => {
                eprintln!("redb unregister_batch: begin_write failed: {e}");
                return vec![None; keys.len()];
            }
        };
        let mut results = Vec::with_capacity(keys.len());
        let mut removed_count = 0usize;
        {
            let mut table = match txn.open_table(PRIMARY_TABLE) {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("redb unregister_batch: open_table failed: {e}");
                    return vec![None; keys.len()];
                }
            };
            for key in keys {
                match table.remove(key.txid) {
                    Ok(Some(guard)) => {
                        results.push(Some(deserialize_entry(&guard.value())));
                        removed_count += 1;
                    }
                    Ok(None) => results.push(None),
                    Err(e) => {
                        eprintln!("redb unregister_batch: remove failed: {e}");
                        results.push(None);
                    }
                }
            }
        }
        if removed_count > 0 {
            match txn.commit() {
                Ok(()) => self.count -= removed_count,
                Err(e) => {
                    eprintln!("redb unregister_batch: commit failed: {e}");
                    return vec![None; keys.len()];
                }
            }
        }
        results
    }

    /// Update cached fields for an existing entry.
    ///
    /// Performs a read-modify-write within a single write transaction.
    ///
    /// # Concurrency
    ///
    /// The caller MUST hold an exclusive lock (e.g. `RwLock::write()`) around
    /// the `PrimaryBackend` before calling this method. The read-modify-write
    /// within the redb transaction is not atomic on its own — without external
    /// locking, concurrent callers could overwrite each other's updates.
    #[allow(clippy::too_many_arguments)]
    pub fn update_cached_fields(
        &mut self,
        key: &TxKey,
        tx_flags: u8,
        block_entry_count: u8,
        spent_utxos: u32,
        dah_or_preserve: u32,
        unmined_since: u32,
        generation: u32,
    ) -> bool {
        let txn = match self.begin_write() {
            Ok(t) => t,
            Err(_) => return false,
        };
        let updated = {
            let mut table = match txn.open_table(PRIMARY_TABLE) {
                Ok(t) => t,
                Err(_) => return false,
            };
            // Read existing entry, copying the value to release the borrow.
            let existing = match table.get(key.txid) {
                Ok(Some(guard)) => {
                    let entry = deserialize_entry(&guard.value());
                    Some(entry)
                }
                _ => None,
            };
            if let Some(mut entry) = existing {
                entry.tx_flags = tx_flags;
                entry.block_entry_count = block_entry_count;
                entry.spent_utxos = spent_utxos;
                entry.dah_or_preserve = dah_or_preserve;
                entry.unmined_since = unmined_since;
                entry.generation = generation;
                if let Err(e) = table.insert(key.txid, serialize_entry(&entry)) {
                    eprintln!("redb update_cached_fields: insert failed: {e}");
                    return false;
                }
                true
            } else {
                false
            }
        };
        if updated && let Err(e) = txn.commit() {
            eprintln!("redb update_cached_fields: commit failed: {e}");
            return false;
        }
        updated
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.count
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Iterate over all entries. Opens a read transaction for the duration.
    ///
    /// Collects all entries into a Vec to avoid lifetime issues with the
    /// read transaction. For large indexes, this allocates significant memory
    /// (~63 bytes per entry — 10M entries requires ~630 MB).
    ///
    /// **Warning**: For very large indexes (>1M entries), consider using
    /// streaming approaches or batch processing instead.
    pub fn iter_collected(&self) -> Vec<(TxKey, TxIndexEntry)> {
        if self.count > 1_000_000 {
            eprintln!(
                "redb iter_collected: materializing {} entries (~{} MB in RAM)",
                self.count,
                self.count * 63 / (1024 * 1024)
            );
        }
        let mut result = Vec::with_capacity(self.count);
        if let Ok(txn) = self.db.begin_read()
            && let Ok(table) = txn.open_table(PRIMARY_TABLE)
            && let Ok(range) = table.iter()
        {
            for (k, v) in range.flatten() {
                let key = TxKey { txid: k.value() };
                let entry = deserialize_entry(&v.value());
                result.push((key, entry));
            }
        }
        result
    }

    /// Index statistics for monitoring.
    pub fn stats(&self) -> IndexStats {
        let file_size = self
            .db
            .begin_read()
            .ok()
            .and_then(|txn| txn.open_table(PRIMARY_TABLE).ok())
            .and_then(|t| {
                // Use count * approximate entry size as memory estimate
                Some(t.len().ok()? as usize * (32 + ENTRY_VALUE_SIZE + 64))
            })
            .unwrap_or(0);

        IndexStats {
            entry_count: self.count,
            capacity: self.count, // B-tree has no fixed capacity
            load_factor: if self.count > 0 { 1.0 } else { 0.0 },
            hugepage_enabled: false,
            max_probe_distance: 0,
            memory_bytes: file_size,
        }
    }

    /// Register multiple transactions in a single write transaction.
    ///
    /// Much faster than calling [`register`](Self::register) in a loop because
    /// only one fsync is performed for the entire batch. Tracks new-vs-update
    /// per entry for accurate count maintenance.
    pub fn register_batch(
        &mut self,
        entries: &[(TxKey, TxIndexEntry)],
    ) -> Result<(), IndexError> {
        if entries.is_empty() {
            return Ok(());
        }
        let txn = self.begin_write().map_err(map_redb_txn_err)?;
        let mut new_count = 0usize;
        {
            let mut table = txn.open_table(PRIMARY_TABLE).map_err(map_redb_table_err)?;
            for (key, entry) in entries {
                let value = serialize_entry(entry);
                let existed = table
                    .insert(key.txid, value)
                    .map_err(map_redb_storage_err)?;
                if existed.is_none() {
                    new_count += 1;
                }
            }
        }
        txn.commit().map_err(map_redb_commit_err)?;
        self.count += new_count;
        Ok(())
    }

    /// Update cached fields for multiple entries in a single write transaction.
    ///
    /// Performs a read-modify-write for each entry within one redb transaction,
    /// amortizing the `begin_write() -> commit()` overhead across all updates.
    /// Returns the number of entries successfully updated (missing keys are skipped).
    ///
    /// # Concurrency
    ///
    /// The caller MUST hold an exclusive lock around the `PrimaryBackend` before
    /// calling this method, same as for individual `update_cached_fields` calls.
    pub fn update_cached_fields_batch(&mut self, updates: &[CachedFieldsUpdate]) -> usize {
        if updates.is_empty() {
            return 0;
        }
        let txn = match self.begin_write() {
            Ok(t) => t,
            Err(_) => return 0,
        };
        let mut count = 0usize;
        {
            let mut table = match txn.open_table(PRIMARY_TABLE) {
                Ok(t) => t,
                Err(_) => return 0,
            };
            for update in updates {
                let existing = match table.get(update.key.txid) {
                    Ok(Some(guard)) => Some(deserialize_entry(&guard.value())),
                    _ => None,
                };
                if let Some(mut entry) = existing {
                    entry.tx_flags = update.tx_flags;
                    entry.block_entry_count = update.block_entry_count;
                    entry.spent_utxos = update.spent_utxos;
                    entry.dah_or_preserve = update.dah_or_preserve;
                    entry.unmined_since = update.unmined_since;
                    entry.generation = update.generation;
                    if table.insert(update.key.txid, serialize_entry(&entry)).is_ok() {
                        count += 1;
                    }
                }
            }
        }
        if count > 0
            && let Err(e) = txn.commit()
        {
            eprintln!("redb update_cached_fields_batch: commit failed: {e}");
            return 0;
        }
        count
    }

    /// Snapshot is a no-op for redb (already crash-durable).
    pub fn snapshot(&self, _path: &Path) -> Result<(), IndexError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

/// Serialize a TxIndexEntry into a fixed 31-byte array.
fn serialize_entry(e: &TxIndexEntry) -> [u8; ENTRY_VALUE_SIZE] {
    let mut buf = [0u8; ENTRY_VALUE_SIZE];
    buf[0] = e.device_id;
    buf[1..9].copy_from_slice(&e.record_offset.to_le_bytes());
    buf[9..13].copy_from_slice(&e.utxo_count.to_le_bytes());
    buf[13] = e.block_entry_count;
    buf[14] = e.tx_flags;
    buf[15..19].copy_from_slice(&e.spent_utxos.to_le_bytes());
    buf[19..23].copy_from_slice(&e.dah_or_preserve.to_le_bytes());
    buf[23..27].copy_from_slice(&e.unmined_since.to_le_bytes());
    buf[27..31].copy_from_slice(&e.generation.to_le_bytes());
    buf
}

/// Deserialize a TxIndexEntry from a 31-byte array.
fn deserialize_entry(buf: &[u8; ENTRY_VALUE_SIZE]) -> TxIndexEntry {
    TxIndexEntry {
        device_id: buf[0],
        record_offset: u64::from_le_bytes(buf[1..9].try_into().unwrap()),
        utxo_count: u32::from_le_bytes(buf[9..13].try_into().unwrap()),
        block_entry_count: buf[13],
        tx_flags: buf[14],
        spent_utxos: u32::from_le_bytes(buf[15..19].try_into().unwrap()),
        dah_or_preserve: u32::from_le_bytes(buf[19..23].try_into().unwrap()),
        unmined_since: u32::from_le_bytes(buf[23..27].try_into().unwrap()),
        generation: u32::from_le_bytes(buf[27..31].try_into().unwrap()),
    }
}

// ---------------------------------------------------------------------------
// Error mapping helpers
// ---------------------------------------------------------------------------

fn map_redb_txn_err(e: redb::TransactionError) -> IndexError {
    IndexError::FormatError {
        detail: format!("redb transaction error: {e}"),
    }
}

fn map_redb_table_err(e: redb::TableError) -> IndexError {
    IndexError::FormatError {
        detail: format!("redb table error: {e}"),
    }
}

fn map_redb_commit_err(e: redb::CommitError) -> IndexError {
    IndexError::FormatError {
        detail: format!("redb commit error: {e}"),
    }
}

fn map_redb_storage_err(e: redb::StorageError) -> IndexError {
    IndexError::FormatError {
        detail: format!("redb storage error: {e}"),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_key(n: u64) -> TxKey {
        let mut txid = [0u8; 32];
        txid[0..8].copy_from_slice(&n.to_le_bytes());
        txid[8..16].copy_from_slice(&(n.wrapping_mul(0x9E3779B97F4A7C15)).to_le_bytes());
        TxKey { txid }
    }

    fn make_entry(offset: u64) -> TxIndexEntry {
        TxIndexEntry {
            device_id: 0,
            record_offset: offset,
            utxo_count: 10,
            block_entry_count: 2,
            tx_flags: 0x05,
            spent_utxos: 3,
            dah_or_preserve: 100,
            unmined_since: 500,
            generation: 7,
        }
    }

    fn open_temp() -> (tempfile::TempDir, RedbPrimary) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.redb");
        let primary = RedbPrimary::open(&db_path, 64 * 1024 * 1024).unwrap();
        (dir, primary)
    }

    #[test]
    fn insert_lookup_single() {
        let (_dir, mut primary) = open_temp();
        let key = make_key(1);
        let entry = make_entry(4096);
        primary.register(key, entry).unwrap();

        let result = primary.lookup(&key).expect("should find entry");
        assert_eq!(result.record_offset, 4096);
        assert_eq!(result.utxo_count, 10);
        assert_eq!(result.block_entry_count, 2);
        assert_eq!(result.tx_flags, 0x05);
        assert_eq!(result.spent_utxos, 3);
        assert_eq!(result.dah_or_preserve, 100);
        assert_eq!(result.unmined_since, 500);
        assert_eq!(result.generation, 7);
    }

    #[test]
    fn lookup_missing_returns_none() {
        let (_dir, primary) = open_temp();
        assert!(primary.lookup(&make_key(999)).is_none());
    }

    #[test]
    fn insert_1000_lookup_all() {
        let (_dir, mut primary) = open_temp();
        for i in 0..1000u64 {
            primary.register(make_key(i), make_entry(i * 100)).unwrap();
        }
        assert_eq!(primary.len(), 1000);

        for i in 0..1000u64 {
            let e = primary.lookup(&make_key(i)).expect("entry should exist");
            assert_eq!(e.record_offset, i * 100);
        }
    }

    #[test]
    fn unregister_returns_entry() {
        let (_dir, mut primary) = open_temp();
        let key = make_key(42);
        primary.register(key, make_entry(8192)).unwrap();

        let removed = primary.unregister(&key).expect("should return removed entry");
        assert_eq!(removed.record_offset, 8192);
        assert_eq!(primary.len(), 0);
        assert!(primary.lookup(&key).is_none());
    }

    #[test]
    fn unregister_missing_returns_none() {
        let (_dir, mut primary) = open_temp();
        assert!(primary.unregister(&make_key(1)).is_none());
    }

    #[test]
    fn update_cached_fields() {
        let (_dir, mut primary) = open_temp();
        let key = make_key(1);
        primary.register(key, make_entry(4096)).unwrap();

        let updated = primary.update_cached_fields(&key, 0xFF, 5, 8, 200, 600, 99);
        assert!(updated);

        let e = primary.lookup(&key).unwrap();
        assert_eq!(e.tx_flags, 0xFF);
        assert_eq!(e.block_entry_count, 5);
        assert_eq!(e.spent_utxos, 8);
        assert_eq!(e.dah_or_preserve, 200);
        assert_eq!(e.unmined_since, 600);
        assert_eq!(e.generation, 99);
        // Unchanged fields
        assert_eq!(e.record_offset, 4096);
        assert_eq!(e.utxo_count, 10);
    }

    #[test]
    fn update_cached_fields_missing_returns_false() {
        let (_dir, mut primary) = open_temp();
        assert!(!primary.update_cached_fields(&make_key(1), 0, 0, 0, 0, 0, 0));
    }

    #[test]
    fn iter_collected() {
        let (_dir, mut primary) = open_temp();
        for i in 0..50u64 {
            primary.register(make_key(i), make_entry(i * 100)).unwrap();
        }

        let entries = primary.iter_collected();
        assert_eq!(entries.len(), 50);

        // Verify all entries are present (order may differ from insertion)
        for i in 0..50u64 {
            let found = entries
                .iter()
                .any(|(k, e)| k == &make_key(i) && e.record_offset == i * 100);
            assert!(found, "entry {i} not found in iter");
        }
    }

    #[test]
    fn persistence_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.redb");

        // Write data
        {
            let mut primary = RedbPrimary::open(&db_path, 64 * 1024 * 1024).unwrap();
            for i in 0..100u64 {
                primary.register(make_key(i), make_entry(i * 100)).unwrap();
            }
            assert_eq!(primary.len(), 100);
        }
        // Drop and reopen
        {
            let primary = RedbPrimary::open(&db_path, 64 * 1024 * 1024).unwrap();
            assert_eq!(primary.len(), 100);
            for i in 0..100u64 {
                let e = primary.lookup(&make_key(i)).expect("entry should survive reopen");
                assert_eq!(e.record_offset, i * 100);
            }
        }
    }

    #[test]
    fn serialization_round_trip() {
        let entry = TxIndexEntry {
            device_id: 3,
            record_offset: 0xDEADBEEF_CAFEBABE,
            utxo_count: 42,
            block_entry_count: 7,
            tx_flags: 0xAB,
            spent_utxos: 12345,
            dah_or_preserve: 67890,
            unmined_since: 11111,
            generation: 22222,
        };
        let buf = serialize_entry(&entry);
        let restored = deserialize_entry(&buf);
        assert_eq!(restored.device_id, entry.device_id);
        assert_eq!(restored.record_offset, entry.record_offset);
        assert_eq!(restored.utxo_count, entry.utxo_count);
        assert_eq!(restored.block_entry_count, entry.block_entry_count);
        assert_eq!(restored.tx_flags, entry.tx_flags);
        assert_eq!(restored.spent_utxos, entry.spent_utxos);
        assert_eq!(restored.dah_or_preserve, entry.dah_or_preserve);
        assert_eq!(restored.unmined_since, entry.unmined_since);
        assert_eq!(restored.generation, entry.generation);
    }

    #[test]
    fn snapshot_is_noop() {
        let (_dir, primary) = open_temp();
        let dir2 = tempfile::tempdir().unwrap();
        primary.snapshot(dir2.path().join("noop.snap").as_path()).unwrap();
    }

    #[test]
    fn stats_report() {
        let (_dir, mut primary) = open_temp();
        let stats = primary.stats();
        assert_eq!(stats.entry_count, 0);
        assert!(!stats.hugepage_enabled);

        for i in 0..10u64 {
            primary.register(make_key(i), make_entry(i)).unwrap();
        }
        let stats = primary.stats();
        assert_eq!(stats.entry_count, 10);
    }

    #[test]
    fn register_overwrite_same_key() {
        let (_dir, mut primary) = open_temp();
        let key = make_key(1);
        primary.register(key, make_entry(100)).unwrap();
        primary.register(key, make_entry(200)).unwrap();

        assert_eq!(primary.len(), 1); // Count should not double
        let e = primary.lookup(&key).unwrap();
        assert_eq!(e.record_offset, 200);
    }

    #[test]
    fn empty_iter_collected() {
        let (_dir, primary) = open_temp();
        let entries = primary.iter_collected();
        assert!(entries.is_empty());
    }

    #[test]
    fn is_empty_transitions() {
        let (_dir, mut primary) = open_temp();
        assert!(primary.is_empty());

        primary.register(make_key(1), make_entry(100)).unwrap();
        assert!(!primary.is_empty());

        primary.unregister(&make_key(1));
        assert!(primary.is_empty());
    }

    #[test]
    fn stats_load_factor() {
        let (_dir, mut primary) = open_temp();
        let stats = primary.stats();
        assert_eq!(stats.load_factor, 0.0);
        assert_eq!(stats.max_probe_distance, 0);

        primary.register(make_key(1), make_entry(100)).unwrap();
        let stats = primary.stats();
        assert_eq!(stats.load_factor, 1.0);
    }

    #[test]
    fn stats_memory_bytes_grows() {
        let (_dir, mut primary) = open_temp();
        let stats_empty = primary.stats();

        for i in 0..50u64 {
            primary.register(make_key(i), make_entry(i * 100)).unwrap();
        }
        let stats_filled = primary.stats();
        assert!(
            stats_filled.memory_bytes > stats_empty.memory_bytes,
            "memory_bytes should grow with entries: empty={}, filled={}",
            stats_empty.memory_bytes,
            stats_filled.memory_bytes,
        );
    }

    #[test]
    fn unregister_then_reregister() {
        let (_dir, mut primary) = open_temp();
        let key = make_key(1);

        primary.register(key, make_entry(100)).unwrap();
        primary.unregister(&key);
        assert!(primary.is_empty());
        assert!(primary.lookup(&key).is_none());

        // Re-register same key with different data
        let new_entry = TxIndexEntry {
            device_id: 2,
            record_offset: 9999,
            utxo_count: 42,
            block_entry_count: 0,
            tx_flags: 0,
            spent_utxos: 0,
            dah_or_preserve: 0,
            unmined_since: 0,
            generation: 0,
        };
        primary.register(key, new_entry).unwrap();
        assert_eq!(primary.len(), 1);

        let e = primary.lookup(&key).unwrap();
        assert_eq!(e.device_id, 2);
        assert_eq!(e.record_offset, 9999);
        assert_eq!(e.utxo_count, 42);
    }

    #[test]
    fn unregister_count_consistent_after_ops() {
        let (_dir, mut primary) = open_temp();
        let n = 50u64;
        for i in 0..n {
            primary.register(make_key(i), make_entry(i * 100)).unwrap();
        }
        assert_eq!(primary.len(), n as usize);

        // Unregister all
        for i in 0..n {
            let removed = primary.unregister(&make_key(i));
            assert!(removed.is_some(), "should have removed entry {i}");
        }
        assert_eq!(primary.len(), 0);
        assert!(primary.is_empty());

        // Double-unregister should be no-op
        for i in 0..n {
            assert!(primary.unregister(&make_key(i)).is_none());
        }
        assert_eq!(primary.len(), 0);
    }

    #[test]
    fn register_batch_basic() {
        let (_dir, mut primary) = open_temp();
        let entries: Vec<_> = (0..100u64)
            .map(|i| (make_key(i), make_entry(i * 100)))
            .collect();
        primary.register_batch(&entries).unwrap();

        assert_eq!(primary.len(), 100);
        for i in 0..100u64 {
            let e = primary.lookup(&make_key(i)).expect("entry should exist");
            assert_eq!(e.record_offset, i * 100);
        }
    }

    #[test]
    fn register_batch_with_duplicates() {
        let (_dir, mut primary) = open_temp();
        // Batch with duplicate keys — last write wins, count should be 1
        let entries = vec![
            (make_key(1), make_entry(100)),
            (make_key(1), make_entry(200)),
            (make_key(1), make_entry(300)),
        ];
        primary.register_batch(&entries).unwrap();

        assert_eq!(primary.len(), 1);
        let e = primary.lookup(&make_key(1)).unwrap();
        assert_eq!(e.record_offset, 300);
    }

    #[test]
    fn register_batch_empty() {
        let (_dir, mut primary) = open_temp();
        primary.register_batch(&[]).unwrap();
        assert_eq!(primary.len(), 0);
    }

    #[test]
    fn register_batch_matches_individual() {
        // Register entries individually
        let dir1 = tempfile::tempdir().unwrap();
        let db_path1 = dir1.path().join("test1.redb");
        let mut primary1 = RedbPrimary::open(&db_path1, 64 * 1024 * 1024).unwrap();
        for i in 0..50u64 {
            primary1.register(make_key(i), make_entry(i * 100)).unwrap();
        }

        // Register entries as batch
        let dir2 = tempfile::tempdir().unwrap();
        let db_path2 = dir2.path().join("test2.redb");
        let mut primary2 = RedbPrimary::open(&db_path2, 64 * 1024 * 1024).unwrap();
        let entries: Vec<_> = (0..50u64)
            .map(|i| (make_key(i), make_entry(i * 100)))
            .collect();
        primary2.register_batch(&entries).unwrap();

        // Both should produce identical results
        assert_eq!(primary1.len(), primary2.len());
        for i in 0..50u64 {
            let e1 = primary1.lookup(&make_key(i)).unwrap();
            let e2 = primary2.lookup(&make_key(i)).unwrap();
            assert_eq!(e1.record_offset, e2.record_offset);
            assert_eq!(e1.utxo_count, e2.utxo_count);
        }
    }

    #[test]
    fn serialization_all_zeros() {
        let entry = TxIndexEntry {
            device_id: 0,
            record_offset: 0,
            utxo_count: 0,
            block_entry_count: 0,
            tx_flags: 0,
            spent_utxos: 0,
            dah_or_preserve: 0,
            unmined_since: 0,
            generation: 0,
        };
        let buf = serialize_entry(&entry);
        assert_eq!(buf, [0u8; ENTRY_VALUE_SIZE]);
        let restored = deserialize_entry(&buf);
        assert_eq!(restored.device_id, 0);
        assert_eq!(restored.record_offset, 0);
        assert_eq!(restored.utxo_count, 0);
        assert_eq!(restored.generation, 0);
    }

    #[test]
    fn serialization_max_values() {
        let entry = TxIndexEntry {
            device_id: u8::MAX,
            record_offset: u64::MAX,
            utxo_count: u32::MAX,
            block_entry_count: u8::MAX,
            tx_flags: u8::MAX,
            spent_utxos: u32::MAX,
            dah_or_preserve: u32::MAX,
            unmined_since: u32::MAX,
            generation: u32::MAX,
        };
        let buf = serialize_entry(&entry);
        let restored = deserialize_entry(&buf);
        assert_eq!(restored.device_id, u8::MAX);
        assert_eq!(restored.record_offset, u64::MAX);
        assert_eq!(restored.utxo_count, u32::MAX);
        assert_eq!(restored.block_entry_count, u8::MAX);
        assert_eq!(restored.tx_flags, u8::MAX);
        assert_eq!(restored.spent_utxos, u32::MAX);
        assert_eq!(restored.dah_or_preserve, u32::MAX);
        assert_eq!(restored.unmined_since, u32::MAX);
        assert_eq!(restored.generation, u32::MAX);
    }

    #[test]
    fn update_cached_fields_batch_basic() {
        let (_dir, mut primary) = open_temp();
        for i in 0..5u64 {
            primary.register(make_key(i), make_entry(i * 100)).unwrap();
        }

        let updates: Vec<super::CachedFieldsUpdate> = (0..5u64)
            .map(|i| super::CachedFieldsUpdate {
                key: make_key(i),
                tx_flags: 0xAA,
                block_entry_count: (i as u8) + 1,
                spent_utxos: (i as u32) * 10,
                dah_or_preserve: 500,
                unmined_since: 600,
                generation: 42,
            })
            .collect();

        let updated = primary.update_cached_fields_batch(&updates);
        assert_eq!(updated, 5);

        for i in 0..5u64 {
            let e = primary.lookup(&make_key(i)).unwrap();
            assert_eq!(e.tx_flags, 0xAA);
            assert_eq!(e.block_entry_count, (i as u8) + 1);
            assert_eq!(e.spent_utxos, (i as u32) * 10);
            assert_eq!(e.dah_or_preserve, 500);
            assert_eq!(e.unmined_since, 600);
            assert_eq!(e.generation, 42);
            // Unchanged fields
            assert_eq!(e.record_offset, i * 100);
            assert_eq!(e.utxo_count, 10);
        }
    }

    #[test]
    fn update_cached_fields_batch_with_missing_keys() {
        let (_dir, mut primary) = open_temp();
        primary.register(make_key(1), make_entry(100)).unwrap();
        primary.register(make_key(3), make_entry(300)).unwrap();

        let updates = vec![
            super::CachedFieldsUpdate {
                key: make_key(1),
                tx_flags: 0xFF,
                block_entry_count: 5,
                spent_utxos: 8,
                dah_or_preserve: 200,
                unmined_since: 600,
                generation: 99,
            },
            super::CachedFieldsUpdate {
                key: make_key(2), // missing
                tx_flags: 0xFF,
                block_entry_count: 5,
                spent_utxos: 8,
                dah_or_preserve: 200,
                unmined_since: 600,
                generation: 99,
            },
            super::CachedFieldsUpdate {
                key: make_key(3),
                tx_flags: 0xBB,
                block_entry_count: 7,
                spent_utxos: 12,
                dah_or_preserve: 300,
                unmined_since: 700,
                generation: 50,
            },
        ];

        let updated = primary.update_cached_fields_batch(&updates);
        assert_eq!(updated, 2);

        let e1 = primary.lookup(&make_key(1)).unwrap();
        assert_eq!(e1.tx_flags, 0xFF);
        assert_eq!(e1.generation, 99);

        assert!(primary.lookup(&make_key(2)).is_none());

        let e3 = primary.lookup(&make_key(3)).unwrap();
        assert_eq!(e3.tx_flags, 0xBB);
        assert_eq!(e3.generation, 50);
    }

    #[test]
    fn update_cached_fields_batch_empty() {
        let (_dir, mut primary) = open_temp();
        primary.register(make_key(1), make_entry(100)).unwrap();
        let updated = primary.update_cached_fields_batch(&[]);
        assert_eq!(updated, 0);
        // Original entry unchanged
        let e = primary.lookup(&make_key(1)).unwrap();
        assert_eq!(e.record_offset, 100);
    }

    #[test]
    fn update_cached_fields_batch_matches_individual() {
        // Individual updates
        let dir1 = tempfile::tempdir().unwrap();
        let db_path1 = dir1.path().join("test1.redb");
        let mut p1 = RedbPrimary::open(&db_path1, 64 * 1024 * 1024).unwrap();
        for i in 0..10u64 {
            p1.register(make_key(i), make_entry(i * 100)).unwrap();
        }
        for i in 0..10u64 {
            p1.update_cached_fields(&make_key(i), 0xCC, 3, i as u32, 400, 500, 60);
        }

        // Batch update
        let dir2 = tempfile::tempdir().unwrap();
        let db_path2 = dir2.path().join("test2.redb");
        let mut p2 = RedbPrimary::open(&db_path2, 64 * 1024 * 1024).unwrap();
        for i in 0..10u64 {
            p2.register(make_key(i), make_entry(i * 100)).unwrap();
        }
        let updates: Vec<super::CachedFieldsUpdate> = (0..10u64)
            .map(|i| super::CachedFieldsUpdate {
                key: make_key(i),
                tx_flags: 0xCC,
                block_entry_count: 3,
                spent_utxos: i as u32,
                dah_or_preserve: 400,
                unmined_since: 500,
                generation: 60,
            })
            .collect();
        let updated = p2.update_cached_fields_batch(&updates);
        assert_eq!(updated, 10);

        // Both should produce identical results
        for i in 0..10u64 {
            let e1 = p1.lookup(&make_key(i)).unwrap();
            let e2 = p2.lookup(&make_key(i)).unwrap();
            assert_eq!(e1.tx_flags, e2.tx_flags);
            assert_eq!(e1.block_entry_count, e2.block_entry_count);
            assert_eq!(e1.spent_utxos, e2.spent_utxos);
            assert_eq!(e1.dah_or_preserve, e2.dah_or_preserve);
            assert_eq!(e1.unmined_since, e2.unmined_since);
            assert_eq!(e1.generation, e2.generation);
            assert_eq!(e1.record_offset, e2.record_offset);
            assert_eq!(e1.utxo_count, e2.utxo_count);
        }
    }

    #[test]
    fn update_cached_fields_batch_persists() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.redb");

        {
            let mut primary = RedbPrimary::open(&db_path, 64 * 1024 * 1024).unwrap();
            for i in 0..3u64 {
                primary.register(make_key(i), make_entry(i * 100)).unwrap();
            }
            let updates: Vec<super::CachedFieldsUpdate> = (0..3u64)
                .map(|i| super::CachedFieldsUpdate {
                    key: make_key(i),
                    tx_flags: 0xDD,
                    block_entry_count: 9,
                    spent_utxos: 77,
                    dah_or_preserve: 888,
                    unmined_since: 999,
                    generation: 44,
                })
                .collect();
            primary.update_cached_fields_batch(&updates);
        }

        // Reopen and verify
        let primary = RedbPrimary::open(&db_path, 64 * 1024 * 1024).unwrap();
        for i in 0..3u64 {
            let e = primary.lookup(&make_key(i)).unwrap();
            assert_eq!(e.tx_flags, 0xDD);
            assert_eq!(e.generation, 44);
            assert_eq!(e.record_offset, i * 100);
        }
    }

    #[test]
    fn update_cached_fields_preserves_device_id() {
        let (_dir, mut primary) = open_temp();
        let key = make_key(1);
        let entry = TxIndexEntry {
            device_id: 5,
            record_offset: 4096,
            utxo_count: 10,
            block_entry_count: 2,
            tx_flags: 0x05,
            spent_utxos: 3,
            dah_or_preserve: 100,
            unmined_since: 500,
            generation: 7,
        };
        primary.register(key, entry).unwrap();

        primary.update_cached_fields(&key, 0xFF, 10, 20, 300, 700, 50);

        let e = primary.lookup(&key).unwrap();
        // device_id and record_offset and utxo_count should be unchanged
        assert_eq!(e.device_id, 5);
        assert_eq!(e.record_offset, 4096);
        assert_eq!(e.utxo_count, 10);
    }

    #[test]
    fn persistence_with_updates() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.redb");

        {
            let mut primary = RedbPrimary::open(&db_path, 64 * 1024 * 1024).unwrap();
            primary.register(make_key(1), make_entry(100)).unwrap();
            primary.update_cached_fields(&make_key(1), 0xAA, 9, 99, 999, 9999, 42);
        }

        // Reopen and verify the updated fields persisted
        let primary = RedbPrimary::open(&db_path, 64 * 1024 * 1024).unwrap();
        let e = primary.lookup(&make_key(1)).unwrap();
        assert_eq!(e.tx_flags, 0xAA);
        assert_eq!(e.block_entry_count, 9);
        assert_eq!(e.spent_utxos, 99);
        assert_eq!(e.dah_or_preserve, 999);
        assert_eq!(e.unmined_since, 9999);
        assert_eq!(e.generation, 42);
        // Unchanged
        assert_eq!(e.record_offset, 100);
    }

    #[test]
    fn unregister_batch_basic() {
        let (_dir, mut primary) = open_temp();
        for i in 0..5u64 {
            primary.register(make_key(i), make_entry(i * 100)).unwrap();
        }
        assert_eq!(primary.len(), 5);

        let keys: Vec<_> = (1..4u64).map(make_key).collect();
        let results = primary.unregister_batch(&keys);

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].unwrap().record_offset, 100);
        assert_eq!(results[1].unwrap().record_offset, 200);
        assert_eq!(results[2].unwrap().record_offset, 300);
        assert_eq!(primary.len(), 2);

        // Remaining entries still present
        assert!(primary.lookup(&make_key(0)).is_some());
        assert!(primary.lookup(&make_key(4)).is_some());
        // Removed entries gone
        assert!(primary.lookup(&make_key(1)).is_none());
        assert!(primary.lookup(&make_key(2)).is_none());
        assert!(primary.lookup(&make_key(3)).is_none());
    }

    #[test]
    fn unregister_batch_with_missing_keys() {
        let (_dir, mut primary) = open_temp();
        primary.register(make_key(1), make_entry(100)).unwrap();
        primary.register(make_key(3), make_entry(300)).unwrap();

        let keys = vec![make_key(1), make_key(2), make_key(3), make_key(4)];
        let results = primary.unregister_batch(&keys);

        assert_eq!(results.len(), 4);
        assert!(results[0].is_some());
        assert!(results[1].is_none());
        assert!(results[2].is_some());
        assert!(results[3].is_none());
        assert_eq!(primary.len(), 0);
    }

    #[test]
    fn unregister_batch_empty() {
        let (_dir, mut primary) = open_temp();
        primary.register(make_key(1), make_entry(100)).unwrap();
        let results = primary.unregister_batch(&[]);
        assert!(results.is_empty());
        assert_eq!(primary.len(), 1);
    }

    #[test]
    fn unregister_batch_all_missing() {
        let (_dir, mut primary) = open_temp();
        primary.register(make_key(1), make_entry(100)).unwrap();

        let keys = vec![make_key(99), make_key(100)];
        let results = primary.unregister_batch(&keys);
        assert_eq!(results.len(), 2);
        assert!(results[0].is_none());
        assert!(results[1].is_none());
        assert_eq!(primary.len(), 1);
    }
}
