//! Secondary index backend abstractions.
//!
//! Two-phase durability (see C4): every mutating [`insert`](DahBackend::insert) /
//! [`remove`](DahBackend::remove) on an on-disk backend appends and fsyncs a
//! redo intent record BEFORE committing the redb transaction. In-memory
//! backends have no on-disk state to protect and accept a `None` redo log —
//! a crash rebuilds them from the primary redo log replay plus device scan.

use crate::index::IndexError;
use crate::index::dah_index::{DahIndex, DahRedoEntry};
use crate::index::hashtable::TxKey;
use crate::index::redb_dah::{RedbDahIndex, RedbDahIter};
use crate::redo::RedoLog;
use parking_lot::Mutex;

// ---------------------------------------------------------------------------
// Enum iterators (concrete dispatch, matching PrimaryIter pattern)
// ---------------------------------------------------------------------------

/// Iterator over all `(height, TxKey)` pairs from a DAH backend.
pub enum DahIter<'a> {
    /// In-memory index iterator (opaque `impl Iterator`).
    InMemory(Box<dyn Iterator<Item = (u32, TxKey)> + 'a>),
    /// Bounded-batch redb iterator.
    Redb(RedbDahIter<'a>),
}

impl Iterator for DahIter<'_> {
    type Item = (u32, TxKey);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::InMemory(it) => it.next(),
            Self::Redb(it) => it.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::InMemory(it) => it.size_hint(),
            Self::Redb(it) => it.size_hint(),
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

    /// Insert a transaction into the DAH index with two-phase durability.
    ///
    /// For on-disk backends, the redo log (if provided) receives a
    /// [`crate::redo::RedoOp::SecondaryDahUpdate`] entry that is fsynced BEFORE the redb
    /// commit. For in-memory backends, the redo log is ignored — a crash
    /// rebuilds in-memory state from the primary redo replay + device scan.
    ///
    /// Returns an error only if the redo log flush or the redb commit fails;
    /// in-memory updates are infallible.
    pub fn insert(
        &mut self,
        height: u32,
        key: TxKey,
        redo_log: Option<&Mutex<RedoLog>>,
    ) -> Result<(), IndexError> {
        match self {
            Self::InMemory(idx) => {
                idx.insert(height, key);
                Ok(())
            }
            Self::OnDisk(redb) => redb.insert(height, key, redo_log),
        }
    }

    /// Remove a transaction from the DAH index with two-phase durability.
    pub fn remove(
        &mut self,
        key: &TxKey,
        redo_log: Option<&Mutex<RedoLog>>,
    ) -> Result<(), IndexError> {
        match self {
            Self::InMemory(idx) => {
                idx.remove(key);
                Ok(())
            }
            Self::OnDisk(redb) => redb.remove(key, redo_log),
        }
    }

    /// Replay a DAH redo entry idempotently.
    pub fn replay_redo(&mut self, entry: &DahRedoEntry) -> Result<(), IndexError> {
        match self {
            Self::InMemory(idx) => {
                idx.replay_redo(entry);
                Ok(())
            }
            Self::OnDisk(redb) => redb.replay_redo(entry),
        }
    }

    /// Return all txids with delete_at_height in `[0, current_height]`.
    pub fn range_query(&self, current_height: u32) -> Vec<TxKey> {
        match self {
            Self::InMemory(idx) => idx.range_query(current_height),
            Self::OnDisk(redb) => redb.range_query(current_height),
        }
    }

    /// Return at most `limit` txids with delete_at_height in
    /// `[0, current_height]`, lowest-height first. Bounds the DAH sweep's
    /// per-call work (issue #25 follow-up — the unbounded query + delete batch
    /// failed every pruner call once the due-set exceeded `max_batch`).
    pub fn range_query_limited(&self, current_height: u32, limit: usize) -> Vec<TxKey> {
        match self {
            Self::InMemory(idx) => idx.range_query_limited(current_height, limit),
            Self::OnDisk(redb) => redb.range_query_limited(current_height, limit),
        }
    }

    /// The current `delete_at_height` recorded for `key`, or `None` if absent.
    ///
    /// The authoritative live due-height for a record (Task 16d): the DAH sweep
    /// gate reads it here rather than trusting the stale on-device
    /// `delete_at_height` field.
    pub fn get_height(&self, key: &TxKey) -> Option<u32> {
        match self {
            Self::InMemory(idx) => idx.get_height(key),
            Self::OnDisk(redb) => redb.get_height(key),
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
    ///
    /// Returns an [`IndexError`] if the redb backend fails to commit the
    /// drop+recreate transaction. The in-memory variant is infallible.
    pub fn clear(&mut self) -> Result<(), IndexError> {
        match self {
            Self::InMemory(idx) => {
                idx.clear();
                Ok(())
            }
            Self::OnDisk(redb) => redb.clear(),
        }
    }

    /// Iterate over all `(height, key)` pairs (for snapshot/export).
    pub fn iter(&self) -> DahIter<'_> {
        match self {
            Self::InMemory(idx) => DahIter::InMemory(Box::new(idx.iter())),
            Self::OnDisk(redb) => DahIter::Redb(redb.iter_streaming()),
        }
    }

    /// Force all backend state durable (G-1 audit fix). No-op for the
    /// in-memory variant; fsyncs all prior `Durability::Eventual` commits
    /// for the redb variant. See
    /// [`crate::index::PrimaryBackend::flush_durable`] for the checkpoint
    /// contract.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if the redb flush transaction fails.
    pub fn flush_durable(&self) -> Result<(), IndexError> {
        match self {
            Self::InMemory(_) => Ok(()),
            Self::OnDisk(redb) => redb.flush_durable(),
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::redb_dah::RedbDahIndex;

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
        let redb =
            RedbDahIndex::open(dir.path().join("dah.redb").as_path(), 16 * 1024 * 1024).unwrap();
        let mut disk = DahBackend::OnDisk(redb);
        f(&mut disk);
    }

    // -----------------------------------------------------------------------
    // DahBackend: parameterized tests
    // -----------------------------------------------------------------------

    /// G-1: `flush_durable` must succeed on every backend variant (no-op
    /// in-memory, `Durability::Immediate` empty commit for redb) and leave
    /// the backend fully usable for subsequent reads and writes.
    #[test]
    fn flush_durable_succeeds_and_backend_stays_usable() {
        with_both_dah_backends(|backend| {
            backend.insert(100, key(1), None).unwrap();
            backend.flush_durable().expect("DAH flush must succeed");
            assert_eq!(backend.len(), 1);
            assert_eq!(backend.range_query(100), vec![key(1)]);
            backend.insert(200, key(2), None).unwrap();
            assert_eq!(backend.len(), 2, "backend must stay writable after flush");
        });
    }

    #[test]
    fn dah_both_insert_and_range_query() {
        with_both_dah_backends(|backend| {
            backend.insert(100, key(1), None).unwrap();
            backend.insert(200, key(2), None).unwrap();
            backend.insert(300, key(3), None).unwrap();

            assert_eq!(backend.len(), 3);
            assert!(!backend.is_empty());

            let result = backend.range_query(200);
            assert_eq!(result.len(), 2);
            assert!(result.contains(&key(1)));
            assert!(result.contains(&key(2)));

            let result = backend.range_query(300);
            assert_eq!(result.len(), 3);

            let result = backend.range_query(99);
            assert!(result.is_empty());
        });
    }

    #[test]
    fn dah_both_insert_updates_height() {
        with_both_dah_backends(|backend| {
            backend.insert(100, key(1), None).unwrap();
            backend.insert(200, key(1), None).unwrap();
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
            backend.insert(100, key(1), None).unwrap();
            backend.insert(200, key(2), None).unwrap();
            backend.remove(&key(1), None).unwrap();

            assert_eq!(backend.len(), 1);
            let result = backend.range_query(300);
            assert_eq!(result.len(), 1);
            assert_eq!(result[0], key(2));

            backend.remove(&key(99), None).unwrap();
            assert_eq!(backend.len(), 1);
        });
    }

    #[test]
    fn dah_both_clear() {
        with_both_dah_backends(|backend| {
            backend.insert(100, key(1), None).unwrap();
            backend.insert(200, key(2), None).unwrap();
            backend.clear().unwrap();
            assert!(backend.is_empty());
            assert!(backend.range_query(1000).is_empty());
        });
    }

    #[test]
    fn dah_both_iter() {
        with_both_dah_backends(|backend| {
            backend.insert(100, key(1), None).unwrap();
            backend.insert(200, key(2), None).unwrap();
            backend.insert(300, key(3), None).unwrap();

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
        let redb =
            RedbDahIndex::open(dir.path().join("dah.redb").as_path(), 16 * 1024 * 1024).unwrap();
        let disk = DahBackend::OnDisk(redb);
        assert!(format!("{disk:?}").contains("OnDisk"));
    }

    #[test]
    fn dah_iter_size_hint() {
        // On-disk streaming iterators only report the currently buffered batch.
        let dir = tempfile::tempdir().unwrap();
        let mut redb =
            RedbDahIndex::open(dir.path().join("dah.redb").as_path(), 16 * 1024 * 1024).unwrap();
        redb.insert(100, key(1), None).unwrap();
        redb.insert(200, key(2), None).unwrap();
        redb.insert(300, key(3), None).unwrap();
        let disk = DahBackend::OnDisk(redb);
        let mut iter = disk.iter();
        assert_eq!(iter.size_hint(), (0, None));
        assert_eq!(iter.by_ref().count(), 3);
        assert_eq!(iter.size_hint(), (0, None));
    }
}
