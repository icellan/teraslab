//! Preserve secondary index backend.
//!
//! Unlike [`DahBackend`](crate::index::secondary_backend::DahBackend) and
//! [`UnminedBackend`](crate::index::secondary_backend::UnminedBackend), the
//! preserve index has ONLY an in-memory variant: it is never journaled to the
//! redo log and never persisted via redb. A crash re-derives it from each
//! record's authoritative on-device `preserve_until` (see
//! [`crate::ops::engine::Engine::rebuild_preserve_index_from_device`]), the
//! same way the conflicting index is rebuilt from its cached flag.
//!
//! The enum carries a single `InMemory` arm rather than being a bare struct so
//! that a future on-disk arm (should a very large preserve set ever warrant a
//! durable carrier) is a purely additive change and the engine/recovery call
//! sites stay variant-agnostic. The `insert`/`remove` signatures intentionally
//! mirror [`DahBackend`](crate::index::secondary_backend::DahBackend) —
//! including the `Option<&Mutex<RedoLog>>` redo argument, which is **ignored**
//! here — so the two-phase-durability call convention is identical across all
//! three secondary backends.

use crate::index::IndexError;
use crate::index::hashtable::TxKey;
use crate::index::preserve_index::PreserveIndex;
use crate::redo::RedoLog;
use parking_lot::Mutex;

/// Iterator over all `(height, TxKey)` pairs from a preserve backend.
pub enum PreserveIter<'a> {
    /// In-memory index iterator (opaque `impl Iterator`).
    InMemory(Box<dyn Iterator<Item = (u32, TxKey)> + 'a>),
}

impl Iterator for PreserveIter<'_> {
    type Item = (u32, TxKey);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::InMemory(it) => it.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::InMemory(it) => it.size_hint(),
        }
    }
}

/// Preserve (preserve-until) secondary index backend.
///
/// In-memory only (see module docs). Modeled as an enum for parity with
/// [`DahBackend`](crate::index::secondary_backend::DahBackend) /
/// [`UnminedBackend`](crate::index::secondary_backend::UnminedBackend).
pub enum PreserveBackend {
    /// In-memory BTreeMap + HashMap (the only variant).
    InMemory(PreserveIndex),
}

impl std::fmt::Debug for PreserveBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InMemory(_) => f.write_str("PreserveBackend::InMemory"),
        }
    }
}

impl PreserveBackend {
    /// Create a new empty in-memory preserve backend.
    pub fn new_in_memory() -> Self {
        Self::InMemory(PreserveIndex::new())
    }

    /// Insert a transaction into the preserve index.
    ///
    /// The `redo_log` argument is accepted for signature parity with
    /// [`DahBackend::insert`](crate::index::secondary_backend::DahBackend::insert)
    /// but is **ignored**: the preserve index is not journaled to the redo log
    /// (see module docs). The in-memory update is infallible, so this never
    /// returns `Err`; the `Result` is kept for call-site symmetry with the
    /// other secondary backends.
    pub fn insert(
        &mut self,
        height: u32,
        key: TxKey,
        _redo_log: Option<&Mutex<RedoLog>>,
    ) -> Result<(), IndexError> {
        match self {
            Self::InMemory(idx) => {
                idx.insert(height, key);
                Ok(())
            }
        }
    }

    /// Remove a transaction from the preserve index.
    ///
    /// See [`Self::insert`] for the ignored-`redo_log` contract.
    pub fn remove(
        &mut self,
        key: &TxKey,
        _redo_log: Option<&Mutex<RedoLog>>,
    ) -> Result<(), IndexError> {
        match self {
            Self::InMemory(idx) => {
                idx.remove(key);
                Ok(())
            }
        }
    }

    /// Return all txids whose `preserve_until` is in `[1, current_height]`.
    pub fn range_query(&self, current_height: u32) -> Vec<TxKey> {
        match self {
            Self::InMemory(idx) => idx.range_query(current_height),
        }
    }

    /// Number of entries in the index.
    pub fn len(&self) -> usize {
        match self {
            Self::InMemory(idx) => idx.len(),
        }
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::InMemory(idx) => idx.is_empty(),
        }
    }

    /// Remove all entries.
    ///
    /// Infallible for the in-memory variant; returns `Result` for parity with
    /// the other secondary backends' `clear`.
    pub fn clear(&mut self) -> Result<(), IndexError> {
        match self {
            Self::InMemory(idx) => {
                idx.clear();
                Ok(())
            }
        }
    }

    /// Iterate over all `(height, key)` pairs (for snapshot/export).
    pub fn iter(&self) -> PreserveIter<'_> {
        match self {
            Self::InMemory(idx) => PreserveIter::InMemory(Box::new(idx.iter())),
        }
    }

    /// Force all backend state durable. No-op for the in-memory variant; kept
    /// for parity with [`DahBackend::flush_durable`](crate::index::secondary_backend::DahBackend::flush_durable)
    /// so the checkpoint contract is uniform across the three secondaries.
    pub fn flush_durable(&self) -> Result<(), IndexError> {
        match self {
            Self::InMemory(_) => Ok(()),
        }
    }
}

impl Default for PreserveBackend {
    fn default() -> Self {
        Self::new_in_memory()
    }
}

impl From<PreserveIndex> for PreserveBackend {
    fn from(idx: PreserveIndex) -> Self {
        Self::InMemory(idx)
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
    fn insert_range_remove_clear_iter_in_memory() {
        let mut backend = PreserveBackend::new_in_memory();
        backend.insert(100, key(1), None).unwrap();
        backend.insert(200, key(2), None).unwrap();
        backend.insert(300, key(3), None).unwrap();

        assert_eq!(backend.len(), 3);
        assert!(!backend.is_empty());

        let result = backend.range_query(200);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&key(1)));
        assert!(result.contains(&key(2)));

        backend.remove(&key(1), None).unwrap();
        assert_eq!(backend.len(), 2);
        assert!(!backend.range_query(200).contains(&key(1)));

        let entries: Vec<_> = backend.iter().collect();
        assert_eq!(entries.len(), 2);

        backend.clear().unwrap();
        assert!(backend.is_empty());
        assert!(backend.range_query(u32::MAX).is_empty());
    }

    #[test]
    fn insert_update_replaces_height() {
        let mut backend = PreserveBackend::new_in_memory();
        backend.insert(100, key(1), None).unwrap();
        backend.insert(200, key(1), None).unwrap();
        assert_eq!(backend.len(), 1);
        assert!(backend.range_query(100).is_empty());
        assert_eq!(backend.range_query(200).len(), 1);
    }

    /// Pin the no-journal contract: insert/remove with `redo_log == None`
    /// return `Ok` and mutate the in-memory index. Mirrors
    /// `unmined_in_memory_insert_no_redo_dependency` in `secondary_backend.rs`.
    #[test]
    fn preserve_in_memory_insert_no_redo_dependency() {
        let mut backend = PreserveBackend::new_in_memory();
        backend
            .insert(100, key(1), None)
            .expect("in-memory insert must not need a redo log");
        backend
            .insert(200, key(2), None)
            .expect("in-memory insert must not need a redo log");
        assert_eq!(backend.len(), 2);

        backend
            .remove(&key(1), None)
            .expect("in-memory remove must not need a redo log");
        assert_eq!(backend.len(), 1);
        let kept = backend.range_query(300);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0], key(2));
    }

    #[test]
    fn flush_durable_is_noop_and_backend_stays_usable() {
        let mut backend = PreserveBackend::new_in_memory();
        backend.insert(100, key(1), None).unwrap();
        backend
            .flush_durable()
            .expect("preserve flush must succeed");
        assert_eq!(backend.len(), 1);
        backend.insert(200, key(2), None).unwrap();
        assert_eq!(backend.len(), 2, "backend must stay writable after flush");
    }

    #[test]
    fn default_is_in_memory_and_debug_format() {
        let backend = PreserveBackend::default();
        assert!(backend.is_empty());
        assert!(format!("{backend:?}").contains("InMemory"));
    }

    #[test]
    fn from_preserve_index() {
        let mut idx = PreserveIndex::new();
        idx.insert(100, key(1));
        let backend: PreserveBackend = idx.into();
        assert_eq!(backend.len(), 1);
    }
}
