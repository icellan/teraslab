//! Sharded primary index.
//!
//! Splits the key space across `N` independent [`PrimaryBackend`] instances,
//! each guarded by its own [`parking_lot::RwLock`]. This eliminates the single
//! global write lock that previously serialised all index mutations and allows
//! concurrent reads and writes on different shards.
//!
//! # Shard selection
//!
//! The shard is derived from bytes `[24..32]` of the txid via a SplitMix64
//! finaliser XOR-ed with a per-process random seed. The byte range is disjoint
//! from:
//! - bucket hash in [`crate::index::hashtable`] which uses `[0..8]`, and
//! - stripe hash in [`crate::locks::StripedLocks`] which uses `[16..24]`.
//!
//! # Construction
//!
//! ```rust,ignore
//! let idx = ShardedIndex::new_in_memory(1_000_000, 16)?;
//! ```
//!
//! The `shard_count` is rounded up to the next power of two and clamped to
//! `[1, 256]`.

use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::cluster::shards::ShardTable;
use crate::index::backend::PrimaryBackend;
use crate::index::redb_primary::CachedFieldsUpdate;
use crate::index::{IndexError, IndexStats, TxIndexEntry, TxKey};
use crate::record::TxFlags;

// ---------------------------------------------------------------------------
// Process-local seed
// ---------------------------------------------------------------------------

/// Returns the process-local random seed used for shard selection.
///
/// Initialised once from `getrandom`; falls back to `RandomState` if the
/// syscall is unavailable (e.g. restricted sandboxes).
fn index_shard_seed() -> u64 {
    static SEED: OnceLock<u64> = OnceLock::new();
    *SEED.get_or_init(|| {
        let mut buf = [0u8; 8];
        if getrandom::getrandom(&mut buf).is_ok() {
            return u64::from_le_bytes(buf);
        }
        use std::collections::hash_map::RandomState;
        use std::hash::{BuildHasher, Hasher};
        let mut h = RandomState::new().build_hasher();
        h.write_u64(0x9e37_79b9_7f4a_7c15);
        h.finish()
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Round `n` up to the nearest power of two, clamped to `[1, 256]`.
fn clamp_shard_count(n: usize) -> usize {
    let n = n.clamp(1, 256);
    n.next_power_of_two().min(256)
}

// ---------------------------------------------------------------------------
// ShardedIndex
// ---------------------------------------------------------------------------

/// A sharded primary index.
///
/// Spreads the key space across `shard_count` independent [`PrimaryBackend`]
/// instances, each behind its own [`parking_lot::RwLock`]. Operations on
/// different shards proceed concurrently without contention.
///
/// # Thread safety
///
/// `ShardedIndex` is `Send + Sync`. All mutation methods take `&self` (shared
/// reference) and acquire the appropriate shard lock internally, so callers can
/// share a single `Arc<ShardedIndex>` across threads.
///
/// # Shard count
///
/// `shard_count` is rounded up to the next power of two and clamped to `[1, 256]`
/// by [`clamp_shard_count`]. A power-of-two count allows the shard selection to
/// use a bitmask instead of a modulo.
pub struct ShardedIndex {
    shards: Vec<RwLock<PrimaryBackend>>,
    shard_mask: usize,
    seed: u64,
}

impl ShardedIndex {
    /// Create a new in-memory sharded index.
    ///
    /// `expected_records` is the total expected record count across all shards;
    /// each shard is initialised for `expected_records / shard_count` entries.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError`] if any shard's underlying hash table allocation
    /// fails.
    pub fn new_in_memory(expected_records: usize, shard_count: usize) -> Result<Self, IndexError> {
        let count = clamp_shard_count(shard_count);
        let per_shard = expected_records.div_ceil(count).max(1);
        let seed = index_shard_seed();
        let mut shards = Vec::with_capacity(count);
        for _ in 0..count {
            shards.push(RwLock::new(PrimaryBackend::new_in_memory(per_shard)?));
        }
        Ok(Self {
            shards,
            shard_mask: count - 1,
            seed,
        })
    }

    /// Wrap an existing [`PrimaryBackend`] as a single-shard `ShardedIndex`.
    ///
    /// This is the transparent pass-through used by the engine migration: a
    /// `ShardedIndex` with `shard_count == 1` and `shard_mask == 0` routes every
    /// key to the one shard, so its behaviour is byte-for-byte identical to the
    /// wrapped backend behind a plain `RwLock`. A fresh process-local seed is
    /// installed; at one shard the seed never affects routing (the mask is `0`),
    /// so its only role is to keep the type uniform with the multi-shard
    /// constructors.
    ///
    /// Used to migrate `Engine.index` to `ShardedIndex` without changing the
    /// `PrimaryBackend` semantics or the recovery/snapshot on-disk formats —
    /// the recovered or rebuilt backend is wrapped here at engine construction.
    pub fn from_single(backend: PrimaryBackend) -> Self {
        Self {
            shards: vec![RwLock::new(backend)],
            shard_mask: 0,
            seed: index_shard_seed(),
        }
    }

    /// Number of shards in this index.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Compute which shard a key belongs to.
    ///
    /// Uses bytes `[24..32]` of the txid (disjoint from bucket `[0..8]` and
    /// stripe `[16..24]`) XOR-mixed with the per-process seed through a
    /// SplitMix64 finaliser.
    pub fn index_shard_for_key(&self, key: &TxKey) -> usize {
        // The `try_into` on a statically-32-byte array with a fixed slice range
        // cannot fail; `unwrap_or` maps the impossible error to 0 (panic-free
        // library code per project rules, mirroring `locks.rs`).
        let raw = u64::from_le_bytes(key.txid[24..32].try_into().unwrap_or([0u8; 8]));
        let mut x = raw ^ self.seed;
        x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
        x ^= x >> 31;
        (x as usize) & self.shard_mask
    }

    /// Acquire a read lock on the shard that owns `key`.
    pub fn read_shard(&self, key: &TxKey) -> RwLockReadGuard<'_, PrimaryBackend> {
        self.shards[self.index_shard_for_key(key)].read()
    }

    /// Acquire a write lock on the shard that owns `key`.
    pub fn write_shard(&self, key: &TxKey) -> RwLockWriteGuard<'_, PrimaryBackend> {
        self.shards[self.index_shard_for_key(key)].write()
    }

    // -----------------------------------------------------------------------
    // Point operations — single-key read / write
    // -----------------------------------------------------------------------

    /// Look up where a transaction's record lives on disk.
    ///
    /// Returns the entry if present, `None` if the key is absent. Acquires a
    /// shared read lock on the owning shard only.
    pub fn lookup(&self, key: &TxKey) -> Option<TxIndexEntry> {
        self.read_shard(key).lookup(key)
    }

    /// Fallible variant of [`Self::lookup`] that propagates redb read errors.
    ///
    /// Returns `Ok(Some(entry))` if the key is present, `Ok(None)` if absent,
    /// and an [`IndexError`] if the redb backend's read transaction fails.
    pub fn lookup_checked(&self, key: &TxKey) -> Result<Option<TxIndexEntry>, IndexError> {
        self.read_shard(key).lookup_checked(key)
    }

    /// Register a newly created transaction record in the index.
    ///
    /// Acquires an exclusive write lock on the owning shard.
    ///
    /// # Errors
    ///
    /// Propagates [`IndexError`] from the underlying backend (hash-table
    /// allocation failure or redb commit failure).
    pub fn register(&self, key: TxKey, entry: TxIndexEntry) -> Result<(), IndexError> {
        self.write_shard(&key).register(key, entry)
    }

    /// Register or update an entry without performing the mmap hash-table
    /// resize inline.
    ///
    /// See [`PrimaryBackend::register_without_resize`] for the contract.
    /// Acquires an exclusive write lock on the owning shard.
    pub fn register_without_resize(
        &self,
        key: TxKey,
        entry: TxIndexEntry,
    ) -> Result<(), IndexError> {
        self.write_shard(&key).register_without_resize(key, entry)
    }

    /// Remove a transaction from the index.
    ///
    /// Returns the removed entry, or `None` if the key was not present.
    /// Acquires an exclusive write lock on the owning shard.
    pub fn unregister(&self, key: &TxKey) -> Option<TxIndexEntry> {
        self.write_shard(key).unregister(key)
    }

    /// Fallible variant of [`Self::unregister`] that propagates redb write errors.
    ///
    /// Returns `Ok(Some(entry))` if the key was found and removed, `Ok(None)`
    /// if absent, and an [`IndexError`] if the redb backend's write transaction
    /// fails.
    pub fn unregister_checked(&self, key: &TxKey) -> Result<Option<TxIndexEntry>, IndexError> {
        self.write_shard(key).unregister_checked(key)
    }

    /// Update the cached fields in the index entry for `key`.
    ///
    /// Returns `Ok(true)` if the key was found and updated, `Ok(false)` if
    /// absent. Acquires an exclusive write lock on the owning shard.
    ///
    /// # Errors
    ///
    /// Propagates [`IndexError`] from the redb backend on commit failure.
    #[allow(clippy::too_many_arguments)]
    pub fn update_cached_fields(
        &self,
        key: &TxKey,
        tx_flags: u8,
        block_entry_count: u8,
        spent_utxos: u32,
        dah_or_preserve: u32,
        unmined_since: u32,
        generation: u32,
    ) -> Result<bool, IndexError> {
        self.write_shard(key).update_cached_fields(
            key,
            tx_flags,
            block_entry_count,
            spent_utxos,
            dah_or_preserve,
            unmined_since,
            generation,
        )
    }

    // -----------------------------------------------------------------------
    // Batch operations — group by shard, one lock acquisition per shard
    // -----------------------------------------------------------------------

    /// Register multiple `(key, entry)` pairs.
    ///
    /// Entries are grouped by shard so each shard lock is acquired at most
    /// once. Ordering within a shard is preserved.
    ///
    /// # Errors
    ///
    /// Returns on the first [`IndexError`] encountered (a partial insert may
    /// have occurred).
    pub fn register_batch(&self, entries: &[(TxKey, TxIndexEntry)]) -> Result<(), IndexError> {
        let shard_count = self.shards.len();
        let mut by_shard: Vec<Vec<(TxKey, TxIndexEntry)>> = vec![Vec::new(); shard_count];
        for &(key, entry) in entries {
            by_shard[self.index_shard_for_key(&key)].push((key, entry));
        }
        for (i, items) in by_shard.iter().enumerate() {
            if items.is_empty() {
                continue;
            }
            let mut guard = self.shards[i].write();
            for &(key, entry) in items {
                guard.register(key, entry)?;
            }
        }
        Ok(())
    }

    /// Remove multiple keys, returning results in the same order as the input.
    ///
    /// Keys are grouped by shard so each shard lock is acquired at most once.
    /// The returned `Vec` is parallel to `keys`: `Some(entry)` for keys that
    /// were present, `None` for missing keys.
    ///
    /// # Errors
    ///
    /// Propagates [`IndexError`] from the redb backend on commit failure.
    pub fn unregister_batch(
        &self,
        keys: &[TxKey],
    ) -> Result<Vec<Option<TxIndexEntry>>, IndexError> {
        let shard_count = self.shards.len();
        let mut results: Vec<Option<TxIndexEntry>> = vec![None; keys.len()];

        // Group by shard, preserving original indices so output order is stable.
        let mut by_shard: Vec<Vec<(usize, TxKey)>> = vec![Vec::new(); shard_count];
        for (i, key) in keys.iter().enumerate() {
            by_shard[self.index_shard_for_key(key)].push((i, *key));
        }

        for (shard_idx, items) in by_shard.iter().enumerate() {
            if items.is_empty() {
                continue;
            }
            // Collect just the keys for this shard into a contiguous slice so
            // we can call `unregister_batch` once per shard (mirrors how
            // `update_cached_fields_batch` delegates in a single call per
            // shard).
            let shard_keys: Vec<TxKey> = items.iter().map(|&(_, k)| k).collect();
            let mut guard = self.shards[shard_idx].write();
            let shard_results = guard.unregister_batch(&shard_keys)?;
            for (&(orig_idx, _), removed) in items.iter().zip(shard_results) {
                results[orig_idx] = removed;
            }
        }
        Ok(results)
    }

    /// Update cached fields for multiple entries.
    ///
    /// Updates are grouped by shard so each shard lock is acquired at most
    /// once. Returns the total number of entries that were found and updated.
    ///
    /// # Errors
    ///
    /// Propagates [`IndexError`] from the redb backend on commit failure.
    pub fn update_cached_fields_batch(
        &self,
        updates: &[CachedFieldsUpdate],
    ) -> Result<usize, IndexError> {
        let shard_count = self.shards.len();
        // Group update indices by shard.
        let mut by_shard: Vec<Vec<usize>> = vec![Vec::new(); shard_count];
        for (i, u) in updates.iter().enumerate() {
            by_shard[self.index_shard_for_key(&u.key)].push(i);
        }

        let mut total = 0usize;
        for (shard_idx, indices) in by_shard.iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let shard_updates: Vec<CachedFieldsUpdate> =
                indices.iter().map(|&i| updates[i].clone()).collect();
            let mut guard = self.shards[shard_idx].write();
            total += guard.update_cached_fields_batch(&shard_updates)?;
        }
        Ok(total)
    }

    // -----------------------------------------------------------------------
    // Whole-table read fan-out — acquires each shard's read lock in turn
    // -----------------------------------------------------------------------

    /// Total number of entries across all shards.
    ///
    /// Acquires each shard's read lock in sequence and sums the counts.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }

    /// Whether the index contains no entries.
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|s| s.read().is_empty())
    }

    /// Merged statistics across all shards.
    ///
    /// Aggregates per-shard [`IndexStats`]:
    /// - `entry_count`, `capacity`, and `memory_bytes` are summed.
    /// - `load_factor` is recomputed as `total_entry_count / total_capacity`
    ///   (avoiding the arithmetic-mean distortion that arises if shards have
    ///   different capacities).
    /// - `max_probe_distance` is the maximum observed across all shards.
    /// - `hugepage_enabled` is `true` when ALL shards have hugepages (any
    ///   shard without hugepages means the full table is not backed).
    pub fn stats(&self) -> IndexStats {
        let mut total_entries = 0usize;
        let mut total_capacity = 0usize;
        let mut total_memory = 0usize;
        let mut max_probe = 0usize;
        let mut all_huge = true;

        for shard in &self.shards {
            let s = shard.read().stats();
            total_entries += s.entry_count;
            total_capacity += s.capacity;
            total_memory += s.memory_bytes;
            if s.max_probe_distance > max_probe {
                max_probe = s.max_probe_distance;
            }
            if !s.hugepage_enabled {
                all_huge = false;
            }
        }

        let load_factor = if total_capacity == 0 {
            0.0
        } else {
            total_entries as f64 / total_capacity as f64
        };

        IndexStats {
            entry_count: total_entries,
            capacity: total_capacity,
            load_factor,
            hugepage_enabled: all_huge,
            max_probe_distance: max_probe,
            memory_bytes: total_memory,
        }
    }

    /// Non-blocking merged statistics.
    ///
    /// Attempts a `try_read` on every shard. If ANY shard's read lock is
    /// momentarily held by a writer, returns `None` immediately rather than
    /// blocking — the observability/admin path (`/admin/top`) must never stall
    /// behind the write path. When all shards are readable, returns the same
    /// merged [`IndexStats`] as [`Self::stats`].
    pub fn try_stats(&self) -> Option<IndexStats> {
        let mut total_entries = 0usize;
        let mut total_capacity = 0usize;
        let mut total_memory = 0usize;
        let mut max_probe = 0usize;
        let mut all_huge = true;

        for shard in &self.shards {
            let guard = shard.try_read()?;
            let s = guard.stats();
            total_entries += s.entry_count;
            total_capacity += s.capacity;
            total_memory += s.memory_bytes;
            if s.max_probe_distance > max_probe {
                max_probe = s.max_probe_distance;
            }
            if !s.hugepage_enabled {
                all_huge = false;
            }
        }

        let load_factor = if total_capacity == 0 {
            0.0
        } else {
            total_entries as f64 / total_capacity as f64
        };

        Some(IndexStats {
            entry_count: total_entries,
            capacity: total_capacity,
            load_factor,
            hugepage_enabled: all_huge,
            max_probe_distance: max_probe,
            memory_bytes: total_memory,
        })
    }

    /// The active backend name (from the first shard; all shards share the
    /// same backend type).
    pub fn backend_name(&self) -> &'static str {
        self.shards[0].read().backend_name()
    }

    /// Invoke `f` for every `(key, entry)` pair across all shards.
    ///
    /// Each shard's read lock is acquired, iterated, and released before
    /// moving to the next shard. This avoids the self-referential
    /// guard+iterator lifetime problem while still being allocation-free
    /// on the caller's side.
    ///
    /// The callback receives `TxKey` by value and `&TxIndexEntry` by
    /// reference. Entry order across shards is unspecified.
    pub fn for_each(&self, mut f: impl FnMut(TxKey, &TxIndexEntry)) {
        for shard in &self.shards {
            let guard = shard.read();
            for (key, entry) in guard.iter() {
                f(key, &entry);
            }
        }
    }

    /// Collect all registered keys into a `Vec`.
    ///
    /// Equivalent to calling `for_each` and collecting only the keys.
    pub fn all_keys(&self) -> Vec<TxKey> {
        let mut keys = Vec::new();
        self.for_each(|k, _| keys.push(k));
        keys
    }

    /// Scan for records whose preservation window has expired.
    ///
    /// Returns the keys of entries whose `HAS_PRESERVE_UNTIL` flag is set
    /// and whose `dah_or_preserve` value is non-zero and `<= current_height`.
    /// Mirrors `Engine::scan_expired_preservations` — filters the primary
    /// index; never touches the device.
    pub fn scan_expired_preservations(&self, current_height: u32) -> Vec<TxKey> {
        let mut keys = Vec::new();
        self.for_each(|k, e| {
            let has_preserve =
                TxFlags::from_bits_truncate(e.tx_flags).contains(TxFlags::HAS_PRESERVE_UNTIL);
            if has_preserve && e.dah_or_preserve != 0 && e.dah_or_preserve <= current_height {
                keys.push(k);
            }
        });
        keys
    }

    /// Return keys belonging to a specific cluster shard.
    ///
    /// Iterates all index shards and filters entries by
    /// [`ShardTable::shard_for_key`]. Note: the "cluster shard" (`shard: u16`)
    /// is the data-placement concept; it is DISTINCT from the in-process index
    /// shard used for lock striping.
    pub fn keys_for_shard(&self, shard: u16) -> Vec<TxKey> {
        let mut keys = Vec::new();
        self.for_each(|k, _| {
            if ShardTable::shard_for_key(&k) == shard {
                keys.push(k);
            }
        });
        keys
    }

    /// Group all keys by cluster shard in a single pass.
    ///
    /// Returns a `HashMap` from cluster-shard number to the list of keys
    /// that belong to that shard. O(N) in total index entries.
    pub fn keys_by_shard(&self) -> HashMap<u16, Vec<TxKey>> {
        let mut result: HashMap<u16, Vec<TxKey>> = HashMap::new();
        self.for_each(|k, _| {
            let shard = ShardTable::shard_for_key(&k);
            result.entry(shard).or_default().push(k);
        });
        result
    }

    /// Group keys by cluster shard, but only for shards in `shard_filter`.
    ///
    /// More memory-efficient than [`Self::keys_by_shard`] when only a subset
    /// of cluster shards is needed (e.g. only the outbound migration shards).
    /// Keys belonging to shards not in the filter are skipped entirely.
    pub fn keys_by_shard_filtered(&self, shard_filter: &HashSet<u16>) -> HashMap<u16, Vec<TxKey>> {
        let mut result: HashMap<u16, Vec<TxKey>> = HashMap::new();
        self.for_each(|k, _| {
            let shard = ShardTable::shard_for_key(&k);
            if shard_filter.contains(&shard) {
                result.entry(shard).or_default().push(k);
            }
        });
        result
    }

    /// Invoke `f` for every key whose `CONFLICTING` flag is set.
    ///
    /// Used to rebuild the in-memory conflicting index after recovery.
    /// Iterates the primary index; never touches the device.
    pub fn for_each_conflicting(&self, mut f: impl FnMut(TxKey)) {
        self.for_each(|k, e| {
            if TxFlags::from_bits_truncate(e.tx_flags).contains(TxFlags::CONFLICTING) {
                f(k);
            }
        });
    }

    // -----------------------------------------------------------------------
    // Write / flush
    // -----------------------------------------------------------------------

    /// Flush every shard durable.
    ///
    /// Iterates all shard locks (read is sufficient — `flush_durable` takes
    /// `&self` on `PrimaryBackend`) and flushes each. Returns on the first
    /// error encountered (subsequent shards are NOT flushed on error).
    ///
    /// # Errors
    ///
    /// Returns [`IndexError`] if any shard's backend flush fails.
    pub fn flush_durable(&self) -> Result<(), IndexError> {
        for shard in &self.shards {
            shard.read().flush_durable()?;
        }
        Ok(())
    }

    /// Snapshot the primary index and both secondary indexes to `path`.
    ///
    /// At a single shard this delegates straight to the wrapped backend's
    /// [`PrimaryBackend::snapshot_all`], producing a snapshot byte-for-byte
    /// identical to the pre-sharding engine. For more than one shard a v2
    /// N-shard manifest format is required (Task 4); until that lands a
    /// multi-shard snapshot would silently capture only one shard, so this
    /// fails closed instead.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError`] from the underlying backend's snapshot, or an
    /// [`IndexError::FormatError`] if called with more than one shard (the
    /// v2 multi-shard format is not yet implemented).
    pub fn snapshot_all(
        &self,
        dah: &crate::index::DahBackend,
        unmined: &crate::index::UnminedBackend,
        path: &std::path::Path,
    ) -> Result<(), IndexError> {
        if self.shards.len() != 1 {
            return Err(IndexError::FormatError {
                detail: format!(
                    "ShardedIndex::snapshot_all requires the v2 N-shard format \
                     for shard_count > 1 (have {}); not yet implemented",
                    self.shards.len()
                ),
            });
        }
        self.shards[0].read().snapshot_all(dah, unmined, path)
    }

    /// Test-only: arm a synthetic read failure on every shard's backend.
    ///
    /// Mirrors [`PrimaryBackend::arm_fail_next_read`] across all shards so the
    /// G-4 engine tests can force the next `lookup_checked` to return an
    /// [`IndexError`] regardless of which shard the probed key routes to.
    #[cfg(test)]
    pub fn arm_fail_next_read(&self) {
        for shard in &self.shards {
            shard.read().arm_fail_next_read();
        }
    }

    // -----------------------------------------------------------------------
    // Per-shard resize entry point
    // -----------------------------------------------------------------------

    /// Resize index shard `shard_idx` if it currently needs one.
    ///
    /// Checks [`PrimaryBackend::resize_target_capacity`] under the write lock.
    /// If no resize is needed, returns `Ok(())` immediately. Otherwise builds
    /// a resized copy under the write lock, marks the old backend defunct, and
    /// swaps it in — exactly mirroring the engine's
    /// `resize_primary_index_without_blocking_readers` logic but for a single
    /// shard.
    ///
    /// `shard_idx` must be in `[0, shard_count())`.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError`] if `shard_idx` is out of range or if the
    /// underlying resize fails.
    pub fn resize_shard_if_needed(&self, shard_idx: usize) -> Result<(), IndexError> {
        if shard_idx >= self.shards.len() {
            return Err(IndexError::FormatError {
                detail: format!(
                    "shard_idx {shard_idx} out of range (shard_count = {})",
                    self.shards.len()
                ),
            });
        }

        let mut guard = self.shards[shard_idx].write();
        let Some(target_capacity) = guard.resize_target_capacity() else {
            return Ok(());
        };

        let resized = guard.resized_copy(target_capacity)?;
        guard.mark_defunct_for_resize();
        *guard = resized;
        Ok(())
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
        txid[8..16].copy_from_slice(&n.wrapping_mul(0x9E3779B97F4A7C15).to_le_bytes());
        // Vary bytes [24..32] so keys spread across shards
        txid[24..32].copy_from_slice(&n.wrapping_mul(0x517C_C1B7_2722_0A95).to_le_bytes());
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

    // -----------------------------------------------------------------------
    // Test 0: from_single pass-through equivalence
    // -----------------------------------------------------------------------

    /// `from_single` wraps an existing backend as a 1-shard index that behaves
    /// as a transparent pass-through: shard_count is 1, every key routes to
    /// shard 0, and register/lookup/len match the wrapped backend exactly.
    #[test]
    fn from_single_is_transparent_passthrough() {
        // Seed a standalone backend, then wrap it.
        let mut backend = PrimaryBackend::new_in_memory(1000).unwrap();
        for i in 0..200u64 {
            backend.register(make_key(i), make_entry(i * 4)).unwrap();
        }
        let pre_len = backend.len();

        let sharded = ShardedIndex::from_single(backend);

        // Exactly one shard; mask routes everything to shard 0.
        assert_eq!(sharded.shard_count(), 1, "from_single must yield one shard");
        for i in 0..200u64 {
            assert_eq!(
                sharded.index_shard_for_key(&make_key(i)),
                0,
                "every key must route to shard 0 at shard_count = 1"
            );
        }

        // Pre-existing entries survived the wrap (len + lookup match).
        assert_eq!(sharded.len(), pre_len, "wrapped len must match backend len");
        for i in 0..200u64 {
            let entry = sharded
                .lookup(&make_key(i))
                .unwrap_or_else(|| panic!("wrapped key {i} missing"));
            assert_eq!(
                entry.record_offset,
                i * 4,
                "wrong offset for wrapped key {i}"
            );
        }

        // Further register/lookup/len behave like the single backend: build a
        // fresh oracle and replay the same additional ops.
        let mut oracle = PrimaryBackend::new_in_memory(1000).unwrap();
        for i in 0..200u64 {
            oracle.register(make_key(i), make_entry(i * 4)).unwrap();
        }
        for i in 200..400u64 {
            let key = make_key(i);
            let entry = make_entry(i * 4);
            sharded.register(key, entry).unwrap();
            oracle.register(key, entry).unwrap();
        }
        assert_eq!(sharded.len(), oracle.len(), "len must track the oracle");
        for i in 0..400u64 {
            assert_eq!(
                sharded.lookup(&make_key(i)),
                oracle.lookup(&make_key(i)),
                "lookup mismatch vs oracle for key {i}"
            );
        }
    }

    /// At one shard, `snapshot_all` produces a snapshot that restores to the
    /// same contents; the multi-shard case fails closed (v2 format is Task 4).
    #[test]
    fn from_single_snapshot_roundtrips_and_multishard_fails_closed() {
        use crate::index::{DahBackend, DahIndex, UnminedBackend, UnminedIndex};

        let dir = std::env::temp_dir().join(format!(
            "teraslab-sharded-snap-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("snapshot.bin");

        let sharded = ShardedIndex::from_single(PrimaryBackend::new_in_memory(1000).unwrap());
        for i in 0..150u64 {
            sharded.register(make_key(i), make_entry(i * 2)).unwrap();
        }
        let dah = DahBackend::InMemory(DahIndex::new());
        let unmined = UnminedBackend::InMemory(UnminedIndex::new());
        sharded.snapshot_all(&dah, &unmined, &path).unwrap();

        let (restored, _dah, _unmined, _flags) = PrimaryBackend::restore_all(&path).unwrap();
        assert_eq!(restored.len(), 150, "restored entry count must match");
        for i in 0..150u64 {
            let e = restored
                .lookup(&make_key(i))
                .unwrap_or_else(|| panic!("restored key {i} missing"));
            assert_eq!(e.record_offset, i * 2, "restored offset wrong for key {i}");
        }

        // Multi-shard snapshot must fail closed (Task 4 implements v2).
        let multishard = ShardedIndex::new_in_memory(1000, 4).unwrap();
        let err = multishard.snapshot_all(&dah, &unmined, &path);
        assert!(
            matches!(err, Err(IndexError::FormatError { .. })),
            "multi-shard snapshot must fail closed, got {err:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -----------------------------------------------------------------------
    // Test 1: routing_distribution
    // -----------------------------------------------------------------------

    /// Verify that the SplitMix64 shard hash distributes uniformly (±20% of
    /// mean) and that shard assignment is deterministic and only depends on
    /// bytes `[24..32]` of the txid.
    #[test]
    fn routing_distribution() {
        for &shard_count in &[1usize, 4, 16] {
            let idx = ShardedIndex::new_in_memory(1000, shard_count).unwrap();
            let actual_shards = idx.shard_count();
            let mut counts = vec![0usize; actual_shards];

            let n = 100_000usize;
            for i in 0..n {
                let mut txid = [0u8; 32];
                // Drive shard assignment via [24..32]
                txid[24..32].copy_from_slice(&(i as u64).to_le_bytes());
                // Also vary other bytes to exercise realistic inputs
                txid[0..8]
                    .copy_from_slice(&(i as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15).to_le_bytes());
                let key = TxKey { txid };
                counts[idx.index_shard_for_key(&key)] += 1;
            }

            let mean = n / actual_shards;
            let tolerance = mean / 5; // ±20%
            for (i, &c) in counts.iter().enumerate() {
                assert!(
                    c >= mean.saturating_sub(tolerance) && c <= mean + tolerance,
                    "shard {i} got {c} out of {n} (mean={mean}, tolerance=±{tolerance}) \
                     for shard_count={shard_count}",
                );
            }

            // Deterministic: same key → same shard
            let mut txid = [0u8; 32];
            txid[24..32].copy_from_slice(&42u64.to_le_bytes());
            let key = TxKey { txid };
            let s1 = idx.index_shard_for_key(&key);
            let s2 = idx.index_shard_for_key(&key);
            assert_eq!(s1, s2, "shard assignment must be deterministic");

            // Disjoint-byte property: changing [0..8] must not change shard
            let mut key2 = key;
            key2.txid[0..8].copy_from_slice(&0xdead_beef_cafe_babeu64.to_le_bytes());
            assert_eq!(
                idx.index_shard_for_key(&key),
                idx.index_shard_for_key(&key2),
                "changing txid[0..8] must not change shard assignment",
            );

            // Disjoint-byte property: changing [16..24] must not change shard
            let mut key3 = key;
            key3.txid[16..24].copy_from_slice(&0x1234_5678_90ab_cdefu64.to_le_bytes());
            assert_eq!(
                idx.index_shard_for_key(&key),
                idx.index_shard_for_key(&key3),
                "changing txid[16..24] must not change shard assignment",
            );
        }
    }

    // -----------------------------------------------------------------------
    // Test 2: contract_read_not_blocked_by_other_shard_write
    // -----------------------------------------------------------------------

    /// Verify the core contract: a write lock on one shard does not block reads
    /// on a different shard.
    ///
    /// Uses `std::thread::scope` so the spawned thread can borrow `idx`
    /// directly — no `Arc` wrapper required.
    #[test]
    fn contract_read_not_blocked_by_other_shard_write() {
        use std::sync::Barrier;
        use std::time::{Duration, Instant};

        let idx = ShardedIndex::new_in_memory(1000, 16).unwrap();

        // Find two keys that map to different shards
        let (key_a, key_b) = find_different_shard_keys(&idx);

        // Register key_b so lookup has something to find
        idx.register(key_b, make_entry(4096)).unwrap();

        let barrier = Barrier::new(2);
        // Use an Arc<AtomicBool> as the release signal so the spawned thread
        // can be notified without requiring `Receiver` (which is `!Sync`) to
        // cross a scoped-thread boundary.
        let release_flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let release_flag_clone = std::sync::Arc::clone(&release_flag);

        // `thread::scope` borrows `idx` so no Arc / Send requirement on ShardedIndex.
        std::thread::scope(|s| {
            s.spawn(|| {
                let _guard = idx.write_shard(&key_a);
                barrier.wait(); // signal "lock held"
                // Spin-wait for release (flag is set by the main thread below).
                while !release_flag_clone.load(std::sync::atomic::Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                // _guard dropped here
            });

            // Wait until the other thread holds the write lock
            barrier.wait();

            // Read key_b (different shard) — must not block
            let start = Instant::now();
            let result = idx.lookup(&key_b);
            let elapsed = start.elapsed();

            // Release the parked thread
            release_flag.store(true, std::sync::atomic::Ordering::Release);

            assert!(
                result.is_some(),
                "lookup of key_b should find the registered entry"
            );
            assert!(
                elapsed < Duration::from_millis(200),
                "lookup on a different shard must not block (took {elapsed:?})",
            );
        });
    }

    /// Find two keys in the index that hash to different shards.
    fn find_different_shard_keys(idx: &ShardedIndex) -> (TxKey, TxKey) {
        let mut by_shard: std::collections::HashMap<usize, TxKey> = Default::default();
        for i in 0u64..100_000 {
            let mut txid = [0u8; 32];
            txid[24..32].copy_from_slice(&i.to_le_bytes());
            let key = TxKey { txid };
            let s = idx.index_shard_for_key(&key);
            by_shard.entry(s).or_insert(key);
            if by_shard.len() >= 2 {
                let mut iter = by_shard.values();
                let a = *iter.next().unwrap();
                let b = *iter.next().unwrap();
                return (a, b);
            }
        }
        panic!("could not find two keys with different shard assignments");
    }

    // -----------------------------------------------------------------------
    // Test 3: per_key_crud_parity
    // -----------------------------------------------------------------------

    /// Verify that ShardedIndex produces byte-identical results to a plain
    /// PrimaryBackend oracle for register / lookup / update / unregister and
    /// their batch variants.
    #[test]
    fn per_key_crud_parity() {
        let sharded = ShardedIndex::new_in_memory(2000, 16).unwrap();
        let mut oracle = PrimaryBackend::new_in_memory(2000).unwrap();

        // Register 1000 keys in both
        for i in 0..1000u64 {
            let key = make_key(i);
            let entry = make_entry(i * 100);
            sharded.register(key, entry).unwrap();
            oracle.register(key, entry).unwrap();
        }

        // Verify all lookups match oracle
        for i in 0..1000u64 {
            let key = make_key(i);
            let sharded_result = sharded.lookup(&key);
            let oracle_result = oracle.lookup(&key);
            assert_eq!(sharded_result, oracle_result, "lookup mismatch for key {i}",);
        }

        // Update cached fields for the first half
        for i in 0..500u64 {
            let key = make_key(i);
            let sharded_updated = sharded
                .update_cached_fields(&key, 0xAA, 3, 7, 100, 200, 42)
                .unwrap();
            let oracle_updated = oracle
                .update_cached_fields(&key, 0xAA, 3, 7, 100, 200, 42)
                .unwrap();
            assert_eq!(
                sharded_updated, oracle_updated,
                "update_cached_fields return mismatch for key {i}"
            );
        }

        // Verify updated fields match
        for i in 0..500u64 {
            let key = make_key(i);
            let sharded_entry = sharded.lookup(&key).unwrap();
            let oracle_entry = oracle.lookup(&key).unwrap();
            assert_eq!(
                sharded_entry, oracle_entry,
                "entry mismatch after update for key {i}",
            );
        }

        // Unregister the second half
        for i in 500..1000u64 {
            let key = make_key(i);
            let sharded_removed = sharded.unregister(&key);
            let oracle_removed = oracle.unregister(&key);
            assert_eq!(
                sharded_removed, oracle_removed,
                "unregister mismatch for key {i}",
            );
        }

        // First half still present, second half gone
        for i in 0..500u64 {
            assert!(
                sharded.lookup(&make_key(i)).is_some(),
                "key {i} should still be present",
            );
        }
        for i in 500..1000u64 {
            assert!(
                sharded.lookup(&make_key(i)).is_none(),
                "key {i} should be gone",
            );
        }

        // Batch unregister the first 100 (still-present) keys
        let batch_keys: Vec<TxKey> = (0..100u64).map(make_key).collect();
        let sharded_batch = sharded.unregister_batch(&batch_keys).unwrap();
        let oracle_batch = oracle.unregister_batch(&batch_keys).unwrap();
        assert_eq!(
            sharded_batch, oracle_batch,
            "unregister_batch result mismatch",
        );
    }

    // -----------------------------------------------------------------------
    // Test 4: len_and_is_empty_match_oracle
    // -----------------------------------------------------------------------

    /// `len` and `is_empty` must equal an equivalent single-backend oracle.
    #[test]
    fn len_and_is_empty_match_oracle() {
        let sharded = ShardedIndex::new_in_memory(2000, 16).unwrap();
        let mut oracle = PrimaryBackend::new_in_memory(2000).unwrap();

        assert_eq!(sharded.len(), 0);
        assert!(sharded.is_empty());

        for i in 0..500u64 {
            let key = make_key(i);
            let entry = make_entry(i * 64);
            sharded.register(key, entry).unwrap();
            oracle.register(key, entry).unwrap();
        }

        assert_eq!(sharded.len(), oracle.len(), "len must match oracle");
        assert_eq!(sharded.len(), 500);
        assert!(!sharded.is_empty());

        // Unregister all and check empty
        for i in 0..500u64 {
            sharded.unregister(&make_key(i));
        }
        assert_eq!(sharded.len(), 0);
        assert!(sharded.is_empty());
    }

    // -----------------------------------------------------------------------
    // Test 5: stats_merged_matches_oracle
    // -----------------------------------------------------------------------

    /// Merged stats must have a sane entry_count, load_factor, and memory_bytes.
    #[test]
    fn stats_merged_matches_oracle() {
        let sharded = ShardedIndex::new_in_memory(2000, 16).unwrap();
        let mut oracle = PrimaryBackend::new_in_memory(2000).unwrap();

        for i in 0..400u64 {
            let key = make_key(i);
            let entry = make_entry(i * 64);
            sharded.register(key, entry).unwrap();
            oracle.register(key, entry).unwrap();
        }

        let stats = sharded.stats();
        let oracle_stats = oracle.stats();

        assert_eq!(
            stats.entry_count, oracle_stats.entry_count,
            "merged entry_count must equal oracle"
        );

        // load_factor must be in (0, 1) and reflect actual utilisation
        assert!(
            stats.load_factor > 0.0 && stats.load_factor < 1.0,
            "load_factor must be in (0, 1), got {}",
            stats.load_factor
        );

        // memory_bytes must be non-zero (the index is populated)
        assert!(
            stats.memory_bytes > 0,
            "memory_bytes must be > 0, got {}",
            stats.memory_bytes
        );

        // Summed capacity must be at least as large as the total entry_count
        assert!(
            stats.capacity >= stats.entry_count,
            "capacity ({}) must be >= entry_count ({})",
            stats.capacity,
            stats.entry_count
        );
    }

    // -----------------------------------------------------------------------
    // Test 6: all_keys_matches_oracle
    // -----------------------------------------------------------------------

    /// `all_keys` must return exactly the same set as the oracle (order-agnostic).
    #[test]
    fn all_keys_matches_oracle() {
        let sharded = ShardedIndex::new_in_memory(2000, 16).unwrap();
        let mut oracle = PrimaryBackend::new_in_memory(2000).unwrap();

        for i in 0..300u64 {
            let key = make_key(i);
            let entry = make_entry(i * 32);
            sharded.register(key, entry).unwrap();
            oracle.register(key, entry).unwrap();
        }

        let mut sharded_keys = sharded.all_keys();
        let mut oracle_keys: Vec<TxKey> = oracle.iter().map(|(k, _)| k).collect();

        // Sort by txid bytes for order-agnostic comparison
        sharded_keys.sort_by_key(|k| k.txid);
        oracle_keys.sort_by_key(|k| k.txid);

        assert_eq!(
            sharded_keys, oracle_keys,
            "all_keys must return the same set as the oracle"
        );
    }

    // -----------------------------------------------------------------------
    // Test 7: for_each_visits_exactly_registered_set
    // -----------------------------------------------------------------------

    /// `for_each` must visit every registered `(key, entry)` pair exactly once.
    #[test]
    fn for_each_visits_exactly_registered_set() {
        use std::collections::HashMap;

        let sharded = ShardedIndex::new_in_memory(500, 16).unwrap();
        let mut expected: HashMap<[u8; 32], TxIndexEntry> = HashMap::new();

        for i in 0..200u64 {
            let key = make_key(i);
            let entry = make_entry(i * 16);
            sharded.register(key, entry).unwrap();
            expected.insert(key.txid, entry);
        }

        let mut visited: HashMap<[u8; 32], TxIndexEntry> = HashMap::new();
        sharded.for_each(|k, e| {
            let prev = visited.insert(k.txid, *e);
            assert!(prev.is_none(), "for_each visited key {:?} twice", k.txid);
        });

        assert_eq!(
            visited.len(),
            expected.len(),
            "for_each visited {} entries, expected {}",
            visited.len(),
            expected.len()
        );
        for (txid, entry) in &expected {
            let got = visited
                .get(txid)
                .unwrap_or_else(|| panic!("for_each missed key {txid:?}"));
            assert_eq!(got, entry, "entry mismatch for key {txid:?}");
        }
    }

    // -----------------------------------------------------------------------
    // Test 8: keys_by_shard_groups_correctly
    // -----------------------------------------------------------------------

    /// `keys_by_shard` must group keys by the same cluster-shard function as
    /// a manual scan, and `keys_for_shard` must be consistent with it.
    #[test]
    fn keys_by_shard_groups_correctly() {
        use crate::cluster::shards::ShardTable;
        use std::collections::HashMap;

        let sharded = ShardedIndex::new_in_memory(500, 16).unwrap();

        for i in 0..200u64 {
            let key = make_key(i);
            sharded.register(key, make_entry(i * 8)).unwrap();
        }

        let by_shard = sharded.keys_by_shard();

        // Manual oracle: group the same keys
        let mut oracle: HashMap<u16, Vec<TxKey>> = HashMap::new();
        for i in 0..200u64 {
            let key = make_key(i);
            oracle
                .entry(ShardTable::shard_for_key(&key))
                .or_default()
                .push(key);
        }

        // Total entry count across all groups must match
        let total: usize = by_shard.values().map(|v| v.len()).sum();
        assert_eq!(total, 200, "total keys across all groups must be 200");

        // Each group must match the oracle (sets, not ordered)
        for (shard, mut keys) in by_shard {
            let mut expected = oracle.remove(&shard).unwrap_or_default();
            keys.sort_by_key(|k| k.txid);
            expected.sort_by_key(|k| k.txid);
            assert_eq!(
                keys, expected,
                "keys_by_shard group {shard} does not match oracle"
            );

            // Also check that keys_for_shard is consistent
            let mut single = sharded.keys_for_shard(shard);
            single.sort_by_key(|k| k.txid);
            assert_eq!(
                single, keys,
                "keys_for_shard({shard}) inconsistent with keys_by_shard"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Test 9: keys_by_shard_filtered_subset
    // -----------------------------------------------------------------------

    /// `keys_by_shard_filtered` must return exactly the shards in the filter.
    #[test]
    fn keys_by_shard_filtered_subset() {
        use crate::cluster::shards::ShardTable;
        use std::collections::HashSet;

        let sharded = ShardedIndex::new_in_memory(500, 16).unwrap();
        for i in 0..200u64 {
            sharded.register(make_key(i), make_entry(i * 8)).unwrap();
        }

        // Build a filter with just 3 arbitrary cluster shards
        let mut filter: HashSet<u16> = HashSet::new();
        for i in 0..200u64 {
            let k = make_key(i);
            let s = ShardTable::shard_for_key(&k);
            filter.insert(s);
            if filter.len() >= 3 {
                break;
            }
        }

        let filtered = sharded.keys_by_shard_filtered(&filter);

        // All returned shards must be in the filter
        for shard in filtered.keys() {
            assert!(
                filter.contains(shard),
                "keys_by_shard_filtered returned shard {shard} not in filter"
            );
        }

        // Cross-check with keys_by_shard: filtered result must match the
        // corresponding groups in the full map
        let full = sharded.keys_by_shard();
        for shard in &filter {
            let mut filtered_keys = filtered.get(shard).cloned().unwrap_or_default();
            let mut full_keys = full.get(shard).cloned().unwrap_or_default();
            filtered_keys.sort_by_key(|k| k.txid);
            full_keys.sort_by_key(|k| k.txid);
            assert_eq!(
                filtered_keys, full_keys,
                "filtered shard {shard} does not match full map"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Test 10: conflicting_scan_returns_correct_keys
    // -----------------------------------------------------------------------

    /// `for_each_conflicting` must return exactly the keys with CONFLICTING set.
    #[test]
    fn conflicting_scan_returns_correct_keys() {
        use crate::record::TxFlags;
        use std::collections::HashSet;

        let sharded = ShardedIndex::new_in_memory(500, 16).unwrap();

        // Register 100 keys; mark every third one as CONFLICTING
        let mut expected_conflicting: HashSet<[u8; 32]> = HashSet::new();
        for i in 0..100u64 {
            let key = make_key(i);
            let mut entry = make_entry(i * 16);
            if i % 3 == 0 {
                entry.tx_flags |= TxFlags::CONFLICTING.bits();
                expected_conflicting.insert(key.txid);
            }
            sharded.register(key, entry).unwrap();
        }

        let mut got: HashSet<[u8; 32]> = HashSet::new();
        sharded.for_each_conflicting(|k| {
            got.insert(k.txid);
        });

        assert_eq!(
            got, expected_conflicting,
            "for_each_conflicting returned wrong set of keys"
        );
    }

    // -----------------------------------------------------------------------
    // Test 11: flush_durable_ok_on_memory_backend
    // -----------------------------------------------------------------------

    /// `flush_durable` must succeed on the in-memory backend (it is a no-op).
    #[test]
    fn flush_durable_ok_on_memory_backend() {
        let sharded = ShardedIndex::new_in_memory(1000, 16).unwrap();
        for i in 0..50u64 {
            sharded.register(make_key(i), make_entry(i * 8)).unwrap();
        }
        let result = sharded.flush_durable();
        assert!(
            result.is_ok(),
            "flush_durable must succeed on the in-memory backend, got {result:?}"
        );
        // Entries survive flush
        assert_eq!(sharded.len(), 50);
    }

    // -----------------------------------------------------------------------
    // Test 12: resize_shard_survives_and_lookups_still_work
    // -----------------------------------------------------------------------

    /// After filling one shard past the resize threshold, `resize_shard_if_needed`
    /// must succeed, entries in that shard must remain findable, and entries in
    /// OTHER shards must be untouched.
    ///
    /// Strategy: use N=1 (single shard) so every key goes to shard 0, and
    /// start with a known initial capacity of 64 (via `new_in_memory(44, 1)`
    /// which gives capacity = ceil(44/0.7) ≈ 64 buckets). Inserting 46 entries
    /// with `register_without_resize` pushes the load factor above 0.7 (46/64
    /// ≈ 0.72) without overflowing the table.
    #[test]
    fn resize_shard_survives_and_lookups_still_work() {
        // N=1: all keys land in shard 0.
        // expected_records=44 → initial capacity ≈ 64 (next pow2 of ceil(44/0.7)=63).
        let sharded = ShardedIndex::new_in_memory(44, 1).unwrap();

        // Determine initial capacity so we can compute how many inserts reach >70%.
        let initial_capacity = sharded.shards[0].read().stats().capacity;
        // Insert 71% of the capacity without triggering an inline resize.
        // Floating-point: threshold = 0.7 × capacity. Insert (capacity × 71 / 100) items.
        let fill_count = (initial_capacity * 71 / 100) as u64;

        for i in 0..fill_count {
            sharded
                .register_without_resize(make_key(i), make_entry(i * 8))
                .unwrap();
        }

        // Shard 0 must now report a resize is needed
        let needs_resize = sharded.shards[0].read().resize_target_capacity().is_some();
        assert!(
            needs_resize,
            "shard 0 must need a resize after inserting {fill_count} entries \
             into a {initial_capacity}-bucket table without inline resize"
        );

        // Resize must succeed
        sharded.resize_shard_if_needed(0).unwrap();

        // After resize, no further resize should be needed
        let still_needs = sharded.shards[0].read().resize_target_capacity().is_some();
        assert!(
            !still_needs,
            "shard 0 must not need a resize immediately after one was applied"
        );

        // All inserted entries must still be findable
        for i in 0..fill_count {
            let entry = sharded
                .lookup(&make_key(i))
                .unwrap_or_else(|| panic!("key {i} not found after resize"));
            assert_eq!(entry.record_offset, i * 8, "wrong offset for key {i}");
        }

        // Out-of-range shard_idx must return an error
        let err = sharded.resize_shard_if_needed(99);
        assert!(
            matches!(err, Err(IndexError::FormatError { .. })),
            "out-of-range shard_idx must return FormatError, got {err:?}"
        );
    }
}
