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

use std::sync::OnceLock;

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::index::backend::PrimaryBackend;
use crate::index::redb_primary::CachedFieldsUpdate;
use crate::index::{IndexError, TxIndexEntry, TxKey};

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
            let mut guard = self.shards[shard_idx].write();
            for &(orig_idx, key) in items {
                results[orig_idx] = guard.unregister(&key);
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
    /// Uses `std::thread::scope` so `ShardedIndex` does not need to be `Send`
    /// (it is `!Sync` in test builds because `RedbPrimary` carries `Cell<bool>`
    /// fault-injection fields).
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
}
