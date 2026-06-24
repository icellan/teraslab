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

use crate::allocator::SlotAllocator;
use crate::cluster::shards::ShardTable;
use crate::device::BlockDevice;
use crate::index::backend::PrimaryBackend;
use crate::index::redb_primary::CachedFieldsUpdate;
use crate::index::{
    DahIndex, Index, IndexError, IndexStats, RestoreFlags, TxIndexEntry, TxKey, UnminedIndex,
};
use crate::record::TxFlags;

// ---------------------------------------------------------------------------
// v2 N-shard snapshot format
// ---------------------------------------------------------------------------

/// Magic identifying a v2 N-shard sharded-index snapshot manifest.
///
/// DISTINCT from the v1 single-table primary magic
/// ([`crate::index::PRIMARY_SNAPSHOT_MAGIC`] = `b"TSIX"`) so the two formats
/// are unambiguous: [`ShardedIndex::restore_all`] dispatches on the first four
/// bytes and never has to guess. `b"TSX2"` = "TeraSlab indeX, format 2".
const V2_MAGIC: [u8; 4] = *b"TSX2";

/// Version stamp inside a v2 manifest header. A reader that sees `V2_MAGIC`
/// but a version other than this fails closed rather than guessing the layout.
const V2_VERSION: u32 = 2;

/// Byte length of the fixed v2 manifest header that precedes the per-shard
/// region table:
/// `magic(4) + version(4) + shard_count(4) + seed(8)`.
const V2_HEADER_SIZE: usize = 4 + 4 + 4 + 8;

/// Byte length of one entry in the v2 per-shard region table:
/// `region_offset(8) + region_len(8)`. Offsets are absolute from the start of
/// the file; lengths cover one full v1 primary payload.
const V2_REGION_ENTRY_SIZE: usize = 8 + 8;

/// Upper bound on `shard_count` accepted from an on-disk v2 manifest. Mirrors
/// the `[1, 256]` clamp the live constructors enforce; a manifest claiming an
/// absurd shard count (corruption or a hostile file) is rejected up front so
/// the region-table allocation cannot be driven huge.
const V2_MAX_SHARD_COUNT: usize = 256;

// ---------------------------------------------------------------------------
// Process-local seed
// ---------------------------------------------------------------------------

/// Returns the process-local random seed used for shard selection.
///
/// Initialised once from `getrandom`; falls back to `RandomState` if the
/// syscall is unavailable (e.g. restricted sandboxes).
fn index_shard_seed() -> u64 {
    static SEED: OnceLock<u64> = OnceLock::new();
    *SEED.get_or_init(crate::index::hashmix::init_process_seed)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Round `n` up to the nearest power of two, clamped to `[1, 256]`.
fn clamp_shard_count(n: usize) -> usize {
    let n = n.clamp(1, 256);
    n.next_power_of_two().min(256)
}

/// Panic-free `&[u8]` → `[u8; 4]`. The v2 parser only ever calls this on a
/// slice it has already length-checked, so the fallback (all-zeros) is
/// unreachable in practice; it keeps the library code free of `unwrap`.
#[inline]
fn arr4(s: &[u8]) -> [u8; 4] {
    s.try_into().unwrap_or([0u8; 4])
}

/// Panic-free `&[u8]` → `[u8; 8]`. See [`arr4`].
#[inline]
fn arr8(s: &[u8]) -> [u8; 8] {
    s.try_into().unwrap_or([0u8; 8])
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
/// by `clamp_shard_count`. A power-of-two count allows the shard selection to
/// use a bitmask instead of a modulo.
pub struct ShardedIndex {
    shards: Vec<RwLock<PrimaryBackend>>,
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
        Ok(Self { shards, seed })
    }

    /// Wrap an existing [`PrimaryBackend`] as a single-shard `ShardedIndex`.
    ///
    /// This is the transparent pass-through used by the engine migration: a
    /// `ShardedIndex` with `shard_count == 1` routes every key to the one shard
    /// (the inline mask `shards.len() - 1` is `0`), so its behaviour is
    /// byte-for-byte identical to the wrapped backend behind a plain `RwLock`. A
    /// fresh process-local seed is installed; at one shard the seed never
    /// affects routing (the mask is `0`), so its only role is to keep the type
    /// uniform with the multi-shard constructors.
    ///
    /// Used to migrate `Engine.index` to `ShardedIndex` without changing the
    /// `PrimaryBackend` semantics or the recovery/snapshot on-disk formats —
    /// the recovered or rebuilt backend is wrapped here at engine construction.
    pub fn from_single(backend: PrimaryBackend) -> Self {
        Self {
            shards: vec![RwLock::new(backend)],
            seed: index_shard_seed(),
        }
    }

    /// Build a `ShardedIndex` from a vector of already-populated backends under
    /// an explicit `seed`.
    ///
    /// `backends.len()` MUST already be a power of two in `[1, 256]` (the
    /// caller is the v2 restore path, which validates the manifest shard count
    /// before calling). The `seed` is installed verbatim so that
    /// [`Self::index_shard_for_key`] maps every key back to the shard region it
    /// was snapshotted from — i.e. routing is consistent with the layout the
    /// backends were placed in. This is why the v2 manifest persists the seed:
    /// without it a fresh process seed would scatter keys to different shards
    /// than their region, breaking the direct (matching-count) restore.
    fn from_backends_with_seed(backends: Vec<PrimaryBackend>, seed: u64) -> Self {
        let shards: Vec<RwLock<PrimaryBackend>> = backends.into_iter().map(RwLock::new).collect();
        Self { shards, seed }
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
        // SplitMix64 finalizer over (raw XOR seed) — shared impl in
        // `crate::index::hashmix`.
        let x = crate::index::hashmix::splitmix64_finalize(raw ^ self.seed);
        // `shards.len()` is always a clamped power-of-two ≥ 1 (every constructor
        // routes through `clamp_shard_count`), so `len - 1` is always the
        // correct bitmask — no separate `shard_mask` field needed.
        (x as usize) & (self.shards.len() - 1)
    }

    /// Acquire a read lock on the shard that owns `key`.
    pub fn read_shard(&self, key: &TxKey) -> RwLockReadGuard<'_, PrimaryBackend> {
        self.shards[self.index_shard_for_key(key)].read()
    }

    /// Acquire a write lock on the shard that owns `key`.
    pub fn write_shard(&self, key: &TxKey) -> RwLockWriteGuard<'_, PrimaryBackend> {
        self.shards[self.index_shard_for_key(key)].write()
    }

    /// Acquire a write lock on shard `index_shard` directly (no routing).
    ///
    /// For callers that have already computed the owning shard via
    /// [`Self::index_shard_for_key`] and want to avoid recomputing it inside
    /// [`Self::write_shard`]. `index_shard` MUST be in `[0, shard_count())` —
    /// guaranteed when it comes from `index_shard_for_key` on the same
    /// `ShardedIndex` instance (the mask bounds it to a valid shard).
    #[inline]
    pub fn write_shard_at(&self, index_shard: usize) -> RwLockWriteGuard<'_, PrimaryBackend> {
        self.shards[index_shard].write()
    }

    /// Acquire a read lock on shard `index_shard` directly (no routing).
    ///
    /// See [`Self::write_shard_at`] for the index contract.
    #[inline]
    pub fn read_shard_at(&self, index_shard: usize) -> RwLockReadGuard<'_, PrimaryBackend> {
        self.shards[index_shard].read()
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
    /// See `PrimaryBackend::register_without_resize` for the contract.
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

    /// Non-blocking merged statistics — all-or-nothing.
    ///
    /// Attempts a [`try_read`](parking_lot::RwLock::try_read) on every shard.
    /// Returns `Some(merged)` only when ALL shard reads succeed (no shard is
    /// contended by a writer); returns `None` if ANY shard's read lock is
    /// momentarily held by a writer.
    ///
    /// When `None` is returned, the `/admin/top` HTTP layer serves the last
    /// consistent snapshot stored in `TOP_STATS_CACHE` (seeded on cold start
    /// and refreshed on every `Some` result).
    ///
    /// The merged result follows the same aggregation rules as [`Self::stats`]:
    /// `entry_count`, `capacity`, and `memory_bytes` are summed; `load_factor`
    /// is recomputed as the ratio of the totals; `max_probe_distance` is the
    /// maximum across shards; `hugepage_enabled` is `true` only when ALL shards
    /// report it.
    pub fn try_stats(&self) -> Option<IndexStats> {
        let mut guards = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            match shard.try_read() {
                Some(g) => guards.push(g),
                None => return None,
            }
        }

        let mut total_entries = 0usize;
        let mut total_capacity = 0usize;
        let mut total_memory = 0usize;
        let mut max_probe = 0usize;
        let mut all_huge = true;

        for guard in &guards {
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
    ///
    /// # Invariant
    ///
    /// `shards[0]` is always valid: every constructor calls `clamp_shard_count`
    /// which returns `n.clamp(1, 256).next_power_of_two()`, so `shard_count >= 1`
    /// is guaranteed and `shards` is never empty.
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

    /// Attach a redo log to every shard for file-backed resize journaling.
    ///
    /// Delegates to [`PrimaryBackend::set_redo_log`] on each shard. For
    /// in-memory and redb shards this is a no-op on each shard; for
    /// file-backed shards it enables crash-atomic resize (Begin/Commit
    /// journaling + parent-dir fsync).
    pub fn set_redo_log(&self, redo_log: std::sync::Arc<parking_lot::Mutex<crate::redo::RedoLog>>) {
        for shard in &self.shards {
            shard.write().set_redo_log(redo_log.clone());
        }
    }

    /// Flush every shard durable.
    ///
    /// Iterates all shard locks (read is sufficient — `flush_durable` takes
    /// `&self` on `PrimaryBackend`) and attempts to flush EVERY shard, then
    /// returns the FIRST error encountered (if any). Continuing past an early
    /// failure ensures a durability flush is never silently partial: an error
    /// from shard `i` no longer leaves shards `i+1..N` unflushed.
    ///
    /// # Errors
    ///
    /// Returns the first [`IndexError`] encountered across all shards, after
    /// every shard's flush has been attempted.
    pub fn flush_durable(&self) -> Result<(), IndexError> {
        let mut first_err: Option<IndexError> = None;
        for shard in &self.shards {
            if let Err(e) = shard.read().flush_durable() {
                // Keep only the FIRST error; still attempt the remaining shards
                // so the durability flush is never silently partial.
                first_err.get_or_insert(e);
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Snapshot the primary index and both secondary indexes to `path`.
    ///
    /// # Format
    ///
    /// - **At one shard** this delegates straight to the wrapped backend's
    ///   [`PrimaryBackend::snapshot_all`], producing a v1 (`TSIX`) snapshot
    ///   byte-for-byte identical to the pre-sharding engine. The engine runs at
    ///   one shard for now, so its checkpoint snapshots stay readable by the
    ///   existing [`PrimaryBackend::restore_all`] path.
    /// - **At more than one shard** this writes a v2 N-shard manifest: a fixed
    ///   header `{ magic = TSX2, version = 2, shard_count, seed }`, a per-shard
    ///   region table `{ offset, len }`, the `N` per-shard v1 primary payloads,
    ///   the (unsharded) dah/unmined secondary sections, and a trailing CRC32
    ///   over everything. The `seed` is persisted so a matching-count restore
    ///   can place region `i` back into shard `i` and have
    ///   [`Self::index_shard_for_key`] route every key to its own region.
    ///
    /// Secondary indexes (dah/unmined) are NOT sharded — they are written once.
    ///
    /// Only the in-memory backend produces a file; the redb / file-backed
    /// shards are self-persistent, so a non-`InMemory` shard is rejected
    /// (mirroring [`PrimaryBackend::snapshot_all`], which is a no-op for those
    /// variants — a sharded multi-region file would be meaningless for them).
    ///
    /// The snapshot file is written atomically via a temp file + rename +
    /// parent directory fsync.
    ///
    /// # Cross-shard atomicity
    ///
    /// The FILE write is atomic, but at N>1 shards the per-shard read locks are
    /// taken serially (one region at a time in `Self::serialize_v2`), so the
    /// snapshot is NOT a single point-in-time view across shards: region `i+1`
    /// may include mutations that landed after region `i` was serialized. A
    /// caller that needs a consistent cross-shard snapshot MUST quiesce writes
    /// first — the checkpoint path does this by holding
    /// `dispatch_visibility_barrier.write()`.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::FormatError`] if the secondary backends are not
    /// the in-memory variants, or if any shard is not an in-memory backend;
    /// [`IndexError::Io`] on a filesystem failure; and propagates the v1
    /// delegate's error at one shard.
    pub fn snapshot_all(
        &self,
        dah: &crate::index::DahBackend,
        unmined: &crate::index::UnminedBackend,
        path: &std::path::Path,
    ) -> Result<(), IndexError> {
        // One shard: keep the v1 format so the engine's checkpoint snapshots
        // round-trip through the unchanged `PrimaryBackend::restore_all`.
        if self.shards.len() == 1 {
            return self.shards[0].read().snapshot_all(dah, unmined, path);
        }

        // Multi-shard: build the v2 manifest. Secondary backends must be the
        // in-memory variants (redb secondaries are self-durable and the v1
        // `snapshot_all` skips them; we mirror that by requiring InMemory here
        // so the written sections are well-defined).
        let (
            crate::index::DahBackend::InMemory(dah_idx),
            crate::index::UnminedBackend::InMemory(unmined_idx),
        ) = (dah, unmined)
        else {
            return Err(IndexError::FormatError {
                detail: "ShardedIndex::snapshot_all v2 requires in-memory dah/unmined backends"
                    .into(),
            });
        };

        let data = self.serialize_v2(dah_idx, unmined_idx)?;

        // Atomic write: temp file + fsync + rename + parent dir fsync, matching
        // `Index::snapshot_all`.
        let tmp_path = path.with_extension("tmp");
        std::fs::write(&tmp_path, &data)?;
        let f = std::fs::File::open(&tmp_path)?;
        f.sync_all()?;
        drop(f);
        std::fs::rename(&tmp_path, path)?;
        crate::index::util::fsync_parent_dir(path)?;
        Ok(())
    }

    /// Serialize this index into the v2 N-shard manifest byte layout.
    ///
    /// Each shard's read lock is taken serially (one region at a time), so the
    /// resulting bytes are NOT atomic across shards at N>1: later regions may
    /// reflect mutations that occurred after earlier regions were read. The
    /// caller ([`Self::snapshot_all`]) must quiesce writes via the visibility
    /// barrier when a consistent cross-shard snapshot is required.
    ///
    /// Returns [`IndexError::FormatError`] if any shard is not an in-memory
    /// backend (the only variant that participates in the in-memory snapshot).
    fn serialize_v2(&self, dah: &DahIndex, unmined: &UnminedIndex) -> Result<Vec<u8>, IndexError> {
        let shard_count = self.shards.len();

        // Serialize each shard's primary payload first so we know region sizes.
        let mut regions: Vec<Vec<u8>> = Vec::with_capacity(shard_count);
        for (i, shard) in self.shards.iter().enumerate() {
            let guard = shard.read();
            let idx = guard
                .as_in_memory_index()
                .ok_or_else(|| IndexError::FormatError {
                    detail: format!(
                        "ShardedIndex::snapshot_all v2 requires in-memory shards; shard {i} is {}",
                        guard.backend_name()
                    ),
                })?;
            regions.push(idx.serialize_primary());
        }

        let region_table_size = shard_count * V2_REGION_ENTRY_SIZE;
        let regions_start = V2_HEADER_SIZE + region_table_size;

        // Compute absolute offsets for each region.
        let mut region_offsets: Vec<u64> = Vec::with_capacity(shard_count);
        let mut cursor = regions_start as u64;
        for region in &regions {
            region_offsets.push(cursor);
            cursor += region.len() as u64;
        }

        let secondary = crate::index::serialize_secondary_sections(dah, unmined);
        let total_body: usize =
            regions_start + regions.iter().map(|r| r.len()).sum::<usize>() + secondary.len();

        let mut buf = Vec::with_capacity(total_body + 4);
        // Header.
        buf.extend_from_slice(&V2_MAGIC);
        buf.extend_from_slice(&V2_VERSION.to_le_bytes());
        buf.extend_from_slice(&(shard_count as u32).to_le_bytes());
        buf.extend_from_slice(&self.seed.to_le_bytes());
        // Region table.
        for (off, region) in region_offsets.iter().zip(&regions) {
            buf.extend_from_slice(&off.to_le_bytes());
            buf.extend_from_slice(&(region.len() as u64).to_le_bytes());
        }
        // Shard regions.
        for region in &regions {
            buf.extend_from_slice(region);
        }
        // Unsharded secondary sections.
        buf.extend_from_slice(&secondary);
        // Trailing CRC32 over everything above.
        let checksum = crc32fast::hash(&buf);
        buf.extend_from_slice(&checksum.to_le_bytes());
        Ok(buf)
    }

    /// Restore a sharded in-memory index from a snapshot at `path`, targeting
    /// `target_shard_count` shards.
    ///
    /// Dispatches on the snapshot's leading magic:
    ///
    /// - **v2 manifest** (`TSX2`) whose `shard_count == clamp(target_shard_count)`:
    ///   each region is restored directly into its shard and the persisted seed
    ///   is reinstalled, so [`Self::index_shard_for_key`] maps every key back to
    ///   its region — no rehash beyond the per-shard table restore.
    /// - **v2 manifest with a mismatched shard count**, or a **v1 single-table
    ///   snapshot** (`TSIX`): RE-SHARD. Every entry is read out and `register`ed
    ///   into the shard the *target* [`Self::index_shard_for_key`] selects under
    ///   a fresh process seed.
    /// - **unknown magic, or `TSX2` with an unsupported version**: fail closed
    ///   with [`IndexError::FormatError`] — never guess, never panic.
    ///
    /// The dah/unmined secondary indexes are returned unsharded, restored
    /// exactly as the v1 path does (independent-section recovery via
    /// `crate::index::parse_secondary_sections`).
    ///
    /// # Errors
    ///
    /// [`IndexError::Io`] if the file cannot be read; [`IndexError::FormatError`]
    /// on an unknown/short/corrupt manifest or unsupported version;
    /// [`IndexError::ChecksumMismatch`] if the v2 trailer CRC fails; and any
    /// error propagated from the underlying primary restore.
    pub fn restore_all(
        path: &std::path::Path,
        target_shard_count: usize,
    ) -> Result<(Self, DahIndex, UnminedIndex, RestoreFlags), IndexError> {
        let data = std::fs::read(path)?;
        if data.len() < 4 {
            return Err(IndexError::FormatError {
                detail: format!(
                    "snapshot too small ({} bytes) to contain a magic",
                    data.len()
                ),
            });
        }
        let magic = &data[0..4];
        if magic == V2_MAGIC {
            Self::restore_v2(&data, target_shard_count)
        } else if magic == crate::index::PRIMARY_SNAPSHOT_MAGIC {
            // v1 single-table snapshot: load via the existing path, then
            // re-shard every entry into the target layout.
            let (index, dah, unmined, flags) = Index::restore_all(path)?;
            let sharded = Self::reshard_from_index(&index, target_shard_count)?;
            Ok((sharded, dah, unmined, flags))
        } else {
            Err(IndexError::FormatError {
                detail: format!(
                    "unknown index snapshot magic {magic:?}; expected v2 {V2_MAGIC:?} or \
                     v1 {:?}",
                    crate::index::PRIMARY_SNAPSHOT_MAGIC
                ),
            })
        }
    }

    /// Parse and restore a v2 N-shard manifest from `data`.
    fn restore_v2(
        data: &[u8],
        target_shard_count: usize,
    ) -> Result<(Self, DahIndex, UnminedIndex, RestoreFlags), IndexError> {
        if data.len() < V2_HEADER_SIZE + 4 {
            return Err(IndexError::FormatError {
                detail: format!(
                    "v2 snapshot too small ({} bytes) for header + crc",
                    data.len()
                ),
            });
        }
        // Magic already matched by the caller; check the version.
        let version = u32::from_le_bytes(arr4(&data[4..8]));
        if version != V2_VERSION {
            return Err(IndexError::FormatError {
                detail: format!("unsupported v2 snapshot version {version}; expected {V2_VERSION}"),
            });
        }
        let shard_count = u32::from_le_bytes(arr4(&data[8..12])) as usize;
        if shard_count == 0 || shard_count > V2_MAX_SHARD_COUNT {
            return Err(IndexError::FormatError {
                detail: format!(
                    "v2 snapshot shard_count {shard_count} out of range (1..={V2_MAX_SHARD_COUNT})"
                ),
            });
        }
        // The live constructors always write a power-of-two count; a non-power-of-two
        // here means the manifest was corrupted or produced by a code path that
        // bypassed `clamp_shard_count`. The fail-closed range check above is the
        // production gate; this assert catches logic bugs in test / debug builds.
        debug_assert!(
            shard_count.is_power_of_two(),
            "snapshot shard_count must be power of two (got {shard_count})"
        );
        let seed = u64::from_le_bytes(arr8(&data[12..20]));

        // Verify the trailing CRC over the whole file (minus the 4-byte CRC).
        let body_end = data.len() - 4;
        let stored_crc = u32::from_le_bytes(arr4(&data[body_end..]));
        let computed_crc = crc32fast::hash(&data[..body_end]);
        if stored_crc != computed_crc {
            return Err(IndexError::ChecksumMismatch {
                expected: stored_crc,
                actual: computed_crc,
            });
        }

        // Region table.
        let region_table_size = shard_count
            .checked_mul(V2_REGION_ENTRY_SIZE)
            .ok_or_else(|| IndexError::FormatError {
                detail: "v2 region table size overflows usize".into(),
            })?;
        let regions_start = V2_HEADER_SIZE
            .checked_add(region_table_size)
            .ok_or_else(|| IndexError::FormatError {
                detail: "v2 regions start overflows usize".into(),
            })?;
        if body_end < regions_start {
            return Err(IndexError::FormatError {
                detail: "v2 snapshot truncated before region table end".into(),
            });
        }

        let mut region_bounds: Vec<(usize, usize)> = Vec::with_capacity(shard_count);
        let mut secondary_start = regions_start;
        for i in 0..shard_count {
            let base = V2_HEADER_SIZE + i * V2_REGION_ENTRY_SIZE;
            let off = u64::from_le_bytes(arr8(&data[base..base + 8])) as usize;
            let len = u64::from_le_bytes(arr8(&data[base + 8..base + 16])) as usize;
            let end = off
                .checked_add(len)
                .ok_or_else(|| IndexError::FormatError {
                    detail: format!("v2 region {i} offset+len overflows usize"),
                })?;
            if off < regions_start || end > body_end {
                return Err(IndexError::FormatError {
                    detail: format!(
                        "v2 region {i} bounds [{off}, {end}) out of range \
                         [{regions_start}, {body_end})"
                    ),
                });
            }
            region_bounds.push((off, end));
            secondary_start = secondary_start.max(end);
        }

        // Secondary sections live after the last region. The trailing CRC (checked
        // above) covers the entire file body including the secondary bytes, so the
        // "secondary starts at max(region_end)" assumption is safe: any truncation
        // or rearrangement of the secondary section would have been caught by the
        // checksum gate before we reach this point.
        let (dah, unmined, flags) =
            crate::index::parse_secondary_sections(&data[secondary_start..]);

        let target = clamp_shard_count(target_shard_count);
        if shard_count == target {
            // Matching count: restore each region directly into its shard and
            // reinstall the persisted seed so routing matches the layout.
            let mut backends: Vec<PrimaryBackend> = Vec::with_capacity(shard_count);
            for &(off, end) in &region_bounds {
                let (idx, _consumed) = Index::deserialize_primary_with_offset(&data[off..end])?;
                backends.push(PrimaryBackend::InMemory(idx));
            }
            let sharded = Self::from_backends_with_seed(backends, seed);
            Ok((sharded, dah, unmined, flags))
        } else {
            // Mismatched count: re-shard every entry into the target layout
            // under a fresh process seed.
            //
            // Deserialize every region first and sum their entry counts so the
            // fresh index is sized for the actual total. Passing `0` here (the
            // old behaviour) hinted 1 entry per shard and triggered a rehash
            // storm on a large snapshot — each shard resized O(log N) times as
            // entries streamed in. The sibling matching-count path and
            // `reshard_from_index` already size from the real count.
            let mut regions: Vec<Index> = Vec::with_capacity(region_bounds.len());
            let mut total_entries = 0usize;
            for &(off, end) in &region_bounds {
                let (idx, _consumed) = Index::deserialize_primary_with_offset(&data[off..end])?;
                total_entries += idx.len();
                regions.push(idx);
            }
            let sharded = Self::new_in_memory(total_entries.max(1), target)?;
            for idx in &regions {
                for (key, entry) in idx.iter() {
                    sharded.register(key, entry)?;
                }
            }
            // Drop interior mutability: `register` took `&self`, but we own
            // `sharded` here, so returning it by value is fine.
            Ok((sharded, dah, unmined, flags))
        }
    }

    /// Re-shard every entry of an existing single [`Index`] into a fresh
    /// `target_shard_count`-shard in-memory index (the v1 → v2 load path).
    fn reshard_from_index(index: &Index, target_shard_count: usize) -> Result<Self, IndexError> {
        let sharded = Self::new_in_memory(index.len().max(1), target_shard_count)?;
        for (key, entry) in index.iter() {
            sharded.register(key, entry)?;
        }
        Ok(sharded)
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
    // Device-scan rebuild into sharded layout
    // -----------------------------------------------------------------------

    /// Rebuild a sharded in-memory index from a full device scan.
    ///
    /// This is the LOW-RISK path: it delegates to the proven
    /// [`PrimaryBackend::rebuild`] to produce a single complete backend, then
    /// routes every entry from that backend into the correct shard of a fresh
    /// `ShardedIndex` using [`Self::index_shard_for_key`]. The single-backend
    /// scan is already battle-tested (issue-#14 tolerance, allocator-freelist
    /// hole skipping, malformed-header resilience), so all that work is reused
    /// without duplication.
    ///
    /// # Parameters
    ///
    /// - `device`: the block device whose allocated regions are scanned.
    /// - `allocator`: the slot allocator whose freelist is used to skip free
    ///   holes during the scan (passed unchanged to `PrimaryBackend::rebuild`).
    /// - `shard_count`: the target number of index shards. Rounded up to the
    ///   next power of two and clamped to `[1, 256]` by `clamp_shard_count`.
    ///   Use `1` for the N=1 degenerate case (single-lock pass-through).
    ///
    /// # Errors
    ///
    /// Propagates [`IndexError`] from `PrimaryBackend::rebuild` (device I/O
    /// errors, unrecoverable scan failures) or from `ShardedIndex::register`
    /// (hash-table allocation failure when routing entries to shards).
    pub fn rebuild_in_memory(
        device: &dyn BlockDevice,
        allocator: &SlotAllocator,
        shard_count: usize,
    ) -> Result<Self, IndexError> {
        // Single-backend device scan — reuses all proven scan logic.
        let single = PrimaryBackend::rebuild(device, allocator)?;
        let count = single.len();

        // Capacity hint: at least 64 per shard so small devices don't
        // allocate tiny tables; scale proportionally for larger scans.
        let per_shard_hint = (count / clamp_shard_count(shard_count))
            .max(64)
            .saturating_mul(2);

        let sharded =
            Self::new_in_memory(per_shard_hint * clamp_shard_count(shard_count), shard_count)?;

        // Route every entry from the single backend into its target shard.
        for (key, entry) in single.iter() {
            sharded.register(key, entry)?;
        }

        Ok(sharded)
    }

    // -----------------------------------------------------------------------
    // Per-shard resize entry point
    // -----------------------------------------------------------------------

    /// Resize index shard `shard_idx` if it currently needs one.
    ///
    /// Acquires the shard's write lock and delegates to
    /// `PrimaryBackend::resize_if_needed`, which checks
    /// `PrimaryBackend::resize_target_capacity`, and (if a resize is needed)
    /// builds a resized copy, marks the old backend defunct, and swaps it in.
    /// Retained for callers that do not already hold the shard guard; the engine
    /// register paths now resize directly on their held guard.
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
        self.shards[shard_idx].write().resize_if_needed()
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

    /// At one shard, `snapshot_all` produces a v1 snapshot that the unchanged
    /// `PrimaryBackend::restore_all` reads back to the same contents. At more
    /// than one shard it now writes the v2 manifest (Task 4) which round-trips
    /// through `ShardedIndex::restore_all`.
    #[test]
    fn from_single_snapshot_roundtrips_and_multishard_uses_v2() {
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

        // Multi-shard snapshot now writes v2 and round-trips through the
        // sharded restore (the pre-Task-4 fail-closed expectation is gone).
        let ms_path = dir.join("snapshot-ms.bin");
        let multishard = ShardedIndex::new_in_memory(1000, 4).unwrap();
        for i in 0..150u64 {
            multishard.register(make_key(i), make_entry(i * 2)).unwrap();
        }
        multishard.snapshot_all(&dah, &unmined, &ms_path).unwrap();
        let head = std::fs::read(&ms_path).unwrap();
        assert_eq!(&head[0..4], &V2_MAGIC, "multi-shard snapshot must be v2");
        let (ms_restored, _d, _u, _f) = ShardedIndex::restore_all(&ms_path, 4).unwrap();
        assert_eq!(ms_restored.len(), 150, "v2 multi-shard restore count");
        for i in 0..150u64 {
            assert!(
                ms_restored.lookup(&make_key(i)).is_some(),
                "v2 multi-shard restore missing key {i}"
            );
        }

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

    // -----------------------------------------------------------------------
    // Task 4: v2 N-shard snapshot + re-shard-on-restore
    // -----------------------------------------------------------------------

    use crate::index::{DahBackend, DahIndex, UnminedBackend, UnminedIndex};
    use crate::record::TxFlags;

    /// Collect every `(key, entry)` from a `ShardedIndex` into a txid-keyed map
    /// for order-agnostic content comparison.
    fn collect_entries(idx: &ShardedIndex) -> std::collections::HashMap<[u8; 32], TxIndexEntry> {
        let mut m = std::collections::HashMap::new();
        idx.for_each(|k, e| {
            m.insert(k.txid, *e);
        });
        m
    }

    /// Build a populated 16-shard index plus matching dah/unmined backends with
    /// a mix of flags so snapshot/restore exercises every field.
    fn populate_dah_unmined() -> (DahBackend, UnminedBackend) {
        let mut dah = DahIndex::new();
        let mut unmined = UnminedIndex::new();
        for i in 0..40u64 {
            dah.insert(1000 + i as u32, make_key(i));
        }
        for i in 0..25u64 {
            unmined.insert(2000 + i as u32, make_key(i + 100));
        }
        (DahBackend::InMemory(dah), UnminedBackend::InMemory(unmined))
    }

    /// Test 1: a v2 N=16 snapshot round-trips — every key lands back in its own
    /// shard, dah/unmined match, and per-entry flags (CONFLICTING /
    /// HAS_PRESERVE_UNTIL) are preserved.
    #[test]
    fn v2_roundtrip_n16() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v2.snap");

        let sharded = ShardedIndex::new_in_memory(4000, 16).unwrap();
        assert_eq!(sharded.shard_count(), 16);

        // Register a spread of keys; vary flags so the bits are non-trivial.
        for i in 0..2000u64 {
            let mut entry = make_entry(i * 7 + 1);
            if i % 5 == 0 {
                entry.tx_flags |= TxFlags::CONFLICTING.bits();
            }
            if i % 7 == 0 {
                entry.tx_flags |= TxFlags::HAS_PRESERVE_UNTIL.bits();
                entry.dah_or_preserve = 5_000 + i as u32;
            }
            sharded.register(make_key(i), entry).unwrap();
        }

        let before = collect_entries(&sharded);
        // Record which shard each key was in so we can assert the layout
        // survives a matching-count restore.
        let shard_of: std::collections::HashMap<[u8; 32], usize> = before
            .keys()
            .map(|txid| (*txid, sharded.index_shard_for_key(&TxKey { txid: *txid })))
            .collect();

        let (dah, unmined) = populate_dah_unmined();
        sharded.snapshot_all(&dah, &unmined, &path).unwrap();

        // File must start with the v2 magic, NOT the v1 magic.
        let head = std::fs::read(&path).unwrap();
        assert_eq!(&head[0..4], &V2_MAGIC, "multi-shard snapshot must be v2");

        let (restored, rdah, runmined, flags) = ShardedIndex::restore_all(&path, 16).unwrap();
        assert!(!flags.dah_needs_rebuild && !flags.unmined_needs_rebuild);
        assert_eq!(restored.shard_count(), 16);

        // Identical contents.
        let after = collect_entries(&restored);
        assert_eq!(after, before, "restored entry set must equal the original");

        // Every key must be findable AND in the SAME shard as before (the
        // persisted seed makes routing consistent with the region layout).
        for (txid, &shard) in &shard_of {
            let key = TxKey { txid: *txid };
            assert!(
                restored.lookup(&key).is_some(),
                "key {txid:?} missing after restore"
            );
            assert_eq!(
                restored.index_shard_for_key(&key),
                shard,
                "key {txid:?} moved shard across matching-count restore"
            );
        }

        // Secondary indexes match.
        assert_eq!(rdah.len(), 40, "dah entry count must survive");
        assert_eq!(runmined.len(), 25, "unmined entry count must survive");
        assert_eq!(
            rdah.range_query(1039).len(),
            40,
            "all dah heights must be queryable"
        );
        assert_eq!(
            runmined.range_query(2024).len(),
            25,
            "all unmined heights must be queryable"
        );
    }

    /// Test 2: re-shard 8 → 16. Snapshot at N=8, restore at N=16; every entry
    /// must be present and live in the shard `index_shard_for_key` picks under
    /// the N=16 instance.
    #[test]
    fn v2_reshard_8_to_16() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v2-8.snap");

        let src = ShardedIndex::new_in_memory(4000, 8).unwrap();
        assert_eq!(src.shard_count(), 8);
        for i in 0..1500u64 {
            src.register(make_key(i), make_entry(i * 3 + 2)).unwrap();
        }
        let before = collect_entries(&src);

        let (dah, unmined) = populate_dah_unmined();
        src.snapshot_all(&dah, &unmined, &path).unwrap();

        let (restored, _rdah, _runmined, _flags) = ShardedIndex::restore_all(&path, 16).unwrap();
        assert_eq!(
            restored.shard_count(),
            16,
            "restore must honour target N=16"
        );

        // All entries present and identical.
        let after = collect_entries(&restored);
        assert_eq!(after, before, "every entry must survive the re-shard");

        // Each key sits in the shard the N=16 router selects, and lookup finds
        // it there.
        for txid in before.keys() {
            let key = TxKey { txid: *txid };
            let shard = restored.index_shard_for_key(&key);
            assert!(shard < 16, "shard index {shard} out of range for N=16");
            assert!(
                restored.lookup(&key).is_some(),
                "key {txid:?} not found after re-shard to N=16"
            );
        }
    }

    /// Test 3a: a genuine v1 single-table snapshot (written by `Index::snapshot_all`)
    /// loads through `ShardedIndex::restore_all`, re-sharding every entry into
    /// the N=16 layout.
    #[test]
    fn v1_single_table_loads_and_reshards_to_16() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v1.snap");

        // Produce a real v1 snapshot via the single-table Index path.
        let mut idx = Index::new(2000).unwrap();
        let mut expected: std::collections::HashMap<[u8; 32], TxIndexEntry> =
            std::collections::HashMap::new();
        for i in 0..1000u64 {
            let key = make_key(i);
            let entry = make_entry(i * 11 + 4);
            idx.register(key, entry).unwrap();
            expected.insert(key.txid, entry);
        }
        let mut dah = DahIndex::new();
        dah.insert(123, make_key(1));
        dah.insert(456, make_key(2));
        let mut unmined = UnminedIndex::new();
        unmined.insert(789, make_key(3));
        idx.snapshot_all(&dah, &unmined, &path).unwrap();

        // Sanity: it really is a v1 file (TSIX magic).
        let head = std::fs::read(&path).unwrap();
        assert_eq!(
            &head[0..4],
            &crate::index::PRIMARY_SNAPSHOT_MAGIC,
            "fixture must be a v1 TSIX snapshot"
        );

        let (restored, rdah, runmined, flags) = ShardedIndex::restore_all(&path, 16).unwrap();
        assert!(!flags.dah_needs_rebuild && !flags.unmined_needs_rebuild);
        assert_eq!(restored.shard_count(), 16, "v1 load must target N=16");

        // Every v1 entry must be present in the correct N=16 shard.
        let after = collect_entries(&restored);
        assert_eq!(after, expected, "v1 entries must all load");
        for txid in expected.keys() {
            let key = TxKey { txid: *txid };
            assert!(
                restored.lookup(&key).is_some(),
                "v1 key {txid:?} missing after re-shard"
            );
        }
        assert_eq!(rdah.len(), 2, "v1 dah must survive");
        assert_eq!(runmined.len(), 1, "v1 unmined must survive");
    }

    /// Test 3b: the v1 magic is explicitly recognised by `restore_all` (it does
    /// not fall into the unknown-magic fail-closed branch).
    #[test]
    fn v1_magic_is_recognised() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v1-tiny.snap");

        let mut idx = Index::new(16).unwrap();
        idx.register(make_key(1), make_entry(64)).unwrap();
        idx.snapshot_all(&DahIndex::new(), &UnminedIndex::new(), &path)
            .unwrap();

        // Restoring at a single shard exercises the v1 recognition path
        // (target N=1 == clamp(1)); the result must NOT be an error.
        let result = ShardedIndex::restore_all(&path, 1);
        let (restored, _d, _u, _f) = result.expect("v1 snapshot must be recognised, not rejected");
        assert_eq!(restored.len(), 1, "the single v1 entry must load");
        assert!(restored.lookup(&make_key(1)).is_some());
    }

    /// Test 4: an unknown magic and an unsupported v2 version both fail closed
    /// with `FormatError` (never panic, never guess).
    #[test]
    fn restore_all_fails_closed_on_unknown_format() {
        let dir = tempfile::tempdir().unwrap();

        // (a) Garbage magic.
        let bad_path = dir.path().join("garbage.snap");
        std::fs::write(&bad_path, b"NOPEnot-a-real-snapshot-payload").unwrap();
        match ShardedIndex::restore_all(&bad_path, 16) {
            Err(IndexError::FormatError { detail }) => {
                assert!(
                    detail.contains("unknown index snapshot magic"),
                    "unexpected detail: {detail}"
                );
            }
            Err(other) => panic!("unknown magic must fail closed with FormatError, got {other:?}"),
            Ok(_) => panic!("unknown magic must not restore successfully"),
        }

        // (b) A real v2 snapshot whose version byte is bumped to an unsupported
        //     value (and CRC restamped so the version check is the gate).
        let v2_path = dir.path().join("v2-badver.snap");
        let sharded = ShardedIndex::new_in_memory(500, 4).unwrap();
        for i in 0..100u64 {
            sharded.register(make_key(i), make_entry(i * 5)).unwrap();
        }
        sharded
            .snapshot_all(
                &DahBackend::InMemory(DahIndex::new()),
                &UnminedBackend::InMemory(UnminedIndex::new()),
                &v2_path,
            )
            .unwrap();
        let mut data = std::fs::read(&v2_path).unwrap();
        // version lives at bytes [4..8]; set it to an unsupported value.
        data[4..8].copy_from_slice(&(V2_VERSION + 99).to_le_bytes());
        // Restamp the trailing CRC so the version check (not the CRC) is hit.
        let end = data.len() - 4;
        let crc = crc32fast::hash(&data[..end]);
        data[end..].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(&v2_path, &data).unwrap();

        match ShardedIndex::restore_all(&v2_path, 4) {
            Err(IndexError::FormatError { detail }) => {
                assert!(
                    detail.contains("unsupported v2 snapshot version"),
                    "unexpected detail: {detail}"
                );
            }
            Err(other) => panic!("unsupported v2 version must fail closed, got {other:?}"),
            Ok(_) => panic!("unsupported v2 version must not restore successfully"),
        }

        // (c) A v2 snapshot with a corrupted body byte must fail the trailing
        //     CRC (ChecksumMismatch), not panic.
        let v2_corrupt = dir.path().join("v2-corrupt.snap");
        sharded
            .snapshot_all(
                &DahBackend::InMemory(DahIndex::new()),
                &UnminedBackend::InMemory(UnminedIndex::new()),
                &v2_corrupt,
            )
            .unwrap();
        let mut data = std::fs::read(&v2_corrupt).unwrap();
        let mid = data.len() / 2;
        data[mid] ^= 0xFF;
        std::fs::write(&v2_corrupt, &data).unwrap();
        match ShardedIndex::restore_all(&v2_corrupt, 4) {
            Err(IndexError::ChecksumMismatch { .. }) => {}
            Err(other) => panic!("corrupt v2 body must yield ChecksumMismatch, got {other:?}"),
            Ok(_) => panic!("corrupt v2 body must not restore successfully"),
        }
    }

    /// Test 5: the engine still runs at one shard and `snapshot_all` at N=1
    /// produces a v1 (`TSIX`) snapshot that round-trips through BOTH the
    /// unchanged `PrimaryBackend::restore_all` (the engine/checkpoint path) and
    /// the new `ShardedIndex::restore_all`.
    #[test]
    fn n1_snapshot_is_v1_and_roundtrips_both_paths() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("n1.snap");

        let sharded = ShardedIndex::from_single(PrimaryBackend::new_in_memory(2000).unwrap());
        assert_eq!(sharded.shard_count(), 1);
        for i in 0..500u64 {
            sharded
                .register(make_key(i), make_entry(i * 9 + 3))
                .unwrap();
        }
        let before = collect_entries(&sharded);

        let mut dah = DahIndex::new();
        dah.insert(11, make_key(1));
        let mut unmined = UnminedIndex::new();
        unmined.insert(22, make_key(2));
        sharded
            .snapshot_all(
                &DahBackend::InMemory(dah),
                &UnminedBackend::InMemory(unmined),
                &path,
            )
            .unwrap();

        // N=1 must be the v1 format so the engine/checkpoint path keeps reading.
        let head = std::fs::read(&path).unwrap();
        assert_eq!(
            &head[0..4],
            &crate::index::PRIMARY_SNAPSHOT_MAGIC,
            "N=1 snapshot must be v1 TSIX"
        );

        // Path A: the unchanged PrimaryBackend restore.
        let (pb, pb_dah, pb_unmined, _flags) = PrimaryBackend::restore_all(&path).unwrap();
        assert_eq!(
            pb.len(),
            500,
            "PrimaryBackend::restore_all must read N=1 v1"
        );
        assert_eq!(pb_dah.len(), 1);
        assert_eq!(pb_unmined.len(), 1);
        for txid in before.keys() {
            assert!(
                pb.lookup(&TxKey { txid: *txid }).is_some(),
                "PrimaryBackend restore missing key {txid:?}"
            );
        }

        // Path B: the sharded restore (v1 recognition → re-shard at target).
        let (restored, _d, _u, _f) = ShardedIndex::restore_all(&path, 1).unwrap();
        let after = collect_entries(&restored);
        assert_eq!(after, before, "ShardedIndex restore of N=1 v1 must match");
    }

    // -----------------------------------------------------------------------
    // Task 6 tests: rebuild_in_memory (TDD — written before implementation)
    // -----------------------------------------------------------------------

    /// Set up a `MemoryDevice` populated with `record_count` valid records,
    /// mirroring the pattern used in `index::mod::setup_device_with_records`.
    ///
    /// Returns the populated device, the corresponding `SlotAllocator`, and a
    /// `Vec<(TxKey, u64)>` of `(key, record_offset)` pairs.
    fn setup_device_for_rebuild(
        record_count: usize,
    ) -> (
        std::sync::Arc<crate::device::MemoryDevice>,
        crate::allocator::SlotAllocator,
        Vec<(TxKey, u64)>,
    ) {
        use crate::io::write_full_record;
        use crate::record::{TxMetadata, UtxoSlot};

        let dev =
            std::sync::Arc::new(crate::device::MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let mut alloc = crate::allocator::SlotAllocator::new(dev.clone()).unwrap();
        let mut records = Vec::with_capacity(record_count);

        for i in 0..record_count {
            let mut meta = TxMetadata::new(5);
            let mut txid = [0u8; 32];
            // Vary [0..8] for record identity
            txid[0..8].copy_from_slice(&(i as u64).to_le_bytes());
            txid[8..16]
                .copy_from_slice(&((i as u64).wrapping_mul(0x9E3779B97F4A7C15)).to_le_bytes());
            // Vary [24..32] so keys spread across index shards
            txid[24..32]
                .copy_from_slice(&((i as u64).wrapping_mul(0x517C_C1B7_2722_0A95)).to_le_bytes());
            meta.tx_id = txid;

            let record_size = TxMetadata::record_size_for(5);
            let offset = alloc.allocate(record_size).unwrap();

            let slots: Vec<UtxoSlot> = (0..5)
                .map(|s| {
                    let mut h = [0u8; 32];
                    h[0] = s;
                    UtxoSlot::new_unspent(h)
                })
                .collect();

            write_full_record(&*dev, offset, &meta, &slots).unwrap();
            records.push((TxKey { txid }, offset));
        }

        (dev, alloc, records)
    }

    /// Test 1 (Task 6): `rebuild_in_memory` round-trips — every key written to
    /// the device is present in the rebuilt `ShardedIndex`, lives in the shard
    /// that `index_shard_for_key` selects, and the `record_offset` matches.
    /// Results also agree with a `PrimaryBackend::rebuild` oracle.
    #[test]
    fn rebuild_in_memory_roundtrip() {
        let (dev, alloc, records) = setup_device_for_rebuild(50);

        let sharded = ShardedIndex::rebuild_in_memory(&*dev, &alloc, 16).unwrap();
        assert_eq!(sharded.shard_count(), 16);
        assert_eq!(
            sharded.len(),
            records.len(),
            "rebuilt entry count must match records written"
        );

        // Build a single-backend oracle for comparison
        let oracle = PrimaryBackend::rebuild(&*dev, &alloc).unwrap();
        assert_eq!(
            oracle.len(),
            records.len(),
            "oracle must also have all records"
        );

        for (key, expected_offset) in &records {
            // Entry is present
            let entry = sharded
                .lookup(key)
                .unwrap_or_else(|| panic!("key {:?} not found in rebuilt ShardedIndex", key.txid));

            // record_offset matches what was written
            assert_eq!(
                entry.record_offset, *expected_offset,
                "record_offset mismatch for key {:?}",
                key.txid
            );

            // The entry lives in the shard `index_shard_for_key` selects
            let expected_shard = sharded.index_shard_for_key(key);
            let guard = sharded.shards[expected_shard].read();
            assert!(
                guard.lookup(key).is_some(),
                "key {:?} not found in its expected shard {}",
                key.txid,
                expected_shard
            );

            // Entry matches the oracle
            let oracle_entry = oracle
                .lookup(key)
                .unwrap_or_else(|| panic!("oracle missing key {:?}", key.txid));
            assert_eq!(
                entry.record_offset, oracle_entry.record_offset,
                "record_offset disagrees with oracle for key {:?}",
                key.txid
            );
            assert_eq!(
                entry.utxo_count, oracle_entry.utxo_count,
                "utxo_count disagrees with oracle for key {:?}",
                key.txid
            );
        }
    }

    /// Test 2 (Task 6): empty device → `rebuild_in_memory` succeeds with N=16,
    /// all shards empty, no panic.
    #[test]
    fn rebuild_in_memory_empty_device() {
        let dev =
            std::sync::Arc::new(crate::device::MemoryDevice::new(4 * 1024 * 1024, 4096).unwrap());
        let alloc = crate::allocator::SlotAllocator::new(dev.clone()).unwrap();

        let result = ShardedIndex::rebuild_in_memory(&*dev, &alloc, 16);
        let sharded = result.expect("rebuild_in_memory on empty device must succeed");

        assert_eq!(sharded.shard_count(), 16, "must produce 16 shards");
        assert_eq!(sharded.len(), 0, "empty device → empty index");
        assert!(sharded.is_empty(), "is_empty must be true");
        // Verify every individual shard is empty
        for (i, shard) in sharded.shards.iter().enumerate() {
            let guard = shard.read();
            assert_eq!(guard.len(), 0, "shard {i} must be empty");
        }
    }
}
