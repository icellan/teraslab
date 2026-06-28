//! Sharded secondary indexes (DAH and unmined).
//!
//! The two secondary indexes — [`DahBackend`] (delete-at-height) and
//! [`UnminedBackend`] (`unmined_since`) — were each guarded by a single global
//! `parking_lot::Mutex` in the engine. Under high concurrency every `Create`
//! (→ unmined), `Spend` (→ dah) and `SetMined` (→ both) serialised on those two
//! mutexes; an A/B test showed the unmined mutex alone cost ~21 % of throughput.
//!
//! [`ShardedSecondary`] removes that contention by spreading the key space
//! across `N` independent backends, each behind its own `parking_lot::Mutex`,
//! mirroring the primary [`crate::index::ShardedIndex`]. Concurrent mutations on
//! different keys hit independent shard locks.
//!
//! # Shard selection
//!
//! Routing reuses the SAME hashing as the primary
//! ([`crate::index::sharded::shard_for_key`]: SplitMix64 finaliser over txid
//! bytes `[24..32]`, masked by `shard_count - 1`) under the SAME process seed
//! ([`crate::index::sharded::index_shard_seed`]). A given key therefore maps to
//! the same shard NUMBER in the primary and both secondaries — which keeps the
//! cross-subsystem lock-order reasoning simple: the per-key secondary shard lock
//! is taken while the engine already holds the primary key's shard lock, but the
//! two are different lock sets, so no deadlock cycle exists.
//!
//! # Which variant is sharded
//!
//! Only the IN-MEMORY backend is sharded. The redb / on-disk backend is
//! self-durable and manages its own concurrency, so it stays single-shard
//! (`from_single`), exactly like the primary's redb path.
//!
//! # Ordering of `range_query`
//!
//! At `N > 1` shards [`ShardedSecondary::range_query`] concatenates each shard's
//! `range_query` output, so the result is NOT globally height-ordered. This is
//! deliberate and safe: the pruner consumers re-validate every candidate against
//! the authoritative on-device metadata and collect unordered, so they neither
//! rely on nor promise a global order.

use parking_lot::Mutex;

use crate::index::dah_index::DahRedoEntry;
use crate::index::secondary_backend::{DahBackend, UnminedBackend};
use crate::index::unmined_index::UnminedRedoEntry;
use crate::index::{IndexError, TxKey};
use crate::redo::RedoLog;

/// Behaviour shared by the two secondary backends so [`ShardedSecondary`] can
/// drive them generically.
///
/// Implemented for [`DahBackend`] and [`UnminedBackend`]; both expose identical
/// `insert` / `remove` / `range_query` / `len` / `clear` / `flush_durable`
/// signatures and differ only in their redo-entry type (routed by `txid`).
pub trait SecondaryBackend: Send + Sync + 'static {
    /// The redo-entry type replayed by [`Self::replay_redo`].
    type RedoEntry;

    /// Insert `key` at `height` with two-phase durability (the redo log, when
    /// `Some`, is the on-disk variant's intent journal — a no-op for in-memory).
    fn insert(
        &mut self,
        height: u32,
        key: TxKey,
        redo_log: Option<&Mutex<RedoLog>>,
    ) -> Result<(), IndexError>;

    /// Remove `key` with two-phase durability. No-op if `key` is absent.
    fn remove(&mut self, key: &TxKey, redo_log: Option<&Mutex<RedoLog>>) -> Result<(), IndexError>;

    /// Replay a redo entry idempotently.
    fn replay_redo(&mut self, entry: &Self::RedoEntry) -> Result<(), IndexError>;

    /// The `TxKey` a redo entry routes to (its `txid`), used to pick the shard.
    fn redo_entry_key(entry: &Self::RedoEntry) -> TxKey;

    /// All txids whose height is in `[0, cutoff]`.
    fn range_query(&self, cutoff: u32) -> Vec<TxKey>;

    /// Number of entries.
    fn len(&self) -> usize;

    /// Whether the backend is empty.
    fn is_empty(&self) -> bool;

    /// Remove all entries.
    fn clear(&mut self) -> Result<(), IndexError>;

    /// Force backend state durable on its own storage.
    fn flush_durable(&self) -> Result<(), IndexError>;

    /// Snapshot every `(height, key)` pair (for the unsharded snapshot section
    /// and for re-sharding a single backend into N shards at construction).
    fn collect_pairs(&self) -> Vec<(u32, TxKey)>;

    /// Whether this backend is the in-memory variant (the only one that is
    /// sharded; the redb variant stays single-shard).
    fn is_in_memory(&self) -> bool;

    /// Build a fresh empty in-memory backend of this type.
    fn new_in_memory() -> Self;

    /// Infallible insert used ONLY to re-shard a single in-memory backend into
    /// `N` in-memory shards at construction ([`ShardedSecondary::shard_in_memory`]).
    ///
    /// Always called on a freshly built [`Self::new_in_memory`] shard, whose
    /// inner insert is infallible (no redo journal, no redb commit). The
    /// non-in-memory arm is unreachable — `shard_in_memory` only builds in-memory
    /// shards — so it logs and drops (panic-free per project rules) rather than
    /// existing as a fallible path that the infallible constructor would have to
    /// `unwrap`.
    fn reshard_insert(&mut self, height: u32, key: TxKey);
}

impl SecondaryBackend for DahBackend {
    type RedoEntry = DahRedoEntry;

    fn insert(
        &mut self,
        height: u32,
        key: TxKey,
        redo_log: Option<&Mutex<RedoLog>>,
    ) -> Result<(), IndexError> {
        DahBackend::insert(self, height, key, redo_log)
    }

    fn remove(&mut self, key: &TxKey, redo_log: Option<&Mutex<RedoLog>>) -> Result<(), IndexError> {
        DahBackend::remove(self, key, redo_log)
    }

    fn replay_redo(&mut self, entry: &Self::RedoEntry) -> Result<(), IndexError> {
        DahBackend::replay_redo(self, entry)
    }

    fn redo_entry_key(entry: &Self::RedoEntry) -> TxKey {
        TxKey { txid: entry.txid }
    }

    fn range_query(&self, cutoff: u32) -> Vec<TxKey> {
        DahBackend::range_query(self, cutoff)
    }

    fn len(&self) -> usize {
        DahBackend::len(self)
    }

    fn is_empty(&self) -> bool {
        DahBackend::is_empty(self)
    }

    fn clear(&mut self) -> Result<(), IndexError> {
        DahBackend::clear(self)
    }

    fn flush_durable(&self) -> Result<(), IndexError> {
        DahBackend::flush_durable(self)
    }

    fn collect_pairs(&self) -> Vec<(u32, TxKey)> {
        self.iter().collect()
    }

    fn is_in_memory(&self) -> bool {
        matches!(self, DahBackend::InMemory(_))
    }

    fn new_in_memory() -> Self {
        DahBackend::new_in_memory()
    }

    fn reshard_insert(&mut self, height: u32, key: TxKey) {
        match self {
            DahBackend::InMemory(idx) => idx.insert(height, key),
            // Unreachable: `shard_in_memory` only builds in-memory shards.
            DahBackend::OnDisk(_) => {
                tracing::error!(
                    target: "teraslab::index",
                    "reshard_insert reached the redb DAH arm; this is unreachable and the entry was dropped",
                );
            }
        }
    }
}

impl SecondaryBackend for UnminedBackend {
    type RedoEntry = UnminedRedoEntry;

    fn insert(
        &mut self,
        height: u32,
        key: TxKey,
        redo_log: Option<&Mutex<RedoLog>>,
    ) -> Result<(), IndexError> {
        UnminedBackend::insert(self, height, key, redo_log)
    }

    fn remove(&mut self, key: &TxKey, redo_log: Option<&Mutex<RedoLog>>) -> Result<(), IndexError> {
        UnminedBackend::remove(self, key, redo_log)
    }

    fn replay_redo(&mut self, entry: &Self::RedoEntry) -> Result<(), IndexError> {
        UnminedBackend::replay_redo(self, entry)
    }

    fn redo_entry_key(entry: &Self::RedoEntry) -> TxKey {
        TxKey { txid: entry.txid }
    }

    fn range_query(&self, cutoff: u32) -> Vec<TxKey> {
        UnminedBackend::range_query(self, cutoff)
    }

    fn len(&self) -> usize {
        UnminedBackend::len(self)
    }

    fn is_empty(&self) -> bool {
        UnminedBackend::is_empty(self)
    }

    fn clear(&mut self) -> Result<(), IndexError> {
        UnminedBackend::clear(self)
    }

    fn flush_durable(&self) -> Result<(), IndexError> {
        UnminedBackend::flush_durable(self)
    }

    fn collect_pairs(&self) -> Vec<(u32, TxKey)> {
        self.iter().collect()
    }

    fn is_in_memory(&self) -> bool {
        matches!(self, UnminedBackend::InMemory(_))
    }

    fn new_in_memory() -> Self {
        UnminedBackend::new_in_memory()
    }

    fn reshard_insert(&mut self, height: u32, key: TxKey) {
        match self {
            UnminedBackend::InMemory(idx) => {
                idx.insert(height, key);
            }
            // Unreachable: `shard_in_memory` only builds in-memory shards.
            UnminedBackend::OnDisk(_) => {
                tracing::error!(
                    target: "teraslab::index",
                    "reshard_insert reached the redb unmined arm; this is unreachable and the entry was dropped",
                );
            }
        }
    }
}

/// A sharded secondary index.
///
/// Spreads the key space across `shards.len()` independent backends, each behind
/// its own [`parking_lot::Mutex`]. Routing reuses the primary's seed + hashing
/// so a key maps to the same shard number in the primary and both secondaries.
///
/// # Thread safety
///
/// `Send + Sync`. All mutation methods take `&self` and lock only the shard that
/// owns the key, so a single `Arc<ShardedSecondary<_>>` can be shared across
/// threads with disjoint keys never contending.
pub struct ShardedSecondary<B> {
    shards: Vec<Mutex<B>>,
    seed: u64,
}

/// Sharded DAH (delete-at-height) secondary index.
pub type ShardedDahIndex = ShardedSecondary<DahBackend>;

/// Sharded `unmined_since` secondary index.
pub type ShardedUnminedIndex = ShardedSecondary<UnminedBackend>;

impl<B: SecondaryBackend> ShardedSecondary<B> {
    /// Wrap a single backend as a one-shard `ShardedSecondary` (transparent
    /// pass-through).
    ///
    /// Used for the redb / on-disk variant (which stays single-shard) and for
    /// every existing single-backend constructor: with one shard the routing
    /// mask is `0`, so behaviour is byte-for-byte identical to the wrapped
    /// backend behind a plain `Mutex`. The process seed is installed but never
    /// affects routing at one shard.
    pub fn from_single(backend: B) -> Self {
        Self {
            shards: vec![Mutex::new(backend)],
            seed: crate::index::sharded::index_shard_seed(),
        }
    }

    /// Build an `N`-shard in-memory secondary, sharding `single`'s existing
    /// entries across the shards by their key.
    ///
    /// `shard_count` is clamped to a power of two in `[1, 256]` (matching the
    /// primary's `clamp_shard_count`) so the routing bitmask is always valid.
    ///
    /// If `single` is NOT the in-memory variant (i.e. redb / on-disk), or the
    /// clamped count is `1`, this is equivalent to [`Self::from_single`]: the
    /// on-disk backend is self-durable and manages its own concurrency, so it is
    /// never split. Otherwise `single`'s `(height, key)` pairs are drained into
    /// the shard each key routes to.
    ///
    /// Infallible: the multi-shard path only ever builds in-memory shards (whose
    /// insert is infallible — no redo journal, no redb commit), and the redb
    /// path takes the non-inserting [`Self::from_single`] branch.
    pub fn shard_in_memory(single: B, shard_count: usize) -> Self {
        let count = clamp_shard_count(shard_count);
        if count == 1 || !single.is_in_memory() {
            return Self::from_single(single);
        }
        let seed = crate::index::sharded::index_shard_seed();
        let mut shards: Vec<Mutex<B>> = Vec::with_capacity(count);
        for _ in 0..count {
            shards.push(Mutex::new(B::new_in_memory()));
        }
        let sharded = Self { shards, seed };
        for (height, key) in single.collect_pairs() {
            // Route by key into the owning in-memory shard via the infallible
            // re-shard insert (no redo log — the in-memory variant ignores it).
            let idx = sharded.shard_for(&key);
            sharded.shards[idx].lock().reshard_insert(height, key);
        }
        sharded
    }

    /// Number of shards.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// The shard index that owns `key` (same seed + hashing as the primary).
    #[inline]
    fn shard_for(&self, key: &TxKey) -> usize {
        crate::index::sharded::shard_for_key(self.seed, key, self.shards.len())
    }

    // -----------------------------------------------------------------------
    // Hot path — lock ONE shard by key
    // -----------------------------------------------------------------------

    /// Insert `key` at `height` into its shard with two-phase durability.
    ///
    /// Locks only the shard owning `key`. The `redo_log` is the on-disk
    /// variant's intent journal (a no-op for in-memory shards).
    ///
    /// # Errors
    ///
    /// Propagates [`IndexError`] from the backend (redo flush / redb commit).
    pub fn insert(
        &self,
        height: u32,
        key: TxKey,
        redo_log: Option<&Mutex<RedoLog>>,
    ) -> Result<(), IndexError> {
        let idx = self.shard_for(&key);
        self.shards[idx].lock().insert(height, key, redo_log)
    }

    /// Remove `key` from its shard with two-phase durability. No-op if absent.
    ///
    /// Locks only the shard owning `key`.
    ///
    /// # Errors
    ///
    /// Propagates [`IndexError`] from the backend (redo flush / redb commit).
    pub fn remove(&self, key: &TxKey, redo_log: Option<&Mutex<RedoLog>>) -> Result<(), IndexError> {
        let idx = self.shard_for(key);
        self.shards[idx].lock().remove(key, redo_log)
    }

    /// Replay a redo entry idempotently into the shard its `txid` routes to.
    ///
    /// # Errors
    ///
    /// Propagates [`IndexError`] from the backend.
    pub fn replay_redo(&self, entry: &B::RedoEntry) -> Result<(), IndexError> {
        let key = B::redo_entry_key(entry);
        let idx = self.shard_for(&key);
        self.shards[idx].lock().replay_redo(entry)
    }

    // -----------------------------------------------------------------------
    // Cold path — fan out over ALL shards
    // -----------------------------------------------------------------------

    /// All txids whose height is in `[0, cutoff]`, across all shards.
    ///
    /// Concatenates each shard's `range_query`. The result is NOT globally
    /// height-ordered at `N > 1` shards (see the module docs); the pruner
    /// consumers re-validate against authoritative metadata and collect
    /// unordered, so order does not matter.
    pub fn range_query(&self, cutoff: u32) -> Vec<TxKey> {
        let mut out = Vec::new();
        for shard in &self.shards {
            out.extend(shard.lock().range_query(cutoff));
        }
        out
    }

    /// Total number of entries across all shards.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.lock().len()).sum()
    }

    /// Whether every shard is empty.
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|s| s.lock().is_empty())
    }

    /// Remove all entries from every shard.
    ///
    /// Attempts EVERY shard and returns the FIRST error encountered (after all
    /// shards were attempted), so a `clear` is never silently partial.
    ///
    /// # Errors
    ///
    /// Returns the first [`IndexError`] from any shard.
    pub fn clear(&self) -> Result<(), IndexError> {
        let mut first_err: Option<IndexError> = None;
        for shard in &self.shards {
            if let Err(e) = shard.lock().clear() {
                first_err.get_or_insert(e);
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Force every shard durable on its own storage.
    ///
    /// Attempts EVERY shard and returns the FIRST error (after all were
    /// attempted), so a durability flush is never silently partial.
    ///
    /// # Errors
    ///
    /// Returns the first [`IndexError`] from any shard.
    pub fn flush_durable(&self) -> Result<(), IndexError> {
        let mut first_err: Option<IndexError> = None;
        for shard in &self.shards {
            if let Err(e) = shard.lock().flush_durable() {
                first_err.get_or_insert(e);
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Collect every `(height, key)` pair across all shards.
    ///
    /// Used by the v2 snapshot path to serialise the full (unsharded) secondary
    /// section: each shard is locked in turn, its pairs appended, and the lock
    /// released before the next shard. Order across shards is unspecified —
    /// matching the unordered `range_query` contract.
    pub fn collect_pairs(&self) -> Vec<(u32, TxKey)> {
        let mut out = Vec::new();
        for shard in &self.shards {
            out.extend(shard.lock().collect_pairs());
        }
        out
    }

    /// Whether EVERY shard is the in-memory variant.
    ///
    /// The checkpoint snapshot path
    /// ([`crate::index::ShardedIndex::snapshot_all_concurrent`]) uses this to
    /// reproduce the prior "in-memory only" contract: a redb secondary is
    /// self-durable and never participates in the in-memory snapshot file.
    pub fn all_in_memory(&self) -> bool {
        self.shards.iter().all(|s| s.lock().is_in_memory())
    }

    /// Lock shard `0`.
    ///
    /// Only meaningful at `shard_count() == 1` (the single-shard pass-through),
    /// where it yields the one backend behind the wrapper — used by the v1
    /// checkpoint snapshot path, which needs `&B` to delegate to the existing
    /// single-backend `snapshot_all`. The caller guarantees one shard.
    pub fn lock_shard0(&self) -> parking_lot::MutexGuard<'_, B> {
        self.shards[0].lock()
    }
}

/// Round `n` up to the nearest power of two, clamped to `[1, 256]`.
///
/// Mirrors `crate::index::sharded::clamp_shard_count` so the secondary shard
/// count tracks the primary's exactly.
fn clamp_shard_count(n: usize) -> usize {
    let n = n.clamp(1, 256);
    n.next_power_of_two().min(256)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;

    /// Deterministic txid varying across ALL bytes so the shard-routing bytes
    /// `[24..32]` spread keys across shards.
    fn key(n: u64) -> TxKey {
        let mut txid = [0u8; 32];
        txid[0..8].copy_from_slice(&n.to_le_bytes());
        txid[8..16].copy_from_slice(&n.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes());
        txid[16..24].copy_from_slice(&n.wrapping_mul(0xD1B5_4A32_D192_ED03).to_le_bytes());
        txid[24..32].copy_from_slice(&n.wrapping_mul(0xC2B2_AE3D_27D4_EB4F).to_le_bytes());
        TxKey { txid }
    }

    // -----------------------------------------------------------------------
    // 1. Round-trip vs a single reference backend (both backend types)
    // -----------------------------------------------------------------------

    #[test]
    fn sharded_unmined_roundtrips_single_backend() {
        const N: u64 = 2000;
        let sharded: ShardedUnminedIndex =
            ShardedSecondary::shard_in_memory(UnminedBackend::new_in_memory(), 16);
        let mut reference = UnminedBackend::new_in_memory();

        for i in 0..N {
            let h = (i as u32 % 500) + 1; // varied heights, all non-zero
            sharded.insert(h, key(i), None).unwrap();
            reference.insert(h, key(i), None).unwrap();
        }

        assert!(sharded.shard_count() > 1, "must actually shard (N>1)");
        assert_eq!(
            sharded.len(),
            reference.len(),
            "sharded len must equal reference len",
        );
        assert_eq!(sharded.len(), N as usize);

        // Every key present at u32::MAX cutoff in both.
        let sharded_all: HashSet<TxKey> = sharded.range_query(u32::MAX).into_iter().collect();
        let ref_all: HashSet<TxKey> = reference.range_query(u32::MAX).into_iter().collect();
        assert_eq!(sharded_all, ref_all);
        for i in 0..N {
            assert!(sharded_all.contains(&key(i)), "missing key {i}");
        }
    }

    #[test]
    fn sharded_dah_roundtrips_single_backend() {
        const N: u64 = 2000;
        let sharded: ShardedDahIndex =
            ShardedSecondary::shard_in_memory(DahBackend::new_in_memory(), 16);
        let mut reference = DahBackend::new_in_memory();

        for i in 0..N {
            let h = (i as u32 % 500) + 1;
            sharded.insert(h, key(i), None).unwrap();
            reference.insert(h, key(i), None).unwrap();
        }

        assert!(sharded.shard_count() > 1);
        assert_eq!(sharded.len(), reference.len());
        assert_eq!(sharded.len(), N as usize);

        let sharded_all: HashSet<TxKey> = sharded.range_query(u32::MAX).into_iter().collect();
        let ref_all: HashSet<TxKey> = reference.range_query(u32::MAX).into_iter().collect();
        assert_eq!(sharded_all, ref_all);
        for i in 0..N {
            assert!(sharded_all.contains(&key(i)), "missing key {i}");
        }
    }

    // -----------------------------------------------------------------------
    // 2. range_query SET equals the single backend's for randomized inputs +
    //    cutoffs, including a height UPDATE (catches by_txid/by_height drift)
    // -----------------------------------------------------------------------

    #[test]
    fn range_query_set_matches_single_backend() {
        const N: u64 = 3000;
        let sharded: ShardedDahIndex =
            ShardedSecondary::shard_in_memory(DahBackend::new_in_memory(), 32);
        let mut reference = DahBackend::new_in_memory();

        // Pseudo-random heights in [1, 1000] from a SplitMix-ish mix of i.
        let height_of = |i: u64| -> u32 { ((i.wrapping_mul(2654435761) % 1000) as u32) + 1 };

        for i in 0..N {
            let h = height_of(i);
            sharded.insert(h, key(i), None).unwrap();
            reference.insert(h, key(i), None).unwrap();
        }

        // Height UPDATE: re-insert a subset of keys at a NEW height. The DAH
        // index must move them in BOTH by_height and by_txid; a sharded bug that
        // left a stale bucket entry would show up as a membership mismatch.
        for i in (0..N).step_by(7) {
            let new_h = height_of(i).wrapping_add(500).max(1);
            sharded.insert(new_h, key(i), None).unwrap();
            reference.insert(new_h, key(i), None).unwrap();
        }

        let mid = 600u32;
        for cutoff in [0u32, mid, u32::MAX] {
            let sharded_set: HashSet<TxKey> = sharded.range_query(cutoff).into_iter().collect();
            let ref_set: HashSet<TxKey> = reference.range_query(cutoff).into_iter().collect();
            assert_eq!(
                sharded_set, ref_set,
                "range_query({cutoff}) membership must match the single backend",
            );
        }

        // Total membership identical and length equal (no double-count from the
        // updates, no dropped keys).
        assert_eq!(sharded.len(), reference.len());
        assert_eq!(sharded.len(), N as usize);
    }

    // -----------------------------------------------------------------------
    // 3. Concurrent inserts of distinct keys lose nothing
    // -----------------------------------------------------------------------

    #[test]
    fn concurrent_inserts_lose_nothing() {
        const THREADS: u64 = 16;
        const PER_THREAD: u64 = 4096;
        let sharded: Arc<ShardedUnminedIndex> = Arc::new(ShardedSecondary::shard_in_memory(
            UnminedBackend::new_in_memory(),
            16,
        ));

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let idx = Arc::clone(&sharded);
                std::thread::spawn(move || {
                    for j in 0..PER_THREAD {
                        let global = t * PER_THREAD + j;
                        let h = (global as u32 % 1000) + 1;
                        idx.insert(h, key(global), None).unwrap();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().expect("worker thread panicked");
        }

        let expected = (THREADS * PER_THREAD) as usize;
        assert_eq!(sharded.len(), expected, "no insert may be lost");
        assert_eq!(
            sharded.range_query(u32::MAX).len(),
            expected,
            "range_query must surface every inserted key",
        );
        // Spot-check membership across the whole key range.
        let all: HashSet<TxKey> = sharded.range_query(u32::MAX).into_iter().collect();
        for g in (0..THREADS * PER_THREAD).step_by(101) {
            assert!(
                all.contains(&key(g)),
                "missing concurrently-inserted key {g}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Extra: remove + clear + collect_pairs fan-out, and the on-disk variant
    // stays single-shard.
    // -----------------------------------------------------------------------

    #[test]
    fn remove_routes_to_correct_shard() {
        let sharded: ShardedDahIndex =
            ShardedSecondary::shard_in_memory(DahBackend::new_in_memory(), 16);
        for i in 0..500u64 {
            sharded.insert(100, key(i), None).unwrap();
        }
        assert_eq!(sharded.len(), 500);
        for i in 0..250u64 {
            sharded.remove(&key(i), None).unwrap();
        }
        assert_eq!(sharded.len(), 250);
        let remaining: HashSet<TxKey> = sharded.range_query(u32::MAX).into_iter().collect();
        for i in 0..250u64 {
            assert!(!remaining.contains(&key(i)), "key {i} should be removed");
        }
        for i in 250..500u64 {
            assert!(remaining.contains(&key(i)), "key {i} should remain");
        }
    }

    #[test]
    fn clear_empties_all_shards() {
        let sharded: ShardedUnminedIndex =
            ShardedSecondary::shard_in_memory(UnminedBackend::new_in_memory(), 16);
        for i in 0..300u64 {
            sharded.insert(50, key(i), None).unwrap();
        }
        assert!(!sharded.is_empty());
        sharded.clear().unwrap();
        assert!(sharded.is_empty());
        assert_eq!(sharded.len(), 0);
        assert!(sharded.range_query(u32::MAX).is_empty());
    }

    #[test]
    fn collect_pairs_returns_full_union() {
        let sharded: ShardedDahIndex =
            ShardedSecondary::shard_in_memory(DahBackend::new_in_memory(), 16);
        let mut expected: HashSet<(u32, TxKey)> = HashSet::new();
        for i in 0..400u64 {
            let h = (i as u32 % 200) + 1;
            sharded.insert(h, key(i), None).unwrap();
            expected.insert((h, key(i)));
        }
        let got: HashSet<(u32, TxKey)> = sharded.collect_pairs().into_iter().collect();
        assert_eq!(got, expected);
    }

    #[test]
    fn shard_in_memory_preserves_existing_entries() {
        // Pre-populate a single backend, then shard it: every entry must survive
        // the re-sharding drain.
        let mut single = DahBackend::new_in_memory();
        for i in 0..600u64 {
            single.insert((i as u32 % 300) + 1, key(i), None).unwrap();
        }
        let sharded: ShardedDahIndex = ShardedSecondary::shard_in_memory(single, 16);
        assert!(sharded.shard_count() > 1);
        assert_eq!(sharded.len(), 600);
        let all: HashSet<TxKey> = sharded.range_query(u32::MAX).into_iter().collect();
        for i in 0..600u64 {
            assert!(all.contains(&key(i)), "re-sharded key {i} lost");
        }
    }

    #[test]
    fn on_disk_variant_stays_single_shard() {
        // The redb on-disk backend is self-durable and must NOT be split, even
        // when a multi-shard count is requested.
        let dir = tempfile::tempdir().unwrap();
        let redb = crate::index::redb_dah::RedbDahIndex::open(
            dir.path().join("dah.redb").as_path(),
            16 * 1024 * 1024,
        )
        .unwrap();
        let sharded: ShardedDahIndex =
            ShardedSecondary::shard_in_memory(DahBackend::OnDisk(redb), 64);
        assert_eq!(
            sharded.shard_count(),
            1,
            "on-disk backend must stay single-shard",
        );
        // Still functional through the wrapper.
        sharded.insert(100, key(1), None).unwrap();
        assert_eq!(sharded.len(), 1);
        assert_eq!(sharded.range_query(100), vec![key(1)]);
    }

    #[test]
    fn replay_redo_routes_by_txid() {
        let sharded: ShardedUnminedIndex =
            ShardedSecondary::shard_in_memory(UnminedBackend::new_in_memory(), 16);
        let entry = UnminedRedoEntry {
            txid: key(42).txid,
            old_height: 0,
            new_height: 700,
        };
        sharded.replay_redo(&entry).unwrap();
        assert_eq!(sharded.len(), 1);
        assert_eq!(sharded.range_query(700), vec![key(42)]);

        // Replaying the inverse removes it.
        let undo = UnminedRedoEntry {
            txid: key(42).txid,
            old_height: 700,
            new_height: 0,
        };
        sharded.replay_redo(&undo).unwrap();
        assert!(sharded.is_empty());
    }

    #[test]
    fn from_single_is_one_shard_passthrough() {
        let sharded: ShardedDahIndex = ShardedSecondary::from_single(DahBackend::new_in_memory());
        assert_eq!(sharded.shard_count(), 1);
        sharded.insert(10, key(1), None).unwrap();
        sharded.insert(20, key(2), None).unwrap();
        assert_eq!(sharded.len(), 2);
        assert_eq!(sharded.range_query(15), vec![key(1)]);
    }
}
