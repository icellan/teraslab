//! Per-transaction lock striping for concurrent access.
//!
//! Uses bytes 16–23 of the txid (different from index bucket and fingerprint
//! bytes) to select one of the configured mutex stripes. This ensures that concurrent
//! operations on different transactions do not contend.

use crate::index::TxKey;
use std::sync::OnceLock;

/// Process-wide random seed mixed into [`StripedLocks::stripe_index`]
/// (C-7).
///
/// # Why a seed
///
/// Before the seed existed, the lock stripe was selected from raw txid
/// bytes (`txid[16..24] & mask`). Because those bytes are fully
/// attacker-controlled, an adversary submitting transactions could grind
/// txids whose bytes 16–23 all collide modulo the stripe count, funnelling
/// every operation onto a single mutex and serialising the engine — a
/// targeted contention / DoS. The index hashtable already defends against
/// exactly this with a per-process seed (`bucket_index` in
/// `src/index/hashtable.rs`); the lock table did not, so it inherited the
/// weakness.
///
/// Mixing in a 64-bit process-local seed makes the stripe mapping
/// unpredictable to a remote attacker (who cannot observe the seed) while
/// staying perfectly deterministic *within* a process — a `lock(k)` always
/// lands on the same stripe as a prior `lock(k)`, which is all the lock
/// table requires for correctness (a stripe collision only costs
/// contention, never safety).
///
/// Unlike the hashtable seed this value is never persisted and has no
/// on-disk layout implications: lock striping is purely in-memory, so a
/// fresh random seed every process start is free of compatibility concerns.
pub(crate) fn stripe_seed() -> u64 {
    // Prefer the OS CSPRNG. Unlike the index hashtable — where a non-random
    // seed would silently re-enable a DoS and therefore justifies a panic — a
    // lock stripe collision only costs contention, never correctness. So on the
    // (essentially impossible) `getrandom` failure the shared initializer
    // (`crate::index::hashmix::init_process_seed`) falls back to a still
    // process-unpredictable `RandomState`-derived seed rather than aborting the
    // process. No `expect()` in library code.
    static SEED: OnceLock<u64> = OnceLock::new();
    *SEED.get_or_init(crate::index::hashmix::init_process_seed)
}

// ---------------------------------------------------------------------------
// Record-offset striped RwLock — used by the direct-pointer I/O path in
// `crate::io` to serialize writer↔reader access to a single record region.
//
// F-X-007 / BC-02: The direct-pointer write/read helpers in `crate::io`
// memcpy `TxMetadata` (320 bytes) or `UtxoSlot` (73 bytes) non-atomically.
// Without serialization a concurrent reader can observe a byte mix from
// two writes; CRC may coincidentally validate against the mix and a
// "torn" value silently reaches the caller (regression test:
// `io::tests::direct_read_write_concurrent_stress_never_returns_torn_data`).
// The CRC-as-only-defense story documented at `src/io.rs:206` was
// empirically false on aarch64 release builds.
//
// This lock table is keyed by `record_offset` (the only context the
// `_direct` helpers have) and provides read-shared / write-exclusive
// semantics, so concurrent readers on the same record still parallelize.
// Different records hash to different stripes (false-sharing only when
// two distinct offsets collide modulo the stripe count) so the fix
// preserves the contention profile the engine already has via
// `StripedLocks` on the write path.
// ---------------------------------------------------------------------------

/// Striped read/write lock table keyed by a `u64` (typically a record
/// offset). Used by the direct-pointer I/O helpers in `crate::io` to
/// give writers exclusive access to a record region and readers shared
/// access — closing the torn-read window the CRC alone cannot cover
/// (F-X-007).
pub struct StripedRwLocks {
    locks: Vec<parking_lot::RwLock<()>>,
    mask: usize,
}

impl StripedRwLocks {
    /// Create a lock table with `stripe_count` stripes (rounded up to
    /// the next power of two, with a floor of 16). The default for the
    /// I/O subsystem is 65_536 stripes — matching `StripedLocks`.
    pub fn new(stripe_count: usize) -> Self {
        let count = stripe_count.next_power_of_two().max(16);
        let locks = (0..count).map(|_| parking_lot::RwLock::new(())).collect();
        Self {
            locks,
            mask: count - 1,
        }
    }

    /// Compute which stripe a record offset maps to.
    ///
    /// Records are allocated at coarse alignment (4096-byte device
    /// alignment plus the record-size step), so the low bits carry no
    /// distribution. Shift the offset down before masking to keep the
    /// effective hash spread across stripes even on adjacent records.
    #[inline]
    pub fn stripe_index(&self, record_offset: u64) -> usize {
        // The allocator quantum is at least 4 KiB; lop those bits off
        // before masking so adjacent records hash to adjacent stripes.
        let scaled = (record_offset >> 12) as usize;
        scaled & self.mask
    }

    /// Acquire a shared (read) guard for the given record offset.
    #[inline]
    pub fn read(&self, record_offset: u64) -> parking_lot::RwLockReadGuard<'_, ()> {
        let idx = self.stripe_index(record_offset);
        self.locks[idx].read()
    }

    /// Acquire an exclusive (write) guard for the given record offset.
    #[inline]
    pub fn write(&self, record_offset: u64) -> parking_lot::RwLockWriteGuard<'_, ()> {
        let idx = self.stripe_index(record_offset);
        self.locks[idx].write()
    }

    /// Acquire an exclusive (write) guard for a specific stripe index.
    ///
    /// Used by a coalesced multi-record write that must hold the write guard
    /// for EVERY record offset it covers (torn-read safety vs a stale reader
    /// of a reused offset — F-X-007 / g2). The caller maps each offset to its
    /// stripe via [`Self::stripe_index`], deduplicates, sorts the indices, and
    /// acquires each unique stripe ONCE through this method — dedup is
    /// mandatory (the `RwLock` write side is not reentrant) and the sort gives
    /// a global order so concurrent multi-offset acquirers cannot deadlock.
    #[inline]
    pub fn write_index(&self, idx: usize) -> parking_lot::RwLockWriteGuard<'_, ()> {
        self.locks[idx].write()
    }

    /// Number of stripes in the lock table.
    pub fn stripe_count(&self) -> usize {
        self.locks.len()
    }
}

/// Striped lock table for per-transaction mutual exclusion.
///
/// Default: 65536 stripes. The stripe is chosen from a portion of the txid
/// that does not overlap with the index bucket or fingerprint bytes.
pub struct StripedLocks {
    locks: Vec<parking_lot::Mutex<()>>,
    mask: usize,
    /// Process-local seed mixed into [`Self::stripe_index`] so an attacker
    /// cannot grind txids to collide stripes (C-7).
    seed: u64,
}

impl StripedLocks {
    /// Create a lock table with `stripe_count` stripes (rounded up to power of 2).
    ///
    /// The stripe mapping is seeded with a process-wide random value
    /// (`stripe_seed`) so the txid→stripe assignment is unpredictable to
    /// a remote attacker while staying deterministic within the process.
    pub fn new(stripe_count: usize) -> Self {
        Self::with_seed(stripe_count, stripe_seed())
    }

    /// Create a lock table with an explicit `seed`. Exposed for tests that
    /// need to compare the mapping under two distinct seeds; production
    /// callers should use [`StripedLocks::new`].
    pub fn with_seed(stripe_count: usize, seed: u64) -> Self {
        let count = stripe_count.next_power_of_two().max(16);
        let locks = (0..count).map(|_| parking_lot::Mutex::new(())).collect();
        Self {
            locks,
            mask: count - 1,
            seed,
        }
    }

    /// Acquire the lock for the given key. Returns a RAII guard.
    pub fn lock(&self, key: &TxKey) -> parking_lot::MutexGuard<'_, ()> {
        let idx = self.stripe_index(key);
        self.locks[idx].lock()
    }

    /// Acquire the lock for a specific stripe index. Returns a RAII guard.
    ///
    /// Used by batch mutation paths (e.g. the spend-batch handler) that need
    /// to hold the stripe locks for many keys simultaneously across a single
    /// WAL flush. The caller first maps each key to its stripe via
    /// [`Self::stripe_index`], deduplicates, sorts the indices, and acquires
    /// each unique stripe ONCE through this method. Deduplication is mandatory:
    /// the per-stripe `Mutex` is not reentrant, so two keys that hash to the
    /// same stripe must share one guard or the second `lock_index` would
    /// self-deadlock. Sorting the unique indices gives a global acquisition
    /// order that prevents deadlock between concurrent batch acquirers.
    ///
    /// # Panics
    /// Panics if `idx >= stripe_count()`. Callers must pass an index produced
    /// by [`Self::stripe_index`], which is always masked into range.
    pub fn lock_index(&self, idx: usize) -> parking_lot::MutexGuard<'_, ()> {
        self.locks[idx].lock()
    }

    /// Compute which stripe a key maps to.
    ///
    /// Uses bytes 16–23 of the txid (different from bucket index [0–7] and
    /// fingerprint [8–15]) XOR-mixed with the per-process `seed`
    /// through a SplitMix64 finalizer (C-7). The mix both defeats
    /// txid-grinding stripe-collision attacks and improves distribution
    /// when operators configure more than 65,536 stripes (the raw bytes
    /// alone only spread across the low bits).
    pub fn stripe_index(&self, key: &TxKey) -> usize {
        // F-G1-018: the slice→array conversion lets the compiler emit a
        // single 8-byte load. `txid` is always 32 bytes so the 16..24
        // slice is statically 8 bytes; the `try_into` cannot fail. We map
        // the impossible error to a 0 fallback rather than `expect` to keep
        // library code panic-free — a 0 here would still be deterministic.
        let raw = u64::from_le_bytes(key.txid[16..24].try_into().unwrap_or([0u8; 8]));
        // SplitMix64 finalizer over (raw XOR seed): shared impl in
        // `crate::index::hashmix`, mirroring `hashtable::bucket_index`.
        let x = crate::index::hashmix::splitmix64_finalize(raw ^ self.seed);
        (x as usize) & self.mask
    }

    /// Number of stripes in the lock table.
    pub fn stripe_count(&self) -> usize {
        self.locks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_key(n: u64) -> TxKey {
        let mut txid = [0u8; 32];
        txid[0..8].copy_from_slice(&n.to_le_bytes());
        // Vary bytes 16-23 for stripe distribution
        txid[16..24].copy_from_slice(&n.to_le_bytes());
        TxKey { txid }
    }

    #[test]
    fn different_stripes_lock_simultaneously() {
        let locks = StripedLocks::new(65536);
        let k1 = make_key(1);
        let k2 = make_key(2);
        assert_ne!(locks.stripe_index(&k1), locks.stripe_index(&k2));

        let _g1 = locks.lock(&k1);
        let _g2 = locks.lock(&k2);
        // Both held simultaneously — no deadlock
    }

    #[test]
    fn same_key_exclusive() {
        use std::sync::Arc;

        let locks = Arc::new(StripedLocks::new(65536));
        let key = make_key(42);

        let held = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let contended = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let locks2 = locks.clone();
        let held2 = held.clone();
        let contended2 = contended.clone();

        let _g = locks.lock(&key);
        held.store(true, std::sync::atomic::Ordering::SeqCst);

        let handle = std::thread::spawn(move || {
            // This should block until the main thread releases
            contended2.store(true, std::sync::atomic::Ordering::SeqCst);
            let _g2 = locks2.lock(&key);
            assert!(held2.load(std::sync::atomic::Ordering::SeqCst));
        });

        // Give the thread a moment to try to acquire
        std::thread::sleep(std::time::Duration::from_millis(10));
        drop(_g); // Release
        handle.join().unwrap();
    }

    #[test]
    fn stripe_distribution() {
        let locks = StripedLocks::new(65536);
        let mut seen = std::collections::HashSet::new();
        for i in 0..1000u64 {
            let key = make_key(i);
            seen.insert(locks.stripe_index(&key));
        }
        // With 1000 distinct keys, should hit many different stripes
        assert!(
            seen.len() > 500,
            "poor stripe distribution: only {} distinct",
            seen.len()
        );
    }

    #[test]
    fn stripe_index_large_lock_count_uses_more_than_16_bits() {
        let locks = StripedLocks::new(1 << 17);
        let low = make_key(1);
        let high = make_key((1 << 16) + 1);

        assert_ne!(locks.stripe_index(&low), locks.stripe_index(&high));
    }

    // -- C-7: seeded stripe selection --

    #[test]
    fn stripe_selection_depends_on_seed() {
        // Two lock tables with different seeds must (for at least some
        // keys) map the same txid to different stripes — otherwise an
        // attacker who knows the raw-byte mapping could grind collisions
        // regardless of the seed. With 65,536 stripes over 256 keys a
        // seed change moves the overwhelming majority; we only require
        // that it moves *some*.
        let a = StripedLocks::with_seed(65536, 0x1111_1111_1111_1111);
        let b = StripedLocks::with_seed(65536, 0xEEEE_EEEE_EEEE_EEEE);
        let mut differing = 0usize;
        for i in 0..256u64 {
            let k = make_key(i);
            if a.stripe_index(&k) != b.stripe_index(&k) {
                differing += 1;
            }
        }
        assert!(
            differing > 200,
            "seed must change the stripe mapping for most keys; only {differing}/256 differed"
        );
    }

    #[test]
    fn stripe_selection_is_stable_within_a_seed() {
        // Determinism within a process: the same key+seed always lands on
        // the same stripe (required for lock correctness).
        let locks = StripedLocks::with_seed(65536, 0xABCD_1234_5678_9F00);
        let k = make_key(7);
        let first = locks.stripe_index(&k);
        for _ in 0..1000 {
            assert_eq!(locks.stripe_index(&k), first);
        }
    }

    #[test]
    fn raw_byte_collision_is_broken_by_seed() {
        // Two txids whose bytes 16..24 are IDENTICAL collide under the old
        // raw-byte scheme on EVERY stripe count. With the seed mix they
        // still collide with each other (same input bytes → same stripe),
        // which is correct — the seed defends against an attacker
        // *predicting* the mapping, not against genuine byte-equality.
        // This test pins the property that the mapping is a pure function
        // of (bytes16..24, seed): identical bytes map identically, and a
        // different seed relocates that shared stripe.
        let mut t1 = make_key(1);
        let mut t2 = make_key(2);
        // Force bytes 16..24 equal but other bytes different.
        t1.txid[16..24].copy_from_slice(&42u64.to_le_bytes());
        t2.txid[16..24].copy_from_slice(&42u64.to_le_bytes());

        let s1 = StripedLocks::with_seed(65536, 7);
        assert_eq!(
            s1.stripe_index(&t1),
            s1.stripe_index(&t2),
            "identical stripe bytes must map to the same stripe under one seed"
        );

        // The shared stripe should move under *some* other seed — the seed
        // relocates it. We scan several seeds to make the assertion
        // deterministic rather than relying on a single 1/65536 draw.
        let base = s1.stripe_index(&t1);
        let relocated =
            (9u64..30).any(|s| StripedLocks::with_seed(65536, s).stripe_index(&t1) != base);
        assert!(
            relocated,
            "a different seed must relocate the stripe for the same bytes"
        );
    }

    #[test]
    fn lock_then_drop_reacquire() {
        let locks = StripedLocks::new(65536);
        let key = make_key(99);
        {
            let _g = locks.lock(&key);
        }
        // Should succeed immediately after drop
        let _g2 = locks.lock(&key);
    }

    // -- StripedRwLocks tests --

    #[test]
    fn striped_rwlocks_stripe_count_is_power_of_two() {
        let locks = StripedRwLocks::new(65_536);
        assert_eq!(locks.stripe_count(), 65_536);
        let locks2 = StripedRwLocks::new(1000);
        // 1000 rounds up to 1024.
        assert_eq!(locks2.stripe_count(), 1024);
        // Floor of 16.
        let locks3 = StripedRwLocks::new(1);
        assert_eq!(locks3.stripe_count(), 16);
    }

    #[test]
    fn striped_rwlocks_records_in_same_4k_block_share_a_stripe() {
        // Packed-record coordination (PACKED_RECORD_STORAGE_DESIGN.md §3.2): the
        // stripe index shifts off the low 12 bits (4096) before masking, so any
        // two record offsets within the SAME 4 KiB block map to the SAME stripe
        // and therefore serialize their read-modify-write. Combined with the
        // allocator's no-straddle invariant (no record crosses a 4 KiB block),
        // this is exactly the block-level mutual exclusion packed neighbours
        // need — no per-call-site rekey required. Holds for device alignments
        // <= 4096 (the standard); larger alignments must not enable packing.
        let locks = StripedRwLocks::new(65_536);
        let block_base = 0x40_000u64; // some 4 KiB-aligned block
        let s = locks.stripe_index(block_base);
        // Several packed records within the block (offsets < 4096 apart).
        for intra in [0u64, 8, 320, 393, 700, 1400, 2048, 4000, 4095] {
            assert_eq!(
                locks.stripe_index(block_base + intra),
                s,
                "offset {intra} within the block must share the block's stripe",
            );
        }
        // The next 4 KiB block maps to a different stripe (adjacent block ->
        // adjacent stripe, distinct here), so disjoint blocks don't needlessly
        // serialize.
        assert_ne!(
            locks.stripe_index(block_base + 4096),
            s,
            "the next 4 KiB block must map to a different stripe",
        );
    }

    #[test]
    fn striped_rwlocks_concurrent_readers_do_not_block() {
        let locks = StripedRwLocks::new(64);
        let _g1 = locks.read(0x10_000);
        let _g2 = locks.read(0x10_000);
        // Two readers on the same stripe must coexist.
    }

    #[test]
    fn striped_rwlocks_writer_excludes_readers() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::thread;
        use std::time::Duration;

        let locks = Arc::new(StripedRwLocks::new(64));
        let writer_holding = Arc::new(AtomicBool::new(false));
        let reader_observed_write = Arc::new(AtomicBool::new(false));

        let l = locks.clone();
        let wh = writer_holding.clone();
        let wt = thread::spawn(move || {
            let _g = l.write(0x20_000);
            wh.store(true, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(20));
            wh.store(false, Ordering::SeqCst);
        });

        // Give the writer a chance to acquire.
        thread::sleep(Duration::from_millis(5));

        let l2 = locks.clone();
        let wh2 = writer_holding.clone();
        let row = reader_observed_write.clone();
        let rt = thread::spawn(move || {
            let _g = l2.read(0x20_000);
            // Once we acquire the read guard the writer must have
            // released — observe the flag inside the guard.
            if !wh2.load(Ordering::SeqCst) {
                row.store(true, Ordering::SeqCst);
            }
        });

        wt.join().unwrap();
        rt.join().unwrap();
        assert!(
            reader_observed_write.load(Ordering::SeqCst),
            "reader must wait for writer to release"
        );
    }

    #[test]
    fn striped_rwlocks_different_offsets_dont_collide() {
        // Two record offsets in different 4 KiB pages should land on
        // distinct stripes (assuming stripe count > 1).
        let locks = StripedRwLocks::new(65_536);
        let a = 0x10_000;
        let b = 0x11_000;
        assert_ne!(locks.stripe_index(a), locks.stripe_index(b));
    }
}
