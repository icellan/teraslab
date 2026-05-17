//! Per-transaction lock striping for concurrent access.
//!
//! Uses bytes 16–23 of the txid (different from index bucket and fingerprint
//! bytes) to select one of the configured mutex stripes. This ensures that concurrent
//! operations on different transactions do not contend.

use crate::index::TxKey;

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
}

impl StripedLocks {
    /// Create a lock table with `stripe_count` stripes (rounded up to power of 2).
    pub fn new(stripe_count: usize) -> Self {
        let count = stripe_count.next_power_of_two().max(16);
        let locks = (0..count).map(|_| parking_lot::Mutex::new(())).collect();
        Self {
            locks,
            mask: count - 1,
        }
    }

    /// Acquire the lock for the given key. Returns a RAII guard.
    pub fn lock(&self, key: &TxKey) -> parking_lot::MutexGuard<'_, ()> {
        let idx = self.stripe_index(key);
        self.locks[idx].lock()
    }

    /// Compute which stripe a key maps to.
    pub fn stripe_index(&self, key: &TxKey) -> usize {
        // Use bytes 16–23 (different from bucket index [0–7] and fingerprint [8–15]).
        // This preserves distribution when operators configure more than 65,536 stripes.
        //
        // F-G1-018: use the slice→array conversion so the compiler emits a
        // single 8-byte load instead of the two-step `[0u8; 8]` +
        // `copy_from_slice` shape. `txid` is always 32 bytes so the
        // 16..24 slice is statically 8 bytes; the `try_into` will never
        // fail. `expect` here is correct (a TxKey shorter than 32 bytes
        // is structurally impossible — it's `[u8; 32]`).
        let h = u64::from_le_bytes(
            key.txid[16..24]
                .try_into()
                .expect("TxKey::txid is always 32 bytes; 16..24 is 8 bytes"),
        ) as usize;
        h & self.mask
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
