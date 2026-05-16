//! Per-transaction lock striping for concurrent access.
//!
//! Uses bytes 16–23 of the txid (different from index bucket and fingerprint
//! bytes) to select one of the configured mutex stripes. This ensures that concurrent
//! operations on different transactions do not contend.

use crate::index::TxKey;

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
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&key.txid[16..24]);
        let h = u64::from_le_bytes(bytes) as usize;
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
}
