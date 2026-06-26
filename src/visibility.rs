//! Per-key visibility barrier for the dispatch read/mutation/checkpoint
//! discipline.
//!
//! Replaces the single engine-wide `RwLock<()>` that serialized **all**
//! mutations against each other (and against reads). That global lock existed to
//! give a client read a batch-atomic view — a `GET` must never observe a
//! half-applied batch — and to let a checkpoint snapshot a quiescent engine. But
//! making *every* mutation take it exclusively meant mutations ran strictly one
//! at a time, capping write throughput at `1 / apply_latency` regardless of how
//! many keys or cores were available.
//!
//! [`VisibilityBarrier`] keeps the exact guarantees but at per-key granularity:
//!
//! * **global** `RwLock<()>` — the checkpoint gate. Mutations and reads take its
//!   SHARED side; the checkpoint takes its EXCLUSIVE side, so a checkpoint still
//!   excludes every in-flight mutation and read (quiescence), but mutations and
//!   reads no longer exclude *each other* through it.
//! * **per-key stripes** `RwLock<()>[]` — keyed by txid. A mutation write-locks
//!   the stripes of the keys it touches; a read read-locks the stripes of the
//!   keys it reads. So a read of key K is still excluded from a mutation of K
//!   (batch-atomic per key — a read never sees K mid-batch), while mutations on
//!   disjoint keys, and reads sharing a stripe, run concurrently.
//!
//! Deadlock-freedom: each call sorts+dedups its stripe indices and acquires them
//! in ascending order (global order), and the global guard is always taken
//! before the per-key guards — so no two callers can build a cycle.

use std::sync::Arc;

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::index::TxKey;

/// Per-key read/mutation visibility coordination plus a global checkpoint gate.
pub struct VisibilityBarrier {
    /// Checkpoint gate: shared by mutations+reads, exclusive by the checkpoint.
    global: RwLock<()>,
    /// Per-key stripes for read-vs-mutation exclusion.
    stripes: Box<[RwLock<()>]>,
    mask: usize,
    seed: u64,
}

/// RAII guard for a mutation: the global SHARED guard plus the per-key WRITE
/// guards for the mutation's keys. Held across local apply and dropped before
/// replication (mirroring the old `MutationBarrier`).
pub struct MutationVisibility<'a> {
    _global: RwLockReadGuard<'a, ()>,
    _stripes: Vec<RwLockWriteGuard<'a, ()>>,
}

/// RAII guard for a read: the global SHARED guard plus the per-key READ guards
/// for the keys being read.
pub struct ReadVisibility<'a> {
    _global: RwLockReadGuard<'a, ()>,
    _stripes: Vec<RwLockReadGuard<'a, ()>>,
}

/// RAII guard for a checkpoint: the global EXCLUSIVE guard, which excludes every
/// mutation and read (both hold the global shared side).
pub struct CheckpointVisibility<'a> {
    _global: RwLockWriteGuard<'a, ()>,
}

impl VisibilityBarrier {
    /// Create a barrier with `stripe_count` per-key stripes (rounded up to a
    /// power of two, floor 16). 65_536 matches [`crate::locks::StripedLocks`].
    pub fn new(stripe_count: usize) -> Arc<Self> {
        let count = stripe_count.next_power_of_two().max(16);
        let stripes = (0..count).map(|_| RwLock::new(())).collect::<Vec<_>>();
        Arc::new(Self {
            global: RwLock::new(()),
            stripes: stripes.into_boxed_slice(),
            mask: count - 1,
            seed: crate::locks::stripe_seed(),
        })
    }

    /// Map a key to its stripe. Uses bytes 16..24 of the txid (disjoint from the
    /// index bucket/fingerprint bytes) through a seeded SplitMix64 finalizer, the
    /// same scheme as `StripedLocks`, so distribution and grind-resistance match.
    #[inline]
    fn stripe_index(&self, key: &TxKey) -> usize {
        let raw = u64::from_le_bytes(key.txid[16..24].try_into().unwrap_or([0u8; 8]));
        let x = crate::index::hashmix::splitmix64_finalize(raw ^ self.seed);
        (x as usize) & self.mask
    }

    /// Unique stripe indices for `keys`, sorted ascending (deadlock-free order).
    fn unique_sorted_stripes(&self, keys: &[TxKey]) -> Vec<usize> {
        let mut idx: Vec<usize> = keys.iter().map(|k| self.stripe_index(k)).collect();
        idx.sort_unstable();
        idx.dedup();
        idx
    }

    /// Acquire mutation visibility for `keys`: global SHARED + per-key WRITE.
    /// Disjoint-key mutations proceed concurrently; same-key mutations and any
    /// read of those keys are excluded for the guard's lifetime.
    pub fn mutation(&self, keys: &[TxKey]) -> MutationVisibility<'_> {
        let global = self.global.read();
        let stripes = self
            .unique_sorted_stripes(keys)
            .into_iter()
            .map(|i| self.stripes[i].write())
            .collect();
        MutationVisibility {
            _global: global,
            _stripes: stripes,
        }
    }

    /// Acquire read visibility for `keys`: global SHARED + per-key READ. Reads
    /// share stripes with each other; a read of key K is excluded from a
    /// concurrent mutation of K (so it never observes K mid-batch).
    pub fn read(&self, keys: &[TxKey]) -> ReadVisibility<'_> {
        let global = self.global.read();
        let stripes = self
            .unique_sorted_stripes(keys)
            .into_iter()
            .map(|i| self.stripes[i].read())
            .collect();
        ReadVisibility {
            _global: global,
            _stripes: stripes,
        }
    }

    /// Acquire the global EXCLUSIVE side for a checkpoint — excludes every
    /// in-flight mutation and read (both hold the global shared side), so the
    /// engine is quiescent for the snapshot.
    pub fn checkpoint(&self) -> CheckpointVisibility<'_> {
        CheckpointVisibility {
            _global: self.global.write(),
        }
    }

    /// Acquire only the global SHARED side (no per-key stripes). Used by cold
    /// mutation/read paths not yet migrated to per-key granularity — they keep
    /// the coarse behavior but still coordinate with the checkpoint.
    pub fn global_read(&self) -> RwLockReadGuard<'_, ()> {
        self.global.read()
    }

    /// Acquire the global EXCLUSIVE side. Used by the checkpoint and by cold
    /// mutations that have not been migrated to per-key write locks (they keep
    /// the coarse "exclude everyone" behavior).
    pub fn global_write(&self) -> RwLockWriteGuard<'_, ()> {
        self.global.write()
    }

    /// Number of per-key stripes.
    pub fn stripe_count(&self) -> usize {
        self.stripes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    fn key(n: u64) -> TxKey {
        let mut txid = [0u8; 32];
        txid[16..24].copy_from_slice(&n.to_le_bytes());
        TxKey { txid }
    }

    /// Find two keys that map to DISTINCT stripes (almost always the first try).
    fn distinct_pair(b: &VisibilityBarrier) -> (TxKey, TxKey) {
        let a = key(1);
        for n in 2..10_000u64 {
            let c = key(n);
            if b.stripe_index(&a) != b.stripe_index(&c) {
                return (a, c);
            }
        }
        panic!("no distinct stripe found");
    }

    #[test]
    fn disjoint_mutations_do_not_block_each_other() {
        let b = VisibilityBarrier::new(65536);
        let (k1, k2) = distinct_pair(&b);
        let _g1 = b.mutation(&[k1]);
        // Acquiring a disjoint-key mutation must not block (different stripe).
        let _g2 = b.mutation(&[k2]);
        // Both held simultaneously — no deadlock, proving concurrency.
    }

    #[test]
    fn read_of_same_key_blocks_until_mutation_releases() {
        let b = VisibilityBarrier::new(65536);
        let k = key(42);
        let started = Arc::new(AtomicBool::new(false));
        let acquired = Arc::new(AtomicBool::new(false));

        let g = b.mutation(&[k]);

        let b2 = b.clone();
        let started2 = started.clone();
        let acquired2 = acquired.clone();
        let handle = std::thread::spawn(move || {
            started2.store(true, Ordering::SeqCst);
            let _r = b2.read(&[key(42)]); // same stripe -> must block on the write
            acquired2.store(true, Ordering::SeqCst);
        });

        // Let the reader run and block.
        while !started.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        std::thread::sleep(Duration::from_millis(30));
        assert!(
            !acquired.load(Ordering::SeqCst),
            "read of the same key must NOT acquire while the mutation holds it"
        );
        drop(g);
        handle.join().unwrap();
        assert!(
            acquired.load(Ordering::SeqCst),
            "read must acquire once the mutation releases"
        );
    }

    #[test]
    fn read_of_disjoint_key_runs_concurrently_with_mutation() {
        let b = VisibilityBarrier::new(65536);
        let (k1, k2) = distinct_pair(&b);
        let _m = b.mutation(&[k1]);
        // A read of a disjoint key must acquire immediately (no block).
        let _r = b.read(&[k2]);
    }

    #[test]
    fn concurrent_reads_of_same_key_share() {
        let b = VisibilityBarrier::new(65536);
        let _r1 = b.read(&[key(7)]);
        // Second read of the same key shares the read side — no block.
        let _r2 = b.read(&[key(7)]);
    }

    #[test]
    fn checkpoint_excludes_mutations_and_reads() {
        let b = VisibilityBarrier::new(65536);
        let cp = b.checkpoint();
        let acquired = Arc::new(AtomicBool::new(false));

        let b2 = b.clone();
        let acquired2 = acquired.clone();
        let handle = std::thread::spawn(move || {
            let _m = b2.mutation(&[key(1)]); // global shared blocked by cp's exclusive
            acquired2.store(true, Ordering::SeqCst);
        });
        std::thread::sleep(Duration::from_millis(30));
        assert!(
            !acquired.load(Ordering::SeqCst),
            "a mutation must not start while a checkpoint holds the global exclusive"
        );
        drop(cp);
        handle.join().unwrap();
        assert!(
            acquired.load(Ordering::SeqCst),
            "mutation proceeds after checkpoint"
        );
    }

    #[test]
    fn batch_mutation_blocks_a_read_that_overlaps_any_key() {
        // A read overlapping ANY key of a mutation batch is excluded (batch-atomic
        // per overlapping key).
        let b = VisibilityBarrier::new(65536);
        let _m = b.mutation(&[key(1), key(2), key(3)]);
        let acquired = Arc::new(AtomicBool::new(false));
        let b2 = b.clone();
        let acquired2 = acquired.clone();
        let h = std::thread::spawn(move || {
            let _r = b2.read(&[key(99), key(2)]); // key(2) overlaps -> blocks
            acquired2.store(true, Ordering::SeqCst);
        });
        std::thread::sleep(Duration::from_millis(30));
        assert!(
            !acquired.load(Ordering::SeqCst),
            "read overlapping a batch key must block until the batch releases"
        );
        drop(_m);
        h.join().unwrap();
        assert!(acquired.load(Ordering::SeqCst));
    }
}
