//! Fast non-cryptographic hashers for the create-batch dedup/grouping maps.
//!
//! The standard-library `HashMap`/`HashSet` use SipHash, a keyed hash chosen
//! for HashDoS resistance against adversary-chosen keys. The create batch path
//! has neither property that motivates SipHash:
//!
//! - **Keys are already uniformly random.** A txid is a double-SHA256 digest, so
//!   its bytes are indistinguishable from random. Re-hashing random bytes with
//!   SipHash buys no extra distribution — any 8 of those bytes are already a
//!   first-rate hash. (A flamegraph of the create path showed ~5% of create CPU
//!   sitting in `BuildHasher::hash_one` / `SipHasher::write` for exactly these
//!   two maps.)
//! - **These structures are not the authoritative dedup.** They are a batch-local
//!   early/grouping optimisation. The authoritative duplicate rejector is the
//!   index's `register_*_if_absent`, which performs an atomic reject under the
//!   per-shard write lock. A hash collision here therefore never affects
//!   correctness — `[u8; 32]` `Eq` is unchanged, so colliding keys simply probe
//!   to the right bucket and dedup exactly as before; the index still rejects any
//!   true duplicate that slips past.
//!
//! So a fast, non-keyed hash is correctness-neutral here and removes the SipHash
//! overhead. Two hashers are provided:
//!
//! - [`FastTxHasher`] for `[u8; 32]` txid keys: it returns the first 8 bytes of
//!   the key interpreted as a little-endian `u64`.
//! - [`FastU8Hasher`] for `u8` store-id keys: a trivial identity hash.
//!
//! Both are intended for use via [`std::hash::BuildHasherDefault`], e.g.
//! `HashSet<[u8; 32], BuildHasherDefault<FastTxHasher>>`.

use std::hash::Hasher;

/// Hasher for `[u8; 32]` txid keys: returns the first 8 bytes of the txid as a
/// little-endian `u64`.
///
/// `<[u8; 32] as Hash>::hash` drives a `Hasher` with two calls: first
/// `write_usize(32)` (the array length prefix), then `write(&self[..])` carrying
/// the 32 key bytes. The length is a constant (always 32) and so carries no
/// discriminating information — [`write_usize`](Self::write_usize) is therefore a
/// no-op, and the first 8 bytes of the *data* `write` are captured as the hash.
///
/// Equality-consistency holds: equal `[u8; 32]` values produce byte-identical
/// `write` calls and therefore identical hashes, which is the only invariant the
/// `Hash`/`Eq` contract requires. Distinct txids that happen to share their
/// first 8 bytes collide in the hash but remain distinct under `Eq`, so they
/// land in the same bucket and probe correctly — never merged.
#[derive(Default, Clone, Copy)]
pub struct FastTxHasher {
    hash: u64,
    /// Number of key bytes captured so far (saturates at 8).
    filled: u8,
}

impl Hasher for FastTxHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        // Capture the first up-to-8 bytes of the key data as a little-endian
        // u64. For `[u8; 32]` the data arrives in a single `write` carrying the
        // whole txid, so this fills from bytes [0..8] in one shot. The leading
        // `write_usize(32)` length prefix is dropped (see `write_usize`).
        for &b in bytes {
            if self.filled >= 8 {
                break;
            }
            self.hash |= (b as u64) << (self.filled * 8);
            self.filled += 1;
        }
    }

    /// No-op: the only `write_usize` the `[u8; 32]` `Hash` impl emits is the
    /// constant array-length prefix (32), which carries no information. Dropping
    /// it means [`write`](Self::write) captures the first 8 bytes of the actual
    /// txid rather than the length bytes.
    #[inline]
    fn write_usize(&mut self, _i: usize) {}

    #[inline]
    fn finish(&self) -> u64 {
        self.hash
    }
}

/// Hasher for `u8` store-id keys: a trivial identity hash (the byte value).
///
/// Store ids are small, dense, distinct integers (one per device), so the
/// identity map is already collision-free for them. `<u8 as Hash>::hash` calls
/// `write_u8`, which this captures directly.
#[derive(Default, Clone, Copy)]
pub struct FastU8Hasher {
    hash: u64,
}

impl Hasher for FastU8Hasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        // `u8` hashing goes through `write_u8` -> `write(&[b])`; capture it.
        // Fold any (unexpected) extra bytes in so the hash still depends on all
        // input rather than silently dropping it.
        for &b in bytes {
            self.hash = (self.hash << 8) | (b as u64);
        }
    }

    #[inline]
    fn write_u8(&mut self, i: u8) {
        self.hash = i as u64;
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};
    use std::hash::{BuildHasherDefault, Hash};

    /// Drive a `[u8; 32]` through `Hash` exactly as a `HashSet` would, returning
    /// the resulting `FastTxHasher` finish value.
    fn tx_hash(txid: &[u8; 32]) -> u64 {
        let mut h = FastTxHasher::default();
        txid.hash(&mut h);
        h.finish()
    }

    #[test]
    fn fast_tx_hasher_equal_keys_hash_equal() {
        let a = [7u8; 32];
        let b = [7u8; 32];
        assert_eq!(tx_hash(&a), tx_hash(&b), "equal txids must hash equal");
    }

    #[test]
    fn fast_tx_hasher_reads_first_eight_bytes_little_endian() {
        let mut txid = [0u8; 32];
        txid[..8].copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
        // The trailing bytes must not influence the hash.
        txid[8] = 0xFF;
        txid[31] = 0xAB;
        let expected = u64::from_le_bytes([1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(
            tx_hash(&txid),
            expected,
            "hash must equal the first 8 bytes as little-endian u64"
        );
    }

    #[test]
    fn fast_tx_hasher_distinct_prefixes_hash_distinct() {
        let mut a = [0u8; 32];
        a[0] = 1;
        let mut b = [0u8; 32];
        b[0] = 2;
        assert_ne!(
            tx_hash(&a),
            tx_hash(&b),
            "txids differing in byte 0 must hash differently"
        );
    }

    #[test]
    fn fast_hashset_dedups_same_unique_count_as_std() {
        // A vec of txids with deliberate duplicates: ids 0..50 each inserted
        // twice (100 inserts, 50 unique), plus a run that collides only in the
        // tail bytes to exercise hash-collision-but-distinct-key probing.
        let mut input: Vec<[u8; 32]> = Vec::new();
        for n in 0u8..50 {
            let mut t = [0u8; 32];
            t[0] = n;
            t[31] = n.wrapping_mul(7);
            input.push(t);
            input.push(t); // exact duplicate
        }
        // Two keys equal in the first 8 bytes but different in the tail: they
        // collide in FastTxHasher yet must stay distinct (2 unique entries).
        let mut c1 = [9u8; 32];
        c1[31] = 1;
        let mut c2 = [9u8; 32];
        c2[31] = 2;
        input.push(c1);
        input.push(c2);

        let std_unique: HashSet<[u8; 32]> = input.iter().copied().collect();
        let fast_unique: HashSet<[u8; 32], BuildHasherDefault<FastTxHasher>> =
            input.iter().copied().collect();

        assert_eq!(
            fast_unique.len(),
            std_unique.len(),
            "fast hasher must yield the same unique count as std SipHash"
        );
        assert_eq!(
            std_unique.len(),
            52,
            "50 deduped pairs + 2 tail-colliding distinct keys"
        );
        // Every std-unique key is present in the fast set (set equality).
        for k in &std_unique {
            assert!(fast_unique.contains(k), "fast set missing a unique key");
        }
        // The colliding-but-distinct pair both survived.
        assert!(fast_unique.contains(&c1));
        assert!(fast_unique.contains(&c2));
    }

    #[test]
    fn fast_hashset_insert_returns_false_on_duplicate() {
        let mut set: HashSet<[u8; 32], BuildHasherDefault<FastTxHasher>> =
            HashSet::with_hasher(BuildHasherDefault::default());
        let txid = [42u8; 32];
        assert!(set.insert(txid), "first insert is new");
        assert!(!set.insert(txid), "second insert reports duplicate");
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn fast_u8_hasher_groups_by_store_id() {
        // Mirror the bulk_by_store grouping: entries keyed by store id must
        // bucket identically to std.
        let entries: Vec<(u8, u32)> = vec![(0, 1), (1, 2), (0, 3), (2, 4), (1, 5), (0, 6)];

        let mut std_map: HashMap<u8, Vec<u32>> = HashMap::new();
        let mut fast_map: HashMap<u8, Vec<u32>, BuildHasherDefault<FastU8Hasher>> =
            HashMap::with_hasher(BuildHasherDefault::default());
        for &(store, v) in &entries {
            std_map.entry(store).or_default().push(v);
            fast_map.entry(store).or_default().push(v);
        }

        assert_eq!(
            fast_map.len(),
            std_map.len(),
            "same number of store buckets"
        );
        for (store, vals) in &std_map {
            assert_eq!(
                fast_map.get(store),
                Some(vals),
                "store {store} bucket contents must match std grouping"
            );
        }
    }

    #[test]
    fn fast_u8_hasher_distinct_ids_hash_distinct() {
        let mut a = FastU8Hasher::default();
        a.write_u8(3);
        let mut b = FastU8Hasher::default();
        b.write_u8(4);
        assert_ne!(a.finish(), b.finish(), "distinct store ids hash distinctly");

        let mut c = FastU8Hasher::default();
        c.write_u8(3);
        assert_eq!(a.finish(), c.finish(), "equal store ids hash equally");
    }
}
