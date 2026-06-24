//! Shared SplitMix64 finalizer and process-local seed initializer.
//!
//! Three hot-path routers in this crate share the same SplitMix64 finalizer
//! (constants `0xbf58476d1ce4e5b9`, `0x94d049bb133111eb`):
//! - [`crate::index::hashtable::bucket_index`] — uses txid bytes `[0..8]`
//! - [`crate::locks::StripedLocks::stripe_index`] — uses txid bytes `[16..24]`
//! - [`crate::index::sharded::ShardedIndex::index_shard_for_key`] — uses txid bytes `[24..32]`
//!
//! Only the MIX is shared; byte selection (and masking) stays the caller's
//! responsibility. Keeping the constants in one place prevents silent drift.

/// SplitMix64 finalizer.
///
/// Takes a pre-mixed 64-bit value (raw txid bytes XOR seed) and applies the
/// standard SplitMix64 avalanche: 2 multiplies + 3 xorshifts, ~2 ns. All three
/// per-txid routers in this crate use identical constants; centralising them
/// here guarantees the three routers can never drift apart.
#[inline]
pub(crate) fn splitmix64_finalize(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

/// Initializer logic for a process-local random seed used by the per-txid
/// routers.
///
/// Prefers the OS CSPRNG via [`getrandom`]; falls back to a still
/// process-unpredictable seed derived from `RandomState` if the syscall is
/// unavailable (e.g. restricted sandboxes). No `expect()` — library code stays
/// panic-free.
///
/// Each call site owns its own `OnceLock<u64>` and passes this function to
/// `get_or_init`, so the three seeds (bucket hasher, lock striper, shard
/// router) remain independent values; this helper only supplies the shared
/// initialization logic, not a shared seed.
#[inline]
pub(crate) fn init_process_seed() -> u64 {
    let mut buf = [0u8; 8];
    if getrandom::getrandom(&mut buf).is_ok() {
        return u64::from_le_bytes(buf);
    }
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let mut h = RandomState::new().build_hasher();
    h.write_u64(0x9e37_79b9_7f4a_7c15);
    h.finish()
}
