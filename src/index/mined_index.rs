//! Dedicated authoritative in-RAM mined-state store. See
//! `specs/MINEDINDEX_SETMINED_DESIGN.md`. Replaces on-device block entries +
//! the unmined secondary index.
use crate::record::BlockEntry;
use std::collections::{HashMap, HashSet};

/// `flags` bit: the record's UTXOs are all spent (maintained by the spend path).
pub const MINED_ALL_SPENT: u8 = 1;
/// `flags` bit: this tx is mined in >1 block; extra tuples live in `overflow`.
pub const MINED_HAS_OVERFLOW: u8 = 2;

/// One tx's mined-state: the first block tuple inline + lifecycle bits.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MinedEntry {
    pub block_id: u32,
    pub block_height: u32,
    pub subtree_idx: u32,
    /// 0 == mined on the longest chain.
    pub unmined_since: u32,
    pub flags: u8,
}

/// A `u32::MAX` slot means "no mined slot assigned" (stored in the primary entry).
pub const NO_MINED_SLOT: u32 = u32::MAX;

#[derive(Default)]
#[allow(dead_code)]
struct MinedShard {
    entries: Vec<MinedEntry>,
    /// `true` at index i == entry i is live; used so freed slots read as absent.
    live: Vec<bool>,
    free: Vec<u32>,
    overflow: HashMap<u32, Vec<BlockEntry>>,
    unmined: HashMap<u32, HashSet<u32>>,
}

impl MinedShard {
    #[allow(dead_code)]
    fn alloc(&mut self, e: MinedEntry) -> u32 {
        if let Some(slot) = self.free.pop() {
            self.entries[slot as usize] = e;
            self.live[slot as usize] = true;
            slot
        } else {
            let slot = self.entries.len() as u32;
            self.entries.push(e);
            self.live.push(true);
            slot
        }
    }

    #[allow(dead_code)]
    fn free_slot(&mut self, slot: u32) {
        if (slot as usize) < self.live.len() && self.live[slot as usize] {
            // Remove slot from its unmined bucket (if it was in one)
            if let Some(entry) = self.entries.get(slot as usize) {
                let unmined_height = entry.unmined_since;
                if unmined_height != 0 {
                    self.set_unmined(slot, unmined_height, 0);
                }
            }
            self.live[slot as usize] = false;
            self.overflow.remove(&slot);
            self.free.push(slot);
        }
    }

    #[allow(dead_code)]
    fn get(&self, slot: u32) -> Option<&MinedEntry> {
        match self.live.get(slot as usize) {
            Some(true) => self.entries.get(slot as usize),
            _ => None,
        }
    }

    #[allow(dead_code)]
    fn get_mut(&mut self, slot: u32) -> Option<&mut MinedEntry> {
        match self.live.get(slot as usize) {
            Some(true) => self.entries.get_mut(slot as usize),
            _ => None,
        }
    }

    #[allow(dead_code)]
    fn set_unmined(&mut self, slot: u32, old_height: u32, new_height: u32) {
        if old_height == new_height {
            return;
        }
        if old_height != 0
            && let Some(set) = self.unmined.get_mut(&old_height)
        {
            set.remove(&slot);
            if set.is_empty() {
                self.unmined.remove(&old_height);
            }
        }
        if new_height != 0 {
            self.unmined.entry(new_height).or_default().insert(slot);
        }
    }

    #[allow(dead_code)]
    fn unmined_below(&self, height: u32, out: &mut Vec<u32>) {
        for (&h, set) in &self.unmined {
            if h < height {
                out.extend(set.iter().copied());
            }
        }
        out.sort_unstable(); // deterministic order for tests + downstream batching
    }
}

use crate::index::TxKey;

/// Sharded mined-state index routing by txid hash.
///
/// Distributes entries across multiple [`MinedShard`] instances based on the
/// transaction ID to enable concurrent access without a global lock.
/// Each shard stores its entries locally; the shard index is always
/// re-derived from the txid via [`shard_for`](Self::shard_for) on every access,
/// never packed into the returned slot value.
pub struct ShardedMinedIndex {
    shards: Box<[parking_lot::Mutex<MinedShard>]>,
    mask: usize,
    seed: u64,
}

impl ShardedMinedIndex {
    /// Create a new sharded index with at least `shard_count` shards.
    ///
    /// The actual shard count is the next power of two at least 16.
    pub fn new(shard_count: usize) -> Self {
        let count = shard_count.next_power_of_two().max(16);
        let shards = (0..count)
            .map(|_| parking_lot::Mutex::new(MinedShard::default()))
            .collect::<Vec<_>>();
        Self {
            shards: shards.into_boxed_slice(),
            mask: count - 1,
            seed: crate::locks::stripe_seed(),
        }
    }

    /// Determine which shard a key belongs to.
    ///
    /// Routes by bytes 16..24 of the txid through `splitmix64_finalize`,
    /// seeded with the stripe seed, using a mask to select one of the shards.
    #[inline]
    pub fn shard_for(&self, key: &TxKey) -> usize {
        let raw = u64::from_le_bytes(key.txid[16..24].try_into().unwrap_or([0u8; 8]));
        (crate::index::hashmix::splitmix64_finalize(raw ^ self.seed) as usize) & self.mask
    }

    /// Allocate a slot for a freshly-created (unmined) transaction.
    ///
    /// Returns the shard-local slot (u32) to store in the primary entry's
    /// `mined_slot` field. The shard index is NOT packed into the return value;
    /// it is always re-derived from the txid via [`shard_for`](Self::shard_for).
    pub fn alloc_created(&self, key: &TxKey, block_height: u32) -> u32 {
        let mut sh = self.shards[self.shard_for(key)].lock();
        let slot = sh.alloc(MinedEntry {
            unmined_since: block_height,
            ..Default::default()
        });
        sh.set_unmined(slot, 0, block_height);
        slot
    }

    /// Apply a closure to the entry at the given shard-local slot.
    ///
    /// Returns `Some(R)` if the slot is live, or `None` if the slot is absent
    /// or has been freed.
    pub fn with_entry<R>(
        &self,
        key: &TxKey,
        slot: u32,
        f: impl FnOnce(&MinedEntry) -> R,
    ) -> Option<R> {
        let sh = self.shards[self.shard_for(key)].lock();
        sh.get(slot).map(f)
    }

    /// Collect all unmined entries below a given height.
    ///
    /// Iterates through all shards and returns `(shard_index, shard_local_slot)`
    /// pairs for all entries with `unmined_since < height`.
    pub fn collect_unmined_below(&self, height: u32, out: &mut Vec<(usize, u32)>) {
        for (si, shard) in self.shards.iter().enumerate() {
            let mut local = Vec::new();
            shard.lock().unmined_below(height, &mut local);
            out.extend(local.into_iter().map(|sl| (si, sl)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(block_id: u32) -> MinedEntry {
        MinedEntry {
            block_id,
            block_height: 100,
            subtree_idx: 0,
            unmined_since: 0,
            flags: 0,
        }
    }

    #[test]
    fn alloc_get_free_reuses_slot() {
        let mut s = MinedShard::default();
        let a = s.alloc(entry(10));
        assert_eq!(s.get(a).map(|e| e.block_id), Some(10));
        s.free_slot(a);
        assert!(s.get(a).is_none(), "freed slot reads as absent");
        let b = s.alloc(entry(20));
        assert_eq!(b, a, "free-list reuses the vacated slot index");
        assert_eq!(s.get(b).map(|e| e.block_id), Some(20));
    }

    #[test]
    fn height_buckets_track_unmined_and_range_query() {
        let mut s = MinedShard::default();
        let a = s.alloc(MinedEntry {
            unmined_since: 5,
            ..Default::default()
        });
        s.set_unmined(a, 0, 5); // enter bucket 5
        let b = s.alloc(MinedEntry {
            unmined_since: 9,
            ..Default::default()
        });
        s.set_unmined(b, 0, 9);
        let mut out = Vec::new();
        s.unmined_below(8, &mut out); // want slots with unmined_since in 1..8 => only `a`
        assert_eq!(out, vec![a]);
        s.set_unmined(a, 5, 0); // mined: leave the bucket
        out.clear();
        s.unmined_below(100, &mut out);
        assert_eq!(out, vec![b], "mined slot left its bucket");
    }

    #[test]
    fn free_slot_clears_unmined_bucket() {
        let mut s = MinedShard::default();
        let a = s.alloc(MinedEntry {
            unmined_since: 7,
            ..Default::default()
        });
        s.set_unmined(a, 0, 7); // enter bucket 7
        let mut out = Vec::new();
        s.unmined_below(100, &mut out);
        assert_eq!(out, vec![a], "slot should be in unmined bucket");

        s.free_slot(a); // free the slot
        out.clear();
        s.unmined_below(100, &mut out);
        assert!(
            out.is_empty(),
            "freed slot should not appear in unmined_below"
        );
    }

    #[test]
    fn sharded_alloc_and_lookup_by_key_roundtrip() {
        use crate::index::TxKey;
        let idx = ShardedMinedIndex::new(16);
        let k = TxKey { txid: [7u8; 32] };
        let slot = idx.alloc_created(&k, 42);
        idx.with_entry(&k, slot, |e| {
            assert_eq!(
                e.unmined_since, 42,
                "created tx is unmined at its block height"
            );
            assert_eq!(e.block_id, 0);
        });
        // range query sees it as unmined below 100
        let mut out = Vec::new();
        idx.collect_unmined_below(100, &mut out);
        assert!(
            out.iter().any(|&(_s, sl)| sl == slot),
            "created tx appears in unmined range"
        );
    }
}
