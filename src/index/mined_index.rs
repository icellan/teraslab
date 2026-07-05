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

/// Version byte for the [`ShardedMinedIndex::serialize`] snapshot format.
const MINED_SNAPSHOT_VERSION: u8 = 1;

/// Errors from [`ShardedMinedIndex::deserialize`].
#[derive(Debug, thiserror::Error)]
pub enum MinedIndexError {
    /// The snapshot's version byte doesn't match [`MINED_SNAPSHOT_VERSION`].
    #[error("mined-index snapshot version mismatch: got {0}, want {1}")]
    VersionMismatch(u8, u8),
    /// The snapshot bytes are truncated or otherwise malformed.
    #[error("mined-index snapshot truncated/corrupt")]
    Corrupt,
}

/// Minimal fail-closed byte cursor for the mined-index snapshot format.
///
/// Every read returns `None` (never panics or indexes out of bounds) when
/// the requested bytes aren't available, so a truncated or corrupt buffer
/// fails the read instead of panicking.
struct SnapshotCursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> SnapshotCursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn read_u8(&mut self) -> Option<u8> {
        let b = *self.data.get(self.pos)?;
        self.pos += 1;
        Some(b)
    }

    fn read_u32(&mut self) -> Option<u32> {
        let bytes: [u8; 4] = self.data.get(self.pos..self.pos + 4)?.try_into().ok()?;
        self.pos += 4;
        Some(u32::from_le_bytes(bytes))
    }

    fn read_u64(&mut self) -> Option<u64> {
        let bytes: [u8; 8] = self.data.get(self.pos..self.pos + 8)?.try_into().ok()?;
        self.pos += 8;
        Some(u64::from_le_bytes(bytes))
    }
}

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

    /// Append this shard's full state (entries + live flags, free list, and
    /// overflow map) to `out` in plain little-endian length-prefixed form.
    /// The `unmined` height buckets are deliberately NOT serialized — they're
    /// re-derived by [`Self::deserialize`] from the loaded entries'
    /// `unmined_since` fields, which is self-correcting against corruption
    /// and shrinks the snapshot. See [`ShardedMinedIndex::serialize`] for the
    /// overall snapshot layout this is embedded in.
    fn serialize(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for (i, e) in self.entries.iter().enumerate() {
            let live = self.live.get(i).copied().unwrap_or(false);
            out.push(live as u8);
            out.extend_from_slice(&e.block_id.to_le_bytes());
            out.extend_from_slice(&e.block_height.to_le_bytes());
            out.extend_from_slice(&e.subtree_idx.to_le_bytes());
            out.extend_from_slice(&e.unmined_since.to_le_bytes());
            out.push(e.flags);
        }

        out.extend_from_slice(&(self.free.len() as u32).to_le_bytes());
        for &slot in &self.free {
            out.extend_from_slice(&slot.to_le_bytes());
        }

        out.extend_from_slice(&(self.overflow.len() as u32).to_le_bytes());
        for (&slot, blocks) in &self.overflow {
            out.extend_from_slice(&slot.to_le_bytes());
            out.extend_from_slice(&(blocks.len() as u32).to_le_bytes());
            for be in blocks {
                // `BlockEntry` is `#[repr(C, packed)]`; copy fields out to
                // locals before referencing them to avoid unaligned
                // references (E0793).
                let block_id = be.block_id;
                let block_height = be.block_height;
                let subtree_idx = be.subtree_idx;
                out.extend_from_slice(&block_id.to_le_bytes());
                out.extend_from_slice(&block_height.to_le_bytes());
                out.extend_from_slice(&subtree_idx.to_le_bytes());
            }
        }
    }

    /// Parse one shard's state from `cur`, in the format written by
    /// [`Self::serialize`]. Fails closed with [`MinedIndexError::Corrupt`] on
    /// any truncated read, or any `free`/`overflow` slot reference that is
    /// out of bounds for the loaded `entries` (which would otherwise panic
    /// later, deferred to the next `alloc`); never panics itself. The
    /// `unmined` height buckets are re-derived from the loaded entries
    /// rather than trusted from the wire.
    fn deserialize(cur: &mut SnapshotCursor) -> Result<Self, MinedIndexError> {
        let entry_count = cur.read_u32().ok_or(MinedIndexError::Corrupt)? as usize;
        // Grow via `push` rather than `Vec::with_capacity(entry_count)` so a
        // poisoned/huge declared count fails fast on the first truncated
        // read instead of driving a large upfront allocation.
        let mut entries = Vec::new();
        let mut live = Vec::new();
        for _ in 0..entry_count {
            let l = cur.read_u8().ok_or(MinedIndexError::Corrupt)?;
            let block_id = cur.read_u32().ok_or(MinedIndexError::Corrupt)?;
            let block_height = cur.read_u32().ok_or(MinedIndexError::Corrupt)?;
            let subtree_idx = cur.read_u32().ok_or(MinedIndexError::Corrupt)?;
            let unmined_since = cur.read_u32().ok_or(MinedIndexError::Corrupt)?;
            let flags = cur.read_u8().ok_or(MinedIndexError::Corrupt)?;
            entries.push(MinedEntry {
                block_id,
                block_height,
                subtree_idx,
                unmined_since,
                flags,
            });
            live.push(l != 0);
        }

        let free_count = cur.read_u32().ok_or(MinedIndexError::Corrupt)? as usize;
        let mut free = Vec::new();
        for _ in 0..free_count {
            let slot = cur.read_u32().ok_or(MinedIndexError::Corrupt)?;
            // A free-list entry that doesn't reference a real slot would
            // panic later in `alloc` (`self.entries[slot] = e`), deferred
            // and far from this parse — reject it here instead.
            if slot as usize >= entries.len() {
                return Err(MinedIndexError::Corrupt);
            }
            free.push(slot);
        }

        let overflow_count = cur.read_u32().ok_or(MinedIndexError::Corrupt)? as usize;
        let mut overflow = HashMap::new();
        for _ in 0..overflow_count {
            let slot = cur.read_u32().ok_or(MinedIndexError::Corrupt)?;
            if slot as usize >= entries.len() {
                return Err(MinedIndexError::Corrupt);
            }
            let vec_len = cur.read_u32().ok_or(MinedIndexError::Corrupt)? as usize;
            let mut v = Vec::new();
            for _ in 0..vec_len {
                let block_id = cur.read_u32().ok_or(MinedIndexError::Corrupt)?;
                let block_height = cur.read_u32().ok_or(MinedIndexError::Corrupt)?;
                let subtree_idx = cur.read_u32().ok_or(MinedIndexError::Corrupt)?;
                v.push(BlockEntry {
                    block_id,
                    block_height,
                    subtree_idx,
                });
            }
            overflow.insert(slot, v);
        }

        // `unmined` is deliberately not part of the wire format (see
        // `serialize`): re-derive each live slot's height bucket from its own
        // `unmined_since` field rather than trusting a separately-serialized
        // (and therefore independently corruptible) bucket structure.
        let mut unmined: HashMap<u32, HashSet<u32>> = HashMap::new();
        for (slot, &is_live) in live.iter().enumerate() {
            if !is_live {
                continue;
            }
            let since = entries[slot].unmined_since;
            if since != 0 {
                unmined.entry(since).or_default().insert(slot as u32);
            }
        }

        Ok(MinedShard {
            entries,
            live,
            free,
            overflow,
            unmined,
        })
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
    /// The actual shard count is the next power of two at least 16. Routing
    /// uses a fresh process-random seed ([`crate::locks::stripe_seed`]);
    /// use [`Self::new_with_seed`] (or [`Self::deserialize`], which calls it
    /// internally) when the seed must be a specific, previously-persisted
    /// value instead.
    pub fn new(shard_count: usize) -> Self {
        Self::new_with_seed(shard_count, crate::locks::stripe_seed())
    }

    /// Create a new sharded index with at least `shard_count` shards and an
    /// explicit routing `seed`, bypassing the process-random default.
    ///
    /// Used by [`Self::deserialize`] to reinstall the exact seed a snapshot
    /// was written with — routing (`shard_for`) is a function of `seed`, so
    /// a restored index must reuse the original seed byte-for-byte or every
    /// key silently routes to the wrong shard. Also useful in tests that need
    /// deterministic (or deliberately mismatched) routing.
    pub(crate) fn new_with_seed(shard_count: usize, seed: u64) -> Self {
        let count = shard_count.next_power_of_two().max(16);
        let shards = (0..count)
            .map(|_| parking_lot::Mutex::new(MinedShard::default()))
            .collect::<Vec<_>>();
        Self {
            shards: shards.into_boxed_slice(),
            mask: count - 1,
            seed,
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

    /// Record a tx as mined in `block_id`, adding the block tuple inline (if
    /// the slot has none yet) or into the shard's overflow list (if the tx is
    /// already mined in a different block — a competing-chain reorg case).
    ///
    /// Idempotent: re-applying a `block_id` already recorded for this slot
    /// (inline or in overflow) is a no-op and returns `changed: false`.
    ///
    /// When `on_longest_chain` is set, clears `unmined_since` (the tx is no
    /// longer unmined) and updates the shard's height bucket accordingly.
    ///
    /// Both the setMined hot path and crash-recovery redo replay call this so
    /// their mutation semantics stay identical.
    pub fn apply_set_mined(
        &self,
        key: &TxKey,
        slot: u32,
        block_id: u32,
        block_height: u32,
        subtree_idx: u32,
        on_longest_chain: bool,
    ) -> MinedApplyResult {
        let mut sh = self.shards[self.shard_for(key)].lock();

        let Some(inline_block_id) = sh.get(slot).map(|e| e.block_id) else {
            // Slot absent (freed or never allocated): nothing to apply.
            return MinedApplyResult {
                changed: false,
                new_unmined_since: 0,
            };
        };

        // Dedup: this block_id is already recorded, inline or in overflow.
        let already_inline = inline_block_id != 0 && inline_block_id == block_id;
        let already_overflow = sh
            .overflow
            .get(&slot)
            .is_some_and(|v| v.iter().any(|be| be.block_id == block_id));
        if already_inline || already_overflow {
            let new_unmined_since = sh.get(slot).map(|e| e.unmined_since).unwrap_or(0);
            return MinedApplyResult {
                changed: false,
                new_unmined_since,
            };
        }

        if inline_block_id == 0 {
            if let Some(e) = sh.get_mut(slot) {
                e.block_id = block_id;
                e.block_height = block_height;
                e.subtree_idx = subtree_idx;
            }
        } else {
            sh.overflow.entry(slot).or_default().push(BlockEntry {
                block_id,
                block_height,
                subtree_idx,
            });
            if let Some(e) = sh.get_mut(slot) {
                e.flags |= MINED_HAS_OVERFLOW;
            }
        }

        let old_unmined = sh.get(slot).map(|e| e.unmined_since).unwrap_or(0);
        let new_unmined_since = if on_longest_chain { 0 } else { old_unmined };
        if on_longest_chain && let Some(e) = sh.get_mut(slot) {
            e.unmined_since = 0;
        }
        if old_unmined != new_unmined_since {
            sh.set_unmined(slot, old_unmined, new_unmined_since);
        }

        MinedApplyResult {
            changed: true,
            new_unmined_since,
        }
    }

    /// Remove a block tuple previously recorded by [`apply_set_mined`].
    ///
    /// If `block_id` is the inline tuple, pulls one entry from overflow into
    /// its place (if any remain), else clears the inline slot. If `block_id`
    /// is only in overflow, removes it there. If the record ends up with zero
    /// blocks, the tx becomes unmined again as of `current_height`.
    ///
    /// No-op if the slot is absent or `block_id` isn't recorded for it.
    pub fn apply_unset(&self, key: &TxKey, slot: u32, block_id: u32, current_height: u32) {
        let mut sh = self.shards[self.shard_for(key)].lock();
        let Some(inline_block_id) = sh.get(slot).map(|e| e.block_id) else {
            return;
        };

        if inline_block_id == block_id {
            // Removing the inline tuple: backfill from overflow if present.
            let replacement = sh
                .overflow
                .get_mut(&slot)
                .filter(|v| !v.is_empty())
                .map(|v| v.remove(0));
            let overflow_now_empty = sh.overflow.get(&slot).is_none_or(|v| v.is_empty());
            if overflow_now_empty {
                sh.overflow.remove(&slot);
            }

            if let Some(e) = sh.get_mut(slot) {
                match replacement {
                    Some(be) => {
                        e.block_id = be.block_id;
                        e.block_height = be.block_height;
                        e.subtree_idx = be.subtree_idx;
                        if overflow_now_empty {
                            e.flags &= !MINED_HAS_OVERFLOW;
                        }
                    }
                    None => {
                        e.block_id = 0;
                        e.block_height = 0;
                        e.subtree_idx = 0;
                    }
                }
            }
        } else if let Some(v) = sh.overflow.get_mut(&slot) {
            if let Some(pos) = v.iter().position(|be| be.block_id == block_id) {
                v.swap_remove(pos);
            }
            if v.is_empty() {
                sh.overflow.remove(&slot);
                if let Some(e) = sh.get_mut(slot) {
                    e.flags &= !MINED_HAS_OVERFLOW;
                }
            }
        } else {
            // block_id not recorded for this slot at all; nothing to do.
            return;
        }

        let has_blocks =
            sh.get(slot).is_some_and(|e| e.block_id != 0) || sh.overflow.contains_key(&slot);
        if !has_blocks {
            let old_unmined = sh.get(slot).map(|e| e.unmined_since).unwrap_or(0);
            if let Some(e) = sh.get_mut(slot) {
                e.unmined_since = current_height;
            }
            sh.set_unmined(slot, old_unmined, current_height);
        }
    }

    /// Set or clear the `MINED_ALL_SPENT` flag on the slot's entry.
    ///
    /// No-op if the slot is absent.
    pub fn set_all_spent(&self, key: &TxKey, slot: u32, all_spent: bool) {
        let mut sh = self.shards[self.shard_for(key)].lock();
        if let Some(entry) = sh.get_mut(slot) {
            if all_spent {
                entry.flags |= MINED_ALL_SPENT;
            } else {
                entry.flags &= !MINED_ALL_SPENT;
            }
        }
    }

    /// Serialize the entire index (the routing seed, plus every shard's
    /// entries, free list, and overflow map) into a versioned snapshot,
    /// appended to `out`.
    ///
    /// Format: a 1-byte version ([`MINED_SNAPSHOT_VERSION`]), followed by the
    /// 8-byte little-endian routing `seed`, followed by each shard's state in
    /// order, in plain little-endian length-prefixed form. The seed MUST be
    /// persisted: `shard_for` mixes it into every routing decision, and a
    /// fresh process only ever picks a new random seed
    /// ([`crate::locks::stripe_seed`]) — without the persisted value,
    /// [`Self::deserialize`] after a real process restart would route every
    /// key to the wrong shard. Round-trips through [`Self::deserialize`]
    /// given the same `shard_count` this index was created with.
    pub fn serialize(&self, out: &mut Vec<u8>) {
        out.push(MINED_SNAPSHOT_VERSION);
        out.extend_from_slice(&self.seed.to_le_bytes());
        for shard in self.shards.iter() {
            shard.lock().serialize(out);
        }
    }

    /// Reconstruct a [`ShardedMinedIndex`] from a snapshot produced by
    /// [`Self::serialize`].
    ///
    /// `shard_count` must be the same value the original index was
    /// constructed with — it re-derives the identical `mask` so the parsed
    /// shard sections line up positionally. The routing `seed` is NOT
    /// re-derived from the process; it is read verbatim from the snapshot
    /// and installed via [`Self::new_with_seed`], so `shard_for` routes every
    /// key to the same shard index it did before the snapshot even across a
    /// process restart (a fresh [`crate::locks::stripe_seed`] would scatter
    /// every key to a different, wrong shard).
    ///
    /// # Errors
    ///
    /// Fails closed (never panics) with:
    /// - [`MinedIndexError::VersionMismatch`] if the version byte doesn't
    ///   match [`MINED_SNAPSHOT_VERSION`].
    /// - [`MinedIndexError::Corrupt`] if the bytes are truncated, reference
    ///   out-of-bounds slots, or are otherwise malformed at any point during
    ///   parsing.
    pub fn deserialize(bytes: &[u8], shard_count: usize) -> Result<Self, MinedIndexError> {
        let mut cur = SnapshotCursor::new(bytes);
        let version = cur.read_u8().ok_or(MinedIndexError::Corrupt)?;
        if version != MINED_SNAPSHOT_VERSION {
            return Err(MinedIndexError::VersionMismatch(
                version,
                MINED_SNAPSHOT_VERSION,
            ));
        }
        let seed = cur.read_u64().ok_or(MinedIndexError::Corrupt)?;

        let restored = Self::new_with_seed(shard_count, seed);
        for shard_mutex in restored.shards.iter() {
            let parsed = MinedShard::deserialize(&mut cur)?;
            *shard_mutex.lock() = parsed;
        }
        Ok(restored)
    }
}

/// Result of [`ShardedMinedIndex::apply_set_mined`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MinedApplyResult {
    /// `true` if this call recorded a new block tuple (not a dedup no-op).
    pub changed: bool,
    /// The entry's `unmined_since` after this call.
    pub new_unmined_since: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_roundtrip_reproduces_state() {
        use crate::index::TxKey;

        let idx = ShardedMinedIndex::new(16);

        // 1. A created/unmined slot.
        let k_unmined = TxKey { txid: [10u8; 32] };
        let slot_unmined = idx.alloc_created(&k_unmined, 42);

        // 2. A mined slot on the longest chain.
        let k_mined = TxKey { txid: [11u8; 32] };
        let slot_mined = idx.alloc_created(&k_mined, 5);
        idx.apply_set_mined(&k_mined, slot_mined, 500, 20, 3, true);

        // 3. A slot with an overflow block (mined in two competing blocks).
        let k_overflow = TxKey { txid: [12u8; 32] };
        let slot_overflow = idx.alloc_created(&k_overflow, 5);
        idx.apply_set_mined(&k_overflow, slot_overflow, 600, 30, 1, true);
        idx.apply_set_mined(&k_overflow, slot_overflow, 601, 31, 2, true);

        // 4. A freed slot (must restore as absent).
        let k_freed = TxKey { txid: [13u8; 32] };
        let slot_freed = idx.alloc_created(&k_freed, 5);
        {
            let mut sh = idx.shards[idx.shard_for(&k_freed)].lock();
            sh.free_slot(slot_freed);
        }

        let mut buf = Vec::new();
        idx.serialize(&mut buf);

        let restored = ShardedMinedIndex::deserialize(&buf, 16)
            .expect("roundtrip of a well-formed snapshot must succeed");

        restored.with_entry(&k_unmined, slot_unmined, |e| {
            assert_eq!(e.block_id, 0, "unmined slot has no block tuple");
            assert_eq!(e.unmined_since, 42, "unmined_since must survive roundtrip");
        });

        restored.with_entry(&k_mined, slot_mined, |e| {
            assert_eq!(e.block_id, 500, "inline block tuple must survive roundtrip");
            assert_eq!(e.block_height, 20);
            assert_eq!(e.subtree_idx, 3);
            assert_eq!(
                e.unmined_since, 0,
                "mined-on-longest-chain clears unmined_since"
            );
        });

        restored.with_entry(&k_overflow, slot_overflow, |e| {
            assert_eq!(e.block_id, 600, "inline tuple keeps the first block");
            assert_ne!(
                e.flags & MINED_HAS_OVERFLOW,
                0,
                "overflow flag must survive roundtrip"
            );
        });
        {
            let sh = restored.shards[restored.shard_for(&k_overflow)].lock();
            let overflow = sh
                .overflow
                .get(&slot_overflow)
                .expect("overflow entry must survive roundtrip");
            assert_eq!(overflow.len(), 1);
            let be = overflow[0];
            assert_eq!({ be.block_id }, 601);
            assert_eq!({ be.block_height }, 31);
            assert_eq!({ be.subtree_idx }, 2);
        }

        assert!(
            restored
                .with_entry(&k_freed, slot_freed, |e| e.block_id)
                .is_none(),
            "freed slot must restore as absent"
        );

        // Pair each slot with its OWN shard index — different keys can land
        // in different shards yet still get the same raw (shard-local) slot
        // number, so comparing bare slot numbers across shards would produce
        // false collisions.
        let unmined_pair = (restored.shard_for(&k_unmined), slot_unmined);
        let mined_pair = (restored.shard_for(&k_mined), slot_mined);
        let overflow_pair = (restored.shard_for(&k_overflow), slot_overflow);
        let freed_pair = (restored.shard_for(&k_freed), slot_freed);

        let mut out = Vec::new();
        restored.collect_unmined_below(1_000, &mut out);
        assert!(
            out.contains(&unmined_pair),
            "unmined slot must still appear in the unmined range query"
        );
        assert!(
            !out.contains(&mined_pair),
            "mined slot must not appear as unmined"
        );
        assert!(
            !out.contains(&overflow_pair),
            "overflow slot is mined on the longest chain, must not appear as unmined"
        );
        assert!(
            !out.contains(&freed_pair),
            "freed slot must not appear as unmined"
        );
    }

    #[test]
    fn deserialize_wrong_version_fails_closed() {
        use crate::index::TxKey;

        let idx = ShardedMinedIndex::new(16);
        let k = TxKey { txid: [1u8; 32] };
        idx.alloc_created(&k, 10);

        let mut buf = Vec::new();
        idx.serialize(&mut buf);
        let original_version = buf[0];
        buf[0] = original_version.wrapping_add(1);

        let err = ShardedMinedIndex::deserialize(&buf, 16)
            .err()
            .expect("a flipped version byte must fail closed");
        match err {
            MinedIndexError::VersionMismatch(got, want) => {
                assert_eq!(got, original_version.wrapping_add(1));
                assert_eq!(want, MINED_SNAPSHOT_VERSION);
            }
            other => panic!("expected VersionMismatch, got {other:?}"),
        }
    }

    #[test]
    fn deserialize_truncated_fails_closed() {
        use crate::index::TxKey;

        let idx = ShardedMinedIndex::new(16);
        let k = TxKey { txid: [1u8; 32] };
        idx.alloc_created(&k, 10);

        let mut buf = Vec::new();
        idx.serialize(&mut buf);
        let truncated = &buf[..buf.len() / 2];

        let err = ShardedMinedIndex::deserialize(truncated, 16)
            .err()
            .expect("truncated snapshot bytes must fail closed");
        assert!(
            matches!(err, MinedIndexError::Corrupt),
            "expected Corrupt, got {err:?}"
        );
    }

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

    #[test]
    fn apply_set_mined_is_idempotent() {
        use crate::index::TxKey;
        let idx = ShardedMinedIndex::new(16);
        let k = TxKey { txid: [1u8; 32] };
        let slot = idx.alloc_created(&k, 10);

        let first = idx.apply_set_mined(&k, slot, 500, 20, 3, true);
        assert!(
            first.changed,
            "first application of a new block is a real transition"
        );
        assert_eq!(
            first.new_unmined_since, 0,
            "on_longest_chain clears unmined_since"
        );

        let second = idx.apply_set_mined(&k, slot, 500, 20, 3, true);
        assert!(!second.changed, "re-applying the same block_id is a no-op");
        assert_eq!(second.new_unmined_since, 0);

        idx.with_entry(&k, slot, |e| {
            assert_eq!(e.block_id, 500, "inline tuple holds the single block");
            assert_eq!(
                e.flags & MINED_HAS_OVERFLOW,
                0,
                "no overflow for a single block"
            );
        });
    }

    #[test]
    fn apply_set_mined_second_block_spills_to_overflow() {
        use crate::index::TxKey;
        let idx = ShardedMinedIndex::new(16);
        let k = TxKey { txid: [2u8; 32] };
        let slot = idx.alloc_created(&k, 10);

        let first = idx.apply_set_mined(&k, slot, 500, 20, 3, true);
        assert!(first.changed);
        let second = idx.apply_set_mined(&k, slot, 501, 21, 4, true);
        assert!(second.changed, "a distinct block_id is a real transition");

        idx.with_entry(&k, slot, |e| {
            assert_eq!(e.block_id, 500, "inline tuple keeps the first block");
            assert_ne!(
                e.flags & MINED_HAS_OVERFLOW,
                0,
                "second distinct block sets the overflow flag"
            );
        });

        let sh = idx.shards[idx.shard_for(&k)].lock();
        let overflow = sh
            .overflow
            .get(&slot)
            .expect("overflow entry for this slot");
        assert_eq!(overflow.len(), 1);
        let be = overflow[0];
        // BlockEntry is `#[repr(C, packed)]`; copy fields out before comparing
        // to avoid taking unaligned references (E0793).
        assert_eq!({ be.block_id }, 501);
        assert_eq!({ be.block_height }, 21);
        assert_eq!({ be.subtree_idx }, 4);
    }

    #[test]
    fn apply_unset_to_zero_restores_unmined() {
        use crate::index::TxKey;
        let idx = ShardedMinedIndex::new(16);
        let k = TxKey { txid: [3u8; 32] };
        let slot = idx.alloc_created(&k, 10);

        let applied = idx.apply_set_mined(&k, slot, 500, 20, 3, true);
        assert!(applied.changed);
        assert_eq!(applied.new_unmined_since, 0);

        // Mined tx must NOT show up as unmined below any height.
        let mut out = Vec::new();
        idx.collect_unmined_below(1_000, &mut out);
        assert!(
            !out.iter().any(|&(_s, sl)| sl == slot),
            "mined tx should be out of the unmined bucket"
        );

        idx.apply_unset(&k, slot, 500, 30);

        idx.with_entry(&k, slot, |e| {
            assert_eq!(
                e.block_id, 0,
                "inline tuple cleared once its only block is unset"
            );
            assert_eq!(
                e.unmined_since, 30,
                "unset restores unmined_since to current height"
            );
        });

        out.clear();
        idx.collect_unmined_below(1_000, &mut out);
        assert!(
            out.iter().any(|&(_s, sl)| sl == slot),
            "unset tx is visible again in the unmined range query"
        );
    }

    #[test]
    fn set_all_spent_toggles_flag() {
        use crate::index::TxKey;
        let idx = ShardedMinedIndex::new(16);
        let k = TxKey { txid: [4u8; 32] };
        let slot = idx.alloc_created(&k, 10);

        idx.with_entry(&k, slot, |e| {
            assert_eq!(e.flags & MINED_ALL_SPENT, 0, "not set by default");
        });

        idx.set_all_spent(&k, slot, true);
        idx.with_entry(&k, slot, |e| {
            assert_ne!(
                e.flags & MINED_ALL_SPENT,
                0,
                "flag set after set_all_spent(true)"
            );
        });

        idx.set_all_spent(&k, slot, false);
        idx.with_entry(&k, slot, |e| {
            assert_eq!(
                e.flags & MINED_ALL_SPENT,
                0,
                "flag cleared after set_all_spent(false)"
            );
        });
    }

    #[test]
    fn snapshot_survives_different_seed() {
        use crate::index::TxKey;

        // Stand in for "whatever random seed the process happened to pick
        // before it crashed" with an explicit, known seed.
        let original_seed = 0xDEAD_BEEF_CAFE_F00Du64;
        let idx = ShardedMinedIndex::new_with_seed(16, original_seed);
        let k = TxKey { txid: [42u8; 32] };
        let slot = idx.alloc_created(&k, 7);

        let mut buf = Vec::new();
        idx.serialize(&mut buf);

        // A real restart would call `Self::new`, which draws a fresh
        // process-random seed via `stripe_seed()` — deserialize must ignore
        // that entirely and install the persisted seed instead.
        let restored = ShardedMinedIndex::deserialize(&buf, 16)
            .expect("well-formed snapshot must deserialize");

        assert_eq!(
            restored.seed, original_seed,
            "deserialize must install the persisted seed, not a fresh process seed"
        );

        // Behavioral proof: a DIFFERENT seed routes this key to a different
        // shard selection function, so if `deserialize` had silently used a
        // fresh/different seed, `shard_for` on the restored index would very
        // likely disagree with the original's routing for this key.
        let differently_seeded = ShardedMinedIndex::new_with_seed(16, original_seed ^ 0xFFFF_FFFF);
        assert_ne!(
            differently_seeded.shard_for(&k),
            idx.shard_for(&k),
            "test setup invariant: the alternate seed must actually route differently \
             for this key, otherwise this test can't distinguish correct from buggy seeding"
        );

        assert_eq!(
            restored.shard_for(&k),
            idx.shard_for(&k),
            "routing must match the original index's routing"
        );
        restored.with_entry(&k, slot, |e| {
            assert_eq!(e.unmined_since, 7, "entry must be reachable after restore");
        });
    }

    #[test]
    fn deserialize_corrupt_free_list_fails_closed() {
        // Hand-craft a snapshot (rather than mutating a real one) so the
        // corrupt `free` slot's position is exact and independent of the
        // rest of the format.
        fn empty_shard_bytes() -> Vec<u8> {
            let mut b = Vec::new();
            b.extend_from_slice(&0u32.to_le_bytes()); // entries_len
            b.extend_from_slice(&0u32.to_le_bytes()); // free_len
            b.extend_from_slice(&0u32.to_le_bytes()); // overflow_len
            b
        }

        // One entry (so `entries.len() == 1`), and a free-list value of 99
        // which is out of bounds for it.
        let mut corrupt_shard = Vec::new();
        corrupt_shard.extend_from_slice(&1u32.to_le_bytes()); // entries_len = 1
        corrupt_shard.push(0); // live = false
        corrupt_shard.extend_from_slice(&0u32.to_le_bytes()); // block_id
        corrupt_shard.extend_from_slice(&0u32.to_le_bytes()); // block_height
        corrupt_shard.extend_from_slice(&0u32.to_le_bytes()); // subtree_idx
        corrupt_shard.extend_from_slice(&0u32.to_le_bytes()); // unmined_since
        corrupt_shard.push(0); // flags
        corrupt_shard.extend_from_slice(&1u32.to_le_bytes()); // free_len = 1
        corrupt_shard.extend_from_slice(&99u32.to_le_bytes()); // free[0] = 99, OOB
        corrupt_shard.extend_from_slice(&0u32.to_le_bytes()); // overflow_len

        let mut buf = Vec::new();
        buf.push(MINED_SNAPSHOT_VERSION);
        buf.extend_from_slice(&0u64.to_le_bytes()); // seed
        buf.extend_from_slice(&corrupt_shard);
        // `ShardedMinedIndex::new_with_seed(16, ..)` allocates
        // `16.next_power_of_two().max(16) == 16` shards; the corrupt one is
        // first, the rest are trivially empty and well-formed.
        for _ in 1..16 {
            buf.extend_from_slice(&empty_shard_bytes());
        }

        let err = ShardedMinedIndex::deserialize(&buf, 16)
            .err()
            .expect("an out-of-bounds free-list slot must fail closed, not panic on next alloc");
        assert!(
            matches!(err, MinedIndexError::Corrupt),
            "expected Corrupt, got {err:?}"
        );
    }
}
