# MinedIndex / MAX setMined — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decouple mined-state out of the on-device record into a dedicated in-RAM authoritative `MinedIndex`, making setMined a batched-WAL + in-RAM-slot write with zero data-device I/O.

**Architecture:** A new sharded `MinedIndex` (dense arena + free-list + height-bucket view) holds block-entries + `unmined_since` + an `all_spent` bit. The slim primary entry gains a 4-byte `mined_slot` pointer. setMined/WAL/replication go batch-native (`SetMinedBatch`). Pure store-auth: no mined-state on the device; recovery = MinedIndex snapshot + redo replay. Subsumes the unmined secondary index. Design spec: `specs/MINEDINDEX_SETMINED_DESIGN.md`.

**Tech Stack:** Rust, `parking_lot` locks, the existing `ShardedSecondary`/`hashmix`/`stripe_seed` sharding, the redo WAL (`src/redo.rs`), the replication protocol (`src/replication/protocol.rs`), the checkpoint snapshot (`src/checkpoint.rs`).

## Global Constraints

- No `unwrap()`/`expect()` in library code (tests only); no `todo!()`/`unimplemented!()`/`#[ignore]`; no vacuous asserts. (CLAUDE.md)
- Error types are `thiserror` enums; byte-layout structs are `#[repr(C, packed)]` with compile-time size asserts.
- `cargo clippy --all-targets -- -D warnings` clean; `cargo fmt --all -- --check` clean; also `cargo clippy --all-targets --features fault-injection -- -D warnings` (CI anti-bitrot step).
- Full gate before declaring a phase done: `cargo test --all`, `cargo test --manifest-path client/rust/Cargo.toml --all`, clippy (both feature sets), fmt.
- Base branch: `feat/mined-index-setmined` off `main@f00ad34` (slim pure-locator index). Work TDD; commit per task.
- Consensus-adjacent: every replay/recovery/replication task carries an idempotency + crash-window test.

## File Structure

| File | Responsibility | Action |
|---|---|---|
| `src/index/mined_index.rs` | The `MinedIndex` type: sharded dense arenas, free-list, height buckets, slot CRUD, range queries, snapshot ser/de | Create |
| `src/index/mod.rs` | `pub mod mined_index;` + re-exports (`MinedEntry`, `MinedSlot`, `ShardedMinedIndex`) | Modify |
| `src/index/hashtable.rs` | Add `mined_slot: u32` to `TxIndexEntry` (`f00ad34` made it `{device_id, record_offset}`) | Modify |
| `src/record.rs` | Remove the on-device block-entry region + `block_entry_count` from `TxMetadata` (pure store-auth) | Modify (late) |
| `src/redo.rs` | New `RedoOp::SetMinedBatch` (opcode 22) + encode/decode/replay-dispatch; retire `SetMined` | Modify |
| `src/recovery.rs` | `replay_set_mined_batch` targets the MinedIndex, not the device | Modify |
| `src/replication/protocol.rs` | New `ReplicaOp::SetMinedBatch`; retire per-tx `SetMined` | Modify |
| `src/replication/receiver.rs` | Apply `SetMinedBatch` into the MinedIndex | Modify |
| `src/cluster/coordinator.rs` | Migration ships mined-state from the MinedIndex (was device block-entries, ~5811) | Modify |
| `src/ops/engine.rs` | Hold `mined_index`; rewrite `set_mined_inner`→batch; `all_spent` on spend; slot lifecycle on create/delete; reroute readers | Modify |
| `src/ops/delete_eval.rs` | Read mined facts from the slot, `all_spent` from the slot | Modify |
| `src/server/dispatch.rs` | `handle_set_mined_batch`: one `SetMinedBatch` redo + per-target replica op; GET reads block fields from the MinedIndex | Modify |
| `src/checkpoint.rs` | Snapshot/restore the MinedIndex section | Modify |

---

## Phase 1 — `MinedIndex` module (isolated, greenfield)

### Task 1: `MinedEntry` + single-shard arena with CRUD

**Files:**
- Create: `src/index/mined_index.rs`
- Modify: `src/index/mod.rs` (add `pub mod mined_index;`)
- Test: inline `#[cfg(test)] mod tests` in `src/index/mined_index.rs`

**Interfaces:**
- Produces:
  - `pub struct MinedEntry { pub block_id: u32, pub block_height: u32, pub subtree_idx: u32, pub unmined_since: u32, pub flags: u8 }` with `const MINED_ALL_SPENT: u8 = 1; const MINED_HAS_OVERFLOW: u8 = 2;`
  - `struct MinedShard { entries: Vec<MinedEntry>, free: Vec<u32>, overflow: HashMap<u32, Vec<BlockEntry>>, unmined: HashMap<u32, HashSet<u32>> }`
  - `impl MinedShard`: `fn alloc(&mut self, e: MinedEntry) -> u32`, `fn free_slot(&mut self, slot: u32)`, `fn get(&self, slot: u32) -> Option<&MinedEntry>`, `fn get_mut(&mut self, slot: u32) -> Option<&mut MinedEntry>`
  - Reuse `crate::record::BlockEntry`.

- [ ] **Step 1: Write the failing test** (in `src/index/mined_index.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn entry(block_id: u32) -> MinedEntry {
        MinedEntry { block_id, block_height: 100, subtree_idx: 0, unmined_since: 0, flags: 0 }
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
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib index::mined_index::tests::alloc_get_free_reuses_slot`
Expected: FAIL — `MinedShard`/`MinedEntry` not found.

- [ ] **Step 3: Write minimal implementation** (top of `src/index/mined_index.rs`)

```rust
//! Dedicated authoritative in-RAM mined-state store. See
//! `specs/MINEDINDEX_SETMINED_DESIGN.md`. Replaces on-device block entries +
//! the unmined secondary index.
use std::collections::{HashMap, HashSet};
use crate::record::BlockEntry;

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
struct MinedShard {
    entries: Vec<MinedEntry>,
    /// `true` at index i == entry i is live; used so freed slots read as absent.
    live: Vec<bool>,
    free: Vec<u32>,
    overflow: HashMap<u32, Vec<BlockEntry>>,
    unmined: HashMap<u32, HashSet<u32>>,
}

impl MinedShard {
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

    fn free_slot(&mut self, slot: u32) {
        if (slot as usize) < self.live.len() && self.live[slot as usize] {
            self.live[slot as usize] = false;
            self.overflow.remove(&slot);
            self.free.push(slot);
        }
    }

    fn get(&self, slot: u32) -> Option<&MinedEntry> {
        match self.live.get(slot as usize) {
            Some(true) => self.entries.get(slot as usize),
            _ => None,
        }
    }

    fn get_mut(&mut self, slot: u32) -> Option<&mut MinedEntry> {
        match self.live.get(slot as usize) {
            Some(true) => self.entries.get_mut(slot as usize),
            _ => None,
        }
    }
}
```

Add to `src/index/mod.rs`: `pub mod mined_index;` (keep alphabetical with the other `pub mod` lines).

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib index::mined_index::tests::alloc_get_free_reuses_slot`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/index/mined_index.rs src/index/mod.rs
git commit -m "feat(mined-index): MinedEntry + single-shard arena with free-list CRUD"
```

### Task 2: height-bucket view (subsumes the unmined index) + range query

**Files:**
- Modify: `src/index/mined_index.rs`

**Interfaces:**
- Consumes: `MinedShard` (Task 1).
- Produces: `impl MinedShard`: `fn set_unmined(&mut self, slot: u32, old_height: u32, new_height: u32)` (moves bucket membership; height 0 == not bucketed) and `fn unmined_below(&self, height: u32, out: &mut Vec<u32>)` (collect slots with `unmined_since` in `1..height`).

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn height_buckets_track_unmined_and_range_query() {
    let mut s = MinedShard::default();
    let a = s.alloc(MinedEntry { unmined_since: 5, ..Default::default() });
    s.set_unmined(a, 0, 5); // enter bucket 5
    let b = s.alloc(MinedEntry { unmined_since: 9, ..Default::default() });
    s.set_unmined(b, 0, 9);
    let mut out = Vec::new();
    s.unmined_below(8, &mut out); // want slots with unmined_since in 1..8 => only `a`
    assert_eq!(out, vec![a]);
    s.set_unmined(a, 5, 0); // mined: leave the bucket
    out.clear();
    s.unmined_below(100, &mut out);
    assert_eq!(out, vec![b], "mined slot left its bucket");
}
```

- [ ] **Step 2: Run — expect FAIL** (`set_unmined`/`unmined_below` missing).
Run: `cargo test --lib index::mined_index::tests::height_buckets_track_unmined_and_range_query`

- [ ] **Step 3: Implement** (add to `impl MinedShard`)

```rust
fn set_unmined(&mut self, slot: u32, old_height: u32, new_height: u32) {
    if old_height == new_height { return; }
    if old_height != 0 {
        if let Some(set) = self.unmined.get_mut(&old_height) {
            set.remove(&slot);
            if set.is_empty() { self.unmined.remove(&old_height); }
        }
    }
    if new_height != 0 {
        self.unmined.entry(new_height).or_default().insert(slot);
    }
}

fn unmined_below(&self, height: u32, out: &mut Vec<u32>) {
    for (&h, set) in &self.unmined {
        if h < height {
            out.extend(set.iter().copied());
        }
    }
    out.sort_unstable(); // deterministic order for tests + downstream batching
}
```

- [ ] **Step 4: Run — expect PASS.**
- [ ] **Step 5: Commit**

```bash
git add src/index/mined_index.rs
git commit -m "feat(mined-index): height-bucket view + unmined_below range query"
```

### Task 3: `ShardedMinedIndex` (txid → shard routing) + high-level ops

**Files:**
- Modify: `src/index/mined_index.rs`, `src/index/mod.rs` (re-export `ShardedMinedIndex`, `MinedEntry`, `NO_MINED_SLOT`)

**Interfaces:**
- Consumes: `MinedShard`, `crate::locks::stripe_seed`, `crate::index::hashmix::splitmix64_finalize`, `crate::index::TxKey`.
- Produces:
  - `pub struct ShardedMinedIndex { shards: Box<[parking_lot::Mutex<MinedShard>]>, mask: usize, seed: u64 }`
  - `pub fn new(shard_count: usize) -> Self` (pow2, floor 16, matching `VisibilityBarrier::new`)
  - `pub fn shard_for(&self, key: &TxKey) -> usize` (bytes 16..24 through `splitmix64_finalize`, the `VisibilityBarrier`/`StripedLocks` scheme)
  - `pub fn alloc_created(&self, key: &TxKey, block_height: u32) -> u32` → returns the global slot handle `(shard << 32 | slot)` packed as `u64`? — **No**: keep slot local to the shard and store `(shard_is_implied_by_key)`. The primary entry stores only the **local** `slot: u32`; the shard is always re-derived from the txid on access. Document this invariant.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn sharded_alloc_and_lookup_by_key_roundtrip() {
    use crate::index::TxKey;
    let idx = ShardedMinedIndex::new(16);
    let k = TxKey { txid: [7u8; 32] };
    let slot = idx.alloc_created(&k, 42);
    idx.with_entry(&k, slot, |e| {
        assert_eq!(e.unmined_since, 42, "created tx is unmined at its block height");
        assert_eq!(e.block_id, 0);
    });
    // range query sees it as unmined below 100
    let mut out = Vec::new();
    idx.collect_unmined_below(100, &mut out);
    assert!(out.iter().any(|&(s, sl)| sl == slot), "created tx appears in unmined range");
}
```

- [ ] **Step 2: Run — expect FAIL.**
Run: `cargo test --lib index::mined_index::tests::sharded_alloc_and_lookup_by_key_roundtrip`

- [ ] **Step 3: Implement** (append to `src/index/mined_index.rs`)

```rust
use crate::index::TxKey;

pub struct ShardedMinedIndex {
    shards: Box<[parking_lot::Mutex<MinedShard>]>,
    mask: usize,
    seed: u64,
}

impl ShardedMinedIndex {
    pub fn new(shard_count: usize) -> Self {
        let count = shard_count.next_power_of_two().max(16);
        let shards = (0..count)
            .map(|_| parking_lot::Mutex::new(MinedShard::default()))
            .collect::<Vec<_>>();
        Self { shards: shards.into_boxed_slice(), mask: count - 1, seed: crate::locks::stripe_seed() }
    }

    #[inline]
    pub fn shard_for(&self, key: &TxKey) -> usize {
        let raw = u64::from_le_bytes(key.txid[16..24].try_into().unwrap_or([0u8; 8]));
        (crate::index::hashmix::splitmix64_finalize(raw ^ self.seed) as usize) & self.mask
    }

    /// Create a slot for a freshly-created (unmined) tx. Returns the shard-local slot
    /// to store in the primary entry's `mined_slot`.
    pub fn alloc_created(&self, key: &TxKey, block_height: u32) -> u32 {
        let mut sh = self.shards[self.shard_for(key)].lock();
        let slot = sh.alloc(MinedEntry { unmined_since: block_height, ..Default::default() });
        sh.set_unmined(slot, 0, block_height);
        slot
    }

    pub fn with_entry<R>(&self, key: &TxKey, slot: u32, f: impl FnOnce(&MinedEntry) -> R) -> Option<R> {
        let sh = self.shards[self.shard_for(key)].lock();
        sh.get(slot).map(f)
    }

    /// Range query over ALL shards. Returns `(shard, slot)` pairs.
    pub fn collect_unmined_below(&self, height: u32, out: &mut Vec<(usize, u32)>) {
        for (si, shard) in self.shards.iter().enumerate() {
            let mut local = Vec::new();
            shard.lock().unmined_below(height, &mut local);
            out.extend(local.into_iter().map(|sl| (si, sl)));
        }
    }
}
```

- [ ] **Step 4: Run — expect PASS.**
- [ ] **Step 5: Commit**

```bash
git add src/index/mined_index.rs src/index/mod.rs
git commit -m "feat(mined-index): ShardedMinedIndex — txid routing, alloc_created, range query"
```

### Task 4: apply_set_mined / apply_unset (the mutation the hot path + replay share)

**Files:** Modify `src/index/mined_index.rs`.

**Interfaces:**
- Produces `impl ShardedMinedIndex`:
  - `pub fn apply_set_mined(&self, key: &TxKey, slot: u32, block_id: u32, block_height: u32, subtree_idx: u32, on_longest_chain: bool) -> MinedApplyResult` — idempotent; adds the block tuple (inline or overflow), clears the unmined bucket when `on_longest_chain`, returns whether it was a real transition + the post-state `unmined_since`.
  - `pub fn apply_unset(&self, key: &TxKey, slot: u32, block_id: u32, current_height: u32)`
  - `pub struct MinedApplyResult { pub changed: bool, pub new_unmined_since: u32 }`
- Test: idempotency (double-apply same block_id == no-op), overflow (4th block spills), unset-to-zero restores `unmined_since`.

*(Full test + impl code: mirror `set_mined_inner`'s inline/overflow/dedup logic from `src/ops/engine.rs:3929-4083`, but against `MinedShard.entries[slot]` + `overflow` instead of `TxMetadata`. Dedup on `block_id`. On `on_longest_chain` set `unmined_since=0` and call `set_unmined(slot, old, 0)`. Write the three tests first, run FAIL, implement, run PASS, commit `feat(mined-index): apply_set_mined/apply_unset with dedup, overflow, unset-restore`.)*

### Task 5: snapshot serialize/deserialize (versioned)

**Files:** Modify `src/index/mined_index.rs`.

**Interfaces:**
- Produces: `pub fn serialize(&self, out: &mut Vec<u8>)` and `pub fn deserialize(bytes: &[u8], shard_count: usize) -> Result<Self, MinedIndexError>` with a version byte; round-trips entries + free-list + overflow + buckets. `#[derive(thiserror::Error)] pub enum MinedIndexError { ... }`.
- Test: `serialize`→`deserialize` reproduces `get`/`collect_unmined_below` for a populated index; version-mismatch fails closed.

*(Write the round-trip + version-fail tests first. Commit `feat(mined-index): versioned snapshot ser/de`.)*

**Phase 1 gate:** `cargo test --lib index::mined_index`, clippy (both feature sets), fmt. The module is fully self-contained and green before any engine wiring.

---

## Phase 2 — primary `mined_slot` + slot lifecycle

### Task 6: add `mined_slot` to `TxIndexEntry`

**Files:**
- Modify: `src/index/hashtable.rs` (`TxIndexEntry` @ ~line 111 is `{device_id, record_offset}` post-`f00ad34`) — add `pub mined_slot: u32` (init `NO_MINED_SLOT`), update the bucket ser/de + the 20 B→24 B size assert, update every `TxIndexEntry { .. }` literal (grep `TxIndexEntry {`).
- Test: existing `index::` tests still pass; add one asserting `mined_slot` round-trips through register→lookup and survives a rehash/resize.

*(This is the "re-touch the slim locator" task — coordinate. Write the round-trip+resize test, run FAIL, add the field + fix all literals + the size assert, run PASS. Commit `feat(index): add mined_slot pointer to the primary locator entry`.)*

### Task 7: engine holds `ShardedMinedIndex`; create allocs a slot, delete frees it

**Files:**
- Modify: `src/ops/engine.rs` — add `mined_index: ShardedMinedIndex` to `Engine` (@115), all constructors (`Engine::new`, `new_with_sharded_index`, `new_multi_store`), a `pub fn mined_index(&self) -> &ShardedMinedIndex`. In the create path (`register_create_at_offset`/`create_at_offset`) call `alloc_created` and store the returned slot in the `TxIndexEntry.mined_slot`. In `delete_inner` call `free_slot` + clear the primary `mined_slot`.
- Test: `tests/` or inline — create a tx → `mined_index` has an unmined slot at the tx's block height; delete → slot freed (range query no longer returns it).

*(Write the create/delete lifecycle test first. Commit `feat(engine): wire ShardedMinedIndex; create allocs a mined slot, delete frees it`.)*

**Phase 2 gate:** full `cargo test --all` green (mined_index populated but not yet authoritative — still parallel to the device block-entries; no behavior change yet).

---

## Phase 3 — setMined onto the MinedIndex + `all_spent` on spend

### Task 8: `all_spent` bit maintained by the spend path

**Files:** Modify `src/ops/engine.rs` — in `PreparedSpend::apply_locked` (@8433), after the spent counter update, when `new_spent == utxo_count` set `MINED_ALL_SPENT` in the tx's MinedIndex slot (via `engine.mined_index().set_all_spent(&tx_key, slot, true)` — add that method in Task 4/8).
- Test: spend the last UTXO → the slot's `all_spent` bit is set; a partial spend does not set it.

*(Commit `feat(spend): maintain all_spent bit in the MinedIndex slot on last spend`.)*

### Task 9: rewrite `set_mined_inner` to mutate the MinedIndex (RAM), not the device

**Files:** Modify `src/ops/engine.rs` (`set_mined_inner` @3748). Replace the device read + RMW + `write_metadata_fast` with: primary lookup → `mined_index.apply_set_mined(...)` → DAH eval from the slot (`all_spent` + mined-state, no device read, §10) → update the DAH secondary index only on transition. Return block_ids from the slot.
- Test: setMined a created tx → `GET`-equivalent block_ids come from the slot; the device record is **not written** (assert via a write-counting device wrapper); DAH unchanged when `!all_spent`; DAH set when `all_spent`.

*(This is the core hot-path task. Keep it per-key first — batching is Task 11. Commit `feat(setmined): apply mined-state to the MinedIndex in RAM; zero device write`.)*

### Task 10: reroute readers — GET + delete_eval read the slot

**Files:**
- Modify `src/server/dispatch.rs` `decorate_get_item` (@8180): the `BLOCK_ENTRIES/BLOCK_ENTRY_COUNT/UNMINED_SINCE/BLOCK_ENTRIES_ALL` branches read the MinedIndex slot (via the primary entry's `mined_slot`) instead of `meta.block_entry_count`/`meta.block_entries_inline`.
- Modify `src/ops/delete_eval.rs` (@116): `has_blocks`/`on_longest_chain` from the slot; keep `spent_utxos` from the device record.
- Test: `GET(BLOCK_ENTRIES)` after setMined returns the tuples from the slot; `delete_eval` produces the same DAH decision as the pre-change baseline across create/setMined/spend orders.

*(Commit `feat(read): GET + delete_eval read mined-state from the MinedIndex`.)*

**Phase 3 gate:** `cargo test --all`. setMined now writes RAM only; readers merged. The device block-entry region is now write-dead (removed in Phase 6).

---

## Phase 4 — batch WAL (`SetMinedBatch`)

### Task 11: `RedoOp::SetMinedBatch` — encode/decode + dispatch batch redo

**Files:**
- Modify `src/redo.rs`: add `const OP_SET_MINED_BATCH: u8 = 22;` and `RedoOp::SetMinedBatch { block_id, block_height, subtree_idx, on_longest_chain, current_block_height, block_height_retention, unset: bool, txids: Vec<TxKey> }`; encode = shared fields + `[u32 count][txids]`; decode inverse; retire `SetMined`/`OP_SET_MINED` (greenfield).
- Modify `src/server/dispatch.rs` `handle_set_mined_batch` (@5476): build **one** `SetMinedBatch` from `valid_items` (split per store via the existing per-store redo routing) instead of N `SetMined`.
- Test (redo.rs): `SetMinedBatch` encode→decode round-trip incl. empty + multi txids; (dispatch) a batch writes exactly one redo entry per store.

*(Commit `feat(redo): SetMinedBatch op + batch redo in handle_set_mined_batch`.)*

### Task 12: `replay_set_mined_batch` targets the MinedIndex

**Files:** Modify `src/recovery.rs` (`replay_set_mined` @2055 → `replay_set_mined_batch`): iterate txids, `mined_index.apply_set_mined/apply_unset` per key; idempotent. Wire into the replay dispatch (@1027/1661).
- Test: replay a `SetMinedBatch` reproduces the live MinedIndex state; double-replay is a no-op (idempotency); a crash mid-batch (replay a prefix then the full op) converges.

*(Commit `feat(recovery): replay SetMinedBatch into the MinedIndex, idempotent`.)*

### Task 13: checkpoint snapshot of the MinedIndex

**Files:** Modify `src/checkpoint.rs`: add a versioned MinedIndex section using `ShardedMinedIndex::serialize`/`deserialize` (Task 5); restore rebuilds it; the primary `mined_slot` pointers are restored with the primary snapshot (or rebuilt from the MinedIndex on load — pick one and assert it in a test).
- Test: checkpoint → drop → restore reproduces `get`/range-query; a full recovery (snapshot + `SetMinedBatch` redo replay) reproduces the pre-crash live state exactly.

*(Commit `feat(checkpoint): snapshot + restore the MinedIndex; recovery = snapshot + redo`.)*

**Phase 4 gate:** `cargo test --all` incl. `--features fault-injection --test crash_sweep_ops` and the recovery suites. This closes the durability contract (§12).

---

## Phase 5 — batch replication + migration

### Task 14: `ReplicaOp::SetMinedBatch` — protocol + per-target split + receiver apply

**Files:**
- Modify `src/replication/protocol.rs` (@125): add `SetMinedBatch { block_id, block_height, subtree_idx, on_longest_chain, current_block_height, block_height_retention, txids: Vec<TxKey> }` + its wire ser/de + the `tx_key`-matching arms (@259/289 group by key → now group a batch's keys); retire per-tx `SetMined`.
- Modify `src/server/dispatch.rs` `handle_set_mined_batch` replication: build the batch, let `build_replication_targets` (@2315) route — a `SetMinedBatch` is split per target addr carrying only that target's owned txids.
- Modify `src/replication/receiver.rs` (@1555): apply `SetMinedBatch` into the MinedIndex.
- Test: master applies a cross-shard batch, each replica converges on its owned subset; a partially-mis-routed batch redirects the rest per-item.

*(Commit `feat(replication): SetMinedBatch — per-target split + receiver apply`.)*

### Task 15: migration ships mined-state from the MinedIndex

**Files:** Modify `src/cluster/coordinator.rs` (@5811, "Replay block entries"): read the migrating shard's mined-state from the `MinedIndex` (per txid slot) and ship as `SetMinedBatch`, instead of reading `meta.block_entries_inline` from the device record.
- Test: migrate a shard whose txs are mined → the receiver's MinedIndex converges store-to-store; the device record (which no longer carries block-entries) is not consulted.

*(Commit `feat(cluster): migrate mined-state store-to-store from the MinedIndex`.)*

**Phase 5 gate:** the cluster integration suites (`tests/cluster_*.rs`, `tests/segment_cluster_e2e.rs`) — note loopback-bind is sandbox-blocked locally; rely on CI + the in-process convergence tests.

---

## Phase 6 — drop on-device mined-state + the unmined index

### Task 16: remove the block-entry region from `TxMetadata` + the unmined secondary index

**Files:**
- Modify `src/record.rs`: remove `block_entry_count` + `block_entries_inline` + overflow plumbing from `TxMetadata` (pure store-auth); update the size assert + `to_bytes`/`from_bytes`.
- Modify `src/ops/engine.rs`: remove `unmined_index`; route its range-query callers (`ProcessExpiredPreservations`, DAH sweeps) to `mined_index.collect_unmined_below`.
- Remove `RedoOp::SecondaryUnminedUpdate` handling (the unmined index is gone; the MinedIndex is redo-durable via `SetMinedBatch`).
- Test: the full suite; a preservation-expiry sweep uses the MinedIndex range query and matches prior behavior.

*(This is the largest deletion — do it only after Phases 3–5 are green so nothing still reads the device block-entries. Commit `refactor: remove on-device block-entries + the unmined secondary index (pure store-auth)`.)*

**Phase 6 gate:** full `cargo test --all` + both clippy feature sets + fmt + `git grep block_entries_inline` returns only MinedIndex/overflow/UI-read sites.

---

## Phase 7 — perf validation + adversarial review

### Task 17: setMined burst perf harness (before/after) + zero-device-write assertion

**Files:** a dispatch-level burst test (pattern: the `perf_measure_setmined_burst` harness used in analysis) driving `OP_SET_MINED_BATCH` over N created txs through a `CachingDevice`-wrapped `MemoryDevice`, asserting (a) ns/tx improved to the ~200–400 ns target and (b) **zero data-device writes** via a write-counting inner device.
- Commit `test(perf): setMined burst — assert RAM-only hot path + throughput target`.

### Task 18: adversarial recovery/consensus review

Dispatch the review personas (per `~/.claude/REVIEW.md`: `bitcoin-expert` + `security-auditor` for the consensus/mined-state correctness; `qa-expert` for the recovery/idempotency/crash-window matrix). Focus: pure-store-auth recovery has no device fallback (§12); double-spend/finality implications of mined-state living only in RAM+redo; migration convergence.

---

## Self-Review (completed inline against the spec)

- **Coverage:** D1 pure store-auth → Tasks 9,13,16; D2 slim base → Task 6; D3 subsume unmined → Tasks 2,16; D4 slot pointer → Tasks 3,6,7; D5 batch WAL+repl → Tasks 11,14; D6 subtree_idx in slot → Task 1. §10 DAH/all_spent → Tasks 8,9. §11 cluster/migration → Tasks 14,15. §12 recovery → Tasks 12,13. Every spec section maps to a task.
- **Sequencing safety:** device block-entries are removed (Task 16) only after every reader is rerouted (Task 10) and the store is durable (Tasks 12,13) and replicated/migrated (Tasks 14,15) — no window where mined-state is unreadable.
- **Type consistency:** `MinedEntry`, `NO_MINED_SLOT`, `apply_set_mined`, `collect_unmined_below`, `SetMinedBatch` (redo + replica) are named identically across tasks.
