# Phase 4: The setMined path

## Goal

Implement `setMined` (including `unsetMined` variant) as a complete, tested, concurrent operation. This is the second-hottest path after spend — called for every transaction in every new block.

## Dependencies

Phases 1-3 must be complete with all tests passing.

## Reference

- `specs/teranode.lua` lines 543-656 (`setMined`)
- `specs/BSV_UTXO_STORE_SPEC.md` §3.6 (setMined) and §2.5 (block entry overflow)

## What to build

### 4.1 SetMined operation — `src/ops/set_mined.rs`

```rust
pub struct SetMinedRequest {
    pub tx_key: TxKey,
    pub block_id: u32,
    pub block_height: u32,
    pub subtree_idx: u32,
    pub current_block_height: u32,
    pub block_height_retention: u32,
    pub on_longest_chain: bool,
    pub unset_mined: bool,         // true = remove this block entry
}

pub struct SetMinedResponse {
    pub signal: Signal,
    pub block_ids: Vec<u32>,       // current blockIDs after this operation
}
```

#### Implementation (matching Lua lines 543-656):

1. Acquire lock for `tx_key`
2. Index lookup → record_offset
3. If not found → TxNotFound
4. Read metadata from device
5. If `unset_mined`:
   a. Scan `block_entries[0..block_entry_count]` for matching `block_id`
   b. If found at index `i`: swap with last entry, decrement `block_entry_count` (O(1) removal — order doesn't matter, unlike the Lua code that does `list.remove` which is O(n))
   c. If not found: no-op
6. Else (set mined):
   a. Scan existing entries for duplicate `block_id`
   b. If not found: append `BlockEntry { block_id, block_height, subtree_idx }` at `block_entry_count`, increment count
   c. If found: no-op (already mined in this block)
   d. If `block_entry_count > INLINE_BLOCK_ENTRIES`: entries go to overflow extension block (see spec §2.5)
7. Update `unmined_since`:
   - If `block_entry_count > 0` AND `on_longest_chain`: set `unmined_since = 0` (mined)
   - If `block_entry_count == 0`: set `unmined_since = current_block_height`
8. Clear `LOCKED` flag if set
9. Evaluate deleteAtHeight (reuse from Phase 3)
10. Write metadata to device (only metadata region — NOT the UTXO slots)
11. If overflow needed (block_entry_count > INLINE_BLOCK_ENTRIES): allocate/write extension block, set block_overflow_offset
12. Release lock
13. Build response with current block_ids list and signal

### 4.2 MarkOnLongestChain operation — `src/ops/mark_longest_chain.rs`

This is a **separate operation** from `setMined` (see spec §3.15). It modifies only `unmined_since` without touching block entries. Called during chain reorganizations to bulk-update longest-chain status.

```rust
pub struct MarkOnLongestChainRequest {
    pub tx_key: TxKey,
    pub on_longest_chain: bool,
    pub current_block_height: u32,
    pub block_height_retention: u32,
}
```

#### Implementation (matching Go `longest_chain.go`):

1. Acquire lock for `tx_key`
2. Index lookup → record_offset
3. If not found → TxNotFound (**fatal** — indicates data corruption)
4. Read metadata from device
5. If `on_longest_chain == true`: set `unmined_since = 0`
6. If `on_longest_chain == false`: set `unmined_since = current_block_height`
7. Update unmined secondary index:
   - `true` → remove entry (tx is mined on longest chain)
   - `false` → insert/update entry with `current_block_height`
8. Evaluate `setDeleteAtHeight` (longest chain status affects DAH eligibility)
9. Write metadata to device (metadata region only)
10. Release lock

### 4.3 Collect block_ids helper

```rust
/// Extract the current list of block IDs from metadata.
pub fn collect_block_ids(metadata: &TxMetadata) -> Vec<u32> {
    metadata.block_entries[..metadata.block_entry_count as usize]
        .iter()
        .map(|e| e.block_id)
        .collect()
}
```

## Acceptance criteria

### setMined correctness tests

```
- [ ] setMined on non-existent tx: returns TxNotFound
- [ ] setMined with new blockID: block_entry_count increments, entry stored correctly
- [ ] setMined: response block_ids contains the new blockID
- [ ] setMined with same blockID twice: idempotent, count stays same, no duplicate entry
- [ ] setMined with 3 different blockIDs: all three stored, block_entry_count = 3
- [ ] setMined stores block_height and subtree_idx correctly for each entry
- [ ] setMined with 4+ entries: overflow extension block allocated, entries stored correctly
- [ ] setMined with overflow: read back all entries including overflow block
- [ ] setMined clears LOCKED flag: create locked record, setMined, verify LOCKED flag cleared
- [ ] setMined does NOT modify UTXO slots: read slots before and after, verify identical
```

### unsetMined correctness tests

```
- [ ] unsetMined removes existing blockID: block_entry_count decrements
- [ ] unsetMined removes correct entry: other entries remain unchanged
- [ ] unsetMined on non-existent blockID: no-op, no error
- [ ] unsetMined last block: block_entry_count = 0
- [ ] unsetMined from middle of 3 entries: remaining 2 entries correct
      (order may change due to swap-remove, that's fine)
- [ ] unsetMined does NOT modify UTXO slots
```

### unmined_since tests

```
- [ ] setMined on_longest_chain=true: unmined_since set to 0
- [ ] setMined on_longest_chain=false: unmined_since unchanged
- [ ] unsetMined leaving 0 blocks: unmined_since = current_block_height
- [ ] Multiple setMined on_longest_chain=true: unmined_since stays cleared
- [ ] setMined then unsetMined all blocks: unmined_since set to current height
```

### Signal/deleteAtHeight integration

```
- [ ] setMined on fully-spent tx on longest chain: signal includes DAHSET
- [ ] setMined on partially-spent tx: no DAH signal
- [ ] unsetMined leaving 0 blocks on fully-spent tx: DAH cleared, signal DAHUNSET
- [ ] setMined on external tx that's fully spent: signal = DeleteAtHeightSet
```

### Concurrency tests

```
- [ ] 10 threads calling setMined with different blockIDs on same tx:
      all succeed, block_entry_count matches number of unique blockIDs
- [ ] Concurrent setMined and spend on same tx: no corruption,
      both operations complete correctly
- [ ] Concurrent setMined and unsetMined for same blockID:
      final state is consistent (entry either present or absent, not corrupted)
```

### MarkOnLongestChain tests

```
- [ ] markOnLongestChain(true) on tx with unmined_since != 0: unmined_since becomes 0
- [ ] markOnLongestChain(false) on tx with unmined_since == 0: unmined_since set to current_block_height
- [ ] markOnLongestChain(true) on already longest-chain tx: no-op (unmined_since stays 0)
- [ ] markOnLongestChain(false) on already off-chain tx: unmined_since updated to new current height
- [ ] markOnLongestChain on non-existent tx: returns TxNotFound (fatal indicator)
- [ ] markOnLongestChain does NOT modify block_entries or UTXO slots
- [ ] markOnLongestChain(true) on fully-spent tx: evaluates DAH (may set delete_at_height)
- [ ] markOnLongestChain(false) on fully-spent tx with DAH set: clears delete_at_height
- [ ] Concurrent markOnLongestChain and setMined: no corruption
```

### Performance benchmarks

```
- [ ] Single-threaded setMined throughput: ops/sec
- [ ] setMined only writes metadata region: verify UTXO slots are not read/written
      (can check via I/O byte counters or by verifying read/write offsets)
- [ ] Measure metadata read + modify + write latency
- [ ] Single-threaded markOnLongestChain throughput: ops/sec
```

## NOT in this phase

- No batch setMined across multiple transactions (each call is for one tx)
- No networking
