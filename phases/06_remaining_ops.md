# Phase 6: Remaining operations

**Status:** shipped — `src/ops/remaining.rs` (freeze/unfreeze/reassign/setConflicting/setLocked/preserveUntil) and `src/ops/delete_eval.rs` in main; F-G2-020 (`delete()` perf) is a P3 optimisation, not a correctness gap.

## Goal

Implement all remaining mutation operations: `freeze`, `unfreeze`, `reassign`, `setConflicting`, `setLocked`, `preserveUntil`, and record deletion/pruning. These are lower-frequency than spend/setMined but must be fully correct.

## Dependencies

Phases 1-5 must be complete with all tests passing.

## Reference

- `specs/BSV_UTXO_STORE_SPEC.md` §3.7-3.12 (freeze, unfreeze, reassign, setConflicting, setLocked, preserveUntil), §3.16 (getSpend)
- `specs/teranode.lua` for original Lua validation logic

## What to build

### 6.1 Freeze — `src/ops/freeze.rs`

Matching Lua lines 666-738.

```rust
pub struct FreezeRequest {
    pub tx_key: TxKey,
    pub offset: u32,
    pub utxo_hash: [u8; 32],
}
```

Steps:
1. Lock, index lookup, read metadata + slot
2. Validate: record exists, UTXO slot exists, hash matches
3. If already frozen: return AlreadyFrozen error
4. If spent (not frozen): return AlreadySpent error with spending_data hex
5. If unspent: verify status == UNSPENT (0x00)
6. Write slot: status = FROZEN (0xFF), spending_data = all 0xFF (36 bytes of 0xFF)
7. Write slot to device
8. Metadata is NOT modified (no counter change — frozen doesn't count as "spent")

### 6.2 Unfreeze — `src/ops/unfreeze.rs`

Matching Lua lines 748-811.

Steps:
1. Lock, index lookup, read metadata + slot
2. Validate: record exists, slot exists, hash matches
3. Must be frozen (status == FROZEN, 0xFF)
4. If not frozen: return NotFrozen error
5. Write slot: status = UNSPENT, spending_data = zeroed
6. Write slot to device

### 6.3 Reassign — `src/ops/reassign.rs`

Matching Lua lines 823-911. This replaces a frozen UTXO's hash with a new hash.

```rust
pub struct ReassignRequest {
    pub tx_key: TxKey,
    pub offset: u32,
    pub utxo_hash: [u8; 32],      // current (old) hash
    pub new_utxo_hash: [u8; 32],  // replacement hash
    pub block_height: u32,
    pub spendable_after: u32,      // blocks after block_height before this UTXO is spendable
}
```

Steps:
1. Lock, index lookup, read metadata + slot
2. Validate: exists, hash matches current hash
3. Slot must be frozen (status == FROZEN)
4. Write slot: hash = new_utxo_hash, status = UNSPENT (0x00), `spending_data[0..4] = (block_height + spendable_after) as u32 LE`, rest zeroed
   — The spendable height is encoded directly in the slot's spending_data field (see spec §2.4)
5. Append to reassignment extension block (audit trail):
   - If `reassignment_offset == 0`: allocate extension block from freelist, set `reassignment_offset`
   - Append `ReassignmentEntry` to the extension block
6. Write metadata (to update `reassignment_offset` if first reassign)

**Note on all-spent check:** Reassign does NOT increment any counter. Freeze doesn't increment `spent_utxos`, so after reassign (frozen → unspent), `spent_utxos` is still less than `utxo_count`. The all-spent check (`spent_utxos == utxo_count`) naturally remains false until the reassigned UTXO is spent.

**Note on spendable height:** The restriction is cleared naturally when the UTXO is spent (spending_data overwritten with txid+vin) or unspent during a reorg (spending_data zeroed, spendable_height becomes 0 = immediately spendable).

### 6.4 SetConflicting — `src/ops/set_conflicting.rs`

Matching Lua lines 1025-1051.

```rust
pub struct SetConflictingRequest {
    pub tx_key: TxKey,
    pub value: bool,
    pub current_block_height: u32,
    pub block_height_retention: u32,
}
```

Steps:
1. Lock, index lookup, read metadata
2. Validate: record exists
3. Set/clear `CONFLICTING` flag in `flags` byte
4. Evaluate deleteAtHeight
5. Write metadata

### 6.5 SetLocked — `src/ops/set_locked.rs`

Matching Lua lines 1109-1135.

```rust
pub struct SetLockedRequest {
    pub tx_key: TxKey,
    pub value: bool,
}

pub struct SetLockedResponse {
    // No child_count — pagination is eliminated.
}
```

Steps:
1. Lock, index lookup, read metadata
2. Validate: record exists
3. Set/clear `LOCKED` flag in `flags` byte
4. If locking (value=true) and `delete_at_height != 0`: set `delete_at_height = 0`
5. Write metadata

### 6.6 PreserveUntil — `src/ops/preserve_until.rs`

Matching Lua lines 1067-1095.

```rust
pub struct PreserveUntilRequest {
    pub tx_key: TxKey,
    pub block_height: u32,
}
```

Steps:
1. Lock, index lookup, read metadata
2. Validate: record exists
3. Set `delete_at_height = 0`
4. Set preserve_until = block_height
5. Write metadata
6. If external tx: signal PRESERVE

### 6.7 Record deletion / pruning — `src/ops/delete.rs`

```rust
pub struct DeleteRequest {
    pub tx_key: TxKey,
}
```

Steps:
1. Lock, index lookup
2. If not found: return TxNotFound (or no-op)
3. Get record size from index entry
4. Optionally: overwrite the magic number to invalidate the record on disk
5. Free the allocation via `allocator.free(offset, size)`
6. Remove from index
7. Release lock

### 6.8 GetSpend — `src/ops/get_spend.rs`

Point read of a single UTXO slot plus the record's locktime. Used for double-spend detection — the validator needs to check "is this output already spent? if so, by whom?" See spec §3.16.

```rust
pub struct GetSpendRequest {
    pub tx_key: TxKey,
    pub offset: u32,
    pub utxo_hash: [u8; 32],
}

pub struct GetSpendResponse {
    pub status: u8,                     // UTXO status byte
    pub spending_data: Option<[u8; 36]>, // present if spent or frozen
    pub locktime: u32,                  // from record metadata
}
```

Steps:
1. Index lookup → record_offset
2. If not found → TxNotFound
3. Read metadata (for locktime and utxo_count)
4. Validate offset < utxo_count → UTXO_NOT_FOUND
5. Read UTXO slot at `record_offset + METADATA_SIZE + offset * 69`
6. Validate hash matches utxo_hash → UTXO_HASH_MISMATCH
7. Return status, spending_data (if present), locktime

No lock needed — this is a read-only operation. Consistency guaranteed by the atomic metadata+slot write ordering in spend.

### 6.9 IncrementSpentExtraRecs — ELIMINATED

Document why this operation is eliminated: TeraSlab doesn't split transactions across multiple records. The entire master/child/extra-recs machinery is gone. The `spent_utxos` counter on the single record is sufficient.

If the Go client sends this operation, the server should accept and ignore it (return OK). Add a compatibility shim:

```rust
pub fn increment_spent_extra_recs_compat(/* ... */) -> Result<Response> {
    // No-op in TeraSlab — pagination is eliminated.
    // No-op — pagination is eliminated in TeraSlab.
    Ok(Response::ok())
}
```

## Acceptance criteria

### Freeze tests

```
- [ ] Freeze unspent UTXO: status becomes FROZEN, spending_data all 0xFF
- [ ] Freeze already-frozen UTXO: returns AlreadyFrozen
- [ ] Freeze already-spent UTXO: returns AlreadySpent with spending_data hex
- [ ] Freeze non-existent tx: returns TxNotFound
- [ ] Freeze with wrong hash: returns UtxoHashMismatch
- [ ] Freeze does NOT change spent_utxos counter
- [ ] After freeze, spend attempt returns Frozen
```

### Unfreeze tests

```
- [ ] Unfreeze frozen UTXO: status becomes UNSPENT, spending_data zeroed
- [ ] Unfreeze non-frozen UTXO: returns NotFrozen
- [ ] Unfreeze unspent UTXO: returns NotFrozen
- [ ] After unfreeze, UTXO is spendable again
```

### Reassign tests

```
- [ ] Reassign frozen UTXO: hash changes to new hash, status = UNSPENT
- [ ] Reassign non-frozen UTXO: returns NotFrozen
- [ ] Reassign with wrong current hash: returns HashMismatch
- [ ] After reassign, slot spending_data[0..4] contains correct spendable height
- [ ] After reassign, UTXO is not spendable until spendable_after blocks
- [ ] After spendable_after blocks pass, UTXO is spendable with new hash
- [ ] Spend with old hash after reassign: returns HashMismatch
- [ ] Spend with new hash after reassign + wait: succeeds
```

### SetConflicting tests

```
- [ ] Set conflicting=true: flag set, deleteAtHeight evaluated
- [ ] Set conflicting=true triggers DAH: signal DAHSET
- [ ] Set conflicting=false: flag cleared
- [ ] Conflicting tx blocks spend (without ignoreConflicting)
```

### SetLocked tests

```
- [ ] Set locked=true: flag set
- [ ] Set locked=true clears existing deleteAtHeight
- [ ] Set locked=false: flag cleared
- [ ] Locked tx blocks spend (without ignoreLocked)
```

### PreserveUntil tests

```
- [ ] Set preserveUntil: value stored, deleteAtHeight cleared
- [ ] PreserveUntil blocks DAH evaluation on subsequent spend
- [ ] External tx: signal PRESERVE returned
```

### Deletion tests

```
- [ ] Delete existing record: removed from index, space freed
- [ ] Delete then lookup: returns None
- [ ] Delete then allocate new record: can reuse freed space
- [ ] Delete non-existent record: no-op or TxNotFound
- [ ] Concurrent delete and spend on same tx: one succeeds, other gets TxNotFound
```

### GetSpend tests

```
- [ ] GetSpend on unspent UTXO: returns status=0x00, spending_data=None, correct locktime
- [ ] GetSpend on spent UTXO: returns status=0x01, spending_data=Some(txid+vin), correct locktime
- [ ] GetSpend on frozen UTXO: returns status=0xFF, spending_data=Some(all 0xFF)
- [ ] GetSpend on pruned UTXO: returns status=0x02
- [ ] GetSpend non-existent tx: returns TxNotFound
- [ ] GetSpend with wrong hash: returns UtxoHashMismatch
- [ ] GetSpend with offset >= utxo_count: returns UtxoNotFound
- [ ] GetSpend is read-only: does not modify any state
```

### Compatibility tests

```
- [ ] incrementSpentExtraRecs compatibility shim: returns OK, no state change
```

## NOT in this phase

- No networking
- No replication
- No tiered storage (external blob handling)
