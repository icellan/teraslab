# Conflicting Children Implementation Plan

## Problem

The Teranode shared test `Conflicting()` expects that when a transaction is created with `WithConflicting(true)`, each of its parent transactions gets the child's txid appended to a `ConflictingChildren` list. When `Get` is called on the parent, `meta.Data.ConflictingChildren` must contain the conflicting child's txid.

TeraSlab currently has no `ConflictingChildren` field. The `SetConflicting` operation only toggles a flag on the transaction itself — it does not touch parent records.

## What the test expects

```
1. Create(ParentTx, blockHeight=999)
2. Create(Tx, blockHeight=1000, WithConflicting(true))
      Tx has one input referencing ParentTx output 3
3. Get(Tx) → Conflicting=true, ConflictingChildren=[]
4. Get(ParentTx) → Conflicting=false, ConflictingChildren=[Tx.TxID]
```

Step 4 is what fails today. The parent must know which of its children are conflicting.

## What Aerospike does

During `Create` with `conflicting=true`:
1. For each input in the new tx, extract the parent txid
2. For each unique parent txid, append the new tx's txid to the parent's `conflictingCs` list (atomic list-append-unique)

During `SetConflicting(txHashes, value=true)`:
1. For each tx, read it to get its inputs
2. For each unique parent txid, append the tx's txid to the parent's `conflictingCs` list
3. Then set the CONFLICTING flag on the tx itself

During `Get` with `fields.ConflictingChildren`:
- Read the `conflictingCs` list from the record and return it as `meta.Data.ConflictingChildren`

## Design: Separate allocation with pointer in metadata

### Size constraint

Current `TxMetadata` is exactly 256 bytes (244 raw + 12 padding). The padding gives us exactly enough room for a count + pointer, but nothing inline.  Every byte added to metadata applies to every record on disk, so we must stay at 256.

### Approach: count + device-offset pointer (9 bytes, fits in existing padding)

Add to `TxMetadata` (in `src/record.rs`), consuming 9 of the 12 padding bytes:

```rust
/// Number of conflicting children txids stored for this transaction.
pub conflicting_children_count: u8,
/// Device offset to a separately-allocated block containing the txids (0 = none).
/// Block layout: [txid: 32 bytes] × count.
pub conflicting_children_offset: u64,
```

The children txids are stored in a separately allocated device block, the same way `block_overflow_offset` works for block entries beyond 3. The allocated block is simply `count × 32` bytes of concatenated txids.

**Tradeoff**: Every read of conflicting children requires a second device I/O. This is acceptable because:
- Conflicting transactions are rare (< 0.01% of all transactions)
- The field is only requested when resolving conflicts, not on the hot path
- The block is small (typically 32-64 bytes) and sequential

**No schema version bump needed** — the new fields occupy what was previously zero-filled padding. Old records read back `count=0, offset=0` which is correct (no children).

## Server changes

All changes are in the `teraslab` repo (Rust server + Go client).

### 1. `src/record.rs` — Add 2 fields to metadata

Add to both `TxMetadataRaw` and `TxMetadata`, **before** `_padding`:

```rust
// TxMetadataRaw:
_conflicting_children_count: u8,
_conflicting_children_offset: u64,

// TxMetadata:
pub conflicting_children_count: u8,
pub conflicting_children_offset: u64,
```

`_padding` size shrinks from 12 to 3 bytes automatically via the existing `METADATA_PADDING_AMOUNT` formula. `METADATA_SIZE` stays at 256. Add a compile-time assertion:

```rust
const _: () = assert!(METADATA_SIZE == 256); // must not grow
```

Update `TxMetadata::new()` to zero-initialize:
```rust
conflicting_children_count: 0,
conflicting_children_offset: 0,
```

### 2. `src/ops/engine.rs` — `append_conflicting_child()`

New internal method:

```rust
/// Append a child txid to a parent record's conflicting-children list.
/// Deduplicates: if the child already exists in the list, this is a no-op.
/// Returns Ok(()) if parent not found (parent may be on another node).
fn append_conflicting_child(
    &self,
    parent_key: &TxKey,
    child_txid: [u8; 32],
) -> Result<(), SpendError>
```

Implementation:
1. Acquire stripe lock on `parent_key`
2. Look up parent in index. If not found → return `Ok(())` (parent may live on another node in cluster mode)
3. Read parent metadata from device
4. If `count > 0`, read existing children from device at `conflicting_children_offset` (count × 32 bytes)
5. Check if `child_txid` already in list → return `Ok(())` (dedup)
6. Append `child_txid` to list
7. If old block exists and new list is larger → free old block via allocator, allocate new block
8. If no old block → allocate new block
9. Write children block to device
10. Update metadata: increment `conflicting_children_count`, set `conflicting_children_offset`
11. Bump `generation` and `updated_at`
12. Write metadata back to device

This follows the same allocate-write-update pattern as `write_overflow_block_entries()`.

### 3. `src/ops/engine.rs` — `read_conflicting_children()`

New public method:

```rust
pub fn read_conflicting_children(
    &self,
    key: &TxKey,
) -> Result<Vec<[u8; 32]>, SpendError>
```

1. Look up key, read metadata
2. If `conflicting_children_count == 0` → return empty vec
3. Read `count × 32` bytes from device at `conflicting_children_offset`
4. Parse into `Vec<[u8; 32]>`

### 4. `src/ops/engine.rs` — Modify `create()`

The Go client sends parent txids in the `CreateRequest` when `conflicting=true` (see section 8). After the record is created and indexed:

```rust
if req.conflicting {
    for parent_txid in &req.parent_txids {
        let parent_key = TxKey { txid: *parent_txid };
        let _ = self.append_conflicting_child(&parent_key, req.tx_id);
    }
}
```

The parent txids come from the transaction's inputs — the Go client extracts them from `bt.Tx.Inputs[i].PreviousTxIDChainHash()` and sends them in the wire request. This avoids parsing Bitcoin serialization in Rust.

### 5. `src/ops/engine.rs` — Modify `set_conflicting()`

When `req.value == true`, after toggling the flag, update parents. `set_conflicting` does NOT receive parent txids in the wire request, so it must read the transaction's cold data to extract them:

```rust
if req.value {
    let cold = self.read_cold_data(&req.tx_key)?;
    let parent_txids = extract_unique_input_txids(&cold);
    for parent_txid in parent_txids {
        let parent_key = TxKey { txid: parent_txid };
        let _ = self.append_conflicting_child(&parent_key, req.tx_key.txid);
    }
}
```

The `extract_unique_input_txids` helper deserializes the `ColdData` (using `ColdData::deserialize` from `src/storage/tiers.rs`), then parses the `inputs` field as serialized Bitcoin inputs to extract `prev_txid` from each. Bitcoin input format: `[prev_txid: 32 LE][prev_vout: 4 LE][script_len: varint][script: ...][sequence: 4]`.

Alternatively, if we add `parent_txids` to the `SetConflictingRequest` wire format, the client can send them. But `SetConflicting` takes only txids in its current wire format and the client would need to do a Get first to find the inputs. Parsing cold data server-side is simpler since the server already has the data.

**Lock ordering**: `set_conflicting` holds a lock on `req.tx_key`. `append_conflicting_child` acquires a lock on the parent key. These are always different keys, so no deadlock.

### 6. Wire protocol — GetBatch response (new field mask bit)

In `src/server/dispatch.rs` (or the field mask constants file):

```rust
pub const FIELD_CONFLICTING_CHILDREN: u16 = 0x0010;  // bit 4
```

Update `FieldAll` from `0x000F` to `0x001F`.

In the GetBatch dispatch handler, after block entries:

```rust
if field_mask.has(FieldMask::CONFLICTING_CHILDREN) {
    match engine.read_conflicting_children(&key) {
        Ok(children) => {
            data.push(children.len() as u8);  // count
            for child in &children {
                data.extend_from_slice(child);  // 32 bytes each
            }
        }
        Err(_) => {
            data.push(0u8);
        }
    }
}
```

Response wire format for this section: `[count: 1 byte][txids: count × 32 bytes]`

### 7. Wire protocol — Create request (add parent txids)

Extend the create item wire format. After the existing mined block info section, append:

```
[has_parent_txids: 1 byte (bool)]
if has_parent_txids:
    [count: 4 LE]
    [txids: count × 32 bytes]
```

Only sent when `conflicting=true` in practice. The Go client populates this from the transaction's inputs.

Update `CreateRequest` in `src/ops/create.rs`:
```rust
/// Parent txids to update with conflicting-children when conflicting=true.
pub parent_txids: Vec<[u8; 32]>,
```

Update `decode_create_batch` in `src/protocol/codec.rs` to parse the new field.

### 8. Go client changes (`client/go/`)

#### `opcodes.go`
```go
const FieldConflictingChildren uint16 = 0x0010
```
Change `FieldAll` from `0x000F` to `0x001F`.

#### `types.go`
Add to `CreateItem`:
```go
// ParentTxIDs lists the input parent txids. The server uses these to update
// each parent's conflicting-children list when creating a conflicting tx.
ParentTxIDs []TxID
```

#### `codec.go`
In `encodeCreateBatch`, after the mined block info section:
```go
hasParents := len(item.ParentTxIDs) > 0
buf = appendBool(buf, hasParents)
if hasParents {
    buf = appendU32(buf, uint32(len(item.ParentTxIDs)))
    for _, pid := range item.ParentTxIDs {
        buf = append(buf, pid[:]...)
    }
}
```

#### `record.go`
Add to `TxRecord`:
```go
// ConflictingChildren contains txids of transactions that were created as
// conflicting and reference this transaction's UTXOs as inputs.
ConflictingChildren []TxID
```

In `decodeRecord`, add a new section after block entries:
```go
if fieldMask&FieldConflictingChildren != 0 {
    if pos < len(data) {
        count := int(data[pos]); pos++
        if count > 0 {
            rec.ConflictingChildren = make([]TxID, count)
            for i := 0; i < count; i++ {
                copy(rec.ConflictingChildren[i][:], data[pos:pos+32])
                pos += 32
            }
        }
    }
}
```

### 9. Teranode store changes (`teranode/stores/utxo/teraslab/`)

#### `convert.go`
In `txToCreateItem`, populate `ParentTxIDs` when conflicting:
```go
if opts.Conflicting {
    seen := make(map[teraslab.TxID]bool)
    for _, input := range tx.Inputs {
        prevTxID := input.PreviousTxIDChainHash()
        if prevTxID != nil {
            tid := hashToTxID(prevTxID)
            if !seen[tid] {
                item.ParentTxIDs = append(item.ParentTxIDs, tid)
                seen[tid] = true
            }
        }
    }
}
```

In `recordToMetaData`, read `ConflictingChildren`:
```go
if len(rec.ConflictingChildren) > 0 {
    data.ConflictingChildren = make([]chainhash.Hash, len(rec.ConflictingChildren))
    for i, txid := range rec.ConflictingChildren {
        data.ConflictingChildren[i] = chainhash.Hash(txid)
    }
}
```

#### `conflicting.go`
Remove the no-op `updateParentConflictingChildren` and its call site. The server now handles parent updates during `SetConflicting` and `Create`.

## Replication

The `Create` replica op already includes metadata bytes and cold data. Add `parent_txids` to `ReplicaOp::Create` so the replica receiver can call `append_conflicting_child` on parents without re-parsing cold data.

For `SetConflicting`, either:
- Add parent txids to `ReplicaOp::SetConflicting`, or
- Have the replica receiver parse cold data itself (it has the record)

Option 1 is cleaner but slightly larger wire messages. Either works.

## Testing

### Server unit tests (`src/ops/engine.rs`)

```rust
#[test]
fn create_conflicting_updates_parent_children() {
    // 1. Create parent tx with 2 outputs
    // 2. Create child tx with conflicting=true, one input referencing parent
    // 3. read_conflicting_children(parent_key) → [child_txid]
    // 4. Read parent metadata → conflicting_children_count == 1
}

#[test]
fn set_conflicting_updates_parent_children() {
    // 1. Create parent tx
    // 2. Create child tx (non-conflicting), input referencing parent
    // 3. set_conflicting(child, true)
    // 4. read_conflicting_children(parent_key) → [child_txid]
}

#[test]
fn conflicting_children_dedup() {
    // 1. Create parent
    // 2. append_conflicting_child(parent, child) twice
    // 3. read_conflicting_children → count == 1
}

#[test]
fn conflicting_children_multiple() {
    // 1. Create parent
    // 2. Create 5 conflicting children referencing parent
    // 3. read_conflicting_children → all 5 returned
}

#[test]
fn metadata_size_unchanged() {
    assert_eq!(METADATA_SIZE, 256);
}
```

### Teranode shared test

The existing `tests.Conflicting()` should pass unchanged after these changes.

## File summary

| File | Change |
|------|--------|
| `src/record.rs` | Add `conflicting_children_count` (u8) + `conflicting_children_offset` (u64) to metadata, fits in existing padding, METADATA_SIZE stays 256 |
| `src/ops/engine.rs` | Add `append_conflicting_child`, `read_conflicting_children`; modify `create` and `set_conflicting` to update parents |
| `src/ops/create.rs` | Add `parent_txids: Vec<[u8; 32]>` to `CreateRequest` |
| `src/server/dispatch.rs` | Parse parent_txids in create dispatch; emit ConflictingChildren section in get response |
| `src/protocol/codec.rs` | Encode/decode parent_txids in create; add `FIELD_CONFLICTING_CHILDREN` mask bit |
| `src/replication/protocol.rs` | Add parent_txids to Create replica op |
| `src/replication/receiver.rs` | Call `append_conflicting_child` on replica create |
| `client/go/opcodes.go` | Add `FieldConflictingChildren`, update `FieldAll` |
| `client/go/types.go` | Add `ParentTxIDs` to `CreateItem` |
| `client/go/codec.go` | Encode parent_txids in create request |
| `client/go/record.go` | Decode `ConflictingChildren` in `TxRecord` |
| `teranode/.../convert.go` | Populate `ParentTxIDs` on create; read `ConflictingChildren` from record |
| `teranode/.../conflicting.go` | Remove no-op `updateParentConflictingChildren` |
