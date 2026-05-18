# Phase 10: Wire protocol

**Status:** partial — `src/protocol/` (frame, codec, opcodes) and `src/server/` (TCP dispatch, HTTP observability) in main; F-G5/G6 fix campaigns closed inflight-bytes limiter, HMAC verification, etc. Outstanding production bug: `src/server/mod.rs:264-272` accept loop spins on a 10 ms sleep, burning CPU and slowing shutdown (`_review/follow_ups.md` A-2). Frame zero-copy / streaming HMAC / typed wire error codes are deferred perf items (`_review/follow_ups.md` C-6/C-7/C-8).

## Goal

Implement a high-performance client-server binary protocol over TCP. At millions of operations per second, individual network round-trips per operation are unacceptable. **Batching is the default, not the exception** — every operation has a batch variant, and the protocol is designed so that the Go client can pack hundreds of operations into a single network frame.

## Dependencies

Phases 1-9 must be complete with all tests passing.

## Reference

- `specs/BSV_UTXO_STORE_SPEC.md` §10 (Wire Protocol) — frame format, opcodes, batch support, streaming
- The current Go client batches via `storeBatcher`, `spendBatcher`, `getBatcher` etc. (100-4096 items per batch). The wire protocol must support these batch sizes natively without per-item network round-trips.

## Design principles

1. **Batch-first**: Every mutation and read has a batch opcode. Single-item operations are just batches of size 1 — no separate code path.
2. **Partial success**: Batch responses include per-item status. If 1020 of 1024 spends succeed, the client gets 1020 successes and 4 errors in a single response — not a full failure.
3. **Minimal round-trips**: The client groups operations by shard (same target node), packs them into one frame, sends one TCP write, gets one TCP read back. At 10M ops/sec with batches of 1024, that's ~10,000 network round-trips/sec — easily achievable.
4. **Zero-copy friendly**: Fixed-size fields laid out for direct `memcpy` from wire to struct. No JSON, no protobuf, no variable-length encoding for fixed-size data.
5. **Pipelining**: Multiple batch requests in-flight on one connection, matched by `request_id`. The server processes them concurrently and returns responses out of order.

## What to build

### 10.1 Protocol framing — `src/protocol/frame.rs`

```
Request frame:
┌─────────────────────────────────────────┐
│ total_length: u32    // bytes after this │
│ request_id: u64      // client-assigned  │
│ op_code: u16         // operation type   │
│ flags: u16           // reserved         │
│ payload: [u8]        // op-specific data │
└─────────────────────────────────────────┘

Response frame:
┌─────────────────────────────────────────┐
│ total_length: u32                        │
│ request_id: u64      // matches request  │
│ status: u8           // 0=OK, 1=Error,   │
│                      // 2=NotFound,      │
│                      // 3=Redirect,      │
│                      // 4=PartialError   │
│ payload: [u8]        // op-specific data │
└─────────────────────────────────────────┘
```

Header is 14 bytes (request) / 13 bytes (response). Minimal overhead.

The `PartialError` status (4) indicates a batch where some items succeeded and some failed — the payload contains per-item results.

#### Frame completeness guarantee

The `total_length` field is always known upfront — the sender computes the full serialized size before writing the frame. The receiver reads the 4-byte `total_length` first, then reads exactly that many bytes to obtain the complete frame. There is **no streaming within a single frame**. If a response requires streaming (e.g., large transaction blob reads), the server sends multiple separate frames using `OP_STREAM_CHUNK` / `OP_STREAM_END`, each of which is a self-contained frame with its own `total_length`.

This means:
- The sender MUST buffer the entire payload and compute `total_length` before writing any bytes of the frame to the socket.
- The receiver MUST NOT attempt to process a frame until all `total_length` bytes have been received.
- The maximum frame size is 16 MiB (`total_length <= 16,777,216`). Frames exceeding this limit are rejected by the receiver with an error.

### 10.2 Op codes — `src/protocol/opcodes.rs`

Every operation has a batch variant. Single-item ops are syntactic sugar for batch size=1.

```rust
// Mutations — all have batch semantics
pub const OP_SPEND_BATCH: u16 = 1;         // N spends, potentially across multiple txids
pub const OP_UNSPEND_BATCH: u16 = 2;       // N unspends
pub const OP_SET_MINED_BATCH: u16 = 3;     // N txids × 1 block entry
pub const OP_CREATE_BATCH: u16 = 4;        // N record creations
pub const OP_FREEZE_BATCH: u16 = 5;        // N freezes
pub const OP_UNFREEZE_BATCH: u16 = 6;      // N unfreezes
pub const OP_REASSIGN_BATCH: u16 = 7;      // N reassignments
pub const OP_SET_CONFLICTING_BATCH: u16 = 8; // N txids
pub const OP_SET_LOCKED_BATCH: u16 = 9;    // N txids
pub const OP_PRESERVE_UNTIL_BATCH: u16 = 10; // N txids
pub const OP_DELETE_BATCH: u16 = 11;       // N txids
pub const OP_MARK_LONGEST_CHAIN_BATCH: u16 = 12; // N txids

// Reads — all have batch semantics
pub const OP_GET_BATCH: u16 = 20;          // N txids with field masks
pub const OP_GET_SPEND_BATCH: u16 = 21;    // N spend lookups

// Pruner operations
pub const OP_QUERY_OLD_UNMINED: u16 = 30;
pub const OP_PRESERVE_TRANSACTIONS: u16 = 31;
pub const OP_PROCESS_EXPIRED_PRESERVATIONS: u16 = 32;

// Cluster / admin
pub const OP_GET_PARTITION_MAP: u16 = 100;
pub const OP_HEALTH: u16 = 101;
pub const OP_PING: u16 = 102;

// Streaming responses
pub const OP_STREAM_CHUNK: u16 = 200;
pub const OP_STREAM_END: u16 = 201;

// Replication (inter-node)
pub const OP_REPLICA_BATCH: u16 = 240;     // batch of ReplicaOps
pub const OP_REPLICA_ACK: u16 = 241;

// Cluster (inter-node)
pub const OP_HEARTBEAT: u16 = 250;

// Compatibility
pub const OP_INCREMENT_SPENT_EXTRA_RECS: u16 = 255; // no-op compat shim
```

### 10.3 Batch payload format — `src/protocol/codec.rs`

All batch payloads follow the same pattern:

```
Batch request:
  count: u32                           // number of items
  shared_params: [op-specific]         // parameters common to all items in batch
  items: [item; count]                 // per-item data, fixed or length-prefixed

Batch response (status = OK):
  // Empty payload — all items succeeded. The count matches the request count.

Batch response (status = PartialError):
  // See §10.4 for the error response format.
```

#### SpendBatch payload (the hottest operation):

```
SpendBatch request:
  count: u32
  ignore_conflicting: u8
  ignore_locked: u8
  current_block_height: u32
  block_height_retention: u32
  items: [
    txid: [u8; 32]
    vout: u32
    utxo_hash: [u8; 32]
    spending_data: [u8; 36]
  ] × count
  // Per item: 104 bytes. 1024 items = ~104 KB per batch frame.

SpendBatch response:
  count: u32
  items: [
    status: u8                         // 0=OK, 1=Error
    error_code: u16                    // 0 if OK
    signal: u8                         // Signal enum
    block_ids_count: u8
    block_ids: [u32] × block_ids_count
    spending_data_hex_len: u16         // 0 if no error data
    spending_data_hex: [u8]            // only present for SPENT/INVALID_SPEND errors
  ] × count
```

#### UnspendBatch payload:

```
UnspendBatch request:
  count: u32
  current_block_height: u32
  block_height_retention: u32
  items: [
    txid: [u8; 32]
    vout: u32
    utxo_hash: [u8; 32]
  ] × count
  // Per item: 68 bytes. 1024 items = ~68 KB per batch frame.

UnspendBatch response:
  count: u32
  items: [
    status: u8                         // 0=OK, 1=Error
    error_code: u16                    // 0 if OK
    signal: u8                         // Signal enum
  ] × count
```

#### SetMinedBatch payload:

```
SetMinedBatch request:
  count: u32
  block_id: u32                        // shared: same block for all txs in batch
  block_height: u32
  subtree_idx: u32
  on_longest_chain: u8
  unset_mined: u8
  current_block_height: u32
  block_height_retention: u32
  txids: [[u8; 32]] × count
  // 32 bytes per item. 1024 items = ~32 KB.

SetMinedBatch response:
  count: u32
  items: [
    status: u8
    error_code: u16
    signal: u8
    block_ids_count: u8
    block_ids: [u32] × block_ids_count
  ] × count
```

#### CreateBatch payload:

```
CreateBatch request:
  count: u32
  items: [
    txid: [u8; 32]
    tx_version: u32
    locktime: u32
    fee: u64
    size_in_bytes: u64
    extended_size: u64
    is_coinbase: u8
    spending_height: u32
    created_at: u64
    flags: u8                          // conflicting, locked, frozen
    utxo_count: u32
    utxo_hashes: [[u8; 32]] × utxo_count
    has_cold_data: u8
    cold_data_len: u32                 // 0 if no inline cold data
    cold_data: [u8] × cold_data_len
    has_mined_info: u8
    mined_block_id: u32               // only if has_mined_info
    mined_block_height: u32
    mined_subtree_idx: u32
  ] × count

CreateBatch response:
  count: u32
  items: [
    status: u8
    error_code: u16
  ] × count
```

#### GetBatch payload:

```
GetBatch request:
  count: u32
  field_mask: u16                      // shared: which fields to return
  txids: [[u8; 32]] × count

GetBatch response:
  count: u32
  items: [
    status: u8
    // if OK: serialized metadata + requested fields
    data_len: u32
    data: [u8] × data_len
  ] × count
```

#### FreezeBatch payload:

Freeze sets a UTXO slot's status to `0xFF` (frozen). Each item identifies a specific UTXO by txid + vout + hash.

```
FreezeBatch request:
  count: u32
  items: [
    txid: [u8; 32]
    vout: u32
    utxo_hash: [u8; 32]
  ] × count
  // Per item: 68 bytes. 50 items = ~3.3 KB.

FreezeBatch response:
  count: u32
  items: [
    status: u8                         // 0=OK, 1=Error
    error_code: u16                    // TX_NOT_FOUND, UTXO_HASH_MISMATCH, ALREADY_FROZEN, SPENT
    spending_data_len: u16             // non-zero only for SPENT error (returns existing spending data)
    spending_data: [u8]                // only present when spending_data_len > 0
  ] × count
```

#### UnfreezeBatch payload:

Unfreeze clears a frozen UTXO slot back to unspent status (`0x00`). Same item structure as FreezeBatch.

```
UnfreezeBatch request:
  count: u32
  items: [
    txid: [u8; 32]
    vout: u32
    utxo_hash: [u8; 32]
  ] × count
  // Per item: 68 bytes. Same wire layout as FreezeBatch request.

UnfreezeBatch response:
  count: u32
  items: [
    status: u8                         // 0=OK, 1=Error
    error_code: u16                    // TX_NOT_FOUND, UTXO_HASH_MISMATCH, UTXO_NOT_FROZEN
  ] × count
```

#### ReassignBatch payload:

Reassign replaces a frozen UTXO's hash with a new hash and unfreezes it. Each item carries both the old (current) hash and the new hash, plus a spendable-after height.

```
ReassignBatch request:
  count: u32
  block_height: u32                    // shared: current block height
  spendable_after: u32                 // shared: number of blocks before the UTXO is spendable (default 1000)
  items: [
    txid: [u8; 32]
    vout: u32
    utxo_hash: [u8; 32]               // current (frozen) hash — must match
    new_utxo_hash: [u8; 32]           // replacement hash
  ] × count
  // Per item: 100 bytes. 50 items = ~5 KB.

ReassignBatch response:
  count: u32
  items: [
    status: u8                         // 0=OK, 1=Error
    error_code: u16                    // TX_NOT_FOUND, UTXO_HASH_MISMATCH, UTXO_NOT_FROZEN
  ] × count
```

#### SetConflictingBatch payload:

Sets or clears the CONFLICTING flag on N transactions. All items in the batch share the same `value`, `current_block_height`, and `block_height_retention` parameters.

```
SetConflictingBatch request:
  count: u32
  value: u8                            // shared: 1 = mark conflicting, 0 = clear
  current_block_height: u32            // shared: for setDeleteAtHeight evaluation
  block_height_retention: u32          // shared: for setDeleteAtHeight evaluation
  txids: [[u8; 32]] × count
  // Per item: 32 bytes. 100 items = ~3.2 KB.

SetConflictingBatch response:
  count: u32
  items: [
    status: u8                         // 0=OK, 1=Error
    error_code: u16                    // TX_NOT_FOUND
    signal: u8                         // DAHSET / DAHUNSET / NONE
  ] × count
```

#### SetLockedBatch payload:

Sets or clears the LOCKED flag on N transactions. If locking and `delete_at_height != 0`, the server clears `delete_at_height` (locked records should not be pruned).

```
SetLockedBatch request:
  count: u32
  value: u8                            // shared: 1 = lock, 0 = unlock
  txids: [[u8; 32]] × count
  // Per item: 32 bytes. 1024 items = ~32 KB.

SetLockedBatch response:
  count: u32
  items: [
    status: u8                         // 0=OK, 1=Error
    error_code: u16                    // TX_NOT_FOUND
  ] × count
```

#### PreserveUntilBatch payload:

Sets `preserve_until` to the given block height and clears `delete_at_height`. Used by the pruner to protect parent transactions from deletion.

```
PreserveUntilBatch request:
  count: u32
  block_height: u32                    // shared: preserve until this height
  txids: [[u8; 32]] × count
  // Per item: 32 bytes. 1024 items = ~32 KB.

PreserveUntilBatch response:
  count: u32
  items: [
    status: u8                         // 0=OK, 1=Error
    error_code: u16                    // TX_NOT_FOUND
    signal: u8                         // PRESERVE (if external tx) or NONE
  ] × count
```

#### DeleteBatch payload:

Deletes N transactions. Removes index entry, returns record space to freelist, schedules external blob deletion if applicable.

```
DeleteBatch request:
  count: u32
  txids: [[u8; 32]] × count
  // Per item: 32 bytes. 256 items = ~8 KB.

DeleteBatch response:
  count: u32
  items: [
    status: u8                         // 0=OK, 1=Error
    error_code: u16                    // TX_NOT_FOUND
  ] × count
```

#### MarkLongestChainBatch payload:

Updates `unmined_since` for N transactions based on whether they are on the longest chain. Called during chain re-org processing. All items in the batch share the same `on_longest_chain` flag, `current_block_height`, and `block_height_retention`.

```
MarkLongestChainBatch request:
  count: u32
  on_longest_chain: u8                 // shared: 1 = on longest chain, 0 = not
  current_block_height: u32            // shared: for unmined_since and setDeleteAtHeight
  block_height_retention: u32          // shared: for setDeleteAtHeight evaluation
  txids: [[u8; 32]] × count
  // Per item: 32 bytes. 1024 items = ~32 KB.

MarkLongestChainBatch response:
  count: u32
  items: [
    status: u8                         // 0=OK, 1=Error
    error_code: u16                    // TX_NOT_FOUND
    signal: u8                         // DAHSET / DAHUNSET / NONE
  ] × count
```

#### GetSpendBatch payload:

Looks up the spend status and spending data for N specific UTXO slots.

```
GetSpendBatch request:
  count: u32
  items: [
    txid: [u8; 32]
    vout: u32
  ] × count
  // Per item: 36 bytes. 1024 items = ~36 KB.

GetSpendBatch response:
  count: u32
  items: [
    status: u8                         // 0=OK (found), 1=Error
    error_code: u16                    // TX_NOT_FOUND, VOUT_OUT_OF_RANGE
    slot_status: u8                    // 0x00=Unspent, 0x01=Spent, 0x02=Pruned, 0xFF=Frozen
    spending_data: [u8; 36]            // txid(32) + vin(4) — zeroed if not spent
  ] × count
```

### 10.4 Error response format — `src/protocol/error.rs`

When a batch response has `status = PartialError` (4), the payload uses a **sparse error format** that includes only failed items. Successful items are omitted to minimize response size. The client infers success for any item index not present in the error list.

```
PartialError response payload:
  error_count: u32                     // number of failed items (NOT total batch count)
  items: [
    item_index: u32                    // 0-based index into the original request batch
    error_code: u16                    // operation-specific error code
    error_data_len: u16                // length of additional error context (0 if none)
    error_data: [u8; error_data_len]   // op-specific error data (e.g., existing spending_data for SPENT errors)
  ] × error_count
```

**Invariants:**
- `error_count > 0` — if all items succeed, status is `OK` (0), not `PartialError`
- `error_count < batch_count` — if all items fail, status is `Error` (1) with a global error code
- `item_index` values are strictly ascending (sorted) — enables binary search on the client side
- Each `item_index` is less than the original request's `count`

**Error codes** (shared across all batch operations):

```rust
pub const ERR_OK: u16 = 0;
pub const ERR_TX_NOT_FOUND: u16 = 1;
pub const ERR_UTXO_HASH_MISMATCH: u16 = 2;
pub const ERR_ALREADY_SPENT: u16 = 3;
pub const ERR_ALREADY_FROZEN: u16 = 4;
pub const ERR_UTXO_NOT_FROZEN: u16 = 5;
pub const ERR_INVALID_SPEND: u16 = 6;       // terminal state (pruned)
pub const ERR_FROZEN: u16 = 7;               // attempted spend on frozen UTXO
pub const ERR_CONFLICTING: u16 = 8;          // spend on conflicting tx (unless ignore_conflicting)
pub const ERR_LOCKED: u16 = 9;               // spend on locked tx (unless ignore_locked)
pub const ERR_COINBASE_IMMATURE: u16 = 10;   // coinbase spending_height not reached
pub const ERR_VOUT_OUT_OF_RANGE: u16 = 11;   // vout >= utxo_count
pub const ERR_ALREADY_EXISTS: u16 = 12;      // create for already-existing txid
pub const ERR_INTERNAL: u16 = 255;           // unexpected server-side error
```

**Typed wire error codes (P3.10 / F-G5-017, PROTOCOL_VERSION=2):**

Pre-P3.10 the dispatcher returned `ERR_INTERNAL` for any failure that was
not one of the named per-item codes. That collapsed wire-decode failures,
storage I/O failures, unknown opcodes, rate-limiter rejections, and
invariant violations into a single bucket — clients could only distinguish
them by substring-matching the free-text message. The P3.10 revision
introduces typed codes for every distinct dispatch failure class. Clients
on the new protocol version match on the typed code directly; old clients
that match on `ERR_INTERNAL` for these specific failures must be updated.

```rust
pub const ERR_PAYLOAD_MALFORMED:    u16 = 28;  // wire-decode failed
pub const ERR_OPCODE_UNSUPPORTED:   u16 = 29;  // op_code not recognised
pub const ERR_STORAGE_IO:           u16 = 30;  // device read/write failure
pub const ERR_RATE_LIMITED:         u16 = 31;  // in-flight / max-conn cap exceeded
pub const ERR_NOT_CLUSTERED:        u16 = 32;  // cluster opcode on single-node server
pub const ERR_INVARIANT_VIOLATION:  u16 = 33;  // request_id shard encoding etc.
pub const ERR_STREAM_INVARIANT:     u16 = 34;  // stream state-machine violation
pub const PROTOCOL_VERSION:         u16 = 2;   // bumped from implicit v1
```

**Routing the new codes to handler sites:**

| Dispatch failure                                    | P3.10 code               | Pre-P3.10 code |
|-----------------------------------------------------|--------------------------|----------------|
| `decode_*_checked` rejected the frame               | `ERR_PAYLOAD_MALFORMED`  | `ERR_INTERNAL` |
| `OP_*` unknown to the dispatcher                    | `ERR_OPCODE_UNSUPPORTED` | `ERR_INTERNAL` |
| Redo-log write / fsync failed                       | `ERR_STORAGE_IO`         | `ERR_INTERNAL` |
| `engine.allocator().allocate_batch` failed          | `ERR_STORAGE_IO`         | `ERR_INTERNAL` |
| `engine.read_metadata` / `read_slots` / blob `Err`  | `ERR_STORAGE_IO`         | `ERR_INTERNAL` |
| Aggregate in-flight byte limit exceeded             | `ERR_RATE_LIMITED`       | `ERR_INTERNAL` |
| `max_connections` cap reached on accept             | `ERR_RATE_LIMITED`       | `ERR_INTERNAL` |
| Topology / partition opcode on non-clustered server | `ERR_NOT_CLUSTERED`      | `ERR_INTERNAL` |
| `request_id` shard-encoding upper-bits non-zero     | `ERR_INVARIANT_VIOLATION`| `ERR_INTERNAL` |
| Stream offset / byte-counter / cap violation        | `ERR_STREAM_INVARIANT`   | `ERR_INTERNAL` |
| Replication compensation aborted, node degraded     | `ERR_INTERNAL`           | `ERR_INTERNAL` |

`ERR_INTERNAL` is retained as the sentinel for genuinely unclassified
failures (e.g. replication-compensation aborts that leave the node in a
state the dispatcher cannot prove safe). Clients should always accept
`ERR_INTERNAL` as a fallback.

**Error data by error code:**

| Error Code | `error_data` Contents | Length |
|------------|----------------------|--------|
| `ERR_ALREADY_SPENT` | Existing `spending_data` (txid + vin) | 36 bytes |
| `ERR_INVALID_SPEND` | Existing `spending_data` (txid + vin) | 36 bytes |
| `ERR_COINBASE_IMMATURE` | `spending_height: u32` (LE) | 4 bytes |
| All others | Empty | 0 bytes |

**Example**: A SpendBatch of 1024 items where items 7, 42, and 999 fail:

```
Response frame:
  total_length: <computed>
  request_id: <matches request>
  status: 4 (PartialError)
  payload:
    error_count: 3
    [0]: item_index=7,   error_code=3 (ALREADY_SPENT), error_data_len=36, error_data=[spending_data bytes]
    [1]: item_index=42,  error_code=1 (TX_NOT_FOUND),  error_data_len=0
    [2]: item_index=999, error_code=7 (FROZEN),         error_data_len=0
```

**When to use each response status:**

| Status | Meaning | Payload |
|--------|---------|---------|
| 0 (OK) | All items succeeded | Operation-specific success payload (e.g., SpendBatch per-item signals) |
| 1 (Error) | Global error (all items failed, or request-level error) | `error_code: u16, message_len: u16, message: [u8]` |
| 2 (NotFound) | Single-item batch, record not found | Empty |
| 3 (Redirect) | Shard not owned by this node | `node_addr_len: u16, node_addr: [u8]` (target node address) |
| 4 (PartialError) | Some items succeeded, some failed | Sparse error format above |

**Note on SpendBatch/SetMinedBatch with PartialError:** These operations return per-item signals (ALLSPENT, DAHSET, etc.) and block_ids even on success. When status is `PartialError`, the response contains **two sections**:

```
SpendBatch PartialError payload:
  // Section 1: Success results (for items that need signal/block_id data)
  success_count: u32
  successes: [
    item_index: u32
    signal: u8
    block_ids_count: u8
    block_ids: [u32] × block_ids_count
  ] × success_count

  // Section 2: Error results
  error_count: u32
  errors: [
    item_index: u32
    error_code: u16
    error_data_len: u16
    error_data: [u8; error_data_len]
  ] × error_count
```

For operations without per-item success data (DeleteBatch, SetLockedBatch, etc.), the success section is omitted — only the error section is present.

### 10.5 Server-side batch dispatch — `src/server.rs`

The server processes batch items in parallel where possible:

```rust
pub struct Server {
    config: ServerConfig,
    listener: TcpListener,
    devices: Vec<Arc<dyn BlockDevice>>,
    index: Arc<RwLock<Index>>,
    allocator: Arc<Mutex<SlotAllocator>>,
    locks: Arc<StripedLocks>,
    redo_log: Arc<Mutex<RedoLog>>,
    replication: Option<ReplicationSender>,
    cluster: Option<ClusterCoordinator>,
}

pub struct ServerConfig {
    pub bind_addr: SocketAddr,
    pub max_connections: usize,
    pub read_buffer_size: usize,       // default 256 KB (fits large batch frames)
    pub write_buffer_size: usize,      // default 256 KB
    pub request_timeout: Duration,
    pub max_batch_size: u32,           // default 8192, reject larger batches
}
```

#### Batch dispatch strategy:

1. **SpendBatch**: Group items by txid (same lock). Items for different txids execute concurrently (different lock stripes). Items for the same txid execute under one lock hold.
2. **SetMinedBatch**: Each txid is independent — fully parallel across lock stripes.
3. **CreateBatch**: Each creation is independent — parallel allocation + write.
4. **GetBatch**: All reads are independent — submit as parallel io_uring reads.
5. **FreezeBatch / UnfreezeBatch**: Group items by txid (same lock stripe as spends — they touch UTXO slots). Items for different txids execute concurrently.
6. **ReassignBatch**: Group items by txid (touches UTXO slots + metadata). Items for different txids execute concurrently.
7. **DeleteBatch / SetConflictingBatch / SetLockedBatch / PreserveUntilBatch / MarkLongestChainBatch**: Each txid independent — fully parallel.

The server does NOT serialize batch items. It fans them out to the thread pool / io_uring and collects results.

### 10.6 Request handler — `src/server/handler.rs`

```rust
pub async fn handle_request(
    frame: RequestFrame,
    ctx: &ServerContext,
) -> ResponseFrame
```

The handler:
1. Decode op_code
2. Decode batch count and shared parameters
3. For key-based operations: partition items by shard ownership
   - Items for local shards: process in parallel
   - Items for remote shards: return Redirect per item (client re-routes)
4. Fan out to operation functions
5. Collect per-item results, encode response
6. Handle errors: per-item errors in partial-success responses

### 10.7 Read operations — `src/ops/read.rs`

```rust
pub struct GetRequest {
    pub tx_key: TxKey,
    pub fields: FieldMask,
}

pub struct FieldMask(pub u16);

impl FieldMask {
    pub const METADATA: u16     = 0x0001;
    pub const UTXO_SLOTS: u16   = 0x0002;
    pub const COLD_DATA: u16    = 0x0004;
    pub const BLOCK_ENTRIES: u16 = 0x0008;
    pub const ALL: u16          = 0x000F;
}
```

GetBatch submits all reads as parallel io_uring operations. The response is assembled once all completions arrive.

### 10.8 Streaming for large reads

For records with large inline cold data or external blob references:
- Metadata and UTXO slots sent immediately in the first response frame
- Cold data streamed as `OP_STREAM_CHUNK` frames (64 KiB each)
- Final `OP_STREAM_END` frame with total size and checksum
- Streaming uses the same `request_id` so the client can correlate chunks
- Each streaming frame is self-contained with its own `total_length` — the frame completeness guarantee (see 10.1) applies to every individual streaming frame

### 10.9 Connection management

- **Pipelining**: Multiple batch requests in-flight on one connection, matched by `request_id`. Server processes them concurrently and may return responses out-of-order.
- **Keep-alive**: Connections are long-lived. The server does not close idle connections (the client manages its pool).
- **Graceful shutdown**: Server sends a "going away" frame before closing, giving the client time to drain in-flight requests.
- **Back-pressure**: If the server's write buffer is full, it stops reading from the connection until the client consumes responses. This naturally throttles fast producers.

### 10.10 Client-side batching expectations

The Go client library (`teraslab-client-go`, separate repo) and its Teranode adapter (`stores/utxo/teraslab/`) will use a batcher pattern:

| Batcher | Items accumulated | Flush trigger | Result |
|---------|------------------|---------------|--------|
| spendBatcher | Individual spends | 1024 items OR 10ms | One `OP_SPEND_BATCH` frame |
| storeBatcher | Individual creates | 2048 items OR 10ms | One `OP_CREATE_BATCH` frame |
| minedBatcher | Individual setMined | 1024 items OR 5ms | One `OP_SET_MINED_BATCH` frame |
| getBatcher | Individual gets | 4096 items OR 10ms | One `OP_GET_BATCH` frame |
| lockedBatcher | Individual setLocked | 1024 items OR 5ms | One `OP_SET_LOCKED_BATCH` frame |
| deleteBatcher | Individual deletes | 256 items OR 10ms | One `OP_DELETE_BATCH` frame |

The client groups items by target node (shard table lookup), accumulates per-node, and flushes each node's batch independently. This means one TCP write per node per flush — not one TCP write per operation.

**Throughput math**: At 10M ops/sec with 1024-item spend batches → ~9,766 batch frames/sec → ~1.0 GB/sec wire throughput (at 104 KB/batch). Fits within 10 GbE capacity (1.25 GB/sec theoretical).

## Acceptance criteria

### Framing tests

```
- [ ] Encode request frame → decode → matches original
- [ ] Encode response frame → decode → matches original
- [ ] Frame with maximum payload size (16 MB): encodes/decodes correctly
- [ ] Truncated frame: decoder returns error (not panic)
- [ ] Frame with wrong length field: decoder returns error
- [ ] Multiple frames in a stream: decoder handles boundaries correctly
- [ ] PartialError status correctly parsed with per-item results
- [ ] total_length computed correctly before frame is written (no post-hoc patching)
- [ ] Receiver rejects frames with total_length > 16 MiB
```

### Codec tests (batch round-trips)

```
- [ ] SpendBatch with 1 item: round-trip
- [ ] SpendBatch with 1024 items: round-trip, all items preserved
- [ ] SpendBatch response with mixed OK/Error items: round-trip
- [ ] UnspendBatch with 1 item: round-trip
- [ ] UnspendBatch with 512 items: round-trip, all items preserved
- [ ] UnspendBatch response with mixed OK/Error items: round-trip
- [ ] SetMinedBatch with 512 items: round-trip
- [ ] SetMinedBatch response with signals and block_ids: round-trip
- [ ] CreateBatch with 100 items (varying UTXO counts): round-trip
- [ ] CreateBatch with cold data: round-trip
- [ ] GetBatch with 4096 items: round-trip
- [ ] GetBatch response with mixed OK/NotFound items: round-trip
- [ ] GetSpendBatch with 1024 items: round-trip
- [ ] GetSpendBatch response with mixed slot statuses: round-trip
- [ ] FreezeBatch with 1 item: round-trip
- [ ] FreezeBatch with 50 items: round-trip
- [ ] FreezeBatch response with SPENT error including spending_data: round-trip
- [ ] UnfreezeBatch with 1 item: round-trip
- [ ] UnfreezeBatch with 50 items: round-trip
- [ ] UnfreezeBatch response with UTXO_NOT_FROZEN error: round-trip
- [ ] ReassignBatch with 1 item: round-trip
- [ ] ReassignBatch with 50 items: round-trip, old and new hashes preserved
- [ ] ReassignBatch response with UTXO_NOT_FROZEN error: round-trip
- [ ] SetConflictingBatch with 1 item: round-trip
- [ ] SetConflictingBatch with 100 items: round-trip
- [ ] SetConflictingBatch response with DAHSET signal: round-trip
- [ ] SetLockedBatch with 1 item: round-trip
- [ ] SetLockedBatch with 1024 items: round-trip
- [ ] SetLockedBatch response with TX_NOT_FOUND error: round-trip
- [ ] PreserveUntilBatch with 1 item: round-trip
- [ ] PreserveUntilBatch with 1024 items: round-trip
- [ ] PreserveUntilBatch response with PRESERVE signal: round-trip
- [ ] DeleteBatch with 1 item: round-trip
- [ ] DeleteBatch with 256 items: round-trip
- [ ] DeleteBatch response with TX_NOT_FOUND errors: round-trip
- [ ] MarkLongestChainBatch with 1 item: round-trip
- [ ] MarkLongestChainBatch with 1024 items: round-trip
- [ ] MarkLongestChainBatch response with DAHSET signal: round-trip
- [ ] PartialError response: sparse error format round-trip, item_index ordering preserved
- [ ] PartialError response: error_data bytes preserved for ALREADY_SPENT (36 bytes)
- [ ] PartialError response: error_data empty for TX_NOT_FOUND
- [ ] SpendBatch PartialError with success signals + error items: both sections round-trip
- [ ] Error response: round-trip with error code and message preserved
- [ ] Redirect response per item: includes target node address
```

### Server integration tests (full TCP round-trip)

```
- [ ] Connect, send Ping, receive Pong
- [ ] CreateBatch 10 records → GetBatch same 10: all data matches
- [ ] CreateBatch → SpendBatch across multiple txids → GetBatch: all spent correctly
- [ ] CreateBatch → SetMinedBatch: block entries present on all
- [ ] CreateBatch → FreezeBatch → GetSpendBatch: slot status is 0xFF (frozen)
- [ ] CreateBatch → FreezeBatch → UnfreezeBatch → GetSpendBatch: slot status is 0x00 (unspent)
- [ ] CreateBatch → FreezeBatch → ReassignBatch → GetSpendBatch: new hash present, status unspent
- [ ] CreateBatch → SetConflictingBatch: conflicting flag set on GetBatch
- [ ] CreateBatch → SetLockedBatch: locked flag set on GetBatch
- [ ] CreateBatch → PreserveUntilBatch → GetBatch: preserve_until field set
- [ ] CreateBatch → DeleteBatch → GetBatch: NotFound for deleted txids
- [ ] CreateBatch → SetMinedBatch → MarkLongestChainBatch: unmined_since updated
- [ ] All operations from Phases 3-6 work over TCP in batch form
- [ ] Concurrent connections (10 clients, each sending batches): all operations correct
- [ ] Pipelined requests (5 batch requests without waiting): all responses correct, matched by request_id
- [ ] SpendBatch with items for mixed shards: local items processed, remote items get Redirect
- [ ] Batch with partial failures (some txids exist, some don't): PartialError status,
      correct per-item error codes, sparse format with ascending item_index
```

### Batch dispatch tests

```
- [ ] SpendBatch 1024 items across 100 different txids: items grouped by txid,
      processed in parallel, counter increments correct
- [ ] SpendBatch 100 items all same txid: single lock hold, all processed atomically
- [ ] SetMinedBatch 1024 items: all processed in parallel (different txids = different locks)
- [ ] GetBatch 4096 items: parallel io_uring reads, all results correct
- [ ] CreateBatch 500 items: parallel allocation + write
- [ ] FreezeBatch 50 items across 25 txids: items grouped by txid, parallel across txids
- [ ] ReassignBatch 20 items across 10 txids: items grouped by txid, parallel across txids
- [ ] DeleteBatch 256 items: all processed in parallel
- [ ] SetConflictingBatch 100 items: all processed in parallel
- [ ] SetLockedBatch 1024 items: all processed in parallel
- [ ] MarkLongestChainBatch 1024 items: all processed in parallel
- [ ] Batch exceeding max_batch_size: rejected with error (not processed)
```

### Error handling tests

```
- [ ] Invalid op_code: returns error response (not disconnect)
- [ ] Malformed payload: returns error response
- [ ] Request for non-existent tx in batch: per-item NotFound, other items unaffected
- [ ] Server shutdown while client connected: client gets clean disconnect
- [ ] Client disconnect mid-request: server doesn't crash, in-flight batch completes
- [ ] Frame with total_length > 16 MiB: rejected before reading payload
```

### Performance tests

```
- [ ] SpendBatch throughput: ops/sec (batch size 1, 10, 100, 1024)
- [ ] SpendBatch throughput: 10 concurrent connections, batch size 1024
- [ ] CreateBatch throughput: ops/sec (batch size 100, 1000)
- [ ] GetBatch throughput: ops/sec (batch size 100, 4096)
- [ ] SetMinedBatch throughput: ops/sec (batch size 1024)
- [ ] FreezeBatch throughput: ops/sec (batch size 50)
- [ ] DeleteBatch throughput: ops/sec (batch size 256)
- [ ] Latency: p50/p99 for SpendBatch with 1024 items
- [ ] Latency: p50/p99 for GetBatch with 4096 items
- [ ] Protocol overhead: bytes on wire vs payload for SpendBatch 1024
- [ ] Pipelining benefit: throughput with 1 vs 4 pipelined batches per connection
- [ ] Network saturation: find the batch size that saturates 10 GbE
```

### Observability HTTP endpoints

In addition to the binary wire protocol TCP port, this phase adds a separate HTTP port (default 9100) using `axum` for observability (see spec §11.6):

- `/metrics` — Prometheus text format export (aggregate ThreadMetrics + ThreadHistograms + GlobalGauges)
- `/health/live` and `/health/ready` — health checks
- `/status` — cluster health overview JSON (see spec §11.6 for schema)
- `/debug/index`, `/debug/freelist`, `/debug/redo` — diagnostic JSON endpoints
- `/debug/log-level` — PUT to change runtime log level
- `/debug/records/{txid}` — diagnostic record metadata dump

### Observability acceptance criteria

```
- [ ] /metrics returns valid Prometheus text format
- [ ] /metrics includes all counters from ThreadMetrics (ops by type, errors by code, bytes)
- [ ] /metrics includes latency histograms with p50/p95/p99/p99.9 buckets
- [ ] /metrics includes storage/memory/record inventory gauges
- [ ] /health/live returns 200 when device is accessible
- [ ] /health/ready returns 503 during startup, 200 after index loaded
- [ ] /status returns complete JSON matching spec schema (records, storage, memory, cluster, throughput)
- [ ] /debug/log-level PUT changes level, subsequent logs reflect change
- [ ] /debug/records/{txid} returns JSON metadata for existing record
- [ ] Metrics scrape does not block or slow the binary protocol path
```

## NOT in this phase

- No TLS (can be added later)
- No authentication
- No Go client implementation (that's a separate repo/phase)
- No compression (batch frames are already compact binary; compression adds latency)
