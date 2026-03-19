# Phase 3: The spend path

## Goal

Implement `spend`, `spendMulti`, and `unspend` as complete, tested, concurrent operations. This is the single most performance-critical code path in the entire system. Also introduce io_uring and lock striping in this phase, as both are needed here first.

## Dependencies

Phases 1-2 must be complete with all tests passing.

## Reference

- `specs/teranode.lua` lines 261-466 (`spend`, `spendMulti`), lines 478-540 (`unspend`)
- `specs/teranode.lua` lines 194-231 (`getUTXOAndSpendingData`)
- `specs/teranode.lua` lines 233-246 (`isFrozen`)
- `specs/teranode.lua` lines 927-1008 (`setDeleteAtHeight`)
- `specs/BSV_UTXO_STORE_SPEC.md` §3.4-3.5 (spend/unspend) and §3.13 (setDeleteAtHeight)
- `specs/BSV_UTXO_STORE_SPEC.md` §5.5 (secondary indexes: DAH, Unmined)
- `specs/BSV_UTXO_STORE_SPEC.md` §6 (concurrency) and §11 (observability)

## What to build

### 3.1 Error types — `src/ops/error.rs`

Define all error types matching the Lua error codes. These are NOT string errors — they are enum variants.

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum SpendError {
    TxNotFound,
    Conflicting,
    Locked,
    CoinbaseImmature {
        spending_height: u32,
        current_height: u32,
    },
    UtxoNotFound {
        offset: u32,
    },
    UtxoHashMismatch {
        offset: u32,
    },
    AlreadySpent {
        offset: u32,
        spending_data: [u8; 36],  // raw bytes; hex encoding is client-side
    },
    Frozen {
        offset: u32,
    },
    FrozenUntil {
        offset: u32,
        spendable_at_height: u32,
    },
    InvalidSpend {
        offset: u32,
        spending_data: [u8; 36],  // raw bytes; hex encoding is client-side
    },
    Pruned {
        offset: u32,
    },
}
```

### 3.2 Signal types — `src/ops/signal.rs`

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum Signal {
    None,
    AllSpent,
    NotAllSpent,
    DeleteAtHeightSet,
    DeleteAtHeightUnset,
}
```

### 3.3 Device I/O abstraction — `src/device_io.rs`

Define a `DeviceIo` trait that both an io_uring backend and a synchronous fallback implement. This gives the spend path (and all other I/O consumers) a single interface regardless of kernel support.

#### 3.3.1 The `DeviceIo` trait

```rust
pub struct Completion {
    pub user_data: u64,
    pub result: i32,  // bytes transferred, or negative errno
}

/// Trait abstracting batched device I/O.
/// Both IoUringBackend and SyncFallback implement this identically.
pub trait DeviceIo: Send + Sync {
    /// Submit a pread operation. Returns immediately; I/O is deferred until
    /// `submit_and_wait` or `submit`.
    fn submit_read(
        &mut self,
        fd: RawFd,
        buf: &mut AlignedBuf,
        offset: u64,
        user_data: u64,
    ) -> Result<()>;

    /// Submit a pwrite operation. Same deferred semantics.
    fn submit_write(
        &mut self,
        fd: RawFd,
        buf: &AlignedBuf,
        offset: u64,
        user_data: u64,
    ) -> Result<()>;

    /// Submit all pending SQEs and block until at least `min_complete` finish.
    fn submit_and_wait(&mut self, min_complete: usize) -> Result<Vec<Completion>>;

    /// Submit all pending SQEs without waiting.
    fn submit(&mut self) -> Result<()>;

    /// Harvest completed operations (non-blocking).
    fn completions(&mut self) -> Vec<Completion>;

    /// How many operations are currently pending (submitted but not completed).
    fn pending(&self) -> usize;
}
```

#### 3.3.2 `IoUringBackend` — `src/device_io/io_uring_backend.rs`

The primary backend for Linux >= 5.6. Uses the `io-uring` crate (not tokio-uring -- we want direct control over submission and completion).

```rust
pub struct IoUringBackend {
    ring: io_uring::IoUring,
    pending: usize,
}

impl IoUringBackend {
    pub fn new(queue_depth: u32) -> Result<Self>;
}

impl DeviceIo for IoUringBackend {
    // All methods delegate to the io_uring ring.
    // submit_read: push a Read SQE with user_data tag.
    // submit_write: push a Write SQE with user_data tag.
    // submit_and_wait: io_uring_enter with min_complete, then drain CQ.
    // submit: io_uring_enter without waiting.
    // completions: drain CQ non-blocking.
    // ...
}
```

#### 3.3.3 `SyncFallback` — `src/device_io/sync_fallback.rs`

Fallback for systems without io_uring support (Linux < 5.6, macOS, test environments). Provides the exact same `DeviceIo` trait surface but executes each operation synchronously with `libc::pread` / `libc::pwrite`.

```rust
struct PendingOp {
    kind: OpKind,  // Read or Write
    fd: RawFd,
    buf_ptr: *mut u8,
    len: usize,
    offset: u64,
    user_data: u64,
}

pub struct SyncFallback {
    pending: Vec<PendingOp>,
}

impl SyncFallback {
    pub fn new(_queue_depth: u32) -> Result<Self> {
        Ok(Self { pending: Vec::new() })
    }
}

impl DeviceIo for SyncFallback {
    fn submit_read(&mut self, fd, buf, offset, user_data) -> Result<()> {
        // Record the operation; do not execute yet (preserves batching semantics).
        self.pending.push(PendingOp { kind: Read, fd, buf_ptr, len, offset, user_data });
        Ok(())
    }

    fn submit_write(&mut self, fd, buf, offset, user_data) -> Result<()> {
        self.pending.push(PendingOp { kind: Write, fd, buf_ptr, len, offset, user_data });
        Ok(())
    }

    fn submit_and_wait(&mut self, min_complete: usize) -> Result<Vec<Completion>> {
        // Execute ALL pending ops via pread/pwrite, return completions.
        let mut completions = Vec::with_capacity(self.pending.len());
        for op in self.pending.drain(..) {
            let result = match op.kind {
                Read  => libc::pread(op.fd, op.buf_ptr as *mut _, op.len, op.offset as i64),
                Write => libc::pwrite(op.fd, op.buf_ptr as *const _, op.len, op.offset as i64),
            };
            completions.push(Completion { user_data: op.user_data, result: result as i32 });
        }
        Ok(completions)
    }

    fn submit(&mut self) -> Result<()> { Ok(()) }  // no-op; ops execute in submit_and_wait

    fn completions(&mut self) -> Vec<Completion> { Vec::new() }  // always empty

    fn pending(&self) -> usize { self.pending.len() }
}
```

#### 3.3.4 Runtime detection and construction

At startup, try to create an `IoUringBackend`. If that fails (unsupported kernel, missing syscall), fall back to `SyncFallback`. The result is returned as `Box<dyn DeviceIo>`.

```rust
pub fn create_device_io(queue_depth: u32) -> Box<dyn DeviceIo> {
    match IoUringBackend::new(queue_depth) {
        Ok(backend) => {
            tracing::info!("io_uring initialized (queue_depth={queue_depth})");
            Box::new(backend)
        }
        Err(e) => {
            tracing::warn!("io_uring unavailable ({e}), using synchronous pread/pwrite fallback");
            Box::new(SyncFallback::new(queue_depth).expect("SyncFallback cannot fail"))
        }
    }
}
```

All call sites (spend, unspend, create, setMined, read, etc.) receive a `&mut dyn DeviceIo` and are backend-agnostic. This means:
- The spend path code is written once against `DeviceIo`
- Tests on macOS and CI without io_uring run identically via `SyncFallback`
- `MemoryDevice` (from Phase 1) can also implement `DeviceIo` for pure in-memory testing

#### 3.3.5 Usage in the spend path

The spend path calls `DeviceIo` methods inside the lock critical section (see 3.4.1 below):

```rust
fn spend_multi(
    io: &mut dyn DeviceIo,
    index: &Index,
    locks: &StripedLocks,
    secondary: &SecondaryIndexes,
    req: SpendMultiRequest,
) -> Result<SpendMultiResponse> {
    let _guard = locks.lock(&req.tx_key);

    // --- All I/O happens inside the lock ---

    // 1. Read metadata
    io.submit_read(fd, &mut meta_buf, record_offset, TAG_META);
    let completions = io.submit_and_wait(1)?;
    // parse metadata from meta_buf...

    // 2. Batch-read UTXO slots
    for item in &req.spends {
        let slot_offset = record_offset + METADATA_SIZE + (item.offset as u64 * SLOT_SIZE);
        io.submit_read(fd, &mut slot_bufs[i], slot_offset, TAG_SLOT_BASE + i as u64);
    }
    let completions = io.submit_and_wait(req.spends.len())?;

    // 3. Validate all slots in memory (no I/O)
    // ...

    // 4. Batch-write mutated slots
    for valid_spend in &valid_spends {
        io.submit_write(fd, &slot_bufs[valid_spend.i], slot_offset, TAG_WRITE_BASE + i as u64);
    }
    let completions = io.submit_and_wait(valid_spends.len())?;

    // 5. Update + write metadata (generation, updated_at, spent_utxos, DAH patch)
    io.submit_write(fd, &meta_buf, record_offset, TAG_META_WRITE);
    let completions = io.submit_and_wait(1)?;

    // 6. Update secondary indexes if DAH/unmined changed (see 3.5.1)
    // ...

    // --- Lock released when _guard drops ---
    Ok(response)
}
```

### 3.4 Lock striping — `src/locks.rs`

```rust
pub struct StripedLocks {
    locks: Vec<parking_lot::Mutex<()>>,
    mask: usize,
}

impl StripedLocks {
    /// Create a lock table with `stripe_count` stripes (rounded up to power of 2).
    pub fn new(stripe_count: usize) -> Self;

    /// Acquire the lock for the given key. Returns a guard.
    pub fn lock(&self, key: &TxKey) -> parking_lot::MutexGuard<'_, ()>;

    /// Compute which stripe a key maps to (for testing).
    pub fn stripe_index(&self, key: &TxKey) -> usize;
}
```

Default stripe count: 65536. The stripe is chosen by hashing bytes 16-24 of the txid (different from both the index bucket and the fingerprint bytes).

#### 3.4.1 Lock + I/O interaction

**All I/O operations are submitted and completed INSIDE the lock hold.** The lock protects the entire read-validate-write sequence to ensure no concurrent mutation of the same record can interleave. The critical section is:

```
acquire lock
  -> pread metadata
  -> pread slot(s)
  -> validate in memory
  -> pwrite slot(s)
  -> pwrite metadata
  -> update secondary indexes (in-memory, no device I/O)
release lock
```

This means the lock hold time includes device I/O latency. At NVMe latencies (5-15 us for a single 4K read/write), a typical spend critical section is ~20-40 us. This is acceptable because:

1. The lock is per-txid (65536 stripes), so contention only occurs when multiple spends target the same transaction simultaneously.
2. `spendMulti` batches all slot reads into a single io_uring submission and all slot writes into another, so the lock hold time grows sub-linearly with batch size (dominated by the single round-trip, not per-item I/O).
3. The alternative -- releasing the lock between read and write -- would require optimistic concurrency control (generation checks, retries), adding complexity and tail latency for the common uncontended case.
4. With `SyncFallback` the I/O is sequential inside the lock; with `IoUringBackend` the batched reads/writes execute in parallel within a single `submit_and_wait` call, keeping the lock hold time short even for large `spendMulti` batches.

### 3.5 deleteAtHeight evaluation — `src/ops/delete_eval.rs`

Port the `setDeleteAtHeight` logic from teranode.lua lines 927-1008. This is called at the end of spend/unspend/setMined.

```rust
pub fn evaluate_delete_at_height(
    metadata: &TxMetadata,
    current_block_height: u32,
    block_height_retention: u32,
) -> (Signal, Option<TxMetadataPatch>)
```

The function returns:
- A `Signal` to send to the caller (ALLSPENT, NOTALLSPENT, DAHSET, DAHUNSET, or None)
- An optional metadata patch to apply (updated `delete_at_height`, `last_spent_state`)

Logic to port (from spec 3.13):
1. If `block_height_retention == 0`: return no signal, no patch
2. If `preserve_until != 0`: return no signal, no patch
3. If `CONFLICTING` flag set: set deleteAtHeight if not already set, signal DAHSET for external txs
4. Check all-spent: `spent_utxos == utxo_count` (utxo_count from the record header/metadata)
5. Track state transitions via `LAST_SPENT_ALL` flag -- only signal on changes
6. If allSpent AND hasBlocks AND onLongestChain (`unmined_since == 0`) -> set DAH
7. If conditions no longer met and DAH is set -> clear DAH, signal DAHUNSET

**Important**: This function must be pure -- it reads metadata and returns what to change, it does NOT perform I/O. The caller applies the patch.

#### 3.5.1 Secondary index updates after deleteAtHeight evaluation

When `evaluate_delete_at_height` returns a `TxMetadataPatch` that changes `delete_at_height` or when any operation changes `unmined_since`, the corresponding secondary indexes (spec 5.5) must be updated. This happens **inside the lock**, after the metadata pwrite, before the lock is released:

```rust
// After metadata write, still inside the lock:
if let Some(ref patch) = dah_patch {
    // DAH index (spec §5.5.1)
    if patch.new_delete_at_height != 0 && old_delete_at_height == 0 {
        // DAH was set: insert into DAH index
        secondary.dah_index.insert(patch.new_delete_at_height, req.tx_key);
    } else if patch.new_delete_at_height == 0 && old_delete_at_height != 0 {
        // DAH was cleared: remove from DAH index
        secondary.dah_index.remove(old_delete_at_height, &req.tx_key);
    } else if patch.new_delete_at_height != old_delete_at_height
              && patch.new_delete_at_height != 0 {
        // DAH value changed (e.g., bumped forward): move entry
        secondary.dah_index.remove(old_delete_at_height, &req.tx_key);
        secondary.dah_index.insert(patch.new_delete_at_height, req.tx_key);
    }
}

// Unmined index (spec §5.5.2) — only relevant if unmined_since changes,
// which does NOT happen in spend/unspend (it changes in setMined). Included
// here for completeness: setMined (Phase 4) follows the same pattern.
if new_unmined_since != old_unmined_since {
    if old_unmined_since != 0 {
        secondary.unmined_index.remove(old_unmined_since, &req.tx_key);
    }
    if new_unmined_since != 0 {
        secondary.unmined_index.insert(new_unmined_since, req.tx_key);
    }
}
```

The secondary index updates are in-memory operations (B-tree insert/remove) and do not involve device I/O. They are fast (sub-microsecond) and add negligible time to the critical section. The DAH index is not crash-critical (it is rebuilt from a device scan on recovery -- see spec 5.5), but keeping it up to date avoids stale pruner decisions during normal operation.

### 3.6 Spend operation — `src/ops/spend.rs`

The main spend function. Must implement ALL validation logic from the Lua code.

```rust
pub struct SpendRequest {
    pub tx_key: TxKey,
    pub offset: u32,
    pub utxo_hash: [u8; 32],
    pub spending_data: [u8; 36],  // txid(32) + vin(4 LE)
    pub ignore_conflicting: bool,
    pub ignore_locked: bool,
    pub current_block_height: u32,
    pub block_height_retention: u32,
}

pub struct SpendResponse {
    pub signal: Signal,
    pub block_ids: Vec<u32>,  // current blockIDs on the record
}

pub struct SpendMultiRequest {
    pub tx_key: TxKey,
    pub spends: Vec<SpendItem>,
    pub ignore_conflicting: bool,
    pub ignore_locked: bool,
    pub current_block_height: u32,
    pub block_height_retention: u32,
}

pub struct SpendItem {
    pub offset: u32,
    pub utxo_hash: [u8; 32],
    pub spending_data: [u8; 36],  // txid(32) + vin(4 LE)
    pub idx: u32,  // caller's identifier for this spend (for error mapping)
}

pub struct SpendMultiResponse {
    pub signal: Signal,
    pub block_ids: Vec<u32>,
    pub errors: HashMap<u32, SpendError>,  // idx -> error for failed spends
}
```

#### Spend implementation (matching Lua spendMulti lines 284-466):

```
fn spend_multi(
    io: &mut dyn DeviceIo,
    index: &Index,
    locks: &StripedLocks,
    secondary: &SecondaryIndexes,
    req: SpendMultiRequest,
) -> Result<SpendMultiResponse>
```

Step by step:
1. Acquire lock for `req.tx_key`
2. Index lookup -> get record_offset and utxo_count
3. If not found -> return TxNotFound
4. Read metadata from device via `io.submit_read` + `io.submit_and_wait`
5. If `CONFLICTING` flag and not `ignore_conflicting` -> return Conflicting
6. If `LOCKED` flag and not `ignore_locked` -> return Locked
7. If `IS_COINBASE` flag and `spending_height > current_block_height` -> return CoinbaseImmature
8. **Batch read** all requested UTXO slots: submit all via `io.submit_read`, then `io.submit_and_wait(count)`
9. For each spend item, validate:
    a. Slot exists (offset < utxo_count)
    b. Hash matches (memcmp first 32 bytes)
    c. If status == UNSPENT and `u32_from_le(spending_data[0..4]) != 0` and `>= current_block_height`: error FrozenUntil
    d. If status == PRUNED (0x02): error Pruned (terminal state, child tx was deleted)
    e. If status == SPENT (0x01) with same spending_data: idempotent success
    f. If status == SPENT with different data: error AlreadySpent with existing hex
    g. If status == FROZEN (0xFF): error Frozen
10. **Batch write** all valid slot mutations: submit via `io.submit_write`, then `io.submit_and_wait(count)`
11. Update metadata: increment `spent_utxos` by number of successful spends
12. **Mutation bookkeeping** (spec 3 note): increment `generation` by 1, set `updated_at` to current millisecond timestamp
13. Evaluate `deleteAtHeight`, apply metadata patch if needed (may update `delete_at_height`)
14. Write metadata via `io.submit_write` + `io.submit_and_wait(1)`
15. **Secondary index update**: if `delete_at_height` changed, update the DAH index (see 3.5.1)
16. Release lock (guard drops)
17. Return response with per-item errors and signal

#### Hex encoding for error messages

The Lua code returns spending data as reversed hex strings (lines 128-158). The Rust implementation must produce the same hex format for compatibility with the Go client. Port `spendingDataBytesToHex` exactly:
- First 32 bytes reversed, each byte as 2 hex digits
- Next 4 bytes in order, each as 2 hex digits

### 3.7 Unspend operation — `src/ops/unspend.rs`

Matching Lua lines 478-540.

```rust
pub struct UnspendRequest {
    pub tx_key: TxKey,
    pub offset: u32,
    pub utxo_hash: [u8; 32],
    pub current_block_height: u32,
    pub block_height_retention: u32,
}

pub struct UnspendResponse {
    pub signal: Signal,
}
```

Steps:
1. Acquire lock
2. Index lookup
3. Read metadata + the specific UTXO slot (via `io.submit_read` + `io.submit_and_wait`)
4. Validate hash
5. If slot is FULL_SIZE (spent or frozen):
   - If frozen: return FROZEN error
   - Otherwise: write slot back as unspent (status=0, zeroed spending_data) via `io.submit_write`
   - Decrement `spent_utxos`
6. **Mutation bookkeeping** (spec 3 note): increment `generation` by 1, set `updated_at` to current millisecond timestamp
7. Evaluate `deleteAtHeight`
8. Write metadata via `io.submit_write` + `io.submit_and_wait(1)`
9. **Secondary index update**: if `delete_at_height` changed, update the DAH index (see 3.5.1)
10. Release lock

### 3.8 Spending data encoding — NOT in server

Hex encoding of spending data (reversed txid + vin) is the client's responsibility.
TeraSlab returns raw bytes in error responses and over the wire protocol. The Lua
`spendingDataBytesToHex` format is a display concern that belongs in the Go client,
not in the storage server. This avoids unnecessary allocations on the hot path and
keeps the server encoding-agnostic.

## Acceptance criteria

### Spend correctness tests — one test per validation rule from the Lua code

```
- [ ] Spend unspent UTXO: succeeds, reading slot back shows status=SPENT, spending_data matches
- [ ] Spend on non-existent tx (not in index): returns TxNotFound
- [ ] Spend on conflicting tx, ignore_conflicting=false: returns Conflicting
- [ ] Spend on conflicting tx, ignore_conflicting=true: succeeds
- [ ] Spend on locked tx, ignore_locked=false: returns Locked
- [ ] Spend on locked tx, ignore_locked=true: succeeds
- [ ] Spend immature coinbase (spending_height=100, current=50): returns CoinbaseImmature
      with spending_height=100 and current_height=50
- [ ] Spend mature coinbase (spending_height=100, current=100): succeeds
- [ ] Spend mature coinbase (spending_height=100, current=200): succeeds
- [ ] Spend with non-matching utxoHash: returns UtxoHashMismatch
- [ ] Spend already-spent with SAME spending_data: no error (idempotent), counter NOT incremented again
- [ ] Spend already-spent with DIFFERENT spending_data: returns AlreadySpent with existing raw bytes
- [ ] Spend frozen UTXO: returns Frozen
- [ ] Spend PRUNED UTXO (status=0x02): returns Pruned error (terminal state)
- [ ] Spend UTXO with spendableIn height > current: returns FrozenUntil
- [ ] Spend UTXO with spendableIn height == current: returns FrozenUntil (matching Lua >= check)
- [ ] Spend UTXO with spendableIn height < current: succeeds
- [ ] Spend offset beyond utxo_count: returns UtxoNotFound
- [ ] spent_utxos counter increments by exactly 1 on successful spend
- [ ] spent_utxos counter does NOT increment on failed spend
- [ ] spent_utxos counter does NOT increment on idempotent re-spend
- [ ] generation increments by 1 on every successful spend (including idempotent)
- [ ] updated_at is set to current time on every successful spend
```

### SpendMulti tests

```
- [ ] Batch of 10 spends on same tx, all valid: all succeed, counter increments by 10
- [ ] Batch of 10 spends, items 3 and 7 have wrong hash: 8 succeed, errors map has keys 3,7
- [ ] Batch with mix of error types (one frozen, one already spent, one hash mismatch):
      each error correctly identified with correct error variant
- [ ] Empty spends list: returns OK with no errors
- [ ] Batch of 1 spend: same result as single spend
- [ ] Batch with duplicate offsets (spend same UTXO twice in one batch):
      first succeeds, second is idempotent (same spending_data)
- [ ] Batch with duplicate offsets but different spending_data:
      first succeeds, second returns AlreadySpent
- [ ] Response includes block_ids from the record's current mining state
- [ ] generation increments by exactly 1 for the whole batch (not per item)
- [ ] DAH index entry inserted when spendMulti triggers setDeleteAtHeight
```

### Unspend tests

```
- [ ] Unspend a spent UTXO: succeeds, slot shows status=UNSPENT, spending_data zeroed
- [ ] Unspend an already-unspent UTXO: succeeds (no-op, counter not decremented)
- [ ] Unspend a frozen UTXO: returns Frozen error
- [ ] Unspend on non-existent tx: returns TxNotFound
- [ ] Unspend with wrong hash: returns UtxoHashMismatch
- [ ] spent_utxos counter decrements by 1 on successful unspend
- [ ] spent_utxos counter does NOT decrement below 0 (if already 0)
- [ ] generation increments by 1 on successful unspend
- [ ] DAH index entry removed when unspend clears delete_at_height
```

### Signal / deleteAtHeight tests

```
- [ ] Spend last UTXO of tx that has blockIDs and is on longest chain:
      signal = DeleteAtHeightSet, metadata.delete_at_height = current + retention
- [ ] Spend last UTXO but tx has no blockIDs: no DAH set
- [ ] Spend non-last UTXO: signal = None (or NotAllSpent if transitioning)
- [ ] Unspend making it not-all-spent after AllSpent: signal = NotAllSpent
- [ ] Signal only fires on state CHANGE: spend two UTXOs in a row (not the last one each time),
      signal is None for both (no transition)
- [ ] All-spent to not-all-spent transition: LAST_SPENT_ALL flag updated in metadata
- [ ] Conflicting tx with no existing DAH: signal = DeleteAtHeightSet
- [ ] Conflicting tx with existing DAH: no signal (already set)
- [ ] PreserveUntil != 0: no DAH evaluation happens, signal = None
- [ ] block_height_retention = 0: no DAH evaluation happens, signal = None
- [ ] DAH conditions met, tx is external (EXTERNAL flag): signal = DeleteAtHeightSet
- [ ] DAH index contains the (height, txid) entry after DAH is set
- [ ] DAH index entry is removed after DAH is cleared
- [ ] DAH index entry is moved when DAH value changes (e.g., bumped forward)
```

### DeviceIo trait + io_uring tests

```
- [ ] IoUringBackend implements DeviceIo: single read matches synchronous pread
- [ ] IoUringBackend implements DeviceIo: single write persisted, readable via pread
- [ ] SyncFallback implements DeviceIo: single read matches synchronous pread
- [ ] SyncFallback implements DeviceIo: single write persisted, readable via pread
- [ ] Batch of 50 reads on different offsets via IoUringBackend: all return correct data
- [ ] Batch of 50 writes via IoUringBackend: all persisted correctly
- [ ] Batch of 50 reads via SyncFallback: all return correct data (executed sequentially)
- [ ] Batch of 50 writes via SyncFallback: all persisted correctly
- [ ] Interleaved reads and writes in same batch: all correct (both backends)
- [ ] io_uring with queue depth exceeded: returns error (not hang)
- [ ] create_device_io returns IoUringBackend on Linux >= 5.6
- [ ] create_device_io returns SyncFallback on macOS (or when io_uring init fails)
- [ ] Spend path produces identical results regardless of backend (run full spend test
      suite against both IoUringBackend and SyncFallback)
- [ ] Completion user_data matches submitted user_data for each operation
```

### Lock striping tests

```
- [ ] Two keys that map to different stripes: can be locked simultaneously
- [ ] Lock is exclusive: second lock on same key blocks until first released
- [ ] 65536 stripes: verify keys distribute across stripes (not all to stripe 0)
- [ ] Lock then drop guard: subsequent lock on same key succeeds immediately
```

### Concurrency tests

```
- [ ] 100 threads spending different UTXOs on same tx simultaneously:
      all succeed, counter = 100 at end
- [ ] 100 threads trying to spend SAME UTXO with SAME spending_data:
      all return success (idempotent), counter increments by exactly 1
- [ ] 100 threads trying to spend SAME UTXO with DIFFERENT spending_data:
      exactly one succeeds, rest get AlreadySpent
- [ ] 50 threads spending and 50 threads unspending different UTXOs on same tx:
      no corruption, counter is consistent at end
- [ ] Concurrent spendMulti on overlapping UTXOs:
      no double-increment, no corruption, partial errors are correct
- [ ] Concurrent operations on DIFFERENT transactions (different locks):
      no blocking, full parallelism
```

### Mutation bookkeeping tests

```
- [ ] Every mutation (spend, spendMulti, unspend) increments generation by exactly 1
- [ ] updated_at is set to a recent millisecond timestamp after each mutation
- [ ] Idempotent re-spend still increments generation (it is a mutation that was evaluated)
- [ ] No-op unspend (already unspent) does NOT increment generation
- [ ] spend increments spent_utxos; pruneSlot (Phase 6) increments pruned_utxos
```

### Secondary index integration tests

```
- [ ] Spend triggers DAH set: DAH index contains entry with correct height
- [ ] Unspend triggers DAH clear: DAH index no longer contains entry
- [ ] Two spends that both set DAH at different heights: both entries present
- [ ] Delete record removes DAH index entry
- [ ] DAH index range_scan(0..=height) returns correct set of txids
```

### Performance benchmarks (measured, not pass/fail)

```
- [ ] Single-threaded spend throughput on MemoryDevice: ops/sec
- [ ] Single-threaded spend throughput on real NVMe device: ops/sec
- [ ] 8-thread spend throughput (different UTXOs, same tx): ops/sec
- [ ] 16-thread spend throughput (different txs): ops/sec
- [ ] spendMulti throughput with batch sizes 1, 5, 10, 50: ops/sec per batch
- [ ] Latency histogram (p50, p90, p99, p99.9) at sustained load
- [ ] IoUringBackend vs SyncFallback comparison: throughput difference
- [ ] Lock hold time histogram (p50, p99) — should be < 50us at p99
```

### Observability integration

The spend path is where metrics instrumentation must be zero-cost. Introduce in this phase:

- **ThreadMetrics struct** with `CachePadded<AtomicU64>` counters (see spec 11.3.1)
- **ThreadHistograms struct** with pre-allocated HDR histograms for latency (see spec 11.3.2)
- Instrument `spend`, `spendMulti`, `unspend` with counter increments and latency recording
- **`tracing` integration**: ERROR for I/O failures, WARN for unexpected validation failures, DEBUG gated behind runtime flag. No INFO-level logging on the spend path.
- Verify: metrics recording adds < 20ns overhead per operation (benchmark with and without)

## NOT in this phase

- No networking (operations are called as library functions)
- No replication
- No creation path (records are pre-created in test setup)
- No freeze/unfreeze/reassign (Phase 6) — but note: Phase 6's `pruneSlot` increments `pruned_utxos` in the same metadata region that spend increments `spent_utxos`
- No tiered storage
- No unmined index updates (those happen in setMined, Phase 4) — the DAH index updates are introduced here because spend/unspend trigger `setDeleteAtHeight`
