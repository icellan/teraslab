# Phase 5: Creation path

**Status:** shipped — `src/ops/create.rs` + allocator integration in main.

## Goal

Implement record creation — the path that allocates space, initializes metadata and UTXO slots, and registers the record in the index. This is the write-once path that sets up the fixed-layout record that all subsequent mutations operate on.

## Dependencies

Phases 1-4 must be complete with all tests passing.

## Reference

- `specs/BSV_UTXO_STORE_SPEC.md` §3.3 (create/createBatch) and §4.4 (creation pipeline)
- The Go `create.go` / `createBatch` in the Teranode repo (analyze when available)

## What to build

### 5.1 Creation request/response — `src/ops/create.rs`

```rust
pub struct CreateRequest {
    pub tx_id: [u8; 32],
    pub tx_version: u32,
    pub locktime: u32,
    pub fee: u64,
    pub size_in_bytes: u64,
    pub extended_size: u64,
    pub is_coinbase: bool,
    pub spending_height: u32,       // 0 if not coinbase
    pub utxo_hashes: Vec<[u8; 32]>, // one hash per output
    pub inputs: Option<Vec<u8>>,    // raw input data (None if external)
    pub outputs: Option<Vec<u8>>,   // raw output data (None if external)
    pub is_external: bool,          // if true, inputs/outputs stored in blob store
    pub created_at: u64,            // millisecond timestamp
}

pub struct CreateResponse {
    pub record_offset: u64,
    pub utxo_count: u32,
}

pub struct BatchCreateRequest {
    pub transactions: Vec<CreateRequest>,
}

pub struct BatchCreateResponse {
    pub results: Vec<Result<CreateResponse, CreateError>>,
}

#[derive(Debug)]
pub enum CreateError {
    DeviceFull,
    DuplicateTxId,
    InvalidUtxoCount,  // 0 UTXOs
    IoError(std::io::Error),
}
```

### 5.2 Creation implementation

```rust
pub fn create(
    device: &dyn BlockDevice,
    index: &mut Index,
    allocator: &mut SlotAllocator,
    req: CreateRequest,
) -> Result<CreateResponse, CreateError>
```

Steps:
1. Check index for duplicate txid → return DuplicateTxId if exists
2. Validate: utxo_hashes must not be empty
3. Calculate record size: `TxMetadata::record_size(utxo_count)`
4. If inline cold data provided: add cold data size to allocation
5. Allocate space via `allocator.allocate(total_size)`
6. Initialize TxMetadata with all fields from request:
   - magic = METADATA_MAGIC
   - version = METADATA_VERSION
   - utxo_count = utxo_hashes.len()
   - spent_utxos = 0
   - block_entry_count = 0
   - flags = TxFlags from request (IS_COINBASE, CONFLICTING, LOCKED, EXTERNAL as needed)
   - unmined_since = block_height (if no mined_block_infos), else 0
   - delete_at_height = 0
   - preserve_until = 0
7. Initialize all UTXO slots:
   - Each slot: hash = utxo_hashes[i], status = UNSPENT (0x00), spending_data = zeroed
   - If frozen option: status = FROZEN (0xFF), spending_data = all 0xFF
   - All slots are full 69-byte size from the start
8. Write the complete record in one operation: metadata + all slots (+ inline cold data if present)
   - Use `write_full_record` or a single large `pwrite`
   - This should be ONE write, not N+1 separate writes
9. Register in index: key=txid, entry=TxIndexEntry with offsets and sizes
10. Return CreateResponse

### 5.3 Batch creation

`create_batch` should process multiple creations efficiently:
- Allocate all regions first (fail fast if device is full)
- Write all records (potentially via io_uring batch)
- Register all in index
- If any individual creation fails, roll back its allocation but continue with others

### 5.4 Cold data placement

For records with inline inputs/outputs (small txs, < threshold):
- Cold data is appended after the UTXO slots in the same allocation
- cold_offset in the index entry points to this data
- cold_size records the length

```
[TxMetadata | Slot0 | Slot1 | ... | SlotN-1 | inputs_bytes | outputs_bytes]
                                               ^-- cold_offset points here
```

The cold data is written once during creation and never modified.

### 5.5 No creating flag

The `creating` flag from the previous design is eliminated. It only existed to block spending during multi-record 2-phase commit (master + child records). Since TeraSlab writes the entire record atomically in one operation, there is no window where a partially-created record could be spent. The record either exists in the index (fully created) or doesn't.

## Acceptance criteria

### Creation correctness tests

```
- [ ] Create tx with 1 UTXO: record exists, metadata correct, slot has correct hash, status=UNSPENT
- [ ] Create tx with 100 UTXOs: all 100 slots correct, all unspent
- [ ] Create tx with 10000 UTXOs: all slots correct (tests large allocation)
- [ ] Created record has correct magic number
- [ ] Created record has correct schema version
- [ ] Created record has all metadata fields matching request
- [ ] spent_utxos = 0 after creation
- [ ] block_entry_count = 0 after creation
- [ ] Index lookup after creation returns correct record_offset and utxo_count
- [ ] Create then spend UTXO 0: full lifecycle works
- [ ] Create then setMined: full lifecycle works
```

### Duplicate detection tests

```
- [ ] Create same txid twice: second returns DuplicateTxId
- [ ] Create txid A, delete it, create txid A again: succeeds (txid can be reused)
```

### Allocation tests

```
- [ ] Record is allocated at aligned offset
- [ ] Two records don't overlap on device
- [ ] Record size matches metadata_size + utxo_count * slot_size + cold_size
```

### Cold data tests

```
- [ ] Create with inline inputs/outputs: cold_offset and cold_size are set in index
- [ ] Read back cold data at cold_offset: matches original inputs/outputs
- [ ] Create without inputs/outputs (external): cold_offset = 0, cold_size = 0
- [ ] Cold data region is NOT modified by spend or setMined operations
```

### Batch creation tests

```
- [ ] Batch create 10 txs: all 10 created successfully
- [ ] Batch create with one duplicate txid: 9 succeed, 1 returns DuplicateTxId
- [ ] Batch create when device has space for 5 of 10: first 5 succeed, rest fail with DeviceFull
      (failed allocations are rolled back)
- [ ] Batch create with io_uring: verify all records written correctly
```

### Edge case tests

```
- [ ] Create with 0 UTXOs: returns InvalidUtxoCount
- [ ] Create coinbase tx: IS_COINBASE flag set, spending_height set correctly
- [ ] Create with spending_height=0 (non-coinbase): verify no maturity check on spend
- [ ] Create with frozen=true: all slots have status=FROZEN, spending_data all 0xFF
- [ ] Create with conflicting=true: CONFLICTING flag set
- [ ] Create with unmined (no block info): unmined_since = block_height
- [ ] Create with mined block info: unmined_since = 0, block_entries populated
```

### Performance benchmarks

```
- [ ] Single record creation throughput (varying UTXO counts: 1, 10, 100, 1000)
- [ ] Batch creation throughput (10, 100, 1000 records per batch)
- [ ] Creation with inline cold data vs without: I/O overhead comparison
```

## NOT in this phase

- No external blob store writing (Phase 11)
- No lock records (this was a mechanism from the previous design that may not be needed)
- No read/query path (that's part of Phase 10 wire protocol)
