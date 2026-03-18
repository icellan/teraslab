# Phase 1: Storage layout and raw device I/O

## Goal

Implement the on-disk record format and raw device read/write primitives. No networking, no clustering, no replication, no concurrency. Just bytes on disk with correct alignment and layout.

## Dependencies

None — this is the foundation.

## Reference

- `specs/BSV_UTXO_STORE_SPEC.md` §2 (Data Model) — the authoritative source for record layout
- `specs/teranode.lua` → UTXO_HASH_SIZE (32), SPENDING_DATA_SIZE (36), FROZEN_BYTE (255)

## What to build

### 1.1 Record layout types — `src/record.rs`

Define the on-disk data structures. All must be `#[repr(C, packed)]` with zero implicit padding.

#### UtxoSlot (exactly 69 bytes)

```rust
#[repr(C, packed)]
pub struct UtxoSlot {
    pub hash: [u8; 32],          // UTXO hash, always present
    pub status: u8,              // see status constants below
    pub spending_data: [u8; 36], // txid(32) + vin(4 LE), zeroed when unspent
}
```

Status values (define as constants):
- `UTXO_UNSPENT = 0x00`
- `UTXO_SPENT = 0x01`
- `UTXO_PRUNED = 0x02` — terminal state, child tx was pruned/deleted
- `UTXO_FROZEN = 0xFF`

The 4-byte vin matches the Bitcoin protocol and Go SpendingData struct. Frozen slots have all 36 bytes of spending_data set to 0xFF. Pruned slots preserve the spending_data from the last spend (for audit/debugging) but are permanently non-spendable.

#### BlockEntry (exactly 12 bytes)

```rust
#[repr(C, packed)]
pub struct BlockEntry {
    pub block_id: u32,
    pub block_height: u32,
    pub subtree_idx: u32,
}
```

This replaces the three parallel lists (`blockIDs`, `blockHeights`, `subtreeIdxs`) from the Lua code. 3 entries are stored inline in the metadata region. Overflow beyond 3 uses an extension block on device (see §2.5 of the spec).

#### ExternalRef (for large transactions)

```rust
#[repr(C, packed)]
pub struct ExternalRef {
    pub store_type: u8,         // 0=inline, 1=local_file, 2=object_store
    pub content_hash: [u8; 32], // txID, used as blob key
    pub total_size: u64,        // original size in bytes
    pub input_count: u32,
    pub output_count: u32,
    pub inputs_offset: u64,     // byte offset within blob for inputs
    pub outputs_offset: u64,    // byte offset within blob for outputs
}
```

#### TxFlags — packed bitfield

```rust
bitflags! {
    #[repr(transparent)]
    pub struct TxFlags: u8 {
        const IS_COINBASE    = 0b0000_0001;  // bit 0 — write-once (Create)
        const CONFLICTING    = 0b0000_0010;  // bit 1 — mutable (SetConflicting)
        const LOCKED         = 0b0000_0100;  // bit 2 — mutable (SetLocked, SetMined clears)
        const EXTERNAL       = 0b0000_1000;  // bit 3 — write-once (Create, large tx)
        const LAST_SPENT_ALL = 0b0001_0000;  // bit 4 — mutable (setDeleteAtHeight signaling)
        // bits 5-7 reserved
    }
}
```

This replaces the separate `is_coinbase`, `conflicting`, `locked`, `external`, and `last_spent_state` fields. The `creating` flag from the Aerospike design is eliminated — it only existed for multi-record 2-phase commit, which is unnecessary with single-record atomic writes.

#### TxMetadata

The full metadata header. This must be a fixed size. All fields from the spec:

```rust
pub const INLINE_BLOCK_ENTRIES: usize = 3;

#[repr(C, packed)]
pub struct TxMetadata {
    // Record identification
    pub magic: u32,                          // Magic number for validation (0x534C4142)
    pub schema_version: u32,                 // Schema version for forward compat
    pub record_size: u32,                    // Total record size in bytes
    pub utxo_count: u32,                     // Number of UTXO slots allocated

    // Transaction data (write-once)
    pub tx_id: [u8; 32],                     // Transaction hash
    pub tx_version: u32,                     // Bitcoin tx version field
    pub locktime: u32,
    pub fee: u64,
    pub size_in_bytes: u64,
    pub extended_size: u64,
    pub flags: TxFlags,                      // packed bitfield (see above)
    pub spending_height: u32,                // coinbase maturity height
    pub created_at: u64,                     // millisecond timestamp

    // Mutable counters
    pub spent_utxos: u32,                    // number of spent UTXOs
    pub pruned_utxos: u32,                   // number of PRUNED slots (child tx deleted)
    pub generation: u32,                     // incremented on every mutation (spend, setMined, etc.)
    pub updated_at: u64,                     // millis timestamp, set on every mutation

    // Block/mining state — 3 inline + overflow pointer
    pub block_entry_count: u8,               // total entries (inline + overflow)
    pub block_entries_inline: [BlockEntry; INLINE_BLOCK_ENTRIES], // 36 bytes
    pub block_overflow_offset: u64,          // 0 = no overflow; else device offset to extension block
    pub reassignment_offset: u64,            // 0 = no reassignments; else device offset to extension block
    pub reassignment_count: u8,              // number of reassignments (without reading extension block)

    // Deletion/retention (0 = not set for all three)
    pub unmined_since: u32,                  // block height, 0 = mined on longest chain
    pub delete_at_height: u32,              // 0 = not set
    pub preserve_until: u32,                // 0 = not set

    // External reference (for large txs)
    pub external_ref: ExternalRef,

    // Padding to align to a known boundary
    pub _padding: [u8; METADATA_PADDING],    // pad to METADATA_SIZE
}
```

Calculate `METADATA_PADDING` so that `size_of::<TxMetadata>()` rounds up to the nearest multiple of 64 bytes (cache line alignment). Define `METADATA_SIZE` as the padded size.

**Metadata is placed first** in the record at a fixed compile-time size. This eliminates the need for a separate record header — metadata IS the header. UTXO slot offsets are deterministic: `METADATA_SIZE + vout * 69`. This saves one `pread` on every hot-path operation.

**Fields eliminated from the Aerospike design:**
- `record_utxos` — redundant; the all-spent check uses `utxo_count`
- `total_utxos` — redundant; same as `utxo_count`
- `creating` — no multi-record 2-phase commit
- `unmined_since_set` / `delete_at_height_set` / `preserve_until_set` — use 0 as "not set" sentinel
- `spendable_in` / `utxoSpendableIn` — spendable height encoded directly in UtxoSlot's `spending_data[0..4]` for unspent slots (0 = immediately spendable)

#### TxRecord helpers

Implement methods to compute byte offsets:

```rust
impl TxMetadata {
    /// Byte offset from the start of the record to UTXO slot N
    pub fn utxo_slot_offset(slot_index: u32) -> u64 {
        METADATA_SIZE as u64 + (slot_index as u64) * UTXO_SLOT_SIZE as u64
    }

    /// Total byte size of a record with N UTXO slots (metadata + slots)
    pub fn record_size(utxo_count: u32) -> u64 {
        METADATA_SIZE as u64 + (utxo_count as u64) * UTXO_SLOT_SIZE as u64
    }
}
```

Constants:

```rust
pub const UTXO_SLOT_SIZE: usize = std::mem::size_of::<UtxoSlot>();  // must be 69
pub const BLOCK_ENTRY_SIZE: usize = std::mem::size_of::<BlockEntry>(); // must be 12
pub const METADATA_MAGIC: u32 = 0x534C4142; // "SLAB" in ASCII
pub const METADATA_VERSION: u32 = 1;
```

### 1.2 Device abstraction — `src/device.rs`

A trait and implementation for raw block device I/O.

```rust
pub trait BlockDevice: Send + Sync {
    /// Read `buf.len()` bytes starting at `offset`. Both must be aligned.
    fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// Write `buf` starting at `offset`. Both must be aligned.
    fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize>;

    /// The minimum I/O alignment for this device (512 or 4096).
    fn alignment(&self) -> usize;

    /// Total usable size in bytes.
    fn size(&self) -> u64;

    /// Sync all pending writes to stable storage.
    fn sync(&self) -> Result<()>;
}
```

Implement two backends:

1. **`DirectDevice`** — opens a file or block device with `O_DIRECT | O_RDWR`. Detects the device's minimum I/O size from `/sys/block/<dev>/queue/physical_block_size` (for block devices) or defaults to 4096 for files. All reads and writes must be aligned to this boundary — return a clear error if alignment is violated. Use `posix_memalign` or aligned allocators for the I/O buffers.

2. **`MemoryDevice`** — an in-memory `Vec<u8>` that simulates a block device for testing. Same alignment enforcement. This is NOT a stub — it must enforce the same alignment constraints as `DirectDevice` so that tests catch alignment bugs.

#### Aligned buffer helper

Provide a helper for allocating properly aligned buffers:

```rust
pub struct AlignedBuf {
    ptr: *mut u8,
    len: usize,
    alignment: usize,
}
```

This must be backed by `posix_memalign` (or `std::alloc::alloc` with `Layout::from_size_align`) and implement `Deref<Target=[u8]>`, `DerefMut`, and `Drop`.

### 1.3 Slot allocator — `src/allocator.rs`

Manages free space on the device. The device is divided into:
- A fixed header region at offset 0 (device metadata + freelist)
- The data region from `DATA_REGION_OFFSET` onward

```rust
pub struct SlotAllocator {
    device: Arc<dyn BlockDevice>,
    freelist: Vec<FreeRegion>,    // sorted by offset
    next_offset: u64,             // append point for new allocations
    data_region_start: u64,
    device_size: u64,
}

struct FreeRegion {
    offset: u64,
    size: u64,
}
```

Methods:

- `allocate(size: u64) -> Result<u64>` — returns the byte offset of the allocated region. First checks the freelist for a best-fit region. If none found, appends at `next_offset`. The returned offset must be aligned to the device's minimum I/O size.
- `free(offset: u64, size: u64) -> Result<()>` — returns a region to the freelist. Merges adjacent free regions.
- `persist(&self) -> Result<()>` — writes the freelist and `next_offset` to the device header region so it survives restart. The freelist is checkpointed alongside the primary index (see spec §4.2).
- `recover(device: Arc<dyn BlockDevice>) -> Result<Self>` — reads the persisted freelist from the device header and reconstructs the allocator state.

The freelist is serialized as a simple length-prefixed array of `(offset: u64, size: u64)` pairs in the device header.

### 1.4 Read/write helpers — `src/io.rs`

Convenience functions that handle alignment padding. When the data to write is smaller than the device's minimum I/O size, these functions must:

1. Read the full aligned block from disk
2. Modify the relevant bytes within the block
3. Write the full aligned block back

This is the read-modify-write pattern necessary for sub-block writes with `O_DIRECT`.

```rust
/// Read a single UtxoSlot at the given slot index within a record.
pub fn read_utxo_slot(
    device: &dyn BlockDevice,
    record_offset: u64,
    slot_index: u32,
) -> Result<UtxoSlot>;

/// Write a single UtxoSlot at the given slot index within a record.
/// Handles alignment: reads the containing block, modifies the slot bytes, writes back.
pub fn write_utxo_slot(
    device: &dyn BlockDevice,
    record_offset: u64,
    slot_index: u32,
    slot: &UtxoSlot,
) -> Result<()>;

/// Read the TxMetadata header of a record.
pub fn read_metadata(
    device: &dyn BlockDevice,
    record_offset: u64,
) -> Result<TxMetadata>;

/// Write the TxMetadata header of a record.
pub fn write_metadata(
    device: &dyn BlockDevice,
    record_offset: u64,
    metadata: &TxMetadata,
) -> Result<()>;

/// Write a complete new record (metadata + all UTXO slots) in one operation.
/// Used at creation time. The buffer must be properly aligned.
pub fn write_full_record(
    device: &dyn BlockDevice,
    record_offset: u64,
    metadata: &TxMetadata,
    slots: &[UtxoSlot],
) -> Result<()>;

/// Read a range of UTXO slots (for spendMulti batch reads).
pub fn read_utxo_slots(
    device: &dyn BlockDevice,
    record_offset: u64,
    slot_indices: &[u32],
) -> Result<Vec<(u32, UtxoSlot)>>;
```

## Acceptance criteria

ALL of the following tests must exist and pass.

### Layout size tests

```
- [ ] assert_eq!(std::mem::size_of::<UtxoSlot>(), UTXO_SLOT_SIZE)
- [ ] assert_eq!(std::mem::size_of::<BlockEntry>(), BLOCK_ENTRY_SIZE)
- [ ] assert_eq!(std::mem::size_of::<BlockEntry>(), 12)
- [ ] METADATA_SIZE is a multiple of 64
- [ ] UTXO_SLOT_SIZE == 69
- [ ] size_of::<TxFlags>() == 1
```

### Layout field offset tests

```
- [ ] UtxoSlot::hash is at offset 0
- [ ] UtxoSlot::status is at offset 32
- [ ] UtxoSlot::spending_data is at offset 33
- [ ] BlockEntry::block_id is at offset 0
- [ ] BlockEntry::block_height is at offset 4
- [ ] BlockEntry::subtree_idx is at offset 8
```

Use `std::mem::offset_of!` (Rust 1.77+) or `memoffset` crate.

### Round-trip serialization tests

```
- [ ] Create UtxoSlot with known data → write to [u8] → read back → all fields equal
- [ ] Create UtxoSlot as unspent (status=0x00, zeroed spending_data) → verify
- [ ] Create UtxoSlot as spent (status=0x01, known spending_data) → verify
- [ ] Create UtxoSlot as pruned (status=0x02, preserved spending_data) → verify
- [ ] Create UtxoSlot as frozen (status=0xFF, all 0xFF spending_data) → verify
- [ ] Create BlockEntry with known data → write to [u8] → read back → equal
- [ ] Create TxMetadata with all fields set → write → read back → all fields equal
- [ ] Create TxMetadata with magic number → verify magic is correct after read
- [ ] Create TxMetadata with zero block entries → verify block_entry_count is 0
- [ ] Create TxMetadata with 3 block entries → verify all 3 read back correctly
- [ ] TxFlags bitfield: set IS_COINBASE | LOCKED → verify bits correct, other bits zero
- [ ] TxFlags bitfield: set all flags → verify byte value
```

### Device tests (use MemoryDevice AND tempfile-backed DirectDevice)

```
- [ ] Open MemoryDevice, write 4096 bytes at offset 0, read back, verify equal
- [ ] Open DirectDevice with tempfile, write aligned data, read back, verify equal
- [ ] Write at non-aligned offset → returns alignment error
- [ ] Write buffer with non-aligned length → returns alignment error
- [ ] Read at non-aligned offset → returns alignment error
- [ ] Write at multiple offsets, read each back, verify no cross-contamination
- [ ] Write to last valid offset on device, verify success
- [ ] Write past device boundary → returns error
- [ ] Sync completes without error
- [ ] MemoryDevice enforces same alignment constraints as DirectDevice
```

### AlignedBuf tests

```
- [ ] Allocate AlignedBuf with alignment 512, verify pointer alignment
- [ ] Allocate AlignedBuf with alignment 4096, verify pointer alignment
- [ ] Write data to AlignedBuf, read back via slice, verify equal
- [ ] Drop AlignedBuf, verify no memory leak (run under valgrind or miri)
- [ ] AlignedBuf with zero length → does not panic
```

### Allocator tests

```
- [ ] Allocate region, verify returned offset >= data_region_start
- [ ] Allocate region, verify returned offset is aligned to device alignment
- [ ] Allocate two regions, verify no overlap
- [ ] Allocate 100 regions of varying sizes, verify all are non-overlapping and aligned
- [ ] Free a region, allocate same size, verify reuse (offset matches freed region)
- [ ] Free two adjacent regions, verify they merge into one free region
- [ ] Free a region in the middle, allocate smaller size, verify it fits in freed space
- [ ] Persist freelist, create new allocator via recover(), verify identical state
- [ ] Persist, recover, allocate, verify new allocation doesn't overlap recovered state
- [ ] Allocate until device is full (within alignment), verify returns error on next alloc
- [ ] Fragment: allocate A,B,C,D; free B and D; allocate E(size=B+D); verify E uses B's space
```

### I/O helper tests (using MemoryDevice)

```
- [ ] write_full_record then read_metadata: metadata matches
- [ ] write_full_record with 10 slots, read_utxo_slot for each: all match
- [ ] write_full_record, then write_utxo_slot to modify slot 5: only slot 5 changed
- [ ] write_utxo_slot to slot 5, read slots 4 and 6: they are unchanged
- [ ] write_metadata to update spent_utxos counter: counter changed, UTXO slots unchanged
- [ ] read_utxo_slots with indices [0, 5, 9]: returns all three correctly
- [ ] read_utxo_slots with empty indices: returns empty vec
- [ ] Record with 1000 slots: write and read slot 999 correctly
- [ ] Record with 1000 slots: write slot 0 doesn't corrupt slot 999
```

### Integration test

```
- [ ] Full lifecycle:
      1. Create allocator on MemoryDevice (1 GB)
      2. Allocate space for a record with 100 UTXO slots
      3. Write full record with metadata + 100 unspent slots
      4. Read back metadata, verify utxo_count=100, spent_utxos=0
      5. Read back each slot, verify all are unspent (status=0x00)
      6. Write spent data to slot 50
      7. Read slot 50: verify status=0x01, spending_data matches
      8. Read slot 49 and 51: verify still unspent
      9. Update metadata spent_utxos=1
      10. Read metadata: verify spent_utxos=1, all other fields unchanged
      11. Free the record
      12. Allocate new record at same location
      13. Write new record, verify old data is gone
```

## NOT in this phase

- No io_uring (use synchronous pread/pwrite — io_uring is added in Phase 3)
- No index (use allocator offsets directly in tests)
- No concurrency (single-threaded only)
- No networking
- No replication
- No cold data / tiered storage (Phase 11)
- No block entry overflow handling (extension blocks are Phase 4 concern)
