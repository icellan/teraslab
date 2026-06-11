# Phase 7: Crash safety

**Status:** shipped — `src/redo.rs`, `src/recovery.rs`, `src/checkpoint.rs` in main; F-G4-* fix campaign closed the next_sequence + compaction issues; F-G5-022 engine-side atomic apply is a concurrency hypothesis with no live repro, tracked as P3 documentation work.

## Goal

Implement the redo log for crash recovery. After this phase, the system can survive power loss at any point during a write and recover to a consistent state.

## Dependencies

Phases 1-6 must be complete with all tests passing.

## Reference

- `specs/BSV_UTXO_STORE_SPEC.md` §7 (Crash Safety) and §5.5 (secondary index crash safety)
- The key property: all mutation operations (spend, unspend, setMined, freeze, etc.) are idempotent. Replaying an already-applied operation is harmless.

**Secondary index crash safety**: The Unmined index is critical and must be included in the redo log (unmined_since changes are logged and replayed). The DAH index is non-critical and can be rebuilt from a full device scan on recovery — delayed pruning is harmless. The freelist is checkpointed alongside the primary index and reconciled via Create/Delete redo entries on recovery.

## What to build

### 7.1 Redo log — `src/redo.rs`

A small circular log on a dedicated region of the device (or a separate device/file). Operations are appended before the corresponding data write, so that on crash recovery, any operation that was logged but not fully applied can be re-executed.

```rust
pub struct RedoLog {
    device: Arc<dyn BlockDevice>,
    log_offset: u64,           // start of redo log region on device
    log_size: u64,             // total size of redo log region
    write_pos: u64,            // current write position (wraps around)
    checkpoint_pos: u64,       // last checkpointed position (entries before this are committed)
    entry_buffer: Vec<u8>,     // current batch being assembled
}

#[derive(Debug, Clone)]
pub struct RedoEntry {
    pub sequence: u64,         // monotonically increasing sequence number
    pub op: RedoOp,
    pub checksum: u32,         // CRC32 of sequence + op bytes
}

#[derive(Debug, Clone)]
pub enum RedoOp {
    Spend {
        tx_key: TxKey,
        offset: u32,
        spending_data: [u8; 36],
        new_spent_count: u32,
    },
    Unspend {
        tx_key: TxKey,
        offset: u32,
        new_spent_count: u32,
    },
    SetMined {
        tx_key: TxKey,
        block_id: u32,
        block_height: u32,
        subtree_idx: u32,
        unset: bool,
    },
    Freeze {
        tx_key: TxKey,
        offset: u32,
    },
    Unfreeze {
        tx_key: TxKey,
        offset: u32,
    },
    Reassign {
        tx_key: TxKey,
        offset: u32,
        new_hash: [u8; 32],
        block_height: u32,
        spendable_after: u32,
    },
    PruneSlot {
        tx_key: TxKey,
        offset: u32,
    },
    MetadataUpdate {
        tx_key: TxKey,
        field: MetadataField,
        value: MetadataValue,
    },
    Create {
        tx_key: TxKey,
        record_offset: u64,
        utxo_count: u32,
    },
    Delete {
        tx_key: TxKey,
        record_offset: u64,
        record_size: u64,
    },
    Checkpoint,
}
```

### 7.2 RedoLog operations

```rust
impl RedoLog {
    /// Create or open a redo log at the given device region.
    pub fn open(device: Arc<dyn BlockDevice>, log_offset: u64, log_size: u64) -> Result<Self>;
    
    /// Append an operation to the redo log buffer.
    /// Does NOT flush to device yet.
    pub fn append(&mut self, op: RedoOp) -> Result<u64>; // returns sequence number
    
    /// Flush the current buffer to device.
    /// After this returns, all appended operations are durable.
    pub fn flush(&mut self) -> Result<()>;
    
    /// Append and flush in one call (for single-op transactions).
    pub fn append_and_flush(&mut self, op: RedoOp) -> Result<u64>;
    
    /// Write a checkpoint marker. All entries before this are committed
    /// and their data writes are guaranteed durable.
    pub fn checkpoint(&mut self) -> Result<()>;
    
    /// Read all entries after the last checkpoint (for crash recovery).
    pub fn recover(&self) -> Result<Vec<RedoEntry>>;
    
    /// Advance the checkpoint position, allowing old entries to be overwritten.
    pub fn advance_checkpoint(&mut self, up_to_sequence: u64) -> Result<()>;
    
    /// Current write position (for monitoring).
    pub fn write_position(&self) -> u64;
    
    /// Space remaining before the log wraps.
    pub fn available_space(&self) -> u64;
}
```

### 7.3 Log entry serialization

Each entry on disk:
```
[length: u32][sequence: u64][op_type: u8][op_data: variable][checksum: u32]
```

- `length` includes everything after itself (sequence + op_type + op_data + checksum)
- `checksum` is CRC32 of (sequence + op_type + op_data)
- Entry is valid only if length > 0 AND checksum matches
- A zero length marks the end of valid entries (for recovery scanning)

### 7.4 Recovery procedure — `src/recovery.rs`

```rust
pub fn recover(
    device: &dyn BlockDevice,
    redo_log: &RedoLog,
    index: &mut Index,
    allocator: &mut SlotAllocator,
) -> Result<RecoveryStats>

pub struct RecoveryStats {
    pub entries_replayed: u64,
    pub entries_skipped: u64,  // already applied (idempotent)
    pub errors: Vec<RecoveryError>,
}
```

Recovery steps:
1. Read all redo entries after last checkpoint
2. For each entry, in order:
   a. Look up the tx in the index
   b. Read the current state from device
   c. Check if the operation is already applied (idempotent check):
      - For Spend: is the slot already spent with this spending_data?
      - For SetMined: does the block entry already exist?
      - For Create: does the record already exist in the index?
   d. If not applied: re-execute the operation
   e. If already applied: skip (increment skipped counter)
3. After all entries replayed: write a new checkpoint

### 7.5 Integration with mutation operations

Modify all operations from Phases 3-6 to log before writing:

```rust
// In spend():
redo_log.append(RedoOp::Spend { ... })?;
redo_log.flush()?;
// Now do the actual data write
write_utxo_slot(device, ...)?;
write_metadata(device, ...)?;
```

The flush ensures the redo entry is durable before the data write. If power fails between the redo flush and the data write, recovery will re-apply the spend.

For batched operations (spendMulti), log the entire batch before writing any data:
```rust
for item in &successful_spends {
    redo_log.append(RedoOp::Spend { ... })?;
}
redo_log.flush()?;  // single flush for the batch
// Now batch-write all slots
```

### 7.6 Checkpoint strategy

Checkpoints are written periodically (e.g., every N operations or every T seconds). A checkpoint means: all operations before this point have been fully applied to the data device and are durable. The redo log space before the checkpoint can be reclaimed.

The checkpoint is a special redo entry. When the data device is synced (`device.sync()`), a checkpoint can safely be written.

## Acceptance criteria

### Redo log basic tests

```
- [ ] Append entry, flush, recover: entry is returned
- [ ] Append 100 entries, flush, recover: all 100 returned in order
- [ ] Append entry, no flush, recover: entry NOT returned (not durable)
- [ ] Append, flush, checkpoint, recover: no entries (all before checkpoint)
- [ ] Append A, flush, checkpoint, append B, flush, recover: only B returned
- [ ] Entry checksum validation: corrupt one byte in entry, recover skips it
- [ ] Truncated entry (power fail mid-write): recover returns entries before it
```

### Circular log tests

```
- [ ] Fill log to near capacity: verify wrapping works
- [ ] Checkpoint to reclaim space, write more: verify no corruption
- [ ] Log exactly full: returns error on next append (before checkpoint frees space)
- [ ] Rapid checkpoint + append cycles: no space leak
```

### Serialization tests

```
- [ ] Each RedoOp variant: serialize → deserialize round-trip matches
- [ ] Entry with maximum-size op: serializes and deserializes correctly
- [ ] Zero-length entry marks end of valid data
```

### Recovery correctness tests

```
- [ ] Crash between redo flush and data write (spend):
      Recovery replays the spend, slot is now spent
- [ ] Crash between redo flush and data write (setMined):
      Recovery replays setMined, block entry is now present
- [ ] Crash between redo flush and data write (create):
      Recovery completes the creation
- [ ] Crash between redo flush and data write (delete):
      Recovery completes the deletion
- [ ] No crash, recover with already-applied entries:
      All entries skipped, no double-application, counters correct
- [ ] Double-spend after recovery: second spend is correctly idempotent
- [ ] Recovery of spendMulti batch: all spends in batch are replayed
```

### Idempotency verification tests

```
- [ ] Spend same UTXO twice via recovery: counter increments only once
- [ ] SetMined same block twice via recovery: only one entry in block list
- [ ] Create same tx twice via recovery: no duplicate, no corruption
- [ ] Unspend already-unspent via recovery: no counter underflow
- [ ] Freeze already-frozen via recovery: no error, state unchanged
```

### Crash simulation tests

Use the MemoryDevice to simulate crashes at specific byte offsets:

```
- [ ] Crash at every byte of a redo entry write: recovery always succeeds
      (may skip the incomplete entry, but never corrupts state)
- [ ] Crash at every byte of a data write (after redo): recovery replays correctly
- [ ] 1000 random crash points in a sequence of 100 operations:
      recovery always produces consistent state
- [ ] Verify: after recovery, index and on-disk data agree
      (every index entry points to a valid record, every valid record is in the index)
```

### Performance impact tests

```
- [ ] Measure spend throughput WITH redo log vs WITHOUT: report overhead percentage
- [ ] Measure redo log flush latency (single entry, batch of 10, batch of 100)
- [ ] Verify redo log I/O is sequential (not random)
```

## NOT in this phase

- No replication (redo log becomes the replication stream in Phase 8)
- No network
