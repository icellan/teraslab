# Phase 2: Index

## Goal

Implement the in-memory hash index that maps transaction keys to their on-disk locations. This is the lookup structure that makes every operation O(1) — hash the key, find the device offset, do the I/O.

## Dependencies

Phase 1 must be complete with all tests passing.

## Reference

- `specs/BSV_UTXO_STORE_SPEC.md` §5 (Index) — including §5.5 (secondary indexes for pruner)
- The original implementation's primary index uses 64 bytes per entry in red-black tree sprigs. Our index should use ~24-32 bytes per entry in a flat hash table — no tree traversal, no pointer chasing.

**Note**: This phase covers the primary hash index only. The secondary indexes for pruner queries (DAH index, Unmined index per spec §5.5) should also be built in this phase — they are lightweight B-tree/sorted structures maintained in memory alongside the primary index.

## What to build

### 2.1 Index entry — `src/index.rs`

```rust
/// What the index stores for each transaction.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TxIndexEntry {
    pub device_id: u16,       // which device this record lives on (for multi-device)
    pub record_offset: u64,   // byte offset on that device to the start of TxMetadata
    pub utxo_count: u32,      // number of UTXO slots in this record
    pub cold_offset: u64,     // byte offset to inline cold data (0 if none/external)
    pub cold_size: u32,       // size of inline cold data (0 if none/external)
    pub flags: u8,            // bit flags: has_external_ref, etc.
}
```

### 2.2 Transaction key — `src/index.rs`

The primary key for lookups. In Teranode, records are keyed by txid (32 bytes) plus an optional record index (for the current multi-record pagination). Since TeraSlab eliminates pagination, the key is just the txid:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TxKey {
    pub txid: [u8; 32],
}
```

Since txid is already a cryptographic hash (double-SHA256), it has excellent distribution. Use the first 8 bytes directly as the hash table bucket index — no need for a secondary hash function.

### 2.3 Hash table — `src/index/hashtable.rs`

Implement a Robin Hood open-addressing hash table. Robin Hood hashing has more predictable probe sequences than basic linear probing, which matters for performance at high load factors.

```rust
pub struct HashTable {
    buckets: *mut Bucket,         // mmap'd region
    capacity: usize,              // total number of buckets (power of 2)
    count: usize,                 // number of occupied entries
    mask: usize,                  // capacity - 1, for fast modulo
    mmap_len: usize,              // length of the mmap'd region
    hugepage: bool,               // whether hugepages were used
}

#[repr(C)]
struct Bucket {
    fingerprint: u64,             // first 8 bytes of txid (for fast comparison)
    entry: TxIndexEntry,
    probe_distance: u16,          // Robin Hood probe distance
    occupied: u8,                 // 0=empty, 1=occupied, 2=tombstone
    _padding: [u8; 5],           // pad to cache-line-friendly size
}
```

Size each `Bucket` to be a power-of-2 or cache-line-friendly size. At billions of entries, even 1 byte of waste per bucket adds up to gigabytes. Target 64 bytes per bucket (one cache line) if possible.

#### Core operations:

```rust
impl HashTable {
    /// Create a new hash table with the given initial capacity.
    /// Capacity is rounded up to the next power of 2.
    /// Uses mmap with MAP_HUGETLB if available, falls back to regular mmap.
    pub fn new(initial_capacity: usize) -> Result<Self>;
    
    /// Look up a transaction by key. O(1) expected.
    pub fn get(&self, key: &TxKey) -> Option<&TxIndexEntry>;
    
    /// Insert or update an entry. Returns the previous entry if updating.
    pub fn insert(&mut self, key: TxKey, entry: TxIndexEntry) -> Result<Option<TxIndexEntry>>;
    
    /// Remove an entry. Returns the removed entry if it existed.
    pub fn remove(&mut self, key: &TxKey) -> Option<TxIndexEntry>;
    
    /// Number of entries in the table.
    pub fn len(&self) -> usize;
    
    /// Load factor (count / capacity).
    pub fn load_factor(&self) -> f64;
    
    /// Resize the table when load factor exceeds threshold.
    /// Creates new mmap, rehashes all entries, unmaps old region.
    pub fn resize(&mut self, new_capacity: usize) -> Result<()>;
    
    /// Iterate over all occupied entries.
    pub fn iter(&self) -> HashTableIter<'_>;
}
```

#### Memory management:

- Primary allocation via `mmap` with `MAP_ANONYMOUS | MAP_PRIVATE`
- Attempt `MAP_HUGETLB` with 2MB pages first, fall back to regular pages
- The mmap region is a flat array of `Bucket` structs
- On drop, `munmap` the region

#### Hash function:

```rust
fn bucket_index(key: &TxKey, mask: usize) -> usize {
    // txid is already a cryptographic hash — use first 8 bytes as u64
    let h = u64::from_le_bytes(key.txid[0..8].try_into().unwrap());
    (h as usize) & mask
}

fn fingerprint(key: &TxKey) -> u64 {
    // Use bytes 8-16 for fingerprint (different from bucket index bytes)
    u64::from_le_bytes(key.txid[8..16].try_into().unwrap())
}
```

Using different bytes for the bucket index and fingerprint means collisions in bucket placement don't correlate with fingerprint collisions — reducing false positive matches.

### 2.4 Index manager — `src/index/mod.rs`

Higher-level wrapper that manages the hash table lifecycle:

```rust
pub struct Index {
    table: HashTable,
    resize_threshold: f64,        // default 0.7
}

impl Index {
    pub fn new(expected_records: usize) -> Result<Self>;
    
    /// Look up where a transaction's data lives on disk.
    pub fn lookup(&self, key: &TxKey) -> Option<TxIndexEntry>;
    
    /// Register a newly created transaction record.
    pub fn register(&mut self, key: TxKey, entry: TxIndexEntry) -> Result<()>;
    
    /// Remove a transaction (on deletion/pruning).
    pub fn unregister(&mut self, key: &TxKey) -> Option<TxIndexEntry>;
    
    /// Snapshot the entire index to a file for fast restart.
    pub fn snapshot(&self, path: &std::path::Path) -> Result<()>;
    
    /// Restore index from a snapshot file.
    pub fn restore(path: &std::path::Path) -> Result<Self>;
    
    /// Rebuild index by scanning all records on the device.
    /// This is the cold-start path — slow but correct.
    pub fn rebuild(device: &dyn BlockDevice, allocator: &SlotAllocator) -> Result<Self>;
    
    /// Statistics for monitoring.
    pub fn stats(&self) -> IndexStats;
}

pub struct IndexStats {
    pub entry_count: usize,
    pub capacity: usize,
    pub load_factor: f64,
    pub hugepage_enabled: bool,
    pub memory_bytes: usize,
}
```

### 2.5 Snapshot format

The snapshot is a simple binary file:

```
[Header: magic(4) + version(4) + entry_count(8) + capacity(8)]
[Entry 0: TxKey(32) + TxIndexEntry]
[Entry 1: TxKey(32) + TxIndexEntry]
...
[Checksum: CRC32 of all preceding bytes]
```

The snapshot is written atomically: write to a temp file, fsync, rename over the snapshot file. On restore, verify the checksum before using.

### 2.6 Index rebuild from device scan

For cold starts when no snapshot exists, scan the device:

1. Walk the allocator's occupied regions
2. For each region, read the TxMetadata header
3. Verify the magic number
4. Extract the txid and record parameters
5. Insert into the index

This is inherently slow (reads every record header) but only happens on first boot or after snapshot corruption.

## Acceptance criteria

### Hash table correctness tests

```
- [ ] Insert one entry, get by key: returns correct entry
- [ ] Insert 100 entries, get each: all found with correct values
- [ ] Get non-existent key: returns None
- [ ] Insert same key twice: second insert returns previous entry, new value stored
- [ ] Remove existing key: returns removed entry, subsequent get returns None
- [ ] Remove non-existent key: returns None
- [ ] Insert then remove then insert same key: latest value stored
- [ ] Verify fingerprint is used: two keys with same bucket index but different 
      fingerprints are stored separately and both retrievable
```

### Capacity and resize tests

```
- [ ] Create table with capacity 16, insert 16 entries: all found (100% load)
- [ ] Insert until load factor exceeds 0.7: auto-resize triggers
- [ ] After resize: all previously inserted entries still found
- [ ] After resize: capacity is doubled
- [ ] After resize: new inserts work correctly
- [ ] Create table with capacity 1024, fill to 70%, verify all lookups succeed
- [ ] Create table with capacity 1024, fill to 90%, verify all lookups still succeed
```

### Robin Hood invariant tests

```
- [ ] Insert entries that hash to same bucket: all found despite collisions
- [ ] Insert 1000 entries with adversarial keys (all hash to bucket 0 mod capacity):
      all entries found correctly
- [ ] After many inserts with collisions, verify maximum probe distance is reasonable
      (log2(n) or better for Robin Hood at <90% load)
```

### Tombstone tests (for delete)

```
- [ ] Insert A, B, C (all colliding). Delete B. Get C: still found
- [ ] Insert A, B, C (all colliding). Delete A. Get B and C: both found
- [ ] Insert A, delete A, insert A: new value stored and retrievable
- [ ] Many insert/delete cycles: table doesn't degrade (tombstones are cleaned on resize)
```

### Memory mapping tests

```
- [ ] HashTable allocates memory via mmap (check /proc/self/maps or memory usage)
- [ ] Hugepage fallback: if MAP_HUGETLB fails, table still works with regular pages
- [ ] Drop HashTable: memory is released (check before/after RSS)
```

### Scale tests

```
- [ ] Insert 1 million entries: all 1M are retrievable
- [ ] Insert 1 million entries: memory usage is approximately 1M * bucket_size
- [ ] Insert 10 million entries: all retrievable (may be slow, that's ok)
- [ ] Lookup performance at 1M entries: measure avg nanoseconds per lookup
```

### Snapshot and restore tests

```
- [ ] Insert 1000 entries, snapshot, restore: all 1000 entries present
- [ ] Snapshot includes checksum: verified on restore
- [ ] Corrupt snapshot file (flip one byte): restore returns checksum error
- [ ] Truncated snapshot file: restore returns error (not panic)
- [ ] Empty table snapshot and restore: works correctly
- [ ] Snapshot to non-writable path: returns error (not panic)
```

### Rebuild from device tests

```
- [ ] Write 10 records to device (using Phase 1 write_full_record)
      Rebuild index from device scan. All 10 entries found in index.
- [ ] Write records, delete one (clear magic number), rebuild: only valid records indexed
- [ ] Write records with corrupted magic: rebuild skips them without panic
- [ ] Empty device: rebuild produces empty index
```

### Index manager integration tests

```
- [ ] Full lifecycle:
      1. Create index for 1000 expected records
      2. Register 500 records
      3. Lookup each: all found
      4. Unregister 100 records
      5. Lookup unregistered: None
      6. Lookup remaining 400: all found
      7. Stats show count=400
      8. Snapshot to temp file
      9. Drop index, restore from snapshot
      10. All 400 still found, 100 still absent
```

### Performance benchmarks (measured, not pass/fail)

```
- [ ] Lookup latency at 1M entries: report p50, p99 in nanoseconds
- [ ] Lookup latency at 10M entries: report p50, p99
- [ ] Insert throughput: millions of inserts per second
- [ ] Snapshot write time for 10M entries
- [ ] Restore time for 10M entries
- [ ] Rebuild time for 10K records on device
```

---

## Secondary Indexes (Spec §5.5)

The pruner needs to find records by `delete_at_height` and `unmined_since` — queries the primary txid hash index cannot serve. These are lightweight in-memory sorted structures maintained alongside the primary index. Both are small relative to the primary index (at most a few million entries each) so a `BTreeMap` is the right trade-off: simple, correct, cache-friendly iteration for range queries, and no custom implementation to maintain.

### 2.7 DAH Secondary Index — `src/index/dah_index.rs`

Maps `delete_at_height` values to the set of transactions scheduled for deletion at that height. The pruner calls `range_query(0..=current_height)` each block to find records eligible for deletion.

```rust
use std::collections::BTreeMap;
use crate::index::TxKey;

/// Secondary index mapping delete_at_height → transactions.
///
/// NOT critical for crash safety — a stale DAH index only delays pruning,
/// which is a background optimization. Rebuilt from device scan on recovery.
pub struct DahIndex {
    /// Forward map: height → set of txids scheduled for deletion at that height.
    by_height: BTreeMap<u32, Vec<TxKey>>,

    /// Reverse map: txid → current delete_at_height.
    /// Enables O(1) removal when a transaction's DAH changes.
    by_txid: HashMap<TxKey, u32>,
}

impl DahIndex {
    pub fn new() -> Self;

    /// Insert a transaction into the DAH index.
    /// If the txid already has a DAH entry, the old entry is removed first
    /// (handles the case where DAH is updated, e.g. re-org changes the deletion height).
    pub fn insert(&mut self, height: u32, key: TxKey);

    /// Remove a transaction from the DAH index.
    /// Called when:
    /// - delete_at_height is cleared (set to 0)
    /// - the record is actually deleted/pruned
    /// No-op if the key is not present.
    pub fn remove(&mut self, key: &TxKey);

    /// Return all txids with delete_at_height in [0, current_height].
    /// The pruner calls this once per block.
    /// Results are returned in ascending height order.
    pub fn range_query(&self, current_height: u32) -> Vec<TxKey>;

    /// Number of entries in the index.
    pub fn len(&self) -> usize;

    /// Drain all entries (used during rebuild).
    pub fn clear(&mut self);
}
```

**When the DAH index is updated:**

| Operation | DAH index action |
|-----------|-----------------|
| `spend` (sets `delete_at_height` on fully-spent tx) | `insert(height, key)` |
| `setDeleteAtHeight` (explicit) | `insert(height, key)` or `remove(key)` if height=0 |
| `setConflicting` (marks tx for deletion) | `insert(height, key)` |
| Record deleted/pruned | `remove(key)` |
| `unspend` (reverses a spend, clears DAH) | `remove(key)` |

**Crash safety**: Non-critical. On recovery, the DAH index is rebuilt by scanning the device and reading `delete_at_height` from each record's `TxMetadata`. This is acceptable because: (a) DAH records are a small fraction of total records, (b) delayed pruning is harmless, (c) the scan runs in the background while the node serves traffic.

### 2.8 Unmined Secondary Index — `src/index/unmined_index.rs`

Maps `unmined_since` values to the set of transactions that have been unmined since that height. The pruner uses this to find old unmined transactions whose parents should be preserved (or whose resources can be reclaimed).

```rust
use std::collections::BTreeMap;
use crate::index::TxKey;

/// Secondary index mapping unmined_since → transactions.
///
/// CRITICAL for crash safety — a stale unmined index would miss transactions
/// that need parent preservation, leading to data loss. Mutations to this
/// index are logged in the redo log and replayed on recovery.
pub struct UnminedIndex {
    /// Forward map: unmined_since height → set of txids.
    by_height: BTreeMap<u32, Vec<TxKey>>,

    /// Reverse map: txid → current unmined_since value.
    by_txid: HashMap<TxKey, u32>,
}

impl UnminedIndex {
    pub fn new() -> Self;

    /// Insert a transaction into the unmined index.
    /// If the txid already has an entry, the old entry is removed first
    /// (handles unmined_since being updated on re-org).
    ///
    /// IMPORTANT: The caller MUST write a redo log entry for this mutation
    /// before acknowledging the client. See §7 (Crash Safety).
    pub fn insert(&mut self, height: u32, key: TxKey);

    /// Remove a transaction from the unmined index.
    /// Called when:
    /// - Transaction is mined on the longest chain (unmined_since set to 0)
    /// - Record is deleted/pruned
    ///
    /// IMPORTANT: The caller MUST write a redo log entry for this mutation.
    pub fn remove(&mut self, key: &TxKey);

    /// Return all txids with unmined_since in [0, cutoff_height].
    /// Used by pruner to find old unmined transactions.
    pub fn range_query(&self, cutoff_height: u32) -> Vec<TxKey>;

    /// Number of entries in the index.
    pub fn len(&self) -> usize;

    /// Drain all entries (used during rebuild).
    pub fn clear(&mut self);
}
```

**When the unmined index is updated:**

| Operation | Unmined index action |
|-----------|---------------------|
| `create` (with `unmined_since != 0`) | `insert(unmined_since, key)` |
| `setMined` (tx mined on longest chain, `unmined_since` → 0) | `remove(key)` |
| `markOnLongestChain` / re-org (sets new `unmined_since`) | `insert(new_height, key)` (removes old entry internally) |
| Record deleted/pruned | `remove(key)` |

**Crash safety**: Critical. Each mutation generates a redo log entry:

```rust
/// Redo log entry for unmined index mutations.
/// ~36 bytes per entry (txid + old_height + new_height).
pub struct UnminedRedoEntry {
    pub txid: [u8; 32],
    pub old_height: u32,  // 0 if this is an insert (no previous value)
    pub new_height: u32,  // 0 if this is a remove
}
```

On recovery, the redo log is replayed to bring the unmined index up to date from the last checkpoint. This adds ~36 bytes per redo entry, but `unmined_since` changes are infrequent relative to spends, so the overhead is negligible.

### 2.9 Checkpoint format for secondary indexes

Both secondary indexes are checkpointed alongside the primary index snapshot. The snapshot format from §2.5 is extended:

```
[Primary Index Header + Entries + Checksum]     (existing, unchanged)

[DAH Index Section]
  magic: b"DAHI" (4 bytes)
  version: u32 (4 bytes)
  entry_count: u64 (8 bytes)
  entries: [delete_at_height: u32, txid: [u8; 32]] × entry_count   (36 bytes each)
  checksum: CRC32 (4 bytes)

[Unmined Index Section]
  magic: b"UNMI" (4 bytes)
  version: u32 (4 bytes)
  entry_count: u64 (8 bytes)
  entries: [unmined_since: u32, txid: [u8; 32]] × entry_count      (36 bytes each)
  checksum: CRC32 (4 bytes)
```

The sections are appended after the primary index data in the same snapshot file. Each section has its own checksum so that corruption in a secondary index section does not invalidate the primary index snapshot. If a secondary index section is missing or corrupt:

- **DAH index**: Rebuild from device scan (non-critical, safe to run in background).
- **Unmined index**: Replay redo log from the last valid checkpoint. If the redo log is also corrupt/missing, fall back to device scan with a warning — this is a degraded recovery path.

Update the `Index` manager (§2.4) to coordinate checkpointing:

```rust
impl Index {
    /// Snapshot primary index + both secondary indexes atomically.
    pub fn snapshot_all(
        &self,
        dah: &DahIndex,
        unmined: &UnminedIndex,
        path: &std::path::Path,
    ) -> Result<()>;

    /// Restore all indexes from a snapshot file.
    /// Returns (primary_index, dah_index, unmined_index).
    /// If secondary index sections are corrupt, returns Ok with empty
    /// secondary indexes and sets a flag indicating rebuild is needed.
    pub fn restore_all(path: &std::path::Path) -> Result<(Self, DahIndex, UnminedIndex, RestoreFlags)>;
}

pub struct RestoreFlags {
    pub dah_needs_rebuild: bool,
    pub unmined_needs_rebuild: bool,
}
```

### 2.10 Acceptance criteria for secondary indexes

#### DAH index tests

```
- [ ] Insert (height=100, key_a): range_query(100) returns [key_a]
- [ ] Insert (100, key_a), (100, key_b), (200, key_c):
      range_query(100) returns [key_a, key_b]
      range_query(200) returns [key_a, key_b, key_c]
      range_query(99) returns []
- [ ] Insert (100, key_a), remove(key_a): range_query(100) returns []
- [ ] Insert (100, key_a), insert(200, key_a): only one entry exists (at height 200).
      range_query(100) returns []. range_query(200) returns [key_a].
- [ ] Remove non-existent key: no-op, no panic
- [ ] Insert 10,000 entries across 100 heights, range_query returns correct subset
- [ ] len() reflects actual entry count after inserts and removes
- [ ] clear() empties the index completely
```

#### Unmined index tests

```
- [ ] Insert (height=500, key_a): range_query(500) returns [key_a]
- [ ] Insert (500, key_a), (500, key_b), (600, key_c):
      range_query(500) returns [key_a, key_b]
      range_query(600) returns [key_a, key_b, key_c]
      range_query(499) returns []
- [ ] Insert (500, key_a), remove(key_a): range_query(500) returns []
- [ ] Insert (500, key_a), insert(700, key_a): entry moves to height 700.
      range_query(500) returns []. range_query(700) returns [key_a].
- [ ] Remove non-existent key: no-op, no panic
- [ ] Insert 10,000 entries across 100 heights, range_query returns correct subset
- [ ] len() reflects actual entry count after inserts and removes
- [ ] clear() empties the index completely
```

#### Rebuild from device scan tests

```
- [ ] Write 20 records to device: 10 with delete_at_height != 0, 5 with unmined_since != 0.
      Rebuild DAH index from scan: exactly 10 entries.
      Rebuild unmined index from scan: exactly 5 entries.
- [ ] Write records, delete some (clear magic), rebuild: only valid records indexed
- [ ] Empty device: rebuild produces empty secondary indexes
- [ ] Rebuild DAH index, verify range_query results match expected based on record metadata
- [ ] Rebuild unmined index, verify range_query results match expected based on record metadata
```

#### Checkpoint and restore tests for secondary indexes

```
- [ ] Insert entries into both secondary indexes, snapshot_all, restore_all:
      all entries present in both restored indexes
- [ ] Corrupt DAH section in snapshot (flip byte in DAHI region):
      restore_all succeeds, primary + unmined indexes restored,
      RestoreFlags.dah_needs_rebuild = true, DAH index is empty
- [ ] Corrupt unmined section in snapshot:
      restore_all succeeds, primary + DAH restored,
      RestoreFlags.unmined_needs_rebuild = true, unmined index is empty
- [ ] Snapshot with empty secondary indexes: restore produces empty secondary indexes
      with no rebuild flags set
- [ ] Snapshot, add more entries, restore: only snapshotted entries present (no leakage)
```

#### Redo log integration tests (unmined index)

```
- [ ] Insert into unmined index → verify redo log entry written
      (redo log entry: old_height=0, new_height=500)
- [ ] Remove from unmined index → verify redo log entry written
      (redo log entry: old_height=500, new_height=0)
- [ ] Update unmined index (move key from height 500 to 700) → verify redo log entry
      (redo log entry: old_height=500, new_height=700)
- [ ] Restore from checkpoint, replay 3 redo entries, verify final state correct
- [ ] Restore from checkpoint, replay redo with duplicate entries: idempotent, no corruption
```

## NOT in this phase

- No concurrent access (locking added in Phase 3)
- No NUMA pinning (note as a future optimization in comments)
- No hugepage configuration tuning
- No network / replication
