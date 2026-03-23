# BSV UTXO Store: Purpose-Built Rust Implementation — Specification Briefing

> **Note:** This document is the design session briefing that served as input for the full specification. The complete, code-validated OpenSpec is in [BSV_UTXO_STORE_SPEC.md](./BSV_UTXO_STORE_SPEC.md). Where the two documents differ, the OpenSpec takes precedence — it reflects the actual codebase analysis and subsequent refinements (69-byte slots, PRUNED status, flags bitfield, eliminated fields, block entry overflow, secondary indexes, etc.).

## Project context

This document captures the full analysis from a multi-turn design session for building a purpose-built Rust database server as the UTXO store for BSV Teranode, replacing the original general-purpose database backend. The goal is a dramatically faster and more efficient system by exploiting the known, fixed workload patterns of the UTXO store.

The original implementation uses a forked general-purpose database server (branch `master`, module `modules/mod-teranode`) with Lua UDFs for atomic record mutations. The Go client code lives in `bsv-blockchain/teranode` — the UTXO store interface, field definitions, and Go-side expression operations are in that repo.

### Scale targets

- Sustained throughput: 3+ million operations/sec (demonstrated in production trials with the original backend)
- Latency: sub-10ms p99
- Dataset: billions of UTXO records
- Largest single transaction seen on the network: 320 MB
- Replication factor: 2 (master + replica)
- Cluster size: variable, horizontally scalable

---

## Original architecture (what we're replacing)

### Why the original system is fast (and where it isn't fast enough)

The existing system's performance comes from six interlocked design decisions:

1. **Raw block device access** — bypasses the filesystem entirely, opens NVMe devices with `O_DIRECT`, treats SSDs as a flat array of write-blocks (`wblocks`).

2. **Streaming Write Buffer (SWB)** — each device gets an 8 MiB in-memory buffer. Client writes are memcpy'd into the buffer. When full (or after `flush-max-ms`), the buffer is flushed as one large sequential write. Converts thousands of small random writes into a handful of large sequential ones.

3. **Primary index in DRAM** — 64-byte entries in a forest of per-partition red-black trees ("sprigs"). Each entry contains a 20-byte RIPEMD-160 digest + a direct pointer to the record's physical location on SSD. Reads are single-IO: hash → sprig lookup → `pread` at exact offset.

4. **Copy-on-write + log-structured append** — records are never updated in-place. A write appends to the current SWB, and the index pointer is swung to the new location. No write-ahead log needed (writes happen once, not twice).

5. **Application-level defragmentation** — when a wblock's live-record fraction drops below threshold, surviving records are repacked and the block is reclaimed.

6. **Dedicated thread pools** — service threads handle clients, `run_write` threads flush SWBs, defrag threads run independently. Write path is fully async from the service thread's perspective.

### Why the original system is suboptimal for this specific workload

The copy-on-write model is the core mismatch:

- **`spend` (the hottest operation)** flips a status and writes 36 bytes of spending data into a UTXO entry. But because the UTXO entry grows from 32 to 68 bytes, the existing system must copy-on-write the ENTIRE record (all UTXOs, all metadata, everything) to a new SWB position. For a tx with 1000 outputs, spending output #1 copies all 1000 UTXOs + metadata.

- **`setMined`** appends one integer to `blockIDs` and touches a few flags. Still rewrites the entire record including all UTXO data.

- **Lua UDF overhead** — every mutation runs through the Lua interpreter: record deserialization into Lua tables, byte-by-byte hash comparison in interpreted Lua (not native `memcmp`), Lua object allocation for every response map, full record serialization back on update.

- **Three parallel lists** — `blockIDs`, `blockHeights`, `subtreeIdxs` are maintained as three separate lists that must be kept in sync. Linear scan for removal. Fragile.

- **Multi-record pagination** — large transactions are split across multiple records (master + extras). The `spentExtraRecs` counter drifts under load (there's explicit clamping at lines 1172-1181 of the Lua code to paper over the race). The entire master/child coordination machinery adds complexity.

---

## Proposed Rust architecture

### Core principle: in-place mutation on raw device

Since we control the record format and know the layout at compile time, we can do something the previous design fundamentally cannot: true in-place mutation. The `spend` path becomes a single `pwrite` of 68 bytes at a known offset instead of a full record copy-on-write.

### Record layout

#### Per-transaction on NVMe:

```
┌─────────────────────────────────────────────┐
│ Hot metadata (~200B)                        │  ← mutated by setMined, flags, counters
│   - txID (32B)                              │
│   - version, locktime, fee (integers)       │
│   - sizeInBytes, extendedSize               │
│   - spentUtxos counter (atomic)             │
│   - recordUtxos counter                     │
│   - block entries array (fixed capacity)    │
│   - flags: conflicting, locked, creating    │
│   - unminedSince, deleteAtHeight            │
│   - preserveUntil, lastSpentState           │
│   - external_ref (if large tx)              │
│   - isCoinbase, spendingHeight              │
│   - createdAt                               │
├─────────────────────────────────────────────┤
│ UTXO slots (N × 68B, pre-allocated)        │  ← mutated in-place by spend/freeze
│   Slot 0: [hash:32B][status:1B][spending:35B]│
│   Slot 1: [hash:32B][status:1B][spending:35B]│
│   ...                                       │
│   Slot N-1: ...                             │
├─────────────────────────────────────────────┤
│ [Cold: raw inputs/outputs]                  │  ← only if < threshold, write-once
│ [Input refs: outpoints]                     │  ← write-once, for validation
└─────────────────────────────────────────────┘

External (for large txs):
┌─────────────────────────────────────────────┐
│ Full serialized transaction                 │  ← blob store, write-once, stream on read
└─────────────────────────────────────────────┘
```

#### UTXO slot structure (68 bytes, fixed size from creation):

```rust
#[repr(C, packed)]
struct UtxoSlot {
    hash: [u8; 32],          // UTXO hash, always present
    status: u8,              // 0=unspent, 1=spent, 2=frozen
    spending_data: [u8; 35], // txid(32) + vout(3), zeroed when unspent
}
```

**Critical design decision**: Always allocate at full 68-byte size from creation, even when unspent. This means the record NEVER GROWS on spend, eliminating the copy-on-write penalty entirely. The cost is 36 bytes of zeroed space per unspent UTXO — trivial compared to the I/O savings.

#### Block entry structure (replaces three parallel lists):

```rust
#[repr(C, packed)]
struct BlockEntry {
    block_id: u32,
    block_height: u32,
    subtree_idx: u32,
}
// 12 bytes per entry, pre-allocate space for 8 entries (96 bytes)
```

### Separation of concerns: hot metadata vs UTXO data

The current design puts everything in one record. The Rust version separates them:

- **Region A: UTXO array** — only touched by `spend`, `unspend`, `freeze`, `unfreeze`, `reassign`. In-place updates at known offsets.
- **Region B: Transaction metadata** — touched by `setMined`, `setConflicting`, `setLocked`, `setDeleteAtHeight`, etc. Small, separate writes.

`spend` writes to Region A (one `pwrite` at the UTXO slot offset) + atomic increment of `spentUtxos` in Region B. `setMined` writes only to Region B. Neither rewrites the other's data.

### No multi-record pagination needed

With fixed-size 68-byte slots on raw device, there's no record size limit imposed by the storage engine. A tx with 10,000 outputs = 680 KB of contiguous pre-allocated space. The entire master/child/extra-recs machinery disappears, along with the counter-drift bug.

---

## Hot path designs

### spend / spendMulti

```
1. Compute slot_offset = index.lookup(tx_key).utxo_region_offset + offset * 68
2. pread 68 bytes (the slot)
3. memcmp hash (native, not Lua byte-by-byte)
4. Check status byte (0=unspent, 1=spent, 2=frozen)
5. Validate: frozen check, coinbase maturity, deletedChildren, spendableIn
6. pwrite status=1 + spending_data in-place (68 bytes)
7. Atomic increment spentUtxos in metadata
8. Evaluate deleteAtHeight (event-driven, not recomputed every time)
```

For `spendMulti`: batch all slot reads as parallel `io_uring` SQEs → validate all in-memory → batch all writes as parallel SQEs → single counter update.

### setMined

```
1. Read metadata region (small, ~200B)
2. Find or append BlockEntry in the fixed-capacity array
3. Update unminedSince, locked, creating flags
4. pwrite only the metadata region
5. Evaluate deleteAtHeight
```

### unspend

```
1. Read slot at known offset
2. Validate hash, check status=spent and not frozen
3. pwrite status=0, zero out spending_data (68 bytes)
4. Atomic decrement spentUtxos
```

### freeze / unfreeze

```
1. Read slot
2. Validate state (unspent for freeze, frozen for unfreeze)
3. pwrite status=2 with 0xFF spending data (freeze) or status=0 with zeroed data (unfreeze)
```

### reassign

```
1. Read slot, validate frozen
2. pwrite new utxo hash + status=0 (unspent with new hash)
3. Append to reassignments list in metadata
4. Update spendableIn map
5. Increment recordUtxos
```

---

## Tiered storage for inputs/outputs

### Tier 1: Inline cold data (< ~8 KiB)

Raw inputs/outputs stored in a contiguous block adjacent to the hot record on NVMe. Written once at creation, never modified. Covers the vast majority of transactions.

### Tier 2: Separate NVMe block (8 KiB - ~1 MiB)

Cold data on same NVMe device but in separate write. Hot record committed first — tx is spendable immediately. Cold data written asynchronously.

### Tier 3: External blob store (> ~1 MiB)

Content-addressed blob store (local files, S3, MinIO). Hot record stores an `ExternalRef` with store type, content hash, size, and byte offsets for inputs/outputs within the blob. Write-once, stream on read.

For the 320 MB case: NEVER on the NVMe UTXO devices. A 320 MB write would monopolize `io_uring` for milliseconds. Goes directly to blob store, streamed on read.

### Creation pipeline for large transactions:

1. Hot record (UTXO slots + metadata) committed first — tx is spendable
2. Blob upload happens in parallel/async
3. Between commit and upload completion, tx exists but full data not yet available
4. `external_ref` populated once upload completes

### Input references (for validation without fetching full blob):

For large txs, store compact outpoint references (36 bytes per input: 32-byte txid + 4-byte vout) on NVMe. Validator can check "are all inputs valid?" without fetching the multi-MB blob.

---

## io_uring batching architecture

Design the entire write path around submission batching, not individual pwrite calls.

- Incoming mutations push descriptors into per-device lock-free ring buffers
- Dedicated submission thread drains ring every N microseconds (or at batch size threshold)
- Single `io_uring_enter` call with dozens/hundreds of SQEs
- NVMe controller parallelizes queued operations internally
- Completions harvested in bulk, client futures resolved

At 375K ops/device/sec with 50µs batching window ≈ 19 ops per submission batch.

For `spendMulti`: coalesce reads and writes for the same tx into vectored I/O.

---

## Index design

### Requirements

Pure point lookups by outpoint hash (txid, or txid+vout depending on key scheme). No range queries, no ordered traversal. Must support billions of entries.

### Recommendation: fixed-size open-addressing hash table

- Cuckoo or Robin Hood hash table
- Outpoint is already a strong hash — use first 8 bytes as bucket index directly
- ~16 bytes per entry (8-byte slot pointer + 8-byte fingerprint for collision detection)
- Billions of entries = ~16 GB, fits in DRAM

### Performance critical details:

- Map with 2 MB or 1 GB hugepages (`MAP_HUGETLB`) — avoids TLB misses on random lookups (20-30% improvement)
- NUMA-aware placement — pin index partition to same NUMA node as the NVMe controller's PCIe slot
- Cross-NUMA memory access adds ~100ns per probe — at 3M lookups/sec that's 300ms/sec of waste

---

## Concurrency: per-transaction locks (not Lua VM)

The Lua UDF provided atomicity via the original system's per-record write lock. Replace with:

- Sharded lock table: array of `parking_lot::Mutex<()>`, ~65536 stripes
- Hash tx key to lock stripe, acquire, do read-validate-write, release
- Critical section is microseconds (memcmp + pwrite), not milliseconds (Lua interpretation)
- For `spendMulti` to same tx: one lock held for the whole batch

---

## Crash safety without a WAL

Both `spend` and `setMined` are naturally idempotent:
- Spending an already-spent UTXO is a detectable no-op
- Writing same txid to mined array at same position is harmless

Approach:
- Small circular **redo log** per device (~64 MB)
- Before each batch: append operations (not data) to redo log
- On crash recovery: replay from last checkpoint — idempotent replay is safe
- Redo log entries are ~40 bytes per operation, flushed via `io_uring` alongside data writes
- The redo log doubles as the replication stream

---

## Replication: operation-based, not record-based

### Operation types:

```rust
enum ReplicaOp {
    CreateTx { key: TxKey, metadata: TxMetadata, utxo_count: u32, cold_data: Option<Vec<u8>> },
    Spend { key: TxKey, offset: u32, spending_data: [u8; 36] },
    Unspend { key: TxKey, offset: u32 },
    SetMined { key: TxKey, entry: BlockEntry, on_longest_chain: bool },
    UnsetMined { key: TxKey, block_id: u32 },
    Freeze { key: TxKey, offset: u32 },
    Unfreeze { key: TxKey, offset: u32 },
    Reassign { key: TxKey, offset: u32, new_hash: [u8; 32], block_height: u32, spendable_after: u32 },
    SetConflicting { key: TxKey, value: bool, block_height: u32, retention: u32 },
    SetLocked { key: TxKey, value: bool },
    SetDeleteAtHeight { key: TxKey, height: Option<u32> },
    PreserveUntil { key: TxKey, height: u32 },
    IncrementSpentExtraRecs { key: TxKey, inc: i32, block_height: u32, retention: u32 },
}
```

### Advantages over full-record replication:

- `spend` replication = ~40 bytes vs hundreds of bytes (full record)
- All operations are idempotent — simplifies recovery (re-send and replay)
- Natural audit trail for debugging / reorg handling
- At 3M TPS with RF=2: ~120 MB/s fabric traffic vs ~600+ MB/s

### Synchronous replication path:

1. Client → master node (partition map lookup, single hop)
2. Master applies mutation locally
3. Master sends `ReplicaOp` to replica node(s)
4. Replica applies operation (in-place write on its device)
5. Replica ACKs
6. Master returns success to client

---

## Cluster design

### Partitioning: static hash-based sharding

- Shard by first N bits of outpoint hash (e.g., 256 shards with 8 bits)
- Assignment is purely static: `target_node = shard_table[key_hash[0]]`
- No partition map gossip needed in steady state
- On node join: split some shards to new node
- On node leave: absorb shards by designated successor
- Migration unit = contiguous keyspace range = sequential device read + network send

### Cluster membership

- Heartbeat protocol (TCP mesh)
- For AP mode: simple membership agreement + deterministic partition map (without Paxos complexity)
- For SC mode (if needed later): consider Raft via `openraft` crate, but note RF=3 requirement

---

## deleteAtHeight: event-driven, not poll-computed

Current Lua code recomputes deletion eligibility on EVERY spend and setMined by reading 8+ fields. Make it event-driven:

- `spentUtxos` transitions to equal `recordUtxos` → evaluate all-spent condition once
- `blockIDs` transitions empty → non-empty → evaluate mined condition once
- `unminedSince` transitions → re-evaluate once

Store derived deletion state as a pre-maintained flag, not a recomputed function.

---

## BSV-specific optimization: transaction output co-location

UTXOs from the same transaction share a txid. When that tx is later spent, many outputs may be spent in the same block. Exploit this:

- At creation: allocate N contiguous slots for N outputs
- Index maps `(txid, vout)` to base_slot + offset
- "Spend all outputs of tx X" becomes one `pwritev` covering contiguous range
- A general-purpose database can't do this because it doesn't understand the data model

---

## Expected performance improvements

| Metric | Current (Legacy) | Expected (Rust) | Reason |
|--------|-------------------|-----------------|--------|
| Write throughput/node | Baseline | 3-5x higher | Eliminate copy-on-write + defrag |
| Write latency p50 | Baseline | Similar | Both dominated by single pwrite |
| Write latency p99.9 | Baseline | 2-5x better | No SWB flush contention, no defrag spikes |
| SSD wear/lifetime | Baseline | 10-50x better | Write 68B instead of full record per spend |
| Memory per record | 64B/record | ~16B/record | Compact hash index vs red-black tree |
| Replication bandwidth | ~600+ MB/s | ~120 MB/s | Operation-based vs full-record replication |
| Multi-record pagination | Complex, racy | Eliminated | No record size limit on raw device |

---

## Testing strategy

### Deterministic simulation testing (FoundationDB-style)

Build a deterministic event-loop simulator that injects:
- Power loss at any point in write path (mid-redo, between redo and data, mid-data, after data but before ACK)
- Network partitions between master and replica (loss, reordering, duplication)
- NVMe I/O errors (EIO on specific blocks, latency spikes)

Run continuously in CI.

### Workload-specific benchmarks

- `spend` throughput: single UTXO spends/sec per device
- `spendMulti` throughput: batch spends with varying batch sizes
- `setMined` throughput: mining state updates/sec
- Mixed workload: realistic ratio of spend:setMined:create:read
- Large transaction handling: creation and reads for 1MB, 10MB, 100MB, 320MB transactions
- Cluster rebalancing: time to rebalance after node add/remove
- Crash recovery: time to replay redo log and become available

---

## Files to analyze in Teranode repo

The following files in `bsv-blockchain/teranode` are critical for understanding the workload:

### UTXO store interface and field definitions
- `internal/utxostore/` — the Go interface that calls into the UTXO store backend
- `internal/utxostore/fields/` — field name constants and bin definitions
- `internal/utxostore/legacy/` — legacy database-specific client implementation
- Look for: `create.go`, `spend.go`, `spend_expressions.go`, `set_mined.go`, `set_mined_expressions.go`, `longest_chain.go`, `conflicting.go`

### Transaction creation flow
- How records are created, what fields are set at creation time
- The `createBatch` path for multi-record transactions
- The `createLock` mechanism and the `creating` flag

### Expression-based operations (alternative to Lua)
- `spend_expressions.go` — database expressions for spend (parallel path to Lua)
- `set_mined_expressions.go` — expressions for setMined
- These show what database operations are used when Lua is not available

### Configuration and tuning
- `settings.conf` — database connection strings, policies, timeouts, batch sizes
- Look for: `utxoBatchSize`, `maxMinedRoutines`, connection pool sizing

### Block validation and mining
- How blocks are validated against the UTXO store
- The `setMined` call pattern during block processing
- The longest chain management (`longest_chain.go`)

### Pruning and deletion
- The `pruner/` package — how records are deleted
- The `deleteAtHeight` lifecycle
- The `deletedChildren` tracking

---

## Current Lua UDF operations (from mod-teranode/teranode.lua)

| Function | Hot path? | What it does | Key optimization opportunity |
|----------|-----------|-------------|------------------------------|
| `spend` | **YES** | Sets spending data on a UTXO | In-place 68B write at known offset |
| `spendMulti` | **YES** | Batch spend multiple UTXOs | Parallel io_uring reads + writes |
| `setMined` | **YES** | Appends block entry, updates flags | Write only to metadata region |
| `unspend` | Medium | Clears spending data | In-place 68B write (zero spending data) |
| `freeze` | Low | Sets UTXO as frozen (0xFF spending data) | In-place status byte write |
| `unfreeze` | Low | Clears frozen state | In-place status byte write |
| `reassign` | Low | Replaces UTXO hash (for token reassignment) | In-place slot write |
| `setConflicting` | Low | Sets conflicting flag | Metadata-only write |
| `setLocked` | Low | Sets locked flag | Metadata-only write |
| `preserveUntil` | Low | Sets preservation height | Metadata-only write |
| `incrementSpentExtraRecs` | Medium | Updates master record when child fully spent | ELIMINATED (no multi-record) |
| `setDeleteAtHeight` | Internal | Evaluates deletion eligibility | Event-driven, not poll-computed |
