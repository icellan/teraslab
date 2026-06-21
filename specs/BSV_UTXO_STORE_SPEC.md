# BSV UTXO Store — Purpose-Built Rust Implementation: Full OpenSpec

**Version:** 1.0
**Date:** 2026-03-18
**Status:** Draft
**Source:** Generated from [SPEC_BRIEFING.md](./SPEC_BRIEFING.md) (design session notes) validated against the `bsv-blockchain/teranode` Go codebase, the legacy database server mod-teranode module, and `teranode.lua` Lua UDF
**Companion:** [BSV_UTXO_STORE_RUST_CRATES.md](./BSV_UTXO_STORE_RUST_CRATES.md) — recommended Rust crates per subsystem

---

## Table of Contents

1. [Introduction & Goals](#1-introduction--goals)
2. [Data Model](#2-data-model)
3. [API / Operations](#3-api--operations)
4. [Storage Engine](#4-storage-engine)
5. [Index](#5-index)
6. [Concurrency](#6-concurrency)
7. [Crash Safety](#7-crash-safety)
8. [Replication](#8-replication)
9. [Cluster Management](#9-cluster-management)
10. [Wire Protocol](#10-wire-protocol)
11. [Observability](#11-observability)
12. [Testing Strategy](#12-testing-strategy)
13. [Go Client](#13-go-client)
14. [Admin CLI](#14-admin-cli)
15. [Admin Web UI](#15-admin-web-ui)

---

## 1. Introduction & Goals

### 1.1 Project Purpose

Build a purpose-built Rust database server as the UTXO store for BSV Teranode, replacing the original general-purpose database backend. The system exploits the known, fixed workload patterns of UTXO management to achieve dramatically higher throughput and efficiency than a general-purpose copy-on-write architecture.

The original implementation uses a forked general-purpose database server (branch `master`, module `modules/mod-teranode`) with Lua UDFs (and an equivalent native C port in `mod-teranode`) for atomic record mutations. The Go client code resides in `bsv-blockchain/teranode` — specifically the `stores/utxo/` package and its legacy sub-package.

### 1.2 Performance Targets

| Metric | Current (Legacy) | Target (Rust) |
|--------|---------------------|---------------|
| Sustained throughput | 3+ million ops/sec (current) | 10+ million ops/sec |
| Latency p50 | Baseline | ≤ Baseline |
| Latency p99 | Baseline | Sub-10ms |
| Latency p99.9 | Baseline | 2-5x better |
| SSD wear per spend | Full record rewrite | 69 bytes (10-50x reduction) |
| Memory per index entry | 64 bytes (red-black tree) | 64-byte hash-table bucket (one cache line), as implemented in `src/index/hashtable.rs` — the original ~16-byte estimate predates the current bucket layout |
| Replication bandwidth (RF=2 at 10M TPS) | N/A (legacy backend can't sustain 10M) | ~400 MB/s |
| Dataset scale | Billions of UTXO records | 10-100 billion UTXO records |
| Largest single transaction | 320 MB | 320 MB (blob store) |
| Replication factor | 2 (master + replica) | Configurable (2-3+, default 2) |

### 1.3 Non-Goals

- **General-purpose database**: This system serves exactly one workload — UTXO management. No SQL, no secondary indexes, no ad-hoc queries.
- **Multi-tenancy**: Single namespace, single dataset per cluster.
- **ACID transactions spanning multiple records**: Atomicity is per-record only (matching the existing system's semantics).
- **Legacy wire protocol compatibility**: A new purpose-built binary protocol is designed for maximum performance. A new Go client will be created that speaks this protocol.
- **Strong consistency (initially)**: AP mode with operation-based replication. SC mode can be added later via Raft (note: SC requires RF≥3).

### 1.4 Why the Original Backend Is Suboptimal

1. **Copy-on-write penalty**: `spend` (the hottest operation) flips a status byte and writes 36 bytes, but the existing system rewrites the entire record (all UTXOs, all metadata) to a new SWB position. For a 1000-output transaction, spending output #1 copies all 1000 UTXOs.

2. **Lua UDF overhead**: Every mutation passes through the Lua interpreter — record deserialization into Lua tables, byte-by-byte hash comparison (not native `memcmp`), object allocation for response maps, full re-serialization on record update.

3. **Three parallel lists**: `blockIDs`, `blockHeights`, `subtreeIdxs` maintained as separate lists requiring synchronized linear scans.

4. **Multi-record pagination complexity**: Large transactions split across records with a `spentExtraRecs` counter that drifts under load (clamping at lines 1172-1181 of teranode.lua to paper over the race).

5. **Defragmentation spikes**: Log-structured storage requires background defrag, causing unpredictable p99.9 latency.

---

## 2. Data Model

### 2.1 Overview

Each BSV transaction is stored as a single contiguous record on raw NVMe. There is no external record size limit — the multi-record pagination system (`totalExtraRecs`, `spentExtraRecs`, child records) is entirely eliminated.

### 2.2 Complete Field Inventory

The following fields are derived from the Go field definitions (`stores/utxo/fields/fields.go`), Lua bin names (`teranode.lua`), and the legacy create/spend/setMined code paths:

| # | Field Name | Legacy Bin | Type | Size | Mutability | Operations |
|---|-----------|---------------|------|------|-----------|-----------|
| 1 | `txid` | `txID` | `[u8; 32]` | 32B | Write-once | Create |
| 2 | `version` | `version` | `u32` | 4B | Write-once | Create |
| 3 | `locktime` | `locktime` | `u32` | 4B | Write-once | Create |
| 4 | `fee` | `fee` | `i64` | 8B | Write-once | Create |
| 5 | `size_in_bytes` | `sizeInBytes` | `u32` | 4B | Write-once | Create |
| 6 | `extended_size` | `extendedSize` | `u32` | 4B | Write-once | Create |
| 7 | `flags` | (packed bitfield) | `u8` | 1B | Mixed | See flags table below |
| 8 | `spending_height` | `spendingHeight` | `u32` | 4B | Write-once | Create (coinbase only: blockHeight + 100) |
| 9 | `spent_utxos` | `spentUtxos` | `u32` | 4B | Mutable (atomic) | Spend (+1), Unspend (-1) |
| 10 | `pruned_utxos` | (new) | `u32` | 4B | Mutable | PruneSlot (+1) |
| 11 | `generation` | (new) | `u32` | 4B | Mutable | Every mutation (+1) |
| 12 | `updated_at` | (new) | `i64` | 8B | Mutable | Every mutation (millis timestamp) |
| 13 | `unmined_since` | `unminedSince` | `u32` | 4B | Mutable | Create, SetMined, MarkOnLongestChain |
| 14 | `delete_at_height` | `deleteAtHeight` | `u32` | 4B | Mutable | setDeleteAtHeight (event-driven) |
| 15 | `preserve_until` | `preserveUntil` | `u32` | 4B | Mutable | PreserveUntil |
| 16 | `created_at` | `createdAt` | `i64` | 8B | Write-once | Create |
| 17 | `block_entries` | blockIDs/blockHeights/subtreeIdxs | `[BlockEntry; MAX_BLOCKS]` | 12B × N | Mutable | SetMined (append/remove) |
| 18 | `block_entry_count` | (derived) | `u8` | 1B | Mutable | SetMined |

**`flags` bitfield (1 byte, replaces 6 separate bools):**

```rust
bitflags! {
    #[repr(transparent)]
    struct TxFlags: u8 {
        const IS_COINBASE       = 0b0000_0001;  // bit 0 — write-once (Create)
        const CONFLICTING       = 0b0000_0010;  // bit 1 — mutable (SetConflicting)
        const LOCKED            = 0b0000_0100;  // bit 2 — mutable (SetLocked, SetMined clears)
        const EXTERNAL          = 0b0000_1000;  // bit 3 — write-once (Create, large tx)
        const LAST_SPENT_ALL    = 0b0001_0000;  // bit 4 — mutable (setDeleteAtHeight signaling)
        // bits 5-7 reserved for future use
    }
}
```

This packs what was 5 separate fields into a single byte. The `CREATING` flag from the original design is eliminated — it only existed to block spending during multi-record 2-phase commit, which is no longer needed since records are single atomic writes. Flag mutations are atomic read-modify-write on the flags byte within the per-txid lock.
| 19 | `reassignment_count` | (new) | `u8` | 1B | Mutable | Reassign (+1) |
| 20 | `utxo_slots` | `utxos` | `[UtxoSlot; N]` | 69B × N | Mutable (in-place) | Spend, Unspend, Freeze, Unfreeze, Reassign |
| 21 | `reassignments` | `reassignments` | `Vec<Reassignment>` | Variable | Mutable (append-only) | Reassign (extension block) |
| 22 | `inputs` | `inputs` | `Vec<u8>` | Variable | Write-once | Create (inline cold data) |
| 23 | `outputs` | `outputs` | `Vec<u8>` | Variable | Write-once | Create (inline cold data) |
| 24 | `tx_inpoints` | `txInpoints` | `Vec<u8>` | Variable | Write-once | Create |
| 25 | `external_ref` | (new) | `ExternalRef` | ~73B | Write-once | Create (in metadata) |

Total logical fields: **25**.

**Fields eliminated:**
- `totalExtraRecs` — no child records (no pagination)
- `spentExtraRecs` — no child records
- `recordUtxos` — redundant; the all-spent check uses `utxo_count`
- `totalUtxos` — redundant; same as `utxo_count`. Reassign doesn't affect the all-spent check because freeze doesn't increment `spent_utxos`.
- `creating` — only existed for multi-record 2-phase commit. Single-record atomic writes make this unnecessary.
- `conflictingChildren` — tracked at application layer
- `deletedChildren` — replaced by UtxoSlot `status = 0x02 (PRUNED)`. See §2.4.
- `spendable_in` / `utxoSpendableIn` — replaced by encoding the spendable height directly in the UTXO slot's `spending_data` field. See §2.4.

### 2.3 Record Layout on NVMe

Metadata is placed **first** at a fixed compile-time size (`METADATA_SIZE`). This eliminates the need for a separate record header and removes one `pread` from every hot-path operation — metadata is always at `record_offset + 0`.

```
┌──────────────────────────────────────────────────────────────┐
│ METADATA (fixed size, compile-time constant METADATA_SIZE)   │
│   magic: u32               // 0x534C4142 ("SLAB")            │
│   schema_version: u32                                        │
│   record_size: u32         // total record size in bytes     │
│   utxo_count: u32          // number of UTXO slots           │
│   txid: [u8; 32]                                             │
│   tx_version: u32                                            │
│   locktime: u32                                              │
│   fee: i64                                                   │
│   size_in_bytes: u32                                         │
│   extended_size: u32                                         │
│   flags: u8                // packed bitfield (see §2.2)     │
│   spending_height: u32                                       │
│   spent_utxos: u32         // atomic increment/decrement     │
│   pruned_utxos: u32        // count of PRUNED slots          │
│   generation: u32          // incremented on every mutation   │
│   updated_at: i64          // millis timestamp, every mutation│
│   unmined_since: u32       // 0 = mined on longest chain     │
│   delete_at_height: u32   // 0 = not set                     │
│   preserve_until: u32     // 0 = not set                     │
│   created_at: i64                                            │
│   block_entry_count: u8                                      │
│   block_entries_inline: [BlockEntry; 3]  // 36 bytes         │
│   block_overflow_offset: u64    // 0 = none, else ext block  │
│   reassignment_offset: u64     // 0 = none, else ext block   │
│   reassignment_count: u8       // number of reassignments    │
│   external_ref: ExternalRef     // ~73 bytes                 │
│   [padding to METADATA_SIZE alignment]                       │
├──────────────────────────────────────────────────────────────┤
│ UTXO SLOTS (utxo_count × 69 bytes, pre-allocated)           │
│   Slot 0:  [hash:32B][status:1B][spending_data:36B]          │
│   Slot 1:  [hash:32B][status:1B][spending_data:36B]          │
│   ...                                                        │
│   Slot N-1: [hash:32B][status:1B][spending_data:36B]         │
├──────────────────────────────────────────────────────────────┤
│ COLD DATA (write-once, variable size)                        │
│   [inputs_len: u32][inputs: Vec<u8>]                         │
│   [outputs_len: u32][outputs: Vec<u8>]                       │
│   [inpoints_len: u32][inpoints: Vec<u8>]                     │
└──────────────────────────────────────────────────────────────┘

Separate extension blocks (allocated on demand, referenced by offset in metadata):
┌──────────────────────────────────────────────────────────────┐
│ BLOCK ENTRY OVERFLOW (~256B)                                 │
│   [BlockEntry; N]           // entries beyond the 3 inline   │
│   (allocated when block_entry_count > 3)                     │
├──────────────────────────────────────────────────────────────┤
│ REASSIGNMENT LOG (~256B+)                                    │
│   [ReassignmentEntry; N]    // audit trail                   │
│   (allocated on first reassign)                              │
└──────────────────────────────────────────────────────────────┘
```

**Why metadata-first**: The spend hot path needs metadata (for flag validation) before it can read a UTXO slot. With metadata at offset 0:
- Read 1: `pread(METADATA_SIZE bytes at record_offset)` → flags, counters, utxo_count
- Read 2: `pread(69 bytes at record_offset + METADATA_SIZE + vout * 69)` → the UTXO slot

That's **2 reads** total. The previous design (header → slots → metadata) required 3 reads because the metadata offset varied per record.

**All offsets are deterministic:**
- Metadata: `record_offset + 0` (always)
- UTXO slot N: `record_offset + METADATA_SIZE + N * 69` (METADATA_SIZE is compile-time constant)
- Cold data: `record_offset + METADATA_SIZE + utxo_count * 69`
- `setMined` only reads/writes the first `METADATA_SIZE` bytes — never touches UTXO slots

### 2.4 UTXO Slot Structure (69 bytes, fixed)

```rust
#[repr(C, packed)]
struct UtxoSlot {
    /// UTXO hash (always present, set at creation)
    hash: [u8; 32],
    /// Status: 0x00=unspent, 0x01=spent, 0x02=pruned, 0xFF=frozen
    status: u8,
    /// Multi-purpose field (36 bytes), interpretation depends on status:
    ///   Unspent:  [spendable_height:4 LE][zeros:32] — height 0 = immediately spendable
    ///   Spent:    [txid:32][vin:4 LE]
    ///   Pruned:   preserved from last spend
    ///   Frozen:   all 0xFF
    spending_data: [u8; 36],
}
// Total: 69 bytes
// Offset calculation: slot_offset = record_offset + METADATA_SIZE + (vout * 69)
```

**Status byte encoding:**

| Value | State | spending_data content | Transitions to |
|-------|-------|----------------------|----------------|
| `0x00` | Unspent | `[spendable_height:4 LE][0:32]` — height 0 = immediately spendable | → Spent, Frozen |
| `0x01` | Spent | `[txid:32][vin:4 LE]` | → Unspent (unspend), Pruned |
| `0x02` | Pruned | Preserved from last spend | Terminal (no transitions) |
| `0xFF` | Frozen | All 0xFF (36 bytes) | → Unspent (unfreeze), Reassign |

**Spendable height in spending_data** (replaces `spendable_in` / `utxoSpendableIn`): For unspent slots, the first 4 bytes of `spending_data` encode a block height restriction. A value of 0 means immediately spendable (the common case — all 36 bytes zeroed). A non-zero value means the UTXO cannot be spent until `current_block_height > spendable_height`. This is set by the `reassign` operation to enforce a cooldown period. The data lives IN the slot that's already read during spend validation — zero extra I/O for any case.

**`PRUNED` (0x02)** replaces the `deletedChildren` map from the original design. When a child transaction is pruned/deleted, the pruner sets the parent's UTXO slot status to `PRUNED`. This means:
- The spending_data is preserved (for audit/debugging) but the UTXO cannot be re-spent
- Spend validation rejects `PRUNED` slots the same way it rejects `FROZEN` — a single byte comparison instead of a map lookup + string comparison against `deletedChildren`
- No per-record variable-size map to maintain, no `MapPutOp`, no hex-encoded txid keys

**Pruner write path:** When deleting child TX C that spent parent output N:
1. Set parent's `utxo_slots[N].status = 0x02` (PRUNED) via `pwrite` of 1 byte
2. Delete child TX C's record

This is atomic per-slot (single byte write under the parent's lock) and naturally idempotent — pruning an already-pruned slot is a no-op.

**Critical design decision**: Always allocate at full 69-byte size from creation, even when unspent. This means the record **never grows** on spend, eliminating the copy-on-write penalty. The cost is 37 bytes of zeroed space per unspent UTXO (1 status + 36 spending_data) — trivial compared to the I/O savings.

### 2.5 Block Entry Structure (replaces three parallel lists)

```rust
#[repr(C, packed)]
struct BlockEntry {
    block_id: u32,
    block_height: u32,
    subtree_idx: u32,
}
// 12 bytes per entry
// 3 inline entries (36 bytes) in metadata region
```

This replaces the three synchronized legacy lists (`blockIDs`, `blockHeights`, `subtreeIdxs`) with a single array of structs, eliminating the fragile parallel-list synchronization.

**Inline capacity**: 3 entries covers 99.9%+ of cases (transactions are typically in 1-2 blocks).

**Overflow handling**: The metadata region has a fixed layout — 3 inline slots plus an overflow pointer that doubles as the 4th slot position:

```rust
// In the metadata:
block_entry_count: u8,                    // total entries (inline + overflow)
block_entries_inline: [BlockEntry; 3],    // 36 bytes
block_overflow_offset: u64,               // 0 = no overflow; else device offset to extension block
```

When `block_entry_count <= 3`: all entries are inline, `block_overflow_offset = 0`. No extra I/O.

When `block_entry_count > 3`: the first 3 entries remain inline, all entries beyond that (including the 4th) are stored in an extension block at `block_overflow_offset`. The extension block is a small device allocation (~256 bytes, room for ~21 entries) containing a contiguous array of `BlockEntry` values.

- **Read**: one extra `pread` on `setMined`/read path for the overflow block. The hot `spend` path never touches block entries.
- **Write**: `pwrite` to the extension block + update `block_overflow_offset` in metadata. No record reallocation or copy-on-write.
- **First overflow**: allocate extension block from freelist, write the 4th entry, set `block_overflow_offset`.
- **Subsequent overflows**: append to existing extension block.

### 2.6 External Reference Structure

```rust
#[repr(C, packed)]
struct ExternalRef {
    store_type: u8,           // 0=local_file, 1=S3, 2=MinIO, 3=HTTP
    content_hash: [u8; 32],   // content-addressed hash for verification
    total_size: u64,          // total blob size in bytes
    inputs_offset: u64,       // byte offset to inputs within blob
    inputs_len: u64,          // byte length of inputs
    outputs_offset: u64,      // byte offset to outputs within blob
    outputs_len: u64,         // byte length of outputs
}
// ~73 bytes
```

### 2.7 Reassignment Entry

```rust
#[repr(C, packed)]
struct ReassignmentEntry {
    offset: u32,              // UTXO slot index
    old_hash: [u8; 32],       // previous UTXO hash
    new_hash: [u8; 32],       // new UTXO hash
    block_height: u32,        // height when reassignment occurred
}
// 72 bytes per entry

```

### 2.8 Index Entry Structure

```rust
#[repr(C, packed)]
struct IndexEntry {
    fingerprint: u64,         // first 8 bytes of txid hash (collision detection)
    record_offset: u64,       // physical offset on NVMe device
}
// 16 bytes per entry
```

---

## 3. API / Operations

All operations implement the contract defined by `stores/utxo/Interface.go` (`Store` interface) with 21+ public methods. Each operation below specifies exact parameters, validation rules, error codes, atomicity, and idempotency — derived from the Lua UDF source, the native C port, and the Go client code.

**Mutation bookkeeping:** All mutation operations (spend, unspend, setMined, freeze, unfreeze, reassign, setConflicting, setLocked, preserveUntil, pruneSlot) increment `generation` by 1 and set `updated_at` to the current millisecond timestamp as part of the metadata write. This is not repeated in each operation's behavior steps below.

### 3.1 Error Codes

Carried over from Lua/C implementation:

| Error Code | Constant | Description |
|-----------|----------|-------------|
| `TX_NOT_FOUND` | Record does not exist |
| `CONFLICTING` | Transaction marked as conflicting |
| `LOCKED` | Transaction is locked |
| `FROZEN` | UTXO is frozen |
| `ALREADY_FROZEN` | UTXO already frozen |
| `FROZEN_UNTIL` | UTXO not yet spendable (reassignment cooldown) |
| `COINBASE_IMMATURE` | Coinbase maturity not reached |
| `SPENT` | Already spent by different transaction |
| `INVALID_SPEND` | Spending data targets a deleted child |
| `UTXO_NOT_FOUND` | UTXO at given offset does not exist |
| `UTXO_HASH_MISMATCH` | Expected hash does not match stored hash |
| `UTXO_NOT_FROZEN` | UTXO is not in frozen state |
| `INVALID_PARAMETER` | Invalid function parameter |
| `STORAGE_ERROR` | Device I/O failure during operation |

### 3.2 Signal Codes

Operations return signals that drive follow-up actions:

| Signal | Meaning | Triggered By |
|--------|---------|-------------|
| `ALLSPENT` | All UTXOs in record are spent | spend, unspend, setMined |
| `NOTALLSPENT` | Not all UTXOs are spent (state change) | spend, unspend |
| `DAHSET` | deleteAtHeight was set | spend, setMined, setConflicting |
| `DAHUNSET` | deleteAtHeight was cleared | spend, setMined |
| `PRESERVE` | preserveUntil was set on external record | preserveUntil |

### 3.3 create / createBatch

**Go interface**: `Create(ctx, tx, blockHeight, opts...CreateOption) (*meta.Data, error)`

**Request parameters:**
- `tx: &Transaction` — full serialized BSV transaction
- `block_height: u32` — current block height
- `options: CreateOptions`:
  - `mined_block_infos: Vec<MinedBlockInfo>` — block IDs if already mined
  - `txid: Option<[u8; 32]>` — override txid (for pre-computed)
  - `is_coinbase: Option<bool>` — coinbase flag
  - `frozen: bool` — create all UTXOs in frozen state
  - `conflicting: bool` — create as conflicting
  - `locked: bool` — create as locked

**Behavior (from `create.go` lines 153-980):**
1. Compute `txid` from transaction (or use override)
2. Determine storage tier:
   - Inline (< ~8 KiB): full tx data stored in cold region
   - Separate NVMe block (8 KiB - ~1 MiB): cold data written async
   - External blob (> ~1 MiB): hot record first, blob uploaded async
3. Allocate contiguous record: metadata + (N × 69B UTXO slots) + cold data
4. Initialize UTXO slots: `hash` from output hash, `status=0x00`, `spending_data` zeroed
   - If `frozen`: status=`0xFF`, spending_data all `0xFF`
5. Initialize metadata:
   - `txid`, `version`, `locktime`, `fee`, `size_in_bytes`, `extended_size`
   - `spent_utxos = 0` (total count derived from header's `utxo_count`)
   - `is_coinbase`, `spending_height = blockHeight + 100` (if coinbase)
   - `conflicting`, `locked`
   - `unmined_since = block_height` (if no mined_block_infos), else `0`
   - `block_entries` from `mined_block_infos`
   - `created_at = now()`
6. Write record atomically via io_uring
7. Insert index entry: `txid → record_offset`
8. Return metadata

**For large transactions (external storage):**
1. Write hot record (UTXO slots + metadata) first — tx is immediately spendable
2. Upload blob to external store in parallel/async
3. Populate `external_ref` once upload completes
4. Between commit and upload, `external = true` but full data not yet available

**Batch creation (`createBatch`):**
- Accumulate via batcher (default: 100 items, 100ms window — from `StoreBatcherSize` / `StoreBatcherDurationMillis`)
- Submit as batch io_uring operation
- Index insertions batched correspondingly

**Atomicity**: Record is fully written before index entry is inserted.
**Idempotency**: Creating an existing txid is an error (index collision).

**Disk regions written**: Entire record (metadata + UTXO slots + cold data).

### 3.4 spend / spendMulti

**Go interface**: `Spend(ctx, tx, blockHeight, ignoreFlags...IgnoreFlags) ([]*Spend, error)`

**Request parameters (per spend item):**
- `txid: [u8; 32]` — parent transaction ID
- `vout: u32` — output index
- `utxo_hash: [u8; 32]` — expected UTXO hash (32 bytes)
- `spending_data: [u8; 36]` — spending txid (32 bytes) + vin (4 bytes, little-endian)
- `ignore_conflicting: bool`
- `ignore_locked: bool`
- `current_block_height: u32`
- `block_height_retention: u32`

**Validation rules (from `teranode.lua` lines 284-466):**

1. Record must exist → `TX_NOT_FOUND`
2. `conflicting == true` AND NOT `ignore_conflicting` → `CONFLICTING`
3. `locked == true` AND NOT `ignore_locked` → `LOCKED`
4. Coinbase maturity: `spending_height > 0` AND `spending_height > current_block_height` → `COINBASE_IMMATURE`
5. Per-UTXO validation:
   - Slot at offset must exist → `UTXO_NOT_FOUND`
   - Hash must match (native `memcmp`, not Lua byte-by-byte) → `UTXO_HASH_MISMATCH`
   - If status == `0x00` and `u32_from_le(spending_data[0..4]) != 0` and `u32_from_le(spending_data[0..4]) > current_block_height` → `FROZEN_UNTIL` (the UTXO is spendable exactly AT the unlock height; the comparator is `>`, **not** `>=`, matching `teranode.lua:373` and Teranode PR #949. The implementation in `engine.rs` uses `>` deliberately; an earlier draft of this rule said `>=`, which false-rejected at the exact unlock height — do not "fix" the code back to `>=`.) This cooldown check is scoped to unspent (`0x00`) slots only.
   - If status == `PRUNED` (0x02) → `INVALID_SPEND` (child tx was pruned, UTXO is permanently consumed)
   - If already spent (status == 0x01):
     - Same spending data → idempotent success
     - Frozen (all 0xFF) → `FROZEN`
     - Different spender → `SPENT` (returns existing spending data hex)

**Behavior:**
1. Acquire per-txid lock
2. Read metadata at `record_offset + 0` (METADATA_SIZE bytes) → flags, counters, utxo_count
3. Validate record-level preconditions (flags, coinbase maturity)
4. Read UTXO slot(s) via io_uring `pread` at known offset(s)
5. Validate each spend item (per-UTXO rules above)
6. For each valid spend:
   - `pwrite` 69 bytes at slot offset: hash unchanged, status=`0x01`, spending_data filled
7. Update metadata: increment `spent_utxos`
8. Evaluate `setDeleteAtHeight` (event-driven)
9. `pwrite` metadata
10. Release lock

**For `spendMulti`**: Batch all slot reads as parallel io_uring SQEs → validate all in-memory → batch all writes as parallel SQEs → single counter update.

**Response (wire contract):**
- `status: OK | PARTIAL_ERROR | ERROR`
- `errors: Map<idx, {error_code, message, spending_data?}>` — per-item errors (sparse, sorted by index)

> **Decision (LP-5 — signals and `childCount` / `block_ids` are server-internal, NOT on the wire).**
> The legacy Lua `spend`/`spendMulti`/`setMined`/`setConflicting`/`preserveUntil` responses carry
> `signal` (ALLSPENT/NOTALLSPENT/DAHSET/DAHUNSET/PRESERVE), `childCount`, and `block_ids` so the
> *Aerospike Go client* can drive follow-up actions (set/clear DAH on the external `.tx` blob, fan
> out to pagination child records, evict the tx-meta cache). In TeraSlab **every one of those
> consumers is internalized**: pagination records do not exist (so `childCount` is meaningless — see
> §2.2), the blob store is server-side with its own GC, and parent-prune / DAH transitions are
> performed inside the engine during the same op. The engine still *computes* the signal internally
> to drive that work, but the dispatcher does **not** serialize `signal`, `childCount`, or `block_ids`
> into mutation responses. Mutation responses are therefore status + per-item errors only. This is the
> authoritative contract; the earlier `block_ids` / `signal` response fields above are retained for
> historical reference only and are NOT emitted. (If a future client genuinely needs post-op block
> IDs, use a follow-up `GetBatch` with the block-entry field mask rather than reviving the spend/
> setMined response fields.)

**Atomicity**: All spends to the same txid are atomic (single lock held).
**Idempotency**: Spending with identical spending_data is a no-op (detected by memcmp).

**Disk regions read**: UTXO slots (UTXO slots region), metadata flags (metadata).
**Disk regions written**: UTXO slots (UTXO slots region), spent_utxos counter (metadata), delete_at_height (metadata).

### 3.5 unspend

**Go interface**: `Unspend(ctx, spends, flagAsLocked...bool) error`

**Request parameters:**
- `txid: [u8; 32]`
- `vout: u32` (offset into UTXO slots)
- `utxo_hash: [u8; 32]`
- `spending_data: [u8; 36]` (`expectedSpendingData` — the spend the caller claims to own)
- `current_block_height: u32`
- `block_height_retention: u32`

**Validation rules (from `teranode.lua` lines 484-555):**
1. Record must exist → `TX_NOT_FOUND`
2. Hash must match → `UTXO_HASH_MISMATCH`
3. If status == `0x02` (pruned) → `INVALID_SPEND` (terminal state, cannot unspend; postdates the Lua, which had no pruned state)
4. Ownership check (mirrors the Lua `callerOwnsSpend`): the caller owns the spend iff the slot is spent (status == `0x01`) AND the stored `spending_data` byte-equals the request `spending_data`. The frozen marker (all-`0xFF`) never equals a real caller's expected data, so a frozen slot is never owned.

**Behavior:**
1. Acquire per-txid lock
2. Read slot at offset; validate (rules 1-3 above)
3. If the caller owns the spend: `pwrite` 69 bytes (hash unchanged, status=`0x00`, spending_data zeroed) and atomically decrement `spent_utxos`. If that owned slot were frozen → `FROZEN` (structurally unreachable, since the frozen marker excludes ownership; preserved to mirror the Lua).
4. If the caller does NOT own the spend (slot already unspent, stored spend belongs to a different tx, or slot frozen): **silent no-op returning `STATUS_OK`** — no slot or counter mutation, generation not bumped.
5. On both paths, evaluate `setDeleteAtHeight` housekeeping (Lua runs it before every OK return)
6. Release lock

**Atomicity**: Per-record.
**Idempotency**: This is an *ownership check with idempotent semantics* — "never wipe a spend we don't own", not "error on every no-op". Unspending an already-unspent UTXO, a UTXO spent by a different transaction, or a frozen UTXO is a no-op success, not an error. This is load-bearing: `ProcessConflicting` builds its unspend set from every input of every losing tx — including parents whose stored spend is nil or belongs to the conflict winner — and the Go caller aborts the whole loop on any non-OK status other than `TX_NOT_FOUND`.

**Disk regions written**: UTXO slot (UTXO slots region), spent_utxos (metadata).

### 3.6 setMined

**Go interface**: `SetMinedMulti(ctx, hashes, minedBlockInfo) (map[Hash][]uint32, error)`

**Request parameters:**
- `txid: [u8; 32]`
- `block_id: u32`
- `block_height: u32`
- `subtree_idx: u32`
- `on_longest_chain: bool`
- `unset_mined: bool`
- `current_block_height: u32`
- `block_height_retention: u32`

**Validation (from `teranode.lua` lines 543-656):**
1. Record must exist → `TX_NOT_FOUND`

**Behavior (setMined):**
1. Acquire per-txid lock
2. Read metadata region
3. If `unset_mined`:
   - Linear scan `block_entries` for matching `block_id`
   - Remove entry (shift remaining entries)
   - Decrement `block_entry_count`
4. Else (set mined):
   - Check if `block_id` already exists (idempotent)
   - Append `BlockEntry {block_id, block_height, subtree_idx}`
   - Increment `block_entry_count`
5. Update `unmined_since`:
   - If `block_entry_count > 0` AND `on_longest_chain`: set to `0` (not unmined)
   - If `block_entry_count == 0`: set to `current_block_height`
6. Clear `locked` flag if set
7. Evaluate `setDeleteAtHeight`
9. `pwrite` metadata region only
10. Release lock

**Batch pattern**: Up to `MaxMinedBatchSize` (default 1024) transactions per batch, with `MaxMinedRoutines` (default 128) concurrent workers.

**Response**: status + per-item errors only. Per the LP-5 decision (§3.4), the per-txid block-ID list is **not** serialized — the engine computes it internally (to clear the LOCKED flag and evaluate DAH) but the dispatcher does not put it on the wire. A client that needs the post-setMined block IDs issues a follow-up `GetBatch` with the block-entry field mask.

**Atomicity**: Per-record.
**Idempotency**: Setting mined with same block_id is a no-op (detected by scan).

**Disk regions written**: Metadata only (metadata). UTXO slots are **not touched**.

### 3.7 freeze

**Request parameters:**
- `txid: [u8; 32]`
- `vout: u32` (offset)
- `utxo_hash: [u8; 32]`

**Validation (from `teranode.lua` lines 666-738):**
1. Record exists → `TX_NOT_FOUND`
2. Hash matches → `UTXO_HASH_MISMATCH`
3. Not already frozen (status == 0xFF) → `ALREADY_FROZEN`
4. Not already spent (status == 0x01) → `SPENT` (returns spending data)
5. Must be unspent (status == 0x00)

**Behavior:**
1. Acquire per-txid lock
2. Read slot
3. Validate
4. `pwrite` 69 bytes: hash unchanged, status=`0xFF`, spending_data all `0xFF`
5. Release lock

**Atomicity**: Per-record.
**Idempotency**: Freezing an already-frozen UTXO returns `ALREADY_FROZEN` error.

**Disk regions written**: UTXO slot only (UTXO slots region).

### 3.8 unfreeze

**Request parameters:**
- `txid: [u8; 32]`
- `vout: u32` (offset)
- `utxo_hash: [u8; 32]`

**Validation (from `teranode.lua` lines 748-811):**
1. Record exists → `TX_NOT_FOUND`
2. Hash matches → `UTXO_HASH_MISMATCH`
3. Must be frozen (status == 0xFF) → `UTXO_NOT_FROZEN`

**Behavior:**
1. Acquire per-txid lock
2. Read slot
3. Validate
4. `pwrite` 69 bytes: hash unchanged, status=`0x00`, spending_data zeroed
5. Release lock

**Atomicity**: Per-record.
**Idempotency**: Unfreezing a non-frozen UTXO returns `UTXO_NOT_FROZEN`.

**Disk regions written**: UTXO slot only (UTXO slots region).

### 3.9 reassign

**Request parameters:**
- `txid: [u8; 32]`
- `vout: u32` (offset)
- `utxo_hash: [u8; 32]` — current (frozen) hash
- `new_utxo_hash: [u8; 32]` — replacement hash
- `block_height: u32`
- `spendable_after: u32` (default: 1000 blocks)

**Validation (from `teranode.lua` lines 823-911):**
1. Record exists → `TX_NOT_FOUND`
2. Hash matches → `UTXO_HASH_MISMATCH`
3. Must be frozen (status == 0xFF) → `UTXO_NOT_FROZEN`

**Behavior:**
1. Acquire per-txid lock
2. Read slot
3. Validate frozen state
4. `pwrite` UTXO slot: `new_utxo_hash`, status=`0x00`, `spending_data[0..4] = (block_height + spendable_after) as u32 LE`, rest zeroed
   — The spendable height is encoded directly in the slot's spending_data field (see §2.4)
5. Append to reassignment extension block (audit trail):
   - If `reassignment_offset == 0`: allocate extension block from freelist, set `reassignment_offset` in metadata
   - Append `ReassignmentEntry` to the extension block
6. `pwrite` metadata (to update `reassignment_offset` if first reassign)
7. Release lock

**Note on all-spent check (LP-3 — reference parity):** A reassigned record is **never** DAH-eligible, matching the Aerospike Lua `reassign` which inflates `recordUtxos` by 1 (`teranode.lua:945`) so `spent_utxos == record_utxos` can never become true. TeraSlab cannot fabricate a phantom slot, so reassign instead sets a persisted `REASSIGNED` flag (`TxFlags` bit 6); `evaluate_delete_at_height` / `evaluate_dah_cached` / the DAH-sweep predicate all force the all-spent check false when it is set, and the CONFLICTING DAH branch is unaffected (the Lua `+1` only touches the all-spent computation, so a reassigned record later marked conflicting is still DAH'd). Rationale: a reassignment is a court-ordered / alert-system action whose old-hash → new-hash audit trail and reorg evidence the reference deliberately keeps on the store permanently. A **live** reassigned UTXO was already safe from deletion (freeze does not increment `spent_utxos`, so the all-spent check is false until the reassigned slot is itself spent); the flag additionally covers the after-final-spend window. **Limitation:** the `REASSIGNED` flag (like `reassignment_count`) is not carried by the migration `Create`-op metadata path (`create_metadata_flag_bytes`), so a record migrated between cluster nodes loses the reassignment retention guarantee — same pre-existing gap as the reassignment audit trail itself.

**Note on spendable height (LP-4 — cooldown survives freeze/unfreeze):** The reassign cooldown lives in the unspent slot's `spending_data[0..4]` (§2.4). The Aerospike reference keeps it in a separate `utxoSpendableIn` bin that freeze/unfreeze never touch; TeraSlab stores it in the slot, so `freeze` must **preserve** the 4-byte cooldown across the all-`0xFF` frozen marker and `unfreeze` must **restore** it, rather than wiping it (pre-fix a freeze→unfreeze round-trip on a reassigned output made it immediately spendable, bypassing the safety window). A frozen reassigned slot therefore carries the cooldown in `spending_data[0..4]` (the `UTXO_FROZEN` status byte remains the authoritative frozen signal; the legacy all-`0xFF` data pattern is one representation, not a requirement). The restriction is otherwise cleared naturally when the UTXO is spent (spending_data overwritten with txid+vin) or unspent during a reorg (spending_data zeroed → spendable_height 0 = immediately spendable).

**Atomicity**: Per-record.
**Idempotency**: Not naturally idempotent — reassigning an already-unspent UTXO with different hash would fail hash check.

**Disk regions written**: UTXO slot, metadata, reassignment extension block (if first reassign).

### 3.10 setConflicting

**Request parameters:**
- `txid: [u8; 32]`
- `value: bool` — true to mark conflicting, false to clear
- `current_block_height: u32`
- `block_height_retention: u32`

**Validation:**
1. Record exists → `TX_NOT_FOUND`

**Behavior (from `teranode.lua` lines 1025-1051):**
1. Acquire per-txid lock
2. Set `conflicting = value`
3. Evaluate `setDeleteAtHeight`:
   - If conflicting and no existing DAH: set `delete_at_height = current_block_height + retention`
4. `pwrite` metadata
5. Release lock

**Atomicity**: Per-record.
**Idempotency**: Setting same value is a no-op (writes same byte).

**Response:** status + per-item errors only.

> The legacy `signal` (DAHSET) is server-internal per the LP-5 decision (§3.4) and not serialized.
> An earlier draft of this section required the response to carry per-txid "UTXO slot spending data
> (needed by Go client for counter-conflicting cascade)". That was a **spec error** (KO-4, retracted):
> the real `teranode.lua:1066-1092` `setConflicting` returns no spending data either — the Go client
> gathers it itself via per-output `GetSpend` (`stores/utxo/conflicting.go`). TeraSlab is at parity
> with the reference; the response is status + errors only.

**Disk regions written**: Metadata only (metadata).

### 3.11 setLocked

**Request parameters:**
- `txid: [u8; 32]`
- `value: bool`

**Behavior (from `teranode.lua` lines 1109-1135):**
1. Acquire per-txid lock
2. Set `locked = value`
3. If locking (`value = true`) and `delete_at_height != 0`: set `delete_at_height = 0`
4. `pwrite` metadata
5. Release lock

**Atomicity**: Per-record.
**Idempotency**: Setting same value is a no-op.

**Disk regions written**: Metadata only (metadata).

### 3.12 preserveUntil

**Request parameters:**
- `txid: [u8; 32]`
- `block_height: u32`

**Behavior (from `teranode.lua` lines 1067-1095):**
1. Acquire per-txid lock
2. Clear `delete_at_height` to `0`
3. Set `preserve_until = block_height`
4. `pwrite` metadata
5. Release lock
6. If `external == true`: return signal `PRESERVE`

**Atomicity**: Per-record.
**Idempotency**: Setting same value is a no-op.

**Disk regions written**: Metadata only (metadata).

### 3.13 setDeleteAtHeight (internal, event-driven)

**Not exposed as an API operation.** Called internally at the end of `spend`, `unspend`, `setMined`, and `setConflicting`.

**Logic (from `teranode.lua` lines 927-1008):**

```
if block_height_retention == 0:
    return (no signal)

if preserve_until != 0:
    return (no signal)

if conflicting:
    if delete_at_height == 0:
        set delete_at_height = current_block_height + retention
        if EXTERNAL flag set: signal DAHSET
    return

// Main all-spent evaluation:
let all_spent = (spent_utxos == utxo_count)  // utxo_count from record header
let has_blocks = (block_entry_count > 0)
let on_longest_chain = (unmined_since == 0)

if all_spent AND has_blocks AND on_longest_chain:
    let new_dah = current_block_height + retention
    if delete_at_height == 0 OR delete_at_height < new_dah:
        set delete_at_height = new_dah
        if EXTERNAL flag set: signal DAHSET
elif delete_at_height != 0:
    set delete_at_height = 0
    if EXTERNAL flag set: signal DAHUNSET
```

**State transition signaling:**
- Track `LAST_SPENT_ALL` flag in `flags` byte
- Only signal on state **transitions** (all-spent ↔ not-all-spent) to avoid redundant signals
- Signal drives pruner: when all UTXOs are spent and tx is mined on longest chain, set `delete_at_height`

### 3.14 incrementSpentExtraRecs

**ELIMINATED** in the Rust system. No multi-record pagination means no child records and no counter synchronization.

The Lua implementation (`teranode.lua` lines 1145-1199) with its clamping logic (lines 1172-1181) to paper over counter drift is entirely unnecessary. This removes a significant source of complexity and potential data inconsistency.

### 3.15 markOnLongestChain

**Go interface**: `MarkTransactionsOnLongestChain(ctx, txHashes, onLongestChain) error`

This is a **separate operation** from `setMined`. It modifies only the `unmined_since` field without touching block entries. Used during chain reorganizations to bulk-update longest-chain status for transactions.

**Request parameters:**
- `txids: Vec<[u8; 32]>` — batch of transaction IDs
- `on_longest_chain: bool`

**Validation:**
1. Record must exist → `TX_NOT_FOUND` (fatal — indicates data corruption if missing)

**Behavior:**
1. For each txid, acquire per-txid lock
2. If `on_longest_chain == true`: set `unmined_since = 0` (transaction is on longest chain)
3. If `on_longest_chain == false`: set `unmined_since = current_block_height` (transaction is not on longest chain)
4. Update unmined secondary index accordingly:
   - `on_longest_chain == true`: remove from unmined index
   - `on_longest_chain == false`: insert/update in unmined index
5. Evaluate `setDeleteAtHeight` (longest chain status affects DAH eligibility)
6. `pwrite` metadata
7. Release lock

**Batch pattern**: Up to `MaxMinedBatchSize` (default 1024) transactions per batch, with `MaxMinedRoutines` (default 128) concurrent workers.

**Atomicity**: Per-record.
**Idempotency**: Setting same value is a no-op (writes same byte).

**Disk regions written**: Metadata only.

### 3.16 getSpend

**Go interface**: `GetSpend(ctx, spend) (*SpendResponse, error)`

Point read of a single UTXO slot plus the record's locktime. Used for double-spend detection — the validator needs to know "is this output already spent? if so, by whom?"

**Request parameters:**
- `txid: [u8; 32]`
- `vout: u32`
- `utxo_hash: [u8; 32]`

**Validation:**
1. Record must exist → `TX_NOT_FOUND`
2. `vout` must be within bounds (`vout < utxo_count`) → `UTXO_NOT_FOUND`
3. Hash must match → `UTXO_HASH_MISMATCH`

**Behavior:**
1. Index lookup: `txid → record_offset`
2. Read metadata at `record_offset + 0` (for locktime and utxo_count)
3. Read UTXO slot at `record_offset + METADATA_SIZE + vout * 69`
4. Validate hash matches `utxo_hash`
5. Return status, spending_data (if spent/frozen), locktime

**Response:**
- `status: u8` — UTXO status (0x00=unspent, 0x01=spent, 0x02=pruned, 0xFF=frozen)
- `spending_data: Option<[u8; 36]>` — present when status is 0x01 (spent) or 0xFF (frozen)
- `locktime: u32` — from record metadata

**Disk regions read**: Metadata + single UTXO slot (2 reads, or 1 if metadata is cached).

### 3.17 Point Read / Batch Read

**Go interface**: `Get(ctx, hash, fields...) (*meta.Data, error)`

**Request parameters:**
- `txid: [u8; 32]`
- `fields: Vec<FieldName>` — optional field selection (empty = all fields)

**Standard field sets:**
- `MetaFields`: locktime, fee, sizeInBytes, txInpoints, blockIDs, isCoinbase, conflicting, locked
- `MetaFieldsWithTx`: MetaFields + full transaction data

**Behavior:**
1. Index lookup: `txid → record_offset`
2. If field selection specifies only metadata: `pread` metadata region only
3. If field selection includes UTXO data or tx data: read appropriate regions
4. For external transactions: fetch from blob store (with 10-second LRU cache, semaphore-limited concurrency)

**Batch reads** (via `BatchDecorate`):
- Accumulate via getBatcher (default: 1 item / 10ms — effectively disabled)
- In high-throughput mode: configurable up to 4096 items
- Submit as batch io_uring readv operations

**Disk regions read**: Varies by field selection — typically metadata only, or metadata + cold data.

### 3.18 Delete / Prune

**Go interface**: `Delete(ctx, hash) error`

**Behavior:**
1. Index lookup: `txid → record_offset`
2. Remove index entry
3. Add record space to freelist (physical space reclaimed)
4. If external: schedule blob deletion

**Pruning lifecycle (from pruner service):**

1. **Phase 1 — Parent preservation** (MUST succeed before Phase 2):
   - Scan for unmined transactions with `unmined_since <= cutoff_block_height`
   - For each: identify parent transactions, set `preserve_until = current_height + ParentPreservationBlocks`
2. **Phase 2 — DAH cleanup** (only if Phase 1 succeeds):
   - Query records where `delete_at_height <= current_height`
   - Delete each record (freelist + index removal + external blob cleanup)
   - **KO-2:** the sweep re-validation deletes a candidate when it is either
     CONFLICTING (a double-spend loser, DAH'd unconditionally by
     `setConflicting` and never all-spent / never on the longest chain) OR
     all-spent ∧ on-longest-chain. The pre-fix predicate required all-spent ∧
     on-longest-chain for *every* candidate, so conflicting records were DAH'd
     but never deleted (stale DAH re-scanned every block forever).
   - **KO-3:** re-validation runs under the per-tx stripe lock
     (`Engine::is_due_for_sweep`) and the actual delete re-checks the same
     predicate under the lock (`DeleteRequest::due_guard`). A
     `PreserveUntilBatch` that lands between selection and delete therefore
     wins the race — the record is kept (`SpendError::NotDue` / wire
     `ERR_NOT_DUE`), not silently deleted. Direct client `OP_DELETE_BATCH`
     (`due_guard == None`) stays unconditional.
3. **Phase 3 — Expired preservation processing** (KO-1, implemented as a
   pre-pass inside `OP_PROCESS_EXPIRED_PRESERVATIONS`):
   - Scan for records where `preserve_until` is in `[1, current_height]`
   - For each: set `delete_at_height = current_height + BlockHeightRetention`
     and clear `preserve_until` (unconditional, matching the Aerospike pruner
     `ProcessExpiredPreservations`; the elapsed preservation window is itself
     the signal that the record may be reclaimed). The record is then deleted
     `BlockHeightRetention` blocks later by the Phase 2 sweep.
   - **Wire payload:** `OP_PROCESS_EXPIRED_PRESERVATIONS` takes
     `[current_height:4]` (legacy: expiry pre-pass skipped) or
     `[current_height:4][block_height_retention:4]` (the 8-byte form supplies
     the retention used to schedule the expired records' DAH; the Aerospike
     store reads it from server config, TeraSlab's pruner client supplies it
     the same way the hot-path mutations carry `block_height_retention`).

**Configuration:**
- `BlockHeightRetention`: 288 blocks (~2 days)
- `UnminedTxRetention`: 144 blocks (~1 day)
- `ParentPreservationBlocks`: 1440 blocks (~10 days)

---

## 4. Storage Engine

### 4.1 Storage Backends

The storage engine is abstracted behind a `DeviceBackend` trait, allowing two implementations:

```rust
trait DeviceBackend: Send + Sync {
    fn read(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;
    fn write(&self, offset: u64, buf: &[u8]) -> io::Result<usize>;
    fn sync(&self) -> io::Result<()>;
    fn capacity(&self) -> u64;
}
```

#### 4.1.1 Raw Device Backend (Production)

- Open NVMe block devices with `O_DIRECT` — bypass filesystem entirely
- All I/O aligned to device sector size (typically 512B or 4096B)
- Use `io_uring` for all I/O operations (no `read`/`write` syscalls)
- No filesystem, no page cache, no VFS overhead
- Maximum throughput and predictable latency

**Configuration**: `storage_backend = "raw"`, `device_paths = ["/dev/nvme0n1", "/dev/nvme1n1"]`

#### 4.1.2 File Backend (Development / Testing)

- Uses regular files on a formatted filesystem (ext4, XFS, etc.)
- Supports `O_DIRECT` on filesystems that allow it, falls back to buffered I/O + `fsync`
- `io_uring` used when available (Linux 5.6+), falls back to `pread`/`pwrite` on macOS or older kernels
- Pre-allocates files via `fallocate` (or `ftruncate` on macOS) to desired capacity
- Multiple files can simulate multiple devices for testing sharding/replication

**Configuration**: `storage_backend = "file"`, `device_paths = ["/var/data/utxo0.dat", "/var/data/utxo1.dat"]`

**Trade-offs vs raw device:**

| | Raw Device | File Backend |
|---|-----------|-------------|
| Throughput | Maximum (no VFS) | ~80-95% of raw (filesystem overhead) |
| Latency p99.9 | Best (no page cache contention) | Slightly higher (VFS locking) |
| Setup | Requires unformatted block device | Any filesystem, no root needed |
| Portability | Linux only | Linux, macOS, CI environments |
| Debugging | Harder (no ls/du) | Files visible in filesystem |

Both backends present identical semantics to the record allocator — the same freelist, same alignment, same allocation logic. The only difference is how bytes reach the device.

### 4.2 Record Allocation

**Allocation strategy:**

1. **Size classes**: Pre-define allocation buckets:
   - Tiny: ≤ 4 KiB (1-50 outputs typical)
   - Small: ≤ 64 KiB (50-900 outputs)
   - Medium: ≤ 1 MiB (900-15,000 outputs)
   - Large: ≤ 16 MiB (15,000-235,000 outputs)
   - Huge: > 16 MiB (extremely rare)

2. **Per-size-class freelists**: Doubly-linked list of free blocks per size class
3. **Allocation**: Pop from freelist → `O(1)`. If empty, extend device allocation.
4. **Deallocation**: Push to freelist → `O(1)`.

**Freelist persistence**: The freelist is checkpointed to disk alongside the primary index. On recovery, load from checkpoint and replay redo log (Create/Delete entries) to reconcile. Without a checkpoint, the freelist can be rebuilt by walking all primary index entries to determine occupied offsets — the complement is free space.

**Record size calculation:**
```
record_size = METADATA_SIZE (fixed, compile-time constant)
            + utxo_count × 69 (UTXO slots)
            + cold_data_size (variable, aligned)
```

**Alignment**: All records aligned to 4 KiB boundaries. This satisfies `O_DIRECT` requirements on both raw devices and filesystem files, and maps naturally to NVMe page sizes.

### 4.3 Tiered Storage for Cold Data

#### Tier 1: Inline (< ~8 KiB)

Raw inputs/outputs stored in the cold data section of the record (after UTXO slots). Written once at creation, never modified. Covers the vast majority of transactions.

#### Tier 2: Separate NVMe block (8 KiB - ~1 MiB)

Cold data on same NVMe device but in separate write. Hot record committed first — tx is spendable immediately. Cold data written asynchronously.

#### Tier 3: External blob store (> ~1 MiB)

Content-addressed blob store (local files, S3, MinIO). Hot record stores an `ExternalRef`. Write-once, stream on read.

For the 320 MB case: **NEVER** on the NVMe UTXO devices. A 320 MB write would monopolize io_uring for milliseconds. Goes directly to blob store, streamed on read.

### 4.4 Creation Pipeline

**Small transactions (< 8 KiB cold data):**
1. Allocate single contiguous record
2. Write metadata + UTXO slots + cold data in one io_uring SQE
3. Insert index entry

**Medium transactions (8 KiB - 1 MiB):**
1. Allocate hot record + separate cold block
2. Write hot record first (tx is spendable)
3. Write cold block asynchronously
4. Insert index entry after hot record is committed

**Large transactions (> 1 MiB):**
1. Allocate hot record only
2. Write hot record (tx is spendable, `external = true`)
3. Upload blob to external store in parallel
4. Once uploaded, `pwrite` the `external_ref` field in metadata
5. Insert index entry after hot record is committed

### 4.5 No Defragmentation

Unlike the previous system's log-structured storage, records are updated **in-place**. There is no copy-on-write and therefore no fragmentation. The `spend` operation writes 69 bytes at a known offset — the record space is reused.

Deleted record space is returned to the freelist. Over time, the freelist may become fragmented (many small blocks where large records cannot fit), but this can be addressed by:
- Background compaction of adjacent free blocks
- Periodic offline compaction (rare, not on hot path)

---

## 5. Index

### 5.1 Requirements

- Pure point lookups by txid hash. No range queries, no ordered traversal.
- Must support 10-100 billion entries as the network grows.
- Must fit in DRAM for single-hop lookup (or use tiered index for very large scales).

### 5.2 Hash Table Design

**Type**: Fixed-size open-addressing hash table with Robin Hood hashing.

**Why Robin Hood**: Provides bounded worst-case probe length, good cache behavior, and simple implementation. The txid is already a strong cryptographic hash — use first 8 bytes directly as bucket index.

**Entry format:**
```rust
struct IndexEntry {
    fingerprint: u64,    // bytes [8..16] of txid (for collision detection)
    record_offset: u64,  // physical NVMe offset
}
// 16 bytes per entry
```

**Sizing examples** (at 75% load factor, 16 bytes per entry):

| Dataset | Slots | Memory |
|---------|-------|--------|
| 10 billion entries | 13.3B slots | ~213 GB |
| 50 billion entries | 66.7B slots | ~1.07 TB |
| 100 billion entries | 133B slots | ~2.13 TB |

At 10B entries, fits in DRAM on a large server (256-512 GB). At 50-100B entries, requires either multi-node sharding (each node holds a subset of the index) or a tiered index with a hot DRAM layer and warm NVMe layer for overflow. The sharding approach (Section 9) naturally distributes the index — with 16 nodes, each holds ~6.25B entries (~100 GB) for a 100B-entry dataset.

**Bucket index**: `bucket = u64_from_le(txid[0..8]) % table_size`
**Collision detection**: Compare `fingerprint` (txid[8..16]) — false positive rate ≈ 1/2^64.

### 5.3 Memory Management

- Map with **2 MB hugepages** (`mmap` with `MAP_HUGETLB`) — avoids TLB misses on random lookups (20-30% improvement measured)
- For very large tables: **1 GB hugepages** via `MAP_HUGE_1GB`
- **NUMA-aware placement**: Pin index partition to same NUMA node as the NVMe controller's PCIe slot
  - Cross-NUMA memory access adds ~100ns per probe
  - At 10M lookups/sec that's 1s/sec of waste — effectively halving throughput

### 5.4 Persistence and Recovery

The index is a derived data structure — it can be rebuilt from a scan of all records on device. However, for fast startup:

1. **Checkpoint to disk**: Periodically snapshot the hash table to a file (mmap + msync)
2. **Recovery**: Load checkpoint, then replay redo log entries since checkpoint
3. **Cold start**: If no checkpoint, full device scan to rebuild index (minutes, not hours)

### 5.5 Secondary Indexes for Pruner Queries

The pruner needs to efficiently find records by `delete_at_height` and `unmined_since` — queries that the primary txid hash index cannot serve. The previous system used secondary indexes on those bins. The Rust system maintains two lightweight secondary structures:

#### 5.5.1 DAH Index (delete_at_height)

A sorted structure mapping `delete_at_height → Vec<txid>`, enabling the pruner to efficiently query "all records where `delete_at_height <= current_height`".

**Implementation**: B-tree or sorted array of `(height: u32, txid: [u8; 32])` entries, kept in memory and checkpointed to disk.

**Maintenance**:
- **Insert**: When `setDeleteAtHeight` sets a non-zero DAH value, insert `(height, txid)` into the DAH index
- **Remove**: When `setDeleteAtHeight` clears DAH (sets to 0), or when the record is deleted, remove the entry
- **Query**: `range_scan(0..=current_height)` returns all txids eligible for deletion

**Size**: At most one entry per record with a non-zero DAH. At ~4M entries (records pending deletion), this is ~144 MB — trivially fits in memory.

#### 5.5.2 Unmined Index (unmined_since)

A sorted structure mapping `unmined_since → Vec<txid>`, enabling the pruner to query "all unmined transactions older than cutoff height".

**Implementation**: Same B-tree/sorted-array approach as the DAH index.

**Maintenance**:
- **Insert**: When a record is created with `unmined_since != 0`, or when `setMined` sets `unmined_since` to a non-zero value
- **Remove**: When `unmined_since` is set to `0` (transaction mined on longest chain), or record deleted
- **Query**: `range_scan(0..=cutoff_height)` returns old unmined txids

**Size**: Proportional to mempool size. Even with millions of unmined transactions, this is manageable in memory.

**Crash safety differs between the two indexes:**

- **Unmined Index**: Critical for correctness — a stale unmined index would miss transactions that need parent preservation, leading to data loss. Unmined index mutations (`unmined_since` changes) are included in the redo log and replayed on recovery. This adds ~36 bytes per redo entry (txid + old/new height) but unmined_since changes are infrequent relative to spends.

- **DAH Index**: Not critical — a stale DAH index only delays pruning, which is a background optimization. The DAH index is rebuilt from a full device scan on recovery. This is acceptable because: (a) DAH records are a small fraction of total records, (b) delayed pruning is harmless, (c) the scan can run in the background while the node serves traffic.

Both indexes are checkpointed alongside the primary index for fast normal-case startup.

---

## 6. Concurrency

### 6.1 Per-Transaction Lock Striping

Replace the original system's per-record write lock + Lua VM with:

```rust
const LOCK_STRIPES: usize = 65536;
static LOCKS: [parking_lot::Mutex<()>; LOCK_STRIPES];

fn lock_for_txid(txid: &[u8; 32]) -> &Mutex<()> {
    let stripe = u16::from_le_bytes([txid[0], txid[1]]) as usize;
    &LOCKS[stripe]
}
```

- 65,536 stripes — collision probability for concurrent operations on different txids: ~1/65536
- Critical section is **microseconds** (memcmp + pwrite), not milliseconds (Lua interpretation)
- For `spendMulti` on same txid: single lock held for the entire batch

### 6.2 io_uring Batching Architecture

Design the entire I/O path around submission batching:

```
Client requests → per-device lock-free ring buffer
                         ↓
Submission thread (per device):
    drain ring every 50µs OR at batch-size threshold
    → single io_uring_enter() with N SQEs
                         ↓
NVMe controller parallelizes queued operations
                         ↓
Completion thread:
    harvest CQEs in bulk
    resolve client futures
```

At 375K ops/device/sec with 50µs batching window ≈ 19 ops per submission batch.

### 6.3 Thread Model

| Thread Type | Count | Responsibility |
|------------|-------|---------------|
| Service threads | N (configurable, ~CPU cores) | Accept client connections, parse requests, dispatch |
| Submission threads | 1 per NVMe device | Drain per-device ring, submit io_uring batches |
| Completion threads | 1 per NVMe device | Harvest CQEs, resolve futures |
| Replication threads | 1 per replica connection | Send/receive ReplicaOp streams |
| Index maintenance | 1 | Periodic checkpoint, statistics |
| Metrics | 1 | Aggregate and export counters |

### 6.4 Batching Configuration (matching Go settings)

| Batcher | Default Size | Default Duration | Concurrency |
|---------|-------------|-----------------|-------------|
| Store (create) | 100 | 100ms | — |
| Spend | 100 | 100ms | 32 |
| Get | 1 (disabled) | 10ms | — |
| SetMined | 1024 | — | 128 workers |
| Locked | 1024 | 5ms | — |
| LongestChain | 1024 | 5ms | — |
| Increment | 256 | 10ms | — (ELIMINATED) |
| SetDAH | 256 | 10ms | — |
| Outpoint | 100 | 5ms | 32 |

---

## 7. Crash Safety

### 7.1 Redo Log Design

Small circular redo log per device (~64 MB):

```rust
struct RedoEntry {
    sequence: u64,           // monotonic sequence number
    operation: RedoOp,       // operation type enum
    txid: [u8; 32],          // affected transaction
    payload: [u8; N],        // operation-specific data (variable)
    checksum: u32,           // CRC32 of above
}

enum RedoOp {
    Spend { offset: u32, spending_data: [u8; 36] },  // 36 = txid(32) + vin(4)
    Unspend { offset: u32 },
    SetMined { entry: BlockEntry, on_longest_chain: bool },
    UnsetMined { block_id: u32 },
    SetConflicting { value: bool },
    SetLocked { value: bool },
    SetDeleteAtHeight { height: u32 },  // 0 = clear
    PreserveUntil { height: u32 },
    Freeze { offset: u32 },
    Unfreeze { offset: u32 },
    Reassign { offset: u32, new_hash: [u8; 32], block_height: u32, spendable_after: u32 },
    PruneSlot { offset: u32 },  // set UTXO slot status to PRUNED (child tx deleted)
    Create { record_offset: u64, record_size: u32 },
    Delete { record_offset: u64 },
}
```

### 7.2 Write Protocol

For each mutation batch:
1. Append redo entries to log (sequential write via io_uring)
2. Apply data mutations (parallel pwrite via io_uring) — can overlap with redo flush
3. On io_uring completion of BOTH: acknowledge to client
4. Periodically advance checkpoint pointer (redo entries before this point can be overwritten)

### 7.3 Recovery Procedure

1. Open redo log, find last valid checkpoint
2. Scan forward from checkpoint, verify checksums
3. For each valid redo entry: re-apply operation to data
4. All operations are **idempotent**:
   - Spending an already-spent UTXO with same data: no-op
   - Setting mined with same block_id: no-op (detected by scan)
   - Writing same metadata flags: idempotent write
5. Rebuild or verify index against on-disk records

### 7.4 Crash Scenarios

| Crash Point | State | Recovery Action |
|------------|-------|----------------|
| Before redo flush | No redo entry | Operation lost, client retries |
| After redo, before data write | Redo entry exists | Replay re-applies data write |
| After data write, before ack | Data committed | Replay is no-op (idempotent), client may retry |
| Mid-batch (partial writes) | Some data written | Replay re-applies all; idempotent |

---

## 8. Replication

### 8.1 Operation-Based Protocol

Replace the previous system's full-record replication with operation-level replication:

```rust
enum ReplicaOp {
    CreateTx {
        key: [u8; 32],
        metadata: TxMetadata,
        utxo_count: u32,
        cold_data: Option<Vec<u8>>,
    },
    Spend {
        key: [u8; 32],
        offset: u32,
        spending_data: [u8; 36],
    },
    Unspend {
        key: [u8; 32],
        offset: u32,
    },
    SetMined {
        key: [u8; 32],
        entry: BlockEntry,
        on_longest_chain: bool,
    },
    UnsetMined {
        key: [u8; 32],
        block_id: u32,
    },
    Freeze {
        key: [u8; 32],
        offset: u32,
    },
    Unfreeze {
        key: [u8; 32],
        offset: u32,
    },
    Reassign {
        key: [u8; 32],
        offset: u32,
        new_hash: [u8; 32],
        block_height: u32,
        spendable_after: u32,
    },
    PruneSlot {
        key: [u8; 32],
        offset: u32,
    },
    SetConflicting {
        key: [u8; 32],
        value: bool,
        current_block_height: u32,
        block_height_retention: u32,
    },
    SetLocked {
        key: [u8; 32],
        value: bool,
    },
    SetDeleteAtHeight {
        key: [u8; 32],
        height: u32,          // 0 = clear
    },
    PreserveUntil {
        key: [u8; 32],
        height: u32,
    },
    Delete {
        key: [u8; 32],
    },
}
```

### 8.2 Bandwidth Comparison

| Operation | Legacy (full record) | Rust (operation-based) |
|-----------|------------------------|----------------------|
| Spend | ~500-5000 bytes | ~72 bytes |
| SetMined | ~500-5000 bytes | ~52 bytes |
| Unspend | ~500-5000 bytes | ~40 bytes |
| SetConflicting | ~500-5000 bytes | ~44 bytes |

At 10M TPS with RF=2: **~400 MB/s** fabric traffic (operation-based replication).
At 10M TPS with RF=3: **~800 MB/s** — still manageable on 10 GbE. Full-record replication (as in the legacy system) would require ~2+ GB/s at this throughput.

### 8.3 Configurable Replication Factor

The replication factor (RF) is configurable per deployment:

| RF | Behavior | Use Case |
|----|----------|----------|
| 1 | No replication (master only) | Development, testing |
| 2 | Master + 1 replica (default) | Production — matches current deployment |
| 3 | Master + 2 replicas | High availability, prerequisite for SC mode (Raft) |

With RF=3, the master sends `ReplicaOp` to **both** replicas in parallel. Acknowledgment policy is configurable:
- **write-all**: wait for all replicas to ACK before responding to client
- **write-majority**: wait for ⌊RF/2⌋+1 ACKs (e.g., 2 of 3) — lower latency, used for SC mode with Raft
- **auto** (default): resolves per RF — `write-all` for RF≤2 (so RF=2 still
  requires 2-of-2) and `write-majority` for RF≥3 (so RF=3 requires 2-of-3, not
  3-of-3). This matches the implementation's `ack_policy = auto` resolution; a
  deployment that needs strict 3-of-3 at RF=3 must set `write-all` explicitly.

**Isolated-remnant write gating (`ERR_NO_QUORUM`, code 15):** a node that was
part of a larger committed topology will not accept mutations once its live,
committed membership drops below the majority of the **peak** cluster size it
has ever observed — it returns `ERR_NO_QUORUM` instead. This prevents a minority
remnant of a partitioned cluster from accepting writes that the majority side
cannot see. (Not present in earlier drafts of this spec; it is implemented and
tested.)

### 8.4 Synchronous Replication Path

1. Client → master node (partition map lookup, single hop)
2. Master applies mutation locally (redo log + data write)
3. Master sends `ReplicaOp` to all replica node(s) in parallel
4. Each replica applies operation (in-place write on its device)
5. Each replica ACKs (includes sequence number)
6. Master waits for required ACKs per acknowledgment policy
7. Master returns success to client

### 8.5 Failure Handling

- **Replica timeout**: when the master cannot confirm the required replica ACKs
  within the timeout it returns **`ERR_REPLICATION_FAILED` (code 20)** — an
  *ambiguous* outcome — and runs the asynchronous compensation/convergence path,
  NOT a client-visible success. (Earlier drafts of this spec said the master
  returns success after local commit; the implementation fails closed instead.
  See §8.7 for the full client contract.) The lagging replica is also tracked
  for catch-up.
- **Replica crash**: On recovery, replica requests replay from master's redo log starting at last ACK'd sequence
- **Master crash**: Replica promotes to master, serves reads; new replica backfills from promoted master's redo log
- **Network partition**: Operations continue on master side; replica catches up when partition heals

### 8.6 Replication Lag Monitoring

- Track `last_ack_sequence` per replica
- Expose `replication_lag_ops` = `master_sequence - last_ack_sequence`
- Alert if lag exceeds configurable threshold

### 8.7 `ERR_REPLICATION_FAILED` — ambiguous outcome, idempotent-retry-safe

`ERR_REPLICATION_FAILED` (code `20`) is returned when the master could not
confirm the durability contract (the required number of replica ACKs) for a
mutation within the timeout. **It is an ambiguous outcome:** when a client
receives code `20`, the write may have become durable on the master only, on
one or more replicas only, on both, or on neither. The error reports a failure
to *confirm* durability — not a definitive rollback — so the client must not
assume either that the write took effect or that it did not.

- **Convergence is the server's responsibility.** The master's compensation
  machinery (see §9 cluster management and the dispatch/coordinator
  compensation path) reconciles the divergent state asynchronously: a write
  that landed on only a subset of the replica set is either driven forward to
  the full set or compensated (deleted/unwound) so that, once replication
  quiesces, every replica converges to a single consistent outcome for that
  op. A record observed immediately after a code `20` may still be
  compensation-deleted moments later; readers must let replication settle
  before treating a post-error read as authoritative.

- **The prescribed client recovery is an idempotent retry.** Because every
  TeraSlab mutation is idempotent by txid/op semantics — re-spending an
  already-spent output with the same spending data, re-mining an
  already-mined transaction, re-creating an already-present record, etc., all
  converge to the same state rather than double-applying — a client that
  receives code `20` should re-issue the *identical* op. The retry is safe
  regardless of which of the four durability outcomes actually occurred: if
  the op had already taken effect the retry is a no-op against the converged
  state; if it had not, the retry re-drives it. Clients retry with bounded
  attempts and backoff (the reference client retries up to its transient
  budget, refreshing routing between attempts).

- **Classification.** Code `20` is therefore a *transient, same-target
  retryable* code alongside `ERR_MIGRATION_IN_PROGRESS` (19) and
  `ERR_STALE_EPOCH` (24) — distinct from `ERR_REDIRECT` (14), which instructs
  the client to re-route to a different node rather than retry the same one.

---

## 9. Cluster Management

### 9.1 Hash-Based Sharding

- **Shard by first 12 bits** of txid hash → 4096 shards
- Assignment is static: `target_node = shard_table[u16_from_le(txid[0..2]) & 0x0FFF]`
- No partition map gossip in steady state
- Client receives shard table on connect and caches it

### 9.2 Shard Assignment and Rebalancing

**Node join:**
1. New node announces to cluster via heartbeat
2. Controller assigns subset of shards to new node
3. Migration: sequential read of source device → network send → write to target device
4. Atomic shard ownership transfer once migration complete

**Node leave:**
1. Heartbeat failure detected
2. Designated successor absorbs orphaned shards
3. If replica exists: promote replica, no data migration needed

### 9.3 Membership Protocol (SWIM)

Use the SWIM protocol (Scalable Weakly-consistent Infection-style Membership) instead of a full TCP mesh. SWIM scales to thousands of nodes with O(1) network load per node, constant failure detection time, and no N² connection overhead.

**Why SWIM over TCP mesh:** A TCP mesh requires N×(N-1) connections — at 256 nodes that's 65K connections, at 512 nodes it's 262K. SWIM uses UDP probes with bounded fan-out, so each node sends a fixed number of messages per interval regardless of cluster size.

**Implementation:** Use the `foca` crate (Rust SWIM implementation) or implement directly:

- **Probe interval**: 200ms
- **Probe target**: each interval, probe one randomly selected node via UDP
- **Indirect probes**: if direct probe fails, ask K random nodes (default K=3) to probe the suspect on our behalf
- **Failure detection**: if direct + indirect probes all fail → mark suspect. After configurable suspicion timeout (5s) → declare dead.
- **Dissemination**: membership changes (join/leave/suspect) are piggybacked on probe messages — no separate gossip protocol needed. Changes propagate in O(log N) probe rounds.

**Message payload** (piggybacked on probes):
- Node ID, address, incarnation number
- Membership updates: `[{node_id, state(alive|suspect|dead), incarnation}]`
- Shard table version (so nodes detect when they need to re-fetch the shard table)

**Properties:**
- O(1) network load per node regardless of cluster size
- Failure detection time: bounded, independent of N
- False positive rate: tunable via K (indirect probes) and suspicion timeout
- Scales to 1000+ nodes without design changes

### 9.4 Capacity Planning

#### Per-node throughput assumptions

| Resource | Specification | Throughput contribution |
|----------|--------------|----------------------|
| NVMe device | ~500K-1M random 4KB IOPS each | Each device supports ~500K-1M spend ops/sec |
| NVMe devices per node | 4 typical | ~2M-4M raw I/O ops/sec |
| CPU cores | 32 typical | Spend critical section ~1-2µs → 500K-1M ops/core → 10M+ per node |
| Network (10 GbE) | 1.25 GB/sec | Saturates at ~10M ops/sec with RF=2 replication |
| Network (25 GbE) | 3.125 GB/sec | Comfortable at 10M+ ops/sec per node |
| Network (100 GbE) | 12.5 GB/sec | Plenty of headroom |

The bottleneck at 10M ops/sec per node is typically **NVMe IOPS**, not CPU. Adding NVMe devices scales linearly.

#### Theoretical cluster maximums

| Cluster size | Shards/node | Aggregate TPS | Total records (10B each) | Network requirement |
|-------------|-------------|--------------|-------------------------|-------------------|
| 4 nodes | 1024 | 40M | 40B | 25 GbE |
| 16 nodes | 256 | 160M | 160B | 25 GbE |
| 64 nodes | 64 | 640M | 640B | 25 GbE |
| 256 nodes | 16 | 2.5B+ | 2.5T+ | 25-100 GbE |
| 512 nodes | 8 | 5B+ | 5T+ | 100 GbE |

#### Scaling bottlenecks by tier

| Nodes | Bottleneck | Mitigation |
|-------|-----------|-----------|
| 1-16 | NVMe IOPS per node | Add NVMe devices, use faster drives |
| 16-64 | Network bandwidth (RF=2 replication) | Upgrade to 25/100 GbE |
| 64-256 | Shard granularity (4096 shards, need ≥4/node for balance) | Increase shard count if needed |
| 256-512 | Index memory per node at extreme record counts | Distribute index via sharding |
| 512+ | 4096 shards limits to 512 nodes at 8 shards/node | Increase to 8192+ shards |

The SWIM membership protocol scales to 1000+ nodes with O(1) network load. The practical ceiling is shard count (4096 default, can be increased) and network bandwidth per node.

### 9.5 Client Routing

**No server-side proxying.** At millions of ops/sec, forwarding between nodes would add latency and create bottlenecks. The client routes directly to the correct node.

#### Normal flow (99.9%+ of requests):
1. On connect, client fetches the shard table via `OP_GET_PARTITION_MAP` — maps all 4096 shards to `(master_node, replica_nodes)`
2. Client caches the shard table locally
3. Before sending a batch, client groups items by target node: `shard_for_key(txid) → node_id`
4. One batch frame per node, sent directly to the correct master — no intermediate hops

#### Stale map handling (after rebalance):
1. Client sends a batch containing items for shards this node no longer owns
2. Server processes items it owns locally
3. For items it doesn't own, server returns **per-item Redirect** with the correct node address and the current shard table version
4. Client detects the version mismatch, re-fetches the shard table via `OP_GET_PARTITION_MAP`
5. Client re-sends only the redirected items to the correct node

The client never hits the same redirect twice — after one refresh, all subsequent routing is correct until the next rebalance. During steady state (no rebalances), the shard table is static and no redirects occur.

#### Batch routing example:
```
Client has 1024 spends for 200 different txids.
Shard table maps these to 3 nodes: node-1 (410 items), node-2 (380 items), node-3 (234 items).
Client sends 3 batch frames in parallel — one to each node.
Each node processes its batch independently, returns per-item results.
```

---

## 10. Wire Protocol

> **SUPERSEDED BY THE IMPLEMENTATION.** §10.2 and §10.3 below are the original draft design and do
> **not** match the shipped protocol. The authoritative wire format is defined in `src/protocol/frame.rs`
> and `src/protocol/opcodes.rs`, and documented in the README ("Wire protocol" section). Key
> divergences: the implemented frame is `[total_length:u32][request_id:u64][op_code:u16][flags:u16][payload]`
> — there is **no** magic number, **no** version byte, and **no** trailing frame CRC32 (the draft below
> shows all three). The handshake/version is carried by the `Hello` opcode (107), not a per-frame version
> byte. Opcode numbers also differ: the implementation uses `OP_HEARTBEAT = 250` (the draft's `0x00FF` /
> 255 is `OP_INCREMENT_SPENT_EXTRA_RECS`), and the implementation has post-draft opcodes (103-107,
> 242-243, 251-253) absent below. Implement clients from the README / `opcodes.rs`, not from §10.2-§10.3.

### 10.1 Design Principles

- Binary protocol, not text
- Request-response with pipelining
- Batch operations as first-class citizens
- Framing for streaming large transaction reads

### 10.2 Frame Format

```
┌──────────────────────────────────────┐
│ Magic: u32 (0x55545830 = "UTX0")     │
│ Version: u8                          │
│ Flags: u8                            │
│ OpCode: u16                          │
│ Request ID: u64                      │
│ Payload Length: u32                   │
│ Payload: [u8; payload_length]        │
│ Checksum: u32 (CRC32)               │
└──────────────────────────────────────┘
```

### 10.3 OpCodes

**Every operation is batch-first.** Single-item operations are batches of size 1 — no separate code paths. This matches the existing Go client pattern, which uses batch operations for all calls.

| OpCode | Operation | Direction | Notes |
|--------|----------|-----------|-------|
| `0x0001` | SpendBatch | Request | N spends, grouped by txid server-side |
| `0x0002` | UnspendBatch | Request | N unspends |
| `0x0003` | SetMinedBatch | Request | N txids × 1 shared block entry |
| `0x0004` | CreateBatch | Request | N record creations |
| `0x0005` | FreezeBatch | Request | N freezes |
| `0x0006` | UnfreezeBatch | Request | N unfreezes |
| `0x0007` | ReassignBatch | Request | N reassignments |
| `0x0008` | SetConflictingBatch | Request | N txids |
| `0x0009` | SetLockedBatch | Request | N txids |
| `0x000A` | PreserveUntilBatch | Request | N txids |
| `0x000B` | DeleteBatch | Request | N txids |
| `0x000C` | MarkLongestChainBatch | Request | N txids |
| `0x0014` | GetBatch | Request | N txids with shared field mask |
| `0x0015` | GetSpendBatch | Request | N spend lookups |
| `0x0020` | QueryOldUnmined | Request | Pruner scan |
| `0x0021` | PreserveTransactions | Request | Pruner batch preserve |
| `0x0022` | ProcessExpiredPreservations | Request | Pruner batch expire |
| `0x0064` | GetPartitionMap | Request | Cluster routing |
| `0x0065` | Health | Request | Health check |
| `0x0066` | Ping | Request | Keep-alive |
| `0x00C8` | StreamChunk | Response | Streaming large reads |
| `0x00C9` | StreamEnd | Response | End of stream |
| `0x00F0` | ReplicaBatch | Replication | Batch of ReplicaOps |
| `0x00F1` | ReplicaAck | Replication | Sequence ACK |
| `0x00FF` | Heartbeat | Cluster | Membership gossip |

### 10.4 Batch Operation Support

Batch requests contain a count field followed by N individual operation payloads. Responses include per-item status codes, allowing partial success.

```
BatchSpendMulti request:
  count: u32
  items: [
    { txid: [u8;32], vout: u32, utxo_hash: [u8;32], spending_data: [u8;36] },
    ...
  ]
  ignore_conflicting: bool
  ignore_locked: bool
  current_block_height: u32
  block_height_retention: u32

BatchSpendMulti response:
  status: u8 (OK=0, PARTIAL_ERROR=1, ERROR=2)
  items: [
    { status: u8, error_code: Option<u16>, spending_data: Option<[u8;72]> },
    ...
  ]
  # NOTE: block_ids / signal are NOT serialized — see the LP-5 decision in §3.4.
  # The implemented BatchSpendMulti response is status + sparse per-item entries only.
```

### 10.5 Streaming for Large Transaction Reads

For transactions > 1 MiB (external blob store):
1. Client sends `Get` with `fields` including `Tx`
2. Server returns metadata immediately in `Response`
3. Server streams blob data as `StreamChunk` frames (64 KiB each)
4. Final `StreamEnd` frame with total size and checksum

### 10.6 New Go Client

A reusable Go client library (`teraslab-client-go`, separate repo) handles the wire protocol. The Teranode adapter (`stores/utxo/teraslab/` in the Teranode repo) imports it and implements the `Store` interface. See §13 for details.

**Design principles:**
- Purpose-built for the binary protocol defined above — no generic database abstraction layer
- Connection pooling with configurable pool size per node
- Partition map caching with automatic refresh on routing errors
- Client-side batcher architecture preserved (storeBatcher, spendBatcher, getBatcher, etc.), submitting batch requests natively
- Zero-copy where possible: reuse `bytes.Buffer` pools, avoid intermediate serialization

**What is removed vs the legacy client:**
- All Lua UDF registration and invocation
- All expression-based alternative paths
- All multi-record pagination logic (`totalExtraRecs`, `spentExtraRecs`, `incrementSpentExtraRecs`)
- Legacy database Go SDK dependency entirely

---

## 11. Observability

### 11.1 Design Principle

**No allocations, no locks, no syscalls on the metrics/logging hot path.** At 10M+ ops/sec, even a formatted log line per operation would be devastating. The observability layer must be invisible to the spend path.

- `AtomicU64::fetch_add(1, Relaxed)` ≈ 1ns — acceptable
- HDR histogram record ≈ 5-10ns — acceptable
- `format!()` or `String` allocation ≈ 50-200ns — **not acceptable on hot path**
- `write()` syscall for logging ≈ 1-5µs — **not acceptable on hot path**

### 11.2 Logging

Use the `tracing` crate with structured fields. All log points use typed fields, not string formatting — the formatting only happens if the log level is enabled and a subscriber is attached.

#### Log levels by path

| Level | Hot path (spend/spendMulti) | Warm path (setMined/create) | Cold path (cluster/pruner/replication) |
|-------|---------------------------|----------------------------|----------------------------------------|
| ERROR | Always — I/O failures, data corruption, index inconsistency | Always | Always |
| WARN | Always — unexpected validation failures (should be rare) | Always | Always |
| INFO | **Never** | Batch completion summaries | All significant events (rebalance, migration, node join/leave) |
| DEBUG | Gated behind runtime flag | Per-operation detail | Full detail |
| TRACE | **Never in production** | **Never in production** | Per-step detail (only dev/test) |

#### Batch log aggregation

For high-throughput operations, log a single summary per batch, not per item:

```
INFO spend_batch_complete{count=1024, succeeded=1020, failed=4, duration_us=850}
INFO create_batch_complete{count=256, duration_us=1200}
INFO set_mined_batch_complete{count=512, signals=3, duration_us=600}
```

#### Runtime log level control

The log level must be changeable at runtime without restart — via the `/debug/log-level` endpoint or a config reload signal. This enables temporarily enabling DEBUG on the spend path for investigation, then turning it off.

#### Error context

Errors include structured context for diagnosis without requiring DEBUG level:

```
ERROR spend_io_failed{txid=abcd..., offset=42, device="/dev/nvme0n1", errno=5, record_offset=0x1A000}
WARN index_probe_depth_exceeded{txid=abcd..., depth=64, load_factor=0.92}
```

### 11.3 Metrics

Three types, all zero-allocation on the recording path:

#### 11.3.1 Counters (per-thread AtomicU64)

Each service thread maintains thread-local counters using `CachePadded<AtomicU64>` to prevent false sharing. A background aggregator merges them every 1 second into global counters for export.

```rust
#[repr(align(64))]  // cache-line aligned
struct ThreadMetrics {
    ops_create: CachePadded<AtomicU64>,
    ops_spend: CachePadded<AtomicU64>,
    ops_spend_multi: CachePadded<AtomicU64>,
    ops_unspend: CachePadded<AtomicU64>,
    ops_set_mined: CachePadded<AtomicU64>,
    ops_freeze: CachePadded<AtomicU64>,
    ops_unfreeze: CachePadded<AtomicU64>,
    ops_reassign: CachePadded<AtomicU64>,
    ops_set_conflicting: CachePadded<AtomicU64>,
    ops_set_locked: CachePadded<AtomicU64>,
    ops_preserve_until: CachePadded<AtomicU64>,
    ops_prune_slot: CachePadded<AtomicU64>,
    ops_get: CachePadded<AtomicU64>,
    ops_get_batch: CachePadded<AtomicU64>,
    ops_delete: CachePadded<AtomicU64>,

    errors_by_code: [CachePadded<AtomicU64>; 16],  // indexed by error code enum

    bytes_read: CachePadded<AtomicU64>,
    bytes_written: CachePadded<AtomicU64>,
}
```

Recording: `thread_metrics.ops_spend.fetch_add(1, Ordering::Relaxed)` — ~1ns, no contention.

#### 11.3.2 Histograms (per-thread HDR, merged on export)

Latency histograms use pre-allocated HDR histograms (from the `hdrhistogram` crate) — one per operation type per thread. No allocation on record. On `/metrics` scrape, thread-local histograms are merged into a global snapshot and percentiles extracted.

```rust
struct ThreadHistograms {
    latency_spend: Histogram<u64>,        // pre-allocated, 1µs-10s range, 3 significant digits
    latency_spend_multi: Histogram<u64>,
    latency_set_mined: Histogram<u64>,
    latency_create: Histogram<u64>,
    latency_get: Histogram<u64>,
    // ... one per operation type
    io_uring_batch_size: Histogram<u64>,   // SQEs per submission
}
```

Recording: `thread_histograms.latency_spend.record(duration_us)` — ~5-10ns, no allocation.

Export percentiles: p50, p95, p99, p99.9, p99.99, max.

#### 11.3.3 Gauges (global atomics)

Low-frequency values updated by background threads or on-demand:

```rust
struct GlobalGauges {
    // Record counts
    total_records: AtomicU64,               // total records across all devices
    total_utxo_slots: AtomicU64,            // sum of utxo_count across all records
    total_spent_slots: AtomicU64,           // sum of spent_utxos across all records
    total_pruned_slots: AtomicU64,          // sum of pruned_utxos across all records

    // Storage — per device
    device_total_bytes: [AtomicU64; MAX_DEVICES],
    device_used_bytes: [AtomicU64; MAX_DEVICES],
    device_free_bytes: [AtomicU64; MAX_DEVICES],   // freelist total
    device_record_bytes: [AtomicU64; MAX_DEVICES],  // bytes occupied by records (used - overhead)

    // Memory
    index_memory_bytes: AtomicU64,          // primary hash table RSS
    index_entries: AtomicU64,
    index_load_factor_permille: AtomicU32,  // load factor × 1000
    index_max_probe_depth: AtomicU32,
    secondary_index_memory_bytes: AtomicU64, // DAH + unmined indexes
    freelist_memory_bytes: AtomicU64,        // in-memory freelist size
    external_cache_memory_bytes: AtomicU64,  // blob cache RSS
    redo_log_memory_bytes: AtomicU64,        // redo log buffer

    // Freelist
    freelist_blocks: [AtomicU64; NUM_SIZE_CLASSES],
    freelist_fragmentation_permille: AtomicU32, // fragmentation × 1000

    // Replication
    replication_lag_ops: [AtomicU64; MAX_REPLICAS],
    replication_lag_bytes: [AtomicU64; MAX_REPLICAS],

    // Redo log
    redo_sequence: AtomicU64,
    redo_checkpoint: AtomicU64,
    redo_used_bytes: AtomicU64,
    redo_total_bytes: AtomicU64,

    // Cluster
    cluster_node_count: AtomicU32,
    cluster_shards_owned: AtomicU32,
    cluster_migrations_active: AtomicU32,

    // External blob store
    external_cache_entries: AtomicU64,
    external_cache_memory_bytes: AtomicU64,

    // Lock contention
    lock_contentions: AtomicU64,
}
```

### 11.4 Key Metrics Table

#### Operations

| Metric | Type | Description |
|--------|------|-------------|
| `teraslab.ops.total{op=spend}` | Counter | Total operations by type |
| `teraslab.ops.errors{code=TX_NOT_FOUND}` | Counter | Errors by error code |
| `teraslab.latency.spend_us` | Histogram | Spend latency in microseconds |
| `teraslab.latency.spend_multi_us` | Histogram | SpendMulti latency |
| `teraslab.latency.set_mined_us` | Histogram | SetMined latency |
| `teraslab.latency.create_us` | Histogram | Create latency |
| `teraslab.latency.get_us` | Histogram | Get/read latency |

#### Record inventory

| Metric | Type | Description |
|--------|------|-------------|
| `teraslab.records.total` | Gauge | Total records stored on this node |
| `teraslab.records.utxo_slots` | Gauge | Total UTXO slots across all records |
| `teraslab.records.spent_slots` | Gauge | Total spent UTXO slots |
| `teraslab.records.pruned_slots` | Gauge | Total pruned UTXO slots |
| `teraslab.records.unspent_slots` | Gauge | Derived: total - spent - pruned - frozen |
| `teraslab.records.external` | Gauge | Records with EXTERNAL flag (large txs in blob store) |

#### Storage

| Metric | Type | Description |
|--------|------|-------------|
| `teraslab.device.total_bytes{dev=0}` | Gauge | Total device capacity |
| `teraslab.device.used_bytes{dev=0}` | Gauge | Bytes occupied by records + overhead |
| `teraslab.device.free_bytes{dev=0}` | Gauge | Bytes available in freelist |
| `teraslab.device.utilization{dev=0}` | Gauge | used / total (0.0-1.0) |
| `teraslab.device.record_bytes{dev=0}` | Gauge | Bytes occupied by record data only |
| `teraslab.freelist.blocks{class=tiny}` | Gauge | Free blocks per size class |
| `teraslab.freelist.fragmentation` | Gauge | Fragmentation ratio (0.0-1.0) |
| `teraslab.io.read_bytes` | Counter | Total bytes read from device |
| `teraslab.io.write_bytes` | Counter | Total bytes written to device |
| `teraslab.io.uring_batch_size` | Histogram | SQEs per io_uring_enter call |
| `teraslab.io.uring_queue_depth` | Gauge | Current io_uring submission queue depth |

#### Memory

| Metric | Type | Description |
|--------|------|-------------|
| `teraslab.memory.index_bytes` | Gauge | Primary hash table RSS |
| `teraslab.memory.secondary_index_bytes` | Gauge | DAH + unmined secondary indexes |
| `teraslab.memory.freelist_bytes` | Gauge | In-memory freelist structures |
| `teraslab.memory.external_cache_bytes` | Gauge | External blob cache RSS |
| `teraslab.memory.redo_buffer_bytes` | Gauge | Redo log write buffer |
| `teraslab.memory.total_bytes` | Gauge | Sum of all managed memory |

#### Index

| Metric | Type | Description |
|--------|------|-------------|
| `teraslab.index.entries` | Gauge | Number of primary index entries |
| `teraslab.index.load_factor` | Gauge | Hash table load factor (0.0-1.0) |
| `teraslab.index.max_probe_depth` | Gauge | Worst-case probe chain length |
| `teraslab.index.dah_entries` | Gauge | DAH secondary index entries |
| `teraslab.index.unmined_entries` | Gauge | Unmined secondary index entries |

#### Replication & crash safety

| Metric | Type | Description |
|--------|------|-------------|
| `teraslab.replication.lag_ops{replica=0}` | Gauge | Operations behind per replica |
| `teraslab.replication.lag_bytes{replica=0}` | Gauge | Bytes behind per replica |
| `teraslab.redo.sequence` | Counter | Current redo log sequence number |
| `teraslab.redo.checkpoint` | Counter | Last checkpoint sequence number |
| `teraslab.redo.utilization` | Gauge | Redo log space utilization (0.0-1.0) |

#### Cluster

| Metric | Type | Description |
|--------|------|-------------|
| `teraslab.cluster.nodes` | Gauge | Active cluster nodes |
| `teraslab.cluster.shards_owned` | Gauge | Shards owned by this node |
| `teraslab.cluster.migrations_active` | Gauge | In-progress shard migrations |

#### External blob store

| Metric | Type | Description |
|--------|------|-------------|
| `teraslab.external.fetches` | Counter | Blob store fetch operations |
| `teraslab.external.cache_hits` | Counter | Blob cache hits (avoided fetches) |
| `teraslab.external.cache_entries` | Gauge | Current cache entry count |
| `teraslab.external.cache_bytes` | Gauge | Current cache memory usage |

#### Other

| Metric | Type | Description |
|--------|------|-------------|
| `teraslab.lock.contentions` | Counter | Lock stripe acquisition wait count |

### 11.5 Health Checks

**Liveness** (`/health/live`): Verify NVMe device is readable, io_uring is functional, lock system responsive. Fast — must complete in < 10ms.

**Readiness** (`/health/ready`): Liveness + index loaded + redo log replayed + replication caught up (within configurable threshold). Returns 503 during startup/recovery.

**Go interface compatibility**: `Health(ctx, checkLiveness) (int, string, error)` — returns HTTP-style status code (200=healthy, 503=degraded).

### 11.6 HTTP Endpoints

All served by `axum` on a separate HTTP port (default 9100), independent of the binary wire protocol port:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/metrics` | GET | Prometheus text format export (scraped every 15-30s) |
| `/health/live` | GET | Liveness check — 200 or 503 |
| `/health/ready` | GET | Readiness check — 200 or 503 |
| `/status` | GET | JSON: cluster health overview (see schema below) |
| `/debug/index` | GET | JSON: load factor, probe depth distribution, entry count, memory usage |
| `/debug/freelist` | GET | JSON: free blocks per size class, fragmentation stats |
| `/debug/redo` | GET | JSON: sequence, checkpoint, lag, log utilization |
| `/debug/log-level` | PUT | Change runtime log level (e.g., `{"level": "debug", "target": "teraslab::ops::spend"}`) |
| `/debug/records/{txid}` | GET | JSON: metadata dump for a specific record (diagnostic only) |

#### `/status` response schema

The `/status` endpoint provides a single-glance view of what the node is storing and how healthy it is:

```json
{
  "node": {
    "id": "node-1",
    "version": "0.1.0",
    "uptime_seconds": 86400,
    "state": "ready"
  },
  "records": {
    "total": 2500000000,
    "utxo_slots": 12000000000,
    "spent_slots": 9800000000,
    "pruned_slots": 150000000,
    "unspent_slots": 2040000000,
    "frozen_slots": 10000000,
    "external_records": 1200
  },
  "storage": {
    "devices": [
      {
        "path": "/dev/nvme0n1",
        "total_bytes": 3840000000000,
        "used_bytes": 2900000000000,
        "free_bytes": 940000000000,
        "utilization": 0.755,
        "freelist_fragmentation": 0.02
      }
    ],
    "total_bytes": 3840000000000,
    "used_bytes": 2900000000000,
    "utilization": 0.755
  },
  "memory": {
    "index_bytes": 107000000000,
    "secondary_index_bytes": 180000000,
    "freelist_bytes": 12000000,
    "external_cache_bytes": 52000000,
    "redo_buffer_bytes": 67108864,
    "total_managed_bytes": 107312108864
  },
  "index": {
    "entries": 2500000000,
    "load_factor": 0.75,
    "max_probe_depth": 12,
    "dah_entries": 3200000,
    "unmined_entries": 850000
  },
  "replication": {
    "factor": 2,
    "replicas": [
      {
        "node_id": "node-2",
        "lag_ops": 150,
        "lag_bytes": 10800,
        "state": "synced"
      }
    ]
  },
  "redo": {
    "sequence": 98500000000,
    "checkpoint": 98499990000,
    "utilization": 0.12
  },
  "cluster": {
    "nodes": 4,
    "shards_owned": 1024,
    "shards_total": 4096,
    "migrations_active": 0
  },
  "throughput": {
    "ops_per_sec": {
      "spend": 6200000,
      "set_mined": 1800000,
      "create": 500000,
      "get": 1500000,
      "total": 10000000
    },
    "bytes_per_sec": {
      "read": 3200000000,
      "write": 1100000000
    }
  }
}
```

This is designed for operators and dashboards. The `records` section shows what the node holds, `storage` shows capacity, `memory` shows RAM budget, `throughput` shows current rates (rolling 10-second window). A monitoring system can scrape `/status` for alerting on utilization thresholds, replication lag, or index pressure.

---

## 12. Testing Strategy

### 12.1 Deterministic Simulation Testing (FoundationDB-style)

Build a deterministic event-loop simulator that injects:

- **Power loss** at any point in write path:
  - Mid-redo log write
  - Between redo flush and data write
  - Mid-data write (partial 69-byte slot write)
  - After data write but before client ACK
- **Network partitions** between master and replica:
  - Total loss, packet reordering, duplication
  - Asymmetric partitions (A→B works, B→A doesn't)
- **NVMe I/O errors**:
  - EIO on specific blocks
  - Latency spikes (simulated slow device)
  - Partial write (torn write)

Run continuously in CI. Invariants checked after every simulated crash:
- No UTXO double-spent
- No lost spends (spend committed but not visible after recovery)
- Index consistent with on-disk records
- Redo log correctly replayed

### 12.2 Workload-Specific Benchmarks

| Benchmark | Description | Target |
|-----------|-------------|--------|
| `bench_spend_single` | Single UTXO spends/sec per device | > 1M |
| `bench_spend_multi` | Batch spends (varying batch sizes 1-1000) | > 3M |
| `bench_set_mined` | Mining state updates/sec | > 1M |
| `bench_create` | Record creation throughput | > 500K |
| `bench_mixed` | Realistic ratio: 60% spend, 20% setMined, 10% create, 10% read | > 10M total |
| `bench_large_tx` | Create + read for 1MB, 10MB, 100MB, 320MB transactions | < 5s create |
| `bench_index_lookup` | Point lookup throughput | > 10M/sec |
| `bench_crash_recovery` | Time to replay redo log (64MB) | < 2s |
| `bench_rebalance` | Time to migrate 1M records between nodes | < 30s |

### 12.3 Crash Recovery Testing

1. Run workload for N seconds
2. Kill process at random point (SIGKILL)
3. Restart, verify:
   - All committed operations are visible
   - No uncommitted operations are visible
   - Index is consistent
   - Redo log replayed correctly
4. Continue workload — no data corruption

### 12.4 Cluster Operation Testing

- Node join: verify shard migration completes without data loss
- Node leave: verify replica promotion and shard absorption
- Network partition: verify split-brain prevention, reconciliation after heal
- Rolling restart: verify zero-downtime upgrade path

### 12.5 Property-Based Testing (proptest)

- **Spend idempotency**: `spend(x) ; spend(x) == spend(x)` (same result)
- **Unspend reversal**: `spend(x) ; unspend(x)` → UTXO is unspent
- **Freeze/unfreeze**: `freeze(x) ; unfreeze(x)` → UTXO is unspent
- **SetMined idempotency**: `setMined(block_id) ; setMined(block_id)` → single entry
- **Counter consistency**: `spent_utxos` always equals count of slots with status=0x01
- **Index consistency**: Every index entry points to a valid record with matching txid

---

## 13. Go Client

### 13.1 Architecture

The Go client is split across two repositories:

- **`teraslab-client-go`** (standalone repo) — reusable Go client library for the TeraSlab wire protocol. Handles connection pooling, partition map caching, batch frame encoding/decoding, pipelining, and shard routing. No Teranode dependency — can be used by any Go application that needs to talk to a TeraSlab cluster.

- **`stores/utxo/teraslab/`** (in the Teranode repo) — Teranode-specific adapter that imports `teraslab-client-go` and implements the `stores/utxo.Store` interface. Wires the client into Teranode's batcher architecture, configuration system, and lifecycle management.

### 13.2 Backend selection

Teranode selects the UTXO store backend based on the URL scheme in configuration:
- `legacy://...` → existing legacy client (unchanged)
- `teraslab://host:port` → new TeraSlab adapter using `teraslab-client-go`

Both coexist as selectable backends. The legacy implementation is not modified or replaced.

### 13.3 `teraslab-client-go` design

- **Batch-first**: All operations use the batch wire protocol opcodes. Single-item calls are batch-of-1.
- **Connection pooling**: Per-node connection pool with configurable size. Long-lived TCP connections with pipelining.
- **Partition map**: Client fetches the shard table on connect (`OP_GET_PARTITION_MAP`), caches it, and routes requests directly to the correct master node. Refreshed automatically on `Redirect` responses.
- **Zero legacy dependencies**: Clean implementation — no legacy database Go SDK, no Lua, no expressions, no multi-record pagination.
- **Reusable**: The client exposes a clean Go API (`teraslab.Client`) that any application can use, independent of Teranode.

### 13.4 `stores/utxo/teraslab/` adapter

- Implements `stores/utxo.Store` interface (same as the legacy adapter)
- Preserves the batcher architecture (storeBatcher, spendBatcher, getBatcher, etc.), flushing accumulated items as batch frames via `teraslab-client-go`
- Translates between Teranode's internal types (`meta.Data`, `spend.SpendingData`, `fields.FieldName`) and the wire protocol types
- Handles Teranode-specific concerns: circuit breaker, external blob cache, pruner integration

### 13.5 No migration needed

TeraSlab is deployed as a fresh cluster. There is no data migration from the legacy system. Nodes in a new TeraSlab cluster start empty and receive data through normal Teranode operations (create, spend, setMined, etc.). The legacy cluster continues operating independently for nodes configured to use it.

---

## 14. Admin CLI

### 14.1 Overview

`teraslab-cli` is a command-line tool for operators to inspect, manage, and troubleshoot TeraSlab clusters. It connects to any node's HTTP observability port (default 9100) and/or the binary wire protocol port. All commands produce human-readable output by default and JSON with `--json` for scripting.

### 14.2 Commands

#### Cluster status

```
teraslab-cli status                     # cluster overview (same as /status JSON, formatted)
teraslab-cli status --node <addr>       # single node status
teraslab-cli nodes                      # list all nodes with state, shards owned, uptime
teraslab-cli shards                     # shard distribution table
```

#### Storage and capacity

```
teraslab-cli storage                    # per-device capacity, used, free, utilization
teraslab-cli storage --device 0         # single device detail
teraslab-cli memory                     # memory breakdown (index, cache, freelist, redo)
teraslab-cli records                    # record inventory (total, spent, pruned, unspent, frozen)
teraslab-cli records --external         # list records with EXTERNAL flag (large txs in blob store)
```

#### Index inspection

```
teraslab-cli index                      # index stats (entries, load factor, probe depth)
teraslab-cli index --secondary          # DAH + unmined secondary index stats
```

#### Record inspection

```
teraslab-cli record <txid>              # full metadata dump for a record
```

#### Replication

```
teraslab-cli replication                # replication status per replica (lag, state)
```

#### Redo log

```
teraslab-cli redo                       # redo log position, checkpoint, utilization
```

#### Cluster operations

```
teraslab-cli rebalance --dry-run        # show what migrations would happen
teraslab-cli rebalance                  # trigger rebalance
teraslab-cli drain <node-id>            # migrate all shards off a node (for decommission)
```

#### Log management

```
teraslab-cli log-level                  # show current log level
teraslab-cli log-level debug            # set global level
```

#### Benchmarking / smoke test

```
teraslab-cli bench spend --count 10000  # quick spend throughput test
teraslab-cli bench create --count 1000  # quick create throughput test
teraslab-cli healthcheck                # verify all nodes reachable and ready
```

### 14.3 Implementation

- Built as a separate binary (`teraslab-cli`) in the same Rust workspace
- Uses `clap` for argument parsing
- Communicates via HTTP (for `/status`, `/metrics`, `/debug/*` endpoints) and the binary protocol (for `record` lookups and bench commands)
- Output formatting: table format by default (using `comfy-table` or similar), `--json` for machine-readable output

---

## 15. Admin Web UI

### 15.1 Overview

A browser-based dashboard served directly by each TeraSlab node on the HTTP observability port (default 9100, same as `/metrics` and `/status`). Provides a visual overview of cluster health, storage capacity, throughput, and record inventory — designed for operators who want a quick visual check without CLI access.

### 15.2 Pages

#### Dashboard (home page: `/ui/`)

Single-page overview with live-updating panels:

- **Cluster map**: nodes with status indicators (green/yellow/red), shard count per node
- **Throughput gauge**: current ops/sec by type (spend, setMined, create, get) — rolling 10s window
- **Storage bar**: per-device capacity bar (used/free) with utilization percentage
- **Memory breakdown**: pie/bar chart of index, cache, freelist, redo buffer
- **Record inventory**: total records, UTXO slots (spent/unspent/pruned/frozen) as stacked bar
- **Replication status**: per-replica lag (ops and bytes) with health indicator
- **Alerts panel**: any conditions requiring attention (high utilization, replication lag, index pressure)

#### Nodes page (`/ui/nodes`)

- Table of all cluster nodes: ID, address, state, uptime, shards owned, throughput, storage utilization
- Click a node to see its detailed `/status` data
- Highlight nodes that are draining, joining, or unhealthy

#### Storage page (`/ui/storage`)

- Per-device detail: capacity, used, free, fragmentation, I/O rates
- Freelist breakdown by size class
- Historical utilization (if time-series data available from Prometheus)

#### Records page (`/ui/records`)

- Search by txid: shows full record metadata, UTXO slot states, block entries
- Bulk stats: records by state (mined/unmined), by size class, external records count

#### Replication page (`/ui/replication`)

- Per-replica: lag timeline, throughput, connection state
- Redo log: sequence, checkpoint, utilization gauge

#### Migrations page (`/ui/migrations`)

- Active migrations: source/target node, shard, progress (records migrated / total, bytes sent)
- Migration history: completed migrations with duration and record count

#### Config page (`/ui/config`)

- Current configuration values (read-only view)
- Runtime-adjustable settings (log level) with apply button

### 15.3 Implementation

- **Static SPA**: HTML + CSS + JavaScript, bundled into the Rust binary via `include_dir` or `rust-embed`
- **Data source**: polls the `/status`, `/metrics`, and `/debug/*` JSON endpoints via `fetch()` — no separate backend API needed
- **No external dependencies**: no Node.js, no npm, no build step for the frontend. Keep it simple — vanilla JS or a lightweight framework (Alpine.js, htmx, or Preact)
- **Served by axum**: the same HTTP server that serves `/metrics` also serves `/ui/*` — zero additional infrastructure
- **Auto-refresh**: dashboard polls every 2-5 seconds for live updates
- **Responsive**: works on desktop and tablet (operators may check from mobile)

### 15.4 Alert conditions shown in UI

| Condition | Threshold | Severity |
|-----------|-----------|----------|
| Device utilization > 85% | Configurable | Warning |
| Device utilization > 95% | Configurable | Critical |
| Index load factor > 0.85 | Fixed | Warning |
| Replication lag > 10,000 ops | Configurable | Warning |
| Replication lag > 100,000 ops | Configurable | Critical |
| Node unreachable | 3 missed heartbeats | Critical |
| Redo log utilization > 80% | Fixed | Warning |
| Freelist fragmentation > 30% | Configurable | Warning |
| Memory usage > 90% of available | Configurable | Warning |

These are displayed in the dashboard alerts panel and reflected in node status indicators.

---

## Appendix A: Field Cross-Reference

| Go Field (fields.go) | Lua Bin (teranode.lua) | C Bin (mod_teranode) | Rust Field | Region |
|----------------------|----------------------|---------------------|-----------|--------|
| `Tx` | (not a bin) | (not a bin) | reconstructed from inputs+outputs | C |
| `TxID` | `txID` | `txID` | `txid` | B |
| `Inputs` | `inputs` | `inputs` | `inputs` | C |
| `Outputs` | `outputs` | `outputs` | `outputs` | C |
| `External` | `external` | `external` | `flags & EXTERNAL` | B |
| `LockTime` | `locktime` | `locktime` | `locktime` | B |
| `Version` | `version` | `version` | `version` | B |
| `Fee` | `fee` | `fee` | `fee` | B |
| `SizeInBytes` | `sizeInBytes` | `sizeInBytes` | `size_in_bytes` | B |
| `ExtendedSize` | `extendedSize` | `extendedSize` | `extended_size` | B |
| `TxInpoints` | (not a Lua bin) | (not a C bin) | `tx_inpoints` | C |
| `IsCoinbase` | `isCoinbase` | `isCoinbase` | `flags & IS_COINBASE` | B |
| `Conflicting` | `conflicting` | `conflicting` | `flags & CONFLICTING` | B |
| `ConflictingChildren` | `conflictingCs` | — | (application-layer) | — |
| `Locked` | `locked` | `locked` | `flags & LOCKED` | B |
| `Creating` | `creating` | `creating` | **ELIMINATED** — no multi-record creation | — |
| `UtxoSpendableIn` | `utxoSpendableIn` | `utxoSpendableIn` | **ELIMINATED** — encoded as `spendable_height` in UtxoSlot `spending_data[0..4]` | A (slot) |
| `SpendingHeight` | `spendingHeight` | `spendingHeight` | `spending_height` | B |
| `Utxos` | `utxos` | `utxos` | `utxo_slots` | A |
| `TotalUtxos` | `totalUtxos` | — | **ELIMINATED** — derived from `utxo_count` in record header | Header |
| `RecordUtxos` | `recordUtxos` | `recordUtxos` | **ELIMINATED** — derived from `utxo_count` in record header | — |
| `SpentUtxos` | `spentUtxos` | `spentUtxos` | `spent_utxos` | B |
| `TotalExtraRecs` | `totalExtraRecs` | `totalExtraRecs` | **ELIMINATED** | — |
| `SpentExtraRecs` | `spentExtraRecs` | `spentExtraRecs` | **ELIMINATED** | — |
| `BlockIDs` | `blockIDs` | `blockIDs` | `block_entries[].block_id` | B |
| `BlockHeights` | `blockHeights` | `blockHeights` | `block_entries[].block_height` | B |
| `SubtreeIdxs` | `subtreeIdxs` | `subtreeIdxs` | `block_entries[].subtree_idx` | B |
| `Reassignments` | `reassignments` | `reassignments` | `reassignments` | D |
| `DeleteAtHeight` | `deleteAtHeight` | `deleteAtHeight` | `delete_at_height` | B |
| `CreatedAt` | `createdAt` | — | `created_at` | B |
| `UnminedSince` | `unminedSince` | `unminedSince` | `unmined_since` | B |
| `PreserveUntil` | `preserveUntil` | `preserveUntil` | `preserve_until` | B |
| `DeletedChildren` | `deletedChildren` | `deletedChildren` | **ELIMINATED** — replaced by `UtxoSlot.status = 0x02 (PRUNED)` | A |
| — | `lastSpentState` | `lastSpentState` | `flags & LAST_SPENT_ALL` | B |

## Appendix B: Configuration Defaults

From `settings/utxostore_settings.go` and `settings.conf`:

| Setting | Default | settings.conf Override | Description |
|---------|---------|----------------------|-------------|
| `UtxoBatchSize` | 128 | 128 (docker: 50, docker.m: 512) | **ELIMINATED** — no pagination in Rust |
| `BlockHeightRetention` | 288 | — | Blocks before spent record deletion |
| `UnminedTxRetention` | 144 | — | Blocks before parent preservation triggers |
| `ParentPreservationBlocks` | 1440 | — | Extended retention for parent txs |
| `StoreBatcherSize` | 100 | 2048 | Create batch size |
| `StoreBatcherDurationMillis` | 100 | 10 | Create batch window |
| `SpendBatcherSize` | 100 | 1024 | Spend batch size |
| `SpendBatcherDurationMillis` | 100 | 10 | Spend batch window |
| `SpendBatcherConcurrency` | 32 | 4 | Parallel spend workers |
| `SpendWaitTimeout` | 30s | — | End-to-end spend timeout |
| `SpendCircuitBreakerFailureCount` | 10 | — | Failures before circuit opens |
| `SpendCircuitBreakerCooldown` | 30s | — | Recovery cooldown |
| `SpendCircuitBreakerHalfOpenMax` | 4 | — | Test requests in half-open |
| `GetBatcherSize` | 1 | 4096 | Get batch size |
| `GetBatcherDurationMillis` | 10 | — | Get batch window |
| `MaxMinedRoutines` | 128 | 128 (docker.m: 8, teratestnet: 4) | Concurrent mined workers |
| `MaxMinedBatchSize` | 1024 | — | Txs per mined batch |
| `OutpointBatcherSize` | 100 | 4096 | Outpoint batch size |
| `OutpointBatcherDurationMillis` | 10 | 5 | Outpoint batch window |
| `DBTimeout` | 5s | — | Per-operation timeout |
| `UseExternalTxCache` | true | true | Cache external txs |
| `ExternalStoreConcurrency` | 16 | 16 (docker.m: 4) | Concurrent blob ops |
| `ReAssignedUtxoSpendableAfterBlocks` | 1000 | — | Reassignment cooldown |
| `LockedBatcherSize` | 1024 | — | Lock batch size |
| `LockedBatcherDurationMillis` | 5 | — | Lock batch window |
| `LongestChainBatcherSize` | 1024 | — | Reorg batch size |
| `LongestChainBatcherDurationMillis` | 5 | — | Reorg batch window |
| `IncrementBatcherSize` | 256 | — | **ELIMINATED** |
| `SetDAHBatcherSize` | 256 | — | DAH batch size |
| `SetDAHBatcherDurationMillis` | 10 | — | DAH batch window |

## Appendix C: Spending Data Format

From `stores/utxo/spend/spending_data.go` and Lua `spendingDataBytesToHex`:

```
Spending data: 36 bytes total
  Bytes 0-31:  Spending transaction ID (32 bytes, little-endian)
  Bytes 32-35: Spending input index (vin) (4 bytes, little-endian)

The Rust implementation uses 4-byte vin matching the Bitcoin protocol and the Go
SpendingData struct. This gives UtxoSlot = 32 (hash) + 1 (status) + 36 (spending_data) = 69 bytes.

Hex display format (from Lua):
  - TxID bytes reversed (32→1) for display
  - Vin bytes in order (33→36)

Frozen sentinel: all 36 bytes = 0xFF
```
