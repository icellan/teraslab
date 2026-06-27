# Packed Record Storage — Design

Status: PROPOSED (not yet implemented). Author note: this is the "lever 5" design
for sustained write throughput. It keeps TeraSlab's in-place update model — it is
**not** a log-structured rewrite.

## 1. Problem: 4 KB-per-record write amplification

Every record is reserved with its size rounded **up to the device I/O alignment**
(`SlotAllocator::align_up` → `device_alignment`, typically 4096). A record is small:

```
METADATA_SIZE      = 320 B   (270 B raw, padded to a 64-B boundary)
UTXO_SLOT stride   = 73 B
record(N utxos)    = 320 + N*73 B
```

Measured (typical UTXO transactions):

| utxos | record bytes | reserved (4 K-padded) | amplification | records that fit in one 4 K block |
|------:|-------------:|----------------------:|--------------:|----------------------------------:|
| 1 | 393 | 4096 | 10.4× | 10 |
| 2 | 466 | 4096 |  8.8× |  8 |
| 3 | 539 | 4096 |  7.6× |  7 |
| 4 | 612 | 4096 |  6.7× |  6 |
| 5 | 685 | 4096 |  6.0× |  5 |
| 8 | 904 | 4096 |  4.5× |  4 |

A mixed UTXO workload (≈2–5 outputs, mean ≈3.5) writes ≈575 B of real data but
consumes a full 4096 B block per create — **≈7× write amplification on brand-new
bytes**. Creates manufacture new dirty blocks that cannot be served from cache, so
the **device write bandwidth saturates ≈7× sooner than the data warrants**.

This is the dominant ceiling on *sustained create throughput*. Measured on the
single-node test rig: sustained ≈33k ops/s (device-write-bandwidth bound), while a
packed layout on the same device should reach ≈150–250k.

Note what is **not** the problem: updates (spend / set_mined) are already lean —
`write_utxo_slot` patches only the 73-B slot via a read-modify-write of the slot's
block, and the read path (`read_record_identity_and_slot`) reads only the identity
header + target slot, not the whole record. Updates are cache-friendly (they reuse
an existing block). The amplification is purely on **new-record ingestion**.

## 2. Goal and prize

Pack ≈7 small records into each 4 KB block instead of one. This cuts create write
amplification from ≈7× to ≈1.05× (only the per-block tail is wasted), which —
since sustained creates are device-write-bandwidth bound — should raise sustained
create throughput **≈7×** on the same hardware, while **preserving in-place
updates** (no append-log, no compaction GC).

## 3. Design overview

Three coordinated changes, smallest-blast-radius first:

1. **Allocator packs records** — stop rounding reservations up to the device block;
   pack records contiguously, ≤ one 4 KB block apart, never splitting a record
   across a block boundary.
2. **`io_locks` serialize by BLOCK, not by record** — because packed records share
   a 4 KB block, an in-place RMW of one record touches a block holding others.
3. **The write-back cache is the sub-block engine** — record reads/writes become
   sub-range operations on cached 4 KB blocks (patch-in-RAM, no per-update device
   RMW); the background writeback (already built) flushes full blocks, each now
   carrying several records' worth of changes.

The on-device record format (metadata + slots) is **unchanged**. Only *where*
records are placed (packed vs 4 K-aligned) and *how* shared blocks are coordinated
change.

### 3.1 Packed, block-aware allocation

`SlotAllocator` already keeps a byte-precise freelist of `FreeRegion { offset, size }`
with coalescing — it is only the `align_up(size)` to the device block in
`reserve_aligned` that creates the padding. Changes:

- Reserve `align_up(size, RECORD_ALIGN)` where `RECORD_ALIGN` is small (8 B, for
  struct access), not the device block.
- **No record spans a 4 KB block.** `reserve_aligned`/`best_fit` must reject a
  candidate range that would straddle a `device_alignment` boundary and instead
  advance to the next block. Concretely: when bump-allocating at the high-water
  mark, if `offset % BLOCK + size > BLOCK`, round `offset` up to the next block
  first (wasting the ≤575 B block tail — ~1% vs today's ~600%). When reusing a
  freelist hole, only accept a hole that lies wholly within one block.
- The header freelist persistence (`MAX_PERSISTED_FREE_REGIONS`, CRC) is unchanged;
  it now stores byte-precise ranges, which it already supports.

Rationale for "no spanning": it guarantees every record's RMW touches **exactly one
4 KB block**, which makes the block-granular lock (below) a single acquire and keeps
the read/write paths single-block.

### 3.2 Block-granular `io_locks`

Today `io_locks()` is keyed by `record_offset`. Two packed records in the same block
have different offsets → no mutual exclusion → concurrent spends on neighbours would
each `pread` the block, patch their slot, and `pwrite` the block, losing one update.

Change: key the record write/read guards by **block index** = `record_offset /
device_alignment`. All readers and writers of any record in a block then serialize
on one stripe. With ≈7 records/block this is ≈7× coarser than today but still
per-block fine-grained, and it is the natural unit since a block is the atomic
device I/O unit. (The striped lock table is unchanged; only the key derivation
changes — audit every `io_locks().read(record_offset)` / `.write(record_offset)`
call site to switch to the block key, and the ABA/coherent-snapshot reasoning in
`read_record_identity_and_slots` carries over verbatim under the block key.)

### 3.3 Cache-coordinated sub-block in-place I/O

With the cache enabled (recommended default for this layout):

- **Write (create slot region / spend slot / set_mined fields):** load the covering
  block into the cache (RAM read; device read only on a cold miss), patch the
  record's byte range in the cached block under the block lock, mark dirty. No
  synchronous device RMW.
- **Read:** serve the record's byte range from the cached block (RAM); device read
  only on a cold miss.
- **Flush:** the background writeback thread (`cache.writeback_interval_ms`) and the
  checkpoint flush dirty 4 KB blocks. Because a block now holds ≈7 records, one
  flush persists ≈7 records' changes — *fewer, denser* device writes.

This is where the win is realized: creates write into shared cached blocks (one
device write per ≈7 creates), and updates patch in RAM. Durability is unchanged —
the redo WAL + checkpoint `sync()` remain the guarantee; the cache only defers when
dirty blocks reach the device. The existing `flush_all_dirty` safety (snapshot under
shard lock, `pwrite` outside it, clear dirty only if bytes unchanged) is reused as-is.

Without the cache (`cache.bytes = 0`), packing is still correct: each sub-block op
becomes a device RMW under the block lock. It is bandwidth-correct (≈1× amplification
on flush) but pays a device RMW per op; the cache is what removes that. Recommend
write-back cache on for packed deployments.

## 4. Correctness

- **Lost-update / torn-block:** prevented by the block-granular `io_locks` (§3.2);
  every RMW of a block is mutually exclusive, and with no record spanning, each op
  locks exactly one block.
- **Coherent reads / ABA:** `read_record_identity_and_slots` already takes one guard
  across the identity + slot reads; under the block key this still excludes the
  block's writers for the snapshot. The `tx_id` ownership recheck is unchanged.
- **Durability:** the redo log is the source of truth; recovery replays
  `AllocateRegion`/`Create`/slot ops to rebuild both the packed freelist and the
  records. The cache is durability-neutral (§3.3). Checkpoint ordering (snapshot →
  data+allocator sync → fence → reclaim) is unchanged.
- **Allocator recovery:** `replay_allocate`/`replay_free` operate on byte ranges
  already; they only need the same "no align_up to block" treatment so replay
  reconstructs packed offsets identically to the live path.

## 5. On-disk format & migration

Packed layout is **not** read-compatible with the current 4 K-padded layout (record
offsets differ). Gate it behind a format version:

- Bump the allocator header `HEADER_VERSION` (or add a layout flag) so `recover()`
  knows which layout a device holds and refuses to mix.
- New deployments: fresh device in packed mode.
- Existing deployments: a one-time offline migration tool (scan old records, rewrite
  packed) or simply require a fresh device + re-replication. Out of scope for the
  first cut; ship behind a config flag (`storage.packed = true`, default false)
  until the migration path exists.

## 6. Edge cases & risks

- **Large records (many utxos / external blobs):** a record larger than a block
  still spans multiple blocks. Either (a) keep the "no spanning within the
  sub-block-packed region" rule only for records ≤ block size and place oversized
  records in their own aligned run (a size class), or (b) allow spanning and lock
  the block *range*. Recommend size classes: small (< block) packed, large
  (≥ block) block-aligned as today. Simplest and keeps the hot path single-block.
- **Fragmentation:** deletes punch byte-precise holes; the existing coalescing
  freelist + best-fit reuse them, but a hole only fits a record of ≤ its size that
  also stays within one block. Long-running churn can fragment; mitigate with size
  classes (per-class free lists) and, if needed later, an opportunistic in-block
  compactor. Not required for the first cut (create-heavy ingestion bump-allocates).
- **Lock coarsening:** block-granular locks serialize ≈7 records. For workloads that
  hammer many records in one hot block this reduces parallelism vs per-record; in
  practice records in a block are unrelated txids, and the block is the device
  atomicity unit anyway. Acceptable; revisit only if profiling shows block-lock
  contention.
- **Cache pressure:** packed blocks are denser, so the same cache byte budget covers
  ≈7× more records — strictly better for the cache hit rate.

## 7. Phased implementation plan (TDD)

1. **Size classes + allocator packing.** Add `RECORD_ALIGN` + block-boundary-aware
   `reserve_aligned`/`best_fit` (no spanning for small class; large class as today).
   Unit-test: packed offsets, no straddle, freelist reuse within a block, replay
   parity. No behavior change for large records.
2. **Block-keyed `io_locks`.** Switch all record-guard keys to `offset /
   device_alignment`. Re-run the spend/create race tests (`tests/g2_*`) — they must
   still pass; add a test that two records sharing a block serialize their RMW.
3. **Cache-coordinated sub-block I/O.** Ensure all record reads/writes route through
   the cache when enabled (they already do via the `BlockDevice` trait); verify a
   create writes into a shared cached block and a single flush persists multiple
   records. Reuse the background writeback.
4. **Format version + config flag** (`storage.packed`, default false) + recover()
   guard. Migration tool deferred.
5. **Bench** (create-heavy, single node, write-back cache on): expect sustained
   creates ≈7× the 4 K-padded baseline; confirm updates unaffected and crash-safety
   tests green.

## 8. Expected outcome

Create write amplification ≈7× → ≈1×; sustained create throughput on the test rig
≈33k → ≈150–250k, device-write-bandwidth bound at the new (≈7× lower) bytes/op.
In-place updates retained; no log-structured store, no compaction GC, no change to
the on-device record format.
