# Device cache spec — read cache (#1) and write-back buffer (#2)

## Motivation

TeraSlab uses `O_DIRECT` for the data device, which **bypasses the OS page
cache by design**. On the read-modify-write paths (`spend`, `set_mined`) every
op re-reads the record from the device. On a low-latency NVMe that read is
~10–100 µs and cheap; on slower/cloud/virtualized disks it dominates batch
latency (the load-test harness measured `spend` ≈ 20 ms per 256-item batch on
the macOS Docker virtual disk, with the server I/O-wait bound at ~0.75 of 8
cores).

Buffered-write stores hide this with an in-RAM cache: writes ack from a
streaming write buffer and flush to the device asynchronously, and reads of
recent records are served from RAM. This spec adds the equivalent to TeraSlab as
a single, optional layer, **configurable down to zero for maximum safety** (zero
= today's exact `O_DIRECT` behavior).

## Why a `BlockDevice` wrapper (the altitude)

All record I/O on the `O_DIRECT` path funnels through the `BlockDevice` trait
(`pread`/`pwrite`/`sync`); `DirectDevice::as_raw_ptr()` returns `None`, so the
engine never bypasses the trait with a raw `mmap` pointer for that device. A
`CachingDevice<inner: BlockDevice>` that itself implements `BlockDevice` and
returns `as_raw_ptr() = None` therefore intercepts **every** record read and
write with **no changes to the engine, ops, or recovery code**. The cache is
keyed by physical device offset (block-aligned), so it is record-format-agnostic
and needs no allocator-free invalidation: whatever bytes were last written to an
offset are what the cache holds (write-through patches the block; a freed +
reused offset is simply overwritten by the next `Create`'s `pwrite`).

```
Engine ── pread/pwrite/sync ──▶ CachingDevice ──▶ DirectDevice (O_DIRECT)
                                   │
                                   └─ sharded block cache (RAM)
```

## Durability — three separable knobs, only one trades safety

TeraSlab's contract (`recovery.rs` "Durability Contract (WAL-first)"): a mutation
is acked only after (1) validate, (2) **redo append + fsync**, (3) data-device
`pwrite` (NOT necessarily durable on return — the checkpoint issues a
data-device `sync()` barrier before compacting redo). The data write is already
lazy and WAL-covered.

| Knob | Behavior | Safety vs today |
|---|---|---|
| **#1 read cache (write-through)** | `pwrite` → inner device immediately **and** cache the block; `pread` served from cache on hit | **Identical.** Ack still gated by redo fsync; device still written in step 3. Pure read acceleration. |
| **#2 write-back buffer** | `pwrite` → cache block, mark dirty, **do not** touch inner; `sync()` flushes dirty then `inner.sync()` | **Identical**, because the checkpoint already calls the data-device `sync()` barrier before compacting redo (`recovery.rs:19`). A dirty block lost on crash is replayed from the fsynced redo entry. Only step 3 becomes lazier — which the WAL already covers. |
| ack-before-redo-fsync (an async-commit mode) | ack from RAM before redo is durable | **Weaker** — loses acked writes on crash. **Out of scope / not built.** "Max safety = 0" means redo-fsync-before-ack, which is the default. |

The single critical invariant for #2: the checkpoint's data-device barrier must
flush the cache before redo compaction. This is satisfied **for free** because
the barrier is `BlockDevice::sync()` (via `Engine::sync_all_store_devices`), and
`CachingDevice::sync()` flushes dirty blocks first. No checkpoint code changes
are required beyond confirming it routes through `sync()`.

## Config (`ServerConfig`, TOML `[cache]`)

```toml
[cache]
# Per-store data-device cache budget in bytes. 0 (default) = no cache:
# CachingDevice is not interposed, behavior is byte-for-byte today's O_DIRECT.
bytes = 0
# false (default) = #1 write-through (zero durability change).
# true = #2 write-back (data writes deferred to sync(); still WAL-safe).
writeback = false
```

- `bytes = 0` ⇒ the device is **not wrapped** at all (no overhead, no behavior
  change). This is the "maximum safety" setting and the default.
- `writeback = true` with `bytes = 0` is rejected at config validation (write-back
  needs a buffer to defer into).

## `CachingDevice` design

- `block_size = inner.alignment()` (4096 for `DirectDevice`). All `O_DIRECT`
  access is block-aligned; partial-block access is still handled correctly for
  non-aligned inners (read-modify the covering blocks).
- **Sharded** by block index (`N = min(64, 2·cores)` shards), each a
  `parking_lot::Mutex<Shard>`, so the cache does not serialize the engine's
  parallel read fan-out.
- Per-shard **true LRU** via a monotonic `last_used` tick (linear-scan eviction;
  shards are small, eviction is the cold path). Per-shard capacity =
  `bytes / block_size / shards` (≥ 1).
- `Block { data: Box<[u8]>, dirty: bool, last_used: u64 }`.
- `pread(buf, off)`: for each covering block — hit ⇒ copy out; miss ⇒
  `inner.pread` the block, insert, copy out. Bump `last_used`.
- `pwrite(buf, off)`: for each covering block — ensure loaded (write-back partial
  writes must preserve untouched bytes), patch the written sub-range, bump
  `last_used`. Write-through ⇒ also `inner.pwrite(buf, off)` (exact bytes).
  Write-back ⇒ mark `dirty`.
- Eviction: pick min `last_used` in the shard; if `dirty` (write-back), flush to
  inner first; then drop.
- `sync()`: write-back ⇒ flush every dirty block to inner, clear dirty; always
  `inner.sync()`. Write-through ⇒ just `inner.sync()`.
- `as_raw_ptr() = None`, `size()/alignment()` delegate to inner.

## Failure handling

- `pwrite` flush errors (write-back) surface from `sync()` as the inner error,
  so the checkpoint barrier fails loud (the node fences) exactly as a raw
  data-device sync failure would today.
- A read miss that fails `inner.pread` returns the inner error unchanged.

## Integration points

1. `src/cache.rs` — new `CachingDevice` module (this spec), unit-tested in
   isolation against `MemoryDevice`/`CountingSyncDevice`.
2. `src/config.rs` — `CacheConfig { bytes, writeback }` under `ServerConfig`;
   validation rejects `writeback && bytes == 0`.
3. `src/bin/server.rs` — when `cache.bytes > 0`, wrap each opened store device in
   `CachingDevice` before handing it to the engine/allocator/recovery. Nothing
   else changes: the engine uses it as a `BlockDevice`; `sync_all_store_devices`
   already calls `sync()`; the checkpoint barrier already calls `sync()`.

## Test plan (TDD)

Unit (`src/cache.rs`):
- read-through: miss reads inner once, second read is a hit (inner read count
  unchanged) and returns identical bytes.
- write-through: `pwrite` reaches inner immediately; a subsequent `pread`
  (after dropping/clearing) returns the written bytes; cache stays coherent.
- write-back: `pwrite` does **not** reach inner; `sync()` flushes it; a reader
  on the inner sees the bytes only after `sync()`.
- write-back crash sim: writes without `sync()`, then read inner directly →
  old bytes (proving deferral); the WAL (engine-level test) replays.
- eviction: capacity-bounded; evicting a dirty block (write-back) flushes it.
- coherence: overwrite an offset, read returns the latest bytes (hit path).
- partial-block write preserves untouched bytes in the block.
- `bytes = 0` path (server wiring): device is the raw inner (covered by config
  test).

Engine/integration:
- a spend/set_mined batch through an engine backed by `CachingDevice`
  (write-through and write-back) yields identical results to the raw device.
- write-back crash test: mutate, drop the cache without `sync()` (simulating
  crash), recover from the redo log, assert the record is correct — proving the
  WAL covers the deferred data write.
