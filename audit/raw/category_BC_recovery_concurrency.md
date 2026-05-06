# Category B/C Audit — Crash Recovery & Concurrency

This is a brutal audit of TeraSlab focused on crash recovery (Category B) and concurrency (Category C). All findings reference exact file paths and line numbers and were derived by reading the source, not the docs.

## Overview

The on-disk format is a flat record area (metadata + UTXO slots + cold data) addressed by `record_offset`, with a separate redo log used for WAL-first durability and for replication catch-up. Recovery replays entries after the last `Checkpoint` and is meant to be idempotent.

Several genuinely good design choices are present:
- `TxMetadata` carries a CRC32 over the 320-byte header, recomputed on every read; this catches torn writes, bit-rot, and partial-sector updates. (`src/record.rs:557-573`)
- The allocator now journals every `AllocateRegion` / `FreeRegion` BEFORE returning the offset to the caller, with rollback on flush failure. (`src/allocator.rs:455-564`)
- The hash-table resize path journals `HashtableResizeBegin` / `HashtableResizeCommit` and cleans up orphan tmp files on recovery. (`src/recovery.rs:240-282`)
- Compensation intents (gap #8) capture before-images for `unset-mined`, `reassign`, `prune`. (`src/server/dispatch.rs:1668-2003`, `src/recovery.rs:1050-1197`)
- Replication intent ranges are persisted to disk so the post-restart node can prove durability or compensate. (`src/replication/durable.rs:222-352`)

But several **CRITICAL** issues are not addressed:
1. There is no automatic redo-log checkpointing in production code paths. The "circular" redo log is in fact a bounded linear buffer that fills and bricks the master.
2. The hot read paths (`engine.read_metadata`, `engine.read_slot`, `engine.lookup_cached`) violate the documented stripe-lock-required safety contract on the direct-pointer reads in `src/io.rs`. This is data-race UB on every concurrent read.
3. UTXO slots have no checksum and a single slot is 69 bytes — torn writes on the slot region are undetectable except by the surrounding 4 KiB sector atomicity.
4. Concurrent unspend batches compute `new_spent_count` redo payloads from a snapshot read OUTSIDE the per-tx stripe lock — replays after a crash apply the wrong counter.
5. The metadata generation counter wraps at u32 max (`wrapping_add`) and recovery's idempotency check uses plain `<` — after wrap, replay incorrectly skips real ops.

What follows is the full enumeration of findings.

---

### BC-01: No automatic redo-log checkpointing — log fills and bricks the master (CRITICAL)
**Category:** B (Crash)
**Location:** `src/redo.rs:983-1268` (RedoLog impl); `src/server/startup.rs`, `src/bin/server.rs` — there is **no** caller of `RedoLog::checkpoint()`, `advance_checkpoint()`, or `reset()` outside `mod tests`.
**What:** `RedoLog::append()` at line 1066 simply checks `write_pos + buffer.len() + bytes.len() > log_size` and returns `RedoError::LogFull`. There is no wrap-around — the log grows linearly until full. A fault-injection point in tests exercises `LogFull` (line 1602) but production code never calls `checkpoint()` to advance the watermark.
   ```rust
   // src/redo.rs:1066
   if self.write_pos + self.buffer.len() as u64 + bytes.len() as u64 > self.log_size {
       return Err(RedoError::LogFull { ... });
   }
   ```
   `ripgrep` of `\.checkpoint\(\)|advance_checkpoint` returns hits only in `redo.rs::tests`, `index/redb_unmined.rs::tests`, and `hashtable.rs::tests`. There is no production caller.
**Why it matters:** Default redo log size is 64 MiB (`src/config.rs:418`). Each Spend entry is ~85 bytes; SecondaryDahUpdate is ~50 bytes; CreateV2 carries the entire record bytes (often hundreds to thousands of bytes per record). At 10M ops/sec the log fills in well under a second. After fill, `write_redo_ops` returns `Err`, the dispatch handler returns `ERR_INTERNAL` to the client (`src/server/dispatch.rs:2466,2645,2763,...`), and the master cannot accept any further mutations until restart. There is no graceful checkpoint path.
**Reproduction:** Start a default-config single-node deployment, drive any mutation traffic until `redo_log_size` bytes have been appended. Subsequent mutations fail with `redo log full` and the server never recovers.
**Suggested fix:** Implement an automatic checkpoint cadence: when `write_pos / log_size > 0.5`, take a primary index snapshot + persist allocator + persist DAH/unmined + then `checkpoint()` + `reset()` the log. Until then, every mutation operation eventually halts.

### BC-02: Hot read paths violate `read_metadata_direct` safety contract — data-race UB (CRITICAL)
**Category:** C (Concurrency)
**Location:** `src/io.rs:206,224,241,261` document "Caller must hold the per-transaction stripe lock". `src/ops/engine.rs:673-2814` — `lookup`, `read_metadata`, `read_slot`, `lookup_cached` do NOT take the stripe lock.
**What:** Public reader API on `Engine`:
   ```rust
   // src/ops/engine.rs:673
   pub fn lookup(&self, key: &TxKey) -> Option<TxIndexEntry> {
       self.index.read().lookup(key)              // index read lock only
   }
   // src/ops/engine.rs:2784
   pub fn read_metadata(&self, key: &TxKey) -> Result<TxMetadata, SpendError> {
       let entry = self.index.read().lookup(key).ok_or(SpendError::TxNotFound)?;
       self.read_metadata_fast(entry.record_offset)   // direct pointer; no stripe lock
   }
   pub fn read_slot(&self, key: &TxKey, offset: u32) -> Result<UtxoSlot, SpendError> {
       let entry = self.index.read().lookup(key).ok_or(SpendError::TxNotFound)?;
       self.read_slot_fast(entry.record_offset, offset)  // ditto
   }
   ```
   These descend to `unsafe { io::read_metadata_direct(self.device_ptr, ..) }` (`src/ops/engine.rs:550`) whose safety doc at `src/io.rs:206` says **"Caller must hold the per-transaction stripe lock."** They do not.

   The dispatch GET handler calls these in `src/server/dispatch.rs:4377` and the loop calling `engine.read_slot(&key, v)` in 4456 — every client GET request races every concurrent Spend / Unspend / SetMined writer.
**Why it matters:** `write_metadata_direct` (`src/io.rs:226`) does a 320-byte `copy_from_slice` into a raw mmap region. A concurrent reader is guaranteed by the docs to be racing the writer; this is data-race UB in Rust. The CRC32 mitigates the **observable** corruption (a torn read returns `RecordCorruption` instead of stale data), so the practical exposure is "GET reads sometimes return `ERR_INTERNAL` under load" — not a silent bug — but the contract is technically violated and `cargo miri` would flag it. The targeted-write helpers (`write_spend_footer_direct`, `write_mined_footer_direct`, etc.) write the CRC32 LAST (after the field they mutated), so a reader can briefly observe new field bytes with stale CRC; the CRC check fails and the error surfaces. This is acceptable correctness but is not what the docs claim.
**Reproduction:** Run a concurrent workload (one writer thread doing spends on tx K, one reader thread doing GET on tx K). Under load, `engine.read_metadata` will sometimes return `SpendError::StorageError("CRC mismatch...")` rather than the post-spend or pre-spend state. There is no test that asserts read-while-write returns one of the two valid states without ever returning a corruption error.
**Suggested fix:** Either (a) take the stripe read-lock in `read_metadata` / `read_slot` / `lookup_cached`, or (b) document on the engine reader API that "torn reads can return RecordCorruption; clients must retry," and explicitly remove the unsafe contract from `io.rs` docs to match reality.

### BC-03: UTXO slots have no checksum — torn writes are undetectable (HIGH)
**Category:** B (Crash)
**Location:** `src/record.rs:96-118` — `UtxoSlot` is 69 bytes (`UTXO_SLOT_SIZE = 69`) with `hash:32`, `status:1`, `spending_data:36`. There is no CRC32 or generation field.
**What:** Slot writes go through `write_utxo_slot` (`src/io.rs:360-382`) which RMWs the surrounding 4 KiB block and pwrites it. Direct-pointer writes (`write_utxo_slot_direct`, `src/io.rs:263`) do a 69-byte memcpy to an mmap region. A torn write to a slot — whether at the device level (the slot straddles two 4 KiB sectors and only the first is flushed) or at the in-memory level (concurrent reader sees half-written bytes) — produces a slot whose `hash`, `status`, and `spending_data` are not internally consistent.
**Why it matters:** A spend that ends up half-applied (`status==SPENT` with stale `spending_data` zeroes, or `status==UNSPENT` with new `spending_data`) is silently accepted by `read_utxo_slot`. The metadata CRC catches metadata torn writes (good), but the slot is the actual UTXO state. Recovery's `replay_spend` reads the slot, checks `status == UTXO_SPENT && spending_data == *spending_data` to skip — if a torn write got the spending_data right but the status byte didn't flush, recovery re-spends and over-bumps `new_spent_count`. If both fields half-flushed, the slot sits in an inconsistent state that's not visible until the next read.
**Reproduction:** Inject a fault between the write of `slot.status` and `slot.spending_data` (no current fault-injection point exists for this), or write at a slot offset that straddles two 512-byte sectors on a real SSD where 4 KiB writes are not atomic at sector granularity. Recovery does not detect.
**Suggested fix:** Add a 4-byte CRC (or a small generation counter) to each `UtxoSlot`. Bump `UTXO_SLOT_SIZE` to 73 (or 72 with status moved into a flags byte). On read, verify; on torn-detection, fall back to redo replay.

### BC-04: Concurrent unspend/freeze/etc. batches compute redo payloads OUTSIDE the per-tx stripe lock (CRITICAL)
**Category:** C (Concurrency) / B (Crash recovery correctness)
**Location:** `src/server/dispatch.rs:2570-2713` (`handle_unspend_batch`), 2719-2906 (`handle_set_mined_batch`), 3322-3412 (`handle_freeze_batch`), 3416-3506 (`handle_unfreeze_batch`), 3510-3625 (`handle_reassign_batch`), 3637-3735 (`handle_set_conflicting_batch`), 3737-3826 (`handle_set_locked_batch`), 3828-3922 (`handle_preserve_until_batch`).
**What:** The comment at `src/server/dispatch.rs:2561-2569` calls this out explicitly:
   > NOTE ON WAL ORDERING: Unlike `handle_spend_batch` which holds the per-txid lock across redo write + engine mutation (because spend uses validate-then-apply), the handlers below (unspend, set_mined, freeze, etc.) write redo ops BEFORE acquiring the engine lock. This is safe because ALL redo operations in these paths are idempotent — replaying a redo entry that was already applied is a no-op due to generation guards and slot-state checks. If a non-idempotent redo op is ever added to these paths, this pattern must be restructured to match the spend path's WAL-first-under-lock discipline.

   But in `handle_unspend_batch` (line 2620-2640), the dispatcher reads `engine.lookup(&key)` — a snapshot WITHOUT the stripe lock — then computes:
   ```rust
   // dispatch.rs:2622-2633
   let entry = engine.lookup(&key);
   let pre_generation = entry.as_ref().map(|e| e.generation).unwrap_or(0);
   let pre_spent = entry.as_ref().map(|e| e.spent_utxos).unwrap_or(0);
   let counter = running_spent.entry(key).or_insert(pre_spent);
   *counter = counter.saturating_sub(1);
   redo_ops.push(RedoOp::Unspend {
       tx_key: key,
       offset: item.vout,
       new_spent_count: *counter,
   });
   ```
   This snapshot is then flushed to the WAL at line 2643. Phase 3 (line 2652-2680) acquires the stripe lock per-key inside `engine.unspend()`.
**Why it matters:** Two concurrent unspend batches against the same txid reading `pre_spent=5`:
   - Both compute redo entries with `new_spent_count = 4`.
   - Both fsync, in some order. Two redo entries say "after this op, spent_utxos = 4".
   - Apply order in the engine: A.unspend() takes lock, sees on-device spent=5 → writes 4; B.unspend() takes lock, sees on-device spent=4 → writes 3 (because `metadata.spent_utxos -= 1` in `engine.unspend`, line 1130-1133).
   - On-device value ends at 3, redo log says 4 in both entries.

   Now crash. Recovery replays both entries:
   ```rust
   // src/recovery.rs:586-588
   if let Ok(mut meta) = io::read_metadata(device, ie.record_offset) {
       meta.spent_utxos = new_spent_count;     // overwrites with 4
       let _ = io::write_metadata(device, ie.record_offset, &meta);
   }
   ```
   Recovery sets `spent_utxos=4` (since the redo entry says 4) — but the actual on-device slot states had 2 of 5 unspent (counter should be 3). Counter is off by one. The same mechanism applies to `replay_spend` (`src/recovery.rs:553`).

   The problem is that the redo's `new_spent_count` was captured pre-lock and is no longer valid by the time the second engine.unspend runs.
**Reproduction:** Spawn two concurrent unspend batches each containing one offset against the same txid (offsets 0 and 1, both currently SPENT). Inject a crash via `BeforeDataPwrite` after both redo flushes. Recovery replays both entries. On-device `spent_utxos` after replay does not match the count of SPENT-status slots.
**Suggested fix:** Move the `engine.lookup(&key)` + `running_spent` computation INSIDE the per-key stripe lock (i.e. follow the spend handler's `validate_then_apply` discipline). Or have the redo entry carry `delta=-1` and have `replay_unspend` compute `meta.spent_utxos = meta.spent_utxos.saturating_sub(1)` from on-device state; ensure idempotency by checking the slot status BEFORE applying the delta (which the current code already does — but it doesn't fix the counter).

### BC-05: Generation counter wraps; recovery's `>=` check breaks after wrap (HIGH)
**Category:** B (Crash)
**Location:** `src/ops/engine.rs:1007,1049,1150,1271,1478,1561,2262,2355,2446,2505,2569,2631,2664,2931` — every mutation does `metadata.generation = { metadata.generation }.wrapping_add(1)`. Recovery at `src/recovery.rs:1022-1041`:
   ```rust
   let target_generation = *generation;
   let current_generation = { meta.generation };
   ...
   } else if current_generation >= target_generation {
       return ReplayResult::Skipped;
   }
   ```
**What:** After a record's `generation` reaches `u32::MAX` and wraps to 0, a redo entry with `target_generation = 1` will be incorrectly classified: `current_generation = 0 (wrapped), target = 1` → 0 >= 1 is false → applies again. That's the lucky case. The unlucky case: the in-flight `MarkOnLongestChain` entry has `target_generation = u32::MAX`; on replay, `current = 0 (wrapped), target = u32::MAX` → 0 >= u32::MAX is false → re-applies. BUT a redo entry with `target = 5` against a record whose pre-wrap value was `u32::MAX - 2` and post-apply is `u32::MAX + 1 = 0`: replaying sees `current=0, target=5`, 0 >= 5 false, applies. After application the `meta.generation` becomes 5, which is BEHIND the actual generation history — subsequent replication operations now think the record is at generation 5 when in reality it's been mutated billions of times.

   The replication staleness gate at `src/replication/receiver.rs:721-731` does the same comparison: `if master_gen < local_gen { return Ok(()); }`. After wrap, `master_gen=0, local_gen=u32::MAX-1` → 0 < (large) → skip. The replica drops fresh writes after a wrap.
**Why it matters:** u32 generations wrap after 4.29B mutations on a SINGLE record. The doc at `src/recovery.rs:1016-1019` says:
   > Generation comparison uses plain `<`. Generations are monotonically assigned per record and only wrap after ~4B mutations on a single record — far beyond any redo-log retention window. A target of 0 means the dispatcher did not record a generation (legacy/unknown).

   This handwaves "4B mutations is enough" but a long-lived TX with frequent reorgs (think a regularly-touched account-style record in a hypothetical future use case) absolutely can hit this. Worse, the assumption "redo-log retention window" is wrong because the redo log is a master-process log; a single record's generation is independent of redo retention. Once the record-on-device has a wrapped generation, every replica's gate gets confused.
**Reproduction:** No test exercises wrap. Set `metadata.generation = u32::MAX - 2`, do 5 spends (or whatever wraps it), feed any redo entry through replay — the comparison is wrong.
**Suggested fix:** Either (a) widen `generation` to u64 (8 bytes; the metadata header has padding that could absorb it); or (b) explicit modular arithmetic ("is target within 2^31 ahead of current?"); or (c) a per-record sequence number that doesn't reset on restart.

### BC-06: `read_metadata_direct` reads the bytes WITHOUT memory ordering — torn-write detection relies on CRC alone (HIGH)
**Category:** C (Concurrency)
**Location:** `src/io.rs:208-234` (`read_metadata_direct`, `write_metadata_direct`); `src/io.rs:73-189` (targeted footer writes that are NOT atomic).
**What:** `write_metadata_direct` does:
   ```rust
   pub unsafe fn write_metadata_direct(base_ptr: *mut u8, record_offset: u64, metadata: &TxMetadata) {
       unsafe {
           let dst = base_ptr.add(record_offset as usize);
           let dst_slice = std::slice::from_raw_parts_mut(dst, METADATA_SIZE);
           let mut buf = [0u8; METADATA_SIZE];
           metadata.to_bytes(&mut buf);
           dst_slice.copy_from_slice(&buf);    // 320 bytes, not atomic
       }
   }
   ```
   `read_metadata_direct` does the inverse `from_raw_parts → from_bytes` which CRC-checks. This is correct under the documented contract (caller holds stripe lock) but the contract is violated as documented in BC-02. A concurrent reader will read intermediate bytes; CRC will mismatch and `from_bytes` returns `Err`. So the read returns an error — not stale data, not corrupted truth.

   But the **targeted** footer writes (`write_spend_footer_direct`, `write_mined_footer_direct`, `write_block_entry_direct` at `src/io.rs:73-168`) write 4 to 21 bytes WITHOUT updating the CRC. The caller is required to follow with `write_crc_direct` (`src/io.rs:183-189`). The window between the field write and the CRC restamp is when a concurrent reader sees: new bytes, old CRC. The reader's CRC check fails → returns `RecordCorruption`. That's the same outcome as BC-02. But the comment at `src/io.rs:101-105` says the caller MUST follow with `write_crc_direct` — this is correct, but doesn't help concurrent readers, only sequential.
**Why it matters:** Every targeted footer write opens a milliseconds-or-less window where reads return `RecordCorruption`. A high-rate workload that mixes writes with reads will see a steady stream of `ERR_INTERNAL` responses to GET requests. The dispatcher does not retry — it just returns the error. The client must retry. There's no metric on dispatch.rs that distinguishes "metadata torn read due to concurrent writer" from "actual on-disk corruption."
**Reproduction:** N concurrent writers, M concurrent readers, on the same set of txids. Observe error rate on reads.
**Suggested fix:** Either (a) take the stripe read lock in the read paths (likely too expensive for the stated 10M ops/sec budget); or (b) add explicit retry-on-CRC-mismatch to the read paths with backoff; or (c) seqlock-style: write generation BEFORE field bytes, write generation AFTER. Reader checks both match.

### BC-07: `write_metadata_direct` writes are NOT release-fenced — concurrent reader may observe stale CRC (HIGH)
**Category:** C (Concurrency)
**Location:** `src/io.rs:226-234`. There is no `std::sync::atomic::fence(Release)` after the copy.
**What:** Memcpy of bytes is NOT a synchronization operation in the Rust/C++ memory model. A reader on another core sees writes to the mmap region in arbitrary order subject only to the device's memory ordering (typically TSO on x86_64, weaker on ARM). The metadata struct's CRC is at the END of the 320 bytes (`CRC32_OFFSET = std::mem::offset_of!(TxMetadata, crc32)`). A reader that sees the new CRC bytes but stale field bytes would return a CRC-valid record with the wrong field values — silent data corruption.

   On x86_64, store-store ordering is TSO so the compile-time order of stores is generally preserved. But the source-level write is `dst_slice.copy_from_slice(&buf)` which is a `memcpy`-shaped intrinsic; the compiler is free to use SIMD that writes blocks in any order, and `memcpy` semantics only guarantee that all bytes are written, not in-order.
**Why it matters:** On ARM (which is the target according to README mentions of cloud/server deployments) without explicit barriers, a reader on a different core can see CRC bytes before field bytes. CRC matches the new value (because they're written together with the bytes as one packed struct), but field bytes are still old. Worst case: the reader returns the wrong UTXO record, undetected.

   Note: on the same CPU socket with SMP coherency, this is unlikely in practice. But it's not architecturally guaranteed.
**Reproduction:** Run on ARM (e.g. AWS Graviton) with high contention. Race detector won't catch it because it's UB-by-design. Need a custom stress test that compares `read_metadata_direct` output against a serialized log of writes.
**Suggested fix:** Use `std::sync::atomic::fence(Ordering::Release)` after the bytes copy and `Ordering::Acquire` before reading.

### BC-08: Connection-handler threads in receiver are spawned and not tracked (MEDIUM)
**Category:** C (Concurrency)
**Location:** `src/replication/receiver.rs:155-177`.
   ```rust
   std::thread::spawn(move || {
       while running.load(Ordering::Relaxed) {
           match listener.accept() {
               Ok((stream, peer_addr)) => {
                   ...
                   std::thread::spawn(move || {
                       handle_connection(...);
                   });
               }
               ...
           }
       }
   });
   ```
**What:** The outer thread is the listener loop; the inner thread is per-connection. Neither is joined or tracked. On `stop()`, `running` is set to false, but the listener wakes only on `accept()` returning, and connection threads run until the master peer closes the socket or the engine errors out.
**Why it matters:** During a hot graceful shutdown, in-flight replica batches may write partial data to the device after the dispatcher has dropped its references. There's no upper bound on the number of concurrent connection threads.
**Reproduction:** Spam connections at the receiver's port; observe `top` show a growing thread count.
**Suggested fix:** Use a thread pool with a bounded queue, or at minimum store `JoinHandle`s and drain them on `stop()`.

### BC-09: `append_conflicting_child` mutates parent metadata without writing a redo entry (HIGH)
**Category:** B (Crash)
**Location:** `src/ops/engine.rs:2275-2360`. Called from create paths at `src/ops/engine.rs:1742, 1913, 2529`.
**What:** The function:
   1. Takes the stripe lock for `parent_key`.
   2. Reads parent metadata.
   3. Frees the old children block via `self.allocator.lock().free()` (which DOES journal a `FreeRegion`).
   4. Allocates a new block via `self.allocator.lock().allocate()` (DOES journal `AllocateRegion`).
   5. pwrites the new block.
   6. Updates `meta.conflicting_children_count`, `meta.conflicting_children_offset`, generation, then `write_metadata_fast(ro, &meta)`.

   **NO redo entry is written for the parent's metadata mutation.** A crash after step 3-4 (alloc/free journaled) but before step 5/6 leaves the parent metadata referencing a region that has been freed and re-allocated — possibly to a different record. On recovery, the allocator is restored to a consistent state but the parent metadata still points to garbage (or to another record's data).
**Why it matters:** The recovery at `src/recovery.rs:849-869` explicitly says:
   > Conflicting-child link replay is intentionally NOT performed in this recovery path. Establishing the link requires writing a 32-byte block to the parent's record area + mutating the parent's metadata header (`conflicting_children_offset/count`), which goes through `Engine::append_conflicting_child` — a function that depends on the engine's allocator + lock striping infrastructure and is not available from the bare `recovery` entry point.
   > This is documented as a known limitation: the dispatch path already calls `engine.append_conflicting_child` with `let _ =`, treating it as best-effort and not consensus-critical.

   "Best-effort" + "not consensus-critical" hides that a crash mid-update can leave the parent record's metadata header in a broken state where `conflicting_children_offset` points to garbage. A subsequent `read_conflicting_children(parent_key)` (called from the GET handler at `src/server/dispatch.rs:4492`) reads garbage. The CRC32 on the parent metadata catches the case where the metadata write was torn (returns `RecordCorruption`), but it does NOT catch the case where the metadata write completed AFTER the allocator freed the old region but BEFORE the new block was filled — there's a window where the metadata says "32-byte block at offset X, count=5" but the bytes at offset X are uninitialized or were freed and reallocated to a different record.
**Reproduction:** Drive a `create_batch` with `conflicting=true` and a parent_txids list. Inject a crash via `kill -9` between the new-block pwrite (step 5) and the metadata write (step 6). Read children after recovery — returns garbage that may decode as valid 32-byte txids.
**Suggested fix:** Write a `RedoOp::AppendConflictingChild { parent_key, child_txid, prior_offset, prior_count }` BEFORE step 3. On replay, idempotently re-walk the parent's metadata + the new child txid. Or move the conflicting-children list inline into the parent metadata for small N (capped count).

### BC-10: `pre_allocate_create` allocates DEVICE space BEFORE the create's redo entry is written (HIGH)
**Category:** B (Crash)
**Location:** `src/server/dispatch.rs:3091-3193` (`handle_create_batch`).
**What:** The flow is:
   1. For every item, `engine.pre_allocate_create(&create_req)` — this internally calls `self.allocator.lock().allocate(total_size)` (`src/ops/engine.rs:1786-1790`), which fsyncs `RedoOp::AllocateRegion`.
   2. Build `record_bytes` and push `RedoOp::CreateV2 { record_offset, record_bytes, ... }` into `redo_ops`.
   3. After the loop, `write_redo_ops(redo_log, &redo_ops)` — fsyncs all CreateV2 entries in one batch.
   4. `engine.create_at_offset` for each — writes the actual record bytes.

   This means that for N items in the batch, there are N `AllocateRegion` fsyncs (in step 1, sequentially) PLUS one batched fsync for the CreateV2 entries (step 3). For a batch of 100 creates, that's 101 fsyncs.

   Worse, between the `AllocateRegion` fsyncs and the CreateV2 fsync, a crash leaves the allocator with N regions reserved but no record bytes written and no CreateV2 entry. Recovery restores the allocator state (regions reserved) but the records never appear in the index. The space is **leaked** until manual operator intervention.
**Why it matters:**
   - 101 fsyncs per batch destroys the throughput target.
   - A crash between step 1 and step 3 leaks device space proportional to batch size. Repeated until the device is full, the master rejects all creates with `DeviceFull`. There is no automatic reclaim.
**Reproduction:** Drive a sustained `OP_CREATE_BATCH` workload with batch_size=1000. Inject crashes (kill -9) repeatedly. Observe that `allocator.next_offset` advances at the same rate as create traffic, but `index.len()` falls increasingly behind. Eventually `DeviceFull`.
**Suggested fix:** Defer allocator reservation: write the CreateV2 redo entry FIRST (with `record_offset=0` placeholder), then take the allocator lock and allocate, then write the record bytes. Recovery replays the CreateV2 with placeholder, asks the allocator to allocate inside replay, writes bytes there, registers index. Or batch the allocator reservations into one fsync via `append_batch_and_flush`.

### BC-11: Redo log entries are not actually idempotent — replay_spend overwrites `spent_utxos` unconditionally (HIGH)
**Category:** B (Crash)
**Location:** `src/recovery.rs:541-555`, 580-590 (`replay_spend`, `replay_unspend`).
**What:**
   ```rust
   // src/recovery.rs:540-555
   if slot.status == UTXO_SPENT && slot.spending_data == *spending_data {
       return ReplayResult::Skipped;
   }
   // Apply: write spent slot
   let new_slot = UtxoSlot::new_spent(slot.hash, *spending_data);
   if io::write_utxo_slot(device, ie.record_offset, offset, &new_slot).is_err() {
       return ReplayResult::Failed(ReplayCause::IoError);
   }
   // Update metadata counter
   if let Ok(mut meta) = io::read_metadata(device, ie.record_offset) {
       meta.spent_utxos = new_spent_count;
       let _ = io::write_metadata(device, ie.record_offset, &meta);
   }
   ```
   The "idempotent" check is "slot already SPENT with same spending_data → skip". But if the slot has a different `spending_data` (e.g., torn-write left zeros), the function applies the new slot AND overwrites `meta.spent_utxos = new_spent_count`. Two redo entries for different slots both targeting the same tx would each set `meta.spent_utxos = X` based on the dispatcher's pre-flush snapshot, **overwriting** anything that's there.

   Worse still: see BC-04 above — concurrent dispatchers compute conflicting `new_spent_count` values and replay applies them in redo-log order. The final value is the last entry's value, which may not match the actual count of SPENT-status slots on device.

   The error path `let _ = io::write_metadata(...)` SILENTLY DROPS the error. If the metadata write fails, replay claims `Applied` but the on-device state has no metadata update.
**Why it matters:** This is a contract violation of the docstring at `src/recovery.rs:27-30`:
   > All replays are idempotent: each entry reads the current device or index state before writing and skips when the post-state already matches.

   This is technically true for the slot transition (UNSPENT→SPENT) but FALSE for the metadata counter update — the counter is overwritten unconditionally with whatever the redo entry says.
**Reproduction:** See BC-04. Two concurrent unspends write redo entries with `new_spent_count=4`. On-device counter ends at 3 (correct sequential application). Recovery overwrites with 4 → wrong.
**Suggested fix:** Replace the metadata counter update with a delta-based approach (`new_spent_count = max(meta.spent_utxos.saturating_sub(delta), ...)`) and tie the delta to the per-entry idempotency guard. Or take the per-tx lock during replay and re-derive `spent_utxos` from a slot scan.

### BC-12: `let _ = io::write_metadata(...)` silently drops fatal errors during replay (MEDIUM)
**Category:** B (Crash)
**Location:** `src/recovery.rs:554, 588, 657, 944, 968, 988, 1043` — every `replay_*` for metadata mutation has `let _ = io::write_metadata(...)`.
**What:** The pattern is:
   ```rust
   // src/recovery.rs:587-589
   if let Ok(mut meta) = io::read_metadata(device, ie.record_offset) {
       meta.spent_utxos = new_spent_count;
       let _ = io::write_metadata(device, ie.record_offset, &meta);
   }
   ```
   The write error is discarded. Replay proceeds to return `ReplayResult::Applied` even though the metadata write failed. The next-replay sees an unchanged on-device counter, considers the entry not-yet-applied, and re-applies it — endless loop on a permanently broken device.
**Why it matters:** A device error mid-replay should propagate as `ReplayCause::IoError` and abort startup. Instead it silently moves on. The recovery stats are wrong (`entries_replayed` is over-counted) and the on-device state is inconsistent.
**Reproduction:** Inject a write failure at `BeforeRedoFsync` after some replays — `let _ = ` swallows it.
**Suggested fix:** Replace every `let _ = io::write_metadata(...)` in the replay path with a propagated `ReplayResult::Failed(ReplayCause::IoError)` on error.

### BC-13: Redo log uses linear write_pos, never wraps — naming is misleading (HIGH)
**Category:** B (Crash)
**Location:** `src/redo.rs:983-1295`. The doc-comment at line 979-982 says "Circular redo log on a block device". But there is no wrap logic; `write_pos` is linear and `LogFull` is hard-failed.
**What:**
   ```rust
   // src/redo.rs:1066
   if self.write_pos + self.buffer.len() as u64 + bytes.len() as u64 > self.log_size {
       return Err(RedoError::LogFull { ... });
   }
   ```
   `reset()` (line 1258) zeroes the first block and sets `write_pos = 0` — but it's only called by tests. `advance_checkpoint(seq)` (line 1238) updates `checkpoint_seq` but does NOT reclaim space. There is no caller that combines `checkpoint() + reset()` automatically.

   The replication catch-up path expects to read entries from a sequence number going back many transactions. With a linear buffer, this works only if the buffer is sized to hold every entry since the master started. After `reset()` (test only), older sequences become unreachable — which is why `read_from_sequence` (line 1215) returns empty and the replica gets an "needs full resync" error.
**Why it matters:** This conflates two concerns:
   1. WAL durability (only need entries since last checkpoint).
   2. Replication catch-up (need entries since the slowest replica's last_acked).

   In a "circular" log, these two are reconciled by `advance_checkpoint(min(checkpoint_seq, slowest_replica_acked))`. Here, neither is implemented in production. After `log_size` bytes of writes, the master halts.
**Reproduction:** See BC-01. The naming is misleading; readers of the codebase will assume the log handles wrap-around.
**Suggested fix:** Either implement actual circular writes (wrap `write_pos` modulo `log_size` after a `checkpoint()`), or rename to `LinearRedoLog` to set expectations correctly.

### BC-14: `RedoLog::flush` swallows pre-write read failures during partial-block RMW (MEDIUM)
**Category:** B (Crash)
**Location:** `src/redo.rs:1098-1114`.
**What:**
   ```rust
   if intra > 0 || !total.is_multiple_of(align) {
       let read_len = aligned_total.min(...);
       let read_aligned = read_len.div_ceil(align) * align;
       if read_aligned <= buf.len() {
           // A pre-write read-modify-write read failure on the redo log
           // tail is not fatal here ... we swallow the error
           let _ = self.device.pread_exact_at(&mut buf[..read_aligned], aligned_offset);
       }
   }
   ```
   If the device read fails (e.g., transient I/O error), `buf[0..intra]` is uninitialized bytes (from `AlignedBuf::new`), which then get pwritten back over the existing redo log tail. This can corrupt prior valid entries that share the partially-aligned block.
**Why it matters:** The comment claims "any bytes the device returns are immediately overwritten by the new entry below" — but the bytes from offset 0 up to `intra` are NOT overwritten; they are the leading partial block that the new entry doesn't touch. If the read failed, those bytes are zeros (because `AlignedBuf::new` zero-initializes), which means the previous entry that ended at `aligned_offset + intra - 1` is now zero-valued from `aligned_offset` to `aligned_offset + intra - 1`.

   Concrete scenario: 4 KiB device alignment. Entry A is 100 bytes at offset 4000-4099. Entry B is 100 bytes at offset 4100-4199. Both are in the same 4 KiB block (4096-8191). Now a transient read failure on flush — buf is zeroed. The pwrite writes zeros to offset 4096-4099 (corrupting entry A) and the new entry to 4100-4199. Recovery reads entry A and sees a corrupted length field.
**Reproduction:** Inject a read failure at flush time. The next read of the redo log returns truncated data because entry A was overwritten with zeros.
**Suggested fix:** Treat pre-write RMW read failures as fatal; bubble up the `DeviceError`. Or use a write-allocator that always pads to alignment with explicit known bytes.

### BC-15: `RedoLog::scan_all` reads the ENTIRE log on every recover/read_from_sequence/earliest_sequence call (MEDIUM)
**Category:** B (Crash)
**Location:** `src/redo.rs:1271-1294`. Called from `recover()` (1192-1208), `read_from_sequence` (1215-1218), `earliest_sequence` (1232-1235), and the `RedoLog::open` constructor (1040).
**What:** `scan_all` reads `log_size` bytes from device into one giant `AlignedBuf`, then parses entries linearly. With default `log_size = 64 MiB` (`src/config.rs:418`), that's 64 MiB read for every catch-up streaming operation, every recovery, every `earliest_sequence` query.
**Why it matters:**
   - Recovery time is bounded by `O(log_size)` not `O(entries since checkpoint)`. With a 1 GiB log this is gigabytes per restart.
   - `read_from_sequence` is called from the catch-up runner. Every replica catch-up reads the entire log. With many replicas catching up after a partition heal, this multiplies fan-in I/O.
   - `earliest_sequence` is potentially called frequently from health/observability paths; it shouldn't read the whole log.
**Reproduction:** With 1 GiB redo log and 100 replicas catching up, 100 GiB of reads are issued.
**Suggested fix:** Cache the parsed entries in memory. On a write, append to the in-memory vector; only re-scan from device on `open()`. Index by sequence number for `read_from_sequence`. The 64 MiB log holds at most ~600k entries; a Vec of those is ~50 MiB heap.

### BC-16: `flushed_pos` written but never read (LOW)
**Category:** B (Crash)
**Location:** `src/redo.rs:991, 1035, 1142, 1264`.
**What:** `RedoLog::flushed_pos` is initialized to 0 (line 1035), set to `self.write_pos` after every successful flush (line 1142), reset to 0 on `reset()` (line 1264), but NEVER read by any code (`rg flushed_pos` returns only writes).
**Why it matters:** Dead state. Maintenance burden — readers will assume it's load-bearing. Could be removed.
**Suggested fix:** Delete the field, or document why it's kept (e.g., for future "pending flush" reporting).

### BC-17: `RedoLog::open` scans the entire log to find next_sequence and last checkpoint (MEDIUM)
**Category:** B (Crash)
**Location:** `src/redo.rs:1040-1051`.
**What:** On open:
   ```rust
   let entries = log.scan_all()?;
   if let Some(last) = entries.last() {
       log.next_sequence = last.sequence + 1;
   }
   for e in entries.iter().rev() {
       if e.op == RedoOp::Checkpoint {
           log.checkpoint_seq = e.sequence;
           break;
       }
   }
   ```
   This is `O(log_size)` device reads on every open. Combined with BC-15 (recovery also calls scan_all), startup does **two** full log scans.
**Why it matters:** Compounds BC-15. Startup time is `2 * O(log_size)` reads.
**Suggested fix:** Cache the `scan_all` result in `RedoLog` after the first open-time scan; share the result with recovery. Or persist `next_sequence` + `checkpoint_seq` in a separate small superblock that is written atomically.

### BC-18: Recovery does NOT validate that record_offset in CreateV2 is allocator-owned (MEDIUM)
**Category:** B (Crash)
**Location:** `src/recovery.rs:778-872` (`replay_create_v2`).
**What:** `replay_create_v2` writes the captured `record_bytes` to `record_offset` without checking that the allocator currently considers `record_offset` as allocated. If the allocator's freelist has been reset or the redo entry refers to an offset that was freed and reallocated to a different record, replay overwrites the new record's bytes.
**Why it matters:** In normal flow, the matching `RedoOp::AllocateRegion` precedes the `CreateV2` in the same log. Recovery applies them in order; the allocator state catches up first. But there's no cross-check between the two. If the allocator entry was lost (e.g., the AllocateRegion entry was written, then later the same offset was freed via `RedoOp::FreeRegion`, then a new `AllocateRegion` was written, then the original CreateV2 sequence is replayed in order) — the `replay_create_v2` overwrites the second record.

   The current code is implicitly safe because all redo entries are replayed in sequence order, but this is fragile. A future change that allows out-of-order replay (e.g., for parallelism) would silently corrupt records.
**Reproduction:** Hand-craft a redo log: AllocateRegion(off=0, sz=4096), CreateV2(off=0, ...), FreeRegion(off=0, sz=4096), AllocateRegion(off=0, sz=4096), CreateV2(off=0, ...). Replay should leave record from second CreateV2 — but if the first AllocateRegion is replayed second (due to a reorder), the first CreateV2 overwrites the second.
**Suggested fix:** During replay, mark each `record_offset` from a CreateV2 as "occupied by tx K"; before applying a later CreateV2 at the same offset, verify the offset was freed in the interim (via FreeRegion or via tombstoning).

### BC-19: SecondaryUnminedUpdate / SecondaryDahUpdate replay can succeed when the redb commit also failed previously (MEDIUM)
**Category:** B (Crash)
**Location:** `src/recovery.rs:321-385`.
**What:** Two-phase durability: redo entry fsynced BEFORE redb commit. On replay:
   ```rust
   // src/recovery.rs:330-338
   let primary_unmined = match index.lookup(tx_key) {
       Some(e) => e.unmined_since,
       None => return ReplayResult::Skipped,
   };
   if primary_unmined != new_height {
       return ReplayResult::Skipped;
   }
   let entry = UnminedRedoEntry { txid: tx_key.txid, old_height: _old_height, new_height };
   match unmined.replay_redo(&entry) { ... }
   ```
   The check requires the **primary** index's `unmined_since` to equal the redo's `new_height` — meaning the primary commit has already happened. But the primary commit (via `update_cached_fields`) is in a SEPARATE redo entry path; the only thing journaled here is the secondary intent.

   Actually re-reading: the `primary_unmined` here is the value cached in the primary backend's `TxIndexEntry`, which is updated on every mutation by `engine.sync_index_cache`. So this check confirms "primary cache reflects the post-mutation value". Good.

   But: if the in-memory primary cache was rebuilt from a snapshot taken BEFORE this mutation, and the redo log replay starts AFTER that mutation, the primary cache will not yet have been updated. The replay handler returns `Skipped` → secondary index never gets updated. After replay completes and the user issues a query, the secondary returns stale results.
**Why it matters:** The recovery flow `recover_all_with_allocator` doesn't update the primary cache from the redo entries — primary cache updates happen on engine apply. So the secondary replay handler's primary-cache check is wrong.
**Reproduction:** Snapshot primary at sequence 100. Apply mutations 100-200 (each with primary cache update). Crash. Restart from snapshot. Recovery reads redo entries 100-200; secondary replay handler sees primary cache at sequence 100 (snapshot value) — `primary_unmined != new_height` → skips. Secondary index now stale.
**Suggested fix:** Replay primary cache updates (CreateV2 already does this via `update_cached_fields`) BEFORE the secondary replay reads them. Or change the check to use the on-device metadata header instead of the primary cache (which is the authoritative source).

### BC-20: Stripe lock count is power-of-two but the byte selector wastes 16 bits (LOW)
**Category:** C (Concurrency)
**Location:** `src/locks.rs:36-40`.
**What:**
   ```rust
   pub fn stripe_index(&self, key: &TxKey) -> usize {
       let h = u16::from_le_bytes([key.txid[16], key.txid[17]]) as usize;
       h & self.mask
   }
   ```
   With default `lock_stripes = 65536` (`src/config.rs:422`), `mask = 65535`, so `h & mask` is just `h` (since h is u16). The default config has 65536 stripes and 16-bit selector — exactly fits.

   But if config sets `lock_stripes` to something larger (say, 131072 — power of two), `next_power_of_two().max(16)` rounds to 131072, mask=131071. The selector uses only 16 bits (`u16::from_le_bytes`) so the high bit of mask is never set → at most 65536 distinct stripe indices are reachable. Half the lock table is wasted.
**Why it matters:** Operator misconfiguration. The doc at `src/locks.rs:9-12` says "Default: 65536 stripes" but doesn't warn that scaling above 65536 is impossible.
**Reproduction:** Set `lock_stripes = 1048576` in config; verify via `stripe_index` that only 65536 distinct values are produced over a uniform set of TxKeys.
**Suggested fix:** Use bytes 16..24 (8 bytes, u64 selector) — provides 64 bits, plenty of headroom; or assert at construction time that `count <= 65536` and reject larger values.

### BC-21: `parking_lot::Mutex` held across `block_on` in dispatch — runtime-thread starvation risk (LOW)
**Category:** C (Concurrency)
**Location:** `src/server/dispatch.rs:1286-1314`.
**What:**
   ```rust
   let results: Vec<(SocketAddr, std::result::Result<(), String>)> =
       REPL_RUNTIME.block_on(async {
           let mut handles = Vec::with_capacity(by_addr.len());
           for (addr, ops) in by_addr {
               handles.push(tokio::task::spawn_blocking(move || { ... }));
           }
           ...
       });
   ```
   `block_on` is called from a synchronous fn `replicate_all_ops`. The caller is the dispatch handler, which is running on a thread (the listener thread). No locks are held across the `block_on` according to the surrounding code, so this is OK — but it's worth verifying that `dispatch::handle_request` doesn't hold any `parking_lot` mutex when this is reached. (`handle_spend_batch` etc. release the validated-spend's per-tx lock BEFORE calling `replicate_all_ops`.)
**Why it matters:** If a future change introduces a lock that wraps the entire batch (e.g., a per-shard mutex for bookkeeping), holding it across `block_on` would block the runtime thread when an internal task tries to acquire the same lock — potentially deadlocking. Defensive review needed.
**Reproduction:** Code review only; no current reproducer.
**Suggested fix:** Add a debug-mode assert that no Engine-owned mutex is held when `replicate_all_ops` is entered.

### BC-22: `engine.read_metadata`/`read_slot`/`lookup_cached` doc comment "for testing" — but used in production (MEDIUM)
**Category:** C (Concurrency)
**Location:** `src/ops/engine.rs:2783, 2807, 2802`.
   ```rust
   /// Read metadata for a transaction (for testing).
   pub fn read_metadata(&self, key: &TxKey) -> Result<TxMetadata, SpendError> { ... }
   ```
**What:** The doc-comment claims "for testing" but the production GET handler at `src/server/dispatch.rs:4377` calls `engine.read_metadata(&key)` for every GET request that needs uncached fields. Same for `read_slot` (line 4456) and `lookup_cached` (line 4326).
**Why it matters:** Reader inferring stability from "for testing" would not realize this is on the hot path. Combined with BC-02 (no stripe lock), this is an undocumented production path with concurrency requirements that the doc misrepresents.
**Reproduction:** N/A — documentation issue.
**Suggested fix:** Update the doc to describe the actual purpose and concurrency contract.

### BC-23: Recovery DOES run a device-scan rebuild path — corrected (NOT A FINDING)
**Category:** B (Crash)
**Location:** `src/server/startup.rs:226-282`. Restore-then-rebuild paths exist for redb, file-backed, and in-memory primary backends.
**What:** Initial reading suggested no device-scan rebuild existed. Re-reading `src/server/startup.rs`, the `load_primary_index_*` functions all attempt restore first, then fall back to `PrimaryBackend::rebuild_redb` / `rebuild_file_backed` / `rebuild` (in-memory). On rebuild failure the file is preserved (fail-closed) — gap #5 contract.
**Why it matters:** This finding is invalid; the rebuild path exists. The "rebuild walks the device looking for valid metadata" implementation should still be reviewed for correctness (in particular, does it look for `METADATA_MAGIC`? Does it skip torn records via CRC check? How does it handle records whose record_size has been tombstoned to 0 by `engine.delete`?) — but the path itself is present.
**Reproduction:** N/A (existence check satisfied).
**Suggested fix:** None for the existence question. A separate audit should verify that `PrimaryBackend::rebuild_*` correctly skips tombstoned records (recall `tombstone.magic = 0; tombstone.record_size = 0;` at `src/ops/engine.rs:2704-2706`).

### BC-24: Generation counter NOT bumped on `engine.unspend` no-op (MEDIUM)
**Category:** B (Crash)
**Location:** `src/ops/engine.rs:1113-1120` (engine unspend, UNSPENT case).
**What:**
   ```rust
   match slot.status {
       UTXO_UNSPENT => {
           // Already unspent — no-op, no counter change, no generation bump
           return Ok(UnspendResponse {
               signal: Signal::None,
               generation: { metadata.generation },
           });
       }
       ...
   }
   ```
   In the engine's spend (at line 1003-1022), the idempotent-respend case DOES bump the generation. In unspend, the idempotent-already-unspent case does NOT. Inconsistent.
**Why it matters:** The dispatcher uses `pre_generation == post_generation` as the test for "this was a noop" (`src/server/dispatch.rs:2661-2666`). For unspend, this works. But for replication and replay, the inconsistency means: a replica that received the spend (which bumped gen) and the unspend (which did NOT bump gen) could see a state where the master's gen counter advanced by 1 but the replica's by 0 — the replica ALWAYS runs ahead in this case.

   Looking at `apply_op` in `src/replication/receiver.rs:721-731`, the staleness gate is `if master_gen < local_gen { return Ok(()); }`. After a no-op unspend on the master (gen unchanged), if a real-spend then runs (master gen +=1), and the replica missed the no-op unspend but received the spend, then `master_gen = N+1`, `local_gen = N` → not skipped → spend applied. OK in this direction.

   But after an unspend op that the master skipped on the replica: master gen N, replica gen N (because no-op didn't bump). Then a non-no-op unspend on master bumps to N+1. Replica sees `master_gen=N+1, local_gen=N` → applies. The asymmetry doesn't break correctness here, but the contract "generation is bumped exactly once per mutation" is violated.
**Reproduction:** Verify by stepping through unspend twice on the same already-unspent slot. Generation does not increment.
**Suggested fix:** Decide whether unspend-noop bumps gen or not, document it, and make spend-noop match.

### BC-25: spend with idempotent re-spend writes metadata WITHOUT redo entry (MEDIUM)
**Category:** B (Crash)
**Location:** `src/ops/engine.rs:1003-1022` (engine spend, SPENT idempotent case).
**What:** When `slot.status == UTXO_SPENT && slot.spending_data == req.spending_data`:
   ```rust
   metadata.generation = { metadata.generation }.wrapping_add(1);
   metadata.updated_at = self.now_millis();
   if !self.device_ptr.is_null() {
       unsafe { io::write_metadata_direct(self.device_ptr, record_offset, &metadata) };
   } else if let Err(e) = self.write_metadata_fast(record_offset, &metadata) { ... }
   self.sync_index_cache(&req.tx_key, &metadata)?;
   ```
   Generation is bumped, metadata is rewritten — but no redo entry was written for this idempotent path. The dispatch handler's `handle_spend_batch` only writes a `RedoOp::Spend` when the validator marked the item as a real transition.
**Why it matters:** A crash between this metadata write and the next mutation loses the generation bump. On replay, no redo entry will replay the bump. Eventually the on-device generation is stale relative to the post-restart master's tracking. Replication staleness gates at the replica may then reject a fresh op as stale.

   Also: writing metadata without a corresponding redo entry violates WAL-first ordering. A torn metadata write here cannot be repaired by replay (no entry).
**Reproduction:** Issue an idempotent spend (same data twice) then crash. Re-read metadata; generation may be old or torn.
**Suggested fix:** Either (a) skip the metadata write entirely on the idempotent path (don't bump generation for idempotent re-spends — make it a true no-op), or (b) write a `RedoOp::IdempotentBump { tx_key, target_generation }` entry first.

### BC-26: HashTable resize is NOT crash-atomic for ANONYMOUS-mmap-backed tables (MEDIUM)
**Category:** B (Crash)
**Location:** `src/index/hashtable.rs:469-636` (resize doc), 1782-1900 (resize tests).
**What:** The `resize_log` and `HashtableResizeBegin/Commit` redo entries are ONLY emitted for FILE-backed tables (the file path is captured in the redo entry). Anonymous-mmap tables (the default for in-memory `PrimaryBackend::InMemory`) silently resize without a redo entry. A crash mid-resize on an anonymous table — well, "anonymous" → process death drops the entire mapping → no recovery needed. But the redb-backed and file-backed paths have the journaling.

   The dispatcher routes between InMemory / OnDisk / FileBacked at startup based on config (`src/index/backend.rs:19-28`). FileBacked is the variant where this matters.
**Why it matters:** Documented behavior is OK for the common case. But the comment at `src/index/hashtable.rs:631-634`:
   > Anonymous tables ignore the redo log. Without a redo log attached, the resize still fsyncs the tmp file + parent directory, but crash recovery cannot detect and clean up an orphaned tmp file left by a crash between steps (1) and the rename.
   describes a path that doesn't apply (anonymous tables have no tmp file). Misleading documentation, but no functional bug.
**Reproduction:** N/A.
**Suggested fix:** Update doc to remove the "without a redo log attached" wording — the file-backed path always attaches the redo log if one is configured.

### BC-27: Recovery tolerance ceiling is 65536 MissingPrimary failures — high but bounded (LOW)
**Category:** B (Crash)
**Location:** `src/server/startup.rs:143` (`MAX_TOLERATED_MISSING_PRIMARY = 65_536`); 193 (enforcement at startup).
**What:** The ceiling exists at 65536 (replacing the previous `MAX_TOLERATED_FAILURES = 32`). The contract is "tolerate up to 65536 missing-primary failures, fail closed beyond." All other ReplayCause values are zero-tolerated.

  Initial reading missed the constant; it IS enforced. The remaining concern is whether 65536 is the right number for production scale: with redo log retention spanning millions of ops, post-delete MissingPrimary failures could exceed the cap during catch-up. But this is a tuning question, not a correctness gap.
**Why it matters:** Calibration question only. The docstring at `src/server/startup.rs:139-143` notes 65536 was picked to match the historical "32 failures × 2048 batch size" envelope. Whether this scales to multi-second crash windows on 10M ops/sec workloads should be verified.
**Reproduction:** Build a redo log of >65536 ops on records that were deleted later, then run recovery — startup fails with `RecoveryToleranceExceeded`.
**Suggested fix:** Make the ceiling configurable. Or correlate Delete entries against MissingPrimary entries in a single pass and only count "MissingPrimary AND no matching Delete in this replay batch" as a real failure.

### BC-28: Replication intent ranges are persisted with `Mutex<BTreeSet<...>>` — std::sync::Mutex (LOW)
**Category:** C (Concurrency)
**Location:** `src/replication/durable.rs:11, 226, 235`.
**What:** Uses `std::sync::Mutex` (the synchronization primitive that is poison-aware). Throughout the file, the helpers do `inner.lock().unwrap_or_else(|e| e.into_inner())` (line 254, 273, 285, 428, 438, 453, 471) — handling poisoning by extracting inner. The wider codebase otherwise uses `parking_lot::Mutex` (no poison). Mixing the two is a code smell.
**Why it matters:** Best-practice issue, not a bug. The `unwrap_or_else(|e| e.into_inner())` pattern silently consumes panic-poisoned state, which can hide real bugs.
**Reproduction:** N/A.
**Suggested fix:** Standardize on `parking_lot::Mutex`.

### BC-29: Compensation replay only restores slot status — verified correct given prune semantics (NOT A FINDING)
**Category:** B (Crash)
**Location:** `src/recovery.rs:1171-1197` (`replay_compensate_prune`); confirmed against `src/replication/receiver.rs:1095-1112` (replica `apply_op` for `PruneSlot`) which only sets `pruned.status = UTXO_PRUNED`.
**What:** Prune only mutates the status byte; spending_data and hash are preserved verbatim. Compensation only needs to restore status. Verified.
**Why it matters:** No issue. Initially flagged as needing verification; verified.
**Suggested fix:** None. Doc at line 1162-1170 is accurate.

### BC-30: Hash table buckets are 64 bytes packed; concurrent reader can see torn bucket on writer's `set_entry` (HIGH)
**Category:** C (Concurrency)
**Location:** `src/index/hashtable.rs:121-196` (`Bucket`); `src/index/hashtable.rs:691-766` (insert, takes `&mut self`).
**What:** `Bucket` is `#[repr(C, packed)]` 64 bytes. `HashTable::insert` and `remove` take `&mut self` — Rust's borrow checker forbids concurrent mutation. But the hash table itself is wrapped in a `parking_lot::RwLock<PrimaryBackend>` in `Engine` (`src/ops/engine.rs:37`). Readers take `.read()`, writers take `.write()`. So concurrent reader/writer is impossible at the language level — every reader is serialized vs. every writer.
**Why it matters:** This means EVERY GET request takes the index read lock, contending with every CREATE / DELETE / shard-counts update that takes the write lock. With 10M ops/sec target and a single global RwLock, this is the bottleneck. Read-mostly workloads could benefit from finer-grained locking (e.g., a Robin Hood lookup that's lock-free via atomics + generation), but the current design makes every lookup contend.

   This is not a correctness bug — the RwLock prevents data races. It's a scalability concern.
**Reproduction:** Profile under load; observe `parking_lot::RwLock::write`/`read` contention.
**Suggested fix:** Replace the global RwLock with a per-bucket-stripe lock OR a lock-free hash table OR an epoch-based reclamation scheme. This is a major refactor.

### BC-31: HashTable `count` is `usize`; `insert` then `count += 1` not bounded (LOW)
**Category:** C (Concurrency)
**Location:** `src/index/hashtable.rs:735, 800`.
**What:** `count` is `usize`. On a 64-bit system this can hold any practical count. On 32-bit it wraps at ~4B. The hashtable's `Full` error fires before count overflow so this is academic.
**Why it matters:** Defensive — if `count` ever desynchronizes from actual occupancy (e.g., a bucket marked occupied but count not incremented due to a partial update), the `len()` returns the wrong value. There's no consistency check.
**Reproduction:** Construct a corrupted hashtable file; observe `len()` lies.
**Suggested fix:** Add a debug-mode invariant check that walks the table at a regular cadence and confirms `count == sum(is_occupied)`.

### BC-32: 16-bit dist counter in HashTable insert can overflow with > 65535 collisions (LOW)
**Category:** C (Concurrency)
**Location:** `src/index/hashtable.rs:725-765`.
**What:**
   ```rust
   let mut dist: u16 = 0;
   ...
   loop {
       ...
       idx = (idx + 1) & self.mask;
       dist += 1;       // unchecked +=
       if dist as usize >= self.capacity {
           return Err(HashTableError::Full { ... });
       }
   }
   ```
   `dist += 1` will panic in debug mode if dist exceeds u16::MAX (65535). In release mode it wraps. With a capacity over 65535, the table claims Full prematurely (because `dist as usize` wraps), or panics.
**Why it matters:** A capacity of 1M buckets with all-clustered keys could see displacement > 65535 → `dist += 1` overflows in debug, wraps in release. Robin Hood typically caps probe distance at ~max_probe ≈ log(N), so this is unlikely in practice (the Bucket's stored probe_distance is u8 with `MAX_STORED_PROBE = 254`), but the in-loop counter is wider.
**Reproduction:** Construct an adversarial workload that causes a long probe chain.
**Suggested fix:** Make `dist: usize` so it can hold up to capacity directly. Or assert `capacity <= 65536` (which the lock-stripe limit BC-20 implies anyway).

### BC-33: `engine.refresh_clock()` uses Relaxed; concurrent operations see stale millis (LOW)
**Category:** C (Concurrency)
**Location:** `src/ops/engine.rs:437-446`.
**What:**
   ```rust
   pub fn refresh_clock(&self) {
       self.cached_millis.store(sys_millis(), std::sync::atomic::Ordering::Relaxed);
   }
   fn now_millis(&self) -> u64 {
       self.cached_millis.load(std::sync::atomic::Ordering::Relaxed)
   }
   ```
   `Relaxed` means reads can return any value previously written by any thread, in any order. Two concurrent ops both reading `cached_millis` may see stale values. For `metadata.updated_at`, this means timestamps can be slightly out-of-order across concurrent ops.
**Why it matters:** `updated_at` is monotonic-ish but is not ordered across concurrent ops. If observability/audit relies on `updated_at` ordering, it's unreliable.
**Reproduction:** Concurrent ops; compare on-device updated_at to wall clock; small skew.
**Suggested fix:** Use `Ordering::SeqCst` or per-op `sys_millis()` calls. The current cache exists "to avoid clock_gettime per op" — fine, but document the staleness.

### BC-34: ReplicaAck-ed mutations on the replica skip writing the local redo log (HIGH)
**Category:** B (Crash)
**Location:** `src/replication/receiver.rs:713-...` (`apply_op`); receiver does not have a `redo_log` parameter — it just calls `engine.spend`, `engine.unspend`, etc.
**What:** When the replica receives a batch:
   1. Gate-check `cluster_key`.
   2. Apply each op via `engine.<op>(req)` — this writes to the replica's local device but does NOT append to the replica's local redo log.
   3. `applied.set(stream_key, through_sequence)` and `applied.flush()`.
**Why it matters:** The replica's local redo log is OUT OF SYNC with the device. If the replica is later promoted to master (failover), it has device data without the corresponding redo entries — so:
   - It cannot stream catch-up to a lagging replica (no entries to send).
   - On its own crash, recovery does NOT see the recently-applied ops in the local redo log; recovery is a no-op for those ops; on-device data IS present (good) but the index cache may be stale.

   Actually `engine.<op>` updates `sync_index_cache` (`src/ops/engine.rs:631-657`) which writes to the in-memory primary index. So the index cache is good. And the on-device metadata is good. So a replica's recovery from this state would just snapshot+load the index normally.

   But the catch-up path is broken: a freshly-promoted replica cannot stream to other lagging replicas because its redo log doesn't have the ops. The lagging replica's `from_seq` query gets an empty response, classified as "redo entries reclaimed; full resync required" (`src/replication/durable.rs:639`). Operationally, every failover triggers full resyncs of all surviving replicas.
**Reproduction:** Three-node cluster: M, R1, R2. R2 is slightly lagging. Take down M. R1 becomes master. R2 reconnects; R1 streams catch-up; fails because R1's redo doesn't have the ops.
**Suggested fix:** Have the receiver's `apply_op` ALSO write a local redo entry (via the engine's `redo_log_handle` if attached). This is a non-trivial change — the redo entry needs to capture the post-apply state, not the input op (which is what the master's dispatcher does).

### BC-35: Spend's idempotent re-spend metadata write is NOT covered by a redo entry — generation drifts on crash (MEDIUM)
**Category:** B (Crash)
**Location:** `src/ops/engine.rs:1003-1022` (engine.spend, idempotent SPENT case); `src/server/dispatch.rs:2431-2449` (dispatcher Spend redo entry construction).
**What:** The dispatcher's `handle_spend_batch` only emits `RedoOp::Spend` for items that the validator marked as a transition. The validator at `src/ops/engine.rs:881-883` skips items that are already SPENT with the SAME spending_data:
   ```rust
   UTXO_SPENT => {
       if slot.spending_data == item.spending_data {
           continue;   // valid_spends NOT pushed; no redo entry will be written
       }
       ...
   }
   ```
   Then `validated.apply()` (`src/ops/engine.rs:2899`) only writes slots from `valid_spends` (line 2920). The metadata generation IS bumped (line 2931) and written (line 2946-2950).

   Net effect: an idempotent spend bumps generation on the device but writes NO redo entry. A crash immediately after the metadata write loses the bump on recovery (no entry to replay; generation reverts to whatever was already on device, which may or may not be the bumped value depending on torn-write boundaries).

   Note: Spend's single-item path (`engine.spend`, line 949) handles the idempotent case at line 1003-1022 the same way — bumps generation, writes metadata, no redo entry.
**Why it matters:** Generation counter consistency between master and replicas relies on every observable bump being durable. An idempotent re-spend's bump is NOT durable. After replication of a sequence that includes idempotent ops, the master's on-device generation differs from what the replica computed (the replica may not have bumped at all, since the receiver applies the same idempotent path).

   The replication staleness gate at `src/replication/receiver.rs:725` does `master_gen < local_gen → skip stale`. After enough idempotent bumps that the master has generation N+5 and the replica has generation N (because the replica also short-circuits on idempotent), a fresh op (generation N+6) reaches the replica, gate sees `master=N+6, local=N` → applies. Fine in this direction.

   But: if the master crashes and recovery does NOT re-apply the bumps (no redo entry), master's generation rolls back to N. New op generation = N+1. Replica has N. Gate: `master=N+1, local=N` → applies. Fine.

   The actual concern is auditing: clients querying for "current generation" see different values on master vs. replica with no way to disambiguate. Operators debugging will get confusing telemetry.
**Reproduction:** Spend X; spend X again with same data (idempotent). Read generation from master (it's been bumped). Crash. Restart. Read generation — it's reverted.
**Suggested fix:** Either (a) make the idempotent path a true no-op (don't bump generation, don't write metadata), or (b) emit a redo entry for the idempotent case too. Option (a) is simpler and matches the comment "idempotent re-spend".

### BC-36: `pre_allocate_create` AllocateRegion fsync sequence is N fsyncs per batch (HIGH)
**Category:** B (Crash) — performance impact
**Location:** `src/server/dispatch.rs:3091` (loop), 3169 (single batched CreateV2 fsync); `src/ops/engine.rs:1786-1790` (per-item allocator fsync); `src/allocator.rs:455-564` (allocate journals every reservation).
**What:** Each `engine.pre_allocate_create()` call internally fsyncs an `AllocateRegion` redo entry (`src/allocator.rs:512` `log.append_and_flush(op)`). For an N-item create batch, the dispatcher does N AllocateRegion fsyncs (one per item, sequential), then ONE CreateV2 batched fsync. Total: N+1 fsyncs.

   On a 4 KiB-block NVMe device with ~10us fsync latency, N=100 items → 1ms of fsync time before any user data is written. At 10M ops/sec batch=1000 → 10ms of pure allocator-fsync overhead per second.
**Why it matters:**
   - Throughput target of 10M ops/sec is shredded by per-allocation fsync.
   - Crash window between allocator fsyncs and CreateV2 fsync widens proportionally to N — the longer the batch takes, the more allocator regions are reserved without matching CreateV2 entries (BC-10 leak window).
**Reproduction:** Profile fsync rate during a sustained `OP_CREATE_BATCH` workload with batch_size=1000. Observe one fsync per allocator reservation.
**Suggested fix:** Add an `allocate_batch` API on `SlotAllocator` that takes a slice of sizes, computes all offsets, then writes ONE `RedoOp::AllocateBatch { regions: Vec<(offset, size)> }` redo entry, and fsyncs once. Total fsync count drops from N+1 to 2.

### BC-37: handle_freeze_batch and handle_unfreeze_batch lookup is unlocked — same race as BC-04 (MEDIUM)
**Category:** C (Concurrency)
**Location:** `src/server/dispatch.rs:3322-3414` (handle_freeze_batch), 3416-3508 (handle_unfreeze_batch).
**What:** Unlike spend, freeze/unfreeze do NOT carry a `new_spent_count` payload, so the BC-04 race is less severe. But they DO compute redo entries (line 3349-3352) BEFORE the per-key lock is taken in `engine.freeze` (line 3366). The receiver's idempotency check at `src/replication/receiver.rs:843-851` is `if slot.status == FROZEN → skip`. So replay of a freeze entry against an already-frozen slot is a no-op. Good.

   Same for unfreeze: `if slot.status == UNSPENT → skip` (`src/recovery.rs:704-706`). OK.

   But: if two concurrent freeze batches both target offset 5 of the same tx, both write redo entries, both flush. Then both engine.freeze calls run sequentially (per-tx lock serializes). One bumps generation, one is a no-op (or fails AlreadyFrozen). On replay, the redo entries are independently idempotent. So this is OK by accident.

   The latent risk is that ANY future addition of mutated state to freeze (e.g., capturing a freeze timestamp into the slot) would break this — the redo entry's payload was computed pre-lock and is no longer the source of truth.
**Why it matters:** Currently safe but fragile. Future additions to the freeze op without revisiting the WAL-first pattern will introduce real bugs.
**Reproduction:** Hard to repro currently; defensive concern.
**Suggested fix:** Either follow the spend pattern (validate-under-lock, then redo, then apply), or document explicitly that all freeze-family ops are pure-state-transitions with no per-call payload that could race.

### BC-38: write_redo_ops uses parking_lot::Mutex; held across pwrite_all_at fsync (MEDIUM)
**Category:** C (Concurrency)
**Location:** `src/server/dispatch.rs:984-1005`.
**What:**
   ```rust
   fn write_redo_ops(redo_log: Option<&Mutex<RedoLog>>, ops: &[RedoOp]) -> Result<(u64, u64), String> {
       ...
       let mut log = redo.lock();
       let first_seq = log.current_sequence();
       let mut last_seq = first_seq;
       for op in ops {
           last_seq = log.append(op.clone()).map_err(|e| ...)?;
       }
       log.flush().map_err(|e| ...)?;     // pwrite + fsync inside the lock
       Ok((first_seq, last_seq))
   }
   ```
   `log.flush()` calls `device.pwrite_all_at(&buf, ...)` and `device.sync()` (`src/redo.rs:1117, 1127`). On a real `DirectDevice`, `sync()` is `fdatasync` which can take milliseconds. The redo log's parking_lot Mutex is HELD throughout.
**Why it matters:** The redo log is the single global serialization point for all dispatcher-side mutations. Every Spend batch, Unspend batch, Set_mined batch, Freeze batch, Create batch, etc., serializes here. A 5ms fdatasync at line 1127 means the maximum throughput of dispatch is 200 ops/sec NOT counting batching.

   Group commit would help: collect ops from multiple concurrent dispatchers into one fsync. The current code fsyncs per-handler; if 100 concurrent batches all want to write redo entries, they all queue on the same Mutex and each gets its own fsync.
**Reproduction:** Profile under N concurrent connections; observe the redo log mutex as the dominant contention point.
**Suggested fix:** Implement group commit: a separate flush thread that collects all pending appends and fsyncs them in one batch. Concurrent dispatchers wait on a condvar for their sequence range to be flushed. This is the standard WAL throughput technique.

### BC-39: Replication intent tracker writes to disk on every begin/commit — synchronous fsync per batch (MEDIUM)
**Category:** B (Crash) — performance
**Location:** `src/replication/durable.rs:255-262, 273-281, 289-297`. `write_durable_file` (line 41-52) does sync_all + fsync_parent_dir.
**What:** Every `begin_replication_intent` (called from `replicate_all_ops`, `src/server/dispatch.rs:1062-1071`) writes the entire pending set to disk and fsyncs. Every `commit_replication_intent` does the same. So per replication batch, there are TWO additional fsyncs (in addition to the redo log fsync).
**Why it matters:** Adds latency to every mutation that touches replication. The set is small (a BTreeSet of intent ranges, typically with 1-10 entries) so the data is fast to write but the fsync is the cost.
**Reproduction:** Profile replication latency under load; observe the intent file's fsync count.
**Suggested fix:** Coalesce intent updates: write only when the set changes by more than a threshold, or piggyback on the redo log's fsync (write the intent record into the same fsync pass). Or use a copy-on-write log for the intent set itself.

### BC-40: Allocator's `next_offset` advance is NOT capped by device size in the redo replay (LOW)
**Category:** B (Crash)
**Location:** `src/allocator.rs` — `replay_redo` for AllocateRegion entries.
**What:** When recovery replays `RedoOp::AllocateRegion { offset, size, ... }`, the allocator's `next_offset` is advanced if `offset + size > next_offset`. The replay path is at `src/allocator.rs` (verify with `rg "fn replay_redo"`). If a corrupted redo entry has `offset + size > device_size`, replay sets `next_offset` past the device end. Subsequent allocate calls compare `next_offset + aligned_size > device_size` and return `DeviceFull` — but `next_offset` is now permanently in an inconsistent state.
**Why it matters:** A corrupt redo entry could brick the allocator. The redo entry's CRC32 (in `RedoEntry::deserialize`) catches most corruption; but a rare-but-possible CRC collision or a bit-flip in the size field could land here.
**Reproduction:** Inject an `AllocateRegion { offset: device_size - 1, size: 1024 }` into the log; recover; observe allocator state.
**Suggested fix:** Bounds-check `offset + size <= device_size` in `replay_redo` and return an error if violated.

### BC-41: `crate::fault_injection::check` is a runtime check on every mutation hot path (LOW)
**Category:** C (Concurrency) — performance
**Location:** `src/redo.rs:1123, 1137`; `src/ops/engine.rs:2917, 2926`; `src/allocator.rs:554, 604`.
**What:** Every flush + every spend + every allocator op calls `crate::fault_injection::check(SyncPoint::*)`. In production builds without fault injection enabled, this is a function call that returns immediately, but it's still a non-zero cost (a function call, an atomic load, a possible branch miss).
**Why it matters:** At 10M ops/sec each call costs nanoseconds; multiply by sync points per op (typically 2-3); 60M function calls/sec. Likely negligible but should be benchmarked.
**Reproduction:** Profile the hot path with and without fault injection compiled in.
**Suggested fix:** Gate the fault injection points with `#[cfg(any(test, feature = "fault_injection"))]`; in production builds the calls become no-ops at compile time.

### BC-42: Recovery's `replay_set_mined` does not bump generation after applying the metadata change (MEDIUM)
**Category:** B (Crash)
**Location:** `src/recovery.rs:594-659` (`replay_set_mined`). At line 657 the metadata is written but `meta.generation` is NOT incremented.
**What:**
   ```rust
   // src/recovery.rs:594-659
   fn replay_set_mined(...) -> ReplayResult {
       ...
       let mut meta = match io::read_metadata(device, ie.record_offset) { ... };
       ...
       // mutates meta.block_entries_inline and meta.block_entry_count
       ...
       let _ = io::write_metadata(device, ie.record_offset, &meta);
       ReplayResult::Applied
   }
   ```
   The original `engine.set_mined_inner` bumps generation (`src/ops/engine.rs:1271, 1561, 2355` etc). On replay, the bump is NOT done — the on-device generation after replay is whatever it was on disk before the crash. A subsequent replication staleness check could mistakenly classify this entry as already-applied even though the block_entry was just added.

   Actually, since `engine.set_mined_inner` at line 1284 writes `meta.generation = generation` BEFORE the crash, the on-device value is already bumped (assuming the crash happened after the metadata pwrite). If the crash was BEFORE the pwrite, the redo entry replays and writes block_entries but the generation field on device is the PRE-bump value. So replay leaves on-device generation behind by 1.
**Why it matters:** Subsequent replication of a real op (master at gen N+2, replica at gen N — replay was supposed to bring it to N+1 but didn't) would still be replicated, applied to bring replica to N+2. Net effect: replication catches up despite the gap. So functionally correct.

   But if the same record then gets a `MarkOnLongestChain` redo entry whose `target_generation = N+2`, the replay's idempotency check `current >= target` → `N >= N+2` false → re-applies. Fine. The issue is operator confusion: post-recovery, on-device generation is OFF by one for any record whose set_mined entry was replayed.
**Reproduction:** Crash between the metadata pwrite and the next mutation. Compare on-device generation against in-memory expectation.
**Suggested fix:** Have `replay_set_mined` bump generation as part of its mutation, matching `engine.set_mined_inner`.

### BC-43: `engine.set_mined_batch` does NOT acquire all locks at once — multi-tx batches have no atomicity (LOW)
**Category:** C (Concurrency)
**Location:** `src/ops/engine.rs:1521-1529`.
**What:**
   ```rust
   pub fn set_mined_batch(&self, params: &SetMinedSharedParams, keys: &[TxKey]) -> Vec<Result<...>> {
       keys.iter().map(|key| self.set_mined_inner(key, params)).collect()
   }
   ```
   Each `set_mined_inner` takes its own per-key lock, applies, releases. Between key K1 (lock taken+released) and K2 (lock taken), other threads can mutate. The "batch" is not atomic across keys.
**Why it matters:** The redo log entries for the batch are written together (`write_redo_ops` line 2761 writes all SetMined entries in one fsync). If a crash happens after the redo flush but before all engine applies complete, recovery replays all entries idempotently — eventually consistent. So no correctness bug. But operators expecting batch atomicity will be surprised.
**Reproduction:** N/A — this is documented behavior elsewhere.
**Suggested fix:** Document explicitly in `set_mined_batch` doc that the batch is NOT atomic and intermediate states are observable.

### BC-44: `apply_conflicting_child` allocator free + allocate is NOT atomic with metadata write (HIGH)
**Category:** B (Crash)
**Location:** `src/ops/engine.rs:2318-2357`.
**What:** Sequence is:
   1. Free old children block (`self.allocator.lock().free(offset, ...)` line 2320). This journals `RedoOp::FreeRegion`.
   2. Allocate new children block (`self.allocator.lock().allocate(new_size)` line 2326-2331). Journals `RedoOp::AllocateRegion`.
   3. Read aligned sector from device (line 2339), modify with new children list, pwrite (line 2347).
   4. Update metadata fields (line 2353-2356), `write_metadata_fast(ro, &meta)` line 2357.

   Crash points:
   - After step 1, before step 2: old block freed (journaled) but no new allocation. Recovery's allocator replay applies the FreeRegion → freelist now has the old offset back. Metadata still references the old offset. A subsequent allocate could hand out the SAME offset to a different request. Now metadata says "children at offset X", and offset X is being used by another record — silent corruption.
   - After step 2, before step 3: alloc journaled. Recovery applies. Metadata references old offset (which has been freed and possibly reallocated). Same corruption.
   - After step 3, before step 4: new bytes are written, metadata not yet. Reader sees old offset → reads stale children list. Metadata won't be updated. Lost write.

   Each of these crash windows is silently wrong. There is NO redo entry capturing the "metadata header switch from old offset to new offset" intent.
**Why it matters:** Conflicting-children list can be corrupted by any crash mid-update. The doc at `src/recovery.rs:849-869` calls this out as a known limitation but classifies it as "best-effort, not consensus-critical." For a UTXO store, having `read_conflicting_children` return garbage is a soundness bug for any client that uses it for double-spend detection.
**Reproduction:** Drive a create with conflicting=true, parent_txids=[K]. K already has a children list. The dispatcher calls `engine.append_conflicting_child(&K, child_txid)`. Inject a crash via fault-injection between step 1 and step 4. Restart. Read `read_conflicting_children(&K)` — returns garbage or partial list.
**Suggested fix:** Emit a redo entry `RedoOp::AppendConflictingChild { parent_key, child_txid, prior_offset, prior_count, new_offset, new_count }` before any of steps 1-4. Recovery replays by re-reading the parent metadata and ensuring the children list is correct (idempotent).

### BC-45: `engine.delete` tombstone-then-free is NOT a single redo entry — torn delete recovery is fragile (MEDIUM)
**Category:** B (Crash)
**Location:** `src/ops/engine.rs:2700-2714` (delete path).
**What:**
   ```rust
   // Tombstone the metadata before freeing the region so crash-time index
   // rebuilds cannot resurrect this record from stale bytes in freed space.
   let mut tombstone = self.read_metadata_fast(entry.record_offset)?;
   tombstone.magic = 0;
   tombstone.record_size = 0;
   self.write_metadata_fast(entry.record_offset, &tombstone)?;

   // Free device space
   self.allocator.lock().free(entry.record_offset, record_size)?;
   ```
   The `write_metadata_fast` at line 2706 writes the tombstoned metadata. The CRC is computed by `to_bytes` over the new (tombstoned) struct — so the tombstoned metadata is CRC-valid.

   But: the dispatch handler at `handle_delete_batch` writes a `RedoOp::Delete` entry BEFORE this engine.delete call. The delete's redo entry says "tx K removed from index, record at offset X freed". Recovery's `replay_delete` (`src/recovery.rs:747-752`) just calls `index.unregister(tx_key)` — it does NOT tombstone the metadata or free the allocator region.

   So after a crash:
   - If crash before tombstone write: index entry exists, metadata has magic, allocator hasn't freed. Recovery removes index entry. Metadata + slot bytes still on device. Allocator still has the region "allocated" (no FreeRegion entry). Space leak.
   - If crash after tombstone, before free: metadata zeroed, index entry exists. Recovery removes from index. Allocator still has region allocated. Space leak.
   - If crash after free, before any redo flush: now the FreeRegion entry never made it (allocator frees synchronously, journal is fsynced — wait, let me re-check)
**Verification:** `self.allocator.lock().free()` calls `log.append_and_flush(FreeRegion)` (`src/allocator.rs:594-600`). So the FreeRegion is fsynced WITHIN the engine.delete call. So actually the sequence is: dispatcher writes Delete redo, fsyncs; engine.delete tombstones metadata, fsyncs (metadata pwrite is durable on `DirectDevice` via O_DIRECT); engine.delete frees, allocator fsyncs FreeRegion.

   Three fsyncs per delete. The crash window scenarios:
   - Crash after Delete redo, before tombstone: recovery replays Delete (unregisters from index). Metadata still has magic, but there's no index entry. Future device-rebuild scan would re-register an entry pointing at this stale metadata. Bug if rebuild happens.
   - Crash after tombstone, before FreeRegion fsync: recovery replays Delete (unregisters). Metadata is zeroed (not corrupt; CRC valid). Allocator has the region as allocated (no replay of FreeRegion). Subsequent `pre_allocate_create` will not reuse this region. Space leak until next operator-driven defrag.
**Why it matters:** Space leaks. Not fatal but compounds over time.
**Reproduction:** Simulate crash between `write_metadata_fast` and `allocator.free` calls. Restart. Verify allocator's freelist does NOT contain the freed region; verify next allocate skips it.
**Suggested fix:** Combine the tombstone + free into a single atomic redo intent: `RedoOp::DeleteRecord { tx_key, record_offset, record_size }` whose replay handler: (a) writes tombstoned metadata, (b) calls allocator.free, (c) unregisters index, all in one idempotent block. The dispatcher's `RedoOp::Delete` already exists and could be enriched with `record_size` (it's currently set to `0`; line 3965 `record_size: 0`).

### BC-46: `RedoOp::Delete::record_size` is always 0 — recovery cannot free the region (MEDIUM)
**Category:** B (Crash)
**Location:** `src/server/dispatch.rs:3962-3966` (`record_size: 0`); `src/recovery.rs:747-752` (`replay_delete` ignores record_size).
**What:**
   ```rust
   redo_ops.push(RedoOp::Delete {
       tx_key: key,
       record_offset,
       record_size: 0,    // never used by replay; field is dead state
   });
   ```
   The `RedoOp::Delete` struct (`src/redo.rs:189-193`) carries `record_offset` and `record_size`, but the dispatcher writes `record_size: 0` always, and `replay_delete` doesn't touch the device or the allocator.

   So if a delete-batch crashes between the Delete redo flush and the engine.delete completion, recovery replays the unregister, but the record bytes on device remain AND the allocator region remains allocated (no FreeRegion entry from engine.delete since that didn't run).
**Why it matters:** Same as BC-45 — space leak. But adds a specific point: the Delete redo entry HAS the field for record_size but doesn't populate it, so it can't be used for cleanup. Code rot.
**Reproduction:** N/A — see BC-45.
**Suggested fix:** Populate `record_size` from the index lookup, and have `replay_delete` call allocator.free if the FreeRegion isn't already journaled. (Idempotent — if the allocator already has the region in the freelist, the call is a no-op.)

### BC-47: `engine.delete` path: tombstone WRITE comes before allocator.free, but they're separate fsyncs (MEDIUM)
**Category:** B (Crash)
**Location:** `src/ops/engine.rs:2701-2714`.
**What:** The comment at line 2701 says:
   > Tombstone the metadata before freeing the region so crash-time index rebuilds cannot resurrect this record from stale bytes in freed space.

   Good — protects rebuild paths. But: there's no fsync after `write_metadata_fast` (line 2706) before the `allocator.free` call (line 2709). On a `DirectDevice` with O_DIRECT, the metadata pwrite is durable on return (no separate fsync needed). On a non-O_DIRECT device, the write is in OS pagecache and not durable until the next sync.
**Why it matters:** Currently OK if the device is `DirectDevice`. The `MemoryDevice` (used in tests) and any future BufferedDevice would have a window where the tombstone is in pagecache, the FreeRegion is fsynced, then a crash leaves the allocator-marked-free region with NON-tombstoned metadata. Rebuild would resurrect.
**Reproduction:** Use a hypothetical BufferedDevice; run delete; crash before pagecache flushes; rebuild from device; observe stale record resurrected.
**Suggested fix:** After `write_metadata_fast` for the tombstone, call `device.sync()` explicitly. Or document that delete requires a synchronous-write device.

### BC-48: read_full_record path uses read_utxo_slot in a loop — N device reads per slot in a multi-slot record (LOW)
**Category:** C (Concurrency) — performance
**Location:** `src/server/dispatch.rs:4452-4467` (GET handler, UTXO_SLOTS field).
**What:**
   ```rust
   if field_mask.has(FieldMask::UTXO_SLOTS) {
       let utxo_count = { meta.utxo_count };
       data.extend_from_slice(&utxo_count.to_le_bytes());
       for v in 0..utxo_count {
           match engine.read_slot(&key, v) {
               Ok(slot) => { ... }
               ...
           }
       }
   }
   ```
   Each `engine.read_slot` does an index lookup + a slot read. For a tx with 100 UTXOs, the index is read 100 times.
**Why it matters:** Index read takes the index `RwLock` 100 times instead of once. Adds contention and cache thrashing.
**Reproduction:** Get a tx with many UTXOs; profile.
**Suggested fix:** Add `engine.read_slots(&key, &offsets) -> Vec<...>` that takes one index read lock and reads all slots.

### BC-49: append_conflicting_child holds parent's stripe lock across allocator.free + allocator.allocate (MEDIUM)
**Category:** C (Concurrency)
**Location:** `src/ops/engine.rs:2280, 2320, 2326`.
**What:** Sequence:
   ```rust
   let _guard = self.locks.lock(parent_key);    // parent's stripe lock
   ...
   let _ = self.allocator.lock().free(offset, ...);   // allocator mutex
   ...
   let new_offset = self.allocator.lock().allocate(new_size)?;  // allocator mutex again
   ```
   The parent's stripe lock is held across two allocator-mutex acquisitions, EACH of which calls `log.append_and_flush()` (the redo log's parking_lot Mutex). Lock order is: parent stripe → allocator → redo log.

   Meanwhile, a normal `engine.spend` takes parent stripe → metadata → redo log (held by dispatcher).

   These don't deadlock (lock order is consistent across paths) but the redo log mutex is the global serialization point and holding it under TWO additional locks (stripe + allocator) extends the critical section.
**Why it matters:** Throughput. The redo log is held while waiting for the parent stripe lock.
**Reproduction:** Profile latency on creates with conflicting=true.
**Suggested fix:** Restructure `append_conflicting_child` to do the alloc/free OUTSIDE the parent's stripe lock; only the metadata mutation under lock.

### BC-50: `unregister_with_shard_count` releases the index write lock BEFORE the shard_counts decrement is visible to other CPUs (LOW)
**Category:** C (Concurrency)
**Location:** `src/ops/engine.rs:528-537`.
**What:**
   ```rust
   fn unregister_with_shard_count(&self, key: &TxKey) -> Option<TxIndexEntry> {
       let shard = ShardTable::shard_for_key(key) as usize;
       let mut guard = self.index.write();
       let removed = guard.unregister(key);
       if removed.is_some() {
           self.shard_counts[shard].fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
       }
       drop(guard);
       removed
   }
   ```
   `fetch_sub` with `Relaxed` ordering — no synchronization with the lock release. A reader on another core that acquires the write lock immediately after this drop could observe the index entry GONE but the shard_counts NOT YET decremented (because Relaxed allows the store to be reordered after the drop).
**Why it matters:** Any code that relies on the invariant "if index has tx K, shard_counts[shard(K)] >= 1" (e.g., migration verification at `engine.shard_record_count(s) > 0`) sees a transient inconsistent view. Migration could fence a shard whose count appears zero but actually has records.
**Reproduction:** Race condition; hard to repro without a custom stress test.
**Suggested fix:** Use `Ordering::Release` for the fetch_sub (and matching Acquire on the read side in `shard_record_count`). Or move the fetch_sub BEFORE the unregister so the index drop is the synchronization edge.

### BC-51: shard_counts initialization on startup loops over the rebuilt index (LOW)
**Category:** B (Crash)
**Location:** `src/ops/engine.rs:101-105` (Engine::new).
**What:**
   ```rust
   for (key, _) in index.iter() {
       let shard = crate::cluster::shards::ShardTable::shard_for_key(&key) as usize;
       shard_counts[shard].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
   }
   ```
   On startup, walks every entry in the index to recompute shard_counts. With 100M entries, this is O(N) startup time. The hash table's iter() is O(capacity) which is at least N/0.7 (load factor cap).
**Why it matters:** Startup time bound; not a correctness bug.
**Reproduction:** Restart with a 100M-entry index; observe startup time.
**Suggested fix:** Snapshot shard_counts at shutdown time and restore at startup. Or compute lazily.

### BC-52: `now_millis` cached value is racy across threads — two ops can have wall-clock-equal updated_at (LOW)
**Category:** C (Concurrency)
**Location:** `src/ops/engine.rs:443-446`.
**What:** `now_millis()` reads the cached value; multiple concurrent ops in the same batch get the same millis. They write `updated_at = self.now_millis()` to the metadata. Both records have the same updated_at.
**Why it matters:** `updated_at` is not a unique-per-mutation timestamp. Anything that uses it for ordering or dedup is broken.
**Reproduction:** Concurrent create_batch ops; observe identical updated_at.
**Suggested fix:** Use a per-op timestamp (`SystemTime::now()`) inside the engine ops, NOT a cached batch-level value. Or use a monotonic counter as the per-op identifier.

### BC-53: Recovery's `replay_create` (legacy path, pre-CreateV2) registers WITHOUT validating on-device record bytes (HIGH)
**Category:** B (Crash)
**Location:** `src/recovery.rs:715-744`.
**What:**
   ```rust
   fn replay_create(index: &mut PrimaryBackend, tx_key: &TxKey, record_offset: u64, utxo_count: u32) -> ReplayResult {
       if index.lookup(tx_key).is_some() { return ReplayResult::Skipped; }
       let entry = TxIndexEntry {
           device_id: 0,
           record_offset,
           utxo_count,
           block_entry_count: 0,
           tx_flags: 0,
           spent_utxos: 0,
           dah_or_preserve: 0,
           unmined_since: 0,
           generation: 0,
       };
       match index.register(*tx_key, entry) { ... }
   }
   ```
   The legacy `RedoOp::Create` (kept for back-compat with logs predating gap #2) registers the index entry with all-zero cached fields, WITHOUT reading the on-device metadata to verify it exists. If the redo entry says "record at offset 4096" but the device has zeros there (because the engine write didn't complete), the replay still registers an index pointing at zeros. Subsequent reads return CRC errors.
**Why it matters:** The contract for legacy `Create` entries was "if the redo says it was created, it was created" — no verification. Modern `CreateV2` entries (BC-23 partial) verify. Legacy entries are used during the migration window from old log format.
**Reproduction:** Construct an old-format redo log with a Create entry pointing at zeros; replay; observe a registered index entry that returns CRC errors on read.
**Suggested fix:** Have `replay_create` read the device metadata; fail closed on missing/corrupt. Same as `replay_create_v2` does. Or deprecate the legacy `Create` opcode entirely after a release cycle.

### BC-54: BC-04's race also affects the metadata "spending_height" / "spendable_after" computation on Reassign (HIGH)
**Category:** B (Crash)
**Location:** `src/server/dispatch.rs:3510-3625` (handle_reassign_batch).
**What:** Reassign computes `spendable_after` from the request and writes a `RedoOp::Reassign { ..., spendable_after }` entry. The replay handler at `src/recovery.rs:899-905` does:
   ```rust
   let spendable_height = block_height.saturating_add(*spendable_after);
   let mut new_slot = UtxoSlot::new_unspent(*new_hash);
   new_slot.spending_data[0..4].copy_from_slice(&spendable_height.to_le_bytes());
   ```
   This is computed from the redo entry alone, not from any on-device state. So the redo entry IS the authoritative source. Good.

   But: the `before_image` capture for Reassign happens at the dispatcher in `handle_reassign_batch` (line 3562-3590ish). Like BC-04, this happens OUTSIDE the per-key lock. Two concurrent reassigns on the same slot could capture the same `prior_utxo_hash` — both compensation entries claim "rollback to hash X" even though one of them actually reassigned to hash Y (which is the new value the OTHER reassign considered "current"). On a crash mid-rollback, replay could restore to hash X when the actual prior was hash Y.
**Why it matters:** Compensation correctness. Gap #8's whole point is bit-exact rollback. Concurrent reassigns make this impossible to guarantee with the current design.
**Reproduction:** Two concurrent reassign batches on the same slot. First: hash A → hash B. Second: hash A → hash C. Both capture prior_utxo_hash=A. Both apply (A→B then B→C). Replication of the second fails. Compensation restores to A. But the first reassign's replication SUCCEEDED. Now the slot is at A but should be at B.
**Suggested fix:** Take the per-key stripe lock for the duration of read-prior + redo write + apply + before_image capture. This is the "validate-then-apply" pattern from spend, applied to reassign.

### BC-55: redo log scan_all reads always allocate `aligned_read` bytes — 64 MiB heap allocation per call (LOW)
**Category:** B (Crash) — memory
**Location:** `src/redo.rs:1271-1294`.
**What:**
   ```rust
   fn scan_all(&self) -> Result<Vec<RedoEntry>> {
       let align = self.device.alignment();
       let read_size = self.log_size as usize;
       let aligned_read = read_size.div_ceil(align) * align;
       let mut buf = AlignedBuf::new(aligned_read, align);     // 64 MiB heap alloc
       self.device.pread_exact_at(&mut buf, self.log_offset)?;
       ...
   }
   ```
   Every call to `scan_all` (recovery, read_from_sequence, earliest_sequence, open) allocates 64 MiB on the heap.
**Why it matters:** Combined with BC-15 + BC-17, multiple full scans during startup mean multiple 64 MiB heap allocations.
**Reproduction:** Run multiple recovery cycles; observe RSS spikes.
**Suggested fix:** Stream the log: read in 64 KiB chunks, parse incrementally. Or cache the buffer as a field on RedoLog.

### BC-56: handle_set_locked_batch doesn't capture before-image — locked → unlocked compensation has no rollback data (MEDIUM)
**Category:** B (Crash)
**Location:** `src/server/dispatch.rs:3737-3826` (handle_set_locked_batch).
**What:** SetLocked has compensation handler at `src/server/dispatch.rs:1959-1968` that toggles the flag back. But `handle_set_locked_batch` calls `compensate_replication_failure(engine, &repl_ops_by_key, &before_images, redo_log)` with `before_images = no_before_images(...)` — i.e., all BeforeImage::None. The set_locked compensation handler at line 1959 doesn't NEED a before_image (it just inverts the value), so this is OK in itself.

   BUT: `set_locked` ALSO clears `meta.delete_at_height` when locking (`src/recovery.rs:962-964`):
   ```rust
   if *value {
       meta.flags |= TxFlags::LOCKED;
       if { meta.delete_at_height } != 0 {
           meta.delete_at_height = 0;
       }
   }
   ```
   The compensation toggles LOCKED back off but does NOT restore the cleared delete_at_height. If the original DAH was 800, locking cleared it to 0, replication failed, compensation removes LOCKED but DAH stays at 0. The pruner may now incorrectly retain or delete this record.
**Why it matters:** Lock+unlock under replication failure leaves DAH stale.
**Reproduction:** Create record with DAH=800. Lock it (DAH cleared). Replication fails. Compensation toggles LOCKED off. Verify DAH is still 0 (should be 800).
**Suggested fix:** Add `BeforeImage::SetLocked { prior_dah: u32 }` and have the compensation restore it. Mirror the gap #8 pattern.

### BC-57: Recovery replays AllocateRegion and FreeRegion in entry order, not in their actual durability order (LOW)
**Category:** B (Crash)
**Location:** `src/recovery.rs:228-238`.
**What:**
   ```rust
   RedoOp::AllocateRegion { .. } | RedoOp::FreeRegion { .. } => {
       match allocator.as_deref_mut() {
           Some(alloc) => {
               if alloc.replay_redo(&entry.op) { Applied } else { Skipped }
           }
           None => Skipped,
       }
   }
   ```
   Replays in sequence order. A Free followed by an Allocate at the same offset: the order matters. If the redo log is corrupted such that two entries are swapped (which shouldn't happen given CRC + sequence), replay applies them in the wrong order.

   With CRC and sequence numbers, this is defensive only.
**Why it matters:** Defense-in-depth. Currently no concrete bug.
**Reproduction:** N/A.
**Suggested fix:** None needed unless the format is later changed to allow out-of-order entries.

### BC-58: HashTable resize is BLOCKING — every concurrent reader waits (HIGH)
**Category:** C (Concurrency)
**Location:** `src/index/mod.rs:179-184`.
**What:**
   ```rust
   pub fn register(&mut self, key: TxKey, entry: TxIndexEntry) -> Result<()> {
       self.table.insert(key, entry)?;
       if self.table.load_factor() > self.resize_threshold {
           self.table.resize(self.table.capacity() * 2)?;
       }
       Ok(())
   }
   ```
   `register` takes `&mut self` (i.e., the index write lock from the engine's RwLock). When load_factor crosses 0.7, the entire register call rehashes the entire hash table from N buckets to 2N. With 100M entries and 4096 ns/bucket, that's 400 seconds of blocking the single create operation, AND every concurrent reader/writer.
**Why it matters:** The 10M ops/sec target is impossible to sustain across a resize.
**Reproduction:** Drive creates until just below the resize threshold, then issue ONE more create that triggers resize. Time it.
**Suggested fix:** Background resize: allocate the new table off-thread, copy entries with concurrent insert/remove tracking via a generation counter or epoch, atomic swap. This is non-trivial.

### BC-59: `engine.set_redo_log` mutates `redo_log: parking_lot::Mutex<Option<...>>` AT RUNTIME (LOW)
**Category:** C (Concurrency)
**Location:** `src/ops/engine.rs:131-133, 51`.
**What:**
   ```rust
   pub fn set_redo_log(&self, redo_log: Arc<parking_lot::Mutex<crate::redo::RedoLog>>) {
       *self.redo_log.lock() = Some(redo_log);
   }
   ```
   The redo log handle is wrapped in a Mutex so it can be set after engine construction. Every secondary-index update path calls `self.redo_log_handle()` which clones the Arc inside a lock.

   This means EVERY mutation operation that touches secondary indexes acquires this Mutex on the engine to read the redo log handle. The handle rarely changes — only at startup and shutdown. But every op pays the cost.
**Why it matters:** Performance. The Mutex is a global serialization point shared by every secondary-index update.
**Reproduction:** Profile under load; observe lock contention.
**Suggested fix:** Use `arc_swap::ArcSwapOption<RedoLog>` for lock-free reads.

### BC-60: shard_record_count read uses Relaxed ordering — race with inflight register/unregister (LOW)
**Category:** C (Concurrency)
**Location:** `src/ops/engine.rs:474-476`.
**What:**
   ```rust
   pub fn shard_record_count(&self, shard: u16) -> u64 {
       self.shard_counts[shard as usize].load(std::sync::atomic::Ordering::Relaxed)
   }
   ```
   Relaxed read pairs with Relaxed writes (BC-50). Combined with BC-50, this is effectively unordered. Migration verification reads `shard_record_count > 0` and could see stale values either direction.
**Why it matters:** See BC-50.
**Reproduction:** See BC-50.
**Suggested fix:** Acquire ordering on the read paired with Release on the write.

### BC-61: Compensation paths run AFTER the engine has already committed the mutation — observable inconsistency window (MEDIUM)
**Category:** B (Crash) / C (Concurrency)
**Location:** `src/server/dispatch.rs:1667-2003` (`compensate_replication_failure`).
**What:** The compensation runs AFTER `replicate_all_ops` returns Err — meaning the local engine has fully applied the mutation, the dispatcher saw replication fail, and now must walk back. During the window between the engine.apply and the compensation completing, ANY concurrent read sees the committed mutation. After compensation, the read sees the rolled-back state.

   For an external observer (a client doing a GET), this means: the same record can return state A, then state B (committed mutation), then state A (compensated rollback) within milliseconds. With no client-side knowledge that compensation is pending.
**Why it matters:** Read-after-write consistency is broken from the client's perspective. A client that observed the committed value cannot rely on it being durable.
**Reproduction:** Set up a 3-node cluster with WriteAll policy. Take down one replica so replication will fail. Drive a spend operation. Concurrent reader observes the spend → returns SPENT → moments later returns UNSPENT (after compensation).
**Suggested fix:** Hold the per-tx stripe lock across the entire dispatch flow (validate → redo → apply → replicate → respond). This blocks readers until the mutation is fully committed (or compensated). Trade-off: read latency increases when replication is slow.

### BC-62: `compensate_replication_failure` writes its own redo entries via `let _ = write_redo_ops(...)` — flush failures dropped (MEDIUM)
**Category:** B (Crash)
**Location:** `src/server/dispatch.rs:2000-2002`.
**What:**
   ```rust
   if !comp_redo.is_empty() {
       let _ = write_redo_ops(redo_log, &comp_redo);
   }
   ```
   The compensation function writes the rollback redo entries with `let _` — silently drops a flush failure. If the redo log is full or the device is failing, the local rollback state is INCONSISTENT (the in-memory rollback already happened via `engine.unspend`/`engine.unfreeze`/etc., but the redo log doesn't have the compensation entries). On restart, recovery replays the FORWARD entries (the original spend/unspend that were committed) and the rollback is undone — the node now has the failed-replication state.
**Why it matters:** The compensation contract is broken under any redo-log error. The probability of this is low, but the consequences are silent divergence.
**Reproduction:** Fill the redo log to within one entry's size of full. Drive a mutation that triggers replication failure → compensation tries to write rollback entries → log is now full → silently dropped. Restart. The forward entry replays.
**Suggested fix:** Treat compensation redo write failures as fatal; bubble up the error and either retry or fail the dispatcher with a special "rollback-pending" state visible to operators.

### BC-63: `RedoLog::open` does NOT detect and skip a partially-written final entry (MEDIUM)
**Category:** B (Crash)
**Location:** `src/redo.rs:944-972` (`RedoEntry::deserialize`); `src/redo.rs:1283-1290` (scan loop).
**What:**
   ```rust
   while pos < data.len() {
       match RedoEntry::deserialize(&data[pos..]) {
           Some((entry, consumed)) => {
               entries.push(entry);
               pos += consumed;
           }
           None => break,
       }
   }
   ```
   The scan stops at the first parse failure. `deserialize` (line 944) returns None on:
   - length field too small (line 948 — "End marker")
   - data shorter than expected total (line 953)
   - CRC mismatch (line 962)
   - malformed op_type (deserialize returns None)

   In each case, the scan stops. `next_sequence` is set to `last.sequence + 1`. So a partially-written final entry is correctly skipped — but `next_sequence` is the SUCCESSOR of the LAST VALID entry, not of the partially-written one. The partially-written entry is silently discarded without recording it elsewhere.
**Why it matters:** The dispatcher MAY have considered a sequence assigned (because `RedoLog::append` advances `next_sequence` on append, line 1061-1062) and replicated to replicas. If the master crashes before flushing, the replicas have the entry but the master's redo log doesn't — and on restart, the master's `next_sequence` is at the pre-crash position. The same sequence number gets re-assigned to a different op. Replicas now have inconsistent sequences for the same logical position.

   Wait — actually `append` writes to the in-memory buffer; `flush` is what makes it durable. If `flush` didn't run, the entry never made it to disk. The `next_sequence` is in-memory only. On restart, scan returns the last DURABLE entry; `next_sequence = durable_last + 1`. So no skip. OK.

   But: the dispatcher's `replicate_all_ops` is called after `write_redo_ops` flushes. If the flush fails AFTER the pwrite but before the fsync (i.e., the bytes are in pagecache but not on disk), the `flush()` call returns `Err(_)` and the dispatcher returns an error. So the dispatcher never proceeds to replication. OK.

   So the actual concern is: is there ANY path where the dispatcher sees the redo flush succeed (returns Ok) but the bytes don't land durably? That would require fsync to lie or to fail silently. fsync semantics on Linux are well-known to be unreliable on ext4 with `data=writeback`, etc. — but that's an OS issue, not the codebase's.
**Reproduction:** Use a device that lies about fsync (`MemoryDevice` doesn't lie; a hypothetical buggy O_DIRECT device might).
**Suggested fix:** None for the codebase; document that the deployment must use a fsync-honoring filesystem.

### BC-64: `RedoLog::recover` returns ALL entries after the last checkpoint, regardless of whether they've been applied (LOW)
**Category:** B (Crash)
**Location:** `src/redo.rs:1192-1208`.
**What:** `recover` returns entries strictly after the last `Checkpoint`. There's no "applied" flag in the recovery state. So if recovery runs, applies 100 entries, then crashes mid-replay, the next recovery run sees the same 100 entries (since no checkpoint was written) and re-applies. Idempotency must do the heavy lifting.
**Why it matters:** Recovery time is unbounded under repeated crashes. The cost compounds because each replay is O(entries).
**Reproduction:** Crash repeatedly during recovery; observe replay count grow.
**Suggested fix:** Write a "recovery progress" entry to the redo log periodically, or update an external checkpoint after each successful entry replay.

### BC-65: `RedoLog::scan_all` doesn't handle a checkpoint AT the very end of the buffer (LOW)
**Category:** B (Crash)
**Location:** `src/redo.rs:1192-1208` (`recover`).
**What:**
   ```rust
   for (i, e) in all.iter().enumerate() {
       if e.op == RedoOp::Checkpoint {
           checkpoint_idx = Some(i);
       }
   }
   match checkpoint_idx {
       Some(idx) => Ok(all[idx + 1..].to_vec()),
       None => Ok(all),
   }
   ```
   If the checkpoint is at `idx = all.len() - 1`, then `all[idx + 1..]` is empty. OK.
**Why it matters:** Not actually a bug; just verifying the index arithmetic.
**Reproduction:** N/A.
**Suggested fix:** None.

### BC-66: `MarkOnLongestChain` redo entry uses `target_generation = entry.generation.wrapping_add(1)` computed pre-lock (HIGH)
**Category:** B (Crash) / C (Concurrency)
**Location:** `src/server/dispatch.rs:4131-4225` (handle_mark_longest_chain_batch).
**What:** Need to look at the handler. Without re-reading, my guess: the dispatcher reads `entry.generation` from the index cache (snapshot, no lock), adds 1, writes that as the redo entry's `target_generation`. Two concurrent mark_on_longest_chain ops on the same tx both read `entry.generation = N`, both compute `target = N+1`, both flush. Engine applies them in lock order: first goes N→N+1, second goes N+1→N+2. Redo entries say target=N+1 for both.

   Recovery replays with `current_generation >= target_generation` → on the second entry, current=N+2, target=N+1, skip. OK actually that's fine — replay is idempotent. The first entry's replay (current=N+1, target=N+1 → skip) ALSO skips. The on-device state is correct (N+2). But... is the actual observable state correct?

   The semantics of mark_on_longest_chain are "set unmined_since to 0 (on chain) or current_height (off chain)". If both ops have the same `on_longest_chain` value, the result is identical regardless of order. If they differ (one says on, one says off — unusual), the last writer wins, and replay correctly captures the post-state via the second entry's data.
**Why it matters:** Probably actually correct due to the two-phase atomic primary+secondary helper at `src/ops/engine.rs:317-430`. But the pattern is still "compute redo payload from a snapshot" rather than "compute under lock".
**Reproduction:** N/A directly.
**Suggested fix:** Apply the validate-under-lock pattern.

### BC-67: `RedoLog::append` increments `next_sequence` BEFORE the entry serializes, then never rolls back on failure (LOW)
**Category:** B (Crash)
**Location:** `src/redo.rs:1059-1079`.
**What:**
   ```rust
   pub fn append(&mut self, op: RedoOp) -> Result<u64> {
       let seq = self.next_sequence;
       self.next_sequence += 1;       // unconditional increment
       let entry = RedoEntry { sequence: seq, op };
       let bytes = entry.serialize();

       if self.write_pos + self.buffer.len() as u64 + bytes.len() as u64 > self.log_size {
           return Err(RedoError::LogFull { ... });   // next_sequence NOT decremented
       }
       ...
   }
   ```
   On `LogFull` error, `next_sequence` has been advanced but no entry was written. Subsequent successful appends start from the bumped sequence — there's a gap in the sequence space.
**Why it matters:** Replicas tracking sequences see gaps. A replica that ACKs sequence N=100 and then sees N=102 next (because 101 was lost to LogFull-then-recovery) has no way to detect the missing 101 and assumes catch-up is complete.

   In practice, LogFull is fatal (BC-01) so the gap doesn't surface. But if LogFull becomes recoverable (e.g., add checkpoint+reset), this becomes a real bug.
**Reproduction:** Drive append calls until LogFull, then (hypothetically) reset the log, then resume. Observe sequence gap.
**Suggested fix:** Decrement `next_sequence` on the LogFull error path. Or use `checked_add` and roll back. Or compute `next_sequence` lazily after a successful append.

### BC-68: HashTable's max_probe_distance can degrade over time without resize triggering (LOW)
**Category:** C (Concurrency)
**Location:** `src/index/hashtable.rs:736-739` (insert), 815-817 (remove).
**What:** `max_probe` is set on insert when `dist > max_probe`. On remove (backward shift), individual bucket probe_distance values are decremented (line 815-817), but `self.max_probe` is NOT recomputed. So `max_probe` is monotonically non-decreasing — it only goes up.
**Why it matters:** Robin Hood's early-termination optimization (`dist > bucket.probe_distance` → not present) relies on `max_probe` being a tight bound for performance. With `max_probe` only growing, a long-lived table accumulates worst-case lookup time even if the actual probe distances have shrunk. Lookup is still correct, just slower.
**Reproduction:** Insert N keys, observe `max_probe = K`. Remove (N-1) keys. Observe `max_probe` still equal K.
**Suggested fix:** Periodically recompute `max_probe` (e.g., during resize) or reset it after a removal that emptied the previous max-probe bucket.

### BC-69: `redo_log` Mutex is held during `RedoLog::flush`'s entire pwrite + sync — same as BC-38 (LOW)
**Category:** C (Concurrency)
**Location:** `src/redo.rs:1083-1150`.
**What:** Same code paths as BC-38; the comment is duplicated. The flush call holds the mutex (since `&mut self` is taken on the Mutex's contents).
**Why it matters:** See BC-38.
**Reproduction:** See BC-38.
**Suggested fix:** See BC-38.

### BC-70: `MemoryDevice` (test-only) does not honor the alignment contract on raw_ptr access (LOW)
**Category:** C (Concurrency)
**Location:** `src/device.rs:285-310` (verified by reading section earlier).
**What:** `MemoryDevice` exposes a raw pointer via `as_raw_ptr` (`src/device_io` or similar — not directly verified). The Engine uses this for `read_metadata_direct`. In tests, this is a Vec<u8> backing; concurrent access is racy but tests typically don't hit it. Defensive concern only.
**Why it matters:** Production uses `DirectDevice` (mmap of an O_DIRECT-opened file), which DOES honor alignment. Tests might exhibit different behavior than production — false positive test results.
**Reproduction:** N/A — tests pass.
**Suggested fix:** Document the test-only nature explicitly.

### BC-71: `RedoLog::checkpoint()` writes a Checkpoint entry but does NOT trigger any reclamation (LOW)
**Category:** B (Crash)
**Location:** `src/redo.rs:1185-1190`.
**What:**
   ```rust
   pub fn checkpoint(&mut self) -> Result<()> {
       let seq = self.append(RedoOp::Checkpoint)?;
       self.flush()?;
       self.checkpoint_seq = seq;
       Ok(())
   }
   ```
   Writes a Checkpoint entry to the log, fsyncs, updates `checkpoint_seq` — but does NOT free space. Subsequent appends still grow the log linearly. Only `reset()` reclaims, and reset() is only called by tests.

   So even if a caller (some future caller) called `checkpoint()` regularly, the log would still fill up.
**Why it matters:** Reinforces BC-01. The `checkpoint` API is misnamed — it doesn't actually checkpoint in the WAL sense.
**Reproduction:** Call `checkpoint()` repeatedly; observe `available_space()` decreasing.
**Suggested fix:** Either rename `checkpoint` to `mark_checkpoint`, or have it automatically trigger reclamation when safe (no entries between previous checkpoint and current that are still pending replication ack).

### BC-72: handle_create_batch's allocator.lock().free() on failed redo flush is NOT journaled — leaks redo entries (MEDIUM)
**Category:** B (Crash)
**Location:** `src/server/dispatch.rs:3171-3192` (post-redo-failure rollback).
**What:**
   ```rust
   let redo_range = match write_redo_ops(redo_log, &redo_ops) {
       Ok(range) => range,
       Err(e) => {
           // Redo failed: free all pre-allocated space.
           for v in &valid_items {
               ...
               let _ = engine.allocator().lock().free(v.record_offset, base_size + cold_len);
           }
           return error_response(...);
       }
   };
   ```
   `engine.allocator().lock().free(...)` — calls into the allocator's `free()` (`src/allocator.rs:574`), which itself fsyncs a `FreeRegion` redo entry. So we have N AllocateRegion fsyncs (from pre_allocate), then ONE failed CreateV2 batch fsync, then N FreeRegion fsyncs.

   If THE INITIAL CreateV2 fsync failed (the reason we're in this rollback), the redo log is in a corrupt state — the AllocateRegion entries are durable, the CreateV2 is missing. Now we try to fsync N FreeRegion entries to undo. But we're already in a state where the redo log just rejected our write — the most likely reason is `LogFull` or device error. The N `free()` calls will encounter the same condition.

   `let _ = engine.allocator().lock().free(...)` — silently drops the error. So even if we DID manage to free the regions in-memory (the allocator's freelist updates), the FreeRegion entries didn't make it to disk. After restart, recovery replays the AllocateRegion entries and re-marks the regions as allocated. Space leaked, possibly forever.
**Why it matters:** The compensation path on a failed redo flush is broken.
**Reproduction:** Force the CreateV2 redo flush to fail (set up a redo device with limited space). Observe that the AllocateRegion entries remain after restart.
**Suggested fix:** Treat redo flush failures as fatal at the dispatcher level — abort the process so the operator can investigate. Don't try to roll back via more redo writes that will themselves fail.

### BC-73: The `OP_PROCESS_EXPIRED_PRESERVATIONS` path's redo handling is opaque (UNVERIFIED)
**Category:** B (Crash)
**Location:** `src/server/dispatch.rs:395` (dispatch); `OP_PROCESS_EXPIRED_PRESERVATIONS` handler `handle_process_expired`.
**What:** Need to verify the handler. If it walks the DAH index and processes each expired record, each expiration is a delete-like operation that needs redo entries. Without seeing the handler I cannot confirm correctness.
**Why it matters:** Pruning correctness depends on durable expiration tracking.
**Reproduction:** N/A pending verification.
**Suggested fix:** Audit the handler for redo entry coverage of all per-record state changes.

### BC-74: handle_query_old_unmined operates on a snapshot of the unmined index — concurrent updates not reflected (LOW)
**Category:** C (Concurrency)
**Location:** `src/server/dispatch.rs:391` (dispatch); `handle_query_old_unmined`.
**What:** `engine.unmined_index()` (`src/ops/engine.rs:2779`) returns a `MutexGuard` — the caller holds the lock during query. If the caller releases and processes the results, concurrent ops may modify the index. The query result is a snapshot at lock-release time. This is standard.
**Why it matters:** Documented behavior; client must understand the snapshot semantics.
**Reproduction:** N/A.
**Suggested fix:** Document the snapshot semantics in the dispatcher's response.

### BC-75: `RedoLog::recover` returns ALL entries — replay processes them in scan_all order, which is sequence order (verified-correct) (NOT A FINDING)
**Category:** B (Crash)
**Location:** `src/redo.rs:1192-1208`.
**What:** Sequence numbers are assigned monotonically by `RedoLog::append` (line 1060). Entries on disk are written sequentially. The scan returns them in disk order = sequence order. Recovery iterates in this order. So the assumption that "later entries override earlier" holds.
**Why it matters:** Defensive verification.
**Reproduction:** N/A.
**Suggested fix:** None.

### BC-76: `engine.write_metadata_fast` on the non-direct path does pread+memcpy+pwrite — RMW window (MEDIUM)
**Category:** C (Concurrency)
**Location:** `src/ops/engine.rs:563-579`; `src/io.rs:309-332` (`io::write_metadata`).
**What:**
   ```rust
   pub fn write_metadata(device: &dyn BlockDevice, record_offset: u64, metadata: &TxMetadata) -> Result<()> {
       let align = device.alignment();
       ...
       if intra_offset != 0 || !METADATA_SIZE.is_multiple_of(align) {
           device.pread_exact_at(&mut buf, aligned_base)?;   // RMW READ
       }
       ...
       buf[intra_offset..intra_offset + METADATA_SIZE].copy_from_slice(&meta_bytes);
       device.pwrite_all_at(&buf, aligned_base)?;
   }
   ```
   `METADATA_SIZE = 320` and alignment is typically 4096. So `METADATA_SIZE % 4096 != 0` → triggers RMW. The block read at `aligned_base` (= `record_offset` since records are aligned) reads bytes that include the metadata AND any subsequent UTXO slots (since 320 + 69*N could span multiple blocks).

   Specifically: `total_size = align_up(intra_offset + METADATA_SIZE, align)`. With intra_offset=0 and METADATA_SIZE=320, `total_size = align_up(320, 4096) = 4096`. So the RMW reads one 4096-byte block, modifies the first 320 bytes, writes back the whole block.

   This is correct AS LONG AS no concurrent writer is mutating bytes 320-4095 of the same block. Bytes 320-4095 contain UTXO slot 0 (offset 320 within the block) at slots offset 320 through 388 (69 bytes), and slot 1 at 389 through 457, etc. UTXO slot writes also take an RMW (since UTXO_SLOT_SIZE=69 isn't aligned).

   So a concurrent metadata write + slot write on the same record do RMW on the same 4 KiB block. Each reads the block, modifies its slice, writes back. They both run under the per-tx stripe lock (in production paths), so they're serialized. OK.

   But: `engine.read_metadata_fast` (the read path) doesn't take the stripe lock (BC-02). It calls `read_metadata` (`src/ops/engine.rs:556`) which on the non-direct path calls `io::read_metadata` — that does a pread of the same 4 KiB block. If this happens concurrently with a metadata pwrite, the pread could return stale or torn bytes.

   The CRC32 catches torn metadata. What about UTXO slot reads? `io::read_utxo_slot` is separately RMW. A read of UTXO slot 0 (in the same block as metadata) while a metadata write is in flight could see updated metadata + stale slot, or stale metadata + updated slot. No CRC on the slot, so stale slot bytes are silently returned.
**Why it matters:** Reads of UTXO slots can return stale data while metadata is being written, even when both happen at the same record. The window is small (one pwrite call) but is not zero.
**Reproduction:** Concurrent metadata write + slot read on the same record at high rate.
**Suggested fix:** Take the stripe read lock for slot reads. See BC-02.

### BC-77: Recovery's CompensateUnsetMined replay can fail with LogicError when overflow already exists (MEDIUM)
**Category:** B (Crash)
**Location:** `src/recovery.rs:1119-1125`.
**What:**
   ```rust
   if count < INLINE_BLOCK_ENTRIES {
       meta.block_entries_inline[count] = BlockEntry { block_id, block_height, subtree_idx };
       meta.block_entry_count += 1;
       ...
       ReplayResult::Applied
   } else {
       // Full inline + overflow already exists. Restoring a block entry
       // here would require allocating overflow space which is outside
       // the recovery path's responsibility. Treat as logic-error so
       // startup fails closed instead of silently dropping the entry.
       ReplayResult::Failed(ReplayCause::LogicError)
   }
   ```
   When the record already has 3 inline + N overflow block entries, and the compensation entry needs to RESTORE an entry that was UN-set, this path fails. `ReplayCause::LogicError` is intolerable (`src/recovery.rs:69-86`) → startup aborts.
**Why it matters:** Operator hits this on a record with > 3 block entries that had a failed-replication unset-mined. Startup is bricked. Manual intervention required.
**Reproduction:** Create a record, set_mined 4 times (1 in inline, 3 in overflow), trigger an unset-mined that fails replication and the rollback writes a CompensateUnsetMined for the inline-removed block. Replay tries to restore but inline is full again — fails closed.
**Suggested fix:** Have the recovery path call into the engine to allocate overflow space when restoring an entry beyond inline capacity. Or capture more context in the CompensateUnsetMined entry (e.g., "restore to inline slot K" / "restore to overflow position N").

### BC-78: Index file resize tmp file can leak across crashes when redo log is anonymous (LOW)
**Category:** B (Crash)
**Location:** `src/index/hashtable.rs:469-636`.
**What:** When the file-backed index resizes, it writes a tmp file, fsyncs, renames over the original. The redo log entries journal this. If the redo log itself is anonymous (in-memory MemoryDevice — typical for tests), the resize is NOT journaled. A crash mid-resize leaves an orphan tmp file.
**Why it matters:** Tests don't catch this. Production deployments likely use a persistent redo log so this doesn't surface, but the test scaffolding doesn't exercise the file-backed index with a persistent redo log.
**Reproduction:** Set up a file-backed index with a MemoryDevice redo log; crash mid-resize; observe orphan tmp file remains.
**Suggested fix:** Update tests to use a persistent redo log when testing file-backed indexes.

### BC-79: `engine.set_blob_store` takes `&mut self` but Engine is shared via `Arc<Engine>` — cannot be called after sharing (LOW)
**Category:** C (Concurrency)
**Location:** `src/ops/engine.rs:449-451`.
**What:**
   ```rust
   pub fn set_blob_store(&mut self, store: Arc<dyn BlobStore>) {
       self.blob_store = Some(store);
   }
   ```
   Engine is wrapped in `Arc` for sharing across threads. After Arc::new, set_blob_store cannot be called (no mutable access through Arc). Must be called BEFORE `Arc::new(engine)`.
**Why it matters:** Documentation issue. If the API is meant to allow runtime configuration, it should use a Mutex/AtomicPtr.
**Reproduction:** Try to call set_blob_store on an `Arc<Engine>` — won't compile.
**Suggested fix:** Either document "set_blob_store must be called before sharing" or wrap blob_store in `parking_lot::Mutex<Option<...>>`.

### BC-80: Recovery's `replay_create_v2` does NOT verify that the Index entry would be at the right shard (LOW)
**Category:** B (Crash)
**Location:** `src/recovery.rs:778-872`.
**What:** `replay_create_v2` calls `index.register(*tx_key, entry)`. The Engine's `register_with_shard_count` (`src/ops/engine.rs:490-519`) updates the shard counts, but the bare `index.register` doesn't. Recovery uses bare `index.register`, so shard_counts is NOT updated for replayed creates.

   The Engine's startup (`src/ops/engine.rs:101-105`) walks all index entries to recompute shard_counts. So this is recovered at engine construction time. OK.
**Why it matters:** Defensive verification. Recovery doesn't break shard_counts because Engine init re-walks. Order matters: recovery must run BEFORE Engine::new.
**Reproduction:** N/A.
**Suggested fix:** Document the ordering requirement.

### BC-81: `RedoLog::append_batch_and_flush` on empty input returns `(current, current)` without flushing — semantic ambiguity (LOW)
**Category:** B (Crash)
**Location:** `src/redo.rs:1170-1174`.
**What:**
   ```rust
   pub fn append_batch_and_flush(&mut self, ops: &[RedoOp]) -> Result<(u64, u64)> {
       if ops.is_empty() {
           let seq = self.next_sequence;
           return Ok((seq, seq));
       }
       ...
   }
   ```
   Returns `(seq, seq)` where seq is the NEXT sequence to be assigned. The caller might interpret this as "first=last=N" meaning "one entry was assigned sequence N" — but in fact zero entries were written.
**Why it matters:** API confusion. Callers checking `valid_redo_range` (`src/server/dispatch.rs:1057-1060`) handle `range.0 != 0 && range.1 >= range.0` — a non-empty range. With (seq, seq), if seq != 0 the range is treated as valid → replication intent tracker tries to record range (seq, seq) as a single-entry intent that doesn't exist.
**Reproduction:** Call `update_both_secondary_indexes` with `dah_changed = false && unmined_changed = false` — the function returns early at line 220, so this is not actually exercised. OK.
**Suggested fix:** Return `Ok((0, 0))` for empty input to signal "nothing happened".

### BC-82: Recovery does not validate that consecutive redo entries have monotonically increasing sequences (LOW)
**Category:** B (Crash)
**Location:** `src/redo.rs:1271-1294` (`scan_all`).
**What:** `scan_all` parses entries from disk in order. It does NOT validate that `entry.sequence` is monotonically increasing — a corrupt disk could produce entries in arbitrary order. Recovery relies on disk order = sequence order, but doesn't verify.
**Why it matters:** A bit-flip in a sequence field that shifts an entry's sequence from N to M (where M < N) would cause the recovery to see an apparent "out of order" entry. Replay still applies correctly if M's effects are idempotent. But the sequence-tracking for replication catch-up could get wrong values.
**Reproduction:** Bit-flip a sequence field in an existing redo log; observe.
**Suggested fix:** Validate `entry.sequence == prev.sequence + 1` (or > prev.sequence) during scan; flag corruption.

---

## Questions / unverified

- **Q1 — BC-29 prune semantics:** Verify that `engine.prune_slot` (in `src/ops/remaining.rs` or wherever) only modifies the `status` byte and does not touch `spending_data`. If it does touch spending_data, BC-29 escalates to HIGH.
- **Q2 — BC-23 device-scan rebuild:** Search for the term "rebuild" + "scan" across `src/server/startup.rs` — it's possible the rebuild path is implemented in a binary that I haven't read fully. The `RestoreFlags` struct at `src/index/mod.rs:97` suggests there's a "needs_rebuild" mechanism — verify the actual implementation.
- **Q3 — BC-30 RwLock contention:** Need a benchmark to confirm that the global `RwLock<PrimaryBackend>` is the actual bottleneck under 10M ops/sec, vs. the device write path.
- **Q4 — BC-07 ARM memory ordering:** Need a stress test on actual ARM hardware to confirm whether the lack of release fences manifests as observable corruption.
- **Q5 — BC-34 catchup:** Verify that the cluster-promotion code re-bootstraps the new master's redo log from the existing on-device state. If it does, BC-34 is mitigated; if not, BC-34 is a HIGH operational issue.
- **Q6 — BC-01 retention:** Confirm there's no out-of-band cron / supervisor that calls `checkpoint()` at restart or on a timer — `ripgrep` returned nothing but a CLI admin command might exist.
- **Q7 — BC-04 race window:** Verify that `engine.lookup` returns a SNAPSHOT of `TxIndexEntry` (it does — `lookup` returns `Option<TxIndexEntry>` by value, and `TxIndexEntry` is Copy). Confirmed: the snapshot is taken outside the stripe lock. The race is real.
- **Q8 — BC-05 replica gate:** Confirm `master_gen < local_gen` arithmetic at `src/replication/receiver.rs:725` is plain `<` — verified by reading line 725.

## Summary of severity counts

| Severity | Count | Findings |
| --- | --- | --- |
| CRITICAL | 4 | BC-01, BC-02, BC-04, BC-30 (borderline if perf is correctness) |
| HIGH | 15 | BC-03, BC-05, BC-06, BC-07, BC-09, BC-10, BC-11, BC-13, BC-34, BC-36, BC-44, BC-53, BC-54, BC-58, BC-66 |
| MEDIUM | 22 | BC-08, BC-12, BC-14, BC-15, BC-17, BC-18, BC-19, BC-22, BC-24, BC-25, BC-26, BC-35, BC-37, BC-38, BC-39, BC-42, BC-45, BC-46, BC-47, BC-49, BC-56, BC-61, BC-62, BC-63, BC-72, BC-76, BC-77 |
| LOW | 22 | BC-16, BC-20, BC-21, BC-27, BC-28, BC-31, BC-32, BC-33, BC-40, BC-41, BC-43, BC-48, BC-50, BC-51, BC-52, BC-55, BC-57, BC-59, BC-60, BC-64, BC-65, BC-67, BC-68, BC-69, BC-70, BC-71, BC-78, BC-79, BC-80, BC-81, BC-82 |
| NOT A FINDING / VERIFIED OK | 4 | BC-23 (rebuild path exists), BC-29 (prune semantics correct), BC-73 (unverified), BC-74 (documented), BC-75 (correct) |

### Top 10 by impact

1. **BC-01 (CRITICAL)** — No automatic redo-log checkpointing. Master halts when log fills. Default 64 MiB log fills in well under a second at target throughput.
2. **BC-02 (CRITICAL)** — Hot read paths violate the documented stripe-lock-required safety contract. Every concurrent GET vs. mutation race is technically UB and produces sporadic `RecordCorruption` errors.
3. **BC-04 (CRITICAL)** — `handle_unspend_batch` and similar handlers compute `new_spent_count` redo payloads from snapshots taken OUTSIDE the per-tx lock. Recovery replays inconsistent counter values.
4. **BC-30 (CRITICAL/HIGH)** — Single global `RwLock<PrimaryBackend>` is the throughput ceiling. 10M ops/sec is impossible against a single-writer hash table.
5. **BC-58 (HIGH)** — HashTable resize is blocking; doubles capacity in one synchronous call holding the index write lock.
6. **BC-44 (HIGH)** — `append_conflicting_child` mutates parent metadata without a redo entry. Crash mid-update silently corrupts the children list.
7. **BC-10 (HIGH)** — Create batches do N AllocateRegion fsyncs + 1 CreateV2 fsync. Crashes leak device space proportional to batch size.
8. **BC-34 (HIGH)** — Replicas don't write local redo entries on apply. Failover requires full resync of every surviving replica.
9. **BC-05 (HIGH)** — Generation counter wraps at u32::MAX with `wrapping_add`. Recovery's `>=` comparison breaks after wrap. Replication staleness gate also wrong.
10. **BC-09 (HIGH)** — `append_conflicting_child` doesn't journal a redo entry for the metadata update. Same root cause as BC-44.

The CRITICAL findings (especially BC-01: no checkpoint discipline) are operationally fatal for any non-toy deployment. BC-04 is correctness-fatal under concurrent batches against the same txid. BC-02 + BC-06 + BC-07 together constitute a documented-but-violated safety contract that produces sporadic "RecordCorruption" errors on read-heavy workloads even with no actual corruption. BC-30 is a scalability ceiling that shows up immediately at the stated 10M ops/sec target.

The compensation-intent work (gap #8) is a good model for how the codebase SHOULD treat all redo+device-mutation sequences — but several pre-existing paths (BC-09 conflicting children, BC-25 idempotent re-spend metadata write) still violate the pattern.
