# Category A — UTXO Correctness Invariants

## Scope and method

This audit walks every UTXO-mutating operation (`spend`, `unspend`, `create`, `set_mined`, `mark_on_longest_chain`, `freeze`, `unfreeze`, `reassign`, `set_locked`, `set_conflicting`, `preserve_until`, `delete`, `get_spend`) end-to-end:

- Wire decode → lock acquisition → index lookup → device read → validation → mutation → redo log → device write → secondary-index sync → response.
- Each step is examined for: silent error swallowing, partial-state mutation, lost-update windows, divergence between fast and slow paths, allocator/index leaks, missing flag checks, and missing test coverage.

Reference implementation `specs/teranode.lua` was **NOT present** in the repository at audit time — the path resolves to a missing file (`ls /Users/siggioskarsson/gitcheckout/teraslab/specs/` returns only `BSV_UTXO_STORE_RUST_CRATES.md`, `BSV_UTXO_STORE_SPEC.md`, `SPEC_BRIEFING.md`). All Lua-parity findings are inferred from `BSV_UTXO_STORE_SPEC.md`, error-code definitions, and code comments that explicitly cite Lua line numbers.

## Headline findings

The spend / unspend / freeze / unfreeze / reassign / preserve_until paths all violate at least one invariant. The most severe issues:

1. **Slot and metadata write errors in spend are silently swallowed via `tracing::warn!`** (5 sites) — single-spend, idempotent-respend, and `ValidatedSpend::apply` all return `Ok(...)` to the client even when the on-disk write returned `Err`. This is the worst data-integrity bug in the surface and is not covered by any test. (A-01)
2. **`unspend` does not validate spending_data — the `UnspendRequest` and the wire `WireSlotItem` have no `spending_data` field at all.** Any client that knows a `(txid, vout, utxo_hash)` triple can erase a spend they never authored. This violates the inverse-of-spend invariant. (A-04)
3. **Concurrent-spend tests do not verify the AlreadySpent error carries the actual winner's 36-byte spending_data.** Pattern matches use `AlreadySpent { offset: 5, .. }` in every test. The single passing concurrent test `concurrent_spend_same_utxo_different_data` would still pass if the engine returned a zeroed or arbitrary `spending_data` to the 99 losers. Invariant 1 is therefore unverified by the suite. (A-02)
4. **`freeze` and `unfreeze` neither bump `meta.generation` nor write metadata back nor sync the index cache.** They return the *pre-mutation* generation, change the slot on disk, and leave the cached `TxIndexEntry` desynchronised with the device. Subsequent fast-path operations (set_mined, set_conflicting, set_locked) read the stale cache and miscompute DAH. (A-08)
5. **`preserve_until` writes `meta.preserve_until` to disk but never calls `sync_index_cache`.** The cached `TxIndexEntry.tx_flags` does not get the `HAS_PRESERVE_UNTIL` bit; cached `dah_or_preserve` is never updated. Every fast-path op (set_mined, set_conflicting, set_locked) consults the cache and concludes `has_preserve = false`, meaning preserve_until protection is *only* honoured by the slow-path (`device_ptr == null`) branches. (A-15)
6. **`pre_allocate_create` + `create_at_offset` leak device space on `DuplicateTxId`** (and any other failure after pre-allocation). Neither the engine nor the dispatcher frees the allocated region when a concurrent insert wins the race. (A-05)
7. **Reassign does not enforce `LOCKED` or `CONFLICTING` flags**, contrary to spec invariant 7 (flags checked on every spend path including via Reassign). (A-09)
8. **`spend_multi` apply path computes `meta.spent_utxos += spent_count` after slot writes but the slot writes themselves can fail silently** (A-01). Result: counter says N spent, fewer than N actually changed status on disk. (A-03)
9. **`Pruned` UTXOs lose their spending_data on the wire** — engine returns `SpendError::Pruned { offset }` (no spending_data), dispatch maps to `ERR_INVALID_SPEND` with `vec![]` payload. The audit-trail spending_data preserved in the slot per spec §record-layout is never surfaced to clients. (A-07)
10. **`FROZEN_UNTIL` payload is empty on the wire** — `spend_error_to_batch_error` returns `vec![]` for `SpendError::FrozenUntil { spendable_at_height }`, dropping the 4-byte block height. (A-10)
11. **Recovery replay of `RedoOp::Spend` / `RedoOp::Unspend` silently swallows metadata write errors** (`let _ = io::write_metadata(...)`) and never updates DAH, secondary indexes, generation, updated_at, or LAST_SPENT_ALL. Post-recovery state is structurally inconsistent with a non-crashed run. (A-06)

The remaining findings cover the boundary-condition mismatch in `FROZEN_UNTIL`, missing index-cache sync on freeze paths, GetSpend wire-level skipping hash validation, generation overflow on long-running records, the saturating-add in reassign that can pin a UTXO unspendable forever, and several test coverage gaps.

---

### A-01: Spend slot/metadata writes silently swallowed via `tracing::warn!` (CRITICAL)
**Location:** `src/ops/engine.rs:1042-1044`, `src/ops/engine.rs:1066-1068`, `src/ops/engine.rs:1013-1015`, `src/ops/engine.rs:2920-2924`, `src/ops/engine.rs:2948-2950`
**What:** All five mutation sites in `Engine::spend` (single), `Engine::spend` idempotent-respend branch, and `ValidatedSpend::apply` (batch) use the pattern:
```
if let Err(e) = self.write_slot_fast(...) {
    tracing::warn!(err = ?e, "engine: write_utxo_slot failed");
}
```
The function then proceeds to mutate metadata in memory, write metadata, sync the index cache, update DAH, and finally return `Ok(SpendResponse { ... })`. The caller (and through it, the BSV node) sees a successful spend even when the on-disk slot was never updated.
**Why it matters:** This breaks the most fundamental UTXO-store invariant: a successful spend must mean the UTXO is durably marked SPENT on disk. Under disk-write failure (NVMe full, EIO, mmap fault, partial sector), the engine reports OK to the client and bumps `spent_utxos` in the metadata, while the slot on disk remains UNSPENT. Subsequent re-spends from a different transaction will succeed, double-spending the UTXO. This is not crash-safety; it is in-flight write loss being masked.
**Reproduction:** No existing test. Fault-injecting `write_utxo_slot` to return `Err` after the validation step would expose the issue immediately. The spend would return `Ok`, but a follow-up `read_slot` would show `UTXO_UNSPENT`.
**Suggested fix:** Replace every `tracing::warn!` swallow with `?` propagation. The lock is held; on failure the caller (dispatch) must return `ERR_INTERNAL` and the redo log entry must drive replay on next startup. The current behaviour assumes "writes never fail" which is false on production NVMe.

---

### A-02: AlreadySpent winner spending_data never verified by any test (HIGH)
**Location:** `src/ops/engine.rs:4194-4209` (`concurrent_spend_same_utxo_different_data`), `src/ops/engine.rs:3433-3444` (`spend_already_spent_different_data`), `tests/integration.rs:810` (`spend_multi_partial_errors`)
**What:** The engine's `SpendError::AlreadySpent { offset, spending_data }` carries the 36-byte spending_data of the slot at the time of conflict. Every existing test uses pattern `Err(SpendError::AlreadySpent { offset: N, .. })` — the spending_data is matched with `..` and never compared against an expected winner. A second-wave test could pass even if the engine returned `[0u8; 36]`, the requester's *own* spending_data, or any other 36 bytes.
**Why it matters:** Invariant 1 of this audit — "concurrent spends on same UTXO yield exactly 1 success and N-1 ALREADY_SPENT carrying the correct 36-byte spending data of the actual winner" — is not tested. A regression that returned the wrong spending_data (e.g. caller's own data, all-zero, the previous spend on a different slot) would not fail any existing test. In Teranode's reorg/conflict path, clients depend on AlreadySpent's spending_data to walk back to the spending tx; a wrong payload sends them on a wild-goose chase.
**Reproduction:** Existing test `concurrent_spend_same_utxo_different_data` (engine.rs:4166) needs to record the unique spending_data each thread sent, and the loser threads must verify the AlreadySpent payload exactly matches one specific winner's data. Currently it counts to `99` and stops.
**Suggested fix:** Augment the concurrent test to keep a `Vec<[u8;36]>` of all attempts. Find the unique successful winner from the result vector, then assert every loser's `AlreadySpent.spending_data == winner_sd` exactly.

---

### A-03: spend_multi increments spent_utxos by validation count even if slot writes silently failed (CRITICAL)
**Location:** `src/ops/engine.rs:2899-2950`
**What:** In `ValidatedSpend::apply`, the `spent_count` is fixed at validation time (lines 826-878 of `validate_spend_multi`). The apply loop writes each slot, swallowing per-slot write errors with `tracing::warn!`. Then `metadata.spent_utxos = wrapping_add(spent_count)` runs unconditionally. If write_slot_fast fails for some slots but not others, the metadata claims more slots are spent than actually are.
**Why it matters:** The on-disk state is now inconsistent: `meta.spent_utxos` is e.g. 5, but only 3 slots actually have status SPENT. DAH evaluation depends on `spent_utxos == utxo_count`, so the record may be flagged as "all spent" when it isn't, causing premature pruning. Replication ships a metadata that disagrees with the slot bytes.
**Reproduction:** Inject `write_slot_fast` to fail on the second of three slots in a batch. The spend_multi returns `Ok` with `spent_count: 3`. Read the metadata: `spent_utxos == 3`. Read each slot: only two are SPENT.
**Suggested fix:** Track a runtime `actually_written: u32` counter in the apply loop. Increment only when `write_slot_fast` returned `Ok`. Use that for the metadata update and the response. Better: make `write_slot_fast` propagate errors via `?` (see A-01) so the entire op fails atomically.

---

### A-04: Unspend does not validate spending_data — wire format omits it entirely (CRITICAL)
**Location:** `src/ops/unspend.rs:9-22` (`UnspendRequest` struct), `src/ops/engine.rs:1085-1181` (`Engine::unspend`), `src/protocol/codec.rs:407-411` (`WireSlotItem` struct used by both spend-list and unspend-list ops)
**What:** Spend writes `(txid:32, vin:4)` into `slot.spending_data`. The inverse operation must require that the unspender prove they were the spender — i.e. supply a matching `spending_data` and only succeed when it matches. The current `UnspendRequest` has fields `{ tx_key, offset, utxo_hash, current_block_height, block_height_retention }` — no `spending_data`. The engine's `UTXO_SPENT` branch in `unspend` (line 1121) only checks `slot.spending_data == [FROZEN_BYTE; 36]` (the frozen sentinel) and otherwise overwrites the slot with `UtxoSlot::new_unspent`. Wire-level `WireSlotItem` is `(txid, vout, utxo_hash)` — only 68 bytes; there is no field for spending_data. The protocol cannot transmit it even if engine wanted to check.
**Why it matters:** Anyone with knowledge of `(txid, vout, utxo_hash)` — all of which are public on-chain data — can erase a spend they did not author. This breaks invariant 3. In a Teranode operator with multiple BSV nodes pointing at the same TeraSlab cluster, a buggy or malicious peer can rewrite the chain's spending data via repeated unspends.
**Reproduction:** Spend a UTXO with spending_data `[0xAA; 36]`. Send an `UnspendRequest` from a different connection with no spending_data. The slot returns to UNSPENT. The original spender's record of having spent it is gone.
**Suggested fix:** Extend `UnspendRequest` with `spending_data: [u8; 36]`. Extend `WireSlotItem` (or introduce a 104-byte `WireUnspendItem`). In `Engine::unspend`, after the hash check, add `if slot.spending_data != req.spending_data { return Err(SpendError::UtxoHashMismatch { ... }) }` (or a new `SpendingDataMismatch` variant). Add a regression test that demonstrates an unspend with the wrong spending_data fails.

---

### A-05: pre_allocate_create + create_at_offset leak device space on duplicate txid race (HIGH)
**Location:** `src/ops/engine.rs:1761-1793` (`pre_allocate_create`), `src/ops/engine.rs:1801-1921` (`create_at_offset`), `src/server/dispatch.rs:3271-3277` (dispatch handler for `DuplicateTxId`)
**What:** The WAL-first create path is: `pre_allocate_create` (allocates `record_offset`) → `build_create_record_bytes` → write redo → `create_at_offset`. `create_at_offset` performs a second duplicate check (line 1815-1817):
```rust
if self.index.read().lookup(&key).is_some() {
    return Err(CreateError::DuplicateTxId);
}
```
If a concurrent thread inserted between the pre-allocate and the create, this branch fires. The dispatch handler (dispatch.rs:3271) only pushes a `BatchItemError`. **Neither the engine nor the dispatcher calls `allocator.free(record_offset, base_size + cold_len)`** on this branch. The `pre_allocate_create` doc comment (line 1759-1760) states "If the caller decides not to finalize... it must free the allocated space" — but the duplicate path inside `create_at_offset` is exactly that case, and the dispatcher doesn't honour the contract.
**Why it matters:** Every concurrent duplicate-txid race in the WAL-first path leaks one record's worth of device space. Under sustained Teranode load with reorgs (which produce duplicate-txid races regularly), the device free-list shrinks monotonically until allocator returns DeviceFull. Symptoms: slow OOM-style death, no observable error path.
**Reproduction:** Start two threads that race to create the same txid via the dispatcher. After both finish, allocator stats show one extra allocation that doesn't correspond to an index entry. Repeat 10K times → measurable space loss.
**Suggested fix:** In dispatch.rs ~line 3271, when `Err(CreateError::DuplicateTxId)` arrives, call `engine.allocator().lock().free(v.record_offset, base_size + cold_len)` before pushing the error. Same for the `Err(_)` branch at 3278. Extract a helper `free_pre_allocated_for(create_req)` to avoid duplicating the size calculation. Better: have `create_at_offset` do the free internally on its own duplicate-detection branch so the contract is enforced at the engine boundary.

Also note: `Engine::create` (the non-WAL convenience method) has the same shape but the duplicate check is *before* allocation (line 1622), so it's not affected; `create_at_offset` uniquely allocates first then re-checks.

---

### A-06: Recovery replay of Spend/Unspend silently swallows metadata write errors and never updates derived state (HIGH)
**Location:** `src/recovery.rs:520-558` (`replay_spend`), `src/recovery.rs:560-592` (`replay_unspend`)
**What:** Both replay functions write the new slot, then attempt to read+modify+write the metadata's `spent_utxos`:
```rust
if let Ok(mut meta) = io::read_metadata(device, ie.record_offset) {
    meta.spent_utxos = new_spent_count;
    let _ = io::write_metadata(device, ie.record_offset, &meta);
}
```
The metadata write error is dropped (`let _ =`) and replay still returns `ReplayResult::Applied`. The metadata read error is treated as "skip the metadata update" but the slot was already changed. Neither replay function:
- Recomputes generation
- Updates `meta.updated_at`
- Re-evaluates DAH (so `delete_at_height` and `LAST_SPENT_ALL` are stale)
- Updates the DAH or unmined secondary indexes
- Updates the cached `TxIndexEntry` fields (tx_flags / dah_or_preserve / unmined_since)
**Why it matters:** After a crash-and-recover, the on-disk record can be in a different state than a successful non-crashing run would have produced:
- Slot is SPENT but metadata says spent_utxos=0 (if metadata write failed).
- LAST_SPENT_ALL flag is wrong → next DAH evaluation produces a different signal.
- DAH index is stale → pruning skips records that should be pruned, or prunes records that shouldn't be.
- Generation didn't move → replicas that resync from generation watermark will not realise this record changed.
**Reproduction:** No existing test. The `recovery_crash_boundaries.rs` tests only assert that the slot bytes match expected (boundary 2). They don't assert metadata or secondary-index state.
**Suggested fix:** Replay must call into the engine's normal mutation path under a synthetic guard, OR the redo log must capture every derived field that needs to be re-stamped. The current design captures only `new_spent_count` (one of perhaps eight fields that change).

---

### A-07: Pruned UTXO loses its preserved spending_data on the wire (HIGH)
**Location:** `src/record.rs:46` (UTXO_PRUNED status doc says "Preserved from last spend"), `src/ops/engine.rs:1031`, `src/ops/engine.rs:1135-1136`, `src/server/dispatch.rs:5092` (dispatch maps `Pruned` → `ERR_INVALID_SPEND` with `vec![]` payload)
**What:** Per the on-disk layout doc in `record.rs`, a PRUNED slot preserves the spending_data from its last spend as an audit trail. The `SpendError::InvalidSpend` variant carries spending_data and is the right error for a spend that targets a pruned child. But `Engine::spend` returns `SpendError::Pruned { offset }` (no spending_data) on the UTXO_PRUNED branch (line 1031). `spend_error_to_batch_error` (dispatch.rs:5092) maps `Pruned` to `ERR_INVALID_SPEND` with `vec![]`. The audit-trail spending_data is read by neither the engine error nor the wire response. Meanwhile `SpendError::InvalidSpend` is **never produced by the engine** — `grep InvalidSpend` shows only the error definition, dispatch mapping, and an unused branch in replication/receiver.rs:787.
**Why it matters:** The whole point of preserving spending_data through pruning is so that a peer who later asks "what spent this UTXO?" gets the original spender's identity even after the immediate child tx was deleted. The current implementation throws that data away on the wire.
**Reproduction:** Manually set a slot's status to UTXO_PRUNED with a known spending_data via direct device write, then call spend on it via the dispatcher. The response is `ERR_INVALID_SPEND` with empty payload. The known spending_data is in the slot but is never returned.
**Suggested fix:** Make `Engine::spend` return `SpendError::InvalidSpend { offset, spending_data: slot.spending_data }` on the `UTXO_PRUNED` branch. Update tests to verify the payload. Decide whether `Pruned` should remain as a separate variant or be retired.

---

### A-08: Freeze and Unfreeze do not bump generation, write metadata, or sync index cache (HIGH)
**Location:** `src/ops/engine.rs:2161-2199` (`freeze`), `src/ops/engine.rs:2202-2228` (`unfreeze`)
**What:** `freeze` reads metadata, validates the slot, writes a frozen slot, and returns:
```rust
let generation = { meta.generation };
Ok(generation)
```
The metadata is **not written back**. The `meta.generation` is the *pre-call* value. The cached `TxIndexEntry` is never synced. `unfreeze` does the same. Compare to `reassign` (line 2261-2266) which does `meta.generation = wrapping_add(1); write_metadata_fast(...); sync_index_cache(...)`.
**Why it matters:**
1. **Replication watermark broken.** Replicas track `generation` to know when to resync. Freeze/unfreeze change observable state but not generation. A replica's "is my copy of this tx fresh?" check passes incorrectly.
2. **Cached index drift.** Subsequent fast-path ops (`set_mined`, `set_conflicting`, `set_locked`) read `entry.spent_utxos / entry.tx_flags / entry.unmined_since` from the cached index. After freeze, the on-device slot is FROZEN but the cache still says UNSPENT. The fast path's DAH evaluation operates on `entry.spent_utxos == entry.utxo_count`, which is unaffected by freeze (frozen slots are not counted as spent), but cached `tx_flags` may include LAST_SPENT_ALL that's now invalid.
3. **`updated_at` not bumped.** Tooling that uses updated_at for monitoring or reorg detection misses freeze events.
**Reproduction:** Read metadata.generation = G. Call freeze. Read metadata.generation = G (unchanged). Existing tests don't assert on generation bumps for freeze/unfreeze.
**Suggested fix:** After `write_slot_fast`, do `meta.generation = wrapping_add(1); meta.updated_at = self.now_millis(); self.write_metadata_fast(ro, &meta)?; self.sync_index_cache(&req.tx_key, &meta)?; Ok(meta.generation)`. Add a regression test that asserts `gen_after > gen_before` for both freeze and unfreeze.

---

### A-09: Reassign does not enforce LOCKED, CONFLICTING, or coinbase maturity flags (HIGH)
**Location:** `src/ops/engine.rs:2231-2270` (`reassign`)
**What:** `reassign` checks: index lookup, vout in range, slot hash matches, `slot.status == UTXO_FROZEN`. It does NOT check:
- `metadata.flags.contains(TxFlags::CONFLICTING)` — a conflicting tx can have its UTXOs reassigned.
- `metadata.flags.contains(TxFlags::LOCKED)` — a locked tx can have its UTXOs reassigned.
- Coinbase maturity (no `current_block_height` parameter at all in `ReassignRequest`).
The audit's stated invariant 7 is: "Frozen / locked / conflicting flags are checked on EVERY spend path, including via Reassign." Reassign is morally a controlled hash-replacement, but the post-reassign UTXO is immediately a fresh UNSPENT slot that can be spent (after cooldown). If reassign is allowed on a CONFLICTING tx, a spend on the new hash will then encounter `Conflicting` at the next spend op — but only because spend rechecks. There's still a window where a tool that reads cached fields trusts that a CONFLICTING tx hasn't moved.
**Why it matters:** Reassign on a locked tx silently bypasses lock semantics. Reassign on a conflicting tx confuses downstream tooling about reorg state.
**Reproduction:** Create a tx with `TxFlags::LOCKED`, freeze a UTXO, reassign it. The reassign succeeds. Repeat with CONFLICTING — same result.
**Suggested fix:** After reading metadata, add the same validation block as `Engine::spend` (engine.rs:783-798): reject CONFLICTING (without `ignore_conflicting`), reject LOCKED (without `ignore_locked`), reject coinbase below maturity. Add `current_block_height: u32` to `ReassignRequest`.

---

### A-10: FROZEN_UNTIL wire response drops the spendable-at-height payload (HIGH)
**Location:** `src/server/dispatch.rs:5088`
**What:**
```rust
SpendError::FrozenUntil { .. } => (ERR_FROZEN_UNTIL, vec![]),
```
The engine's error variant carries `spendable_at_height: u32` (engine.rs:867-872). The dispatch maps it to an empty wire payload. Compare to `CoinbaseImmature` (dispatch.rs:5076) which correctly emits `spending_height.to_le_bytes().to_vec()`.
**Why it matters:** A client that hits FROZEN_UNTIL has no way to know how long to wait. The wire-level retry logic in Teranode's BSV node has to treat all FROZEN_UNTIL identically, e.g. retry on every block, instead of waiting precisely until the spendable height.
**Reproduction:** Call spend on a reassigned slot at `current_block_height = 1099` when spendable_height = 1100. The error response payload is empty. The client cannot recover the 1100 to retry at exactly 1101.
**Suggested fix:** Change to `(ERR_FROZEN_UNTIL, spendable_at_height.to_le_bytes().to_vec())`. Add a regression test mirroring `partial_error_coinbase_immature_4_bytes` (codec.rs:2404).

---

### A-11: GetSpend wire path skips utxo_hash validation entirely (MEDIUM)
**Location:** `src/server/dispatch.rs:4783-4805`
**What:** The comment in dispatch is explicit:
```
// GetSpend needs the utxo_hash for validation. Since the wire format
// only sends txid+vout, we skip hash validation at this level and
// return whatever is at that slot offset.
```
The engine's `Engine::get_spend` (engine.rs:2746-2776) does check `slot.hash != req.utxo_hash`. The dispatch path bypasses this by reading the slot directly via `engine.read_slot` (which doesn't take a hash). After a reassign, an old peer asking `(txid, vout)` for the original UTXO gets the new UTXO's status without any indication that the hash changed.
**Why it matters:** Invariant 10 (UTXOHash mismatch returns UTXO_HASH_MISMATCH and does NOT mutate) is partially violated for the wire path. While GetSpend doesn't mutate, it returns wrong data without an error, which can cause downstream reasoning errors. Specifically: a peer caching `UtxoMeta(hash, txid, vout)` from a previous GetSpend will not learn about the reassign.
**Reproduction:** Create tx with hash H1 at vout 0. Freeze and reassign to H2. Call wire-level GetSpend with `(txid, 0)`. The response is the slot at vout 0 (now H2). The caller thought they were reading H1's data; nothing in the response says otherwise.
**Suggested fix:** Extend the wire `WireGetSpendItem` to include `utxo_hash: [u8;32]`. In dispatch, validate it against `slot.hash` and return `ERR_UTXO_HASH_MISMATCH` on disagreement. Same as the engine-level get_spend already does.

---

### A-12: preserve_until does not sync index cache → fast-path ops ignore preserve_until protection (HIGH)
**Location:** `src/ops/engine.rs:2647-2682`
**What:** `preserve_until` writes `meta.preserve_until = req.block_height` and `meta.delete_at_height = 0` to the device, but never calls `sync_index_cache`. The cached `TxIndexEntry.tx_flags` does not get the `HAS_PRESERVE_UNTIL` bit (which is a *cache-only* synthetic flag — see `record.rs:341-343`); cached `dah_or_preserve` is never updated to hold the preserve_until value.

Subsequent fast-path mutations (`set_mined` fast path engine.rs:1228-1326, `set_conflicting` fast path engine.rs:2412-2492, `set_locked` fast path engine.rs:2551-2616) all derive `has_preserve = tf.contains(TxFlags::HAS_PRESERVE_UNTIL)` from the cached `entry.tx_flags`. Because that bit was never set, `has_preserve = false` even though the on-disk record has preserve_until set. The fast paths then call `evaluate_dah_cached(... has_preserve_until: false ...)` which interprets the cached `dah_or_preserve` as the prior `delete_at_height` (which is 0 because preserve_until cleared it). Then DAH evaluation can re-set `delete_at_height` to a non-zero value.

Crucially, the fast-path then writes a fresh DAH back to disk, **overwriting `meta.preserve_until`'s blocking effect indirectly via the DAH index** — `update_dah_index` inserts the key into the DAH-keyed pruning index. Pruning will then reach this record despite `preserve_until` being set on disk, because pruning reads from the DAH index, not from on-disk `preserve_until`.
**Why it matters:** Invariant 13 says preserveUntil prevents pruning until the height. After preserve_until + setMined (fast path) + spend-all, the record will appear in the DAH index and be eligible for pruning at height `current_block_height + retention`, completely bypassing `preserve_until`. The slow path (no `device_ptr`) reads metadata from disk and respects preserve_until correctly, but production runs the fast path.
**Reproduction:** Create a tx, mine it, spend all UTXOs, observe DAH set at height H1. Call preserve_until(5000). Now call set_locked or set_conflicting via the fast path with retention=288 at height 1000. The fast path's `evaluate_dah_cached` is called with `has_preserve_until=false`, computes a new DAH=1288, re-inserts into the DAH index. The pruning sweep at height 1289 will pick up this key even though preserve_until=5000.
**Suggested fix:** In `preserve_until`, after `write_metadata_fast`, call `sync_index_cache(&req.tx_key, &meta)`. The existing `sync_index_cache` already handles the HAS_PRESERVE_UNTIL discriminant correctly (engine.rs:631-657). Add a regression test that does preserve_until → set_locked → assert DAH index does not contain the key.

---

### A-13: Reassign uses saturating_add for spendable_height — silently pins UTXO unspendable forever on overflow (MEDIUM)
**Location:** `src/ops/engine.rs:2254`
**What:**
```rust
let spendable_height = req.block_height.saturating_add(req.spendable_after);
```
When `block_height + spendable_after > u32::MAX`, the result clamps to `u32::MAX`. The spend check `spendable_height >= req.current_block_height` (engine.rs:865, 996) compares against current height. If `spendable_height = u32::MAX`, no current height can ever exceed it, so the UTXO is permanently FrozenUntil. There is no error path — the operator and client see no signal that a misconfiguration occurred.

This is the same anti-pattern already corrected in `evaluate_delete_at_height` (delete_eval.rs:31-41 uses `checked_add` and returns `DahOverflow`). Reassign was missed.
**Why it matters:** A misconfigured reassign call permanently bricks a UTXO with no error feedback. The defensive analogue (`DahOverflow`) exists for DAH but not for reassign. In normal operation BSV block heights are well below u32::MAX, but a fuzz/misconfig of `spendable_after = u32::MAX` would cause silent loss.
**Reproduction:** Call reassign with `block_height = 1000, spendable_after = u32::MAX`. The slot is now permanently unspendable. The reassign returns Ok.
**Suggested fix:** Use `req.block_height.checked_add(req.spendable_after).ok_or(SpendError::ReassignOverflow { ... })`. Add the new error variant. Add a regression test analogous to `dah_overflow_one_past_boundary_errors`.

---

### A-14: spendable_height boundary semantics — UTXO not spendable AT spendable_height, only after (LOW)
**Location:** `src/ops/engine.rs:865`, `src/ops/engine.rs:996`
**What:** The check is `spendable_height >= req.current_block_height` → at `current_block_height == spendable_height`, the spend is rejected. With `block_height = 1000, spendable_after = 100`, the computed `spendable_height = 1100`. The spend at block 1100 fails; only block 1101 succeeds.

Tests `reassign_not_spendable_until_cooldown` (engine.rs:7382) tests block 1099 (rejected) and `reassign_spendable_after_cooldown` (engine.rs:7426) tests block 1101 (succeeds). The boundary block 1100 is **untested** — it would expose the off-by-one if anyone read the spec as "spendable at block 1100".
**Why it matters:** Naming is "spendable_after" — the semantic could reasonably be either ">=" (spendable at the height) or ">" (spendable strictly after). The current code is ">". Without a Lua reference at `specs/teranode.lua` (file is missing) we cannot verify which Lua chose.
**Reproduction:** Create the boundary test at block 1100 with current code → FrozenUntil.
**Suggested fix:** Add a boundary test, verify behaviour matches spec/Lua. If Lua used `>` (current code), document explicitly in `ReassignRequest::spendable_after`. If Lua used `>=`, change `>=` to `>` in both spend paths.

---

### A-15: Coinbase maturity check uses `>` strictly — at exactly spending_height it succeeds (LOW)
**Location:** `src/ops/engine.rs:790-797`, `src/ops/engine.rs:971-978`
**What:**
```rust
if metadata.flags.contains(TxFlags::IS_COINBASE)
    && spending_height > 0
    && spending_height > req.current_block_height
{
    return Err(SpendError::CoinbaseImmature { ... });
}
```
At exactly `current_block_height == spending_height`, the spend succeeds (test `spend_mature_coinbase_equal` engine.rs:3387 verifies this). This is the BSV-correct semantic (`spending_height = block_height + 100` and you can spend at the 100th confirmation). The audit invariant 6 says the wire error must include the 4-byte required height — already verified correct (dispatch.rs:5076-5081).
**Why it matters:** Behaviour is correct here. Documenting as LOW for completeness — no change needed. Boundary is correctly tested.

---

### A-16: spend(idempotent re-spend) increments generation but `spend_multi` idempotent re-spend still increments by 1 for the whole batch (LOW)
**Location:** `src/ops/engine.rs:1003-1022` (single spend idempotent path), `src/ops/engine.rs:881-883` (validate spend_multi idempotent path), `src/ops/engine.rs:2929-2931` (apply)
**What:** A single `spend()` on an already-SPENT slot with the same spending_data bumps generation+1, writes metadata. The `spend_multi` code, when one item is an idempotent re-spend, sets `spent_count` for that item to 0 (line 882-883: `continue` without adding to valid_spends). If ALL items in a spend_multi are idempotent, `spent_count = 0`, `valid_spends` is empty, but apply still bumps generation (line 2931) and writes metadata. This is consistent with single-spend behaviour (test `idempotent_respend_increments_generation` engine.rs:4938-4950 explicitly accepts this).

The behavioural inconsistency: a single SpendMultiRequest with N=10 idempotent re-spends produces ONE generation increment (whole batch), while sending N=10 single-spend RPCs produces 10 increments. This is correct semantically but worth flagging as a behavioural quirk a Teranode-side reorg detection might depend on.
**Why it matters:** Generation is a watermark for replication; a batched-idempotent-respend ships fewer generation increments than the same operations done individually. If anywhere in the system "ten spend events at the same time" is used as a heartbeat, batching collapses the heartbeat.
**Reproduction:** Existing tests document the behaviour but don't flag the inconsistency.
**Suggested fix:** None mandatory. Document explicitly in the doc comment of `Engine::spend_multi` that the generation bump is once per batch regardless of item count.

---

### A-17: Reassign does not validate utxo_hash race against concurrent spend (MEDIUM)
**Location:** `src/ops/engine.rs:2231-2270`
**What:** `reassign` holds the per-record stripe lock for the duration. The lock prevents concurrent spend on the SAME tx_key. However, the slot was previously frozen — its hash is the *original* hash. After `reassign`, the slot has a new hash. If a different op (e.g. a pre-existing pending spend held by another component, or a stale wire request in the dispatcher's queue) was constructed with the old hash and arrives after the reassign, it gets `UtxoHashMismatch`. That's correct.

But: reassign **does not check that the slot's recorded hash equals the request's `utxo_hash`** before checking the FROZEN status. Wait — it does, at line 2246-2248: `if slot.hash != req.utxo_hash { return Err(SpendError::UtxoHashMismatch ...) }`. OK, this is fine. Demoting from finding to LOW — no actual issue here. Consider this a verified-OK note.
**Why it matters:** N/A — included to document that the suspected reassign hash race is properly handled. The freeze must have been called with the original hash; reassign verifies the same hash is still stored; only then proceeds.

---

### A-18: Coinbase maturity test missing for IS_COINBASE without spending_height (LOW)
**Location:** `src/ops/engine.rs:790-797`, `src/ops/engine.rs:971-978`
**What:** The check is `is_coinbase && spending_height > 0 && spending_height > current_block_height`. The `spending_height > 0` clause is a guard that disables the maturity check when spending_height is 0. There is no test that creates an `IS_COINBASE` tx with `spending_height = 0` and verifies the spend is allowed at any height (i.e. that the maturity guard is correctly skipped).
**Why it matters:** During recovery from a malformed redo (or replication from a misconfigured peer), a coinbase record with `spending_height = 0` could exist. It would currently be spendable at any height. If that's incorrect (Lua reference may treat is_coinbase + zero-height as an error), there's no test to catch the divergence.
**Reproduction:** Create a coinbase tx with `spending_height = 0`. Spend at any block height. Currently succeeds.
**Suggested fix:** Decide whether this is intended; if so, add an explicit test asserting it. If not, change the guard to `if metadata.flags.contains(TxFlags::IS_COINBASE) && spending_height > req.current_block_height` (drop the `> 0`).

---

### A-19: Conflicting children list rebuild uses pread of stale device buffer to seed new buffer (LOW)
**Location:** `src/ops/engine.rs:2336-2350`
**What:** `append_conflicting_child` does:
```rust
self.device.pread_exact_at(&mut wbuf, aligned_base)
```
*before* writing the new buffer. This is a "read-around" pattern — read the existing aligned region so a sub-aligned write doesn't truncate adjacent data. The read happens at the *new* offset (which the allocator just gave us), so it reads whatever was at that region previously (typically zeros for a fresh block, but may be stale data from a freed and reallocated region). The subsequent write copies our children list into `wbuf[intra..intra + children.len() * 32]` and writes `wbuf` back. Bytes outside our slice are preserved from the read-around — but they're either zeros or stale data unrelated to this record.

This appears defensive but: if the allocator returns an offset that aligns exactly to an alignment boundary AND `children.len() * 32` is exactly a multiple of alignment, the read-around is unnecessary. More importantly, if `aligned_base + read_len` extends beyond `intra + children.len() * 32`, the trailing bytes carry stale data that's now durably written under our record's new offset. That's not a correctness bug for *this* record's reads (we only read `[intra..intra + count*32]`), but it's writing-back garbage that wasn't ours.
**Why it matters:** A future allocator or recovery scanner that reads this region could misinterpret the trailing stale bytes. The existing tests don't cover this. Low severity because the subsequent reads are bounded by `count * 32`.
**Reproduction:** Allocate a region, free it, allocate a smaller region inside it, observe trailing bytes carry old data.
**Suggested fix:** Zero `wbuf` before copying children in. Drop the pread.

---

### A-20: Delete operation does not mark child UTXOs of *spending* transactions as PRUNED (informational; check spec intent)
**Location:** `src/ops/engine.rs:2688-2743`
**What:** When `delete()` is called on tx C that spent UTXO (txP, voutP), the slot at the parent tx P that holds the spending data for C is **not** transitioned to UTXO_PRUNED. The slot stays UTXO_SPENT with C's spending data. After C is gone, anyone reading P's slot at voutP sees C's spending data, but C is unfindable — exactly the condition that UTXO_PRUNED status (with preserved spending_data audit trail) was designed for.

If the spec requires "deleting C's record causes P's slot at voutP to become PRUNED", this is missing. If the spec instead says "PRUNED is only set explicitly via prune ops, not via delete", this is correct.
**Why it matters:** Audit invariant 5 says: "Delete on tx with at least one spent UTXO: confirm intended behavior, find the code path." There is no code path that propagates delete back to parents. A test like `delete_does_not_prune_parent_slot_status` would document the behavior. The Lua reference (missing) likely defines the contract.
**Reproduction:** Create P with utxo_count=1. Spend P[0] from a request whose tx_key is C. (Note: C does not need a record — spend only updates P's slot.) Delete C... but C has no record because spends don't create records on the spender side. So this only applies if C also has its own record. In TeraSlab, both spender and spendee have records; deleting C does not look at C's inputs.
**Suggested fix:** Document explicitly. If propagation is intended, walk C's `inputs` cold-data, find each parent P, and write P's spent-by-C slot to UTXO_PRUNED. This is an O(N inputs) device-write loop and would need lock-ordering analysis.

---

### A-21: `set_conflicting` slow path does not propagate to parent records' conflicting-children list (MEDIUM)
**Location:** `src/ops/engine.rs:2520-2532` (slow path), `src/ops/engine.rs:2400-2493` (fast path)
**What:** The slow path of `set_conflicting` (line 2520-2532) reads cold data, extracts parent txids via `extract_parent_txids_from_cold_data`, and calls `append_conflicting_child` on each. **The fast path (line 2400-2493) does NOT do this.** When `device_ptr` is non-null (mmap path, the production case), set_conflicting on a tx never updates parent records' children lists.
**Why it matters:** The conflicting-children list is the engine's authoritative way to walk reorg-affected children from a parent. With the fast path silently skipping this, any reorg-detection logic that reads parent.conflicting_children will miss every conflicting tx that was set via the fast path (i.e. all of them in production). The behaviour is silently inconsistent between paths.
**Reproduction:** Create parent P. Create child C with conflicting=true and parent_txids=[P]. (This goes through `Engine::create` line 1739-1744, which DOES propagate.) Now create child C' (separate, not conflicting). Call `set_conflicting(C', true)`. Read P's conflicting children — only C is there, not C'. The fast path never appended.
**Suggested fix:** Move the parent propagation block out of the slow path (line 2520-2532) and run it after both paths. Drop the lock first (it does already), then iterate. Add a regression test that asserts fast-path `set_conflicting` updates parent children.

---

### A-22: `evaluate_delete_at_height` "all_spent != was_all_spent" branch is unreachable when DAH was already cleared and conditions are unmet (LOW)
**Location:** `src/ops/delete_eval.rs:163-180`
**What:** The post-conditional branch (lines 148-179) handles "no DAH set, conditions unmet, but the LAST_SPENT_ALL flag transitioned". The state-transition signaling logic emits AllSpent / NotAllSpent. There's no code path that emits `Signal::AllSpent` for a record that has blocks AND is on longest chain AND is now all-spent — that case is handled by the earlier `all_spent && has_blocks && on_longest_chain` branch (lines 112-145), which emits `Signal::None` (or `DeleteAtHeightSet` for external) and a DAH patch.

The branch at line 164-177 emits `Signal::AllSpent` only when conditions are not met (no blocks OR not on longest chain). Test `all_spent_no_blocks_no_dah` (delete_eval.rs:373-382) covers this. But there's no test for "all_spent && has_blocks && NOT on_longest_chain" — that's the case where a record was unmined, became all-spent. Likely produces `Signal::AllSpent` with no DAH (via `existing_dah == 0` path, then the all_spent != was_all_spent branch). One existing test (`unmined_tx_no_dah` line 446) does cover this and asserts `Signal::AllSpent`.

OK, branches look complete. Demoting to "verified" — no finding.
**Why it matters:** Documenting the verification.

---

### A-23: spent_count counter underflow protection in unspend ineffective if device counter is zero but slot is spent (LOW)
**Location:** `src/ops/engine.rs:1130-1133`
**What:**
```rust
let current = { metadata.spent_utxos };
if current > 0 {
    metadata.spent_utxos = current - 1;
}
```
This guards against underflow when `metadata.spent_utxos == 0` but the slot is observed SPENT (an inconsistent state from e.g. direct device write or replay corruption). Test `unspend_counter_not_below_zero` (engine.rs:4508-4530) verifies this guard. But the guard masks the inconsistency rather than reporting it. The slot is SPENT, the counter is 0, the unspend silently reports success without decrementing — and the on-disk metadata is now even more inconsistent (slot UNSPENT, counter 0, but the tx may still have other SPENT slots).
**Why it matters:** Defensive code that silently masks invariant violations makes debugging harder. The underlying inconsistency goes unreported.
**Reproduction:** Existing test `unspend_counter_not_below_zero` is exactly this scenario. It passes — but logs nothing.
**Suggested fix:** Add `tracing::error!(tx=?req.tx_key, offset=req.offset, "unspend on SPENT slot but counter==0 — record state corrupt");` in the guard. Or: return `SpendError::StorageError { detail: "counter desync" }` to surface the issue.

---

### A-24: Generation overflow can wrap to 0 silently (LOW)
**Location:** Many sites: `engine.rs:1007`, `engine.rs:1049`, `engine.rs:1150`, `engine.rs:1271`, `engine.rs:1478`, `engine.rs:2262`, `engine.rs:2446`, `engine.rs:2569`, `engine.rs:2664`, `engine.rs:2931`
**What:** Every mutation does `meta.generation = wrapping_add(1)`. After 2^32 mutations on the same record, generation wraps to 0. Replication watermark relies on generation increasing monotonically.
**Why it matters:** A long-lived hot record (e.g. a coinbase that's the spend target of many incrementally created spends) can hit this. Probability is low but the consequence is hard-to-debug replication divergence.
**Reproduction:** Force u32::MAX wrap → next mutation produces generation = 0; replicas with watermark > 0 ignore the update.
**Suggested fix:** Use `u64` for generation. Compile-time-asserted record layout currently has 4 bytes — would need a schema bump. Document the limitation in the meantime; flag as MEDIUM if production traffic could realistically hit this within the operational window.

---

### A-25: Spend `if let Ok(...)` pattern in set_conflicting hides cold-data parse errors (LOW)
**Location:** `src/ops/engine.rs:2525-2532`
**What:**
```rust
if let Ok(cold_bytes) = self.read_cold_data(&req.tx_key) {
    let parent_txids = extract_parent_txids_from_cold_data(&cold_bytes);
    for parent_txid in parent_txids {
        let parent_key = TxKey { txid: parent_txid };
        let _ = self.append_conflicting_child(&parent_key, req.tx_key.txid);
    }
}
```
Two error swallows. If `read_cold_data` errors (e.g. blob store unavailable for an external tx, or device I/O failure), the conflicting-children propagation is silently skipped. If `append_conflicting_child` fails on any parent, the rest are still attempted but errors are dropped.
**Why it matters:** Conflicting propagation is best-effort; partial success can leave the cluster in a state where some parents know about a conflict and others don't, with no audit trail.
**Reproduction:** Make read_cold_data fail (e.g. external tx with blob store offline). set_conflicting silently skips the propagation; subsequent reorg detection on parents misses this child.
**Suggested fix:** At minimum: `tracing::warn!(parent=?parent_key, err=?e, "set_conflicting: failed to append child")`. Better: collect failed parents into the response and let the operator retry.

---

### A-26: Reassign does not check if the slot was reassigned to FROZEN (e.g. concurrent freeze) (LOW)
**Location:** `src/ops/engine.rs:2231-2270`
**What:** The status check is `if slot.status != UTXO_FROZEN { return NotFrozen }` — i.e. it requires the slot to be FROZEN. After reassign, the slot becomes UNSPENT with a spendable_after marker. If two concurrent reassigns hit the same FROZEN slot, the stripe lock serializes them: the first wins, second sees status=UNSPENT and returns `NotFrozen`. That's fine. But: if the first reassign is followed by a freeze (now legal because slot is UNSPENT), and then a second reassign is sent with the original hash, the second reassign sees FROZEN status but the hash check fails (slot.hash is now the new_hash from first reassign, not original). The second returns `UtxoHashMismatch` correctly.

OK, this is also defensively correct. Not a finding. Documenting verification.

---

### A-27: `freeze` on already-FROZEN slot doesn't include hash mismatch check before status check (LOW)
**Location:** `src/ops/engine.rs:2179-2192`
**What:** Order:
1. Hash check (line 2176-2178): `if slot.hash != req.utxo_hash → UtxoHashMismatch`
2. Status check (line 2179-2192): match on status — UTXO_FROZEN → AlreadyFrozen, etc.

This ordering means the hash is validated before status. Good. If the caller sends the wrong hash for an already-FROZEN slot, they get `UtxoHashMismatch`, not `AlreadyFrozen`. This may differ from Lua. Without the Lua reference, can't verify which is correct.
**Why it matters:** Error code stability between paths.
**Suggested fix:** Verify against Lua if/when the file is restored.

---

### A-28: Test `concurrent_spend_same_utxo_same_data` doesn't verify all 100 threads see the actual stored spending_data (LOW)
**Location:** `src/ops/engine.rs:4131-4163`
**What:** All 100 threads spend with the same `(hash, sd)`. All return `Ok` (because idempotent re-spend is OK). The post-test asserts `meta.spent_utxos == 1`. It does NOT read the slot and verify `slot.spending_data == sd`.
**Why it matters:** Invariant 2 (spent UTXO spending data is stable across reads) is not directly tested. If a race produced an arbitrary spending_data for some loser, the test wouldn't catch it.
**Reproduction:** Existing test is fine but coverage is shallow.
**Suggested fix:** Add `let slot = engine.read_slot(&key, 5).unwrap(); assert_eq!(slot.spending_data, sd);`.

---

### A-29: Tombstone-on-delete races with allocator.free (MEDIUM)
**Location:** `src/ops/engine.rs:2701-2714`
**What:** Order:
1. Read metadata (line 2703).
2. Set `tombstone.magic = 0; tombstone.record_size = 0;`.
3. Write metadata (line 2706).
4. Allocator.free (line 2709-2714).
5. Unregister from index (line 2721).
6. Update secondary indexes (line 2735-2740).

If a crash happens between step 3 and step 4, the on-device record is tombstoned (good for crash-time recovery skipping), but the allocator's persistent freelist hasn't been updated. The space is leaked. Conversely: if the metadata write succeeds but the allocator's persistence fails (allocator persists at shutdown only — see `engine.persist_allocator`), a crash before next shutdown leaks the space too.

If a crash happens between step 4 and step 5, the index still shows the entry pointing to a region that's been freed by the allocator. Future allocations could overwrite this region. A subsequent read via the still-registered index entry would read garbage (or a different record's metadata if reallocated). This is mitigated by the tombstone — but only if the recovery path re-validates the magic on every index lookup, which it doesn't (the index entry is trusted; metadata read happens but with CRC check that fails on the zeroed magic).

Actually let's verify: `read_metadata_fast` returns the bytes, calls `TxMetadata::from_bytes` which validates CRC. The tombstone writes were done via `write_metadata_fast` which re-stamps CRC. So a tombstoned region has a valid CRC over a zero-magic header. A read of the tombstoned region succeeds with `magic=0, record_size=0`. The caller doesn't check magic explicitly. This is a recipe for confusion.
**Why it matters:** Mid-delete crash boundaries are not uniformly handled; leaks and stale-index reads are both possible.
**Reproduction:** Inject crash between line 2706 and 2709 — allocator state is unchanged, on-disk metadata is tombstone, index still has entry. Recovery from snapshot: index entry's record_offset points at tombstone. read_metadata returns magic=0. Caller doesn't error.
**Suggested fix:** Either (a) make the redo log entry for delete cover the entire mutation atomically — record_offset, record_size, key — and let recovery roll forward all three steps consistently; or (b) validate `meta.magic == METADATA_MAGIC` in the read paths and surface a clear `RecordError::Tombstoned` variant.

---

### A-30: SetMined fast path applies inline only when count=0 → reorg with overflow always takes slow path (LOW)
**Location:** `src/ops/engine.rs:1228`
**What:** Fast path condition: `!req.unset_mined && cached_count == 0 && !self.device_ptr.is_null()`. If the cached `block_entry_count >= 1`, set_mined goes to the slow path. This is correct and intentional, but it means in heavy-reorg scenarios where each tx is mined-unmined-mined repeatedly, only the first mining hits the fast path. Performance characteristic, not a correctness issue.
**Why it matters:** Performance, not correctness.

---

### A-31: Set_locked and set_conflicting fast/slow path divergence — slow path bumps generation on metadata, fast path bumps on cached entry (LOW)
**Location:** Fast path: `engine.rs:2569` (set_locked), `engine.rs:2446` (set_conflicting). Slow path: `engine.rs:2631`, `engine.rs:2505`.
**What:** Both paths produce the same on-disk generation. But the fast path computes `generation = entry.generation.wrapping_add(1)` from the cached value, while the slow path reads metadata and bumps `meta.generation`. If the cached entry has a stale generation (e.g. due to a previous freeze that didn't sync — see A-08), the fast path will produce a *lower* generation than the slow path would have produced from disk. The replicas tracking generation will see a backwards motion.
**Why it matters:** The cache-staleness (A-08) compounds with the fast-path generation source to produce visible regression.
**Reproduction:** Create tx, freeze (skips generation bump), set_locked via fast path. Compare generation to a parallel-universe run that took the slow path: fast path is 1 less.
**Suggested fix:** Fix A-08 (freeze syncs cache) → cached generation is always current → fast path produces correct value.

---

### A-32: spend_multi response.errors is HashMap — non-deterministic iteration when serialised (LOW)
**Location:** `src/ops/spend.rs:49`, `src/ops/engine.rs:824`
**What:** `errors: HashMap<u32, SpendError>`. Iteration order is non-deterministic. The dispatch eventually iterates and writes wire bytes — if the order differs between runs, the wire output differs. For tests that snapshot bytes, this is brittle. For replication, this could differ between primary and replica when both produce error sets.
**Why it matters:** Determinism in error reporting matters for cross-checking replication.
**Reproduction:** Run identical batches with multiple errors twice; observe error iteration order differs.
**Suggested fix:** Change to `BTreeMap<u32, SpendError>` or sort before serialising in dispatch.

---

### A-33: `Engine::pre_allocate_create` doesn't check `is_external` ↔ `external_ref` consistency before allocating (LOW)
**Location:** `src/ops/engine.rs:1761-1793`
**What:** `pre_allocate_create` calls `Self::external_ref_for_create(req)?` (line 1773) which returns `MissingExternalRef` if `is_external && external_ref.is_none()`. So this case is caught — good. But it allocates after the external_ref check (line 1786-1790). If the caller decides not to call `create_at_offset` (e.g. redo flush fails), the dispatcher must free. The doc comment says so. But if `external_ref_for_create` is changed to fail later (after allocation), the doc would silently lie. Brittle but currently correct.
**Why it matters:** Defensive concern.
**Suggested fix:** Add an internal invariant: every `pre_allocate_create` Err return must be before allocation. Currently true. Keep it that way via a comment.

---

### A-34: spend's idempotent re-spend writes metadata even when no state changed (LOW)
**Location:** `src/ops/engine.rs:1003-1022`
**What:**
```rust
UTXO_SPENT => {
    if slot.spending_data == req.spending_data {
        // Idempotent re-spend — increment generation to match
        // spend_multi behavior, then return current state.
        metadata.generation = ...
        metadata.updated_at = ...
        ...write metadata...
        ...sync_index_cache...
        return Ok(SpendResponse { signal: Signal::None, block_ids });
    }
```
Every idempotent re-spend = one device write (metadata) + one sync_index_cache. Under attack, an adversary spamming idempotent spends causes write amplification.
**Why it matters:** DoS amplifier. Performance and write-wear.
**Reproduction:** Send 1M spends with the same (txid, vout, hash, sd) — produces 1M metadata writes.
**Suggested fix:** Detect idempotent at the dispatch layer (the outermost public boundary) and short-circuit. Or skip the metadata write in idempotent cases (the test `idempotent_respend_increments_generation` would need updating).

---

## Questions / unverified

These items require running the code or having access to the missing Lua reference to confirm.

1. **Lua parity is unverifiable.** `specs/teranode.lua` is missing from the repository at audit time. Every `// Implements ... from teranode.lua line N–M` comment in the codebase cannot be cross-checked. If/when the Lua file is restored, A-07 (Pruned vs InvalidSpend), A-14 (spendable_after `>` vs `>=`), A-15 (coinbase strict-`>`), A-18 (zero-spending_height coinbase), and A-27 (freeze-on-frozen ordering) all need a Lua-reference re-check.

2. **Whether `freeze`'s lack of generation bump is a deliberate "freeze is internal-only" design choice** — the spec doc may say so. If yes, demote A-08 from HIGH to MEDIUM (still applies because cache desync is independently bad). If no, A-08 is HIGH as-stated.

3. **Whether the fast-path miss in `preserve_until` (A-12) ever fires in production** — depends on whether the production binary always uses memory-mapped device (`device_ptr != null`). The unit tests use `MemoryDevice` which sets `device_ptr` non-null (engine.rs:96). Production NVMe ditto. So A-12 likely fires in 100% of production traffic.

4. **Whether `delete()` is supposed to mark spent-by-this-tx UTXOs in parent records as PRUNED (A-20)**. The PRUNED status exists; the delete code never sets it. Lua reference would clarify. Right now the only code that writes UTXO_PRUNED is in tests (engine.rs:3465, 4537, 8027). Production has no path that produces PRUNED slots — meaning the entire "PRUNED with audit-trail spending_data" feature in `record.rs:46` is dead.

5. **Whether `metadata.spent_utxos` in `set_conflicting`'s fast path should be re-counted from slots after the flag flip** — the fast path uses `entry.spent_utxos` which reflects the slots' state. CONFLICTING flag does not change slot status; counter remains correct. Verified OK.

6. **Whether the dispatch layer's WAL-first spend pipeline correctly handles a spend that internally errored at validate time but successfully wrote redo entries for the OK items.** The validate path returns the `ValidatedSpend` with both errors and valid_spends populated. Apply runs through valid_spends. The redo entries should match valid_spends, not all items. If the redo entries include items that ultimately erred, recovery would replay those — but the slot state on disk would already have rejected them. Likely OK but worth a focused trace.

7. **Whether `read_metadata_fast`'s direct-pointer path (`io::read_metadata_direct`) validates CRC** — `record.rs::TxMetadata::from_bytes` does validate, but `read_metadata_direct` is a separate code path in `src/io.rs`. Reading that path was out-of-scope for this audit; if it skips CRC, the engine has yet another silent corruption window.

---

## Summary count

- CRITICAL (data loss / double-spend / silent corruption): **A-01, A-03, A-04** (3)
- HIGH (correctness bug under realistic conditions): **A-05, A-06, A-07, A-08, A-09, A-10, A-12** (7)
- MEDIUM (degraded behavior / important coverage gap): **A-02, A-11, A-13, A-21, A-29** (5)
- LOW (polish / hardening): **A-14, A-15, A-16, A-17, A-18, A-19, A-22, A-23, A-24, A-25, A-26, A-27, A-28, A-30, A-31, A-32, A-33, A-34** (18)

Total: **33 findings + verified-OK notes**.

The combination of A-01, A-03, A-04, A-08, A-12, and A-21 means that *under normal production load* — fast paths only, mmap device, occasional disk pressure or transient I/O glitch — TeraSlab can: silently lose spend writes, miscount spent_utxos in batches, accept unauthorized unspends, mis-track generations, bypass preserve_until, and skip conflicting-children propagation. Each individually is a correctness defect; in combination they are a UTXO store that is not safe to run as a Bitcoin SV consensus-critical backend without the fixes called out here.
