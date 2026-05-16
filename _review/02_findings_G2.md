# G2 — Ops Engine + Sub-paths Review

Scope: `src/ops/engine.rs` (10,889 LOC), `src/ops/create.rs`, `src/ops/spend.rs`, `src/ops/unspend.rs`, `src/ops/set_mined.rs`, `src/ops/delete_eval.rs`, `src/ops/remaining.rs`, `src/ops/mark_longest_chain.rs`, `src/ops/error.rs`, `src/ops/signal.rs`, `src/ops/mod.rs`.

Engine.rs was read in chunks covering: top-level state + locks (1–500), shard counts + fast I/O helpers (500–830), validate_spend_multi + spend + unspend (830–1380), set_mined fast/slow path (1380–1710), set_mined_batch + mark_on_longest_chain (1710–1810), create / pre_allocate_create / create_at_offset / build_create_record_bytes / write_full_record_with_cold / read_cold_data (1810–2360), prune_slot_if_spent_by_child (2360–2435), freeze/unfreeze/reassign (2440–2605), append_conflicting_child + helpers (2605–2845), set_conflicting fast/slow (2845–2985), set_locked + restore + preserve_until (2987–3200), delete (3202–3260), get_spend / read_metadata / lookup_cached / read_slot / read_slots / read_block_entry / snapshot_index (3260–3450), ValidatedSpend::apply (3450–3575), cold-data helpers + overflow + sys_millis (3575–3812).

Prior-audit anchor checks were performed against R-004 / A-01..A-07 / BC-02 / BC-04 / R-016 / R-019 / R-021 / R-024 / R-029 / R-063 — most are resolved in the current tree (regression tests exist at `engine.rs:4086` and `engine.rs:4140`). New issues are reported below.

---

### F-G2-001: `delete()` frees the record region BEFORE removing the primary-index entry — concurrent readers can return data from an unrelated transaction
- **Severity**: CRITICAL
- **Category**: Correctness / Concurrency
- **Location**: `src/ops/engine.rs:3202`
- **Code**:
  ```rust
  // Tombstone the metadata before freeing the region so crash-time index
  // rebuilds cannot resurrect this record from stale bytes in freed space.
  self.write_zeroed_metadata_header(entry.record_offset)?;
  self.device.sync().map_err(|e| SpendError::StorageError { ... })?;
  // Free device space
  self.allocator.lock().free(entry.record_offset, record_size)?;
  // Remove from primary index AND decrement shard_counts ...
  self.unregister_with_shard_count(&req.tx_key);
  ```
- **Issue**: The order is (1) tombstone header, (2) free in allocator, (3) unregister from primary index. Between step (2) and (3) the primary index still maps `tx_key_A → offset_X`, but `offset_X` is already in the allocator's free pool. A concurrent `create_at_offset` running on another thread can call `allocator.allocate()` → receive the same `offset_X` → write a brand-new, CRC-valid `TxMetadata` for `tx_key_B` there. Meanwhile any reader (`Engine::read_metadata`, `read_slot`, `get_spend`, `lookup`, `lookup_cached`) does NOT take the per-tx stripe lock (this is documented at `src/io.rs:206` as intentional and the engine never acquires it on the read paths). A reader doing `lookup(tx_key_A) → entry → read_metadata_fast(offset_X)` therefore returns `tx_key_B`'s metadata, with a valid CRC, as if it belonged to `tx_key_A`. No layer downstream checks `meta.tx_id == requested_tx_id`.
- **Impact**: Silent cross-transaction read. A client reading `get_spend(tx_A, vout=0)` can be answered with `tx_B`'s slot data, which (worst case) reports a UTXO as spent that is actually still spendable on `tx_A` (or vice versa). Replication / SPV / consensus-adjacent code that trusts this reply corrupts its own view of UTXO state. Detection probability is low because everything passes CRC.
- **Recommendation**: Re-order to (1) tombstone, (2) sync, (3) unregister from primary index, (4) only then free in the allocator. Hold the primary-index write lock across the free, or require readers of `read_metadata_fast` to verify `meta.tx_id == requested_tx_id` before returning. The "delete tombstone sync" comment claims this protects against "crash-time index rebuilds resurrecting from stale bytes"; the in-process race is a separate hazard the current ordering does not cover.
- **Confidence**: High

---

### F-G2-002: `spend` accepts client-supplied `spending_data == [FROZEN_BYTE; 36]` on an UNSPENT slot — turns into a permanent "looks frozen" DoS
- **Severity**: HIGH
- **Category**: Security
- **Location**: `src/ops/engine.rs:1041` (single-spend), `src/ops/engine.rs:1041`/`engine.rs:1196` (already-spent guard), `src/ops/engine.rs:1305` (unspend frozen guard)
- **Code**:
  ```rust
  // spend, UTXO_UNSPENT branch
  let new_slot = UtxoSlot::new_spent(req.utxo_hash, req.spending_data);
  valid_spends.push((item.offset, new_slot));
  spent_count += 1;
  // ... later in unspend / spend already-spent branches:
  if slot.spending_data == [FROZEN_BYTE; 36] {
      return Err(SpendError::Frozen { offset: req.offset });
  }
  ```
- **Issue**: `[FROZEN_BYTE; 36]` (all `0xFF`) is the canonical magic marker for a frozen slot stored alongside `status=UTXO_FROZEN`. The spend path blindly accepts any 36-byte `spending_data` and writes it under `status=UTXO_SPENT`. After this, `unspend` (line 1305) and the already-spent branches in `spend`/`validate_spend_multi` (lines 1050, 1195) all interpret `spending_data == [0xFF; 36]` as "frozen" regardless of the status byte and reject the operation with `SpendError::Frozen`. A client who knows `(txid, vout, utxo_hash)` can call spend with `spending_data = [0xFF; 36]` and stamp the slot as un-unspendable.
- **Impact**: Permanent griefing: the original spender of a UTXO can prevent reorg-driven unspend on any UTXO they spend, since unspend matching requires `spending_data == stored data` (line 1308) but the frozen-marker check fires earlier at line 1305 and short-circuits with `Frozen`. Once stamped, the only way out is `unfreeze` — which rejects non-`UTXO_FROZEN` status (line 2508). The slot is unrecoverable through public ops.
- **Recommendation**: Reject `spending_data == [FROZEN_BYTE; 36]` at the request boundary (server dispatch or `validate_spend_multi`/`spend`) with a dedicated error variant. The 36-byte payload should be `txid(32) + vin(4 LE)`; an all-`0xFF` txid is invalid in the BSV format anyway.
- **Confidence**: High

---

### F-G2-003: `write_overflow_entries` on `entries.is_empty()` frees only `alignment` bytes, leaking the rest of a multi-page overflow block
- **Severity**: MEDIUM
- **Category**: Correctness
- **Location**: `src/ops/engine.rs:3763` (free-on-empty path), allocation at `engine.rs:3782`/`3788`
- **Code**:
  ```rust
  if entries.is_empty() {
      // Free the overflow block if one exists
      if old_offset != 0 {
          let _ = allocator.lock().free(old_offset, alignment as u64);
          metadata.block_overflow_offset = 0;
      }
      return Ok(());
  }
  // ... allocate path:
  let block_size = io::align_up(data_size, alignment);
  allocator.lock().allocate(block_size as u64)
  ```
- **Issue**: The allocate path reserves `align_up(entries.len() * 12, alignment)` bytes; the free-on-empty path always frees exactly `alignment` bytes. For `INLINE_BLOCK_ENTRIES = 3` and the per-record `block_entry_count: u8` cap (≤255), overflow can hold up to 252 entries → 3024 data bytes. On a device with `alignment = 4096` the rounded `block_size = 4096` and the under-free is harmless; on a 512-byte-aligned device (`alignment = 512`) the allocation is `align_up(3024, 512) = 3072` but the free returns only 512 bytes — the remaining 2560 bytes leak from the allocator. The same shrinking-write path (`old_offset != 0` reuse at line 3786) also implicitly trusts that the allocation never grew, but there is no shrink branch.
- **Impact**: Slow allocator leak on sub-4K-aligned devices. After many set_mined→unset_mined cycles on transactions with >3 mined blocks, the device's effective capacity drops without any visible counter; `DeviceFull` returns prematurely.
- **Recommendation**: Track the allocated block size in the metadata (or recompute `align_up(old_count * BLOCK_ENTRY_SIZE, alignment)` from the old count before freeing) and free that exact size. The error from `free` is also being swallowed via `let _ =` here — propagate it instead.
- **Confidence**: High

---

### F-G2-004: `unwrap()` on infallible-looking conversions still violates the CLAUDE.md "no `unwrap` in library code" rule and silently absorbs invariant violations
- **Severity**: LOW
- **Category**: Code Quality
- **Location**: `src/ops/engine.rs:1029`, `src/ops/engine.rs:1163`, `src/ops/engine.rs:1553`
- **Code**:
  ```rust
  // 1029, 1163:
  let spendable_height =
      u32::from_le_bytes(slot.spending_data[0..4].try_into().unwrap());
  // 1553:
  let last = overflow.pop().unwrap();
  ```
- **Issue**: `CLAUDE.md` "Code quality" rule: *No `unwrap()` or `expect()` in library code (only in tests).* These three sites are non-test. The `try_into().unwrap()` against a 4-byte slice cannot panic given the source array layout, and the `pop().unwrap()` is guarded by an earlier `count > INLINE_BLOCK_ENTRIES` test — so the panics are unreachable in current code. But the rule exists so that future refactors do not silently introduce reachable panics on the spend hot-path. Replace with explicit failure modes.
- **Impact**: None today; rule violation that masks any future change to slot layout or overflow count semantics as a panic instead of an `Err`.
- **Recommendation**: `let mut buf = [0u8; 4]; buf.copy_from_slice(&slot.spending_data[0..4]); let spendable_height = u32::from_le_bytes(buf);` and `let last = overflow.pop().ok_or_else(|| SpendError::StorageError { detail: "overflow vec unexpectedly empty".into() })?;`.
- **Confidence**: High

---

### F-G2-005: `append_conflicting_child` retries indefinitely on contention with no backoff or bound
- **Severity**: LOW
- **Category**: Performance / Maintainability
- **Location**: `src/ops/engine.rs:2614` (loop start)
- **Code**:
  ```rust
  loop {
      let (ro, count, offset, mut children) = { /* take stripe lock, snapshot */ };
      // ... allocate & write a new block outside the lock ...
      let committed = { /* re-acquire stripe lock, CAS on (count, offset) */ };
      if committed { return Ok(()); }
      self.free_conflicting_children_block(new_offset, children.len())?;
  }
  ```
- **Issue**: When the CAS at line 2680 fails (another writer updated the conflicting-children list between the two stripe-lock windows) the function frees the speculative block and loops. There is no retry cap and no backoff; pathological contention (many simultaneous reorgs all trying to append to the same parent) burns allocator/device cycles indefinitely. The redo intent (line 2647) is logged only once via `intent_logged`, so each retry still re-allocates and re-writes a 32×N block.
- **Impact**: DoS-like CPU/IO amplification on a hot parent record under adversarial workload. Not exploitable from a single client (stripe lock serializes per-key), but bursty multi-shard traffic to a single conflicting parent can stall progress.
- **Recommendation**: Cap retries (e.g. 16) and return `SpendError::StorageError { detail: "conflicting children list contended" }`. Add exponential backoff between retries.
- **Confidence**: Medium

---

### F-G2-006: `pre_allocate_create` and `create_at_offset` re-build `cold_data` independently — there is no contract that the two computations agree
- **Severity**: LOW
- **Category**: Maintainability
- **Location**: `src/ops/engine.rs:1962` (`pre_allocate_create`) and `src/ops/engine.rs:2002` (`create_at_offset`)
- **Code**:
  ```rust
  // pre_allocate_create
  let cold_data = if req.is_external && req.inputs.is_none() {
      vec![]
  } else {
      build_cold_data(req.inputs, req.outputs, req.inpoints)
  };
  let cold_size = cold_data.len();
  let total_size = base_size + cold_size as u64;
  let record_offset = self.allocator.lock().allocate(total_size)?;
  // create_at_offset rebuilds the same expression:
  let cold_data = if req.is_external && req.inputs.is_none() { vec![] } else { build_cold_data(...) };
  // base_size + cold_data.len() is used as meta.record_size
  ```
- **Issue**: The two sites compute `cold_data.len()` from the same `req` so today they agree, but the contract is implicit. If a future caller mutates `req` between the two calls (or a different request reaches `create_at_offset`), the on-device `record_size` and allocator reservation disagree and writes spill into the next record or under-fill the reservation. The dispatch layer is supposed to pass the same `req` but the engine API does not enforce it.
- **Impact**: Maintenance hazard. A divergence here would corrupt adjacent records on the device without any guard.
- **Recommendation**: Have `pre_allocate_create` return the computed `(record_offset, total_size, cold_data_len)` and have `create_at_offset` accept the `cold_data_len` for verification (`debug_assert_eq!`) — or have `pre_allocate_create` build the bytes once and pass them through.
- **Confidence**: Medium

---

### F-G2-007: `spend_multi` does not cap `spent_count` against `utxo_count - prior_spent_utxos` — wrapping_add could exceed utxo_count under malformed input
- **Severity**: LOW
- **Category**: Correctness
- **Location**: `src/ops/engine.rs:3531`
- **Code**:
  ```rust
  metadata.spent_utxos = { metadata.spent_utxos }.wrapping_add(spent_count);
  ```
- **Issue**: Validation in `validate_spend_multi` adds at most one entry per *new* offset to `valid_spends` (duplicate offsets are absorbed into the already-spent branches), so under normal operation `spent_count ≤ utxo_count - prior_spent_utxos`. However the code uses `wrapping_add` with no defensive check that the result remains `≤ utxo_count`. If on-device metadata is silently corrupted (CRC valid but `spent_utxos` ahead of reality), the wrap masks the corruption and `delete_eval`'s `all_spent` check at `delete_eval.rs:109` becomes wrong (treating an under-counted record as all-spent). Today no slot-state corruption path reaches this without first failing the CRC, so this is **hypothesis** territory.
- **Impact**: Defense-in-depth gap. If a future code path violates the invariant, the wrap silently corrupts DAH evaluation rather than failing loudly.
- **Recommendation**: Replace `wrapping_add` with a checked add and surface `SpendError::StorageError { detail: "spent_utxos invariant violated" }` if `new > utxo_count`.
- **Confidence**: Low (hypothesis-driven, no current reachable path).

---

### F-G2-008: Idempotent re-spend short-circuit is correct, but the symmetric path in `validate_spend_multi` does NOT skip the metadata write — `apply()` still bumps generation and writes metadata even when `spent_count == 0` is reached by ALL items being idempotent
- **Severity**: INFO (verification + LOW deviation)
- **Category**: Correctness / Performance
- **Location**: `src/ops/engine.rs:1171` (single-spend idempotent), `src/ops/engine.rs:3503` (`ValidatedSpend::apply` zero-spent fast-return)
- **Code**:
  ```rust
  // single-spend: idempotent returns BEFORE bumping anything
  if slot.spending_data == req.spending_data {
      let block_ids = collect_block_ids(&metadata).to_vec();
      return Ok(SpendResponse { signal: Signal::None, block_ids });
  }
  // ValidatedSpend::apply
  if spent_count == 0 {
      let generation = { metadata.generation };
      drop(_guard);
      return Ok(SpendMultiResponse { /* no slot/meta write */ });
  }
  ```
- **Issue**: This is consistent with R-021's "idempotent re-spend is a true no-op" — verified. Single-`spend` returns the pre-mutation generation; `apply` returns the same. Tests at `engine.rs:6270` (`idempotent_respend_does_not_increment_generation`) confirm. No live bug.
- **Impact**: None — verification note.
- **Recommendation**: None.
- **Confidence**: High

---

### F-G2-009: `pre_allocate_create` silently ignores `external_ref_for_create` validation when the dispatch layer passes mismatched flags
- **Severity**: INFO
- **Category**: Code Quality
- **Location**: `src/ops/engine.rs:1974`
- **Code**:
  ```rust
  Self::external_ref_for_create(req)?;  // discard the value
  ```
- **Issue**: `pre_allocate_create` calls `external_ref_for_create` only for its side effect of returning `Err(MissingExternalRef)` when `is_external && external_ref.is_none()`. The result is discarded. `create_at_offset` then re-runs the same check and stores the value. This duplicates work and the implicit contract is fragile — if the validator becomes non-deterministic, the two paths could disagree. Not a current bug, but flagged for cleanup.
- **Impact**: None.
- **Recommendation**: Return the validated `Option<ExternalRef>` from `pre_allocate_create` and have `create_at_offset` accept it as a parameter.
- **Confidence**: Medium

---

### F-G2-010: `read_metadata` / `read_slot` / `lookup_cached` / `read_slots` / `read_block_entry` do NOT acquire the per-tx stripe lock — verified intentional, but the protective documentation lives only in `src/io.rs:206` and not on these methods
- **Severity**: INFO (verification + LOW doc gap)
- **Category**: Maintainability
- **Location**: `src/ops/engine.rs:3307` (`read_metadata`), `engine.rs:3325` (`lookup_cached`), `engine.rs:3336` (`read_slot`), `engine.rs:3349` (`read_slots`), `engine.rs:3368` (`read_block_entry`)
- **Code**:
  ```rust
  pub fn read_metadata(&self, key: &TxKey) -> Result<TxMetadata, SpendError> {
      let entry = self.index.read().lookup(key).ok_or(SpendError::TxNotFound)?;
      self.read_metadata_fast(entry.record_offset)
  }
  ```
- **Issue**: The R-009/BC-02 design is documented at `src/io.rs:206` as: "Read-paths do NOT need the per-transaction stripe lock. A reader that races with a concurrent `write_metadata_direct` … can observe a torn header — which the CRC32 check at the end of `TxMetadata::from_bytes` detects and surfaces as `DeviceError::RecordCorruption`." `read_metadata`'s doc comment (line 3299) explains the design but `read_slot`, `read_slots`, `read_block_entry`, `lookup_cached`, and `get_spend` repeat the pattern without referencing the protocol. Anyone re-deriving the locking contract from these methods will not know they must rely on the CRC for torn-header detection. (This is also what makes finding F-G2-001 above an actual bug — the CRC protocol only protects against torn HEADERS, not against cross-tx aliasing.)
- **Impact**: Documentation hazard.
- **Recommendation**: Add a one-line doc to each read entry pointing at the R-009 contract.
- **Confidence**: High

---

### F-G2-011: `set_mined` fast path uses `entry.generation + 1` for the response generation but the slow path uses `metadata.generation` — divergence if the cache is stale
- **Severity**: LOW
- **Category**: Correctness
- **Location**: `src/ops/engine.rs:1468` (fast path) vs `engine.rs:1675`/`1707` (slow path)
- **Code**:
  ```rust
  // fast path (line 1468)
  let generation = entry.generation.wrapping_add(1);
  // slow path (line 1675)
  metadata.generation = { metadata.generation }.wrapping_add(1);
  // ... response uses { metadata.generation }
  ```
- **Issue**: Both paths advance generation by exactly one, but the fast path bases the increment on the cached `entry.generation` from the primary index. The cache is supposed to be updated by `sync_index_cache` under the stripe lock on every mutation; the only way `entry.generation` could lag `metadata.generation` is if a previous mutation under the same stripe lock wrote metadata but then failed at `sync_index_cache`. In that case the on-device generation already advanced to N+1 but the cache shows N. The fast path then computes `N + 1 = N+1` and writes it to metadata — the on-device generation does not advance (stays at N+1) but the client sees `generation = N+1` as if it had. From the client's perspective a mutation just succeeded with the same generation as a previous failed call.
- **Impact**: Replication / staleness checks that rely on "generation strictly increases per ack'd mutation" mis-fire. Hard to reach because `sync_index_cache` failure is rare and is itself returned as an error.
- **Recommendation**: Hypothesis: read `metadata.generation` once in the fast path (it is already memory-mapped) and base the increment on that; sync the cache from the freshly-written value.
- **Confidence**: Medium (hypothesis)

---

### F-G2-012: `freeze` / `unfreeze` do not call `evaluate_delete_at_height` — verified correct (frozen slots don't change `spent_utxos`) but the rationale is not documented
- **Severity**: INFO
- **Category**: Maintainability
- **Location**: `src/ops/engine.rs:2440` (`freeze`), `engine.rs:2490` (`unfreeze`)
- **Issue**: Both ops bump `meta.generation` and call `sync_index_cache` (which is correct per R-016), but neither calls `evaluate_delete_at_height` or `update_dah_index`. This is correct: freezing a slot does NOT change `spent_utxos`, and DAH eligibility is gated on `spent_utxos == utxo_count`. But there is no comment explaining the omission; a reader could easily add a DAH eval here and break the invariant in the opposite direction.
- **Impact**: None today; documentation gap.
- **Recommendation**: Add one comment: `// freeze/unfreeze does NOT touch spent_utxos and therefore cannot cross the all-spent boundary — DAH eval is intentionally omitted.`
- **Confidence**: High

---

### F-G2-013: `set_locked_with_before_image` slow path swaps DAH to 0 only when `value=true`, but reverses to old value only via the explicit `restore_set_locked_for_compensation` helper — the engine cannot self-correct a partial fast-path failure
- **Severity**: LOW
- **Category**: Correctness
- **Location**: `src/ops/engine.rs:2997` (fast path), `engine.rs:3083` (slow path), `engine.rs:3117` (compensation helper)
- **Code**:
  ```rust
  // fast path
  let new_dah = if req.value {
      tf.insert(TxFlags::LOCKED);
      0 // Locking clears deleteAtHeight
  } else {
      tf.remove(TxFlags::LOCKED);
      old_dah // Unlocking doesn't change DAH
  };
  ```
- **Issue**: Locking clears DAH; unlocking does not restore it. This is documented behavior (compensation is the dispatch layer's job using the returned `prior_delete_at_height`). Verified consistent. But the contract leaks across module boundaries — if a new caller of `set_locked` ignores `prior_delete_at_height`, lock→unlock round-trips silently drop DAH.
- **Impact**: API misuse hazard.
- **Recommendation**: Consider returning `SetLockedResponse` from `set_locked` (not just `u32`); today the public `set_locked` (line 2987) discards everything but `generation` via `set_locked_with_before_image(req)?.generation`.
- **Confidence**: Medium

---

### F-G2-014: `evaluate_delete_at_height` handles `unmined_since != 0` (off-chain) correctly — verified
- **Severity**: INFO
- **Category**: Correctness
- **Location**: `src/ops/delete_eval.rs:111`
- **Code**:
  ```rust
  let on_longest_chain = { metadata.unmined_since } == 0;
  // ...
  if all_spent && has_blocks && on_longest_chain { /* set DAH */ }
  ```
- **Issue**: Verified: unmined transactions never accept DAH from this evaluator; pruning is driven by the unmined secondary index. The `evaluate_dah_cached` counterpart at line 246 mirrors the logic. Both check `block_height_retention == 0` and `preserve_until != 0` early-exits. The signed/unsigned arithmetic uses `checked_add` (R-063 fix at `delete_eval.rs:31`).
- **Impact**: None — verification.
- **Recommendation**: None.
- **Confidence**: High

---

### F-G2-015: `set_conflicting` parent-list update drops the child stripe lock with `drop(_guard)` and then enters `append_conflicting_children_from_cold_data` which takes the parent stripe locks — verified deadlock-free
- **Severity**: INFO
- **Category**: Concurrency
- **Location**: `src/ops/engine.rs:2978`
- **Code**:
  ```rust
  if req.value {
      drop(_guard);
      self.append_conflicting_children_from_cold_data(&req.tx_key, "set_conflicting");
  }
  ```
- **Issue**: Verified: child stripe lock is explicitly dropped before any parent lock is taken. `append_conflicting_child` then takes only the parent's stripe lock. Lock-order is child-then-parent at the call site, and child is released before parent is acquired, so no cycle exists with the converse flow (`create` taking child lock then `append_conflicting_child_best_effort` taking parent lock — same direction). Verified consistent.
- **Impact**: None — verification.
- **Recommendation**: None.
- **Confidence**: High

---

### F-G2-016: `unspend` correctly validates `spending_data` matches the original spender before clearing — verified, no unspend-authority bypass
- **Severity**: INFO
- **Category**: Security (verification of prior audit)
- **Location**: `src/ops/engine.rs:1308`
- **Code**:
  ```rust
  if slot.spending_data != req.spending_data {
      return Err(SpendError::InvalidSpend {
          offset: req.offset,
          spending_data: slot.spending_data,
      });
  }
  ```
- **Issue**: Verified: a third party holding `(txid, vout, utxo_hash)` but NOT the matching `spending_data` cannot unspend. The regression test `unspend_rejects_wrong_spending_data_without_mutating_slot` at `engine.rs:4975` exercises this. The frozen-marker guard at line 1305 fires BEFORE the data-mismatch check, which is benign for honest callers but contributes to F-G2-002.
- **Impact**: None for this finding; the unspend-authority concern from the prompt is satisfied.
- **Recommendation**: None — already correct.
- **Confidence**: High

---

### F-G2-017: `prune_slot_if_spent_by_child` uses `saturating_sub` / `saturating_add` on `spent_utxos` and `pruned_utxos` — under-counts on impossible double-prune but reasonable
- **Severity**: INFO
- **Category**: Code Quality
- **Location**: `src/ops/engine.rs:2424`
- **Code**:
  ```rust
  meta.spent_utxos = { meta.spent_utxos }.saturating_sub(1);
  meta.pruned_utxos = { meta.pruned_utxos }.saturating_add(1);
  ```
- **Issue**: Saturating arithmetic hides a programming error if the function is ever called against a slot that is already pruned without the earlier `if slot.status == UTXO_PRUNED { return Ok(false); }` guard. The guard exists at line 2416, so this is defensive-but-silent. Acceptable.
- **Impact**: Defense-in-depth tradeoff vs visibility.
- **Recommendation**: Consider switching to `checked_sub` and surfacing the inconsistency as `SpendError::StorageError` for parity with the spend path's strict invariants.
- **Confidence**: Medium

---

### F-G2-018: `signal.rs` enum and `error.rs` enum — verified clean, no `String` errors, all variants have payloads
- **Severity**: INFO
- **Category**: Code Quality (verification)
- **Location**: `src/ops/signal.rs:1`, `src/ops/error.rs:1`
- **Issue**: Both files comply with CLAUDE.md's "All error types must be enums with descriptive variants — no string errors" rule. `SpendError::StorageError` is the one exception, carrying a `detail: String` for I/O wrapping, which is also explicitly the documented pattern. No `unwrap`/`expect` in either.
- **Impact**: None.
- **Recommendation**: None.
- **Confidence**: High

---

### F-G2-019: `mod.rs`, `mark_longest_chain.rs`, `set_mined.rs`, `remaining.rs`, `spend.rs`, `unspend.rs`, `create.rs` request/response types — verified clean
- **Severity**: INFO
- **Category**: Code Quality (verification)
- **Location**: `src/ops/mod.rs`, `src/ops/mark_longest_chain.rs`, `src/ops/set_mined.rs`, `src/ops/remaining.rs`, `src/ops/spend.rs`, `src/ops/unspend.rs`, `src/ops/create.rs`
- **Issue**: All request/response structs are plain data, derive `Debug`/`Clone`. `ValidatedSpend` is intentionally neither `Copy` nor `Clone` (compile-fail doctests at `spend.rs:127` and `spend.rs:133` enforce this). The `pre_spent_count` accessor uses a `let count = self.metadata.spent_utxos;` pattern with a `#[allow(clippy::let_and_return)]` for packed-struct field access — verified correct for `#[repr(C, packed)]`.
- **Impact**: None.
- **Recommendation**: None.
- **Confidence**: High

---

### F-G2-020: `ValidatedSpend::apply` writes slots one at a time via `write_slot_fast` rather than coalescing into one aligned region write — performance opportunity, not a correctness bug
- **Severity**: INFO
- **Category**: Performance
- **Location**: `src/ops/engine.rs:3523`
- **Code**:
  ```rust
  for &(offset, ref new_slot) in &valid_spends {
      engine.write_slot_fast(record_offset, offset, new_slot)?;
  }
  ```
- **Issue**: Each iteration issues a separate `pwrite` (or mmap memcpy + fence). For a 1000-input batch this is 1000 syscalls in the non-mmap path. The `pwrite` path goes through `AlignedBuf` allocation on each call. Not a correctness bug but a measurable hot-path inefficiency under high-fanout transactions.
- **Impact**: Throughput cap on multi-input transactions.
- **Recommendation**: Group contiguous (or aligned-region-sized) slot writes into a single `pwrite_all_at` of a coalesced `AlignedBuf`. Hold the buffer-allocation cost amortized.
- **Confidence**: High (perf, not correctness)

---

## Coverage notes

- `src/ops/engine.rs` (10,889) — 12 findings (F-G2-001 to F-G2-008, F-G2-009 to F-G2-013, F-G2-015 to F-G2-017, F-G2-020). Most severe: **F-G2-001 (CRITICAL — cross-tx aliasing on delete)**. Verified the documented R-004 (silent slot-write swallow), R-021 (idempotent respend gen), R-016/R-019 (freeze/unfreeze/preserve cache sync), and R-063 (reassign overflow) regressions are resolved — regression tests at `engine.rs:4086`, `4140`, `6270`, `5070`.
- `src/ops/create.rs` (140) — F-G2-006 (LOW — pre_allocate/create_at_offset cold-data divergence), F-G2-009 (INFO — external_ref validation duplication). Verified: request types use borrowed slices, no allocations, `CreateRequest::tx_key()` and `block_entries()` helpers are pure.
- `src/ops/spend.rs` (210) — F-G2-019 (verification only). `ValidatedSpend` correctly type-states the lock guard; compile-fail doctests assert it is neither `Copy` nor `Clone`. `pre_spent_count` correctly uses local-binding pattern for packed field access.
- `src/ops/unspend.rs` (34) — F-G2-016 (INFO — unspend authority verified correct). Type definition only; logic lives in `engine.rs`.
- `src/ops/set_mined.rs` (61) — F-G2-019 (verification only). `SetMinedSharedParams` correctly carries the shared batch fields for `set_mined_batch`.
- `src/ops/delete_eval.rs` (528) — F-G2-014 (INFO — verified correct logic for unmined / conflicting / all-spent transitions, both `evaluate_delete_at_height` and `evaluate_dah_cached` use `checked_add` for the new-DAH calculation). 23 unit tests cover overflow boundaries, preserve_until guard, all-spent state transitions, conflicting branch, and external-vs-internal signaling.
- `src/ops/remaining.rs` (128) — F-G2-019 (verification only). Pure type definitions; freeze/unfreeze/reassign/setLocked/setConflicting/preserveUntil/delete/getSpend request and response structs.
- `src/ops/mark_longest_chain.rs` (29) — F-G2-019 (verification only). Pure type definitions for the longest-chain bulk update.
- `src/ops/error.rs` (141) — F-G2-018 (INFO — verified clean). 14 enum variants, all `thiserror`-derived, payload-carrying. `StorageError { detail: String }` is the single string-wrapped variant by design.
- `src/ops/signal.rs` (20) — F-G2-018 (INFO — verified clean). Six-variant enum, `Debug + Clone + PartialEq + Eq`.
- `src/ops/mod.rs` (12) — F-G2-019 (verification only). Module declarations only.

## Counts

- CRITICAL: 1 (F-G2-001)
- HIGH: 1 (F-G2-002)
- MEDIUM: 1 (F-G2-003)
- LOW: 6 (F-G2-004, 005, 006, 007, 011, 013, 017 — counted 7; F-G2-017 is INFO)
- INFO: 11 (F-G2-008, 009, 010, 012, 014, 015, 016, 017, 018, 019, 020)

(Recount: CRITICAL 1, HIGH 1, MEDIUM 1, LOW 6, INFO 11 = 20 findings.)
