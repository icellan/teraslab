# Category A — UTXO Correctness Invariants

Auditor scope: `src/ops/*.rs`, `src/record.rs`, `src/locks.rs`, and the dispatch/test
surfaces that exercise them. Method: walked each op from wire decode → lock → index
lookup → device read → mutation → redo append → device write → response, and read the
backing tests to judge whether they actually prove the invariant. No cargo run (build
lock held by orchestrator); all conclusions are from source reading.

Bottom line: the single-UTXO double-spend invariant is solid and well-tested
(`g2_atomic_apply.rs` is a genuine stress proof, not a vacuous assertion). The one real
defect I found is on the **create** path: the duplicate-txid check is not atomic with the
index insert and `create` takes no per-tx lock, so concurrent creates of the same txid
can both "succeed", overwrite the index, orphan a record, and leak its allocation. Its
regression test is too weak to catch it.

---

### [HIGH] Create duplicate-txid guard is not atomic with index insert; concurrent same-txid creates both succeed

**Location:** `src/ops/engine.rs:2058-2195` (`create`), `:2279-2428`
(`create_at_offset_inner`), `:2064-2069` / `:2294-2296` (the lookup guard),
`:670-710` (`register_with_shard_count` → `register_without_resize` → `hashtable.rs:893
insert`, which **overwrites** on existing key). Weak test:
`src/ops/engine.rs:11514 concurrent_create_duplicate_txid`.

**What's wrong:** `create` (and `create_at_offset_inner`) implement duplicate rejection as
a read-then-write that is **not** serialized:

```
if self.index.read().lookup(&key).is_some() { return Err(DuplicateTxId); }  // 2067
... allocate ... write_full_record_with_cold ...
self.register_with_shard_count(key, index_entry)  // 2170  -> table.insert() OVERWRITES
```

None of `create`, `pre_allocate_create`, or `create_at_offset_inner` acquire the per-tx
stripe lock (`self.locks.lock(&key)` — confirmed absent; every other mutating op at
engine.rs:1055/1302/1490/1629/2793/2847/2881/3537/3961 takes it, create does not).
`HashTable::insert` (`hashtable.rs:910-915`) silently replaces an existing entry and
returns the old one — it is *not* insert-if-absent. So two threads creating the same txid:

- T1 `lookup` → None; T2 `lookup` → None (both before either registers).
- T1 allocates offset X, writes a full record at X, registers (count++ → index→X).
- T2 allocates offset Y, writes a full record at Y, registers → `insert` **overwrites**
  index→Y. `register_with_shard_count` sees `len` unchanged so `inserted=false` and the
  shard count is not double-counted (so counters stay consistent), but **both calls
  return `Ok`**, record X is now unreachable, and X's allocation is never freed.

This violates the spec/checklist invariant "Create rejected on duplicate txid
(ALREADY_EXISTS) without partially mutating state" under concurrency: the duplicate is
*not* rejected, and the store is left with an orphaned on-device record plus a permanent
allocator leak. The production dispatch path
(`dispatch.rs:3723` Phase-1 lookup, then `dispatch.rs:3853 create_at_offset` which
re-checks via the same non-atomic lookup) inherits the identical window across two
concurrent `OP_CREATE_BATCH` requests for the same txid.

**Why it matters:** Not a double-spend (the overwrite only fires while both records are
still pristine/unspent — once T1's record is registered any later create's `lookup`
returns `Some` → `DuplicateTxId`, and spends can only land on the indexed record). But it
is silent state corruption: an unreachable record consuming device space forever, an
allocator reservation that `delete` will never reclaim (delete reads the *indexed* offset
Y and frees only Y), and a broken idempotency contract that callers rely on. On a
device-scan-based freelist rebuild (spec §3.18 / line 937 describes rebuilding the
freelist by walking occupied offsets) the orphan X is a CRC-valid `TxMetadata` for the
same txid as Y, which is exactly the kind of stale-bytes resurrection the delete-tombstone
logic (engine.rs:3973) works hard to prevent elsewhere.

**Reproduction:** The existing test `concurrent_create_duplicate_txid` already races 10
threads on one txid but only asserts `successes >= 1` and `duplicates > 0` — it permits
N>1 successes, so it passes today while masking the bug. Strengthen it to prove the
invariant:

```rust
// after the 10-thread scope join:
assert_eq!(successes, 1, "exactly one create may win for a given txid");
assert_eq!(duplicates, 9, "all other concurrent creates must observe DuplicateTxId");
// and prove no allocation leaked: with one record of 5 utxos, only one region is live
let live = engine.allocator_stats();
// assert the allocated byte count equals exactly one record_size_for(5)+cold
```

With the current code this fails intermittently: multiple threads observe `None` in the
unlocked window and all return `Ok`, leaving 2+ allocated regions for one txid.

**Suggested fix:** Make duplicate-rejection and insert one atomic step. Either (a) take
`self.locks.lock(&key)` across the `lookup` … `register` span in `create` /
`create_at_offset_inner` (matches every other mutating op and is the smallest change), or
(b) add an `insert_if_absent` to the primary backend and have `register_with_shard_count`
return `DuplicateTxId` when the key already exists, freeing the just-reserved region on
that path. Option (b) also closes the dispatch-level window without a second lock
round-trip.

---

### [LOW] `engine.create()` leaks the allocation when index registration fails

**Location:** `src/ops/engine.rs:2154-2173` (and the same shape at `:2386-2404` in
`create_at_offset_inner`).

**What's wrong:** After `write_full_record_with_cold` succeeds, if
`register_with_shard_count` returns `Err`, `create` maps it to
`CreateError::StorageError` and returns **without freeing the region allocated at
`record_offset`** (engine.rs:2087). The dispatch batch path compensates for this on the
`create_at_offset` callers via `release_create_reservation` in its catch-all `Err(_)`
arm (`dispatch.rs:3956-3963`), but the *direct* `engine.create()` callers —
`cluster/coordinator.rs:7151/8308/8527/8711` and `replication/receiver.rs:1012` — have no
such rollback, so a register failure permanently leaks one record's worth of device space.

**Why it matters:** No correctness/double-spend impact (the record is unreachable and the
counter stays consistent), but a slow allocator leak on the replication/cluster ingest
paths under index-write failure. Strictly resource hygiene.

**Reproduction:** Inject a register failure (the engine already has a test-only
`fail_next_register` flag, engine.rs:680) and call `engine.create()` directly, then assert
`allocator_stats().allocated_bytes` returned to its pre-call value. It will not.

**Suggested fix:** In `create` / `create_at_offset_inner`, on the
`register_with_shard_count` error branch, `self.allocator.lock().free(record_offset,
total_size)` before returning the error (the `total_size` is already in scope at the
allocation site; thread it down).

---

### [LOW] Unspend writes the slot before decrementing the counter — interrupt leaves an over-count (unsafe direction) on non-WAL paths

**Location:** `src/ops/engine.rs:1547-1549` then `:1568-1588`.

**What's wrong:** `unspend` writes the UNSPENT slot first (engine.rs:1548) and only then
decrements `spent_utxos` and writes metadata (1549/1584-1588). If the metadata write fails
or the process is interrupted between the two, the slot is UNSPENT while the counter still
counts it as spent — an **over-count**. Because `evaluate_delete_at_height` gates on
`spent_utxos == utxo_count` (delete_eval.rs:109), an over-count is the *unsafe* direction:
a later op on the same record could observe a false all-spent and set `delete_at_height`,
making the record prematurely prunable. (Spend has the same two-write shape but in the
*safe* direction — slot SPENT first, counter incremented after, so an interrupt
under-counts and never falsely triggers all-spent: engine.rs:1447/1451.)

**Why it matters:** Bounded: every production unspend flows through the WAL-first dispatch
path (`dispatch.rs:3180 UnspendV2` redo with `new_spent_count`, fsynced before the engine
mutation) and recovery re-derives the counter from the actual on-device slot states
(`recovery.rs:1075 saturating_sub`), so a crash is repaired on replay. The exposure is
only a live, non-crash device-write failure on the direct `engine.unspend()` callers
(replication receiver, compensation). Real but narrow.

**Reproduction:** With a failable device, force the metadata pwrite to fail after the slot
write in `unspend`; observe `spent_utxos` left one too high relative to the count of SPENT
slots, then run a DAH eval and watch it flip to all-spent. (Specify as a targeted test
using the existing `make_engine_with_failable_device` harness at engine.rs:4939.)

**Suggested fix:** Acceptable as-is given WAL coverage; if hardening, fold the slot and
metadata writes behind a single ordered failure point or document the invariant that the
direct path must only be reached under the dispatch WAL.

---

## Checklist disposition

- ✅ **A UTXO can be spent at most once; concurrent spends → exactly one OK, N−1
  ALREADY_SPENT with correct existing spending_data.** Lock path:
  `validate_spend_multi`/`spend` take `self.locks.lock(&tx_key)` (engine.rs:1055/1302) for
  the full read→validate→write→metadata→sync sequence; the stripe is derived from txid
  bytes 16-23 (`locks.rs:115`) so same-txid ops always serialize on one mutex. The
  already-spent branch returns `AlreadySpent { spending_data: slot.spending_data }`
  (engine.rs:1420/1233) — the *existing* on-disk spender, and the wire encoder ships those
  36 bytes (`dispatch.rs:6579`). Proven by `tests/g2_atomic_apply.rs`
  (`concurrent_spend_same_utxo_yields_exactly_one_winner`, 16 threads × 200 iters,
  distinct spending_data, asserts exactly one OK and the on-device slot == the winner) and
  `engine.rs:11453 concurrent_100_threads_spend_same_utxo_different_data`, plus wire-level
  `server_tcp.rs:662` (`err.error_data == winner_spending_data`).

- ✅ **Spent UTXO's spending_data is stable across reads.** `get_spend` returns
  `slot.spending_data` for SPENT/FROZEN/PRUNED (engine.rs:4066-4071); once SPENT the slot
  is only mutated by unspend (clears) or prune (preserves the bytes), so repeated reads are
  identical. Evidence: `engine.rs:10846 get_spend_spent` asserts the exact 36 bytes;
  `get_spend_is_readonly` (engine.rs:10950) proves the read does not mutate.

- ✅ **Unspend is exact inverse of Spend and only succeeds on the recorded spending_data.**
  `unspend` rejects with `InvalidSpend { spending_data: slot.spending_data }` unless
  `slot.spending_data == req.spending_data` (engine.rs:1530-1535), rejects the frozen
  sentinel and PRUNED, and decrements the counter by exactly one (1549). Evidence:
  `engine.rs:5909 unspend_rejects_wrong_spending_data_without_mutating_slot`,
  `:5967 unspend_decrements_counter`, `:6539 unspend_rejects_spent_slot_when_counter_is_zero`,
  wire `server_tcp.rs:531` (`err.error_data == good_spending_data`).

- ❌ **Create rejected on duplicate txid without partial mutation.** Single-threaded: yes —
  `DuplicateTxId` at engine.rs:2068, `create_duplicate_txid` test at :9250, and the batch
  path frees the reservation on the race-detected duplicate (`dispatch.rs:3930`). Under
  concurrency the guard is non-atomic and both creates can win → see **[HIGH]** above. The
  backing concurrency test (`engine.rs:11514`) is too weak (permits N>1 successes) to catch
  it.

- ✅ **Delete of a tx with spent UTXOs — handled (not rejected), per spec.** `delete`
  (engine.rs:3960) unconditionally tombstones header → syncs → unregisters → frees, with no
  spent-state guard, matching spec §3.18 (Delete = index removal + freelist + blob cleanup;
  the all-spent/DAH gating that makes deletion safe lives in the pruner, not in `delete`).
  Deleting a *child* first prunes the parent slots the child spent
  (`dispatch.rs:4984-5034` → `prune_slot_if_spent_by_child`, engine.rs:2706), which is the
  spent-UTXO handling. Compensation snapshots full per-slot state incl. status+spending_data
  (`dispatch.rs:4899-4960`, R-007) so a failed-delete rollback cannot resurrect a spent slot
  as UNSPENT. Intended behavior matches impl.

- ✅ **Coinbase maturity = 100, COINBASE_IMMATURE carries required height.** Check is
  `IS_COINBASE && spending_height>0 && spending_height > current` (engine.rs:1076/1323),
  returning `CoinbaseImmature { spending_height, current_height }`; wire ships
  `spending_height` (4 LE) at `dispatch.rs:6571-6576`. Spendable at `current >=
  spending_height`. The `+100` lives in the create contract (`spending_height = blockHeight
  + 100`, spec line 95/403; req field `create.rs:67`). Evidence:
  `engine.rs:5169 spend_immature_coinbase`, `:5185 spend_mature_coinbase_equal` (boundary
  spendable), `:5205 spend_coinbase_zero_spending_height_boundary`,
  `tests/integration.rs:1218 coinbase_maturity`. (Note: the literal 100 is the caller's
  responsibility — the store stores/compares the precomputed `spending_height`, it does not
  recompute `+100`; consistent with spec and teranode.lua semantics.)

- ✅ **Frozen / locked / conflicting respected on every spend path incl. Reassign.** Spend
  (engine.rs:1069-1084/1316-1331), spendMulti (same, batch), and Reassign (R-017,
  engine.rs:2906-2921 checks CONFLICTING, LOCKED, coinbase maturity) all enforce the
  record-level flags; the slot-level FROZEN/PRUNED states are rejected per-item
  (engine.rs:1224-1257/1417-1431). Evidence: `spend_conflicting_blocked` (:5135),
  `spend_locked_blocked` (:5152), `spend_frozen_utxo` (:5259), `reassign_rejects_locked`
  (:9954), `reassign_rejects_conflicting` (:9984), `reassign_rejects_immature_coinbase`
  (:10057), `set_conflicting_blocks_spend` (:10426), `locked_blocks_spend` (:10574).

- ✅ **Reassign cooldown (FROZEN_UNTIL) enforced.** Reassign writes `spendable_height =
  block_height + spendable_after` (checked_add, ReassignOverflow on wrap) into
  `spending_data[0..4]` (engine.rs:2938-2945); spend rejects with `FrozenUntil {
  spendable_at_height }` when `spendable_height != 0 && spendable_height > current`
  (engine.rs:1178/1352), half-open `[0, spendable_height)`. Evidence:
  `reassign_not_spendable_until_cooldown` (:10168),
  `reassign_spendable_height_boundary_at_exact_height` (:10217),
  `reassign_overflow_checked_add_rejects_u32_max` (:10019), wire
  `server_tcp.rs:606` (`err.error_data == 1_010u32.to_le_bytes()`). Caveat: the
  reassignment audit-trail extension block described in spec §2.7/§3.9 step 5 is not
  written (only `reassignment_count` is bumped, engine.rs:2950; `reassignment_offset`
  stays 0) — that is an audit/observability gap, not a UTXO-correctness gap, so out of
  scope for category A but worth a note for the spec-conformance auditor.

- ✅ **vout ≥ slot count → VOUT_OUT_OF_RANGE on every vout-bearing op.** Range check present
  in spend (engine.rs:1131/1334), unspend (:1504), freeze (:2802), unfreeze (:2856),
  reassign (:2890), get_spend (:4046), prune_slot_if_spent_by_child (:2718, returns
  no-op). All surface `UtxoNotFound` → wire `ERR_VOUT_OUT_OF_RANGE`
  (`dispatch.rs:6577`, and the get path at :6249). Evidence: `spend_offset_out_of_range`
  (:5362), `get_spend_offset_out_of_range` (:10933), wire `server_tcp.rs:412`. set_mined
  and mark_on_longest_chain take no vout (N/A).

- ✅ **UTXOHash mismatch → UTXO_HASH_MISMATCH, no mutation.** Hash is compared before any
  write in spend (engine.rs:1153/1340), unspend (:1512), freeze (:2807), unfreeze (:2861),
  reassign (:2924), get_spend (:4051); the mismatch returns before the write_slot call.
  Evidence: `spend_hash_mismatch` (:5219), `unspend_hash_mismatch` (:5890),
  `freeze_hash_mismatch` (:9798), `reassign_hash_mismatch` (:10141), `get_spend_hash_mismatch`
  (:10916), and `freeze_already_frozen_wrong_hash_returns_hash_mismatch` (:9728) proves the
  hash check precedes the state check.

**Verified: 8  |  Partial: 0  |  Failed (finding): 1** (of 10 checklist items; the
remaining two LOW findings are hardening notes outside the 10-item checklist).
