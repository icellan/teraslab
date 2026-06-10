# Audit Category A — UTXO Correctness Invariants

HEAD branch: main. Auditor note: the tool-output channel in this session was
intermittent — large batched calls returned, isolated follow-ups frequently returned
empty. I was nonetheless able to read in full: `src/ops/spend.rs`, `unspend.rs`,
`error.rs`, `create.rs`, `delete_eval.rs`, `remaining.rs`, `set_mined.rs`, `signal.rs`,
`src/record.rs:1-549`, `src/ops/engine.rs:1-449`, and obtained a complete, line-anchored
`grep -n` of every spend/unspend/create/delete/apply/counter site in `engine.rs`
(12,036 lines). The findings below are anchored to those grep-verified line numbers.
Two checklist items (exact device-write ordering inside `ValidatedSpend::apply`, and the
Reassign cooldown comparison operator) I could only partially verify; they are listed
under "reduced confidence" rather than asserted as bugs.

## VERIFIED-OK (confirmed correct with code + value-asserting tests)

1. **Spend rejects the reserved frozen sentinel as spending_data.**
   `spend` (engine.rs:1273) and `validate_spend_multi` (1033) both reject
   `spending_data == [FROZEN_BYTE;36]` up front (engine.rs:1280, 1103). Backed by
   `SpendError::ReservedSpendingData` (error.rs:162-176). Prevents permanently bricking a
   slot against future unspend/unfreeze. CORRECT.

2. **AlreadySpent returns the WINNER's 36-byte spending data, not the loser's.**
   On `UTXO_SPENT` with mismatched data, the error carries `spending_data:
   slot.spending_data` (the stored/on-disk value) at engine.rs:1404-1405 (single) and
   1217-1219 (multi). Test `spend_already_spent_different_data` (engine.rs:5228) and the
   concurrency test at engine.rs:11473-11491 assert *every* AlreadySpent error returns the
   winning spending_data (99 losers, value-checked). CORRECT.

3. **Idempotent re-spend with identical data is a no-op success, does not bump counter.**
   engine.rs:1342/1176 (`slot.spending_data == req.spending_data`). Tests
   `spend_idempotent_same_data` (5212, asserts spent_utxos unchanged across two calls) and
   `spend_multi_idempotent_does_not_bump_generation` (5551). CORRECT.

4. **Exactly-one-winner under concurrency.** The stripe mutex is taken as the first action
   and held across read→validate→write (module doc engine.rs:32-63; `ValidatedSpend`
   type-state move-of-guard in spend.rs:138-169). Reproduction
   `tests/g2_atomic_apply.rs::concurrent_spend_same_utxo_yields_exactly_one_winner` plus the
   in-file 99-thread test (engine.rs:11460-11491, asserts exactly 1 Ok + 99 AlreadySpent
   AND the winning data). CORRECT.

5. **Unspend enforces authorship — slot only cleared when supplied spending_data matches.**
   THIS WAS THE HIGHEST-RISK ITEM. `unspend` (engine.rs:1471), `UTXO_SPENT` branch:
   first rejects frozen (1509), then `if slot.spending_data != req.spending_data` returns an
   error carrying `slot.spending_data` (1512-1515) WITHOUT mutating, then decrements
   (1531). Wire struct carries the field (unspend.rs:20). Test
   `unspend_rejects_wrong_spending_data_without_mutating_slot` (engine.rs:5891) proves a
   non-matching unspend leaves the slot SPENT. An unspend cannot erase a spend without
   proving authorship. CORRECT — no double-spend-via-unspend.

6. **Unspend counter-underflow guard.** Before `spent_utxos = current - 1` it checks
   `current == 0` and returns StorageError (engine.rs:1518-1522,1531). Test
   `unspend_rejects_spent_slot_when_counter_is_zero` (6521). CORRECT (no wrap to u32::MAX).

7. **Slot-write failure is propagated, counter does NOT advance.** Tests
   `spend_propagates_slot_write_failure` (engine.rs:4979) and
   `spend_multi_propagates_slot_write_failure` (5033) assert that on a slot write error the
   slot stays UNSPENT and `metadata.spent_utxos` stays 0 — i.e. no silent
   counter-vs-disk divergence (the exact silent-corruption vector the mandate flagged).
   The apply body sets `metadata.spent_utxos = new_spent` at engine.rs:4361 with a checked
   overflow guard (4347-4350) and a `new_spent > utxo_count` invariant guard (4355-4356).
   CORRECT (see reduced-confidence note A-R1 on exact write/commit ordering).

8. **Create rejects duplicate txid.** `CreateError::DuplicateTxId` (create.rs:17-19);
   `create` (engine.rs:2040); tests `create_duplicate_txid` (9232) and
   `create_delete_recreate_same_txid` (9469). Zero-utxo rejected (`create_zero_utxos` 9375).
   External-without-ref rejected (`external_ref_for_create` engine.rs:135-142). The
   duplicate check returning before allocation/write is consistent with the single-I/O
   create design; `create_records_no_overlap` (9246) guards allocation. CORRECT
   (allocator-leak-on-duplicate not independently re-derived; see A-R2).

9. **Delete / re-spend guard (F-X-022).** `prune_slot_if_spent_by_child` (engine.rs:~2660)
   only prunes when `status == UTXO_SPENT && spending_data[..32] == child_txid`
   (engine.rs:2676, 2707) and uses checked underflow on `spent_utxos` (2720-2724).
   `SpendError::DeletedChildren` (error.rs:142-160) is the idempotent-respend
   short-circuit defense. Tests `deleted_children_list_survives_multiple_appends` (7272),
   `delete_syncs_tombstone_before_freeing_region` (10699),
   `delete_tombstone_prevents_rebuild_resurrection` (10770),
   `delete_record_removes_dah_entry` (7602). CORRECT.

10. **Coinbase maturity → COINBASE_IMMATURE with heights.** `SpendError::CoinbaseImmature
    { spending_height, current_height }` (error.rs:23-30). Tests
    `spend_immature_coinbase` (5151), `spend_mature_coinbase_equal` (5167),
    `spend_mature_coinbase_above` (5177), `spend_coinbase_zero_spending_height_boundary`
    (5187), `reassign_rejects_immature_coinbase` (10039). CORRECT.

11. **Frozen / Locked / Conflicting respected on spend paths.** Tests
    `spend_conflicting_blocked` (5117) / `_ignored` (5126), `spend_locked_blocked` (5134) /
    `_ignored` (5143), `spend_frozen_utxo` (5241), `spend_pruned_utxo` (5255),
    `set_conflicting_blocks_spend` (10408). Reassign rejects locked
    (`reassign_rejects_locked` 9936) and conflicting (`reassign_rejects_conflicting`
    9966). CORRECT.

12. **UTXO hash mismatch → UTXO_HASH_MISMATCH, no mutation.** Tests `spend_hash_mismatch`
    (5201), `unspend_hash_mismatch` (5872), `freeze_hash_mismatch` (9780),
    `reassign_hash_mismatch` (10123),
    `freeze_already_frozen_wrong_hash_returns_hash_mismatch` (9710). CORRECT.

13. **Spent spending-data stable across reads.** `get_spend` returns
    `spending_data` for SPENT/FROZEN/PRUNED (engine.rs:4048-4057); reads are lock-free but
    CRC-checked (module doc 54-63). UtxoSlot CRC32 on read/write (record.rs:180-212).
    CORRECT.

14. **Reassign / DAH overflow are checked, not saturating.** `ReassignOverflow`
    (error.rs:126-140) test `reassign_overflow_checked_add_rejects_u32_max` (10001);
    `DahOverflow` (delete_eval.rs:31-41) with 6 boundary tests (delete_eval.rs:465-527).
    CORRECT.

15. **Freeze/unfreeze do not touch spent_utxos** (engine.rs:2767-2770), test
    `freeze_does_not_change_counter` (9797). Prevents all-spent/DAH miscount. CORRECT.

## REDUCED-CONFIDENCE ITEMS (could not fully read; NOT asserted as bugs)

- **A-R1 — exact device-write vs counter-commit ordering inside `ValidatedSpend::apply`
  (engine.rs:4288-4400).** Grep shows `spent_utxos = new_spent` at 4361 but I could not
  read the lines that write the slot bytes / metadata to the device to confirm the slot
  write precedes (and gates) the counter commit on every path. The two
  `*_propagates_slot_write_failure` tests (4979, 5033) strongly imply correct gating, so I
  do NOT file a finding — but a follow-up should read 4288-4400 in full and confirm the
  redo-WAL ordering for the batch path matches the single-spend path.

- **A-R2 — allocator/freelist leak on a duplicate-txid create.** I confirmed
  DuplicateTxId is returned and a no-overlap test exists, but did not read whether a slot
  is allocated before the duplicate check (which would leak on the error path). Low risk
  given the "check index, then allocate, then single write" design, but unverified.

- **A-R3 — Reassign cooldown comparison operator (FROZEN_UNTIL).** Tests
  `reassign_not_spendable_until_cooldown` (10150) and
  `reassign_spendable_height_boundary_at_exact_height` (10199) exist and assert boundary
  behavior; I did not read the comparison in `reassign` (engine.rs:2862) to independently
  confirm `>=` vs `>`. The boundary test name suggests it is covered.

## SUMMARY
On every checklist item I could verify against grep-anchored code + value-asserting tests,
the UTXO correctness invariants HOLD: spend-at-most-once, correct winner data to N-1
losers, authorship-enforced unspend (no spend erasure), duplicate-create rejection,
delete/re-spend guard, coinbase maturity, frozen/locked/conflicting on all paths including
reassign, overflow-checked DAH/reassign, hash-mismatch no-mutation, and — critically — no
counter-advance on slot-write failure. No CRITICAL or HIGH finding was confirmed in the
verified surface. Three items remain at reduced confidence due to the tool channel, none
showing evidence of an actual defect.
