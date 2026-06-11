# Lua UDF Parity Audit — function-by-function semantic diff

**Reference:** `/Users/siggioskarsson/gitcheckout/teranode/stores/utxo/aerospike/teranode.lua` (1280 lines, current as of 2026-06-11).
**Subject:** TeraSlab `src/ops/engine.rs`, `src/ops/delete_eval.rs`, `src/server/dispatch.rs` and related op-type modules.
**Method:** full read of the Lua reference, line-by-line comparison against the Rust engine + dispatch handlers, with Go-caller impact judged from `/Users/siggioskarsson/gitcheckout/teranode/stores/utxo/` (read-only).

This pass supersedes the Lua-related parts of `audit/raw/KO-pruning-bitcoin.md`, which were done against spec quotations because the Lua file was missing. See "Corrections to prior audit" at the end.

---

## 1. Parity table

| Lua function (lines) | Rust location | Verdict |
|---|---|---|
| `spend` (261–275) | `engine.rs:1291-1483` (`Engine::spend`), dispatched via `handle_spend_batch` | **MATCH** — error precedence, operators, idempotent re-spend, frozen/spent handling identical. Two documented additions: all-0xFF request-sentinel rejection (F-G2-002, `engine.rs:1298`) and deleted-children defense at the idempotent short-circuit (F-X-022, `engine.rs:1395-1410`). |
| `spendMulti` (284–466) | `engine.rs:1051-1285` (`validate_spend_multi`) + `engine.rs:4306-4420` (`ValidatedSpend::apply`), `dispatch.rs:2793-3106` | **MATCH core / DIVERGES (response)** — partial application + per-idx errors match; wire response drops `blockIDs`/`signal`/`childCount` (Findings 2, 6). |
| `unspend` (484–555) | `engine.rs:1489-1600`, `dispatch.rs:3121-3293` | **DIVERGES** — errors on spending-data mismatch and on frozen slot where Lua is a silent idempotent OK (Finding 1). |
| `setMined` (558–697) | `engine.rs:1607-1953` (`set_mined`/`set_mined_inner`), `dispatch.rs:3299-3470` | **MATCH core / DIVERGES (response)** — dedupe-by-blockID, unset removal, `unminedSince` rules, LOCKED-clear all match. CREATING-clear is N/A (intentional, see row below). Wire response drops `blockIDs` (Finding 2); `childCount` omission intentional (no pagination). Unset uses swap-remove (block-ID list order not preserved) vs Lua's order-preserving remove — cosmetic. |
| `freeze` (707–779) | `engine.rs:2792-2839`, `dispatch.rs:4016` | **MATCH** — already-frozen → `ALREADY_FROZEN`; already-spent → `SPENT` + 36-byte raw spending data (`dispatch.rs:6579-6581`; raw-vs-hex is a client concern by project rule); unspent → freeze. No DAH eval in either (documented F-G2-012). |
| `unfreeze` (789–852) | `engine.rs:2846-2877` | **DIVERGES** — wipes the reassignment cooldown (Finding 5); unspent-slot error code differs (Finding 8); Lua's `getErrorCodeFromMessage` runtime bug correctly not replicated (Finding 9). |
| `reassign` (864–952) | `engine.rs:2880-2959` | **DIVERGES** — no `recordUtxos + 1` equivalent (Finding 4); reassignment audit-trail entry never written (cross-ref `A-utxo-correctness.md:231`); added LOCKED/CONFLICTING/coinbase guards are documented hardening (R-017, `engine.rs:2894-2921`); `checked_add` overflow error documented (R-063). |
| `setDeleteAtHeight` (968–1049) | `delete_eval.rs:71-186` (`evaluate_delete_at_height`) + `evaluate_dah_cached` | **MATCH (eval) / DIVERGES (sweep enforcement)** — retention==0, preserveUntil, conflicting-sets-DAH-once-never-clears, all-spent ∧ has-blocks ∧ on-longest-chain, forward-only DAH, clear-with-DAHUNSET, LAST_SPENT_ALL transition dedup: all match. But the DAH sweep re-validation excludes conflicting records from deletion (Finding 3). Note: Rust emits ALLSPENT/NOTALLSPENT for the single record where Lua reserves those for pagination records (`totalExtraRecs == nil`); harmless since TeraSlab has no pagination and signals are dropped at the wire anyway. |
| `setConflicting` (1066–1092) | `engine.rs:3533-3672`, `dispatch.rs:4377-4510` | **MATCH** — flag set/clear + DAH eval identical. Returns no spending data — **so does the Lua**; KO-4 is retracted (see corrections). Server-side conflicting-children propagation is a TeraSlab addition, best-effort (KO-5 stands, see corrections). |
| `preserveUntil` (1108–1136) | `engine.rs:3887-3931`, `dispatch.rs:4629` | **MATCH** — clears DAH, sets preserve_until, computes PRESERVE signal for EXTERNAL. Signal dropped at the wire (Finding 6 context). |
| `addDeletedChildren` (1150–1176) | no wire op; `engine.rs:2706-2775` (`prune_slot_if_spent_by_child`) + `engine.rs:3258-3380` (`append_deleted_child`), driven from `handle_delete_batch` (`dispatch.rs:4977-5034`) | **INTENTIONAL** redesign — spec §2.2 ("`deletedChildren` — replaced by UtxoSlot `status = 0x02 (PRUNED)`"). Delete-time parent-slot prune (primary) + deleted-children list (secondary) is performed server-side inside `OP_DELETE_BATCH`, collapsing the Go pruner's Phase 2a (addDeletedChildren) + 2b (delete children). Missing-parent tolerance matches (returns Ok, `engine.rs:3269-3271`). Spend-time check present in both spend paths (`engine.rs:1201-1219`, `1395-1410`). Differences: 255-child cap (Finding 10), distinct error code (Finding 10). |
| `setLocked` (1190–1216) | `engine.rs:3732-3845` (`set_locked_with_before_image`), `dispatch.rs:4513-4626` | **MATCH** — locking clears DAH (both paths), unlocking leaves DAH. `childCount = totalExtraRecs` omitted — **INTENTIONAL** (spec §2.2: pagination eliminated; Go `locked.go:192-219` uses it solely to fan out to pagination child records, which don't exist). |
| `incrementSpentExtraRecs` (1226–1280) | `dispatch.rs:502-516` (`OP_INCREMENT_SPENT_EXTRA_RECS = 255`, `opcodes.rs:198`) | **INTENTIONAL** — spec §3.14 "ELIMINATED". Returns STATUS_OK no-op, which is the safe direction: where Lua clamps-and-OKs under counter drift, TeraSlab OKs unconditionally. No error path exists, so the node-killing "error where Lua clamps" scenario cannot occur (`engine.rs:11074` test documents the rationale). |
| `getUTXOAndSpendingData` (194–231) | `read_slot_fast` + inline hash check (`engine.rs:1131-1161`, `1334-1342`) | **MATCH** — offset-out-of-range → `UTXO_NOT_FOUND` (wire `ERR_VOUT_OUT_OF_RANGE`), hash mismatch → `UTXO_HASH_MISMATCH`. |
| `isFrozen` (234–246) | `UTXO_FROZEN` status byte + legacy all-0xFF spending-data check on SPENT slots (`engine.rs:1224-1232`, `1417-1419`, `1527-1529`) | **MATCH** — both representations recognized. |

**Counts:** MATCH **7** (spend, freeze, setConflicting, preserveUntil, setLocked, getUTXOAndSpendingData, isFrozen) · MATCH-core-with-response-divergence **2** (spendMulti, setMined) · DIVERGES **4** (unspend, unfreeze, reassign, setDeleteAtHeight sweep-enforcement) · INTENTIONAL **2** (addDeletedChildren, incrementSpentExtraRecs). Plus CREATING flag: **MISSING IN RUST — INTENTIONAL** (spec §2.2: "`creating` — only existed for multi-record 2-phase commit. Single-record atomic writes make this unnecessary"; `engine.rs:11179-11223` test documents it; the validator-2PC use case is covered by the LOCKED flag set at create via `CreateRequest.locked`).

---

## 2. Findings

### [HIGH] 1. `unspend` errors on spending-data mismatch / frozen slot where the reference contract is a silent idempotent OK — breaks ProcessConflicting

**Location:** `src/ops/engine.rs:1525-1535` (mismatch → `InvalidSpend`), `engine.rs:1527-1529` and `1557-1559` (frozen → `Frozen`); intent confirmed by test `src/server/dispatch.rs:11598` (`handle_unspend_batch_rejects_wrong_spending_data`). Reference: `teranode.lua:513-540`.

**What's wrong:** The Lua reference (updated header comment, lines 471-474 and 513-519) defines `expectedSpendingData` as an *ownership check with idempotent semantics*: "the safety guarantee is 'never wipe a spend we don't own', not 'error on every no-op'". When the stored spending data is nil or belongs to a different (winning) transaction, Lua performs **no mutation but returns STATUS_OK**, and still runs `setDeleteAtHeight` housekeeping plus `aerospike:update`. It returns `FROZEN` only inside the `callerOwnsSpend` branch (which is dead code in practice, since a caller's expected data can never be the all-0xFF marker) — i.e. unspend of a frozen slot with non-matching expected data is also a silent OK. TeraSlab instead returns `SpendError::InvalidSpend` on mismatch and `SpendError::Frozen` whenever the slot is frozen, and skips DAH housekeeping on every no-op/error path (Lua's mismatch path still forward-extends an all-spent record's DAH; Rust does not — minor sub-divergence).

**Why it matters (Go-caller impact):** `stores/utxo/process_conflicting.go:193` calls `s.Unspend(ctx, affectedParentSpends, true)` where `affectedParentSpends` is built from **every input of every losing tx** (`conflicting.go:99-113`) — including parents whose stored spend belongs to the *winner* (must not be cleared, must not error) or that were never actually spent by the loser. The Go unspend handler (`un_spend.go:200-208`) treats any non-OK status except `TX_NOT_FOUND` as a fatal `StorageError` and aborts the whole loop. With TeraSlab's semantics, every conflict resolution that touches a shared parent fails — and retries hit the same condition deterministically. Conflict processing (double-spend resolution) is permanently wedged for that tx set.

**Reproduction:** Create parent with 1 UTXO; spend it with spending data A. Issue `OP_UNSPEND_BATCH` for the same slot with spending data B. TeraSlab: item error `ERR_INVALID_SPEND`; Lua: `STATUS_OK`, slot untouched, spentUtxos untouched.

**Suggested fix:** In `Engine::unspend`, treat `UTXO_SPENT` with `slot.spending_data != req.spending_data` and `UTXO_FROZEN`/frozen-marker slots as no-op successes (return current generation, run the DAH evaluation exactly as the mutating path does), reserving errors for hash mismatch / out-of-range / storage failures. Keep `UTXO_PRUNED → Pruned` as a hard error (chain history actually diverged — Lua predates the PRUNED state). Update `dispatch.rs` idempotent-vs-succeeded classification accordingly.

---

### [HIGH] 2. `setMined` wire response carries no block IDs — spec-mandated, and the Teranode client flow consumes them

**Location:** `src/server/dispatch.rs:3299-3470` (`handle_set_mined_batch` → `batch_response_with_outcome`, errors-only payload). Engine produces them (`SetMinedResponse.block_ids`, `engine.rs:1948-1952`) and dispatch discards them. Reference: `teranode.lua:632-633` (`response[FIELD_BLOCK_IDS] = blocks` — unconditional, success and unset alike). Spec: `specs/BSV_UTXO_STORE_SPEC.md:556` ("**Response**: Map of txid → current block_ids list").

**What's wrong:** Lua's `setMined` always returns the post-op block-ID list. TeraSlab's own spec requires the same. The dispatch handler returns an empty payload on success — the per-item block-ID lists never reach the wire.

**Why it matters (Go-caller impact):** `stores/utxo/aerospike/set_mined.go:383-398` builds `blockIDs[txHash] = res.BlockIDs` from the response, and `SetMinedMulti` returns that map; `stores/txmetacache/txmetacache.go:620-647` consumes it to update/evict the tx-meta cache, with a defensive postcondition that the map contain an entry per mined hash. A TeraSlab-backed client has no way to satisfy this contract from the setMined response and would need one extra `OP_GET_BATCH` round-trip per setMined batch (every transaction of every block) — or the flow breaks outright.

**Reproduction:** Issue `OP_SET_MINED_BATCH` for an existing tx; observe `STATUS_OK` with empty payload — no block-ID data, despite the engine having computed it.

**Suggested fix:** Encode per-item `(block_id_count, block_ids...)` in the success payload of `OP_SET_MINED_BATCH` (the engine already returns them; the cost is encoding only), and update the protocol spec/README. Alternatively document the GET-after-setMined pattern as the official contract and amend spec §3.6 — but that doubles hot-path load for block processing.

---

### [MEDIUM] 3. Conflicting records receive DAH but can never be deleted by the sweep — Lua intent is that they ARE deleted

**Location:** `src/server/dispatch.rs:6076-6090` (`handle_process_expired` re-validation: unconditionally requires `spent_utxos == utxo_count` and `unmined_since == 0`); DAH set for conflicting records regardless of spent state at `src/ops/delete_eval.rs:89-104`. Reference: `teranode.lua:985-995` (conflicting branch sets DAH immediately, partial spends and unmined state notwithstanding) — in the Aerospike system the Go pruner then deletes purely on `deleteAtHeight <= height`.

**What's wrong:** Re-confirmed against the real Lua (was KO-2 in the prior audit, judged from spec text): the Lua conflicting branch exists precisely so that loser transactions — which by definition will never be fully spent and never mined on the longest chain — get expired after `retention` blocks. TeraSlab sets the DAH identically but its sweep's re-validation gate (`spent_utxos == utxo_count`, `unmined_since == 0`) excludes them forever. The re-validation does not special-case the CONFLICTING flag.

**Why it matters (Go-caller impact):** Conflicting (double-spend loser) records accumulate without bound; their DAH-index entries are re-scanned by every `OP_PROCESS_EXPIRED_PRESERVATIONS` call (`range_query` keeps returning them), so sweep cost grows monotonically. In the reference system these records are pruned `retention` blocks after being marked conflicting.

**Reproduction:** `set_conflicting(value=true)` on a partially spent record; advance height past `dah`; call `OP_PROCESS_EXPIRED_PRESERVATIONS` — the record is skipped at `dispatch.rs:6083` every time.

**Suggested fix:** In the re-validation block, accept a candidate when `meta.flags.contains(CONFLICTING) && dah != 0 && dah <= current_height` (skipping the all-spent and longest-chain checks for that case), mirroring `evaluate_delete_at_height`'s own conflicting short-circuit.

---

### [MEDIUM] 4. `reassign` omits Lua's `recordUtxos + 1` — reassigned records become deletable after final spend; spec note mischaracterizes the Lua

**Location:** `src/ops/engine.rs:2944-2958` (only `reassignment_count` is bumped). Reference: `teranode.lua:944-945` (`-- Ensure record is not DAH'd when all UTXOs are spent` / `rec[BIN_RECORD_UTXOS] = rec[BIN_RECORD_UTXOS] + 1`). Spec: `BSV_UTXO_STORE_SPEC.md:138` and §3.9 "Note on all-spent check".

**What's wrong:** Lua permanently inflates `recordUtxos` so `allSpent = (spentUtxos == recordUtxos)` can never become true on a reassigned record — reassigned records are **never** DAH'd, even after the reassigned UTXO is itself spent. The spec documents dropping the counter with the rationale "Freeze does not increment `spent_utxos`, so after reassign ... the all-spent check naturally remains false **until the reassigned UTXO is spent**" — which is true for the pre-spend window but concedes exactly the case Lua's `+1` exists to prevent: once the reassigned UTXO is spent, TeraSlab sets DAH and deletes the record (with its reassignment history) `retention` blocks later.

To be precise about the feared money-loss scenario: a **live** reassigned UTXO cannot be deleted in TeraSlab — `all_spent` requires the reassigned slot to be SPENT (frozen does not count toward `spent_utxos`, verified at `engine.rs:2783-2791`), so the prompt-level CRITICAL ("reassigned record DAH-deleted while the reassigned UTXO is live") does **not** materialize. What diverges is record lifetime after the final spend: reference = forever; TeraSlab = `retention` blocks, after which a reorg deeper than retention cannot restore the reassigned UTXO and the legal/audit trail of the court-ordered reassignment is gone. Compounding: the reassignment audit-trail extension block (spec §2.7/§3.9 step 5, Lua's `reassignments` list at lines 934-940) is itself not written — only the count — already flagged in `audit/raw/A-utxo-correctness.md:231-232`.

**Why it matters (Go-caller impact):** Reassignments are alert-system/legal actions (`alert_system.go`); the reference deliberately preserves those records permanently. Losing the record erases the only on-store evidence linking old hash → new hash.

**Reproduction:** Create 1-UTXO tx, mine it, freeze, reassign, spend the reassigned UTXO at height H. TeraSlab: DAH = H+retention set, record deleted by sweep. Aerospike/Lua: `recordUtxos=2 != spentUtxos=1`, never DAH'd.

**Suggested fix:** Either (a) exclude records with `reassignment_count > 0` from DAH eligibility in `evaluate_delete_at_height`/`evaluate_dah_cached` (one flag/field check — faithful to Lua), or (b) make the divergence an explicit, justified spec decision with the *correct* rationale and a documented retention story for reassignment evidence. Do not leave the current spec note as-is — it argues from the wrong premise.

---

### [MEDIUM] 5. `freeze` → `unfreeze` wipes the reassignment cooldown — Lua's `utxoSpendableIn` survives freeze/unfreeze cycles

**Location:** `src/ops/engine.rs:2826-2827` (freeze writes `UtxoSlot::new_frozen`, overwriting `spending_data` — where the cooldown height lives per spec §2.4), `engine.rs:2868-2869` (unfreeze writes `UtxoSlot::new_unspent`, zeroed spending data ⇒ `spendable_height = 0` = immediately spendable). Reference: `teranode.lua:928-942` (`utxoSpendableIn` is a **separate record bin** keyed by offset, untouched by freeze 707-779 / unfreeze 789-852).

**What's wrong:** TeraSlab encodes the reassign cooldown (`block_height + spendable_after`) in the unspent slot's `spending_data[0..4]` (spec §2.4). Freezing that slot replaces the bytes with the all-0xFF marker; unfreezing replaces them with zeros. The cooldown is silently erased. In the reference, the cooldown lives in the `utxoSpendableIn` map bin and the spend-time check (`teranode.lua:371-383`) keeps enforcing it across any number of freeze/unfreeze cycles.

**Why it matters (Go-caller impact):** Sequence reassign → freeze → unfreeze (all legitimate alert-system operations on the same disputed output) leaves the UTXO spendable immediately instead of after `spendableAfter` (~1000) blocks. The cooldown is a deliberate safety window for reassigned (court-ordered) outputs; bypassing it via an unrelated freeze/unfreeze round-trip diverges from reference policy enforcement.

**Reproduction:** Reassign offset 0 at height 100 with spendable_after 1000 (cooldown 1100). Freeze offset 0, unfreeze offset 0. Spend at height 101: TeraSlab OK; Lua `FROZEN_UNTIL 1100`.

**Suggested fix:** In `freeze`, preserve `slot.spending_data[0..4]` is not possible (frozen marker must be all 0xFF) — instead stash the cooldown (e.g. in the reassignment extension block or a small metadata side map) and restore it in `unfreeze`; or simplest faithful fix: have `unfreeze` reject slots whose hash appears in the reassignment log without restoring the cooldown, forcing an explicit re-reassign. At minimum document the interaction in spec §2.4, which currently only covers spend/unspend clearing.

---

### [MEDIUM] 6. Mutation responses drop `signal`/`childCount` (and spend drops `blockIDs`) that the reference returns and the spec requires

**Location:** `src/server/dispatch.rs:3032` (`let _ = resp.signal;` in `handle_spend_batch`); `handle_unspend_batch` (3121), `handle_set_conflicting_batch` (4377), `handle_preserve_until_batch` (4629) all return errors-only payloads via `batch_response_with_outcome`. Engine computes signals throughout (`delete_eval.rs`, `engine.rs:3922-3926`). Reference: `teranode.lua:454-463` (spend: blockIDs + signal + childCount), 547-552 (unspend), 663-668 (setMined), 1084-1089 (setConflicting), 1131-1133 (preserveUntil PRESERVE). Spec: §3.4 response (`BSV_UTXO_STORE_SPEC.md:476-477`), §10.4 (`:1533-1534` — `block_ids` + `signal` in BatchSpendMulti response).

**What's wrong:** In the reference architecture the signals drive Go-side follow-ups: `spend.go:738-739` → `handleSpendSignal` (DAHSET/DAHUNSET → set/clear DAH on the external `.tx` blob and pagination child records), `un_spend.go:192-198` (NOTALLSPENT → decrement master `spentExtraRecs`), `set_mined.go:534-557`, preserve-file handling. TeraSlab computes every signal and then discards all of them at the dispatch boundary; no response payload format for them exists.

**Mitigating context (why MEDIUM, not HIGH):** the consumers of these signals are largely internalized — pagination records don't exist, the blob store is server-side with its own GC (`storage/blobstore.rs`, orphan-blob reconciliation), and record deletion handles parent-prune inline. So a redesigned client plausibly never needs the signals. But TeraSlab's **own spec still mandates them in the response** (§3.4, §10.4), the PRESERVE signal's external-blob preserve action has no obvious internal replacement, and silently computing-then-dropping is the worst of both: engine work spent, contract unmet, and `spec-vs-impl` drift.

**Reproduction:** Spend the last unspent UTXO of an external mined tx via `OP_SPEND_BATCH`: engine returns `Signal::DeleteAtHeightSet`; wire response is `STATUS_OK` with empty payload.

**Suggested fix:** Decide explicitly: either (a) add signal/block-ids to the batch response encodings per spec §10.4, or (b) amend the spec to declare signals server-internal and delete the dead `Signal` plumbing from responses the dispatcher never serializes. Surface PRESERVE's blob-side action concretely (verify `preserve_until` protects the blob from GC; if GC only checks index presence it already does — document that).

---

### [LOW] 7. FROZEN_UNTIL check ordering: Lua checks the cooldown before the spent/frozen state; Rust only checks it on UNSPENT slots

**Location:** `src/ops/engine.rs:1163-1192` (cooldown only in the `UTXO_UNSPENT` arm); Reference: `teranode.lua:371-383` (spendableIn checked before the `existingSpendingData` branch, so a frozen or spent slot with a pending cooldown reports `FROZEN_UNTIL` first).
**What's wrong:** For a slot that is frozen (or spent) *and* has a pending cooldown (reassign → freeze again), Lua returns `FROZEN_UNTIL`, Rust returns `FROZEN`/`SPENT`. Reachable only via the reassign-then-refreeze path; in Rust the cooldown is gone after re-freeze anyway (Finding 5).
**Why it matters:** Error-code divergence only; both reject the spend.
**Reproduction:** Reassign offset 0 (cooldown future), freeze it, attempt spend below cooldown height.
**Suggested fix:** None needed beyond Finding 5; note in spec §3.4 that the cooldown check is scoped to unspent slots.

### [LOW] 8. `unfreeze`/`reassign` on an unspent slot: Lua → `UTXO_INVALID_SIZE`, Rust → `UTXO_NOT_FROZEN`

**Location:** `src/ops/engine.rs:2864-2866`, `2927-2929`. Reference: `teranode.lua:822-828` / `897-903` (size check before frozen check; an unspent 32-byte slot fails with `UTXO_INVALID_SIZE`).
**What's wrong:** TeraSlab has fixed-size slots, so the size discriminator collapses into the status check; the unspent case surfaces as `UTXO_NOT_FROZEN` instead of `UTXO_INVALID_SIZE`. Spent-but-not-frozen → `UTXO_NOT_FROZEN` in both.
**Why it matters:** The Go alert system treats both as terminal errors; message text differs only. Cosmetic, but worth a line in the spec's error-mapping table.
**Suggested fix:** Document in spec §3.1 that `UTXO_INVALID_SIZE` is unreachable in TeraSlab and maps to `UTXO_NOT_FROZEN`.

### [LOW] 9. Reference-side bug recorded: Lua `unfreeze`/`reassign` error path calls undefined `getErrorCodeFromMessage`

**Location (reference):** `teranode.lua:813`, `:888` — `getErrorCodeFromMessage` is defined nowhere in the file; additionally the third return of `getUTXOAndSpendingData` is a response *map*, not a message, so the path is doubly wrong. Any `UTXO_NOT_FOUND`/`UTXO_HASH_MISMATCH` during unfreeze/reassign raises a Lua runtime error (UDF execution failure) instead of a structured error response.
**Rust behavior:** correct structured errors (`engine.rs:2856-2862`, `2890-2925`). **Rust must NOT replicate the bug — and doesn't.** Recorded for completeness; consider reporting upstream.

### [LOW] 10. deletedChildren mechanics: error shape differs and list is capped at 255

**Location:** `src/ops/error.rs:154-162` + `src/server/dispatch.rs:6611-6618` (distinct wire code `ERR_DELETED_CHILDREN = 35`, payload = 1-byte count); `engine.rs:3286-3291` (hard cap: 256th child → `StorageError`). Reference: `teranode.lua:391-403` (returns `INVALID_SPEND` with full hex spending data; `deletedChildren` map unbounded).
**What's wrong:** (a) A client porting the Lua mapping (`INVALID_SPEND` ⇒ counter-conflicting cascade) must learn the new code — documented in-code (F-X-022), and `classify_wire_error_code` buckets it with `ERR_INVALID_SPEND`, so metric semantics align. (b) A parent with >255 distinct pruned children makes further `append_deleted_child` calls fail; the primary `UTXO_PRUNED` slot defense still holds for the pruned offsets, so the safety impact is contained, but the secondary defense silently stops growing (the prune path is best-effort, `engine.rs:2754-2773`, so the delete still succeeds with only a log line).
**Why it matters:** Pathological parents (huge fan-out, many pruned children) lose the secondary defense without any client-visible indication.
**Suggested fix:** Document the 255 cap in the spec; emit a metric (not just a log) when the cap is hit.

### [LOW] 11. Coinbase-maturity gate requires the IS_COINBASE flag; Lua keys off `spendingHeight` alone

**Location:** `src/ops/engine.rs:1076-1083`, `1323-1330`. Reference: `teranode.lua:325-332` (any record with `spendingHeight > 0`).
**What's wrong:** A record that somehow has `spending_height > 0` without IS_COINBASE would be spendable in TeraSlab but blocked in Lua. No legitimate writer creates that state (only coinbase creates set spending_height). Operator (`>`) and error payload (spendable height + current, `dispatch.rs:6571-6576`) match the reference exactly.
**Suggested fix:** None required; covered by create-path invariants. Note in spec §3.4.

---

## 3. Explicit verification of the twelve focus items

1. **unspend idempotent-OK contract** — DIVERGES, Finding 1 (HIGH). Go impact verified: `process_conflicting.go:193` + `un_spend.go:200-208` → conflict processing aborts on the Rust error.
2. **spend FROZEN_UNTIL comparator** — MATCH. Lua 373 `spendableHeight > currentBlockHeight` ⇔ Rust `spendable_height > req.current_block_height` (`engine.rs:1178`, `1352`); spendable exactly at the unlock height in both. (Spec §3.4's `>=` is stale — see KO-8 correction.)
3. **Coinbase comparator + payload** — MATCH. Lua 326 `>` ⇔ `engine.rs:1078/1325` `>`; wire payload carries the 4-byte required height (`dispatch.rs:6571-6576`). Flag-gating nuance: Finding 11 (LOW).
4. **deletedChildren** — implemented, both spend paths check it on the idempotent re-spend short-circuit (`engine.rs:1201-1219`, `1395-1410`); registration happens server-side at delete time (`dispatch.rs:4977-5034` → `prune_slot_if_spent_by_child` → `append_deleted_child`). Teranode's delete/re-spend flow is preserved with the pruner's two phases collapsed into `OP_DELETE_BATCH`. Error-shape/cap deltas: Finding 10.
5. **CREATING flag** — MISSING, INTENTIONAL: spec §2.2; single atomic record write removes the multi-record in-flight window; validator-2PC lock semantics covered by `CreateRequest.locked` + setMined's LOCKED-clear (`engine.rs:1656-1657`, `1913-1916`). No severity.
6. **setMined** — dedupe by blockID, triplet removal on unset, `unminedSince` (nil iff hasBlocks ∧ onLongestChain; set to current when no blocks remain), LOCKED clear: all MATCH (`engine.rs:1770-1916`). Response: block IDs computed but dropped on the wire — Finding 2 (HIGH). `childCount`/#1037 pagination-unlock: N/A by design (no pagination records exist to stay locked; the master-record lock is cleared server-side in the same op).
7. **reassign record-utxo count** — NOT incremented; Finding 4 (MEDIUM, not CRITICAL — a live reassigned UTXO can never satisfy the all-spent check, verified). `spendableInMap` equivalent written into slot `spending_data[0..4]` (`engine.rs:2938-2945`); reassignment audit record not written (cross-ref A-utxo-correctness).
8. **setDeleteAtHeight clause-by-clause** — retention==0 ✓ (`delete_eval.rs:76`), preserveUntil ✓ (`:80`), conflicting sets-once-returns-early-never-clears ✓ (`:89-105`), pagination ALLSPENT/NOTALLSPENT with lastState dedup → adapted to single-record via LAST_SPENT_ALL flag ✓ (`:169-183`; emitted on the master record where Lua scopes it to pagination records — wire-invisible), master all-spent ∧ hasBlockIDs ∧ unminedSince==nil ✓ (`:109-118`), DAH forward-only ✓ (`:120`), else-clear with DAHUNSET-for-external ✓ (`:153-167`). Sweep re-validation excludes conflicting — re-confirmed DIVERGENT vs Lua intent, Finding 3.
9. **setLocked** — clears DAH when locking ✓ (`engine.rs:3755-3761`, slow path 3822-3829); childCount omission INTENTIONAL (pagination eliminated; Go usage at `locked.go:192-219` is pagination fan-out only).
10. **incrementSpentExtraRecs** — Lua clamps to [0, totalExtraRecs] and returns OK; TeraSlab opcode 255 returns unconditional STATUS_OK no-op (`dispatch.rs:502-516`). No error path ⇒ the drift scenario cannot kill the node. INTENTIONAL (spec §3.14).
11. **spendMulti shape** — per-idx errors ✓ (`SpendItem.idx` → `BTreeMap` → sparse encoding sorted by index, `dispatch.rs:3099-3104`); partial application ✓ (valid items applied even when others fail — `ValidatedSpend::apply` writes `valid_spends` regardless of `errors`, returns `STATUS_PARTIAL_ERROR`); SPENT carries 36-byte raw spending data ✓ (raw-not-hex is the documented client-side convention); FROZEN carries no payload ✓ (`dispatch.rs:6582`). blockIDs missing from response — Finding 6/2 context; Go's spend path does not consume `res.BlockIDs` (verified: no usage in `spend.go`), so impact rides on the signal finding.
12. **freeze/unfreeze/reassign errors** — freeze on spent → SPENT + spending data ✓; already frozen → ALREADY_FROZEN ✓; unfreeze/reassign require frozen → UTXO_NOT_FROZEN ✓ with the INVALID_SIZE delta (Finding 8); Lua `getErrorCodeFromMessage` reference bug recorded and correctly not replicated (Finding 9).

---

## 4. Corrections to prior audit (`audit/raw/KO-pruning-bitcoin.md`)

| ID | Prior claim | Disposition against the real Lua file |
|---|---|---|
| **KO-6** (MEDIUM) | `specs/teranode.lua` missing — parity unverifiable | **Partially resolved.** The authoritative file now exists in the teranode checkout and this audit verified against it. However `specs/teranode.lua` is **still absent from the teraslab repo** (CLAUDE.md and the spec reference it); copy it in so the parity baseline is version-pinned with the code. |
| **KO-8** (LOW) | Spec §3.4 says `>=` on FROZEN_UNTIL while Rust uses `>` — flagged as spec staleness | **Confirmed, closed in Rust's favor.** `teranode.lua:373` uses `>` exactly as Rust does (`engine.rs:1178`). The spec's `>=` is the only outlier; fix spec §3.4. |
| **KO-9** (LOW) | Rust unspend requires spending-data match, errors `InvalidSpend` — "undocumented tightening" | **Upgraded to HIGH** (Finding 1). The real Lua documents the opposite contract explicitly (lines 513-519): mismatch/nil ⇒ silent idempotent OK with DAH housekeeping; the Rust error breaks `ProcessConflicting`. This is not a tightening — it is a behavioral break of a documented reference contract. |
| **KO-4** (MEDIUM) | `setConflicting` response omits spending data the spec requires for the counter-conflicting cascade | **Retracted as an implementation finding; reclassified as a spec error.** The real Lua `setConflicting` (1066-1092) returns only status/signal/childCount — no spending data. The Go client gathers spending data itself via per-output `GetSpend` (`conflicting.go:126-141`). Rust is at parity with the reference; spec §3.10's "UTXO slot spending data" sentence should be deleted. |
| **KO-5** (MEDIUM) | Conflicting-children tracking best-effort, warn-only, capped at 255 | **Stands, with sharpened context.** In the reference architecture the parent `conflictingChildren` update is *client-side and fatal on failure* (`conflicting.go:79-81` aborts SetConflicting before the flag is written). TeraSlab moved it server-side and made it best-effort (`engine.rs:3666-3669` logs and continues), so a failure mode the reference treats as abort-worthy is silent here. The 255 cap has no Lua counterpart (unbounded list/map). Same severity (MEDIUM). |

---

## 5. Summary

- 2 HIGH: unspend idempotent-contract break (wedges ProcessConflicting); setMined response missing block IDs (spec-mandated, txmetacache depends on it).
- 4 MEDIUM: conflicting records never swept (KO-2 re-confirmed against Lua intent); reassign record-lifetime divergence with mis-rationalized spec note; freeze/unfreeze wipes reassign cooldown; signals computed-then-dropped vs spec contract.
- 5 LOW: cooldown-check ordering; INVALID_SIZE→NOT_FROZEN mapping; Lua-side `getErrorCodeFromMessage` bug (not replicated — correct); deletedChildren error-shape/255-cap; coinbase flag-gating.
- Intentional, verified against doc/spec citations: CREATING elimination, pagination/childCount elimination, incrementSpentExtraRecs OK-shim, addDeletedChildren internalization, FROZEN_UNTIL `>` comparator, reserved-0xFF sentinel rejection, reassign guard hardening (R-017) and overflow error (R-063).
