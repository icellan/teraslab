# Category K — Pruning — Audit (HEAD `1e5659b`)

Scope examined: `src/ops/mark_longest_chain.rs`, `src/ops/delete_eval.rs`,
`src/index/dah_index.rs`, `src/index/unmined_index.rs`, plus the live data path
in `src/server/dispatch.rs` (the five pruner-related opcode handlers and the
spend/unspend/set_mined inline handlers), `src/ops/engine.rs`
(`mark_on_longest_chain`, `preserve_until`, `delete`, the secondary-index sync
helpers), `src/redo.rs` + `src/recovery.rs` (durability), and the spec/phase docs
(`specs/BSV_UTXO_STORE_SPEC.md` §3.13, `phases/06_remaining_ops.md`).

## Result: NO confirmed findings.

Every pruning checklist item was verified correct against the current code. An
earlier draft of this report filed three findings (K-01..K-03); ALL THREE were
RETRACTED after verification — they were artifacts of garbled line-number reads
during a flaky tool session and do not correspond to real code. The retractions
are documented below so a re-reviewer does not re-introduce them.

Important environment note: there is **no `specs/teranode.lua` file in this
repo** (the path in the task brief and in some source doc-comments refers to a
historical reference that is not checked in). The authoritative pruning spec is
`specs/BSV_UTXO_STORE_SPEC.md` §3.13 (lines 715-746) and the `setDeleteAtHeight`
pseudocode therein. The DAH-eval implementation matches that pseudocode.

---

## Verified-OK checklist

### 1. block_height_retention honored — OK
`evaluate_delete_at_height` / `evaluate_dah_cached` compute
`current_block_height + block_height_retention` through `checked_new_dah`
(`delete_eval.rs:31-41`, used at `:85` and `:222`), short-circuit when
`retention == 0` (`:76`, `:213`), and return `SpendError::DahOverflow` instead of
saturating (rationale documented at `:24-30`; regression-tested at
`delete_eval.rs:465-518` and the cached variant at `:512-527`). Matches SPEC
§3.13 pseudocode (`SPEC.md:729-745`). Engine spend/unspend/set_mined/mark all
thread the same `(current_block_height, block_height_retention)` from the request
into the eval (`engine.rs:1438,1555,1654,1905,1992`, and the spend fast path at
`:3543`).

### 2. PreserveUntilBatch prevents pruning until the height — OK
`engine.preserve_until` (`engine.rs:3869-3913`) clears `delete_at_height = 0`,
sets `preserve_until`, syncs the cached `HAS_PRESERVE_UNTIL` discriminant bit via
`sync_index_cache` (`:3898`; the bit is set in `sync_index_cache` at
`:902-909`), and evicts any existing DAH entry (`:3900-3902`). The DAH evaluator
then refuses to assign any DAH while `preserve_until != 0`
(`delete_eval.rs:80-82` metadata path, `:217-219` cached path). The cached-bit
sync is the R-019/A-12 fix that prevents fast-path ops (set_mined /
set_conflicting / set_locked) from concluding `has_preserve = false` and
bypassing the protection. Tests: `preserve_until_blocks_dah` (engine.rs:6072),
`preserve_until_blocks_dah_on_spend` (10616, spends after preserve and asserts
`delete_at_height == 0`), `preserve_until_stores_value` (10588),
`preserve_until_blocks_eval` (delete_eval.rs:341).

### 3. ProcessExpiredPreservations does not delete still-preserved txs — OK
`handle_process_expired` (`dispatch.rs:5858-5973`) queries
`dah_index().range_query(current_height)` then, for EVERY candidate,
re-reads the on-device metadata and re-validates four conditions before deleting
(`dispatch.rs:5898-5911`):
- `preserve_until == 0` (else skip — `:5898-5900`),
- `delete_at_height != 0 && delete_at_height <= current_height` (else skip — `:5901-5904`),
- `spent_utxos == utxo_count` (else skip — `:5905-5907`),
- `unmined_since == 0` (else skip — `:5908-5910`).
This is the R-102/IJK-09 "DAH entry is a hint, metadata is authoritative" fix.
Deletion goes through a synthetic `OP_DELETE_BATCH` so the full
replication+compensation path runs (`:5944-5951`). A preserved record is dropped
from the candidate set by the `preserve_until != 0` check, so it is never
deleted. Test `dispatch_process_expired_deletes_only_truly_eligible`
(dispatch.rs:7186-7294) proves the re-validation: txid_c is injected directly
into the DAH index (`:7271`) but has `spent_utxos == 0`, and the handler skips it
(`:7290-7293`), deleting only the two genuinely-eligible records.

### 4. Pruning during a reorg does not delete data the new chain needs — OK
A tx leaving the longest chain is handled by `mark_on_longest_chain(on=false)`
(`engine.rs:1981-1985`: sets `unmined_since = current_block_height`). The DAH
evaluator's `on_longest_chain` predicate is `unmined_since == 0`
(`delete_eval.rs:111`, `:246`), so an off-chain tx fails the
`all_spent && has_blocks && on_longest_chain` gate (`:118`, `:249`) and any
existing DAH is CLEARED (`:154-167`). The atomic commit
`sync_primary_and_both_secondary_atomic` (`engine.rs:2015-2022`) updates primary
+ DAH + unmined under one critical section (lock order primary→dah→unmined,
documented `:467-474`), so no reader sees a stale DAH pointing at an off-chain
record. Test `mark_off_chain_clears_dah` (engine.rs:8494-8524) asserts
`delete_at_height == 0` after marking a previously-DAH-set record off-chain.
`query_old_unmined` is advisory only (returns txids, never deletes —
dispatch.rs:5712-5723) and skips preserved records (`:5700`).

### 5. MarkLongestChainBatch interaction with pruning correct — OK
Live routing: `OP_MARK_LONGEST_CHAIN_BATCH` → `handle_mark_longest_chain_batch`
(dispatch.rs:459-461). The handler is WAL-first: it builds `RedoOp::MarkOnLongestChain`
entries (`:5171-5182`, with `generation: 0` selecting replay's value-idempotent
path) and writes them via `write_replicated_redo_ops` BEFORE the engine mutation
(`:5187`). It then applies `engine.mark_on_longest_chain` per item and emits a
`ReplicaOp::MarkLongestChain` carrying the master generation (`:5210-5219`,
the R-052 fix for silent master/replica divergence), with a compensation path on
replication failure (`:5248-5261`). The engine (`engine.rs:1963-2028`) updates
`unmined_since`, re-evaluates DAH, writes the primary metadata footer, then
commits primary+DAH+unmined atomically; a DahOverflow from eval propagates before
any secondary write (`:1992-1996`). Crash safety: `acked_mark_longest_chain_survives_crash`
(dispatch.rs:8687-8741) acks a mark, crashes, recovers, and asserts
`unmined_since == 2000` survived. Recovery replays `RedoOp::MarkOnLongestChain`
(recovery.rs:1652). Tests: `mark_on_longest_chain_clears_unmined`,
`mark_off_longest_chain_sets_unmined`, `mark_on_longest_chain_already_on_noop`,
`mark_on_longest_chain_nonexistent_tx`,
`mark_on_longest_chain_does_not_modify_blocks_or_slots`,
`mark_on_chain_fully_spent_evaluates_dah` (engine.rs:8339-8492).

### 6. delete_at_height set/respected for unmined txs older than retention — OK
By design, unmined txs are tracked by the UNMINED index, not DAH. The DAH
evaluator intentionally does NOT assign a DAH while `unmined_since != 0`
(on-longest-chain gate, doc `delete_eval.rs:66-70`). Old unmined txs are surfaced
by `handle_query_old_unmined` (dispatch.rs:5676-5723) via
`unmined_index().range_query(cutoff)` — an inclusive `..=cutoff` range
(unmined_index.rs:100-106) — with a per-candidate re-read that skips
`preserve_until != 0` records (`:5700`) and (in cluster mode) non-mastered keys
(`:5693-5698`). The `UnminedIndex` is the crash-critical secondary
(unmined_index.rs:7-9): its mutations are redo-logged
(`RedoOp::SecondaryUnminedUpdate`, engine.rs:347/451) and replayed on recovery,
and its insert/remove/replay keep `by_height`/`by_txid` consistent and idempotent
(unmined_index.rs:52-166). Tests: `dispatch_query_old_unmined_returns_matching_txids`
(dispatch.rs:7035), `dispatch_query_old_unmined_skips_preserved_records` (7091).

### Secondary-index integrity (supporting)
`dah_index.rs` insert/remove/replay_redo (`:49-138`) keep the forward `by_height`
and reverse `by_txid` maps consistent, clean up empty height buckets
(`:141-148`), are idempotent, and carry a `debug_assert` drift guard
(`:57-64`, F-G3-019). Both backends (in-memory + redb) implement `range_query`
with the same inclusive `..=current_height` semantics
(secondary_backend.rs:154-157; redb_dah.rs:240). Snapshot/rebuild covered by
recovery + index tests (recovery.rs:3638-4013, index/mod.rs:1702-1718).

---

## RETRACTED earlier draft findings (do not re-file)

- **K-01 (claimed off-by-one in process_expired vs Lua `>=`)** — RETRACTED.
  Cited `specs/teranode.lua:124` and an engine fn `clear_preserve_until`;
  NEITHER EXISTS in this repo (`find . -name '*.lua'` empty; `grep -r
  clear_preserve_until src/` empty). The real `handle_process_expired`
  (dispatch.rs:5898-5911) gates on `preserve_until == 0` and
  `delete_at_height <= current_height` against re-read metadata — correct, with
  no `preserve_until <= current_height` comparison anywhere. Fabricated.

- **K-02 (claimed height-source mismatch in clear_preserve_until)** — RETRACTED.
  Depended on the nonexistent `clear_preserve_until`. `handle_process_expired`
  does not recompute any DAH; it deletes via synthetic `OP_DELETE_BATCH`.
  Fabricated.

- **K-03 (claimed dead handler discarding replica ops)** — RETRACTED. There is
  exactly one `handle_mark_longest_chain_batch` (dispatch.rs:5128), it IS routed
  (dispatch.rs:460), and it DOES replicate via `ReplicaOp::MarkLongestChain`
  (5210-5219). Misread of garbled output. Fabricated.
