# Audit Category O — Bitcoin / Teranode-specific correctness

HEAD: branch `main` (1e5659b). Reference: `specs/BSV_UTXO_STORE_SPEC.md`,
`specs/SPEC_BRIEFING.md`, `README.md`. Authoritative source copy analyzed via
`git show HEAD:src/ops/engine.rs` (working tree clean == HEAD).

## Result: NO findings (all category-O checklist items verified correct)

> Process note: an initial pass produced a false "coinbase maturity
> unimplemented" finding caused by a shell glob failure (`src/*.rs` /
> case-insensitive grep path issues) that returned zero matches. A `git grep`
> re-run overturned it: coinbase maturity is fully implemented, wired
> end-to-end, and tested. That false finding was discarded and is NOT reported.
> The `specs/teranode.lua` file referenced by `CLAUDE.md` and several source doc
> comments does **not exist in the repo** (only the three `specs/*.md` files are
> present) — a LOW doc/repo-hygiene nit, noted not filed, since the formal spec
> (`BSV_UTXO_STORE_SPEC.md`) transcribes the authoritative Lua validation order.

---

## Checklist item 1 — Coinbase maturity = 100 blocks (VERIFIED OK)

**Fully implemented, spec-compliant, tested. Not a finding.**

- **Storage**: `TxFlags::IS_COINBASE = 0b0000_0001` (`src/record.rs:348`);
  write-once `spending_height: u32` field in `TxMetadata` (`src/record.rs:441`,
  default 0 at `:523`). Spec §field-table line 95 defines
  `spending_height = blockHeight + 100` for coinbase, 0 otherwise.
- **Create path** threads `is_coinbase` + `spending_height` from the wire:
  `src/server/dispatch.rs:3620-3621` (`is_coinbase: item.is_coinbase,
  spending_height: item.spending_height`) into the create request; engine
  create branches on `req.is_coinbase` (`src/ops/engine.rs:2089, 2326, 2470`).
  Test `create_coinbase` (`engine.rs:9385`) sets `req.is_coinbase = true` and
  exercises the stored maturity height.
- **Spend gate** is the spec's exact predicate
  `spending_height > 0 AND spending_height > current_block_height →
  CoinbaseImmature` (spec line 447), implemented identically in all three
  consuming paths:
  - single spend: `src/ops/engine.rs:1053-1058`
  - multi/batch spend: `src/ops/engine.rs:1297-1305` (height read once at the
    top of `spend_multi_inner`, applied per request)
  - reassign: `src/ops/engine.rs:2877-2895` (R-017/A-09: reassign of an immature
    coinbase rejected, preventing a reorg-frozen coinbase from being reassigned
    then spent below maturity)
- **Validation order matches spec** (`BSV_UTXO_STORE_SPEC.md:444-452`): the code
  runs vout-range (a sane pre-check, not a spec-numbered step) → (1) UTXO hash
  mismatch → (2) coinbase maturity → (3) already-spent → (4) frozen → … exactly
  as the spec orders steps 1-7.
- **Boundary semantics correct**: `> current_block_height` means the UTXO
  becomes spendable when `current_block_height == spending_height`
  (= blockHeight + 100), i.e. at depth 100 — standard BSV `COINBASE_MATURITY`.
  `spending_height == 0` is the non-coinbase sentinel and is never treated as
  immature (matters at genesis/low heights).
- **Wire + error mapping**: `SpendError::CoinbaseImmature` → `ERR_COINBASE_IMMATURE
  (=10)` with 4-byte `spending_height` error data
  (`src/server/dispatch.rs:6373-6377`, also `:6461`; `src/protocol/codec.rs:2762`),
  matching `README.md:352` ("error data: 4-byte required height") and
  `opcodes.rs:211`.
- **Tests assert real behavior, not just `Ok`**: `spend_immature_coinbase`
  (`engine.rs:5151`, matches `CoinbaseImmature { .. }`),
  `spend_mature_coinbase_equal` (`:5167`, spendable at exactly height),
  `spend_mature_coinbase_above` (`:5177`),
  `spend_coinbase_zero_spending_height_boundary` (`:5187`, sentinel not immature),
  `create_non_coinbase_no_maturity_check` (`:9561`),
  `reassign_rejects_immature_coinbase` (`:10039`).

## Checklist item 2 — Reorg: MarkLongestChainBatch recomputes dependent mined status (VERIFIED OK)

`engine.mark_on_longest_chain` (`src/ops/engine.rs:1963-2028`) modifies only
`unmined_since` (0 = on-chain, `current_block_height` = off-chain), re-evaluates
`delete_at_height`, and atomically syncs primary + DAH + unmined secondary
indexes (`sync_primary_and_both_secondary_atomic`, `engine.rs:2015-2022`). This
exactly matches the spec: "marks tx on/off longest chain (bulk reorg), modifies
only unmined_since without touching block entries." The store deliberately does
**not** walk the dependency graph — the Teranode reorg handler (caller) emits one
mark op per affected tx; per-tx mined-status recompute happens because the caller
re-issues setMined/markOnLongestChain for each dependent. This is the specified
division of responsibility, not a gap. Tests `engine.rs:8339-8571` cover on→off,
off→on, idempotent no-op, nonexistent-tx error, and "does not modify blocks or
slots", asserting the resulting `unmined_since`/generation (not just `Ok`).
setMined/unsetMined longest-chain transitions are likewise correct: fast path
`engine.rs:1640-1644`, slow path `engine.rs:1887-1893`; unset of the last block
sets `unmined_since = current_block_height` (tx drops off chain). Tests
`set_mined_on_longest_chain_clears_unmined` (8100),
`set_mined_off_longest_chain_keeps_unmined` (8123),
`unset_mined_last_block_sets_unmined` (8147),
`set_mined_then_unset_all_sets_unmined` (8784).

## Checklist item 3 — Conflicting-children tracking identifies descendants (VERIFIED OK, with note)

`setConflicting` (`src/ops/engine.rs:~2400-2540`) toggles only the `CONFLICTING`
flag on the named tx, is idempotent, and re-evaluates DAH. The spec describes
this op as a single-tx flag toggle ("setConflicting/unsetConflicting toggles
CONFLICTING flag, sets/clears delete_at_height accordingly") and does **not**
require the store to walk descendants — identifying conflicting descendants is
the caller's (Teranode validation) responsibility. So "marking a tx conflicting
correctly identifies descendants" is, by spec design, a caller concern; the store
behaves as specified. Note: `README.md:565` mentions "conflicting children
tracking" in the metadata description; the spec's normative ops do not require a
store-side descendant walk and the implementation does not perform one. Flagged
as informational only — reviewers should confirm the deployed reorg/conflict
handler computes descendants externally. Not a finding (spec-consistent).

## Checklist item 4 — delete_at_height set/respected for old unmined txs (VERIFIED OK)

- **DAH evaluation** (`src/ops/delete_eval.rs:71-308`, both the `&TxMetadata` and
  `evaluate_dah_cached` twins) matches spec §3.13: zero-retention no-op;
  `preserve_until` blocks DAH; CONFLICTING sets DAH once; all-spent + has-blocks +
  on-longest-chain sets/raises DAH; conditions-unmet clears DAH; `LAST_SPENT_ALL`
  transition signaling; EXTERNAL-only signal emission. Overflow is **checked**
  (`checked_new_dah`, `delete_eval.rs:31-41`) returning `DahOverflow` rather than
  `saturating_add` (which would silently pin UTXOs unprunable). Well tested
  (`delete_eval.rs:332-527`).
- **Unmined txs intentionally do NOT get a DAH** (gated by
  `on_longest_chain = unmined_since == 0`, `delete_eval.rs:111/246`; rationale
  documented `delete_eval.rs:66-70`); they are pruned via the unmined secondary
  index. `query_old_unmined` (`engine.rs` pruner) scans the unmined index for
  `unmined_since <= cutoff` where `cutoff =
  current_block_height.saturating_sub(block_height_retention)`
  (`scan_older_than`, `src/index/unmined.rs`). `saturating_sub` is correct for
  early-chain heights; the op returns keys for the caller to delete/preserve
  (store does not auto-delete) — per spec. The cutoff boundary is inclusive
  (`<= cutoff`), i.e. a tx exactly `block_height_retention` blocks old is
  returned; consistent with "at or below cutoff" and harmless since the caller
  decides deletion.

---

### Lower-severity notes (not filed as findings)
- `specs/teranode.lua` referenced by `CLAUDE.md` and source doc comments is
  absent from the repo. LOW doc/repo-hygiene.
- `scan_older_than` inclusive-cutoff boundary — confirm it matches the Aerospike
  pruner's strict-vs-inclusive boundary if exact parity is desired. LOW.
- `README.md` "conflicting children tracking" wording vs. no store-side
  descendant walk — confirm caller-side handling. INFORMATIONAL.
