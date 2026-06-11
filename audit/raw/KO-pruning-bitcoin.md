# Audit K+O — Pruning / DAH + Bitcoin/Teranode-specific concerns

Auditor scope: pruning/DAH lifecycle (`src/ops/delete_eval.rs`, `src/index/dah_index.rs`,
`src/server/dispatch.rs` preservation/sweep handlers), `src/ops/mark_longest_chain.rs`,
`src/ops/set_mined.rs`, `src/ops/remaining.rs` + engine implementations, conflicting-children
logic. Reference: `specs/BSV_UTXO_STORE_SPEC.md`, `phases/04_setmined_path.md`,
`phases/06_remaining_ops.md`, README.md.

**Audit-blocking caveat first:** `specs/teranode.lua` — named by CLAUDE.md and this audit's
charter as the authoritative reference — does not exist in the repository and has never been
committed (verified via `git log --all -- specs/teranode.lua`: no history). Every Lua
comparison below is therefore against `BSV_UTXO_STORE_SPEC.md`'s quoted Lua semantics (which
cite teranode.lua by line number) rather than the Lua source itself. See finding KO-6.

---

## Findings

### [HIGH] KO-1: Expired preservations are never processed — `preserve_until` is permanent

**Location:** `src/server/dispatch.rs:6036-6161` (`handle_process_expired`),
`src/ops/engine.rs:3887-3931` (`preserve_until`), `src/ops/delete_eval.rs:80-82`.

**What's wrong:** Spec §3.18 step 3 ("Expired preservation processing") requires: *"Query
records where `preserve_until <= current_height` → Set `delete_at_height` and clear
`preserve_until`."* Nothing in the codebase implements this:

- `OP_PROCESS_EXPIRED_PRESERVATIONS` (opcode 32) does not process preservations at all. It is
  a DAH-index delete sweep (spec §3.18 *Phase 2*), and its re-validation step explicitly
  **skips** any record with `preserve_until != 0` (`dispatch.rs:6076-6078`) — including
  records whose preservation has long expired (`preserve_until < current_height`).
- There is no `preserve_until` secondary index and no query opcode that can find expired
  preservations. `rg 'preserve_until = 0|preserve_until <='` across `src/` returns no code
  that ever clears the field or compares it against the current height.
- `evaluate_delete_at_height` (`delete_eval.rs:80`) unconditionally returns no-patch when
  `preserve_until != 0`, regardless of expiry, so a preserved record can never re-acquire a
  DAH even through subsequent spend/setMined events.

A record preserved at height H is therefore unprunable forever, not until H. The pruner sets
`preserve_until = current + ParentPreservationBlocks` (1440) on *parents of every old unmined
transaction* each cycle (spec §3.18 Phase 1, `OP_PRESERVE_TRANSACTIONS`), so the preserved set
grows monotonically in normal operation.

**Why it matters:** Unbounded store bloat — the exact failure mode the DAH machinery exists to
prevent. On a 10M+ ops/sec store the preserved-parent population accumulates over weeks and
the space is never reclaimed. It is also a semantic divergence from the reference system
(Aerospike pruner expires preservations every cycle) hiding behind an opcode whose name
(`ProcessExpiredPreservations`) and README description ("Delete expired preserved
transactions", README.md:302) claim the opposite of what the code does.

**Reproduction:** Create a tx, spend all UTXOs, set it mined on the longest chain (DAH gets
set). Send `OP_PRESERVE_UNTIL_BATCH` with `block_height = 100`. Advance to
`current_height = 10_000` and send `OP_PROCESS_EXPIRED_PRESERVATIONS`. Expected (spec): record
gets `delete_at_height` set, `preserve_until` cleared, deleted on a later sweep. Actual:
record skipped forever (`deleted == 0`), `preserve_until` still 100. No test in the repo
exercises preservation expiry — `dispatch_process_expired_*` tests only cover the DAH sweep.

**Suggested fix:** Add a preserve-until secondary index (mirror of `DahIndex`), and make
`handle_process_expired` do what its name says before the DAH sweep: range-query
`preserve_until <= current_height`, and for each match (under the per-tx lock) clear
`preserve_until`, run `evaluate_delete_at_height`, and update the DAH index. Alternatively
expose a `QUERY_EXPIRED_PRESERVATIONS` op and let the Go pruner drive it — but pick one and
document it; today neither exists.

### [HIGH] KO-2: DAH sweep re-validation can never delete conflicting transactions

**Location:** `src/server/dispatch.rs:6079-6090` (re-validation in `handle_process_expired`)
vs `src/ops/delete_eval.rs:89-105` (DAH-setting policy for CONFLICTING).

**What's wrong:** The DAH-*setting* policy has two disjoint eligibility paths (spec §3.13):

1. CONFLICTING → set DAH unconditionally (no all-spent, no on-longest-chain requirement);
2. all-spent AND has-blocks AND on-longest-chain → set DAH.

The DAH-*deleting* re-validation (`R-102 / IJK-09`) encodes only path 2:

```rust
if { meta.spent_utxos } != { meta.utxo_count } { continue; }
if { meta.unmined_since } != 0 { continue; }
```

A conflicting transaction (a rejected double-spend) typically has `spent_utxos <
utxo_count` (its outputs were never spent) and usually `unmined_since != 0`. Its DAH entry —
deliberately scheduled by `delete_eval` at `current + retention` — comes due, appears in
`range_query` every single sweep, fails re-validation, and is skipped. Forever.

Consequences: (a) conflicting records whose `unmined_since == 0` (e.g. setConflicting applied
before MarkLongestChainBatch during a reorg, or replay orderings that leave the field 0) are
deleted by *no* path — not the DAH sweep, not the old-unmined sweep (the unmined index only
contains `unmined_since != 0` records); (b) even when the unmined sweep eventually catches
them, the stale DAH entries are re-scanned and metadata-read every block — O(accumulated
conflicting txs) wasted device reads per sweep, growing without bound since the entry is only
removed on delete.

This diverges from the reference system, where the pruner deletes every record whose
`deleteAtHeight` is due, by design including conflicting records (that is the *only* reason
`setConflicting`'s Lua sets DAH at all — spec §3.10: "If conflicting and no existing DAH: set
delete_at_height").

**Why it matters:** Conflicting transactions are precisely the garbage DAH-on-conflict exists
to collect. The over-tightened re-validation predicate silently disables that collection
path and turns each conflicting tx into a permanent per-block sweep tax.

**Reproduction:** Create a tx with 2 UTXOs, spend none, `OP_SET_CONFLICTING_BATCH` with
`value=1`, `cbh=100`, `bhr=10` → metadata `delete_at_height = 110`, DAH index entry at 110.
Send `OP_PROCESS_EXPIRED_PRESERVATIONS` with `current_height = 200`. Expected (reference
semantics): record deleted. Actual: skipped (`spent_utxos != utxo_count`), DAH entry remains,
candidate re-scanned on every subsequent sweep.

**Suggested fix:** In the re-validation, accept a record when `meta.flags` has CONFLICTING and
`delete_at_height <= current_height` and `preserve_until == 0`, bypassing the all-spent /
unmined checks — i.e. make the delete predicate the mirror image of the set predicate in
`delete_eval.rs`. Add a sweep test for a conflicting, partially-spent, unmined record.

### [MEDIUM] KO-3: TOCTOU between sweep re-validation and delete — late preservation is silently overridden

**Location:** `src/server/dispatch.rs:6060-6129` (`handle_process_expired` phase 1 → phase 2),
`src/ops/engine.rs:3960-4028` (`Engine::delete` — unconditional, no preserve recheck).

**What's wrong:** `handle_process_expired` re-validates candidates (reads metadata, checks
`preserve_until == 0` etc.) *without holding the per-tx stripe lock*, then dispatches a
synthetic `OP_DELETE_BATCH`. `handle_delete_batch` does snapshotting, parent-prune scans, a
WAL write and replication before each `engine.delete`, which itself never re-checks
`preserve_until` (Delete is by-design unconditional per spec §3.18). A
`OP_PRESERVE_UNTIL_BATCH` / `OP_PRESERVE_TRANSACTIONS` that lands on the same key between the
phase-1 metadata read and the eventual `engine.delete` is accepted (returns OK to the pruner)
and then the record is deleted anyway. The window is not a few instructions — it spans slot
snapshot reads, blob reads for external records, redo fsync, and replication round trips for
the whole batch.

This defeats the documented purpose of the R-102/IJK-09 re-validation ("a stale DAH entry
that points at a now-preserved record otherwise results in silent data loss",
dispatch.rs:6026-6028) — the same data loss re-opens under concurrency. The Teranode pruner's
Phase 1 (preserve parents) "MUST succeed before Phase 2" within one cycle, but nothing
prevents overlapping cycles, a second pruner instance, retried RPCs, or cluster sweeps fanned
out across masters from interleaving preserve and process-expired on the same key.

**Why it matters:** Deleting a freshly-preserved record is pruning a record some unmined
child still references — the parent-preservation mechanism exists precisely because that
parent's spend data is needed if the child is later mined or re-validated.

**Reproduction:** Thread A: `OP_PROCESS_EXPIRED_PRESERVATIONS(current=H)` where key K is due.
Pause A after phase-1 revalidation (e.g. fault-injection hook on the redo flush). Thread B:
`OP_PRESERVE_UNTIL_BATCH([K], H+1440)` → OK. Resume A → K is deleted. Final state: B's
acknowledged preservation lost together with the record.

**Suggested fix:** Add a `delete_if_due(key, current_height)` engine entry point used only by
the sweep, which re-checks `preserve_until == 0 && delete_at_height != 0 &&
delete_at_height <= current_height` under the stripe lock before tombstoning (the lock is
already taken in `Engine::delete`; the recheck is one metadata read it already performs).
Direct client `OP_DELETE_BATCH` keeps unconditional semantics.

### [MEDIUM] KO-4: setConflicting response omits the spending data the spec requires for the counter-conflicting cascade

**Location:** `src/server/dispatch.rs:4377-4512` (`handle_set_conflicting_batch`) vs spec
§3.10 Response (`specs/BSV_UTXO_STORE_SPEC.md:671-674`).

**What's wrong:** Spec §3.10 mandates the setConflicting response includes "*For each txid
processed: UTXO slot spending data (needed by Go client for counter-conflicting cascade)*"
(parity with Aerospike `SetConflicting` returning `([]*Spend, []chainhash.Hash, error)`).
`handle_set_conflicting_batch` returns only `batch_response_with_outcome(&errors, ...)` —
sparse per-item errors, no spending data, no conflicting-children hashes. The engine response
(`SetConflictingResponse { signal, generation }`, `src/ops/remaining.rs:56-61`) carries
nothing to encode even if the wire format wanted it.

**Why it matters:** Descendant identification when a tx is marked conflicting is the Go
client's job, driven by the spending data of the newly-conflicting tx's spent slots. Without
it in the response, the client must issue a follow-up `OP_GET_SPEND_BATCH` per output —
N extra round trips per conflicting tx during reorg storms (exactly when latency matters) —
and an integration layer written against the spec/Aerospike signature will find the cascade
data simply absent. The conflicting-children flag-propagation chain (mark child conflicting →
find children of the *children*) is only as complete as this data path.

**Reproduction:** Send `OP_SET_CONFLICTING_BATCH(value=1)` for a fully-spent tx; inspect the
response payload — it contains only the error/outcome framing, no slot spending data.
Cross-check spec §3.10 Response block.

**Suggested fix:** Either extend the wire response to include per-tx spent-slot spending data
(and the record's conflicting-children list, which `read_conflicting_children` already
serves), or document the divergence in the spec and require the Go adapter to follow up with
`GET_SPEND_BATCH`/`GET_BATCH(FieldMask::CONFLICTING_CHILDREN)` — today the spec and the wire
contract contradict each other.

### [MEDIUM] KO-5: Conflicting-children tracking is best-effort and hard-capped at 255 — silent incompleteness

**Location:** `src/ops/engine.rs:2999-3001` (u8 cap), `src/ops/engine.rs:3211-3226`
(`append_conflicting_child_best_effort` — warn-and-continue), `src/ops/engine.rs:3499-3530`
(`append_conflicting_children_from_cold_data` — warn-and-return on cold-data read/parse
failure), `src/record.rs:476` (`conflicting_children_count: u8`).

**What's wrong:** When a tx is marked conflicting, its txid is appended to each parent's
conflicting-children list. Three silent-loss paths:

1. `children.len() > u8::MAX` → `StorageError` — swallowed by the `best_effort` wrapper into a
   `tracing::warn!`. The 256th conflicting child of a parent is simply not recorded. Aerospike
   list bins have no such cap; a 255-double-spend storm against one output is cheap to
   construct for an attacker (one parent UTXO, 256 competing spends).
2. Any cold-data read/parse failure for the child → no parents updated at all, warn only.
3. CAS contention exceeding 16 retries → error → swallowed, warn only.

In all three cases `set_conflicting` still returns OK to the client; nothing in the response
indicates the parent lists are incomplete. Consumers of
`FieldMask::CONFLICTING_CHILDREN` (`dispatch.rs:5739`) — i.e. the Go client's cascade — get a
silently truncated descendant set.

**Why it matters:** An incomplete conflicting-children list means the counter-conflicting
cascade misses descendants: a child of a conflicting tx can remain spendable/un-flagged. The
255 cap is an attacker-reachable hard edge, and "warn-only" turns a correctness property into
an ops-log Easter egg.

**Reproduction:** Create parent P with 1 UTXO; create 256 conflicting children each spending
P:0 (create with `conflicting=true`, which calls `append_conflicting_child_best_effort`,
engine.rs:2187). Read P's conflicting children — 255 entries, 256th lost, all creates
returned OK.

**Suggested fix:** Widen the count to u16/u32 with an overflow-block layout (the list already
lives in a separately allocated block; only the metadata field is u8), or fail the operation
visibly (error/partial status) instead of warn-only when the list cannot be updated. At
minimum surface a per-item warning code in the batch response.

### [MEDIUM] KO-6: Authoritative reference `specs/teranode.lua` is missing from the repository

**Location:** `specs/` (file absent; never in git history). Referenced by
`CLAUDE.md` ("Read `specs/teranode.lua` for the current Lua UDF implementation being
replaced"), `specs/BSV_UTXO_STORE_SPEC.md` (≥12 citations by line number, e.g. "from
`teranode.lua` lines 927-1008"), `src/ops/delete_eval.rs:3` ("Ported from `teranode.lua`
lines 927–1008"), `src/ops/spend.rs:3`.

**What's wrong:** The file the project designates as the semantic ground truth — and which
this audit was directed to line-by-line cross-reference — does not exist and has never been
committed. All "parity with Lua" claims in code comments and the spec are unverifiable from
the repository alone.

**Why it matters:** Comparison-operator and flag-handling parity (the exact class of bug this
audit hunts) cannot be independently re-verified by reviewers or CI. The spec has already
drifted from the implementation in at least one comparator (KO-8); without the Lua source
there is no tiebreaker.

**Reproduction:** `ls specs/` and `git log --all -- specs/teranode.lua` (empty).

**Suggested fix:** Vendor the exact teranode.lua revision the spec line numbers refer to into
`specs/` (it is part of the open-source Teranode repo), or replace all line-number citations
with content quotes in the spec.

### [LOW] KO-7: `unmined_since` sentinel collision at `current_block_height == 0`

**Location:** `src/ops/engine.rs:1999-2003` (`mark_on_longest_chain`),
`src/ops/engine.rs:1905-1911` (set_mined slow path).

**What's wrong:** `unmined_since == 0` means "mined on longest chain" throughout
(`delete_eval.rs:111`, unmined index membership). Marking a tx *off* the longest chain at
`current_block_height = 0` writes `unmined_since = 0` — the record is recorded as ON the
longest chain, skips the unmined index, and (if fully spent with block entries) becomes
DAH-eligible. Same collision in the set_mined `new_count == 0` branch. The spec inherits the
ambiguity ("set `unmined_since = current_block_height`", §3.15), so this is reference-faithful
— but the reference relies on Aerospike clients never sending height 0, and nothing here
validates that.

**Why it matters:** Genesis-adjacent heights only; in practice unreachable for BSV mainnet.
Hardening, not a live bug.

**Reproduction:** `mark_on_longest_chain { on_longest_chain: false, current_block_height: 0 }`
on a fully-spent mined tx → record stays out of the unmined index and retains/acquires DAH.

**Suggested fix:** Reject `current_block_height == 0` for off-chain marks at the dispatch
layer, or use `max(cbh, 1)`.

### [LOW] KO-8: Spec §3.4 FROZEN_UNTIL comparator is stale (`>=`) versus the deliberate `>` in code

**Location:** `specs/BSV_UTXO_STORE_SPEC.md:452` vs `src/ops/engine.rs:1180`
(spend_multi) and the single-spend path.

**What's wrong:** Spec (quoting teranode.lua) says reject when
`spendable_height >= current_block_height`. Code uses `>` and documents why: "matches Teranode
PR #949 / svnode / Aerospike post-fix. Pre-fix this used `>=` which false-rejected at the
exact unlock height." The divergence is intentional and consensus-correct (UTXO spendable AT
the reassign unlock height), but the spec — the document audits are told to treat as
semantics-of-record — still encodes the buggy pre-fix operator.

**Why it matters:** Next implementer/reviewer "fixing" the code back to spec reintroduces the
false-reject at the unlock boundary.

**Reproduction:** Diff spec §3.4 rule 5 against `engine.rs:1175-1187`.

**Suggested fix:** Update spec §3.4 to `>` with a note citing Teranode PR #949.

### [LOW] KO-9: `unspend` requires spending-data match — stricter than spec §3.5, undocumented

**Location:** `src/ops/engine.rs:1530-1535` vs `specs/BSV_UTXO_STORE_SPEC.md` §3.5.

**What's wrong:** Spec §3.5 validation (from teranode.lua 478-540) requires only status ==
SPENT; it never compares the stored spending data against the request. The implementation
returns `SpendError::InvalidSpend` when `slot.spending_data != req.spending_data` — only the
recorded spender can be unspent. This is arguably *better* (a reorg rollback of child B cannot
clobber a slot actually spent by child A), but it is a behavioral divergence from the
documented reference: a Go client that calls Unspend with reconstructed/zeroed spending data
(legal against Aerospike) gets INVALID_SPEND here.

**Why it matters:** Reorg rollback is exactly when client and store state may disagree about
who spent a slot; a hard error here can wedge an unwind loop that the reference system would
have completed.

**Reproduction:** Spend slot 0 with data D1; call unspend with matching txid/vout/hash but
data D2 → `InvalidSpend` (impl) vs success (spec §3.5 text).

**Suggested fix:** Keep the check (it is sound) but document it in spec §3.5 as an intentional
tightening, and confirm the Teranode Go client always passes the original spending data on
Unspend.

### [LOW] KO-10: README mislabels `block_height_retention` as unmined-tx retention

**Location:** `README.md:165`: `block_height_retention = 288  # Blocks to retain unmined
transactions`.

**What's wrong:** `block_height_retention` governs DAH for *spent, mined* records (spec:
`BlockHeightRetention` 288). Unmined transactions are governed by the separate
`UnminedTxRetention` (144) which is client-supplied as the `OP_QUERY_OLD_UNMINED` cutoff and
has no server config knob. The README comment conflates the two; an operator tuning "how long
unmined txs are kept" by editing this value would actually change how quickly *spent* records
are pruned — directly shrinking the reorg-safety window (see K-4 disposition).

**Reproduction:** Compare README.md:165 with spec §3.18 Configuration block.

**Suggested fix:** Fix the comment: "Blocks to retain fully-spent mined records before DAH
deletion (reorg-safety window)".

### [LOW] KO-11: set_mined/set_conflicting fast paths write cached DAH back to the device after a cache-sync failure

**Location:** `src/ops/engine.rs:1663-1721` (set_mined fast path: `old_dah` from
`entry.dah_or_preserve`, RMW writes `meta.delete_at_height = new_dah`),
`src/ops/engine.rs:3546-3596` (set_conflicting fast path, same pattern).

**What's wrong:** The fast paths derive `old_dah`/`has_preserve` from the cached
`TxIndexEntry` and then read-modify-write the on-device header, overwriting
`meta.delete_at_height` with a value computed from the cache. F-G2-011 closed exactly this
staleness class for `generation` (by reading the on-device value during the RMW) but left
DAH/flags cache-derived. If a prior mutation persisted metadata but failed at
`sync_index_cache` (the error is surfaced to that caller, but the device write stands), the
next fast-path op resurrects the stale cached DAH onto the device and into the DAH index. The
process-expired re-validation (preserve/spent/unmined checks) catches most harmful outcomes,
so this is a consistency hardening item, not a live prune-of-preserved bug — but note the
re-validation itself is what KO-2 proposes to loosen for CONFLICTING, so fix both coherently.

**Reproduction:** Fault-inject `update_cached_fields` failure on a `preserve_until` call
(device write OK, cache stale without HAS_PRESERVE_UNTIL); then `set_mined` fast path on the
same key; read device metadata: `delete_at_height` restored to the pre-preserve value while
`preserve_until != 0` — a state `delete_eval` can never produce.

**Suggested fix:** In the fast-path RMW (which already reads the on-device header), take
`old_dah`, `preserve_until`, and flag bits from the freshly read `meta` instead of the cached
entry, mirroring the F-G2-011 generation fix.

---

## Checklist disposition

### Checklist K — Pruning

| Item | Verdict | Evidence |
|---|---|---|
| `block_height_retention` honored | ✅ | `delete_eval.rs:85` `new_dah = current + retention` (checked_add, overflow → error); sweep deletes only when `dah <= current_height` (`dispatch.rs:6080`), so records live exactly `retention` blocks past all-spent. `retention == 0` disables DAH per spec (`delete_eval.rs:76`). Config caps at 10M (`config.rs:1019`) but has **no minimum** — `retention=1` legal, shrinking the reorg window to one block (operator foot-gun, see KO-10). |
| PreserveUntilBatch prevents pruning until specified height | ⚠️ | Prevention itself is solid: `preserve_until` clears DAH + DAH-index entry (`engine.rs:3902-3920`), `delete_eval` skips preserved records, sweep re-validation skips `preserve_until != 0`, `query_old_unmined` filters preserved candidates (`dispatch.rs:5878`), R-019 keeps the index cache in sync. But it prevents pruning **forever**, not "until specified height" — expiry is unimplemented (KO-1) — and a racing sweep can override a just-acknowledged preservation (KO-3). |
| ProcessExpiredPreservations does not delete still-active preservations (off-by-one?) | ⚠️ | No off-by-one and no premature delete: any `preserve_until != 0` is skipped (`dispatch.rs:6076`). But that is because the operation never expires *any* preservation, active or not — the spec'd expire-and-set-DAH behavior is missing entirely (KO-1), and conflicting records are additionally never deletable by this sweep (KO-2). |
| Pruning during reorg does not delete data needed by the new chain | ✅ | `mark_on_longest_chain(false)` sets `unmined_since = cbh` and `delete_eval` then clears DAH (`delete_eval.rs:153-167`) atomically with both secondary indexes (`engine.rs:2032-2040`, H1 critical section); sweep re-validation independently skips `unmined_since != 0` (`dispatch.rs:6086`). Deep protection is the retention window itself (288 blocks ≈ 2 days ≫ any plausible reorg). Residual: TOCTOU vs preserve (KO-3) and operator-set tiny retention (KO-10 note). |
| MarkLongestChainBatch interaction with pruning correct | ✅ | Handler (`dispatch.rs:5296`) → `engine.mark_on_longest_chain` → DAH re-evaluated per tx; on→0/off→cbh matches spec §3.15; primary + DAH + unmined updated atomically; replicated as dedicated ReplicaOp (R-052). `unmined_since` updates feed the unmined index used by the old-unmined prune path. Edge: cbh==0 sentinel collision (KO-7). |

### Checklist O — Bitcoin/Teranode-specific

| Item | Verdict | Evidence |
|---|---|---|
| Coinbase maturity matches consensus (100 blocks); operators vs teranode.lua | ✅ (with note) | Gate: `IS_COINBASE && spending_height > 0 && spending_height > current_block_height → COINBASE_IMMATURE` (`engine.rs:1075-1083`, single-spend `1322-1330`) — identical operator to spec §3.4 rule 4 (quoting teranode.lua 284-466), spendable at `current == spending_height`. The `+100` itself is **client-computed** at create (`create.rs:66` doc: "blockHeight + 100"); the server stores and compares but cannot independently enforce maturity — same trust model as Aerospike. Lua source unavailable for byte-level operator check (KO-6). Reassign also enforces the gate using `req.block_height` as current height (R-017 — hardening beyond Lua, documented in-code). |
| Reorg: MarkLongestChainBatch → dependent txs' mined status recomputed | ✅ | Store-level semantics correct per tx (above). Recomputation across *dependents* is, as in the reference system, the Go client's responsibility — the block-validation reorg path sends the complete tx set; the store offers no traversal and the spec doesn't ask it to (§3.15). `set_mined(unset)` leaving 0 entries sets `unmined_since = cbh` and clears DAH (tested: `set_mined_then_unset_all_sets_unmined`, engine.rs:8802). |
| Conflicting children: descendants identified; traversal complete? | ❌ | Descendant traversal is split: parents' `conflicting_children` lists are maintained server-side (create-conflicting + set_conflicting → `append_conflicting_children_from_cold_data`), readable via `FieldMask::CONFLICTING_CHILDREN`; the cascade itself is client-side per spec ("tracked at application layer", spec:140). Completeness is NOT guaranteed: best-effort warn-only appends, 255-child hard cap, cold-data parse failures all silently truncate the list (KO-5); and the setConflicting response omits the spending data the spec says the client cascade needs (KO-4). |
| `delete_at_height` set and respected for unmined txs older than retention | ✅ (intentional design difference, documented) | Unmined txs deliberately never get DAH (`delete_eval.rs:66-70` doc; `unmined_tx_no_dah` test). Their pruning is driven by the unmined secondary index: `OP_QUERY_OLD_UNMINED(cutoff)` (re-validated against metadata, ownership-filtered, preserved-records excluded) + client-driven `OP_DELETE_BATCH` — matching the spec's pruner lifecycle (§3.18 Phase 1/2) where unmined deletion uses `UnminedTxRetention`, not `block_height_retention`. Delete of a child also prunes parent slots (`PruneSlotIfSpentBy`, R-119) preventing resurrection re-spends. |
| Line-by-line teranode.lua cross-reference | ⚠️ | Lua source absent from repo (KO-6) — cross-reference performed against spec's quoted Lua semantics instead. Results in divergence table below. |

## teranode.lua divergence table

Per-function comparison of Rust implementation vs reference semantics as recorded in
`BSV_UTXO_STORE_SPEC.md` (which quotes teranode.lua by line range). "Spec-match" = no
detectable divergence from the recorded reference behavior.

| Lua function (spec §, Lua lines) | Rust location | Verdict | Notes |
|---|---|---|---|
| `spend` (§3.4, 284-466) | `engine.rs:1036-1290` (multi), `1291+` (single) | ⚠️ intentional divergence + 1 doc gap | Error precedence matches spec exactly: TX_NOT_FOUND → CONFLICTING → LOCKED → COINBASE_IMMATURE → per-item (UTXO_NOT_FOUND → HASH_MISMATCH → status). Frozen-vs-spent precedence: legacy all-0xFF spent slot → FROZEN before AlreadySpent, matching spec order; idempotent-respend memcmp first, matching spec. FROZEN_UNTIL comparator `>` vs spec's `>=` — **intentional** (Teranode PR #949 post-fix), spec stale (KO-8). Additions beyond Lua: reserved all-0xFF request sentinel rejected (F-G2-002), deleted-children defense (F-X-022) — both documented in-code, both fail-safe. Special values: all-zeros spending data is not reserved in either system (status byte disambiguates spent-with-zeros from unspent). |
| `unspend` (§3.5, 478-540) | `engine.rs:1489-1601` | ⚠️ undocumented tightening | Status precedence matches (unspent→no-op, pruned→INVALID_SPEND/Pruned, frozen→FROZEN). Extra requirement: stored spending_data must equal request's, else InvalidSpend — not in spec/Lua (KO-9). Counter-underflow guarded (StorageError instead of wrap). |
| `setMined` (§3.6, 543-656) | `engine.rs:1607-1965` | ✅ spec-match | Idempotent block_id append (inline + overflow), unset with swap-remove, `unmined_since` rules 5a/5b exact, LOCKED cleared, DAH evaluated. Fast path equivalent to slow path for count==0 (DAH from cached fields; KO-11 staleness hardening note). Overflow entries (count>3) are a TeraSlab extension — Lua list had no inline/overflow split; behavior equivalent. |
| `markOnLongestChain` (§3.15) | `engine.rs:1981-2046` | ✅ spec-match | on→0 / off→cbh, unmined index add/remove, DAH re-eval, atomic primary+secondaries. cbh==0 sentinel edge (KO-7) shared with spec text. |
| `freeze` (§3.7, 666-738) | `engine.rs:2792-2840` | ✅ spec-match | Precedence: NOT_FOUND → HASH_MISMATCH → ALREADY_FROZEN → SPENT(with spending data) → ok. Status 0xFF + spending_data all-0xFF exactly as spec. Does not touch `spent_utxos` (all-spent math per spec note). Generation bump + cache sync (R-016) is additive bookkeeping. |
| `unfreeze` (§3.8, 748-811) | `engine.rs:2846-2878` | ✅ spec-match | Must-be-frozen → NotFrozen; restores zeroed spending_data (spendable_height=0 = immediately spendable, per spec note). |
| `reassign` (§3.9, 821-...) | `engine.rs:2880-2960` | ⚠️ intentional hardening | Core matches: old-hash check, must-be-frozen, new hash + spendable_height = block_height + spendable_after (checked_add R-063 vs Lua's unchecked). Added record-level guards CONFLICTING/LOCKED/coinbase-immature (R-017) — not in Lua; documented in-code as deliberate. |
| `setConflicting` (§3.10, 1025-1051) | `engine.rs:3533-3672`, dispatch `4377` | ❌ response divergence | Flag set/clear + DAH eval matches (conflicting+no-DAH → set `cbh+retention`; clear → DAH re-evaluated/cleared). Response omits spec-required per-tx spending data (KO-4). Server-side conflicting-children propagation is a TeraSlab addition with silent-loss paths (KO-5). Sweep can never delete the records this op schedules (KO-2). |
| `setLocked` (§3.11, 1109-1135) | `engine.rs:3679+` (`set_locked_*`) | ✅ spec-match | Locking clears DAH; unlocking does not restore (matching Lua); before-image variant exists for replication compensation of the cleared DAH (F-G2-013). |
| `preserveUntil` (§3.12, 1067-1095) | `engine.rs:3887-3931` | ✅ op itself spec-match; ❌ lifecycle | Clears DAH, sets preserve_until, PRESERVE signal for EXTERNAL, DAH-index entry removed, cache discriminant synced (R-019). But the preservation can never expire (KO-1). |
| `setDeleteAtHeight` (§3.13, 927-1008) | `delete_eval.rs:71-308` | ✅ spec-match | Branch-for-branch identical to spec pseudocode: retention==0 / preserve!=0 early-outs, CONFLICTING set-if-unset, `dah==0 OR dah<new_dah` monotonic update, clear-when-unmet, LAST_SPENT_ALL transition signaling, EXTERNAL-only DAHSET/DAHUNSET signals. Overflow → error rather than Lua's unchecked add (improvement, tested). Cached-field variant mirrors exactly. |
| `incrementSpentExtraRecs` (§3.14, 1145-1199) | — | ✅ intentionally eliminated | No pagination in TeraSlab; spec documents elimination. |
| `getSpend` (§3.16) | `engine.rs:4035-4090` | ✅ spec-match | Status byte + spending data (spent/frozen/pruned) + locktime; lock-free with txid re-verify. |
| `delete` / pruner (§3.18) | `engine.rs:3960-4028`, dispatch `4836` (delete), `6036` (sweep), `5854` (old-unmined query), `5903` (preserve txs) | ❌ Phase 3 missing | Delete: tombstone→sync→unregister→free ordering sound; secondary cleanup; external blob handled in dispatch snapshot/compensation path. Phase 1 (preserve parents) ✅ via QUERY_OLD_UNMINED + PRESERVE_TRANSACTIONS. Phase 2 (DAH cleanup) ⚠️ implemented but over-restricted (KO-2) and racy vs preserve (KO-3). Phase 3 (expired-preservation processing) ❌ not implemented anywhere (KO-1). |

## Finding tally

- CRITICAL: 0
- HIGH: 2 (KO-1 permanent preservations / missing expiry phase; KO-2 conflicting txs never deletable by DAH sweep)
- MEDIUM: 4 (KO-3 preserve/delete TOCTOU; KO-4 setConflicting response missing cascade data; KO-5 conflicting-children silent truncation; KO-6 missing teranode.lua reference)
- LOW: 5 (KO-7 height-0 sentinel; KO-8 stale spec comparator; KO-9 undocumented unspend tightening; KO-10 README retention mislabel; KO-11 fast-path cached-DAH writeback)
