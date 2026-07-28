# R1 — Preservation expiry on a non-master holder

Status: **RESOLVED — Option A implemented.** See §8 (Decision). The
recommendation in §5 below is **known-WRONG** and is kept only as the record of
what was considered and why it was rejected; do not implement it.

All file:line references are against `main` @ `2e69fe4` (the F1 client-delete
merge). Every claim below was read out of a function body, not a doc comment.

## 1. Problem

`OP_PROCESS_EXPIRED_PRESERVATIONS` runs two phases. F2 (`21a9554`) fixed the
second one: the DAH sweep now filters on **holdership**, so a node reclaims the
replica copies it stores as well as the records it masters
(`dispatch.rs:11339-11346`, `sweep_role_snap`). The **first** phase — the
preserve→DAH transition — was left master-gated:

```rust
// dispatch.rs:11280-11287
for key in &expired_candidates {
    // Ownership: only the master schedules expiry for its records.
    if check_shard_ownership_snap(&key.txid, 0, cluster, master_snap.as_ref(), false)
        .is_some()
    {
        continue;
    }
    match engine.expire_preservation_set_dah(key, current_height, block_height_retention) {
```

`expire_preservation_set_dah` (`engine.rs:8844`) is the only production writer
that clears a `preserve_until` and, when the record is sweep-eligible, plants
the replacement `delete_at_height` (`engine.rs:8904-8923`). On a node that
holds the record as a replica it never runs, and nothing else clears
`preserve_until` on that node.

The consequence chain, each link verified:

1. `PreserveUntil` **is** replicated. `handle_preserve_until_batch` ships
   `ReplicaOp::PreserveUntil { block_height, master_generation }`
   (`dispatch.rs:9483-9490`); the receiver applies it through
   `engine.preserve_until` (`receiver.rs:2146-2160`), which writes the footer
   and inserts into the preserve index (`engine.rs:8233`, `:8261`). So the
   replica genuinely carries `preserve_until != 0`.
2. Nothing replicates the expiry. The Phase-0 loop calls the engine directly and
   builds no `ReplicaOp` (`dispatch.rs:11287`).
3. Nothing replicates the eventual reclaim either. A sweep delete is per-holder
   local GC and is deliberately excluded from the fan-out
   (`dispatch.rs:9785-9791`: `repl_ops_by_key` is empty when
   `sweep_due_height.is_some()`).
4. With `preserve_until != 0` the record is permanently excluded from GC on that
   node. `record_due_for_sweep` returns `false` on the first line
   (`engine.rs:8305`), and **every** DAH-planting path is also blocked:
   `evaluate_delete_at_height` early-returns before it can emit a patch
   (`delete_eval.rs:92`, and the flag-cached variant at `delete_eval.rs:290`).
   So a later replicated `Spend` / `SetMined` / `MarkLongestChain` cannot rescue
   the copy — it will not plant a DAH while the stale preservation stands.
5. A restart does not clear it. `rebuild_preserve_index_from_device`
   (`engine.rs:9234`) re-derives the preserve index from the authoritative
   device footer, so the leaked entries come straight back.

The only escapes are (a) a **client** `OP_DELETE_BATCH`, which F1 (`49d06fc`)
now replicates as `ReplicaOp::Delete` and which removes the record and its
preserve entry (`engine.rs:8752`, in `delete_inner`), and (b) a shard migration
or heal, where `restore_migrated_lifecycle` overwrites the target's
`preserve_until` / `delete_at_height` from the source image
(`receiver.rs:1370-1394`). Neither is part of the pruner's steady state.

**Two sub-populations leak, not one.** At expiry the master plants a DAH only if
the record is sweep-eligible (`engine.rs:8904-8916`). For the ineligible case the
master merely clears `preserve_until` and lets the record re-acquire a DAH later
via the normal spend/setMined path. Both cases leak on the replica: in the first
the master schedules and sweeps its own copy while the replica never does; in the
second the replica's stale `preserve_until` blocks the later DAH-planting path
outright (point 4). There is no eventual-consistency argument available for
either.

### Correction to the framing

The brief stated that the new DAH is computed from the observing node's
`last_durable_height` atomic (`engine.rs:374`). It is not. `handle_process_expired`
takes `current_height` and `block_height_retention` **off the wire payload**
(`dispatch.rs:11249-11257`) and passes them straight through; `last_durable_height`
is not read on this path at all (it feeds the tombstone GC horizon —
`engine.rs:365-374`). The divergence concern survives, but its source is the
per-node pruner call: the client drives each node separately
(`client/rust/src/lib.rs:2929`), each call carries whatever height that caller
has reached, and each node's Phase-0 work is capped independently. Two holders
therefore compute DAHs that differ by the pruner-call skew — a bounded skew,
not an unbounded one.

## 2. Blast radius

I could not put a number on "what fraction of records are ever preserved" — it
is a property of the Teranode workload (Phase 1 preserves the *parents of
transactions still unmined after `UnminedTxRetention` = 144 blocks*, spec
§3.18), not of this repo. What I can do is bound the mechanism, and the
mechanism is worse than a space leak. Three effects, in increasing severity.

### 2.1 Space — linear, monotonic, restart-proof

Per leaked record, on each non-master holder: the full record region, one
24-byte primary-index bucket (`hashtable.rs:194`), and one preserve-index entry
that is never removed. Under RF = 2 roughly half of every node's records are
replica copies, so the leak accrues at ≈ `(RF-1)/RF` × the cluster-wide
preserve rate, forever. It is *cumulative over all preservations ever applied*,
not proportional to the live preserved set — which is what makes the spec's own
sizing note (§5.5.3: "a small fraction of the store — smaller than the DAH set")
misleading here. That note bounds the live set; the leak has no such bound.

Observability today is adequate: `preserve_entries` is already exported on
`/metrics` and the status endpoints (`http.rs:708`, `:1177`, `:1880`, `:2150`).
A node whose `preserve_entries` grows monotonically and never drains *is* the
symptom.

### 2.2 Head-of-line starvation of Phase 0 — the escalation

This is the finding that changes the decision. `range_query_limited` returns the
entries with the **lowest `preserve_until` first**, capped at `max_batch`
(`preserve_index.rs:110-124`; default `max_batch_size = 8192`,
`config.rs:1749`). Leaked entries are never removed and, because they expired
earliest, permanently occupy the *head* of that ordering. New preservations
enter at `current_height + ParentPreservationBlocks` — always above them.

Once a node accumulates ≥ `max_batch` leaked entries, every Phase-0 query
returns only leaked entries, all of which the master gate skips, and the node
performs **zero** preservation expiry — including for the shards it masters.
Its own mastered records then never get a DAH, are never swept, and their
preserve entries pile up behind the leaked ones.

This is not a novel hazard; it is exactly the cap-starvation class the codebase
already reasons about for the DAH index. `engine.rs:8882-8887` and spec §3.18
Phase 3 both spell it out ("≥ `max_batch` such low-height entries → every capped
query returns only them → genuinely-due records never reached"), and it is why
the eligibility gate exists at all. The same hole is open on the preserve index,
one loop above, and the master gate is what opens it. The threshold is 8192
records — small in absolute terms for a store sized in billions.

### 2.3 A standing reverse-heal fence on the master

Reverse-heal is **on by default for RF > 1** (`config.rs:1131-1200`, test at
`config.rs:2820`). Its recency fingerprint is per-shard `(count, digest,
max_generation)` where `count` is the live record count and `digest` folds
`(txid, generation)` (`engine.rs:3077-3097`, `coordinator.rs:4948-4955`).

For the leaked population the master eventually sweeps its copy and the replica
never does, so the replica's `count` for that shard is **permanently** higher
than the master's. `is_self_behind_any_replica_coarse` flags on
`r.count > self.count` (`coordinator.rs:5088-5095`), and `trigger_online_reheal`
fences and pulls when that fires: `register_heal_source` raises the
no-serve-before-heal fence so `is_master` answers `Transitioning` rather than
`Yes` until the pull completes (`coordinator.rs:5720-5810`, doc at
`:5703-5712`). A `self_behind` shard is explicitly **not** backoff-suppressed —
`backoff.remove(&c.shard)` on that branch (`coordinator.rs:5776-5781`) — so the
suppression that protects an ahead/equal master does not apply.

The pull itself is delete-safe: the master's sweep removal is
`engine.prune_delete` → `RemovalAuthority::Authoritative`, which records a
generation-aware tombstone (`dispatch.rs:9928-9930`, `engine.rs:8501-8507`), and
RULE-DS drops the resurrecting baseline (`receiver.rs:1765-1781`). So this is
not data loss. It is a **master availability event plus a full shard baseline
transfer**, repeating because the count skew never resolves. The trigger is
topology activation after the partition-view exchange
(`coordinator.rs:2415`), not a periodic ticker — so the cost is "every topology
change fences the affected shards", not a hot loop. That is the honest severity:
recurring, not continuous.

### 2.4 Verdict on severity

The space leak alone might justify "accept and document". §2.2 and §2.3 do not.
§2.2 converts a replica-side leak into a whole-node stall of preservation expiry
after a small, fixed threshold, and §2.3 turns it into a standing
master-fencing trigger under the default RF > 1 configuration. **Accept-and-
document is off the table.**

## 3. The generation constraint, stated precisely

`generation` is the per-record replication ordering token. Two mechanisms
consume it:

- **Pre-apply guard** (`receiver.rs:1727-1742`): an op is dropped iff
  `local_gen != master_gen && generation_at_or_ahead(local_gen, master_gen)`.
  Equal generations are *allowed through*.
- **Post-apply sync** (`receiver.rs:2424-2446`): after every op carrying a
  `master_generation`, the replica's generation is forced to the master's via
  `set_record_generation`.

Consequences that matter for this design, and that the "a replica bumping
generation drifts out of step" framing understates in one place and overstates
in another:

1. **Replica behind is normal and safe.** The master's Phase-0 expiry already
   bumps its own generation (`engine.rs:8924`) and replicates nothing, so master
   and replica generations *already* diverge today with the master ahead. That
   direction is absorbed: the next replicated op syncs the replica forward.
2. **Replica ahead is the hazardous direction**, and the damage is not only
   "spuriously skipped ops". A single local bump makes `local_gen` equal the
   master's *next* `master_generation`, which the pre-apply guard passes — but
   arms that treat equal-generation as an idempotent replay then skip the actual
   mutation. `MarkLongestChain` does exactly this (`receiver.rs:2369-2390`): the
   replica would silently drop a real longest-chain transition. Worse, a replica
   whose generation exceeds its master's raises the shard's reported
   `max_generation`, which is the *other* input to
   `is_self_behind_any_replica_coarse` — i.e. a replica-side bookkeeping bump can
   fence the master (§2.3).
3. **The digest sees generation, nothing else.** `recency_for_keys` folds only
   `(txid, generation)` (`engine.rs:3086`). `delete_at_height` and
   `preserve_until` are invisible to it.
4. **The spec does not require the expiry to bump generation.** The mutation-
   bookkeeping paragraph (`specs/BSV_UTXO_STORE_SPEC.md:341`) enumerates the
   operations that increment it — "spend, unspend, setMined, freeze, unfreeze,
   reassign, setConflicting, setLocked, preserveUntil, pruneSlot". The
   preservation *expiry* is not in that list. The bump at `engine.rs:8924` is an
   implementation choice, not a contract.

And the DAH's status, which is the mirror-image conclusion: **the DAH is already
per-holder derived state, not shipped authoritative state.** `ReplicaOp::Spend`
carries `current_block_height` + `block_height_retention` and the replica
recomputes its own DAH locally through `engine.spend` (`receiver.rs:1804-1841`);
no op ships a `delete_at_height` value in steady state. Nothing cross-checks
DAH values between holders — the digest cannot see them (point 3), and the sweep
that consumes them is per-holder local GC. Only the migration baseline
transports a DAH (`receiver.rs:1370-1394`), and that is a whole-image copy.

So the two fields the brief bundles together have opposite risk profiles:
**diverging the DAH between holders is cheap; diverging generation is not.**

## 4. Options

### A — Replicate the expiry

Master performs the transition and ships it as a new `ReplicaOp` carrying
`master_generation` plus the height inputs, like every other mutation.

- **Correctness:** highest. Generation stays in lockstep by construction; the
  DAH matches because both sides derive it from the same shipped inputs, which
  is precisely the existing `Spend` pattern.
- **Cost:** a replication round-trip per pruner call (up to `max_batch` = 8192
  ops), on a path that today does no network I/O at all. A new wire op code —
  every node must understand it before any node emits it, so this is a
  rolling-upgrade-ordered change.
- **Compensation:** straightforward, and *not* the F1 delete-inversion case. The
  transition is invertible (restore `preserve_until`, drop the DAH), so the
  standard apply-then-compensate ordering works; the existing
  `ReplicaOp::PreserveUntil` compensation arm (`dispatch.rs:4614-4630`) is the
  template — noting it is already lossy, compensating to `block_height = 0`
  rather than restoring the prior height.
- **Failure modes:** a fan-out failure now fails or partially fails the pruner
  call, so the sweep inherits replication's availability. Under partition the
  expiry stalls cluster-wide rather than per-node — GC stops on the master too,
  which is a *new* coupling. Under handoff the new master re-runs Phase 0 and
  re-ships; idempotent by generation. Restart is unaffected.
- **Note:** this makes GC progress depend on replication health. That is the
  opposite of F2's direction, which deliberately made reclaim a local decision.

### B — Holder-gated expiry, derived, no generation bump off-master

Change the Phase-0 gate from mastership to **holdership** — the same edit F2
made one loop below — and make the non-master path leave `generation` untouched.
Each holder clears its own `preserve_until` and plants its own DAH from its own
pruner-supplied height.

- **Correctness:** the generation hazard disappears entirely — no bump means no
  drift, no digest change, no `max_generation` inflation, so §2.3's fence
  trigger is *removed* rather than traded. The DAHs of two holders differ by the
  pruner-call skew, which per §3 nothing observes and nothing compares.
- **Cost:** near zero. No new wire op, no round-trip, no rolling-upgrade
  ordering. Reuses `sweep_role_snap` and should reuse F2's replication-lag fence
  (`replica_stream_hole_within(REPLICA_RECLAIM_QUIET_PERIOD_MS)`,
  `dispatch.rs:11347-11348`) so a node behind on inbound replication does not
  schedule a reclaim it should not.
- **What it actually changes:** it introduces a class of local footer write that
  does *not* bump generation. That needs to be a stated, enforced rule, not a
  local exception: *a non-master holder may mutate only the local GC-schedule
  fields (`preserve_until` → 0, `delete_at_height`) and never `generation` or
  any authoritative field.*
- **Failure modes:** the eligibility inputs (`sweep_eligible_with_mined`, mined
  state from the replicated MinedIndex) can differ transiently under replication
  lag, so one holder may plant a DAH while the other only clears the
  preservation. Both still converge — the second acquires a DAH later via the
  normal path — but the two copies are reclaimed at different times. Under
  partition each holder proceeds independently, which is the intended property.
  Under handoff a new master inherits a copy whose lifecycle fields may differ
  from the old master's; since it re-derives the schedule anyway, this is benign.
  Restart is unaffected (the rebuild reads the footer).
- **The real cost, stated plainly:** the expiry becomes invisible to the recency
  digest on *both* sides. Today a master-side expiry at least perturbs the
  fingerprint. After this change, a holder that fails to expire is
  indistinguishable by digest from one that succeeded. This is the standard
  price of derived state and it argues for the metric in §6, not against the
  option — but it should be a conscious trade, not a side effect.
- **Open sub-decision:** whether the *master's* expiry should also stop bumping
  generation. Doing so would make the two sides' digests match exactly and is
  spec-compliant (§3 point 4); it also removes a pre-existing source of Tier-2
  over-flagging. Not doing so leaves the master permanently one generation ahead
  after each expiry, which is the safe direction but keeps the noise.

### C — Lazy convergence (rejected on the evidence)

Investigated as instructed. There is **no** steady-state path that re-ships
lifecycle state. `restore_migrated_lifecycle` is reached only from
`apply_create_lifecycle_and_blob` on a `Create` carrying ≥ 70 metadata bytes
(`receiver.rs:1354-1409`) — i.e. the migration/heal baseline. The one
steady-state full-image re-ship, `repair_missing_record_target`
(`dispatch.rs:5092`), fires only on a `MissingRecord` NAK, which requires the
record to be *absent* on the replica; a leaked record is present, so it never
triggers. And per §1 point 4 a record whose preservation just expired can expect
no further DAH-planting op anyway, because its own stale `preserve_until` blocks
the evaluation. Nothing converges. This option is dead.

### D — Accept and document (rejected on the evidence)

Defensible against §2.1 alone. Not defensible against §2.2 (whole-node
preservation-expiry stall past 8192 leaked entries) or §2.3 (standing
master-fencing trigger under the default RF > 1 config). Both are step changes
in behaviour, not gradual degradations, and both arrive without warning at a
threshold no operator is currently watching for.

## 5. Recommendation (SUPERSEDED — see §8)

> **This recommendation was falsified and NOT implemented.** An adversarial
> pass found two consumers that require holders to AGREE on `delete_at_height`,
> which is the load-bearing assumption §6.1 flagged as "the question that
> decides the design". The answer turned out to be *yes, there are such
> consumers*, so B is unsafe. §8 records the evidence and the decision. The
> text below is preserved verbatim as the reasoning that was overturned.

**Take B.** Extend the Phase-0 gate from mastership to holdership, and make the
non-master transition leave `generation` untouched.

Reasoning:

1. It is the same correction F2 already made one loop below, for the same
   reason. Phase 0 and Phase 2 are two halves of one lifecycle; leaving them on
   different authority models is what produced this defect. Fixing the half that
   was missed is smaller and more coherent than introducing a new replicated op.
2. It targets the actual hazard. §3 establishes that DAH divergence between
   holders is unobserved and unobservable in steady state, while generation
   divergence drives both the receiver's skip logic and the reverse-heal fence.
   B diverges only the cheap field.
3. It *removes* the §2.3 fence trigger instead of trading it, because the
   replica starts reclaiming and the shard counts converge again.
4. It keeps GC a local decision. Option A would make preservation expiry — and
   therefore all downstream reclaim — depend on replication health, which is a
   new failure coupling on a background maintenance path.

**What I would not do:**

- **Not A**, unless the answer to open question 6.1 is that DAH values must
  agree across holders for a reason I did not find. A carries real cost (new
  wire op, upgrade ordering, a round-trip on a bulk background path, GC coupled
  to replication availability) to buy an agreement that nothing currently reads.
- **Not "extend the gate to holdership and leave the generation bump in place."**
  That is the smallest diff and it is the wrong one: it makes every replica
  independently outrun its master's generation, which drops real mutations in
  the equal-generation arms (`receiver.rs:2369-2390`) and inflates the shard's
  `max_generation` into the master-fencing input. This is precisely the boundary
  the previous agent correctly refused to cross.
- **Not D.** See §2.4.
- **Not a Phase-0 cursor / skip-list** to work around §2.2 alone. It would
  paper over the head-of-line starvation while leaving the leak, the count skew,
  and the fence trigger intact.

## 6. Open questions for the maintainer

1. **Is there any consumer of `delete_at_height` that requires holders to
   agree?** I found none — the digest cannot see it (`engine.rs:3086`), the
   sweep is per-holder, and only the migration baseline transports it. This is
   the load-bearing assumption under B; if it is wrong, B is wrong. **This is
   the question that decides the design.**
2. **Should the master's expiry also stop bumping `generation`?** The spec's
   mutation-bookkeeping list (`specs/BSV_UTXO_STORE_SPEC.md:341`) does not
   include the expiry, so dropping it is
   compliant and would eliminate a pre-existing source of Tier-2 digest
   over-flagging. It also further weakens the digest as a change detector. Decide
   deliberately; do not let it fall out of the implementation.
3. **Is losing digest visibility of the expiry acceptable?** Under B neither
   side's expiry perturbs the fingerprint, so "one holder expired, the other did
   not" becomes invisible to reverse-heal detection. My position: acceptable,
   because the expiry is derived state and the correct detector is a metric on
   the transition itself — but it is a real reduction in cross-node checking.
4. **What is the actual preserve rate on the target workload?** It does not
   change the recommendation (the mechanism is unbounded either way), but it
   determines urgency: how long until a production node crosses the 8192-entry
   Phase-0 starvation threshold of §2.2.
5. **Does the pruner drive every node, or route through the master?** The client
   exposes `process_expired_preservations` per connection
   (`client/rust/src/lib.rs:2929`) and the DAH sweep already requires per-node
   calls to work at all post-F2, so per-node is the assumption B rests on. Worth
   confirming against the deployed pruner rather than the client library.
6. **Is a one-off scrub needed for existing deployments?** Any cluster that has
   run with this defect carries leaked replica copies with stale
   `preserve_until`. B fixes the flow; it does not by itself decide whether the
   backlog drains acceptably through the normal capped Phase-0 passes, or
   whether it needs an explicit catch-up. The backlog drains lowest-height-first,
   which is the right order, but it competes with live work under the same cap.

## 7. What I could not determine

- The preserve rate on a real Teranode workload (§6.4) — not derivable from this
  repo.
- Whether any operator tooling outside this repo reads `delete_at_height` and
  compares it across nodes (§6.1 is answered *within* the repo only).
- The steady-state frequency of topology activation, which sets how often §2.3's
  fence would actually fire. The trigger site is topology activation after the
  partition-view exchange (`coordinator.rs:2415`); I did not trace what drives
  that loop in a stable cluster with no membership changes.


## 8. Decision — Option A, and why B was falsified

Added after implementation. Everything in §§1-4 (the defect, the blast radius,
the generation constraint, the option space) still holds. §5's *recommendation*
does not.

### 8.1 The question that decided it

§6.1 asked: **is there any consumer of `delete_at_height` that requires holders
to agree?** The design pass found none and rested Option B on that. An
adversarial falsification pass found two. Both were read out of function bodies.

1. **`Engine::restore_migrated_lifecycle`** (`src/ops/engine.rs`) assigns
   `meta.delete_at_height = delete_at_height` **unconditionally** — no
   equal-generation short-circuit anywhere on that path. It is reached from
   `apply_create_lifecycle_and_blob` for any migration or heal baseline. The
   reverse-heal *source* is a non-master by construction, and it ships its own
   lifecycle as authoritative. The only downgrade guard is
   `incoming_generation < existing_generation` in `receiver.rs` — which under B
   (which deliberately leaves `generation` untouched off-master) is **inert
   exactly where the DAHs would differ**, because the two holders' generations
   are equal by design. A heal at equal generation therefore installs one
   holder's DAH — possibly 0 — over the other's on an all-spent record that no
   later path will re-DAH: an immortal record, the `#25` class.

   A stale comment in `receiver.rs` asserted that this apply "is an idempotent
   no-op when generations are equal", which is exactly the belief that makes B
   look safe. It is false against the body. It has been corrected (constraint 4
   of the implementation brief) — the third stale comment in this campaign found
   sitting directly on top of the defect it obscured.

2. **`payloads_match`** (`teraslab-tests/client/tests/common/mod.rs`) — the
   repo's own cross-holder consistency oracle — byte-compares two holders' full
   `FIELD_ALL` GET payloads masking **only** `updated_at` (item-data offsets
   61..69). `delete_at_height` sits at 73..77 and is compared, with
   `assert_eq!(mismatches, 0)` in E2E scenarios 03/08/12/15/16/17. The repo
   already treats DAH as a must-agree field; §3's "nothing compares it" was
   wrong.

Widening that mask to make B pass was explicitly rejected: it would weaken a
cross-holder consistency check to accommodate a design, which is the opposite
direction from everything else this campaign has done.

### 8.2 What was built

Option A, with one refinement over §4's sketch: the op ships the master's
**result**, not the inputs it derived the result from.

`ReplicaOp::ExpirePreservation { tx_key, delete_at_height, master_generation }`
(wire tag 19), applied by holders through
`Engine::apply_replicated_preservation_expiry`, which sets `preserve_until = 0`
and `delete_at_height` to the master's value. §4-A assumed the `Spend` pattern
(ship heights, re-derive locally) would suffice. It would not: expiry
eligibility reads the node-local `MinedIndex` (`sweep_eligible_with_mined`),
which lags under replication delay, so two holders can legitimately reach
opposite verdicts from identical inputs. Deriving would have reintroduced the
divergence the op exists to prevent.

Both branches replicate. The ineligible one (`delete_at_height == 0`) is not
optional — §1 point 4 is why: a holder whose stale `preserve_until` survives has
every DAH-planting path blocked and is invisible to its own sweep, so its copy
is immortal.

**Wire compatibility.** A NEW tag, not extra fields on `PreserveUntil`.
`ReplicaOp::deserialize` bounds each op with a MINIMUM length check and
`ReplicaBatch::deserialize` discards the per-op consumed length in favour of the
framed `op_len`, so a pre-upgrade peer handed a widened `PreserveUntil` would
decode the old prefix, silently ignore the appended DAH, and apply
`delete_at_height = 0` — permanent, undetected divergence. An unknown tag fails
closed instead (`UnknownOp` → batch fails → master retries). The cost is
rolling-upgrade ordering: every node must understand tag 19 before any node
emits it.

**A new redo entry was unavoidable.** `RedoOp::ExpirePreservation { tx_key,
delete_at_height }` (tag 44), journalled WAL-first inside the engine by BOTH the
master and the applying holder. No pre-existing entry expresses the transition:
`PreserveUntil { block_height: 0 }` clears the preservation but its replay
forces `delete_at_height = 0`, which would **erase** the schedule the eligible
branch just planted, and `SecondaryDahUpdate` restores the index entry but not
the footer. Journalling the same entry on both sides is what makes the two
holders' crash-recovery behaviour symmetric rather than merely similar. It is
also the conversion source for the replication intent's crash re-ship and for
migration deltas.

**Ordering: apply → journal → intent → release barrier → replicate → compensate
on failure.** The standard mutation shape, *not* the delete path's inverted
replicate-then-apply. §4-A got this right and the reason is worth stating
positively rather than as "we are allowed to": the transition is exactly
invertible (a preserved record carries no DAH, so writing the prior
`preserve_until` back through `preserveUntil` restores the entire pre-expiry
footer AND evicts the scheduled DAH), so rollback is available — whereas
replicate-first would force the master to re-take each stripe lock to apply, and
a preservation landing in that window would make the master skip a record the
holders had already expired. That is divergence with no way back.

A clean rollback does **not** fail the RPC. The Phase-2 DAH sweep is independent
per-holder GC and still runs, so degraded replication delays the expiry without
stalling reclaim. This is the minimum form of §4-A's "GC now depends on
replication health" cost: only the *scheduling* half couples, never the
*reclaiming* half. Only a rollback that did not complete cleanly is surfaced as
an error, because only then can holders actually differ.

### 8.3 The #88 barrier hand-off

`OP_PROCESS_EXPIRED_PRESERVATIONS` is a mutation opcode that does NOT
self-manage visibility, so `handle_request` hands it the **coarse global
EXCLUSIVE** guard (`VisibilityBarrier::global_write`) as a `MutationBarrier`,
and it holds it for the whole handler. Adding a fan-out under that guard is
precisely issue #88: an inbound non-migration `OP_REPLICA_BATCH` takes the same
lock exclusively on its own thread, so A-holds-and-waits-for-B while B does the
same is a cross-node circular wait broken only by the ack timeout.

Held on this thread when Phase 0 finishes its applies: the global write guard,
and nothing else — every per-tx stripe lock taken inside
`expire_preservation_set_dah` was released with its scope, as was each per-store
redo mutex. The barrier is therefore **handed to
`replicate_all_ops_with_barrier`**, which releases it before the round-trip and
before the fan-out admission wait.

That consumes it, so the Phase-2 sweep is then given `barrier = None` and
`handle_delete_batch` acquires its OWN global SHARED + per-key WRITE visibility
— the same regime the client delete path uses, and the one that handler was
designed for (it is in `manages_own_visibility`). Its fail-closed invariant
(`sweep_due_height.is_none() && barrier.is_some()` → `ERR_INVARIANT_VIOLATION`)
is untouched and still holds: the sweep still never replicates. On a pruner call
where Phase 0 expired nothing, the barrier is not taken and is forwarded exactly
as before, so the common no-op path is byte-for-byte unchanged.

### 8.4 Answers to §6

1. **Consumers requiring DAH agreement?** Yes — two, see §8.1. This is what
   killed B.
2. **Should the master's expiry stop bumping `generation`?** No. Under A the
   bump is load-bearing: it is the `master_generation` the op carries, the token
   the receiver's pre-apply staleness guard and post-apply sync both use. The
   spec's mutation-bookkeeping list should be read as under-specified here, not
   as forbidding it.
3. **Digest visibility of the expiry?** Retained, and now meaningful: the master
   bumps and the holders take that same generation, so the shard fingerprint
   moves in lockstep instead of drifting.
4. **Preserve rate on the target workload?** Still unknown — not derivable from
   this repo. It sets urgency, not direction.
5. **Does the pruner drive every node?** No longer load-bearing. Under A only
   the master's Phase-0 pass matters; a pruner that reaches only masters is
   sufficient for the expiry (the Phase-2 reclaim still wants every node, as
   post-F2).
6. **One-off scrub for existing deployments?** Still open, and now narrower.
   Nodes that ran with the defect carry replica copies with a stale
   `preserve_until` that no master will ever expire — the master's own copy has
   already moved on, so nothing re-drives those keys. They drain only via a heal
   / migration baseline, or an explicit scrub. Tracked as a follow-up; this
   change fixes the flow, not the backlog.

### 8.5 Residual risk

The replication intent is opened **after** the applies, not before them, because
the value the redo entry carries — the scheduled `delete_at_height` — is only
decided by the under-lock eligibility verdict, so the usual journal-then-apply
shape is not available. A master crash in that window (in-memory work only, no
I/O between the last apply and the intent write) leaves the expiry durable
locally with no intent to re-ship it: those records' holders stay preserved
while the master carries a DAH, until a heal or migration reconciles them.

Every other mutation has the mirror window (intent durable, apply not) and
resolves it by re-shipping an idempotent op. This one cannot, because after the
crash the master's own preserve index no longer offers the key. The window is
bounded and rare, and it is strictly smaller than the pre-R1 behaviour, in which
the transition reached no holder at all, ever. Closing it fully needs a
reservable redo sequence (journal-before-apply with a value not yet known) —
noted, not built.
