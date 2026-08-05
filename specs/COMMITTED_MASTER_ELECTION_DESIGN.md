# Committed Master Election — design (rev 4)

**Status:** draft. Three review rounds (bitcoin-expert + security-auditor on
rev 1, 2, 3). Rev 3's reviewers both ended with "with these fixes I would sign
off" and stated a fourth *analysis* round should not be needed — the changes
below are the stated edit list, not new design.

**Context:** TeraSlab is not in production. Formats and wire protocol may
change freely; no migration path required.

## 0. Rev 3 → rev 4

| # | rev 3 defect | rev 4 |
|---|---|---|
| E1 | §4.4 fenced on vote-digest mismatch — but term numbers are **not globally reserved**, so an ordinary aborted proposal round bricks honest voters with no attacker present | §4.4 rejects, never fences |
| E2 | the attestation split lands on **different terms**, which §9 (equal-term only) cannot see, and §6.1 left both halves serving | §9 gains the generalised-C11 arm: persistently refusing a quorum-advertised higher term |
| E3 | §5's anchor had **no reversion edge** — a named master receives its migration, therefore holds data, therefore is never "proven data-less", so one term of influence is permanent | §5 anchor is **conditional** |
| E4 | §7 blanket-fenced through `NOT local_holder_check`: any data-derived predicate is false for a legitimately empty shard, and deadlocks | §7 uses **provenance**, not data |
| E5 | `OP_GET_COMMITTED_TOPOLOGY` **fabricates** the commit, so digest/voters/rule 8 are vacuous on the catch-up path | §10 persists and replays the winning commit verbatim |
| E6 | §12's table printed `RF/n` while claiming `min(RF,k)/n`; the secret gate did not cover §9; permanence unstated | §12 corrected |
| E7 | rule 6 `k=1.5` **rejects the honest assignment** on a routine wipe-and-rejoin, and rejection wedges the cluster | rule 6 recalibrated + demoted |

Rev 3's B1/B2/B4 substance, §3's two-object split, §6.1's reject≠fence, §8's
audit table, §10's versioning and §11's swap algorithm are confirmed correct by
both reviewers and are unchanged.

---

## 1. The defect

`apply_master_election` runs *after* commit, per node, from two **per-node**
inputs: `partition_view` (partial, divergent) and `prev_table` (differs by
history). Two nodes can elect different masters for one shard while each table
stays internally self-consistent, so `mismatched == 0` everywhere.

### 1.1 Fixes already measured and rejected — do not repeat

- **Faster convergence** (15s/30s → 5s/10s): worse. Scenario 09 hit
  `masters=5461/4096` at `ver=8`. Reverted in `18c7c07`.
- **Remove the refinement**: breaks failover —
  `segment_cluster_master_failover_preserves_replicated_record`.

### 1.2 The fixed point

Today's `det` table is a fixed point: the repair path is *view-independent*
(`activate_topology_with_view` with an empty view makes `apply_master_election`
return immediately), which is why divergence self-heals. **`det` must remain
the fixed point** — §5's conditional anchor is what preserves that while still
allowing failover. Rev 3's absolute anchor replaced the fixed point with
"whatever was last committed", which is why one bad term became permanent.

---

## 2. Requirements

R1 agreement · R2 failover locality · R3 bounded authority · R4 durability ·
R5 fail-closed · R6 in-band detection · R7 **no single frame, and no proposer
bug, causes a cluster-wide or unclearable outage**.

---

## 3. Two distinct objects

- **Committed assignment** — term-scoped, immutable once committed,
  digest-bound, persisted, gossiped. The answer to *who is master*.
- **Local serving table** — effective assignment, handoff state,
  `rollback_shard`, per-node `shard_has_data`. **Per-node by design.**

§8's "read the committed assignment" applies to the *authority* question only;
handoff machinery keeps reading the local table. I11 hashes the committed
assignment, never the serving table.

---

## 4. Agreement: vote attestation

1. Mix `assignment_digest = sha256(canonical(assignment))` and `rf` into
   `compute_digest` as the last two fields. Pin field order and `rf` width (Go
   and `client/rust` must reproduce it).
2. **Every recipient computes `assignment_digest` itself from the received
   bytes.** No path trusts a shipped hash; a frame whose shipped digest
   disagrees with its payload is rejected.
3. Persist `voted_digest` beside `voted_term`, under the existing
   persist-before-vote discipline.
4. **(E1) On `commit.term == voted_term` with `commit.digest != voted_digest`:
   reject + metric + ERROR. Never fence.** Term numbers are *not* globally
   reserved — every producer derives `max(committed, voted) + 1` from local
   state, so two proposers routinely mint the same term with different content,
   and today the digest cleanly separates them. Rev 3 fenced on that benign
   race: a proposer reaching C and D but not A, then crashing, lets A re-mint T
   with a different assignment and commit on a fresh quorum — bricking C and D
   with no attacker present. Voting at T is evidence of nothing being
   committed.
5. **(P1-8) Also compare against the *committed* digest.** A commit for
   `term <= committed_term` whose digest differs from the one this node
   committed at that term is hard proof of a committed-history fork — stronger
   evidence than any vote mismatch. Today it is discarded before any digest
   comparison. Note `voted_term != committed_term`: a node that caught up by
   commit never advances `voted_term`, so §4.4 alone never fires for it.
6. `TopologyVote` carries the assignment digest; its payload is versioned (§10).
7. **(P1-6) Ordering.** The digest/equivocation checks run **after** the
   membership-safety and quorum-proof gates, so a structurally invalid frame
   can never reach a detector that changes state.
8. The proposer's self-vote is in-memory with no persist before broadcast.
   Harmless, but noted so nobody assumes symmetry.

### 4.9 The non-voter path (Q4)

Quorum is a majority, so in any n ≥ 3 cluster a node that misses one propose
round — a per-peer send failure, a restart, a brief partition — never voted at
T. That is the **normal** catch-up path, not an edge case, and withholding
forever is wrong.

**Adopt durably, serve conditionally**: persist the assignment (so it survives
restart and can anchor later terms), and withhold *authority* only until one
corroboration arrives — an authenticated peer gossiping the same
`(committed_term, assignment_digest)`, which lands within ~1 gossip round in a
healthy cluster. Self-resolving, bounded in practice, fail-closed otherwise.

A sole partition survivor is a **minority** and must not serve regardless; the
peak-derived quorum already enforces that. The answer there is "restore
quorum", not "relax the gate".

---

## 5. The election, conditionally anchored (E3)

```
for each shard s:
    base := prev_committed[s]
    if base ∉ det.candidates(s) or base not live:
        base := det.master(s)

    # (E3) A deviation is kept ONLY while its reason still holds.
    if base != det.master(s):
        keep base only if base still self-reports full
                     AND det.master(s) still self-reports data-less
                     AND that has held for k consecutive terms (hysteresis)
        else: base := det.master(s)

    assignment[s] := base

    if base self-reports data-less and some c ∈ det.candidates(s) self-reports full:
        assignment[s] := c        # c picked by §5.1 tiebreak order
```

Without the conditional clause the anchor has no reversion edge: a named master
receives its migration, therefore holds the data, therefore is never provably
data-less — so one term of influence (hostile, or merely a skewed partial view)
buys **permanent** per-shard mastership, laundered by every honest term
thereafter. The conditional form keeps `det` as the fixed point (§1.2) while
preserving failover.

**Deviation preconditions (restated as MUST, not inherited).** Deviation
requires `all_candidates_reported` and the existing no-data skip. Without them
a partial view "proves" a candidate full merely because the real holder did not
report — and rev 3 would then have anchored that mistake permanently.

**"Proven" is "self-reported".** The signal is `PartitionVersionEntry.
last_applied_seq`, a peer's own report. A malicious peer can report `0` for
shards it holds and `>0` for shards it does not, steering an *honest*
proposer's deviation inside the rule 4/6 bound. The spec says self-reported
throughout; the hysteresis above is the mitigation.

**§5.1 Tiebreak order:** self-reported-holder > `was_committed_master` >
`was_deterministic_master` > lowest NodeId. Lowest-NodeId is the final
tiebreak only. (Rev 2 dropped `was_previous_master` with nothing in its place,
which made lowest-NodeId win every tie: n=3 RF=2 gave 2731 / 1365 / **0**.)

**§5.2 Proposer obligations.**
- Self-validate against **all** of rules 1–9 before proposing, and clamp
  deterministically on failure. Never fall back to plain `det` — that reverts
  every prior promotion.
- **(P1-1) A node holding no committed assignment MUST NOT propose.** A node
  that caught up via the partition map holds none by design (§10) yet is
  eligible to be `members[0]`; its anchor would fall through to `det` for all
  4096 shards, reverting every promotion and firing ~4096 migrations.
- Per-shard fallback when no candidate qualifies: `det.master(s)` +
  `unproven_master`. Never leave an entry unset — a zero-filled u16 section
  decodes as **index 0 = `members[0]`**, handing over the whole keyspace.

---

## 6. Validation

Voters before voting, appliers before installing:

1. exactly `NUM_SHARDS` entries — never pad or truncate
2. `commit.members` **strictly ascending** (sorted + duplicate-free in one
   check). `compute_digest` hashes members **as received** while
   `compute_with_epoch` sorts a local copy, so non-ascending members make two
   conforming implementations derive different assignments from one
   digest-matching commit. **Land independently of this design.** *(The
   rationale is the ordering mismatch — duplicates cannot inflate quorum, since
   the voter proof dedups and requires membership.)*
3. every entry is a committed member of this term
4. `assignment[s] ∈ {det.master} ∪ det.replicas` — the wire-level equivalent of
   `set_master_for_shard`'s refusal; a pure function of digest-bound inputs
5. `commit.members` contains no `NodeId(0)`
6. **(E7) per-node master count → metric + ERROR, not a reject.** `k = 1.5`
   rejects the *honest* assignment on a routine wipe-and-rejoin: n=3 RF=2, node
   `m[2]` restored empty, all 1365 shards where it is det master deviate to
   `m[0]`, which then holds 2731 vs a 2048 cap. Under all-or-nothing that
   wedges permanently, since the deterministic proposer just re-proposes it.
   Exported as `assignment_master_count_ratio`; alert above 1.1×.
7. `master ∉ replicas` (§11)
8. `commit.proposer ∈ commit.members` — a sanity check on a plaintext field,
   **not** authorization (§12)
9. `commit.term <= committed + MAX_TERM_JUMP` (≈16), checked **before** hashing
10. **(M2) `commit.members.len() <= max(committed_peak, alive) +
    MAX_MEMBER_GROWTH`.** Today a commit carrying 1024 *distinct fabricated*
    NodeIds passes the superset check, persists `peak_cluster_size >= 1024`,
    and permanently wedges the cluster at a 513-voter quorum — reboot-surviving,
    one frame. Pre-existing; fix here since §6 rewrites this validator anyway.
11. u16 index `>= members.len()` is malformed → reject.

The move-delta rule is **deleted** (it rejected the v1→v2 upgrade outright and
was bypassable by a one-node membership change). It survives as
`assignment_move_delta_shards`.

### 6.1 Reject is not fence (R7)

- Any rule 1–11 failure, digest mismatch, malformed payload, **and vote-digest
  mismatch (E1)** → reject + metric + ERROR. **No fence.** The node keeps
  serving under its existing committed term.
- **Fencing cases only:** §9's two.
- **(P1-7)** The existing C11 placement-version refusal is **not** one of rules
  1–11 and survives unchanged — it is a live v1/v2 dual-authority guard.

---

## 7. `unproven_master` — provenance-based, raise-only (E4)

```
local_holder_check(s) :=
      ( self ∈ prev_committed_holders(s)          # master ∪ replicas, previous committed
     OR proven completion for s at epoch >= current commit epoch
     OR no previous committed assignment exists )  # genesis only
  AND NOT inbound_atomic.test(s)

local_fence(s) := committed_unproven(s) OR NOT local_holder_check(s)
```

**Provenance, not data.** Any data-derived predicate (`shard_record_count > 0`
and every variant) is false for a legitimately empty shard, so rev 3 would have
fenced most of a fresh cluster's keyspace — and deadlocked: fenced ⇒ no writes
⇒ still empty ⇒ still fenced. Provenance passes empty shards via the first or
third clause.

`inbound_atomic` alone does **not** close the under-fence direction: a node
named master of a shard nobody ever sends it has a clear bit and serves it
empty, because that fence is raised on data *arrival*. The
`prev_committed_holders` clause is what actually closes it.

- **Raise-only.** The committed bit may only raise a fence, never lower it.
- **Clearing edge, always** — a completion handshake, or the node's own
  provenance.
- **(P0-4) Never raise a fence with no concrete source.** The pull source is
  `prev_committed[s]`, always a real node (rule 5 bans `NodeId(0)` from
  members). If no source can be named, **alert instead of fencing** — a
  `NodeId(0)` inbound is filtered out of pull-repair and has no clearing edge,
  which is on record as having blocked repair in the E2E campaign.
- **No blanket fallback** in either operand.

I0-compatible: withholding service does not change who the committed master is
(§14).

---

## 8. Every path that derives a master reads the committed assignment

Unchanged from rev 3 and confirmed accurate against all ten production
`compute_with_epoch` sites (`coordinator.rs` 745, 836, 980, 1107, 1172, 1610,
1631, 3976, 3998, 4240 plus `http.rs:2338`). Row 5 keeps recomputing (§10);
row 9's G8 guard stays valid because §11 preserves the holder set; row 10 is
the exempt bootstrap. `ShardTable::compute` has no production caller — include
it in I14's audit anyway.

**(B2) R5 needs its own predicate.** "The joining node withholds authority" is
not implied by anything today: the only gate is `table.version <
committed_term`, and the partition-map path installs a locally recomputed table
stamped with the peer's *higher* version, so the gate passes. Add: **no
committed assignment matching my committed term ⇒ `NodeId(0)` sentinel for
every shard.**

The tripwire (assert the committed assignment satisfies rule 4 against a local
recompute) catches **malformed assignments only, never a split** — a split is
by definition two assignments that are each valid. It is largely redundant with
rule 4. Prevention is §4; detection is §9.

---

## 9. Fencing and divergence detection (E2)

Two arms, both self-clearing:

1. **(E2) Persistently refusing a quorum-advertised higher term.** This is the
   condition that is actually diagnostic, and it is the one rev 3 missed: the
   attestation split lands on *different* terms — voters reject and stay at
   T-1, non-voters accept T — so an equal-term detector never fires. Generalise
   C11's existing arming condition beyond "unsupported placement", with
   hysteresis and the `MAX_TERM_JUMP` bound. Self-clears when `committed_term`
   catches up.
2. **Same-term, different digest via gossip** — covers the case where both
   groups did apply T.

**(P1-4) Arm 2 requires corroboration; never fence on a single gossip frame.**
SWIM is UDP and verifies only when `cluster_secret` is set, so an unauthenticated
source could otherwise fence the whole cluster with one datagram — and the node
that fences is the node that *listens*, not the one that lies. Requirements:
sender is a committed member of that term; ≥2 independent observations or a
persistence window; confirm over the HMAC'd TCP path before arming; **§12's
secret gate covers this arm**; explicit format-byte + length-prefixed trailer
parsed at a known offset (today's committed-term trailer is positional and a
truncated datagram would become a fence trigger); and a dedicated latch —
C11's `unapplicable > committed` cannot express "same term" (arming at T is a
no-op, at T+1 it auto-clears on the next commit).

**Default posture is alert-and-hold**, with the global fence behind an operator
policy switch. This repo already made that call once, rejecting auto-escalation
for the same reason.

**(P1-5) Publication ordering.** The committed assignment is stored **before**
`committed_term`, and the gossiped digest is read from that record — never from
the serving table. Otherwise every node briefly advertises `(T, digest(T-1))`
and they mutually fence.

---

## 10. Durability, recovery, wire format

**(E5) Persist the winning `TopologyCommit` verbatim and replay those exact
bytes on `OP_GET_COMMITTED_TOPOLOGY`.** Today that path *fabricates* the commit
(proposer = `members[0]`, voters defaulted, digest recomputed), so it is
self-consistent by construction and rule 8, the digest check and the quorum
proof are all vacuous there. Replaying real bytes makes digest equality
meaningful, gives §4.9 an object to corroborate, and — since `rf` is now
digest-bound — prevents a catch-up commit recomputed from the *serving* node's
local `rf` from reading as equivocation.

**Assignment travels on propose, commit, and `OP_GET_COMMITTED_TOPOLOGY` —
nowhere else.** `OP_GET_PARTITION_MAP` keeps recomputing;
`routing.shard_assignments` is `preferred_master_for_shard`, the *effective*
master during handoff keyed on the serving node's own liveness view, carrying
no quorum proof.

**Persistence.** `PersistedTopologyState` gains the committed assignment and
`voted_digest`, riding the single fsync in `handle_commit_durable`. It needs a
length-prefixed, CRC-covered record with `MAX_TOPOLOGY_MEMBERS` bounds on all
three counts — today `count` is an unbounded `u32` feeding
`Vec::with_capacity`, and partial member reads are silently accepted, yielding
a **shorter `committed_members` with an unchanged `committed_term`** (worse
than a zero-filled assignment: it feeds quorum and `committed_peak`). Persist
`assignment_digest` alongside and re-verify at load. **"No assignment" must be
structurally distinguishable from "zero-filled assignment"** — an explicit
presence flag — or a torn write reads as a legal anchor giving `members[0]`
everything (Q1).

**Wire format.** Assignment = fixed-length `NUM_SHARDS` × `u16` index into
`commit.members` as received (== sorted, by rule 2). 8 KiB, never
length-prefixed. `unproven_master` = fixed 512-byte bitmap.

**Versioning.** `TopologyTerm::deserialize` is the one **exact-length**-gated
parser, and appending to it silently decodes `placement_version = 1` and
`committed_peak = members.len()`, dropping the G8 split-brain floor.
`TopologyCommit::deserialize`, `PersistedTopologyState::deserialize`,
`RoutingInfo::decode` and `TopologyVote::deserialize` use permissive prefix
parsing — their defect is **silent defaulting**, not exact-length. Same remedy
(explicit format byte + section lengths), different diagnosis.

---

## 11. Replica derivation

```
replicas := det.replicas(s)
if assignment[s] != det.master(s):
    i := index of assignment[s] in replicas
    replicas[i] := det.master(s)        # swap: promoted out, old master in
```

Preserves `{master} ∪ replicas == det`'s holder set — the invariant §8 row 9's
G8 guard depends on, and confirmed to match `set_master_for_shard` exactly.
Rule 7 enforces `master ∉ replicas`. Include replica **order** in I9.

---

## 12. Trust posture (E6)

There is **no proposer authorization**: `handle_propose` never checks the
sender, `commit_passes_gates` never reads `commit.proposer`, and the
`members[0]` restriction is sender-side only. Rule 8 is a sanity check.

**Containment is rules 4 + 6.** With rule 6 as a metric (E7) the ceiling is
rule 4's alone: a member is a legal master for `s` iff it is one of the RF
candidates, so it can seize up to **`RF/n`** of the keyspace — 66.7% at n=3
RF=2, **100% at n=3 RF=3**. *(Rev 3 printed this table while claiming
`min(RF,k)/n`; with rule 6 demoted the honest number is the larger one. Say
the larger number.)*

**(E3/H2) The seizure is sticky unless §5's conditional clause holds.** That
clause is the only thing that de-anchors a bad assignment. Add an
operator-triggered, quorum-committed **reset-to-`det`** term so a suspicious
assignment can be cleared without evicting a node.

**(E6) `cluster_secret` gate — and it covers §9's gossip fence.** Committed
master election refuses to arm without a secret; the node keeps today's
deterministic derivation.

**(H4) The gate must not disable the feature in CI.** Every scenario config
omits `cluster_secret` by design, so as stated the feature would never arm in
scenarios 01–17 — the consensus code justified by scenarios 05–09 would run
only the legacy path. Two required changes: give the harness a `cluster_secret`
and sign the test client's frames (`round_trip_signed` exists; the non-scenario
configs already do this); and make the armed state a **digest-bound,
quorum-committed, unanimity-gated** property, exactly as `placement_version`
already solves this — proposer-side unanimity, voter-side refusal, applier-side
refusal. Otherwise mixed mode is reachable and undetectable (both tables are
internally consistent, so §8's tripwire cannot see it), and an armed node
silently downgrades to `det` if the secret is absent on restart.

**Per-node signing keys are the real fix for `n <= RF`** (Q3). Each vote signs
`(term, assignment_digest)`; the commit carries the signed votes. That converts
`voters` from a forgeable plaintext field into a real quorum proof, makes rule
8 and `has_quorum_voter_proof` meaningful, makes equivocation *externally
provable* rather than only self-detectable, and gives §4.9 a verifiable object.
Cost: one signature per vote per term, over an existing TCP round-trip. If out
of scope, `DEPLOYMENT_ASSUMPTIONS.md` must say that for `n <= RF` a single
compromised member has total authority — and §5's conditional anchor must land
first, because permanence is what makes that residual unacceptable.

`docs/DEPLOYMENT_ASSUMPTIONS.md:80-96` must state: no proposer authorization;
the `RF/n` bound with the table; that `members` is byte-identical across a
hostile assignment change so every member-set-keyed defence is blind; that
detection is alerting-only; the secret requirement; and a recommended
production minimum of `n >= 4`, `strict_auth = true`, alerting above 1.1× fair
share.

---

## 13. Invariants

I1 byte-identical **committed assignments** · I2 non-viewer matches proposer ·
I3 failover promotes a data-holding replica (re-verify against §5's conditional
anchor **and** §7's provenance check, which together cover the replica-moved
case) · I5 rejected/superseded proposals never install · I6 no bundling ·
I8 no committed assignment ⇒ authority withheld (absent **or corrupt**;
**bounded** — a deadline after which the node exits rather than sitting in
permanent `Transitioning`, with its own metric and documented recovery) ·
I9 durability round-trip incl. **replica order**, after restart and
`OP_GET_COMMITTED_TOPOLOGY`; after `OP_GET_PARTITION_MAP`, no assignment and
authority withheld · I10 flipped entry rejected by digest · I11 cross-node
`sha256(committed_assignment)` equality · I12 never serve without data and
without proven completion · I13 rule failure ⇒ reject with a distinct variant,
never a silent skip, **never a fence** · I14 one caller each for
`set_master_for_shard` / `apply_master_election`; no path builds an assignment
by recomputation · I15 a committed assignment never fences a shard with no
clearing edge · **I16 (E7) no honest election is *rejected* for master-count
skew** — the wipe-and-rejoin case legitimately reaches 2× fair share ·
I17 **(E1) a vote-digest mismatch rejects and does NOT fence** · I18 gossip
divergence fences only with corroboration · I19 non-equivocation failures do
not fence · **I20 (E2) a node persistently refusing a quorum-advertised higher
term fences, and unfences when it adopts** · **I21 (P1-1) a node with no
committed assignment does not propose**.

Not an invariant: "convergence in one round" — it cannot hold when the
committed master must receive data. Target: ≤ N rounds / ≤ T seconds.

---

## 14. Sequencing with #99

Land this first; #99 (regime-fenced failover) after — this answers *who is
master*, #99 answers *how a transfer is made safe*, and a regime fence is
meaningless while nodes disagree about who the master is. Carried over now:
**I0** (no node-local commit-apply gates) and **I6** (no bundling). **Carve-out:**
§7's raise-only serving fence is not an I0 violation — withholding service does
not change who the committed master is.

---

## 15. Expected effect on the E2E suite

Scenarios 05–09 are non-deterministic because convergence needs several
reactivation rounds near their 60–120s budgets; two CI runs of byte-identical
code returned 6/14 and 8/14. Agreement plus §5's anchor should cut the round
count. **Prediction, not a claim**: the harness has a known independent
flakiness cause on record (`migration_pool_size 128 > max_connections_per_ip
64`). Falsify by running twice at a fixed SHA.

---

## 16. Implementation order

1. **Rule 2 (strictly-ascending `members`)** — both reviewers say land it
   independently; it is a latent same-term split vector in today's code.
2. Rule 10 (member-growth bound) — closes a pre-existing one-frame permanent
   wedge.
3. §10 persistence: CRC + bounds + presence flag; persist the winning commit.
4. §4 attestation (digest, `voted_digest`, recompute-from-bytes).
5. §5 conditional anchor + §11 replica swap.
6. §6/§6.1 validation and reject-not-fence.
7. §7 provenance fence.
8. §8 the ten sites + R5's authority predicate.
9. §12 harness secret + armed-state unanimity gate.
10. §9 detection — **alert-only first**; the fence arms behind the operator
    switch once 1–9 are green.
