# Committed Master Election — design (rev 3)

**Status:** draft, revised after two review rounds (bitcoin-expert +
security-auditor on rev 1 and rev 2). Both rev-2 verdicts were "do not
implement". Consensus-critical: needs a third review round before code.

**Context:** TeraSlab is not in production. On-disk formats and the wire
protocol may change freely; no migration path is required.

## 0. What changed from rev 2, and why

Rev 2's analysis was right and its mechanisms were wrong. Five blockers:

| # | rev 2 defect | rev 3 |
|---|---|---|
| B1 | `assignment_digest` in the digest does **not** close equivocation — voters store `voted_term` and **no `voted_digest`**, so the commit-side check is a self-consistency checksum, not an attestation | §4 persisted vote attestation |
| B2 | installing `routing.shard_assignments` makes a peer's local handoff + liveness state durable authority, with no quorum proof | §6 assignment travels **only** on the committed-topology path |
| B3 | `unproven_master` had **no clearing edge** — a flagged shard with no migration in flight fences forever, and both fallbacks flagged *every* shard | §7 advisory raise-only, locally clearable |
| B4 | dropping `was_previous_master` made lowest-NodeId win every tie (n=3 RF=2: 2731 / 1365 / **0** shards) and left the election with no anchor, so a stale view silently reverted every prior promotion | §5 anchored on the previous **committed** assignment |
| B5 | no in-band split detector — I11 was an E2E assertion only | §9 SWIM-gossiped digest + self-fence |

Rev 2 also had to drop rule 7 (move-delta), which rejected the v1→v2 placement
upgrade outright and was a wedge primitive; and its global self-fence on any
validation failure was a one-packet cluster kill. Both corrected below.

---

## 1. The defect

`apply_master_election` runs *after* commit, per node, from two **per-node**
inputs: `partition_view` (partial and divergent) and `prev_table` (differs by
history). Two nodes can elect different masters for the same shard while each
table stays internally self-consistent, so `mismatched == 0` everywhere and the
divergence is invisible locally.

### 1.1 Why the obvious fixes fail (both measured on CI — do not repeat)

- **Run convergence faster** (15s/30s → 5s/10s): *worse*. Scenario 09 hit
  `masters=5461/4096` at `ver=8`. Every activation is a fresh chance to
  diverge. Reverted in `18c7c07`.
- **Remove the refinement**: breaks failover —
  `segment_cluster_master_failover_preserves_replicated_record` fails. The
  refinement is what promotes the replica that *holds the data*.

Election is load-bearing for failover. It cannot be removed or accelerated. It
must gain cluster-wide agreement.

### 1.2 The property rev 1 destroyed, and rev 3 restores

Today's round-robin table is a **fixed point**: any confused node converges
back to it. That is why current divergence is transient and self-healing. Any
design that replaces it must supply a new fixed point, or transient divergence
becomes permanent. **§5's anchoring is that fixed point** — an empty or stale
view now *preserves* the committed assignment instead of reverting to `det`.

---

## 2. Requirements

- **R1 Agreement.** For a committed term, every node installs a byte-identical
  committed assignment.
- **R2 Failover locality.** The assignment may prefer a master that holds data.
- **R3 Bounded authority.** No participant may assign mastership outside a
  bound every node verifies independently.
- **R4 Durability.** The assignment survives restart and catch-up.
- **R5 Fail-closed.** A node that cannot obtain or validate the committed
  assignment withholds authority; it never falls back to a local derivation.
- **R6 In-band detection.** Divergence is detected in production, within one
  gossip round — not only by an E2E assertion.
- **R7 Availability.** No single frame, and no proposer bug, may cause a
  cluster-wide or unclearable outage.

---

## 3. Two distinct objects (this was implicit in rev 2 and caused B3/I11 confusion)

- **Committed assignment** — term-scoped, immutable once committed, digest-bound,
  persisted, gossiped. `NUM_SHARDS` entries. The answer to *"who is master"*.
- **Local serving table** — the effective assignment, including handoff state
  (`Copying`/`ServingNew`), `rollback_shard` after a failed migration, and the
  per-node `shard_has_data` predicate. **Per-node by design**, and must stay so:
  `rollback_shard` exists to prevent an unreachable shard.

§8's "must read the committed assignment" applies to the **authority** question
only. The handoff machinery keeps reading the local table. I11 hashes the
committed assignment, never the serving table — rev 2's I11 was unsatisfiable
because it conflated them, and would have driven an implementer to delete
`rollback_shard`.

---

## 4. Agreement: vote attestation (B1)

`compute_digest` covers `(term, cluster_id, members, placement_version,
committed_peak)` — **verified**. Rev 2 mixed `assignment_digest` in. Necessary,
but **not sufficient**: `PersistedTopologyState` stores `voted_term` and no
`voted_digest`, so `commit_passes_gates` recomputes the expected digest *from
the commit's own fields*. That is a checksum, not an attestation — a proposer
can still collect votes for term T and commit a different assignment.

Rev 3:

1. **Extend the digest**: mix `assignment_digest = sha256(canonical(assignment))`
   and `rf` into `compute_digest`, as the last two fields. Pin field order and
   `rf` width — the Go client and `client/rust` must reproduce it.
2. **Every recipient computes `assignment_digest` itself from the received
   assignment bytes.** No code path may trust a shipped hash. A frame carrying
   a precomputed `assignment_digest` that disagrees with the payload is
   rejected. *(Without this the binding is vacuous one indirection deeper: ship
   `(A, H(B))` and `(A', H(B))` and both match.)*
3. **Persist `voted_digest` alongside `voted_term`**, written under the same
   persist-before-vote discipline.
4. At commit: if `commit.term == voted_term`, require
   `commit.digest == voted_digest`. **Mismatch is equivocation — a live attack,
   not a stale frame — so it rejects *and* arms the C11 self-fence.**
5. A node that never voted at T cannot verify. It **must not silently accept**:
   it withholds authority (R5) until it obtains the term from a voter, and
   cross-checks via §9's gossip digest.
6. `TopologyVote` must carry the assignment digest so `handle_vote`'s match is
   over the full binding. Its payload is versioned too (§10).

---

## 5. The election, anchored (B4)

Rev 2 dropped `was_previous_master` to remove proposer-history dependence. That
removed the wrong term. Consequences measured by review: with it gone the
ranking reduces to `(score, Reverse(node_id))`, and since replication ships
every mutation to master *and* replicas, every candidate ties on score and
**lowest NodeId always wins** — n=3 RF=2 v1 gives `m[0]` 2731 shards, `m[1]`
1365, `m[2]` **zero**, with ~2000 migrations per term.

And the real churn source was never `was_previous_master` — it was the **view**,
which is per-proposer, unshipped, and intermittently available. A term proposed
on a stale or empty view proposed plain `det`, silently reverting every prior
failover promotion.

**Rev 3 election, evaluated by the proposer:**

```
for each shard s:
    base := prev_committed[s]                 # the anchor
    if base ∉ det.candidates(s) or base not live:
        base := det.master(s)
    assignment[s] := base
    # deviate only on a PROVEN reason:
    if base is proven data-less and some c ∈ det.candidates(s) is proven full:
        assignment[s] := c
```

Properties this buys:

- **An empty or stale view is a true no-op**: it preserves the committed
  assignment. That is the fixed point §1.2 requires.
- Deltas per term are small by construction, so a per-node master-count bound
  becomes meaningful (§6 rule 6) and the migration storm disappears without a
  move-delta gate.
- Stability no longer depends on the proposer's local `prev_table` — the anchor
  is the agreed, persisted, shipped previous committed assignment.
- Tiebreak order: proven-holder > `was_committed_master` > `was_deterministic_
  master` > lowest NodeId. Lowest-NodeId survives only as the final tiebreak.

**Proposer pre-intersection (required).** Before proposing, the proposer
intersects its result with the *current* `det` candidate set and replaces any
entry that falls outside with `det.master(s)` + `unproven_master`. Otherwise a
stale view names a node that rule 3 rejects, and — under §6's all-or-nothing —
the proposer wedges the cluster on its own proposal.

**Per-shard fallback.** When `elect_master` would return `None` (every
candidate evicted), use `det.master(s)` and set `unproven_master`. Never leave
an entry unset: under §10's u16 encoding a zero-filled section decodes as
**index 0 = `members[0]`**, handing the whole keyspace to the lowest member.

---

## 6. Validation

Applied by voters before voting and by appliers before installing:

1. exactly `NUM_SHARDS` entries — never pad, never truncate
2. `commit.members` is **strictly ascending** (enforces sorted *and*
   duplicate-free in one check). Rev 2 missed this: `compute_digest` hashes
   `members` **as received** while `compute_with_epoch` sorts a local copy, so
   a non-ascending `members` makes two conforming implementations derive
   different assignments from one digest-matching commit. Duplicates also
   inflate `members.len()`, which feeds quorum and `committed_peak`.
   **Worth landing independently of this design.**
3. every entry is a committed member of this term
4. **`assignment[s] ∈ {det.master} ∪ det.replicas`** where
   `det = compute_with_epoch(members, rf, 0, placement_version)` — the
   wire-level equivalent of `set_master_for_shard`'s refusal, and a pure
   function of digest-bound inputs so every voter agrees
5. `commit.members` contains no `NodeId(0)` *(stated against the member list,
   not the assignment: under u16 indices `NodeId(0)` is inexpressible unless it
   is a member)*
6. per-node master count ≤ `k ×` fair share. **Load-bearing, not defence in
   depth** — see §12. `k = 1.5` is meaningful only because §5 anchoring keeps
   honest assignments near 1×; under rev 2's election any `k` admitting the
   honest assignment admitted the maximal hostile one. Exempt `n == 1`.
7. `master ∉ replicas` (§11)
8. `commit.proposer ∈ commit.members` — a sanity check on a self-declared
   plaintext field, **not** authorization. See §12.
9. `commit.term ≤ committed + MAX_TERM_JUMP`, checked **before** hashing.

**Rev 2's move-delta rule is deleted.** It rejected the v1→v2 placement upgrade
outright (`upgrade_proposal` reshuffles ≈(1−1/n)·4096 shards with `members`
unchanged), it blocked the very repair this design performs (scenario 09's
correction moves ~33%), a hostile max-displacement commit made every later
honest correction exceed the bound, and it was bypassed by changing membership
by one node. It becomes a **metric + ERROR** (`assignment_move_delta_shards`),
never a reject. Migration storms are rate-limited where rate limits belong — in
the migration scheduler.

### 6.1 Failure handling — reject is not fence (R7)

Rev 2 routed *every* validation failure into C11, whose own doc says recovery
is "a BINARY UPGRADE + REBOOT", armed behind a structural check on a plaintext
`voters` field. One malformed frame would have fenced every node, and rev 2
added 4096 new triggers — so a proposer *bug* bricks the cluster.

- **Digest mismatch, malformed payload, any rule 1–9 failure** → reject the
  commit, increment a metric, log ERROR. **No fence.** The node keeps serving
  under its existing committed term.
- **Equivocation only** (§4.4: `commit.term == voted_term` but
  `commit.digest != voted_digest`) → reject **and** arm C11. This is the one
  case that proves an active attacker.
- **Same-term digest divergence observed via gossip** (§9) → arm C11.

C11 arming is additionally bounded by `MAX_TERM_JUMP`, so a forged
`term = u64::MAX` cannot arm a permanent fence.

---

## 7. `unproven_master` — advisory, raise-only, clearable (B3)

Rev 2 shipped a bare bit and fenced on it. The serving fence (`inbound_atomic`)
is cleared **only** by a migration completion handshake, so a flagged shard
with no migration in flight fences **forever** — and AUDIT M1.5 already warns
that fencing shards whose migration can never run "would brick the shard
forever". Rev 2 then scaled it: both fallbacks flagged *every* shard, on the
failure mode that is normal under load.

Rev 3:

```
local_fence(s) := committed_unproven(s) OR NOT local_holder_check(s)
```

- **Raise-only.** The committed bit may only *raise* a node's fence, never
  lower it. A node named master with no local evidence of holding the shard
  fences regardless of the bit. This closes the under-fence direction, where a
  hostile or merely stale proposer leaves the bit clear and the named master
  serves an **empty** shard.
- **Clearing edge, always.** Cleared by a completion handshake **or** the
  node's own proof that it holds the shard. Never a bit with no clearing edge.
- **A committed-unproven fence names its pull source** (the previous committed
  master). A fence raised with no concrete source registers the `NodeId(0)`
  sentinel, which the pull-repair loop filters out — the code's own comment
  says such an inbound "can never be re-requested … stays fenced".
- **No blanket fallback.** §5's fallbacks flag *individual* shards they could
  not prove. Nothing in this design may flag all 4096.

This is I0-compatible: I0 forbids a node changing *who is master*. Withholding
service is not changing the assignment. §14 states that carve-out explicitly.

---

## 8. Every path that derives a master must read the committed assignment

The **repair action** is what reverts an election, not the detectors:
reactivation calls `activate_topology_with_view`, which recomputes and installs
— and `apply_master_election` with an empty view is a deliberate no-op
*precisely so reactivation installs round-robin*. `activate_topology_with_view`
must take the committed assignment as an **input** and stop computing masters.

| # | site | change |
|---|---|---|
| 1 | `committed_topology_reactivation_metrics:745,754` | compare vs committed |
| 2 | `phantom_master_shard_count:836` | compare vs committed |
| 3 | `missing_master_shard_count:980` | compare vs committed |
| 4 | `failed_handoff_disposition:1107` | read committed — **drops data** if wrong |
| 5 | `install_active_routing_snapshot:1172` | **keep recomputing** (§6 of rev 2 was wrong — see B2) |
| 6 | `restored_committed_shard_table:1610` | load persisted assignment |
| 7 | `activate_topology_with_view:3976,3998` | take assignment as input |
| 8 | `phantom_planned:4240` | diagnostic; update or drop |
| 9 | `http.rs:2338` shrink guard | **unchanged** — see below |
| 10 | `coordinator.rs:1631` bootstrap | exempt (single-member, stale-table gate covers it) — listed so it is not mistaken for an omission |

`ShardTable::compute` is a `pub` legacy wrapper; include it in I14's audit.

**Row 9 correction.** `shrink_drops_a_shard_holder` computes a *prospective*
table for membership that is not yet committed, so it has no committed
assignment to inherit. What keeps the G8 shrink guards valid is that the
election **preserves the holder set** — the `set_master_for_shard` swap plus
rule 7 mean `{master} ∪ replicas` is identical to `det`'s. State that as a
load-bearing invariant of §11 with the G8 guard named as its dependent.

**Keep one independent recompute as a tripwire**, but state honestly what it
does: it asserts the committed assignment satisfies rule 4 against a local
`compute_with_epoch`. **It catches malformed assignments only, never a split** —
a split is by definition two assignments that are each valid, so both pass. It
is largely redundant with rule 4. The only split detectors are §4's attestation
(prevention) and §9's gossip (detection).

The stale rationale at `coordinator.rs:787-819` and the comment at `:2448-2455`
(asserting the prompt path re-runs the exchange "so the resulting master
assignments match byte-for-byte" — the property §1 disproves) must be rewritten.

---

## 9. In-band divergence detection (B5)

Piggyback `(committed_term, assignment_digest)` on the SWIM heartbeat, which
already gossips every ~1 s. A node observing a peer advertising the **same
term** with a **different** assignment digest arms the C11 self-fence
immediately.

That converts a silent, permanent dual authority into a loud fail-closed within
one gossip round, in production, in-band. It is what R6 actually asks for; I11
on `/metrics` remains as the E2E assertion but is not the mechanism.

---

## 10. Durability, recovery, wire format

**Durability.** `PersistedTopologyState` gains the committed assignment and
`voted_digest`. It must ride `persisted_state_for_commit` → the single fsync in
`handle_commit_durable`, not a second file. The current format has **no
integrity check** and is maximally lenient (`unwrap_or` defaults, partial member
reads accepted), so a torn write would decode to a zero-filled assignment —
i.e. `members[0]` masters everything. **Add a length-prefixed, CRC-covered
record and persist `assignment_digest` alongside, re-verified at load**;
otherwise I8's "corrupt" arm is untestable.

**Recovery (B2).** The assignment travels on propose, on commit, and on
`OP_GET_COMMITTED_TOPOLOGY` — and **nowhere else**. §3.3 of rev 2 said "carry
the hash, not the payload" while §3.5 said the opposite; the payload travels,
only the digest is folded into `compute_digest`.

`OP_GET_PARTITION_MAP` catch-up **keeps recomputing** and installs routing only.
`routing.shard_assignments` is `preferred_master_for_shard` — the *effective*
master during handoff, with a fallback keyed on the serving node's *local* SWIM
liveness — and the code says so: *"only a routing snapshot. Committed-term
catch-up must use encode_committed_topology()."* The path already logs "lacks
topology quorum proof" and installs anyway; making that durable would let one
peer, with no quorum, dictate mastership on a joining node. Under R5 the
joining node holds no assignment and withholds authority until
`OP_GET_COMMITTED_TOPOLOGY` supplies one with its proof.

Note `encode_committed_topology` **fabricates** a commit locally (proposer =
`members[0]`, voters defaulted, digest recomputed), so it is self-consistent by
construction and rules 8 and the digest check provide no assurance on that path
— another reason §9's gossip cross-check is required.

The `topology_commit_tx` channel is typed `(Vec<NodeId>, u64)` and must carry
the assignment.

**Wire format.** Assignment = fixed-length `NUM_SHARDS` entries of `u16`
**index into `commit.members` as received** (provably == sorted, by rule 2).
8 KiB. Fixed-length, never length-prefixed — that removes the count-driven
allocation class F-G5-002 had to fix. `unproven_master` = fixed 512-byte bitmap.

**Four parsers share the exact-length-trailer defect**, not one:
`TopologyTerm::deserialize`, `TopologyCommit::deserialize`,
`PersistedTopologyState::deserialize`, `RoutingInfo::decode` — plus
`TopologyVote::deserialize`, whose payload changes per §4.6. All get explicit
format-byte versioning with explicit section lengths, in the same change.
Appending to the current scheme silently decodes `placement_version` as 1 and
`committed_peak` as `members.len()`, **which drops the G8 split-brain floor**.

---

## 11. Replica derivation (algorithm, not prose)

Replica *order* is consensus-relevant (`replicas.first()` is used as a
source/heal pick), so prose is insufficient:

```
replicas := det.replicas(s)
if assignment[s] != det.master(s):
    i := index of assignment[s] in replicas
    replicas[i] := det.master(s)          # swap: promoted out, old master in
```

This preserves the holder set `{master} ∪ replicas == det`'s — the invariant
§8 row 9's G8 guard depends on. Deriving replicas from `compute_with_epoch` and
overriding only the master would give `master=B, replicas=[B]` for a deviated
shard: RF effectively 1, and the actual data holder in no role, hence eligible
for orphan cleanup. Rule 7 (`master ∉ replicas`) enforces it. Include the
derivation in I9's round-trip test.

---

## 12. Trust posture (state it, do not imply it)

There is **no proposer authorization**. `handle_propose` never checks the
sender; `commit_passes_gates` never reads `commit.proposer`; the `members[0]`
restriction is sender-side only. Rule 8 is a sanity check on a plaintext field.

**Containment is rules 4 + 6 alone**, and rule 4's bound is weak in small
clusters. A member is a legal master for `s` iff it is one of the RF
candidates, so it can seize up to `min(RF, k)/n` of the keyspace:

| n | RF=2 | RF=3 |
|---|---|---|
| 2 | 100% | 100% |
| 3 | 66.7% | **100%** |
| 4 | 50% | 75% |
| 8 | 25% | 37.5% |

**Whenever `n ≤ RF`, rule 4 provides zero containment** — and 3-node RF=3 is an
ordinary deployment. Being sole committed master of a shard means being the
authority for those UTXOs, so a compromised member can approve a double-spend
within its fraction. **Composition attack:** shrink the cluster first, then
assign against the smaller `n`. I6's no-bundling forces two terms, not one.

**Gate the feature on authentication.** Committed master election refuses to
arm when `cluster_secret` is unset: the node keeps today's deterministic
derivation instead of accepting shipped assignments. The E2E harness runs
trusted-overlay by design, so CI would otherwise exercise the unauthenticated
path as the normal path — where an unauthenticated TCP peer gains surgical
per-shard mastership assignment with `members` byte-identical (invisible to
every member-set-keyed defence), plus the C11 and fence primitives. Even
authenticated, the `cluster_secret` is symmetric: it proves cluster membership,
not correctness, and authorizes every holder equally.

`docs/DEPLOYMENT_ASSUMPTIONS.md:80-96` must say all of the above in those terms.

---

## 13. Invariants

- **I1** Two nodes reaching the same term from different histories and views
  install byte-identical **committed assignments** (not serving tables — §3).
- **I2** A node that never received a partition view installs the same
  committed assignment as the proposer.
- **I3** Failover promotes a data-holding replica —
  `segment_cluster_master_failover_preserves_replicated_record` passes.
  Re-verify against §5's anchor + §7's local holder check, which together cover
  the case where the *replica* moved between the view and the failure (rev 2
  failed this: the stale view "proved" a node that no longer held the shard).
- **I5** A rejected or superseded proposal never installs its assignment.
- **I6** No term carrying a fresh assignment also changes membership/peak.
- **I8** A node holding a committed term with **no** committed assignment
  withholds authority; never falls back to a recomputed table. Test: boot with
  the assignment absent **or corrupt** → `is_master` non-`Yes` for all keys,
  metric + ERROR.
- **I9** Durability round-trip: committed assignment **and replica order**
  byte-identical after restart and after `OP_GET_COMMITTED_TOPOLOGY` catch-up.
  After `OP_GET_PARTITION_MAP` catch-up: no assignment installed, authority
  withheld.
- **I10** Flipping one entry is rejected by the digest check.
- **I11** Every node exports `(committed_term, sha256(committed_assignment))`;
  E2E asserts hash equality across nodes. Hashes the **committed assignment**,
  never the serving table.
- **I12** No node serves a shard for which it has no data and no proven
  completion, even when the committed assignment names it master.
- **I13** An assignment failing any rule 1–9 causes the **commit** to be
  rejected with a distinct error variant — never a silent per-entry skip, and
  (except equivocation) **never a fence**.
- **I14** Architectural: `set_master_for_shard` and `apply_master_election`
  have exactly one caller each, on the install path; no production path
  constructs a master assignment by recomputation (§8's table is the audit
  list, including `ShardTable::compute`).
- **I15** A committed assignment never fences a shard that has no clearing edge.
- **I16** For any honest election, no node's master count exceeds `1 + ε ×`
  fair share. *(Fails under rev 2's election; §5 is what makes it hold.)*
- **I17** A commit whose digest differs from this node's persisted
  `voted_digest` for the same term is rejected **and** self-fences.
- **I18** A peer advertising the same `committed_term` with a different
  `assignment_digest` causes a self-fence within one gossip round.
- **I19** A validation failure other than equivocation does **not** fence: the
  node keeps serving under its existing committed term.

**Not an invariant:** "convergence in one reactivation round". It cannot hold
when the committed master must receive data (Copying → CommitReady →
ServingNew). Track as a target: ≤ N rounds / ≤ T seconds for settled membership.

---

## 14. Sequencing with #99

**Land this first; the regime-fenced failover spec (#99) after.** This answers
*who is master*; #99 answers *how a transfer is made safe*. A regime fence is
meaningless while two nodes disagree about who the master is.

Carried over now: **I0 — no node-local commit-apply gates**, and **I6 — no
bundling**. I0 forbids a node keeping its own master when the shipped one looks
wrong; it must be accept-or-reject, cluster-visibly. **Carve-out:** §7's
raise-only serving fence is *not* an I0 violation — withholding service does not
change who the committed master is.

---

## 15. Expected effect on the E2E suite

Scenarios 05–09 are non-deterministic because convergence needs several
reactivation rounds and lands near their 60–120s budgets; two CI runs of
byte-identical code returned 6/14 and 8/14. Agreement plus §5's anchor should
cut the round count.

**Prediction, not a claim**, and weaker than rev 1 implied: the harness has a
known independent flakiness cause on record (`migration_pool_size 128 >
max_connections_per_ip 64` starving shard migration). Attributing part of the
15/17 residue here is speculation. Falsify by running twice at a fixed SHA.

---

## 16. Open for the third review round

1. §5's anchor makes `prev_committed` an input to the election. On the **first**
   term after this ships there is none. Is bootstrapping from `det` safe, or
   does it need its own term?
2. §7's raise-only fence depends on `local_holder_check`. What exactly proves
   "holds the shard" — `shard_record_count > 0` is wrong for a legitimately
   empty shard.
3. §12 concludes containment is weak for `n ≤ RF` and proposes gating on
   authentication. Is that sufficient for a 3-node RF=3 deployment, or does
   small-cluster mastership need a different bound entirely?
4. §4.5: what should a node that never voted at T do in a cluster where it is
   the only survivor of a partition — withhold authority forever?
