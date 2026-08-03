# Committed Master Election — design (rev 2)

**Status:** draft, revised after bitcoin-expert + security-auditor review of
rev 1. Consensus-critical: needs a second review round before implementation.

**Context:** TeraSlab is not in production use. On-disk formats and the wire
protocol may change freely; no migration path is required.

**Rev 1 verdict was "do not implement".** Both reviewers independently found
that rev 1 would have made things *worse*: it moved where election runs without
specifying where the assignment is stored, recovered, validated, or re-derived.
Today the round-robin table is a **fixed point** — any confused node converges
back to it, which is what makes the current divergence transient and
self-healing. Rev 1 deleted that fixed point and put nothing durable in its
place, turning transient divergence into **permanent, silent, per-shard dual
authority**: a double-spend surface. This revision exists to close that.

---

## 1. The defect

`apply_master_election` (`cluster/coordinator.rs`) runs *after* a topology is
committed, on each node independently, refining the deterministic
`compute_with_epoch` table using two **per-node** inputs: `partition_view` (the
exchange result, partial and divergent across nodes) and `prev_table` (differs
by history). Two nodes can therefore elect different masters for the same
shard. Each table stays internally self-consistent, so the steady-state
mismatch metric reads `mismatched == 0` everywhere and the divergence is
invisible locally.

### 1.1 Why the two obvious fixes fail

Both were implemented and measured on CI. Do not repeat them.

- **Run convergence faster** (cooldowns 15s/30s → 5s/10s): *worse*. Scenario 09
  reached `masters=5461/4096` at `ver=8` — 1365 excess masters, about one
  node's entire share. Every activation is a fresh chance to diverge, so a
  higher rate multiplies divergence. Reverted in `18c7c07`.
- **Remove the refinement** (empty partition view, table a pure function of
  `(members, rf, placement_version)`): gives identical tables but **breaks
  failover** — `segment_cluster_master_failover_preserves_replicated_record`
  fails. The refinement is what promotes the replica that actually *holds the
  data* when a master dies.

Election is load-bearing for failover, not a placement optimization. It cannot
be removed and cannot be run faster. It must gain cluster-wide agreement.

### 1.2 The churn it also causes

`phantom_master_shard_count` exists solely to clean up after this, forcing a
deviating node to relinquish to the deterministic master. The cluster pays
twice: elect locally, then migrate back. Agreement removes that cycle.

---

## 2. Requirements

- **R1 Agreement.** For a committed term, every node installs a byte-identical
  master assignment.
- **R2 Failover locality.** The assignment may prefer a master that holds the
  data.
- **R3 Bounded authority.** No participant may assign mastership outside a
  bound that every node can verify independently.
- **R4 Durability.** The assignment survives restart and both catch-up paths.
- **R5 Fail-closed.** A node that cannot obtain or validate the committed
  assignment withholds authority rather than falling back to a local
  derivation.
- **R6 Observable agreement.** Divergence is detectable cluster-wide, cheaply.

---

## 3. Design

### 3.1 Elect once, inside the commit

The proposer computes the base table via `compute_with_epoch`, applies the
election refinement, and the resulting assignment travels in the propose and
commit frames. Every node installs the committed assignment. **No node derives
mastership locally, ever** (this is #99's I0 — no node-local commit-apply
gates — carried forward).

### 3.2 Bound what a proposer may assign (R3)

Rev 1 said "install verbatim" with no validation, which deleted the only
containment that exists: `ShardTable::set_master_for_shard` refuses any
candidate not already in that shard's replica set — the guard that stops a
stale view fabricating an owner.

**Rule: `assignment[s] ∈ {det.master} ∪ det.replicas`** where
`det = compute_with_epoch(members, rf, 0, placement_version)`.

This is the wire-level equivalent of that refusal and is the single most
important gate. It is a pure function of digest-bound inputs, so every voter
reaches an identical verdict, and it grants exactly the power R2 needs and
nothing more. A compromised proposer can then only choose among the RF nodes
that placement already put on the shard.

Full validation, applied by voters before voting and by appliers before
installing:

1. exactly `NUM_SHARDS` entries — never pad, never truncate
2. every entry is a committed member of this term
3. **candidate-set rule above**
4. reject `NodeId(0)` explicitly — it is a live sentinel meaning "stale table,
   refetch" and an inbound-fence wildcard; a committed `NodeId(0)` leaves a
   shard masterless *and unrepairable* while every node reports self-consistent
5. `master ∉ replicas` for every shard (see §3.6)
6. per-node master count within `k ×` fair share — rule 3 alone does not stop
   "assign everything to me" when RF > 1 makes a node a wide candidate
7. when `members` is unchanged, the assignment moves at most N% of shards
   relative to the previous committed assignment — bounds the migration storm
   (one 32 KiB frame can otherwise trigger 4096 migrations × `migration_pool_size`
   connections) and doubles as an accident detector
8. `commit.proposer ∈ commit.members`

Validation is **all-or-nothing**. On failure the node rejects the commit and
self-fences via the existing `unapplicable_committed_term` / C11 machinery. It
must never partial-apply, never silently skip an entry (today
`set_master_for_shard` no-ops with a WARN, which would split installs), and
never fall back to a local derivation.

### 3.3 Bind the assignment to the digest (R1)

`TopologyTerm::compute_digest` covers exactly
`(term, cluster_id, members, placement_version, committed_peak)` — **verified
by direct read**. An assignment shipped alongside it is agreed by nobody: a
voter attests to a hash that does not cover it, so a proposer could collect
legitimate votes for term T and then commit assignment A to one node and B to
another. Every gate passes — notably `membership_change_is_safe` passes
*trivially* because `members` is byte-identical — both install, and neither
will ever accept a correcting commit for T. Committed, sticky, per-shard dual
authority.

**Add `assignment_digest = sha256(canonical encoding)` and mix it into
`compute_digest` as the last field**, exactly as `placement_version` and
`committed_peak` were folded in before. Carrying the hash (not the payload)
keeps `encode_committed_topology` and catch-up cheap.

**Also add `rf` to the commit and the digest.** It is currently local config,
and the candidate set in §3.2 is a function of it — an rf mismatch would
otherwise become a divergence source.

### 3.4 Ship the fence, not just the identity (R2)

Rev 1 claimed a wrong-but-agreed master costs only a migration. False: what
stops a data-less master from serving is the per-node **inbound fence**,
derived from local views, not from the committed payload. An agreed master that
holds nothing and is not fenced serves *empty* while the previous master serves
real data — dual content for one shard.

**Ship one bit per shard alongside the assignment: `unproven_master`** (4096
bits = 512 bytes). The proposer sets it whenever it cannot prove the named
master is a full holder. A node installing the assignment sets its inbound
fence for its own flagged shards and refuses to serve them until it has a
proven completion.

This makes "suboptimal beats divergent" actually true: suboptimal now means
**agreed and fenced**, which is strictly better than divergent.

### 3.5 Durability and recovery (R4)

Rev 1's fatal omission. Three paths must carry the assignment:

- **Restart.** `PersistedTopologyState` has no assignment field, so
  `restored_committed_shard_table` rebuilds via `compute_with_epoch` and stamps
  `version = committed_term`. `is_master` then returns `Yes` and the node
  serves **stale** data for a shard it no longer masters. Persist the
  assignment with the rest of the committed state.
- **`OP_GET_PARTITION_MAP` catch-up.** `install_active_routing_snapshot`
  *recomputes* and discards `routing.shard_assignments`, which is already on
  the wire. Install it (validated) instead of recomputing.
- **`OP_GET_COMMITTED_TOPOLOGY` catch-up.** `encode_committed_topology`
  fabricates a commit from local state. It must serve the real committed
  assignment, not a re-derivation.

The `topology_commit_tx` channel is typed `(Vec<NodeId>, u64)` — members and
term only — so the assignment is dropped between commit and activation. It must
carry the assignment.

### 3.6 Replica derivation

Replicas stay deterministic from `(members, rf, placement_version)` and are not
shipped — only mastership is elected. But the demotion rule must be explicit:
`set_master_for_shard` *swaps*, promoting the replica and demoting the old
master **into the replica slot**, preserving the holder set. Deriving replicas
from `compute_with_epoch` and overriding only the master would give
`master=B, replicas=[B]` for a deviated shard — RF effectively 1, and the
actual data holder in no role at all, which makes it eligible for orphan
cleanup. Hence validation rule 5.

### 3.7 Wire format

- Assignment: fixed-length `NUM_SHARDS` entries of `u16` **index into the
  sorted member list** — compact, self-validating (`idx < members.len()`), and
  already digest-bound via `members`. 8 KiB. (`NodeId` is a `u64`; a raw
  `NodeId` array would be 32 KiB, and rev 1's "2 bytes per NodeId" was simply
  wrong.) **Fixed-length, never length-prefixed** — that removes the
  `count`-driven allocation class F-G5-002 had to fix.
- `unproven_master`: fixed 512-byte bitmap.
- `TopologyTerm::deserialize` currently dispatches trailers by **exact total
  length**, so appending anything silently decodes `placement_version` as 1 and
  `committed_peak` as `members.len()` — which either wedges topology forever or
  **silently drops the G8 split-brain floor**. Delete the length-sniffing and
  version the payload with an explicit format byte plus explicit section
  lengths. Nothing is deployed, so there is no shim to keep.

### 3.8 Where the proposer's view comes from

`run_exchange_phase` runs only *post-commit* and on prompt catch-up — there is
no pre-propose view, so rev 1's step 1 described machinery that does not exist.

**Decision: reuse the last post-commit view, with a freshness bound.** A
pre-propose exchange would add ~2 s to every topology change and would have to
query the very nodes whose death triggered it. For the failover case the
previous view is the *right* input anyway — it records who held the data before
the master died. Beyond the freshness bound, propose the deterministic table
with every shard flagged `unproven_master`. I3 must be re-verified against this
choice, not assumed.

### 3.9 Removing the churn source

`apply_master_election` ranks on `was_previous_master`, which under this design
reads the *proposer's* prev table — so alternating proposers produce different
assignments for identical membership, and nothing terminates the churn.
**Drop `was_previous_master` from the committed election**, making it a pure
function of `(members, rf, placement_version, view)`.

### 3.10 The five proposal producers

`TopologyTerm::new` is called from `on_membership_changed`, `retry_proposal`,
`upgrade_proposal`, `propose_shrink`, and `check_timeout` — in a module with no
access to shards, engine, or views. Each must obtain an assignment.
`upgrade_proposal` (v1→v2 placement) reshuffles every shard and runs from a
timer; `propose_shrink` runs on the HTTP admin thread. A producer that cannot
compute an election proposes the deterministic table with all shards flagged
unproven. **It must never leave the section empty** — a zero-filled section
decodes as `NodeId(0)` for every shard and makes the whole keyspace
unroutable.

---

## 4. Every path that derives a master must read the committed assignment

Rev 1 named two detectors. That was the wrong target: the **repair action** is
what reverts the election. Reactivation calls `activate_topology_with_view`,
which recomputes and installs — and `apply_master_election` with an empty view
is a deliberate no-op *precisely so that reactivation installs round-robin*. So
the first legitimate trigger overwrites the committed election on that node.

`activate_topology_with_view` must take the committed assignment as an
**input** and stop computing masters at all. All nine production sites:

| # | site | change |
|---|---|---|
| 1 | `committed_topology_reactivation_metrics:745,754` | compare vs committed assignment |
| 2 | `phantom_master_shard_count:836` | compare vs committed assignment |
| 3 | `missing_master_shard_count:980` | compare vs committed assignment |
| 4 | `failed_handoff_disposition:1107` | read committed — **drops data** if wrong |
| 5 | `install_active_routing_snapshot:1172` | install shipped assignment |
| 6 | `restored_committed_shard_table:1610` | load persisted assignment |
| 7 | `activate_topology_with_view:3976,3998` | take assignment as input |
| 8 | `phantom_planned:4240` | diagnostic only; update or drop |
| 9 | `http.rs:2338` shrink guard | inherits 5/6 once fixed |

**Keep one independent recompute as a tripwire.** The detectors should use the
committed assignment for the work signal *and* additionally assert it satisfies
the §3.2 candidate-set rule against a locally recomputed `compute_with_epoch`,
with a loud metric and ERROR on failure. That costs one table recompute per
term and is the only remaining independent check that the committed assignment
is sane.

The stale rationale at `coordinator.rs:787-819` and the comment at `:2448-2455`
(which asserts the prompt path re-runs the exchange "so the resulting master
assignments match byte-for-byte" — the property §1 disproves) must be rewritten,
not left to mislead an implementer.

---

## 5. Invariants

- **I1** Two nodes reaching the same term from different histories and views
  install byte-identical assignments.
- **I2** A node that never received a partition view installs the same
  assignment as the proposer.
- **I3** Failover still promotes a data-holding replica —
  `segment_cluster_master_failover_preserves_replicated_record` passes.
  Re-verify against §3.8's view choice.
- **I5** A rejected or superseded proposal never installs its assignment.
- **I6** No term carrying a fresh assignment also changes membership/peak
  (#99's I6 — unbundling; otherwise the two mechanisms fight inside one commit).
- **I8** A node holding a committed term for which it has **no** committed
  assignment withholds authority. Never falls back to a recomputed table.
  Test: boot from persisted state with the assignment absent/corrupt →
  `is_master` returns non-`Yes` for all keys, node does not serve, metric +
  ERROR fire. *(This one would have caught every Critical in rev 1.)*
- **I9** Durability round-trip: the installed assignment is byte-identical
  after restart, after `OP_GET_PARTITION_MAP` catch-up, and after
  `OP_GET_COMMITTED_TOPOLOGY` catch-up.
- **I10** Integrity: flipping one entry in a propose or commit payload is
  rejected by the digest check; a commit whose assignment differs from the one
  voted on is rejected.
- **I11** Observable agreement: every node exports
  `(committed_term, sha256(installed_assignment))` on `/metrics`, and the E2E
  asserts **hash equality across nodes**. *(Replaces rev 1's I4, which was
  useless — `sum(master_shard_count) == NUM_SHARDS` passes under any
  permutation: A masters B's shard and vice versa gives sum 4096 with two
  shards dual-authority and two with none.)*
- **I12** No node serves a shard for which it has no data and no proven
  completion, even when the committed assignment names it master.
- **I13** An assignment naming a non-member, `NodeId(0)`, a node outside the
  shard's candidate set, or with `master ∈ replicas` causes the **commit** to
  be rejected and the node to self-fence — never a silent per-entry skip.
- **I14** Architectural: `set_master_for_shard` and `apply_master_election`
  have exactly one caller each, on the install path, and no production path
  constructs a master assignment by recomputation.

Rev 1's **I7** ("convergence in one round") is demoted from invariant to
**target**: it cannot hold whenever the committed master must receive data, since
the handoff is Copying → CommitReady → ServingNew across a real migration.
Track it as "≤ N rounds / ≤ T seconds for settled membership".

---

## 6. Expected effect on the E2E suite

Scenarios 05, 06, 07, 08, 09 are non-deterministic because convergence needs
several reactivation rounds and lands near their 60–120s budgets; two CI runs of
byte-identical code returned 6/14 and 8/14. Agreement should cut the round
count and move them off the margin.

This is a **prediction, not a claim**, and it is weaker than rev 1 implied: the
harness has at least one known independent flakiness cause on record
(`migration_pool_size 128 > max_connections_per_ip 64` starving shard
migration). Attributing part of the 15/17 residue here is speculation. Falsify
by running the suite twice at a fixed SHA.

---

## 7. Sequencing with the regime-fenced failover spec (#99)

**Land this first; #99 after.** They are not competing designs: this answers
*who is master*, #99 answers *how a transfer is made safe*. A regime fence is
meaningless while two nodes disagree about who the master is, so committed
election is a prerequisite.

Carried over from #99 now: **I0 — no node-local commit-apply gates** (§3.1),
and **I6 — no bundling** (§5). I0 specifically forbids the tempting
implementation of §3.2 where a node keeps its own master when the shipped one
"looks wrong"; it must be accept-or-self-fence, cluster-visibly.

---

## 8. Also required in this change

- Update `docs/DEPLOYMENT_ASSUMPTIONS.md:80-96`. It currently says a forged
  commit can drive split-brain via membership and the size floor. It must say a
  forged commit can **directly assign per-shard mastership with `members` left
  byte-identical** — a far quieter, more targeted capability that every
  existing split-brain defence (keyed on the member set) misses.
- Add a term-jump bound (`commit.term <= committed + MAX_TERM_JUMP`). Today
  only `term > committed` is checked, so a forged `u64::MAX` wedges topology
  permanently. Pre-existing, but each term now carries far more authority.

## 9. Open for the second review round

1. Is the §3.2 candidate-set rule sufficient to bound a hostile proposer, or is
   a per-node master-count cap (rule 6) load-bearing rather than defence in
   depth?
2. §3.8 reuses a pre-failure view. Does that satisfy I3 in the case where the
   *replica* also changed between the view and the failure?
3. Rule 7's move-delta bound needs a concrete N. What legitimate rebalance moves
   the most shards with `members` unchanged?
4. Does the §4 tripwire (committed assignment + local candidate-set assertion)
   actually catch a same-term split assignment, or only a malformed one?
