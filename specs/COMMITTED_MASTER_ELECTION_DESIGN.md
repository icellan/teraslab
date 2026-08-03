# Committed Master Election — design

**Status:** draft, awaiting review. Consensus-critical: per `REVIEW.md` this
needs `bitcoin-expert` + `security-auditor` sign-off before implementation.

**Context:** TeraSlab is not in production use. On-disk formats and the wire
protocol may change freely; no migration path is required.

## 1. The defect

`apply_master_election` (`cluster/coordinator.rs`) runs *after* a topology is
committed, on each node independently. It refines the deterministic
`compute_with_epoch` table using two inputs that are **per-node**:

- `partition_view` — the exchange-phase result, which returns partial and
  divergent views on different nodes
- `prev_table` — that node's previous topology, which differs by history

So two nodes can elect **different masters for the same shard**. Each node's
table stays internally self-consistent (`target_assignment == intended_master`,
and the elected master is always inside the shard's candidate set), so the
steady-state mismatch metric reads `mismatched == 0` on every node. The
divergence is invisible locally and only appears in a cross-node sum of
`master_shard_count`.

### 1.1 Why the two obvious fixes fail

Both were implemented and measured on CI. Do not repeat them.

- **Run convergence faster** (reactivation cooldowns 15s/30s → 5s/10s): made
  it *worse*. Scenario 09 reached `masters=5461/4096` at `ver=8` — 1365 excess
  masters, about one node's entire share. Every activation is a fresh chance
  to diverge, so a higher activation rate multiplies divergence rather than
  converging faster. Reverted in `18c7c07`.
- **Remove the refinement** (pass an empty partition view, making the table a
  pure function of `(members, rf, placement_version)`): does give
  cross-node-identical tables, but breaks failover —
  `segment_cluster_master_failover_preserves_replicated_record` fails. The
  refinement is what promotes the replica that actually **holds the data**
  when a master dies; without it the deterministic table can hand mastership
  to a node with no copy.

**Election is load-bearing for failover, not merely a placement
optimization.** It cannot be removed and cannot be run faster. It must gain
cluster-wide agreement.

### 1.2 The churn this also causes

`phantom_master_shard_count` exists solely to clean up after this: it forces a
node whose local election deviated from the round-robin pick to relinquish the
shard to the deterministic master. So the cluster currently pays **twice** —
election promotes a data-holder locally, then the phantom detector migrates
the shard back to the round-robin node. Fixing agreement removes that
promote-then-relinquish cycle.

## 2. Requirement

For a given committed term, **every node must install a byte-identical master
assignment**, and that assignment must still prefer masters that hold the
data.

## 3. Design: elect inside the commit

The agreement machinery already exists — `OP_TOPOLOGY_PROPOSE` /
`OP_TOPOLOGY_VOTE` / `OP_TOPOLOGY_COMMIT`. Election is currently applied
*after* commit, locally. Move it *into* the committed payload.

1. The **proposer** computes the base table via `compute_with_epoch`, then
   applies the election refinement using the partition views it has collected.
2. The resulting **master assignment is carried in the proposal** and therefore
   in the committed payload.
3. Every node **installs the committed assignment verbatim**. No node applies
   any local refinement. `apply_master_election` is no longer called on the
   activation path.

Agreement is then a property of the existing quorum commit, not a new
protocol. Nodes that never saw a partition view install the same table as the
proposer, because they are told the answer rather than deriving it.

### 3.1 Suboptimal beats divergent

If the proposer's partition view is stale or partial, the committed assignment
may place a master on a node that does not hold the data. That costs one
migration. It does **not** cost correctness: the assignment is still *agreed*,
so single-master-per-shard holds. This is the correct trade — a suboptimal
agreed table is always better than a locally-optimal divergent one.

### 3.2 Wire format

`OP_TOPOLOGY_PROPOSE` and `OP_TOPOLOGY_COMMIT` gain a master-assignment
section: `NUM_SHARDS` (4096) entries of `NodeId`. At 2 bytes per entry that is
8 KiB per frame, well inside `MAX_FRAME_SIZE`. Replicas continue to be derived
deterministically from `(members, rf, placement_version)` and are **not**
shipped — only mastership is elected, so only mastership needs agreeing.

No back-compat shim: the cluster is not deployed.

## 4. The trap that will break this if missed

`phantom_master_shard_count` and `missing_master_shard_count` currently
recompute `compute_with_epoch(committed_members, rf, 0, placement_version)`
and treat any deviation as work outstanding.

**Both must instead compare against the COMMITTED assignment.** If they keep
recomputing round-robin, they will flag every legitimately elected
data-locality deviation as a phantom and force a relinquish — fighting the
committed election forever and reintroducing exactly the churn this design
removes. This is the single highest-risk item in the change.

The detectors keep their purpose: they still catch a node whose *local* table
drifted from what was committed. Only their reference changes, from "recompute
the deterministic table" to "the assignment this term committed".

## 5. Invariants to test

- **I1** Two nodes reaching the same term from different histories and
  different partition views install byte-identical master assignments.
- **I2** A node that never received a partition view installs the same
  assignment as the proposer.
- **I3** Failover still promotes a data-holding replica —
  `segment_cluster_master_failover_preserves_replicated_record` must pass.
- **I4** Cross-node `sum(master_shard_count) == NUM_SHARDS` after any
  rebalance settles, with no reactivation rounds outstanding.
- **I5** `phantom_master_shard_count == 0` for a node whose table matches the
  committed assignment, even where that assignment deviates from round-robin.
- **I6** A rejected or superseded proposal never installs its assignment.
- **I7** Convergence completes in ONE reactivation round for a settled
  membership (this is what makes the E2E scenarios deterministic and fast).

## 6. Expected effect on the E2E suite

Scenarios 05, 06, 07, 08, 09 are non-deterministic because convergence needs
several reactivation rounds and lands near their 60–120s budgets; two CI runs
of byte-identical code returned 6/14 and 8/14. With an agreed assignment,
convergence should complete in one round (I7), moving the scenarios well off
their timeout margin — satisfying both halves of the goal, deterministic *and*
fast. Part of the 15/17 residue plausibly traces here too, since a shard with
two masters cannot complete a handoff.

This is a prediction, not a claim. It is falsifiable by re-running the suite
twice at a fixed SHA.

## 7. Open questions for review

1. **Proposer view quality.** Should a proposer with an empty or badly partial
   partition view decline to propose, or propose the pure deterministic table?
   Declining risks stalling; proposing risks a needless migration wave.
2. **Eviction.** `apply_master_election` also handles the evicted set, which is
   cross-node deterministic. Does eviction move into the committed payload too,
   or stay a local post-step?
3. **Re-election on membership change without a topology change.** If a master
   dies but membership has not yet changed, what triggers a new agreed
   assignment — and does that path need its own term?
4. **Interaction with `[[project_cluster_hardening_campaign]]`'s regime-fenced
   failover spec (#99).** That design also touches mastership authority; these
   two must be reconciled before either lands.
