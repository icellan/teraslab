# Cluster Hardening Design — Regime-Fenced Failover and Protocol Roadmap

Status: DESIGN, revision 4 — after three adversarial review rounds
(bitcoin-expert + security-auditor; 39 findings round 1, 22 round 2, 14
round 3 — every round's findings fully folded, converging from
mechanism-breaking to definitional). Round-3 verdicts: "mechanism
settled" / "implementable as specified once folded" — both folds are in
this revision. §9 is the full review log. P1/P6a implementation is gated
on user build/no-build sign-off, the named I0–I13 invariant tests, and
the §8 cross-subsystem review. P6b/P5 are shipped (PRs #97, #98); P2/P4
are in implementation.

This document records the comparative analysis behind the clustering
hardening campaign and specifies its consensus-critical core: safe,
automatic, quorum-committed master failover. Techniques are drawn from how
mature production clustered stores structure partition ownership, and from
the distributed-systems literature (SWIM, Lifeguard, consensus-managed
topology, primary-backup reconfiguration). No external product is named
here by policy; the mechanics stand on their own.

---

## 1. Problem statement

TeraSlab clustering is safe but brittle in three ways:

1. **No automatic failover.** A `TopologyCommit` carries members only;
   per-shard masters are derived by a pure placement function. When a master
   dies or a heal wedges, the shard answers `Transitioning` until an
   operator intervenes, and the heal-deadline action (`AlertAndHold`) alerts
   forever without escalating. Two prior attempts at "recency"-based master
   election (#76 revert, R4) were rejected as double-spend-unsafe (§5.1).
2. **Failure detection is fixed-timeout.** SWIM with a 200 ms probe and 5 s
   suspicion declares a CPU-starved or briefly-stalled node dead; every
   false death triggers a topology transition and a migration storm.
3. **Recovery data paths are throttled far below the workload.** Replica
   catch-up ships at most 10,000 redo ops per 30 s tick; there is no fill
   delay, so a rolling restart triggers full data movement.

The mature-store pattern this design adopts: **partition-granular ownership
state, committed through consensus, with the old owner fenced at write
granularity by a per-partition fencing value, and promotion eligibility
determined by copy-completeness lineage — never by wall-clock recency.**

## 2. Current architecture (what we build on)

- Membership: SWIM (`src/cluster/swim.rs` transport thread,
  `src/cluster/membership.rs` pure incarnation-keyed state machine).
  Suspect-counts-alive (E-03); suspects excluded from `alive_node_count`
  quorum checks.
- Topology: `TopologyAuthority` (`src/cluster/topology.rs`) — term/vote/
  commit over the member set, digest-covered, `committed_peak` as the
  durable split-brain quorum floor (Gate A/B shrink).
- Placement: pure function over (sorted members, RF): v1 round-robin, v2
  rendezvous (HRW). `NUM_SHARDS = 4096`.
- Replication: master fans out `OP_REPLICA_BATCH` stamped with the committed
  `cluster_key` (the same atomic as `committed_term`); receiver rejects
  stale keys (`ERR_STALE_EPOCH`). RF=2 default, `WriteAll`. The production
  ack classifier is `classify_per_key_replication`
  (`src/server/dispatch.rs`), per key against that key's own replica set.
- Recovery: redo shipping for catch-up; fence + baseline pull for
  reverse-heal; manifest-verified migration.

Load-bearing UTXO-workload properties: mutations idempotent by txid (one
exception: `reassign` — §4.9); spend one-shot monotonic; creates
immutable-shape; routed reads go to the master.

## 3. Roadmap

| # | Item | Depends on | Consensus-critical | Status |
|---|------|-----------|--------------------|--------|
| P6b | Migration throttle condvar | — | no | shipped (PR #97) |
| P5 | Commit dissemination hardening | — | light | shipped (PR #98) |
| P2 | Local-health-aware failure detection | — | no (§7.3) | in implementation |
| P4 | Catch-up streaming + fill-delay | P6b | light (§7.4) | in implementation |
| P1 | Per-shard regime + lineage + committed promotion | P5 | **yes** | rev 3 — final verify gate |
| P3 | Single decisive transition; v2 placement default; quorum upgrade | P1, P2, P5 | partial | after P1 |
| P6a | Heal-deadline escalation ladder; Degraded/Lost states | P1 | **yes** | rev 3 — final verify gate |
| P6c | Self-fence advertisement + auto-exclusion | P1, P4 | adjacent | after P1 |

## 4. P1 — Per-shard regime, version lineage, committed promotion

### 4.0 Invariants

Every claim in §4.6 derives from these numbered, individually testable
invariants.

**I0 — No node-local commit-apply gates.** No gate that decides whether a
commit is applied may depend on node-local configuration or node-local
observation. Every rejecting clause must be derivable from (the commit) +
(the node's installed committed state) alone — otherwise nodes with
different views split on installing a mastership-carrying commit, which is
dual-serving by construction (the R4/#76 failure mode). Node-local
knowledge (heal-deadline expiry, self-observed lineage, resolved ack
policy, migration runtime state, local timers) may gate what a node
*proposes* or what it *serves*, never what it *applies*.

**The committed derivation.** All I10 clauses evaluate against the pure
function

```
committed_master(s) = override_map.get(s)
                        .unwrap_or(placement(committed_members,
                                             placement_version)(s))
```

over **installed committed state only** — never `effective_assignment`
(per-node migration runtime), never the live `ShardTable` (which carries
handoff mutations), never a local timer. The override map is **full and
cumulative**: every shard whose master deviates from placement appears;
a shard absent from the map is placement-derived; the proposer recomputes
the whole map each term; and **retiring an override is itself a master
change** (bumps that shard's regime) — otherwise the map grows without
bound and a long-dead override resurrects a stale master after an
unrelated membership change. (The regime array being absolute while the
map was delta-encoded would fix the fence and break the thing it protects:
a term-skipping node would derive a different master. Both are absolute.)
Named acceptance test: two nodes at the same committed term, one
mid-handoff, must accept/reject an override-carrying commit identically.

- **I1 — Install ordering (not atomicity).** On every node, the regime
  state for a term is installed inside `apply_commit_locked`, under the
  `commit_apply` mutex, no later than the `committed_term` store and
  strictly before the shard table for that term is activated. Permitted
  skew: gate-closed-but-not-yet-serving (fail-closed). The reverse skew is
  a double-spend and must be structurally impossible. Pinned by a test that
  a batch stamped at the old regime is rejected the instant
  `committed_term` advances, before the table version does.
- **I2 — Lineage currency.** `Full` implies membership in the shard's
  **holder set**, defined as

  ```
  holder_set(s) = target_assignment(s) ∪ { effective_assignment(s).master }
  ```

  — the actively-serving departing master stays a holder (it is still
  taking and acking the shard's writes mid-handoff; `target_assignment`
  alone would mark it `Subset` while it serves), but a departing
  **replica** is NOT in this set: the write fan-out reads
  `target_assignment` only, so a departing replica receives nothing from
  the instant the new assignment installs, and the full
  `effective ∪ target` union would let it keep a facially valid `Full`
  while missing every acked write — the round-1 stale-`Full` hazard
  re-entering at the replica-swap seam. Do not "simplify" this back to the
  full union. A node's lineage for `s` degrades to `Subset` the instant it
  exits `holder_set(s)`, or the regime advances while the node is not in
  the new holder set — in addition to the §4.3 data-motion triggers.
  `Full` must be re-earned after any holder-set exit. **Stamp refresh:**
  on installing any commit, a node that remains in `s`'s holder set
  refreshes its `Full` stamp to the new regime (same `commit_apply`
  section as I1/I13ii; one batched durable write per commit, never
  per-shard fsyncs — a whole-node failover re-stamps ~1024 shards inside
  the `commit_apply` mutex); without the refresh, every regime-equality
  comparison in §4.4.2 and I13(ii) livelocks one regime behind.
- **I3 — Promotion is a holder swap.** An override target MUST be a member
  of the shard's pre-override holder set. The override's post-state is
  defined: `master := R`, `replicas := (pre-override holder set ∖ {R})` —
  cardinality preserved, the old master M demoted into a replica slot (so
  M keeps receiving writes and can re-earn `Full`, and the post-override
  replica set is never empty — a promoted shard must never degenerate to
  zero replication targets).
- **I4 — Ack-policy gate, committed not local.** Automatic promotion is
  available only when the committed `promotion_enabled` field is true.
  **Cluster-wide evidence, not one node's config:** each node advertises
  its resolved ack policy in the same `(NodeId, incarnation)` advert
  channel as regime support; `promotion_enabled = true` may only be
  **proposed** when every committed member has advertised
  WriteAll-equivalence (`required_replica_acks(targets, policy) ==
  targets`) and `replication_degraded_mode != "best_effort"` — a single
  node's startup check would commit a cluster-wide safety claim from local
  config (a heterogeneous cluster with one `auto`-at-RF≥3 member breaks
  §4.6 on that member's shards). The startup `ConfigError` (added to
  `validate_cluster_safety`) additionally refuses to *run with*
  `promotion_enabled`-proposal capability on a non-qualifying local
  config; note `ack_policy = "auto"` resolves to `WriteMajority` at RF≥3,
  so RF=3 stock config refuses. Appliers check the committed field only
  (I0). Governance: flipping `promotion_enabled` false requires
  `admin_token` + `cluster_secret`; the state is surfaced in `/status` and
  as a metric (failover-off must never be silent); a later `false` does
  NOT un-promote an already-committed override (regimes ratchet — I10(d);
  un-promoting is a new mastership change needing its own term); §6.3's
  ladder names `promotion_enabled = false` as the reason when step 3
  cannot escalate. RF=2 + `auto` is the only configuration this design
  validates.
- **I5 — Fenced replay.** A pending replication intent records the
  `(shard, regime)` it was created under. An intent whose shard regime has
  advanced is never re-shipped: it is converted into a **per-key** resync
  against the current master and the intent is committed. Full-shard
  resync is reserved for the case where per-key repair is impossible
  (reclaimed redo range) — otherwise one failover of a node mastering
  ~1024 shards converts pending intents into a whole-dataset migration
  storm through the recovery path. Startup never fails on a
  regime-superseded intent.
- **I6 — No bundling.** A term carrying overrides MUST NOT also lower
  `committed_peak`, and a shrink term MUST NOT carry overrides. A regime
  rebase term (I7) carries neither overrides nor a peak change. An
  override-carrying term MUST NOT change `promotion_enabled`, and vice
  versa — bundling them makes I4's check order-dependent within one
  commit, an I0 violation through the back door. (I6 is load-bearing for
  I0 in a second way: Gate B can legitimately split appliers on a shrink
  term, and only I6 guarantees such a split never splits mastership.)
- **I7 — Term binding + rebase.** `regime[s] ≤ committed_term` for every
  shard (definitional under §4.1; the structural gate in
  `commit_passes_gates` defends against forged/corrupted state). Recovery:
  a quorum-committed **regime rebase** (operator-initiated: requires
  `admin_token` + `cluster_secret`; normal activation quorum; Gate A/B
  apply; I6-unbundled) repairs inconsistent or operator-unwanted regime
  state. Scope stated honestly: under the absolute representation a forged
  bump self-heals on the next legitimate higher-term commit; the rebase
  exists for operator repair, and forged near-max **terms** remain the
  pre-existing fail-open residual, out of scope here.
- **I8 — Promotion bumps the term (precise form).** A promotion is only
  applied via a term strictly greater than the previous committed term.
  Consequence, stated precisely: any receiver that has installed the
  promoting commit rejects a replayed pre-failover batch wholesale at the
  existing `cluster_key < local_cluster_key` gate; a receiver that has NOT
  installed it takes the accept-newer arm — but such a receiver is not
  serving the promoted mastership either, so the replay cannot land beside
  a new master's writes. The §4.7 captured-frame replay test asserts this
  exact property, not the stronger "dies everywhere" form.
- **I9 — Regime provenance.** A receiver's regime state is updated ONLY by
  installing a commit (or a proof-carrying anti-entropy pull, §4.8). Never
  learned from a replica batch; an `ERR_STALE_REGIME` NAK is a routing
  hint, never authority. Anti-entropy merges are max-merge and durable.
- **I10 — Structural commit validation (all clauses I0-clean).** Every
  node re-validates an override-carrying commit at apply, rejecting when:
  (a) any override target violates I3 (membership in the pre-override
  holder set, computable from installed committed state); (b) any override
  regime ≠ the carrying term; (c) I7 is violated; (d) **never-lower** —
  any `commit.regime[s] < local_regime[s]` (the array ratchets; this
  removes the fence-disarm primitive, including from a stale-but-honest
  proposer); (e) **bump-justification** — any `commit.regime[s] >
  local_regime[s]` for a shard whose master under this commit equals the
  locally installed master for `s` (a legitimate bump always coincides
  with a master change; with absolute arrays every installer agrees);
  (f) the commit's `proposer` — which becomes digest-covered — is not a
  member of the commit's own member set (`proposer ∈ commit.members` is
  the only I0-clean form: "is the deterministic proposer" is derivable,
  but "a valid `check_timeout` fallback" is a local timer, and a
  timer-split on a mastership commit is dual-serving; fallback-proposer
  abuse is already bounded by one-vote-per-term + quorum intersection,
  and deterministic-proposer discipline remains a proposer-side rule).
  All clauses evaluate against the **committed derivation** (I0) — clause
  (a)'s holder set for apply-validation purposes is computed from
  committed state (`committed_master(s)` + committed replicas), NOT from
  I2's serving-side holder set, which contains `effective_assignment`
  runtime state. There is **no numeric override cap**: a single master
  death legitimately overrides up to ~`NUM_SHARDS/N` shards, and clauses
  (a)–(e) bound forgery damage more tightly than any count. Proposer-side
  justification (master dead/excluded, heal-deadline expired,
  subset-destination master) is a proposal precondition plus applier-side
  advisory logging — never an apply gate (I0).
- **I11 — Secret gating.** Regime enforcement requires `cluster_secret`.
  A node without a secret never advertises regime support, never proposes
  `regime_enforced`, and — critically — **ignores a committed
  `regime_enforced` entirely** (no enforcement, no self-fence): in
  fail-open, commits are forgeable, and a C11-style permanent self-fence
  armed by one forged frame would be a reboot-resistant cluster-wide kill;
  ignoring keeps the fail-open blast radius exactly where it is today.
  (Fail-open is already documented as unfit for multi-node production; in
  fail-open, forged partition-view reports are likewise an
  availability-control lever — §4.4.5's Subset check included.) With a
  secret: enforcement state is the committed `regime_enforced` field;
  support adverts are keyed by `(NodeId, incarnation)` so a same-id binary
  downgrade resets them; a secret-holding node that observes committed
  `regime_enforced = true` it cannot honor self-fences like the C11
  unapplicable-placement fence.
- **I12 — No sender opt-out; total sender coverage.** Once
  `regime_enforced` is committed, a secret-holding receiver rejects
  **regime-absent** batches (V2 frames) with `ERR_STALE_REGIME`,
  mirroring the C24 zero-wildcard closure — a demoted master must not
  evade the fence by continuing to emit V2. **Every `OP_REPLICA_BATCH`
  producer emits V3 from the enabling commit, without exception**:
  foreground fan-out, `repair_missing_record_target`, migration baseline
  and delta batches, reverse-heal pulls, full-shard resync, lag-monitor
  catch-up, and the compensation path — each stamps the sender's regime
  view and is subject to the same gate (§4.7 records that migration/
  out-of-band batches bypass sequence dedup, so leaving any of them on V2
  would leave the fence's most dangerous paths unfenced, and enabling
  enforcement without converting them would halt all data movement,
  including the heal traffic whose failure feeds §6.3's ladder). A
  migration/heal/resync sender receiving `ERR_STALE_REGIME` treats it as
  **abort-and-re-plan after topology refresh**, never as a data error — a
  heal source is not necessarily the shard's master and may legitimately
  be behind on terms. **V3 acceptance is a binary capability, not
  committed state:** a V3-capable receiver accepts V3 unconditionally,
  independent of `regime_enforced`; only the *rejection of V2* is gated
  on the committed field — otherwise every enable event is a
  cluster-wide write outage for the commit-propagation window.
- **I13 — Transition-scoped lineage gate + promotion re-stamp.** A node
  must self-observe `Full` to **begin serving a shard it was not serving
  in the previous regime — override-driven or placement-driven** (scoping
  by mechanism would leave the commonest path, HRW re-mastering after a
  death, free to serve from a stale copy). Two completions: (i)
  **boot/standing re-derivation** — a node that is the committed master of
  `s` with an intact, un-reclaimed redo range re-derives `Full(current
  regime)` locally **regardless of the stored stamp's regime** (a lost
  lineage file must not total-outage a healthy node, AND a crash after
  the `committed_term` persist but before the lineage re-stamp persist
  must be self-healing — the install is never replayed, so without the
  regardless-of-stamp form that microsecond-window crash strands the
  shard `Transitioning` forever with its own committed master unable to
  serve and P6a unable to help); (ii) **the promotion re-stamp**: when a
  node installs a commit naming itself master of `s` and its lineage at
  install is `Full` at the pre-override regime (kept current by I2's
  stamp refresh), the install atomically re-stamps `Full(new regime)`
  inside the same `commit_apply` section (I1), durably before serving —
  batched with I2's refresh, one write per commit. The re-stamp IS the
  promotion.

### 4.1 Regime: a per-shard fencing value

**Definition:** `regime[s]` := the committed term in which shard `s`'s
master last changed.

Revision 1's independent delta-encoded counter diverged on term-skipping
nodes (no contiguity requirement exists in `commit_passes_gates`) and the
fence failed open; see §5.4. Under the term-stamped definition the full
regime state is carried **absolutely in every commit** — a pure function of
the installed commit, immune to skips, no wraparound (u64), no separate
counter lifecycle.

- **Wire.** `TopologyTerm`/`TopologyCommit` carry the override map and
  regime array in canonical sorted-by-shard order, digest-covered
  (canonical ordering makes independent proposers of the same override set
  produce identical digests; `proposer` joins the digest per I10f). The
  existing encodings are positional with silently-defaulting tail-read
  trailers (a short read yields default `committed_peak` = members.len() —
  i.e. a truncation would silently disarm the split-brain floor); adding a
  variable-length block there is unacceptable. The **wire encodings get
  the same versioned, length-prefixed, integrity-checked envelope as the
  state file**: every trailer explicitly present/absent-tagged, any decode
  shortfall a hard reject (never a default), and decode-side enforcement
  of canonical order, no duplicates, `count ≤ NUM_SHARDS`, reservations
  clamped (the `decode_ops` untrusted-count lesson, applied to the commit
  block as well as the batch trailer). Size: ≤ 4096 entries ≈ tens of KiB,
  measured against the topology-commit latency budget (rare-path fsync),
  accepted.
- **Persistence.** The topology state file becomes a versioned,
  length-prefixed, **checksummed** envelope
  (`[magic][version:u16][len:u32][body][sha256]`). Integrity mismatch is
  fail-closed: a clustered node refuses to start. Legacy tolerant decode
  survives only behind a **CLI-only** (never TOML), **self-consuming**
  upgrade flag: the first legacy decode immediately rewrites the file in
  the new envelope, and passing the flag when the file is already
  new-format is a hard error — otherwise the flag is a standing downgrade
  switch back to silent-truncation parsing.
- **Armed marker.** A presence-only `.regime-armed` sidecar (the
  `.multinode` precedent: unreadable ⇒ present). Marker present + regime
  state absent/zeroed ⇒ refuse to start — **except**: the apply path of a
  committed `regime_enforced = false` (and a rebase that zeroes state)
  **deletes the marker**, exactly as the shrink-to-1 commit deletes
  `.multinode` (this repo already shipped a sticky-marker
  boot-unreachability bug once; the deletion path is not optional). Marker
  present on a node that has never observed a committed term is a loud
  warning, not a refusal (a zero-byte file dropped in the data dir must
  not be a boot-DoS).
- **Rollout.** Replica-batch wire V3 is a new version byte with the regime
  table appended **after** the op stream (inside the V2 header a V2
  decoder misparses it as an op length and wedges; as trailing bytes under
  the V2 byte it is silently ignored — half-enforcement). Old receivers
  fail closed on the unknown version byte (the posture that already
  removed V1). V3 is sent only after `regime_enforced` commits; from the
  same commit, I12 rejects V2 senders (with I12's
  acceptance-is-a-capability rule covering the propagation window).
  **Enable sequencing:** advertise support → verify every committed
  member's `(NodeId, incarnation)` advert → commit `regime_enforced`.
  During commit-propagation skew, not-yet-installed masters get batches
  rejected — surfacing as retryable `ERR_REPLICATION_FAILED`; enable in a
  maintenance window. **Stated availability consequence:** a committed
  member on a pre-P1 binary after enforcement is a bidirectional
  replication blackout (~2/N of the keyspace: rejects V3 on the unknown
  version byte as receiver, dies on I12 as sender) — and it cannot
  self-fence, because a binary that predates the field cannot parse it,
  and the digest break also excludes its votes. The blackout is correct
  (fail-closed) but must not be a surprise: the cluster raises a distinct
  `regime_unsupported_member` alert for any committed member whose advert
  is below regime-support while `regime_enforced` is true, as an
  operator-gated eviction candidate. **Disarm (`regime_enforced = false`)
  requires quiescence:** every committed member alive and acknowledging
  the current committed term (full-membership ack, not a quorum) plus
  operator confirmation — dropping enforcement instantly re-admits a
  demoted-but-partitioned master's V2 batches, which is dual-serving
  produced by a legitimate operator action. A node that misses the disarm
  commit keeps enforcing (fail-closed availability blip). Marker
  ordering: the disarm apply deletes `.regime-armed` and fsyncs the
  parent directory **before** zeroing/rewriting regime state (the
  `delete_topology_multi_node_marker` precedent supplies the fsync half;
  the ordering half prevents a crash leaving marker-present +
  state-zeroed = refuse-to-start).

### 4.2 Write fencing: regime-stamped replica batches

V3 appends after the op stream: `[touched_count: u16][(shard: u16,
regime: u64)...]`, sorted ascending, no duplicates, `touched_count ≤
NUM_SHARDS`, reservations clamped.

Receiver gate, per touched shard, alongside the `cluster_key` gate:

| Condition | Action |
|---|---|
| `batch_regime[s] < local_regime[s]` | reject whole batch, `ERR_STALE_REGIME(shard, local_regime)` |
| `batch_regime[s] > local_regime[s]` | accept (commit in flight; regime state NOT updated — I9) |
| equal | accept |

The stamp is captured once at fan-out entry alongside `cluster_key` and
threaded through every resend path — including
`repair_missing_record_target` — so a demoted master can never re-stamp
itself past its own demotion mid-repair.

On `ERR_STALE_REGIME`: **the whole client batch fails with
`ERR_REPLICATION_FAILED` and is compensated** — every key in the rejected
batch lost its replica ACK, and per-key classification already enforces
that; no key is carved out or salvaged from a rejected batch (an earlier
draft's "fail only the stale shard's mutations" instructed exactly the
client-ack-without-replica-ack §4.6 forbids). "Continue shipping other
shards" means subsequent, independent fan-outs on the same per-node stream
are not blocked — never partial salvage. The master refreshes topology
before the client's retry. Burned stream positions heal via the existing
`Gap`/relabel renegotiation; the receiver's watermark never advances on a
rejected batch, so no op is skipped and nothing deadlocks. There is no
re-slice of a partially-stale batch.

The regime gate is a **staleness fence, not an authorization check** — a
sender able to produce authenticated frames can inflate its own stamp,
exactly as it can inflate `cluster_key` today. Documented residual.

### 4.3 Lineage: Full vs Subset copies

Per-shard, per-node, **self-observed**, persisted:

```
Lineage = Full(regime: u64, data_epoch, node_id) | Subset
```

- → `Subset` when (first trigger load-bearing — I2): the node exits the
  shard's holder set (`effective ∪ target`) or the regime advances without
  it; inbound migration begins; full-shard resync begins; a heal fence is
  raised; `RedoReclaimed` covers a needed range.
- → `Full` when: inbound migration completes with verified manifest; heal
  completes; resync completes; catch-up watermark reaches the master's
  sequence over an intact baseline — while in the holder set. Plus the two
  I13 completions (standing-master re-derivation; promotion re-stamp).

Fail-closed defaults (polarity inverted vs the inbound-state file):
**absent ⇒ `Subset`; unreadable ⇒ `Subset`; partially parsed ⇒ `Subset`
for every shard not positively decoded.** Never `unwrap_or_default()`.

Identity binding: `data_epoch` is a restore-stamped identity written by
`restore()`; `node_id` is the claiming node. Mismatch on **either**
degrades every shard to `Subset` — the `node_id` binding is what catches a
cloned data directory (dd/LVM/VM-image copy) coming up as a different
node with frozen-in-time data and a facially valid `Full`; a clone under
the same id is already rejected as a duplicate NodeId. `data_epoch` MUST
NOT change on device add/resize/reformat — only `restore()` stamps it —
or routine device maintenance would degrade every shard cluster-wide.
`restore()` deletes the lineage and inbound/outbound cluster state files
outright, and the restore runbook says so.

**`Subset` fences background reclamation too:** self-observed `Subset`
for `s` fences `SweepRole::HeldCopy` for `s`, alongside the existing
migration fences — I13 fences serving, but the DAH/retention sweep is
holder-scoped background work whose existing fences
(`has_pending_inbound`, write-fence) do not fire for a
holder-exit-and-re-entry `Subset`, and a sweep against an incomplete copy
is deletion, not staleness.

**Node replacement/renumbering:** an identity mismatch (either
`data_epoch` or `node_id`) degrades lineage to `Subset` but does **NOT**
invalidate the baseline — re-earning goes through the
catch-up-over-intact-baseline trigger, never a forced full-shard resync
(wiring "mismatch ⇒ resync" turns a five-minute node swap into a
whole-dataset migration).

Nodes report `(lineage, regime)` per shard in the partition-view exchange
— **proposer input only**; serving eligibility is always self-observed
(I13). `PARTITION_FLAG_PENDING_INBOUND` is retired as an
election/eligibility classifier only; it remains load-bearing for
reverse-heal source selection and migration-skip decisions.

### 4.4 Promotion: a quorum-committed override

Master deviation stops being a per-node local computation
(`apply_master_election`'s local deviation is retired; the serving-side
protection it provided is replaced by I13's transition-scoped gate). The
proposer:

1. Collects the partition view (polled, advisory — staleness is safe).
2. Computes overrides `(s: M → R)` — each sets `regime[s] = carrying term`
   and the I3 swap post-state.
3. Rides them through the existing term/vote/commit machinery at the
   normal activation quorum over `committed_peak` — no lowered bars, no
   fabricated quorum, Gate A/B untouched.

Proposer-side preconditions (advisory-logged by appliers, never apply
gates — I0):

1. The carrying term reaches `activation_quorum_needed` over the peak
   floor. *(This one is structural and IS an apply gate, as today.)*
2. R reported `Full` at the current committed regime and satisfies I3.
3. One of: (a) M is dead/excluded from the new member set; (b) the
   override is a P6a escalation (§6.3 rails); (c) **availability arm** —
   the shard's committed master is a **subset destination** (has a pending
   inbound migration or heal fence for `s`) while R reports `Full`. Arm
   (c) is scoped to subset-*destination* masters precisely — never to a
   master that is `Subset` merely by holder-exit during its own outbound
   handoff (under I2's union definition an actively-serving departing
   master is still a holder, so routine migrations do not trip this arm) —
   and it inherits §6.3's cooldown/budget/progress rails.
4. The committed `promotion_enabled` field is true (I4).
5. At RF=2, if the sole replica is `Subset`: no promotion — the shard
   stays `Transitioning` (strictly no worse than today). (Fail-open
   caveat: I11 — in fail-open this check's inputs are forgeable.)

Proposer determinism: only the deterministic proposer (lowest `NodeId` of
committed membership, `check_timeout` fallback) may originate an
override-carrying term — enforced structurally by I10(f) (digest-covered
`proposer`), not by etiquette. An override term is not superseded by
another override term until it commits or times out; override proposals
observe a jittered minimum interval.

Formation-recovery exception: the `handle_propose` arm accepting
`term ≤ voted_term` for ≤1-member committed states MUST NOT apply to
override-carrying terms (double-vote at one term ⇒ two same-term commits
with different masters). Override-carrying proposals require strict
`term > committed && term > voted`.

### 4.5 Fencing order

```
collect view → quorum vote on term (override + regime = term) → commit
→ each node installs regime state under commit_apply, no later than the
  committed_term store (I1) — the gate closes to the old master here;
  the promotion re-stamp (I13ii) happens in the same section
→ table activation for the term (strictly after)
→ a node begins serving a newly-acquired shard ONLY IF self-observed Full
  (I13)
```

### 4.6 Safety argument (the R4 test)

The write surface that matters is a double-spend: the old master M acking
a spend the promoted node R does not know. At RF=2 WriteAll (I4):

- **R is M's only quorum partner — by I3, not by assumption.** Every write
  M acks requires R's ack. By I1, R's regime gate closes no later than R's
  `committed_term` store, which precedes R serving. At every instant at
  most one of {M can complete acked writes, R serves} holds. A batch
  racing the commit either lands before R's gate closes (acked AND held by
  R) or is rejected (nothing acked). Voting does not close the gate, so a
  batch between R's vote and R's install is applied, acked, and held by R
  at install. Safe.
- **No acked write is rolled back by promotion.** WriteAll + I2 + I3: R
  was a required-ACK holder for the entire window since it last earned
  `Full`. In-doubt writes are the ambiguous-outcome class the durability
  contract defines; idempotent re-send converges them; `reassign` via
  §4.9.
- **The R4 interleavings:** no local election exists to diverge; a stale
  view at worst proposes a `Subset` node that self-observes and refuses
  (fail-closed to availability); `PENDING_INBOUND` no longer participates
  in eligibility; and I0 guarantees appliers cannot split on a
  mastership-carrying commit.
- **The wedged-but-alive master (P6a):** fenced transitively through its
  sole quorum partner installing the commit. I3 makes this hold; I12
  closes the V2-frames escape.
- **The placement-driven master change:** I13 gates it identically to the
  override path — HRW naming a stale-copy node after a death does not
  serve until that node's lineage is genuinely `Full`.
- **Stale reads:** a partitioned M serves stale reads until anti-entropy
  reaches it — today's window. It cannot ack a spend.
- **Quorum floor:** regime values change only via commits clearing the
  peak-anchored quorum; I10(d)/(e) additionally forbid lowering and
  unjustified bumps. A minority remnant cannot move a regime.

### 4.7 Fail-closed engineering invariants

- At RF>1, a key resolving to zero regular replica targets is a hard
  `ERR_REPLICATION_FAILED` + operator metric — never silent `Durable`.
  (I3's swap post-state keeps the promoted shard's replica set non-empty,
  so promotion never manufactures this case.)
- The short-replica-set resolution error is load-bearing for §4.6 and must
  never be relaxed. Cross-referenced in §7.4.
- The `auth.rs` replay-defense table is corrected: sequence dedup does NOT
  cover `FLAG_MIGRATION_BATCH` or `first_sequence == 0` batches; the
  defenses there are the `cluster_key` gate (I8, precise form) and the
  generation guard. Companion test: replay a captured migration-delta
  frame carrying a `Reassign` against a receiver that installed the
  promoting commit; assert wholesale rejection.

### 4.8 Anti-entropy and dissemination (extends P5)

Regimes ride only proof-carrying pulls (`OP_GET_COMMITTED_TOPOLOGY`'s
validated commit — term + voters + digest at ≥ local `committed_term`),
max-merge only, durable (persist-before-adopt). Proof freshness needs no
clock: strict term monotonicity + max-merge + I7 bound every accepted
value. The unproven routing snapshot never carries regime state.

### 4.9 Reassign: close the replay gap on the replication wire

`RedoOp::ReassignV2` carries `prior_utxo_hash`; the wire
`ReplicaOp::Reassign` never got the fix — the receiver reads the live slot
hash at apply time, making the op "reassign whatever is there now", and
two production paths bypass sequence dedup (§4.7). V3 adds
`prior_utxo_hash: [u8; 32]`; the receiver passes it as the expected hash.
Replay then no-ops structurally.

Compensation classes: a compensation rejected by the regime gate is safe
to drop for idempotent monotonic ops (`Spend`, `SetMined`, `Freeze`) but
MUST escalate to a **per-key** resync against the current master for
`Unspend`, `Reassign`, `SetLocked`, `ExpirePreservation` — the same
mechanism as I5.

## 5. Rejected alternatives

### 5.1 Recency-ranked local election (rejected twice: #76, R4)

Observer-dependent local election with an unfenced mis-elect. This design
keeps the sound parts (partition view as input; prefer the complete copy)
and moves the decision into one proposer whose output is quorum-committed,
structurally re-validated by every applier under I0-clean clauses (I10),
and fenced by regime + self-observed lineage.

### 5.2 Consensus log on the data path

Would serialize the hot path through a leader and defeat the 10M+ ops/sec
target. Idempotency provides per-record convergence; consensus is reserved
for rare topology transitions.

### 5.3 Wall-clock leases for master serving

Lease fencing needs bounded clock error; the only clock assumption in the
system is the 5-minute HMAC skew allowance. Regime fencing needs no clock.

### 5.4 Delta-encoded regime trailers (rejected, round 1)

Regime state must be a function of the installed commit, not the sequence
of installed commits — term-skipping nodes diverge and the fence fails
open (BE-F1). Absolute, term-stamped values replace it.

### 5.5 Numeric override caps (rejected, round 2)

A per-commit override count cap blocks the primary use case (one dead
master legitimately overrides ~`NUM_SHARDS/N` shards) while I10(a)–(e)
bound forgery damage more tightly with zero false negatives (BE-N8,
SA-N10-adjacent).

## 6. P6a — Heal-deadline escalation ladder

Replaces the single `AlertAndHold` state. Step 3 mints commits —
consensus-critical.

1. Window 1..N: `AlertAndHold` unchanged.
2. At each expiry: alert severity escalates monotonically; severity resets
   only on progress, never on deadline refresh.
3. **Escalated promotion** — only if a committed **holder** (I3) reports
   `Full` at the current regime: the deterministic proposer proposes the
   override through the normal quorum path. Rails: per-shard promotion
   cooldown ≥ several heal windows; a cluster-wide promotion budget per
   unit time (excess alert-and-holds); progress precondition — no second
   promotion for a shard until the previous one produced a serving `Full`
   master. (These rails also govern §4.4.3(c).) Without them, sustained
   packet loss turns the ladder into a promotion oscillator minting
   legitimate commits. When step 3 cannot escalate because
   `promotion_enabled` is false, the alert names that as the reason —
   otherwise the ladder loops with no diagnosis.
4. **Degraded (`NoFullCopy`)** — reachable holders exist but none reports
   `Full`: alert, hold, never escalate to Lost. All `Subset` triggers are
   reachable from sustained packet loss, so absence-of-`Full` is
   liveness-fragile evidence. Per-shard Degraded identity is
   **admin-token-gated** on `/status` (public shows the count) — the same
   targeting-oracle rule as Lost, and the same applies to §7.4's
   reduced-redundancy list.
5. **Lost** — requires **affirmative evidence**: every committed member
   that holds the shard has affirmatively reported a zero record count /
   null manifest digest for it within the current committed term — absence
   of a report is never evidence (an unreachable holder satisfies nothing)
   — plus operator confirmation that any non-reporting members are
   permanently gone. Report-only (never an input to automatic action);
   public `/status` shows the count, per-shard identity behind the admin
   token. The restore runbook requires fence-before-restore: last-known
   holders verified down/evicted or the restore refuses, and the restored
   copy enters at a fresh regime via the normal commit path. Holders
   merely unreachable ⇒ repair-the-partition situation, not restore.

The same fence-before-remove rule applies at the **other** end of the
composition: `/admin/shrink` must require confirmation that each removed
node is verified down/evicted — not merely unreachable — and must surface
any shard whose **entire holder set** is being removed as a blocking
condition. A shrink that orphans a shard's RF=2 holder pair leaves a
self-consistent partitioned pair that still masters and acks each other's
writes; §6.5's restore rule guards one end of that composition, this
guards the creation of it.

## 7. Approved items (implementation notes)

### 7.1 P6b — throttle condvar (shipped, PR #97)

Lock-free admission + waiter condvar; token drops notify; 250 ms wait
timeout bounds only the epoch-abort re-check.

### 7.2 P5 — commit dissemination (shipped, PR #98)

Proven durable term adopt first; routing snapshot installed only when
backed by the proven term (`routing_snapshot_installable`); `TopologyStale`
catch-up deduplicated by a panic-safe RAII slot; broadcast straggler
retries (200 ms → 3.2 s) run after the proposer's own durable apply.

### 7.3 P2 — local-health-aware failure detection

LHM (saturating 0..8) scales the node's own probe interval/timeout; relays
answer authenticated NACKs; suspicion deadlines shrink with independent
confirmers, floor ≥ 2× the LHM-scaled probe interval. Confirmer counting:
a confirmation counts ONLY when the claimed suspector IS the authenticated
sender of the datagram carrying it; one confirmation per sender max;
relayed suspector ids are convergence gossip with zero deadline effect;
signed attestations out of scope. E-03 and suspect-excluded quorum
counting untouched.

### 7.4 P4 — catch-up streaming + fill-delay

Convergence loop (chunked, per-chunk ACK backpressure, byte budget shared
with the migration throttle) replacing the capped pass. Delta validity:
un-reclaimed redo range AND no in-progress inbound migration/resync.
`migrate_fill_delay` defers fill migrations per departed node. Invariants:
the short-replica-set hard error is load-bearing (a fill-delay window is a
visible write outage, never silent RF=1); fill delay applies only while
≥ RF live holders remain (never the last-copy case); the degraded window
is explicit, bounded, metric'd, identity admin-gated; pinned test — no
client ack without every replica-set ack on every path throughout the
window.

## 8. Verification strategy

Every implementation PR: TDD with event-driven waits; `cargo test --all`;
`cargo clippy --all-targets -- -D warnings`; `cargo fmt --check`; client
crates separately. P1/P6a additionally: each invariant I0–I13 gets a
dedicated named test; the §4.7 captured-frame replay test (I8 precise
form); a final targeted verification review of this revision; and a
cross-subsystem (failover × migration × reverse-heal × shrink ×
backup/restore) review before merge — rounds 1–2 found composition
defects in exactly those seams (holder-exit lineage, restore-over-live-
holders, packet-loss→Lost, handoff-boundary holder sets), confirming the
G8 lesson.

## 9. Review log

- 2026-07-28: Revision 1 drafted.
- 2026-07-29: **Round 1** — bitcoin-expert 18 findings (3 CRITICAL),
  security-auditor 21 findings (2 CRITICAL + 3 P0). Verdict: mechanism
  sound, revision 1 unimplementable. All folded into revision 2.
  Reviewer conflict (delta vs absolute regime encoding) resolved for
  absolute on correctness; recorded in §5.4.
- 2026-07-29: **Round 2** — verification of revision 2.
  - bitcoin-expert: 14/18 CLOSED, 4 PARTIAL, 0 OPEN; 10 new findings
    (N1 regime-array validation, N2 rejected-batch carve-out, N3
    undefined post-override holder set, N4 handoff-boundary holder set,
    N5 positional wire trailers, N6 node-local apply gates =
    topology-fork primitive, N7 placement-path lineage gap, N8 cap blocks
    failover, N9 Lost contradiction, N10 I8 overclaim).
  - security-auditor: 18/21 CLOSED, 2 PARTIAL, 1 resolved-conflict; 12
    new findings (N1 V2 sender opt-out, N2 sticky armed-marker, N3
    downgrade flag, N4 clone carries Full, N5 promotion re-stamp missing,
    N6 proposer not digest-covered, N7 Lost contradiction, N8 fail-open
    self-fence kill, N9 I5/§4.9 granularity mismatch, N10 cap vs
    availability arm, N11 Degraded oracle, N12 rebase scope).
  - All round-2 findings folded into revision 3 (this document): new I0
    (no node-local apply gates), I12 (no sender opt-out), I13
    (transition-scoped gate + promotion re-stamp); I2 holder-set union;
    I3 swap post-state; I4 committed `promotion_enabled`; I5 per-key
    granularity; I7 rebase scope + authorization; I8 precise form; I10
    rewritten (ratchet + bump-justification + digest-covered proposer, no
    numeric cap — §5.5); I11 fail-open ignore; §4.1 wire envelope +
    marker deletion path + self-consuming upgrade flag; §4.2 whole-batch
    failure semantics; §4.3 node_id-bound lineage; §6.4/6.5 affirmative-
    evidence Lost + Degraded oracle gating.
  - Round 3 (final targeted verification) pending.
- 2026-07-29: **Round 3** — targeted verification of revision 3.
  - bitcoin-expert: 7/10 round-2 findings CLOSED, 3 PARTIAL, 0 OPEN.
    5 blockers, all definitional: B1 node-local state inside I10(a)/(e)/(f)
    → the committed-derivation rule; B2 override-map cumulativity
    unstated → full cumulative map + retirement-bumps-regime; B3
    `effective ∪ target` one node too wide → holder set narrowed to
    `target ∪ {effective.master}`; B4 no stamp-refresh on regime advance
    → I2 refresh rule; B5 V3 sender coverage unstated → I12 total-sender
    enumeration + abort-and-re-plan. Must-fix: B6 Subset fences the
    HeldCopy sweep; B7 shrink verified-down. Verdict: mechanism settled;
    revision 4 verifiable in a single targeted pass.
  - security-auditor: 14/14 round-2 findings + both partials CLOSED; no
    new CRITICAL/P0. 5 P1 text fixes: N13 V3-acceptance-is-a-capability;
    N14 stale-binary blackout consequence + `regime_unsupported_member`
    alert; N15 disarm quiescence + marker delete-before-zero ordering;
    N16 boot re-derivation regardless-of-stamp (self-heals the I13ii
    crash window); N17 promotion_enabled proposed only on cluster-wide
    advertised WriteAll evidence. Wording: N18 promotion_enabled
    governance + I6 extension; N19 mismatch-degrades-not-resyncs +
    batched re-stamp. Verdict: implementable once folded.
  - All round-3 items folded into revision 4 (this document): I0 gains
    the committed-derivation definition; I2 narrowed holder set + stamp
    refresh; I4 advert-evidence + governance; I6 promotion_enabled
    bundling ban; I10(f) `proposer ∈ commit.members`; I12 total sender
    coverage + capability-based V3 acceptance; I13 boot re-derivation +
    batched re-stamp; §4.1 enable sequencing / stale-member alert /
    disarm quiescence / marker ordering; §4.3 sweep fence + replacement
    note; §6.3 promotion-disabled diagnosis; §6.5 shrink verified-down.
- Awaiting user build/no-build sign-off; per §8, implementation
  additionally requires the named I0–I13 invariant tests and the
  cross-subsystem review before merge.
