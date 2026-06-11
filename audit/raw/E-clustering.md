# Audit E — Clustering and quorum

Scope: `src/cluster/{swim,membership,coordinator,topology,auth,mod}.rs`, dispatch quorum gate
in `src/server/dispatch.rs`, tests `tests/cluster_*.rs` + `tests/g8_*.rs`, README cluster
section, `phases/09_clustering.md`. Sharding/migration/routing internals deferred to another
agent; cross-cutting interactions noted where relevant.

Method: static read of all sources (no cargo — orchestrator holds the build lock). All
"Reproduction" fields specify experiments rather than runs.

---

### [HIGH] SWIM replay window is keyed by NodeId only — a rebooted node cannot rejoin
**Location:** `src/cluster/swim.rs:401` (`seen_seq: HashMap<NodeId, ReplayWindow>`),
`:765-768` (`check_and_record`), `:272-311` (window logic), `:648-663` (dead-node GC),
`:411-423` (`next_outbound_seq` resets to 1 every boot; not persisted).

**What's wrong:** The F-G8-003 replay defense tracks `(highest_seq, 256-bit bitmap)` per peer
keyed purely by `NodeId`. The per-sender outbound counter `next_outbound_seq` starts at `1` on
every process start and is never persisted. When a peer reboots it re-sends from seq=1 while the
receiver still holds the *old run's* `highest` (potentially in the thousands/millions for a
long-lived node). In `check_and_record`, those low seqs are rejected: seqs at-or-below positions
already marked in the bitmap return `false` (already-seen), and the doc comment's claim that "the
`sender_incarnation` bump separates new boots from prior runs" is **false for the replay window**
— incarnation is consulted for membership state (`mark_alive`) but never in `check_and_record`.
The check runs *before* `mark_alive`, so the rebooted node's JOIN/PING/ACK are dropped at the seq
gate and never reach membership. The window is also never cleared: the 24h dead-node GC
(`swim.rs:648-663`) removes peers from `peer_addrs`/`swim_peer_addrs` but **not** from `seen_seq`
(contradicting the doc at `:398-400`). Effect: a rebooted node is invisible to every peer that
remembers its old seq until its counter climbs back past the stale `highest` (≈`old_highest ×
probe_interval`, which for a production node is effectively forever) or 24h GC elapses. This
applies to all clustered deployments — the seq check at `:765` is unconditional and does not
require `cluster_secret`.

**Why it matters:** Node restart is the single most common cluster operation (deploys, crashes,
rolling upgrades). A node that reboots silently fails to rejoin; peers keep it Dead, its writes
are seq-dropped, and the cluster runs degraded/under-quorum until 24h passes. This is a
liveness/availability failure that masquerades as a healthy security control.

**Reproduction:** Unit-level in `swim.rs` tests: build receiver `B`, feed it 200 signed messages
from `A` (seq 1..200) so `B.seen_seq[A].highest == 200`. Construct a *fresh* `SwimRunner` for `A`
(simulating reboot: `next_outbound_seq == 1`, higher `persisted_incarnation`), encode a JOIN, feed
to `B.handle_message_for_test`. Assert the returned events are empty (message dropped) and `B`
never learns `A` — the bug. The existing `cluster_swim::dead_node_restarts_with_new_incarnation`
passes only because the first instance is short-lived (`highest` ≈ tens) and `WAIT_CEILING` is 30s,
long enough for the restarted node's seq to overtake; raise the first instance's lifetime (or its
message count) and it will fail.

**Suggested fix:** Key the window by `(NodeId, incarnation)` (or reset/replace the `ReplayWindow`
when a strictly-higher `sender_incarnation` is observed for a peer — incarnation is already
authenticated inside the HMAC envelope, so it cannot be forged). Reset on incarnation advance is
the minimal change: in `handle_message`, before `check_and_record`, if `sender_incarnation` >
last-seen incarnation for that peer, replace `seen_seq[sender]` with a fresh `ReplayWindow`. Also
remove `seen_seq` entries in the dead-node GC to honor the documented bound.

---

### [HIGH] Topology commit-apply path ignores cluster_id — split-brain merge bypasses every propose-side guard
**Location:** `src/cluster/topology.rs:1098-1153` (`handle_commit`), called unchecked from
`src/server/dispatch.rs:1065` (OP_TOPOLOGY_COMMIT handler) and `src/cluster/coordinator.rs:1548`
/`:1601` (catch-up path).

**What's wrong:** `handle_commit` validates only three things: `commit.term > committed`, a
*self-consistent* digest (`compute_digest(term, commit.cluster_id, members)` — computed from the
commit's own `cluster_id`, so a foreign id still matches its own digest), and
`has_quorum_voter_proof` (voters ⊇ majority of the commit's *own* member list). It never compares
`commit.cluster_id` against `self.cluster_id`, and it never runs `membership_change_is_safe` /
`ever_seen_check`. All the split-brain defenses (F-G8-001/002, P1.1 cluster_id) live on the
**propose/vote** path (`on_membership_changed`, `handle_propose`) — the commit-apply path is wide
open. Two clusters that share a `cluster_secret` (the exact misconfiguration / known-secret threat
that `cluster_id` was introduced to defend) but hold distinct `cluster_id`s: if cluster B's
proposer broadcasts a fresh commit at a term higher than A's committed term to an A-node (A-nodes
appear in B's `node_addrs` once SWIM gossip leaks across), A's dispatch calls `handle_commit`,
which accepts it — A abandons its own `{1,2,3}` topology and adopts B's `{4,5,6}`. The
shard table is then activated against nodes that were never in A's quorum: divergence.

**Why it matters:** This is the split-brain-heal hole the `cluster_id` mechanism was specifically
built to close, left open on the one code path that actually mutates committed state. Mitigations
exist but are partial: the catch-up *fetch* loop filters peers to `committed_members`
(`coordinator.rs:1523-1525`), so A won't *pull* from B — but the *push* path (B broadcasting
OP_TOPOLOGY_COMMIT) has no such filter, and a node with empty `committed_members` (fresh boot)
bypasses the catch-up filter too. Rated HIGH rather than CRITICAL only because it requires a shared
secret plus a freshly-broadcast higher-term commit; under the documented "authenticated peers are
trusted" threat model (coordinator.rs:6661-6689) it is argued acceptable, but that argument
contradicts the existence of `cluster_id` and the g8 tests that assert distinct ids must refuse
each other.

**Reproduction:** In `topology.rs` tests: authority A with `set_cluster_id(A)`,
`handle_commit` a `{1,2,3}` term-5 commit. Build a `TopologyCommit` for `{4,5,6}` term-7 with
`cluster_id = B` and `voters = {4,5,6}` (self-consistent digest, valid quorum proof). Call
`a.handle_commit(&commit_B)`. Current code returns `Some(7)` and `a.committed_members()` becomes
`{4,5,6}` — the bug. Expected: `None` (rejected on cluster_id mismatch).

**Suggested fix:** In `handle_commit`, after the digest check, reject when both
`self.cluster_id()` and `commit.cluster_id` are set and differ. Optionally also run
`is_safe_membership_change(committed_members, commit.members)` to catch same-id split-brain
(non-monotonic) merges, mirroring `handle_propose`.

---

### [MEDIUM] Minority can accept writes during the SWIM suspicion window (bounded staleness)
**Location:** `src/server/dispatch.rs:2490-2510` (`check_quorum`),
`src/cluster/coordinator.rs:6420-6457` (`alive_node_count`), `src/cluster/membership.rs:88-98,210-235`
(suspect retained in alive view), `coordinator.rs:1375-1378` (NodeLeft removes from `node_addrs`).

**What's wrong:** `check_quorum` is evaluated per-write (good — no cached verdict), reading
`alive_node_count()` and `peak_cluster_size()` live. But `alive_node_count` counts committed
members present in `node_addrs`, and `node_addrs` only loses a peer on the `NodeLeft` event, which
fires only after Suspect → Dead expiry (`membership.rs` keeps Suspect nodes in the alive view by
design, E-03). So between the moment a partition cuts a minority off and the moment its SWIM
suspicion timeout elapses (`suspicion_timeout`, default multi-second, plus probe/indirect-probe
backoff), the minority still counts the now-unreachable peers as alive and `check_quorum` passes.
During that window an isolated minority master applies the local write before replication.

**Why it matters:** This is an inherent SWIM detection-latency window, not unbounded, and
replication provides a second gate (WriteMajority can't collect acks from unreachable replicas, so
the *client* write fails) — but the local engine state is mutated before that failure is known, so
a minority can briefly diverge locally even though the durable/acked result is rejected. The
window scales with `suspicion_timeout` + indirect-probe exponential backoff (`swim.rs:194-203`,
up to 16× base), which can be tens of seconds.

**Reproduction:** 3-node proxied cluster (as in `cluster_partition::partitioned_minority_never_self_activates_topology`).
Immediately after `net.isolate(node1, …)` and before `alive_node_count()` drops to 1, send an
`OP_CREATE_BATCH` to node1 and assert it is *not* applied locally. Current behavior: it passes
`check_quorum` until suspicion expiry. The existing test only checks the *post-expiry* steady state.

**Suggested fix:** Document the window as accepted, or tighten by gating writes on
`alive_node_count` derived from *directly-acked* liveness within one probe interval rather than the
suspect-inclusive view. At minimum add a test asserting the in-window behavior so the tradeoff is
explicit.

---

### [MEDIUM] Inter-node TCP frame auth has no nonce/sequence replay protection — only a 5-minute timestamp window
**Location:** `src/cluster/auth.rs:106-157` (`verify`), `:356-481` (`verify_frame_streaming_with_now`),
`MAX_CLOCK_SKEW = 5min` (`:46`).

**What's wrong:** SWIM UDP messages get per-sender seq replay defense (`swim.rs` `seen_seq`), but
the TCP frame path (`verify_frame` / `verify_frame_streaming`, used for OP_TOPOLOGY_*,
OP_REPLICA_BATCH, OP_GET_COMMITTED_TOPOLOGY, etc.) authenticates with HMAC + a timestamp-skew
check only. There is no nonce or monotonic counter, so any captured valid frame can be replayed
verbatim within the 5-minute window and will pass auth. Application-level monotonic checks absorb
most of the damage (a replayed OP_TOPOLOGY_COMMIT with `term <= committed` is dropped by
`handle_commit`; replica batches carry idempotent sequence numbers), so this is defense-in-depth
rather than a direct break — but the auth layer itself provides no replay guarantee, and any
future opcode lacking its own idempotency would inherit the hole.

**Why it matters:** A on-path attacker who captures one authenticated mutation frame can replay it
for 5 minutes. Today's opcodes are mostly idempotent so impact is limited, but the property is
fragile and undocumented as a layer responsibility.

**Reproduction:** Capture a signed OP_REPLICA_BATCH frame (or build one via `sign_frame`), feed it
twice to the receiver within `MAX_CLOCK_SKEW`; both pass `verify_frame`. Assert at the auth layer
that the second is rejected (it is not, today).

**Suggested fix:** Either document that TCP-frame replay defense is delegated to per-opcode
monotonic checks (and audit every mutating opcode for idempotency), or add a per-connection
monotonic nonce covered by the HMAC.

---

### [MEDIUM] Clock skew beyond 5 minutes silently partitions the whole cluster
**Location:** `src/cluster/auth.rs:46` (`MAX_CLOCK_SKEW = 5min`), `:149-155`, `:471-478`.

**What's wrong:** Both SWIM and TCP auth reject any message whose embedded wall-clock timestamp
differs from local `now` by > 5 minutes. If two nodes' clocks drift apart by more than 5 minutes
(NTP outage, VM pause/migrate, bad RTC), every message between them fails auth and they cannot
form or maintain membership — the cluster silently partitions along clock-skew lines. There is no
metric or log distinguishing "skew rejection" from "wrong secret" at the cluster level (auth
returns a generic drop), making this very hard to diagnose.

**Why it matters:** Time-based assumption with a hard failure mode and poor observability. A
correlated NTP failure across a fleet could take down the cluster with no obvious cause.

**Reproduction:** `auth::verify_with_now(key, &signed, ts + 6*60*1000)` returns
`Err(InvalidData "stale timestamp")` (already covered by `hmac_with_old_timestamp_is_rejected`).
The cluster-level effect is untested: run two SWIM runners with `verify`'d transport and a
6-minute `now` offset; assert (currently) they never converge.

**Suggested fix:** Emit a distinct metric/log on skew-window rejection vs HMAC mismatch so
operators can tell clock drift from auth misconfig. Consider widening the window or making it
configurable for high-skew environments.

---

### [LOW] `seen_seq` is unbounded — never GC'd even at 24h dead-node forget
**Location:** `src/cluster/swim.rs:401`, `:648-663` (GC removes `peer_addrs`/`swim_peer_addrs`
only), doc claim at `:398-400`.

**What's wrong:** The per-peer replay-window map grows once per distinct `NodeId` ever heard from
and is never pruned (the dead-node reaper omits it). For a long-lived cluster with churning
NodeIds this is a slow memory leak. Each entry is ~40 bytes, so it is small, but it is unbounded
and contradicts the in-code documentation.

**Why it matters:** Minor leak; primarily a correctness/documentation discrepancy that compounds
the HIGH reboot bug above (same root cause: the window is never reset for a peer).

**Reproduction:** Feed `handle_message` signed messages from 10,000 distinct NodeIds; observe
`seen_seq.len() == 10_000` with no path to shrink it.

**Suggested fix:** Remove `seen_seq` entries in the dead-node GC alongside the address maps.

---

### [LOW] No test for cluster formation with a dead seed in the seed list
**Location:** seed retry logic `src/cluster/swim.rs:499-507,613-642`; tests `tests/cluster_swim.rs`,
`tests/cluster_tcp.rs`.

**What's wrong:** The seed-join code tolerates an unreachable seed by design (`socket.send_to`
errors are discarded, retries cycle all seeds with exponential backoff). But no test exercises a
seed list containing a dead/unreachable address to confirm the cluster still forms via the live
seeds. The checklist item "seed node list with a dead seed: cluster still forms" is unproven.

**Why it matters:** Common real-world config (static seed list, one seed down during a deploy).
The behavior looks correct by inspection but is untested.

**Reproduction:** Start a 3-node SWIM cluster where each non-bootstrap node's seed list is
`[<live seed port>, <unused/closed port>]`; assert all nodes still converge to 3 members within
`WAIT_CEILING`.

**Suggested fix:** Add the test to `cluster_swim.rs`.

---

## Checklist disposition

- ✅ **Peak cluster size persisted & read back across restarts** — `persist_cluster_state`
  (`coordinator.rs:5386`, `[peak:8][epoch:8]` + atomic rename + fsync) and the richer
  `persist_topology_state`/`.multinode` marker (`:5410,5461`); loaded via `load_startup_topology_state`
  (`:5508`) at `bin/server.rs:843`. Tests: `coordinator.rs:9073 peak_cluster_size_persists_and_loads`,
  `dispatch.rs:11223 deleted_topo_file_prevents_single_node_bootstrap` (marker survives `.topo` deletion).
- ✅ **Isolated 1-of-3 remnant rejects writes with NO_QUORUM** — `check_quorum` (`dispatch.rs:2490`)
  evaluated **per-write** (not cached), `quorum_needed = (peak/2)+1`. Test
  `cluster_partition.rs:486 partitioned_minority_never_self_activates_topology` asserts ERR_NO_QUORUM(15).
  ⚠️ Staleness caveat: see MEDIUM (suspicion-window).
- ✅ **Fresh bootstrap not stuck; "never had peers" vs "lost peers" distinguished** —
  `check_quorum` returns `None` for `peak <= 1` (`dispatch.rs:2498`); `load_cluster_state` defaults to
  `(1,0)` when absent (`:5533`); the `.multinode` marker forces `peak >= 2` only when prior multi-node
  evidence exists (`:5440-5472`). Exact condition verified.
- ⚠️ **SWIM probe/suspect/dead transitions, incarnation, refutation** — membership state machine is
  correct and well-tested (`membership.rs` tests cover stale-incarnation guards, direct-vs-gossip
  refutation, flapping, exactly-once death). BUT the **replay window reboot bug** (HIGH) breaks
  rejoin-after-restart, which is a transition-correctness failure under the realistic message-loss +
  reboot case.
- ⚠️ **HMAC auth drops bad/missing-HMAC before parsing; replay** — wrong-secret and missing/truncated
  HMAC correctly rejected before parse (`swim.rs:731-738`, `auth.rs` tests). SWIM has seq replay defense
  (but see HIGH reboot bug). TCP-frame path has **no nonce — timestamp-window only** (MEDIUM); replayed
  valid frames within 5 min pass auth.
- ❌ **Two clusters learning about each other (split-brain heal) — defined behavior, heal not just
  detection** — propose/vote paths correctly refuse (g8 tests + `cluster_edge_cases.rs:1751`), behavior
  is "refuse + pin + require operator." BUT the **commit-apply path ignores cluster_id** (HIGH): a
  foreign higher-term commit is adopted, and no test covers `handle_commit` cluster_id rejection. Coverage
  and the control itself are incomplete.
- ✅ **cluster_id enforcement** — `tests/g8_cluster_id.rs` covers propose-level rejection of distinct
  ids and scale-up with matching ids; digest mixes `cluster_id` (`topology.rs:98`). ⚠️ but commit path
  unguarded (see HIGH above).
- ⚠️ **Membership change during in-flight writes** — `check_quorum` is per-write and
  `node_addrs`/`peak` are read live, so a mid-flight change is observed; but the suspicion-window
  staleness (MEDIUM) means an in-window write on a freshly-isolated minority is applied locally before
  the membership view catches up. Not tested at the in-window granularity.
- ⚠️ **Time-based assumptions / clock skew** — 5-minute HMAC skew window: skew > 5 min silently
  partitions the cluster with poor observability (MEDIUM). Timestamp is HMAC-covered (good, tamper-proof)
  but availability-fragile.
- ❌ **Quorum uses peak-size not current-size; minority can't proceed under every interleaving** —
  peak-derived quorum is correctly threaded through write gate (`check_quorum`), activation
  (`activation_quorum_needed`, `topology.rs:669`), fallback proposer, retry, and catch-up; peak is
  monotonic (`fetch_max`) and never lowered even on graceful drain (`coordinator.rs:6603-6617`).
  The propose/activation interleavings are well-covered by `g8_split_brain.rs`. NOT fully closed: the
  commit-apply path (HIGH) lets a minority/foreign commit install a topology without re-deriving the
  peak quorum, so "writes can't proceed in a minority partition under *every* interleaving" does not
  hold once a foreign or stale higher-term commit is accepted.

Tally: ✅ 4 · ⚠️ 5 · ❌ 2 (checklist items). Findings: 2 HIGH, 3 MEDIUM, 2 LOW.
