# Audit Category E — Clustering and Quorum

HEAD: branch `main` (1e5659b). Scope: `src/cluster/{coordinator,membership,swim,topology,auth}.rs`
plus call sites in `src/server/mod.rs`, `src/protocol/opcodes.rs`.

Fully read: membership.rs (1148 lines incl. tests); auth.rs:1-449 (all sign/verify);
the inter-node auth gate in server/mod.rs:732-1010; SWIM verify-before-parse
swim.rs:718-726; topology.rs quorum path (on_membership_changed :851-923,
handle_propose :929-965, handle_vote :1011-1023, membership_change_is_safe
:703-739, is_safe_membership_change :542-551); coordinator quorum/peak
(coordinator.rs:647, 738, 1370-1405, 2540-2680, 5367-5527).

## CRITICAL FINDING

### E-01 (CRITICAL) — Isolated minority remnant self-commits a single-node topology (split-brain). Quorum threshold uses the *shrunken proposed* membership, not the persisted peak.

Locations:
- `src/cluster/topology.rs:911` — `let quorum_needed = (members.len() / 2) + 1;`
  where `members` is the **SWIM-observed alive set passed in**, NOT the persisted
  peak. Mirror at handle_vote path: `topology.rs:1023` `accept_count >= proposal.quorum_needed`.
- `src/cluster/topology.rs:542-551` — `is_safe_membership_change` treats a **pure
  subset** (members departed) as SAFE.
- `src/cluster/topology.rs:703-739` — `membership_change_is_safe` only rejects
  cluster_id mismatch, non-monotonic change, or unseen NodeIds. A pure shrink to
  `[self]` passes all three.
- `src/cluster/coordinator.rs:738` — `peak_size_event.fetch_max(members.len())`:
  peak is tracked and persisted (coordinator.rs:5367 `persist_cluster_state`,
  topology.rs:346/382 `peak_cluster_size`) but is **never** used as the quorum
  divisor.
- `src/cluster/coordinator.rs:1386-1405` — single-node fast-path: after
  `on_membership_changed` returns the proposal, `handle_vote(self_vote)` is called
  and, because `quorum_needed == 1`, immediately returns a commit and the node
  activates the topology unilaterally.

What's wrong: take a healthy 3-node cluster `{1,2,3}` with committed
membership `{1,2,3}` and persisted `peak_cluster_size == 3`. Partition node 1 from
2 and 3. SWIM on node 1 expires 2 and 3 to Dead and fires
`MembershipChanged([1])`. The coordinator routes this to
`on_membership_changed([1])` (topology.rs:851). The split-brain guard
(`membership_change_is_safe`, :867/703) sees a **pure subset** of the committed
set with all members previously seen → returns SAFE (not blocked). Node 1 is the
lowest NodeId so it proposes (`:897-899`). `quorum_needed = (1/2)+1 = 1`
(`:911`). The self-vote alone satisfies quorum (`handle_vote` :1011-1023,
`accept_count(1) >= quorum_needed(1)`). Node 1 commits a NEW single-node topology
and `activate_topology` makes node 1 master of **all** shards
(coordinator.rs:1395-1405). Meanwhile nodes 2 and 3 (a real majority) continue
serving their shards. Two disjoint masters now own the same UTXOs.

Why it matters: this is textbook split-brain. The minority side accepts writes
(spends/creates) it must not, because it falsely believes it is the whole cluster.
On partition heal, the two histories conflict → double-spend / vanished-UTXO /
silent corruption. This is the exact money-loss failure the audit charter calls
out. The persisted peak — the one piece of state that exists specifically to
prevent this ("for quorum safety across restarts", coordinator.rs:629-630) — is
written and read back but never consulted when computing whether a self-vote is a
quorum. CRITICAL.

Reproduction: integration test (does not currently exist):
1. Form a 3-node cluster, let it commit membership `{1,2,3}` (peak persisted = 3).
2. Drop all traffic between node 1 and nodes 2,3 (partition).
3. Wait > `swim_suspicion_timeout_ms` so node 1 marks 2,3 Dead.
4. Assert node 1 does NOT activate a new single-node topology and that a write to
   node 1 returns `ERR_NO_QUORUM` (15) / `ERR_CLUSTER_NOT_READY` (25).
   Today it WILL activate (quorum_needed becomes 1) and accept the write.
Unit-level confirmation that the threshold is wrong: `quorum_reached_produces_commit`
(topology.rs:1366) and `quorum_not_reached_without_enough_votes` (:1386) both
construct the authority with a *fixed* member list and check `(len/2)+1` — they
never exercise the shrink-from-peak case, so the bug is uncovered by tests.

Suggested fix: the quorum divisor for activating a NEW topology must be derived
from the persisted peak (last committed majority), not the live shrunken set:
`quorum_needed = (peak_cluster_size / 2) + 1`. A minority proposing `[self]` then
needs `(3/2)+1 = 2` votes and cannot self-commit. A graceful, *quorum-approved*
shrink (drain) is still possible because the departing-node commit is itself
ratified by the old majority before peak is allowed to decrease; peak should only
decrease through a committed term, never through a SWIM-observed loss. Alternative
/ additional defence: gate `activate_topology` on
`alive_count >= (peak_cluster_size/2)+1` before committing. Note the membership
self-inclusion (membership.rs:88) means an isolated node reports `alive_count==1`,
which is correctly below `(3/2)+1==2` — so the alive-vs-peak comparison is the
clean guard; it is simply absent from the activation path.

## MEDIUM / LOW findings

### E-02 (MEDIUM) — OP_HEARTBEAT (250) and OP_REPLICA_ACK (241) excluded from inter-node HMAC auth
Locations:
- `src/protocol/opcodes.rs:488-503` — `is_inter_node_auth_opcode` omits both.
- `src/server/mod.rs:732` — `is_inter_node_op = is_inter_node_auth_opcode(op)`,
  so heartbeat/replica-ack bypass `verify_signed_body_streaming`
  (server/mod.rs:809-819).

What's wrong: even with a `cluster_secret` configured, a peer that knows only node
addresses can inject forged `OP_HEARTBEAT` frames over TCP. Heartbeat influences
liveness/membership accounting; `OP_REPLICA_ACK` influences replica-durability
accounting (ack_policy / degraded-durability). SWIM UDP membership IS authed
(swim.rs:718-726, drops bad-HMAC before parse), so this is specifically a
TCP-path liveness/durability spoofing gap. Not a direct double-spend (heartbeat
cannot activate a topology or spend a UTXO; transitions remain incarnation-guarded
at membership.rs:125,208,238) — hence MEDIUM. Note: combined with E-01, forged
heartbeats that keep a partitioned node looking alive could *delay* the very
suspicion that triggers the bad single-node proposal; they do not prevent it.

Suggested fix: add OP_HEARTBEAT and OP_REPLICA_ACK to is_inter_node_auth_opcode,
or document the actual threat model (the comment at opcodes.rs implies a "see
below" rationale that does not exist).

### E-03 (LOW) — `mark_suspect` mutates the alive set but emits no `MembershipChanged`
Locations: `src/cluster/membership.rs:203-225` (rebuild at :217, only NodeSuspect
at :218); contrast mark_dead :253-256, mark_alive :167,:183; asserted by
full_lifecycle_event_sequence :893-901.

For the suspicion-timeout window (~5s), a consumer polling `alive_count()` sees
the suspect excluded while a consumer reacting to the `MembershipChanged` event
stream still treats it as alive — two divergent "who is alive" views. Low impact
on the activation path (which uses the event stream → `fetch_max(peak)`), but a
latent footgun and, given E-01, any inconsistency in the alive-count signal that
feeds a quorum guard is worth eliminating. Fix: emit `MembershipChanged` on
suspect, or keep Suspect nodes in `cached_alive` until Dead.

## Verified-OK checklist

- **Peak persisted + read back across restart:** YES (but unused for quorum — see
  E-01). `persist_cluster_state` (coordinator.rs:5367-5382) writes `[peak:8][epoch:8]`
  to a `.cluster.tmp`, `sync_all`, atomic `rename`. `load_cluster_state`
  (:5514-5527) reads it back, clamps peak to `>=1`, seeds
  `ClusterCoordinator`’s `initial_peak` → `peak_size` atomic (:647). Round-trip and
  crash-safety of the WRITE are sound; the defect is that the value is never
  consulted in the quorum decision.
- **Inter-node TCP frames authenticated (topology propose/vote/commit, replica
  batch, migration complete):** YES, verify-before-decode, fail-closed under
  strict_auth (server/mod.rs:732-819, opcodes.rs:489-501). Tests
  unsigned_topology_frame_rejected / unsigned_migration_frame_rejected /
  unsigned_inter_node_frame_rejected (server/mod.rs:1004-1011) assert
  ERR_CLUSTER_AUTH_FAILED. EXCEPTION: heartbeat + replica-ack (E-02).
- **SWIM drops bad-HMAC BEFORE parse, constant-time, replay-bounded:** YES.
  swim.rs:718-726 calls `auth::verify` and `return vec![]` on error, before the
  27-byte header parse (:728). `auth::verify` (auth.rs:135) uses constant-time
  `verify_slice` BEFORE reading the timestamp, then enforces ±5min skew
  (auth.rs:149-155). Tested: verify_rejects_tampered/wrong_key,
  hmac_with_old/future_timestamp_rejected, hmac_timestamp_is_covered_by_tag,
  streaming_verify_rejects_stale_timestamp.
- **Self-message rejection:** YES (membership.rs:119-121; tests self_message_ignored
  :802, self_node_not_tracked_as_member :448).
- **Incarnation monotonicity / stale rejection / direct-vs-gossip suspicion
  clearing / idempotent death / post-GC lower-inc rebirth block:** YES, all
  genuinely tested (membership.rs:125,208,238,138-139,232-267,62-63; tests
  stale_suspect_ignored, stale_dead_ignored, same_incarnation_gossip_does_not_clear,
  death_event_fires_exactly_once, dead_node_reborn_cannot_use_lower_incarnation).
- **Split-brain *merge* (two clusters that each advanced) heal:** DEFENDED on the
  merge side. `membership_change_is_safe` (topology.rs:703-739) rejects non-monotonic
  changes, cluster_id mismatch, and unseen NodeIds; follower-side guard in
  handle_propose (:943-962) prevents laundering a merged set through a single round.
  This is the OPPOSITE direction from E-01 (which is a pure-shrink, not a merge) and
  IS handled.

## Not exhaustively traced (no finding)
- Replay WITHIN the 5-min skew window relies on the window only (no nonce cache);
  incarnation numbers and record-layer idempotency blunt impact. Defense-in-depth
  opportunity, not a finding.
- Dead-seed bootstrap: a fresh node with a dead seed falls into single-node
  bootstrap (coordinator.rs:1776-1779, peak starts at 1) — correct (peak==1 means
  quorum_needed==1 legitimately). The E-01 bug is specifically about a node that
  ONCE had peak>1 and then shrank.
