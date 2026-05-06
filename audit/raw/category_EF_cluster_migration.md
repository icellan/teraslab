# Audit: Category E (Cluster + Quorum) and F (Sharding + Migration)

**Scope.** TeraSlab's cluster control plane: SWIM membership, HMAC authentication,
quorum-committed topology authority, deterministic shard table, REDIRECT /
MIGRATION_IN_PROGRESS / NO_QUORUM dispatch handling, the inbound/outbound
migration manager, and the data migration pipeline.

**Files audited.**

- `src/cluster/mod.rs`
- `src/cluster/coordinator.rs` (~9.5 kloc — the bulk of the cluster machinery)
- `src/cluster/membership.rs`
- `src/cluster/swim.rs`
- `src/cluster/auth.rs` (HMAC-SHA256 + 5-minute clock-skew window)
- `src/cluster/topology.rs` (propose / vote / commit + persisted state)
- `src/cluster/shards.rs` (4096-shard table, handoff state machine)
- `src/cluster/routing.rs` (`RoutingInfo` wire format)
- `src/cluster/migration.rs` (active / inbound / fenced / dual-write tracking)
- `src/index/migration.rs` (snapshot import/export, **unrelated** to runtime
  shard migration; covered briefly because it shares the name)
- `src/server/dispatch.rs` (`check_quorum`, `check_shard_ownership`,
  `OP_MIGRATION_COMPLETE`, `OP_TOPOLOGY_PROPOSE/VOTE/COMMIT`, etc.)
- `src/bin/server.rs` (cluster bootstrap path)
- `src/config.rs` (cluster_secret validation)
- Tests: `tests/cluster_swim.rs`, `tests/cluster_tcp.rs`,
  `tests/cluster_edge_cases.rs`

The audit follows the question list in the brief. Findings reference exact
file:line locations in the present working tree (`main`, with the
TERANODE_PRODUCTION_READINESS_GAPS branch's modifications staged). Severities
are CRITICAL / HIGH / MEDIUM / LOW with a brief justification per finding.

---

## Overview / executive summary

The cluster surface area is large (~17 kloc audited). Several pieces are well
designed: the HMAC primitive itself (RFC-4231 vector tested, constant-time
compare, timestamp covered by tag, 5-minute skew window), the propose/vote/commit
state machine (proper digest validation, persisted voted_term before reply,
duplicate-commit rejection), and the shard handoff state machine
(ServingCurrent / Copying / CommitReady / ServingNew with rollback). The 4096-shard
mask `0x0FFF` is correct (12 bits, 4096 = NUM_SHARDS), tested by
`shard_for_key_distribution`.

There are nonetheless **multiple high-severity findings** that affect both
safety and availability:

1. **HMAC is applied only to SWIM UDP — not to inter-node TCP frames**
   (topology proposals/votes/commits, replica batches, migration data,
   `OP_MIGRATION_COMPLETE`, partition-version reports, drain commits). Any
   network attacker reachable on the binary protocol port can spoof a
   topology commit and rewrite the shard map. (CRITICAL — EF-01.)
2. **`alive_node_count` excludes self**, so a healthy 3-node cluster that
   loses one peer can be incorrectly tipped into NO_QUORUM even though
   self+1=2 is exactly majority. (HIGH — EF-02.)
3. **Quorum check uses persisted `peak` strictly even on first boot**, but
   `peak <= 1 ⇒ skip quorum` is a single-node escape hatch that lets an
   isolated remnant of a 3-node cluster bootstrap as a fresh single-node
   cluster *if its `*.topo` file is lost or deleted* — a recovery footgun
   without a tripwire. (MEDIUM — EF-04.)
4. **REDIRECT has no hop count / TTL / loop counter** — a client following
   a stale partition map can be redirected indefinitely between two
   serially-stale masters. (HIGH — EF-09.)
5. **`OP_MIGRATION_COMPLETE` is unauthenticated**: any TCP peer can mark an
   inbound shard "complete" and lift the new master's MIGRATION_IN_PROGRESS
   gate, even though a manifest hash is required for record_count > 0.
   (HIGH — EF-12.)
6. **Cluster_secret is mandatory only when RF>1**: with `replication_factor
   = 1`, `cluster_secret` may be empty/missing; SWIM and inter-node TCP run
   unauthenticated. There is no audit for whether the operator has
   knowingly opted in. (MEDIUM — EF-08.)
7. **Migration plan when both old and new master are alive uses the OLD
   master as source even though no record may exist there yet for newly
   created keys**, but the dual-write window protects this. The fence is
   held until the FULL `OP_MIGRATION_COMPLETE` succeeds with manifest
   verification — that part is robust. (Verified safe — no finding.)

Several scenarios in the brief are **untested in the test suite**:

- Isolated 1-node remnant of a 3-node cluster rejecting writes (EF-03)
- Wrong-secret / missing-HMAC / replayed-HMAC (EF-06 — only unit tests of
  the primitive exist, no end-to-end integration tests)
- Two clusters that learn about each other (split-brain heal — EF-10)
- Bootstrapping cluster does not get stuck — quorum logic distinguishes
  "never had peers" from "lost peers" (only partially tested — EF-04)

The code paths intended to handle these scenarios have implementation logic,
but the absence of integration coverage means latent regressions are likely
to ship undetected.

The shard mask, deterministic round-robin allocation, monotonic shard table
version, and core handoff state machine are correct.

---

## Findings

### EF-01: Inter-node TCP frames are unauthenticated; HMAC is SWIM-only (CRITICAL)

**Subcategory:** E (cluster)
**Location:** `src/cluster/swim.rs:433-441,844-848,881-884` (SWIM applies HMAC),
`src/cluster/coordinator.rs:2589-2605` (`send_topology_frame` — no HMAC),
`src/server/dispatch.rs:811-931` (`OP_TOPOLOGY_PROPOSE` /
`OP_TOPOLOGY_VOTE` / `OP_TOPOLOGY_COMMIT` — no HMAC verification),
`src/cluster/auth.rs:1-19` (the design doc claims HMAC covers "SWIM UDP
messages and inter-node TCP frames" but only SWIM uses it).

**What:** The HMAC primitive in `src/cluster/auth.rs` is correct and the SWIM
runner gates message *parsing* on `verify` (line 433-441 in `swim.rs`).
However the inter-node TCP control plane is plain `RequestFrame` / `ResponseFrame`:

```rust
// src/cluster/coordinator.rs:2589-2605
fn send_topology_frame(addr: SocketAddr, op_code: u16, payload: &[u8]) -> ... {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(500))?;
    ...
    let request = RequestFrame { request_id: 0, op_code, flags: 0, payload };
    let response = exchange_frame(&mut stream, &request)?;  // <-- no auth
    ...
}
```

The dispatcher (`src/server/dispatch.rs:811-931`) accepts `OP_TOPOLOGY_PROPOSE`,
`OP_TOPOLOGY_VOTE`, and `OP_TOPOLOGY_COMMIT` from any TCP peer with no HMAC
check or peer-id assertion. The same is true for the migration / replication /
admin / partition-version paths (`OP_REPLICA_BATCH`, `OP_MIGRATION_COMPLETE`,
`OP_MIGRATION_BATCH_COMPLETE`, `OP_PARTITION_VERSION_REPORT`).

The `auth.rs` doc string at line 4-7 explicitly states: *"all SWIM UDP messages
and inter-node TCP frames carry an 8-byte millisecond Unix timestamp plus a
32-byte HMAC tag"* — but the TCP code path was never wired up to the auth
module (`grep "auth::sign\|auth::verify"` in `src/server/`,
`src/replication/`, and `src/cluster/coordinator.rs` returns nothing).

**Why it matters:** A network attacker reachable on the binary protocol port
(intentionally exposed in cluster mode by `enable_remote_bind`) can:

- Send a forged `OP_TOPOLOGY_COMMIT` with arbitrary members and a recomputed
  digest. `topology.rs:569-599` validates only that the term is strictly
  greater than the local `committed_term` and that the digest matches the
  members. There is no proposer-identity check beyond accepting whoever
  shows up. After ack, `signal_topology_committed` activates a new shard
  table on every node that received the forged frame.
- Send forged `OP_REPLICA_BATCH` frames to any node, applying writes
  with a falsified `cluster_key`. Receiver gating (`local_cluster_key`)
  rejects mismatches but the attacker can read the value from a single
  honest probe.
- Send a forged `OP_MIGRATION_COMPLETE` to clear the inbound write fence
  on a target node before data has finished arriving.

The cluster `cluster_secret` provides zero defense against any TCP-side
attack despite the project documentation suggesting otherwise.

**Reproduction:** Start a 3-node cluster with `cluster_secret = "..."`. From
a 4th machine that can reach node 1's binary port, build a `RequestFrame`
with `op_code = OP_TOPOLOGY_COMMIT` and a payload containing a higher term
+ a member list of attacker's choice + recomputed SHA-256 digest. Send it.
The receiver will apply `handle_commit` and `signal_topology_committed`,
silently rotating shard ownership. No SWIM-level signal flows so no
suspicion is raised.

**Suggested fix:** Either (a) wire the `auth::sign`/`auth::verify` calls
into `send_topology_frame` and into the dispatcher's `OP_TOPOLOGY_*`,
`OP_REPLICA_BATCH`, `OP_MIGRATION_*`, `OP_PARTITION_VERSION_REPORT`
handlers — sign on send, verify before parse, drop on mismatch — or
(b) explicitly require mTLS for inter-node TCP and surface a config
error if `enable_remote_bind=true && cluster_secret_set && !mtls`.
The current state is the worst of both worlds: an HMAC implementation
that gives false assurance.

---

### EF-02: `alive_node_count` excludes self — false NO_QUORUM in healthy clusters (HIGH)

**Subcategory:** E (cluster)
**Location:** `src/cluster/coordinator.rs:5860-5871`,
`src/server/dispatch.rs:2084-2104` (the consumer).

**What:** `alive_node_count` counts members whose addresses are present in
`node_addrs`, but `node_addrs` is **never populated for self** in production:

```rust
// src/cluster/swim.rs:454-472
if sender_id == self.config.self_id {
    return vec![];  // ignore self loopback
}
...
self.peer_addrs.lock().unwrap().insert(sender_id, sender_tcp_addr);
```

```rust
// src/cluster/coordinator.rs:1226-1228
ClusterEvent::NodeJoined(node, addr) => {
    node_addrs.write().unwrap().insert(*node, *addr);   // <-- never self
}
```

The unit test `alive_node_count_only_counts_live_committed_members`
(`coordinator.rs:8412`) deliberately puts self in the `live_nodes` slice it
passes to the test helper, so the test passes — but production code paths
do not insert self.

In a healthy 3-node cluster {1,2,3}, self=1, both peers reachable:
- `node_addrs = {2:addr, 3:addr}` (peer-only)
- `alive_node_count = 2`
- `peak = 3`, `quorum_needed = 3/2+1 = 2`, `2 >= 2` → quorum met (✓ accidentally correct)

In a 3-node cluster, lose 1 peer (node 3 dies):
- `node_addrs = {2:addr}` (1 peer)
- `alive_node_count = 1`
- `quorum_needed = 2`, `1 < 2` → **NO_QUORUM**

But the *true* alive count is 2 (self + node 2), which IS majority. Writes
should be allowed. The check incorrectly rejects.

In a 5-node cluster, lose 2 peers:
- `node_addrs = {2,3}` (2 peers reachable)
- `alive_node_count = 2`
- `quorum_needed = 5/2+1 = 3`, `2 < 3` → **NO_QUORUM**

True alive = 3 (self + 2 peers) which IS majority. Again falsely rejected.

**Why it matters:** This is a classic off-by-one in availability. A 3-node
cluster cannot tolerate the loss of one node when it should — the surviving
two-node majority is the entire point of running 3 nodes. Same for 5-node
losing 2.

Note: this only manifests when `peak > 1`. Single-node clusters bypass quorum
via the `if peak <= 1 { return None }` early-return in `check_quorum`.

**Reproduction:** Spin up a 3-node cluster, persist all three nodes' state,
kill node 3, wait for SWIM to fire `NodeLeft` (suspicion timeout default
~5s). Issue any mutation against node 1 → returns `ERR_NO_QUORUM`. There is
no integration test for this in the audited test files
(`grep -n "NO_QUORUM\|no_quorum"` in `tests/cluster_*.rs` finds none).

**Suggested fix:** Change `alive_node_count` to `addrs.len() + 1` in the
committed-members branch when `committed_members.contains(self_id)`, OR
explicitly add the self entry to `node_addrs` at coordinator-startup and
keep it there. The latter also fixes EF-05 (partition map missing self).

---

### EF-03: No integration coverage for "isolated 1-node remnant rejects writes" (HIGH)

**Subcategory:** E (cluster)
**Location:** `tests/cluster_tcp.rs`, `tests/cluster_edge_cases.rs`,
`tests/cluster_swim.rs`.

**What:** The brief asks: *"Isolated 1-node remnant of a 3-node cluster rejects
writes with NO_QUORUM."* Searching the test files
(`grep -n "NO_QUORUM\|no_quorum\|isolated\|split_brain"` over all three
cluster tests) finds zero hits. The implementation in `dispatch.rs:2084-2104`
has the right shape — `peak > 1 && alive < quorum_needed → ERR_NO_QUORUM` —
but is exercised only by indirect single-node cases.

The closest test, `kill_node_detection_affected_shards`
(`tests/cluster_tcp.rs:1227`), kills a node and then *pings* the survivor;
it does not attempt a mutation and does not assert on `ERR_NO_QUORUM`. So
this safety property is not regression-protected.

Combined with EF-02 (the off-by-one in `alive_node_count`), an integration
test would expose both issues.

**Why it matters:** Quorum-rejection of an isolated remnant is the entire
basis of the project's split-brain protection. The implementation that
delivers it is unverified end-to-end.

**Suggested fix:** Add a multi-node integration test that:
1. Starts 3 nodes, waits for stable membership.
2. Kills nodes 2 and 3 (or partitions them off).
3. Waits past `swim_suspicion_timeout` so node 1 declares them dead.
4. Sends `OP_CREATE_BATCH` to node 1 → asserts response carries
   `ERR_NO_QUORUM`.
5. As a control: starts a fresh single-node cluster (peak=1) and confirms
   the same op succeeds.

---

### EF-04: Recovery footgun — losing the `*.topo` file lets a remnant re-bootstrap as single-node (MEDIUM)

**Subcategory:** E (cluster)
**Location:** `src/cluster/coordinator.rs:5025-5063`,
`src/server/dispatch.rs:2092-2094`.

**What:** Quorum logic distinguishes "never had peers" (peak=1, skip quorum)
from "lost peers" (peak>1, require majority). This is correct. But the peak
is read from disk via `load_topology_state`:

```rust
// src/cluster/coordinator.rs:5025-5037
pub fn load_topology_state(path: &Path) -> PersistedTopologyState {
    match std::fs::read(path) {
        Ok(data) => PersistedTopologyState::deserialize(&data),
        _ => PersistedTopologyState { peak_cluster_size: 1, ... },  // <-- DEFAULT
    }
}
```

If an operator manually deletes the cluster state file (e.g. troubleshooting,
moving the data dir, reformatting), peak resets to 1, the
`if peak <= 1 { return None }` shortcut in `check_quorum` triggers, and the
remnant accepts writes as a fresh single-node cluster — **silently abandoning
the split-brain protection**.

There is no warning, no tripwire, and no marker file outside the
single-purpose `*.topo` to cross-check that the node "knew" about peers.
The redo log, the engine state, and the device may all carry residue from
the multi-node era; only this one file controls the safety gate.

**Why it matters:** Operational accidents (rsyncing without `*.topo`, the
file being on a separate volume that fails to mount, a bad shutdown that
truncates the file pre-rename) are realistic. The recovery default of "peak
= 1" trades availability for safety — the wrong direction.

**Reproduction:** Run a 3-node cluster, populate it. Stop node 1. Delete
`<data>/cluster.state.topo` (or whatever the resolved path is). Restart
node 1 with the *same* SWIM port but the seed list pointing nowhere. Send
a write. It will succeed locally even though nodes 2 and 3 may still be
holding majority elsewhere.

**Suggested fix:** Either (a) change the file-missing default to
`peak_cluster_size = 0` and treat 0 as "fresh node, no decision yet — wait
for SWIM convergence before accepting writes"; (b) write a marker file at
first multi-node membership change that, if present, forces peak >= 2 even
if `*.topo` is missing; (c) refuse to start a node whose data device
contains records but whose `*.topo` is missing, forcing the operator to
explicitly opt in via a `--allow-bootstrap` flag.

The brief noted `peak.max(1)` is applied in `PersistedTopologyState::deserialize`
(`src/cluster/topology.rs:270,281,291,299,415`) — the issue is not the
deserializer's clamp, it is the *default when the file is absent*.

---

### EF-05: Partition map omits self — clients can be told the cluster has zero nodes (MEDIUM)

**Subcategory:** E (cluster)
**Location:** `src/cluster/coordinator.rs:5792-5870`,
`src/cluster/routing.rs:67-93`.

**What:** `encode_partition_map` builds the wire payload from `node_addrs`,
which (per EF-02) does NOT contain self in production. Consequently, in a
single-node cluster the partition map advertises `node_count = 0`. In a
3-node cluster, only the 2 peers appear. Clients that implement
"if `node_count == 0`, refuse to talk to this node" (a reasonable defense)
will fail to connect to a healthy single-node cluster.

The decoded shard assignments still reference NodeIds that the client
cannot resolve to an address — the `(0, NodeId(self_id), addr)` triple is
missing. `RoutingInfo::decode` (`routing.rs:98-159`) silently accepts a
node list that doesn't cover all referenced node IDs.

**Why it matters:** Client routing relies on the partition map being a
*complete* description of the cluster. A self-omitting map breaks routing
in subtle ways: requests sent to node 2 for a shard mastered by self
will REDIRECT, but the redirect target's address may not appear in
the partition map at all (only the self-known node addresses do), making
the client either retry blindly or escalate to "all nodes unhealthy".

**Reproduction:** Single-node cluster, client connects, fetches partition
map via `OP_GET_PARTITION_MAP` → `node_count = 0`, all 4096 shards point
at NodeId(self_id), but no entry maps self_id to an address.

**Suggested fix:** Insert `self_id → self_addr` into `nodes` before
encoding in `encode_partition_map`; alternatively (per EF-02 fix), keep
self in `node_addrs` from coordinator-init.

---

### EF-06: HMAC has unit tests but no integration tests (MEDIUM)

**Subcategory:** E (cluster)
**Location:** `src/cluster/auth.rs:251-381` (unit tests),
`tests/cluster_swim.rs:97-100,519-521,529-532` (`cluster_secret: None`),
`tests/cluster_tcp.rs:104` (`cluster_secret: None`).

**What:** The auth.rs unit tests cover:

- RFC 4231 test case 2 (line 274) ✓
- Tampered payload rejected (line 293) ✓
- Wrong key rejected (line 301) ✓
- Truncated message rejected (line 307) ✓
- Old timestamp rejected (line 329) ✓
- Future timestamp rejected (line 348) ✓
- Tampered timestamp rejected on tag mismatch (line 362) ✓

These are good. But there are NO integration tests where two SWIM runners
with different secrets attempt to converge — the `tests/cluster_*.rs` files
all pass `cluster_secret: None`. The brief explicitly asks for:

- **Wrong secret:** Two runners with different secrets must NOT discover
  each other. Untested.
- **Missing HMAC** (peer with secret receives a peer with secret=None
  message): Untested. Looking at `swim.rs:434` the receiving side drops
  unsigned messages when its own secret is set, but the symmetric case
  (signed sender → unsigned receiver) is not covered: the receiver
  parses the appended timestamp+tag bytes as if they were membership
  updates, which `parse_ping_req_target` and friends MAY accept as
  garbage. The `let data = if let Some(ref secret)` branch at
  `swim.rs:434-441` only verifies when *self* has a secret; it does not
  check whether the message has the SIGNED_SUFFIX format.
- **Replayed HMAC:** The 5-minute skew window is correct, but a replay
  *within* 5 minutes is fully accepted. There is no nonce / sequence
  number bound to the HMAC. An attacker can replay a captured PING_REQ
  to force a relay action up to 5 minutes after capture.

**Why it matters:** The HMAC primitive is sound in isolation but the wire
contract has gaps. An asymmetric secret deployment (mid-rotation) will
silently accept attacker frames, and replay protection is loose.

**Suggested fix:** Add integration tests for asymmetric-secret SWIM
non-convergence and replay rejection. Consider adding an in-message nonce
or tying the HMAC input to the per-peer last-seen incarnation.

---

### EF-07: SWIM "garbage-collect dead nodes" cliff at 1 hour permits stale-node forgery (LOW)

**Subcategory:** E (cluster)
**Location:** `src/cluster/swim.rs:399-418`,
`src/cluster/membership.rs:317` (`forget_dead_older_than`).

**What:** Every probe interval, dead nodes older than 1 hour are forgotten:

```rust
// src/cluster/swim.rs:402-406
let forgotten = self
    .membership
    .lock()
    .unwrap()
    .forget_dead_older_than(Duration::from_secs(3600));
```

After the 1-hour window, the dead node is fully evicted from membership,
peer_addrs, and swim_peer_addrs. If that NodeId rejoins, it goes through
the same fresh-join path as a brand-new node, allowing an attacker who
knows the cluster_secret to spoof an old NodeId. This isn't catastrophic
(the rejoined node still has to run topology proposals and gain quorum)
but it interacts badly with EF-01 in cleartext-TCP topology paths.

**Why it matters:** Node-ID identity is implicit (no certificate). The
1-hour forget-dead window means an operator decommissioning a node and
later recycling its NodeId (e.g. for a different physical machine) gets
a 1-hour grace; outside that window there is no memory of the old node.

**Suggested fix:** Either bump the window much higher (24h+) or persist
a "previously-seen NodeIds with last incarnation" set so the same NodeId
cannot be reborn at a lower incarnation than the historic peak.

---

### EF-08: Cluster_secret only enforced for RF>1 (MEDIUM)

**Subcategory:** E (cluster)
**Location:** `src/config.rs:665-676`.

**What:** Validation rule:

```rust
// src/config.rs:665-676
if self.replication_factor > 1
    && self.cluster_secret.as_ref().map(|s| s.is_empty()).unwrap_or(true)
{
    return Err(ConfigError::ClusterSecretRequired { rf: self.replication_factor });
}
```

So a `replication_factor = 1` cluster can run with `cluster_secret = ""` /
unset. SWIM and inter-node TCP run unauthenticated. Yet a single-node
cluster can still grow into a multi-node cluster (a second node joins the
SWIM seed) and at that point the lack of secret becomes a problem only
*for new joiners* — the existing single-node config is treated as already
valid.

The brief asks: *"Cluster_secret = "" — is unauthenticated cluster actually
allowed in production? Should it be?"* The answer is: only when `RF=1`,
which is itself unsafe in production for any data the user values, and the
rule does not distinguish "RF=1 by intent" from "RF=1 because the operator
hasn't configured replicas yet".

**Why it matters:** "Set RF later when adding replicas" is a footgun: by
that point a node may have been running unauthenticated SWIM for hours.

**Suggested fix:** Make `cluster_secret` strictly mandatory whenever cluster
mode is active (i.e. whenever the SWIM port is bound), regardless of RF.
If the operator wants single-node, they can run `RF=1` with a secret too —
no harm done, and the configuration is forward-compatible.

---

### EF-09: REDIRECT has no hop count, TTL, or loop counter — clients can chase stale routes forever (HIGH)

**Subcategory:** F (sharding/migration)
**Location:** `src/server/dispatch.rs:2287-2311,4283-4307,4763-4779`,
`src/cluster/coordinator.rs:5598-5620`.

**What:** When a request arrives for a shard not mastered by the receiving
node, the dispatcher returns `ERR_REDIRECT` plus the address bytes of the
believed-master from the *local* shard table:

```rust
// src/server/dispatch.rs:2287-2311
crate::cluster::coordinator::MasterQueryResult::No => {
    ...
    let route = cluster.route(&key);
    let error_data = match route {
        RouteDecision::RedirectTo { node, .. } => {
            match cluster.node_addr(&node) {
                Some(addr) => addr.to_string().into_bytes(),
                None => Vec::new(),
            }
        }
        ...
    };
    Some(BatchItemError {
        item_index, error_code: ERR_REDIRECT, error_data,
    })
}
```

The wire format is just `[error_code:2][addr_bytes:N]`. There is **no TTL,
no hop counter, no client-supplied chain**. If node A's shard table is
behind, A redirects to B; if B's shard table is also behind, B redirects
to C; if C's shard table is behind, it can redirect back to A. The client
receives a fresh REDIRECT each time and has no means to detect a loop.

`coordinator.rs:5598-5620` even *encodes* a `shard_table_version` in the
`RouteDecision::RedirectTo`, but the dispatcher discards it (the byte
field encoded into `error_data` is just the address string). So the
client cannot use the version as a tiebreaker either.

When the local shard table is older than the committed term (line 5602-5607),
the dispatcher returns `RedirectTo { node: NodeId(0) }` — but the bytes
encoded are `cluster.node_addr(&NodeId(0))` which is `None`, producing an
empty error_data. The client receives `ERR_REDIRECT` with no address, with
no way to discriminate this from a network glitch or a normal redirect to
an unreachable node. (The intent according to the comment is to signal
"refetch the partition map" — but the wire encoding doesn't carry that
signal.)

**Why it matters:** A misbehaving cluster mid-topology-change can have
multiple nodes with disjoint shard tables. A client that follows redirects
without bounds will spin until human intervention. Combined with retries
on `ERR_MIGRATION_IN_PROGRESS`, the retry storm can be self-amplifying.

**Reproduction:** Stand up a 3-node cluster, force a partition that
causes node 1 and node 2 to commit different topology terms (this requires
defeating the quorum gate, but the inability to detect can be tested
through the unit-level helpers). Drive a client at node 1 with a key
whose shard is mastered by node 2 in node 1's view, but by node 3 in
node 2's view; if node 3's view points back at node 1, the client loops.

**Suggested fix:** Add a hop counter to the request frame
(`flags`-shifted-bits or a header byte). Reject redirects whose
hop_count exceeds N (suggest 4 — covers worst-case
node1→node2→node3→committed-master). Alternatively, encode the
`shard_table_version` from `RouteDecision::RedirectTo` into `error_data`
so the client can detect "the master they pointed me at is at the same or
older version" and terminate.

---

### EF-10: Split-brain heal — two clusters that learn about each other have no rejection path (HIGH)

**Subcategory:** E (cluster)
**Location:** `src/cluster/topology.rs:482-532` (`handle_propose`),
`src/cluster/membership.rs:108-183` (`mark_alive`).

**What:** The topology authority has cluster-formation-recovery logic
(line 511-518) that lets a single-node cluster accept a multi-node proposal
that includes self. There is NO symmetric protection for two formerly-
independent multi-node clusters that suddenly see each other:

```rust
// topology.rs:511-518
if !accepted && valid_digest && propose.members.len() > 1 {
    let committed_members = self.committed_members.read().unwrap();
    let our_cluster_is_single_node = committed > 0 && committed_members.len() <= 1;
    let proposal_subsumes_us = propose.members.contains(&self.self_id);
    if our_cluster_is_single_node && proposal_subsumes_us && propose.term > voted {
        accepted = true;
    }
}
```

The clause only fires when *our* cluster is single-node. Two healthy 3-node
clusters that gossip their members to each other (e.g. a previously-
partitioned WAN heal) will:

1. SWIM sees the foreign nodes and emits `MembershipChanged([1..6])`.
2. The lowest-NodeId member of the merged set proposes term=N+1 covering
   all 6.
3. Each side has `committed_members.len() = 3`, not `<= 1`, so the
   formation-recovery clause does not apply.
4. Each side compares `propose.term > committed && propose.term > voted` —
   if the foreign cluster's term is higher, it gets accepted, and one
   side adopts the merged 6-node view. The other side might already
   have committed its own term=N+1 and reject this. The cluster splits
   into two committed states.

There is no detection of "I and the foreign nodes have non-overlapping
committed_members; this is a brain-split, not a normal membership change".
The shard table is recomputed deterministically over the new 6-node set,
which masters every shard at exactly one node — so writes to a single
key from each side hit the same master. But during the transition window,
each side's *previous* shard table (committed_members ⊊ {1..6}) is the
authoritative one (per `effective_assignment`), and writes can land on a
node that the other side does not yet know is master. Records can diverge
silently if the merged cluster never converges to one term.

**Why it matters:** WAN partitions and split-brain heal scenarios are real
operational events in any geographically distributed BSV node deployment.
The current code has no explicit defense.

**Reproduction:** Stand up two physically separate 3-node clusters with the
same `cluster_secret` (the relevant scenario — same ops team, same
secret). Connect their SWIM ports through a previously-blocked link.
Observe topology proposals from each side; observe at least one node
on each side accepts the higher term, while the other side has already
committed its own. Diff the resulting shard tables.

**Suggested fix:** When SWIM emits `MembershipChanged` with a member list
that is NOT a strict superset of the local `committed_members`, refuse to
propose unless the operator has set a `--allow-merge` flag. Alternatively,
add a "cluster ID" field (separate from cluster_secret) that all nodes in
the same cluster share; reject any SWIM gossip from peers reporting a
different cluster_id.

---

### EF-11: Shard mask `0x0FFF` is correct (verified — no finding)

**Subcategory:** F (sharding)
**Location:** `src/cluster/shards.rs:9-10,313-317`,
`src/cluster/coordinator.rs:6484`.

**What:** The shard hash uses 12 bits of the txid for 4096 shards:

```rust
// src/cluster/shards.rs:10,313-317
pub const NUM_SHARDS: usize = 4096;
...
pub fn shard_for_key(key: &TxKey) -> u16 {
    let h = u16::from_le_bytes([key.txid[0], key.txid[1]]);
    h & 0x0FFF
}
```

`0x0FFF == 4095 == NUM_SHARDS - 1` — correct mask. The other use at
`coordinator.rs:6484` is consistent. The distribution test at
`shards.rs:561-580` verifies shards 0..NUM_SHARDS are populated within
50% of the expected uniform distribution over 100k random keys. The
bounds match: `shard < NUM_SHARDS`. ✓

The `0x0FFF` is also defended by `assignments` being indexed by `shard as
usize` directly without bounds checks (`shards.rs:317-329`), which would
panic on an off-by-one — the test coverage indirectly defends this.

**No finding.**

---

### EF-12: `OP_MIGRATION_COMPLETE` is unauthenticated and trusts the source's claims (HIGH)

**Subcategory:** F (migration)
**Location:** `src/server/dispatch.rs:471-810`.

**What:** Once an attacker can send TCP frames (EF-01), the
`OP_MIGRATION_COMPLETE` handler accepts a wire payload that controls when
the receiver lifts its inbound write fence:

```rust
// src/server/dispatch.rs:471-505 (excerpt)
OP_MIGRATION_COMPLETE => {
    let shard = request.request_id as u16;
    let expected_records = ...;
    let migration_epoch = ...;
    let source_manifest: Option<[u8; 32]> = ...;
    let (source_entries, completion_from_node) = ...;
    ...
}
```

Mitigation present:

- Lines 581-589 reject `record_count > 0` with no manifest (good — H3
  safety requirement).
- Lines 686-714 verify the manifest hash by re-hashing local index
  state when the source-supplied hash is set.
- Lines 549-562 reject very stale topology epochs (`migration_epoch < current - 2`).
- Lines 595-615 prune any local key not in the source's exact-entry
  manifest (when the latter is supplied with full coverage).

But:

- A `record_count = 0` completion bypasses manifest verification:
  `no_data_completion = true` at line 567-571, then `count_ok = true` at
  line 628-634, and the receiver calls `mark_inbound_complete_*` at
  line 724-735 — clearing the write fence with no proof at all.
- The handler does not authenticate the *sender*. Any TCP peer can issue
  a zero-record `OP_MIGRATION_COMPLETE` for any shard.
- The `completion_from_node` field is read from the payload (line 534-540)
  with no cross-check that the connecting peer's identity matches.
- The `migration_epoch + 2` slack means an attacker can rewind a shard's
  ownership by up to 2 epochs (in practice this often spans an entire
  data migration cycle).

**Why it matters:** A network attacker can mark every shard as "migration
complete" for the new master, lifting the inbound write fence before any
data has actually arrived. The receiver then accepts writes against an
empty shard, producing data loss the moment a real `OP_REPLICA_BATCH`
catches up and finds the keys already gone (or worse, replaced by attacker
writes).

**Reproduction:** From any TCP-reachable host, build a `RequestFrame`
with `op_code = OP_MIGRATION_COMPLETE`, `request_id = <shard_id>`, and a
zero-byte payload (or a 24-byte payload with `expected_records = 0`,
`fence_seq = 0`, `migration_epoch = current_epoch`). Send to the new
master. The fence clears; subsequent writes succeed against an empty
shard.

**Suggested fix:** (a) require `OP_MIGRATION_COMPLETE` to be HMAC-signed;
(b) cross-check the peer-declared `from_node` against the
SWIM/`peer_addrs` view of who is connected; (c) reject zero-record
completions from peers that the receiver's `MigrationManager` does not
list as a valid `inbound_migrations` source for that shard.

---

### EF-13: Migration writes during inbound migration return MIGRATION_IN_PROGRESS for ALL write opcodes (verified — no finding)

**Subcategory:** F (migration)
**Location:** `src/server/dispatch.rs:2229-2282`.

**What:** The brief asks: *"Writes to a shard with pending inbound migration
return MIGRATION_IN_PROGRESS on EVERY write op (not just spend)."*
`check_shard_ownership` is called from every mutation handler:

- `OP_SPEND_BATCH` → `handle_spend_batch` → `check_shard_ownership` (line 2360)
- `OP_UNSPEND_BATCH` → `handle_unspend_batch` → `check_shard_ownership`
- `OP_SET_MINED_BATCH` → `handle_set_mined_batch` → `check_shard_ownership`
- `OP_CREATE_BATCH` → `handle_create_batch` → `check_shard_ownership`
- ...all mutation handlers check ownership.

Inside `check_shard_ownership` at line 2238-2266, the `Yes` branch checks
`has_pending_inbound` and `is_shard_write_fenced` and returns
`ERR_MIGRATION_IN_PROGRESS` for both. The Transitioning branch at line
2268-2282 returns the same. So writes are uniformly blocked.

`OP_DELETE_BATCH` is also gated through this path
(`handle_delete_batch`).

**No finding.** The implementation is uniform. It is worth noting that
**read** operations (`OP_GET_BATCH`, `OP_GET_SPEND_BATCH`) ALSO return
`ERR_MIGRATION_IN_PROGRESS` when the new master hasn't received its data
yet (`dispatch.rs:4310-4321,4748-4762`), which the brief's question about
read timeouts asks about — see EF-14.

---

### EF-14: Reads on the new master before migration completes return immediately with no wait — clients must poll (LOW)

**Subcategory:** F (migration)
**Location:** `src/server/dispatch.rs:4310-4321,4747-4762`.

**What:** The brief asks: *"Reads on the new master before migration
completes — what's the timeout? What does the client see if it expires?"*

There is **no server-side wait or timeout**. The dispatcher returns
`ERR_MIGRATION_IN_PROGRESS` immediately:

```rust
// src/server/dispatch.rs:4310-4321
if is_master && engine.read_metadata(&key).is_err() && cluster.has_pending_inbound(&key) {
    let shard = ...;
    tracing::debug!(shard, "dispatch: read still waiting for inbound migration");
    results.push(WireGetResult {
        status: ERR_MIGRATION_IN_PROGRESS as u8,
        data: vec![],
    });
    continue;
}
```

The client sees the per-item error and must retry. There is no
server-imposed deadline; if migration never completes (e.g. the source
crashed), the client sees ERR_MIGRATION_IN_PROGRESS forever.

**Why it matters:** This is a documented design choice (avoid parking
threads behind migration progress) — see the comment at line 4309-4311.
But the client SDK is then responsible for backoff + give-up, and the
brief explicitly requested verification of the failure mode. There is
no test covering "node never finishes migration → client timeout".

**Suggested fix:** Document the client SDK contract somewhere visible
(currently only the inline comment exists). Optionally surface a metric
`migration_inbound_pending_seconds` that operators can alert on when
shards stay pending too long.

---

### EF-15: Migration interrupted by node crash — restored outbound state is marked Failed and replanned (verified — partial finding)

**Subcategory:** F (migration)
**Location:** `src/cluster/coordinator.rs:6186-6231`,
`src/cluster/migration.rs:769-790` (`mark_failed`).

**What:** The brief asks: *"Migration interrupted by node crash: no
records lost, no records duplicated, no records on both old and new
master after recovery."*

Implementation flow on restart:

1. `restore_outbound_state` (line 6186-6231) reads the persisted
   outbound state, marks every restored task as `Failed`,
   calls `cleanup_completed`, and sets a `startup_reactivation_needed`
   flag.
2. The event loop sees the flag and re-activates topology after a
   5-second cool-down (line 882-883).
3. The new activation builds a fresh migration plan via
   `ShardTable::migration_plan` and `replica_migration_plan`, ignoring
   the previously-failed tasks (they're cleared by `take_failed_tasks`
   at line 1232).
4. The shard table's per-shard handoff state is re-driven. Shards that
   had been in `Copying` or `CommitReady` are rolled back to the old
   master via `rollback_shard` (`shards.rs:241-272`), which restores
   `prev_assignments`.

Inbound state restore (`restore_inbound_state` at line 6162-6178) keeps
shards blocked until either a fresh migration completes them or the
`clear_stale_inbound` path (every 5s, 30s threshold — line 833-848)
forcibly clears them.

So far this prevents data loss/duplication assuming:

- The source survives the crash (or another replica took over).
- The receiver's persisted inbound state survives.
- The 30-second `clear_stale_inbound` threshold doesn't fire prematurely
  on a slow but progressing migration.

The 30-second threshold is a concern: a large shard (millions of
records) on a saturated network can take longer than 30 seconds. The
clear is gated by `mgr.active_count() == 0 && pending_handoff_count() == 0`
at line 835-838 (`clear_settled_inbound` branch), but the
`else { mgr.clear_stale_inbound(Duration::from_secs(30)) }` branch fires
based purely on age, which can race with a slow migration.

**Reproduction:** Drive a slow large-shard migration with throttled
network (e.g. tc qdisc 1 MiB/s, shard with ~50 MiB of records). The
source migration thread keeps making progress for >30s. If the receiver's
migration_manager has no `active` outbound for the shard *and* zero
pending handoffs locally (e.g. the receiver has no inbound work other
than this one shard), the `clear_stale_inbound` may evict the entry
before the source's `OP_MIGRATION_COMPLETE` arrives. The receiver lifts
the fence; subsequent ops can succeed against partial data.

**Why it matters:** Borderline; the 30-second threshold is generous for
typical migrations but not for backfill of a fully populated shard on a
slow link.

**Suggested fix:** Make `clear_stale_inbound` time-out a function of
`migration_pool_size * migration_batch_size * record_size /
known_throughput`, OR plumb in the source's last-progress timestamp
(via piggyback on `OP_REPLICA_BATCH`) and only evict if the source has
not made progress in 30s.

---

### EF-16: Round-robin shard assignment is deterministic given same node set (verified — no finding)

**Subcategory:** F (sharding)
**Location:** `src/cluster/shards.rs:103-134`,
`tests/cluster_edge_cases.rs:339-362` (regression test).

**What:** `compute_with_epoch` requires the caller to pass already-sorted
members; round-robin `members[shard % n]` produces a deterministic
master-to-shard mapping. The
`shard_table_deterministic_across_100_configurations` test covers
n=1..20, rf=1..min(n,4) and asserts pairwise equality across two
independent computations.

`compute_same_on_different_nodes` (`shards.rs:594-607`) verifies the same.
The version is set by the caller via `compute_with_epoch(epoch)` (the
authority's monotonic counter), avoiding the legacy hash-based version
that depended on member order. Production callers always pass
sort-canonical member lists — `topology.rs:60-67` sorts before computing
the digest, and `restore_outbound`/`activate_topology` pass already-sorted
sets.

**No finding.** The deterministic invariant is verified.

---

### EF-17: `migration_pool_size` and `migration_batch_size` actually affect parallelism (verified — no finding)

**Subcategory:** F (migration)
**Location:** `src/cluster/coordinator.rs:556-557,2074+,3753+`,
`src/config.rs:378-443`.

**What:** Configuration:

```rust
// src/config.rs:378-443
pub migration_pool_size: usize,    // default 128
pub migration_batch_size: usize,   // default 500
```

Plumbed through `ClusterConfig` (`coordinator.rs:442-444`), clamped to
`>= 1` (line 556-557), then passed to
`run_migration_tasks_with_global_limit` (line 2074), which spawns up to
`migration_pool_size` worker threads, each processing up to
`migration_batch_size` keys per outbound TCP frame (`migrate_single_shard`
at line 3753).

Both values are consumed in production code paths
(`grep -n "migration_pool_size\|migration_batch_size"` in
`src/cluster/coordinator.rs` shows ~30 references each).

**No finding.** The knobs are wired up.

---

### EF-18: Drain (`/admin/drain/N`) — only drains the LOCAL node (verified — no finding, but operator-visible quirk)

**Subcategory:** F (migration)
**Location:** `src/server/http.rs:1126-1151`,
`src/cluster/coordinator.rs:6011-6062`.

**What:** The `/admin/drain/{node_id}` endpoint enforces that the requested
`node_id` matches the local `self_id`:

```rust
// src/server/http.rs:1131-1148
if cluster.self_id().0 == node_id {
    cluster.quiesce();
    (StatusCode::OK, format!("drain initiated for node {node_id}"))
} else {
    (StatusCode::BAD_REQUEST,
     format!("can only drain local node ({}), use --addr to target node {node_id} directly", ...))
}
```

`quiesce` (line 6011-6062) computes a new topology *excluding self*,
applies the commit locally, and broadcasts `OP_TOPOLOGY_COMMIT` to all
peers. This causes the cluster to re-elect masters away from self, which
implicitly migrates data away. It does NOT block until migration completes —
the operator must poll `/admin/migration/status` to confirm zero active
migrations before stopping the binary.

**Reproduction:** `PUT /admin/drain/<self_id>` returns immediately with
"drain initiated"; `GET /admin/migration/status` shortly afterward shows
`active_count > 0`. There is no `/admin/drain/<self_id>?wait=true`.

**Why it matters:** The brief asks: *"Drain (`/admin/drain/N`) actually
drains before declaring done."* It does NOT. The HTTP response says
"drain initiated" and 200 OK, but the operator must externally verify
completion. This is a UX quirk rather than a bug, but worth flagging
because operators may shut down the node prematurely.

**Suggested fix:** Either rename the endpoint to `/admin/drain/initiate`,
or accept a `?wait_seconds=N` parameter that polls until
`active_count + inbound_pending == 0`.

---

### EF-19: Shard table version is monotonic, but partition-map `nodes` and `shard_table.version` can disagree (LOW)

**Subcategory:** F (sharding/migration)
**Location:** `src/cluster/coordinator.rs:5778-5784,5792-5870`.

**What:** `shard_table_version` deliberately returns
`topology_authority.committed_term()` (NOT `table.version`):

```rust
// src/cluster/coordinator.rs:5778-5784
pub fn shard_table_version(&self) -> u64 {
    self.topology_authority.committed_term()
}
```

But `encode_partition_map` (line 5793-5803) writes `table.version` into
the wire payload, which can differ from `committed_term` during the
window between commit-apply and event-loop activation. The doc comment
at line 5778-5783 explicitly notes this: *"all nodes that committed the
same topology term will report the same version. Using table.version
would cause disagreement during the brief window between commit and
event-loop activation."*

The brief asks: *"Shard table version number — monotonic? Conflicts
resolved how?"* The version IS monotonic per node (each membership change
calls `compute_with_epoch(... new_term)`), but two nodes that committed
term T but where one has activated T and the other is mid-activation
will report different `partition_map_version` values to the same client.

**Why it matters:** Clients caching the partition map use
`shard_table_version` to detect staleness. A client could get version
T from node A and version T-1 from node B, both responding to the same
`OP_GET_PARTITION_MAP`, even though both nodes committed T. The client
(reasonably) assumes node B has an older view and sticks with A's data —
which is correct in this case but only by accident.

**Suggested fix:** Use `committed_term` consistently in the encoded
partition map. The `table.version` lag is a private implementation
concern.

---

### EF-20: SWIM `MAX_MSG_SIZE = 4096` is hard-coded; large piggyback can be silently truncated (LOW)

**Subcategory:** E (cluster)
**Location:** `src/cluster/swim.rs:30,289-305,887-959`.

**What:** SWIM messages are bounded:

```rust
// src/cluster/swim.rs:30
const MAX_MSG_SIZE: usize = 4096;
```

`collect_member_updates` (line 887-959) caps piggyback entries at 20
(line 924), which is reasonable. But each entry is variable-length
(includes two address strings — TCP and SWIM — each of which can be
IPv6 like `[::1]:65535` ≈ 21 bytes). Worst case: 20 * (8 + 1 + 8 + 2 +
40 + 2 + 40) ≈ 2020 bytes for piggyback alone, plus 19 + addr_len for
the header, plus 8 for the committed-term suffix, plus 40 for HMAC.
Total ~2090 bytes — well under 4096.

The receive buffer (line 289) is `[0u8; MAX_MSG_SIZE]` and `recv_from`
truncates silently to that buffer size — no `EMSGSIZE` is reported.
With IPv6, an attacker-crafted payload with 20 long-address entries
plus piggyback abuse could approach the cap, but the 20-entry hard cap
on the encoder side prevents this in practice.

**Why it matters:** The bound is fine today. If a future change raises
the per-entry limit (e.g. adds a node-tag string) without increasing
the buffer, message truncation becomes a silent failure mode.

**Suggested fix:** Add a `debug_assert!(buf.len() <= MAX_MSG_SIZE)`
around `socket.send_to(...)` calls in `swim.rs`, plus a runtime warning
log when the encoder exceeds 80% of the cap.

---

### EF-21: `mark_inbound_complete` accepts shard completion from any source — no source identity required (MEDIUM)

**Subcategory:** F (migration)
**Location:** `src/cluster/migration.rs:577-616`,
`src/server/dispatch.rs:723-735`.

**What:** Two completion paths exist:

```rust
// src/cluster/migration.rs:581-599
pub fn mark_inbound_complete(&mut self, shard: u16) {
    if let Some(m) = self.inbound_migrations.iter_mut()
        .find(|m| m.shard == shard && !m.completed) { ... }
    ...
}
```

vs.

```rust
// src/cluster/migration.rs:618-641
pub fn mark_inbound_complete_from_source(&mut self, shard: u16, from_node: NodeId) {
    if let Some(m) = self.inbound_migrations.iter_mut()
        .find(|m| m.shard == shard && m.from_node == from_node && !m.completed) { ... }
    else if let Some(m) = self.inbound_migrations.iter_mut()
        .find(|m| m.shard == shard && m.from_node == NodeId(0) && !m.completed) { ... }
    ...
}
```

The dispatcher decides which to call based on whether the
`OP_MIGRATION_COMPLETE` payload includes a `completion_from_node` field
(`dispatch.rs:723-735`):

```rust
// src/server/dispatch.rs:723-735
if no_data_completion {
    if let Some(from_node) = completion_from_node {
        cluster.mark_inbound_complete_from_source(shard, from_node);
    } else {
        cluster.mark_inbound_complete_all(shard);
    }
} else if let Some(from_node) = completion_from_node {
    cluster.mark_inbound_complete_from_source(shard, from_node);
} else {
    cluster.mark_inbound_complete(shard);
}
```

There is no validation that the connecting peer's identity matches the
declared `completion_from_node`. An attacker (per EF-01) can send any
NodeId. Combined with EF-12's zero-record fast-path, the receiver
clears the fence based purely on attacker-claimed source identity.

**Why it matters:** Defense-in-depth. Even with EF-01 fixed (HMAC on
TCP), the migration manager itself would still trust whichever NodeId
the caller declared. This is fine when the HMAC verifies the source's
secret-bearing identity, but is a problem if HMAC is shared across all
peers (the current design — there is no per-peer asymmetric key).

**Suggested fix:** Cross-check `completion_from_node` against the SWIM
view: `node_addrs.get(&from_node).map(|a| a == &peer_addr)`. If the
peer's TCP source address does not match the SWIM-known address of
`from_node`, refuse the completion.

---

### EF-22: `set_master_for_shard` silently ignores unrelated nodes — but does not log (LOW)

**Subcategory:** F (sharding)
**Location:** `src/cluster/shards.rs:360-374`.

**What:**

```rust
// shards.rs:367-370
let promote_idx = current.replicas.iter().position(|n| *n == new_master);
let Some(replica_idx) = promote_idx else {
    // `new_master` is not in this shard's assignment — refuse to mutate
    // so we don't fabricate an arbitrary cross-shard owner.
    return;
};
```

This is correct safety behavior, but a silent no-op makes diagnosing
"why isn't apply_master_election doing what I expect" harder.

**Suggested fix:** Add a `tracing::warn!(shard, ?new_master, ?current,
"election picked a node not in shard's assignment — ignoring")` in the
no-op branch.

---

### EF-23: Empty-shard fast-path in `begin_handoff_with` skips Copying — correct, but interaction with `master_subset` check requires care (verified — no finding)

**Subcategory:** F (migration)
**Location:** `src/cluster/shards.rs:149-182`.

**What:** Empty shards (per `shard_has_data` callback) skip Copying and go
directly to ServingNew. Master-subset is correspondingly cleared
(line 178-181). The interaction with `is_subset_master` (line 292-294)
is correct: a shard that skipped Copying never has `master_subset[shard] =
true` set, so `is_master` returns `Yes` immediately for the new master.

This is verified by `shard_table_handoff_identical_tables` and
`migration_of_empty_shard_completes_without_error`.

**No finding.**

---

### EF-24: `compute_with_epoch` panics on empty members (DEFENSIVE — no finding)

**Subcategory:** F (sharding)
**Location:** `src/cluster/shards.rs:103-107`.

**What:** `assert!(!members.is_empty(), "cannot compute shard table with 0 members")` —
the only way to hit this is a programming error in the caller (every
production caller filters by `committed_members.is_empty()` first or
ensures the local self-id is in the list). The defensive assertion
turns a silent corruption (division by zero) into a panic.

**No finding.**

---

### EF-25: Topology `propose_timeout` is computed from probe_interval × 3 — coupled to probe rate (LOW)

**Subcategory:** E (cluster)
**Location:** `src/bin/server.rs:659`.

**What:**

```rust
// src/bin/server.rs:659
topology_propose_timeout: probe_interval * 3,
```

A high probe interval (e.g. low-traffic environment, 1s probe) makes
fallback proposers wait 3s before stepping up. A slow proposer (e.g.
the one whose disk just hung on fsync of voted_term) would not be
superseded for 3s, during which clients receive
`MasterQueryResult::Transitioning` → `ERR_MIGRATION_IN_PROGRESS`. This
is an availability cost.

**Why it matters:** Tuning probe_interval up for bandwidth reasons
(small clusters in WAN deployments) inflates topology recovery time
3x. The coupling is non-obvious.

**Suggested fix:** Decouple the two configs so probe_interval can be
raised without also raising the propose timeout. Make
`topology_propose_timeout` an explicit config field that defaults to
`max(probe_interval * 3, 500ms)`.

---

### EF-26: `seed_attempt` exponential backoff resets only on "cluster looks settled" — slow seeds in degraded clusters back off forever (LOW)

**Subcategory:** E (cluster)
**Location:** `src/cluster/swim.rs:374-396`.

**What:** The seed-retry exponential-backoff path:

```rust
// src/cluster/swim.rs:374-396
let degraded = alive_count < total_known + 1 || total_known == 0;
if degraded {
    for seed in &self.config.seed_nodes { ... }
    next_seed_retry_delay = exponential_seed_backoff(
        seed_attempt, seed_backoff_initial, seed_backoff_max,
    );
    seed_attempt = seed_attempt.saturating_add(1);
} else {
    seed_attempt = 0;
    next_seed_retry_delay = healthy_seed_check_interval;
}
```

The `degraded` check fires whenever any known peer is dead or no peers
are known. Once the backoff hits the 5-second cap, it stays there. In
a long-running 3-node cluster where one node is permanently down and
the operator has not replaced it yet, the surviving 2-node cluster
backs off seed retries to once-per-5s, which is fine. But if the
seed_attempt counter has saturated, a sudden network heal that brings
the dead node back up doesn't re-energize seeding — the backoff stays
at 5s until the next `degraded == false` observation. This is fine
in practice (5s isn't catastrophic), but the convergence is slower
than necessary.

**No finding** — this is acceptable behavior. Flagged for completeness.

---

### EF-27: `forget_dead_older_than(3600s)` interaction with cluster-state persistence (LOW)

**Subcategory:** E (cluster)
**Location:** `src/cluster/swim.rs:399-418`,
`src/cluster/topology.rs:225-235` (persisted committed_members).

**What:** After 1 hour, dead nodes are removed from membership. But they
remain in the persisted `committed_members` until the next quorum-committed
topology change drops them. If a node is dead for 1.5 hours, then briefly
recovers, then dies again before the cluster commits a new topology, the
"in committed_members but not in membership" state can confuse
`alive_node_count` (per EF-02 — but in the *opposite* direction:
overcounting now? actually still undercounting because the filter is
`addrs.contains_key`).

**No finding** — the interaction is benign because filtering by
`node_addrs` (which is updated by SWIM events) is the conservative
choice. Flagged for completeness.

---

### EF-28: `OP_TOPOLOGY_COMMIT` quorum proof is implicit; the digest covers (term, members) but not who voted (LOW)

**Subcategory:** E (cluster)
**Location:** `src/cluster/topology.rs:166-198,569-599`.

**What:** A `TopologyCommit` carries `term`, `proposer`, `members`, and
`digest = SHA256(term || members)`. There is no quorum certificate —
no list of voters and their signatures. Any peer that knows the term
and members can manufacture a valid commit (per EF-01).

The propose/vote phase has voter identity (`TopologyVote.voter`) but
the commit drops it. After a commit broadcast, no audit trail remains
of *who* approved.

**Why it matters:** Forensics; combined with EF-01, makes attribution
impossible. Even with HMAC fixed, the on-disk persisted state has no
"this commit was approved by these N nodes" record.

**Suggested fix:** Extend `TopologyCommit` to include a `voters: Vec<NodeId>`
field with a corresponding signature aggregate (HMAC over each voter's
share). Persist alongside `committed_members`. Defensive in depth.

---

### EF-29: SWIM `committed_term` piggyback can drive a node to request topology catchup from any peer — including a malicious one (MEDIUM)

**Subcategory:** E (cluster)
**Location:** `src/cluster/swim.rs:569-578`,
`src/cluster/coordinator.rs:1422-1467`.

**What:** SWIM messages carry a piggybacked `committed_term`
(swim.rs:835-842). When a peer's piggyback term exceeds local, an
event is emitted:

```rust
// src/cluster/swim.rs:569-578
if remote_committed > local_committed {
    events.push(ClusterEvent::TopologyStale(remote_committed));
}
```

The coordinator handles `TopologyStale` by fetching the partition map
from the peer (`coordinator.rs:1422-1467`) and adopting its
`committed_term` and `members` if validation passes:

```rust
// coordinator.rs:1422-1467 (excerpt)
if let Ok(routing) = ... {
    if routing.shard_table_version > local_active_version
        && routing.shard_table_version > topology_authority.committed_term() {
        let synthetic = TopologyCommit { term: routing.shard_table_version, ... };
        topology_authority.handle_commit(&synthetic);
        ...
    }
}
```

The `OP_GET_PARTITION_MAP` response is unauthenticated (per EF-01).
A malicious peer can advertise a high `committed_term` via the SWIM
piggyback (UDP, but if the attacker has the cluster_secret they pass
HMAC), then serve a forged `RoutingInfo` over TCP (no HMAC). The
synthetic commit is constructed by the *local* node from
`routing.committed_members` and is then applied without further
authority check — `handle_commit` only verifies the digest matches
the term + members the local node just deserialized, not that the
peer is a legitimate proposer.

**Why it matters:** Even without inter-node TCP HMAC, this fast path
gives a single peer (with cluster_secret access) full control over the
local committed_term + member set. The SWIM piggyback is the trigger,
the TCP fetch is the payload, and there is no quorum check on the
synthetic commit.

**Reproduction:** With the cluster_secret, send a SWIM PING with
piggybacked `committed_term = 2^60` (or any value larger than current).
Then respond to the target's `OP_GET_PARTITION_MAP` request with a
crafted RoutingInfo containing `shard_table_version = 2^60` and any
`committed_members` you choose. The target adopts the new term locally.

**Suggested fix:** The synthetic commit path must require a quorum
proof (per EF-28) before adopting. Or at minimum, require that the
new `committed_members` is a strict superset of the local
`committed_members`, OR require the full propose/vote round before
adopting any term advertised by a single peer.

---

### EF-30: `OP_GET_PARTITION_MAP` includes neither timestamp nor signature — clients caching stale maps cannot detect tampering (LOW)

**Subcategory:** F (sharding)
**Location:** `src/server/dispatch.rs:5161-5188`,
`src/cluster/routing.rs:67-93`.

**What:** Clients fetch the partition map via `OP_GET_PARTITION_MAP`
to populate their local routing cache. The wire format
(`routing.rs:67-93`) has version, nodes, and shard assignments, but
no signature, no fresh-timestamp, and no MAC. A man-in-the-middle on
the binary protocol can rewrite it arbitrarily.

**Why it matters:** Less severe than EF-01/EF-29 because the client's
"truth" is what each individual node serves on a write, not what the
client cached. But a malicious partition map can still cause clients
to flood specific nodes (DoS amplification).

**Suggested fix:** When per-connection auth lands (mTLS), this is
covered. Until then, accept this as a known limitation.

---

## Items not verified due to scope / time constraints

- **Live integration testing** of EF-01, EF-09, EF-10, EF-12, EF-29:
  these findings are based on source-code analysis. End-to-end
  reproduction requires standing up multi-node clusters with crafted
  network attackers. The reasoning is straightforward but unconfirmed
  by execution.
- **Performance** of the migration throttle (`MigrationThrottle`,
  `migration.rs:328-401`) was not benchmarked under contention.
- **Crash-recovery interactions** between the migration manager's
  `restore_inbound_state` / `restore_outbound_state` and the redo
  log's truncation sentinel were not exhaustively traced. Single-step
  reasoning suggests they are correct; multi-step interleavings (e.g.
  crash during partial outbound state persist) require simulator
  testing.
- **The full execution path of `apply_master_election`**
  (`coordinator.rs:5206-5271`) was sketched but not exhaustively
  traced for all combinations of (was_evicted, is_subset,
  was_previous_master, partition_view empty). The unit tests at
  `coordinator.rs:7038+` cover the common paths.
- **`OP_PARTITION_VERSION_REPORT` payload format** was not parsed
  in detail; the audit assumed correctness based on the round-trip
  tests in `coordinator.rs:8000+`.
- **The catch-up path** triggered by `TopologyStale` is partially
  audited (EF-29) but the full flow including stream rate limits
  was not traced.

---

## Summary table

| ID | Severity | Subcat | Title | File:Line |
|----|----------|--------|-------|-----------|
| EF-01 | CRITICAL | E | Inter-node TCP frames are unauthenticated | `src/cluster/coordinator.rs:2589` |
| EF-02 | HIGH | E | `alive_node_count` excludes self | `src/cluster/coordinator.rs:5860` |
| EF-03 | HIGH | E | No coverage for isolated 1-node remnant | `tests/cluster_*.rs` |
| EF-04 | MEDIUM | E | Missing `*.topo` resets peak to 1 | `src/cluster/coordinator.rs:5025` |
| EF-05 | MEDIUM | E | Partition map omits self | `src/cluster/coordinator.rs:5792` |
| EF-06 | MEDIUM | E | HMAC unit-tested, not integration-tested | `src/cluster/auth.rs` |
| EF-07 | LOW | E | Dead-node forget-window cliff at 1h | `src/cluster/swim.rs:402` |
| EF-08 | MEDIUM | E | cluster_secret only required for RF>1 | `src/config.rs:665` |
| EF-09 | HIGH | F | REDIRECT has no hop count / TTL | `src/server/dispatch.rs:2287` |
| EF-10 | HIGH | E | Split-brain heal has no rejection path | `src/cluster/topology.rs:511` |
| EF-11 | OK  | F | 0x0FFF mask is correct | `src/cluster/shards.rs:316` |
| EF-12 | HIGH | F | OP_MIGRATION_COMPLETE unauthenticated | `src/server/dispatch.rs:471` |
| EF-13 | OK  | F | All write opcodes gated uniformly | `src/server/dispatch.rs:2229` |
| EF-14 | LOW | F | Reads return MIGRATION_IN_PROGRESS, no wait | `src/server/dispatch.rs:4310` |
| EF-15 | LOW | F | clear_stale_inbound 30s race vs slow migration | `src/cluster/coordinator.rs:847` |
| EF-16 | OK  | F | Round-robin assignment deterministic | `src/cluster/shards.rs:103` |
| EF-17 | OK  | F | migration_pool_size / batch_size wired | `src/cluster/coordinator.rs:556` |
| EF-18 | LOW | F | /admin/drain returns before drain completes | `src/server/http.rs:1126` |
| EF-19 | LOW | F | partition_map version vs committed_term lag | `src/cluster/coordinator.rs:5793` |
| EF-20 | LOW | E | SWIM MAX_MSG_SIZE hard-coded 4096 | `src/cluster/swim.rs:30` |
| EF-21 | MEDIUM | F | mark_inbound_complete trusts caller-declared from_node | `src/cluster/migration.rs:618` |
| EF-22 | LOW | F | set_master_for_shard silently ignores unrelated nodes | `src/cluster/shards.rs:367` |
| EF-23 | OK  | F | Empty-shard handoff fast-path correct | `src/cluster/shards.rs:149` |
| EF-24 | OK  | F | compute_with_epoch defensive panic | `src/cluster/shards.rs:103` |
| EF-25 | LOW | E | propose_timeout coupled to probe_interval | `src/bin/server.rs:659` |
| EF-26 | OK  | E | seed_attempt backoff acceptable | `src/cluster/swim.rs:374` |
| EF-27 | OK  | E | forget_dead_older_than vs persisted members | `src/cluster/swim.rs:402` |
| EF-28 | LOW | E | TopologyCommit lacks voter list | `src/cluster/topology.rs:166` |
| EF-29 | MEDIUM | E | SWIM piggyback drives synthetic catch-up | `src/cluster/swim.rs:569` |
| EF-30 | LOW | F | OP_GET_PARTITION_MAP has no signature | `src/server/dispatch.rs:5161` |

**Critical/High count:** 6 (EF-01, EF-02, EF-03, EF-09, EF-10, EF-12).
**Medium count:** 5 (EF-04, EF-05, EF-06, EF-08, EF-21, EF-29).
**Low/OK:** the remainder.

The most urgent fixes are EF-01 (inter-node TCP authentication) and
EF-02 (alive count off-by-one); these are independent and can be
fixed in parallel. EF-12 / EF-21 / EF-29 are all downstream of EF-01
and benefit substantially from fixing it.
