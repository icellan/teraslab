# Category D: Replication — Audit Report

**Scope:** Operation-based synchronous replication subsystem (`src/replication/*`,
the dispatch fan-out layer in `src/server/dispatch.rs`, replica receiver, TCP
transport, persistent ACK / intent / applied trackers, and the replication
configuration surface).

**Files examined:**
- `src/replication/mod.rs`
- `src/replication/manager.rs` (2214 lines — manager, AckPolicy, transport
  trait, in-memory transport)
- `src/replication/durable.rs` (1098 lines — AckTracker,
  ReplicationIntentTracker, ReplicaAppliedTracker, catchup runner,
  spawn_lag_monitor)
- `src/replication/receiver.rs` (2641 lines — `handle_replica_batch_*`,
  `apply_op`, generation guards)
- `src/replication/protocol.rs` (1359 lines — `ReplicaOp`, `ReplicaBatch`,
  `ReplicaAck`, `CatchupRequest`, V1/V2 wire format)
- `src/replication/tcp_transport.rs` (619 lines)
- `src/replication/batching.rs` (120 lines)
- `src/server/dispatch.rs` — `replicate_all_ops`,
  `classify_replication_outcome`, compensation paths
- `src/config.rs` — `ack_policy`, `replication_degraded_mode`,
  `replication_timeout_ms`, `replica_lag_check_interval_secs`,
  `resolved_ack_policy`, `validate_cluster_safety`
- `tests/replication_tcp.rs` (937 lines — 8 integration tests)

---

## Overview

TeraSlab implements a fairly thoughtful synchronous, operation-based
replication subsystem. The hot path is well-engineered:

* **Wire protocol is versioned** (V1 / V2) with explicit `cluster_key`
  carried in V2 frames and an explicit `ProtocolError::UnknownVersion`
  rejection for any other byte.
* **Cluster epoch / stale-master gate** (`ERR_STALE_EPOCH`) is implemented
  on the receiver side with documented relaxed semantics for the
  `local_cluster_key == 0` (post-restart) and
  `batch.cluster_key > local_cluster_key` (newer-than-local) cases — those
  are accepted, not rejected, and that decision is correctly explained
  inline.
* **Per-stream applied-sequence dedup** in `ReplicaAppliedTracker` is
  durable, monotonic, and persisted before ACK (so a receiver crash after
  the master saw the ACK cannot regress the high-water mark).
* **Replication intent journal** (`ReplicationIntentTracker`) records
  durable redo ranges before fan-out and clears them only after the ACK
  policy is met (or after compensation) so a master crash that loses the
  ACK is replayed on startup.
* **Compensation path** captures real before-images for `unset_mined`,
  `reassign`, and `prune_slot` (the recent gap #8 work) and emits
  `Compensate*` redo entries so a crash mid-rollback can be replayed
  exactly.
* **TCP transport** disables Nagle, enables OS-level keepalive (idle=5s,
  interval=1s, count=3 → ~8s broken-connection detection), and the
  dispatch-side `send_replica_batch_to` has a single-attempt reconnect
  retry on `send_batch` failure.
* **Fan-out is parallel** (`std::thread::scope` in the manager,
  `tokio::task::spawn_blocking` from dispatch), and a dedicated test
  asserts the wall-clock proves concurrency.
* **Master-side `WriteAll` / `WriteMajority`** policy is honored; failure
  short-circuits to `ERR_REPLICATION_FAILED` after invoking
  `compensate_replication_failure`.

The subsystem is materially better than the surrounding production
hardening (e.g. cluster membership / migrations) and several long-cited
correctness gaps are already closed.

That said, the audit found a number of real defects, ranging from
**HIGH-severity silent dead code** (`replica_lag_check_interval_secs` and
`spawn_lag_monitor` are never wired) to **MEDIUM-severity inconsistencies
between two parallel ack-counting code paths** that disagree on whether
to count the master, to **LOW-severity hot-path inefficiencies** in the
TCP receive loop. There is also a meaningful **observability gap**: the
ACK tracker (`AckTracker`) is updated on every ACK but its content has no
runtime consumer (the lag monitor that would read it is never spawned).

Findings below are ordered roughly by severity / blast radius.

---

## Findings

### D-01: `replica_lag_check_interval_secs` config option is dead code; lag monitor is never spawned (HIGH)

**Location:**
- `src/config.rs:387` (config field)
- `src/config.rs:444` (default value `30`)
- `src/replication/durable.rs:679-709` (`spawn_lag_monitor`)
- All other locations: zero call sites

**What:**
The `ServerConfig` exposes
```rust
pub replica_lag_check_interval_secs: u64,
```
and `src/replication/durable.rs` defines a complete `spawn_lag_monitor(...)`
helper that periodically reads `tracker.all_acked()` and emits
`tracing::warn!` when any replica's lag exceeds a threshold. But:

```
$ rg -n "spawn_lag_monitor" /Users/siggioskarsson/gitcheckout/teraslab/
src/replication/durable.rs:679:pub fn spawn_lag_monitor(

$ rg -n "replica_lag_check_interval_secs" /Users/siggioskarsson/gitcheckout/teraslab/
src/config.rs:387:    pub replica_lag_check_interval_secs: u64,
src/config.rs:444:            replica_lag_check_interval_secs: 30,
README.md:156:replica_lag_check_interval_secs = ...
```

`spawn_lag_monitor` has **zero call sites** in `src/`, `src/bin/`, or
`tests/`. `replica_lag_check_interval_secs` is **read nowhere**. The
`AckTracker` is updated on every successful replica ACK
(`dispatch.rs:2058-2060`), but nothing consumes that data — neither at
runtime, on `/healthz`, nor on `/metrics` (the lag warnings the function
would emit would be tracing-level only anyway).

**Why it matters:**
This is the canonical "monitoring claim that doesn't exist" failure mode.
Operators reading `README.md` line 156 (or the `replica_lag_check_interval_secs`
TOML key) will believe the cluster has live replica-lag monitoring at the
documented 30-second cadence. It does not. Replicas can fall arbitrarily
far behind the master and the operator gets no signal until either
(a) a write fails the configured ACK policy, or (b) a partition heal
triggers full catch-up. Both of those are too late: they're symptoms,
not predictive lag warnings.

**Reproduction:**
```
$ rg -n "spawn_lag_monitor\(" /Users/siggioskarsson/gitcheckout/teraslab/
src/replication/durable.rs:679:pub fn spawn_lag_monitor(    ← definition only

$ rg -n "lag_monitor|lag_check" /Users/siggioskarsson/gitcheckout/teraslab/src/bin/server.rs
(no matches)
```

The function is also never `#[allow(dead_code)]`-annotated — clippy
should have flagged it under any `dead_code` lint, suggesting the lint
configuration is not actually catching this.

**Suggested fix:**
1. Wire `spawn_lag_monitor` into `bin/server.rs` (next to the `init_ack_tracker(...)`
   call at `src/bin/server.rs:699`) when `config.replica_lag_check_interval_secs > 0`.
2. Add a Prometheus gauge `repl_replica_lag_ops{replica="..."}` populated
   by the same loop so lag is observable from `/metrics` (currently the
   `replication_metrics` struct exposes leader_sequence but not the
   per-replica delta).
3. Surface lag in `/healthz` so the cluster goes "degraded" if any
   replica's lag exceeds a configurable bound — the TODO is implicit in
   `replica_lag_check_interval_secs`'s name, the implementation is the
   gap.
4. Keep the existing function's signature; it accepts a callback for the
   master sequence so it composes cleanly with `cluster.current_sequence()`.

---

### D-02: `WriteMajority` semantics differ between manager and dispatch (MEDIUM, correctness diverges as RF grows)

**Location:**
- `src/replication/manager.rs:487-496` (`required_ack_count`)
- `src/server/dispatch.rs:1544-1573` (`classify_replication_outcome`)

**What:**
There are two code paths that compute the WriteMajority threshold; both
disagree on what they're counting.

`ReplicationManager::required_ack_count` (manager.rs:487):
```rust
let rf = self.senders.len() + 1; // replicas + master
match self.config.ack_policy {
    AckPolicy::WriteAll => self.senders.len(),
    AckPolicy::WriteMajority => {
        let majority = rf / 2 + 1;
        majority.saturating_sub(1) // master counts as 1
    }
}
```

`classify_replication_outcome` (dispatch.rs:1550):
```rust
let required = match ack_policy {
    Some(AckPolicy::WriteAll) => total_targets,
    Some(AckPolicy::WriteMajority) => total_targets.div_ceil(2),
    None => 0,
};
```

| RF | replicas | manager required | dispatch required | match? |
|----|----------|------------------|-------------------|--------|
| 2  | 1        | 1                | 1                 | yes    |
| 3  | 2        | 1                | 1                 | yes    |
| 4  | 3        | 2                | 2                 | yes    |
| 5  | 4        | 2                | 2                 | yes    |
| 6  | 5        | 3                | 3                 | yes    |
| 7  | 6        | 3                | 3                 | yes    |

The numerical results coincide *for now* — both formulas give
`ceil(replicas/2)` for the input shapes that matter. But the manager's
formula is `floor(rf/2) + 1 - 1 = floor(rf/2)` where `rf = replicas + 1`,
and the dispatch's formula is `ceil(replicas/2)`. These coincide only
because `floor((replicas+1)/2) == ceil(replicas/2)` for all
non-negative `replicas`. **There is no test that pins this equivalence**
and a refactor of either side could silently break the other.

Worse, the two paths *count the master differently*:
* Manager treats the master as a counted copy (master + replicas = RF) and
  derives "majority of RF, minus the master".
* Dispatch passes `total_targets` = number of replica addresses
  (excluding master) and ignores the master's own contribution.

This is also the **wrong threshold for RF=2** by the principle the
config docs claim. `src/config.rs:345` says:
> `"write_majority"`: Wait for floor(RF/2)+1 copies (including master).

For RF=2 that means `floor(2/2)+1 = 2` copies; subtracting the
master = 1 replica ACK required. Both implementations give 1. **Good.**

For RF=3: `floor(3/2)+1 = 2` copies; minus master = 1 replica ACK.
**This is what the test at `manager.rs:993-1003` asserts**:
```rust
// RF=3, WriteMajority: need 1 ACK (master + 1 = majority of 3)
assert_eq!(mgr.required_ack_count(), 1);
```

For RF=2 and `WriteMajority`, manager's `write_majority_rf2_succeeds`
(manager.rs:1201) and the comment "RF=2: need 1 replica ACK (majority of
2 = 1 + master)" are consistent.

**Why this still matters:**
The audit's claim was that RF=2 + `WriteMajority` is "a common bug spot"
— "does majority mean 2 of 2, or 1 of 2?" The answer the code chose is
**1 of 2**. This is *defensible* (it's classic Paxos majority-counting:
3-of-5 is majority, 2-of-3 is majority, 1-of-1 is majority, **and 1-of-1
on a 2-node cluster yields master + 1 replica = 2 of 2 acknowledgments**).
But it has a startling consequence:

> With `replication_factor = 2` and `ack_policy = "write_majority"`,
> losing a replica leaves the master ACKing all writes from itself
> alone. There is no quorum, but `WriteMajority` is *satisfied*.

That's not what most operators expect from a two-node "replicated" setup.
RF=2 + WriteMajority is effectively *single-node durability mode under
any replica failure*. The default `ack_policy = "auto"` correctly
mitigates this by mapping RF=2 to `WriteAll` (`config.rs:499`), but
operators that explicitly set `write_majority` get this surprise.

**Reproduction:**
```rust
let mgr = ReplicationManager::new(
    ReplicationConfig {
        ack_policy: AckPolicy::WriteMajority,
        ..Default::default()
    },
    vec![Box::new(InMemoryTransport::pair().0)],  // 1 replica = RF=2
);
assert_eq!(mgr.required_ack_count(), 1);  // master alone is enough
```

This is the existing test. It passes. The behaviour is **as designed**,
but it is not what an operator will assume.

**Suggested fix:**
1. Add an equivalence test that locks the manager and dispatch formulas
   together (or refactor both to call a single shared
   `required_replica_acks(rf, policy)` helper). Today the two are
   independently maintained.
2. Update the docstring on `AckPolicy::WriteMajority` (manager.rs:38) to
   spell out the RF=2 corner: *"With RF=2 this requires zero replica
   ACKs once any replica is `Down` — effectively single-node durability.
   Use `WriteAll` for RF=2 if availability is the concern."*
3. Surface a startup warning when `replication_factor = 2 && ack_policy
   = "write_majority"` because the combination is almost always not
   what the operator wanted. The existing
   `validate_cluster_safety` (config.rs:522) catches the
   `best_effort + RF>1` mistake; a sibling check belongs here.

---

### D-03: AckTracker writes are racy — record-then-flush window can lose the most recent ACK on master crash (MEDIUM)

**Location:** `src/replication/durable.rs:95-107`

**What:**
`AckTracker::record_ack` updates the in-memory map and only flushes if
`last_flush.elapsed() >= 1000ms`:

```rust
pub fn record_ack(&self, addr: SocketAddr, through_sequence: u64) {
    let mut inner = self.inner.lock().unwrap();
    let entry = inner.last_acked.entry(addr).or_insert(0);
    if through_sequence > *entry {
        *entry = through_sequence;
        inner.dirty = true;
    }
    if inner.dirty && inner.last_flush.elapsed().as_millis() >= FLUSH_INTERVAL_MS {
        self.flush_locked(&mut inner);
    }
}
```

So up to 1 second of replica ACKs are held in memory. If the master
process is `kill -9`'d in that window, the next master restart reads a
stale `last_acked` for every tracked replica. Catch-up logic (`run_catchup_for_replica`,
durable.rs:619) starts from `last_acked + 1`. If the on-disk
`last_acked` is older than what the replica actually applied, **the
master will re-send already-applied redo entries**.

This is mostly safe because the receiver dedups via `ReplicaAppliedTracker`
— the receiver-side `applied.get(stream_key)` will skip the duplicate
prefix. **However**, for a *new* TCP connection from the same master,
the receiver keys by `peer_addr.to_string()` (receiver.rs:226):

> `let stream_key = peer_addr.to_string();`

A reconnect from the master almost always uses the *same* source IP +
ephemeral port pair, but the ephemeral port can change. If the source
port changes the receiver treats it as a different stream and replays
all the duplicate ops. The receiver then upgrades to the
`source_node_id`-derived key inside `handle_replica_batch_with_tracker`
(receiver.rs:456-459):

```rust
let effective_stream_key = batch
    .source_node_id
    .map(|id| format!("node:{id}"))
    .unwrap_or_else(|| stream_key.to_string());
```

— so as long as the master sends `source_node_id: Some(...)`
(`dispatch.rs:1297`), the dedup key is stable across reconnects with
different ephemeral ports. The double-application is therefore avoided
**iff** the master is actually clustered and `source_node_id` is set.
It is *not* avoided in the test path
(`handle_replica_batch_with_cluster_key`) where each thread has its
own thread-local in-memory tracker.

**Why it matters:**
The 1-second flush window is a real silent-data-loss risk under master
crash. The receiver-side dedup makes it idempotent **except in tests
and in single-stream paths that don't pass `source_node_id`**. The risk
to production is small but the risk to test-stability and to anybody
building on this with their own testing fixtures is real.

**Reproduction:**
1. Drive 100 sequential ACKs through `AckTracker::record_ack` within
   500ms — none are flushed.
2. `kill -9` the process.
3. On restart, `AckTracker::load_from_disk` reads the previous flush,
   missing all 100 most-recent ACKs.

There is no test for this path; the persistence test
(`durable.rs:741-757`) calls `tracker.flush()` explicitly before
shutting down.

**Suggested fix:**
1. Add a write-after-N counter alongside the time-based flush so a burst
   of ACKs cannot accumulate unbounded.
2. Document the 1-second window prominently on `AckTracker::record_ack`
   so callers know the recovery semantics.
3. Add a test that verifies the catchup stream-key derivation is
   actually stable across master reconnects (the `node:{id}` path), with
   `source_node_id = None` exercising the fallback.

---

### D-04: `is_connected` probe creates a 1ms read window — false positives on flaky links (LOW)

**Location:** `src/replication/tcp_transport.rs:237-251`

**What:**
```rust
fn is_connected(&self) -> bool {
    let orig = self.stream.read_timeout().ok().flatten();
    let _ = self.stream.set_read_timeout(Some(Duration::from_millis(1)));
    let mut probe = [0u8; 1];
    let connected = match self.stream.peek(&mut probe) {
        Ok(0) => false,
        Ok(_) => true,
        Err(ref e) if e.kind() == ErrorKind::WouldBlock => true,
        Err(ref e) if e.kind() == ErrorKind::TimedOut => true,
        Err(_) => false,
    };
    let _ = self.stream.set_read_timeout(orig);
    connected
}
```

The 1ms peek with `TimedOut → connected` masks broken-pipe errors that
take longer than 1ms to surface. The keepalive
(`configure_tcp_keepalive`, idle=5s) is the primary fast-detection
mechanism; until keepalive declares the socket dead, `is_connected()`
will report "connected" for any TCP-pipe-still-half-open state.

**Why it matters:**
The dispatch-side connection pool reuses cached transports based on
`is_connected()` result (`dispatch.rs:2034-2036`):

```rust
let mut transport = match slot_guard.connection.take() {
    Some(t) if t.is_connected() => t,
    _ => TcpReplicaTransport::connect(&addr.to_string(), Duration::from_secs(5))
        .map_err(|e| format!("connect: {e}"))?,
};
```

A `is_connected = true` on a stale TCP pipe means we attempt a
`send_batch` on a dead connection, fail, and only *then* reconnect. The
single-retry path (`dispatch.rs:2040-2050`) catches this, so the
correctness impact is bounded — but it adds avoidable latency on every
broken-pipe event.

**Reproduction:**
Send a batch on a half-open TCP connection where the peer crashed but
keepalive has not yet expired. `is_connected()` returns `true`,
`send_batch` returns `Err`, the retry path opens a fresh connection.
The single-RTT extra latency can be 100ms+ if the keepalive window is
generous.

**Suggested fix:**
Either (a) drop `is_connected()` entirely and accept the single-RTT
retry as the canonical detection path (it works, and the second branch
is already required for cold cache), or (b) elevate the probe to also
read SOL_SOCKET / SO_ERROR after the peek. (a) is simpler and removes
a measurable latency tax.

---

### D-05: Receiver's stream-key fallback uses `peer_addr.to_string()` — ephemeral-port roll triggers full re-replay before `source_node_id` resolution (MEDIUM)

**Location:** `src/replication/receiver.rs:223-227, 456-459`

**What:**
The receiver's per-connection handler picks a stream key from the
peer's TCP socket address (`receiver.rs:226`):
```rust
let stream_key = peer_addr.to_string();
```

But inside `handle_replica_batch_with_tracker` it overrides this if the
batch carries a `source_node_id` (line 456):
```rust
let effective_stream_key = batch
    .source_node_id
    .map(|id| format!("node:{id}"))
    .unwrap_or_else(|| stream_key.to_string());
```

So the **first-batch** path looks at the batch payload to get the right
stream key. **But** the receiver does not require `source_node_id`. If
a batch arrives without it, the receiver falls back to the TCP
peer-key, which **changes on every master reconnect** (different
ephemeral source port). Result: the receiver treats every reconnect as
a fresh stream, sets `applied[new_addr_string] = 0`, and re-applies the
entire backlog of (ostensibly idempotent) ops.

`apply_op` is idempotent for every variant (the engine's
`AlreadySpent`, `AlreadyFrozen`, etc. are mapped to `Ok(())`), so this
does not corrupt state on the *replica*. But:

1. It **wastes substantial CPU** on a reconnect storm (every replay
   re-validates each op against device state).
2. The receiver's persistent applied tracker accumulates a key per
   `IP:port` it ever saw. The file grows monotonically; there is no
   reaper.
3. Tests that don't go through dispatch and don't pass `source_node_id`
   (e.g. `tests/replication_tcp.rs:213-217`, where every batch has
   `source_node_id: None`) silently exercise the "every reconnect is
   fresh" path. The result happens to be ok because the tests are short
   and use idempotent ops.

**Reproduction:**
```rust
// Test code from tests/replication_tcp.rs:206-223
let batch = ReplicaBatch {
    first_sequence: 1,
    ops: vec![ReplicaOp::Spend { ... }],
    trace_ctx: None,
    source_node_id: None,  // <-- triggers IP:port fallback key
    cluster_key: 0,
};
```
Send the same batch twice on two separate `TcpReplicaTransport::connect`
sessions. Each connection has a different ephemeral source port, so the
receiver's tracker holds *two* entries for what is logically the same
stream and applies the batch twice.

**Why it matters:**
The receiver's persistent `repl-applied.dat` accumulates one entry per
distinct `IP:port` it ever saw, growing without bound. On a realistic
six-month-old cluster with frequent reconnects this can be many
thousand entries — every flush re-serializes the entire HashMap to
disk.

**Suggested fix:**
1. **Require** `source_node_id` for any non-test path. The receiver
   should reject (or at least warn) on batches without it; the master
   in dispatch.rs:1297 always sets it.
2. Reap `applied` entries that haven't been touched in N hours, or
   gate the file grow-size with a hard cap and a compaction step.
3. Document the contract: "stream key is `node:{source_node_id}` if
   set, else `peer_addr.to_string()` — the latter is for tests only,
   never reuses TCP-peer-keying as a load-bearing dedup primitive in
   production."

---

### D-06: `replication_timeout_ms` ignored when migration pressure is active (MEDIUM, undocumented)

**Location:** `src/server/dispatch.rs:1404-1410`

**What:**
```rust
fn replication_ack_timeout_for(base: Duration, migration_pressure: bool) -> Duration {
    if migration_pressure {
        base.max(MIGRATION_REPLICATION_TIMEOUT_FLOOR)
    } else {
        base
    }
}
```

`MIGRATION_REPLICATION_TIMEOUT_FLOOR` is `Duration::from_secs(30)` (per
the test at dispatch.rs:7100-7113). When migration is active, the
configured `replication_timeout_ms` is silently extended to **at least
30 seconds**. Operators who set `replication_timeout_ms = 1000` to fail
fast under load will see writes take 30 seconds during any rebalance.

**Why it matters:**
This is a common operational footgun: the operator dials the timeout
down to drop slow tail latency, then a rebalance kicks in and writes
unfreeze for 30s. The behavior is *intentional* (catch-up batches are
slower) and the test at dispatch.rs:7100 *pins* the 30-second floor —
but `config.rs:349-350` documents only:

> "Timeout in milliseconds for each replication batch ACK. Default: 3000."

There is no mention of the migration override.

**Reproduction:**
Set `replication_timeout_ms = 500`, trigger a rebalance, observe
30-second p99 spikes during the migration window.

**Suggested fix:**
Document the migration-pressure floor in `config.rs:349-350` and
expose it as a separate `replication_timeout_during_migration_ms`
config knob with the documented default of `30000`.

---

### D-07: `apply_op` reads the slot from device for every Spend/Freeze/Unfreeze/Reassign — duplicated I/O on the receiver hot path (LOW, performance)

**Location:** `src/replication/receiver.rs:740-895`

**What:**
For Spend, Freeze, Unfreeze, and Reassign the replica receiver does:

```rust
let hash = match engine.read_slot(tx_key, *offset) {
    Ok(slot) => slot.hash,
    Err(_) => return Ok(()),
};
```

— a device read per op. The master *has* the hash (it just wrote it)
but the wire format does not carry it. Every replicated Spend on the
hot path costs an extra block read on the replica.

**Why it matters:**
For a master targeting 10M ops/sec the replica must apply at the same
rate. Adding one device read per op is a real bottleneck — the cache
layer mitigates but does not eliminate it.

The engine already exposes `lookup → record_offset → slot_offset`
arithmetic; the missing piece is just adding the hash to the wire op.

**Suggested fix:**
Add `utxo_hash: [u8; 32]` to `ReplicaOp::Spend`, `Freeze`, `Unfreeze`,
and `Reassign`. This is a wire-protocol change (V3) but the deserializer
already understands version-gated layouts; bumping the version is the
clean path. The cost is 32 bytes per op — Spend goes from ~77 bytes to
~109 bytes — well within the per-op budget.

---

### D-08: ReplicaAck error variant is *always* sent over `STATUS_OK` even when the receiver flush failed (LOW, but confusing)

**Location:** `src/replication/receiver.rs:541-575`

**What:**
When `applied.flush()` fails, the receiver produces an
`ReplicaAck::Error` payload but wraps it in a `STATUS_OK` response
frame:

```rust
if let Err(e) = applied.flush() {
    let ack = ReplicaAck::Error {
        failed_sequence: through,
        message: format!("flush applied tracker: {e}"),
    };
    return ResponseFrame {
        request_id: request.request_id,
        status: STATUS_OK,           // <-- STATUS_OK with Error payload
        payload: ack.serialize(),
    };
}
```

The same pattern repeats for `apply_op` errors and even for
deserialize errors (lines 395-404). The TCP transport's `recv_ack`
helper (`tcp_transport.rs:213-228`) handles this:

```rust
if resp.status != STATUS_OK {
    if let Ok(ack) = ReplicaAck::deserialize(&resp.payload) {
        return Ok(ack);
    }
    return Err(...);
}
ReplicaAck::deserialize(&resp.payload)...
```

— so it falls through to a normal `ReplicaAck::deserialize`
regardless. The `STATUS_OK + Error payload` shape is therefore
*correct* but it conflates "my framing layer worked" with "the op
succeeded." A naive client that checks only `resp.status == STATUS_OK`
would believe the ACK was successful.

**Why it matters:**
There is no naive client today — only `TcpReplicaTransport::recv_ack`
consumes these — but the contract is fragile. If someone adds a
read-side health probe that reads the status byte, it'll mis-classify
errors as successes.

**Suggested fix:**
Either:
1. Use `STATUS_ERROR` for `ReplicaAck::Error` to make the wire status
   reflect the application status, or
2. Document the convention prominently on `ReplicaAck::Error`:
   "Always sent inside a `STATUS_OK` frame; the application layer
   discriminates via the ACK type tag."

The Phase B2 stale-epoch path (lines 432-454) *does* use
`STATUS_ERROR + ERR_STALE_EPOCH`, so the convention is already
inconsistent within this very file.

---

### D-09: `connect()` timeout is reused for read AND write timeouts, masking master-side stalls (LOW)

**Location:** `src/replication/tcp_transport.rs:99-123`

**What:**
```rust
pub fn connect(addr: &str, timeout: Duration) -> Result<Self, ReplicationError> {
    ...
    stream.set_write_timeout(Some(timeout))...?;
    stream.set_read_timeout(Some(timeout))...?;
    ...
}
```

The single `timeout` parameter sets both `set_write_timeout` and
`set_read_timeout`. The connect-time timeout (used for
`TcpStream::connect_timeout`) is the right value for the connect, but
`set_write_timeout` should typically be much shorter than
`set_read_timeout` (write blocks only on a backed-up TCP send buffer;
read blocks waiting for a remote ACK).

`recv_ack` later calls `set_read_timeout(Some(timeout))` (line 174)
so the read timeout *is* reset per call. But `set_write_timeout` is
set once at connect time and never updated. A master with a 5-second
connect timeout and a slow replica will see writes block up to 5
seconds before timing out, regardless of `replication_timeout_ms`.

**Why it matters:**
The replication batching layer expects write-side latency to be
bounded by `replication_timeout_ms` (3 seconds default). With a
5-second connect timeout, the actual write-side bound is 5 seconds.
This is mostly invisible because send buffers rarely fill, but a
backpressure spike can push the actual ACK latency above the
`replication_timeout_ms` budget.

**Reproduction:**
Set `replication_timeout_ms = 500`, then drive a replica into a
backpressure state where the kernel send buffer fills. The
`send_batch` call will block for up to ~5 seconds (the connect
timeout default) before returning a write-timeout error.

**Suggested fix:**
Add a `set_write_timeout(Some(timeout))` call inside `send_batch`
just like `recv_ack` already does for the read timeout. Or add a
separate `replication_write_timeout_ms` config knob (and default it
to `replication_timeout_ms`).

---

### D-10: Wire protocol: `ReplicaOp::Create` is_external default is silently `false` on truncated payloads (LOW)

**Location:** `src/replication/protocol.rs:582-591`

**What:**
```rust
let is_external = if pos < rest.len() {
    let v = rest[pos] != 0;
    pos += 1;
    v
} else {
    false
};
```

A truncated `Create` op (one missing the trailing `is_external` byte)
silently decodes with `is_external = false`. The comment above
(`protocol.rs:582-584`) calls this "backward-compatible: if there is a
byte remaining, read is_external; otherwise default to false so old
replication streams still work."

**Why it matters:**
This silently masks a wire-protocol mismatch. An older master that
forgets to write the `is_external` byte when sending a create for an
external blob will create the record on the replica as **non-external**
— a meaningful semantic difference, since `is_external = true` triggers
blobstore-backed cold-data storage. The replica's record then has a
local copy of the cold data when it should be referencing an external
content-hash.

This is a strict V1 vs V2 discriminator that the encoding does not
make explicit. Compare with the `ReplicaBatch` decoder, which uses an
explicit version byte (`BATCH_PROTOCOL_V1` vs `V2`, protocol.rs:54-65)
and rejects unknown versions.

**Reproduction:**
Hand-construct a `Create` op without the trailing byte; deserialize it.
You get `is_external: false` even if the master meant `true`.

**Suggested fix:**
Bump the wire-format invariant: serialize an explicit
`is_external_present: u8` flag, or extend the `ReplicaOp` op-tag table
to a `Create_v2` opcode that always includes the byte. Reject truncated
old-format frames with `ProtocolError::BufferTooShort` so cross-version
mistakes surface immediately instead of silently corrupting the state.

---

### D-11: Catch-up has no rate limit; `run_catchup` can starve the live-replication path (MEDIUM)

**Location:** `src/replication/manager.rs:541-638`

**What:**
`run_catchup` iterates over every sender in `CatchingUp` state and, for
each, sends *all* outstanding ops in `catchup_batch_size`-sized chunks
in a tight serial loop:

```rust
for chunk in ops.chunks(batch_size) {
    let batch = ReplicaBatch { ... };
    if let Err(_e) = sender.transport.send_batch(&batch) { ... }
    match sender.transport.recv_ack(timeout) { ... }
    chunk_seq += chunk.len() as u64;
}
```

Two issues:

1. **No yield between chunks**: a replica that's 1M ops behind sends 1M
   ops as fast as the TCP link allows, blocking the calling thread for
   the entire catch-up window.
2. **Catch-up path runs on the same `ReplicaSender::transport` as the
   live-replication path**: while `run_catchup` is blocked sending
   chunk N, no live `replicate_batch` to the same sender can proceed.
   The live path will block for the entire catch-up duration.

The manager's parallel fan-out optimization (`std::thread::scope`,
manager.rs:368) helps live writes to **other** senders, but a sender
in catch-up is unavailable for live traffic until catch-up finishes.

**Why it matters:**
A replica that's been down for 10 minutes can have hundreds of
thousands of pending ops. With no rate limit, catch-up monopolizes the
sender's transport for the entire stream. Live writes to that replica
don't fail (they're not even attempted because the sender is in
`CatchingUp` state, manager.rs:374-376), but the master's view of
"replicas live" stays at N-1 until catch-up finishes.

**Reproduction:**
1. Start an RF=3 cluster.
2. Drop a replica for 5 minutes while traffic flows.
3. Reconnect; observe catch-up runs serially, and live-write
   replication to the recovering replica blocks until done.

**Suggested fix:**
1. Run catch-up in a *separate* worker thread per recovering replica,
   draining the redo log and forwarding to the replica without holding
   the manager's mutable `&mut sender` borrow.
2. Add a configurable rate cap (`catchup_max_ops_per_sec`) so the
   recovering replica's ingestion budget doesn't crowd out the live
   path.
3. Stream catch-up over a *separate* TCP connection to the same
   replica so live and catch-up traffic don't queue behind each other.

---

### D-12: `recv_ack` has no `Content-Length` upper bound check before allocating (LOW, DoS surface)

**Location:** `src/replication/tcp_transport.rs:188-196`

**What:**
```rust
let total_len = u32::from_le_bytes(len_buf) as usize;

if total_len as u32 > MAX_FRAME_SIZE {
    return Err(ReplicationError::Transport(format!(
        "response frame too large: {total_len}"
    )));
}

let mut body = vec![0u8; total_len];
```

`MAX_FRAME_SIZE` is `16 * 1024 * 1024` (16 MiB,
`opcodes.rs:324`). A malicious replica can advertise a length up to
that and force the master to allocate 16 MiB before it discovers the
body is bogus. With many replicas and connection multiplexing this is a
real RAM-amplification attack vector.

The same pattern repeats in the receiver (receiver.rs:243-251).

**Why it matters:**
A compromised replica (or an attacker who can MITM the unauthenticated
replication channel) can OOM the master via repeated 16 MiB
allocations. mTLS / cluster_secret authentication mitigates but does
not eliminate this because the upper bound is fundamentally
client-controlled.

The comment on `MAX_FRAME_SIZE` (`opcodes.rs:303-324`) acknowledges
the issue but the only mitigation in place is "the OS will OOM-kill the
process." That's not a mitigation, it's a failure mode.

**Reproduction:**
Connect to the receiver, send a length prefix of 0x00FFFFFF
(16 MiB - 1), then close the connection. The receiver allocates
~16 MiB before discovering the truncation. Repeat from many
connections in parallel.

**Suggested fix:**
1. Cap allocations to `min(MAX_FRAME_SIZE, expected_op_count *
   max_op_size + header_size)`.
2. For ACK frames specifically (which are tiny — 9 bytes for
   `ReplicaAck::Ok`, ~30 bytes for `ReplicaAck::Error`), drop the cap
   to a small constant (1 KiB).
3. Track allocation pressure; refuse new replication connections if
   accumulated outstanding allocations exceed a configurable budget.

---

### D-13: `replication_intent_tracker` startup recovery does not advance `next_sequence` — could race a fresh master (LOW)

**Location:** `src/server/dispatch.rs:1420-1503`

**What:**
`recover_pending_replication_intents` reads pending intent ranges,
re-replicates them, and clears the markers — *but* it does not
advance the master's `replication_manager.next_sequence`. The new
master takes over with `next_sequence = redo_log.current_sequence()`
(`bin/server.rs` install path), but during the recovery window
the manager's sequence counter is whatever was loaded.

If the recovery succeeds and clears the marker but a fresh client
write arrives before `next_sequence` is advanced past the recovered
range, the new client write reuses an already-applied sequence.

**However**: the `with_initial_sequence_and_cluster_key` constructor
already syncs from the redo log on startup, and the recovery path runs
before client traffic is accepted (`bin/server.rs:719-744` is in the
gate before the listener is announced). So the race is **closed by
construction** as long as the startup ordering is preserved.

**Why it still matters:**
The ordering is fragile: `recover_pending_replication_intents` is
called in `bin/server.rs:722`, but the listener bind in the same file
happens later. A future refactor that interleaves these (e.g., to start
serving health probes earlier) would expose the race. There is no test
that locks the ordering.

**Reproduction:**
Modify `bin/server.rs` to start the listener before the recovery
loop, send a write, observe the duplicate-sequence write applied
twice (or, more likely, the receiver's dedup catches it but only
because the master happened to emit the right `source_node_id`).

**Suggested fix:**
Add a barrier in `bin/server.rs` that explicitly asserts the recovery
loop has completed before the listener is bound, and document the
ordering invariant. Also consider re-loading `next_sequence` from the
redo log after recovery completes.

---

### D-14: No bounded backpressure between dispatch and replication — large write bursts can OOM the replication runtime (LOW)

**Location:** `src/server/dispatch.rs:76-79, 1285-1314`

**What:**
The replication fan-out runs through `REPL_RUNTIME` (a static
`tokio::runtime::Runtime`) using `block_on(async { ... })`:

```rust
static REPL_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(num_cpus::get().max(4))
        .build()
        .unwrap()
});
```

Each `replicate_all_ops` call spawns one `tokio::task::spawn_blocking`
per replica. There is no permit pool or queue; every concurrent client
mutation that hits replication contributes more spawn_blocking tasks.
Tokio's blocking thread pool defaults to 512 threads — a write burst
that exceeds 512 concurrent replications will block subsequent calls
on the spawn-blocking queue, but there's no upper bound on memory.

Each `ReplicaBatch` carries its `ops: Vec<ReplicaOp>` cloned per
target (`dispatch.rs:1193`):
```rust
by_addr.entry(addr).or_default().extend(ops.clone());
```

A 1 MiB batch with 10 replica targets allocates 10 MiB. Multiply by
concurrent in-flight batches and the working-set grows fast.

**Why it matters:**
This is not currently exploitable because client connections are
themselves capped (`max_connections = 1024` default, config.rs:424),
but the per-connection write rate is not capped. A client that pumps
spends as fast as the protocol allows can buffer arbitrarily large
working sets in the replication runtime.

**Reproduction:**
Open one TCP connection, pump `OP_SPEND_BATCH` requests as fast as
possible without waiting for ACKs (the client doesn't have to wait —
the server processes them sequentially per connection but each one
fans out concurrently). Observe RSS grow.

**Suggested fix:**
1. Bound `replicate_all_ops` calls in flight via a `Semaphore` keyed
   to the runtime's worker count.
2. Drop the per-target `ops.clone()` by sharing an `Arc<Vec<ReplicaOp>>`
   inside the `ReplicaBatch` struct. The batch is read-only once
   constructed.
3. Add a metric that exports outstanding-replication memory so the
   issue is observable.

---

### D-15: `ReplicaOp::Create` `master_generation` is `None` on the wire — replicas cannot detect stale `Create` after `Delete` (MEDIUM)

**Location:**
- `src/replication/protocol.rs:217-251` (`master_generation()`)
- `src/replication/protocol.rs:108-191` (op definitions)

**What:**
```rust
pub fn master_generation(&self) -> Option<u32> {
    match self {
        Self::Spend { master_generation, .. } |
        Self::Unspend { master_generation, .. } |
        ...
        Self::Create { .. } | Self::Delete { .. } | Self::PruneSlot { .. } => None,
    }
}
```

Mutation ops carry `master_generation` so the receiver's pre-apply
guard (`receiver.rs:721-731`) rejects out-of-order replays. But
`Create`, `Delete`, and `PruneSlot` skip this guard:

> "Ops without master_generation (Create, Delete, PruneSlot) skip this
> check; they rely on idempotency in their match arms instead."

For `Create`, the receiver checks `existing_create_payload_matches`
and falls back to delete-then-create on divergence
(`receiver.rs:649-663`). For `Delete`, the engine returns
`TxNotFound` for already-deleted records and the receiver maps that
to `Ok(())`.

The gap:
> Sequence: Create (gen=1) → Delete → Create (gen=3, payload differs).
> Replica receives them in order 3, 1.
> Op 3 creates the record at gen=3.
> Op 1 (master_generation = 1, but `master_generation()` returns
> None for Create) tries to create the same key — divergent
> `existing_create_payload_matches` returns `false` — so the replica
> calls `engine.delete()` then `engine.create(create_req)` with the
> *gen=1* payload. The replica's record now reflects the older create.

The receiver correctly maintains generation **after** a successful
mutation, but the *Create itself* has no generation guard. A
replication-layer reorder for Create+Delete sequences *can* leave the
replica diverged. The redo-log sequence number on the batch
(`first_sequence`) prevents the *batch* from going out of order, but
within a single batch the ops are applied in order — so this only
breaks if the same `tx_key` appears in two separate batches that
arrive out of order. The applied-tracker prevents that
(`receiver.rs:506-517`), so within the per-stream-key serialization
this is safe.

But: this only holds when `source_node_id` is set (D-05). Without it
the per-IP:port stream key is unstable and the dedup tracker can be
bypassed.

**Reproduction:**
Test path: hand-construct two `ReplicaBatch`es with `source_node_id =
None`, send them on different TCP connections (different ephemeral
ports), arrange the second to arrive at the receiver first. Without
`source_node_id`, the dedup-by-stream key splits, and the receiver
applies both Creates.

**Suggested fix:**
1. Add `master_generation: u32` to the wire op for `Create` (probably
   stored in the metadata_bytes already — verify) and
   gate the create on it in `apply_op`.
2. Combined with the D-05 fix (require `source_node_id`), the
   end-to-end ordering would be sound regardless of TCP-ephemeral-port
   churn.

---

### D-16: `validate_cluster_safety` rejects `best_effort + RF>1` but doesn't reject `auto` mapping to `WriteAll` then degrading silently (LOW)

**Location:** `src/config.rs:522-533, 491-503`

**What:**
`resolved_ack_policy` returns `Some(WriteAll)` for `auto + RF=2` and
`Some(WriteMajority)` for `auto + RF>=3`. `is_replication_best_effort`
is *only* `true` when `replication_degraded_mode == "best_effort"`.

The validator at `validate_cluster_safety` rejects
`best_effort + RF>1`. **Good.** But there's no validation that
`ack_policy = "best_effort"` doesn't quietly disable the policy
enforcement *separately* from `replication_degraded_mode`.

```rust
match self.ack_policy.as_str() {
    "best_effort" => None,    // <-- explicit best_effort returns None
    ...
}
```

When `ack_policy = "best_effort"` and `replication_degraded_mode =
"reject"`, the dispatch path looks like:
- `cluster.ack_policy()` → `None`
- `classify_replication_outcome(_, _, None, false)` → required = 0
- result: `FullAck` for any number of ACKs ≥ 0

So `ack_policy = "best_effort"` *alone* (without
`replication_degraded_mode = "best_effort"`) silently disables ACK
enforcement — but it *passes* `validate_cluster_safety` because
`replication_degraded_mode` is the gate, not `ack_policy`.

**Why it matters:**
Two config knobs that look like they mean different things — one
controls the policy threshold, the other controls failure handling —
but `ack_policy = "best_effort"` is functionally equivalent to
"no enforcement," same as `replication_degraded_mode = "best_effort"`.
An operator who reads "best_effort = log failures but don't fail the
client" (config.rs:355) and sets `ack_policy = "best_effort"` thinking
that's the *threshold* will get *both* behaviors silently.

**Reproduction:**
Set `replication_factor = 3`, `ack_policy = "best_effort"`,
`replication_degraded_mode = "reject"`. The validator passes. Drive a
write where 0 of 2 replicas ACK. The dispatch path returns `FullAck`
(treated as STATUS_OK to the client). Master crashes. Write is lost
on every replica. Replicas resync from a master that no longer has
the data.

**Suggested fix:**
1. Reject `ack_policy = "best_effort"` in `validate_cluster_safety`
   when `replication_factor > 1` AND `replication_degraded_mode !=
   "best_effort"`. Either both knobs agree on best-effort or neither
   does.
2. Or rename `ack_policy = "best_effort"` to something less
   inviting like `ack_policy = "fire_and_forget"` so the operational
   tradeoff is immediately legible.

---

### D-17: `apply_create_replica` divergent-duplicate path silently re-creates without verifying the new payload's intent (LOW)

**Location:** `src/replication/receiver.rs:646-664`

**What:**
```rust
fn apply_create_replica(...) -> std::result::Result<(), String> {
    match engine.create(create_req) {
        Ok(_) => {}
        Err(CreateError::DuplicateTxId)
            if existing_create_payload_matches(...) => {}
        Err(CreateError::DuplicateTxId) => {
            // Existing record has a DIFFERENT payload → delete it & re-create
            match engine.delete(&DeleteRequest { tx_key: *tx_key }) { ... }
            engine.create(create_req)?;
        }
        Err(e) => return Err(...),
    }
    apply_create_lifecycle_and_blob(...)
}
```

If the receiver finds a record with the same `tx_key` but a divergent
payload (different metadata, different utxo_count, different hashes),
it **deletes the existing record and re-creates** with the master's
payload.

**Why it matters:**
This is the right behavior for normal divergence (master is the source
of truth). But:

1. The deleted record may have *committed* spends that the new create
   doesn't reflect (the master's `Create` payload says "5 unspent
   utxos" but the record on this replica had "5 spent utxos" because
   spends were applied before the master's `Create` arrived). The
   delete-then-create wipes the spend history.
2. The `cold_data` blob is **not deleted** when the existing record
   is replaced. `apply_create_replica` only stores cold_data if
   the new create has it (line 691-697). The orphaned blob remains.

(1) is theoretically impossible if `apply_op`'s generation guard
worked for `Create` — but it doesn't, see D-15. (2) is a real space
leak that grows over time.

**Reproduction:**
1. Replica has a record with `cold_data`.
2. Master sends a divergent Create (e.g., utxo_count differs) for the
   same `tx_key` *without* `cold_data`.
3. Receiver deletes-and-recreates. Old blob remains in the blobstore
   forever.

**Suggested fix:**
1. Add a delete-cold-data step to the divergent-duplicate path:
   `if let Some(bs) = engine.blob_store() { bs.delete(&tx_key.txid); }`
2. Combined with D-15 (Create generation guard), the divergent-duplicate
   case becomes much rarer.

---

### D-18: `ReplicationManager::run_catchup` `chunk_seq` may double-advance on partial-batch ack (LOW, latent)

**Location:** `src/replication/manager.rs:586-617`

**What:**
```rust
let mut ok = true;
let mut chunk_seq = from_seq;
for chunk in ops.chunks(batch_size) {
    let batch = ReplicaBatch {
        first_sequence: chunk_seq,
        ops: chunk.to_vec(),
        ...
    };
    if let Err(_e) = sender.transport.send_batch(&batch) {
        sender.state = ReplicaState::Down;
        ok = false;
        break;
    }
    match sender.transport.recv_ack(timeout) {
        Ok(ReplicaAck::Ok { through_sequence }) => {
            sender.last_acked = through_sequence;
        }
        _ => {
            sender.state = ReplicaState::Down;
            ok = false;
            break;
        }
    }
    chunk_seq += chunk.len() as u64;  // <-- advances regardless of ACK content
}
```

`chunk_seq` is incremented by `chunk.len()` regardless of what
`through_sequence` the replica returned. If the replica's
`last_applied` is **less** than `chunk_seq + chunk.len() - 1` (e.g.,
because the replica's tracker decided to skip some ops as duplicate),
the next chunk's `first_sequence` is wrong.

In practice the replica's ACK should always match the batch's
`last_sequence()` because of the dedup-skip-prefix logic, and
mismatches lead to an `Err(...)` path that breaks out of the loop. But
the code doesn't *check* this — `last_acked` is overwritten with the
replica's view, and `chunk_seq` advances on the master's view, which
could diverge silently.

**Why it matters:**
Latent. The current dedup logic prevents divergence in practice, but
the function would pass for an intentionally-broken receiver that
acknowledges fewer ops than received.

**Suggested fix:**
After `recv_ack`, validate `through_sequence == batch.last_sequence()`
and either fail-stop or trust the replica's view (advance
`chunk_seq` to `through_sequence + 1`). Currently it does neither
explicitly.

---

### D-19: `recover_pending_replication_intents` reads from the redo log without a lower bound — wrap-around can produce stale ops (MEDIUM)

**Location:** `src/server/dispatch.rs:1454-1494`

**What:**
```rust
for range in pending {
    let entries = {
        let log = redo_log.lock();
        log.read_from_sequence(range.first_sequence)
            .map_err(|e| format!("read redo for pending replication intent: {e}"))?
    };
    let entries: Vec<_> = entries
        .into_iter()
        .filter(|entry| {
            entry.sequence >= range.first_sequence && entry.sequence <= range.last_sequence
        })
        .collect();
    if entries.is_empty()
        || entries.first().map(|e| e.sequence) != Some(range.first_sequence)
        || entries.last().map(|e| e.sequence) != Some(range.last_sequence)
    {
        return Err(format!(
            "pending replication intent {}..{} cannot be resolved: redo entries missing",
            range.first_sequence, range.last_sequence,
        ));
    }
    ...
}
```

The check requires the recovered range to be exactly contiguous in the
redo log. Good. But:

* If the redo log is circular (per the `check_redo_truncation` helper
  at `durable.rs:553-565`), a range whose start has been overwritten
  is detected and reported, but as a hard error that prevents the
  master from starting.
* The recovery path has no fallback: there is no "skip this range and
  move on" or "this is too old to matter" branch. Startup blocks
  forever (the bin/server.rs retry loop, lines 720-740, retries
  for 60 seconds, then exits).

**Why it matters:**
A pending replication intent that survives a full redo-log
wrap-around (e.g., master crashed under heavy load while a 30-minute
write was in flight) bricks the master at startup. The operator's
only recourse is to manually delete the `repl-intent` file — which
loses the durability guarantee the file was tracking.

**Reproduction:**
1. Enable RF=2.
2. Start a write that records an intent.
3. Crash the master mid-replication.
4. Restart with a redo log small enough to wrap before recovery
   happens. The startup loop retries for 60s and exits.

**Suggested fix:**
1. Add an explicit "older than redo log floor → log warning, clear
   marker, surface a metric" path. Replicas that need the lost data
   will resync via the migration / catch-up paths.
2. Document the recovery-failure behavior in
   `recover_pending_replication_intents` — currently the function
   docstring says "If any range cannot be resolved, startup must
   fail closed." That's unambiguous but operationally hostile when
   the alternative is a single-line config knob.

---

### D-20: TLS / cluster-secret authentication is **not enforced** on the replication socket (HIGH, security)

**Location:** `src/replication/tcp_transport.rs:99-123`,
`src/replication/receiver.rs:142-198`

**What:**
The replication TCP transport opens plain TCP sockets. There is no
`cluster_secret` HMAC handshake on the replication channel. A cursory
inspection of the receiver's `start()` (`receiver.rs:142`) shows it
binds to a TCP port and accepts any caller. The opcode dispatcher
checks `request.op_code == OP_REPLICA_BATCH` and immediately calls
`handle_replica_batch_with_tracker`.

There is no auth check before the receiver starts mutating state.

The `cluster_secret` is documented in `config.rs:328-333` as:
> "all SWIM messages and inter-node TCP connections are
> authenticated. Peers that cannot produce a valid HMAC are rejected."

But:
```
$ rg -n "cluster_secret|hmac|auth" src/replication/
(no matches)
```

The auth mechanism applies to SWIM membership messages, not to the
replication channel.

**Why it matters:**
Anyone who can reach `replication_listen_port` can write arbitrary
ops as if they were the master. They can spend any UTXO, freeze any
slot, delete any record. The `enable_remote_bind = false` default
(config.rs:286) restricts this to localhost, so production clusters
that need remote access *must* be on a private network — but there
is no defense in depth.

**Reproduction:**
1. Bring up a replication receiver.
2. From an unrelated process on the same machine (or over the
   network if `enable_remote_bind = true`):
   ```rust
   let mut t = TcpReplicaTransport::connect("127.0.0.1:RPORT", ...)?;
   t.send_batch(&ReplicaBatch { ops: vec![ReplicaOp::Delete{...}], ... })?;
   ```
3. The receiver applies the delete. No auth, no signature, no
   cluster_secret.

**Suggested fix:**
1. Apply the same HMAC-SHA256 cluster_secret handshake to the
   replication socket as the SWIM messages.
2. Add a startup-time check: if `cluster_secret` is set but the
   replication transport doesn't enforce it, refuse to bind.
3. The medium-term fix is mTLS, but cluster_secret is the immediate
   defense.

This is in `docs/TERANODE_PRODUCTION_READINESS_GAPS.md` already (per
the `enable_remote_bind` docstring's reference) — verifying the
status of that work was outside this audit's scope.

---

### D-21: Receiver allocates a `Vec<u8>` per ACK frame inside the connection hot loop (LOW, performance)

**Location:** `src/replication/receiver.rs:251`

**What:**
```rust
let frame_len = total_length as usize;
let mut body = vec![0u8; frame_len];
```

— a fresh allocation per inbound batch. With realistic batch rates
(thousands per second), this is allocator pressure that buys nothing.

**Suggested fix:**
Reuse a single `Vec<u8>` across the loop:
```rust
let mut body = Vec::with_capacity(/* default size */);
loop {
    body.clear();
    body.resize(frame_len, 0);
    ...
}
```

A more thorough fix would use `BytesMut` or a slab allocator
shared across all replication connections.

---

### D-22: `lookup_before` ignores out-of-bounds before-image references silently (LOW, correctness)

**Location:** `src/server/dispatch.rs:1683-1691`

**What:**
```rust
let lookup_before = |i: usize, j: usize| -> BeforeImage {
    if let Some((_, vec)) = before_images.get(i)
        && let Some(b) = vec.get(j)
    {
        *b
    } else {
        BeforeImage::None
    }
};
```

If the parallel-arrays invariant breaks (a programmer error in a
dispatch handler), `lookup_before` silently returns `BeforeImage::None`
and compensation downgrades to the no-image fallback. The
no-image-fallback emits *no* `Compensate*` redo entry (good — better
than a stale one), but the in-memory restore path runs anyway, using
`unwrap_or([0u8; 32])` (line 1857) or similar zeros for the missing
field.

For a `Reassign` rollback this writes a record with hash zero. For an
`UnsetMined` rollback it writes block_height=0, subtree_idx=0.

**Why it matters:**
The compensation fallback writes garbage data when a programmer error
occurs in the dispatch handler. The defensive code reads as if it's
"safe" but the result is data corruption (zero hashes, zero block
heights). The fact that *no* `Compensate*` redo entry is emitted means
that on a crash mid-rollback, recovery will not see the corruption,
but the local state still has zeroed slots until the next replication
arrives to overwrite them.

**Reproduction:**
A dispatch handler that captures `before_images_by_key.push((v.key,
vec![]))` (an empty before-image vec for an op that needs one) — this
is unenforceable today. The compensation runs with `BeforeImage::None`
and writes zeros.

**Suggested fix:**
1. Make `before_images` and `repl_ops` *the same `Vec`* by changing
   `replicate_all_ops` to take `&[(TxKey, Vec<(ReplicaOp, BeforeImage)>)]`.
   This makes the parallel-arrays invariant un-violatable by
   construction.
2. Failing that, add a debug-mode `assert_eq!(repl_ops[i].1.len(),
   before_images[i].1.len())` so the issue surfaces in tests.

---

## Items Not Conclusively Verified

1. **Receiver-side dedup across master process restarts** (claim 13):
   The receiver dedup uses `source_node_id` if present (D-05), and
   the master always sends it (`dispatch.rs:1297`). Combined with
   the persistent `ReplicaAppliedTracker`, this *should* be safe
   across master restarts. I did not exercise this in a live
   integration test; the only test that covers the persistence path
   is `applied_tracker_persistence_round_trip`
   (`durable.rs:983-998`), which doesn't use the actual receiver
   path.

2. **Master crash before ACK to client** (claim 9): Replication
   intent recovery (`recover_pending_replication_intents`)
   re-replicates pending ranges on startup. This *should* make
   client retries safe via dedup + idempotent ops. I did not
   exercise the actual crash-restart path; the existing
   `replication_intent_tracker_persistence_round_trip` test only
   covers the on-disk format.

3. **Out-of-order ACK arrival** (claim 7): The manager waits for
   every replica's ACK in `replicate_batch`
   (`manager.rs:300-484`). Each replica is sent the *same* batch in
   the *same* sequence (`first_sequence: self.next_sequence`). The
   replica applies in batch order and ACKs `through_sequence =
   batch.last_sequence()`. The dispatch path matches this. There is
   no scenario in the current code where ACKs can arrive
   out-of-order *to a single replica*, but I did not verify that
   ACKs from *different* replicas cannot interleave with subsequent
   batches incorrectly.

4. **Replica crashes mid-batch — partial state on master**
   (claim 6): I did not find a test that simulates a replica TCP
   disconnect after the master sent a batch but before the ACK is
   received. The `tcp_replica_timeout` test
   (`tests/replication_tcp.rs:709-752`) covers ACK timeout, but the
   master path treats this as a transport failure and marks the
   replica `Down` (`manager.rs:445-449`). Whether the master then
   correctly distinguishes "replica got the ops but couldn't ACK"
   from "replica didn't get the ops" — and the resulting recovery
   behavior — is not exercised.

5. **Cross-version compatibility of V1 frames** (claim 11): The V1
   decoder exists (`protocol.rs:786-801`) but the project never
   produces V1 frames; senders always emit V2. The compat path is
   asymmetric: a V1 sender talking to a V2 receiver works, but a V2
   sender talking to a V1 receiver fails because the V1 receiver
   sees an unknown version byte. I did not find a documented
   rollout plan; the `BATCH_PROTOCOL_V1` constant is preserved
   solely for the receiver-decode side.

6. **Config knob `replication_timeout_ms` impact across all
   handlers**: The dispatch layer's `replication_ack_timeout_for`
   (line 1404) is the only consumer. The manager-level
   `config.replication_timeout` is configured separately and is not
   the same value as `replication_timeout_ms` unless the caller
   wires them together. I did not verify both are kept in sync at
   all configuration entry points.

---

## Summary Table

| ID    | Severity | Title                                                                                          |
|-------|----------|------------------------------------------------------------------------------------------------|
| D-01  | HIGH     | `replica_lag_check_interval_secs` is dead code; `spawn_lag_monitor` never spawned              |
| D-20  | HIGH     | TLS / cluster_secret auth NOT enforced on replication socket                                   |
| D-02  | MEDIUM   | `WriteMajority` semantics — RF=2 effectively requires zero replica ACKs                        |
| D-03  | MEDIUM   | `AckTracker` 1-second flush window can lose ACKs on master crash                               |
| D-05  | MEDIUM   | Receiver stream-key uses TCP-peer-key fallback; ephemeral-port roll triggers re-replay         |
| D-06  | MEDIUM   | `replication_timeout_ms` silently overridden during migration                                  |
| D-11  | MEDIUM   | Catchup has no rate limit; can starve live replication path                                    |
| D-15  | MEDIUM   | `ReplicaOp::Create` lacks `master_generation` — Create+Delete reorder can diverge              |
| D-19  | MEDIUM   | Replication intent recovery hard-fails on redo wrap-around — bricks master                     |
| D-04  | LOW      | `is_connected` 1ms probe causes false positives on flaky links                                 |
| D-07  | LOW      | `apply_op` reads slot from device per Spend/Freeze — duplicated I/O                            |
| D-08  | LOW      | `STATUS_OK` wrapping `ReplicaAck::Error` payload is conflating                                 |
| D-09  | LOW      | `connect()` reuses connect timeout for write timeout                                           |
| D-10  | LOW      | `Create` `is_external` silently defaults to `false` on truncated payload                       |
| D-12  | LOW      | `recv_ack` allocates 16 MiB on attacker-controlled length prefix                               |
| D-13  | LOW      | Replication intent recovery does not advance `next_sequence` — startup-ordering fragility     |
| D-14  | LOW      | No bounded backpressure between dispatch and replication runtime                               |
| D-16  | LOW      | `ack_policy = "best_effort"` silently disables enforcement, bypassing `validate_cluster_safety`|
| D-17  | LOW      | Divergent-Create path doesn't delete orphaned cold-data blob                                   |
| D-18  | LOW      | `run_catchup` `chunk_seq` advances on master view; can diverge silently                        |
| D-21  | LOW      | Receiver allocates a `Vec` per inbound batch; allocator pressure                               |
| D-22  | LOW      | `lookup_before` silently degrades to zeros on parallel-array invariant violation               |

**Total: 2 HIGH, 7 MEDIUM, 13 LOW.**

The replication subsystem's *core* correctness is generally well-handled
— the fan-out, dedup, intent journaling, compensation, and idempotent
op-replay paths are reasoned about carefully and tested. The defects
cluster around (a) **observability holes** (D-01, D-14), (b)
**operational footguns** in config knobs that look like they enforce
behaviors they don't (D-02, D-06, D-16), (c) **performance
inefficiencies** that scale with load (D-04, D-07, D-11, D-21), and
(d) **security** (D-20). The two HIGH-severity items are both gaps
between documented intent and implementation, not bugs in the
implementation that's there.
