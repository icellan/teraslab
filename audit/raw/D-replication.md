# Audit D — Replication

Scope examined: `src/replication/{manager,durable,batching,mod,protocol,receiver,tcp_transport}.rs`,
replication call sites in `src/server/dispatch.rs`, `src/server/mod.rs`, `src/bin/server.rs`,
config in `src/config.rs`, tests `tests/replication_tcp.rs`, `tests/cluster_edge_cases.rs`,
spec `README.md`, `specs/BSV_UTXO_STORE_SPEC.md`, `phases/08_replication.md`.

Architectural note that frames most findings: there are **two replication implementations**.
`src/replication/manager.rs` (`ReplicationManager`, ~1,100 lines of senders/stragglers/catch-up)
is **not on the production path** — it is constructed only in test code
(`src/cluster/coordinator.rs:9750+` is inside `#[cfg(test)]`, plus its own unit tests).
Production replication is `replicate_all_ops()` in `src/server/dispatch.rs:1476`, which fans out
one `ReplicaBatch` per replica address via `send_replica_batch_to()` (dispatch.rs:2410) and counts
ACKs with `classify_replication_outcome()` (dispatch.rs:1828). The receive side is
`handle_replica_batch_with_tracker()` in `src/replication/receiver.rs:679`, dispatched from
`OP_REPLICA_BATCH` (dispatch.rs:519). All correctness analysis below is against the production path.

---

### [CRITICAL] Out-of-order batch delivery is silently dropped by the receiver's high-water-mark dedup — acked but never applied

**Location:** `src/replication/receiver.rs:835-865` (whole-batch skip and prefix skip),
`src/server/dispatch.rs:101` (`MAX_REPLICATION_FANOUTS_IN_FLIGHT = 128`),
`src/server/dispatch.rs:2403-2476` (`send_replica_batch_to`, per-address mutex).

**What's wrong:** The receiver deduplicates per source node (`effective_stream_key = "node:{id}"`,
receiver.rs:783-786) using a single high-water mark: if `batch.last_sequence() <= already_applied`
the entire batch is skipped and ACKed `Ok { through_sequence: already_applied }` (receiver.rs:835-844).
On the master, up to 128 dispatch threads run `replicate_all_ops` concurrently. Sequence ranges are
assigned in redo-mutex order, but the per-address connection mutex in `send_replica_batch_to` is
acquired in arbitrary order, so a batch with range 11–20 can reach the replica before the batch with
range 1–10. The replica applies 11–20 (HWM=20), then receives 1–10, sees `through=10 <= 20`, applies
nothing, and **ACKs success**. The master counts the ACK toward the policy, returns STATUS_OK to the
client, and clears the durable replication intent (`clear_replication_intents_after_success`,
dispatch.rs:1628/1648). Ops 1–10 never exist on the replica. The master-side `AckTracker` records
`through_sequence = 20`, so startup catch-up (`from = last_acked + 1`) will never resend them.

**Why it matters:** This is a silent, permanent, *acknowledged* divergence under nothing more exotic
than two concurrent client mutations. If the replica is later promoted (failover or migration), the
acked write is gone — e.g. a spent UTXO reappears as spendable. The receiver's own comment
(receiver.rs:942-946: "batches complete out of sequence order") acknowledges out-of-order completion
exists, yet the dedup treats sequence-coverage as proof of application. Note that `apply_op`'s
per-record generation guard (receiver.rs:1138-1158) would make *re-applying* the late batch safe —
the HWM skip is pure harm.

**Reproduction:** Two-node cluster, RF=2. Instrument or proxy the replication TCP link
(`tests/net_proxy`) to delay one connection's frame. Issue two concurrent single-key spends on
different keys from two client connections; delay delivery of the first-assigned batch until the
second is ACKed. Both clients get STATUS_OK. Read the first key on the replica: still unspent.
Alternatively a deterministic unit test: call `handle_replica_batch_with_tracker` with batch
(first_sequence=11, 10 ops) then batch (first_sequence=1, 10 ops) for the same `source_node_id`
and assert ops 1–10 were applied — it currently ACKs Ok and applies nothing.

**Suggested fix:** The receiver must enforce contiguity per stream: only skip an incoming op when
it is *individually* known-applied, and either (a) buffer/reorder batches whose `first_sequence >
already_applied + 1`, NACKing or parking until the gap fills, or (b) drop the HWM dedup entirely and
rely on the existing per-record generation guard for idempotency (re-apply is already safe).
Master-side, serializing sequence-assignment and send under one critical section per replica would
also restore order, at a latency cost.

---

### [CRITICAL] Catch-up off-by-one: every chunk after the first silently drops one operation

**Location:** `src/replication/durable.rs:815-827` (`run_catchup_for_replica`), contrast with the
correct test-only implementation `src/replication/manager.rs:1081-1082` (`chunk_seq =
through_sequence.saturating_add(1)`).

**What's wrong:**
```rust
let mut last_acked = from_seq;
for chunk in ops.chunks(batch_size) {
    let batch = build_catchup_batch(last_acked, chunk, local_cluster_key);
    ...
    Ok(ReplicaAck::Ok { through_sequence }) => { last_acked = through_sequence; }
```
The first chunk is correctly labeled `first_sequence = from_seq`. After its ACK,
`last_acked = from_seq + batch_size - 1` — the sequence of the **last op already applied**. The
second chunk is then labeled `first_sequence = last_acked` instead of `last_acked + 1`. The receiver
sees `first_sequence == already_applied`, computes `skip_count = 1` (receiver.rs:856-862), and
**drops the first op of every subsequent chunk** (a real, never-applied operation), while applying
the rest under labels shifted by −1. With the production call (`bin/server.rs:1026`,
`batch_size = 1000`), catch-up silently loses one op per 1,000 replayed.

**Why it matters:** Catch-up is the designated repair path for a lagging replica; it instead
introduces new holes while reporting success (`tracker.record_ack(addr, through)`). Silent
divergence on the recovery path is data loss precisely when durability is being re-established.

**Reproduction:** Unit test: replica receiver with persistent tracker; run
`run_catchup_for_replica(addr, 1, 2001, batch_size=1000, ...)` against ops 1..=2000 (e.g. 2000
distinct-key spends). Assert all 2000 ops applied — op 1001 (first op of chunk 2) will be missing.

**Suggested fix:** `build_catchup_batch(last_acked_plus_one, ...)` — i.e. track the next sequence
(`last_acked + 1`) as the chunk cursor, as `manager.rs:run_catchup` already does. Then deduplicate
this logic: two hand-rolled catch-up loops with different correctness is exactly the Rule-6 hazard.

---

### [CRITICAL] Per-replica batches are stamped with the master-global redo range but carry per-address subsets — the sequence space is fictional

**Location:** `src/server/dispatch.rs:1530-1542` (batch construction: `first_sequence:
redo_seq_range.0`, `ops` = that address's subset), `src/replication/receiver.rs:929`
(`applied.set(key, through)` with no contiguity check), `src/replication/durable.rs` (`AckTracker`
consuming these `through_sequence` values), `src/bin/server.rs:995-1060` (catch-up cursors derived
from them).

**What's wrong:** `build_replication_targets` groups ops by replica address; with ≥3 nodes (or any
multi-shard client batch) each address receives a *subset* of the redo range, yet every batch claims
`first_sequence = redo_seq_range.0` and the receiver computes `through = first_sequence +
ops.len() - 1`. These `through_sequence` values do not correspond to redo sequences actually applied.
Consequences: (a) the receiver's HWM is incomparable across batches, so the gap/dup logic of
receiver.rs:835-865 makes skip decisions on garbage; (b) the master's `AckTracker` persists fictional
positions, so startup catch-up (`from = last_acked + 1` in real redo space) replays from the wrong
point — mostly re-sending already-applied ops (absorbed by the generation guard) but with no
guarantee the truly-missing ranges are covered; (c) `spawn_lag_monitor` and `/health/ready` lag
computations compare a real redo sequence against these fictional ACK positions; (d) the receiver
can never detect a genuinely missed batch, because gaps between `already_applied` and an incoming
`first_sequence` are *normal* in this scheme and are silently jumped (receiver.rs:863-865, 929).

**Why it matters:** Sequencing is the backbone of the replication design (phases/08 specifies
redo-sequence-tagged batches and replica-side resume). As implemented, "replicas apply ops in master
order, gaps detected" is unenforceable: ordering violations (finding 1), missed batches, and holes
are all indistinguishable from normal operation.

**Reproduction:** 3-node cluster, RF=2. Send one client spend batch touching two keys whose shards
have different replica nodes. Capture both `ReplicaBatch` frames: both carry the same
`first_sequence` while each carries one op. Inspect `.repl-ack` afterwards: both replicas record the
same `through_sequence` although each applied a different op.

**Suggested fix:** Give each (master→replica) stream its own dense, per-stream sequence counter
(or send per-op real redo sequence numbers and make the receiver track applied ranges, not a single
HWM). The catch-up cursor must live in the same space as the ACKs.

---

### [HIGH] Replication-failure compensation is local-only: replicas that ACKed (or partially applied) keep the mutation — divergence with no runtime repair

**Location:** `src/server/dispatch.rs:1976-2378` (`compensate_replication_failure` — engine +
local `comp_redo` only), `dispatch.rs:2380-2401` (`compensate_replication_failure_or_error` clears
the intent ranges), call sites e.g. `dispatch.rs:3064-3084` (spend) and the other 13
`ERR_REPLICATION_FAILED` returns.

**What's wrong:** With RF≥3 and `write_all` (or any partial-ACK policy violation), some replicas ACK
and apply the batch while the policy as a whole fails. The master then rolls back its local state via
compensation ops, **clears the replication intents**, and returns `ERR_REPLICATION_FAILED` to the
client. The compensation ops are written only to the master's local redo log — they are not fanned
out to the replicas that already applied the original ops. The same applies to a replica that crashed
mid-batch after persisting a prefix (receiver fsyncs at batch end, receiver.rs:883-919, but applied
ops may reach disk anyway): the master compensates, clears intent, and never resends or reverses.
Because there is no runtime catch-up (see next finding) and the intent was cleared, the divergence
persists indefinitely; it would only coincidentally heal if the master restarts *and* startup
catch-up replays a redo window containing both the op and its compensation (and catch-up is itself
broken — findings 2 and 5).

**Why it matters:** The client was told the operation failed; a replica holds it applied. On
failover/promotion the "failed" mutation becomes authoritative — e.g. a spend the client believes
was rejected is permanently spent. This is the checklist's "replication failure after local commit
producing divergent replicas that are never repaired", answered: yes, it can, and they aren't.

**Reproduction:** 3-node cluster, RF=3, `ack_policy = "write_all"`. Partition one replica after the
other has ACKed (net_proxy: let replica A ACK, blackhole replica B). Client spend returns
`ERR_REPLICATION_FAILED`. Read the key: master unspent (compensated), replica A spent. Heal the
partition; observe no repair occurs while the master stays up.

**Suggested fix:** Either replicate compensation ops through the same fan-out (with at-least-the-
ACKed-set delivery before clearing the intent), or do not clear the intent after compensation —
convert it into an "anti-intent" that startup/runtime repair must push to all holders. Long-term,
this rollback-on-failed-quorum design is fragile; a commit-after-quorum (don't apply locally until
quorum ACK) or a true log-shipping design removes the compensation class entirely.

---

### [HIGH] Startup catch-up sends unauthenticated frames — it always fails in any cluster with a `cluster_secret`

**Location:** `src/replication/durable.rs:808-813` (`TcpReplicaTransport::connect(...)`, no auth);
`run_catchup_for_replica` has no auth-secret parameter at all; receiver gate
`src/server/mod.rs:787-898` (`auth_required = is_inter_node_op && cluster_secret.is_some()` →
unsigned `OP_REPLICA_BATCH` rejected; pinned by test `server/mod.rs:1216-1217`).

**What's wrong:** The steady-state path signs frames (`send_replica_batch_to` passes
`cluster.cluster_secret()`, dispatch.rs:1524/1544; `tcp_transport.rs:218-219` signs with
HMAC-SHA256). The only catch-up runner in production (`bin/server.rs:1026`) connects without the
secret, so every catch-up batch is rejected by the receiving node's auth gate. The failure surfaces
as `CatchupError::Transport`/`ReplicaError` — not `RedoReclaimed` — so it does not even trigger the
full-resync fallback (`bin/server.rs:1077-1093`); it just logs a warning and gives up.

**Why it matters:** README instructs production clusters to set `cluster_secret` (required under
`strict_auth`). In exactly those deployments, the *only* lag-repair mechanism is permanently
non-functional, and the failure mode is a single warn log at master startup.

**Reproduction:** 2-node cluster with `cluster_secret` set. Stop the replica, write 100 ops on the
master, restart the master (so its `.repl-ack` shows the replica behind), start the replica. Observe
`catchup: replica catch-up failed ... authentication` in master logs and the replica permanently
missing the 100 ops.

**Suggested fix:** Plumb the cluster secret into `run_catchup_for_replica` (use
`connect_with_auth`), and add an integration test asserting catch-up succeeds with `cluster_secret`
configured.

---

### [HIGH] `write_majority` is evaluated over the union of target addresses, not per key — a key can be acknowledged with zero replica copies

**Location:** `src/server/dispatch.rs:1560-1607` (`ack_count`/`total_targets` over `results`,
i.e. `by_addr.len()`), `dispatch.rs:1828-1852` + `src/replication/manager.rs:76-85`
(`required_replica_acks(total_targets, policy)`).

**What's wrong:** `total_targets` is the number of distinct replica *addresses* in the fan-out —
which, for a client batch spanning shards with different replica sets (any cluster ≥3 nodes), holds
disjoint per-key op subsets, and additionally includes dual-write-only migration targets. The
majority requirement is computed once over that union. Example, RF=2 + `ack_policy =
"write_majority"`: batch touches shard A (replica R1) and shard B (replica R2); `total_targets = 2`,
`required = required_replica_acks(2, WriteMajority) = (3/2+1)−1 = 1`. R1 ACKs, R2 fails → policy
satisfied, client gets STATUS_OK, intents cleared — but shard B's keys have **zero** replica copies.
Same shape for RF=3 (`auto` → WriteMajority): two ACKs that both belong to shard A's replicas
satisfy "majority" while shard B's key sits on the master alone. Dual-write extras inflate the
denominator further (partially mitigated by the separate ≥1-dual-write-ACK rule at
dispatch.rs:1584-1598, which protects only the migrating shard, not the quorum math of the rest).

**Why it matters:** The durability contract of write_majority is per write: each acknowledged
mutation survives loss of a minority. As implemented it is a batch-global popularity count; a master
crash loses acknowledged writes that the policy promised were majority-replicated.

**Reproduction:** Unit test on `classify_replication_outcome` semantics is insufficient (it is
"pure" but takes the wrong inputs); instead: 3-node cluster RF=2 `write_majority`, kill node R2,
send one client batch with one key replicated to R1 and one to R2. Expect ERR_REPLICATION_FAILED;
observe STATUS_OK. Then kill the master and verify R2's key is unrecoverable.

**Suggested fix:** Enforce the ACK policy per key (or per shard/replica-set): group results by the
replica set each key was sent to, and require the quorum within each set. Fail the batch (or the
affected items) when any key's set misses quorum.

---

### [HIGH] No runtime catch-up: lag repair runs exactly once, at master startup, bounded to 10,000 ops

**Location:** `src/bin/server.rs:968-1098` (one-shot detached thread, one pass per known replica,
`max_ops_per_pass = 10_000`, no loop, never re-invoked), `src/replication/manager.rs`
(`check_reconnected`/`run_catchup` — production-unused).

**What's wrong:** The only invocation of `run_catchup_for_replica` is a single pass per replica in a
thread spawned at startup. There is no periodic trigger, no trigger on replica reconnect, and no
trigger from the lag monitor. A replica that misses batches while the master stays up (transient
network blip shorter than the SWIM suspicion timeout, receiver error, timeout under load) is never
caught up until the *master* restarts. Even at startup, a replica more than 10,000 ops behind is
only partially repaired (the pass truncates to `max_ops_per_pass` and the loop moves on —
`durable.rs:803-806`), and the remaining lag waits for the next master restart.

**Why it matters:** In `reject` mode the client at least saw the failure (but see finding 4's
compensation divergence). For `write_majority`, partial ACK is *normal* (`PartialAck` → STATUS_OK,
dispatch.rs:1610-1630) — the non-ACKing replica falls behind by design and the system has no
mechanism to repair it during normal operation. Combined with finding 3 (gaps are undetectable
receiver-side), lag is both unrepaired and partly invisible.

**Reproduction:** 3-node RF=3 `write_majority` cluster. Pause one replica process (SIGSTOP) for 2s
during a write burst (short enough to avoid SWIM death/topology change), resume. Verify with
`/admin/replication` + direct reads that it never receives the missed ops while the master stays up.

**Suggested fix:** Drive catch-up from the lag monitor (it already computes per-replica lag every
`replica_lag_check_interval_secs`): when lag > 0 and the replica is reachable, run catch-up passes
in a loop until converged, with the auth fix from finding 5.

---

### [MEDIUM] Lagging-replica "action" is a warn log plus readiness flag — and it is computed over the fictional ACK sequence space

**Location:** `src/replication/durable.rs:857-887` (`spawn_lag_monitor` — `tracing::warn!` only),
`src/bin/server.rs:1258-1301` (wiring), `src/server/http.rs:1287-1328` (`/health/ready` degrades on
cached metric lag).

**What's wrong:** The config is no longer dead (R-038 fixed that; `bin/server.rs:1272` gates the
spawn on `replica_lag_check_interval_secs > 0` and RF > 1) — verified wired. But the only actions
are a warn log and HTTP readiness degradation; no catch-up, no quarantine, no resync request. And
both the monitor and `/health/ready` compare the master's real redo sequence against `AckTracker`
values that are fictional per finding 3, so the threshold (`replica_lag_warn_threshold_ops =
10_000`) fires on phantom lag in multi-shard clusters and can under-report real lag.

**Why it matters:** Operators reading "lag monitoring" in README will assume detection implies
eventual repair; neither detection accuracy nor repair holds.

**Reproduction:** 3-node cluster; steady writes across shards; watch `repl_*` lag gauges diverge
from true replica state (compare against per-replica record inspection).

**Suggested fix:** Fix the sequence space (finding 3), then make the monitor trigger catch-up
(finding 7).

---

### [MEDIUM] `best_effort` / `STATUS_DEGRADED_DURABILITY` machinery is unreachable in any valid production configuration; README documents it as live

**Location:** `src/config.rs:947-984` (`validate_cluster_safety` rejects `ack_policy =
"best_effort"` AND `replication_degraded_mode = "best_effort"` whenever RF > 1; enforced at
`bin/server.rs:294`), `src/server/dispatch.rs:1631-1646` (`ZeroAckBestEffort` → `Degraded`),
`README.md:389` (status table documents `DEGRADED_DURABILITY (5)`).

**What's wrong:** `Degraded`/`STATUS_DEGRADED_DURABILITY` requires `best_effort == true`, which the
startup validator forbids for RF > 1. For RF = 1 there are no replica targets, so `replicate_all_ops`
returns `NotApplicable` before classification. Therefore no validly-configured server can ever emit
`STATUS_DEGRADED_DURABILITY`, the `ZeroAckBestEffort` branch, the dual-write best-effort branch
(dispatch.rs:1585-1590), and the `repl_degraded_durability` metric are dead in production, and the
README/protocol doc advertises a status clients will never see. To be fair to the checklist item:
*as designed*, best_effort never returns REPLICATION_FAILED (with RF=1 replication isn't attempted
at all) — trivially satisfied because the mode is inert.

**Why it matters:** Conflicting contracts (config validation vs. README vs. dispatch logic) — Rule 6
material. Either the validator is right and the degraded path + docs should be removed, or
best_effort with RF>1 is a supported availability mode and the validator is wrong. Currently the
code "averages" both positions.

**Reproduction:** Attempt to start with `replication_factor = 2, replication_degraded_mode =
"best_effort"` → startup error (config.rs:965-973). Grep for any other producer of status 5: none.

**Suggested fix:** Pick one: delete the degraded-durability path and the README rows, or relax the
validator (e.g. allow with an explicit `--i-accept-data-loss` style override) and test the path
end-to-end.

---

### [MEDIUM] Dead/divergent code: `ReplicaBatchAccumulator` is unused; `ReplicationManager` is test-only; referenced test file does not exist

**Location:** `src/replication/batching.rs` (no production references — `grep ReplicaBatchAccumulator
src/` matches only the module itself), `src/replication/manager.rs` (production references only in
`#[cfg(test)]` code), `src/server/dispatch.rs:1889-1890` (comment: "Tests in
`tests/replication_rollback.rs` construct this variant" — no such file exists in `tests/`).

**What's wrong:** The batching accumulator (with its overflow hard-cap — the checklist's "durable
queue overflow" knob) protects nothing because nothing uses it. `manager.rs` (3,056 lines) carries
sophisticated, *tested* logic (correct catch-up increment, straggler reclamation, quorum math,
insufficient-replica gating) that the production path does not share — and where the two diverge,
production has the bugs (findings 2, 6). The `BeforeImage::Prune` rationale points at a test file
that doesn't exist, so the compensation path's claimed coverage is partly fictional (compensation is
exercised only via `tests/recovery_crash_boundaries.rs`).

**Why it matters:** Two implementations of the same invariants is how findings 2 and 6 happened.
Coverage claims referencing missing files hide real gaps.

**Reproduction:** `grep -rn ReplicaBatchAccumulator src/ | grep -v batching.rs` → empty.
`ls tests/replication_rollback.rs` → no such file.

**Suggested fix:** Either route production fan-out through `ReplicationManager` (one quorum/catch-up
implementation) or delete/demote the unused manager surface; restore or rewrite the rollback tests;
remove the accumulator or wire it.

---

### [LOW] Replication intent tracker: full-file rewrite + fsync on every mutation batch; unbounded pending set; deferred-commit window

**Location:** `src/replication/durable.rs:318-338` (`begin` → `write_locked` → `write_durable_file`
= write temp + fsync + rename + parent-dir fsync on **every** replicated client batch),
`durable.rs:340-367` (`commit` deferred up to 1 s / 100 commits — safe only because recovery re-send
is dedup/generation-absorbed, per the F-G7-004 comment at durable.rs:101-110).

**What's wrong:** Two extra fsyncs per mutation batch on the hot path of a system targeting 10M+
ops/sec; the pending `BTreeSet` and the on-disk file have no size bound (benign in `reject` mode
since intents clear synchronously, but a slow disk turns the intent write into the throughput
ceiling). The deferred commit is explicitly contingent on the receiver dedup tracker — which finding
3 shows tracks a fictional sequence space, weakening that contract's footing (re-applies are
still absorbed by the per-record generation guard, so this is hardening, not active loss).

**Reproduction:** Benchmark RF=2 spend throughput vs RF=1; profile fsync counts per op
(`strace -e fdatasync,fsync`).

**Suggested fix:** Append-only intent journal with periodic compaction instead of rewrite-the-world
per `begin`; document the generation guard (not the dedup HWM) as the recovery-replay safety
argument.

---

### [LOW] Transport hardening notes (HMAC present and correct on the steady-state path)

**Location:** `src/replication/tcp_transport.rs:218-219, 284-292` (sign/verify each frame),
`src/server/mod.rs:752-898` (streaming verify before body materialization — the pre-allocation DoS
called out in receiver.rs:32-33 is addressed; double length-prefix bug fixed in commit d39a612),
`src/config.rs:641` (`strict_auth` defaults to `false`).

**What's wrong (residual):** (a) With no `cluster_secret` and default `strict_auth = false`, replication
runs unauthenticated forever after a one-shot warn (`server/mod.rs:33`, 813-840) — documented
trusted-overlay stance, but worth restating: anyone who can reach port 3300 can inject
`OP_REPLICA_BATCH` mutations on such clusters. (b) HMAC gives integrity/authenticity only — no
confidentiality, and replay protection rests on the dedup/generation guards rather than a nonce.
(c) `AckTracker::flush` failures only warn + tick a metric (durable.rs:175-190): persistent ACK-state
staleness degrades future catch-up correctness silently.

**Reproduction:** n/a (design observations).

**Suggested fix:** Document (a)/(b) limits in README's security section; consider failing readiness
after N consecutive ack-tracker flush failures.

---

## Checklist disposition

| Item | Status | Evidence / Finding |
|---|---|---|
| `ack_policy = "auto"` resolution per cluster size | ✅ | `config.rs:912-928`: RF 0/1 → None (no enforcement), RF 2 → WriteAll, RF ≥ 3 → WriteMajority; matches the doc comment at config.rs:650-651. README lists the option but never specifies the mapping, so there is no README claim to contradict; phases/08:596 expectation (RF=2 majority = 1 replica ACK) also matches `required_replica_acks`. Unknown policy string falls back to WriteAll at runtime and is rejected at startup (config.rs:948-956). |
| `write_all`: every replica must ACK, else REPLICATION_FAILED | ✅ (per address) / ⚠️ (semantics) | Production: `required = total_targets` (manager.rs:78 via dispatch.rs:1835); `ack_count < required` → `PolicyViolation` → `Err` → `ERR_REPLICATION_FAILED` at 14 handler sites (e.g. dispatch.rs:3083). Caveat: "every replica" = every *resolved address in this batch's fan-out*; unresolved addresses fail resolution (dispatch.rs:1409-1441, 1501-1507) — correct. But the failure path then triggers the divergent local-only compensation (finding 4). |
| `write_majority` rounding, RF=2 | ✅ math / ❌ application | `required_replica_acks(1, WriteMajority)` = ⌈3/2⌉... = `2/2+1−1 = 1` replica ACK → 2-of-2 copies including master — correct strict majority, matches phases/08:596; RF=5 boundary pinned by `tests/cluster_edge_cases.rs:1334-1385`. However the quorum is applied to the union of addresses, not per key — finding 6 (HIGH). |
| `best_effort` never returns REPLICATION_FAILED | ✅ (vacuously) | Only valid with RF ≤ 1 (config.rs:974-982), where `replicate_all_ops` returns `NotApplicable` before any failure path. With RF > 1 the config is rejected at startup. The entire degraded machinery is unreachable — finding 9 (MEDIUM). |
| `replication_degraded_mode = "reject"` actually rejects | ✅ | `PolicyViolation` (best_effort=false) → `Err` → handler returns `ERR_REPLICATION_FAILED` after compensation (dispatch.rs:1606-1609, 3064-3084); pinned by dispatch tests around dispatch.rs:9781-9821 and 14114-14121. |
| Replica crash mid-batch: partial state recorded or idempotent whole-batch retry | ⚠️ | Master knows only batch-level outcome (single `ReplicaAck`). The good half: receiver advances its tracker only after device sync + redo flush (receiver.rs:898-940), and intent recovery re-sends whole ranges, absorbed idempotently by the dedup tracker + per-record generation guard (receiver.rs:1148-1158). The bad half: in `reject` mode the master *compensates and clears the intent* on timeout (dispatch.rs:2388-2391), so a partially-applied crashed replica is never retried OR reversed — finding 4. |
| Sequencing: master order enforced replica-side | ❌ | No contiguity enforcement; HWM dedup drops late out-of-order batches (finding 1, CRITICAL); per-addr subset batches make the sequence space fictional and gaps undetectable (finding 3, CRITICAL); catch-up chunk cursor off-by-one (finding 2, CRITICAL). |
| `replica_lag_check_interval_secs` action | ✅ wired / ⚠️ action | Spawned at bin/server.rs:1271-1301 (R-038 fixed prior dead-config state); action = warn log + `/health/ready` degradation only, over a fictional sequence space; no repair — finding 8 (MEDIUM). |
| Master crash before client ACK: retry safe? | ✅ | No request-level dedup — `request_id` is an echo only (no cache keyed by it anywhere in dispatch). Safety comes from: (1) durable intent `begin` before local apply (dispatch.rs:1296-1304, R-036) + startup barrier `recover_pending_replication_intents` (bin/server.rs:936-966, dispatch.rs:1674-1787) re-replicates before serving; (2) op-level idempotency, verified: spend re-issue with same spending_data counted idempotent (spend handler `idempotent` path), create dedup via `existing_create_payload_matches` (receiver.rs:963+), receiver generation guard. Retry of a mutation is therefore safe; retry observability caveat: a retried spend returns idempotent-success, indistinguishable from first-success (acceptable). |
| Stream HMAC/auth | ✅ steady-state / ❌ catch-up | HMAC-SHA256 per frame when `cluster_secret` set, streaming verify, unsigned inter-node frames rejected (server/mod.rs:864-898, test 1216). Catch-up path sends unsigned and is always rejected in secured clusters — finding 5 (HIGH). Unsecured default accepts unauthenticated forever after one warn — finding 11 (LOW, documented stance). |
| Rejoin: full resync vs incremental, decision correct? | ⚠️ decision / ❌ execution | Decision logic is sane: incremental redo replay from persisted `last_acked`; full-shard resync only when redo wrapped (`CatchupError::RedoReclaimed` → `ResyncRequest` → coordinator synthesizes migration tasks, coordinator.rs:1201-1216). Execution: runs once at startup only, capped at 10k ops, unauthenticated (findings 2, 5, 7), and its cursor lives in the fictional ACK space (finding 3). |
| Durable queue overflow | ⚠️ | The only bounded "queue" (`ReplicaBatchAccumulator`, hard cap 2×, typed overflow error) is dead code (finding 10). Intent tracker pending set is unbounded but self-limiting in reject mode; fan-out concurrency is bounded by the 128-permit gate (dispatch.rs:101-126). Intent `begin` is a full-file rewrite + fsync per batch — finding 12 (LOW). |
| Divergent replicas after replication failure, never repaired | ❌ | Yes — compensation is local-only and clears the intent (finding 4, HIGH); no runtime catch-up exists to heal it (finding 7, HIGH); receiver cannot even detect the divergence (finding 3). |
