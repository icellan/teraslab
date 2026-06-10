# Category D — Replication audit (TeraSlab, HEAD 1e5659b)

> Tooling caveat: the `Read` tool intermittently returned CORRUPTED renderings of
> `src/server/dispatch.rs` (invented lines like "Simpler: recompute statuses…",
> duplicated line numbers, a wrong `rollback_spends` signature). This was caught
> and worked around by re-reading every load-bearing region with `awk`/`grep` and
> cross-checking symbol existence (`grep` returned exit-1 for the hallucinated
> symbols `handle_spend_batch_impl`, `let _unused`, `classify_replication_outcome_doc`).
> All findings below rest on `awk`/`grep`-confirmed text, not the corrupted Reads.

## Summary

The replication ACK-policy math, the master-side dual-write fan-out, the
WAL-first ordering with local-apply rollback, and the receiver apply/dedup path
are all CORRECT against current code. No money-loss defect found. The checklist
items resolve cleanly (details below). The single filed item (D-01) is LOW: a
metric-bookkeeping log message mislabels a non-best-effort PartialAck as
"(best_effort)".

## Checklist resolution (all VERIFIED-OK unless noted)

### ack_policy=auto resolution per cluster size — `config.rs:883-898`
- `RF 0|1 => None` (best-effort, no enforcement)
- `RF 2 => WriteAll`
- `RF >= 3 => WriteMajority`
- explicit `"best_effort" => None`; unknown string => `WriteAll` fallback
  (startup `validate_cluster_safety` rejects unknowns first).

### write_all returns success only after EVERY replica ACKs else REPLICATION_FAILED
- Manager: `target_successes = live_count` for WriteAll (`manager.rs:620-622`);
  loop returns Ok only at `successes >= target_successes` (627), else first error.
- Dispatch: `classify_replication_outcome` (dispatch.rs:1812-1836) — when
  `ack_count < required && !best_effort` returns `PolicyViolation{required}`,
  which `replicate_all_ops` maps to `Err(...)` (dispatch.rs:1595-1599) →
  `ServerError::Replication` → ERR_REPLICATION_FAILED.

### write_majority threshold + RF=2 rounding (the "2-of-2 or 1-of-2?" question)
- `required_replica_acks` (`manager.rs:76-85`): WriteMajority = `(rf/2 + 1) - 1`
  with `rf = replica_targets + 1` (master is one durable copy).
- **RF=2 (1 replica) ⇒ 1 replica ACK required ⇒ 2 total copies counting the
  master.** There is no weaker 1-of-2 option; "majority of 2" = master + 1.
- RF=3 ⇒ 1 replica ACK (2-of-3). RF=5 ⇒ 2.
- Tests assert exact counts, not is_ok(): `write_majority_threshold_consistency_rf2_through_rf7`
  (manager.rs:1520-1540), `required_ack_count_rf5_write_majority/_write_all` (2725/2744).

### best_effort never returns REPLICATION_FAILED
- `PolicyViolation` arm is guarded by `!best_effort` (dispatch.rs:1823), so in
  best-effort mode it is unreachable. Zero-ack best-effort returns
  `ZeroAckBestEffort` → `ReplicationOutcome::Degraded` → STATUS_DEGRADED_DURABILITY
  (dispatch.rs:1827-1829, 1615-1630). Some-but-not-all → `PartialAck` → STATUS_OK.

### replication_degraded_mode=reject actually rejects
- Yes. "reject" ⇒ `best_effort=false` ⇒ unmet policy ⇒ PolicyViolation ⇒ Err ⇒
  ERR_REPLICATION_FAILED, and the local mutation is rolled back (see below).
- Plus a STRONGER Phase-E invariant: during outbound migration, if the
  dual-write (new-master) set produces 0 ACKs the write is rejected even under
  WriteMajority unless best_effort (dispatch.rs:1574-1588).

### Is replication intent started BEFORE or AFTER local apply? (crash window)
- Order in `handle_spend_batch` (dispatch.rs:2814-2877):
  1. `engine.spend(...)` LOCAL apply (2819) — collects redo+replica ops only on success.
  2. `write_replicated_redo_ops` = redo append+fsync THEN `begin` replication
     intent (dispatch.rs:1287-1294; intent is persisted AFTER redo fsync but
     BEFORE replication, as the durable bridge). On redo failure ⇒
     `rollback_spends` + return error (2849-2862).
  3. `replicate_all_ops` (2866). On Err ⇒ `rollback_spends` +
     `clear_replication_intents_after_compensation` + `ServerError::Replication`
     (2869-2876).
  4. Build response (2879-2880).
- So: LOCAL apply happens first, but is COMPENSATED (engine.unspend + compensating
  Unspend redo op, `rollback_spends` dispatch.rs:1948-1971) on BOTH redo-failure
  and replication-policy-failure. A client that receives an error can safely
  retry. The redo log is WAL-first; recovery replays the durable intent.

### Master crash before client ACK → client retry safe
- Idempotence is two-layered: (a) the engine spend path is generation-aware and
  idempotent (receiver-side test `apply_spend_idempotent` manager mirror;
  `engine.spend` rejects already-spent with ALREADY_SPENT, a safe retry signal);
  (b) the master `ReplicationIntentTracker` (dispatch.rs:143-273) records the redo
  range as a pending intent before replication and reconciles on restart, and
  `next_sequence` is never reused (manager.rs:515-525). A retry lands on a new
  sequence range that the replica dedup tracker accepts.

### Replicas apply in master order despite out-of-order ACKs
- Each batch carries `first_sequence`; the receiver computes `skip_count` from the
  persisted `already_applied` high-water and applies only ops with seq >
  already_applied, in array order (receiver.rs:820-857). ACK arrival order on the
  master is irrelevant — the master counts successes per sender idx
  (manager.rs:648-694) and clamps `last_acked` to `next_sequence` (662-663).

### Replica crash mid-batch doesn't leave the master without a record
- Receiver applies ops sequentially; on ANY op error it returns
  `ReplicaAck::Error{failed_sequence}` WITHOUT advancing the applied cursor
  (receiver.rs:836-857) — so the master keeps retrying from that sequence and the
  replica cannot silently skip. The applied high-water is persisted+fsynced
  BEFORE the OK ACK is sent (receiver.rs:859-865), so an ACK is a true durability
  promise; a crash after apply-before-persist re-applies idempotently on replay.

### Does the replica apply path drop write_metadata errors with `let _ =` while ACKing success?
- NO. `apply_op` and `apply_create_lifecycle_and_blob` propagate
  `crate::io::write_metadata(...)?` (receiver.rs:1019, 1661). The only surviving
  `let _ =` in receiver.rs are on the ACK wire-response write (438, 521) and span
  parenting (760, 3708) — NOT the index/device apply path. The comment at
  receiver.rs:1001-1019 records that the previous `let _ =` swallowing was fixed.

### replica_lag_check_interval_secs triggers an action
- NOT located/read this session (config knob exists per recon at config.rs:763).
  Left UNVERIFIED — low money-safety relevance (it gates a warn-threshold metric
  per recon `replica_lag_warn_threshold_ops`), but flagged for completeness.

## FINDING

### D-01 (LOW) — PartialAck warn log is hard-coded "(best_effort)" even for non-best-effort policy paths
`src/server/dispatch.rs:1600-1611`. The `PartialAck` arm logs
`"replication: degraded ack (best_effort)"` unconditionally. But `PartialAck` is
ALSO the legitimate success outcome for a non-best-effort `WriteMajority` policy
that got quorum-but-not-all (e.g. RF=3, 2/3 ACKs — `classify_replication_outcome`
returns PartialAck at 1831 because `ack_count(2) >= required(1)` and
`ack_count < total_targets(2)`... actually total_targets here is replica count).
For a non-best-effort WriteMajority that legitimately met quorum, this still emits
a log line tagged "(best_effort)", which is misleading for operators triaging
durability. Behaviour is correct (returns Full/STATUS_OK); only the log label is
wrong. Fix: branch the message on the `best_effort` flag (already in scope at
dispatch.rs:1591) or drop the parenthetical.
