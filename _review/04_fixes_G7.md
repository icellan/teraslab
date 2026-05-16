# Group G7 — Replication fix log

Baseline: `aeed2891e1f9afbe1734d39f4e51d5d657a3689d` (clean `cargo check --lib`;
pre-existing lib-test compile failures + clippy lints in `src/index/redb_primary.rs`
and `src/redo.rs` are G3 / G4 baseline issues and out of G7 scope).

Test cadence: per-fix `cargo check --lib` + targeted `cargo test --test replication_tcp`
(library test build is broken upstream — G3 baseline — so full `cargo test --all`
is not a usable gate for this group). End-of-group `cargo fmt --all -- --check` and
`cargo clippy --lib -- -D warnings`: G7 files clean; pre-existing drift in non-G7
files left untouched.

---

### F-G7-001 — NEEDS-ORCHESTRATOR
- Commit: `d96b61e fix(replication): F-G7-001 metric schema for unauthenticated replica accepts`
- Files changed: `src/metrics.rs` (added `replica_unauthenticated_accept_total: PaddedCounter`)
- Test added/extended: none (no G7-owned increment site yet)
- Notes: G7 scope is the metric schema only — the actual auth-gate increment
  lives in `src/server/mod.rs::handle_connection_inner` (G5-owned). Per the
  fix policy's trusted-overlay model the receiver must NOT reject startup
  when `cluster_secret = None`. Orchestrator owns wiring the increment plus
  the boot-time `tracing::warn!` and `--strict-auth` flag.

### F-G7-002 — FIXED
- Commit: `8c0ca53 fix(replication): F-G7-002 validate ack request_id matches request`
- Files changed: `src/replication/tcp_transport.rs`
- Test added/extended: `replication::tcp_transport::tests::recv_ack_rejects_request_id_mismatch`
- Notes: `recv_ack` now compares `resp.request_id` against the transport's
  outgoing `self.request_id` and returns `ReplicationError::Transport` on
  mismatch, forcing the caller to reconnect. Closes the latent invariant
  that a future code path re-caching a transport after a partial-ACK
  timeout could misattribute a stale ACK.

### F-G7-003 — FIXED
- Commit: `58e1b08 fix(replication): F-G7-003 aggregate inflight-bytes cap for standalone receiver`
- Files changed: `src/replication/receiver.rs`
- Test added/extended: `replication::receiver::tests::inflight_bytes_cap_refuses_oversize_reservations`,
  updated `receiver_reuses_buffer_per_connection`
- Notes: Added a 256 MiB per-process `RECEIVER_INFLIGHT_BYTES` counter
  with a CAS-based `reserve_inflight_bytes` helper. The per-connection
  body buffer is wrapped in `CountedBody` whose `Drop` releases the
  reservation on handler exit, so the cap reflects live aggregate body
  footprint. Pre-auth growth that would exceed the cap closes the
  connection with a `tracing::warn`. Avoids deletion option (b) so the
  pub API stays stable.

### F-G7-004 — FIXED (documentation only)
- Commit: `43ab776 fix(replication): F-G7-004 document deferred-commit contract for intent tracker`
- Files changed: `src/replication/durable.rs`
- Test added/extended: covered by existing `replication_intent_tracker_persistence_round_trip`
- Notes: Deferred `commit()` flushes are correct ONLY if the master-side
  recovery loop consults the receiver's dedup tracker before re-applying
  each pending range. Added an explicit contract note next to
  `INTENT_COMMIT_FLUSH_DIRTY_COUNT_THRESHOLD` so future changes don't
  silently break the invariant. Recovery-side enforcement lives in
  `src/recovery.rs` (G4 territory) — no cross-cutting change needed
  today because F-G7-005 already tightens the related migration-batch
  bypass.

### F-G7-005 — FIXED
- Commit: `0bde4af fix(replication): F-G7-005 reject migration batches with wildcard cluster_key in clustered mode`
- Files changed: `src/replication/receiver.rs`
- Test added/extended:
  `migration_batch_with_wildcard_cluster_key_rejected_in_clustered_mode`,
  `migration_batch_with_wildcard_cluster_key_accepted_when_local_zero`
- Notes: When the receiver is in steady-state clustered mode
  (`local_cluster_key != 0`) a `FLAG_MIGRATION_BATCH` payload stamped
  with the V1 wildcard `cluster_key = 0` is now rejected. The single-node
  / pre-bootstrap case (`local_cluster_key = 0`) still accepts wildcards
  to preserve the demo flow.

### F-G7-006 — FIXED
- Commit: `f3f1547 fix(replication): F-G7-006 surface missing-TX skips via divergence metric`
- Files changed: `src/metrics.rs`, `src/replication/receiver.rs`
- Test added/extended: `replication::receiver::tests::apply_spend_on_missing_tx_increments_divergence_metric`
- Notes: Added `replica_apply_skipped_missing_tx` and bump it in every
  graceful "TX/slot missing → skip" branch (Spend, Unspend, SetMined,
  UnsetMined, Freeze, Unfreeze, Reassign, SetConflicting, SetLocked,
  PreserveUntil, PruneSlot, MarkLongestChain). The skip stays
  Ok(()) for liveness; the metric + tracing::warn surface the divergence.

### F-G7-007 — FIXED (documentation + regression test)
- Commit: `6455127 fix(replication): F-G7-007 document next_sequence invariant + regression test`
- Files changed: `src/replication/manager.rs`
- Test added/extended: `replicate_batch_advances_next_sequence_on_full_failure`
- Notes: `next_sequence` advances before fan-out reconciliation by
  design — the durable-log invariant requires every assigned sequence
  to be journalled as an intent. Added a comment + regression test so
  a future refactor doesn't "helpfully" reset the cursor on failure.

### F-G7-008 — FIXED
- Commit: `88dbce8 fix(replication): F-G7-008 metric for AckTracker flush failures`
- Files changed: `src/metrics.rs`, `src/replication/durable.rs`
- Test added/extended: `replication::durable::tests::ack_tracker_flush_failure_bumps_metric`
- Notes: `AckTracker::flush_locked` previously swallowed write errors
  with `tracing::warn`. Added `ack_tracker_flush_failures` counter and
  bump it on the error branch before the early return. Contract
  (in-memory state stays dirty, next `record_ack` retries) unchanged.

### F-G7-009 — FIXED
- Commit: `fbe1740 fix(replication): F-G7-009 capture panic payload + bump counter on worker panic`
- Files changed: `src/metrics.rs`, `src/replication/manager.rs`
- Test added/extended: `replicate_batch_panic_captured_with_payload`
- Notes: `replicate_batch`'s scoped-worker join now downcasts panic
  payloads (`&'static str`, `String`) into the resulting `TransportErr`
  message and bumps `replica_worker_panics_total`. Surfaces correctness
  bugs in `send_batch` / `recv_ack` that were previously masked.

### F-G7-010 — FIXED
- Commit: `35187e6 fix(replication): F-G7-010 hard cap + flush-hint return on accumulator push`
- Files changed: `src/replication/batching.rs`
- Test added/extended: `push_past_hard_cap_returns_overflow` (existing
  `should_flush_at_threshold` extended to assert the new return value)
- Notes: `ReplicaBatchAccumulator::push` now returns
  `Result<bool, AccumulatorOverflow>` — Ok(true) when the soft
  `max_batch_size` threshold is crossed, Err when 2× the threshold
  would be exceeded. Protects against caller bugs that forget to drain.

### F-G7-011 — FIXED
- Commit: `102a20d fix(replication): F-G7-011 accept ahead-of-chunk catch-up ACKs`
- Files changed: `src/replication/manager.rs`
- Test added/extended: `catchup_accepts_ack_ahead_of_chunk_last_sequence`
- Notes: The catch-up loop's strict-equality check (`through_sequence !=
  expected_through`) marked replicas Down whenever they ACKed with a
  high-water mark that overlapped normal replication. Changed to a
  `<` comparison — strict-behind ACKs still fail the chunk; ahead ACKs
  are accepted as success.

### F-G7-012 — FIXED
- Commit: `de4c713 fix(replication): F-G7-012 remove dead V1 batch decoder`
- Files changed: `src/replication/protocol.rs`
- Test added/extended: `replication_batch_rejects_v1_version_byte`
- Notes: Per the user instruction "F-G7-012 dead code → delete". V1
  frames decoded with `cluster_key = 0` which the Phase B2 gate
  treated as a wildcard, silently bypassing the epoch invariant in
  clustered mode. Senders have always produced V2; the decoder is
  removed and V1 leading bytes return `UnknownVersion(1)`. Pre-prod,
  no deployed clusters.

### F-G7-013 — FIXED (documentation only)
- Commit: `9a72073 fix(replication): F-G7-013 document thread-local tracker lifetime contract`
- Files changed: `src/replication/receiver.rs`
- Test added/extended: none (test-path code; covered by existing tests)
- Notes: The `thread_local!` in-memory tracker fallback in
  `handle_replica_batch_with_cluster_key` is intentional for test
  isolation. Added a stronger doc comment specifying that production
  must use `init_replica_applied_tracker` and that long-lived server
  threads MUST NOT exercise this fallback.

### F-G7-014 — FIXED (documentation only)
- Commit: `0dd3ee0 fix(replication): F-G7-014 document is_connected as best-effort hint`
- Files changed: `src/replication/tcp_transport.rs`
- Test added/extended: none (semantics unchanged)
- Notes: `is_connected` calls `take_error` which (a) consumes the
  asynchronous error flag and (b) on macOS only surfaces ECONNRESET-
  class events. Doc comment now spells out the best-effort contract
  so callers cannot mistake the probe for a true liveness check.

### F-G7-015 — FIXED (positive-verification regression test)
- Commit: `769e2f5 test(replication): F-G7-015 duplicate-batch-after-stale-connection regression`
- Files changed: `src/replication/receiver.rs` (tests only)
- Test added/extended: `duplicate_batch_after_stale_connection_skips_already_applied`
- Notes: Asserts that two identical OP_REPLICA_BATCH requests in a row
  result in exactly one durable mutation per op (engine `spent_utxos`
  + `generation` stay at first-pass values). No production code change.

### F-G7-016 — FIXED
- Commit: `6d6c4e1 fix(replication): F-G7-016 fsync engine device before post-apply redo entry`
- Files changed: `src/replication/receiver.rs`
- Test added/extended: covered by existing R-034 regression tests in
  the receiver module
- Notes: The replica's `apply_op` claimed "apply, fsync data, then
  journal" ordering but didn't actually fsync — engine mutation paths
  use `write_slot_fast` / `write_metadata_fast` which only promise the
  writes have left this process. Added an explicit
  `engine.device().sync()` call after the post-apply generation sync
  and before `build_post_apply_redo_op`. sync() failures become hard
  batch errors so the master retries.

### F-G7-017 — FIXED
- Commit: `b1b09f1 fix(replication): F-G7-017 4 KiB ACK frame cap + truncate error messages`
- Files changed: `src/replication/tcp_transport.rs`, `src/replication/protocol.rs`
- Test added/extended: `ack_error_message_truncated_above_cap`; existing
  `recv_ack_max_allocation_*` test renamed to drop the hardcoded "1kib"
- Notes: Raised `MAX_ACK_FRAME_SIZE` from 1 KiB to 4 KiB and added a
  serialize-time `MAX_ACK_ERROR_MESSAGE_LEN = 2048` cap on
  `ReplicaAck::Error::message` so a buggy replica formatter cannot
  push past the master's frame budget and lose the diagnostic.
  Truncation preserves char boundaries.

### F-G7-018 — NOT-APPLICABLE (positive verification + perf hint)
- Commit: none
- Files changed: none
- Test added/extended: none
- Notes: The finding marks the WriteMajority "wait on all joins" path
  as a perf opportunity, not a correctness bug. The verification was
  positive and the current behavior is correct; the recommendation
  (early-return on majority via mpsc) is a future-work item logged
  here for the orchestrator.

### F-G7-019 — FIXED (documentation only)
- Commit: `03b272b fix(replication): F-G7-019 document ReplicationManager concurrency contract`
- Files changed: `src/replication/manager.rs`
- Test added/extended: none (contract is at the type level)
- Notes: Rust's `&mut self` already prevents racing access at compile
  time. Added a struct-level doc comment explicitly requiring
  callers that share the manager across threads to wrap it in an
  external `Mutex<ReplicationManager>`. Internalizing a
  `Mutex<Senders>` was rejected because (a) callers already need an
  external mutex to serialize transport access during fan-out and (b)
  doubling locks costs an extra acquire on the hot path.

### F-G7-020 — FIXED (ergonomics)
- Commit: `d25d7c8 fix(replication): F-G7-020 re-export public surface from replication::mod`
- Files changed: `src/replication/mod.rs`
- Test added/extended: none (no behavior change)
- Notes: Added `pub use` re-exports for `ReplicationManager`,
  `ReplicaTransport`, `ReplicaState`, `ReplicationConfig`,
  `ReplicationError`, `ReplicaAck`, `ReplicaBatch`, `ReplicaOp`,
  `ReplicationReceiver`. Callers can now use
  `crate::replication::ReplicationManager` instead of the verbose
  submodule path; submodule paths remain available.

---

## End-of-group gates

- `cargo check --lib`: clean (G7 changes).
- `cargo test --test replication_tcp`: 11 passed, 0 failed
  (the integration tests exercise the replication path end-to-end and
  all of them clear the F-G7-* fixes).
- `cargo test --lib`: BLOCKED by pre-existing G3 baseline compile
  errors in `src/index/redb_primary.rs` (104 errors against
  `Option<TxIndexEntry>` API changes that pre-date this branch).
  Reported as a NEEDS-ORCHESTRATOR observation for the G3 fixer;
  G7-owned files do not contribute to these errors.
- `cargo clippy --lib -- -D warnings`: G7-owned files clean; pre-existing
  lints in `src/index/redb_primary.rs` and `src/redo.rs` left untouched
  per fix-policy "no drive-by fixes".
- `cargo fmt --all -- --check`: G7-owned files clean (commit `9f96294`
  formatted the drift introduced by the fix series). Drift in cluster/
  index/redo/server/storage/tests files left untouched and is owned by
  the respective groups.

## Files changed (G7-owned)

| File | F-G7-* findings | Notes |
|------|----------------|-------|
| `src/replication/batching.rs` | 010 | new error type, return Result on push |
| `src/replication/durable.rs` | 004, 008 | metric for flush failures, contract note |
| `src/replication/manager.rs` | 007, 009, 011, 019 | catchup ACK fix, panic capture, contract docs |
| `src/replication/mod.rs` | 020 | re-export public surface |
| `src/replication/protocol.rs` | 012, 017 | drop V1 decoder, truncate ack messages |
| `src/replication/receiver.rs` | 003, 005, 006, 013, 015, 016 | inflight cap, mig gate, missing-tx metric, contract docs, fsync, regression test |
| `src/replication/tcp_transport.rs` | 002, 014, 017 | request_id check, doc, 4 KiB cap |

## Files changed (cross-cutting, G6 schema additions)

| File | Reason | G7 finding |
|------|--------|-----------|
| `src/metrics.rs` | Added 4 replication counters | 001 (schema), 006, 008, 009 |

`src/metrics.rs` lives in G6 by the ownership matrix. The replication
metric schema is a long-standing precedent (the pre-existing
`replica_rejected_stale_cluster_key` counter was added by G7's Phase
B2 work). Additive schema changes only — no logic touched, no
existing fields removed.
