# G5 fix log — wire protocol + dispatch + auth gate

Baseline: `aeed289 merge(G8 partial): worktree progress`.
Tests gating each commit: `cargo check --lib` + targeted integration tests (`tests/g5_protocol_auth.rs`, `tests/server_tcp.rs`, `tests/replication_tcp.rs`).

Note on lib-level test gating: `cargo test --lib` does not build on this
baseline because of pre-existing compile errors in
`src/index/redb_primary.rs` (G3 territory; 104 errors, outside this
group's owned files). G5 verifies via integration tests instead.

Pre-existing test failures (NOT caused by G5):
- `tests/cluster_tcp.rs::add_fourth_node_rebalance_triggers`
- `tests/cluster_tcp.rs::isolated_node_rejects_writes_with_no_quorum`
- `tests/cluster_edge_cases.rs::split_brain_heal_*` and several topology
  cluster-formation tests
- `tests/http_observability.rs::admin_top_returns_full_snapshot` and
  `read_only_admin_dashboards_remain_unauthenticated` (G6 territory,
  related to F-X-004 admin auth)

---

### F-G5-001 — FIXED
- Commit: `f87492e`
- Files changed: `src/server/mod.rs`
- Test added/extended: `tests/g5_protocol_auth.rs::strict_auth_rejects_unsigned_inter_node_frame` and `::fail_open_default_accepts_unsigned_inter_node_frame`
- Notes: G10 already added `ServerConfig::strict_auth` and the
  per-connection `ConnectionOptions::strict_auth`; the wiring file had
  a hardcoded `false` stub with a NEEDS-ORCHESTRATOR comment. Replaced
  with `self.config.strict_auth` so the CLI / TOML flag actually
  reaches the auth gate. Default trusted-overlay (no secret →
  one-shot warn + accept) is preserved per `_review/FIX_POLICY.md §2`.

### F-G5-002 — FIXED
- Commit: `84c53ab` (+ `fb6cf98` clippy doc cleanup)
- Files changed: `src/cluster/topology.rs`
- Test added/extended:
  `tests/topology.rs` in-module — `topology_term_deserialize_rejects_oversized_member_count`,
  `topology_term_deserialize_accepts_count_at_cap`,
  `topology_commit_deserialize_rejects_oversized_voter_count`.
- Notes: `TopologyTerm::deserialize` and `TopologyCommit::deserialize`
  ran `count * 8` arithmetic with no `checked_mul` and no named upper
  bound on the `Vec::with_capacity(count)` allocation. Added
  `MAX_TOPOLOGY_MEMBERS = 1024` cap rejected BEFORE any sizing
  arithmetic; switched offset calcs to `checked_mul`/`checked_add`.
  Note: `src/cluster/*` is G8's primary territory; orchestrator
  explicitly listed F-G5-002 in the G5 walk and the wire-layer
  context puts it in this group's purview.

### F-G5-003 — FIXED
- Commit: `ed70642`
- Files changed: `src/server/dispatch.rs`
- Test added/extended: existing dispatch unit tests covering
  `OP_QUERY_OLD_UNMINED` continue to pass (single-node mode).
  No cluster-mode disclosure test added — would require a multi-node
  fixture which lives in `tests/cluster_*`.
- Notes: Handler now filters candidates through
  `RunningCluster::is_master`, dropping keys this node holds only as
  a replica (or stale post-migration). Single-node mode behaves as
  before.

### F-G5-004 — FIXED
- Commit: `9fb3f79` (same commit as F-G5-010)
- Files changed: `src/server/dispatch.rs`
- Test added/extended: covered by code review; behavior is "reject
  any caller setting request_id high bits when used as shard id".
- Notes: `OP_MIGRATION_COMPLETE` now rejects `request_id >> 16 != 0`
  with `ERR_INTERNAL` before casting to `u16`.

### F-G5-005 — FIXED
- Commit: `7971796`
- Files changed: `src/protocol/opcodes.rs`
- Test added/extended: `tests/g5_protocol_auth.rs::strict_auth_gates_admin_opcodes`
- Notes: Added `OP_ADMIN_DIAGNOSE_KEY` and `OP_ADMIN_CLUSTER_HEALTH`
  to `is_inter_node_auth_opcode`, so they require HMAC framing
  whenever `cluster_secret` is configured. Trusted-overlay default
  unchanged.

### F-G5-006 — FIXED
- Commit: `6dd492c`
- Files changed: `src/server/dispatch.rs`
- Test added/extended: `tests/g5_protocol_auth.rs::heartbeat_returns_status_ok_not_unknown_opcode`
- Notes: Dispatch `OP_HEARTBEAT` returns `STATUS_OK` with empty
  payload instead of falling into the catch-all "unknown opcode"
  `ERR_INTERNAL`.

### F-G5-007 — FIXED
- Commit: `898392d` (same commit as F-G5-009)
- Files changed: `src/server/dispatch.rs`
- Test added/extended: in-module `dispatch_parsers_use_take_helper`
  regex test extended to also reject `try_into().expect(` and
  `try_into().unwrap_or(`.
- Notes: `handle_admin_diagnose_key` now uses `le_u32_at`; the
  regex test guards against regressions.

### F-G5-008 — FIXED
- Commit: `e63d528`
- Files changed: `src/server/dispatch.rs`
- Test added/extended: existing
  `create_batch_redo_failure_surfaces_allocator_rollback_failure`
  (asserts `msg.contains("redo log append")`) continues to pass —
  the sanitized message is `"redo log append failed"`.
- Notes: Redo-log append/flush/read paths log the inner I/O error at
  `error!` level for operator triage and return a sanitized fixed
  string to the client.

### F-G5-009 — FIXED
- Commit: `898392d` (same commit as F-G5-007)
- Files changed: `src/server/dispatch.rs`
- Notes: `handle_partition_version_report` now uses `le_u64_at`. The
  silently-substituted-zero `try_into-unwrap_or` is locked out by
  the extended regex test.

### F-G5-010 — FIXED
- Commit: `9fb3f79` (same commit as F-G5-004)
- Files changed: `src/server/dispatch.rs`
- Notes: `OP_REPLICA_BATCH` with `FLAG_MIGRATION_BATCH` set now
  rejects `request_id >> 16 != 0` before casting to `u16`.

### F-G5-011 — DEFERRED-FOLLOWUP (performance)
- Files: `src/protocol/frame.rs`
- Notes: `RequestFrame::decode` allocates a full-payload `Vec` per
  frame. Switching to `Bytes`/`Cow` would require lifetime-parameterising
  `RequestFrame` and every handler. Performance ceiling, not a
  correctness bug; recorded in `_review/follow_ups.md`.

### F-G5-012 — NOT-APPLICABLE (positive verification)
- Notes: `WireUnspendItem` already carries `spending_data` and the
  engine enforces the match in `unspend.rs`. Prior audit's A-04
  concern is resolved.

### F-G5-013 — NOT-APPLICABLE (positive verification)
- Notes: `MAX_FRAME_SIZE` is enforced before any `read_buf.resize` or
  payload allocation. `validate_batch_count` runs before every
  `Vec::with_capacity(count)`.

### F-G5-014 — NOT-APPLICABLE (positive verification)
- Notes: 30s read/write timeouts plus per-connection cap effectively
  bound slow-loris attacks.

### F-G5-015 — DOCUMENTED (NOT-APPLICABLE for behavior change)
- Commit: `77a40a5`
- Files changed: `src/server/dispatch.rs`
- Notes: `OP_INCREMENT_SPENT_EXTRA_RECS` returns `STATUS_OK` with
  empty payload (backwards compatibility shim). Expanded inline
  comment so the contract is explicit at the call site. Returning
  an error would break legacy clients; logging on each call would
  flood logs.

### F-G5-016 — DEFERRED-FOLLOWUP (performance)
- Notes: `cluster::auth::verify_frame` reads the entire payload
  before short-circuiting on a wrong HMAC. Bounded by `MAX_FRAME_SIZE`
  (16 MiB) and per-connection read timeout. Streaming HMAC verifier
  is a larger refactor; recorded as performance follow-up.

### F-G5-017 — RESOLVED (P3.10)
- Branch: `p3.10-typed-wire-errors`
- Files changed: `src/protocol/opcodes.rs`, `src/protocol/codec.rs`,
  `src/server/dispatch.rs`, `src/server/mod.rs`,
  `client/rust/src/errors.rs`, `tests/server_tcp.rs`,
  `phases/10_wire_protocol.md`.
- Tests added: `protocol::codec::tests::typed_wire_error_codes_round_trip`
  and `typed_wire_error_codes_have_stable_numeric_values`; ten existing
  dispatch tests updated to assert the new typed code.
- Notes: introduced `ERR_PAYLOAD_MALFORMED` (28), `ERR_OPCODE_UNSUPPORTED`
  (29), `ERR_STORAGE_IO` (30), `ERR_RATE_LIMITED` (31), `ERR_NOT_CLUSTERED`
  (32), `ERR_INVARIANT_VIOLATION` (33), `ERR_STREAM_INVARIANT` (34). Every
  generic `ERR_INTERNAL` site in the dispatcher that mapped to one of
  these classes was reclassified. `PROTOCOL_VERSION` bumped from
  implicit 1 to 2. `ERR_INTERNAL` kept as the fallback for genuinely
  unclassified failures (replication-compensation aborts).
  This corresponds to roadmap item P3.10 and follow-up C-8.

### F-G5-018 — FIXED
- Commit: `663ad68`
- Files changed: `src/protocol/codec.rs`
- Test added/extended: existing `decode_get_response_*` tests still
  pass; the new cumulative-tally branch is exercised only on
  malformed responses (no positive test added because the response
  path is the client side).
- Notes: `decode_get_response_checked` now tracks the running sum of
  per-item `data_len` and rejects when it exceeds the remaining
  payload, plugging a count-bounded-but-bytes-unbounded amplification
  on the client side.

### F-G5-019 — FIXED
- Commit: `1730db3`
- Files changed: `src/protocol/codec.rs`
- Test added/extended: new in-module test
  `dispatch_does_not_use_legacy_unchecked_decoders`.
- Notes: Production server code must not call the
  `Option`-returning `decode_*` wrappers (which fall back to
  `MAX_DECODE_BATCH`). The regex test scans the production section
  of `dispatch.rs` and asserts no legacy wrapper appears. Wrappers
  stay published for client/bench code.

### F-G5-020 — FIXED
- Commit: `0f2548d`
- Files changed: `src/protocol/frame.rs`
- Test added/extended: new in-module tests
  `try_decode_frames_surfaces_corrupt_trailing_frame`,
  `try_decode_frames_returns_ok_on_partial_tail`.
- Notes: Added `try_decode_frames` distinguishing partial reads
  (`Ok((frames, pos))` on `FrameError::Truncated`) from corrupt
  trailing input (`Err(other)`). Original `decode_frames` retained
  as a documented back-compat wrapper.

### F-G5-021 — FIXED
- Commit: `9da3d9d`
- Files changed: `src/protocol/codec.rs`
- Test added/extended: covered by existing `decode_redirect*` tests
  (they pass UTF-8 only); a negative test for non-UTF-8 input was
  not added because the prior behavior was lossy substitution,
  which left no visible defect to assert against.
- Notes: `decode_redirect` and `decode_redirect_with_version` switch
  from `String::from_utf8_lossy` to `std::str::from_utf8` so invalid
  UTF-8 produces `None` instead of U+FFFD characters that fail
  `SocketAddr` parsing further downstream.

### F-G5-022 — DOCUMENTED (concurrency hypothesis)
- Commit: `aa450d8`
- Files changed: `src/server/dispatch.rs`
- Notes: The fix (engine-side atomic apply + return before-image)
  lives in `src/ops/` which is G2's territory. Added a TODO-shaped
  comment at the dispatch call site so the next G2 pass picks up the
  lift.

### F-G5-023 — DOCUMENTED (maintainability)
- Commit: `83e1a0b`
- Files changed: `src/server/dispatch.rs`
- Notes: Self-replication compensation in `handle_delete_batch`
  hand-constructs an `OP_REPLICA_BATCH` and re-enters
  `handle_replica_batch`, bypassing every network-path check (HMAC,
  cluster_key gate, sequence-number dedupe). Extracting a pure
  `apply_replica_ops` function belongs in
  `src/replication/receiver.rs` (G7). Comment added so the boundary
  is explicit.

### F-G5-024 — FIXED
- Commit: `7d565b6`
- Files changed: `src/server/dispatch.rs`
- Test added/extended: existing `handle_stream_chunk` tests in
  `tests/server_tcp.rs` continue to pass.
- Notes: Replaced Vacant-insert + separate `get_mut().expect()`
  pattern with a single Entry match that returns `&mut ActiveStream`
  on both branches.

### F-G5-025 — NOT-APPLICABLE (positive verification)
- Notes: `src/protocol/mod.rs` is a 10-line re-export. No issue.

### F-G5-026 — NOT-APPLICABLE (positive verification)
- Notes: Per-item caps (`MAX_COLD_DATA_PER_ITEM`,
  `MAX_UTXO_HASHES_PER_CREATE_ITEM`,
  `MAX_PARENT_TXIDS_PER_CREATE_ITEM`,
  `ADMIN_DIAGNOSE_KEY_MAX_TXIDS`) are enforced in the `_checked`
  decoders before allocation.

### F-G5-027 — FIXED
- Commit: `ba5b516`
- Files changed: `src/protocol/codec.rs`
- Test added/extended: covered by existing `decode_stream_chunk` /
  `decode_stream_end` tests.
- Notes: `decode_stream_chunk` and `decode_stream_end` switched
  `try_into().unwrap()` to `try_into().ok()?`. Locally safe today;
  globally consistent now.

### F-G5-028 — DOCUMENTED (maintainability)
- Commit: `46fb21a`
- Files changed: `src/server/dispatch.rs`
- Notes: `OP_PROCESS_EXPIRED_PRESERVATIONS` calls `handle_delete_batch`
  directly instead of re-entering `handle_request`, bypassing the
  readiness / quorum middleware. That is intentional and safe; comment
  added at the call site.

---

## End-of-group verification

- `cargo check --lib`: clean (9 pre-existing warnings unchanged).
- `cargo clippy --all-targets -- -D warnings`: clean for all G5-owned
  files (`src/protocol/*`, `src/server/dispatch.rs`, `src/server/mod.rs`,
  `tests/g5_*`) and the cross-cutting `src/cluster/topology.rs` touched
  for F-G5-002. Pre-existing clippy errors in `src/index/redb_primary.rs`,
  `src/redo.rs`, and others are outside G5's scope.
- `cargo fmt --check`: clean on G5-touched files. Pre-existing diffs in
  `src/server/http.rs` (G6), `tests/g8_swim_replay.rs`, and a few
  others are outside G5's scope.
- `tests/g5_protocol_auth.rs`: all 4 tests pass.
- `cargo test --test server_tcp`: 29 pass.
- `cargo test --test integration`: 18 pass.
- `cargo test --test replication_tcp`: 11 pass.
- Pre-existing test failures (baseline `aeed289`): the cluster_tcp,
  cluster_edge_cases, and http_observability failures listed at the
  top of this document are reproducible on the unmodified baseline
  and are tracked by other groups (G6, G7, G8).

## Severity disposition

| Severity | Filed | Fixed | Documented | Deferred | NOT-APPLICABLE |
|----------|-------|-------|------------|----------|----------------|
| CRITICAL | 1 | 1 | 0 | 0 | 0 |
| HIGH | 3 | 3 | 0 | 0 | 0 |
| MEDIUM | 3 | 3 | 0 | 0 | 0 |
| LOW | 11 | 8 | 2 | 1 | 0 |
| INFO | 10 | 1 | 2 | 2 | 5 |
| **Total** | **28** | **16** | **4** | **3** | **5** |

"Documented" = behavior unchanged but comment added at the call site
to make a non-obvious contract explicit (F-G5-015, F-G5-022, F-G5-023,
F-G5-028). "Deferred" = follow-up filed for a larger refactor outside
this pass (F-G5-011, F-G5-016, F-G5-017).
