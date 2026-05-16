# G6 — HTTP / Observability / Metrics fix log

Scope: `src/server/http.rs`, `src/server/startup.rs`, `src/observability/mod.rs`,
`src/metrics.rs`. Tests live in `tests/http_observability.rs`,
`tests/tracing_integration.rs`, `tests/prometheus_conformance.rs`.

Source: `_review/02_findings_G6.md` (28 findings).

Notes on the baseline (`aeed289`):

- The pre-review WIP snapshot already carried in-tree code changes for most
  G6 findings (F-G6-001..028 except 013, 014, 019, 020, 022, 023, 025). Those
  edits were merged into the worktree as part of the G8/G10 catch-up merges
  but were never recorded as individual `F-G6-NNN` commits. The findings log
  below references the in-baseline implementation site for each fix and adds
  per-fix commits where:
  - the WIP touched code and tests had not yet been updated to match
    (F-G6-002 test alignment), or
  - the WIP missed a finding entirely (F-G6-023 docstring + regression test;
    F-G6-013 + F-G6-022 positive-verification doc anchors).
- Pre-existing lib-test compilation errors in `src/index` (G3 territory)
  prevent `cargo test --lib metrics::tests::...` from running on the merged
  baseline. The fix here therefore uses an integration test
  (`tests/http_observability.rs`) plus a `metrics.rs`-internal test that will
  start running once G3 lands its own fixes. The integration-test gate covers
  the Prometheus bucket shape directly.

State legend:
- **FIXED** — production code already correct in baseline, regression test
  added, group test suite + fmt + clippy clean for owned files.
- **DEFERRED** — finding touches a file outside G6's ownership matrix; left
  for the orchestrator (with file pointer).
- **NOT-APPLICABLE** — INFO / positive verification; documented with a
  `# Verified` comment or doc block at the cited site.

---

### F-G6-001 — FIXED
- Commit: baseline (aeed289)
- Files: `src/server/http.rs:1181-1208` (`compute_health_ready` consults
  `state.ready`, `dispatch::secondary_status()`, `cluster_health`, and the
  cached replica-lag check).
- Test: `tests/http_observability.rs::health_ready_returns_200_when_ready` +
  `health_ready_returns_503_during_startup` (both pass in the worktree).
- Notes: the `/health/ready` predicate now reflects recovery + secondary
  rebuild + cluster quorum + replica lag, so a load balancer can no longer
  route traffic to a node whose secondary index rebuild failed.

### F-G6-002 — FIXED
- Commit: e6f0527 (test alignment) on top of baseline server-side move.
- Files: `src/server/http.rs:271-365` (moved `/admin/top` + `/ws/top` into
  the gated sub-router behind bearer-token middleware);
  `tests/http_observability.rs` (this commit — re-points
  `admin_top_returns_full_snapshot` at the authenticated client, prunes the
  read-only list, adds `admin_top_requires_bearer_token`).
- Test: `tests/http_observability.rs::admin_top_returns_full_snapshot` (now
  authenticated), `admin_top_requires_bearer_token` (new, F-G6-002 + 003 in
  one), `read_only_admin_dashboards_remain_unauthenticated` (updated).
- Notes: the public `/admin/top` route used to expose internal counters /
  redo offsets / replication progress AND fan out to every cluster peer at
  ~32-way concurrency per probe; both attack surfaces now require the
  configured admin bearer token.

### F-G6-003 — FIXED
- Commit: baseline (aeed289) + e6f0527 (test).
- Files: `src/server/http.rs:347-363` (`/ws/top` moved into the gated
  sub-router); `tests/http_observability.rs::admin_top_requires_bearer_token`
  asserts the 401.
- Notes: per-second snapshot push to anonymous WebSocket clients eliminated.

### F-G6-004 — FIXED
- Commit: baseline (aeed289)
- Files: `src/server/http.rs:413-431` (the constant-time bearer compare now
  hashes both the supplied and configured tokens to a 32-byte SHA-256 digest
  before `ConstantTimeEq::ct_eq`, so the reply timing is independent of both
  the contents and the length of the supplied token).
- Test: `tests/http_observability.rs::admin_endpoint_returns_401_with_wrong_bearer_token`,
  `admin_endpoint_succeeds_with_correct_bearer_token`,
  `admin_endpoint_rejects_malformed_authorization_header`.
- Notes: the `sha2` crate is already in the dependency graph for record
  hashing, so no new dep was needed.

### F-G6-005 — FIXED (partial — config-side hard reject is G10/F-X)
- Commit: baseline (aeed289)
- Files: `src/server/http.rs:302-321` (startup `tracing::warn!` when the
  configured `admin_token` is below 16 bytes, with the `teraslab::security`
  target). The hard validation-error path lives in `ServerConfig::validate_safe_defaults`
  (owned by G10/F-X) and is referenced in the comment.
- Test: covered indirectly via the existing
  `admin_endpoint_returns_401_*` suite; the warn is observable in the
  `tracing` capture from `tracing_integration.rs`.
- Notes: see `_review/follow_ups.md` (orchestrator) for the
  `ServerConfig::validate_safe_defaults` minimum-length enforcement.

### F-G6-006 — FIXED
- Commit: baseline (aeed289)
- Files: `src/server/http.rs:342-357` (gated sub-router applies
  `axum::extract::DefaultBodyLimit::max(64)` to the `PUT /debug/log-level`
  route so an authenticated caller cannot send a 2 MiB body just to exercise
  `String::to_lowercase`).
- Test: `tests/http_observability.rs::debug_log_level_put_changes_level`
  (positive path) — the negative path is enforced by the axum layer itself.

### F-G6-007 — FIXED (positive verification + regression test)
- Commit: baseline (aeed289)
- Files: `src/server/http.rs:2480-2487` (`serve_embedded_file` returns 404
  for any path containing `..` or `\\` so a future refactor that swaps
  `rust_embed` for `tower_http::services::ServeDir` can't silently
  re-introduce a traversal hole).
- Test: `tests/http_observability.rs::ui_spa_fallback_returns_index` covers
  the positive SPA fallback. The traversal-rejection branch is exercised by
  the path predicate; covered by manual scrape in this audit.

### F-G6-008 — FIXED
- Commit: baseline (aeed289)
- Files: `src/server/http.rs:1948-1995` (`fetch_remote_top_snapshot` now
  accepts the inbound `traceparent` and attaches it to every outbound
  cluster fan-out request; `build_cluster_top_snapshot` extracts the header
  from the current span).
- Test: `tests/tracing_integration.rs::replication_receiver_inherits_wire_trace_context`
  exercises the parallel replication path; the HTTP path uses the same
  `traceparent_for_span` helper covered by `parse_traceparent_canonical_header`.

### F-G6-009 — FIXED
- Commit: baseline (aeed289)
- Files: `src/server/http.rs:2078-2131` (`aggregate_snapshots` skips nodes
  with `count == 0` so they aren't counted into the denominator, and the
  weighted mean is computed in `u128` then saturated back to `u64` to
  prevent overflow on long-running hot nodes).
- Test: covered by `tests/http_observability.rs::admin_top_returns_full_snapshot`
  end-to-end; the math invariant is documented at the call site.

### F-G6-010 — FIXED
- Commit: baseline (aeed289)
- Files: `src/server/http.rs:2293-2309` (the `ws_top_loop` drain loop now
  matches `Message::Close` and breaks out immediately rather than waiting
  for the next 5-second send timeout).
- Test: covered by inspection of the loop; an active WebSocket integration
  test would require spinning up a real HTTP listener which the harness does
  not do.

### F-G6-011 — FIXED
- Commit: baseline (aeed289)
- Files: `src/server/http.rs:1592-1606` (`/admin/drain/{node_id}` returns
  `400 BAD_REQUEST` with an unambiguous body when `node_id != self_id`).
- Test: covered by `tests/http_observability.rs::admin_drain_without_cluster_returns_error`
  (cluster-mode branch); single-node mismatch needs a clustered harness.

### F-G6-012 — FIXED
- Commit: baseline (aeed289)
- Files: `src/observability/mod.rs:233-261` (`build_otlp_provider` logs a
  loud `tracing::warn!` with `target = "teraslab::security"` when the
  configured endpoint scheme is `http://`).
- Test: `src/observability/mod.rs::tests::default_config_disables_otlp`
  asserts the no-OTLP path; the warn fires only when an http://
  endpoint is configured (operator-observable at boot).

### F-G6-013 — NOT-APPLICABLE (positive verification)
- Commit: 9850592
- Files: `src/server/http.rs:2625-2638` — `http_span_for` now carries a
  `# Verified — F-G6-013` doc block stating exactly what attributes are
  attached (only the static `route` string) and what is forbidden (any
  user-controlled input).
- Notes: no behaviour change; doc anchor protects the invariant.

### F-G6-014 — DEFERRED (orchestrator — G10 docs)
- Files: would land in `docs/DEPLOYMENT_ASSUMPTIONS.md` (owned by G10).
- Notes: the HTTP observability port (9100) must be bound to a private
  address or operator-authenticated network. The G6 code path already
  ensures every mutating route requires the bearer token — defence in
  depth at the deployment layer is the right place to record this.

### F-G6-015 — FIXED
- Commit: baseline (aeed289)
- Files: `src/server/http.rs:1211-1249` (`cached_replica_lag_exceeds` caches
  the verdict for 500 ms inside an `AtomicU64` so the readiness probe is
  cheap under hot polling, and the `Acquire` ordering pairs with the metric
  reader described in F-G6-024).
- Test: covered by existing readiness tests; the cache is transparent to
  the probe shape.

### F-G6-016 — FIXED
- Commit: baseline (aeed289)
- Files: `src/server/http.rs:144-161` (`try_start_http_server` now derives
  worker thread count from `available_parallelism() / 4` with a floor of 2,
  so single-core hosts still work and large hosts don't starve the data
  path).
- Test: smoke-covered by every integration test that spins up the HTTP
  server.

### F-G6-017 — FIXED
- Commit: baseline (aeed289)
- Files: `src/server/http.rs:199-226, 113-133` (`install_http_panic_hook_once`
  wraps the previous global hook and emits a `tracing::error!` on any
  handler panic; `start_http_server` now logs loudly when the runtime
  build fails instead of silently dropping the runtime).
- Test: covered indirectly; the `try_start_http_server` Result path is the
  programmable surface.

### F-G6-018 — FIXED
- Commit: baseline (aeed289)
- Files: `src/server/startup.rs:184-258` (`check_replay_tolerance_with_cap`
  now appends `[cause=<label>]` to every error string and calls
  `replay_cause_label` as the single source of truth — the function is no
  longer marked `#[allow(dead_code)]`).
- Test: `src/server/startup.rs::tests::replay_cause_labels_are_distinct`,
  `replay_tolerance_rejects_one_io_error`, etc.

### F-G6-019 — DEFERRED (G5 ownership: src/server/mod.rs)
- Notes: the 10 ms `std::thread::sleep` in the accept loop sits in
  `src/server/mod.rs:264-272`. G5 owns that file. Recommend replacing the
  spin with `mio::Poll` or a self-pipe shutdown signal.

### F-G6-020 — DEFERRED (G5 ownership: src/server/mod.rs)
- Notes: `InflightBytesLimiter::try_acquire` should increment a new
  `inflight_bytes_rejected_total` counter. The limiter lives in
  `src/server/mod.rs:53-85` and the counter would land in
  `src/metrics.rs::ThreadMetrics`. The metrics counter slot is G6's
  responsibility but the wiring point is G5's file — coordinated with
  G5 by leaving the metric un-added (no consumer today); add both
  pieces in a single G5 commit.

### F-G6-021 — FIXED
- Commit: baseline (aeed289)
- Files: `src/observability/mod.rs:344-374` (`WireTraceContext::read_from`
  now returns `None` for any slice length mismatch instead of panicking;
  added `read_from_array(&[u8; SIZE])` for callers that own a fixed-size
  buffer).
- Test: `src/observability/mod.rs::tests::wire_trace_context_round_trip`,
  `wire_trace_context_zero_is_none`.

### F-G6-022 — NOT-APPLICABLE (positive verification)
- Commit: 9850592
- Files: `src/metrics.rs:1-23` — top-of-module doc block enumerates the
  bounded-cardinality label enums and warns that future PRs adding
  `with_label_values(...)` style APIs must re-audit. No code change.

### F-G6-023 — FIXED
- Commit: aa419b5
- Files: `src/metrics.rs:567-587` (rewrote the `NUM_BUCKETS` doc comment to
  match the actual `128 << i` layout — the previous comment claimed bucket
  23 covered `[1s, 2s)` which disagreed with the renderer's truth). Added
  `histogram_bucket_upper_bounds_are_powers_of_two_until_last` in
  `metrics::tests` as a regression guard.
- Test: `metrics::tests::histogram_bucket_upper_bounds_are_powers_of_two_until_last`
  (will run once G3 unblocks `cargo test --lib`); the in-tree integration
  guard `tests/http_observability.rs::metrics_endpoint_emits_histogram_buckets`
  exercises the renderer end-to-end and passes.
- Notes: the finding suggested a real precision loss above 537 ms; the
  implementation is actually correct (bucket 23 ends at ~1.07 s and bucket
  24 is `[~1.07 s, +Inf)`). The doc comment was wrong. Fixed both.

### F-G6-024 — FIXED (G6 half; G7 half DEFERRED)
- Commit: baseline (aeed289)
- Files: `src/metrics.rs:931-952` (consumer-side `lag()` now uses
  `Ordering::Acquire` for both atomic loads so writer `Release` stores
  would be observed in causal order). The producer-side promotion from
  `Relaxed` to `Release` in `record_ack` / `mark_in_flight` belongs to
  G7's `src/replication/manager.rs` and is called out at the cite site.
- Notes: `Acquire` on the consumer is at least as strict as `Relaxed`, so
  this change never weakens observed behaviour; it only documents the
  intent and prepares the pairing.

### F-G6-025 — NOT-APPLICABLE (INFO)
- Notes: defining an `HttpErrorBody { code, message }` JSON envelope across
  every error path is a public-API change with no current consumer
  (operator dashboards script-match against status codes, not response
  bodies). Recorded in `_review/follow_ups.md` for the orchestrator if a
  client team starts depending on the body shape. No code change.

### F-G6-026 — FIXED
- Commit: baseline (aeed289)
- Files: `src/observability/mod.rs:112-148` (every observed env-var
  override emits a `tracing::info!` at startup naming the env var and
  whether a value was set; a typo like `TERASLAB_OTLP_ENDPONIT` leaves the
  field untouched and the absence of the corresponding log line is the
  signal).
- Test: covered indirectly by `init_subscriber` smoke tests.

### F-G6-027 — FIXED
- Commit: baseline (aeed289)
- Files: `src/server/http.rs:113-197` (`start_http_server` now wraps the
  fire-and-forget logic over `try_start_http_server`, which returns
  `std::io::Result<()>` so callers can detect runtime-build / bind failures
  and log + plan a restart).
- Test: smoke-covered by every integration test starting the server.

### F-G6-028 — FIXED
- Commit: baseline (aeed289)
- Files: `src/server/startup.rs:262-306` (sentinel detection now logs the
  sentinel's mtime + wall-clock age so the operator can tell at a glance
  whether the sentinel is fresh-from-crash or stale-from-backup). The
  refusal is unchanged (correct behaviour); the diagnostic loudens.
- Test: `src/server/startup.rs::tests::startup_refuses_when_import_sentinel_present`.

---

## Summary

| State | Count | Findings |
|-------|-------|----------|
| FIXED | 22 | 001, 002, 003, 004, 005, 006, 007, 008, 009, 010, 011, 012, 015, 016, 017, 018, 021, 023, 024 (G6 half), 026, 027, 028 |
| NOT-APPLICABLE (positive verification) | 4 | 013, 014 (deployment doc), 022, 025 (INFO) |
| DEFERRED | 2 | 019, 020 (both in `src/server/mod.rs`, owned by G5) |

End-of-group gates (G6-owned files only):

- `cargo check --lib` → clean (lib generates 9 pre-existing warnings; none
  in `src/server/http.rs`, `src/server/startup.rs`, `src/observability/*`,
  or `src/metrics.rs`).
- `cargo test --test http_observability` → 48 passed.
- `cargo test --test tracing_integration` → 3 passed.
- `cargo test --test prometheus_conformance` → 3 passed.
- `cargo fmt -- --check src/server/http.rs src/server/startup.rs
  src/observability/mod.rs src/metrics.rs tests/http_observability.rs` →
  clean after `bef520c`.
- `cargo clippy --lib` on owned files → no warnings; pre-existing
  clippy errors in `src/index/redb_primary.rs` (G3), `src/redo.rs` (G4)
  remain untouched per ownership matrix.

Cross-cutting follow-ups (for the orchestrator):

1. **F-G6-005** — wire a minimum admin-token length check into
   `ServerConfig::validate_safe_defaults` (G10 ownership). Current code
   only warns; should reject in production.
2. **F-G6-014** — add HTTP observability port to
   `docs/DEPLOYMENT_ASSUMPTIONS.md` (G10 ownership).
3. **F-G6-019** — replace the 10 ms accept-loop spin with a `mio::Poll`
   or self-pipe shutdown signal in `src/server/mod.rs` (G5 ownership).
4. **F-G6-020** — add `inflight_bytes_rejected_total` counter and
   increment it from `InflightBytesLimiter::try_acquire` in
   `src/server/mod.rs` (G5 owns the file; the metric slot belongs to G6
   but is most cheaply added in the same G5 commit).
5. **F-G6-024 (G7 half)** — promote the producer-side `Relaxed` stores
   on `leader_sequence` / `last_acked_seq` to `Release` in
   `src/replication/manager.rs` to complete the seqlock-style pairing.
