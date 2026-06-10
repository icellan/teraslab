# Category M — Observability — Audit (verified against HEAD 1e5659b)

Primary files: `src/observability/mod.rs`, `src/metrics.rs`, `src/server/http.rs`,
plus the metric call sites in `src/server/dispatch.rs` and the startup wiring in
`src/bin/server.rs`.

## Checklist disposition

### 1. `/health/live` succeeds during startup — VERIFIED OK
`handle_health_live` (`src/server/http.rs:1252-1254`) unconditionally returns
`(StatusCode::OK, "ok")` and ignores `state`. It cannot fail or block. Correct
liveness semantics (process is up, regardless of readiness).

### 2. `/health/ready` only ready after index loaded AND (cluster) node joined — VERIFIED OK (with nuance)
`compute_health_ready` (`http.rs:1298-1326`) gates on, in order:
1. `state.ready` (recovery-complete flag),
2. `dispatch::secondary_status().dah_ok` / `.unmined_ok` (degraded secondary index),
3. clustered: `cluster.cluster_health().is_ready()` ("observed at least one committed topology" = node has joined a quorum),
4. clustered: replica lag under threshold (cached 500 ms, `cached_replica_lag_exceeds`, `http.rs:1344-1366`).

The "index loaded" requirement is satisfied transitively: in `src/bin/server.rs`
the index load (steps 3/3b, lines ~362-568), redo recovery (621-693), and engine
construction (719-773) all run **synchronously before** the `HttpState` is built and
the HTTP thread is spawned (`server.rs:1129-1153`). So by the time `/health/ready`
can answer at all, the index is loaded. The cluster-join requirement is enforced by
check 3.

NUANCE (not a finding): `HttpState.ready` is hard-initialized to `true`
(`server.rs:1133`), and there is no code path that ever stores `false` into it at
runtime (`grep` for `ready.store` returns nothing in `src/bin` / `src/server`). The
doc comment at `http.rs:1280` describes a *historical* bug ("Pre-fix state.ready was
hard-coded true at startup and never updated"). The current design relies entirely
on "HTTP starts after recovery" + the cluster-health gate, NOT on the `ready` flag
ever being false. The flag is therefore decorative on the single-node path, but the
ordering guarantee makes it correct. `tests/g10_lifecycle.rs` is cited (server.rs:1534)
as covering "no listener answers during the recovery window." Acceptable as-is;
flagged only as LOW hardening (M-02) because a future refactor that starts HTTP
before recovery would silently re-introduce the original bug with no flag to catch it.

### 3. Every op increments attempted once, succeeded XOR failed once — PARTIALLY VERIFIED; one real gap (M-01)
Happy path is correct and exactly-once. For spend (`dispatch.rs:2789-2997`):
- `attempted` incremented once per batch up-front (2792-2793, `inc_by(items.len())`).
- terminal tally done once after the group loop (2981-2997), with a
  `debug_assert_eq!(succeeded + idempotent + failed == items.len())` (2976-2980)
  proving exhaustive, non-double-counted classification.
- Replication (2999+) runs AFTER the terminal tally, so replication retries / failures
  do NOT re-increment attempted or succeeded — exactly-once across the replication path.
  Confirmed the same shape for unspend / set_mined / create / freeze / … (all tally
  once after their loop).

GAP (M-01): the mid-batch `apply()` error path early-returns BEFORE the terminal
tally. In spend, `validated.apply(engine)` Err → `return error_response(req.request_id,
ERR_STORAGE_IO, …)` at `dispatch.rs:2949` (also 2935). This skips the entire 2981
metrics block. Net effect on a write-path storage / DAH-overflow error:
- `spends_attempted` / `spend_multi_items_attempted` already ticked (2792),
- NONE of `spends_succeeded` / `_idempotent` / `_failed` nor the labeled
  `operations{op="spend",…}` cells tick,
- so `attempted` no longer equals `succeeded + idempotent + failed`, and the error is
  invisible in `/metrics`.

The same early-return-before-tally shape exists for the other mutating handlers
(unspend `dispatch.rs:3137`, set_mined `:3268`, freeze `:3952`, unfreeze `:4056`,
create `:3681`, etc. — all `return error_response(…, ERR_STORAGE_IO, …)` before their
post-loop tally block).

Consequence: `Outcome::ErrStorage` (metrics.rs:179) is effectively DEAD on the write
path — I found no call site that increments it. A burst of device-write failures
(EIO / ENOSPC) returns `ERR_STORAGE_IO` frames to clients but produces a flat
`/metrics` (only the `attempted` rate moves, with no matching terminal counter),
so operators cannot alert on write-path storage failures via the op-outcome metrics.
For a money-critical UTXO store, silent write-error invisibility is a genuine
observability gap. Severity LOW→MEDIUM: it does not corrupt data or mis-ACK; it
blinds the operator to a class of failures. I rate it LOW because (a) it only fires on
an already-error response the client sees, and (b) `redo_flush_errors_total` /
`uring_completion_errors_total` give partial coverage of the underlying device errors.

### 4. Prometheus label cardinality bounded (no per-txid labels) — VERIFIED OK
All labeled metrics are keyed by fixed `#[repr(u8)]` enums with `const all()` slices:
`Outcome` (8), `OpCode` (14), `UringErrClass` (8), `MigrationLabel` (4),
`SwimChurnKind`, and per-replica index `0..MAX_REPLICAS=8`. The labeled renderer
`prom_labeled_counter` (`http.rs:1198-1212`) iterates `OpCode::all() × Outcome::all()`
= 112 fixed lines. No `.with_label_values`, no `String`-keyed label, no client IP /
txid / peer-addr label anywhere (module-level invariant documented at
`metrics.rs:6-19`, enforced in practice). The OTLP span path is equally bounded:
`http_span_for` (`http.rs:2796-2809`) attaches exactly one `&'static str` `route`
attribute and never any request-derived value (verified F-G6-013, comment 2786-2795).
`handle_debug_record` takes a `txid` *path param* but it is used only to look up a
record, never emitted as a metric/span label.

### 5. Auth on mutating admin endpoints — VERIFIED OK
`/admin/quiesce`, `/admin/rebalance`, `/admin/drain/{node_id}` are registered only
inside the `gated` sub-router (`http.rs:388-390`) which is mounted only when
`enable_admin_endpoints == true` AND a non-empty `admin_token` is present
(`http.rs:337-376`); otherwise the builder returns the public router with the
mutation routes entirely absent (fail-closed). Every gated route is wrapped in the
`require_admin_bearer` middleware (`http.rs:428-431`). The middleware
(`http.rs:453-504`):
- rejects missing/empty configured token defensively (459-471),
- rejects missing/malformed `Authorization: Bearer …` (473-482),
- compares `SHA-256(supplied)` vs `SHA-256(expected)` with `subtle::ConstantTimeEq`
  (492-495) — constant-time AND length-independent (both inputs hashed to 32 bytes,
  F-G6-004). Short tokens (<16 B) get a startup warning (354-363). This is a correct,
  solid auth story. No finding.

## Verified-OK list
- `/health/live` unconditional 200 (http.rs:1252).
- `/health/ready` recovery + secondary-degraded + cluster-quorum + replica-lag gating (http.rs:1298-1326).
- Index-loaded-before-HTTP ordering (server.rs synchronous recovery → engine → HttpState at 1129).
- Exactly-once happy-path op accounting incl. `debug_assert_eq!` exhaustiveness check (dispatch.rs:2976-2980) and metrics-before-replication ordering (no retry double-count).
- Bounded Prometheus/OTLP cardinality; no per-txid/IP/peer labels (metrics.rs:6-19, http.rs:1198-1212, 2796-2809).
- Mutating admin endpoints fail-closed + constant-time, length-independent bearer auth (http.rs:337-504).
- OTLP exporter disabled when no endpoint; sampling ratio validated to [0,1] (observability/mod.rs:154-162, 207-229); plaintext-http warning (241-254); shutdown drains synchronously with timeout (277-298, test 552-609).
- `LatencyHistogram` bucket math consistent with `bucket_upper_ns_at` after the F-G6-023 comment fix; one stale doc line remains (M-03 below).

## Findings

### M-01 (LOW) Write-path storage/DAH-overflow errors are invisible in op metrics; `Outcome::ErrStorage` is dead on the write path
See checklist item 3. Locations: `src/server/dispatch.rs:2935,2949` (spend), `3137`
(unspend), `3268` (set_mined), `3681` (create), `3952` (freeze), `4056` (unfreeze) —
each early-returns `ERR_STORAGE_IO` before its post-loop terminal-tally block, so
`attempted` ticks (e.g. 2792) but no succeeded/idempotent/failed/`Outcome::ErrStorage`
counter does. `Outcome::ErrStorage` (`src/metrics.rs:179`) has no incrementing call
site on the write path.
Failure mode: a device EIO/ENOSPC storm returns errors to clients while `/metrics`
shows only rising `*_attempted_total` with no matching failure counter — operators
cannot alert on write-path storage failures via op-outcome metrics.
Fix: before each `return error_response(…, ERR_STORAGE_IO, …)`, increment the op's
`_failed` scalar and `operations.inc_by(op, Outcome::ErrStorage, remaining_items)`
(remaining = items not yet tallied this batch), or restructure so the terminal tally
runs on all exit paths (e.g. tally in a closure / on Drop). Add a test that injects a
device write error mid-batch and scrapes a non-zero `teraslab_…_failed_total` /
`operations{outcome="err_storage"}`.

### M-02 (LOW) `HttpState.ready` is never set to `false`; readiness correctness depends solely on startup ordering
`src/bin/server.rs:1133` sets `ready: AtomicBool::new(true)` and nothing ever stores
`false` (no `ready.store` in `src/bin`/`src/server`). The `state.ready` check in
`compute_health_ready` (`http.rs:1299`) is therefore always-true on the single-node
path; readiness correctness relies entirely on "HTTP thread spawned after synchronous
recovery" (server.rs:1129 vs the recovery block above it) plus the cluster-health
gate. The doc at `http.rs:1280` even references the original "hard-coded true" bug.
Failure mode: a future refactor that moves HTTP startup earlier (or makes recovery
async) silently re-introduces the original "ready before recovery" defect with no flag
to catch it — exactly the regression F-G6-001 was meant to prevent.
Fix: construct `HttpState` with `ready=false`, spawn the HTTP server early, and
`ready.store(true, Release)` only after recovery+engine attach complete; or add an
assertion/test that the flag starts false and is flipped post-recovery. Low severity
because current ordering is correct.

### M-03 (LOW) Stale doc comment on `bucket_upper_ns_at`
`src/metrics.rs:748-752` still says "bucket 23 is `[1s, 2s)`", contradicting the
implementation `128u64 << i` and the corrected `NUM_BUCKETS` doc at metrics.rs:592-612
(F-G6-023 fixed the other copy of this comment but missed this one). Bucket 23 is
actually `[~0.54s, ~1.07s)`. Cosmetic; could mislead someone reading percentile
output. Fix: align the comment with the `128 << i` formula.
