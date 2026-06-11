# Audit L + M — Resource Limits / DoS + Observability

Scope: `src/server/{dispatch,http,mod,startup}.rs`, `src/metrics.rs`,
`src/observability/mod.rs`, `src/protocol/deadline.rs`, connection handling,
admin endpoints. Build not run (orchestrator holds lock); all checks are
source-traced, reproductions specify experiments.

---

## L — Resource limits and DoS

### [HIGH] Unbounded number of concurrent streaming-upload sessions per connection (FD + memory exhaustion)

**Location:** `src/server/dispatch.rs:6318-6430` (`handle_stream_chunk`),
`src/server/mod.rs:155-187` (`ConnectionState.streams`),
`src/storage/blobstore.rs:842-859` (`begin_stream` → `File::create`).

**What's wrong:** Each `OP_STREAM_CHUNK` for a previously-unseen txid inserts a
new `ActiveStream` into `conn_state.streams` (a `HashMap<[u8;32], ActiveStream>`)
and calls `blob_store.begin_stream`, which opens an OS file handle for a temp
file and holds a `Sha256` hasher + write buffer. There is a per-stream byte cap
(`max_stream_total_bytes`, default 4 GiB) and a per-frame cap (`MAX_FRAME_SIZE`,
16 MiB), but **no cap on the number of simultaneously-open streams**. A single
connection can open an `OP_STREAM_CHUNK` with offset 0 and a 1-byte chunk for
millions of distinct txids — never sending `OP_STREAM_END` — accumulating one
open file descriptor + HashMap entry + hasher state per txid. Streams are only
freed on `OP_STREAM_END`, on write/cap error, or when the connection's
`ConnectionState` drops (`mod.rs:180-187`).

**Why it matters:** Exhausts the process file-descriptor table and grows
unbounded heap from one connection. The data plane has no client auth (see
finding below), so on any reachable data port this is an unauthenticated
resource-exhaustion DoS. The existing per-stream and per-frame caps create a
false sense of "streaming is bounded" — the aggregate dimension (stream count)
is the one left open.

**Reproduction:** Unit-style: build a `ConnectionState`, then in a loop call
`handle_stream_chunk` with `chunk.txid = i.to_le_bytes()`-derived 32-byte keys,
`offset = 0`, `data = [0u8]`, for i in 0..100_000, against a `FsBlobStore`.
Assert `conn_state.streams.len()` grows to 100_000 and observe open-fd count
(`lsof -p <pid> | wc -l`) climbing one per txid. No error is returned. Compare
against the per-stream cap test at `dispatch.rs:7798+` which only exercises a
single txid.

**Suggested fix:** Add `ServerConfig::max_active_streams_per_connection` (e.g.
64–256) and reject `begin_stream` insertion past it with `ERR_RATE_LIMITED` /
a stream-specific error; optionally an idle-stream reaper so half-open streams
don't pin fds for the life of the connection. Coordinate the per-connection
bound with a process-wide active-stream count if multiple connections must be
bounded together.

---

### [MEDIUM] Data plane (TCP client protocol) is entirely unauthenticated; no request-level rate limiting

**Location:** `src/server/mod.rs:652-983` (`handle_connection_inner`),
`src/protocol/opcodes.rs:509-524` (`is_inter_node_auth_opcode`).

**What's wrong:** HMAC auth (`cluster_secret`) is applied **only** to the
inter-node opcode set (`OP_REPLICA_BATCH`, topology, migration, admin-diagnose,
cluster-health). All client data-plane opcodes — `OP_SPEND_BATCH`,
`OP_CREATE_BATCH`, `OP_GET_BATCH`, `OP_STREAM_CHUNK`, `OP_DELETE_BATCH`, etc. —
are accepted with no authentication whatsoever. The only DoS controls are
connection-count caps (`max_connections` = 1024, `max_connections_per_ip` = 64),
the aggregate in-flight byte cap (`max_inflight_request_bytes` = 256 MiB), and
the frame-assembly deadline. There is **no per-connection or per-IP request-rate
limit** and no op-cost limiter.

**Why it matters:** Anyone who can reach the data port can issue arbitrary
mutations and reads. This is mitigated-by-deployment: `listen_addr` defaults to
`127.0.0.1` and a non-loopback bind requires `enable_remote_bind = true`
(`config.rs:1143`), and the code comments explicitly defer client auth to a
future "mTLS wave." So this is a documented design posture, not an oversight —
flagged as MEDIUM because the trusted-overlay assumption must hold operationally
and there is no defense-in-depth (rate limit) if the port is exposed.

**Reproduction:** Connect to `listen_addr` with no secret, send a valid
`OP_SPEND_BATCH` frame, observe `STATUS_OK` / mutation applied. Then loop
sending frames as fast as possible from one IP within the 64-connection cap;
observe no throttling beyond the inflight-byte ceiling.

**Suggested fix:** Document the trusted-overlay requirement prominently (partly
done in `docs/DEPLOYMENT_ASSUMPTIONS.md`); add a token-bucket request-rate limit
per connection/IP as defense-in-depth; land the mTLS/client-auth wave before any
remote-bind production use.

---

### [LOW] Idle/slow-loris connection controls — verified present and on by default

**Location:** `src/server/mod.rs:46-54, 658-983`, `src/protocol/deadline.rs`.

**What's wrong / status:** No defect — recording verification for the checklist.
- **Silent connect, never sends:** `set_read_timeout(CONNECTION_READ_TIMEOUT =
  30 s)` at `mod.rs:661-663`; the length-prefix `read_exact` returns on
  `TimedOut`/`UnexpectedEof`/`WouldBlock` treated as clean close
  (`mod.rs:694-699`). On by default; covered by
  `silent_client_dropped_after_idle_timeout` test.
- **Slow-drip frame assembly:** `DeadlineReader` enforces an absolute
  `FRAME_ASSEMBLY_TIMEOUT = 60 s` from the length prefix
  (`deadline.rs:32, 74-92`), defeating the per-syscall-reset weakness of
  `set_read_timeout`. Covered by `dripping_client_disconnected_at_frame_assembly_deadline`.
- **Slow reader / write backpressure:** `set_write_timeout(30 s)`
  (`mod.rs:672-674`) caps a slow-reader from pinning a thread forever. One
  OS thread per connection (`mod.rs:537`) gives per-connection isolation — a
  blocked write stalls only that thread; `max_connections` bounds the total.
- **Per-connection memory:** `read_buf` retained at `READ_BUF_RETAINED_SIZE`
  = 256 KiB and shrunk after oversized frames (`mod.rs:919-933, 985-992`).
  Aggregate request memory bounded by `InflightBytesLimiter`
  (`mod.rs:60-149`, default 256 MiB). Per-request allocation bounded by
  `MAX_FRAME_SIZE` (16 MiB) + `decode_*_checked(max_batch_size)`.

**Reproduction:** The three named unit tests in `mod.rs` exercise the timeouts;
`tests/g5_slow_loris_streaming.rs` exercises the signed-body streaming-reject
path with bounded sink.

**Suggested fix:** None required; consider lowering `FRAME_ASSEMBLY_TIMEOUT`
under attack conditions, but 60 s is reasonable for 16 MiB at ~280 KiB/s.

---

## M — Observability

### [LOW] Readiness gate — verified correct; one ordering subtlety

**Location:** `src/server/http.rs:1219-1333` (`handle_health_live`,
`compute_health_ready`), `src/bin/server.rs:1127-1163`.

**What's wrong / status:** No defect — recording verification.
- `/health/live` returns 200 unconditionally (`http.rs:1219-1221`) — succeeds
  during startup, correct for a liveness probe.
- `/health/ready` (`compute_health_ready`) gates on, in order: (1) `state.ready`
  (constructed `false` at `server.rs:1137`, flipped `true` only after recovery +
  engine attach at `server.rs:1148-1150`, **before** the HTTP thread is spawned
  at `server.rs:1161`); (2) secondary-index status (`dah_ok`/`unmined_ok`);
  (3) clustered: `cluster.cluster_health().is_ready()` (node has observed a
  committed topology = joined quorum); (4) clustered: replica lag under threshold.
  This matches the checklist requirement (index loaded AND, in cluster mode,
  node joined).
- The `ready` flag is set before the listener answers probes, so there is no
  "ready before recovery" window. The comment at `server.rs:1131-1136` explicitly
  guards against a future refactor that starts HTTP earlier — good.

**Reproduction:** `tests/http_observability.rs` drives `/health/ready`; the
ready-state logic is unit-testable via `compute_health_ready` (broken out for
exactly this). To exercise cluster-not-ready, construct `HttpState` with a
cluster whose `cluster_health().is_ready()` is false and assert 503 +
`"cluster not ready"`.

**Suggested fix:** None.

### [MEDIUM] `/health/ready` replica-lag verdict is cached up to 500 ms — minor staleness

**Location:** `src/server/http.rs:1303-1333` (`cached_replica_lag_exceeds`,
`REPLICA_LAG_CACHE`).

**What's wrong:** The replica-lag readiness predicate is memoized in a process-
global `AtomicU64` for `READINESS_LAG_CACHE_TTL_MS` = 500 ms. A node that
crosses the lag threshold can keep answering `/health/ready` = 200 for up to
500 ms after it should report degraded (and vice-versa). The cache is global,
not per-`HttpState`, so in a multi-instance test process the verdict leaks
across instances.

**Why it matters:** Low operational impact (load balancers poll on 1–5 s
intervals, so 500 ms staleness is within noise), but the global-static cache is
a latent cross-instance-contamination bug for any in-process multi-node test or
future multi-tenant embedding.

**Reproduction:** In one process, instantiate two `HttpState`s with different
`replica_lag_warn_threshold_ops`; call `cached_replica_lag_exceeds` on each
within 500 ms and observe the second returns the first's cached verdict.

**Suggested fix:** Move the cache into `HttpState` (per-instance), or key it by a
state identifier. Acceptable to leave for single-instance production.

### [LOW] Per-op metric accounting (attempted / succeeded xor failed) — verified, no double-count

**Location:** `src/server/dispatch.rs` — spend `2806-3061`, unspend
`3136-3263`, get `5809-5820`; replication apply `519-583`.

**What's wrong / status:** No defect — recording the trace for three ops.
- **Spend (`handle_spend_batch`):** `spends_attempted.inc_by(items.len())` once
  after decode (`2808`). On every exit path — redo failure (`2956-2972`), apply
  failure (`2996-3012`), and normal completion (`3045-3061`) — exactly one of
  succeeded/idempotent/failed is tallied per item, guarded by
  `debug_assert_eq!(succeeded + idempotent + failed == items.len())` (`3040`).
  The labeled `operations` table is dual-written from the same buckets.
- **Get (`handle_get_batch`):** each result classified into exactly one of
  ok/not_found/failed (`5788-5808`), then `gets_*` and `operations` incremented
  once (`5809-5820`).
- **Replication apply (`OP_REPLICA_BATCH`):** routes to
  `replication::receiver::handle_replica_batch*` (`558-579`), which applies
  master ops directly to the engine and does **not** touch the client
  `spends_*`/`operations` counters — so a replicated spend is NOT double-counted
  as a client spend. Replication has its own `repl_batches_*` series. The
  batch-level attempted counters at `dispatch.rs:404-418` deliberately exclude
  `OP_REPLICA_BATCH`.

**Reproduction:** `dispatch.rs` tests `handle_spend_batch_increments_items_succeeded_and_failed`
(`11375`), `handle_spend_batch_idempotent_counted_as_idempotent` (`11476`),
`handle_unspend_batch_ticks_outcome_counters` (`11526`); `tests/prometheus_conformance.rs`
drives a full workload and asserts the `{op,outcome}` grid.

**Suggested fix:** None. (The accounting only ticks when `DISPATCH_METRICS.get()`
is `Some`; in tests without init the counters silently no-op — intended.)

### [LOW] Prometheus label cardinality — verified bounded

**Location:** `src/metrics.rs:1-20` (cardinality invariant doc),
`src/server/http.rs:1144-1179, 943-1135`.

**What's wrong / status:** No defect. Every labeled series uses a fixed,
compile-time-bounded label set: `operations{op,outcome}` (14 ops × 8 outcomes,
`prom_labeled_counter`), `repl_*{replica_idx}` and `repl_lag_sequences` bounded
by `MAX_REPLICAS` (`prom_labeled_replica_counter`, `957-963`),
`migration_bytes_transferred_total{direction_role}` over `MigrationLabel::all()`,
`swim_membership_churn_total{kind}` over `SwimChurnKind::all()`. No txid, peer
address, client IP, or request path is ever used as a label — asserted by the
`metrics.rs` module doc (F-G6-022) and the `http_span_for` doc (F-G6-013), which
also confirms HTTP spans carry only a `&'static str route` attribute, never
user input. `grep` for `with_label_values` / `String`-keyed labels: none.

**Reproduction:** `tests/prometheus_conformance.rs` and the `metrics.rs` tests
(`label_strings_*`, `prometheus_emits_operations_total_with_labels`) enumerate
the full fixed grid; no dynamic label source exists to exercise.

**Suggested fix:** None. Keep the re-audit note for any future
`with_label_values` addition.

### [LOW] Admin mutating endpoints — authenticated and gated; shared bind with /metrics

**Location:** `src/server/http.rs:318-435` (`build_http_router`),
`453-533` (`require_admin_bearer`, `extract_bearer_token`),
`1483-1713` (quiesce/rebalance/drain handlers), `src/config.rs:1206-1242`.

**What's wrong / status:** No HIGH finding — the mutating admin surface IS
authenticated, contrary to the "None = HIGH" trigger.
- `/admin/quiesce`, `/admin/rebalance`, `/admin/drain/{node_id}` (plus
  `PUT /debug/log-level` and all read-only `/admin/*` and `/debug/*`) are only
  registered when `enable_admin_endpoints = true` (default `false`,
  `config.rs:757`), and then exclusively behind
  `require_admin_bearer` middleware requiring `Authorization: Bearer <token>`,
  compared in constant time over SHA-256 digests (length-independent timing,
  F-G6-004, `http.rs:484-504`).
- `validate_safe_defaults` rejects `enable_admin_endpoints = true` with an
  empty/absent `admin_token` (`config.rs:1206-1219`), and requires ≥ a minimum
  token length when both admin and remote-bind are on (`config.rs:1224-1242`).
  If the gate is somehow installed without a token, `build_http_router` logs an
  error and returns the public-only router (fail closed, `http.rs:367-375`).
- **Bind separation:** admin endpoints share the HTTP observability port
  (`http_listen_addr`, default `127.0.0.1:9100`) with the unauthenticated
  `/metrics`, `/health/*`, `/status`, `/ui/*`. They are NOT on the data port
  (`listen_addr`). There is no *separate* bind address isolating admin from
  metrics — both ride the one HTTP port — but the admin routes carry their own
  bearer gate, so co-tenancy with `/metrics` is acceptable. MEDIUM-adjacent only
  if an operator assumes the HTTP port is fully unauthenticated and firewalls it
  loosely; LOW as-is.

**Reproduction:** `tests/http_observability.rs` (`start_test_http_server_with_admin`,
`http_put_auth`) exercises 401-without-token and 200-with-token on the admin
routes.

**Suggested fix:** Optionally support a distinct admin bind address for operators
who want network-level isolation of the mutation surface from the scrape surface.

---

## Checklist disposition

### L — Resource limits and DoS
- Idle-connection timeout, on by default — ✅ (`mod.rs:661-699`, 30 s read
  timeout + clean-close handling; `silent_client_dropped_after_idle_timeout`).
- Slow 1 B/s reader doesn't block other connections — ✅ (per-connection OS
  thread `mod.rs:537`; `set_write_timeout` 30 s `mod.rs:672-674`).
- Slow-drip frame assembly bounded — ✅ (`DeadlineReader` /
  `FRAME_ASSEMBLY_TIMEOUT` 60 s, `deadline.rs`; dripping-client test).
- Memory per connection bounded — ✅ (256 KiB retained read_buf +
  `InflightBytesLimiter` 256 MiB aggregate).
- Allocation per request bounded by max_batch_size + frame max — ✅
  (`MAX_FRAME_SIZE` 16 MiB pre-alloc guard `mod.rs:709-725` + per-decoder
  `decode_*_checked(max_batch_size)`).
- Unauthenticated TCP data plane / rate limiting — ⚠️ (no client auth, no
  request-rate limit; mitigated by loopback default + `enable_remote_bind` gate
  — MEDIUM finding above).
- Streaming uploads, per-stream AND total concurrent caps — ❌ (per-stream byte
  cap exists; **no cap on concurrent stream count per connection** — HIGH
  finding: FD/memory exhaustion).

### M — Observability
- `/health/live` up during startup; `/health/ready` only after index loaded
  AND (cluster) joined — ✅ (`compute_health_ready` `http.rs:1265-1293`; ready
  flag set post-recovery pre-listener `server.rs:1148-1163`). Caveat: lag
  verdict cached 500 ms in a global static — MEDIUM.
- attempted once, succeeded xor failed once, no double-count on
  retries/replication — ✅ (spend/get/unspend traced; replica apply bypasses
  client counters `dispatch.rs:558-579`).
- Prometheus label cardinality bounded, no per-txid/per-peer labels — ✅
  (`metrics.rs` F-G6-022 invariant; fixed `{op,outcome}` / `{replica_idx}` /
  `{kind}` grids only).
- Admin mutation auth + bind separation — ✅ (bearer-gated, default-off,
  `validate_safe_defaults` enforced; shares HTTP port with `/metrics` but not
  the data port — LOW note on no dedicated admin bind).
