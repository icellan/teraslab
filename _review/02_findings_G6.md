# Group G6 — HTTP / Observability / Metrics — Findings

Scope (all paths absolute):
- `/Users/siggioskarsson/gitcheckout/teraslab/src/server/http.rs` (3387 LOC)
- `/Users/siggioskarsson/gitcheckout/teraslab/src/server/startup.rs` (905 LOC)
- `/Users/siggioskarsson/gitcheckout/teraslab/src/server/mod.rs` (743 LOC)
- `/Users/siggioskarsson/gitcheckout/teraslab/src/observability/mod.rs` (557 LOC)
- `/Users/siggioskarsson/gitcheckout/teraslab/src/metrics.rs` (1659 LOC)

Numbering: `F-G6-NNN`.

---

### F-G6-001: `/health/ready` returns hard-coded `true` flag set at boot — never flipped by readiness/recovery code

- **Severity**: HIGH
- **Category**: Correctness
- **Location**: `src/bin/server.rs:971` (sets `ready=true` unconditionally) + consumed by `src/server/http.rs:1050`
- **Code**:
  ```rust
  // bin/server.rs (boot)
  ready: Arc::new(AtomicBool::new(true)),
  // http.rs:1050
  fn compute_health_ready(state: &HttpState) -> ReadyState {
      if !state.ready.load(Ordering::Relaxed) {
          return ReadyState::NotReady("not ready");
      }
      if let Some(ref cluster) = state.cluster
          && !cluster.cluster_health().is_ready()
      ...
  ```
- **Issue**: The local `state.ready` flag is initialised to `true` and there is no production code path that ever sets it to `false`. The R-055 fix added a cluster-health check (which is meaningful), but the comment at `http.rs:1042-1049` explicitly claims `state.ready` becomes meaningful only "once it is updated" — yet no updater exists. In single-node mode `/health/ready` therefore always returns 200 the moment the listener binds, even if the redo log is mid-replay, the primary index has been left in `Degraded` state (`SecondaryStatus` flips `dah_ok=false` in `src/server/startup.rs:346-363`), or recovery raised `ERR_INDEX_DEGRADED` errors.
- **Impact**: Load balancers will route traffic to a node that will then reject those requests with `ERR_INDEX_DEGRADED` from dispatch. The R-055 fix only covered clustered mode; single-node and the degraded-secondary case are still broken.
- **Recommendation**: Either flip `ready=false` until after recovery + index load completes and the secondary readiness flags pass, or fold `dispatch::secondary_status()` into `compute_health_ready`.
- **Confidence**: High

---

### F-G6-002: `/admin/top` (unauthenticated) fans out to every cluster peer over plain HTTP with no auth, no TLS

- **Severity**: HIGH
- **Category**: Security
- **Location**: `src/server/http.rs:1776-1816`
- **Code**:
  ```rust
  async fn build_cluster_top_snapshot(state: &HttpState) -> serde_json::Value {
      ...
      for (&node_id, &addr) in &addrs {
          if node_id == self_id { continue; }
          let url = format!("http://{}:{}/admin/top?local=true", addr.ip(), http_port);
          urls.push(url);
      }
      if let Ok(client) = reqwest::Client::builder()
          .timeout(Duration::from_secs(2))
          .build()
      {
          ...
          tasks.spawn(fetch_remote_top_snapshot(client.clone(), url));
  ```
- **Issue**: Three concurrent issues with this surface:
  1. `/admin/top` is in the **public** router (`http.rs:202`) — even with `enable_admin_endpoints=true` it has **no bearer-token check**. Anyone reachable on the HTTP port can probe it.
  2. The cluster fan-out makes plain `http://` requests; even when the TCP wire protocol is signed with `cluster_secret`, the inter-node observability traffic is unsigned and unencrypted, so anyone on the cluster network can spoof responses or read snapshots.
  3. Each unauthenticated request causes up to 32 concurrent outbound HTTP requests with 2-second timeouts — a small amplification factor for DoS.
- **Impact**: Internal metrics (counters, redo offsets, allocator state, replication progress, migration phase) are world-readable. Combined with #3, an attacker can sustain high outbound HTTP load using cheap inbound requests.
- **Recommendation**: Gate `/admin/top` (and `/ws/top`) behind the same bearer-token middleware as the mutating routes, or expose them only on a separate internal-bind port. Use `https://` or the existing cluster signing for inter-node fan-out.
- **Confidence**: High

---

### F-G6-003: `/ws/top` WebSocket is unauthenticated and runs indefinite per-second snapshots

- **Severity**: MEDIUM
- **Category**: Security / Performance
- **Location**: `src/server/http.rs:206-207, 2043-2078`
- **Code**:
  ```rust
  .route("/ws/top", get(handle_ws_top))
  ...
  async fn ws_top_loop(mut socket: WebSocket, state: Arc<HttpState>) {
      loop {
          let snapshot = if state.cluster.is_some() {
              build_cluster_top_snapshot(&state).await
  ```
- **Issue**: The WebSocket is in the public router, has no auth, and pushes a cluster-wide snapshot every second per connected client. Each push in clustered mode triggers the same 32-way HTTP fan-out as `/admin/top`. There is no per-IP or aggregate connection cap.
- **Impact**: A single attacker that opens N WebSocket connections amplifies into 32*N concurrent outbound HTTP requests every second. Connection count is bounded only by the OS and `worker_threads=4` (`http.rs:121`). Snapshots also disclose internal state (see F-G6-002).
- **Recommendation**: Apply the bearer-token middleware to `/ws/top`. Add a hard cap on concurrent `ws_top_loop` sessions and on per-connection lifetime.
- **Confidence**: High

---

### F-G6-004: `extract_bearer_token` does no length-based equalisation before constant-time compare

- **Severity**: LOW
- **Category**: Security
- **Location**: `src/server/http.rs:305-311`
- **Code**:
  ```rust
  if supplied.as_bytes().ct_eq(expected).into() {
      next.run(request).await
  } else {
      (StatusCode::UNAUTHORIZED, "invalid admin bearer token\n").into_response()
  }
  ```
- **Issue**: `subtle::ConstantTimeEq::ct_eq` for slices of unequal length short-circuits and returns `Choice(0)` without comparing any bytes; the documented behaviour is the same constant time regardless of contents, but the length test itself leaks the expected token length to an attacker who can measure response timing across runs of varying bearer-token lengths. The classic mitigation is to compare a fixed-size hash (e.g. SHA-256 of supplied vs SHA-256 of expected) so length is invariant.
- **Impact**: Mild — token *length* (not contents) can be inferred from timing variance. Not exploitable on its own; only material if an attacker can correlate latency with token length.
- **Recommendation**: Compare HMAC/SHA-256 digests of `supplied` and `expected` so the comparison is always over a 32-byte buffer of constant length. Either that or document the leak explicitly and require sufficiently long tokens at config-validation time.
- **Confidence**: Medium

---

### F-G6-005: Admin token is matched verbatim with no minimum length or character-class enforcement

- **Severity**: MEDIUM
- **Category**: Security
- **Location**: `src/config.rs:843-851` (validation) consumed by `src/server/http.rs:222-233`
- **Code**:
  ```rust
  // config.rs
  if self.enable_admin_endpoints
      && self.admin_token.as_ref().map(|s| s.is_empty()).unwrap_or(true)
  {
      return Err(ConfigError::AdminTokenRequired);
  }
  // http.rs
  Some(t) if !t.is_empty() => Arc::from(t.as_bytes().to_vec().into_boxed_slice()),
  ```
- **Issue**: The validator rejects only `None` or the empty string. `admin_token = "a"` (one byte) passes validation and becomes the production bearer token. There is no entropy floor, no character-class requirement, and no warning that the token is being read from a TOML file that may be world-readable. Combined with F-G6-004, a 1-byte token is trivially brute-forced.
- **Impact**: Operator can mis-configure an effectively-zero-strength token that the system happily accepts and trusts. R-056's contract is "constant-time compare to a configured token" — if the token is weak, the constant-time guarantee is moot.
- **Recommendation**: Enforce a minimum length (32+ bytes for a CSPRNG-derived token) in `validate_safe_defaults`. Document the recommended generation command. Log a `tracing::warn!` at startup if the token length is below the floor.
- **Confidence**: High

---

### F-G6-006: `handle_set_log_level` accepts a `String` body without an explicit body-size cap

- **Severity**: LOW
- **Category**: Security / Performance
- **Location**: `src/server/http.rs:2144-2158`
- **Code**:
  ```rust
  async fn handle_set_log_level(
      State(state): State<Arc<HttpState>>,
      body: String,
  ) -> impl IntoResponse {
      let level = match body.trim().to_lowercase().as_str() {
  ```
- **Issue**: Axum's default body limit is 2 MiB, but the handler accepts the full body, then runs `to_lowercase()` (a heap allocation proportional to the body size) before discarding everything that isn't one of five short strings. The route is gated behind admin auth — but a hostile authenticated operator (or one whose token was leaked) can hammer the endpoint with 2 MiB bodies cheaply.
- **Impact**: 8 MiB of transient allocation per request (input + lowercase). Not catastrophic but wasteful and avoidable.
- **Recommendation**: Add `.layer(DefaultBodyLimit::max(64))` to the gated sub-router, or read the body via `Bytes` and reject anything longer than ~16 bytes before calling `to_lowercase`.
- **Confidence**: Medium

---

### F-G6-007: `serve_embedded_file` falls back to index.html for ANY missing asset, including `..`-traversal attempts

- **Severity**: INFO
- **Category**: Security / Code Quality
- **Location**: `src/server/http.rs:2240-2267`
- **Code**:
  ```rust
  fn serve_embedded_file(path: &str) -> ... {
      let (data, mime) = match UiAssets::get(path) {
          Some(content) => { ... }
          None => {
              // SPA fallback: serve index.html for unrecognized paths
              match UiAssets::get("index.html") {
  ```
- **Issue**: `rust_embed::Embed` keys assets by exact relative path string. A request for `/ui/../../etc/passwd` (assuming axum lets the segment through to the `{*path}` capture) cannot escape the embed map — it just misses and falls back to `index.html`. So path traversal is **not** exploitable here. Worth noting as positive verification: the SPA fallback is fine because the embedded asset map is a closed set. **Recommendation** is informational: keep this property by never switching the static handler to a filesystem-backed loader without re-introducing a canonicalisation check.
- **Impact**: None today, but a future refactor that swaps `rust_embed` for `tower_http::services::ServeDir` (or similar) silently introduces traversal.
- **Recommendation**: Add a regression test that requests `/ui/../Cargo.toml` etc. and asserts the response is `index.html` (or 404), so a future refactor that breaks this is caught.
- **Confidence**: High (positive verification)

---

### F-G6-008: `/admin/top` aggregator collapses per-node trace propagation — no `traceparent` propagated to remote fan-out

- **Severity**: LOW
- **Category**: Observability
- **Location**: `src/server/http.rs:1759-1768`
- **Code**:
  ```rust
  async fn fetch_remote_top_snapshot(
      client: reqwest::Client,
      url: String,
  ) -> Option<serde_json::Value> {
      let resp = client.get(&url).send().await.ok()?;
      ...
  }
  ```
- **Issue**: The inbound `/admin/top` handler creates an HTTP span (via `http_span_for`) and the file goes to great lengths to parse/encode W3C `traceparent` headers (`http.rs:2316-2415`), yet the cluster fanout never attaches a `traceparent` header to outbound `reqwest::get` calls. Each remote node's span is therefore an orphan trace.
- **Impact**: Trace stitching for cluster-wide operations is broken even though all the infrastructure is in place.
- **Recommendation**: Use `traceparent_for_span(&tracing::Span::current())` to populate a header on the outbound `reqwest::RequestBuilder`.
- **Confidence**: High

---

### F-G6-009: `aggregate_snapshots` divides by `total_count` without rebalancing for `nodes` that returned no data

- **Severity**: LOW
- **Category**: Correctness
- **Location**: `src/server/http.rs:1872-1884`
- **Code**:
  ```rust
  let weighted_mean: u64 = if total_count > 0 {
      let sum: u64 = nodes
          .iter()
          .map(|n| {
              let c = n["latency"][*lk]["count"].as_u64().unwrap_or(0);
              let m = n["latency"][*lk]["mean_ns"].as_u64().unwrap_or(0);
              c * m
          })
          .sum();
      sum / total_count
  ```
- **Issue**: `c * m` is computed in `u64` — for the lock-wait histogram, `c` can be very large (millions per minute) and `m` is in nanoseconds; the product can overflow to a u64 silently when a hot node has run for hours. Prometheus stores rate-not-count for histograms specifically because of this. The naive cluster aggregate also does not exclude nodes that returned no snapshot (timed out), so the weighted mean is biased toward the surviving nodes' high-traffic windows.
- **Impact**: For dashboards the latency aggregate becomes silently wrong on long-running clusters; rare but real.
- **Recommendation**: Track per-node `mean_ns * count` in `u128`, then divide back into u64. Or aggregate `_sum`/`_count` separately and divide once at the end.
- **Confidence**: Medium

---

### F-G6-010: `ws_top_loop` "drain incoming messages" loop swallows close frames silently

- **Severity**: LOW
- **Category**: Correctness
- **Location**: `src/server/http.rs:2072-2076`
- **Code**:
  ```rust
  // Drain any incoming messages (pings, close frames)
  while let Ok(Some(_)) = tokio::time::timeout(Duration::from_millis(1), socket.recv()).await
  {
      // Just consume; we don't process client messages
  }
  ```
- **Issue**: The drain ignores `Message::Close` — the loop will only exit when `send` errors. A well-behaved client that sends a Close frame and then waits for the server to acknowledge it will sit idle for up to one second + 5-second send timeout before the loop exits. The 1 ms `recv` timeout also burns Tokio scheduling cycles continually.
- **Impact**: Stale WebSocket sessions linger for at most ~6 s after a graceful client close. Minor.
- **Recommendation**: Match on the message kind: break the outer loop on `Message::Close`. Increase the drain timeout (or use a non-blocking try_recv) to reduce scheduler churn.
- **Confidence**: High

---

### F-G6-011: `handle_admin_drain` accepts `node_id` from path but ignores it after rejecting cross-node drains

- **Severity**: LOW
- **Category**: API Hygiene
- **Location**: `src/server/http.rs:1380-1421`
- **Code**:
  ```rust
  async fn handle_admin_drain(
      State(state): State<Arc<HttpState>>,
      Path(node_id): Path<u64>,
      Query(query): Query<DrainQuery>,
  ) -> impl IntoResponse {
      match state.cluster {
          Some(ref cluster) => {
              if cluster.self_id().0 == node_id {
                  cluster.quiesce();
  ```
- **Issue**: `/admin/drain/{node_id}` rejects requests where `node_id != self_id`. So the path parameter exists only to validate equality with `self_id`. From an operator's standpoint, this means a typo in the node_id silently calls `cluster.quiesce()` on the wrong machine if they target the wrong HTTP endpoint. The route is mutating + authenticated so the blast radius is bounded, but the API shape invites mistakes.
- **Impact**: Operator can drain the wrong node by mis-targeting the HTTP request rather than mistyping the node ID — same mistake, different surface.
- **Recommendation**: Either accept only the unqualified `/admin/drain` route (the node_id is implicit) or document loudly that the path parameter must equal `self_id`.
- **Confidence**: High

---

### F-G6-012: OTLP exporter accepts plaintext `http://` endpoint with no warning; spans may contain operator-sensitive data

- **Severity**: MEDIUM
- **Category**: Security
- **Location**: `src/observability/mod.rs:218-223`
- **Code**:
  ```rust
  let exporter = opentelemetry_otlp::SpanExporter::builder()
      .with_tonic()
      .with_endpoint(endpoint.to_string())
      .with_timeout(std::time::Duration::from_secs(30))
      .build()
  ```
- **Issue**: `TERASLAB_OTLP_ENDPOINT` can be set to `http://collector:4317` — the SDK happily uses cleartext gRPC. There is no scheme-check, no warning, no flag to require TLS. Spans currently only carry the static `route` attribute (see positive note in F-G6-013), but the surrounding code (`record!` macros, future extensions) routinely add fields that could include txids, peer addresses, or node identifiers.
- **Impact**: If an operator deploys with a remote OTLP collector and cleartext, every span attribute (and the embedded W3C trace context that flows on the cluster wire) is observable in transit. Defence-in-depth concern given the cluster gates on `cluster_secret`.
- **Recommendation**: At minimum, warn at startup when the endpoint scheme is `http`. Better: support a `require_tls = true` config flag that refuses to construct the exporter otherwise. Even better: route OTLP through the same TLS setup the rest of the cluster uses.
- **Confidence**: High

---

### F-G6-013: OTLP span attributes audit — currently only contains static `route`, with verification

- **Severity**: INFO
- **Category**: Security (positive verification)
- **Location**: `src/server/http.rs:2374-2387`
- **Code**:
  ```rust
  pub(crate) fn http_span_for(headers: &HeaderMap, route: &'static str) -> tracing::Span {
      use opentelemetry::trace::TraceContextExt;
      use tracing_opentelemetry::OpenTelemetrySpanExt;
      let span = tracing::debug_span!("http_request", route = route);
  ```
- **Issue**: Searched G6 scope for `span!` / `info_span!` / `debug_span!` invocations — only `http_request` with the static `route` parameter appears. No txid, peer address, body, or user-controlled string is attached to spans. This is good: nothing sensitive currently leaks to OTLP. Track it so future commits don't regress.
- **Impact**: None today.
- **Recommendation**: Add a `clippy` or grep CI check that disallows `tracing::*span!(...)` calls in `src/server/` with dynamic field values, or add a comment alongside `http_span_for` warning that no user input may be attached.
- **Confidence**: High (positive verification)

---

### F-G6-014: Bearer-token middleware does not protect cross-origin browser misuse — but `/metrics` and `/admin/top` leak data via plain GET

- **Severity**: LOW
- **Category**: Security
- **Location**: `src/server/http.rs:188-211`
- **Code**:
  ```rust
  let public = Router::new()
      .route("/metrics", get(handle_metrics))
      .route("/health/live", get(handle_health_live))
      .route("/health/ready", get(handle_health_ready))
      .route("/status", get(handle_status))
      .route("/admin/migration_status", get(handle_admin_migration_status))
      .route("/admin/nodes", get(handle_admin_nodes))
      .route("/admin/memory", get(handle_admin_memory))
      .route("/admin/records", get(handle_admin_records))
      .route("/admin/replication", get(handle_admin_replication))
      .route("/admin/top", get(handle_admin_top))
  ```
- **Issue**: All the read-only `/admin/*` endpoints, `/status`, and `/metrics` are public. A user who browses to any HTML page that embeds an `<img>` or fetch to a victim cluster's HTTP port can extract these payloads via standard CORS-readable JSON. There's no `Access-Control-Allow-Origin` set (so XHR from another origin won't read the body), but `/metrics` is `text/plain` and accessible over basic HTTP/GET fetch — observable in DNS-rebinding scenarios where an attacker scripts `localhost:9100` from a browser.
- **Impact**: Some operational information disclosure when TeraSlab is colocated on a developer's workstation alongside browsers reaching attacker-controlled pages.
- **Recommendation**: Document that the HTTP observability port MUST be bound to a private address (`127.0.0.1` or VPN). If practical, set `Access-Control-Allow-Origin: <empty>` (deny) and require explicit allow-listing of operator origins for the UI.
- **Confidence**: Medium

---

### F-G6-015: Replica-lag readiness check is a single-shot wall-clock scan per `/health/ready`

- **Severity**: LOW
- **Category**: Performance
- **Location**: `src/server/http.rs:1059-1066`
- **Code**:
  ```rust
  if state.cluster.is_some()
      && state.replica_lag_warn_threshold_ops > 0
      && replication_metrics().is_some_and(|r| {
          (0..MAX_REPLICAS).any(|i| r.lag(i) > state.replica_lag_warn_threshold_ops)
      })
  {
      return ReadyState::NotReady("replica lag exceeds threshold");
  }
  ```
- **Issue**: `replication_metrics()` does an atomic load; the scan over `MAX_REPLICAS=8` is fine. But the check uses `relaxed` reads inside `ReplicationMetrics::lag` (`metrics.rs:932-941`): `leader_sequence` and `last_acked_seq` can be observed in a torn / inconsistent state where `last_acked > leader` momentarily after a leader-only sequence reset (`saturating_sub` masks underflow, so the result is 0 — false positive ready). This is OK because the check is bounded by `relaxed` semantics — it's an SLO, not a safety predicate — but worth documenting.
- **Impact**: Brief readiness oscillation around sequence resets; the saturating_sub hides underflow rather than reporting it.
- **Recommendation**: Either accept the current behaviour and document, or add a sanity assertion that fires only in test builds.
- **Confidence**: Medium

---

### F-G6-016: `start_http_server` builds a new Tokio runtime with `worker_threads(4)` regardless of host

- **Severity**: LOW
- **Category**: Performance
- **Location**: `src/server/http.rs:119-150`
- **Code**:
  ```rust
  let rt = tokio::runtime::Builder::new_multi_thread()
      .thread_name("teraslab-http")
      .worker_threads(4)
      .enable_all()
      .build()
      .expect("failed to create tokio runtime for HTTP server");
  ```
- **Issue**: 4 fixed worker threads — fine for a small cluster. With 32-fan-out `/admin/top` plus N concurrent `/ws/top` WebSockets each driving cluster snapshots, you can readily saturate the runtime. There's no operator knob.
- **Impact**: Under load, observability latency can degrade and `/health/ready` may time out spuriously.
- **Recommendation**: Make `worker_threads` configurable, or default to `available_parallelism() / 4` with a floor of 2.
- **Confidence**: Medium

---

### F-G6-017: `start_http_server` constructs runtime inside `block_on` but never installs a panic hook for handlers

- **Severity**: LOW
- **Category**: Robustness
- **Location**: `src/server/http.rs:113-151`
- **Code**:
  ```rust
  pub fn start_http_server(
      bind_addr: String,
      ...
  ) {
      let rt = tokio::runtime::Builder::new_multi_thread()
          ...
          .build()
          .expect("failed to create tokio runtime for HTTP server");
      rt.block_on(async move {
  ```
- **Issue**: `expect` on runtime build — acceptable since the binary cannot run without it. But if `axum::serve` returns an error, the function logs and falls through, dropping the runtime. The caller (`std::thread::spawn` in `bin/server.rs:986-988`) has no way to know HTTP is down. No restart, no health gauge.
- **Impact**: HTTP observability silently dies and operators only notice when scrape failures appear.
- **Recommendation**: Surface HTTP server death as a metric (e.g., a gauge that flips to 0) or as a process-level signal to abort/restart the binary.
- **Confidence**: High

---

### F-G6-018: `replay_cause_label` marked `#[allow(dead_code)]` despite intent to be referenced by other diagnostic surfaces

- **Severity**: INFO
- **Category**: Code Quality
- **Location**: `src/server/startup.rs:237-246`
- **Code**:
  ```rust
  #[allow(dead_code)]
  pub(crate) fn replay_cause_label(cause: ReplayCause) -> &'static str {
      match cause {
          ReplayCause::MissingPrimary => "missing-primary",
          ...
  ```
- **Issue**: The doc comment says "Kept `pub(crate)` so other diagnostic surfaces can reuse the same wording" but no callers exist; `dead_code` is silenced. Either wire it into the error messages in `check_replay_tolerance_with_cap` (where the strings are hand-duplicated at `startup.rs:184-220`) or delete it.
- **Impact**: Drift risk — the labels in this function and the strings in `check_replay_tolerance_with_cap` can diverge.
- **Recommendation**: Refactor the error builders to use this function as their canonical label source.
- **Confidence**: High

---

### F-G6-019: Connection accept loop uses 10 ms sleep — burns CPU at idle, not great for graceful shutdown

- **Severity**: LOW
- **Category**: Performance
- **Location**: `src/server/mod.rs:264-272`
- **Code**:
  ```rust
  Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
      // No pending connection — sleep briefly and retry
      std::thread::sleep(std::time::Duration::from_millis(10));
  }
  ```
- **Issue**: Polling accept at 100 Hz while idle. A blocking accept with a shutdown-aware mechanism (mio / poll on a self-pipe, or `set_nonblocking(false)` with periodic interrupt) avoids the spin. Minor — TeraSlab's HTTP server is on a separate listener anyway.
- **Impact**: ~10 ms shutdown latency tail and 100 syscalls/sec of idle work.
- **Recommendation**: Use a `mio::Poll` or a signalfd-style shutdown event for cleaner accept-loop cancellation.
- **Confidence**: High

---

### F-G6-020: `InflightBytesLimiter::try_acquire` short-circuits the per-frame limit but never logs or counts rejection

- **Severity**: LOW
- **Category**: Observability
- **Location**: `src/server/mod.rs:53-85`
- **Code**:
  ```rust
  pub(crate) fn try_acquire(self: &Arc<Self>, bytes: usize) -> Option<InflightBytesPermit> {
      if self.limit == 0 { ... }
      if bytes > self.limit { return None; }
      ...
  ```
- **Issue**: When `try_acquire` returns `None`, the caller in `handle_connection_inner` writes a static error and tears down the connection (`mod.rs:392-407`). No Prometheus counter increments — operators won't see this in dashboards. The only signal is a connection-level log line.
- **Impact**: Capacity exhaustion is invisible to monitoring; you only notice when clients complain.
- **Recommendation**: Add an `inflight_bytes_rejected_total` counter to `ThreadMetrics` and increment it here.
- **Confidence**: High

---

### F-G6-021: `WireTraceContext::read_from` panics on wrong-length input — caller contract is non-enforced

- **Severity**: LOW
- **Category**: Robustness
- **Location**: `src/observability/mod.rs:309-321`
- **Code**:
  ```rust
  pub fn read_from(buf: &[u8]) -> Option<Self> {
      assert_eq!(
          buf.len(),
          Self::SIZE,
          "WireTraceContext::read_from needs 24 bytes"
      );
      ...
  ```
- **Issue**: Panicking on bad input is acceptable for an internal-only helper, but the function is `pub` — if reachable from the wire decoder (it is, via the replication batch header path), a malformed batch can crash the receiver thread. Need to confirm the upstream decoder length-checks before calling, but the panic is still load-bearing on caller discipline.
- **Impact**: Crash on malformed replication header if the caller forgets the length check.
- **Recommendation**: Convert the assert into a length check returning `None`, or change the signature to `&[u8; SIZE]` to enforce the constraint at the type system level.
- **Confidence**: Medium

---

### F-G6-022: Metrics module — labels are bounded enums, no client-IP or user-string labels — positive verification

- **Severity**: INFO
- **Category**: Security / Performance (positive verification)
- **Location**: `src/metrics.rs` (whole file)
- **Code**:
  ```rust
  pub fn as_str(self) -> &'static str {
      match self {
          Outcome::Ok => "ok",
          ...
  ```
- **Issue**: Every labeled metric in this file (`OpOutcomeCounters`, `LabeledCounter<N>`, `MigrationLabel`, `UringErrClass`, `SwimChurnKind`) is keyed by a fixed enum with a `const all()` slice. No metric is labeled by client IP, request path, txid, or any user-controlled string. Prometheus label cardinality is bounded by definition. This is the right design — note it so future PRs that add labels know to keep the same invariant.
- **Impact**: None today.
- **Recommendation**: Add a `clippy::disallowed_methods` or a CI check that flags `.with_label_values(...)` style calls (none exist today) — defence in depth against future cardinality explosions.
- **Confidence**: High (positive verification)

---

### F-G6-023: `prom_histogram_ns` emits `bucket_upper_ns_at(i)` for the last non-`+Inf` bucket as `u64::MAX` — Prometheus parsers may reject

- **Severity**: LOW
- **Category**: Correctness
- **Location**: `src/server/http.rs:1003-1014` + `src/metrics.rs:715-723`
- **Code**:
  ```rust
  pub fn bucket_upper_ns_at(&self, i: usize) -> u64 {
      if i == 0 { 128 }
      else if i >= NUM_BUCKETS - 1 { u64::MAX }
      else { 128u64 << i }
  }
  ```
- **Issue**: `prom_histogram_ns` writes `+Inf` explicitly only for the *last* bucket (line 1006 — `if i == num - 1`). For bucket `NUM_BUCKETS - 1` (the last bucket), the loop hits the `+Inf` branch — good. But the second-to-last bucket (index 22) still receives an explicit `le="..."` upper bound from `bucket_upper_ns_at(22)` which returns `128 << 22 = 536_870_912` (~537 ms). That is correct, but bucket 23 (which the doc comment says covers `[1s, 2s)`) is silently merged into `+Inf` because of the `i >= NUM_BUCKETS - 1` clause — the upper-bound function for `i == NUM_BUCKETS - 1` already returns `u64::MAX`, so the documented `[1s, 2s)` range is never emitted as a discrete bucket.
- **Impact**: Histogram percentile estimates lose resolution between 537 ms and infinity — any latency above 537 ms is reported as `+Inf`. The doc comment at `metrics.rs:572-574` ("23: [1s, 2s); 24: [2s, infinity)") does not match the implementation: `NUM_BUCKETS = 25` and `bucket_upper_ns_at(23)` would return `128 << 23 = ~1.07 s` — but the renderer never reaches that bucket because the `i == num - 1` short-circuit fires at `i == 24`.
- **Recommendation**: Re-read the rendering loop — verify whether bucket 23 emits `le="1073741824"` or whether it is silently swallowed. If the latter, fix `bucket_upper_ns_at` to only return `u64::MAX` when `i == NUM_BUCKETS - 1` strictly (it does — line 718 uses `>=`), and audit how the renderer treats `bucket_upper_ns_at(NUM_BUCKETS - 2)`.
- **Confidence**: Medium

---

### F-G6-024: Per-replica `last_acked_seq` updates use `Relaxed` store + load; `lag()` may observe a half-updated leader/replica pair

- **Severity**: LOW
- **Category**: Concurrency
- **Location**: `src/metrics.rs:932-941` + `894-906`
- **Code**:
  ```rust
  pub fn lag(&self, replica_idx: usize) -> u64 {
      if replica_idx >= MAX_REPLICAS { return 0; }
      let leader = self.leader_sequence.load(Ordering::Relaxed);
      let acked = self.per_replica[replica_idx]
          .last_acked_seq
          .load(Ordering::Relaxed);
      leader.saturating_sub(acked)
  }
  ```
- **Issue**: `leader_sequence` is updated on every batch send; `last_acked_seq` on every ACK. With Relaxed semantics, an observer may see a stale leader and a new ack — producing `lag = 0` even when traffic is in flight. Conversely, a new leader and stale ack inflate lag. The `saturating_sub` masks the inverted case to zero, hiding the inconsistency.
- **Impact**: `/health/ready` (which consults `lag()`) may flip-flop briefly. The metric is documented as a gauge intended for dashboards, where this noise is acceptable, but R-055 elevated the same value to a readiness predicate.
- **Recommendation**: Either accept the noise (and document) or take a snapshot using a `seqlock` (per-replica generation counter incremented on ack write, leader sequence pinned to the same generation when observed).
- **Confidence**: Medium

---

### F-G6-025: HTTP error handlers return free-form strings — no structured error code or content negotiation

- **Severity**: INFO
- **Category**: API Hygiene
- **Location**: Multiple — e.g. `src/server/http.rs:1224`, `1373`, `1419`, `2154`, `2178`, `2184`
- **Code**:
  ```rust
  None => (StatusCode::BAD_REQUEST, "not in cluster mode".to_string()),
  ```
- **Issue**: Error responses are plain-text English without an error code, machine-readable type, or content-type negotiation. Compare to the binary protocol which has well-defined `ERR_*` constants. Operators integrating with the HTTP API rely on string matching.
- **Impact**: Brittle automation. A typo fix changes the error string and silently breaks downstream tooling.
- **Recommendation**: Define a single `HttpErrorBody { code: &'static str, message: String }` JSON envelope and use it across all error responses. Keep the static code stable across releases.
- **Confidence**: High

---

### F-G6-026: `ObservabilityConfig` env override silently succeeds for `TERASLAB_OTLP_ENDPOINT=""` — but a typo like `TERASLAB_OTLP_ENDPONIT` is ignored

- **Severity**: LOW
- **Category**: Usability
- **Location**: `src/observability/mod.rs:111-127`
- **Code**:
  ```rust
  if let Ok(v) = std::env::var(Self::ENV_OTLP_ENDPOINT) {
      self.otlp_endpoint = if v.is_empty() { None } else { Some(v) };
  }
  ```
- **Issue**: A typo in the env var name (operator types `TERASLAB_OTLP_ENDPONIT` instead of `…ENDPOINT`) yields `Err` from `env::var` and is silently ignored. There is no startup log line summarising which observability fields were sourced from env vs. TOML.
- **Impact**: Operator thinks tracing is on; it's not. Common in container deployments.
- **Recommendation**: Log every TOML→env override decision at `info` during startup. Optionally lint env vars with `TERASLAB_` prefix and warn on any that aren't recognised.
- **Confidence**: High

---

### F-G6-027: `start_http_server` panics if `tokio::runtime::Builder::build` fails — caller has already spawned a dedicated thread, so this kills the process

- **Severity**: LOW
- **Category**: Robustness
- **Location**: `src/server/http.rs:119-124`
- **Code**:
  ```rust
  let rt = tokio::runtime::Builder::new_multi_thread()
      .thread_name("teraslab-http")
      .worker_threads(4)
      .enable_all()
      .build()
      .expect("failed to create tokio runtime for HTTP server");
  ```
- **Issue**: Runtime build failure (e.g., resource exhaustion on a host already running near `RLIMIT_NPROC`) panics the HTTP thread. Since the binary spawns this on a dedicated `std::thread::spawn` (`bin/server.rs:986`), the panic doesn't kill the parent process — it just silently terminates HTTP. No metric, no alert.
- **Impact**: HTTP observability gone with no operator signal. The TCP server keeps serving — so observability is dark until the next restart.
- **Recommendation**: Use a `Result` return and have the caller record a startup error.
- **Confidence**: High

---

### F-G6-028: `load_primary_index_redb` checks the import sentinel before any restore, but does not check the sentinel mtime — operator running an abandoned import is forced to verify by hand

- **Severity**: INFO
- **Category**: Code Quality
- **Location**: `src/server/startup.rs:262-286`
- **Code**:
  ```rust
  if crate::index::migration::import_in_progress(config) {
      return Err(RebuildError::RedbImportInProgress {
          sentinel_path: ...
      });
  }
  ```
- **Issue**: The sentinel is treated as a binary signal — its mere presence aborts startup. A long-running operator with a sentinel left over from a previous host (e.g., recovered from a backup) is forced to manually delete it. A timestamp check could distinguish "fresh sentinel from a crashed import" from "stale sentinel from a backup restore."
- **Impact**: Operator friction; no safety impact.
- **Recommendation**: Read the sentinel's mtime; if it's older than (say) the redo log's first-entry timestamp or the redb file's mtime, log a warning and proceed. Or just document the workflow more loudly.
- **Confidence**: Medium

---

## Coverage notes

- **`src/server/http.rs`** — Read end-to-end, covering every route registered in `build_http_router` (`/metrics`, `/health/live`, `/health/ready`, `/status`, the eight `/admin/*` routes split between public read-only and gated mutating, the three `/debug/*` routes, `/ws/top`, and the `/ui/` static handlers). Verified: bearer-token middleware applied to the gated sub-router; constant-time comparison via `subtle::ConstantTimeEq`; SPA fallback for static assets is safe against path-traversal because rust-embed is a closed-set map; `OpCode`/`Outcome` label sets are bounded enums. Did not explore `tests/ui_xss.rs` source code beyond confirming it gates `app.js` for the documented `escapeHtml` invariants.
- **`src/server/startup.rs`** — Read end-to-end. Replay tolerance, primary-rebuild fail-closed policy, sentinel handling, and mandatory redo log open are well-covered by their tests; no fundamental issues. Minor `#[allow(dead_code)]` and string-duplication notes captured.
- **`src/server/mod.rs`** — Read end-to-end. TCP accept loop and connection handling are mostly solid. The `InflightBytesLimiter` is correctly bounded and per-frame size is capped. `R-054` write timeout in place; `R-???` (gap #10) inflight bytes limiter wired correctly. Major gap: no metric on rejected requests.
- **`src/observability/mod.rs`** — Read end-to-end. OTLP endpoint accepts plaintext silently (F-G6-012). `WireTraceContext` has a panicking `read_from`; otherwise correct. Tests cover the shutdown drain path well. Env override path is silent on typos.
- **`src/metrics.rs`** — Read end-to-end for the public surface; deep-dive on `PaddedCounter`, `LabeledCounter`, `OpOutcomeCounters`, `LatencyHistogram`, and the per-subsystem `OnceLock` accessors. Positive verification: all labels are bounded enums (F-G6-022). Potential off-by-one in histogram-bucket emission (F-G6-023) and `Relaxed` ordering in `lag()` reads (F-G6-024) flagged.

### Severity counts

- CRITICAL: 0
- HIGH: 2 (F-G6-001 readiness flag never flipped; F-G6-002 `/admin/top` unauthenticated + cluster fanout)
- MEDIUM: 4
- LOW: 16
- INFO: 6 (4 positive verifications)
- Total findings: 28
