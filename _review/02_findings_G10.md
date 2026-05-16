# Group G10 Findings — binaries + config + lib root

Scope:
- `src/bin/server.rs` (1259 LOC) — daemon entry point
- `src/bin/cli.rs` (1280 LOC) — operator CLI
- `src/config.rs` (1778 LOC) — `ServerConfig`, TOML parse, env overrides
- `src/lib.rs` (22 LOC) — top-level lib re-exports

Prior audits cross-checked: R-056 (admin-token gating) integrates correctly with config.rs / server.rs / http.rs.

---

### F-G10-001: `ctrlc_handler` is a no-op — SIGINT/SIGTERM never triggers graceful shutdown
- **Severity**: CRITICAL
- **Category**: Correctness
- **Location**: `src/bin/server.rs:1196`
- **Code**:
  ```rust
  fn ctrlc_handler<F: Fn() + Send + 'static>(handler: F) {
      // Unfortunately without a signal crate, we can't easily catch SIGINT.
      // The server's read timeout + shutdown flag handle graceful shutdown.
      // For production, add the `ctrlc` or `signal-hook` crate.
      drop(handler);
  }
  ```
- **Issue**: The function takes a "handler" closure and immediately drops it. No `ctrlc`/`signal-hook` dependency is present in `Cargo.toml`, so the binary registers no signal handler at all. The `shutdown_flag` passed into the handler closure (line 1000-1005) is therefore never flipped from outside.
- **Impact**: On `kill -TERM` / Ctrl-C the daemon is hard-killed by the OS. The cleanup path in `ServerWithShutdown::run` (cluster shutdown, index snapshot, allocator persist, replication-intent tracker flush, device.sync, OTLP flush) NEVER RUNS in production. Result: stale snapshot, lost OTLP spans, replication-intent file not flushed, device dirty bit left set on platforms that track it. Replays on restart will be longer; in some cases data not yet snapshot-committed will only survive via redo replay.
- **Recommendation**: Add `ctrlc` or `signal-hook` (or use `tokio::signal` if porting to async). Wire SIGTERM/SIGINT to set `shutdown_flag` AND call `server.inner.shutdown()` (see F-G10-002 — the bin's `shutdown_flag` and `Server`'s internal `shutdown` are two different atomics today).
- **Confidence**: High

### F-G10-002: Binary `shutdown_flag` is disconnected from `Server`'s internal shutdown atomic
- **Severity**: CRITICAL
- **Category**: Correctness
- **Location**: `src/bin/server.rs:1000-1005`, `src/server/mod.rs:162` `Arc::new(AtomicBool::new(false))`
- **Code**:
  ```rust
  let shutdown_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
  let shutdown_clone = shutdown_flag.clone();
  ctrlc_handler(move || {
      tracing::info!("shutdown signal received");
      shutdown_clone.store(true, std::sync::atomic::Ordering::Relaxed);
  });
  ```
  Server has its own private flag (`src/server/mod.rs:146,162`):
  ```rust
  shutdown: Arc<AtomicBool>,
  ...
  shutdown: Arc::new(AtomicBool::new(false)),
  ```
- **Issue**: `Server::new` creates an internal `shutdown: Arc<AtomicBool>` with no public setter. The bin builds a separate `shutdown_flag` Arc and passes it only to `checkpoint`, `blob_gc`, and `lag_monitor`. The TCP accept loop in `Server::run` polls its own private flag, which nothing can flip externally. Even if F-G10-001 were fixed, the accept loop would never exit because the two Arcs are unrelated.
- **Impact**: `server.run()` blocks forever; `ServerWithShutdown::run` post-shutdown logic is dead code. Background tasks (`checkpoint`, `blob_gc`, `lag_monitor`) can still see the bin's flag and exit, but the TCP listener and the dispatch loop will not.
- **Recommendation**: Either (a) expose `Server::with_shutdown(Arc<AtomicBool>)` so the bin can share its flag, or (b) make `Server` hold a `Weak<AtomicBool>` provided externally. Then have the signal handler set it.
- **Confidence**: High

### F-G10-003: No redo log fsync on shutdown
- **Severity**: HIGH
- **Category**: Correctness
- **Location**: `src/bin/server.rs:1149-1193` (`ServerWithShutdown::run`)
- **Code**:
  ```rust
  match self.engine.snapshot_index(&self.snap_path) { ... }
  match self.engine.persist_allocator() { ... }
  match teraslab::server::dispatch::flush_replication_intent_tracker() { ... }
  if let Err(e) = self.device.sync() { ... }
  ```
- **Issue**: The shutdown path syncs the data device and the replication-intent tracker but never explicitly fsyncs the redo log (`redo_log` is not even held by `ServerWithShutdown`). If the engine's per-op redo append already fsyncs before ACKing the client, this is mostly defense-in-depth — but on a clean shutdown after a checkpoint reset, an unflushed redo-log header could mean a longer scan on restart. Pair-finding with F-G10-001/002: even if signals worked, this is incomplete.
- **Impact**: Modest. Real durability rides on per-op fsync in the hot path; this is shutdown hygiene.
- **Recommendation**: Hold `redo_log: Option<Arc<Mutex<RedoLog>>>` in `ServerWithShutdown` and call `redo_log.lock().sync()` (or equivalent) before `device.sync()`.
- **Confidence**: Medium

### F-G10-004: `device_paths[0]` panics if TOML supplies an empty `device_paths = []`
- **Severity**: HIGH
- **Category**: Correctness
- **Location**: `src/bin/server.rs:180`
- **Code**:
  ```rust
  let device_path = &config.device_paths[0];
  let device: Arc<dyn BlockDevice> =
      match DirectDevice::open(device_path, config.device_size, config.device_alignment) {
  ```
  And `src/config.rs:557-565` `resolved_redo_log_path`:
  ```rust
  let mut p = self.device_paths[0].clone().into_os_string();
  ```
- **Issue**: `device_paths` is a `Vec<PathBuf>` with default `vec![PathBuf::from("teraslab-data.dat")]`, but the field is serde-deserialized — a user TOML `device_paths = []` overrides the default with an empty vec. Both `bin/server.rs:180` and `config.rs:557` (resolved_redo_log_path) and `config.rs:573` (resolved_cluster_state_path) index `[0]` without bounds check.
- **Impact**: Empty list crashes the server on startup with `index out of bounds` panic, before tracing reports a meaningful error. Operator sees a backtrace, not a config error.
- **Recommendation**: Add a `validate_device_paths` check in `validate_safe_defaults` (or analogous) rejecting empty `device_paths` with a typed `ConfigError::NoDevicePaths`.
- **Confidence**: High

### F-G10-005: No range validation for `device_size`, `expected_records`, `lock_stripes`, `max_batch_size`, `max_connections`
- **Severity**: MEDIUM
- **Category**: Correctness
- **Location**: `src/config.rs:259-285`, `src/config.rs:619-680` (validate functions don't cover these)
- **Code**:
  ```rust
  pub device_size: u64,
  pub device_alignment: usize,
  pub redo_log_size: u64,
  ...
  pub expected_records: usize,
  pub lock_stripes: usize,
  pub max_batch_size: u32,
  pub max_connections: usize,
  ```
- **Issue**: `lock_stripes` is documented as "power of 2, default 65536" but nothing enforces power-of-2. `device_alignment = 0` would divide-by-zero downstream. `expected_records = usize::MAX` overflows in hashtable capacity (caught by `checked_mul` in `src/index/hashtable.rs:228`, but the error message is generic). `max_batch_size = 0` silently disables batching. `max_connections = 0` rejects all clients. `device_size < record_size` is undefined behavior in the allocator.
- **Impact**: Misconfiguration surfaces as cryptic runtime errors or panics rather than a typed config rejection at startup. Trust-the-operator stance is reasonable, but a defense-in-depth gate would shorten incident debugging.
- **Recommendation**: Add `validate_sizes()` to `ServerConfig` that rejects: `device_alignment == 0` or non-power-of-2; `lock_stripes == 0` or non-power-of-2; `max_batch_size == 0`; `max_connections == 0`; `expected_records == 0`. Call from `main` after the existing validators.
- **Confidence**: High

### F-G10-006: `blobstore_path` default `/blobstore` is unusable for non-root processes
- **Severity**: MEDIUM
- **Category**: Security (defaults) / Maintainability
- **Location**: `src/config.rs:505`
- **Code**:
  ```rust
  blobstore_path: "/blobstore".to_string(),
  ```
- **Issue**: Default is an absolute path at filesystem root, unwritable for any non-root user. A fresh install fails to spawn blob-store operations on first OP_CREATE that writes EXTERNAL. The path is also a `String` (not `PathBuf`), so cross-platform handling is inconsistent with `device_paths` and `index_snapshot_path` which use `PathBuf`.
- **Impact**: New deployments hit blob-store IO errors only on the first oversize-record write — far from the startup banner that would surface a permission problem early. Combined with the silent log line `tracing::info!(path = %config.blobstore_path, "blobstore configured")` (`src/bin/server.rs:508`) which doesn't probe writability.
- **Recommendation**: Change default to `"./teraslab-blobstore"` (or derive from `device_paths[0].parent()`). Convert field to `PathBuf` for consistency. Probe writability in startup and fail fast.
- **Confidence**: High

### F-G10-007: `ServerConfig` derives `Debug` — token/secret leak if anyone debug-prints
- **Severity**: MEDIUM
- **Category**: Security
- **Location**: `src/config.rs:243`, `src/config.rs:331,380`
- **Code**:
  ```rust
  #[derive(Debug, Clone, Deserialize)]
  #[serde(default)]
  pub struct ServerConfig {
      ...
      pub admin_token: Option<String>,
      ...
      pub cluster_secret: Option<String>,
  ```
- **Issue**: Both `admin_token` and `cluster_secret` are bare `Option<String>` inside a `#[derive(Debug)]` struct. No grep hit shows the config being debug-formatted in current code, but any future `tracing::debug!(?config, ...)` or panic message would leak both secrets to logs. Industry practice is to wrap secrets in a `Redacted<String>` newtype that overrides `Debug` to print `"***"`.
- **Impact**: Latent. One careless future change writes secrets to log aggregators / OTLP traces / crash dumps.
- **Recommendation**: Introduce `pub struct Secret(String)` with manual `Debug` that prints `"<redacted, len={}>"`. Use it for `admin_token` and `cluster_secret`. Add a clippy/grep CI check forbidding `?config` / `{config:?}`.
- **Confidence**: High

### F-G10-008: `detect_local_ip` connects to `8.8.8.8:53` — silent external network probe on startup
- **Severity**: MEDIUM
- **Category**: Security / Privacy
- **Location**: `src/bin/server.rs:44-50`
- **Code**:
  ```rust
  fn detect_local_ip() -> Option<std::net::IpAddr> {
      let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
      socket.connect("8.8.8.8:53").ok()?;
      socket.local_addr().ok().map(|a| a.ip())
  }
  ```
- **Issue**: When `listen_addr` is `0.0.0.0` and `advertise_addr` is unset, the bin opens a UDP socket to Google DNS to discover the outbound interface. Comment correctly notes "no traffic is sent" — but the kernel route lookup still touches the routing table for `8.8.8.8` and in audited/air-gapped environments this can trip egress-monitoring or DLP alarms. A hardcoded public IP is also surprising in a self-hosted UTXO database.
- **Impact**: Surprise on operator. Misleading in air-gapped clusters (returns gateway IP, not "real" advertised IP, but the code paths still proceed). Subtle: in some clusters `8.8.8.8` is unroutable, function returns `None`, server falls back to `bind_addr.ip()` (= `0.0.0.0`) which is then ADVERTISED to other nodes — making SWIM convergence fail in a non-obvious way.
- **Recommendation**: Replace with iteration over `getifaddrs()`-style local interfaces (already a dependency via `nix` or `socket2`?), or require `advertise_addr` to be set explicitly when `listen_addr` is `0.0.0.0`. At minimum, log a warning that `8.8.8.8` is being probed and let operators opt out.
- **Confidence**: High

### F-G10-009: CLI `data_addr` default `localhost:3000` does not match server's `listen_addr` default `127.0.0.1:3300`
- **Severity**: LOW
- **Category**: Code Quality
- **Location**: `src/bin/cli.rs:55` vs `src/config.rs:479`
- **Code**:
  ```rust
  // cli.rs:55
  #[arg(long, default_value = "localhost:3000", global = true)]
  data_addr: String,
  ```
  ```rust
  // config.rs:479
  listen_addr: "127.0.0.1:3300".to_string(),
  ```
- **Issue**: CLI `bench` command (which uses `data_addr`) defaults to port 3000; server defaults to 3300. Out-of-the-box `teraslab-cli bench ping` will fail to connect to a fresh server.
- **Impact**: Operator footgun on first try. Smoke test fails before any "real" work.
- **Recommendation**: Align CLI default to `127.0.0.1:3300`. Same for the HTTP `addr` default `http://localhost:9100` — `9100` does match config.rs default, good.
- **Confidence**: High

### F-G10-010: `enable_admin_endpoints` does not require `enable_remote_bind` — easy operator footgun
- **Severity**: LOW
- **Category**: Security
- **Location**: `src/config.rs:798-854` (`validate_safe_defaults`)
- **Code**:
  ```rust
  if self.enable_admin_endpoints
      && self.admin_token.as_ref().map(|s| s.is_empty()).unwrap_or(true)
  {
      return Err(ConfigError::AdminTokenRequired);
  }
  ```
- **Issue**: Operator can enable admin endpoints on a non-loopback bind only if they also flip `enable_remote_bind = true`, AND they must set `admin_token`. Currently the only enforced coupling is "if enable_admin_endpoints then admin_token". There is no warning when `enable_admin_endpoints = true` but `enable_remote_bind = false` (admin endpoints on loopback only — actually a fine, secure config — no issue).  However, the reverse: `enable_remote_bind = true` AND `enable_admin_endpoints = true` AND `admin_token = "weak"` passes validation because token strength isn't checked.
- **Impact**: Weak/short tokens are accepted. An operator who sets `admin_token = "x"` and exposes the HTTP port to the internet has effectively no auth.
- **Recommendation**: Add a minimum-token-length check (e.g. 32 bytes / base64-32) in `validate_safe_defaults` when `enable_admin_endpoints && enable_remote_bind`. Warn (not reject) when admin endpoints are enabled on non-loopback even with a long token — recommend mTLS.
- **Confidence**: Medium (it's a balance — over-strict validation pushes operators toward storing tokens in worse places).

### F-G10-011: `cluster_secret` strength is unvalidated — empty-string check only
- **Severity**: LOW
- **Category**: Security
- **Location**: `src/config.rs:826-836`
- **Code**:
  ```rust
  if (self.is_clustered() || self.replication_factor > 1)
      && self.cluster_secret.as_ref().map(|s| s.is_empty()).unwrap_or(true)
  {
      return Err(ConfigError::ClusterSecretRequired { rf: ... });
  }
  ```
- **Issue**: Only "non-empty" is enforced. A `cluster_secret = "a"` passes. The HMAC quality scales with secret entropy; a 1-byte secret offers ~no protection against an attacker who can speak SWIM/replication.
- **Impact**: Combined with the documented "all nodes must use the same secret" requirement, a hand-typed secret is a realistic failure mode. Recommend minimum 16 bytes.
- **Recommendation**: Reject `cluster_secret` shorter than 16 bytes (or 128 bits worth of entropy). Alternative: accept hex/base64 and decode, requiring decoded length ≥ 16.
- **Confidence**: Medium

### F-G10-012: `load_factor * 100.0` is computed but `load_factor` is unitless — likely a labeling bug
- **Severity**: LOW
- **Category**: Code Quality
- **Location**: `src/bin/server.rs:461-464`
- **Code**:
  ```rust
  tracing::info!(
      entries = index.len(),
      load_factor = index.stats().load_factor * 100.0,
      "index loaded",
  );
  ```
- **Issue**: The field is logged as `load_factor` but the value has been multiplied by 100 (so it's a percentage, not a load factor). Either rename to `load_factor_pct` or drop the `* 100.0`.
- **Impact**: Cosmetic. Dashboards / alerts keying on `load_factor=0.85` get `85`.
- **Recommendation**: Rename log field to `load_factor_pct`, or remove the multiplication.
- **Confidence**: High

### F-G10-013: `expect("invalid listen_addr")` after validation works but rule-violating
- **Severity**: LOW
- **Category**: Code Quality
- **Location**: `src/bin/server.rs:672`, `src/bin/server.rs:677`
- **Code**:
  ```rust
  let bind_addr: std::net::SocketAddr =
      config.listen_addr.parse().expect("invalid listen_addr");
  ...
  let self_addr: std::net::SocketAddr = if let Some(ref adv) = config.advertise_addr {
      adv.parse().expect("invalid advertise_addr")
  } else if bind_addr.ip().is_unspecified() {
  ```
- **Issue**: `listen_addr` has already been validated by `validate_safe_defaults`, so this `expect` is defensible. But `advertise_addr` is NEVER validated. If TOML supplies `advertise_addr = "garbage"`, the binary panics here instead of returning a typed config error.
- **Impact**: Cryptic panic on misconfiguration of `advertise_addr`.
- **Recommendation**: Add `advertise_addr` parsing to `validate_safe_defaults`. Use `?`/typed error rather than `expect` even post-validation (CLAUDE.md says "no unwrap/expect in library code"; this is a bin so technically allowed but the spirit of the rule applies).
- **Confidence**: High

### F-G10-014: `lib.rs` exposes every module as `pub mod` — internals leak through public API
- **Severity**: LOW
- **Category**: Maintainability
- **Location**: `src/lib.rs:1-22`
- **Code**:
  ```rust
  #![warn(clippy::disallowed_macros)]

  pub mod allocator;
  pub mod checkpoint;
  pub mod cluster;
  pub mod config;
  pub mod device;
  pub mod device_io;
  pub mod fault_injection;
  pub mod index;
  pub mod io;
  pub mod locks;
  pub mod metrics;
  pub mod observability;
  pub mod ops;
  pub mod protocol;
  pub mod record;
  pub mod recovery;
  pub mod redo;
  pub mod replication;
  pub mod server;
  pub mod storage;
  ```
- **Issue**: Every module is `pub`, including `fault_injection` (test/debugging surface), `device_io` (raw I/O internals), `io`, `recovery`, `redo`. There is no integrity guarantee at the crate boundary; downstream consumers can poke at the redo log or fault injector. This crate appears to be the daemon's own binary lib + a (separately-released) Rust client crate (`client/rust/src/lib.rs`). If teraslab itself is published as a library, every internal detail is part of the SemVer contract.
- **Impact**: Maintenance ceiling. Any internal refactor becomes a breaking change for hypothetical downstream users.
- **Recommendation**: Mark internals (`fault_injection`, `device_io`, `io`, `recovery`, `redo`, `recovery`, `server::dispatch` internals, etc.) as `pub(crate)`. Keep only `protocol`, `record`, `config`, and a small re-export surface as `pub`. If the binaries need broader access, use `pub(crate)` + workspace-internal `pub` via `pub use` in a `internal` feature gate.
- **Confidence**: Medium (depends on whether teraslab-as-a-library is a goal — the README / Cargo.toml would confirm).

### F-G10-015: `pending_conflicting_children` drained via `append_conflicting_child` mid-startup with no idempotency proof
- **Severity**: LOW
- **Category**: Correctness
- **Location**: `src/bin/server.rs:632-649`
- **Code**:
  ```rust
  if !pending_conflicting_children.is_empty() {
      for pending in &pending_conflicting_children {
          if let Err(e) = engine.append_conflicting_child(&pending.parent_key, pending.child_txid)
          {
              tracing::error!(...);
              std::process::exit(1);
          }
      }
  ```
- **Issue**: Recovery returns a list of conflicting-child appends that were observed in the redo log but not yet applied. The startup code re-issues them via the engine. Comment claims "the original AppendConflictingChild intent remains in the log until checkpoint, so writing a duplicate high-level intent here is unnecessary" but the code DOES re-issue the engine call. If the recovery already applied them to the index, this is a duplicate; if `append_conflicting_child` is not idempotent, we get duplicate child entries.
- **Impact**: Depends on engine semantics — not verifiable from this file alone. The comment hints at intent but the code path doesn't match the comment.
- **Recommendation**: Clarify whether `engine.append_conflicting_child` is idempotent (out of scope for this file; flag to whoever owns `src/ops/engine.rs`). If non-idempotent, gate the loop on a pre-check that the child isn't already in the parent's child list. Update the comment so the code matches.
- **Confidence**: Low — would need to read `engine.rs` to be sure.

### F-G10-016: `recovery_completes_before_listener_bind` test relies on source-string ordering — fragile
- **Severity**: LOW
- **Category**: Code Quality
- **Location**: `src/bin/server.rs:1224-1258`
- **Code**:
  ```rust
  #[test]
  fn recovery_completes_before_listener_bind() {
      let source = include_str!("server.rs");
      let recovery = source
          .find("match teraslab::server::dispatch::recover_pending_replication_intents")
          .expect("startup must recover pending replication intents");
      let http_listener = source
          .find("// 6. Start HTTP observability server")
          .expect(...);
  ```
- **Issue**: Test asserts ordering by grepping the source file at compile time. Refactoring (e.g. moving the recovery block into a helper) would silently break the invariant the test was meant to lock down, because the substring may no longer exist. This is "source-as-state" testing.
- **Impact**: False sense of security. Real invariant is "no HTTP socket bound before recovery completes" — a runtime test that builds a fake `recover_pending_replication_intents` that records a timestamp and compares against listener-bind time would catch the actual bug.
- **Recommendation**: Replace with an integration test using fault-injection to delay recovery and assert no listener responds during that window.
- **Confidence**: High

### F-G10-017: Per-replica catch-up panic-free but very fragile error string contract
- **Severity**: LOW
- **Category**: Maintainability
- **Location**: `src/bin/server.rs:922-942`
- **Code**:
  ```rust
  Err(e) => {
      tracing::warn!(%addr, err = %e, "catchup: replica catch-up failed");
      // Phase H — when catchup returns the
      // truncation sentinel, post a resync
      // request ...
      if e.contains("redo entries reclaimed") {
          let queued = resync_handle.signal_for_addr(addr, Vec::new());
  ```
- **Issue**: Error-handling dispatches on `e.contains("redo entries reclaimed")` — a stringly-typed protocol between `run_catchup_for_replica` and this binary. Per CLAUDE.md ("All error types must be enums with descriptive variants — no string errors"), the catchup function should return a structured error variant.
- **Impact**: A future refactor of the error message ("redo entries reclaimed" → "redo segments reclaimed") silently disables the resync request. Replicas with truncation gaps would log a warning and stay stuck.
- **Recommendation**: Make `run_catchup_for_replica` return a `Result<u64, CatchupError>` with a `CatchupError::RedoReclaimed { ... }` variant.
- **Confidence**: High

### F-G10-018: Positive verification — admin-token gating (R-056) is correctly integrated end-to-end
- **Severity**: INFO
- **Category**: Security
- **Location**: `src/config.rs:798-854`, `src/bin/server.rs:980-988`, `src/server/http.rs:113-258` (referenced)
- **Code**:
  ```rust
  // server.rs:980-988
  let admin_endpoints_enabled = config.enable_admin_endpoints;
  let admin_token = config.admin_token.clone();
  std::thread::spawn(move || {
      start_http_server(http_addr, http_state, admin_endpoints_enabled, admin_token);
  });
  ```
- **Issue**: None. The R-056 gating chain (TOML → `apply_admin_token_env_override` → `validate_safe_defaults` → `start_http_server` → `build_http_router` middleware with constant-time compare) is wired through correctly. Tests cover empty-token / no-token / env-override / non-loopback combinations.
- **Recommendation**: None.
- **Confidence**: High

### F-G10-019: Positive verification — `validate_safe_defaults` correctly rejects insecure bind defaults
- **Severity**: INFO
- **Category**: Security
- **Location**: `src/config.rs:476-524` (defaults), `src/config.rs:798-854` (validation)
- **Code**:
  ```rust
  // Defaults
  listen_addr: "127.0.0.1:3300".to_string(),
  http_listen_addr: "127.0.0.1:9100".to_string(),
  enable_remote_bind: false,
  enable_admin_endpoints: false,
  admin_token: None,
  cluster_secret: None,
  replication_factor: 1,
  ```
- **Issue**: None. All security-relevant defaults are conservative: loopback bind, admin endpoints off, remote bind off, single-node. Tests `default_listen_addrs_are_loopback`, `default_config_passes_safe_defaults`, `rf_gt_one_without_cluster_secret_is_rejected`, `non_loopback_listen_without_remote_bind_is_rejected` all assert the right gates.
- **Recommendation**: None.
- **Confidence**: High

### F-G10-020: CLI shells out to nothing — no `Command::new` injection vector
- **Severity**: INFO
- **Category**: Security
- **Location**: `src/bin/cli.rs` (entire file)
- **Issue**: Confirmed via grep that `cli.rs` never uses `std::process::Command::new`, `std::process::Stdio`, or similar subprocess APIs. All operator-supplied input (`txid`, `node_id`, `level`, `operation`) is passed to HTTP (URL-encoded via `format!`) or the binary protocol (raw bytes). No shell injection vector.
- **Recommendation**: None. URL-encoding of `txid` / `node_id` is implicit via `format!("/debug/records/{txid}")` — server-side handlers must percent-decode and validate (out of G10 scope but worth noting).
- **Confidence**: High

### F-G10-021: HTTP port fallback `9100` silently masks malformed `http_listen_addr`
- **Severity**: LOW
- **Category**: Correctness
- **Location**: `src/bin/server.rs:962-966`
- **Code**:
  ```rust
  let http_port: u16 = config
      .http_listen_addr
      .rsplit_once(':')
      .and_then(|(_, p)| p.parse().ok())
      .unwrap_or(9100);
  ```
- **Issue**: `http_listen_addr` is already validated by `validate_safe_defaults` to parse as `host:port`, so this fallback should be unreachable. But the silent default of `9100` would surface as confusing telemetry (`http_port = 9100` reported, server actually bound elsewhere) if validation is ever weakened.
- **Impact**: Latent. Today validation guarantees the rsplit succeeds, but defensive default of `9100` is misleading rather than defensive.
- **Recommendation**: After validation, parse via `SocketAddr` and take `.port()`. Or `expect("http_listen_addr validated above")` to make the invariant explicit (still rule-violating, see F-G10-013, but no worse).
- **Confidence**: High

### F-G10-022: `_blob_gc_handle`, `_lag_monitor_handle`, `_checkpoint_handle`, `_redo_log_device` — leaked join handles
- **Severity**: LOW
- **Category**: Code Quality / Correctness
- **Location**: `src/bin/server.rs:497-499, 1017-1050, 1059-1070, 1085-1115`
- **Code**:
  ```rust
  let _redo_log_device: Arc<dyn BlockDevice> = redo_log_device;
  ...
  let _checkpoint_handle = redo_log.as_ref().map(|log| { ... });
  ...
  let _blob_gc_handle: Option<std::thread::JoinHandle<()>> = ...;
  let _lag_monitor_handle: Option<std::thread::JoinHandle<()>> = ...;
  ```
- **Issue**: Handles bound with leading underscore are intentionally ignored. Combined with F-G10-001/002, this means even if the shutdown_flag flips, nothing `.join()`s the background threads. Daemons that exit via `process::exit` are fine (kernel reaps everything), but `ServerWithShutdown::run` returning normally would skip the join, leaving short-lived threads possibly mid-fsync.
- **Impact**: Subtle. With graceful shutdown wired (F-G10-001 fix), the lack of joins could let a checkpoint thread be mid-snapshot when the device.sync() call completes, racing the OTLP flush.
- **Recommendation**: Move handles into `ServerWithShutdown` and `.join()` them in `run()` after the shutdown flag is set but before `device.sync()`.
- **Confidence**: High

---

## Coverage notes

- **`src/lib.rs`**: covered by F-G10-014 (oversharing `pub mod`). Single finding sufficient — file is 22 lines.
- **`src/config.rs`**: covered by F-G10-005 (size validation gaps), F-G10-006 (blobstore default), F-G10-007 (Debug leak), F-G10-010 (token strength), F-G10-011 (cluster_secret strength), F-G10-018/019 (positive verifications). The two `validate_*` test blocks plus the new R-056 tests are well-shaped — no missing test cases beyond the validation gaps already flagged.
- **`src/bin/server.rs`**: section-by-section reviewed. Critical findings F-G10-001 + F-G10-002 + F-G10-003 form a cluster around shutdown. F-G10-004 startup robustness. F-G10-008 outbound DNS probe. F-G10-012 cosmetic. F-G10-013/021 expect/fallback hygiene. F-G10-015 idempotency concern. F-G10-016 fragile test. F-G10-017 stringly-typed error. F-G10-022 thread-handle hygiene. The recovery → readiness → bind ordering (lines 510-560 → 791-820 → 960-988 → 1117-1131) is correctly arranged — replication-intent recovery completes before any listener spawns.
- **`src/bin/cli.rs`**: section-by-section reviewed. F-G10-009 default-port mismatch. F-G10-020 positive verification — no shell injection. Admin token is correctly threaded through `HttpClient::request` so no command can bypass auth.
- **Cross-references**: R-056 admin-token integration verified end-to-end in F-G10-018; chains through `apply_env_overrides`, `validate_safe_defaults`, `start_http_server`, and the CLI's `HttpClient::with_auth`.
- **Untouched files**: `_review/01_scope.md` and `_review/02_findings.md` not modified. No source files touched.
