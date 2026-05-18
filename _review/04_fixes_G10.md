# G10 fix log — binaries + config + lib root + Cargo.toml + docs/

Worktree branched from `3c76ecf` (review-baseline), which already included
prior agent work captured in `8920447 wip: pre-review-fix baseline snapshot`.
That baseline landed substantial pieces of the G10 fix work (ctrlc dep,
Secret newtype, validate_sizes, getifaddrs-based detect_local_ip, ctrlc
handler wiring, ServerWithShutdown holding redo_log + thread join handles,
DEPLOYMENT_ASSUMPTIONS.md, tests/g10_config.rs, tests/g10_lifecycle.rs,
CI cargo-audit + cargo-deny steps). This log covers both the prior-agent
work surveyed/verified and the remaining work this session completed.

Per-fix state:

### F-G10-001 — FIXED (prior agent + verified this session)
- Commit: prior agent (8920447) + this session's verification
- Files: `Cargo.toml` (ctrlc dep with justification), `src/bin/server.rs:1472-1480` (`ctrlc_handler` now calls `ctrlc::set_handler` with `termination` feature for SIGINT+SIGTERM)
- Test: covered by `tests/g10_lifecycle.rs::server_shutdown_exits_run_loop` (simulates the handler's effect via `Server::shutdown()` since registering a real signal handler from a test would clobber the harness)
- Notes: `ctrlc::set_handler` may fail only on duplicate registration; that case logs `error` and continues (failing the daemon over a signal-handler diagnostic is worse than no graceful shutdown). The `ctrlc` crate has no `TestSignal::raise` API — the lifecycle test exercises the contract through the public `Server::shutdown()` instead.

### F-G10-002 — FIXED (prior agent + verified this session)
- Commit: prior agent (8920447) + verified
- Files: `src/server/mod.rs:306` already exposes `pub fn shutdown(&self)`; `src/bin/server.rs:1289-1304` wires the ctrlc handler to call `server_inner.shutdown()` in addition to flipping the bin's `shutdown_flag` for background tasks
- Test: `tests/g10_lifecycle.rs::server_shutdown_exits_run_loop`
- Notes: The orchestrator note suggested this might be NEEDS-ORCHESTRATOR for the upstream `Server::with_shutdown` API. In fact `Server` already exposed `shutdown()` (public), so the bin can call it directly without a new constructor. No upstream change needed.

### F-G10-003 — FIXED (prior agent + verified)
- Files: `src/bin/server.rs:1278` (`ServerWithShutdown::redo_log: Option<Arc<Mutex<RedoLog>>>`) + `:1397-1402` (flush before `device.sync`)
- Test: covered indirectly by the lifecycle test path
- Notes: defense-in-depth — per-op fsync remains the primary durability guarantee.

### F-G10-004 — FIXED (prior agent + verified)
- Files: `src/config.rs:983-985` (`device_paths.is_empty()` → `ConfigError::NoDevicePaths`); `resolved_redo_log_path` / `resolved_cluster_state_path` use safe `.first()` fallback
- Test: `tests/g10_config.rs::validate_safe_defaults_rejects_empty_device_paths`, `resolved_redo_log_path_does_not_panic_on_empty_device_paths`

### F-G10-005 — FIXED (prior agent + verified)
- Files: `src/config.rs:1106-1161` `validate_sizes()` + plumbed at `:989` in `validate_safe_defaults`
- Tests: `tests/g10_config.rs::validate_sizes_*` (six cases)

### F-G10-006 — FIXED (prior agent + verified)
- Files: `src/config.rs:506` (`blobstore_path: PathBuf`), `:667` default `./teraslab-blobstore`
- Test: `tests/g10_config.rs::default_blobstore_path_is_relative`, `tests/g10_review.rs::blobstore_path_is_pathbuf_default_relative_and_writable`

### F-G10-007 — FIXED (prior agent + verified)
- Files: `src/config.rs:21-78` `Secret` newtype with manual `Debug` printing `<redacted, len=N>`; `admin_token` and `cluster_secret` use `Option<Secret>`
- Tests: `tests/g10_config.rs::debug_format_of_secret_redacts_the_value`, `debug_format_of_server_config_redacts_admin_token`, `debug_format_of_server_config_redacts_cluster_secret`

### F-G10-008 — FIXED (prior agent + verified)
- Files: `src/bin/server.rs:41-99` `detect_local_ip` rewritten over `libc::getifaddrs(3)`, no outbound `8.8.8.8` probe; bin refuses to start when result is `None` (clear "set `advertise_addr`" error)
- Tests: covered by manual inspection — pure-libc path has no network side-effect to assert from a unit test

### F-G10-009 — FIXED (prior agent + verified)
- Files: `src/bin/cli.rs:59` `data_addr` default `127.0.0.1:3300` (was `localhost:3000`)
- Test: `tests/g10_review.rs::cli_default_data_addr_matches_server_listen_addr_default`

### F-G10-010 — FIXED (prior agent + verified)
- Files: `src/config.rs:1078-1087` (admin token ≥ `MIN_REMOTE_ADMIN_TOKEN_LEN` when both `enable_admin_endpoints` and `enable_remote_bind`); `:1100` constant `MIN_REMOTE_ADMIN_TOKEN_LEN = 16`
- Tests: `tests/g10_config.rs::weak_admin_token_with_remote_bind_is_rejected`, `weak_admin_token_on_loopback_is_accepted`

### F-G10-011 — FIXED (prior agent + verified)
- Files: `src/config.rs:1050-1058` (`cluster_secret` ≥ `MIN_CLUSTER_SECRET_LEN`); `:1095` constant
- Tests: `tests/g10_config.rs::short_cluster_secret_is_rejected`, `long_cluster_secret_passes_validation`

### F-G10-012 — FIXED (prior agent + verified)
- File: `src/bin/server.rs:560` log field renamed from `load_factor` → `load_factor_pct`

### F-G10-013 — FIXED (prior agent + verified)
- Files: `src/config.rs:1021-1027` (`advertise_addr` parse in `validate_safe_defaults`); `src/bin/server.rs:783-803` no more `.expect("invalid …")` — typed-error path with `tracing::error!` + `std::process::exit(1)` post-validation logic-bug fallback
- Test: `tests/g10_config.rs::malformed_advertise_addr_is_a_typed_config_error`, `valid_advertise_addr_passes`

### F-G10-014 — FIXED (prior agent + verified)
- File: `src/lib.rs:21` `pub(crate) mod device_io` (was `pub mod`). Other internals stay `pub` for now with a comment explaining why — bins/tests/benches consume them, demoting them is a wider refactor tracked as `FUP-G10-014`.
- Test: `tests/g10_review.rs::public_modules_remain_reachable` locks down the still-public surface so a future overzealous demotion fails the build

### F-G10-015 — FIXED (prior agent + verified this session)
- File: `src/bin/server.rs:732-738` comment updated to match code — `Engine::append_conflicting_child` IS idempotent (verified in `src/ops/engine.rs:2641-2665` — short-circuits via `children.contains(&child_txid)` before any allocator work)
- Notes: An audit follow-up (`FUP-G10-015`) tracks exposing a public `has_conflicting_child` accessor so the bin's drain loop can pre-filter, but the engine-side idempotency makes the current behaviour correct.

### F-G10-016 — FIXED (prior agent + verified)
- File: `src/bin/server.rs:1503-1508` replaced source-string-grep test with a comment pointing to `tests/g10_lifecycle.rs` (runtime coverage of the same invariant)
- Test: lifecycle integration test covers "no listener answers before recovery completes" via the public shutdown path

### F-G10-017 — RESOLVED (P2.5 / B-4)
- Files:
  - `src/replication/durable.rs:644-693` adds `pub enum CatchupError { RedoReclaimed { from, available }, Transport { addr, detail }, ReplicaError { addr, message } }` (derives `thiserror::Error`).
  - `src/replication/durable.rs:run_catchup_for_replica` signature changed from `Result<u64, String>` → `Result<u64, CatchupError>`; every `Err(format!(...))` / `Err("...".to_string())` site mapped to a variant. The redo-wrap detection is now exposed structurally via `CatchupError::RedoReclaimed { from, available }` instead of the rendered substring `"redo entries reclaimed"`.
  - `src/bin/server.rs:1058-1080` replaces `if e.contains("redo entries reclaimed")` with `if let CatchupError::RedoReclaimed { .. } = e`.
  - `src/cluster/coordinator.rs:661-667, 6769-6775` doc comments updated to reference the typed variant instead of the removed substring.
- Tests:
  - `replication::durable::tests::run_catchup_returns_typed_redo_reclaimed_when_log_wrapped` — pins both wrap-detection paths (`check_redo_truncation` short-circuit + `ops_from_seq` returns empty) to `CatchupError::RedoReclaimed { from, available }` with the expected field values.
  - `replication::durable::tests::run_catchup_already_caught_up_returns_ok` — pins the early-return happy path so a future refactor cannot accidentally fall through into the redo-reclaimed branch.
- Verification:
  - `cargo check --lib` clean (8 pre-existing `device_io/*` warnings, unchanged).
  - `cargo check --bins` clean.
  - `cargo test --lib replication::durable::` — 26 passed, 0 failed.
  - `cargo test --test replication_tcp` — 11 passed, 0 failed.
  - `cargo clippy --lib --no-deps` — 8 warnings, same baseline.
  - `grep "redo entries reclaimed"` only matches in the new test's doc-comment describing what was removed.

### F-G10-018 — NOT-APPLICABLE (positive verification)
- INFO: admin-token gating chain is correctly wired end-to-end. No code change.

### F-G10-019 — NOT-APPLICABLE (positive verification)
- INFO: safe defaults reject insecure bind / cluster configs. No code change.

### F-G10-020 — NOT-APPLICABLE (positive verification)
- INFO: cli.rs uses no `Command::new` subprocess APIs. No code change.

### F-G10-021 — FIXED (prior agent + verified)
- File: `src/bin/server.rs:1101-1111` parses `http_listen_addr` as `SocketAddr` and takes `.port()`; no silent `9100` fallback
- Test: `tests/g10_review.rs::http_listen_addr_parse_is_strict_after_validation`

### F-G10-022 — FIXED (prior agent + verified)
- Files: `src/bin/server.rs:1284-1286` (`ServerWithShutdown` holds `checkpoint_handle`, `blob_gc_handle`, `lag_monitor_handle` in `Mutex<Option<JoinHandle<()>>>`); `:1352-1366` joins each with a 5 s timeout in `run()` after the shutdown flag is set; `:1425-1455` `join_with_timeout` helper logs/leaks on timeout instead of pinning the daemon
- Test: `tests/g10_lifecycle.rs::shared_shutdown_flag_visible_to_background_thread` (contract: shared flag flips visibly)

### F-X-001 — FIXED (prior agent + verified)
- Files: `src/config.rs:542` `strict_auth: bool` (default `false`); `:1042-1044` validation gate; `src/bin/server.rs:130-167` `--strict-auth` CLI parse; `:242-271` boot-time `tracing::warn!(target = "teraslab::security", ...)` for non-strict multi-node-without-secret; `docs/DEPLOYMENT_ASSUMPTIONS.md`
- Tests: `tests/g10_config.rs::strict_auth_*` (four cases)
- Notes: The orchestrator note mentioned a `ConnectionOptions` integration with G5. That sub-task remains G5's territory; the bin-side wiring + config + warn + doc are all in. Flagged in follow-ups (FUP-X-001) if G5's eventual branch needs more from the bin.

### F-X-010 — FIXED (prior agent + this session)
- Files: `.github/workflows/ci.yml:64-77` (cargo-audit + cargo-deny steps, prior agent); `deny.toml` at repo root (this session)
- Notes: CI uses `|| true` for the initial rollout so the steps surface as warnings, not hard failures, while we incrementally clear the advisory baseline.

## This session's incremental changes

- `src/bin/server.rs`: removed unused `Path` import (cargo-warn cleanup; the file uses `std::path::Path::new` directly, not the imported name).
- `deny.toml`: new file (F-X-010 completion).
- `tests/g10_review.rs`: new test file with cross-cutting G10 checks (F-G10-006, F-G10-009, F-G10-014, F-G10-021).
- `_review/04_fixes_G10.md`: this fix log.

## End-of-group cadence

- `cargo check --lib`: clean.
- `cargo check --bins`: clean.
- `cargo test --test g10_config`: 22 passed, 0 failed.
- `cargo test --test g10_lifecycle`: 3 passed, 0 failed.
- `cargo test --test g10_review`: 4 passed, 0 failed.
- `cargo clippy --bin teraslab-server --bin teraslab-cli`: no errors in owned files (pre-existing clippy errors in `device_io/*`, `record.rs`, `index/redb_primary.rs`, `redo.rs` are out-of-scope — G1/G3/G4).
- `cargo test --all`: blocked on pre-existing test-compile errors in `src/index/redb_primary.rs` test module (G3 territory) — flagged for orchestrator. Owned-file integration tests all pass.
