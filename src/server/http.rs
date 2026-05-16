//! HTTP observability server (metrics, health, debug, admin, WebSocket, Web UI).
//!
//! Runs on a separate port from the binary wire protocol. Uses `axum`
//! for routing. Does not block or slow the binary protocol path.

use crate::cluster::coordinator::RunningCluster;
use crate::cluster::shards::{NUM_SHARDS, NodeId, ShardTable};
use crate::index::TxKey;
use crate::metrics::{
    LatencyHistogram, MAX_REPLICAS, MigrationLabel, OpCode, OpOutcomeCounters, Outcome,
    SwimChurnKind, ThreadHistograms, ThreadMetrics, UringErrClass, allocator_metrics,
    io_uring_metrics, migration_metrics, redo_metrics, replication_metrics, swim_metrics,
};
use crate::observability::WireTraceContext;
use crate::ops::engine::Engine;
use crate::redo::RedoLog;
use axum::Router;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::IntoResponse;
use axum::routing::{get, put};
use rust_embed::Embed;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use subtle::ConstantTimeEq;

#[derive(Debug, Default, serde::Deserialize)]
struct DrainQuery {
    #[serde(default)]
    wait_seconds: u64,
}

/// Log levels for the runtime log level endpoint.
const LOG_LEVEL_ERROR: u8 = 0;
const LOG_LEVEL_WARN: u8 = 1;
const LOG_LEVEL_INFO: u8 = 2;
const LOG_LEVEL_DEBUG: u8 = 3;
const LOG_LEVEL_TRACE: u8 = 4;

/// Maximum number of remote `/admin/top?local=true` fetches in flight at once.
///
/// A large cluster can have hundreds of members. Without this cap, one
/// operator poll spawns one Tokio task and opens one HTTP request per peer.
const ADMIN_TOP_REMOTE_FANOUT_LIMIT: usize = 32;

/// Maximum time a `/ws/top` client may block one push before the server drops it.
const WS_TOP_SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// Embedded static files for the admin Web UI.
#[derive(Embed)]
#[folder = "ui/"]
struct UiAssets;

/// Shared state for the HTTP server.
pub struct HttpState {
    /// Reference to the engine for data queries.
    pub engine: Arc<Engine>,
    /// Global metrics counters.
    pub metrics: &'static ThreadMetrics,
    /// Global latency histograms.
    pub histograms: &'static ThreadHistograms,
    /// Whether the index has been fully loaded (ready check).
    pub ready: Arc<AtomicBool>,
    /// Runtime log level.
    pub log_level: Arc<AtomicU8>,
    /// Cluster coordinator (None in single-node mode).
    pub cluster: Option<Arc<RunningCluster>>,
    /// Redo log for status queries (None if not available).
    pub redo_log: Option<Arc<parking_lot::Mutex<RedoLog>>>,
    /// Active TCP connection count (shared with the Server struct).
    pub active_connections: Arc<AtomicUsize>,
    /// HTTP port used by this node (for deriving other nodes' HTTP addresses).
    pub http_port: u16,
    /// Replica lag threshold used by `/health/ready` in clustered mode.
    /// A value of 0 disables readiness degradation for lag.
    pub replica_lag_warn_threshold_ops: u64,
}

/// Bearer-token state shared with the admin auth middleware.
///
/// Stored as `Arc<[u8]>` (rather than `String`) so the constant-time
/// comparison can borrow the raw bytes without re-validating UTF-8 or
/// reallocating per request. Wrapped in an `Option` so the middleware can
/// fail closed if it is ever installed without a configured token (a
/// programmer error — `start_http_server` and `build_http_router` only
/// install the gate when this is `Some`).
#[derive(Clone)]
pub(crate) struct AdminAuthState {
    /// The expected bearer token, byte-for-byte. `None` means the gate
    /// is mis-installed; the middleware fails closed in that case.
    expected_token: Option<Arc<[u8]>>,
}

/// Start the HTTP observability server on the given address.
///
/// This spawns a tokio runtime and blocks until shutdown.
/// Call this from a dedicated thread.
///
/// `enable_admin_endpoints` gates registration of the `/admin/*` mutation
/// routes and the mutating `/debug/*` routes. When `false` (the default),
/// those routes are not part of the router at all and any request to them
/// returns 404. When `true`, the routes are registered behind a bearer-token
/// middleware keyed on `admin_token` (constant-time comparison) — every
/// request to a gated route must carry an `Authorization: Bearer <token>`
/// header that matches `admin_token` exactly. `validate_safe_defaults` is
/// expected to have rejected any combination where the gate is enabled
/// without a non-empty token; this function logs a `tracing::error!` and
/// refuses to register the routes if the invariant is violated, rather than
/// installing an open mutation surface.
pub fn start_http_server(
    bind_addr: String,
    state: Arc<HttpState>,
    enable_admin_endpoints: bool,
    admin_token: Option<String>,
) {
    if let Err(e) = try_start_http_server(bind_addr, state, enable_admin_endpoints, admin_token) {
        // F-G6-027: runtime build failure used to panic the dedicated
        // HTTP thread without any operator signal — `/metrics` and
        // `/health` would silently go dark. We log loudly here and
        // return; the caller (a `std::thread::spawn` in `bin/server.rs`)
        // will see the thread exit cleanly. The companion `Err` return
        // from `try_start_http_server` is what allows the caller to
        // observe the failure for future restart logic.
        tracing::error!(
            err = %e,
            "HTTP observability server terminated — endpoints (metrics, health, admin) \
             are now unreachable. The TCP data port is unaffected.",
        );
    }
}

/// Result-returning variant of [`start_http_server`] for callers that
/// want to propagate startup or build failures (F-G6-027). The fire-and-
/// forget wrapper above logs and discards the error so the legacy
/// `std::thread::spawn` site in `bin/server.rs` keeps working without
/// changes.
pub fn try_start_http_server(
    bind_addr: String,
    state: Arc<HttpState>,
    enable_admin_endpoints: bool,
    admin_token: Option<String>,
) -> std::io::Result<()> {
    // F-G6-016: derive worker threads from the host's available
    // parallelism. Floor at 2 so single-core / cgroup-restricted hosts
    // still get a usable runtime; ceiling at 1/4 of CPUs to keep the
    // observability runtime from starving the data path. The previous
    // hard-coded `worker_threads(4)` was fine for a small cluster but
    // saturated under large-fan-out `/admin/top` loads.
    let parallelism = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let worker_threads = (parallelism / 4).max(2);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .thread_name("teraslab-http")
        .worker_threads(worker_threads)
        .enable_all()
        .build()?;

    // F-G6-017: install a per-thread panic hook so a handler panic logs
    // a structured `tracing::error!` instead of vanishing into stderr.
    // axum already converts panics into 500 Internal Server Error
    // responses, so observability is the only thing this hook adds.
    // The chain back to the previous hook is preserved so other
    // installs (test harness, sentry, etc.) still fire.
    install_http_panic_hook_once();

    rt.block_on(async move {
        let app = build_http_router(state, enable_admin_endpoints, admin_token.clone());

        let listener = match tokio::net::TcpListener::bind(&bind_addr).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(%bind_addr, err = %e, "HTTP server failed to bind");
                return;
            }
        };

        if enable_admin_endpoints {
            tracing::warn!(
                %bind_addr,
                "/admin/* and mutating /debug/* endpoints ENABLED — bearer-token \
                 auth is enforced (Authorization: Bearer <admin_token>). Disable in \
                 production by setting enable_admin_endpoints = false.",
            );
        }
        tracing::info!(%bind_addr, worker_threads, "HTTP observability server listening");

        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!(err = %e, "HTTP server error");
        }
    });
    Ok(())
}

/// F-G6-017: install a `tracing`-aware panic hook exactly once for the
/// HTTP server thread. Wrapping the previous global hook preserves
/// existing panic handling (e.g., `RUST_BACKTRACE=1` output).
fn install_http_panic_hook_once() {
    use std::sync::Once;
    static HOOK_INSTALLED: Once = Once::new();
    HOOK_INSTALLED.call_once(|| {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let location = info
                .location()
                .map(|l| format!("{}:{}", l.file(), l.line()))
                .unwrap_or_else(|| "<unknown>".to_string());
            let payload = info
                .payload()
                .downcast_ref::<&'static str>()
                .copied()
                .or_else(|| info.payload().downcast_ref::<String>().map(String::as_str))
                .unwrap_or("<non-string panic payload>");
            tracing::error!(
                location,
                payload,
                "HTTP handler panicked — axum will return 500 to the caller",
            );
            prev(info);
        }));
    });
}

/// Build the axum [`Router`] for the HTTP observability server.
///
/// Always registers the unauthenticated observability surface: `/metrics`,
/// `/health/live`, `/health/ready`, `/status`, the read-only `/admin/*`
/// dashboards (`migration_status`, `nodes`, `memory`, `records`,
/// `replication`, `top`), the read-only `/debug/*` endpoints
/// (`freelist`, `GET /debug/log-level`), the `/ws/top` WebSocket, and the
/// embedded `/ui/...` assets. These routes have no auth so load balancers,
/// Prometheus, and Grafana keep working without operator-issued credentials.
///
/// When `enable_admin_endpoints` is `true`, the mutating sub-router is built
/// (`/admin/quiesce|rebalance|drain/{node_id}`, `/debug/index|redo|records/{txid}`,
/// `PUT /debug/log-level`) and merged behind an
/// [`axum::middleware::from_fn_with_state`] guard that checks
/// `Authorization: Bearer <admin_token>` on every request, comparing against
/// the configured token in constant time via [`subtle::ConstantTimeEq`].
/// If the caller passes `enable_admin_endpoints = true` with `None` or an
/// empty `admin_token`, the gated sub-router is omitted entirely (so nothing
/// is exposed unauthenticated) and a `tracing::error!` is logged — this path
/// is expected to be unreachable because [`crate::config::ServerConfig::validate_safe_defaults`]
/// rejects the combination at startup.
///
/// When `enable_admin_endpoints` is `false`, the gated routes are not part
/// of the router and requests to them return 404.
///
/// Split out from [`start_http_server`] so unit tests can construct the
/// router without binding a TCP listener.
pub(crate) fn build_http_router(
    state: Arc<HttpState>,
    enable_admin_endpoints: bool,
    admin_token: Option<String>,
) -> Router {
    // Always-on routes: metrics, health, status, and read-only WS/UI surface.
    // Build the public router with `state` so the read-only handlers can
    // share the engine / metrics state.
    //
    // F-G6-002 / F-G6-003: `/admin/top` and `/ws/top` USED to live here.
    // They expose internal counters and (in cluster mode) fan out to every
    // peer over plaintext HTTP, so they now sit in the gated sub-router
    // below behind the same bearer-token middleware as the mutating
    // routes. When admin endpoints are disabled they return 404 to
    // unauthenticated callers, which matches every other sensitive
    // surface in this server.
    let public = Router::new()
        .route("/metrics", get(handle_metrics))
        .route("/health/live", get(handle_health_live))
        .route("/health/ready", get(handle_health_ready))
        .route("/status", get(handle_status))
        // Read-only surface: gauge endpoints used by load balancers and the UI.
        .route(
            "/admin/migration_status",
            get(handle_admin_migration_status),
        )
        .route("/admin/nodes", get(handle_admin_nodes))
        .route("/admin/memory", get(handle_admin_memory))
        .route("/admin/records", get(handle_admin_records))
        .route("/admin/replication", get(handle_admin_replication))
        // Read-only debug surface (no state mutation, no record contents).
        .route("/debug/freelist", get(handle_debug_freelist))
        .route("/debug/log-level", get(handle_get_log_level))
        // Web UI
        .route("/ui/", get(handle_ui_root))
        .route("/ui/{*path}", get(handle_ui))
        .with_state(state.clone());

    if !enable_admin_endpoints {
        return public;
    }

    // From here on the gated sub-router is being installed. The token must
    // be present and non-empty — `validate_safe_defaults` is the contract
    // owner. If the contract is violated we log loudly and refuse to mount
    // the gated routes (returning the public router) rather than mounting
    // an unauthenticated mutation surface.
    let token_bytes: Arc<[u8]> = match admin_token.as_deref() {
        Some(t) if !t.is_empty() => {
            // F-G6-005: short tokens are practically brute-forceable
            // (e.g., a 1-byte token through the case-insensitive Bearer
            // accept loop). `validate_safe_defaults` in `src/config.rs`
            // owns the hard reject (NEEDS-ORCHESTRATOR for G10); we
            // surface a startup warning so operators see the weakness
            // even in pre-config-fix builds.
            const ADMIN_TOKEN_MIN_BYTES: usize = 16;
            if t.len() < ADMIN_TOKEN_MIN_BYTES {
                tracing::warn!(
                    target: "teraslab::security",
                    token_len = t.len(),
                    min_recommended = ADMIN_TOKEN_MIN_BYTES,
                    "admin_token is shorter than {} bytes — recommend a 32-byte CSPRNG-derived \
                     value. The constant-time compare cannot defend against a guessable token.",
                    ADMIN_TOKEN_MIN_BYTES,
                );
            }
            Arc::from(t.as_bytes().to_vec().into_boxed_slice())
        }
        _ => {
            tracing::error!(
                "admin endpoints enabled without a configured admin_token — refusing to \
                 register the mutating /admin/* and /debug/* routes. This is a programmer \
                 error: ServerConfig::validate_safe_defaults should have rejected the \
                 startup. Restart with admin_token set or with enable_admin_endpoints = false.",
            );
            return public;
        }
    };

    let auth_state = AdminAuthState {
        expected_token: Some(token_bytes),
    };

    // F-G6-006: cap the body size on `PUT /debug/log-level` so an
    // authenticated-but-hostile caller can't send a 2 MiB body just to
    // exercise `String::to_lowercase`. 64 bytes covers every valid input
    // and a generous client-side mistake margin.
    let gated = Router::new()
        // Admin mutation
        .route("/admin/quiesce", put(handle_admin_quiesce))
        .route("/admin/rebalance", put(handle_admin_rebalance))
        .route("/admin/drain/{node_id}", put(handle_admin_drain))
        // F-G6-002 / F-G6-003: sensitive read surface moved here so the
        // bearer-token middleware below covers `/admin/top` and `/ws/top`.
        .route("/admin/top", get(handle_admin_top))
        .route("/ws/top", get(handle_ws_top))
        // Debug mutation / sensitive read
        .route("/debug/index", get(handle_debug_index))
        .route("/debug/redo", get(handle_debug_redo))
        .route(
            "/debug/log-level",
            put(handle_set_log_level).layer(axum::extract::DefaultBodyLimit::max(64)),
        )
        .route("/debug/records/{txid}", get(handle_debug_record))
        .layer(middleware::from_fn_with_state(
            auth_state,
            require_admin_bearer,
        ))
        .with_state(state);

    public.merge(gated)
}

/// Axum middleware enforcing `Authorization: Bearer <admin_token>` on the
/// gated `/admin/*` and `/debug/*` sub-router.
///
/// Behaviour:
///
/// - Missing or malformed `Authorization` header → 401 Unauthorized.
/// - Header present but the scheme is not `Bearer` (case-insensitive per
///   RFC 6750 §2.1) → 401.
/// - Bearer token does not match the configured token → 401.
/// - Bearer token matches → request is forwarded to the inner handler.
/// - Defensive: if the middleware was installed with no configured token
///   (programmer error in `build_http_router`), every request is rejected
///   with 401 rather than letting it through.
///
/// The token comparison uses [`subtle::ConstantTimeEq`] so reply timing
/// does not leak the matching prefix length of an attacker-supplied token.
async fn require_admin_bearer(
    State(auth): State<AdminAuthState>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> axum::response::Response {
    let expected = match auth.expected_token.as_deref() {
        Some(bytes) if !bytes.is_empty() => bytes,
        _ => {
            // Defensive: this branch is only reached if `build_http_router`
            // mounted the gate without a token, which it never does (the
            // builder returns early in that case). Refuse rather than allow.
            return (
                StatusCode::UNAUTHORIZED,
                "missing admin token configuration\n",
            )
                .into_response();
        }
    };

    let supplied = match extract_bearer_token(&headers) {
        Some(t) => t,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                "missing or malformed Authorization: Bearer <token> header\n",
            )
                .into_response();
        }
    };

    // F-G6-004: `subtle::ConstantTimeEq::ct_eq` for unequal-length
    // slices short-circuits to `Choice(0)` without comparing bytes. The
    // raw-byte path therefore leaks the configured token's length to a
    // timing-savvy attacker who can vary the supplied token length and
    // measure response time. To make the reply timing independent of
    // both content AND length, we hash both inputs into a 32-byte
    // SHA-256 digest and compare those — every call now compares a
    // fixed-size buffer of constant length.
    use sha2::Digest;
    let supplied_digest = sha2::Sha256::digest(supplied.as_bytes());
    let expected_digest = sha2::Sha256::digest(expected);
    if supplied_digest.ct_eq(expected_digest.as_slice()).into() {
        next.run(request).await
    } else {
        (StatusCode::UNAUTHORIZED, "invalid admin bearer token\n").into_response()
    }
}

/// Extract the raw token bytes from an `Authorization: Bearer <token>`
/// header value, or return `None` if the header is missing, not valid
/// ASCII, or does not start with a case-insensitive `Bearer ` prefix.
///
/// Per RFC 6750 §2.1 the scheme name is case-insensitive. Trailing
/// whitespace inside the token is preserved verbatim — clients should not
/// pad their tokens, and the constant-time comparison treats any padding
/// as a mismatch.
fn extract_bearer_token(headers: &HeaderMap) -> Option<&str> {
    let raw = headers.get(axum::http::header::AUTHORIZATION)?;
    let s = raw.to_str().ok()?;
    // Case-insensitive scheme match. `Bearer` is 6 bytes; require a
    // whitespace separator after it so `BearerXYZ` does not match.
    if s.len() < 7 {
        return None;
    }
    let (scheme, rest) = s.split_at(6);
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return None;
    }
    // Skip the single mandatory space; reject if anything else.
    let rest_bytes = rest.as_bytes();
    if rest_bytes.first() != Some(&b' ') {
        return None;
    }
    let token = &rest[1..];
    if token.is_empty() { None } else { Some(token) }
}

// ---------------------------------------------------------------------------
// /metrics — Prometheus text format
// ---------------------------------------------------------------------------

async fn handle_metrics(
    headers: HeaderMap,
    State(state): State<Arc<HttpState>>,
) -> impl IntoResponse {
    let span = http_span_for(&headers, "/metrics");
    let _entered = span.enter();
    let out = render_metrics_text(
        state.metrics,
        state.histograms,
        state.engine.index_len() as u64,
        state.engine.dah_index().len() as u64,
        state.engine.unmined_index().len() as u64,
        state.active_connections.load(Ordering::Relaxed) as u64,
    );

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    attach_traceparent_response(&mut resp_headers, &span);
    (StatusCode::OK, resp_headers, out)
}

/// Render the Prometheus text-format metrics payload.
///
/// Split out as a plain function so unit tests can scrape the output without
/// spinning up an HTTP listener. Parameters are decoupled from `HttpState`
/// to keep test plumbing light.
pub(crate) fn render_metrics_text(
    m: &ThreadMetrics,
    h: &ThreadHistograms,
    index_entries: u64,
    dah_entries: u64,
    unmined_entries: u64,
    active_connections: u64,
) -> String {
    let mut out = String::with_capacity(8192);

    // Spend counters
    prom_counter(
        &mut out,
        "teraslab_spends_attempted_total",
        m.spends_attempted.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_spends_succeeded_total",
        m.spends_succeeded.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_spends_idempotent_total",
        m.spends_idempotent.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_spends_failed_total",
        m.spends_failed.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_unspends_attempted_total",
        m.unspends_attempted.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_unspends_succeeded_total",
        m.unspends_succeeded.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_unspends_noop_total",
        m.unspends_noop.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_unspends_failed_total",
        m.unspends_failed.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_spend_multi_batches_total",
        m.spend_multi_batches.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_spend_multi_items_attempted_total",
        m.spend_multi_items_attempted.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_spend_multi_items_succeeded_total",
        m.spend_multi_items_succeeded.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_spend_multi_items_idempotent_total",
        m.spend_multi_items_idempotent.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_spend_multi_items_failed_total",
        m.spend_multi_items_failed.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_unspend_multi_batches_total",
        m.unspend_multi_batches.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_unspend_multi_items_attempted_total",
        m.unspend_multi_items_attempted.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_unspend_multi_items_succeeded_total",
        m.unspend_multi_items_succeeded.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_unspend_multi_items_idempotent_total",
        m.unspend_multi_items_idempotent.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_unspend_multi_items_failed_total",
        m.unspend_multi_items_failed.get(),
    );
    prom_counter(&mut out, "teraslab_dah_inserts_total", m.dah_inserts.get());
    prom_counter(&mut out, "teraslab_dah_removes_total", m.dah_removes.get());

    // Operation counters (per-op batch counters + per-item outcomes).
    prom_counter(
        &mut out,
        "teraslab_creates_attempted_total",
        m.creates_attempted.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_creates_succeeded_total",
        m.creates_succeeded.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_creates_failed_total",
        m.creates_failed.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_set_mined_attempted_total",
        m.set_mined_attempted.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_set_mined_succeeded_total",
        m.set_mined_succeeded.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_set_mined_items_attempted_total",
        m.set_mined_items_attempted.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_set_mined_items_succeeded_total",
        m.set_mined_items_succeeded.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_set_mined_items_failed_total",
        m.set_mined_items_failed.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_gets_attempted_total",
        m.gets_attempted.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_gets_succeeded_total",
        m.gets_succeeded.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_gets_not_found_total",
        m.gets_not_found.get(),
    );
    prom_counter(&mut out, "teraslab_gets_failed_total", m.gets_failed.get());
    prom_counter(
        &mut out,
        "teraslab_freezes_attempted_total",
        m.freezes_attempted.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_freezes_succeeded_total",
        m.freezes_succeeded.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_freezes_failed_total",
        m.freezes_failed.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_unfreezes_attempted_total",
        m.unfreezes_attempted.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_unfreezes_succeeded_total",
        m.unfreezes_succeeded.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_unfreezes_failed_total",
        m.unfreezes_failed.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_deletes_attempted_total",
        m.deletes_attempted.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_deletes_succeeded_total",
        m.deletes_succeeded.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_deletes_failed_total",
        m.deletes_failed.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_preserve_until_attempted_total",
        m.preserve_until_attempted.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_preserve_until_succeeded_total",
        m.preserve_until_succeeded.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_preserve_until_failed_total",
        m.preserve_until_failed.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_mark_longest_chain_attempted_total",
        m.mark_longest_chain_attempted.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_mark_longest_chain_succeeded_total",
        m.mark_longest_chain_succeeded.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_mark_longest_chain_failed_total",
        m.mark_longest_chain_failed.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_reassign_attempted_total",
        m.reassign_attempted.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_reassign_succeeded_total",
        m.reassign_succeeded.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_reassign_failed_total",
        m.reassign_failed.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_set_conflicting_attempted_total",
        m.set_conflicting_attempted.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_set_conflicting_succeeded_total",
        m.set_conflicting_succeeded.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_set_conflicting_failed_total",
        m.set_conflicting_failed.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_set_locked_attempted_total",
        m.set_locked_attempted.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_set_locked_succeeded_total",
        m.set_locked_succeeded.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_set_locked_failed_total",
        m.set_locked_failed.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_replication_degraded_acks_total",
        m.replication_degraded_acks.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_repl_degraded_durability_total",
        m.repl_degraded_durability.get(),
    );
    prom_counter(
        &mut out,
        "teraslab_stale_routing_request_total",
        m.stale_routing_request_total.get(),
    );

    // Labeled {op, outcome} counters — the new Phase 2 surface. Dual-emitted
    // alongside the scalar counters above; existing dashboards stay intact
    // while Prometheus queries can migrate to the richer labeled form.
    prom_labeled_counter(&mut out, "teraslab_operations_total", &m.operations);

    // Latency histograms (Prometheus histogram format).
    prom_histogram_ns(&mut out, "teraslab_spend_latency_ns", &h.spend_latency);
    prom_histogram_ns(&mut out, "teraslab_unspend_latency_ns", &h.unspend_latency);
    prom_histogram_ns(&mut out, "teraslab_create_latency_ns", &h.create_latency);
    prom_histogram_ns(
        &mut out,
        "teraslab_set_mined_latency_ns",
        &h.set_mined_latency,
    );
    prom_histogram_ns(&mut out, "teraslab_freeze_latency_ns", &h.freeze_latency);
    prom_histogram_ns(
        &mut out,
        "teraslab_unfreeze_latency_ns",
        &h.unfreeze_latency,
    );
    prom_histogram_ns(&mut out, "teraslab_delete_latency_ns", &h.delete_latency);
    prom_histogram_ns(&mut out, "teraslab_get_latency_ns", &h.get_latency);
    prom_histogram_ns(
        &mut out,
        "teraslab_mark_longest_chain_latency_ns",
        &h.mark_longest_chain_latency,
    );
    prom_histogram_ns(
        &mut out,
        "teraslab_reassign_latency_ns",
        &h.reassign_latency,
    );
    prom_histogram_ns(
        &mut out,
        "teraslab_set_conflicting_latency_ns",
        &h.set_conflicting_latency,
    );
    prom_histogram_ns(
        &mut out,
        "teraslab_set_locked_latency_ns",
        &h.set_locked_latency,
    );
    prom_histogram_ns(
        &mut out,
        "teraslab_preserve_until_latency_ns",
        &h.preserve_until_latency,
    );
    prom_histogram_ns(&mut out, "teraslab_lock_wait_ns", &h.lock_wait);

    // Index gauges
    prom_gauge(&mut out, "teraslab_index_entries", index_entries);
    prom_gauge(&mut out, "teraslab_dah_index_entries", dah_entries);
    prom_gauge(&mut out, "teraslab_unmined_index_entries", unmined_entries);

    // Connection gauge
    prom_gauge(&mut out, "teraslab_active_connections", active_connections);

    // Phase 5: subsystem metric surfaces. Each block is a no-op when the
    // corresponding `OnceLock` hasn't been initialized (e.g. in tests that
    // only want the original scalar counters). Full production runs init
    // all of them in `src/bin/server.rs` so every series is always present.
    if let Some(r) = replication_metrics() {
        prom_counter(
            &mut out,
            "teraslab_repl_batches_sent_total",
            r.repl_batches_sent_total.get(),
        );
        prom_counter(
            &mut out,
            "teraslab_repl_bytes_sent_total",
            r.repl_bytes_sent_total.get(),
        );
        prom_labeled_replica_counter(&mut out, "teraslab_repl_batches_acked_total", |i| {
            r.repl_batches_acked_total.get(i)
        });
        prom_labeled_replica_counter(&mut out, "teraslab_repl_batches_failed_total", |i| {
            r.repl_batches_failed_total.get(i)
        });
        prom_histogram_ns(
            &mut out,
            "teraslab_repl_batch_latency_ns",
            &r.repl_batch_latency_ns,
        );
        // Per-replica lag gauge. Cardinality is bounded by MAX_REPLICAS.
        use std::fmt::Write as _;
        let _ = writeln!(out, "# TYPE teraslab_repl_lag_sequences gauge");
        for i in 0..MAX_REPLICAS {
            let lag = r.lag(i);
            let _ = writeln!(
                out,
                "teraslab_repl_lag_sequences{{replica_idx=\"{i}\"}} {lag}"
            );
        }
    }
    if let Some(u) = io_uring_metrics() {
        prom_histogram_ns(
            &mut out,
            "teraslab_uring_submit_latency_ns",
            &u.uring_submit_latency_ns,
        );
        prom_histogram_ns(
            &mut out,
            "teraslab_uring_completion_latency_ns",
            &u.uring_completion_latency_ns,
        );
        prom_gauge(
            &mut out,
            "teraslab_uring_pending",
            u.uring_pending.load(Ordering::Relaxed) as u64,
        );
        prom_counter(
            &mut out,
            "teraslab_uring_submit_errors_total",
            u.uring_submit_errors_total.get(),
        );
        use std::fmt::Write as _;
        let _ = writeln!(out, "# TYPE teraslab_uring_completion_errors_total counter");
        for &cls in UringErrClass::all() {
            let v = u.uring_completion_errors_total.get(cls as u8 as usize);
            let _ = writeln!(
                out,
                "teraslab_uring_completion_errors_total{{errno=\"{}\"}} {}",
                cls.as_str(),
                v
            );
        }
    }
    if let Some(r) = redo_metrics() {
        prom_histogram_ns(
            &mut out,
            "teraslab_redo_flush_latency_ns",
            &r.redo_flush_latency_ns,
        );
        prom_histogram_ns(
            &mut out,
            "teraslab_redo_bytes_per_flush",
            &r.redo_bytes_per_flush,
        );
        prom_histogram_ns(
            &mut out,
            "teraslab_redo_entries_per_flush",
            &r.redo_entries_per_flush,
        );
        prom_counter(
            &mut out,
            "teraslab_redo_append_total",
            r.redo_append_total.get(),
        );
        prom_counter(
            &mut out,
            "teraslab_redo_flush_errors_total",
            r.redo_flush_errors_total.get(),
        );
    }
    if let Some(mm) = migration_metrics() {
        use std::fmt::Write as _;
        let _ = writeln!(
            out,
            "# TYPE teraslab_migration_bytes_transferred_total counter"
        );
        for &label in MigrationLabel::all() {
            let v = mm
                .migration_bytes_transferred_total
                .get(label as u8 as usize);
            let _ = writeln!(
                out,
                "teraslab_migration_bytes_transferred_total{{direction_role=\"{}\"}} {}",
                label.as_str(),
                v
            );
        }
        prom_counter(
            &mut out,
            "teraslab_migration_entries_applied_total",
            mm.migration_entries_applied_total.get(),
        );
        prom_gauge(
            &mut out,
            "teraslab_migration_active",
            mm.migration_active.load(Ordering::Relaxed) as u64,
        );
        prom_gauge(
            &mut out,
            "teraslab_migration_phase_preparing",
            mm.migration_phase_preparing.load(Ordering::Relaxed) as u64,
        );
        prom_gauge(
            &mut out,
            "teraslab_migration_phase_copying",
            mm.migration_phase_copying.load(Ordering::Relaxed) as u64,
        );
        prom_gauge(
            &mut out,
            "teraslab_migration_phase_delta",
            mm.migration_phase_delta.load(Ordering::Relaxed) as u64,
        );
        prom_gauge(
            &mut out,
            "teraslab_migration_phase_serving_new",
            mm.migration_phase_serving_new.load(Ordering::Relaxed) as u64,
        );
        prom_counter(
            &mut out,
            "teraslab_topology_epoch_mismatch_total",
            mm.topology_epoch_mismatch.get(),
        );
    }
    if let Some(sw) = swim_metrics() {
        prom_counter(
            &mut out,
            "teraslab_swim_probes_sent_total",
            sw.swim_probes_sent_total.get(),
        );
        prom_counter(
            &mut out,
            "teraslab_swim_probe_timeouts_total",
            sw.swim_probe_timeouts_total.get(),
        );
        prom_counter(
            &mut out,
            "teraslab_swim_indirect_probes_total",
            sw.swim_indirect_probes_total.get(),
        );
        prom_histogram_ns(
            &mut out,
            "teraslab_swim_suspicion_duration_ns",
            &sw.swim_suspicion_duration_ns,
        );
        use std::fmt::Write as _;
        let _ = writeln!(out, "# TYPE teraslab_swim_membership_churn_total counter");
        for &kind in SwimChurnKind::all() {
            let v = sw.swim_membership_churn_total.get(kind as u8 as usize);
            let _ = writeln!(
                out,
                "teraslab_swim_membership_churn_total{{kind=\"{}\"}} {}",
                kind.as_str(),
                v
            );
        }
    }
    if let Some(a) = allocator_metrics() {
        prom_counter(&mut out, "teraslab_alloc_total", a.alloc_total.get());
        prom_counter(
            &mut out,
            "teraslab_alloc_bytes_total",
            a.alloc_bytes_total.get(),
        );
        prom_counter(&mut out, "teraslab_free_total", a.free_total.get());
        prom_counter(
            &mut out,
            "teraslab_free_bytes_total",
            a.free_bytes_total.get(),
        );
        prom_gauge(
            &mut out,
            "teraslab_freelist_region_count",
            a.freelist_region_count.load(Ordering::Relaxed) as u64,
        );
        prom_gauge(
            &mut out,
            "teraslab_freelist_largest_region_bytes",
            a.freelist_largest_region_bytes.load(Ordering::Relaxed),
        );
    }

    out
}

/// Emit a Prometheus labeled counter for per-replica cells `0..MAX_REPLICAS`.
///
/// Every cell is emitted even when zero so `rate()` queries see the full
/// cardinality on the first scrape.
fn prom_labeled_replica_counter(out: &mut String, name: &str, mut get: impl FnMut(usize) -> u64) {
    use std::fmt::Write;
    let _ = writeln!(out, "# TYPE {name} counter");
    for i in 0..MAX_REPLICAS {
        let v = get(i);
        let _ = writeln!(out, "{name}{{replica_idx=\"{i}\"}} {v}");
    }
}

fn prom_counter(out: &mut String, name: &str, val: u64) {
    use std::fmt::Write;
    let _ = writeln!(out, "# TYPE {name} counter");
    let _ = writeln!(out, "{name} {val}");
}

/// Emit an [`OpOutcomeCounters`] table as a Prometheus labeled counter.
///
/// Produces one `name{op="X",outcome="Y"} V` line per `(op, outcome)` pair,
/// in stable iteration order (opcode-major). Every cell is emitted, even
/// zero-valued ones, so rate() queries see the full cardinality from the
/// first scrape.
fn prom_labeled_counter(out: &mut String, name: &str, counters: &OpOutcomeCounters) {
    use std::fmt::Write;
    let _ = writeln!(out, "# TYPE {name} counter");
    for &op in OpCode::all() {
        let op_str = op.as_str();
        for &outcome in Outcome::all() {
            let outcome_str = outcome.as_str();
            let v = counters.get(op, outcome);
            let _ = writeln!(
                out,
                "{name}{{op=\"{op_str}\",outcome=\"{outcome_str}\"}} {v}"
            );
        }
    }
}

fn prom_gauge(out: &mut String, name: &str, val: u64) {
    use std::fmt::Write;
    let _ = writeln!(out, "# TYPE {name} gauge");
    let _ = writeln!(out, "{name} {val}");
}

/// Emit a `LatencyHistogram` as a Prometheus histogram.
///
/// Produces one `_bucket{le="X"}` line per bucket (cumulative counts, as
/// required by Prometheus), a `_sum` line with the total recorded
/// nanoseconds, and a `_count` line. The final bucket uses
/// `le="+Inf"` to satisfy the Prometheus requirement that every
/// histogram has an unbounded terminating bucket.
fn prom_histogram_ns(out: &mut String, name: &str, hist: &LatencyHistogram) {
    use std::fmt::Write;
    let counts = hist.bucket_counts();
    let num = LatencyHistogram::num_buckets();
    let total = hist.count();
    let sum = hist.sum_ns();
    let _ = writeln!(out, "# TYPE {name} histogram");
    let mut cumulative: u64 = 0;
    for (i, count) in counts.iter().enumerate() {
        cumulative += *count;
        if i == num - 1 {
            let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {cumulative}");
        } else {
            let upper = hist.bucket_upper_ns_at(i);
            let _ = writeln!(out, "{name}_bucket{{le=\"{upper}\"}} {cumulative}");
        }
    }
    let _ = writeln!(out, "{name}_sum {sum}");
    let _ = writeln!(out, "{name}_count {total}");
}

// ---------------------------------------------------------------------------
// /health/live and /health/ready
// ---------------------------------------------------------------------------

async fn handle_health_live(State(_state): State<Arc<HttpState>>) -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn handle_health_ready(State(state): State<Arc<HttpState>>) -> impl IntoResponse {
    match compute_health_ready(&state) {
        ReadyState::Ready => (StatusCode::OK, "ready"),
        ReadyState::NotReady(reason) => (StatusCode::SERVICE_UNAVAILABLE, reason),
    }
}

/// Result of the [`/health/ready`] readiness check, broken out from the
/// axum handler so the readiness logic is testable without spinning up a
/// router.
#[derive(Debug, PartialEq, Eq)]
enum ReadyState {
    Ready,
    NotReady(&'static str),
}

/// R-055 (LMNH-07) / F-G6-001: `/health/ready` must reflect whether
/// this node is actually ready to take traffic, not just the boot-time
/// `ready` flag.
///
/// Pre-fix `state.ready` was hard-coded `true` at startup and never
/// updated, so a load balancer would route requests to a clustered node
/// before it joined a quorum and the node would reject every request
/// with `ERR_CLUSTER_NOT_READY`. F-G6-001 extends that fix to cover the
/// degraded-secondary case: when the DAH or unmined secondary failed to
/// rebuild at startup, dispatch returns `ERR_INDEX_DEGRADED` for any
/// dependent endpoint, so the load balancer must NOT receive a 200 here.
///
/// Order of checks (each returns a body explaining which subsystem is
/// degraded):
///
/// 1. `state.ready` — the binary's recovery-complete flag. False until
///    recovery has finished and the engine is attached.
/// 2. `dispatch::secondary_status()` — flipped at startup if the DAH or
///    unmined index rebuild failed.
/// 3. In clustered mode: `cluster.cluster_health().is_ready()` (the
///    node has observed at least one committed topology).
/// 4. In clustered mode: replication lag below the operator threshold.
fn compute_health_ready(state: &HttpState) -> ReadyState {
    if !state.ready.load(Ordering::Relaxed) {
        return ReadyState::NotReady("not ready (recovery in progress)");
    }
    // F-G6-001: dispatch flips the secondary readiness flags to `false`
    // at startup when the DAH or unmined index rebuild fails. The TCP
    // dispatch path already rejects dependent handlers with
    // `ERR_INDEX_DEGRADED`; the HTTP readiness gate must do the same so
    // load balancers stop routing traffic to a node that will fail.
    let sec = crate::server::dispatch::secondary_status();
    if !sec.dah_ok {
        return ReadyState::NotReady("DAH secondary index degraded");
    }
    if !sec.unmined_ok {
        return ReadyState::NotReady("unmined secondary index degraded");
    }
    if let Some(ref cluster) = state.cluster
        && !cluster.cluster_health().is_ready()
    {
        return ReadyState::NotReady("cluster not ready (no committed quorum yet)");
    }
    if state.cluster.is_some()
        && state.replica_lag_warn_threshold_ops > 0
        && cached_replica_lag_exceeds(state)
    {
        return ReadyState::NotReady("replica lag exceeds threshold");
    }
    ReadyState::Ready
}

/// F-G6-015: the replica-lag readiness predicate runs on every
/// `/health/ready` probe and was originally a fresh atomic scan over
/// `MAX_REPLICAS`. Load balancers typically poll readiness every
/// 1-5 seconds, and we cache the verdict for [`READINESS_LAG_CACHE_TTL`]
/// so a hot poll loop does not turn into an unbounded series of
/// `Acquire` loads on the metrics atomics. The cached value is a single
/// bool (the "exceeds threshold" verdict) packed together with the
/// nanosecond timestamp it was captured.
const READINESS_LAG_CACHE_TTL_MS: u64 = 500;

/// Cached `(timestamp_ns, exceeds_threshold)` pair for the replica-lag
/// readiness check. `timestamp_ns == 0` means "no cached verdict yet".
/// `AtomicU64` packs the bool as the low bit and the timestamp in the
/// upper 63 bits so the read+verdict observation is atomic.
static REPLICA_LAG_CACHE: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

fn cached_replica_lag_exceeds(state: &HttpState) -> bool {
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let cached = REPLICA_LAG_CACHE.load(Ordering::Relaxed);
    if cached != 0 {
        let cached_ts = cached >> 1;
        let cached_val = (cached & 1) != 0;
        let age_ns = now_ns.saturating_sub(cached_ts);
        if age_ns < READINESS_LAG_CACHE_TTL_MS * 1_000_000 {
            return cached_val;
        }
    }
    // Cache miss: recompute and store. The metrics consumer uses
    // `Acquire` semantics (see F-G6-024 in `metrics::ReplicationMetrics::lag`).
    let exceeds = replication_metrics().is_some_and(|r| {
        (0..MAX_REPLICAS).any(|i| r.lag(i) > state.replica_lag_warn_threshold_ops)
    });
    let packed = (now_ns << 1) | u64::from(exceeds);
    REPLICA_LAG_CACHE.store(packed, Ordering::Relaxed);
    exceeds
}

// ---------------------------------------------------------------------------
// /status — cluster health overview JSON
// ---------------------------------------------------------------------------

fn shard_counts(table: &ShardTable, self_id: NodeId) -> (u32, u32, u32, u32, usize) {
    let mut serving_master_count: u32 = 0;
    let mut serving_replica_count: u32 = 0;
    let mut target_master_count: u32 = 0;
    let mut target_replica_count: u32 = 0;

    for shard in 0..NUM_SHARDS as u16 {
        let effective = table.effective_assignment(shard);
        if effective.master == self_id {
            serving_master_count += 1;
        }
        if effective.replicas.contains(&self_id) {
            serving_replica_count += 1;
        }

        let target = table.target_assignment(shard);
        if target.master == self_id {
            target_master_count += 1;
        }
        if target.replicas.contains(&self_id) {
            target_replica_count += 1;
        }
    }

    (
        serving_master_count,
        serving_replica_count,
        target_master_count,
        target_replica_count,
        table.pending_handoff_count(),
    )
}

fn cluster_drain_complete(cluster: &RunningCluster) -> bool {
    let self_id = cluster.self_id();
    let table = cluster.shard_table();
    let table_guard = table.read();
    let (master_count, _, target_master_count, _, pending_handoff_shards) =
        shard_counts(&table_guard, self_id);
    drop(table_guard);

    master_count == 0
        && target_master_count == 0
        && pending_handoff_shards == 0
        && cluster.active_migrations() == 0
}

async fn wait_for_cluster_drain(cluster: &RunningCluster, wait_seconds: u64) -> bool {
    if wait_seconds == 0 {
        return cluster_drain_complete(cluster);
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(wait_seconds);
    loop {
        if cluster_drain_complete(cluster) {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn handle_status(State(state): State<Arc<HttpState>>) -> impl IntoResponse {
    let m = state.metrics;

    let cluster_info = if let Some(ref cluster) = state.cluster {
        let table = cluster.shard_table();
        let table_guard = table.read();
        let self_id = cluster.self_id();
        let (
            master_count,
            replica_count,
            target_master_count,
            target_replica_count,
            pending_handoff_shards,
        ) = shard_counts(&table_guard, self_id);
        let cluster_size = cluster.alive_node_count();
        drop(table_guard);

        serde_json::json!({
            "node_id": self_id.0,
            "cluster_size": cluster_size,
            "shard_table_version": cluster.shard_table_version(),
            "topology_term": cluster.committed_topology_term(),
            "master_shard_count": master_count,
            "replica_shard_count": replica_count,
            "target_master_shard_count": target_master_count,
            "target_replica_shard_count": target_replica_count,
            "pending_handoff_shards": pending_handoff_shards,
            "active_migrations": cluster.active_migrations(),
        })
    } else {
        serde_json::json!({
            "node_id": 0,
            "cluster_size": 1,
            "shard_table_version": 0,
            "master_shard_count": 0,
            "replica_shard_count": 0,
            "target_master_shard_count": 0,
            "target_replica_shard_count": 0,
            "pending_handoff_shards": 0,
            "active_migrations": 0,
        })
    };

    let status = serde_json::json!({
        "node_id": cluster_info["node_id"],
        "cluster_size": cluster_info["cluster_size"],
        "shard_table_version": cluster_info["shard_table_version"],
        "master_shard_count": cluster_info["master_shard_count"],
        "replica_shard_count": cluster_info["replica_shard_count"],
        "target_master_shard_count": cluster_info["target_master_shard_count"],
        "target_replica_shard_count": cluster_info["target_replica_shard_count"],
        "pending_handoff_shards": cluster_info["pending_handoff_shards"],
        "active_migrations": cluster_info["active_migrations"],
        "records": {
            "total": state.engine.index_len(),
            "dah_index": state.engine.dah_index().len(),
            "unmined_index": state.engine.unmined_index().len(),
        },
        "throughput": {
            "spends_attempted": m.spends_attempted.get(),
            "spends_succeeded": m.spends_succeeded.get(),
            "spends_failed": m.spends_failed.get(),
            "unspends_attempted": m.unspends_attempted.get(),
            "spend_multi_batches": m.spend_multi_batches.get(),
        },
        "ready": state.ready.load(Ordering::Relaxed),
    });

    (
        StatusCode::OK,
        [("content-type", "application/json")],
        status.to_string(),
    )
}

// ---------------------------------------------------------------------------
// /admin/* endpoints
// ---------------------------------------------------------------------------

/// Trigger graceful shard drain (quiesce). All master shards migrate away
/// from this node so it can be safely stopped.
async fn handle_admin_quiesce(State(state): State<Arc<HttpState>>) -> impl IntoResponse {
    match state.cluster {
        Some(ref cluster) => {
            cluster.quiesce();
            (StatusCode::OK, "quiesce initiated".to_string())
        }
        None => (StatusCode::BAD_REQUEST, "not in cluster mode".to_string()),
    }
}

/// Return the current migration status as JSON.
async fn handle_admin_migration_status(State(state): State<Arc<HttpState>>) -> impl IntoResponse {
    match state.cluster {
        Some(ref cluster) => {
            let migrations = cluster.migration_status();
            let inbound = cluster.inbound_pending_count();
            let inbound_entries = cluster.pending_inbound_entries();
            let fenced = cluster.fenced_shard_count();
            let active_count = migrations
                .iter()
                .filter(|m| {
                    m.state != crate::cluster::migration::MigrationState::Complete
                        && m.state != crate::cluster::migration::MigrationState::Failed
                })
                .count();
            let failed_count = migrations
                .iter()
                .filter(|m| m.state == crate::cluster::migration::MigrationState::Failed)
                .count();
            let body = serde_json::json!({
                "active_count": active_count,
                "failed_count": failed_count,
                "inbound_pending": inbound,
                "inbound_entries": inbound_entries.iter().map(|(shard, from_node)| {
                    serde_json::json!({
                        "shard": shard,
                        "from_node": from_node.0,
                    })
                }).collect::<Vec<_>>(),
                "fenced_shards": fenced,
                "migrations": migrations.iter().map(|m| {
                    serde_json::json!({
                        "shard": m.shard,
                        "from_node": m.from_node.0,
                        "to_node": m.to_node.0,
                        "state": format!("{:?}", m.state),
                        "migrated_records": m.migrated_records,
                        "total_records": m.total_records,
                        "bytes_sent": m.bytes_sent,
                    })
                }).collect::<Vec<_>>(),
            });
            (StatusCode::OK, body.to_string())
        }
        None => (
            StatusCode::OK,
            serde_json::json!({"active_count": 0, "migrations": []}).to_string(),
        ),
    }
}

/// List all nodes in the cluster with state and shard counts.
async fn handle_admin_nodes(State(state): State<Arc<HttpState>>) -> impl IntoResponse {
    let body = if let Some(ref cluster) = state.cluster {
        let addrs = cluster.node_addresses();
        let table = cluster.shard_table();
        let table_guard = table.read();

        let mut nodes = Vec::new();
        for (&node_id, &addr) in &addrs {
            let mut master_count: u32 = 0;
            let mut replica_count: u32 = 0;
            for shard in 0..NUM_SHARDS as u16 {
                let assignment = table_guard.assignment(shard);
                if assignment.master == node_id {
                    master_count += 1;
                }
                if assignment.replicas.contains(&node_id) {
                    replica_count += 1;
                }
            }
            nodes.push(serde_json::json!({
                "node_id": node_id.0,
                "address": addr.to_string(),
                "state": "alive",
                "master_shards": master_count,
                "replica_shards": replica_count,
            }));
        }
        drop(table_guard);
        serde_json::json!({ "nodes": nodes })
    } else {
        serde_json::json!({
            "nodes": [{
                "node_id": 0,
                "address": "local",
                "state": "alive",
                "master_shards": 0,
                "replica_shards": 0,
            }]
        })
    };

    (
        StatusCode::OK,
        [("content-type", "application/json")],
        body.to_string(),
    )
}

/// Memory breakdown.
async fn handle_admin_memory(State(state): State<Arc<HttpState>>) -> impl IntoResponse {
    let index_stats = state.engine.index_stats();
    let body = serde_json::json!({
        "index_bytes": index_stats.memory_bytes,
        "index_entries": index_stats.entry_count,
        "dah_index_entries": state.engine.dah_index().len(),
        "unmined_index_entries": state.engine.unmined_index().len(),
        "estimated_total_bytes": index_stats.memory_bytes,
    });
    json_response(body)
}

/// Record inventory summary.
async fn handle_admin_records(State(state): State<Arc<HttpState>>) -> impl IntoResponse {
    let body = serde_json::json!({
        "total_records": state.engine.index_len(),
        "dah_index_count": state.engine.dah_index().len(),
        "unmined_count": state.engine.unmined_index().len(),
    });
    json_response(body)
}

/// Replication configuration and status.
async fn handle_admin_replication(State(state): State<Arc<HttpState>>) -> impl IntoResponse {
    let body = if let Some(ref cluster) = state.cluster {
        serde_json::json!({
            "enabled": true,
            "topology_term": cluster.committed_topology_term(),
            "topology_epoch": cluster.committed_topology_term(),
            "peak_cluster_size": cluster.peak_cluster_size(),
            "ack_policy": format!("{:?}", cluster.ack_policy()),
            "best_effort": cluster.is_replication_best_effort(),
        })
    } else {
        serde_json::json!({ "enabled": false })
    };
    json_response(body)
}

/// Trigger cluster rebalance (quiesce current node).
async fn handle_admin_rebalance(State(state): State<Arc<HttpState>>) -> impl IntoResponse {
    match state.cluster {
        Some(ref cluster) => {
            cluster.quiesce();
            (StatusCode::OK, "rebalance initiated".to_string())
        }
        None => (StatusCode::BAD_REQUEST, "not in cluster mode".to_string()),
    }
}

/// Drain a specific node by ID.
async fn handle_admin_drain(
    State(state): State<Arc<HttpState>>,
    Path(node_id): Path<u64>,
    Query(query): Query<DrainQuery>,
) -> impl IntoResponse {
    match state.cluster {
        Some(ref cluster) => {
            if cluster.self_id().0 == node_id {
                cluster.quiesce();
                if query.wait_seconds > 0 {
                    if wait_for_cluster_drain(cluster, query.wait_seconds).await {
                        (StatusCode::OK, format!("drain complete for node {node_id}"))
                    } else {
                        (
                            StatusCode::ACCEPTED,
                            format!(
                                "drain still in progress for node {node_id} after {}s",
                                query.wait_seconds
                            ),
                        )
                    }
                } else {
                    (
                        StatusCode::ACCEPTED,
                        format!(
                            "drain initiated for node {node_id}; use ?wait_seconds=N to wait for completion"
                        ),
                    )
                }
            } else {
                // F-G6-011: the path `node_id` only ever has one
                // legal value — `self_id`. Reject mismatched IDs with
                // 400 and an unambiguous body so operators do not
                // silently mis-target a node.
                (
                    StatusCode::BAD_REQUEST,
                    format!(
                        "drain path node_id ({}) does not match this server's node_id ({}); \
                         each node drains itself — re-issue the request against the HTTP \
                         endpoint of the target node",
                        node_id,
                        cluster.self_id().0,
                    ),
                )
            }
        }
        None => (StatusCode::BAD_REQUEST, "not in cluster mode".to_string()),
    }
}

// ---------------------------------------------------------------------------
// /admin/top — full metrics snapshot for real-time monitoring
// ---------------------------------------------------------------------------

/// Build this node's local metrics snapshot as JSON.
fn build_local_top_snapshot(state: &HttpState) -> serde_json::Value {
    let m = state.metrics;
    let h = state.histograms;
    let index_stats = state.engine.index_stats();
    let alloc_stats = state.engine.allocator_stats();

    let node_id = state.cluster.as_ref().map(|c| c.self_id().0).unwrap_or(0);

    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    let redo = if let Some(ref rl) = state.redo_log {
        let log = rl.lock();
        let avail = log.available_space();
        let pos = log.write_position();
        let total = pos + avail;
        let utilization = if total > 0 {
            pos as f64 / total as f64
        } else {
            0.0
        };
        serde_json::json!({
            "current_sequence": log.current_sequence(),
            "write_position": pos,
            "available_space": avail,
            "utilization": utilization,
        })
    } else {
        serde_json::json!({
            "current_sequence": 0,
            "write_position": 0,
            "available_space": 0,
            "utilization": 0.0,
        })
    };

    serde_json::json!({
        "node_id": node_id,
        "timestamp_ms": timestamp_ms,
        "counters": {
            "spends_attempted": m.spends_attempted.get(),
            "spends_succeeded": m.spends_succeeded.get(),
            "spends_idempotent": m.spends_idempotent.get(),
            "spends_failed": m.spends_failed.get(),
            "unspends_attempted": m.unspends_attempted.get(),
            "unspends_succeeded": m.unspends_succeeded.get(),
            "unspends_noop": m.unspends_noop.get(),
            "unspends_failed": m.unspends_failed.get(),
            "spend_multi_batches": m.spend_multi_batches.get(),
            "creates_attempted": m.creates_attempted.get(),
            "creates_succeeded": m.creates_succeeded.get(),
            "set_mined_attempted": m.set_mined_attempted.get(),
            "set_mined_succeeded": m.set_mined_succeeded.get(),
            "gets_attempted": m.gets_attempted.get(),
            "gets_succeeded": m.gets_succeeded.get(),
            "freezes_attempted": m.freezes_attempted.get(),
            "deletes_attempted": m.deletes_attempted.get(),
        },
        "operations": operations_json(&m.operations),
        "latency": {
            "spend": histogram_json(&h.spend_latency),
            "spend_multi": histogram_json(&h.spend_multi_latency),
            "unspend": histogram_json(&h.unspend_latency),
            "lock_wait": histogram_json(&h.lock_wait),
        },
        "index": {
            "entries": index_stats.entry_count,
            "capacity": index_stats.capacity,
            "load_factor": index_stats.load_factor,
            "memory_bytes": index_stats.memory_bytes,
        },
        "storage": {
            "used_bytes": alloc_stats.used_bytes,
            "total_bytes": alloc_stats.device_size,
            "utilization": alloc_stats.utilization,
            "free_regions": alloc_stats.free_region_count,
        },
        "redo": redo,
        "connections": state.active_connections.load(Ordering::Relaxed),
        "ready": state.ready.load(Ordering::Relaxed),
        "replication_metrics": replication_metrics_json(),
        "uring_metrics": uring_metrics_json(),
        "redo_metrics": redo_metrics_json(),
        "migration_metrics": migration_metrics_json(state),
        "swim_metrics": swim_metrics_json(),
        "allocator_metrics": allocator_metrics_json(),
    })
}

/// Build the `replication_metrics` sub-shape for `/admin/top` and `/ws/top`.
fn replication_metrics_json() -> serde_json::Value {
    let Some(r) = replication_metrics() else {
        return serde_json::json!({
            "batches_sent": 0,
            "bytes_sent": 0,
            "leader_sequence": 0,
            "latency": histogram_json(&LatencyHistogram::new()),
            "per_replica": [],
        });
    };
    let mut per_replica = Vec::with_capacity(MAX_REPLICAS);
    for i in 0..MAX_REPLICAS {
        per_replica.push(serde_json::json!({
            "replica_idx": i,
            "last_acked_seq": r.per_replica[i].last_acked_seq.load(Ordering::Relaxed),
            "in_flight": r.per_replica[i].in_flight.load(Ordering::Relaxed),
            "bytes_sent": r.per_replica[i].bytes_sent.get(),
            "batches_acked": r.repl_batches_acked_total.get(i),
            "batches_failed": r.repl_batches_failed_total.get(i),
            "lag": r.lag(i),
        }));
    }
    serde_json::json!({
        "batches_sent": r.repl_batches_sent_total.get(),
        "bytes_sent": r.repl_bytes_sent_total.get(),
        "leader_sequence": r.leader_sequence.load(Ordering::Relaxed),
        "latency": histogram_json(&r.repl_batch_latency_ns),
        "per_replica": per_replica,
    })
}

/// Build the `uring_metrics` sub-shape for `/admin/top` and `/ws/top`.
fn uring_metrics_json() -> serde_json::Value {
    let Some(u) = io_uring_metrics() else {
        return serde_json::json!({
            "submit_latency": histogram_json(&LatencyHistogram::new()),
            "completion_latency": histogram_json(&LatencyHistogram::new()),
            "pending": 0,
            "submit_errors": 0,
            "completion_errors": {},
        });
    };
    let mut errs = serde_json::Map::new();
    for &cls in UringErrClass::all() {
        errs.insert(
            cls.as_str().to_string(),
            serde_json::json!(u.uring_completion_errors_total.get(cls as u8 as usize)),
        );
    }
    serde_json::json!({
        "submit_latency": histogram_json(&u.uring_submit_latency_ns),
        "completion_latency": histogram_json(&u.uring_completion_latency_ns),
        "pending": u.uring_pending.load(Ordering::Relaxed),
        "submit_errors": u.uring_submit_errors_total.get(),
        "completion_errors": serde_json::Value::Object(errs),
    })
}

/// Build the `redo_metrics` sub-shape for `/admin/top` and `/ws/top`.
fn redo_metrics_json() -> serde_json::Value {
    let Some(r) = redo_metrics() else {
        return serde_json::json!({
            "flush_latency": histogram_json(&LatencyHistogram::new()),
            "bytes_per_flush": histogram_json(&LatencyHistogram::new()),
            "entries_per_flush": histogram_json(&LatencyHistogram::new()),
            "append_total": 0,
            "flush_errors_total": 0,
        });
    };
    serde_json::json!({
        "flush_latency": histogram_json(&r.redo_flush_latency_ns),
        "bytes_per_flush": histogram_json(&r.redo_bytes_per_flush),
        "entries_per_flush": histogram_json(&r.redo_entries_per_flush),
        "append_total": r.redo_append_total.get(),
        "flush_errors_total": r.redo_flush_errors_total.get(),
    })
}

/// Build the `migration_metrics` sub-shape for `/admin/top` and `/ws/top`.
fn migration_metrics_json(state: &HttpState) -> serde_json::Value {
    let mm = migration_metrics();
    let mut bytes = serde_json::Map::new();
    if let Some(m) = mm {
        for &label in MigrationLabel::all() {
            bytes.insert(
                label.as_str().to_string(),
                serde_json::json!(
                    m.migration_bytes_transferred_total
                        .get(label as u8 as usize)
                ),
            );
        }
    }
    let entries_applied = mm
        .map(|m| m.migration_entries_applied_total.get())
        .unwrap_or(0);
    let active = mm
        .map(|m| m.migration_active.load(Ordering::Relaxed) as u64)
        .unwrap_or(0);
    let preparing = mm
        .map(|m| m.migration_phase_preparing.load(Ordering::Relaxed) as u64)
        .unwrap_or(0);
    let copying = mm
        .map(|m| m.migration_phase_copying.load(Ordering::Relaxed) as u64)
        .unwrap_or(0);
    let delta = mm
        .map(|m| m.migration_phase_delta.load(Ordering::Relaxed) as u64)
        .unwrap_or(0);
    let serving_new = mm
        .map(|m| m.migration_phase_serving_new.load(Ordering::Relaxed) as u64)
        .unwrap_or(0);

    // Per-shard `migrations` list mirrors the existing `/admin/migration_status`
    // output but is reshaped for the admin UI — adds `phase`, `started_at_ms`
    // computed from the coordinator's live progress list.
    let mut shards = Vec::new();
    if let Some(ref cluster) = state.cluster {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        for p in cluster.migration_status() {
            shards.push(serde_json::json!({
                "shard": p.shard,
                "from_node": p.from_node.0,
                "to_node": p.to_node.0,
                "phase": format!("{:?}", p.state),
                "records_transferred": p.migrated_records,
                "bytes_transferred": p.bytes_sent,
                "total_records": p.total_records,
                "is_master": p.is_master,
                "started_at_ms": now_ms,
            }));
        }
    }

    serde_json::json!({
        "bytes_transferred": serde_json::Value::Object(bytes),
        "entries_applied_total": entries_applied,
        "active": active,
        "phase": {
            "preparing": preparing,
            "copying": copying,
            "delta": delta,
            "serving_new": serving_new,
        },
        "migrations": shards,
    })
}

/// Build the `swim_metrics` sub-shape for `/admin/top` and `/ws/top`.
fn swim_metrics_json() -> serde_json::Value {
    let Some(sw) = swim_metrics() else {
        return serde_json::json!({
            "probes_sent": 0,
            "probe_timeouts": 0,
            "indirect_probes": 0,
            "suspicion_duration": histogram_json(&LatencyHistogram::new()),
            "churn": {},
        });
    };
    let mut churn = serde_json::Map::new();
    for &kind in SwimChurnKind::all() {
        churn.insert(
            kind.as_str().to_string(),
            serde_json::json!(sw.swim_membership_churn_total.get(kind as u8 as usize)),
        );
    }
    serde_json::json!({
        "probes_sent": sw.swim_probes_sent_total.get(),
        "probe_timeouts": sw.swim_probe_timeouts_total.get(),
        "indirect_probes": sw.swim_indirect_probes_total.get(),
        "suspicion_duration": histogram_json(&sw.swim_suspicion_duration_ns),
        "churn": serde_json::Value::Object(churn),
    })
}

/// Build the `allocator_metrics` sub-shape for `/admin/top` and `/ws/top`.
fn allocator_metrics_json() -> serde_json::Value {
    let Some(a) = allocator_metrics() else {
        return serde_json::json!({
            "alloc_total": 0,
            "alloc_bytes_total": 0,
            "free_total": 0,
            "free_bytes_total": 0,
            "freelist_region_count": 0,
            "freelist_largest_region_bytes": 0,
        });
    };
    serde_json::json!({
        "alloc_total": a.alloc_total.get(),
        "alloc_bytes_total": a.alloc_bytes_total.get(),
        "free_total": a.free_total.get(),
        "free_bytes_total": a.free_bytes_total.get(),
        "freelist_region_count": a.freelist_region_count.load(Ordering::Relaxed),
        "freelist_largest_region_bytes": a.freelist_largest_region_bytes.load(Ordering::Relaxed),
    })
}

/// Serialize the labeled `{op, outcome}` counters table into a nested
/// JSON object:
///
/// ```json
/// {
///   "spend":   { "ok": 123, "idempotent": 4, ... },
///   "unspend": { ... },
///   ...
/// }
/// ```
///
/// Emitted inside the `/admin/top` + `/ws/top` payloads so the admin UI
/// can render per-op outcome tables without a second request. Zero cells
/// are still emitted so clients can build a stable table shape.
fn operations_json(counters: &OpOutcomeCounters) -> serde_json::Value {
    let mut root = serde_json::Map::with_capacity(OpCode::all().len());
    for &op in OpCode::all() {
        let mut inner = serde_json::Map::with_capacity(Outcome::all().len());
        for &outcome in Outcome::all() {
            inner.insert(
                outcome.as_str().to_string(),
                serde_json::json!(counters.get(op, outcome)),
            );
        }
        root.insert(op.as_str().to_string(), serde_json::Value::Object(inner));
    }
    serde_json::Value::Object(root)
}

/// Convert a `LatencyHistogram` to JSON with percentiles.
fn histogram_json(h: &crate::metrics::LatencyHistogram) -> serde_json::Value {
    serde_json::json!({
        "count": h.count(),
        "mean_ns": h.mean_ns(),
        "p50_ns": h.percentile_ns(0.50),
        "p95_ns": h.percentile_ns(0.95),
        "p99_ns": h.percentile_ns(0.99),
    })
}

async fn fetch_remote_top_snapshot(
    client: reqwest::Client,
    url: String,
    traceparent: Option<String>,
) -> Option<serde_json::Value> {
    // F-G6-008: the inbound /admin/top span has the W3C trace context the
    // caller sent us. Without re-emitting it on the cluster fan-out, each
    // remote peer starts an orphan trace. Attach the header byte-for-byte
    // so the operator's tracing backend can stitch the cluster snapshot
    // into one logical trace.
    let mut req = client.get(&url);
    if let Some(tp) = traceparent {
        req = req.header("traceparent", tp);
    }
    let resp = req.send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    resp.json::<serde_json::Value>().await.ok()
}

/// Build a cluster-wide top snapshot by fetching from all nodes with bounded
/// parallelism.
///
/// Returns the local snapshot plus remote node snapshots, with an aggregate.
/// Remote nodes are queried with `?local=true` to prevent recursive fan-out.
/// If a remote node doesn't respond within 2 seconds, it is omitted.
///
/// F-G6-008: the optional `traceparent` is taken from the current
/// `tracing::Span` (set by `http_span_for` on the inbound handler) and
/// re-emitted on every outbound HTTP request so the operator can stitch
/// the cluster trace together.
async fn build_cluster_top_snapshot(state: &HttpState) -> serde_json::Value {
    let local = build_local_top_snapshot(state);
    let mut all_nodes = vec![local.clone()];
    let traceparent = traceparent_for_span(&tracing::Span::current());

    // Fan out to remote nodes (if clustered)
    if let Some(ref cluster) = state.cluster {
        let self_id = cluster.self_id();
        let addrs = cluster.node_addresses();
        let http_port = state.http_port;

        let mut urls = Vec::new();
        for (&node_id, &addr) in &addrs {
            if node_id == self_id {
                continue; // Skip self — already have local snapshot
            }
            let url = format!("http://{}:{}/admin/top?local=true", addr.ip(), http_port);
            urls.push(url);
        }

        if let Ok(client) = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
        {
            let mut urls = urls.into_iter();
            let mut tasks = tokio::task::JoinSet::new();

            for _ in 0..ADMIN_TOP_REMOTE_FANOUT_LIMIT {
                let Some(url) = urls.next() else { break };
                tasks.spawn(fetch_remote_top_snapshot(
                    client.clone(),
                    url,
                    traceparent.clone(),
                ));
            }

            while let Some(joined) = tasks.join_next().await {
                if let Ok(Some(snapshot)) = joined {
                    all_nodes.push(snapshot);
                }
                if let Some(url) = urls.next() {
                    tasks.spawn(fetch_remote_top_snapshot(
                        client.clone(),
                        url,
                        traceparent.clone(),
                    ));
                }
            }
        }
    }

    // Build aggregate by summing counters, index entries, storage, connections
    let aggregate = aggregate_snapshots(&all_nodes);

    serde_json::json!({
        "aggregate": aggregate,
        "nodes": all_nodes,
    })
}

/// Sum counters and system stats across all node snapshots.
fn aggregate_snapshots(nodes: &[serde_json::Value]) -> serde_json::Value {
    let timestamp_ms = nodes
        .iter()
        .filter_map(|n| n["timestamp_ms"].as_u64())
        .max()
        .unwrap_or(0);

    let counter_keys = [
        "spends_attempted",
        "spends_succeeded",
        "spends_idempotent",
        "spends_failed",
        "unspends_attempted",
        "unspends_succeeded",
        "unspends_noop",
        "unspends_failed",
        "spend_multi_batches",
        "creates_attempted",
        "creates_succeeded",
        "set_mined_attempted",
        "set_mined_succeeded",
        "gets_attempted",
        "gets_succeeded",
        "freezes_attempted",
        "deletes_attempted",
    ];

    let mut counters = serde_json::Map::new();
    for key in &counter_keys {
        let sum: u64 = nodes
            .iter()
            .filter_map(|n| n["counters"][*key].as_u64())
            .sum();
        counters.insert(key.to_string(), serde_json::json!(sum));
    }

    // Latency: take the max of p99/p95, weighted mean for p50/mean.
    //
    // F-G6-009: empty-data nodes (timed out or `count == 0`) used to be
    // counted into the denominator anyway because the original sum/sum
    // pattern was symmetric; skipping them explicitly here keeps the
    // weighted mean honest. We also accumulate the product in `u128` so
    // a long-running hot node cannot overflow u64 (lock-wait counts can
    // be in the millions, mean_ns in the nanoseconds — the product fits
    // in 64 bits today but blows past it on multi-hour clusters).
    let latency_keys = ["spend", "spend_multi", "unspend", "lock_wait"];
    let mut latency = serde_json::Map::new();
    for lk in &latency_keys {
        let mut total_count: u128 = 0;
        let mut weighted_sum: u128 = 0;
        for n in nodes {
            let c = n["latency"][*lk]["count"].as_u64().unwrap_or(0);
            if c == 0 {
                continue;
            }
            let m = n["latency"][*lk]["mean_ns"].as_u64().unwrap_or(0);
            total_count += c as u128;
            weighted_sum += (c as u128) * (m as u128);
        }
        let total_count_u64: u64 = total_count.min(u128::from(u64::MAX)) as u64;
        let weighted_mean: u64 = if total_count > 0 {
            (weighted_sum / total_count).min(u128::from(u64::MAX)) as u64
        } else {
            0
        };
        let max_p50: u64 = nodes
            .iter()
            .filter_map(|n| n["latency"][*lk]["p50_ns"].as_u64())
            .max()
            .unwrap_or(0);
        let max_p95: u64 = nodes
            .iter()
            .filter_map(|n| n["latency"][*lk]["p95_ns"].as_u64())
            .max()
            .unwrap_or(0);
        let max_p99: u64 = nodes
            .iter()
            .filter_map(|n| n["latency"][*lk]["p99_ns"].as_u64())
            .max()
            .unwrap_or(0);
        latency.insert(
            lk.to_string(),
            serde_json::json!({
                "count": total_count_u64,
                "mean_ns": weighted_mean,
                "p50_ns": max_p50,
                "p95_ns": max_p95,
                "p99_ns": max_p99,
            }),
        );
    }

    // Index: sum entries/capacity/memory, weighted avg load factor
    let index_entries: u64 = nodes
        .iter()
        .filter_map(|n| n["index"]["entries"].as_u64())
        .sum();
    let index_capacity: u64 = nodes
        .iter()
        .filter_map(|n| n["index"]["capacity"].as_u64())
        .sum();
    let index_memory: u64 = nodes
        .iter()
        .filter_map(|n| n["index"]["memory_bytes"].as_u64())
        .sum();
    let index_lf = if index_capacity > 0 {
        index_entries as f64 / index_capacity as f64
    } else {
        0.0
    };

    // Storage: sum used/total, compute aggregate utilization
    let storage_used: u64 = nodes
        .iter()
        .filter_map(|n| n["storage"]["used_bytes"].as_u64())
        .sum();
    let storage_total: u64 = nodes
        .iter()
        .filter_map(|n| n["storage"]["total_bytes"].as_u64())
        .sum();
    let storage_util = if storage_total > 0 {
        storage_used as f64 / storage_total as f64
    } else {
        0.0
    };
    let storage_free_regions: u64 = nodes
        .iter()
        .filter_map(|n| n["storage"]["free_regions"].as_u64())
        .sum();

    // Redo: sum
    let redo_seq: u64 = nodes
        .iter()
        .filter_map(|n| n["redo"]["current_sequence"].as_u64())
        .sum();
    let redo_avail: u64 = nodes
        .iter()
        .filter_map(|n| n["redo"]["available_space"].as_u64())
        .sum();
    let redo_pos: u64 = nodes
        .iter()
        .filter_map(|n| n["redo"]["write_position"].as_u64())
        .sum();
    let redo_total = redo_pos + redo_avail;
    let redo_util = if redo_total > 0 {
        redo_pos as f64 / redo_total as f64
    } else {
        0.0
    };

    let connections: u64 = nodes.iter().filter_map(|n| n["connections"].as_u64()).sum();
    let all_ready = nodes.iter().all(|n| n["ready"].as_bool().unwrap_or(false));

    // Aggregate the `operations` labeled-counter table: sum each
    // `{op, outcome}` cell across all nodes. Emit the full 14 × 8 shape
    // so consumers always see a stable set of keys.
    let mut operations = serde_json::Map::with_capacity(OpCode::all().len());
    for &op in OpCode::all() {
        let mut inner = serde_json::Map::with_capacity(Outcome::all().len());
        for &outcome in Outcome::all() {
            let sum: u64 = nodes
                .iter()
                .filter_map(|n| n["operations"][op.as_str()][outcome.as_str()].as_u64())
                .sum();
            inner.insert(outcome.as_str().to_string(), serde_json::json!(sum));
        }
        operations.insert(op.as_str().to_string(), serde_json::Value::Object(inner));
    }

    serde_json::json!({
        "timestamp_ms": timestamp_ms,
        "node_count": nodes.len(),
        "counters": counters,
        "operations": serde_json::Value::Object(operations),
        "latency": latency,
        "index": {
            "entries": index_entries,
            "capacity": index_capacity,
            "load_factor": index_lf,
            "memory_bytes": index_memory,
        },
        "storage": {
            "used_bytes": storage_used,
            "total_bytes": storage_total,
            "utilization": storage_util,
            "free_regions": storage_free_regions,
        },
        "redo": {
            "current_sequence": redo_seq,
            "write_position": redo_pos,
            "available_space": redo_avail,
            "utilization": redo_util,
        },
        "connections": connections,
        "ready": all_ready,
    })
}

/// Query parameter for /admin/top.
#[derive(serde::Deserialize)]
struct TopQuery {
    /// When true, return only this node's local snapshot (no fan-out).
    #[serde(default)]
    local: bool,
}

/// Return a metrics snapshot. Without `?local=true`, fans out to all
/// cluster nodes and returns an aggregate + per-node breakdown.
async fn handle_admin_top(
    State(state): State<Arc<HttpState>>,
    Query(query): Query<TopQuery>,
) -> impl IntoResponse {
    if query.local || state.cluster.is_none() {
        json_response(build_local_top_snapshot(&state))
    } else {
        json_response(build_cluster_top_snapshot(&state).await)
    }
}

// ---------------------------------------------------------------------------
// /ws/top — WebSocket push of metrics every second
// ---------------------------------------------------------------------------

/// Upgrade to WebSocket and push metrics snapshots every second.
async fn handle_ws_top(
    ws: axum::extract::WebSocketUpgrade,
    State(state): State<Arc<HttpState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| ws_top_loop(socket, state))
}

/// WebSocket loop: send cluster-wide JSON metrics snapshot every second.
async fn ws_top_loop(mut socket: WebSocket, state: Arc<HttpState>) {
    loop {
        let snapshot = if state.cluster.is_some() {
            build_cluster_top_snapshot(&state).await
        } else {
            // Single-node: wrap local snapshot in the same envelope
            let local = build_local_top_snapshot(&state);
            serde_json::json!({
                "aggregate": local,
                "nodes": [local],
            })
        };
        let msg = Message::Text(snapshot.to_string().into());
        match tokio::time::timeout(WS_TOP_SEND_TIMEOUT, socket.send(msg)).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => break, // Client disconnected
            Err(_) => break,     // Client stopped reading; drop the slow socket
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        // F-G6-010: drain incoming messages (pings, close frames). Break
        // the outer loop on `Message::Close` so a well-behaved client
        // that initiates a graceful close doesn't sit idle for the
        // 5-second send timeout before we notice.
        let mut closed = false;
        while let Ok(Some(Ok(msg))) =
            tokio::time::timeout(Duration::from_millis(10), socket.recv()).await
        {
            if matches!(msg, Message::Close(_)) {
                tracing::debug!("ws/top: client sent Close frame; exiting loop");
                closed = true;
                break;
            }
        }
        if closed {
            break;
        }
    }
}

// ---------------------------------------------------------------------------
// /debug/* endpoints
// ---------------------------------------------------------------------------

async fn handle_debug_index(State(state): State<Arc<HttpState>>) -> impl IntoResponse {
    let stats = state.engine.index_stats();
    let body = serde_json::json!({
        "entries": stats.entry_count,
        "capacity": stats.capacity,
        "load_factor": stats.load_factor,
        "hugepage_enabled": stats.hugepage_enabled,
        "max_probe_distance": stats.max_probe_distance,
        "memory_bytes": stats.memory_bytes,
    });
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        body.to_string(),
    )
}

/// Real allocator/freelist statistics.
async fn handle_debug_freelist(State(state): State<Arc<HttpState>>) -> impl IntoResponse {
    let stats = state.engine.allocator_stats();
    let body = serde_json::json!({
        "data_region_start": stats.data_region_start,
        "next_offset": stats.next_offset,
        "device_size": stats.device_size,
        "alignment": stats.alignment,
        "free_region_count": stats.free_region_count,
        "total_free_bytes": stats.total_free_bytes,
        "largest_free_region": stats.largest_free_region,
        "used_bytes": stats.used_bytes,
        "utilization": stats.utilization,
    });
    json_response(body)
}

/// Real redo log statistics.
async fn handle_debug_redo(State(state): State<Arc<HttpState>>) -> impl IntoResponse {
    let body = if let Some(ref rl) = state.redo_log {
        let log = rl.lock();
        let avail = log.available_space();
        let pos = log.write_position();
        let total = pos + avail;
        let utilization = if total > 0 {
            pos as f64 / total as f64
        } else {
            0.0
        };
        serde_json::json!({
            "available": true,
            "current_sequence": log.current_sequence(),
            "write_position": pos,
            "available_space": avail,
            "log_size": total,
            "utilization": utilization,
        })
    } else {
        serde_json::json!({ "available": false })
    };
    json_response(body)
}

async fn handle_set_log_level(
    State(state): State<Arc<HttpState>>,
    body: String,
) -> impl IntoResponse {
    let level = match body.trim().to_lowercase().as_str() {
        "error" => LOG_LEVEL_ERROR,
        "warn" => LOG_LEVEL_WARN,
        "info" => LOG_LEVEL_INFO,
        "debug" => LOG_LEVEL_DEBUG,
        "trace" => LOG_LEVEL_TRACE,
        _ => return (StatusCode::BAD_REQUEST, "invalid log level".to_string()),
    };
    state.log_level.store(level, Ordering::Relaxed);
    (StatusCode::OK, format!("log level set to {}", body.trim()))
}

async fn handle_get_log_level(State(state): State<Arc<HttpState>>) -> impl IntoResponse {
    let level = state.log_level.load(Ordering::Relaxed);
    let name = match level {
        LOG_LEVEL_ERROR => "error",
        LOG_LEVEL_WARN => "warn",
        LOG_LEVEL_INFO => "info",
        LOG_LEVEL_DEBUG => "debug",
        LOG_LEVEL_TRACE => "trace",
        _ => "unknown",
    };
    (StatusCode::OK, name.to_string())
}

async fn handle_debug_record(
    State(state): State<Arc<HttpState>>,
    Path(txid_hex): Path<String>,
) -> impl IntoResponse {
    if txid_hex.len() > TXID_HEX_LEN {
        return (StatusCode::BAD_REQUEST, "invalid txid length".to_string());
    }

    // Parse hex txid
    let txid_bytes = match parse_hex_txid(&txid_hex) {
        Some(b) => b,
        None => return (StatusCode::BAD_REQUEST, "invalid txid hex".to_string()),
    };

    let key = TxKey { txid: txid_bytes };
    match state.engine.read_metadata(&key) {
        Ok(meta) => {
            let tx_version = { meta.tx_version };
            let locktime = { meta.locktime };
            let fee = { meta.fee };
            let size_in_bytes = { meta.size_in_bytes };
            let utxo_count = { meta.utxo_count };
            let spent_utxos = { meta.spent_utxos };
            let pruned_utxos = { meta.pruned_utxos };
            let generation = { meta.generation };
            let unmined_since = { meta.unmined_since };
            let delete_at_height = { meta.delete_at_height };
            let preserve_until = { meta.preserve_until };
            let block_entry_count = { meta.block_entry_count };
            let flags = { meta.flags }.bits();
            let body = serde_json::json!({
                "tx_version": tx_version,
                "locktime": locktime,
                "fee": fee,
                "size_in_bytes": size_in_bytes,
                "utxo_count": utxo_count,
                "spent_utxos": spent_utxos,
                "pruned_utxos": pruned_utxos,
                "generation": generation,
                "unmined_since": unmined_since,
                "delete_at_height": delete_at_height,
                "preserve_until": preserve_until,
                "block_entry_count": block_entry_count,
                "flags": format!("{:#04x}", flags),
            });
            (StatusCode::OK, body.to_string())
        }
        Err(_) => (StatusCode::NOT_FOUND, "tx not found".to_string()),
    }
}

// ---------------------------------------------------------------------------
// /ui/* — embedded static Web UI
// ---------------------------------------------------------------------------

/// Serve the root UI page.
async fn handle_ui_root() -> impl IntoResponse {
    serve_embedded_file("index.html")
}

/// Serve embedded static files with SPA fallback.
async fn handle_ui(Path(path): Path<String>) -> impl IntoResponse {
    serve_embedded_file(&path)
}

/// Serve a file from the embedded `UiAssets`, falling back to `index.html`
/// for client-side routing (SPA).
///
/// F-G6-007: `rust_embed` already cannot serve files outside the embed
/// map, so `..`-traversal attempts simply miss and SPA-fallback to
/// `index.html`. That's harmless today, but the SPA fallback masks the
/// signal that the caller is probing for traversal. Return 404 for any
/// path containing `..` or `\` so future refactors that swap to a
/// filesystem loader can't silently re-introduce a traversal hole.
fn serve_embedded_file(path: &str) -> (StatusCode, [(axum::http::HeaderName, String); 1], Vec<u8>) {
    if path.contains("..") || path.contains('\\') {
        return (
            StatusCode::NOT_FOUND,
            [(axum::http::header::CONTENT_TYPE, "text/plain".to_string())],
            b"not found".to_vec(),
        );
    }
    let (data, mime) = match UiAssets::get(path) {
        Some(content) => {
            let mime = mime_guess::from_path(path)
                .first_or_octet_stream()
                .to_string();
            (content.data.to_vec(), mime)
        }
        None => {
            // SPA fallback: serve index.html for unrecognized paths
            match UiAssets::get("index.html") {
                Some(content) => (content.data.to_vec(), "text/html".to_string()),
                None => {
                    return (
                        StatusCode::NOT_FOUND,
                        [(axum::http::header::CONTENT_TYPE, "text/plain".to_string())],
                        b"UI not found".to_vec(),
                    );
                }
            }
        }
    };
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, mime)],
        data,
    )
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convenience wrapper for JSON responses.
fn json_response(
    body: serde_json::Value,
) -> (
    StatusCode,
    [(axum::http::HeaderName, &'static str); 1],
    String,
) {
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        body.to_string(),
    )
}

const TXID_HEX_LEN: usize = 64;

fn parse_hex_txid(hex: &str) -> Option<[u8; 32]> {
    if hex.len() != TXID_HEX_LEN {
        return None;
    }
    let mut bytes = [0u8; 32];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let hi = hex_nibble(chunk[0])?;
        let lo = hex_nibble(chunk[1])?;
        bytes[i] = (hi << 4) | lo;
    }
    Some(bytes)
}

fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// W3C trace context propagation (Phase 4)
// ---------------------------------------------------------------------------

/// Parse a W3C `traceparent` header into a [`WireTraceContext`].
///
/// Format: `version-trace_id-span_id-trace_flags` (`00-<32hex>-<16hex>-01`).
/// Returns `None` on any shape error — callers fall back to "no inbound
/// context, start a root span."
pub(crate) fn parse_traceparent(value: &str) -> Option<WireTraceContext> {
    let parts: Vec<&str> = value.trim().split('-').collect();
    if parts.len() != 4 {
        return None;
    }
    if parts[0] != "00" {
        // Only W3C trace context v00 is parsed — future versions may add
        // fields; forwards-compat is out of scope for a local debug UI.
        return None;
    }
    let trace_id = decode_hex::<16>(parts[1])?;
    let span_id = decode_hex::<8>(parts[2])?;
    // trace_flags (parts[3]) is ignored — we always force-sample the
    // receiver span when an inbound traceparent is present.
    Some(WireTraceContext { trace_id, span_id })
}

fn decode_hex<const N: usize>(s: &str) -> Option<[u8; N]> {
    if s.len() != N * 2 {
        return None;
    }
    let bytes = s.as_bytes();
    let mut out = [0u8; N];
    for (i, chunk) in bytes.chunks_exact(2).enumerate() {
        let hi = hex_nibble(chunk[0])?;
        let lo = hex_nibble(chunk[1])?;
        out[i] = (hi << 4) | lo;
    }
    Some(out)
}

/// Encode a [`WireTraceContext`] as a W3C `traceparent` value (version 00,
/// sampled). Used to emit the header on outbound HTTP responses so clients
/// can follow the trace.
pub(crate) fn encode_traceparent(ctx: &WireTraceContext) -> String {
    let mut s = String::with_capacity(55);
    s.push_str("00-");
    for b in ctx.trace_id {
        let _ = std::fmt::Write::write_fmt(&mut s, format_args!("{b:02x}"));
    }
    s.push('-');
    for b in ctx.span_id {
        let _ = std::fmt::Write::write_fmt(&mut s, format_args!("{b:02x}"));
    }
    s.push_str("-01");
    s
}

/// Create and enter a span for an HTTP handler, optionally parenting it on
/// an inbound `traceparent` header.
///
/// Returns a guarded span that — when dropped — ends the span. Callers keep
/// the guard alive for the duration of their handler body.
///
/// # Verified — F-G6-013 (positive verification)
///
/// The span carries exactly one attribute: `route`, a `&'static str`
/// constant chosen at the call site (e.g. `"/metrics"`, `"/admin/top"`).
/// No user-controlled input — txid, peer address, raw header value,
/// request body — is ever attached to this span. Operators deploying
/// OTLP exporters can rely on the fact that span attributes do not
/// leak request payloads. Future PRs that add dynamic span fields
/// here MUST re-audit (see also `metrics::tests` for the parallel
/// label-cardinality invariant).
pub(crate) fn http_span_for(headers: &HeaderMap, route: &'static str) -> tracing::Span {
    use opentelemetry::trace::TraceContextExt;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let span = tracing::debug_span!("http_request", route = route);
    if let Some(tp) = headers.get("traceparent").and_then(|v| v.to_str().ok())
        && let Some(wire) = parse_traceparent(tp)
        && let Some(sc) = wire.to_span_context()
    {
        let cx = opentelemetry::Context::new().with_remote_span_context(sc);
        let _ = span.set_parent(cx);
    }
    span
}

/// Convert a `tracing::Span` into a `traceparent` header value, if the
/// span has a valid OTel context. Returns `None` when tracing is disabled
/// or the span was not sampled.
pub(crate) fn traceparent_for_span(span: &tracing::Span) -> Option<String> {
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    let cx = span.context();
    let span_ref = opentelemetry::trace::TraceContextExt::span(&cx);
    let sc = span_ref.span_context();
    if !sc.is_valid() {
        return None;
    }
    let ctx = WireTraceContext {
        trace_id: sc.trace_id().to_bytes(),
        span_id: sc.span_id().to_bytes(),
    };
    Some(encode_traceparent(&ctx))
}

/// Attach a `traceparent` response header derived from the given span.
/// No-op when the span has no valid OTel context.
pub(crate) fn attach_traceparent_response(headers: &mut HeaderMap, span: &tracing::Span) {
    if let Some(v) = traceparent_for_span(span)
        && let Ok(header_val) = HeaderValue::from_str(&v)
    {
        headers.insert(HeaderName::from_static("traceparent"), header_val);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{ThreadHistograms, ThreadMetrics};

    /// `/metrics` output for the spend histogram must contain a complete set
    /// of cumulative `_bucket{le="..."}` lines, one per bucket plus `+Inf`,
    /// followed by `_sum` and `_count` lines. Cumulative bucket counts must
    /// be non-decreasing.
    #[test]
    fn metrics_endpoint_emits_histogram_buckets() {
        let m = ThreadMetrics::new();
        let h = ThreadHistograms::new();

        // Seed the histogram with known samples spanning multiple buckets.
        h.spend_latency.record_ns(100); // bucket 0 ([0, 128))
        h.spend_latency.record_ns(200); // bucket 1 ([128, 256))
        h.spend_latency.record_ns(1_000_000); // bucket 12 or nearby
        h.spend_latency.record_ns(1_000_000_000); // bucket 22 or nearby

        let text = render_metrics_text(&m, &h, 0, 0, 0, 0);

        // Every bucket boundary must appear exactly once.
        let num = LatencyHistogram::num_buckets();
        let mut last_count: u64 = 0;
        let mut saw_sum = false;
        let mut saw_count = false;
        let mut buckets_seen: usize = 0;
        for line in text.lines() {
            if line.starts_with("teraslab_spend_latency_ns_bucket{") {
                buckets_seen += 1;
                // The count is the token after the closing brace.
                let val: u64 = line
                    .rsplit(' ')
                    .next()
                    .and_then(|s| s.parse().ok())
                    .expect("bucket line ends with a count");
                assert!(
                    val >= last_count,
                    "cumulative bucket counts must be non-decreasing: {last_count} → {val} (line: {line})",
                );
                last_count = val;
            } else if line.starts_with("teraslab_spend_latency_ns_sum ") {
                saw_sum = true;
                let v: u64 = line.rsplit(' ').next().unwrap().parse().unwrap();
                assert_eq!(v, 100 + 200 + 1_000_000 + 1_000_000_000);
            } else if line.starts_with("teraslab_spend_latency_ns_count ") {
                saw_count = true;
                let v: u64 = line.rsplit(' ').next().unwrap().parse().unwrap();
                assert_eq!(v, 4);
            }
        }
        assert_eq!(buckets_seen, num, "one bucket line per bucket");
        assert!(saw_sum, "_sum line must be emitted");
        assert!(saw_count, "_count line must be emitted");
        // Final cumulative count must equal the total count.
        assert_eq!(last_count, 4);

        // The `+Inf` bucket must be present as the terminator.
        assert!(
            text.contains("teraslab_spend_latency_ns_bucket{le=\"+Inf\"}"),
            "expected +Inf terminator bucket, output:\n{text}",
        );

        // Verify that every counter-bearing histogram is also emitted.
        for name in [
            "teraslab_unspend_latency_ns",
            "teraslab_create_latency_ns",
            "teraslab_set_mined_latency_ns",
            "teraslab_freeze_latency_ns",
            "teraslab_unfreeze_latency_ns",
            "teraslab_delete_latency_ns",
            "teraslab_get_latency_ns",
            "teraslab_mark_longest_chain_latency_ns",
            "teraslab_reassign_latency_ns",
            "teraslab_set_conflicting_latency_ns",
            "teraslab_set_locked_latency_ns",
            "teraslab_preserve_until_latency_ns",
            "teraslab_lock_wait_ns",
        ] {
            assert!(
                text.contains(&format!("# TYPE {name} histogram")),
                "/metrics must declare {name} as a histogram; output:\n{text}",
            );
            assert!(
                text.contains(&format!("{name}_bucket{{le=\"+Inf\"}}")),
                "/metrics must emit +Inf bucket for {name}; output:\n{text}",
            );
            assert!(
                text.contains(&format!("{name}_sum ")),
                "/metrics must emit _sum for {name}; output:\n{text}",
            );
            assert!(
                text.contains(&format!("{name}_count ")),
                "/metrics must emit _count for {name}; output:\n{text}",
            );
        }
    }

    /// After incrementing spend_multi_items_succeeded by 10, the
    /// `teraslab_spend_multi_items_succeeded_total` line in the scraped
    /// `/metrics` output must rise by exactly 10.
    #[test]
    fn metrics_endpoint_counters_increment_after_operations() {
        let m = ThreadMetrics::new();
        let h = ThreadHistograms::new();

        // Scrape 1: baseline.
        let before = render_metrics_text(&m, &h, 0, 0, 0, 0);
        let before_val = find_counter(&before, "teraslab_spend_multi_items_succeeded_total");
        assert_eq!(before_val, 0, "fresh ThreadMetrics must start at zero");

        // Simulate a batch of 10 spends completing successfully.
        m.spend_multi_items_succeeded.inc_by(10);

        // Scrape 2: observe the delta.
        let after = render_metrics_text(&m, &h, 0, 0, 0, 0);
        let after_val = find_counter(&after, "teraslab_spend_multi_items_succeeded_total");
        assert_eq!(
            after_val - before_val,
            10,
            "spend_multi_items_succeeded_total should advance by exactly 10",
        );

        // Attempted and batches are independent counters — they should NOT
        // move just because we bumped succeeded.
        let att_val = find_counter(&after, "teraslab_spend_multi_items_attempted_total");
        assert_eq!(att_val, 0, "unrelated counter must remain at zero");
    }

    /// Extract the value of a Prometheus `counter` metric from the scrape
    /// output. Returns 0 if the metric line is missing.
    fn find_counter(text: &str, name: &str) -> u64 {
        for line in text.lines() {
            if let Some(after) = line.strip_prefix(name) {
                // Lines either look like "<name> <value>" or
                // "<name>{labels} <value>". We want the variant without labels.
                if let Some(rest) = after.strip_prefix(' ') {
                    return rest.parse().unwrap_or(0);
                }
            }
        }
        0
    }

    /// `render_metrics_text` must emit a Prometheus line for every
    /// `(op, outcome)` cell, with values matching the underlying counters.
    #[test]
    fn prometheus_emits_operations_total_with_labels() {
        let m = ThreadMetrics::new();
        let h = ThreadHistograms::new();

        // Seed a few cells with distinct values.
        m.operations.inc_by(OpCode::Spend, Outcome::Ok, 123);
        m.operations
            .inc_by(OpCode::Spend, Outcome::ErrConflicting, 4);
        m.operations.inc_by(OpCode::Create, Outcome::ErrStorage, 7);

        let text = render_metrics_text(&m, &h, 0, 0, 0, 0);

        // The counter type declaration must be present.
        assert!(
            text.contains("# TYPE teraslab_operations_total counter"),
            "missing TYPE declaration; output:\n{text}"
        );

        // Every (op, outcome) cell must appear exactly once.
        let mut lines_seen: usize = 0;
        for &op in OpCode::all() {
            for &outcome in Outcome::all() {
                let needle = format!(
                    "teraslab_operations_total{{op=\"{}\",outcome=\"{}\"}} ",
                    op.as_str(),
                    outcome.as_str(),
                );
                let count = text.lines().filter(|l| l.starts_with(&needle)).count();
                assert_eq!(
                    count, 1,
                    "expected exactly one Prometheus line for {needle}, got {count}"
                );
                lines_seen += 1;
            }
        }
        assert_eq!(
            lines_seen,
            OpCode::all().len() * Outcome::all().len(),
            "expected full {op_count}x{outcome_count} grid to be emitted",
            op_count = OpCode::all().len(),
            outcome_count = Outcome::all().len(),
        );

        // Verify the seeded values specifically.
        let spend_ok = find_labeled_counter(
            &text,
            "teraslab_operations_total",
            &[("op", "spend"), ("outcome", "ok")],
        );
        assert_eq!(spend_ok, 123);
        let spend_conflict = find_labeled_counter(
            &text,
            "teraslab_operations_total",
            &[("op", "spend"), ("outcome", "err_conflicting")],
        );
        assert_eq!(spend_conflict, 4);
        let create_storage = find_labeled_counter(
            &text,
            "teraslab_operations_total",
            &[("op", "create"), ("outcome", "err_storage")],
        );
        assert_eq!(create_storage, 7);
        // Untouched cell: zero.
        let delete_ok = find_labeled_counter(
            &text,
            "teraslab_operations_total",
            &[("op", "delete"), ("outcome", "ok")],
        );
        assert_eq!(delete_ok, 0);
    }

    /// `operations_json` must emit a nested object whose leaves match the
    /// counters table exactly. Consumers (admin UI, /ws/top) depend on this
    /// shape — zero cells are still emitted so the UI can render a stable
    /// table.
    #[test]
    fn admin_top_json_includes_operations_table() {
        let m = ThreadMetrics::new();
        m.operations.inc_by(OpCode::Spend, Outcome::Ok, 17);
        m.operations
            .inc_by(OpCode::Spend, Outcome::ErrConflicting, 3);
        m.operations.inc_by(OpCode::Delete, Outcome::ErrFrozen, 9);

        let js = operations_json(&m.operations);
        // Root is an object keyed by op label.
        let root = js.as_object().expect("root must be an object");
        assert_eq!(root.len(), OpCode::all().len(), "one key per opcode");

        // Spend/ok must equal 17.
        assert_eq!(root["spend"]["ok"].as_u64().expect("spend.ok is u64"), 17);
        // Spend/err_conflicting must equal 3.
        assert_eq!(
            root["spend"]["err_conflicting"]
                .as_u64()
                .expect("spend.err_conflicting is u64"),
            3
        );
        // Untouched cells must be 0, not missing.
        assert_eq!(
            root["spend"]["idempotent"].as_u64().unwrap(),
            0,
            "zero cells must be emitted, not omitted"
        );
        // Delete/err_frozen must equal 9.
        assert_eq!(root["delete"]["err_frozen"].as_u64().unwrap(), 9);

        // Every opcode must carry the full set of outcomes.
        for &op in OpCode::all() {
            let inner = root[op.as_str()]
                .as_object()
                .unwrap_or_else(|| panic!("op {} must map to an object", op.as_str()));
            assert_eq!(
                inner.len(),
                Outcome::all().len(),
                "op {} must carry all {} outcomes",
                op.as_str(),
                Outcome::all().len(),
            );
            for &outcome in Outcome::all() {
                let v = inner[outcome.as_str()].as_u64().unwrap_or_else(|| {
                    panic!("({}, {}) must be u64", op.as_str(), outcome.as_str())
                });
                let expected = m.operations.get(op, outcome);
                assert_eq!(
                    v,
                    expected,
                    "({}, {}) mismatch",
                    op.as_str(),
                    outcome.as_str()
                );
            }
        }
    }

    #[test]
    fn admin_top_fanout_documented() {
        let source = include_str!("http.rs");
        assert!(
            source.contains("ADMIN_TOP_REMOTE_FANOUT_LIMIT"),
            "remote /admin/top fan-out must keep an explicit concurrency cap"
        );
        assert!(
            source.contains("tokio::task::JoinSet::new()"),
            "cluster top snapshot should use a bounded task set, not one task per peer"
        );
        let forbidden = ["let mut handles", " = Vec::new();"].concat();
        assert!(
            !source.contains(&forbidden),
            "regression: unbounded remote task collection returned"
        );
    }

    #[test]
    fn websocket_top_send_has_backpressure_timeout() {
        let source = include_str!("http.rs");
        assert!(
            source.contains("WS_TOP_SEND_TIMEOUT"),
            "websocket top loop must keep an explicit send timeout",
        );
        assert!(
            source.contains("tokio::time::timeout(WS_TOP_SEND_TIMEOUT, socket.send(msg))"),
            "socket.send must be bounded so slow readers are dropped",
        );
    }

    /// The `/ws/top` push body is built via `build_local_top_snapshot` in
    /// single-node mode (and the cluster snapshot wraps the same payload in
    /// an envelope). Verify that the local snapshot JSON contains the
    /// `operations` table and that it matches the underlying counters.
    ///
    /// Driving a live WebSocket from a unit test requires spinning up a
    /// full HTTP listener, which the current test harness doesn't do. We
    /// instead assert the *shape* of the JSON that the loop pushes — the
    /// loop's data source is the helper this test exercises directly.
    #[test]
    fn admin_top_ws_push_includes_operations_table() {
        use crate::allocator::SlotAllocator;
        use crate::device::{BlockDevice, MemoryDevice};
        use crate::index::{DahIndex, Index, UnminedIndex};
        use crate::locks::StripedLocks;
        use crate::ops::engine::Engine;
        use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize};

        let dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone()).unwrap();
        let index = Index::new(1024).unwrap();
        let locks = StripedLocks::new(64);
        let engine = Arc::new(Engine::new(
            dev,
            index,
            alloc,
            locks,
            DahIndex::new(),
            UnminedIndex::new(),
        ));

        let metrics: &'static ThreadMetrics = Box::leak(Box::new(ThreadMetrics::new()));
        metrics.operations.inc_by(OpCode::Unspend, Outcome::Ok, 42);
        metrics
            .operations
            .inc_by(OpCode::Unspend, Outcome::Idempotent, 5);

        let histograms: &'static ThreadHistograms = Box::leak(Box::new(ThreadHistograms::new()));

        let state = HttpState {
            engine,
            metrics,
            histograms,
            ready: Arc::new(AtomicBool::new(true)),
            log_level: Arc::new(AtomicU8::new(LOG_LEVEL_INFO)),
            cluster: None,
            redo_log: None,
            active_connections: Arc::new(AtomicUsize::new(0)),
            http_port: 0,
            replica_lag_warn_threshold_ops: 10_000,
        };

        let snap = build_local_top_snapshot(&state);
        // The `operations` object must be present at the top level.
        let ops = snap["operations"]
            .as_object()
            .expect("top snapshot must include operations object");
        assert_eq!(ops.len(), OpCode::all().len());
        assert_eq!(snap["operations"]["unspend"]["ok"].as_u64().unwrap(), 42,);
        assert_eq!(
            snap["operations"]["unspend"]["idempotent"]
                .as_u64()
                .unwrap(),
            5,
        );
        // Untouched cell is zero, not missing.
        assert_eq!(
            snap["operations"]["unspend"]["err_storage"]
                .as_u64()
                .unwrap(),
            0,
        );
    }

    /// Parse a labeled Prometheus counter line of the form
    /// `name{k1="v1",k2="v2"} N` and return `N`. Panics if the line is
    /// missing or malformed.
    fn find_labeled_counter(text: &str, name: &str, labels: &[(&str, &str)]) -> u64 {
        let mut label_str = String::new();
        for (i, (k, v)) in labels.iter().enumerate() {
            if i > 0 {
                label_str.push(',');
            }
            label_str.push_str(k);
            label_str.push_str("=\"");
            label_str.push_str(v);
            label_str.push('"');
        }
        let prefix = format!("{name}{{{label_str}}} ");
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix(&prefix) {
                return rest.parse().expect("value must parse as u64");
            }
        }
        panic!("missing labeled Prometheus line: {prefix}\nin text:\n{text}")
    }

    /// Phase 5: all new metric series emitted by `render_metrics_text`
    /// must appear in the Prometheus text output at least once. This
    /// guards the production scrape surface against regressions.
    #[test]
    fn prometheus_emits_all_new_metrics() {
        use crate::metrics::{
            AllocatorMetrics, IoUringMetrics, MigrationMetrics, RedoMetrics, ReplicationMetrics,
            SwimMetrics, init_allocator_metrics, init_io_uring_metrics, init_migration_metrics,
            init_redo_metrics, init_replication_metrics, init_swim_metrics,
        };
        use std::sync::OnceLock;

        // Install metrics so `/metrics` emits the series (OnceLock makes this
        // a no-op if a prior test already installed them).
        static REPL: OnceLock<ReplicationMetrics> = OnceLock::new();
        static URING: OnceLock<IoUringMetrics> = OnceLock::new();
        static REDO: OnceLock<RedoMetrics> = OnceLock::new();
        static MIG: OnceLock<MigrationMetrics> = OnceLock::new();
        static SWIM: OnceLock<SwimMetrics> = OnceLock::new();
        static ALLOC: OnceLock<AllocatorMetrics> = OnceLock::new();
        init_replication_metrics(REPL.get_or_init(ReplicationMetrics::new));
        init_io_uring_metrics(URING.get_or_init(IoUringMetrics::new));
        init_redo_metrics(REDO.get_or_init(RedoMetrics::new));
        init_migration_metrics(MIG.get_or_init(MigrationMetrics::new));
        init_swim_metrics(SWIM.get_or_init(SwimMetrics::new));
        init_allocator_metrics(ALLOC.get_or_init(AllocatorMetrics::new));

        let m = ThreadMetrics::new();
        let h = ThreadHistograms::new();
        let text = render_metrics_text(&m, &h, 0, 0, 0, 0);

        // Scalar counter / gauge series.
        for name in [
            "teraslab_repl_batches_sent_total",
            "teraslab_repl_bytes_sent_total",
            "teraslab_repl_batches_acked_total",
            "teraslab_repl_batches_failed_total",
            "teraslab_repl_batch_latency_ns",
            "teraslab_repl_lag_sequences",
            "teraslab_uring_submit_latency_ns",
            "teraslab_uring_completion_latency_ns",
            "teraslab_uring_pending",
            "teraslab_uring_submit_errors_total",
            "teraslab_uring_completion_errors_total",
            "teraslab_redo_flush_latency_ns",
            "teraslab_redo_bytes_per_flush",
            "teraslab_redo_entries_per_flush",
            "teraslab_redo_append_total",
            "teraslab_redo_flush_errors_total",
            "teraslab_migration_bytes_transferred_total",
            "teraslab_migration_entries_applied_total",
            "teraslab_migration_active",
            "teraslab_migration_phase_preparing",
            "teraslab_migration_phase_copying",
            "teraslab_migration_phase_delta",
            "teraslab_migration_phase_serving_new",
            "teraslab_swim_probes_sent_total",
            "teraslab_swim_probe_timeouts_total",
            "teraslab_swim_indirect_probes_total",
            "teraslab_swim_suspicion_duration_ns",
            "teraslab_swim_membership_churn_total",
            "teraslab_alloc_total",
            "teraslab_alloc_bytes_total",
            "teraslab_free_total",
            "teraslab_free_bytes_total",
            "teraslab_freelist_region_count",
            "teraslab_freelist_largest_region_bytes",
        ] {
            assert!(
                text.contains(name),
                "/metrics output missing {name}\n--- output ---\n{text}",
            );
        }
    }

    /// Phase 5: `/admin/top` JSON must carry the new top-level
    /// sub-objects with the expected shape.
    #[test]
    fn admin_top_json_exposes_all_new_metric_shapes() {
        use crate::allocator::SlotAllocator;
        use crate::device::{BlockDevice, MemoryDevice};
        use crate::index::{DahIndex, Index, UnminedIndex};
        use crate::locks::StripedLocks;
        use crate::metrics::{
            AllocatorMetrics, IoUringMetrics, MigrationMetrics, RedoMetrics, ReplicationMetrics,
            SwimMetrics, init_allocator_metrics, init_io_uring_metrics, init_migration_metrics,
            init_redo_metrics, init_replication_metrics, init_swim_metrics,
        };
        use crate::ops::engine::Engine;
        use std::sync::OnceLock;
        use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize};

        static REPL: OnceLock<ReplicationMetrics> = OnceLock::new();
        static URING: OnceLock<IoUringMetrics> = OnceLock::new();
        static REDO: OnceLock<RedoMetrics> = OnceLock::new();
        static MIG: OnceLock<MigrationMetrics> = OnceLock::new();
        static SWIM: OnceLock<SwimMetrics> = OnceLock::new();
        static ALLOC: OnceLock<AllocatorMetrics> = OnceLock::new();
        init_replication_metrics(REPL.get_or_init(ReplicationMetrics::new));
        init_io_uring_metrics(URING.get_or_init(IoUringMetrics::new));
        init_redo_metrics(REDO.get_or_init(RedoMetrics::new));
        init_migration_metrics(MIG.get_or_init(MigrationMetrics::new));
        init_swim_metrics(SWIM.get_or_init(SwimMetrics::new));
        init_allocator_metrics(ALLOC.get_or_init(AllocatorMetrics::new));

        let dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone()).unwrap();
        let index = Index::new(1024).unwrap();
        let locks = StripedLocks::new(64);
        let engine = Arc::new(Engine::new(
            dev,
            index,
            alloc,
            locks,
            DahIndex::new(),
            UnminedIndex::new(),
        ));
        let metrics: &'static ThreadMetrics = Box::leak(Box::new(ThreadMetrics::new()));
        let histograms: &'static ThreadHistograms = Box::leak(Box::new(ThreadHistograms::new()));
        let state = HttpState {
            engine,
            metrics,
            histograms,
            ready: Arc::new(AtomicBool::new(true)),
            log_level: Arc::new(AtomicU8::new(LOG_LEVEL_INFO)),
            cluster: None,
            redo_log: None,
            active_connections: Arc::new(AtomicUsize::new(0)),
            http_port: 0,
            replica_lag_warn_threshold_ops: 10_000,
        };

        let snap = build_local_top_snapshot(&state);
        for field in [
            "replication_metrics",
            "uring_metrics",
            "redo_metrics",
            "migration_metrics",
            "swim_metrics",
            "allocator_metrics",
        ] {
            assert!(
                snap[field].is_object(),
                "/admin/top missing or non-object field: {field}\nsnapshot: {snap}"
            );
        }
        // Sub-field sanity checks on each section.
        assert!(snap["replication_metrics"]["per_replica"].is_array());
        assert!(snap["replication_metrics"]["latency"]["count"].is_u64());
        assert!(snap["uring_metrics"]["submit_latency"]["count"].is_u64());
        assert!(snap["uring_metrics"]["completion_errors"].is_object());
        assert!(snap["redo_metrics"]["flush_latency"]["count"].is_u64());
        assert!(snap["migration_metrics"]["bytes_transferred"].is_object());
        assert!(snap["migration_metrics"]["phase"]["preparing"].is_u64());
        assert!(snap["migration_metrics"]["migrations"].is_array());
        assert!(snap["swim_metrics"]["churn"].is_object());
        assert!(snap["swim_metrics"]["probes_sent"].is_u64());
        assert!(snap["allocator_metrics"]["alloc_total"].is_u64());
        assert!(snap["allocator_metrics"]["freelist_region_count"].is_u64());
    }

    /// Phase 5: the `/ws/top` push is built via `build_local_top_snapshot`
    /// (in single-node mode) or wrapped in a cluster envelope. Both shapes
    /// must include the new Phase 5 sections. Since driving a real
    /// WebSocket from unit tests would require an HTTP listener, we verify
    /// the exact JSON body that the loop pushes.
    #[test]
    fn ws_top_push_includes_new_metrics() {
        use crate::allocator::SlotAllocator;
        use crate::device::{BlockDevice, MemoryDevice};
        use crate::index::{DahIndex, Index, UnminedIndex};
        use crate::locks::StripedLocks;
        use crate::metrics::{
            AllocatorMetrics, IoUringMetrics, MigrationMetrics, RedoMetrics, ReplicationMetrics,
            SwimMetrics, init_allocator_metrics, init_io_uring_metrics, init_migration_metrics,
            init_redo_metrics, init_replication_metrics, init_swim_metrics,
        };
        use crate::ops::engine::Engine;
        use std::sync::OnceLock;
        use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize};

        static REPL: OnceLock<ReplicationMetrics> = OnceLock::new();
        static URING: OnceLock<IoUringMetrics> = OnceLock::new();
        static REDO: OnceLock<RedoMetrics> = OnceLock::new();
        static MIG: OnceLock<MigrationMetrics> = OnceLock::new();
        static SWIM: OnceLock<SwimMetrics> = OnceLock::new();
        static ALLOC: OnceLock<AllocatorMetrics> = OnceLock::new();
        init_replication_metrics(REPL.get_or_init(ReplicationMetrics::new));
        init_io_uring_metrics(URING.get_or_init(IoUringMetrics::new));
        init_redo_metrics(REDO.get_or_init(RedoMetrics::new));
        init_migration_metrics(MIG.get_or_init(MigrationMetrics::new));
        init_swim_metrics(SWIM.get_or_init(SwimMetrics::new));
        init_allocator_metrics(ALLOC.get_or_init(AllocatorMetrics::new));

        let dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone()).unwrap();
        let index = Index::new(1024).unwrap();
        let locks = StripedLocks::new(64);
        let engine = Arc::new(Engine::new(
            dev,
            index,
            alloc,
            locks,
            DahIndex::new(),
            UnminedIndex::new(),
        ));
        let metrics: &'static ThreadMetrics = Box::leak(Box::new(ThreadMetrics::new()));
        let histograms: &'static ThreadHistograms = Box::leak(Box::new(ThreadHistograms::new()));
        let state = HttpState {
            engine,
            metrics,
            histograms,
            ready: Arc::new(AtomicBool::new(true)),
            log_level: Arc::new(AtomicU8::new(LOG_LEVEL_INFO)),
            cluster: None,
            redo_log: None,
            active_connections: Arc::new(AtomicUsize::new(0)),
            http_port: 0,
            replica_lag_warn_threshold_ops: 10_000,
        };

        // The single-node `/ws/top` loop serializes this exact object as
        // its push body: {"aggregate": local, "nodes": [local]}.
        let local = build_local_top_snapshot(&state);
        let push = serde_json::json!({
            "aggregate": local,
            "nodes": [local],
        });
        let body = push.to_string();
        // Serialized body must contain every new top-level section key.
        for field in [
            "replication_metrics",
            "uring_metrics",
            "redo_metrics",
            "migration_metrics",
            "swim_metrics",
            "allocator_metrics",
        ] {
            assert!(
                body.contains(field),
                "ws_top push body missing field: {field}\nbody: {body}"
            );
        }
    }

    #[test]
    fn shard_counts_report_serving_master_during_handoff() {
        let old_members = vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
        let new_members = vec![NodeId(1), NodeId(2), NodeId(3)];
        let old_table = ShardTable::compute_with_epoch(&old_members, 2, 10);
        let new_table = ShardTable::compute_with_epoch(&new_members, 2, 11);
        let shard = (0..NUM_SHARDS as u16)
            .find(|&s| {
                old_table.target_assignment(s).master == NodeId(4)
                    && new_table.target_assignment(s).master != NodeId(4)
            })
            .expect("expected a shard whose master moves off node4");

        let mut handoff = old_table.clone();
        handoff.begin_handoff_with(&new_table, |s| s == shard);

        let (
            serving_master_count,
            _serving_replica_count,
            target_master_count,
            _target_replica_count,
            pending_handoffs,
        ) = shard_counts(&handoff, NodeId(4));

        assert_eq!(
            serving_master_count,
            target_master_count + 1,
            "the serving count must include masters that are still in Copying/CommitReady on the old owner"
        );
        assert_eq!(pending_handoffs, 1);
    }

    // -----------------------------------------------------------------
    // Phase 4 — HTTP trace context propagation
    // -----------------------------------------------------------------

    #[test]
    fn parse_traceparent_canonical_header() {
        let raw = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let ctx = parse_traceparent(raw).expect("valid traceparent parses");
        assert_eq!(ctx.trace_id[0], 0x4B);
        assert_eq!(ctx.trace_id[15], 0x36);
        assert_eq!(
            ctx.span_id,
            [0x00, 0xF0, 0x67, 0xAA, 0x0B, 0xA9, 0x02, 0xB7]
        );
    }

    #[test]
    fn parse_traceparent_malformed_returns_none() {
        assert!(parse_traceparent("not a traceparent").is_none());
        assert!(parse_traceparent("00-only-three-parts").is_none());
        assert!(
            parse_traceparent("01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01").is_none(),
            "future versions must not be accepted"
        );
        // Bad trace_id length
        assert!(parse_traceparent("00-short-00f067aa0ba902b7-01").is_none());
    }

    #[test]
    fn encode_traceparent_round_trip() {
        let ctx = WireTraceContext {
            trace_id: [0xABu8; 16],
            span_id: [0xCDu8; 8],
        };
        let s = encode_traceparent(&ctx);
        let parsed = parse_traceparent(&s).unwrap();
        assert_eq!(parsed, ctx);
        assert_eq!(s.len(), 55, "W3C traceparent is always 55 chars");
    }

    /// The `/metrics` handler honors an inbound `traceparent` header by
    /// creating its `http_request` span as a child of the provided
    /// context. We install a local OTel tracer, drive `http_span_for`,
    /// and assert the active span context carries the header's trace_id.
    #[test]
    fn http_metrics_endpoint_honors_incoming_traceparent() {
        use opentelemetry::trace::TracerProvider as _;
        use std::sync::{Arc, Mutex};
        use tracing_opentelemetry::OpenTelemetrySpanExt;
        use tracing_subscriber::prelude::*;

        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_sampler(opentelemetry_sdk::trace::Sampler::AlwaysOn)
            .build();
        let tracer = provider.tracer("teraslab-test");
        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
        let sub = tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new("debug"))
            .with(otel_layer);

        let incoming = "00-0102030405060708090a0b0c0d0e0f10-1112131415161718-01";
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("traceparent"),
            HeaderValue::from_static("00-0102030405060708090a0b0c0d0e0f10-1112131415161718-01"),
        );
        let expected_trace = parse_traceparent(incoming).unwrap().trace_id;

        let observed: Arc<Mutex<Option<[u8; 16]>>> = Arc::new(Mutex::new(None));
        let observed_clone = observed.clone();

        tracing::subscriber::with_default(sub, || {
            let span = http_span_for(&headers, "/metrics");
            let _g = span.enter();
            let cx = tracing::Span::current().context();
            let sp_ref = opentelemetry::trace::TraceContextExt::span(&cx);
            let sc = sp_ref.span_context();
            if sc.is_valid() {
                *observed_clone.lock().unwrap() = Some(sc.trace_id().to_bytes());
            }
        });

        assert_eq!(
            *observed.lock().unwrap(),
            Some(expected_trace),
            "/metrics span should be parented to the inbound traceparent",
        );
    }

    /// Build a minimal `HttpState` for readiness tests. Caller supplies
    /// the local `ready` flag and an optional cluster handle.
    fn build_ready_test_state(
        ready_flag: bool,
        cluster: Option<Arc<RunningCluster>>,
    ) -> Arc<HttpState> {
        use crate::allocator::SlotAllocator;
        use crate::device::{BlockDevice, MemoryDevice};
        use crate::index::{DahIndex, Index, UnminedIndex};
        use crate::locks::StripedLocks;
        use crate::ops::engine::Engine;

        let dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone()).unwrap();
        let index = Index::new(1024).unwrap();
        let locks = StripedLocks::new(64);
        let engine = Arc::new(Engine::new(
            dev,
            index,
            alloc,
            locks,
            DahIndex::new(),
            UnminedIndex::new(),
        ));
        let metrics: &'static ThreadMetrics = Box::leak(Box::new(ThreadMetrics::new()));
        let histograms: &'static ThreadHistograms = Box::leak(Box::new(ThreadHistograms::new()));
        Arc::new(HttpState {
            engine,
            metrics,
            histograms,
            ready: Arc::new(AtomicBool::new(ready_flag)),
            log_level: Arc::new(AtomicU8::new(LOG_LEVEL_INFO)),
            cluster,
            redo_log: None,
            active_connections: Arc::new(AtomicUsize::new(0)),
            http_port: 0,
            replica_lag_warn_threshold_ops: 0,
        })
    }

    /// R-055 baseline: in single-node mode (no cluster) with the
    /// boot-time `ready` flag set, `/health/ready` returns ready. This
    /// asserts the fix did not break the existing single-node contract.
    #[test]
    fn health_ready_returns_ready_in_single_node_mode() {
        let state = build_ready_test_state(true, None);
        assert_eq!(compute_health_ready(&state), ReadyState::Ready);
    }

    /// R-055 baseline: when the local `ready` flag is `false`, the
    /// readiness check still rejects regardless of cluster state.
    #[test]
    fn health_ready_rejects_when_local_ready_flag_false() {
        let state = build_ready_test_state(false, None);
        assert_eq!(
            compute_health_ready(&state),
            ReadyState::NotReady("not ready"),
        );
    }

    /// R-055 regression: in clustered mode, before the node has
    /// observed a committed topology, `/health/ready` must return
    /// SERVICE_UNAVAILABLE so a load balancer does not route traffic
    /// to a node that will reject every request with
    /// ERR_CLUSTER_NOT_READY. Pre-fix the handler returned 200 because
    /// `state.ready` was hard-coded `true` at boot.
    #[test]
    fn health_ready_rejects_when_cluster_has_no_committed_term() {
        use crate::cluster::coordinator::new_test_running_cluster;

        let table = ShardTable::compute(&[NodeId(1)], 1);
        // No committed_members → committed_term stays 0 → cluster_health
        // reports `Joining`, not `Alive`.
        let cluster = Arc::new(new_test_running_cluster(
            NodeId(1),
            table,
            &[],
            &[],
            &[],
            &[],
            &[],
            0,
        ));
        assert!(
            !cluster.cluster_health().is_ready(),
            "test setup precondition: cluster must report not-ready when no commit observed",
        );

        let state = build_ready_test_state(true, Some(cluster));
        assert_eq!(
            compute_health_ready(&state),
            ReadyState::NotReady("cluster not ready (no committed quorum yet)"),
        );
    }

    /// R-055 positive path: once the cluster has observed a committed
    /// topology, `/health/ready` returns ready.
    #[test]
    fn health_ready_returns_ready_once_cluster_committed() {
        use crate::cluster::coordinator::new_test_running_cluster;

        let table = ShardTable::compute(&[NodeId(1)], 1);
        let cluster = Arc::new(new_test_running_cluster(
            NodeId(1),
            table,
            &[],
            &[NodeId(1)],
            &[],
            &[],
            &[],
            0,
        ));
        assert!(
            cluster.cluster_health().is_ready(),
            "test setup precondition: cluster must report ready after a committed term",
        );

        let state = build_ready_test_state(true, Some(cluster));
        assert_eq!(compute_health_ready(&state), ReadyState::Ready);
    }

    #[test]
    fn health_ready_rejects_when_replica_lag_exceeds_threshold() {
        use crate::cluster::coordinator::new_test_running_cluster;
        use crate::metrics::{ReplicationMetrics, init_replication_metrics, replication_metrics};
        use std::sync::OnceLock;

        static REPL: OnceLock<ReplicationMetrics> = OnceLock::new();
        init_replication_metrics(REPL.get_or_init(ReplicationMetrics::new));
        let repl = replication_metrics().expect("replication metrics installed");
        repl.leader_sequence.store(100, Ordering::Relaxed);
        repl.per_replica[0]
            .last_acked_seq
            .store(10, Ordering::Relaxed);

        let table = ShardTable::compute(&[NodeId(1)], 1);
        let cluster = Arc::new(new_test_running_cluster(
            NodeId(1),
            table,
            &[],
            &[NodeId(1)],
            &[],
            &[],
            &[],
            0,
        ));
        let mut state = build_ready_test_state(true, Some(cluster));
        Arc::get_mut(&mut state)
            .expect("unique test state")
            .replica_lag_warn_threshold_ops = 50;

        assert_eq!(
            compute_health_ready(&state),
            ReadyState::NotReady("replica lag exceeds threshold"),
        );

        repl.leader_sequence.store(0, Ordering::Relaxed);
        repl.per_replica[0]
            .last_acked_seq
            .store(0, Ordering::Relaxed);
    }

    #[tokio::test]
    async fn drain_wait_helper_reports_completion_only_after_self_has_no_master_shards() {
        use crate::cluster::coordinator::new_test_running_cluster;

        let draining = Arc::new(new_test_running_cluster(
            NodeId(1),
            ShardTable::compute(&[NodeId(1)], 1),
            &[],
            &[NodeId(1)],
            &[],
            &[],
            &[],
            0,
        ));
        assert!(!wait_for_cluster_drain(&draining, 0).await);

        let drained = Arc::new(new_test_running_cluster(
            NodeId(1),
            ShardTable::compute(&[NodeId(2)], 1),
            &[],
            &[NodeId(2)],
            &[],
            &[],
            &[],
            0,
        ));
        assert!(wait_for_cluster_drain(&drained, 0).await);
    }
}
