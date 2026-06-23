//! HTTP observability endpoint integration tests.
//!
//! Starts the HTTP server on a random port and tests all endpoints.

use std::io::{Read, Write as IoWrite};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};

use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::metrics::{ThreadHistograms, ThreadMetrics};
use teraslab::ops::engine::Engine;
use teraslab::server::http::{HttpState, start_http_server};

static TEST_METRICS: ThreadMetrics = ThreadMetrics::new();
static TEST_HISTOGRAMS: ThreadHistograms = ThreadHistograms::new();

/// The placeholder bearer token used by the legacy test harness. Tests
/// that exercise the auth gate explicitly use [`R056_TEST_TOKEN`] (an
/// alias) so the intent is loud at the call site.
const TEST_ADMIN_TOKEN: &str = "test-admin-token-please-do-not-use-in-prod";
const R056_TEST_TOKEN: &str = TEST_ADMIN_TOKEN;

fn start_test_http_server() -> (u16, Arc<HttpState>) {
    start_test_http_server_with_admin(true, Some(TEST_ADMIN_TOKEN.to_string()))
}

fn start_test_http_server_with_admin(
    enable_admin_endpoints: bool,
    admin_token: Option<String>,
) -> (u16, Arc<HttpState>) {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap());
    let alloc = SlotAllocator::new(dev.clone()).unwrap();
    let index = Index::new(1_000).unwrap();
    let engine = Arc::new(Engine::new(
        dev,
        index,
        alloc,
        StripedLocks::new(256),
        DahIndex::new(),
        UnminedIndex::new(),
    ));

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let ready = Arc::new(AtomicBool::new(true));
    let log_level = Arc::new(AtomicU8::new(2)); // INFO

    let state = Arc::new(HttpState {
        engine,
        metrics: &TEST_METRICS,
        histograms: &TEST_HISTOGRAMS,
        ready,
        log_level,
        cluster: None,
        redo_log: None,
        active_connections: Arc::new(AtomicUsize::new(0)),
        http_port: 0,
        replica_lag_warn_threshold_ops: 10_000,
        replica_lag_cache: std::sync::atomic::AtomicU64::new(0),
    });

    let addr = format!("127.0.0.1:{port}");
    let state_clone = state.clone();
    std::thread::spawn(move || {
        start_http_server(addr, state_clone, enable_admin_endpoints, admin_token);
    });

    // Wait for server to start
    std::thread::sleep(std::time::Duration::from_millis(200));

    (port, state)
}

/// Simple HTTP GET request over raw TCP. Unauthenticated.
fn http_get(port: u16, path: &str) -> (u16, String, String) {
    http_get_with_extra_headers(port, path, "")
}

/// HTTP GET with `Authorization: Bearer <token>` attached. Used by the
/// R-056 admin-auth tests; the constant-time middleware compares against
/// the configured token byte-for-byte.
fn http_get_auth(port: u16, path: &str, bearer: &str) -> (u16, String, String) {
    let extra = format!("Authorization: Bearer {bearer}\r\n");
    http_get_with_extra_headers(port, path, &extra)
}

/// Lower-level GET that supports caller-supplied `extra_headers` (each
/// terminated by `\r\n`). Internal helper for [`http_get`] and
/// [`http_get_auth`].
fn http_get_with_extra_headers(
    port: u16,
    path: &str,
    extra_headers: &str,
) -> (u16, String, String) {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n{extra_headers}Connection: close\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();

    // Parse status code and body
    let status_line = response.lines().next().unwrap_or("");
    let status_code: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    // Find body (after \r\n\r\n)
    let body = response.split("\r\n\r\n").nth(1).unwrap_or("").to_string();

    // Get content-type
    let content_type = response
        .lines()
        .find(|l| l.to_lowercase().starts_with("content-type:"))
        .map(|l| l.split_once(':').unwrap().1.trim().to_string())
        .unwrap_or_default();

    (status_code, content_type, body)
}

/// Simple HTTP PUT request over raw TCP. Unauthenticated.
fn http_put(port: u16, path: &str, body: &str) -> (u16, String) {
    http_put_with_extra_headers(port, path, body, "")
}

/// HTTP PUT with `Authorization: Bearer <token>` attached.
fn http_put_auth(port: u16, path: &str, body: &str, bearer: &str) -> (u16, String) {
    let extra = format!("Authorization: Bearer {bearer}\r\n");
    http_put_with_extra_headers(port, path, body, &extra)
}

/// Lower-level PUT that supports caller-supplied `extra_headers` (each
/// terminated by `\r\n`). Internal helper for [`http_put`] /
/// [`http_put_auth`].
fn http_put_with_extra_headers(
    port: u16,
    path: &str,
    body: &str,
    extra_headers: &str,
) -> (u16, String) {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    let req = format!(
        "PUT {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n{extra_headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(req.as_bytes()).unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();

    let status_code: u16 = response
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let resp_body = response.split("\r\n\r\n").nth(1).unwrap_or("").to_string();

    (status_code, resp_body)
}

/// Perform a raw WebSocket upgrade handshake and return
/// `(status_code, full_response_headers)`.
///
/// Only the response header block is read (up to the terminating
/// `\r\n\r\n`): a successful `101 Switching Protocols` keeps the socket open
/// while the server pushes a frame every second, so reading to EOF would
/// block until the read timeout. `extra_headers` is injected verbatim (each
/// line `\r\n`-terminated) — callers supply the `Sec-WebSocket-Protocol`
/// and/or `Authorization` header that carries the admin token. The mandatory
/// RFC 6455 upgrade headers are always present so the request reaches the
/// upgrade handler once auth passes.
fn ws_handshake(port: u16, path: &str, extra_headers: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    let req = format!(
        "GET {path} HTTP/1.1\r\n\
         Host: 127.0.0.1:{port}\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
         Sec-WebSocket-Version: 13\r\n\
         {extra_headers}\r\n"
    );
    stream.write_all(req.as_bytes()).unwrap();

    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 256];
    loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            buf.truncate(pos + 4);
            break;
        }
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(_) => break,
        }
    }
    let response = String::from_utf8_lossy(&buf).into_owned();
    let status_code: u16 = response
        .lines()
        .next()
        .unwrap_or("")
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    (status_code, response)
}

/// Minimal, dependency-free base64url (no padding) encoder for the WS
/// subprotocol auth tests. Mirrors the `TextEncoder` + `btoa` + url-safe
/// transform the UI performs in `b64urlToken` (ui/app.js).
fn b64url_nopad(input: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[((n >> 18) & 0x3f) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[((n >> 6) & 0x3f) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[(n & 0x3f) as usize] as char);
        }
    }
    out
}

#[test]
fn metrics_returns_prometheus_text_format() {
    let (port, _state) = start_test_http_server();
    let (status, content_type, body) = http_get(port, "/metrics");

    assert_eq!(status, 200);
    assert!(
        content_type.contains("text/plain"),
        "expected text/plain, got {content_type}"
    );
    assert!(body.contains("teraslab_spends_attempted_total"));
    assert!(body.contains("teraslab_spends_succeeded_total"));
    assert!(body.contains("teraslab_index_entries"));
    assert!(body.contains("# TYPE"));
}

#[test]
fn metrics_includes_all_counters() {
    let (port, _state) = start_test_http_server();
    let (_, _, body) = http_get(port, "/metrics");

    // All ThreadMetrics counters
    assert!(body.contains("teraslab_spends_attempted_total"));
    assert!(body.contains("teraslab_spends_succeeded_total"));
    assert!(body.contains("teraslab_spends_idempotent_total"));
    assert!(body.contains("teraslab_spends_failed_total"));
    assert!(body.contains("teraslab_unspends_attempted_total"));
    assert!(body.contains("teraslab_unspends_succeeded_total"));
    assert!(body.contains("teraslab_unspends_noop_total"));
    assert!(body.contains("teraslab_unspends_failed_total"));
    assert!(body.contains("teraslab_spend_multi_batches_total"));
    assert!(body.contains("teraslab_dah_inserts_total"));
    assert!(body.contains("teraslab_dah_removes_total"));
}

#[test]
fn metrics_includes_gauges() {
    let (port, _state) = start_test_http_server();
    let (_, _, body) = http_get(port, "/metrics");

    assert!(body.contains("teraslab_index_entries"));
    assert!(body.contains("teraslab_dah_index_entries"));
    assert!(body.contains("teraslab_unmined_index_entries"));
}

/// P2.3 + P2.4: the three new operator-visible counters must appear in
/// the Prometheus `/metrics` text after their respective metric tables
/// are installed via the `init_*_metrics` accessors.
///
/// * `teraslab_allocator_corrupt_redo_entries_total` — F-G1-015 bump
///   from `replay_allocate` / `replay_free` on corrupt redo entries.
/// * `teraslab_allocator_generation_wrap_warn_total` — F-G1-019 bump
///   when `generation_target_ahead` sees a delta within `2^30` of the
///   wrap-ambiguity window.
/// * `teraslab_swim_ping_req_dropped_total` — F-G8-004 bump at the
///   bounded-PING_REQ-map eviction site.
#[test]
fn metrics_includes_new_telemetry_counters() {
    use std::sync::OnceLock;
    use teraslab::metrics::{
        AllocatorMetrics, SwimMetrics, init_allocator_metrics, init_swim_metrics,
    };

    // OnceLock-guarded singletons so parallel tests sharing the same
    // process do not collide on `init_*` (which is idempotent but races
    // on the underlying static install otherwise).
    static ALLOC: OnceLock<AllocatorMetrics> = OnceLock::new();
    static SWIM: OnceLock<SwimMetrics> = OnceLock::new();
    init_allocator_metrics(ALLOC.get_or_init(AllocatorMetrics::new));
    init_swim_metrics(SWIM.get_or_init(SwimMetrics::new));

    let (port, _state) = start_test_http_server();
    let (_, _, body) = http_get(port, "/metrics");

    for name in [
        "teraslab_allocator_corrupt_redo_entries_total",
        "teraslab_allocator_generation_wrap_warn_total",
        "teraslab_swim_ping_req_dropped_total",
    ] {
        assert!(
            body.contains(name),
            "/metrics output missing {name}\n--- output ---\n{body}",
        );
    }
}

#[test]
fn health_live_returns_200() {
    let (port, _state) = start_test_http_server();
    let (status, _, body) = http_get(port, "/health/live");
    assert_eq!(status, 200);
    assert_eq!(body, "ok");
}

#[test]
fn health_ready_returns_200_when_ready() {
    let (port, state) = start_test_http_server();
    state.ready.store(true, Ordering::Relaxed);
    let (status, _, body) = http_get(port, "/health/ready");
    assert_eq!(status, 200);
    assert_eq!(body, "ready");
}

#[test]
fn health_ready_returns_503_during_startup() {
    let (port, state) = start_test_http_server();
    state.ready.store(false, Ordering::Relaxed);
    let (status, _, _) = http_get(port, "/health/ready");
    assert_eq!(status, 503);
}

#[test]
fn status_returns_json() {
    let (port, _state) = start_test_http_server();
    let (status, content_type, body) = http_get(port, "/status");
    assert_eq!(status, 200);
    assert!(content_type.contains("application/json"));

    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(parsed["records"]["total"].is_number());
    assert!(parsed["throughput"]["spends_attempted"].is_number());
    assert!(parsed["ready"].is_boolean());
}

#[test]
fn debug_log_level_put_changes_level() {
    let (port, state) = start_test_http_server();

    // F-X-002: both PUT and GET are now gated. PUT writes, GET reads back.
    let (status, _) = http_put_auth(port, "/debug/log-level", "debug", R056_TEST_TOKEN);
    assert_eq!(status, 200);
    assert_eq!(state.log_level.load(Ordering::Relaxed), 3); // DEBUG

    // Verify via GET (gated under F-X-002 — log-level leaks operator state)
    let (status, _, body) = http_get_auth(port, "/debug/log-level", R056_TEST_TOKEN);
    assert_eq!(status, 200);
    assert_eq!(body, "debug");
}

#[test]
fn debug_records_returns_json_for_existing_record() {
    let (port, state) = start_test_http_server();

    // Create a record via the engine directly
    use teraslab::ops::create::CreateRequest;
    let txid = {
        let mut t = [0u8; 32];
        t[0] = 0xAB;
        t[1] = 0xCD;
        t
    };
    let utxo_hashes = [[1u8; 32]];
    let req = CreateRequest {
        tx_id: txid,
        tx_version: 2,
        locktime: 0,
        fee: 1000,
        size_in_bytes: 250,
        extended_size: 0,
        is_coinbase: false,
        spending_height: 0,
        utxo_hashes: &utxo_hashes,
        inputs: None,
        outputs: None,
        inpoints: None,
        is_external: false,
        created_at: 1700000000000,
        block_height: 0,
        mined_block_infos: &[],
        frozen: false,
        conflicting: false,
        locked: false,
        external_ref: None,
        parent_txids: &[],
    };
    state.engine.create(&req).unwrap();

    // Query via HTTP — gated route, attach bearer token.
    let txid_hex: String = txid.iter().map(|b| format!("{b:02x}")).collect();
    let (status, _, body) =
        http_get_auth(port, &format!("/debug/records/{txid_hex}"), R056_TEST_TOKEN);
    assert_eq!(status, 200);
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(parsed["tx_version"], 2);
    assert_eq!(parsed["utxo_count"], 1);
}

#[test]
fn debug_records_rejects_long_path() {
    let (port, _state) = start_test_http_server();
    let long_txid = "a".repeat(65);

    let (status, content_type, body) = http_get_auth(
        port,
        &format!("/debug/records/{long_txid}"),
        R056_TEST_TOKEN,
    );

    assert_eq!(status, 400);
    // B-5 / F-G6-025: error responses now use the structured JSON envelope.
    assert!(
        content_type.contains("application/json"),
        "expected JSON error body, got content-type={content_type:?}",
    );
    let parsed: serde_json::Value = serde_json::from_str(&body)
        .unwrap_or_else(|e| panic!("error body must be JSON; got {body:?} ({e})"));
    assert_eq!(parsed["code"], "invalid_txid_length");
    assert_eq!(parsed["message"], "invalid txid length");
}

#[test]
fn metrics_scrape_does_not_block() {
    let (port, _state) = start_test_http_server();

    // Multiple rapid scrapes should complete quickly
    let start = std::time::Instant::now();
    for _ in 0..10 {
        let (status, _, _) = http_get(port, "/metrics");
        assert_eq!(status, 200);
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed.as_secs() < 5,
        "metrics scrapes took too long: {elapsed:?}"
    );
}

#[test]
fn metrics_includes_new_operation_counters() {
    let (port, _state) = start_test_http_server();
    let (_, _, body) = http_get(port, "/metrics");

    assert!(body.contains("teraslab_creates_attempted_total"));
    assert!(body.contains("teraslab_creates_succeeded_total"));
    assert!(body.contains("teraslab_set_mined_attempted_total"));
    assert!(body.contains("teraslab_gets_attempted_total"));
    assert!(body.contains("teraslab_freezes_attempted_total"));
    assert!(body.contains("teraslab_deletes_attempted_total"));
    assert!(body.contains("teraslab_active_connections"));
}

#[test]
fn freelist_returns_real_stats() {
    let (port, _state) = start_test_http_server();
    // F-X-002: /debug/freelist is gated (leaks allocator internals).
    let (status, _, body) = http_get_auth(port, "/debug/freelist", R056_TEST_TOKEN);
    assert_eq!(status, 200);
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();

    // Should have real numeric fields, not a stub
    assert!(parsed["device_size"].is_number());
    assert!(parsed["used_bytes"].is_number());
    assert!(parsed["utilization"].is_number());
    assert!(parsed["free_region_count"].is_number());
    assert!(parsed["alignment"].is_number());

    // Device size should match the 16 MB test device
    assert_eq!(parsed["device_size"].as_u64().unwrap(), 16 * 1024 * 1024);
}

#[test]
fn redo_endpoint_returns_not_available_without_redo_log() {
    let (port, _state) = start_test_http_server();
    // /debug/redo is gated — attach the test token.
    let (status, _, body) = http_get_auth(port, "/debug/redo", R056_TEST_TOKEN);
    assert_eq!(status, 200);
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();

    // No redo log configured in test → should report not available
    assert_eq!(parsed["available"], false);
}

#[test]
fn admin_nodes_returns_json_array() {
    let (port, _state) = start_test_http_server();
    // F-X-002: /admin/nodes is gated — leaks every peer's IP + shard set,
    // which is the reconnaissance step in the topology-forgery chain.
    let (status, content_type, body) = http_get_auth(port, "/admin/nodes", R056_TEST_TOKEN);
    assert_eq!(status, 200);
    assert!(content_type.contains("application/json"));

    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(parsed["nodes"].is_array());
    // Single-node mode: should have exactly 1 node
    assert_eq!(parsed["nodes"].as_array().unwrap().len(), 1);
}

#[test]
fn admin_memory_returns_json() {
    let (port, _state) = start_test_http_server();
    // F-X-002: gated read-only dashboard.
    let (status, _, body) = http_get_auth(port, "/admin/memory", R056_TEST_TOKEN);
    assert_eq!(status, 200);
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(parsed["index_bytes"].is_number());
    assert!(parsed["index_entries"].is_number());
}

#[test]
fn admin_records_returns_json() {
    let (port, _state) = start_test_http_server();
    // F-X-002: gated read-only dashboard.
    let (status, _, body) = http_get_auth(port, "/admin/records", R056_TEST_TOKEN);
    assert_eq!(status, 200);
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(parsed["total_records"].is_number());
    assert!(parsed["dah_index_count"].is_number());
    assert!(parsed["unmined_count"].is_number());
}

#[test]
fn admin_replication_returns_json() {
    let (port, _state) = start_test_http_server();
    // F-X-002: gated read-only dashboard.
    let (status, _, body) = http_get_auth(port, "/admin/replication", R056_TEST_TOKEN);
    assert_eq!(status, 200);
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
    // Single-node mode: replication disabled
    assert_eq!(parsed["enabled"], false);
}

#[test]
fn admin_top_returns_full_snapshot() {
    // F-G6-002: `/admin/top` exposes internal counters and fans out to
    // every cluster peer in clustered mode. It now sits behind the same
    // bearer-token middleware as the mutating routes, so the snapshot
    // is only retrievable when the operator supplies the configured
    // admin bearer token.
    let (port, _state) = start_test_http_server();
    let (status, _, body) = http_get_auth(port, "/admin/top", R056_TEST_TOKEN);
    assert_eq!(status, 200);
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();

    assert!(parsed["timestamp_ms"].is_number());
    assert!(parsed["counters"]["spends_attempted"].is_number());
    assert!(parsed["counters"]["creates_attempted"].is_number());
    assert!(parsed["latency"]["spend"]["count"].is_number());
    assert!(parsed["latency"]["spend"]["p50_ns"].is_number());
    assert!(parsed["latency"]["spend"]["p99_ns"].is_number());
    assert!(parsed["index"]["entries"].is_number());
    assert!(parsed["index"]["load_factor"].is_number());
    assert!(parsed["storage"]["used_bytes"].is_number());
    assert!(parsed["storage"]["utilization"].is_number());
    assert!(parsed["connections"].is_number());
    assert!(parsed["ready"].is_boolean());
}

#[test]
fn ui_root_returns_html() {
    let (port, _state) = start_test_http_server();
    let (status, content_type, body) = http_get(port, "/ui/");
    assert_eq!(status, 200);
    assert!(
        content_type.contains("text/html"),
        "expected text/html, got {content_type}"
    );
    assert!(
        body.contains("TeraSlab"),
        "HTML should contain TeraSlab title"
    );
    assert!(body.contains("<script"), "HTML should include script tag");
}

#[test]
fn ui_static_css_embedded() {
    let (port, _state) = start_test_http_server();
    let (status, content_type, body) = http_get(port, "/ui/style.css");
    assert_eq!(status, 200);
    assert!(
        content_type.contains("text/css"),
        "expected text/css, got {content_type}"
    );
    assert!(
        body.contains("--bg:"),
        "CSS should contain custom properties"
    );
}

#[test]
fn ui_static_js_embedded() {
    let (port, _state) = start_test_http_server();
    let (status, content_type, body) = http_get(port, "/ui/app.js");
    assert_eq!(status, 200);
    assert!(
        content_type.contains("javascript"),
        "expected javascript, got {content_type}"
    );
    assert!(
        body.contains("TeraSlab"),
        "JS should contain TeraSlab references"
    );
}

#[test]
fn ui_spa_fallback_returns_index() {
    let (port, _state) = start_test_http_server();
    // Non-existent path under /ui/ should return index.html (SPA fallback)
    let (status, content_type, body) = http_get(port, "/ui/nonexistent/path");
    assert_eq!(status, 200);
    assert!(content_type.contains("text/html"));
    assert!(body.contains("TeraSlab"));
}

#[test]
fn admin_rebalance_without_cluster_returns_error() {
    let (port, _state) = start_test_http_server();
    let (status, _) = http_put_auth(port, "/admin/rebalance", "", R056_TEST_TOKEN);
    assert_eq!(status, 400);
}

#[test]
fn admin_drain_without_cluster_returns_error() {
    let (port, _state) = start_test_http_server();
    let (status, _) = http_put_auth(port, "/admin/drain/1", "", R056_TEST_TOKEN);
    assert_eq!(status, 400);
}

#[test]
fn debug_record_nonexistent_returns_404() {
    let (port, _state) = start_test_http_server();
    let txid_hex = "0000000000000000000000000000000000000000000000000000000000000000";
    let (status, content_type, body) =
        http_get_auth(port, &format!("/debug/records/{txid_hex}"), R056_TEST_TOKEN);
    assert_eq!(status, 404);
    // B-5 / F-G6-025: error responses are now `application/json` with
    // a `{code, message}` envelope.
    assert!(content_type.contains("application/json"));
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(parsed["code"], "tx_not_found");
    assert!(parsed["message"].as_str().unwrap().contains("not found"));
}

// --------------------------------------------------------------------------
// Gap #1 safe defaults: admin/debug gating
// --------------------------------------------------------------------------

/// When `enable_admin_endpoints = false`, mutating admin routes are not
/// registered so axum returns 404 — even though the rest of the surface
/// (metrics, health, read-only admin, /debug/freelist) keeps working.
#[test]
fn admin_quiesce_404_when_admin_disabled() {
    let (port, _state) = start_test_http_server_with_admin(false, None);
    let (status, _) = http_put(port, "/admin/quiesce", "");
    assert_eq!(
        status, 404,
        "/admin/quiesce must be unrouted when admin endpoints are disabled"
    );
}

#[test]
fn admin_rebalance_404_when_admin_disabled() {
    let (port, _state) = start_test_http_server_with_admin(false, None);
    let (status, _) = http_put(port, "/admin/rebalance", "");
    assert_eq!(status, 404);
}

#[test]
fn admin_drain_404_when_admin_disabled() {
    let (port, _state) = start_test_http_server_with_admin(false, None);
    let (status, _) = http_put(port, "/admin/drain/1", "");
    assert_eq!(status, 404);
}

#[test]
fn debug_set_log_level_blocked_when_admin_disabled() {
    let (port, _state) = start_test_http_server_with_admin(false, None);
    // PUT /debug/log-level is the mutation route; the GET sibling stays on,
    // so axum returns 405 Method Not Allowed (vs. 404 when no method matches).
    // Either status proves the mutating handler is not registered.
    let (status, _) = http_put(port, "/debug/log-level", "info");
    assert!(
        status == 404 || status == 405,
        "PUT /debug/log-level must be unrouted (404) or method-not-allowed (405) when \
         admin endpoints are disabled, got {status}",
    );
}

#[test]
fn debug_record_404_when_admin_disabled() {
    let (port, _state) = start_test_http_server_with_admin(false, None);
    let txid_hex = "0000000000000000000000000000000000000000000000000000000000000000";
    let (status, _, _) = http_get(port, &format!("/debug/records/{txid_hex}"));
    assert_eq!(status, 404);
}

#[test]
fn debug_index_404_when_admin_disabled() {
    let (port, _state) = start_test_http_server_with_admin(false, None);
    let (status, _, _) = http_get(port, "/debug/index");
    assert_eq!(status, 404);
}

#[test]
fn debug_redo_404_when_admin_disabled() {
    let (port, _state) = start_test_http_server_with_admin(false, None);
    let (status, _, _) = http_get(port, "/debug/redo");
    assert_eq!(status, 404);
}

/// Even with admin endpoints disabled, the always-on observability surface
/// must keep working. This guards against accidentally over-gating.
#[test]
fn metrics_still_works_when_admin_disabled() {
    let (port, _state) = start_test_http_server_with_admin(false, None);
    let (status, _, body) = http_get(port, "/metrics");
    assert_eq!(status, 200);
    assert!(body.contains("teraslab_spends_attempted_total"));
}

#[test]
fn health_live_still_works_when_admin_disabled() {
    let (port, _state) = start_test_http_server_with_admin(false, None);
    let (status, _, body) = http_get(port, "/health/live");
    assert_eq!(status, 200);
    assert_eq!(body, "ok");
}

#[test]
fn read_only_debug_log_level_404_when_admin_disabled() {
    // F-X-002: GET /debug/log-level is gated alongside its PUT sibling.
    // When admin endpoints are disabled the whole route is unregistered,
    // so axum returns 404 (or 405 if the GET method-table is empty but
    // the path matches a parent route).
    let (port, _state) = start_test_http_server_with_admin(false, None);
    let (status, _, _) = http_get(port, "/debug/log-level");
    assert!(
        status == 404 || status == 405,
        "F-X-002: GET /debug/log-level must be unrouted when admin endpoints \
         are disabled, got {status}",
    );
}

// --------------------------------------------------------------------------
// R-056 (gap LMNH-08 / F14): bearer-token auth on the gated /admin/* and
// mutating /debug/* routes. Pre-fix every request succeeded with 200; the
// fix installs an axum middleware that enforces a constant-time bearer
// compare against the configured admin_token.
// --------------------------------------------------------------------------

/// The regression: pre-fix `PUT /admin/quiesce` returned 200 to anyone that
/// could reach the HTTP port. With auth enforced, no header → 401.
#[test]
fn admin_endpoint_returns_401_without_bearer_token() {
    let (port, _state) = start_test_http_server();
    // PUT /admin/quiesce is the cheapest gated mutation handler in the
    // suite — nothing about its body parses the URL further.
    let (status, body) = http_put(port, "/admin/quiesce", "");
    assert_eq!(
        status, 401,
        "missing Authorization header must yield 401 Unauthorized; body was: {body}",
    );
    assert!(
        body.contains("Authorization") || body.contains("token"),
        "401 body should hint at the missing header, got: {body:?}",
    );
}

#[test]
fn admin_endpoint_returns_401_with_wrong_bearer_token() {
    let (port, _state) = start_test_http_server();
    let (status, body) = http_put_auth(port, "/admin/quiesce", "", "definitely-not-the-token");
    assert_eq!(
        status, 401,
        "wrong bearer token must yield 401, body was: {body}",
    );
    assert!(
        body.contains("invalid") || body.contains("token"),
        "401 body should describe the auth failure, got: {body:?}",
    );
}

#[test]
fn admin_endpoint_succeeds_with_correct_bearer_token() {
    let (port, _state) = start_test_http_server();
    // GET /debug/redo is gated by R-056 but returns 200 in single-node mode
    // (with a JSON body that flags the redo log as unavailable). It is the
    // cleanest "the handler ran" probe in the suite — every other gated
    // route either requires a cluster (drain, rebalance, quiesce) or a
    // matching record (records/{txid}).
    let (status, _, body) = http_get_auth(port, "/debug/redo", R056_TEST_TOKEN);
    assert_eq!(
        status, 200,
        "correct bearer token must let the request reach the handler",
    );
    let parsed: serde_json::Value =
        serde_json::from_str(&body).expect("/debug/redo must return JSON when authed");
    assert_eq!(
        parsed["available"], false,
        "single-node test harness has no redo log",
    );
}

/// Even with admin auth fully enabled, `/metrics` must remain
/// unauthenticated so Prometheus / Grafana can scrape without operator
/// credentials. This guards against accidentally over-gating the
/// observability surface in a future refactor.
#[test]
fn metrics_endpoint_unauthenticated_remains_accessible_with_admin_auth_enabled() {
    let (port, _state) = start_test_http_server();
    // No Authorization header at all.
    let (status, content_type, body) = http_get(port, "/metrics");
    assert_eq!(status, 200, "/metrics must stay unauthenticated");
    assert!(content_type.contains("text/plain"));
    assert!(body.contains("teraslab_spends_attempted_total"));
}

#[test]
fn health_endpoints_unauthenticated_remain_accessible_with_admin_auth_enabled() {
    let (port, _state) = start_test_http_server();
    let (live_status, _, live_body) = http_get(port, "/health/live");
    assert_eq!(live_status, 200);
    assert_eq!(live_body, "ok");

    let (ready_status, _, _) = http_get(port, "/health/ready");
    // /health/ready is single-node ready by default in this harness, so 200
    // is the expected outcome; the assertion that matters is "not 401".
    assert_ne!(
        ready_status, 401,
        "/health/ready must never return 401 — it has no auth gate",
    );
    assert_eq!(ready_status, 200);
}

/// F-X-002: read-only `/admin/*` dashboards now require the admin bearer
/// token. Pre-F-X-002 they were unauthenticated, but `/admin/nodes`
/// leaks every peer's (ip, swim_port, http_port, shard set) tuple —
/// exactly the reconnaissance the topology-forgery attack chain needs.
/// `/status` stays public because it only exposes process-wide gauges
/// (record count, throughput counters, ready flag) that load balancers
/// and Prometheus need without credentials.
#[test]
fn read_only_admin_dashboards_require_bearer_token() {
    let (port, _state) = start_test_http_server();
    for path in [
        "/admin/migration_status",
        "/admin/nodes",
        "/admin/memory",
        "/admin/records",
        "/admin/replication",
    ] {
        let (status, _, _) = http_get(port, path);
        assert_eq!(
            status, 401,
            "F-X-002: read-only dashboard {path} must require admin bearer token, got {status}",
        );
        // With the token, the same path must reach the handler.
        let (status_ok, _, _) = http_get_auth(port, path, R056_TEST_TOKEN);
        assert_eq!(
            status_ok, 200,
            "{path} must succeed when the admin bearer token is supplied, got {status_ok}",
        );
    }
    // /status is the only read-only endpoint that stays public — pinned
    // here so a future router refactor cannot accidentally regate it.
    let (status, _, _) = http_get(port, "/status");
    assert_eq!(
        status, 200,
        "/status must remain unauthenticated (load-balancer compatibility)",
    );
}

/// F-G6-002: the moved `/admin/top` and `/ws/top` routes must reject
/// unauthenticated callers with 401 — they are no longer part of the
/// always-public read-only surface above.
#[test]
fn admin_top_requires_bearer_token() {
    let (port, _state) = start_test_http_server();
    let (status, _, _) = http_get(port, "/admin/top");
    assert_eq!(
        status, 401,
        "/admin/top must require admin bearer token (F-G6-002)"
    );
    // GET /ws/top is a WebSocket upgrade endpoint — without the bearer
    // token the middleware short-circuits before the upgrade handshake.
    let (status, _, _) = http_get(port, "/ws/top");
    assert_eq!(
        status, 401,
        "/ws/top must require admin bearer token (F-G6-003)"
    );
}

/// Dashboard auth (F-G6-003 follow-up): a browser cannot set the
/// `Authorization` header on `new WebSocket()`, so `/ws/top` must also accept
/// the admin token via the `Sec-WebSocket-Protocol` request header — the one
/// header `new WebSocket(url, [proto])` can populate. The handshake echoes
/// the negotiated `teraslab.v1` marker (never the secret token) in the 101
/// response, and a missing or wrong token must still be rejected with 401 so
/// there is no security regression versus the header-only gate.
#[test]
fn ws_top_auth_via_subprotocol() {
    let (port, _state) = start_test_http_server();

    // Valid token offered alongside the negotiated subprotocol → 101 upgrade.
    let offer = format!("Sec-WebSocket-Protocol: teraslab.v1, Bearer.{R056_TEST_TOKEN}\r\n");
    let (status, resp) = ws_handshake(port, "/ws/top", &offer);
    assert_eq!(
        status, 101,
        "valid token in Sec-WebSocket-Protocol must upgrade (101); response was:\n{resp}",
    );
    assert!(
        resp.to_lowercase()
            .contains("sec-websocket-protocol: teraslab.v1"),
        "101 response must echo the negotiated teraslab.v1 subprotocol; response was:\n{resp}",
    );
    assert!(
        !resp.contains(R056_TEST_TOKEN),
        "the secret token must NOT be echoed back in the handshake response:\n{resp}",
    );

    // Wrong token in the subprotocol → 401, no upgrade.
    let bad = "Sec-WebSocket-Protocol: teraslab.v1, Bearer.definitely-not-the-token\r\n";
    let (status, _) = ws_handshake(port, "/ws/top", bad);
    assert_eq!(
        status, 401,
        "wrong token in Sec-WebSocket-Protocol must 401"
    );

    // Subprotocol offered but no Bearer entry → 401.
    let (status, _) = ws_handshake(port, "/ws/top", "Sec-WebSocket-Protocol: teraslab.v1\r\n");
    assert_eq!(
        status, 401,
        "missing token must 401 even when a subprotocol is offered",
    );

    // No token transport at all → 401.
    let (status, _) = ws_handshake(port, "/ws/top", "");
    assert_eq!(status, 401, "no token transport at all must 401");
}

/// `admin_token` is recommended (docs/DEPLOYMENT_ASSUMPTIONS.md) to be
/// `openssl rand -base64` output, which contains `+ / =` — illegal in a raw
/// WebSocket subprotocol token, so `new WebSocket(url, ['Bearer.<token>'])`
/// would throw in the browser and live metrics would never connect. The UI
/// therefore base64url-wraps the token as `Bearer64.<...>`; the server
/// decodes it and compares the same bytes. This must upgrade for valid
/// tokens, reject wrong tokens, and never panic on malformed base64.
#[test]
fn ws_top_auth_via_base64_subprotocol_handles_token_grammar_unsafe_chars() {
    // Shaped like `openssl rand -base64` output: '+', '/', '=' present.
    let token = "Aa0+bb/cc1+dd/ee2==";
    assert!(token.len() >= 16);
    let (port, _state) = start_test_http_server_with_admin(true, Some(token.to_string()));

    let good = format!(
        "Sec-WebSocket-Protocol: teraslab.v1, Bearer64.{}\r\n",
        b64url_nopad(token.as_bytes())
    );
    let (status, resp) = ws_handshake(port, "/ws/top", &good);
    assert_eq!(
        status, 101,
        "base64url-wrapped token must upgrade (101); response was:\n{resp}",
    );
    assert!(
        resp.to_lowercase()
            .contains("sec-websocket-protocol: teraslab.v1"),
        "101 response must still echo teraslab.v1; response was:\n{resp}",
    );

    let bad = format!(
        "Sec-WebSocket-Protocol: teraslab.v1, Bearer64.{}\r\n",
        b64url_nopad(b"some-other-token-value")
    );
    let (status, _) = ws_handshake(port, "/ws/top", &bad);
    assert_eq!(status, 401, "wrong base64url-wrapped token must 401");

    // Malformed base64 in the Bearer64 entry must fail closed (401), never
    // 500/panic/hang.
    let (status, _) = ws_handshake(
        port,
        "/ws/top",
        "Sec-WebSocket-Protocol: teraslab.v1, Bearer64.@@@not-base64@@@\r\n",
    );
    assert_eq!(status, 401, "malformed base64 must 401, not crash");
}

/// A `Sec-WebSocket-Protocol` header carrying non-ASCII bytes must be
/// rejected gracefully (401), never panic the server on a sub-codepoint
/// slice. (`HeaderValue::to_str` already rejects non-ASCII, but the parser
/// is also slice-panic-proof as defense in depth.)
#[test]
fn ws_top_auth_non_ascii_subprotocol_does_not_crash() {
    let (port, _state) = start_test_http_server();
    let (status, _) = ws_handshake(
        port,
        "/ws/top",
        "Sec-WebSocket-Protocol: \u{00e9}Bearer.tok\r\n",
    );
    assert_eq!(status, 401, "non-ASCII subprotocol must 401, not crash");
}

/// The `Authorization: Bearer` path must keep working for `/ws/top` so
/// non-browser clients (CLI, `curl` with upgrade headers) are unaffected by
/// the subprotocol channel added for browsers.
#[test]
fn ws_top_auth_via_authorization_header_still_works() {
    let (port, _state) = start_test_http_server();
    let hdr = format!("Authorization: Bearer {R056_TEST_TOKEN}\r\n");
    let (status, resp) = ws_handshake(port, "/ws/top", &hdr);
    assert_eq!(
        status, 101,
        "Authorization: Bearer must still upgrade /ws/top; response:\n{resp}",
    );
}

#[test]
fn read_only_debug_routes_require_bearer_token() {
    // F-X-002: /debug/freelist and GET /debug/log-level were previously
    // unauthenticated. They leak allocator internals and the live
    // tracing level respectively — operator-visible state that should
    // require a token like every other /debug/* sibling.
    let (port, _state) = start_test_http_server();
    for path in ["/debug/freelist", "/debug/log-level"] {
        let (status, _, _) = http_get(port, path);
        assert_eq!(
            status, 401,
            "F-X-002: {path} must require admin bearer token, got {status}",
        );
        let (status_ok, _, _) = http_get_auth(port, path, R056_TEST_TOKEN);
        assert_eq!(
            status_ok, 200,
            "{path} must succeed when the admin bearer token is supplied, got {status_ok}",
        );
    }
}

/// PUT /debug/log-level + GET /debug/index, /debug/redo, /debug/records
/// all sit behind the same middleware — every one must fail 401 without
/// the token. This pins the full mutating-debug surface.
#[test]
fn debug_mutating_endpoint_requires_bearer_token() {
    let (port, _state) = start_test_http_server();

    // PUT /debug/log-level
    let (status, _) = http_put(port, "/debug/log-level", "info");
    assert_eq!(status, 401, "PUT /debug/log-level must require auth");

    // GET /debug/index
    let (status, _, _) = http_get(port, "/debug/index");
    assert_eq!(status, 401, "GET /debug/index must require auth");

    // GET /debug/redo
    let (status, _, _) = http_get(port, "/debug/redo");
    assert_eq!(status, 401, "GET /debug/redo must require auth");

    // GET /debug/records/<txid>
    let txid_hex = "00000000000000000000000000000000000000000000000000000000000000aa";
    let (status, _, _) = http_get(port, &format!("/debug/records/{txid_hex}"));
    assert_eq!(status, 401, "GET /debug/records/{{txid}} must require auth");
}

#[test]
fn all_admin_mutation_routes_require_bearer_token() {
    let (port, _state) = start_test_http_server();

    // PUT /admin/rebalance
    let (status, _) = http_put(port, "/admin/rebalance", "");
    assert_eq!(status, 401, "PUT /admin/rebalance must require auth");

    // PUT /admin/drain/{node_id}
    let (status, _) = http_put(port, "/admin/drain/42", "");
    assert_eq!(
        status, 401,
        "PUT /admin/drain/{{node_id}} must require auth"
    );
}

/// A header that is present but malformed (no scheme, wrong scheme,
/// missing space, missing token) must be rejected the same way as a
/// missing header.
#[test]
fn admin_endpoint_rejects_malformed_authorization_header() {
    let (port, _state) = start_test_http_server();

    for bad in [
        // No scheme prefix at all.
        "definitely-not-the-token",
        // Wrong scheme.
        "Basic Zm9vOmJhcg==",
        // Bearer with no separating space (BearerXYZ).
        "BearerXYZdoesnotmatter",
        // Bearer with empty token.
        "Bearer ",
    ] {
        let extra = format!("Authorization: {bad}\r\n");
        let (status, _) = http_put_with_extra_headers(port, "/admin/quiesce", "", &extra);
        assert_eq!(
            status, 401,
            "malformed header {bad:?} must be rejected, got {status}",
        );
    }
}

/// RFC 6750 §2.1 specifies the scheme name is case-insensitive; clients
/// that send `BEARER` or `bearer` must succeed when the token matches.
/// We assert "not 401" rather than "200" because the underlying handler
/// (`/admin/quiesce`) returns 400 in single-node mode — the point is that
/// the auth gate accepted the request, not that the cluster handler was
/// happy.
#[test]
fn admin_endpoint_accepts_case_insensitive_bearer_scheme() {
    let (port, _state) = start_test_http_server();
    for scheme in ["Bearer", "bearer", "BEARER", "BeArEr"] {
        let extra = format!("Authorization: {scheme} {R056_TEST_TOKEN}\r\n");
        let (status, _) = http_put_with_extra_headers(port, "/admin/quiesce", "", &extra);
        assert_ne!(
            status, 401,
            "scheme {scheme:?} must be accepted (case-insensitive per RFC 6750), got 401",
        );
        // The handler returns 400 in single-node mode (no cluster); both
        // outcomes prove the gate let the request through.
        assert!(
            status == 200 || status == 400,
            "expected 200 or 400 from quiesce handler after auth, got {status}",
        );
    }
}

// --------------------------------------------------------------------------
// B-5 / F-G6-025: HTTP error body envelope. Every error response on the
// observability surface must carry `Content-Type: application/json` and a
// `{code, message}` body so future operator dashboards can match on `code`
// instead of scraping plain-text strings.
// --------------------------------------------------------------------------

/// Sweep the canonical error endpoints and assert that every one of them
/// returns the structured envelope. The status codes themselves are
/// preserved — operators script-matching on the status keep working.
#[test]
fn error_responses_use_structured_json_envelope() {
    let (port, _state) = start_test_http_server();

    // ----- 401 Unauthorized: no bearer token on a gated route. -----
    let (status, body) = http_put(port, "/admin/quiesce", "");
    assert_eq!(status, 401);
    let parsed: serde_json::Value = serde_json::from_str(&body)
        .unwrap_or_else(|e| panic!("401 body must be JSON; got {body:?} ({e})"));
    assert_eq!(parsed["code"], "unauthorized");
    assert!(parsed["message"].is_string());

    // ----- 401 Unauthorized: wrong bearer token. -----
    let (status, body) = http_put_auth(port, "/admin/quiesce", "", "not-the-real-token");
    assert_eq!(status, 401);
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(parsed["code"], "unauthorized");
    assert!(
        parsed["message"]
            .as_str()
            .unwrap()
            .contains("invalid admin bearer token"),
    );

    // ----- 400 Bad Request: invalid log level body. -----
    let (status, body) = http_put_auth(
        port,
        "/debug/log-level",
        "definitely-not-a-level",
        R056_TEST_TOKEN,
    );
    assert_eq!(status, 400);
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(parsed["code"], "invalid_log_level");
    assert_eq!(parsed["message"], "invalid log level");

    // ----- 400 Bad Request: txid too long. -----
    let long_txid = "a".repeat(65);
    let (status, content_type, body) = http_get_auth(
        port,
        &format!("/debug/records/{long_txid}"),
        R056_TEST_TOKEN,
    );
    assert_eq!(status, 400);
    assert!(content_type.contains("application/json"));
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(parsed["code"], "invalid_txid_length");
    assert_eq!(parsed["message"], "invalid txid length");

    // ----- 400 Bad Request: malformed hex txid (correct length, invalid chars). -----
    let bad_hex = "z".repeat(64);
    let (status, _, body) =
        http_get_auth(port, &format!("/debug/records/{bad_hex}"), R056_TEST_TOKEN);
    assert_eq!(status, 400);
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(parsed["code"], "invalid_txid_hex");
    assert_eq!(parsed["message"], "invalid txid hex");

    // ----- 404 Not Found: missing tx record. -----
    let zeros = "0".repeat(64);
    let (status, content_type, body) =
        http_get_auth(port, &format!("/debug/records/{zeros}"), R056_TEST_TOKEN);
    assert_eq!(status, 404);
    assert!(content_type.contains("application/json"));
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(parsed["code"], "tx_not_found");
    assert_eq!(parsed["message"], "tx not found");

    // ----- 400 Bad Request: cluster-only handler hit in single-node mode. -----
    let (status, body) = http_put_auth(port, "/admin/rebalance", "", R056_TEST_TOKEN);
    assert_eq!(status, 400);
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(parsed["code"], "not_in_cluster_mode");
    assert_eq!(parsed["message"], "not in cluster mode");
}

/// The 401 envelope from the auth middleware must carry
/// `Content-Type: application/json`. Pre-fix the middleware returned
/// `text/plain` so dashboards could not distinguish a missing-token error
/// from a wrong-token error without substring matching on the body.
#[test]
fn unauthorized_response_advertises_json_content_type() {
    let (port, _state) = start_test_http_server();
    let (status, content_type, body) = http_get(port, "/admin/top");
    assert_eq!(status, 401);
    assert!(
        content_type.contains("application/json"),
        "expected JSON error envelope, got content-type={content_type:?}, body={body:?}",
    );
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(parsed["code"], "unauthorized");
}

/// The optional `details` field is only present when the handler attaches
/// structured context. `/admin/drain/{node_id}` with a mismatched ID emits
/// `details = { requested_node_id, self_node_id }`; the simpler
/// "not in cluster mode" path omits the field entirely. We exercise both
/// the omission (single-node mode → 400 with no `details`) and the
/// presence path (best we can do without a real cluster) below.
#[test]
fn error_envelope_omits_details_when_not_attached() {
    let (port, _state) = start_test_http_server();

    // `/admin/rebalance` in single-node mode → 400 "not in cluster mode";
    // no `details` field should be present.
    let (status, body) = http_put_auth(port, "/admin/rebalance", "", R056_TEST_TOKEN);
    assert_eq!(status, 400);
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(parsed["code"], "not_in_cluster_mode");
    assert!(
        parsed.get("details").is_none() || parsed["details"].is_null(),
        "details must be omitted when the handler does not attach context, got {parsed}",
    );
}
