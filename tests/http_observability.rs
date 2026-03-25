//! HTTP observability endpoint integration tests.
//!
//! Starts the HTTP server on a random port and tests all endpoints.

use std::io::{Read, Write as IoWrite};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::metrics::{ThreadHistograms, ThreadMetrics};
use teraslab::ops::engine::Engine;
use teraslab::server::http::{start_http_server, HttpState};

static TEST_METRICS: ThreadMetrics = ThreadMetrics::new();
static TEST_HISTOGRAMS: ThreadHistograms = ThreadHistograms::new();

fn start_test_http_server() -> (u16, Arc<HttpState>) {
    let dev: Arc<dyn BlockDevice> =
        Arc::new(MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap());
    let alloc = SlotAllocator::new(dev.clone());
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
    });

    let addr = format!("127.0.0.1:{port}");
    let state_clone = state.clone();
    std::thread::spawn(move || {
        start_http_server(addr, state_clone);
    });

    // Wait for server to start
    std::thread::sleep(std::time::Duration::from_millis(200));

    (port, state)
}

/// Simple HTTP GET request over raw TCP.
fn http_get(port: u16, path: &str) -> (u16, String, String) {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();

    let req = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n");
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
    let body = response
        .split("\r\n\r\n")
        .nth(1)
        .unwrap_or("")
        .to_string();

    // Get content-type
    let content_type = response
        .lines()
        .find(|l| l.to_lowercase().starts_with("content-type:"))
        .map(|l| l.split_once(':').unwrap().1.trim().to_string())
        .unwrap_or_default();

    (status_code, content_type, body)
}

/// Simple HTTP PUT request over raw TCP.
fn http_put(port: u16, path: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();

    let req = format!(
        "PUT {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
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

    let resp_body = response
        .split("\r\n\r\n")
        .nth(1)
        .unwrap_or("")
        .to_string();

    (status_code, resp_body)
}

#[test]
fn metrics_returns_prometheus_text_format() {
    let (port, _state) = start_test_http_server();
    let (status, content_type, body) = http_get(port, "/metrics");

    assert_eq!(status, 200);
    assert!(content_type.contains("text/plain"), "expected text/plain, got {content_type}");
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

    // Set to debug
    let (status, _) = http_put(port, "/debug/log-level", "debug");
    assert_eq!(status, 200);
    assert_eq!(state.log_level.load(Ordering::Relaxed), 3); // DEBUG

    // Verify via GET
    let (status, _, body) = http_get(port, "/debug/log-level");
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
        t[0] = 0xAB; t[1] = 0xCD;
        t
    };
    let req = CreateRequest {
        tx_id: txid,
        tx_version: 2,
        locktime: 0,
        fee: 1000,
        size_in_bytes: 250,
        extended_size: 0,
        is_coinbase: false,
        spending_height: 0,
        utxo_hashes: vec![[1u8; 32]],
        inputs: None,
        outputs: None,
        inpoints: None,
        is_external: false,
        created_at: 1700000000000,
        block_height: 0,
        mined_block_infos: vec![],
        frozen: false,
        conflicting: false,
        locked: false,
        parent_txids: vec![],
    };
    state.engine.create(&req).unwrap();

    // Query via HTTP
    let txid_hex: String = txid.iter().map(|b| format!("{b:02x}")).collect();
    let (status, _, body) = http_get(port, &format!("/debug/records/{txid_hex}"));
    assert_eq!(status, 200);
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(parsed["tx_version"], 2);
    assert_eq!(parsed["utxo_count"], 1);
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
    assert!(elapsed.as_secs() < 5, "metrics scrapes took too long: {elapsed:?}");
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
    let (status, _, body) = http_get(port, "/debug/freelist");
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
    let (status, _, body) = http_get(port, "/debug/redo");
    assert_eq!(status, 200);
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();

    // No redo log configured in test → should report not available
    assert_eq!(parsed["available"], false);
}

#[test]
fn admin_nodes_returns_json_array() {
    let (port, _state) = start_test_http_server();
    let (status, content_type, body) = http_get(port, "/admin/nodes");
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
    let (status, _, body) = http_get(port, "/admin/memory");
    assert_eq!(status, 200);
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(parsed["index_bytes"].is_number());
    assert!(parsed["index_entries"].is_number());
}

#[test]
fn admin_records_returns_json() {
    let (port, _state) = start_test_http_server();
    let (status, _, body) = http_get(port, "/admin/records");
    assert_eq!(status, 200);
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(parsed["total_records"].is_number());
    assert!(parsed["dah_index_count"].is_number());
    assert!(parsed["unmined_count"].is_number());
}

#[test]
fn admin_replication_returns_json() {
    let (port, _state) = start_test_http_server();
    let (status, _, body) = http_get(port, "/admin/replication");
    assert_eq!(status, 200);
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
    // Single-node mode: replication disabled
    assert_eq!(parsed["enabled"], false);
}

#[test]
fn admin_top_returns_full_snapshot() {
    let (port, _state) = start_test_http_server();
    let (status, _, body) = http_get(port, "/admin/top");
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
    assert!(content_type.contains("text/html"), "expected text/html, got {content_type}");
    assert!(body.contains("TeraSlab"), "HTML should contain TeraSlab title");
    assert!(body.contains("<script"), "HTML should include script tag");
}

#[test]
fn ui_static_css_embedded() {
    let (port, _state) = start_test_http_server();
    let (status, content_type, body) = http_get(port, "/ui/style.css");
    assert_eq!(status, 200);
    assert!(content_type.contains("text/css"), "expected text/css, got {content_type}");
    assert!(body.contains("--bg:"), "CSS should contain custom properties");
}

#[test]
fn ui_static_js_embedded() {
    let (port, _state) = start_test_http_server();
    let (status, content_type, body) = http_get(port, "/ui/app.js");
    assert_eq!(status, 200);
    assert!(content_type.contains("javascript"), "expected javascript, got {content_type}");
    assert!(body.contains("TeraSlab"), "JS should contain TeraSlab references");
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
    let (status, _) = http_put(port, "/admin/rebalance", "");
    assert_eq!(status, 400);
}

#[test]
fn admin_drain_without_cluster_returns_error() {
    let (port, _state) = start_test_http_server();
    let (status, _) = http_put(port, "/admin/drain/1", "");
    assert_eq!(status, 400);
}

#[test]
fn debug_record_nonexistent_returns_404() {
    let (port, _state) = start_test_http_server();
    let txid_hex = "0000000000000000000000000000000000000000000000000000000000000000";
    let (status, _, body) = http_get(port, &format!("/debug/records/{txid_hex}"));
    assert_eq!(status, 404);
    assert!(body.contains("not found"));
}
