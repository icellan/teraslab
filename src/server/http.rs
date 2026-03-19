//! HTTP observability server (metrics, health, debug endpoints).
//!
//! Runs on a separate port from the binary wire protocol. Uses `axum`
//! for routing. Does not block or slow the binary protocol path.

use crate::index::TxKey;
use crate::metrics::ThreadMetrics;
use crate::ops::engine::Engine;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, put};
use axum::Router;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;

/// Log levels for the runtime log level endpoint.
const LOG_LEVEL_ERROR: u8 = 0;
const LOG_LEVEL_WARN: u8 = 1;
const LOG_LEVEL_INFO: u8 = 2;
const LOG_LEVEL_DEBUG: u8 = 3;
const LOG_LEVEL_TRACE: u8 = 4;

/// Shared state for the HTTP server.
pub struct HttpState {
    /// Reference to the engine for data queries.
    pub engine: Arc<Engine>,
    /// Global metrics counters.
    pub metrics: &'static ThreadMetrics,
    /// Whether the index has been fully loaded (ready check).
    pub ready: Arc<AtomicBool>,
    /// Runtime log level.
    pub log_level: Arc<AtomicU8>,
}

/// Start the HTTP observability server on the given address.
///
/// This spawns a tokio runtime and blocks until shutdown.
/// Call this from a dedicated thread.
pub fn start_http_server(
    bind_addr: String,
    state: Arc<HttpState>,
) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime for HTTP server");

    rt.block_on(async move {
        let app = Router::new()
            .route("/metrics", get(handle_metrics))
            .route("/health/live", get(handle_health_live))
            .route("/health/ready", get(handle_health_ready))
            .route("/status", get(handle_status))
            .route("/debug/index", get(handle_debug_index))
            .route("/debug/freelist", get(handle_debug_freelist))
            .route("/debug/redo", get(handle_debug_redo))
            .route("/debug/log-level", put(handle_set_log_level))
            .route("/debug/log-level", get(handle_get_log_level))
            .route("/debug/records/{txid}", get(handle_debug_record))
            .with_state(state);

        let listener = match tokio::net::TcpListener::bind(&bind_addr).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("HTTP server failed to bind {bind_addr}: {e}");
                return;
            }
        };

        eprintln!("HTTP observability server listening on {bind_addr}");

        if let Err(e) = axum::serve(listener, app).await {
            eprintln!("HTTP server error: {e}");
        }
    });
}

// ---------------------------------------------------------------------------
// /metrics — Prometheus text format
// ---------------------------------------------------------------------------

async fn handle_metrics(State(state): State<Arc<HttpState>>) -> impl IntoResponse {
    let m = state.metrics;
    let mut out = String::with_capacity(4096);

    // Counters
    prom_counter(&mut out, "teraslab_spends_attempted_total", m.spends_attempted.get());
    prom_counter(&mut out, "teraslab_spends_succeeded_total", m.spends_succeeded.get());
    prom_counter(&mut out, "teraslab_spends_idempotent_total", m.spends_idempotent.get());
    prom_counter(&mut out, "teraslab_spends_failed_total", m.spends_failed.get());
    prom_counter(&mut out, "teraslab_unspends_attempted_total", m.unspends_attempted.get());
    prom_counter(&mut out, "teraslab_unspends_succeeded_total", m.unspends_succeeded.get());
    prom_counter(&mut out, "teraslab_unspends_noop_total", m.unspends_noop.get());
    prom_counter(&mut out, "teraslab_unspends_failed_total", m.unspends_failed.get());
    prom_counter(&mut out, "teraslab_spend_multi_batches_total", m.spend_multi_batches.get());
    prom_counter(&mut out, "teraslab_dah_inserts_total", m.dah_inserts.get());
    prom_counter(&mut out, "teraslab_dah_removes_total", m.dah_removes.get());

    // Index gauges
    let index_entries = state.engine.index_len();
    prom_gauge(&mut out, "teraslab_index_entries", index_entries as u64);
    prom_gauge(&mut out, "teraslab_dah_index_entries", state.engine.dah_index().len() as u64);
    prom_gauge(&mut out, "teraslab_unmined_index_entries", state.engine.unmined_index().len() as u64);

    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        out,
    )
}

fn prom_counter(out: &mut String, name: &str, val: u64) {
    use std::fmt::Write;
    let _ = writeln!(out, "# TYPE {name} counter");
    let _ = writeln!(out, "{name} {val}");
}

fn prom_gauge(out: &mut String, name: &str, val: u64) {
    use std::fmt::Write;
    let _ = writeln!(out, "# TYPE {name} gauge");
    let _ = writeln!(out, "{name} {val}");
}

// ---------------------------------------------------------------------------
// /health/live and /health/ready
// ---------------------------------------------------------------------------

async fn handle_health_live(State(_state): State<Arc<HttpState>>) -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn handle_health_ready(State(state): State<Arc<HttpState>>) -> impl IntoResponse {
    if state.ready.load(Ordering::Relaxed) {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready")
    }
}

// ---------------------------------------------------------------------------
// /status — cluster health overview JSON
// ---------------------------------------------------------------------------

async fn handle_status(State(state): State<Arc<HttpState>>) -> impl IntoResponse {
    let m = state.metrics;
    let status = serde_json::json!({
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

async fn handle_debug_freelist(State(_state): State<Arc<HttpState>>) -> impl IntoResponse {
    let body = serde_json::json!({
        "info": "freelist debug endpoint",
    });
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        body.to_string(),
    )
}

async fn handle_debug_redo(State(_state): State<Arc<HttpState>>) -> impl IntoResponse {
    let body = serde_json::json!({
        "info": "redo log debug endpoint",
    });
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        body.to_string(),
    )
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

fn parse_hex_txid(hex: &str) -> Option<[u8; 32]> {
    if hex.len() != 64 {
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
