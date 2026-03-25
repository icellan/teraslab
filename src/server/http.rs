//! HTTP observability server (metrics, health, debug, admin, WebSocket, Web UI).
//!
//! Runs on a separate port from the binary wire protocol. Uses `axum`
//! for routing. Does not block or slow the binary protocol path.

use crate::cluster::coordinator::RunningCluster;
use crate::cluster::shards::NUM_SHARDS;
use crate::index::TxKey;
use crate::metrics::{ThreadHistograms, ThreadMetrics};
use crate::ops::engine::Engine;
use crate::redo::RedoLog;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, put};
use axum::Router;
use rust_embed::Embed;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Log levels for the runtime log level endpoint.
const LOG_LEVEL_ERROR: u8 = 0;
const LOG_LEVEL_WARN: u8 = 1;
const LOG_LEVEL_INFO: u8 = 2;
const LOG_LEVEL_DEBUG: u8 = 3;
const LOG_LEVEL_TRACE: u8 = 4;

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
            // Metrics & health
            .route("/metrics", get(handle_metrics))
            .route("/health/live", get(handle_health_live))
            .route("/health/ready", get(handle_health_ready))
            .route("/status", get(handle_status))
            // Admin
            .route("/admin/quiesce", put(handle_admin_quiesce))
            .route("/admin/migration_status", get(handle_admin_migration_status))
            .route("/admin/nodes", get(handle_admin_nodes))
            .route("/admin/memory", get(handle_admin_memory))
            .route("/admin/records", get(handle_admin_records))
            .route("/admin/replication", get(handle_admin_replication))
            .route("/admin/rebalance", put(handle_admin_rebalance))
            .route("/admin/drain/{node_id}", put(handle_admin_drain))
            .route("/admin/top", get(handle_admin_top))
            // Debug
            .route("/debug/index", get(handle_debug_index))
            .route("/debug/freelist", get(handle_debug_freelist))
            .route("/debug/redo", get(handle_debug_redo))
            .route("/debug/log-level", put(handle_set_log_level))
            .route("/debug/log-level", get(handle_get_log_level))
            .route("/debug/records/{txid}", get(handle_debug_record))
            // WebSocket
            .route("/ws/top", get(handle_ws_top))
            // Web UI
            .route("/ui/", get(handle_ui_root))
            .route("/ui/{*path}", get(handle_ui))
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

    // Spend counters
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

    // New operation counters
    prom_counter(&mut out, "teraslab_creates_attempted_total", m.creates_attempted.get());
    prom_counter(&mut out, "teraslab_creates_succeeded_total", m.creates_succeeded.get());
    prom_counter(&mut out, "teraslab_set_mined_attempted_total", m.set_mined_attempted.get());
    prom_counter(&mut out, "teraslab_set_mined_succeeded_total", m.set_mined_succeeded.get());
    prom_counter(&mut out, "teraslab_gets_attempted_total", m.gets_attempted.get());
    prom_counter(&mut out, "teraslab_gets_succeeded_total", m.gets_succeeded.get());
    prom_counter(&mut out, "teraslab_freezes_attempted_total", m.freezes_attempted.get());
    prom_counter(&mut out, "teraslab_deletes_attempted_total", m.deletes_attempted.get());

    // Index gauges
    let index_entries = state.engine.index_len();
    prom_gauge(&mut out, "teraslab_index_entries", index_entries as u64);
    prom_gauge(&mut out, "teraslab_dah_index_entries", state.engine.dah_index().len() as u64);
    prom_gauge(&mut out, "teraslab_unmined_index_entries", state.engine.unmined_index().len() as u64);

    // Connection gauge
    prom_gauge(&mut out, "teraslab_active_connections", state.active_connections.load(Ordering::Relaxed) as u64);

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

    let cluster_info = if let Some(ref cluster) = state.cluster {
        let table = cluster.shard_table();
        let table_guard = table.read().unwrap();
        let self_id = cluster.self_id();
        let mut master_count: u32 = 0;
        let mut replica_count: u32 = 0;
        for shard in 0..NUM_SHARDS as u16 {
            // Use target_assignment to reflect the committed topology,
            // not the in-flight handoff state. This matches the partition
            // map and is_master which also use target_assignment.
            let assignment = table_guard.target_assignment(shard);
            if assignment.master == self_id {
                master_count += 1;
            }
            if assignment.replicas.contains(&self_id) {
                replica_count += 1;
            }
        }
        let cluster_size = cluster.alive_node_count();
        drop(table_guard);

        serde_json::json!({
            "node_id": self_id.0,
            "cluster_size": cluster_size,
            "shard_table_version": cluster.shard_table_version(),
            "topology_term": cluster.committed_topology_term(),
            "master_shard_count": master_count,
            "replica_shard_count": replica_count,
            "active_migrations": cluster.active_migrations(),
        })
    } else {
        serde_json::json!({
            "node_id": 0,
            "cluster_size": 1,
            "shard_table_version": 0,
            "master_shard_count": 0,
            "replica_shard_count": 0,
            "active_migrations": 0,
        })
    };

    let status = serde_json::json!({
        "node_id": cluster_info["node_id"],
        "cluster_size": cluster_info["cluster_size"],
        "shard_table_version": cluster_info["shard_table_version"],
        "master_shard_count": cluster_info["master_shard_count"],
        "replica_shard_count": cluster_info["replica_shard_count"],
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
            let fenced = cluster.fenced_shard_count();
            let active_count = migrations.iter().filter(|m| {
                m.state != crate::cluster::migration::MigrationState::Complete
                    && m.state != crate::cluster::migration::MigrationState::Failed
            }).count();
            let failed_count = migrations.iter().filter(|m| {
                m.state == crate::cluster::migration::MigrationState::Failed
            }).count();
            let body = serde_json::json!({
                "active_count": active_count,
                "failed_count": failed_count,
                "inbound_pending": inbound,
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
            (
                StatusCode::OK,
                body.to_string(),
            )
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
        let table_guard = table.read().unwrap();

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
            "topology_epoch": cluster.topology_epoch(),
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
) -> impl IntoResponse {
    match state.cluster {
        Some(ref cluster) => {
            if cluster.self_id().0 == node_id {
                cluster.quiesce();
                (StatusCode::OK, format!("drain initiated for node {node_id}"))
            } else {
                (StatusCode::BAD_REQUEST, format!(
                    "can only drain local node ({}), use --addr to target node {node_id} directly",
                    cluster.self_id().0
                ))
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

    let node_id = state.cluster.as_ref()
        .map(|c| c.self_id().0)
        .unwrap_or(0);

    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    let redo = if let Some(ref rl) = state.redo_log {
        let log = rl.lock();
        let avail = log.available_space();
        let pos = log.write_position();
        let total = pos + avail;
        let utilization = if total > 0 { pos as f64 / total as f64 } else { 0.0 };
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
    })
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

/// Build a cluster-wide top snapshot by fetching from all nodes in parallel.
///
/// Returns the local snapshot plus remote node snapshots, with an aggregate.
/// Remote nodes are queried with `?local=true` to prevent recursive fan-out.
/// If a remote node doesn't respond within 2 seconds, it is omitted.
async fn build_cluster_top_snapshot(state: &HttpState) -> serde_json::Value {
    let local = build_local_top_snapshot(state);
    let mut all_nodes = vec![local.clone()];

    // Fan out to remote nodes (if clustered)
    if let Some(ref cluster) = state.cluster {
        let self_id = cluster.self_id();
        let addrs = cluster.node_addresses();
        let http_port = state.http_port;

        let mut handles = Vec::new();
        for (&node_id, &addr) in &addrs {
            if node_id == self_id {
                continue; // Skip self — already have local snapshot
            }
            let url = format!("http://{}:{}/admin/top?local=true", addr.ip(), http_port);
            handles.push(tokio::spawn(async move {
                let client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(2))
                    .build()
                    .ok()?;
                let resp = client.get(&url).send().await.ok()?;
                if !resp.status().is_success() { return None; }
                resp.json::<serde_json::Value>().await.ok()
            }));
        }

        for handle in handles {
            if let Ok(Some(snapshot)) = handle.await {
                all_nodes.push(snapshot);
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
    let timestamp_ms = nodes.iter()
        .filter_map(|n| n["timestamp_ms"].as_u64())
        .max()
        .unwrap_or(0);

    let counter_keys = [
        "spends_attempted", "spends_succeeded", "spends_idempotent", "spends_failed",
        "unspends_attempted", "unspends_succeeded", "unspends_noop", "unspends_failed",
        "spend_multi_batches",
        "creates_attempted", "creates_succeeded",
        "set_mined_attempted", "set_mined_succeeded",
        "gets_attempted", "gets_succeeded",
        "freezes_attempted", "deletes_attempted",
    ];

    let mut counters = serde_json::Map::new();
    for key in &counter_keys {
        let sum: u64 = nodes.iter()
            .filter_map(|n| n["counters"][*key].as_u64())
            .sum();
        counters.insert(key.to_string(), serde_json::json!(sum));
    }

    // Latency: take the max of p99/p95, weighted mean for p50/mean
    let latency_keys = ["spend", "spend_multi", "unspend", "lock_wait"];
    let mut latency = serde_json::Map::new();
    for lk in &latency_keys {
        let total_count: u64 = nodes.iter()
            .filter_map(|n| n["latency"][*lk]["count"].as_u64())
            .sum();
        let weighted_mean: u64 = if total_count > 0 {
            let sum: u64 = nodes.iter()
                .map(|n| {
                    let c = n["latency"][*lk]["count"].as_u64().unwrap_or(0);
                    let m = n["latency"][*lk]["mean_ns"].as_u64().unwrap_or(0);
                    c * m
                })
                .sum();
            sum / total_count
        } else { 0 };
        let max_p50: u64 = nodes.iter()
            .filter_map(|n| n["latency"][*lk]["p50_ns"].as_u64())
            .max().unwrap_or(0);
        let max_p95: u64 = nodes.iter()
            .filter_map(|n| n["latency"][*lk]["p95_ns"].as_u64())
            .max().unwrap_or(0);
        let max_p99: u64 = nodes.iter()
            .filter_map(|n| n["latency"][*lk]["p99_ns"].as_u64())
            .max().unwrap_or(0);
        latency.insert(lk.to_string(), serde_json::json!({
            "count": total_count,
            "mean_ns": weighted_mean,
            "p50_ns": max_p50,
            "p95_ns": max_p95,
            "p99_ns": max_p99,
        }));
    }

    // Index: sum entries/capacity/memory, weighted avg load factor
    let index_entries: u64 = nodes.iter().filter_map(|n| n["index"]["entries"].as_u64()).sum();
    let index_capacity: u64 = nodes.iter().filter_map(|n| n["index"]["capacity"].as_u64()).sum();
    let index_memory: u64 = nodes.iter().filter_map(|n| n["index"]["memory_bytes"].as_u64()).sum();
    let index_lf = if index_capacity > 0 { index_entries as f64 / index_capacity as f64 } else { 0.0 };

    // Storage: sum used/total, compute aggregate utilization
    let storage_used: u64 = nodes.iter().filter_map(|n| n["storage"]["used_bytes"].as_u64()).sum();
    let storage_total: u64 = nodes.iter().filter_map(|n| n["storage"]["total_bytes"].as_u64()).sum();
    let storage_util = if storage_total > 0 { storage_used as f64 / storage_total as f64 } else { 0.0 };
    let storage_free_regions: u64 = nodes.iter().filter_map(|n| n["storage"]["free_regions"].as_u64()).sum();

    // Redo: sum
    let redo_seq: u64 = nodes.iter().filter_map(|n| n["redo"]["current_sequence"].as_u64()).sum();
    let redo_avail: u64 = nodes.iter().filter_map(|n| n["redo"]["available_space"].as_u64()).sum();
    let redo_pos: u64 = nodes.iter().filter_map(|n| n["redo"]["write_position"].as_u64()).sum();
    let redo_total = redo_pos + redo_avail;
    let redo_util = if redo_total > 0 { redo_pos as f64 / redo_total as f64 } else { 0.0 };

    let connections: u64 = nodes.iter().filter_map(|n| n["connections"].as_u64()).sum();
    let all_ready = nodes.iter().all(|n| n["ready"].as_bool().unwrap_or(false));

    serde_json::json!({
        "timestamp_ms": timestamp_ms,
        "node_count": nodes.len(),
        "counters": counters,
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
        if socket.send(msg).await.is_err() {
            break; // Client disconnected
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Drain any incoming messages (pings, close frames)
        while let Ok(Some(_)) = tokio::time::timeout(
            Duration::from_millis(1),
            socket.recv(),
        ).await {
            // Just consume; we don't process client messages
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
        let utilization = if total > 0 { pos as f64 / total as f64 } else { 0.0 };
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
fn serve_embedded_file(path: &str) -> (StatusCode, [(axum::http::HeaderName, String); 1], Vec<u8>) {
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
                None => return (
                    StatusCode::NOT_FOUND,
                    [(axum::http::header::CONTENT_TYPE, "text/plain".to_string())],
                    b"UI not found".to_vec(),
                ),
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
fn json_response(body: serde_json::Value) -> (StatusCode, [(axum::http::HeaderName, &'static str); 1], String) {
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        body.to_string(),
    )
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
