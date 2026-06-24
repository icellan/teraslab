//! TeraSlab admin CLI.
//!
//! Provides operator commands that consume the HTTP observability endpoints
//! and binary wire protocol. Supports both table-formatted and JSON output.

use clap::{Parser, Subcommand};
use comfy_table::{Attribute, Cell, Color, Table};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::ExitCode;
use std::time::{Duration, Instant};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors from CLI operations.
#[derive(Error, Debug)]
enum CliError {
    /// HTTP request failed.
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    /// JSON parse error.
    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),

    /// TCP connection error.
    #[error("connection error: {0}")]
    Connection(#[from] std::io::Error),

    /// Server returned an error.
    #[error("server error ({status}): {message}")]
    ServerError { status: u16, message: String },

    /// Index backend / migration error (offline export-index / import-index).
    #[error("index error: {0}")]
    Index(#[from] teraslab::index::IndexError),

    /// Generic error.
    #[error("{0}")]
    Other(String),
}

// ---------------------------------------------------------------------------
// CLI argument parsing
// ---------------------------------------------------------------------------

/// TeraSlab admin command-line tool.
#[derive(Parser)]
#[command(name = "teraslab-cli", about = "TeraSlab admin CLI", version)]
struct Cli {
    /// HTTP address of the TeraSlab server.
    #[arg(long, default_value = "http://localhost:9100", global = true)]
    addr: String,

    /// Binary protocol address for record lookups and bench.
    // F-G10-009: aligned to the server's default `listen_addr =
    // 127.0.0.1:3300`. Pre-fix this defaulted to `localhost:3000` so a
    // fresh `teraslab-cli bench ping` against a default server failed to
    // connect — an out-of-the-box footgun on first try.
    #[arg(long, default_value = "127.0.0.1:3300", global = true)]
    data_addr: String,

    /// Output JSON instead of formatted tables.
    #[arg(long, global = true)]
    json: bool,

    /// Bearer token for the gated `/admin/*` and mutating `/debug/*` HTTP
    /// routes (R-056). When set, the CLI sends `Authorization: Bearer <token>`
    /// on every request — the server-side middleware compares against
    /// `ServerConfig::admin_token` in constant time. Reads from the
    /// `TERASLAB_ADMIN_TOKEN` env var when the flag is omitted, so secrets
    /// don't appear in shell history. Read-only endpoints (`/metrics`,
    /// `/health/*`, `/status`, read-only `/admin/*` dashboards,
    /// `/debug/freelist`, `GET /debug/log-level`) work with or without it.
    #[arg(long, env = "TERASLAB_ADMIN_TOKEN", global = true)]
    admin_token: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Cluster overview.
    Status,
    /// List cluster nodes.
    Nodes,
    /// Shard distribution.
    Shards,
    /// Storage capacity per device.
    Storage,
    /// Memory breakdown.
    Memory,
    /// Record inventory.
    Records,
    /// Inspect a single record.
    Record {
        /// Transaction ID (64-char hex string).
        txid: String,
    },
    /// Index statistics.
    Index {
        /// Include secondary index stats.
        #[arg(long)]
        secondary: bool,
    },
    /// Replication status.
    Replication,
    /// Redo log info.
    Redo,
    /// Trigger cluster rebalance.
    Rebalance {
        /// Preview without executing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Drain a node (migrate shards off).
    Drain {
        /// Node ID to drain.
        node_id: String,
    },
    /// Log level management.
    LogLevel {
        /// New log level to set (omit to show current).
        level: Option<String>,
    },
    /// Quick benchmark / smoke test.
    Bench {
        /// Operation type: "spend" or "create".
        operation: String,
        /// Number of operations.
        #[arg(long, default_value = "10000")]
        count: u32,
    },
    /// Health check all nodes.
    Healthcheck,
    /// Real-time activity monitor (like top).
    Top,
    /// Export the index to a portable migration file (OFFLINE — stop the
    /// server first; redb's file lock will refuse a live database).
    ExportIndex {
        /// Server TOML config (the same file passed to `teraslab-server`).
        #[arg(long)]
        config: std::path::PathBuf,
        /// Destination file for the portable snapshot.
        #[arg(long)]
        output: std::path::PathBuf,
    },
    /// Import a portable migration file into the configured index backend
    /// (OFFLINE — stop the server first).
    ImportIndex {
        /// Server TOML config (the same file passed to `teraslab-server`).
        #[arg(long)]
        config: std::path::PathBuf,
        /// Source file produced by `export-index`.
        #[arg(long)]
        input: std::path::PathBuf,
    },
    /// B-5: rebuild CRC-failing UTXO slots from the redo log, or report
    /// regions the WAL cannot repair (OFFLINE — stop the server first).
    ///
    /// Reconstructs torn slots covered by a V3 Spend/Unspend redo entry
    /// (which carries the slot hash) and lists slots covered only by a
    /// legacy entry as unrecoverable, so a torn slot is operator-fixable
    /// instead of a permanent startup boot-loop.
    Repair {
        /// Server TOML config (the same file passed to `teraslab-server`).
        #[arg(long)]
        config: std::path::PathBuf,
    },
}

// ---------------------------------------------------------------------------
// HTTP client wrapper
// ---------------------------------------------------------------------------

struct HttpClient {
    client: reqwest::blocking::Client,
    base_url: String,
    /// Bearer token attached to every request as `Authorization: Bearer <token>`.
    /// `None` skips the header entirely so unauthenticated endpoints stay
    /// reachable from a CLI that has no token configured.
    admin_token: Option<String>,
}

impl HttpClient {
    /// Construct an `HttpClient` that attaches `Authorization: Bearer <token>`
    /// to every request when `admin_token` is `Some(non-empty)`. An empty
    /// `Some("")` is treated identically to `None` so a wrapper script that
    /// passes `--admin-token "$VAR"` does not accidentally send `Bearer ` to
    /// the server when `$VAR` is unset.
    fn with_auth(base_url: &str, admin_token: Option<String>) -> Self {
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("failed to create HTTP client");
        let admin_token = admin_token.filter(|t| !t.is_empty());
        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            admin_token,
        }
    }

    /// Build a request builder with the Authorization header attached when
    /// `admin_token` is configured. All HTTP methods route through here so
    /// no path can accidentally bypass the auth header.
    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::blocking::RequestBuilder {
        let url = format!("{}{}", self.base_url, path);
        let mut builder = self.client.request(method, &url);
        if let Some(ref token) = self.admin_token {
            builder = builder.bearer_auth(token);
        }
        builder
    }

    fn get_json(&self, path: &str) -> Result<serde_json::Value, CliError> {
        let resp = self.request(reqwest::Method::GET, path).send()?;
        if !resp.status().is_success() {
            return Err(CliError::ServerError {
                status: resp.status().as_u16(),
                message: resp.text().unwrap_or_default(),
            });
        }
        Ok(resp.json()?)
    }

    fn get_text(&self, path: &str) -> Result<String, CliError> {
        let resp = self.request(reqwest::Method::GET, path).send()?;
        if !resp.status().is_success() {
            return Err(CliError::ServerError {
                status: resp.status().as_u16(),
                message: resp.text().unwrap_or_default(),
            });
        }
        Ok(resp.text()?)
    }

    fn put_text(&self, path: &str, body: &str) -> Result<String, CliError> {
        let resp = self
            .request(reqwest::Method::PUT, path)
            .body(body.to_string())
            .send()?;
        if !resp.status().is_success() {
            return Err(CliError::ServerError {
                status: resp.status().as_u16(),
                message: resp.text().unwrap_or_default(),
            });
        }
        Ok(resp.text()?)
    }

    fn is_ready(&self) -> bool {
        self.request(reqwest::Method::GET, "/health/ready")
            .timeout(Duration::from_secs(3))
            .send()
            .map(|r| r.status().is_success())
            .unwrap_or(false)
    }
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

fn fmt_num(n: u64) -> String {
    if n >= 1_000_000_000_000 {
        format!("{:.1}T", n as f64 / 1e12)
    } else if n >= 1_000_000_000 {
        format!("{:.1}B", n as f64 / 1e9)
    } else if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1e6)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1e3)
    } else {
        n.to_string()
    }
}

fn fmt_bytes(n: u64) -> String {
    if n >= 1_000_000_000_000 {
        format!("{:.1} TB", n as f64 / 1e12)
    } else if n >= 1_000_000_000 {
        format!("{:.1} GB", n as f64 / 1e9)
    } else if n >= 1_000_000 {
        format!("{:.1} MB", n as f64 / 1e6)
    } else if n >= 1_000 {
        format!("{:.1} KB", n as f64 / 1e3)
    } else {
        format!("{n} B")
    }
}

fn fmt_pct(val: f64) -> String {
    format!("{:.1}%", val * 100.0)
}

fn fmt_ns(ns: u64) -> String {
    if ns == 0 {
        return "-".to_string();
    }
    if ns >= 1_000_000_000 {
        format!("{:.1}s", ns as f64 / 1e9)
    } else if ns >= 1_000_000 {
        format!("{:.1}ms", ns as f64 / 1e6)
    } else if ns >= 1_000 {
        format!("{:.1}us", ns as f64 / 1e3)
    } else {
        format!("{ns}ns")
    }
}

fn as_u64(v: &serde_json::Value) -> u64 {
    v.as_u64().unwrap_or(0)
}

fn as_f64(v: &serde_json::Value) -> f64 {
    v.as_f64().unwrap_or(0.0)
}

// ---------------------------------------------------------------------------
// Command implementations
// ---------------------------------------------------------------------------

#[allow(clippy::disallowed_macros)] // CLI user-facing stdout
fn cmd_status(http: &HttpClient, json: bool) -> Result<(), CliError> {
    let status = http.get_json("/status")?;
    let index = http.get_json("/debug/index")?;
    let freelist = http.get_json("/debug/freelist")?;
    let redo = http.get_json("/debug/redo")?;

    if json {
        let combined = serde_json::json!({
            "status": status,
            "index": index,
            "storage": freelist,
            "redo": redo,
        });
        println!("{}", serde_json::to_string_pretty(&combined)?);
        return Ok(());
    }

    println!("TeraSlab Cluster Status");
    println!("=======================");
    println!(
        "Nodes:       {} (node_id: {})",
        as_u64(&status["cluster_size"]),
        as_u64(&status["node_id"])
    );
    println!(
        "Records:     {}",
        fmt_num(as_u64(&status["records"]["total"]))
    );
    println!(
        "Index:       {} entries, LF {}, memory {}",
        fmt_num(as_u64(&index["entries"])),
        fmt_pct(as_f64(&index["load_factor"])),
        fmt_bytes(as_u64(&index["memory_bytes"])),
    );
    println!(
        "Storage:     {} / {} ({})",
        fmt_bytes(as_u64(&freelist["used_bytes"])),
        fmt_bytes(as_u64(&freelist["device_size"])),
        fmt_pct(as_f64(&freelist["utilization"])),
    );
    println!(
        "Throughput:  spends {} (succeeded: {}, failed: {})",
        fmt_num(as_u64(&status["throughput"]["spends_attempted"])),
        fmt_num(as_u64(&status["throughput"]["spends_succeeded"])),
        fmt_num(as_u64(&status["throughput"]["spends_failed"])),
    );
    if redo["available"].as_bool() == Some(true) {
        println!(
            "Redo log:    {} utilized, seq {}",
            fmt_pct(as_f64(&redo["utilization"])),
            fmt_num(as_u64(&redo["current_sequence"])),
        );
    }
    println!("Ready:       {}", status["ready"]);
    Ok(())
}

#[allow(clippy::disallowed_macros)] // CLI user-facing stdout
fn cmd_nodes(http: &HttpClient, json: bool) -> Result<(), CliError> {
    let data = http.get_json("/admin/nodes")?;
    if json {
        println!("{}", serde_json::to_string_pretty(&data)?);
        return Ok(());
    }

    let mut table = Table::new();
    table.set_header(vec![
        Cell::new("Node ID").add_attribute(Attribute::Bold),
        Cell::new("Address").add_attribute(Attribute::Bold),
        Cell::new("State").add_attribute(Attribute::Bold),
        Cell::new("Master Shards").add_attribute(Attribute::Bold),
        Cell::new("Replica Shards").add_attribute(Attribute::Bold),
    ]);

    if let Some(nodes) = data["nodes"].as_array() {
        for node in nodes {
            let state_cell = if node["state"].as_str() == Some("alive") {
                Cell::new("alive").fg(Color::Green)
            } else {
                Cell::new(node["state"].as_str().unwrap_or("unknown")).fg(Color::Red)
            };
            table.add_row(vec![
                Cell::new(as_u64(&node["node_id"])),
                Cell::new(node["address"].as_str().unwrap_or("-")),
                state_cell,
                Cell::new(as_u64(&node["master_shards"])),
                Cell::new(as_u64(&node["replica_shards"])),
            ]);
        }
    }
    println!("{table}");
    Ok(())
}

#[allow(clippy::disallowed_macros)] // CLI user-facing stdout
fn cmd_shards(http: &HttpClient, json: bool) -> Result<(), CliError> {
    let status = http.get_json("/status")?;
    if json {
        println!("{}", serde_json::to_string_pretty(&status)?);
        return Ok(());
    }

    let mut table = Table::new();
    table.set_header(vec!["Metric", "Value"]);
    table.add_row(vec![
        "Shard table version",
        &status["shard_table_version"].to_string(),
    ]);
    table.add_row(vec![
        "Master shards",
        &status["master_shard_count"].to_string(),
    ]);
    table.add_row(vec![
        "Replica shards",
        &status["replica_shard_count"].to_string(),
    ]);
    table.add_row(vec![
        "Active migrations",
        &status["active_migrations"].to_string(),
    ]);
    println!("{table}");
    Ok(())
}

#[allow(clippy::disallowed_macros)] // CLI user-facing stdout
fn cmd_storage(http: &HttpClient, json: bool) -> Result<(), CliError> {
    let data = http.get_json("/debug/freelist")?;
    if json {
        println!("{}", serde_json::to_string_pretty(&data)?);
        return Ok(());
    }

    let mut table = Table::new();
    table.set_header(vec!["Metric", "Value"]);
    table.add_row(vec![
        "Device size",
        &fmt_bytes(as_u64(&data["device_size"])),
    ]);
    table.add_row(vec!["Used", &fmt_bytes(as_u64(&data["used_bytes"]))]);
    table.add_row(vec!["Free", &fmt_bytes(as_u64(&data["total_free_bytes"]))]);
    table.add_row(vec!["Utilization", &fmt_pct(as_f64(&data["utilization"]))]);
    table.add_row(vec!["Free regions", &data["free_region_count"].to_string()]);
    table.add_row(vec![
        "Largest free",
        &fmt_bytes(as_u64(&data["largest_free_region"])),
    ]);
    table.add_row(vec![
        "Alignment",
        &format!("{} bytes", as_u64(&data["alignment"])),
    ]);
    println!("{table}");
    Ok(())
}

#[allow(clippy::disallowed_macros)] // CLI user-facing stdout
fn cmd_memory(http: &HttpClient, json: bool) -> Result<(), CliError> {
    let data = http.get_json("/admin/memory")?;
    if json {
        println!("{}", serde_json::to_string_pretty(&data)?);
        return Ok(());
    }

    let mut table = Table::new();
    table.set_header(vec!["Metric", "Value"]);
    table.add_row(vec![
        "Index memory",
        &fmt_bytes(as_u64(&data["index_bytes"])),
    ]);
    table.add_row(vec![
        "Index entries",
        &fmt_num(as_u64(&data["index_entries"])),
    ]);
    table.add_row(vec![
        "DAH index entries",
        &fmt_num(as_u64(&data["dah_index_entries"])),
    ]);
    table.add_row(vec![
        "Unmined index entries",
        &fmt_num(as_u64(&data["unmined_index_entries"])),
    ]);
    println!("{table}");
    Ok(())
}

#[allow(clippy::disallowed_macros)] // CLI user-facing stdout
fn cmd_records(http: &HttpClient, json: bool) -> Result<(), CliError> {
    let data = http.get_json("/admin/records")?;
    if json {
        println!("{}", serde_json::to_string_pretty(&data)?);
        return Ok(());
    }

    let mut table = Table::new();
    table.set_header(vec!["Metric", "Value"]);
    table.add_row(vec![
        "Total records",
        &fmt_num(as_u64(&data["total_records"])),
    ]);
    table.add_row(vec![
        "DAH index count",
        &fmt_num(as_u64(&data["dah_index_count"])),
    ]);
    table.add_row(vec![
        "Unmined count",
        &fmt_num(as_u64(&data["unmined_count"])),
    ]);
    println!("{table}");
    Ok(())
}

#[allow(clippy::disallowed_macros)] // CLI user-facing stdout
fn cmd_record(http: &HttpClient, json: bool, txid: &str) -> Result<(), CliError> {
    let data = http.get_json(&format!("/debug/records/{txid}"))?;
    if json {
        println!("{}", serde_json::to_string_pretty(&data)?);
        return Ok(());
    }

    let mut table = Table::new();
    table.set_header(vec!["Field", "Value"]);
    if let Some(obj) = data.as_object() {
        for (k, v) in obj {
            table.add_row(vec![k.as_str(), &v.to_string()]);
        }
    }
    println!("{table}");
    Ok(())
}

#[allow(clippy::disallowed_macros)] // CLI user-facing stdout
fn cmd_index(http: &HttpClient, json: bool, secondary: bool) -> Result<(), CliError> {
    let data = http.get_json("/debug/index")?;
    if json {
        println!("{}", serde_json::to_string_pretty(&data)?);
        return Ok(());
    }

    let mut table = Table::new();
    table.set_header(vec!["Metric", "Value"]);
    table.add_row(vec!["Entries", &fmt_num(as_u64(&data["entries"]))]);
    table.add_row(vec!["Capacity", &fmt_num(as_u64(&data["capacity"]))]);
    table.add_row(vec!["Load factor", &fmt_pct(as_f64(&data["load_factor"]))]);
    table.add_row(vec![
        "Hugepage enabled",
        &data["hugepage_enabled"].to_string(),
    ]);
    table.add_row(vec![
        "Max probe distance",
        &data["max_probe_distance"].to_string(),
    ]);
    table.add_row(vec!["Memory", &fmt_bytes(as_u64(&data["memory_bytes"]))]);

    if secondary {
        let status = http.get_json("/status")?;
        table.add_row(vec![
            "DAH index entries",
            &fmt_num(as_u64(&status["records"]["dah_index"])),
        ]);
        table.add_row(vec![
            "Unmined index entries",
            &fmt_num(as_u64(&status["records"]["unmined_index"])),
        ]);
    }
    println!("{table}");
    Ok(())
}

#[allow(clippy::disallowed_macros)] // CLI user-facing stdout
fn cmd_replication(http: &HttpClient, json: bool) -> Result<(), CliError> {
    let data = http.get_json("/admin/replication")?;
    if json {
        println!("{}", serde_json::to_string_pretty(&data)?);
        return Ok(());
    }

    let mut table = Table::new();
    table.set_header(vec!["Metric", "Value"]);
    table.add_row(vec!["Enabled", &data["enabled"].to_string()]);
    if data["enabled"].as_bool() == Some(true) {
        table.add_row(vec![
            "ACK policy",
            data["ack_policy"].as_str().unwrap_or("-"),
        ]);
        table.add_row(vec!["Best effort", &data["best_effort"].to_string()]);
        table.add_row(vec!["Topology term", &data["topology_term"].to_string()]);
        table.add_row(vec!["Topology epoch", &data["topology_epoch"].to_string()]);
        table.add_row(vec![
            "Peak cluster size",
            &data["peak_cluster_size"].to_string(),
        ]);
    }
    println!("{table}");
    Ok(())
}

#[allow(clippy::disallowed_macros)] // CLI user-facing stdout
fn cmd_redo(http: &HttpClient, json: bool) -> Result<(), CliError> {
    let data = http.get_json("/debug/redo")?;
    if json {
        println!("{}", serde_json::to_string_pretty(&data)?);
        return Ok(());
    }

    let mut table = Table::new();
    table.set_header(vec!["Metric", "Value"]);
    if data["available"].as_bool() == Some(true) {
        table.add_row(vec!["Available", "true"]);
        table.add_row(vec![
            "Current sequence",
            &fmt_num(as_u64(&data["current_sequence"])),
        ]);
        table.add_row(vec![
            "Write position",
            &fmt_bytes(as_u64(&data["write_position"])),
        ]);
        table.add_row(vec![
            "Available space",
            &fmt_bytes(as_u64(&data["available_space"])),
        ]);
        table.add_row(vec!["Log size", &fmt_bytes(as_u64(&data["log_size"]))]);
        table.add_row(vec!["Utilization", &fmt_pct(as_f64(&data["utilization"]))]);
    } else {
        table.add_row(vec!["Available", "false"]);
    }
    println!("{table}");
    Ok(())
}

#[allow(clippy::disallowed_macros)] // CLI user-facing stdout
fn cmd_rebalance(http: &HttpClient, json: bool, dry_run: bool) -> Result<(), CliError> {
    if dry_run {
        let status = http.get_json("/status")?;
        if json {
            println!("{}", serde_json::to_string_pretty(&status)?);
        } else {
            println!(
                "Dry run: current node has {} master shards, {} replica shards",
                as_u64(&status["master_shard_count"]),
                as_u64(&status["replica_shard_count"])
            );
        }
        return Ok(());
    }
    let result = http.put_text("/admin/rebalance", "")?;
    if json {
        println!("{}", serde_json::json!({"result": result}));
    } else {
        println!("{result}");
    }
    Ok(())
}

#[allow(clippy::disallowed_macros)] // CLI user-facing stdout
fn cmd_drain(http: &HttpClient, json: bool, node_id: &str) -> Result<(), CliError> {
    let result = http.put_text(&format!("/admin/drain/{node_id}"), "")?;
    if json {
        println!("{}", serde_json::json!({"result": result}));
    } else {
        println!("{result}");
    }
    Ok(())
}

#[allow(clippy::disallowed_macros)] // CLI user-facing stdout
fn cmd_log_level(http: &HttpClient, json: bool, level: Option<&str>) -> Result<(), CliError> {
    match level {
        Some(l) => {
            let result = http.put_text("/debug/log-level", l)?;
            if json {
                println!("{}", serde_json::json!({"result": result}));
            } else {
                println!("{result}");
            }
        }
        None => {
            let current = http.get_text("/debug/log-level")?;
            if json {
                println!("{}", serde_json::json!({"level": current}));
            } else {
                println!("Current log level: {current}");
            }
        }
    }
    Ok(())
}

#[allow(clippy::disallowed_macros)] // CLI user-facing stdout
fn cmd_healthcheck(http: &HttpClient, json: bool) -> Result<bool, CliError> {
    let healthy = http.is_ready();
    if json {
        println!("{}", serde_json::json!({"healthy": healthy}));
    } else if healthy {
        println!("Healthy: all nodes ready");
    } else {
        eprintln!("Unhealthy: node not ready or unreachable");
    }
    Ok(healthy)
}

#[allow(clippy::disallowed_macros)] // CLI user-facing stdout
fn cmd_bench(
    http: &HttpClient,
    data_addr: &str,
    json: bool,
    operation: &str,
    count: u32,
) -> Result<(), CliError> {
    // For bench, we use the binary protocol directly.
    // First verify the server is up.
    if !http.is_ready() {
        return Err(CliError::Other("server not ready".to_string()));
    }

    let mut stream = TcpStream::connect(data_addr)
        .map_err(|e| CliError::Other(format!("failed to connect to {data_addr}: {e}")))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;

    // Use the binary protocol to send PING operations as a benchmark
    let start = Instant::now();
    for i in 0..count {
        // Encode a PING frame: [total_length:4][request_id:8][op_code:2][flags:2]
        let request_id = i as u64;
        let op_code: u16 = 102; // OP_PING
        let flags: u16 = 0;
        let payload_len: u32 = 0;
        let total_len: u32 = 8 + 2 + 2 + payload_len;

        let mut frame = Vec::with_capacity(4 + total_len as usize);
        frame.extend_from_slice(&total_len.to_le_bytes());
        frame.extend_from_slice(&request_id.to_le_bytes());
        frame.extend_from_slice(&op_code.to_le_bytes());
        frame.extend_from_slice(&flags.to_le_bytes());

        stream.write_all(&frame)?;

        // Read response: [total_length:4][request_id:8][status:1][payload...]
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf)?;
        let resp_len = u32::from_le_bytes(len_buf) as usize;
        let mut resp_buf = vec![0u8; resp_len];
        stream.read_exact(&mut resp_buf)?;
    }
    let elapsed = start.elapsed();
    let ops_per_sec = if elapsed.as_secs_f64() > 0.0 {
        count as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    if json {
        println!(
            "{}",
            serde_json::json!({
                "operation": operation,
                "count": count,
                "elapsed_ms": elapsed.as_millis(),
                "ops_per_sec": ops_per_sec as u64,
            })
        );
    } else {
        println!(
            "Bench: {} x {} {operation} operations",
            fmt_num(count as u64),
            if operation == "ping" {
                "PING"
            } else {
                operation
            }
        );
        println!("Elapsed: {:.2}s", elapsed.as_secs_f64());
        println!("Throughput: {} ops/sec", fmt_num(ops_per_sec as u64));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Top command — ratatui terminal UI
// ---------------------------------------------------------------------------

/// View mode for the top command.
#[derive(Clone, Copy, PartialEq)]
enum TopView {
    /// Aggregate cluster-wide totals.
    Aggregate,
    /// Per-node breakdown.
    PerNode,
}

fn cmd_top(http: &HttpClient) -> Result<(), CliError> {
    use crossterm::ExecutableCommand;
    use crossterm::event::{self, Event, KeyCode, KeyEventKind};
    use crossterm::terminal::{self, EnterAlternateScreen, LeaveAlternateScreen};
    use ratatui::prelude::*;

    terminal::enable_raw_mode().map_err(|e| CliError::Other(format!("terminal: {e}")))?;
    let mut stdout = std::io::stdout();
    stdout
        .execute(EnterAlternateScreen)
        .map_err(|e| CliError::Other(format!("terminal: {e}")))?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal =
        Terminal::new(backend).map_err(|e| CliError::Other(format!("terminal: {e}")))?;

    let mut prev_response: Option<serde_json::Value> = None;
    let mut error_msg: Option<String>;
    let mut view = TopView::Aggregate;

    loop {
        // Fetch cluster-wide snapshot
        let response = match http.get_json("/admin/top") {
            Ok(s) => {
                error_msg = None;
                Some(s)
            }
            Err(e) => {
                error_msg = Some(format!("Connection lost: {e}"));
                None
            }
        };

        terminal
            .draw(|frame| {
                draw_top(
                    frame,
                    response.as_ref(),
                    prev_response.as_ref(),
                    error_msg.as_deref(),
                    view,
                );
            })
            .map_err(|e| CliError::Other(format!("render: {e}")))?;

        if response.is_some() {
            prev_response = response;
        }

        // Poll for keys with 1s timeout
        if event::poll(Duration::from_secs(1)).unwrap_or(false)
            && let Ok(Event::Key(key)) = event::read()
            && key.kind == KeyEventKind::Press
        {
            match key.code {
                KeyCode::Char('q') => break,
                KeyCode::Char('v') | KeyCode::Tab => {
                    view = match view {
                        TopView::Aggregate => TopView::PerNode,
                        TopView::PerNode => TopView::Aggregate,
                    };
                }
                _ => {}
            }
        }
    }

    terminal::disable_raw_mode().ok();
    std::io::stdout().execute(LeaveAlternateScreen).ok();
    Ok(())
}

/// Extract the viewable data snapshot from a response.
/// The response may be `{ "aggregate": {...}, "nodes": [...] }` (cluster)
/// or a flat snapshot (single node / ?local=true).
fn get_view_data(response: &serde_json::Value) -> &serde_json::Value {
    if response.get("aggregate").is_some() {
        &response["aggregate"]
    } else {
        response
    }
}

/// Compute per-second rates from two consecutive snapshots of the same shape.
fn compute_rates(
    prev: &serde_json::Value,
    cur: &serde_json::Value,
) -> Vec<(String, u64, u64, u64, String, String)> {
    let dt_ms = as_u64(&cur["timestamp_ms"]).saturating_sub(as_u64(&prev["timestamp_ms"]));
    if dt_ms == 0 {
        return Vec::new();
    }
    let dt = dt_ms as f64 / 1000.0;

    let rate = |key: &str| -> u64 {
        let c = as_u64(&cur["counters"][key]);
        let p = as_u64(&prev["counters"][key]);
        (c.saturating_sub(p) as f64 / dt) as u64
    };

    vec![
        (
            "spend".into(),
            rate("spends_attempted"),
            as_u64(&cur["counters"]["spends_attempted"]),
            as_u64(&cur["counters"]["spends_failed"]),
            fmt_ns(as_u64(&cur["latency"]["spend"]["p50_ns"])),
            fmt_ns(as_u64(&cur["latency"]["spend"]["p99_ns"])),
        ),
        (
            "spend_multi".into(),
            rate("spend_multi_batches"),
            as_u64(&cur["counters"]["spend_multi_batches"]),
            0,
            fmt_ns(as_u64(&cur["latency"]["spend_multi"]["p50_ns"])),
            fmt_ns(as_u64(&cur["latency"]["spend_multi"]["p99_ns"])),
        ),
        (
            "create".into(),
            rate("creates_attempted"),
            as_u64(&cur["counters"]["creates_attempted"]),
            as_u64(&cur["counters"]["creates_attempted"])
                .saturating_sub(as_u64(&cur["counters"]["creates_succeeded"])),
            "-".into(),
            "-".into(),
        ),
        (
            "set_mined".into(),
            rate("set_mined_attempted"),
            as_u64(&cur["counters"]["set_mined_attempted"]),
            as_u64(&cur["counters"]["set_mined_attempted"])
                .saturating_sub(as_u64(&cur["counters"]["set_mined_succeeded"])),
            "-".into(),
            "-".into(),
        ),
        (
            "get".into(),
            rate("gets_attempted"),
            as_u64(&cur["counters"]["gets_attempted"]),
            as_u64(&cur["counters"]["gets_attempted"])
                .saturating_sub(as_u64(&cur["counters"]["gets_succeeded"])),
            "-".into(),
            "-".into(),
        ),
        (
            "unspend".into(),
            rate("unspends_attempted"),
            as_u64(&cur["counters"]["unspends_attempted"]),
            as_u64(&cur["counters"]["unspends_failed"]),
            fmt_ns(as_u64(&cur["latency"]["unspend"]["p50_ns"])),
            fmt_ns(as_u64(&cur["latency"]["unspend"]["p99_ns"])),
        ),
    ]
}

/// Render the top TUI with aggregate or per-node view.
fn draw_top(
    frame: &mut ratatui::Frame,
    response: Option<&serde_json::Value>,
    prev_response: Option<&serde_json::Value>,
    error: Option<&str>,
    view: TopView,
) {
    use ratatui::prelude::*;
    use ratatui::widgets::*;

    let area = frame.area();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // header
            Constraint::Min(10),   // ops table
            Constraint::Length(5), // stats
            Constraint::Length(1), // footer
        ])
        .split(area);

    // Header
    let header_text = if let Some(resp) = response {
        let agg = get_view_data(resp);
        let node_count = agg.get("node_count").and_then(|v| v.as_u64()).unwrap_or(1);
        let view_label = match view {
            TopView::Aggregate => format!("CLUSTER ({node_count} nodes)"),
            TopView::PerNode => format!("PER-NODE ({node_count} nodes)"),
        };
        format!(
            " TeraSlab Top [{view_label}]  |  {} connections  |  {} records  |  ready: {}",
            as_u64(&agg["connections"]),
            fmt_num(as_u64(&agg["index"]["entries"])),
            agg["ready"],
        )
    } else if let Some(err) = error {
        format!(" TeraSlab Top  |  {err}")
    } else {
        " TeraSlab Top  |  connecting...".to_string()
    };

    let header = Paragraph::new(header_text)
        .style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .block(Block::default().borders(Borders::BOTTOM));
    frame.render_widget(header, chunks[0]);

    match view {
        TopView::Aggregate => {
            // Show aggregate rates and stats
            let rates = if let (Some(cur), Some(prev)) = (response, prev_response) {
                compute_rates(get_view_data(cur), get_view_data(prev))
            } else {
                Vec::new()
            };
            render_ops_table(frame, chunks[1], &rates, " Operations (Cluster Aggregate) ");
            if let Some(resp) = response {
                render_system_stats(frame, chunks[2], get_view_data(resp));
            } else {
                render_waiting(frame, chunks[2]);
            }
        }
        TopView::PerNode => {
            // Show per-node breakdown
            if let Some(resp) = response {
                let nodes = resp.get("nodes").and_then(|n| n.as_array());
                if let Some(nodes) = nodes {
                    render_per_node_table(frame, chunks[1], nodes, prev_response);
                } else {
                    // No nodes array — single node, show as aggregate
                    let rates = if let Some(prev) = prev_response {
                        compute_rates(get_view_data(resp), get_view_data(prev))
                    } else {
                        Vec::new()
                    };
                    render_ops_table(frame, chunks[1], &rates, " Operations (Single Node) ");
                }
                render_system_stats(frame, chunks[2], get_view_data(resp));
            } else {
                render_waiting(frame, chunks[1]);
                render_waiting(frame, chunks[2]);
            }
        }
    }

    // Footer
    let footer = Paragraph::new(" 'q' quit  |  'v'/Tab toggle aggregate/per-node")
        .style(Style::default().fg(Color::DarkGray));
    frame.render_widget(footer, chunks[3]);
}

fn render_ops_table(
    frame: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    rates: &[(String, u64, u64, u64, String, String)],
    title: &str,
) {
    use ratatui::prelude::*;
    use ratatui::widgets::*;

    let header_row = Row::new(vec![
        "Operation",
        "Ops/sec",
        "Total",
        "Errors",
        "p50",
        "p99",
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));
    let rows: Vec<Row> = rates
        .iter()
        .map(|(name, rate, total, errors, p50, p99)| {
            Row::new(vec![
                name.clone(),
                fmt_num(*rate),
                fmt_num(*total),
                errors.to_string(),
                p50.clone(),
                p99.clone(),
            ])
        })
        .collect();

    let table = ratatui::widgets::Table::new(
        rows,
        [
            Constraint::Length(14),
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
        ],
    )
    .header(header_row)
    .block(Block::default().title(title).borders(Borders::ALL));
    frame.render_widget(table, area);
}

fn render_per_node_table(
    frame: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    nodes: &[serde_json::Value],
    prev_response: Option<&serde_json::Value>,
) {
    use ratatui::prelude::*;
    use ratatui::widgets::*;

    let header_row = Row::new(vec![
        "Node",
        "Spends/s",
        "Creates/s",
        "Gets/s",
        "Records",
        "Storage",
        "Conns",
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));

    let prev_nodes: Vec<&serde_json::Value> = prev_response
        .and_then(|r| r.get("nodes"))
        .and_then(|n| n.as_array())
        .map(|a| a.iter().collect())
        .unwrap_or_default();

    let rows: Vec<Row> = nodes
        .iter()
        .map(|node| {
            let node_id = as_u64(&node["node_id"]);
            // Find matching previous node for rate calculation
            let prev_node = prev_nodes.iter().find(|p| as_u64(&p["node_id"]) == node_id);
            let dt_ms = prev_node
                .map(|p| as_u64(&node["timestamp_ms"]).saturating_sub(as_u64(&p["timestamp_ms"])))
                .unwrap_or(0);
            let dt = if dt_ms > 0 {
                dt_ms as f64 / 1000.0
            } else {
                1.0
            };

            let spend_rate = prev_node
                .map(|p| {
                    let c = as_u64(&node["counters"]["spends_attempted"]);
                    let prev = as_u64(&p["counters"]["spends_attempted"]);
                    (c.saturating_sub(prev) as f64 / dt) as u64
                })
                .unwrap_or(0);

            let create_rate = prev_node
                .map(|p| {
                    let c = as_u64(&node["counters"]["creates_attempted"]);
                    let prev = as_u64(&p["counters"]["creates_attempted"]);
                    (c.saturating_sub(prev) as f64 / dt) as u64
                })
                .unwrap_or(0);

            let get_rate = prev_node
                .map(|p| {
                    let c = as_u64(&node["counters"]["gets_attempted"]);
                    let prev = as_u64(&p["counters"]["gets_attempted"]);
                    (c.saturating_sub(prev) as f64 / dt) as u64
                })
                .unwrap_or(0);

            Row::new(vec![
                format!("node {node_id}"),
                fmt_num(spend_rate),
                fmt_num(create_rate),
                fmt_num(get_rate),
                fmt_num(as_u64(&node["index"]["entries"])),
                fmt_pct(as_f64(&node["storage"]["utilization"])),
                as_u64(&node["connections"]).to_string(),
            ])
        })
        .collect();

    let table = ratatui::widgets::Table::new(
        rows,
        [
            Constraint::Length(10),
            Constraint::Length(12),
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(6),
        ],
    )
    .header(header_row)
    .block(
        Block::default()
            .title(" Per-Node Breakdown ")
            .borders(Borders::ALL),
    );
    frame.render_widget(table, area);
}

fn render_system_stats(
    frame: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    snap: &serde_json::Value,
) {
    use ratatui::widgets::*;

    let stats_text = format!(
        " Index: {} entries  LF: {}  Memory: {}  |  Storage: {} / {} ({})  Free regions: {}  |  Redo: {} seq: {}",
        fmt_num(as_u64(&snap["index"]["entries"])),
        fmt_pct(as_f64(&snap["index"]["load_factor"])),
        fmt_bytes(as_u64(&snap["index"]["memory_bytes"])),
        fmt_bytes(as_u64(&snap["storage"]["used_bytes"])),
        fmt_bytes(as_u64(&snap["storage"]["total_bytes"])),
        fmt_pct(as_f64(&snap["storage"]["utilization"])),
        as_u64(&snap["storage"]["free_regions"]),
        fmt_pct(as_f64(&snap["redo"]["utilization"])),
        fmt_num(as_u64(&snap["redo"]["current_sequence"])),
    );
    let stats = Paragraph::new(stats_text)
        .block(Block::default().title(" System ").borders(Borders::ALL))
        .wrap(Wrap { trim: true });
    frame.render_widget(stats, area);
}

fn render_waiting(frame: &mut ratatui::Frame, area: ratatui::layout::Rect) {
    use ratatui::widgets::*;
    let p = Paragraph::new(" Waiting for data...").block(Block::default().borders(Borders::ALL));
    frame.render_widget(p, area);
}

// ---------------------------------------------------------------------------
// Offline index migration (export-index / import-index)
// ---------------------------------------------------------------------------

/// Open the configured index backend read-only-ish and export it to the
/// portable migration format (`src/index/migration.rs`, magic `TSMI`).
///
/// OFFLINE operation: the server must be stopped. For the redb backend,
/// redb's own file lock enforces this (opening a live database fails);
/// for the memory backend the export reads the on-disk snapshot, which
/// only reflects the last checkpoint / clean shutdown.
#[allow(clippy::disallowed_macros)] // CLI user-facing stdout/stderr
fn cmd_export_index(
    config_path: &std::path::Path,
    output: &std::path::Path,
    json: bool,
) -> Result<(), CliError> {
    use teraslab::config::{IndexBackendMode, ServerConfig};
    use teraslab::index::{DahBackend, PrimaryBackend, UnminedBackend, migration};

    let cfg = ServerConfig::load(config_path).map_err(CliError::Other)?;
    let (primary, dah, unmined) = match cfg.index.backend {
        IndexBackendMode::Redb => {
            if migration::import_in_progress(&cfg.index) {
                return Err(CliError::Other(format!(
                    "a previous import-index was interrupted (sentinel present next to {}); \
                     re-run import-index to overwrite the partial state, or remove the \
                     sentinel after verifying the redb files are consistent",
                    cfg.index.redb_path.display()
                )));
            }
            let primary = PrimaryBackend::restore_redb(&cfg.index)?;
            let dah = DahBackend::OnDisk(teraslab::index::redb_dah::RedbDahIndex::open(
                &cfg.index.redb_dah_path,
                cfg.index.redb_cache_size,
            )?);
            let unmined =
                UnminedBackend::OnDisk(teraslab::index::redb_unmined::RedbUnminedIndex::open(
                    &cfg.index.redb_unmined_path,
                    cfg.index.redb_cache_size,
                )?);
            (primary, dah, unmined)
        }
        IndexBackendMode::Memory => {
            let (primary, dah, unmined, flags) =
                PrimaryBackend::restore_all(&cfg.index_snapshot_path)?;
            if flags.dah_needs_rebuild || flags.unmined_needs_rebuild {
                return Err(CliError::Other(
                    "the snapshot's secondary index sections need a device-scan rebuild; \
                     start the server once and shut it down cleanly, then retry"
                        .to_string(),
                ));
            }
            eprintln!(
                "note: exporting from the snapshot at {} — redo entries written after the \
                 last clean shutdown/checkpoint are NOT included; stop the server cleanly \
                 before exporting",
                cfg.index_snapshot_path.display()
            );
            (
                primary,
                DahBackend::from(dah),
                UnminedBackend::from(unmined),
            )
        }
        IndexBackendMode::FileBacked => {
            return Err(CliError::Other(
                "export-index does not support the file_backed backend (its secondaries \
                 can only be rebuilt from a device scan); migrate via memory or redb"
                    .to_string(),
            ));
        }
    };

    let stats = migration::export_index(&primary, &dah, &unmined, output)?;
    if json {
        println!(
            "{}",
            serde_json::json!({
                "output": output.display().to_string(),
                "primary_entries": stats.primary_entries,
                "dah_entries": stats.dah_entries,
                "unmined_entries": stats.unmined_entries,
            })
        );
    } else {
        println!(
            "exported {} primary, {} DAH, {} unmined entries to {}",
            stats.primary_entries,
            stats.dah_entries,
            stats.unmined_entries,
            output.display()
        );
    }
    Ok(())
}

/// Import a portable migration file into the configured backend.
///
/// OFFLINE operation. For redb the R-047 sentinel inside
/// `migration::import_index` makes an interrupted import refuse the next
/// server startup instead of silently loading partial state. For the
/// memory backend the imported indexes exist only in this process, so
/// they are persisted via `snapshot_all` — without that the import would
/// be a silent no-op.
#[allow(clippy::disallowed_macros)] // CLI user-facing stdout/stderr
fn cmd_import_index(
    config_path: &std::path::Path,
    input: &std::path::Path,
    json: bool,
) -> Result<(), CliError> {
    use teraslab::config::{IndexBackendMode, ServerConfig};
    use teraslab::index::migration;

    let cfg = ServerConfig::load(config_path).map_err(CliError::Other)?;
    if matches!(cfg.index.backend, IndexBackendMode::FileBacked) {
        return Err(CliError::Other(
            "import-index does not support the file_backed backend; configure memory \
             or redb"
                .to_string(),
        ));
    }

    let (primary, dah, unmined, stats) = migration::import_index(&cfg.index, input)?;
    if matches!(cfg.index.backend, IndexBackendMode::Memory) {
        primary.snapshot_all(&dah, &unmined, &cfg.index_snapshot_path)?;
    }

    if json {
        println!(
            "{}",
            serde_json::json!({
                "input": input.display().to_string(),
                "primary_entries": stats.primary_entries,
                "dah_entries": stats.dah_entries,
                "unmined_entries": stats.unmined_entries,
            })
        );
    } else {
        println!(
            "imported {} primary, {} DAH, {} unmined entries from {}",
            stats.primary_entries,
            stats.dah_entries,
            stats.unmined_entries,
            input.display()
        );
        println!("verify the counts above before restarting the server");
    }
    Ok(())
}

/// B-5: offline torn-slot repair.
///
/// Opens the data device, redo log, and primary index from the server
/// config (the server MUST be stopped), then rebuilds CRC-failing slots
/// from V3 redo entries and reports any that the WAL cannot repair.
#[allow(clippy::disallowed_macros)] // CLI user-facing stdout/stderr
fn cmd_repair(config_path: &std::path::Path, json: bool) -> Result<(), CliError> {
    use std::sync::Arc;
    use teraslab::config::{IndexBackendMode, ServerConfig};
    use teraslab::device::{BlockDevice, DirectDevice};
    use teraslab::index::{PrimaryBackend, ShardedIndex};
    use teraslab::redo::RedoLog;

    let cfg = ServerConfig::load(config_path).map_err(CliError::Other)?;

    // Load the primary index for record-offset lookups. Secondary
    // indexes are not needed for slot repair.
    let primary = match cfg.index.backend {
        IndexBackendMode::Redb => {
            if teraslab::index::migration::import_in_progress(&cfg.index) {
                return Err(CliError::Other(
                    "a previous import-index was interrupted; resolve it before repair".to_string(),
                ));
            }
            PrimaryBackend::restore_redb(&cfg.index)?
        }
        IndexBackendMode::Memory => {
            let (primary, _dah, _unmined, _flags) =
                PrimaryBackend::restore_all(&cfg.index_snapshot_path)?;
            primary
        }
        IndexBackendMode::FileBacked => {
            return Err(CliError::Other(
                "repair does not support the file_backed backend; its index is rebuilt \
                 from a device scan on startup"
                    .to_string(),
            ));
        }
    };

    let device_path = cfg
        .device_paths
        .first()
        .ok_or_else(|| CliError::Other("config has no device_paths".to_string()))?;
    let device: Arc<dyn BlockDevice> = Arc::new(
        DirectDevice::open(device_path, cfg.device_size, cfg.device_alignment)
            .map_err(|e| CliError::Other(format!("open data device: {e}")))?,
    );

    let redo_path = cfg.resolved_redo_log_path();
    let redo_device: Arc<dyn BlockDevice> = Arc::new(
        DirectDevice::open(&redo_path, cfg.redo_log_size, cfg.device_alignment)
            .map_err(|e| CliError::Other(format!("open redo device: {e}")))?,
    );
    let redo_log = RedoLog::open(redo_device, 0, cfg.redo_log_size)
        .map_err(|e| CliError::Other(format!("open redo log: {e}")))?;

    let index = ShardedIndex::from_single(primary);
    let report = teraslab::recovery::repair_torn_slots(&*device, &redo_log, &index)
        .map_err(|e| CliError::Other(format!("repair pass failed: {e}")))?;

    if json {
        let unrecoverable: Vec<serde_json::Value> = report
            .unrecoverable
            .iter()
            .map(|(txid, slot)| serde_json::json!({ "txid": hex_encode(txid), "slot": slot }))
            .collect();
        println!(
            "{}",
            serde_json::json!({
                "entries_scanned": report.entries_scanned,
                "slots_repaired": report.slots_repaired,
                "missing_primary": report.missing_primary,
                "unrecoverable": unrecoverable,
            })
        );
    } else {
        println!(
            "scanned {} spend/unspend redo entries",
            report.entries_scanned
        );
        println!(
            "repaired {} torn slot(s) from the redo log",
            report.slots_repaired
        );
        if report.missing_primary > 0 {
            println!(
                "{} entr(ies) had no primary index record (deleted later) — skipped",
                report.missing_primary,
            );
        }
        if report.unrecoverable.is_empty() {
            println!("no unrecoverable regions; restart the server");
        } else {
            println!(
                "{} slot(s) could NOT be repaired from the WAL:",
                report.unrecoverable.len()
            );
            for (txid, slot) in &report.unrecoverable {
                println!("  txid={} slot={slot}", hex_encode(txid));
            }
            println!("these regions need manual recovery (restore from a replica or snapshot)");
        }
    }
    Ok(())
}

/// Lowercase hex of a byte slice for operator-facing output.
fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[allow(clippy::disallowed_macros)] // CLI user-facing stderr on error
fn main() -> ExitCode {
    let cli = Cli::parse();
    let http = HttpClient::with_auth(&cli.addr, cli.admin_token.clone());

    let result = match cli.command {
        Command::Status => cmd_status(&http, cli.json),
        Command::Nodes => cmd_nodes(&http, cli.json),
        Command::Shards => cmd_shards(&http, cli.json),
        Command::Storage => cmd_storage(&http, cli.json),
        Command::Memory => cmd_memory(&http, cli.json),
        Command::Records => cmd_records(&http, cli.json),
        Command::Record { txid } => cmd_record(&http, cli.json, &txid),
        Command::Index { secondary } => cmd_index(&http, cli.json, secondary),
        Command::Replication => cmd_replication(&http, cli.json),
        Command::Redo => cmd_redo(&http, cli.json),
        Command::Rebalance { dry_run } => cmd_rebalance(&http, cli.json, dry_run),
        Command::Drain { node_id } => cmd_drain(&http, cli.json, &node_id),
        Command::LogLevel { level } => cmd_log_level(&http, cli.json, level.as_deref()),
        Command::Bench { operation, count } => {
            cmd_bench(&http, &cli.data_addr, cli.json, &operation, count)
        }
        Command::Healthcheck => match cmd_healthcheck(&http, cli.json) {
            Ok(true) => return ExitCode::SUCCESS,
            Ok(false) => return ExitCode::FAILURE,
            Err(e) => {
                eprintln!("Error: {e}");
                return ExitCode::FAILURE;
            }
        },
        Command::Top => cmd_top(&http),
        Command::ExportIndex { config, output } => cmd_export_index(&config, &output, cli.json),
        Command::ImportIndex { config, input } => cmd_import_index(&config, &input, cli.json),
        Command::Repair { config } => cmd_repair(&config, cli.json),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("Error: {e}");
            ExitCode::FAILURE
        }
    }
}
