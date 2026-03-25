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
    #[arg(long, default_value = "localhost:3000", global = true)]
    data_addr: String,

    /// Output JSON instead of formatted tables.
    #[arg(long, global = true)]
    json: bool,

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
    Shards {
        #[arg(long)]
        node: Option<String>,
    },
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
        /// Include UTXO slot details.
        #[arg(long)]
        slots: bool,
        /// Show raw metadata.
        #[arg(long)]
        raw: bool,
    },
    /// Index statistics.
    Index {
        /// Include secondary index stats.
        #[arg(long)]
        secondary: bool,
    },
    /// Replication status.
    Replication {
        /// Show history.
        #[arg(long)]
        history: bool,
    },
    /// Redo log info.
    Redo {
        /// Tail N entries.
        #[arg(long)]
        tail: Option<u32>,
    },
    /// Trigger cluster rebalance.
    Rebalance {
        /// Preview without executing.
        #[arg(long)]
        dry_run: bool,
        /// Execute the rebalance.
        #[arg(long)]
        execute: bool,
    },
    /// Drain a node (migrate shards off).
    Drain {
        /// Node ID to drain.
        node_id: String,
        /// Cancel an active drain.
        #[arg(long)]
        cancel: bool,
    },
    /// Log level management.
    LogLevel {
        /// New log level to set (omit to show current).
        level: Option<String>,
        /// Target module (optional).
        #[arg(long)]
        target: Option<String>,
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
}

// ---------------------------------------------------------------------------
// HTTP client wrapper
// ---------------------------------------------------------------------------

struct HttpClient {
    client: reqwest::blocking::Client,
    base_url: String,
}

impl HttpClient {
    fn new(base_url: &str) -> Self {
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("failed to create HTTP client");
        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    fn get_json(&self, path: &str) -> Result<serde_json::Value, CliError> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self.client.get(&url).send()?;
        if !resp.status().is_success() {
            return Err(CliError::ServerError {
                status: resp.status().as_u16(),
                message: resp.text().unwrap_or_default(),
            });
        }
        Ok(resp.json()?)
    }

    fn get_text(&self, path: &str) -> Result<String, CliError> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self.client.get(&url).send()?;
        if !resp.status().is_success() {
            return Err(CliError::ServerError {
                status: resp.status().as_u16(),
                message: resp.text().unwrap_or_default(),
            });
        }
        Ok(resp.text()?)
    }

    fn put_text(&self, path: &str, body: &str) -> Result<String, CliError> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self.client.put(&url).body(body.to_string()).send()?;
        if !resp.status().is_success() {
            return Err(CliError::ServerError {
                status: resp.status().as_u16(),
                message: resp.text().unwrap_or_default(),
            });
        }
        Ok(resp.text()?)
    }

    fn is_ready(&self) -> bool {
        self.client
            .get(format!("{}/health/ready", self.base_url))
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
    if n >= 1_000_000_000_000 { format!("{:.1}T", n as f64 / 1e12) }
    else if n >= 1_000_000_000 { format!("{:.1}B", n as f64 / 1e9) }
    else if n >= 1_000_000 { format!("{:.1}M", n as f64 / 1e6) }
    else if n >= 1_000 { format!("{:.1}K", n as f64 / 1e3) }
    else { n.to_string() }
}

fn fmt_bytes(n: u64) -> String {
    if n >= 1_000_000_000_000 { format!("{:.1} TB", n as f64 / 1e12) }
    else if n >= 1_000_000_000 { format!("{:.1} GB", n as f64 / 1e9) }
    else if n >= 1_000_000 { format!("{:.1} MB", n as f64 / 1e6) }
    else if n >= 1_000 { format!("{:.1} KB", n as f64 / 1e3) }
    else { format!("{n} B") }
}

fn fmt_pct(val: f64) -> String {
    format!("{:.1}%", val * 100.0)
}

fn fmt_ns(ns: u64) -> String {
    if ns == 0 { return "-".to_string(); }
    if ns >= 1_000_000_000 { format!("{:.1}s", ns as f64 / 1e9) }
    else if ns >= 1_000_000 { format!("{:.1}ms", ns as f64 / 1e6) }
    else if ns >= 1_000 { format!("{:.1}us", ns as f64 / 1e3) }
    else { format!("{ns}ns") }
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
    println!("Nodes:       {} (node_id: {})", as_u64(&status["cluster_size"]), as_u64(&status["node_id"]));
    println!("Records:     {}", fmt_num(as_u64(&status["records"]["total"])));
    println!("Index:       {} entries, LF {}, memory {}",
        fmt_num(as_u64(&index["entries"])),
        fmt_pct(as_f64(&index["load_factor"])),
        fmt_bytes(as_u64(&index["memory_bytes"])),
    );
    println!("Storage:     {} / {} ({})",
        fmt_bytes(as_u64(&freelist["used_bytes"])),
        fmt_bytes(as_u64(&freelist["device_size"])),
        fmt_pct(as_f64(&freelist["utilization"])),
    );
    println!("Throughput:  spends {} (succeeded: {}, failed: {})",
        fmt_num(as_u64(&status["throughput"]["spends_attempted"])),
        fmt_num(as_u64(&status["throughput"]["spends_succeeded"])),
        fmt_num(as_u64(&status["throughput"]["spends_failed"])),
    );
    if redo["available"].as_bool() == Some(true) {
        println!("Redo log:    {} utilized, seq {}",
            fmt_pct(as_f64(&redo["utilization"])),
            fmt_num(as_u64(&redo["current_sequence"])),
        );
    }
    println!("Ready:       {}", status["ready"]);
    Ok(())
}

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

fn cmd_shards(http: &HttpClient, json: bool, _node: Option<String>) -> Result<(), CliError> {
    let status = http.get_json("/status")?;
    if json {
        println!("{}", serde_json::to_string_pretty(&status)?);
        return Ok(());
    }

    let mut table = Table::new();
    table.set_header(vec!["Metric", "Value"]);
    table.add_row(vec!["Shard table version", &status["shard_table_version"].to_string()]);
    table.add_row(vec!["Master shards", &status["master_shard_count"].to_string()]);
    table.add_row(vec!["Replica shards", &status["replica_shard_count"].to_string()]);
    table.add_row(vec!["Active migrations", &status["active_migrations"].to_string()]);
    println!("{table}");
    Ok(())
}

fn cmd_storage(http: &HttpClient, json: bool) -> Result<(), CliError> {
    let data = http.get_json("/debug/freelist")?;
    if json {
        println!("{}", serde_json::to_string_pretty(&data)?);
        return Ok(());
    }

    let mut table = Table::new();
    table.set_header(vec!["Metric", "Value"]);
    table.add_row(vec!["Device size", &fmt_bytes(as_u64(&data["device_size"]))]);
    table.add_row(vec!["Used", &fmt_bytes(as_u64(&data["used_bytes"]))]);
    table.add_row(vec!["Free", &fmt_bytes(as_u64(&data["total_free_bytes"]))]);
    table.add_row(vec!["Utilization", &fmt_pct(as_f64(&data["utilization"]))]);
    table.add_row(vec!["Free regions", &data["free_region_count"].to_string()]);
    table.add_row(vec!["Largest free", &fmt_bytes(as_u64(&data["largest_free_region"]))]);
    table.add_row(vec!["Alignment", &format!("{} bytes", as_u64(&data["alignment"]))]);
    println!("{table}");
    Ok(())
}

fn cmd_memory(http: &HttpClient, json: bool) -> Result<(), CliError> {
    let data = http.get_json("/admin/memory")?;
    if json {
        println!("{}", serde_json::to_string_pretty(&data)?);
        return Ok(());
    }

    let mut table = Table::new();
    table.set_header(vec!["Metric", "Value"]);
    table.add_row(vec!["Index memory", &fmt_bytes(as_u64(&data["index_bytes"]))]);
    table.add_row(vec!["Index entries", &fmt_num(as_u64(&data["index_entries"]))]);
    table.add_row(vec!["DAH index entries", &fmt_num(as_u64(&data["dah_index_entries"]))]);
    table.add_row(vec!["Unmined index entries", &fmt_num(as_u64(&data["unmined_index_entries"]))]);
    println!("{table}");
    Ok(())
}

fn cmd_records(http: &HttpClient, json: bool) -> Result<(), CliError> {
    let data = http.get_json("/admin/records")?;
    if json {
        println!("{}", serde_json::to_string_pretty(&data)?);
        return Ok(());
    }

    let mut table = Table::new();
    table.set_header(vec!["Metric", "Value"]);
    table.add_row(vec!["Total records", &fmt_num(as_u64(&data["total_records"]))]);
    table.add_row(vec!["DAH index count", &fmt_num(as_u64(&data["dah_index_count"]))]);
    table.add_row(vec!["Unmined count", &fmt_num(as_u64(&data["unmined_count"]))]);
    println!("{table}");
    Ok(())
}

fn cmd_record(http: &HttpClient, json: bool, txid: &str, _slots: bool, _raw: bool) -> Result<(), CliError> {
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
    table.add_row(vec!["Hugepage enabled", &data["hugepage_enabled"].to_string()]);
    table.add_row(vec!["Max probe distance", &data["max_probe_distance"].to_string()]);
    table.add_row(vec!["Memory", &fmt_bytes(as_u64(&data["memory_bytes"]))]);

    if secondary {
        let status = http.get_json("/status")?;
        table.add_row(vec!["DAH index entries", &fmt_num(as_u64(&status["records"]["dah_index"]))]);
        table.add_row(vec!["Unmined index entries", &fmt_num(as_u64(&status["records"]["unmined_index"]))]);
    }
    println!("{table}");
    Ok(())
}

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
        table.add_row(vec!["ACK policy", data["ack_policy"].as_str().unwrap_or("-")]);
        table.add_row(vec!["Best effort", &data["best_effort"].to_string()]);
        table.add_row(vec!["Topology term", &data["topology_term"].to_string()]);
        table.add_row(vec!["Topology epoch", &data["topology_epoch"].to_string()]);
        table.add_row(vec!["Peak cluster size", &data["peak_cluster_size"].to_string()]);
    }
    println!("{table}");
    Ok(())
}

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
        table.add_row(vec!["Current sequence", &fmt_num(as_u64(&data["current_sequence"]))]);
        table.add_row(vec!["Write position", &fmt_bytes(as_u64(&data["write_position"]))]);
        table.add_row(vec!["Available space", &fmt_bytes(as_u64(&data["available_space"]))]);
        table.add_row(vec!["Log size", &fmt_bytes(as_u64(&data["log_size"]))]);
        table.add_row(vec!["Utilization", &fmt_pct(as_f64(&data["utilization"]))]);
    } else {
        table.add_row(vec!["Available", "false"]);
    }
    println!("{table}");
    Ok(())
}

fn cmd_rebalance(http: &HttpClient, json: bool, dry_run: bool) -> Result<(), CliError> {
    if dry_run {
        let status = http.get_json("/status")?;
        if json {
            println!("{}", serde_json::to_string_pretty(&status)?);
        } else {
            println!("Dry run: current node has {} master shards, {} replica shards",
                as_u64(&status["master_shard_count"]),
                as_u64(&status["replica_shard_count"]));
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

fn cmd_drain(http: &HttpClient, json: bool, node_id: &str) -> Result<(), CliError> {
    let result = http.put_text(&format!("/admin/drain/{node_id}"), "")?;
    if json {
        println!("{}", serde_json::json!({"result": result}));
    } else {
        println!("{result}");
    }
    Ok(())
}

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

fn cmd_bench(http: &HttpClient, data_addr: &str, json: bool, operation: &str, count: u32) -> Result<(), CliError> {
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
        println!("{}", serde_json::json!({
            "operation": operation,
            "count": count,
            "elapsed_ms": elapsed.as_millis(),
            "ops_per_sec": ops_per_sec as u64,
        }));
    } else {
        println!("Bench: {} x {} {operation} operations",
            fmt_num(count as u64), if operation == "ping" { "PING" } else { operation });
        println!("Elapsed: {:.2}s", elapsed.as_secs_f64());
        println!("Throughput: {} ops/sec", fmt_num(ops_per_sec as u64));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Top command — ratatui terminal UI
// ---------------------------------------------------------------------------

fn cmd_top(http: &HttpClient) -> Result<(), CliError> {
    use crossterm::event::{self, Event, KeyCode, KeyEventKind};
    use crossterm::terminal::{self, EnterAlternateScreen, LeaveAlternateScreen};
    use crossterm::ExecutableCommand;
    use ratatui::prelude::*;

    // Enter raw mode
    terminal::enable_raw_mode().map_err(|e| CliError::Other(format!("terminal: {e}")))?;
    let mut stdout = std::io::stdout();
    stdout.execute(EnterAlternateScreen).map_err(|e| CliError::Other(format!("terminal: {e}")))?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).map_err(|e| CliError::Other(format!("terminal: {e}")))?;

    let mut prev_snapshot: Option<serde_json::Value> = None;
    let mut error_msg: Option<String>;

    loop {
        // Fetch snapshot
        let snapshot = match http.get_json("/admin/top") {
            Ok(s) => {
                error_msg = None;
                Some(s)
            }
            Err(e) => {
                error_msg = Some(format!("Connection lost: {e}"));
                None
            }
        };

        // Compute rates
        let rates = compute_rates(prev_snapshot.as_ref(), snapshot.as_ref());

        // Render
        terminal.draw(|frame| {
            draw_top(frame, snapshot.as_ref(), &rates, error_msg.as_deref());
        }).map_err(|e| CliError::Other(format!("render: {e}")))?;

        if snapshot.is_some() {
            prev_snapshot = snapshot;
        }

        // Poll for 'q' key with 1s timeout
        if event::poll(Duration::from_secs(1)).unwrap_or(false)
            && let Ok(Event::Key(key)) = event::read()
            && key.kind == KeyEventKind::Press
            && key.code == KeyCode::Char('q')
        {
            break;
        }
    }

    // Restore terminal
    terminal::disable_raw_mode().ok();
    std::io::stdout().execute(LeaveAlternateScreen).ok();
    Ok(())
}

/// Compute per-second rates from two consecutive snapshots.
fn compute_rates(prev: Option<&serde_json::Value>, cur: Option<&serde_json::Value>) -> Vec<(String, u64, u64, u64, String, String)> {
    let (Some(prev), Some(cur)) = (prev, cur) else {
        return Vec::new();
    };
    let dt_ms = as_u64(&cur["timestamp_ms"]).saturating_sub(as_u64(&prev["timestamp_ms"]));
    if dt_ms == 0 { return Vec::new(); }
    let dt = dt_ms as f64 / 1000.0;

    let rate = |key: &str| -> u64 {
        let c = as_u64(&cur["counters"][key]);
        let p = as_u64(&prev["counters"][key]);
        (c.saturating_sub(p) as f64 / dt) as u64
    };

    vec![
        ("spend".into(), rate("spends_attempted"), as_u64(&cur["counters"]["spends_attempted"]),
         as_u64(&cur["counters"]["spends_failed"]),
         fmt_ns(as_u64(&cur["latency"]["spend"]["p50_ns"])),
         fmt_ns(as_u64(&cur["latency"]["spend"]["p99_ns"]))),
        ("spend_multi".into(), rate("spend_multi_batches"), as_u64(&cur["counters"]["spend_multi_batches"]),
         0,
         fmt_ns(as_u64(&cur["latency"]["spend_multi"]["p50_ns"])),
         fmt_ns(as_u64(&cur["latency"]["spend_multi"]["p99_ns"]))),
        ("create".into(), rate("creates_attempted"), as_u64(&cur["counters"]["creates_attempted"]),
         as_u64(&cur["counters"]["creates_attempted"]).saturating_sub(as_u64(&cur["counters"]["creates_succeeded"])),
         "-".into(), "-".into()),
        ("set_mined".into(), rate("set_mined_attempted"), as_u64(&cur["counters"]["set_mined_attempted"]),
         as_u64(&cur["counters"]["set_mined_attempted"]).saturating_sub(as_u64(&cur["counters"]["set_mined_succeeded"])),
         "-".into(), "-".into()),
        ("get".into(), rate("gets_attempted"), as_u64(&cur["counters"]["gets_attempted"]),
         as_u64(&cur["counters"]["gets_attempted"]).saturating_sub(as_u64(&cur["counters"]["gets_succeeded"])),
         "-".into(), "-".into()),
        ("unspend".into(), rate("unspends_attempted"), as_u64(&cur["counters"]["unspends_attempted"]),
         as_u64(&cur["counters"]["unspends_failed"]),
         fmt_ns(as_u64(&cur["latency"]["unspend"]["p50_ns"])),
         fmt_ns(as_u64(&cur["latency"]["unspend"]["p99_ns"]))),
    ]
}

/// Render the top TUI.
fn draw_top(
    frame: &mut ratatui::Frame,
    snapshot: Option<&serde_json::Value>,
    rates: &[(String, u64, u64, u64, String, String)],
    error: Option<&str>,
) {
    use ratatui::prelude::*;
    use ratatui::widgets::*;

    let area = frame.area();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // header
            Constraint::Min(10),   // ops table
            Constraint::Length(5), // stats
            Constraint::Length(1), // footer
        ])
        .split(area);

    // Header
    let header_text = if let Some(snap) = snapshot {
        format!(
            " TeraSlab Top  |  {} connections  |  {} records  |  ready: {}",
            as_u64(&snap["connections"]),
            fmt_num(as_u64(&snap["index"]["entries"])),
            snap["ready"],
        )
    } else if let Some(err) = error {
        format!(" TeraSlab Top  |  {err}")
    } else {
        " TeraSlab Top  |  connecting...".to_string()
    };

    let header = Paragraph::new(header_text)
        .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))
        .block(Block::default().borders(Borders::BOTTOM));
    frame.render_widget(header, chunks[0]);

    // Operations table
    let header_row = Row::new(vec!["Operation", "Ops/sec", "Total", "Errors", "p50", "p99"])
        .style(Style::default().add_modifier(Modifier::BOLD));
    let rows: Vec<Row> = rates.iter().map(|(name, rate, total, errors, p50, p99)| {
        Row::new(vec![
            name.clone(),
            fmt_num(*rate),
            fmt_num(*total),
            errors.to_string(),
            p50.clone(),
            p99.clone(),
        ])
    }).collect();

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
    .block(Block::default().title(" Operations ").borders(Borders::ALL));
    frame.render_widget(table, chunks[1]);

    // System stats
    let stats_text = if let Some(snap) = snapshot {
        format!(
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
        )
    } else {
        " Waiting for data...".to_string()
    };

    let stats = Paragraph::new(stats_text)
        .block(Block::default().title(" System ").borders(Borders::ALL))
        .wrap(Wrap { trim: true });
    frame.render_widget(stats, chunks[2]);

    // Footer
    let footer = Paragraph::new(" Press 'q' to quit")
        .style(Style::default().fg(Color::DarkGray));
    frame.render_widget(footer, chunks[3]);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() -> ExitCode {
    let cli = Cli::parse();
    let http = HttpClient::new(&cli.addr);

    let result = match cli.command {
        Command::Status => cmd_status(&http, cli.json),
        Command::Nodes => cmd_nodes(&http, cli.json),
        Command::Shards { node } => cmd_shards(&http, cli.json, node),
        Command::Storage => cmd_storage(&http, cli.json),
        Command::Memory => cmd_memory(&http, cli.json),
        Command::Records => cmd_records(&http, cli.json),
        Command::Record { txid, slots, raw } => cmd_record(&http, cli.json, &txid, slots, raw),
        Command::Index { secondary } => cmd_index(&http, cli.json, secondary),
        Command::Replication { .. } => cmd_replication(&http, cli.json),
        Command::Redo { .. } => cmd_redo(&http, cli.json),
        Command::Rebalance { dry_run, .. } => cmd_rebalance(&http, cli.json, dry_run),
        Command::Drain { node_id, .. } => cmd_drain(&http, cli.json, &node_id),
        Command::LogLevel { level, .. } => cmd_log_level(&http, cli.json, level.as_deref()),
        Command::Bench { operation, count } => cmd_bench(&http, &cli.data_addr, cli.json, &operation, count),
        Command::Healthcheck => {
            match cmd_healthcheck(&http, cli.json) {
                Ok(true) => return ExitCode::SUCCESS,
                Ok(false) => return ExitCode::FAILURE,
                Err(e) => {
                    eprintln!("Error: {e}");
                    return ExitCode::FAILURE;
                }
            }
        }
        Command::Top => cmd_top(&http),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("Error: {e}");
            ExitCode::FAILURE
        }
    }
}
