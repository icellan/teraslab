//! CLI integration tests.
//!
//! Starts a test HTTP server and runs teraslab-cli commands against it,
//! verifying output format and exit codes.

use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize};
use std::sync::{Arc, Mutex};

/// Serializes the racy "pick an ephemeral port, drop the probe listener, let
/// the server re-bind it" window across the suite's parallel tests. Without it
/// two tests can pick the SAME just-freed ephemeral port in their
/// bind→drop→rebind gap, and the loser's server bind fails — a port-collision
/// flake that scales with test parallelism (hence worse on many-core CI). Held
/// through the readiness poll so the port is firmly owned by this test's server
/// before the next test starts.
static SERVER_STARTUP: Mutex<()> = Mutex::new(());

use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::metrics::{ThreadHistograms, ThreadMetrics};
use teraslab::ops::engine::Engine;
use teraslab::server::http::{HttpState, start_http_server};

static CLI_METRICS: ThreadMetrics = ThreadMetrics::new();
static CLI_HISTOGRAMS: ThreadHistograms = ThreadHistograms::new();

/// The bearer token wired into both the test HTTP server and the CLI under
/// test. R-056 makes the gated `/admin/*` and `/debug/*` routes require it.
const CLI_TEST_ADMIN_TOKEN: &str = "cli-integration-test-token";

fn start_test_server() -> u16 {
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

    // Serialize port selection + server bind + readiness across parallel tests
    // (see SERVER_STARTUP). Recover from a poisoned lock — a panic in another
    // test's startup must not cascade-fail every later test.
    let _startup = SERVER_STARTUP.lock().unwrap_or_else(|e| e.into_inner());

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let state = Arc::new(HttpState {
        engine,
        metrics: &CLI_METRICS,
        histograms: &CLI_HISTOGRAMS,
        ready: Arc::new(AtomicBool::new(true)),
        log_level: Arc::new(AtomicU8::new(2)),
        cluster: None,
        redo_log: None,
        redo_atomics: None,
        active_connections: Arc::new(AtomicUsize::new(0)),
        http_port: 0,
        replica_lag_warn_threshold_ops: 10_000,
        replica_lag_cache: std::sync::atomic::AtomicU64::new(0),
    });

    let addr = format!("127.0.0.1:{port}");
    let server_addr = addr.clone();
    std::thread::spawn(move || {
        // CLI integration covers /admin/* + /debug/* paths — register them.
        // R-056: gated routes need a bearer token; the CLI under test passes
        // the matching `--admin-token` so every command authenticates.
        start_http_server(
            server_addr,
            state,
            true,
            Some(CLI_TEST_ADMIN_TOKEN.to_string()),
        );
    });
    // Poll until the spawned server actually SERVES an HTTP request, rather
    // than a fixed sleep or a bare TCP connect. A 200 ms sleep flaked when the
    // server thread had not bound the port yet; a bare `TcpStream::connect`
    // also flaked because the kernel completes the TCP handshake from the
    // listen backlog BEFORE axum's accept loop is running — so the connect
    // succeeds while the first real HTTP request still races the (contended,
    // on macOS) runtime startup and gets a reset / no response, and the CLI
    // exits non-zero. Doing a full HTTP round-trip here proves axum is
    // processing requests before the test's CLI runs. `/health` is
    // unauthenticated; ANY HTTP status line (even an error) means the server
    // is serving.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if http_server_responds(&addr) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "test HTTP server did not serve an HTTP response on {addr} within 10s",
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    port
}

/// Open a connection, send a minimal HTTP/1.1 GET, and return `true` once a
/// status line (`HTTP/…`) comes back — i.e. axum is actually serving, not just
/// bound. Any failure (connect, write, read, timeout, non-HTTP bytes) is
/// `false` so the caller keeps polling.
fn http_server_responds(addr: &str) -> bool {
    use std::io::{Read, Write};
    let Ok(mut stream) = std::net::TcpStream::connect(addr) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_millis(500)));
    let _ = stream.set_write_timeout(Some(std::time::Duration::from_millis(500)));
    let req = format!("GET /health HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    if stream.write_all(req.as_bytes()).is_err() {
        return false;
    }
    let mut buf = [0u8; 16];
    match stream.read(&mut buf) {
        Ok(n) if n > 0 => buf[..n].starts_with(b"HTTP/"),
        _ => false,
    }
}

fn cli_bin() -> String {
    // Find the compiled CLI binary
    let mut path = std::env::current_exe().unwrap();
    path.pop(); // remove test binary name
    path.pop(); // remove 'deps'
    path.push("teraslab-cli");
    path.to_string_lossy().to_string()
}

fn run_cli(port: u16, args: &[&str]) -> (String, String, i32) {
    let addr = format!("http://127.0.0.1:{port}");
    let output = Command::new(cli_bin())
        .arg("--addr")
        .arg(&addr)
        .arg("--admin-token")
        .arg(CLI_TEST_ADMIN_TOKEN)
        // Defensive: scrub the env override so the parent process's
        // TERASLAB_ADMIN_TOKEN (if any) doesn't sneak in.
        .env_remove("TERASLAB_ADMIN_TOKEN")
        .args(args)
        .output()
        .expect("failed to run teraslab-cli");

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let code = output.status.code().unwrap_or(-1);
    (stdout, stderr, code)
}

#[test]
fn cli_status_returns_overview() {
    let port = start_test_server();
    let (stdout, _, code) = run_cli(port, &["status"]);
    assert_eq!(code, 0);
    assert!(
        stdout.contains("TeraSlab Cluster Status"),
        "stdout: {stdout}"
    );
    assert!(stdout.contains("Records:"), "stdout: {stdout}");
}

#[test]
fn cli_status_json_is_valid() {
    let port = start_test_server();
    let (stdout, _, code) = run_cli(port, &["--json", "status"]);
    assert_eq!(code, 0);
    let parsed: serde_json::Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("invalid JSON: {e}\nstdout: {stdout}"));
    assert!(parsed["status"].is_object());
    assert!(parsed["index"].is_object());
}

#[test]
fn cli_nodes_lists_nodes() {
    let port = start_test_server();
    let (stdout, _, code) = run_cli(port, &["nodes"]);
    assert_eq!(code, 0);
    assert!(
        stdout.contains("Node ID") || stdout.contains("node_id"),
        "stdout: {stdout}"
    );
}

#[test]
fn cli_storage_shows_utilization() {
    let port = start_test_server();
    let (stdout, _, code) = run_cli(port, &["storage"]);
    assert_eq!(code, 0);
    assert!(
        stdout.contains("Device size") || stdout.contains("device_size"),
        "stdout: {stdout}"
    );
}

#[test]
fn cli_memory_shows_breakdown() {
    let port = start_test_server();
    let (stdout, _, code) = run_cli(port, &["memory"]);
    assert_eq!(code, 0);
    assert!(
        stdout.contains("Index memory") || stdout.contains("index_bytes"),
        "stdout: {stdout}"
    );
}

#[test]
fn cli_records_shows_inventory() {
    let port = start_test_server();
    let (stdout, _, code) = run_cli(port, &["records"]);
    assert_eq!(code, 0);
    assert!(
        stdout.contains("Total records") || stdout.contains("total_records"),
        "stdout: {stdout}"
    );
}

#[test]
fn cli_record_not_found() {
    let port = start_test_server();
    let txid = "0000000000000000000000000000000000000000000000000000000000000000";
    let (_, stderr, code) = run_cli(port, &["record", txid]);
    assert_ne!(code, 0);
    assert!(
        stderr.contains("not found") || stderr.contains("404"),
        "stderr: {stderr}"
    );
}

#[test]
fn cli_index_shows_stats() {
    let port = start_test_server();
    let (stdout, _, code) = run_cli(port, &["index"]);
    assert_eq!(code, 0);
    assert!(
        stdout.contains("Entries") || stdout.contains("entries"),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains("Load factor") || stdout.contains("load_factor"),
        "stdout: {stdout}"
    );
}

#[test]
fn cli_replication_shows_status() {
    let port = start_test_server();
    let (stdout, _, code) = run_cli(port, &["replication"]);
    assert_eq!(code, 0);
    assert!(
        stdout.contains("Enabled") || stdout.contains("enabled"),
        "stdout: {stdout}"
    );
}

#[test]
fn cli_redo_shows_info() {
    let port = start_test_server();
    let (stdout, _, code) = run_cli(port, &["redo"]);
    assert_eq!(code, 0);
    assert!(
        stdout.contains("Available") || stdout.contains("available"),
        "stdout: {stdout}"
    );
}

#[test]
fn cli_log_level_get() {
    let port = start_test_server();
    let (stdout, _, code) = run_cli(port, &["log-level"]);
    assert_eq!(code, 0);
    assert!(stdout.contains("info"), "stdout: {stdout}");
}

#[test]
fn cli_log_level_set() {
    let port = start_test_server();
    let (stdout, _, code) = run_cli(port, &["log-level", "debug"]);
    assert_eq!(code, 0);
    assert!(stdout.contains("debug"), "stdout: {stdout}");
}

#[test]
fn cli_healthcheck_returns_zero_exit_code() {
    let port = start_test_server();
    let (stdout, _, code) = run_cli(port, &["healthcheck"]);
    assert_eq!(code, 0, "healthcheck should succeed, stdout: {stdout}");
}

#[test]
fn cli_healthcheck_nonzero_when_unreachable() {
    // Point at a port nothing is listening on
    let output = Command::new(cli_bin())
        .arg("--addr")
        .arg("http://127.0.0.1:1")
        .arg("healthcheck")
        .output()
        .expect("failed to run teraslab-cli");
    assert_ne!(output.status.code().unwrap_or(0), 0);
}

/// AUDIT §5(b)5: these flags used to parse but were silently ignored —
/// an operator trap. They were removed; clap must now reject them with a
/// usage error (exit code 2) instead of pretending to honor them.
#[test]
fn cli_removed_noop_flags_are_rejected() {
    let removed: &[&[&str]] = &[
        &["shards", "--node", "1"],
        &["record", "deadbeef", "--slots"],
        &["record", "deadbeef", "--raw"],
        &["replication", "--history"],
        &["redo", "--tail", "5"],
        &["rebalance", "--execute"],
        &["drain", "1", "--cancel"],
        &["log-level", "debug", "--target", "teraslab::ops"],
    ];
    for args in removed {
        // No server needed: clap rejects the argv before any connection.
        let output = Command::new(cli_bin())
            .args(*args)
            .output()
            .expect("failed to run teraslab-cli");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            output.status.code(),
            Some(2),
            "args {args:?} should fail argument parsing, stderr: {stderr}"
        );
        assert!(
            stderr.contains("unexpected argument"),
            "args {args:?} stderr should name the rejected flag: {stderr}"
        );
    }
}

#[test]
fn cli_all_commands_json_valid() {
    let port = start_test_server();
    let commands = vec![
        vec!["status"],
        vec!["nodes"],
        vec!["storage"],
        vec!["memory"],
        vec!["records"],
        vec!["index"],
        vec!["replication"],
        vec!["redo"],
        vec!["healthcheck"],
        vec!["shards"],
        vec!["log-level"],
    ];

    for cmd in commands {
        let mut args = vec!["--json"];
        args.extend(cmd.iter());
        let (stdout, stderr, code) = run_cli(port, &args);
        assert_eq!(
            code, 0,
            "command {:?} failed (exit {}): stderr={stderr}",
            cmd, code
        );
        let parsed: Result<serde_json::Value, _> = serde_json::from_str(&stdout);
        assert!(
            parsed.is_ok(),
            "command {:?} returned invalid JSON: {stdout}",
            cmd
        );
    }
}

/// Roundtrip the offline index-migration commands (5c, 2026-05-29 audit:
/// the README runbook documented these subcommands but they did not
/// exist). Populate an in-memory index snapshot, `export-index` it to
/// the portable format, `import-index` into a fresh redb-configured
/// layout, and verify entry counts and a spot-checked entry survive
/// byte-for-byte.
#[test]
fn export_import_index_roundtrip() {
    use teraslab::config::ServerConfig;
    use teraslab::index::{DahBackend, PrimaryBackend, TxIndexEntry, TxKey, UnminedBackend};

    let tmp = tempfile::TempDir::new().unwrap();

    // Source: memory backend with a populated snapshot on disk.
    let snap_path = tmp.path().join("src-index.snap");
    {
        let mut primary = PrimaryBackend::new_in_memory(100).unwrap();
        for n in 0..25u8 {
            let mut txid = [0u8; 32];
            txid[0] = n;
            txid[1] = 0xAB;
            primary
                .register(
                    TxKey { txid },
                    TxIndexEntry {
                        device_id: 0,
                        record_offset: 4096 * n as u64,
                        utxo_count: 3,
                        block_entry_count: 0,
                        tx_flags: 0,
                        spent_utxos: 1,
                        dah_or_preserve: 0,
                        unmined_since: 0,
                        generation: n as u32,
                    },
                )
                .unwrap();
        }
        let dah = DahBackend::new_in_memory();
        let unmined = UnminedBackend::new_in_memory();
        primary.snapshot_all(&dah, &unmined, &snap_path).unwrap();
    }
    let src_cfg_path = tmp.path().join("src.toml");
    std::fs::write(
        &src_cfg_path,
        format!("index_snapshot_path = {:?}\n", snap_path),
    )
    .unwrap();

    // Export through the CLI binary.
    let export_path = tmp.path().join("portable.tsmi");
    let out = Command::new(cli_bin())
        .arg("export-index")
        .arg("--config")
        .arg(&src_cfg_path)
        .arg("--output")
        .arg(&export_path)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "export-index failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(export_path.exists(), "portable file must exist");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("25 primary"),
        "export must report 25 primary entries, got: {stdout}"
    );

    // Destination: redb backend in a fresh directory.
    let dst_dir = tmp.path().join("dst");
    std::fs::create_dir(&dst_dir).unwrap();
    let dst_cfg_path = tmp.path().join("dst.toml");
    std::fs::write(
        &dst_cfg_path,
        format!(
            "[index]\nbackend = \"redb\"\nredb_path = {:?}\nredb_dah_path = {:?}\nredb_unmined_path = {:?}\n",
            dst_dir.join("primary.redb"),
            dst_dir.join("dah.redb"),
            dst_dir.join("unmined.redb"),
        ),
    )
    .unwrap();
    let out = Command::new(cli_bin())
        .arg("import-index")
        .arg("--config")
        .arg(&dst_cfg_path)
        .arg("--input")
        .arg(&export_path)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "import-index failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Verify through the library: counts match and an entry survives intact.
    let dst_cfg = ServerConfig::load(&dst_cfg_path).unwrap();
    let primary = PrimaryBackend::restore_redb(&dst_cfg.index).unwrap();
    assert_eq!(primary.len(), 25, "all entries must survive the roundtrip");
    let mut txid = [0u8; 32];
    txid[0] = 7;
    txid[1] = 0xAB;
    let e = primary
        .lookup(&TxKey { txid })
        .expect("entry 7 survives roundtrip");
    assert_eq!(e.record_offset, 4096 * 7);
    assert_eq!(e.utxo_count, 3);
    assert_eq!(e.spent_utxos, 1);
    assert_eq!(e.generation, 7);
}
