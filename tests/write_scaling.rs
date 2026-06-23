//! Real-TCP write-path scaling + observability-isolation harness.
//!
//! Phase 0 captures baseline numbers (no scaling assertion). The scaling
//! assertion (8 clients >= N x 1 client) is enabled once the write path is
//! parallelized (plan Task 2c). The heavy multi-client run is gated behind
//! the `slow-tests` feature; a small smoke runs per-PR.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize};
use std::time::{Duration, Instant};

/// Total process CPU time (user+sys, all threads) via getrusage(RUSAGE_SELF).
#[cfg(feature = "slow-tests")]
fn process_cpu_time() -> Duration {
    // SAFETY: getrusage with a zeroed rusage out-param is always sound.
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut ru);
        let secs = (ru.ru_utime.tv_sec + ru.ru_stime.tv_sec) as u64;
        let usecs = (ru.ru_utime.tv_usec + ru.ru_stime.tv_usec) as u64;
        Duration::from_secs(secs) + Duration::from_micros(usecs)
    }
}

use parking_lot::Mutex;

use teraslab::allocator::SlotAllocator;
use teraslab::config::ServerConfig;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::metrics::{ThreadHistograms, ThreadMetrics};
use teraslab::ops::engine::Engine;
use teraslab::protocol::codec::{WireCreateItem, encode_create_batch};
use teraslab::protocol::frame::{RequestFrame, ResponseFrame};
use teraslab::protocol::opcodes::{OP_CREATE_BATCH, STATUS_OK, STATUS_PARTIAL_ERROR};
use teraslab::redo::RedoLog;
use teraslab::server::Server;
use teraslab::server::dispatch::init_dispatch_metrics;
use teraslab::server::http::{HttpState, start_http_server};

// Process-global metrics shared between the dispatch path (writes) and the
// HTTP snapshot (reads). One integration-test file = one process, so a single
// init is safe.
static TEST_METRICS: ThreadMetrics = ThreadMetrics::new();
static TEST_HISTOGRAMS: ThreadHistograms = ThreadHistograms::new();
const ADMIN_TOKEN: &str = "test-admin-token-write-scaling";

pub struct WriteServer {
    server: Arc<Server>,
    pub tcp_port: u16,
    pub http_port: u16,
}

impl Drop for WriteServer {
    fn drop(&mut self) {
        self.server.shutdown();
    }
}

/// Build engine + redo log, start the TCP data server and the HTTP admin
/// server on separate loopback ports, wire the shared metrics.
pub fn spawn_write_server() -> WriteServer {
    // init_dispatch_metrics uses a OnceLock; ignore a second call.
    init_dispatch_metrics(&TEST_METRICS);

    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(1024 * 1024 * 1024, 4096).unwrap());
    let alloc = SlotAllocator::new(dev.clone()).unwrap();
    let index = Index::new(2_000_000).unwrap();
    let engine = Arc::new(Engine::new(
        dev,
        index,
        alloc,
        StripedLocks::new(65536),
        DahIndex::new(),
        UnminedIndex::new(),
    ));

    // 512-byte alignment keeps per-entry padding small enough that 256 MiB
    // holds 400 K+ entries (8 clients × 50 K ops in the slow-tests run).
    // With 4096-byte alignment the redo log fills after ~65535 entries,
    // which would cut off the 8-client baseline run prematurely.
    let redo_dev: Arc<dyn BlockDevice> =
        Arc::new(MemoryDevice::new(256 * 1024 * 1024, 512).unwrap());
    let redo = Arc::new(Mutex::new(
        RedoLog::open(redo_dev, 0, 256 * 1024 * 1024).unwrap(),
    ));

    let tcp_port = free_port();
    let http_port = free_port();
    let active = Arc::new(AtomicUsize::new(0));

    let config = ServerConfig {
        listen_addr: format!("127.0.0.1:{tcp_port}"),
        max_connections: 64,
        max_batch_size: 8192,
        ..Default::default()
    };
    let server = Arc::new(
        Server::new(engine.clone(), config)
            .with_redo_log(redo.clone())
            .with_active_connections(active.clone()),
    );
    let server_run = server.clone();
    std::thread::spawn(move || {
        let _ = server_run.run();
    });

    let redo_atomics = redo.lock().atomics();
    let state = Arc::new(HttpState {
        engine,
        metrics: &TEST_METRICS,
        histograms: &TEST_HISTOGRAMS,
        ready: Arc::new(AtomicBool::new(true)),
        log_level: Arc::new(AtomicU8::new(2)),
        cluster: None,
        redo_atomics: Some(redo_atomics),
        redo_log: Some(redo),
        active_connections: active,
        http_port,
        replica_lag_warn_threshold_ops: 10_000,
        replica_lag_cache: AtomicU64::new(0),
    });
    let http_addr = format!("127.0.0.1:{http_port}");
    std::thread::spawn(move || {
        start_http_server(http_addr, state, true, Some(ADMIN_TOKEN.to_string()));
    });

    std::thread::sleep(Duration::from_millis(200));
    WriteServer {
        server,
        tcp_port,
        http_port,
    }
}

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

fn make_tx_id(client: u32, n: u32) -> [u8; 32] {
    let mut t = [0u8; 32];
    t[0..4].copy_from_slice(&n.to_le_bytes());
    t[4..8].copy_from_slice(&client.to_le_bytes());
    t[16..20].copy_from_slice(
        &n.wrapping_mul(0x9E37_79B9)
            .wrapping_add(client)
            .to_le_bytes(),
    );
    t
}

fn make_create_item(txid: [u8; 32]) -> WireCreateItem {
    let mut uh = [0u8; 32];
    uh[0..4].copy_from_slice(&txid[0..4]);
    WireCreateItem {
        txid,
        tx_version: 2,
        locktime: 0,
        fee: 1000,
        size_in_bytes: 250,
        extended_size: 0,
        is_coinbase: false,
        spending_height: 0,
        created_at: 1_700_000_000_000,
        flags: 0,
        utxo_hashes: vec![uh],
        cold_data: vec![],
        block_height: 0,
        mined_block_id: None,
        mined_block_height: None,
        mined_subtree_idx: None,
        parent_txids: vec![],
    }
}

fn send_frame(stream: &mut TcpStream, frame: &RequestFrame) -> ResponseFrame {
    let bytes = frame.encode();
    stream.write_all(&bytes).unwrap();
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).unwrap();
    let total = u32::from_le_bytes(len_buf) as usize;
    let mut body = vec![0u8; total];
    stream.read_exact(&mut body).unwrap();
    let mut full = Vec::with_capacity(4 + total);
    full.extend_from_slice(&len_buf);
    full.extend_from_slice(&body);
    let (resp, _) = ResponseFrame::decode(&full).unwrap();
    resp
}

/// One client connection creating `n` records (one per CreateBatch). Returns
/// (acked records, elapsed). Counts STATUS_OK and STATUS_PARTIAL_ERROR-with-
/// no-error as acked.
pub fn drive_creates(tcp_port: u16, client_id: u32, n: u32) -> (u64, Duration) {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{tcp_port}")).unwrap();
    stream.set_nodelay(true).unwrap();
    let mut acked = 0u64;
    let start = Instant::now();
    for i in 0..n {
        let item = make_create_item(make_tx_id(client_id, i));
        let payload = encode_create_batch(&[item]);
        let resp = send_frame(
            &mut stream,
            &RequestFrame {
                request_id: i as u64,
                op_code: OP_CREATE_BATCH,
                flags: 0,
                payload: payload.into(),
            },
        );
        if resp.status == STATUS_OK || resp.status == STATUS_PARTIAL_ERROR {
            acked += 1;
        }
    }
    (acked, start.elapsed())
}

/// Run `clients` connections concurrently, each creating `per_client` records.
/// Returns (total acked, wall elapsed).
fn run_clients(tcp_port: u16, clients: u32, per_client: u32) -> (u64, Duration) {
    teraslab::metrics::reset_writers_max();
    let start = Instant::now();
    let mut totals = Vec::new();
    std::thread::scope(|s| {
        let handles: Vec<_> = (0..clients)
            .map(|c| s.spawn(move || drive_creates(tcp_port, c, per_client)))
            .collect();
        for h in handles {
            totals.push(h.join().unwrap().0);
        }
    });
    (totals.iter().sum(), start.elapsed())
}

fn ops_per_sec(acked: u64, elapsed: Duration) -> f64 {
    acked as f64 / elapsed.as_secs_f64()
}

// ---- per-PR smoke: cheap, no scaling bar; just proves the harness drives the
// real server and the gauge is reachable. ----
#[test]
fn write_scaling_smoke() {
    let srv = spawn_write_server();
    let (acked, el) = run_clients(srv.tcp_port, 4, 200);
    assert_eq!(acked, 800, "every create must be acked");
    println!(
        "[smoke] 4 clients x 200 = {acked} acked in {el:?} -> {:.0} ops/s, gauge_max={}",
        ops_per_sec(acked, el),
        teraslab::metrics::writers_in_flight_max()
    );
}

// ---- heavy baseline/scaling run: slow-tests only. Phase 0 = measurement,
// no assertion. Task 2c converts the printed ratio into an assertion. ----
#[cfg(feature = "slow-tests")]
#[test]
fn write_scaling_baseline_1_vs_8() {
    let per_client = 50_000u32;
    let srv = spawn_write_server();

    let (a1, e1) = run_clients(srv.tcp_port, 1, per_client);
    let one = ops_per_sec(a1, e1);
    let max1 = teraslab::metrics::writers_in_flight_max();

    let cpu0 = process_cpu_time();
    let (a8, e8) = run_clients(srv.tcp_port, 8, per_client);
    let cpu_used = process_cpu_time() - cpu0;
    let cores = cpu_used.as_secs_f64() / e8.as_secs_f64();
    let eight = ops_per_sec(a8, e8);
    let max8 = teraslab::metrics::writers_in_flight_max();

    println!("[baseline] 1 client : {a1} acked, {one:.0} ops/s, gauge_max={max1}");
    println!("[baseline] 8 clients: {a8} acked, {eight:.0} ops/s, gauge_max={max8}");
    println!("[baseline] scaling ratio (8/1) = {:.2}x", eight / one);
    println!("[baseline] 8-client CPU/wall ratio = {cores:.2} cores (reported, not asserted)");
    // PHASE 0: no assertion. See plan Task 2c.
}
