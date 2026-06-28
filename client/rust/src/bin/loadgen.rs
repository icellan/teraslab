//! Load generator for TeraSlab.
//!
//! Generates a mixed workload of creates, spends, reads, unlocks, and setMined
//! operations against a running TeraSlab server or cluster using the Rust client
//! library.
//!
//! The default workload is the sustained 1:1:1 create/spend/unlock mix used for
//! perf measurement. Per-op latency percentiles (p50/p99/p99.9) are measured
//! over a measurement window that begins after a warmup period, and an optional
//! concurrent setMined burst models "a block is mined every N seconds; setMined
//! must drain fast".
//!
//! Usage:
//!   teraslab-loadgen --addr localhost:3300 --rate 500 --duration 300
//!   teraslab-loadgen --addr localhost:3300 --saturate --workers 16 \
//!       --mix "create=1,spend=1,unlock=1" --burst-size 100000 --burst-interval-secs 60
//!   teraslab-loadgen --seeds localhost:3300,localhost:3310 --workers 8 --rate 2000
//!
//! To reproduce the legacy mixed workload (the old default before the
//! configurable mix), use `--mix "create=4,spend=3,get=2,setmined=1" --warmup-secs 0`.

// CLI binary: stderr/stdout output is the user-facing reporting channel, so
// the workspace-level `disallowed_macros` ban on eprintln!/println! does not
// apply here.
#![allow(clippy::disallowed_macros)]

use clap::Parser;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use teraslab_client::*;

/// Max txids per setMined RPC chunk in a burst. Matches the server's default
/// `max_batch_size` (4096); larger requests are rejected, so a burst over many
/// more txids is split into concurrent chunks of this size.
const BURST_MAX_BATCH: usize = 4096;

/// The op kinds we track latency for. Order is the canonical reporting order.
#[derive(Clone, Copy, PartialEq, Eq)]
enum OpKind {
    Create,
    Spend,
    Get,
    SetMined,
    Unlock,
}

impl OpKind {
    const ALL: [OpKind; 5] = [
        OpKind::Create,
        OpKind::Spend,
        OpKind::Get,
        OpKind::SetMined,
        OpKind::Unlock,
    ];

    fn name(self) -> &'static str {
        match self {
            OpKind::Create => "create",
            OpKind::Spend => "spend",
            OpKind::Get => "get",
            OpKind::SetMined => "setmined",
            OpKind::Unlock => "unlock",
        }
    }

    fn index(self) -> usize {
        match self {
            OpKind::Create => 0,
            OpKind::Spend => 1,
            OpKind::Get => 2,
            OpKind::SetMined => 3,
            OpKind::Unlock => 4,
        }
    }

    fn parse(s: &str) -> Option<OpKind> {
        match s {
            "create" => Some(OpKind::Create),
            "spend" => Some(OpKind::Spend),
            "get" => Some(OpKind::Get),
            "setmined" => Some(OpKind::SetMined),
            "unlock" => Some(OpKind::Unlock),
            _ => None,
        }
    }
}

/// A weighted op mix. `cumulative[i]` is the running sum of weights up to and
/// including op `i` (in `OpKind::ALL` order); `total` is the final sum. Selection
/// picks the first op whose cumulative threshold exceeds `r % total`.
#[derive(Clone)]
struct Mix {
    cumulative: [u64; 5],
    total: u64,
}

impl Mix {
    /// Parse a mix spec like "create=1,spend=1,unlock=1". Unlisted ops get
    /// weight 0. Returns an error string on an unknown op, a malformed pair, or
    /// an all-zero total.
    fn parse(spec: &str) -> Result<Mix, String> {
        let mut weights = [0u64; 5];
        for part in spec.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let (name, val) = part
                .split_once('=')
                .ok_or_else(|| format!("malformed mix entry (expected op=weight): {part:?}"))?;
            let op = OpKind::parse(name.trim())
                .ok_or_else(|| format!("unknown op in mix: {:?}", name.trim()))?;
            let w: u64 = val
                .trim()
                .parse()
                .map_err(|_| format!("invalid weight in mix entry: {part:?}"))?;
            weights[op.index()] = w;
        }
        let mut cumulative = [0u64; 5];
        let mut acc = 0u64;
        for (i, w) in weights.iter().enumerate() {
            acc += w;
            cumulative[i] = acc;
        }
        if acc == 0 {
            return Err("mix has zero total weight".to_string());
        }
        Ok(Mix {
            cumulative,
            total: acc,
        })
    }

    /// Select an op for the given random value by cumulative thresholds. `r` may
    /// be any u64; it is reduced modulo the total weight.
    fn select(&self, r: u64) -> OpKind {
        let point = r % self.total;
        for op in OpKind::ALL {
            if point < self.cumulative[op.index()] {
                return op;
            }
        }
        // Unreachable: point < total <= cumulative[last]. Fall back to last
        // non-zero op defensively rather than panicking in a bench.
        OpKind::ALL[4]
    }
}

/// Nearest-rank percentile (in micros) over a sorted, non-empty slice.
///
/// Uses the nearest-rank method: rank = ceil(p/100 * N), clamped to [1, N], and
/// returns `sorted[rank - 1]`. Returns 0.0 for an empty slice (no samples).
/// `p` is a percentile in [0, 100], e.g. 99.9 for p99.9.
fn percentile(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let n = sorted.len();
    let rank = (p / 100.0 * n as f64).ceil() as usize;
    let rank = rank.clamp(1, n);
    sorted[rank - 1] as f64
}

/// TeraSlab load generator.
#[derive(Parser)]
#[command(
    name = "teraslab-loadgen",
    about = "Generate mixed load against a TeraSlab server"
)]
struct Args {
    /// Server address for single-node mode (host:port).
    #[arg(long)]
    addr: Option<String>,

    /// Cluster seed addresses (comma-separated).
    #[arg(long)]
    seeds: Option<String>,

    /// Target operations per second (total across all workers). 0 = saturate
    /// (no inter-op sleep; equivalent to --saturate).
    #[arg(long, default_value = "500")]
    rate: u64,

    /// Saturation mode: workers issue ops with no inter-op sleep, pushing as
    /// hard as the worker count / in-flight allows. Use to find the throughput
    /// ceiling. Implied by --rate 0.
    #[arg(long, default_value = "false")]
    saturate: bool,

    /// Duration in seconds. This is the MEASUREMENT window; warmup runs in
    /// addition, before it.
    #[arg(long, default_value = "300")]
    duration: u64,

    /// Warmup seconds before the measurement window. Ops run during warmup but
    /// their latencies are not sampled and they are excluded from ops/sec.
    #[arg(long, default_value = "3")]
    warmup_secs: u64,

    /// Number of concurrent worker tasks.
    #[arg(long, default_value = "4")]
    workers: usize,

    /// Items per batched RPC. >1 amortizes the per-batch redo fsync across many
    /// items (one group-commit per RPC), which is how production pushes past the
    /// single-item fsync floor. Counters still tally individual items.
    #[arg(long, default_value = "1")]
    batch: usize,

    /// Weighted op mix, e.g. "create=1,spend=1,unlock=1". Recognized ops:
    /// create, spend, get, setmined, unlock. Unlisted ops get weight 0. Default
    /// is the sustained 1:1:1 create/spend/unlock target.
    #[arg(long, default_value = "create=1,spend=1,unlock=1")]
    mix: String,

    /// Number of outputs (utxo hashes) per created transaction. Default 1 for
    /// 1-in/1-out tx semantics (spend spends 1 output).
    #[arg(long, default_value = "1")]
    outputs_per_create: usize,

    /// setMined burst size: number of created txids snapshotted and drained per
    /// burst. 0 disables the burst task (default).
    #[arg(long, default_value = "0")]
    burst_size: usize,

    /// Interval between setMined bursts, in seconds.
    #[arg(long, default_value = "60")]
    burst_interval_secs: u64,

    /// Emit a machine-readable LOADGEN_RESULT {json} line in addition to the
    /// human-readable summary. On by default.
    #[arg(long, default_value = "true")]
    json: bool,

    /// Connection pool size. Defaults to the worker count so each worker holds
    /// its own connection and all `workers` RPCs are truly in flight at once
    /// (the single-node pool serves one round-trip per connection). Too few
    /// connections re-serializes the workers on the pool regardless of how many
    /// worker tasks there are.
    #[arg(long)]
    conns: Option<usize>,
}

/// Per-worker latency samples (micros), one Vec per op kind. Accumulated lock-
/// free in the worker's own task and merged at the end.
type WorkerSamples = [Vec<u64>; 5];

/// Result of one worker: its per-op latency samples plus per-op ok/failed
/// counts recorded during the measurement window.
struct WorkerResult {
    samples: WorkerSamples,
    ok: [u64; 5],
    failed: [u64; 5],
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    if args.addr.is_none() && args.seeds.is_none() {
        eprintln!("Must specify --addr or --seeds");
        std::process::exit(1);
    }

    let mix = match Mix::parse(&args.mix) {
        Ok(m) => Arc::new(m),
        Err(e) => {
            eprintln!("Invalid --mix: {e}");
            std::process::exit(1);
        }
    };
    let outputs_per_create = args.outputs_per_create.max(1);
    let saturate = args.saturate || args.rate == 0;

    // One connection per worker by default so all `workers` RPCs are concurrent.
    let conns = args.conns.unwrap_or(args.workers).max(4);
    let cfg = ClientConfig {
        addr: args.addr.clone(),
        seeds: args
            .seeds
            .as_ref()
            .map(|s| s.split(',').map(|x| x.trim().to_string()).collect())
            .unwrap_or_default(),
        pool: PoolConfig {
            // Open the FULL connection count up front. ConnPool::get() only
            // round-robins existing connections and never grows past what the
            // health loop maintains (min_conns), so a smaller min_conns would
            // silently cap the bench at min_conns sockets regardless of --conns.
            min_conns: conns,
            max_conns: conns,
            dial_timeout: Duration::from_secs(5),
            // Short health interval so the pool replenishes to `min_conns`
            // connections within the warmup window below. The pool only grows
            // toward min_conns on the health tick (get() never creates beyond
            // the first live conn), so a long interval would leave the bench
            // running on a single socket.
            health_check: Duration::from_millis(200),
            ..Default::default()
        },
        cluster_refresh_interval: Duration::from_secs(30),
        max_redirects: 3,
        addr_map: Default::default(),
        ..Default::default()
    };

    let client = match Client::new(cfg).await {
        Ok(c) => Arc::new(c),
        Err(e) => {
            eprintln!("Failed to connect: {e}");
            std::process::exit(1);
        }
    };

    match client.ping().await {
        Ok(rtt) => eprintln!("Connected (ping {rtt:?})"),
        Err(e) => {
            eprintln!("Ping failed: {e}");
            std::process::exit(1);
        }
    }

    // Warm up the connection pool to `conns` sockets before timing. The pool
    // fills toward min_conns on its (now 200ms) health tick rather than on
    // demand, so without this the timed run would otherwise start on a single
    // connection and ramp up mid-measurement.
    if conns > 1 {
        eprintln!("Warming up {conns} connections...");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    let shutdown = Arc::new(AtomicBool::new(false));
    // `measuring` flips true after warmup, false at end. Workers only sample
    // latencies and count toward the windowed results while it is true.
    let measuring = Arc::new(AtomicBool::new(false));
    let creates = Arc::new(AtomicU64::new(0));
    let spends = Arc::new(AtomicU64::new(0));
    let reads = Arc::new(AtomicU64::new(0));
    let mined_count = Arc::new(AtomicU64::new(0));
    let unlocks = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    // Actual concurrent in-flight RPCs (round-trips awaiting a reply) and its
    // high-water mark. If this stays far below `workers`, the client is NOT
    // offering the concurrency we think — the bottleneck is here, not the server.
    let inflight = Arc::new(AtomicU64::new(0));
    let inflight_hwm = Arc::new(AtomicU64::new(0));

    // Error categorization for debugging.
    let err_partial = Arc::new(AtomicU64::new(0));
    let err_redirect = Arc::new(AtomicU64::new(0));
    let err_connection = Arc::new(AtomicU64::new(0));
    let err_server = Arc::new(AtomicU64::new(0));
    let err_other = Arc::new(AtomicU64::new(0));
    let err_logged = Arc::new(AtomicU64::new(0)); // cap detail logging

    // Inter-op sleep target. In saturate mode there is no sleep. Otherwise
    // spread `rate` total ops/sec across the workers.
    let interval_us = if saturate {
        0
    } else {
        (1_000_000u64 * args.workers as u64)
            .checked_div(args.rate)
            .unwrap_or(0)
    };
    let batch = args.batch.max(1);

    eprintln!(
        "Running: {} workers, {} conns, batch={}, mix=\"{}\", outputs/create={}, {} mode, {}s warmup + {}s measure{}\n",
        args.workers,
        conns,
        batch,
        args.mix,
        outputs_per_create,
        if saturate {
            "SATURATE".to_string()
        } else {
            format!("{} ops/s target", args.rate)
        },
        args.warmup_secs,
        args.duration,
        if args.burst_size > 0 {
            format!(
                ", setMined burst={} every {}s",
                args.burst_size, args.burst_interval_secs
            )
        } else {
            String::new()
        },
    );

    let warmup = Duration::from_secs(args.warmup_secs);
    let measure = Duration::from_secs(args.duration);

    // Shared snapshot of created (txid) for the burst task to drain. Workers
    // push the txids they create; the burst task samples from the back.
    let burst_pool: Arc<std::sync::Mutex<Vec<[u8; 32]>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    // Burst drain times (micros) and total burst count, recorded by the burst
    // task during the measurement window.
    let burst_drains: Arc<std::sync::Mutex<Vec<u64>>> = Arc::new(std::sync::Mutex::new(Vec::new()));

    // Stats printer.
    let shutdown_s = shutdown.clone();
    let (c_s, s_s, r_s, m_s, u_s, e_s) = (
        creates.clone(),
        spends.clone(),
        reads.clone(),
        mined_count.clone(),
        unlocks.clone(),
        errors.clone(),
    );
    let inflight_s = inflight.clone();
    let inflight_hwm_s = inflight_hwm.clone();
    let stats = tokio::spawn(async move {
        let mut last = (0u64, 0u64, 0u64, 0u64, 0u64);
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            if shutdown_s.load(Ordering::Relaxed) {
                break;
            }
            let c = c_s.load(Ordering::Relaxed);
            let s = s_s.load(Ordering::Relaxed);
            let r = r_s.load(Ordering::Relaxed);
            let m = m_s.load(Ordering::Relaxed);
            let u = u_s.load(Ordering::Relaxed);
            let e = e_s.load(Ordering::Relaxed);
            let rate =
                ((c - last.0) + (s - last.1) + (r - last.2) + (m - last.3) + (u - last.4)) / 2;
            eprintln!(
                "  {rate} ops/s | inflight={}/hwm={} | creates={} spends={} reads={} mined={} unlocks={} errors={e} (totals: {c}/{s}/{r}/{m}/{u})",
                inflight_s.load(Ordering::Relaxed),
                inflight_hwm_s.load(Ordering::Relaxed),
                (c - last.0) / 2,
                (s - last.1) / 2,
                (r - last.2) / 2,
                (m - last.3) / 2,
                (u - last.4) / 2,
            );
            last = (c, s, r, m, u);
        }
    });

    // Window controller: flip `measuring` true after warmup, false after the
    // measurement window, then set shutdown.
    let measuring_w = measuring.clone();
    let shutdown_w = shutdown.clone();
    let window = tokio::spawn(async move {
        tokio::time::sleep(warmup).await;
        measuring_w.store(true, Ordering::Relaxed);
        let measure_start = Instant::now();
        tokio::time::sleep(measure).await;
        measuring_w.store(false, Ordering::Relaxed);
        shutdown_w.store(true, Ordering::Relaxed);
        measure_start.elapsed()
    });

    // setMined burst task (concurrent, not a worker op). Disabled when
    // burst_size == 0.
    let burst_handle = if args.burst_size > 0 {
        let client = client.clone();
        let shutdown = shutdown.clone();
        let measuring = measuring.clone();
        let burst_pool = burst_pool.clone();
        let burst_drains = burst_drains.clone();
        let inflight = inflight.clone();
        let inflight_hwm = inflight_hwm.clone();
        let burst_size = args.burst_size;
        let burst_interval = Duration::from_secs(args.burst_interval_secs.max(1));
        Some(tokio::spawn(async move {
            // Sleep until the next interval boundary, checking shutdown so the
            // task exits promptly at end-of-run.
            let wait = |d: Duration, sd: Arc<AtomicBool>| async move {
                let deadline = Instant::now() + d;
                while Instant::now() < deadline {
                    if sd.load(Ordering::Relaxed) {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            };
            loop {
                wait(burst_interval, shutdown.clone()).await;
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                if !measuring.load(Ordering::Relaxed) {
                    continue;
                }
                // Snapshot up to burst_size txids from the shared pool.
                let txids: Vec<[u8; 32]> = {
                    let pool = match burst_pool.lock() {
                        Ok(p) => p,
                        Err(_) => continue,
                    };
                    let take = burst_size.min(pool.len());
                    if take == 0 {
                        continue;
                    }
                    pool[pool.len() - take..].to_vec()
                };
                let block_height: u32 = 900_000;
                let params = SetMinedBatchParams {
                    block_id: block_height,
                    block_height,
                    subtree_idx: 0,
                    on_longest_chain: true,
                    unset_mined: false,
                    current_block_height: block_height,
                    block_height_retention: 288,
                };
                // Fire all chunks concurrently and measure total wall-clock
                // drain time for the whole burst.
                let burst_start = Instant::now();
                let mut futs = Vec::new();
                for chunk in txids.chunks(BURST_MAX_BATCH) {
                    let client = client.clone();
                    let params = params.clone();
                    let chunk = chunk.to_vec();
                    let inflight = inflight.clone();
                    let inflight_hwm = inflight_hwm.clone();
                    futs.push(tokio::spawn(async move {
                        let _g = InflightGuard::new(&inflight, &inflight_hwm);
                        client.set_mined_batch(&params, &chunk).await
                    }));
                }
                for f in futs {
                    let _ = f.await;
                }
                let drain = burst_start.elapsed().as_micros() as u64;
                if let Ok(mut d) = burst_drains.lock() {
                    d.push(drain);
                }
                eprintln!(
                    "  BURST: drained {} txids in {:.1}ms",
                    txids.len(),
                    drain as f64 / 1000.0
                );
            }
        }))
    } else {
        None
    };

    // Worker tasks.
    let mut handles = Vec::new();
    for wid in 0..args.workers {
        let client = client.clone();
        let shutdown = shutdown.clone();
        let measuring = measuring.clone();
        let mix = mix.clone();
        let creates = creates.clone();
        let spends = spends.clone();
        let reads = reads.clone();
        let mined_count = mined_count.clone();
        let unlocks = unlocks.clone();
        let errors = errors.clone();
        let err_partial = err_partial.clone();
        let err_redirect = err_redirect.clone();
        let err_connection = err_connection.clone();
        let err_server = err_server.clone();
        let err_other = err_other.clone();
        let err_logged = err_logged.clone();
        let inflight = inflight.clone();
        let inflight_hwm = inflight_hwm.clone();
        let burst_pool = burst_pool.clone();
        let track_burst = args.burst_size > 0;

        handles.push(tokio::spawn(async move {
            // Per-worker LOCAL queue of created (txid, first-utxo-hash) — no
            // shared mutex, so workers never serialize on each other. Each
            // worker spends/reads/mines/unlocks what it created; random 256-bit
            // txids keep workers' key spaces effectively disjoint, so the
            // server's per-key visibility barrier lets their mutations run
            // concurrently.
            let mut local: std::collections::VecDeque<([u8; 32], [u8; 32])> =
                std::collections::VecDeque::with_capacity(8192);
            let mut block_height: u32 = 800_000 + wid as u32 * 100_000;
            let mut rng: u64 = wid as u64 ^ 0xDEAD_BEEF_CAFE_1234;
            let now_ms = || {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64
            };

            // Per-worker, lock-free latency samples (micros) per op kind, and
            // per-op ok/failed counts. Only filled while `measuring` is true.
            let mut samples: WorkerSamples = Default::default();
            let mut ok_counts = [0u64; 5];
            let mut failed_counts = [0u64; 5];

            let log_err = |op: &str, e: &ClientError| {
                errors.fetch_add(1, Ordering::Relaxed);
                match e {
                    ClientError::Partial(pe) => {
                        err_partial.fetch_add(1, Ordering::Relaxed);
                        if err_logged.fetch_add(1, Ordering::Relaxed) < 20 {
                            let codes: Vec<String> = pe
                                .errors
                                .iter()
                                .map(|ie| {
                                    format!(
                                        "item{}={}",
                                        ie.item_index,
                                        teraslab_client::error_code_string(ie.code)
                                    )
                                })
                                .collect();
                            eprintln!("  ERR [{op}]: partial [{}]", codes.join(", "));
                        }
                    }
                    ClientError::Redirect(_) => {
                        err_redirect.fetch_add(1, Ordering::Relaxed);
                    }
                    ClientError::Connection(_) => {
                        err_connection.fetch_add(1, Ordering::Relaxed);
                    }
                    ClientError::Server { .. } => {
                        err_server.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {
                        err_other.fetch_add(1, Ordering::Relaxed);
                    }
                }
                if !matches!(e, ClientError::Partial(_))
                    && err_logged.fetch_add(1, Ordering::Relaxed) < 20
                {
                    eprintln!("  ERR [{op}]: {e}");
                }
            };

            while !shutdown.load(Ordering::Relaxed) {
                if interval_us > 0 {
                    tokio::time::sleep(Duration::from_micros(interval_us)).await;
                }

                block_height = block_height.wrapping_add(1);
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;

                // Build `batch` fresh CreateItems and remember each one's first
                // utxo hash for later spends/unlocks. Each tx has
                // `outputs_per_create` outputs (default 1 → 1-in/1-out semantics).
                let make_creates = |rng: &mut u64| {
                    let mut items = Vec::with_capacity(batch);
                    let mut firsts = Vec::with_capacity(batch);
                    for _ in 0..batch {
                        let mut txid = [0u8; 32];
                        fill_random(&mut txid, rng);
                        let n = outputs_per_create;
                        let mut hashes = Vec::with_capacity(n);
                        for _ in 0..n {
                            let mut h = [0u8; 32];
                            fill_random(&mut h, rng);
                            hashes.push(h);
                        }
                        firsts.push((txid, hashes[0]));
                        items.push(CreateItem {
                            txid,
                            tx_version: 2,
                            locktime: 0,
                            fee: 1000 + *rng % 5000,
                            size_in_bytes: 250,
                            extended_size: 0,
                            is_coinbase: false,
                            spending_height: 0,
                            created_at: now_ms(),
                            flags: 0,
                            utxo_hashes: hashes,
                            cold_data: vec![],
                            mined_block_id: None,
                            mined_block_height: None,
                            mined_subtree_idx: None,
                            parent_txids: vec![],
                        });
                    }
                    (items, firsts)
                };

                // Pop up to `batch` records from this worker's LOCAL queue.
                let pop_batch = |q: &mut std::collections::VecDeque<([u8; 32], [u8; 32])>| {
                    let mut v = Vec::with_capacity(batch);
                    for _ in 0..batch {
                        match q.pop_front() {
                            Some(e) => v.push(e),
                            None => break,
                        }
                    }
                    v
                };

                // Helper: do a create RPC, account for it, and feed the queues.
                // Returns the latency in micros.
                macro_rules! do_create {
                    () => {{
                        let (items, firsts) = make_creates(&mut rng);
                        let t0 = Instant::now();
                        let res = {
                            let _g = InflightGuard::new(&inflight, &inflight_hwm);
                            client.create_batch(&items).await
                        };
                        let lat = t0.elapsed().as_micros() as u64;
                        let measuring_now = measuring.load(Ordering::Relaxed);
                        match res {
                            Ok(_) => {
                                creates.fetch_add(items.len() as u64, Ordering::Relaxed);
                                if track_burst {
                                    if let Ok(mut p) = burst_pool.lock() {
                                        for (txid, _) in &firsts {
                                            p.push(*txid);
                                        }
                                    }
                                }
                                local.extend(firsts);
                                if measuring_now {
                                    ok_counts[OpKind::Create.index()] += 1;
                                    samples[OpKind::Create.index()].push(lat);
                                }
                            }
                            Err(ref e) => {
                                log_err("create", e);
                                if measuring_now {
                                    failed_counts[OpKind::Create.index()] += 1;
                                }
                            }
                        }
                    }};
                }

                let chosen = mix.select(rng);
                match chosen {
                    OpKind::Create => {
                        do_create!();
                    }
                    OpKind::Spend => {
                        let entries = pop_batch(&mut local);
                        if entries.is_empty() {
                            // Nothing to spend yet — seed with a batch of creates.
                            do_create!();
                        } else {
                            let params = SpendBatchParams {
                                ignore_conflicting: false,
                                ignore_locked: false,
                                current_block_height: block_height,
                                block_height_retention: 288,
                            };
                            let items: Vec<SpendItem> = entries
                                .iter()
                                .map(|(txid, utxo_hash)| {
                                    let mut sd = [0u8; 36];
                                    fill_random(&mut sd, &mut rng);
                                    SpendItem {
                                        txid: *txid,
                                        vout: 0,
                                        utxo_hash: *utxo_hash,
                                        spending_data: sd,
                                    }
                                })
                                .collect();
                            let t0 = Instant::now();
                            let res = {
                                let _g = InflightGuard::new(&inflight, &inflight_hwm);
                                client.spend_batch(&params, &items).await
                            };
                            let lat = t0.elapsed().as_micros() as u64;
                            let measuring_now = measuring.load(Ordering::Relaxed);
                            match res {
                                Ok(_) => {
                                    spends.fetch_add(items.len() as u64, Ordering::Relaxed);
                                    if measuring_now {
                                        ok_counts[OpKind::Spend.index()] += 1;
                                        samples[OpKind::Spend.index()].push(lat);
                                    }
                                }
                                Err(ref e) => {
                                    log_err("spend", e);
                                    if measuring_now {
                                        failed_counts[OpKind::Spend.index()] += 1;
                                    }
                                    for e in entries {
                                        local.push_back(e);
                                    }
                                }
                            }
                        }
                    }
                    OpKind::Unlock => {
                        // Unlock = SetLocked(false) on previously-created txids.
                        // Methodology: records aren't created in a locked state,
                        // so SetLocked(false) is a write-path probe — it still
                        // exercises generation bump + redo + DAH-index update;
                        // that's the intended perf measurement.
                        let entries = pop_batch(&mut local);
                        if entries.is_empty() {
                            do_create!();
                        } else {
                            let txids: Vec<[u8; 32]> = entries.iter().map(|(t, _)| *t).collect();
                            let t0 = Instant::now();
                            let res = {
                                let _g = InflightGuard::new(&inflight, &inflight_hwm);
                                client.set_locked_batch(false, &txids).await
                            };
                            let lat = t0.elapsed().as_micros() as u64;
                            let measuring_now = measuring.load(Ordering::Relaxed);
                            match res {
                                Ok(_) => {
                                    unlocks.fetch_add(txids.len() as u64, Ordering::Relaxed);
                                    if measuring_now {
                                        ok_counts[OpKind::Unlock.index()] += 1;
                                        samples[OpKind::Unlock.index()].push(lat);
                                    }
                                }
                                Err(ref e) => {
                                    log_err("unlock", e);
                                    if measuring_now {
                                        failed_counts[OpKind::Unlock.index()] += 1;
                                    }
                                }
                            }
                            // Unlock is read-like for our queue: keep the
                            // entries so they can be re-used by other ops.
                            for e in entries {
                                local.push_back(e);
                            }
                        }
                    }
                    OpKind::Get => {
                        let entries = pop_batch(&mut local);
                        if !entries.is_empty() {
                            let txids: Vec<[u8; 32]> = entries.iter().map(|(t, _)| *t).collect();
                            let mask = teraslab::protocol::codec::FieldMask::ALL_METADATA;
                            let t0 = Instant::now();
                            let res = {
                                let _g = InflightGuard::new(&inflight, &inflight_hwm);
                                client.get_batch(mask, &txids).await
                            };
                            let lat = t0.elapsed().as_micros() as u64;
                            let measuring_now = measuring.load(Ordering::Relaxed);
                            match res {
                                Ok(_) => {
                                    reads.fetch_add(txids.len() as u64, Ordering::Relaxed);
                                    if measuring_now {
                                        ok_counts[OpKind::Get.index()] += 1;
                                        samples[OpKind::Get.index()].push(lat);
                                    }
                                }
                                Err(ref e) => {
                                    log_err("get", e);
                                    if measuring_now {
                                        failed_counts[OpKind::Get.index()] += 1;
                                    }
                                }
                            }
                            for e in entries {
                                local.push_back(e);
                            }
                        }
                    }
                    OpKind::SetMined => {
                        let entries = pop_batch(&mut local);
                        if !entries.is_empty() {
                            let txids: Vec<[u8; 32]> = entries.iter().map(|(t, _)| *t).collect();
                            let params = SetMinedBatchParams {
                                block_id: block_height,
                                block_height,
                                subtree_idx: 0,
                                on_longest_chain: true,
                                unset_mined: false,
                                current_block_height: block_height,
                                block_height_retention: 288,
                            };
                            let t0 = Instant::now();
                            let res = {
                                let _g = InflightGuard::new(&inflight, &inflight_hwm);
                                client.set_mined_batch(&params, &txids).await
                            };
                            let lat = t0.elapsed().as_micros() as u64;
                            let measuring_now = measuring.load(Ordering::Relaxed);
                            match res {
                                Ok(_) => {
                                    mined_count.fetch_add(txids.len() as u64, Ordering::Relaxed);
                                    if measuring_now {
                                        ok_counts[OpKind::SetMined.index()] += 1;
                                        samples[OpKind::SetMined.index()].push(lat);
                                    }
                                }
                                Err(ref e) => {
                                    log_err("set_mined", e);
                                    if measuring_now {
                                        failed_counts[OpKind::SetMined.index()] += 1;
                                    }
                                }
                            }
                            for e in entries {
                                local.push_back(e);
                            }
                        }
                    }
                }
            }

            WorkerResult {
                samples,
                ok: ok_counts,
                failed: failed_counts,
            }
        }));
    }

    // Collect worker results.
    let mut worker_results = Vec::with_capacity(handles.len());
    for h in handles {
        if let Ok(r) = h.await {
            worker_results.push(r);
        }
    }
    shutdown.store(true, Ordering::Relaxed);
    let _ = stats.await;
    let measured_elapsed = window.await.unwrap_or(measure);
    if let Some(bh) = burst_handle {
        let _ = bh.await;
    }

    // Merge per-worker samples and counts.
    let mut merged: [Vec<u64>; 5] = Default::default();
    let mut ok = [0u64; 5];
    let mut failed = [0u64; 5];
    for wr in &worker_results {
        for op in OpKind::ALL {
            let i = op.index();
            merged[i].extend_from_slice(&wr.samples[i]);
            ok[i] += wr.ok[i];
            failed[i] += wr.failed[i];
        }
    }
    for v in merged.iter_mut() {
        v.sort_unstable();
    }

    let secs = measured_elapsed.as_secs_f64().max(1e-9);

    // Per-op stats.
    struct OpStat {
        op: OpKind,
        ok: u64,
        failed: u64,
        ops_sec: f64,
        p50: f64,
        p99: f64,
        p999: f64,
    }
    let mut op_stats = Vec::new();
    for op in OpKind::ALL {
        let i = op.index();
        // ops_sec counts items (ok). Use the merged sample count where it is the
        // truer per-RPC tally; but to stay consistent with historical ops/s
        // (which counts items), use ok counts scaled by batch... we count RPCs
        // here: ok[i] is per-RPC successes. Report ops_sec over per-RPC ok.
        let ops_sec = ok[i] as f64 / secs;
        op_stats.push(OpStat {
            op,
            ok: ok[i],
            failed: failed[i],
            ops_sec,
            p50: percentile(&merged[i], 50.0),
            p99: percentile(&merged[i], 99.0),
            p999: percentile(&merged[i], 99.9),
        });
    }

    // Burst stats.
    let mut burst_vec = burst_drains.lock().map(|d| d.clone()).unwrap_or_default();
    burst_vec.sort_unstable();
    let burst_count = burst_vec.len();
    let burst_p50 = percentile(&burst_vec, 50.0);
    let burst_p99 = percentile(&burst_vec, 99.0);
    let burst_max = burst_vec.last().copied().unwrap_or(0) as f64;

    // Human-readable summary.
    let total_ok: u64 = ok.iter().sum();
    let e = errors.load(Ordering::Relaxed);
    eprintln!(
        "\nDone. Measurement window {:.1}s, {total_ok} ops (RPCs) over window | peak in-flight RPCs={} (of {} workers)",
        secs,
        inflight_hwm.load(Ordering::Relaxed),
        args.workers,
    );
    eprintln!(
        "  {:<9} {:>10} {:>8} {:>12} {:>10} {:>10} {:>10}",
        "op", "ok", "failed", "ops/s", "p50_us", "p99_us", "p999_us"
    );
    for st in &op_stats {
        eprintln!(
            "  {:<9} {:>10} {:>8} {:>12.0} {:>10.0} {:>10.0} {:>10.0}",
            st.op.name(),
            st.ok,
            st.failed,
            st.ops_sec,
            st.p50,
            st.p99,
            st.p999,
        );
    }
    if burst_count > 0 {
        eprintln!(
            "  burst: count={burst_count} drain_p50={:.1}ms drain_p99={:.1}ms drain_max={:.1}ms",
            burst_p50 / 1000.0,
            burst_p99 / 1000.0,
            burst_max / 1000.0,
        );
    }
    if e > 0 {
        eprintln!(
            "  error breakdown: partial={} redirect={} connection={} server={} other={}",
            err_partial.load(Ordering::Relaxed),
            err_redirect.load(Ordering::Relaxed),
            err_connection.load(Ordering::Relaxed),
            err_server.load(Ordering::Relaxed),
            err_other.load(Ordering::Relaxed),
        );
    }

    // JSON output: one line, LOADGEN_RESULT {json}. Hand-rolled (no serde dep).
    if args.json {
        let mut results = String::new();
        for (idx, st) in op_stats.iter().enumerate() {
            if idx > 0 {
                results.push(',');
            }
            results.push_str(&format!(
                "{{\"op\":\"{}\",\"ok\":{},\"failed\":{},\"ops_sec\":{:.3},\"p50_us\":{:.1},\"p99_us\":{:.1},\"p999_us\":{:.1}}}",
                st.op.name(),
                st.ok,
                st.failed,
                st.ops_sec,
                st.p50,
                st.p99,
                st.p999,
            ));
        }
        let json = format!(
            "{{\"duration_s\":{:.3},\"workers\":{},\"results\":[{}],\"burst\":{{\"count\":{},\"drain_p50_us\":{:.1},\"drain_p99_us\":{:.1},\"drain_max_us\":{:.1}}}}}",
            secs, args.workers, results, burst_count, burst_p50, burst_p99, burst_max,
        );
        println!("LOADGEN_RESULT {json}");
    }
}

/// RAII gauge for one in-flight round-trip. Increment (and bump the high-water
/// mark) on creation, decrement on drop — wrap it around each `.await` so the
/// gauge reflects true concurrent in-flight RPCs.
struct InflightGuard<'a> {
    inflight: &'a AtomicU64,
}

impl<'a> InflightGuard<'a> {
    fn new(inflight: &'a AtomicU64, hwm: &AtomicU64) -> Self {
        let cur = inflight.fetch_add(1, Ordering::Relaxed) + 1;
        hwm.fetch_max(cur, Ordering::Relaxed);
        Self { inflight }
    }
}

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        self.inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

fn fill_random(buf: &mut [u8], state: &mut u64) {
    for chunk in buf.chunks_mut(8) {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        let bytes = state.to_le_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_nearest_rank_known_values() {
        // 1..=100 sorted. Nearest-rank: rank = ceil(p/100 * N).
        let data: Vec<u64> = (1..=100).collect();
        // p50: ceil(0.50*100)=50 → sorted[49] = 50.
        assert_eq!(percentile(&data, 50.0), 50.0);
        // p99: ceil(0.99*100)=99 → sorted[98] = 99.
        assert_eq!(percentile(&data, 99.0), 99.0);
        // p99.9: ceil(0.999*100)=100 → sorted[99] = 100.
        assert_eq!(percentile(&data, 99.9), 100.0);
        // p100: rank 100 → 100. p0: clamped to rank 1 → 1.
        assert_eq!(percentile(&data, 100.0), 100.0);
        assert_eq!(percentile(&data, 0.0), 1.0);
    }

    #[test]
    fn percentile_small_and_empty() {
        // Empty slice → 0.0.
        assert_eq!(percentile(&[], 50.0), 0.0);
        // Single element → that element for any percentile.
        assert_eq!(percentile(&[42], 50.0), 42.0);
        assert_eq!(percentile(&[42], 99.9), 42.0);
        // Ten elements 10,20,...,100. p50: ceil(5)=5 → sorted[4]=50.
        let data: Vec<u64> = (1..=10).map(|x| x * 10).collect();
        assert_eq!(percentile(&data, 50.0), 50.0);
        // p99: ceil(0.99*10)=10 → sorted[9]=100.
        assert_eq!(percentile(&data, 99.0), 100.0);
    }

    #[test]
    fn mix_parse_default() {
        let m = Mix::parse("create=1,spend=1,unlock=1").unwrap();
        assert_eq!(m.total, 3);
        // cumulative in OpKind::ALL order: create, spend, get, setmined, unlock.
        assert_eq!(m.cumulative[OpKind::Create.index()], 1);
        assert_eq!(m.cumulative[OpKind::Spend.index()], 2);
        assert_eq!(m.cumulative[OpKind::Get.index()], 2); // weight 0
        assert_eq!(m.cumulative[OpKind::SetMined.index()], 2); // weight 0
        assert_eq!(m.cumulative[OpKind::Unlock.index()], 3);
    }

    #[test]
    fn mix_select_cumulative_thresholds() {
        let m = Mix::parse("create=1,spend=1,unlock=1").unwrap();
        // total=3. point = r % 3.
        // point 0 < cumulative[create]=1 → create.
        assert!(m.select(0) == OpKind::Create);
        assert!(m.select(3) == OpKind::Create);
        // point 1 < cumulative[spend]=2 → spend.
        assert!(m.select(1) == OpKind::Spend);
        assert!(m.select(4) == OpKind::Spend);
        // point 2 < cumulative[unlock]=3 → unlock (get/setmined have weight 0).
        assert!(m.select(2) == OpKind::Unlock);
        assert!(m.select(5) == OpKind::Unlock);
    }

    #[test]
    fn mix_select_distribution() {
        // 3:1 create:spend. Over 4000 selections of r=0..4000 the ratio of
        // create to spend should be ~3:1.
        let m = Mix::parse("create=3,spend=1").unwrap();
        assert_eq!(m.total, 4);
        let mut creates = 0u64;
        let mut spends = 0u64;
        for r in 0..4000u64 {
            match m.select(r) {
                OpKind::Create => creates += 1,
                OpKind::Spend => spends += 1,
                _ => panic!("unexpected op with zero weight selected"),
            }
        }
        assert_eq!(creates, 3000);
        assert_eq!(spends, 1000);
    }

    #[test]
    fn mix_parse_errors() {
        // Unknown op.
        assert!(Mix::parse("frobnicate=1").is_err());
        // Malformed (no '=').
        assert!(Mix::parse("create").is_err());
        // Invalid weight.
        assert!(Mix::parse("create=abc").is_err());
        // All-zero total.
        assert!(Mix::parse("create=0,spend=0").is_err());
    }

    #[test]
    fn mix_parse_includes_get_and_setmined() {
        let m = Mix::parse("get=2,setmined=5").unwrap();
        assert_eq!(m.total, 7);
        assert_eq!(m.cumulative[OpKind::Get.index()], 2);
        assert_eq!(m.cumulative[OpKind::SetMined.index()], 7);
        // point 0,1 → get; 2..7 → setmined.
        assert!(m.select(0) == OpKind::Get);
        assert!(m.select(1) == OpKind::Get);
        assert!(m.select(2) == OpKind::SetMined);
        assert!(m.select(6) == OpKind::SetMined);
    }
}
