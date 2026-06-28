//! Load generator for TeraSlab.
//!
//! Two workload modes:
//!
//! 1. **Recipe mode** (`--recipe`, the primary realistic workload) faithfully
//!    reproduces the UTXO-DB benchmark recipe in `utxo-db-benchmark-recipe.md`:
//!    four continuous batched streams (create / spend / read / delete) driven at
//!    an equal 1:1:1:1 *per-record* rate, plus one bursty SetMined overlay fired
//!    from a dedicated client every block interval with a ramped (small → peak →
//!    tail) shape. Each stream issues batches of the recipe's per-op size
//!    (create 488, spend 329, read 291, delete 488, setmined 1024). A pre-load
//!    phase populates a shared working set so reads/spends/deletes hit existing
//!    keys.
//!
//! 2. **Mix mode** (the default, legacy simple workload) issues a weighted mix
//!    of single batched ops sized by `--batch`, for quick throughput probes.
//!    Driven by `--mix` / `--saturate` / `--rate`.
//!
//! Per-op latency percentiles (p50/p99/p99.9) are measured over a measurement
//! window that begins after a warmup period. Both modes emit a machine-readable
//! `LOADGEN_RESULT {json}` line plus a human summary.
//!
//! Usage:
//!   # Realistic recipe workload (1:1:1:1 steady + ramped SetMined burst):
//!   teraslab-loadgen --addr localhost:3300 --recipe --saturate \
//!       --workers 12 --preload 200000 \
//!       --burst-interval-secs 360 --burst-width-secs 40
//!
//!   # Simple mix probe:
//!   teraslab-loadgen --addr localhost:3300 --rate 500 --duration 300
//!   teraslab-loadgen --addr localhost:3300 --saturate --workers 16 \
//!       --mix "create=1,spend=1,unlock=1"

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
    Delete,
}

impl OpKind {
    const ALL: [OpKind; 6] = [
        OpKind::Create,
        OpKind::Spend,
        OpKind::Get,
        OpKind::SetMined,
        OpKind::Unlock,
        OpKind::Delete,
    ];

    fn name(self) -> &'static str {
        match self {
            OpKind::Create => "create",
            OpKind::Spend => "spend",
            OpKind::Get => "get",
            OpKind::SetMined => "setmined",
            OpKind::Unlock => "unlock",
            OpKind::Delete => "delete",
        }
    }

    fn index(self) -> usize {
        match self {
            OpKind::Create => 0,
            OpKind::Spend => 1,
            OpKind::Get => 2,
            OpKind::SetMined => 3,
            OpKind::Unlock => 4,
            OpKind::Delete => 5,
        }
    }

    fn parse(s: &str) -> Option<OpKind> {
        match s {
            "create" => Some(OpKind::Create),
            "spend" => Some(OpKind::Spend),
            "get" => Some(OpKind::Get),
            "setmined" => Some(OpKind::SetMined),
            "unlock" => Some(OpKind::Unlock),
            "delete" => Some(OpKind::Delete),
            _ => None,
        }
    }
}

const NUM_OPS: usize = OpKind::ALL.len();

/// A weighted op mix. `cumulative[i]` is the running sum of weights up to and
/// including op `i` (in `OpKind::ALL` order); `total` is the final sum. Selection
/// picks the first op whose cumulative threshold exceeds `r % total`.
#[derive(Clone)]
struct Mix {
    cumulative: [u64; NUM_OPS],
    total: u64,
}

impl Mix {
    /// Parse a mix spec like "create=1,spend=1,unlock=1". Unlisted ops get
    /// weight 0. Returns an error string on an unknown op, a malformed pair, or
    /// an all-zero total.
    fn parse(spec: &str) -> Result<Mix, String> {
        let mut weights = [0u64; NUM_OPS];
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
        let mut cumulative = [0u64; NUM_OPS];
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
        OpKind::ALL[NUM_OPS - 1]
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

/// Per-op batch sizes (records per RPC) for the recipe workload. Defaults match
/// §2b of `utxo-db-benchmark-recipe.md`.
#[derive(Clone, Copy)]
struct BatchSizes {
    create: usize,
    spend: usize,
    read: usize,
    delete: usize,
    setmined: usize,
}

impl BatchSizes {
    fn from_args(a: &Args) -> BatchSizes {
        BatchSizes {
            create: a.create_batch.max(1),
            spend: a.spend_batch.max(1),
            read: a.read_batch.max(1),
            delete: a.delete_batch.max(1),
            setmined: a.setmined_batch.max(1),
        }
    }
}

/// Ramped SetMined burst schedule (recipe §4 stream 5).
///
/// Models a periodic block-found burst: between blocks the SetMined rate is 0;
/// for `width` seconds starting at each `interval` boundary it ramps small →
/// peak → tail (a symmetric triangle peaking at the burst midpoint). The
/// triangle integrates to the same area as a flat `peak` block over `width`, so
/// the configured per-block record total is preserved while the instantaneous
/// rate ramps rather than stepping.
#[derive(Clone, Copy)]
struct BurstSchedule {
    interval: Duration,
    width: Duration,
    /// Peak instantaneous rate in records/sec (apex of the triangle).
    peak_rec_per_s: f64,
}

impl BurstSchedule {
    /// Instantaneous SetMined target rate (records/sec) at `t` seconds into the
    /// run. Returns 0.0 outside any burst window. Inside a window the rate is a
    /// symmetric triangle: 0 at the window edges, `peak_rec_per_s` at the
    /// midpoint.
    fn rate_at(&self, t: Duration) -> f64 {
        let interval = self.interval.as_secs_f64().max(1e-9);
        let width = self.width.as_secs_f64().max(1e-9);
        let phase = t.as_secs_f64() % interval;
        if phase >= width {
            return 0.0;
        }
        let half = width / 2.0;
        // Triangle: rises 0→peak over [0,half], falls peak→0 over [half,width].
        let frac = if phase <= half {
            phase / half
        } else {
            (width - phase) / half
        };
        (frac * self.peak_rec_per_s).max(0.0)
    }

    /// True if `t` lies inside a burst window (rate is non-zero somewhere in the
    /// window; the exact edges are 0 but are still "in burst" for scheduling).
    fn in_burst(&self, t: Duration) -> bool {
        let interval = self.interval.as_secs_f64().max(1e-9);
        let width = self.width.as_secs_f64().max(1e-9);
        let phase = t.as_secs_f64() % interval;
        phase < width
    }
}

/// A working-set key: `(txid, first_utxo_hash)`. The hash is needed to build
/// spend items; the txid drives every other op.
type Key = ([u8; 32], [u8; 32]);

/// One shard of the working-set key pool. Keys are partitioned across many
/// shards (one or more per worker) so the steady streams never serialize on a
/// single global lock — the recipe stresses high concurrency with no hot-key
/// contention, so the pool must not introduce one itself.
///
/// Each list holds `(txid, first_utxo_hash)` for the keys currently in that
/// state. Transitions move a key from one list to another.
struct ShardPool {
    live: Vec<Key>,
    spent: Vec<Key>,
    /// Spent + mined → eligible for delete (prune).
    deletable: Vec<Key>,
}

impl ShardPool {
    fn new() -> ShardPool {
        ShardPool {
            live: Vec::new(),
            spent: Vec::new(),
            deletable: Vec::new(),
        }
    }

    /// Record a freshly created key (Live).
    fn add_live(&mut self, key: Key) {
        self.live.push(key);
    }

    /// Take up to `n` Live keys to spend, moving them to Spent. Returns the keys
    /// taken (may be fewer than `n`, or empty). Spend only ever takes unspent
    /// (Live) keys — never an already-spent one.
    fn take_to_spend(&mut self, n: usize) -> Vec<Key> {
        let take = n.min(self.live.len());
        let taken: Vec<_> = self.live.drain(self.live.len() - take..).collect();
        self.spent.extend_from_slice(&taken);
        taken
    }

    /// Take up to `n` Live keys to read (GetMeta/decorate). Reads don't change
    /// state, so the keys are returned to the Live list. Returns the keys read.
    fn peek_live(&self, n: usize) -> Vec<Key> {
        let take = n.min(self.live.len());
        self.live[self.live.len() - take..].to_vec()
    }

    /// Mark up to `n` Spent keys as mined, moving them to the deletable
    /// (Spent+Mined) list. Returns the keys marked. setMined only ever marks
    /// keys that are already Spent.
    fn mark_mined(&mut self, n: usize) -> Vec<Key> {
        let take = n.min(self.spent.len());
        let taken: Vec<_> = self.spent.drain(self.spent.len() - take..).collect();
        self.deletable.extend_from_slice(&taken);
        taken
    }

    /// Take up to `n` Spent+Mined keys to delete (prune), removing them from the
    /// pool entirely. Returns the keys deleted. Delete only ever takes keys that
    /// are both spent and mined.
    fn take_to_delete(&mut self, n: usize) -> Vec<Key> {
        let take = n.min(self.deletable.len());
        self.deletable
            .drain(self.deletable.len() - take..)
            .collect()
    }

    /// On a failed spend, return keys to the Live list (they were not actually
    /// spent on the server).
    fn return_to_live(&mut self, keys: Vec<Key>) {
        // They were optimistically moved to `spent` by take_to_spend; remove the
        // matching tail and restore. Simpler: just push back to live and trust
        // the caller passes exactly what take_to_spend returned. We must also
        // drop them from `spent`.
        for k in &keys {
            if let Some(pos) = self.spent.iter().rposition(|e| e.0 == k.0) {
                self.spent.swap_remove(pos);
            }
        }
        self.live.extend(keys);
    }

    /// On a failed mark_mined, return keys to the Spent list.
    fn return_to_spent(&mut self, keys: Vec<Key>) {
        for k in &keys {
            if let Some(pos) = self.deletable.iter().rposition(|e| e.0 == k.0) {
                self.deletable.swap_remove(pos);
            }
        }
        self.spent.extend(keys);
    }
}

/// A sharded pool of working-set keys. Worker `w` owns shard `w % shards`, so
/// each worker touches its own lock on the hot path. The SetMined burst and the
/// delete/setmined progression sweep across shards.
struct ShardedPool {
    shards: Vec<std::sync::Mutex<ShardPool>>,
}

impl ShardedPool {
    fn new(n: usize) -> ShardedPool {
        let n = n.max(1);
        let mut shards = Vec::with_capacity(n);
        for _ in 0..n {
            shards.push(std::sync::Mutex::new(ShardPool::new()));
        }
        ShardedPool { shards }
    }

    fn len(&self) -> usize {
        self.shards.len()
    }

    fn shard(&self, i: usize) -> &std::sync::Mutex<ShardPool> {
        &self.shards[i % self.shards.len()]
    }
}

/// TeraSlab load generator.
#[derive(Parser)]
#[command(
    name = "teraslab-loadgen",
    about = "Generate UTXO-DB-recipe or simple mixed load against a TeraSlab server"
)]
struct Args {
    /// Server address for single-node mode (host:port).
    #[arg(long)]
    addr: Option<String>,

    /// Cluster seed addresses (comma-separated).
    #[arg(long)]
    seeds: Option<String>,

    /// Reproduce the realistic UTXO-DB benchmark recipe: 1:1:1:1 steady
    /// create/spend/read/delete streams at per-op batch sizes, over a
    /// pre-loaded working set, with a ramped SetMined burst overlay. When set,
    /// `--mix` is ignored. See utxo-db-benchmark-recipe.md.
    #[arg(long, default_value = "false")]
    recipe: bool,

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

    /// Number of concurrent worker tasks. In recipe mode these drive the steady
    /// streams (recipe: 12).
    #[arg(long, default_value = "4")]
    workers: usize,

    /// Number of steady-stream clients to model (recipe: 12). Currently informs
    /// the worker count when --recipe is set and --workers is left at default.
    #[arg(long, default_value = "12")]
    steady_clients: usize,

    /// Items per batched RPC in MIX mode. >1 amortizes the per-batch redo fsync
    /// across many items. Ignored in recipe mode (use the per-op batch flags).
    #[arg(long, default_value = "1")]
    batch: usize,

    /// Recipe per-op batch sizes (records per RPC). Defaults from recipe §2b.
    #[arg(long, default_value = "488")]
    create_batch: usize,
    /// Records per spend RPC (recipe: 329).
    #[arg(long, default_value = "329")]
    spend_batch: usize,
    /// Records per read RPC (recipe: 291, models GetMeta + BatchDecorate).
    #[arg(long, default_value = "291")]
    read_batch: usize,
    /// Records per delete (prune) RPC (recipe: 488, delete-rate ≈ create-rate).
    #[arg(long, default_value = "488")]
    delete_batch: usize,
    /// Records per SetMined burst RPC (recipe: 1024, fixed power-of-2).
    #[arg(long, default_value = "1024")]
    setmined_batch: usize,

    /// Recipe pre-load: number of records to create into the working set before
    /// timing starts, so reads/spends/deletes hit existing keys. Not sampled.
    #[arg(long, default_value = "200000")]
    preload: usize,

    /// Weighted op mix for MIX mode, e.g. "create=1,spend=1,unlock=1".
    /// Recognized ops: create, spend, get, setmined, unlock, delete. Unlisted
    /// ops get weight 0. Ignored in recipe mode.
    #[arg(long, default_value = "create=1,spend=1,unlock=1")]
    mix: String,

    /// Number of outputs (utxo hashes) per created transaction. Default 1 for
    /// 1-in/1-out tx semantics (spend spends 1 output).
    #[arg(long, default_value = "1")]
    outputs_per_create: usize,

    /// MIX-mode setMined burst size: number of created txids snapshotted and
    /// drained per burst. 0 disables. Ignored in recipe mode (recipe always
    /// runs the ramped burst overlay; set --burst-peak-rec-per-s 0 to disable).
    #[arg(long, default_value = "0")]
    burst_size: usize,

    /// Interval between SetMined bursts, in seconds (recipe: 360, ~6 min).
    #[arg(long, default_value = "360")]
    burst_interval_secs: u64,

    /// Burst width, in seconds: the ramp-up→peak→tail window (recipe: 40).
    #[arg(long, default_value = "40")]
    burst_width_secs: u64,

    /// Peak SetMined instantaneous rate, in RECORDS/sec, at the burst apex
    /// (recipe: ~8_000_000). The triangle ramp integrates to peak×width/2
    /// records per block. Set 0 to disable the recipe burst overlay.
    #[arg(long, default_value = "8000000")]
    burst_peak_rec_per_s: u64,

    /// Emit a machine-readable LOADGEN_RESULT {json} line in addition to the
    /// human-readable summary. On by default.
    #[arg(long, default_value = "true")]
    json: bool,

    /// Connection pool size for the steady workers. Defaults to the worker
    /// count so each worker holds its own connection.
    #[arg(long)]
    conns: Option<usize>,
}

/// Per-worker latency samples (micros), one Vec per op kind. Accumulated lock-
/// free in the worker's own task and merged at the end.
type WorkerSamples = [Vec<u64>; NUM_OPS];

/// Result of one worker: its per-op latency samples plus per-op ok/failed
/// counts (RPCs) and per-op record counts recorded during the measurement
/// window.
struct WorkerResult {
    samples: WorkerSamples,
    ok: [u64; NUM_OPS],
    failed: [u64; NUM_OPS],
    records: [u64; NUM_OPS],
}

/// Build a connection config opening `conns` sockets up front.
fn build_config(args: &Args, conns: usize) -> ClientConfig {
    ClientConfig {
        addr: args.addr.clone(),
        seeds: args
            .seeds
            .as_ref()
            .map(|s| s.split(',').map(|x| x.trim().to_string()).collect())
            .unwrap_or_default(),
        pool: PoolConfig {
            min_conns: conns,
            max_conns: conns,
            dial_timeout: Duration::from_secs(5),
            health_check: Duration::from_millis(200),
            ..Default::default()
        },
        cluster_refresh_interval: Duration::from_secs(30),
        max_redirects: 3,
        addr_map: Default::default(),
        ..Default::default()
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    if args.addr.is_none() && args.seeds.is_none() {
        eprintln!("Must specify --addr or --seeds");
        std::process::exit(1);
    }

    if args.recipe {
        run_recipe(args).await;
    } else {
        run_mix(args).await;
    }
}

// ---------------------------------------------------------------------------
// Recipe workload
// ---------------------------------------------------------------------------

/// Run the realistic UTXO-DB benchmark recipe workload.
async fn run_recipe(args: Args) {
    let batches = BatchSizes::from_args(&args);
    // Recipe drives the steady streams with `steady_clients` (default 12). If
    // the user left --workers at its default (4), use steady_clients instead so
    // `--recipe` alone matches the recipe's 12-client steady load.
    let workers = if args.workers == 4 {
        args.steady_clients.max(1)
    } else {
        args.workers.max(1)
    };
    let saturate = args.saturate || args.rate == 0;
    let conns = args.conns.unwrap_or(workers).max(4);

    let cfg = build_config(&args, conns);
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
    if conns > 1 {
        eprintln!("Warming up {conns} connections...");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // Sharded working set: 4 shards per worker keeps lock contention low while
    // letting the cross-shard sweepers (delete/setmined) find keys.
    let pool = Arc::new(ShardedPool::new(workers * 4));

    // Pre-load: create `preload` records (batched) into the working set so
    // reads/spends/deletes hit existing keys. Not sampled.
    if args.preload > 0 {
        eprintln!(
            "Pre-loading {} records (batch {})...",
            args.preload, batches.create
        );
        let created = preload(
            &client,
            &pool,
            args.preload,
            batches.create,
            args.outputs_per_create,
        )
        .await;
        eprintln!("Pre-loaded {created} records into working set");
    }

    let burst_sched = BurstSchedule {
        interval: Duration::from_secs(args.burst_interval_secs.max(1)),
        width: Duration::from_secs(args.burst_width_secs.max(1)),
        peak_rec_per_s: args.burst_peak_rec_per_s as f64,
    };

    let shutdown = Arc::new(AtomicBool::new(false));
    let measuring = Arc::new(AtomicBool::new(false));
    let inflight = Arc::new(AtomicU64::new(0));
    let inflight_hwm = Arc::new(AtomicU64::new(0));
    // Live counters for the periodic stats line, per op (records).
    let rec_counters: Arc<[AtomicU64; NUM_OPS]> = Arc::new(Default::default());
    let errors = Arc::new(AtomicU64::new(0));

    let warmup = Duration::from_secs(args.warmup_secs);
    let measure = Duration::from_secs(args.duration);

    // Burst drain stats: per-burst (peak records/s achieved, total drain micros,
    // per-RPC latencies recorded inside the burst).
    let burst_rpc_lat: Arc<std::sync::Mutex<Vec<u64>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let burst_records = Arc::new(AtomicU64::new(0));
    let burst_peak_achieved = Arc::new(AtomicU64::new(0)); // records/s, integer

    // Stats printer.
    {
        let shutdown = shutdown.clone();
        let inflight = inflight.clone();
        let inflight_hwm = inflight_hwm.clone();
        let rec_counters = rec_counters.clone();
        let errors = errors.clone();
        tokio::spawn(async move {
            let mut last = [0u64; NUM_OPS];
            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                let mut now = [0u64; NUM_OPS];
                for i in 0..NUM_OPS {
                    now[i] = rec_counters[i].load(Ordering::Relaxed);
                }
                let per = |i: usize| (now[i] - last[i]) / 2;
                eprintln!(
                    "  rec/s create={} spend={} read={} delete={} setmined={} | inflight={}/{} errors={}",
                    per(OpKind::Create.index()),
                    per(OpKind::Spend.index()),
                    per(OpKind::Get.index()),
                    per(OpKind::Delete.index()),
                    per(OpKind::SetMined.index()),
                    inflight.load(Ordering::Relaxed),
                    inflight_hwm.load(Ordering::Relaxed),
                    errors.load(Ordering::Relaxed),
                );
                last = now;
            }
        });
    }

    // Window controller.
    let window = {
        let measuring = measuring.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(warmup).await;
            measuring.store(true, Ordering::Relaxed);
            let start = Instant::now();
            tokio::time::sleep(measure).await;
            measuring.store(false, Ordering::Relaxed);
            shutdown.store(true, Ordering::Relaxed);
            start.elapsed()
        })
    };

    // Burst overlay: a SEPARATE dedicated client/connection (recipe: 1 client
    // drives the burst vs 12 steady). Fires the ramped SetMined schedule.
    let burst_handle = if args.burst_peak_rec_per_s > 0 {
        let burst_cfg = build_config(&args, 4);
        match Client::new(burst_cfg).await {
            Ok(bc) => {
                let bc = Arc::new(bc);
                Some(spawn_burst(
                    bc,
                    pool.clone(),
                    burst_sched,
                    batches.setmined,
                    shutdown.clone(),
                    measuring.clone(),
                    burst_rpc_lat.clone(),
                    burst_records.clone(),
                    burst_peak_achieved.clone(),
                    rec_counters.clone(),
                    errors.clone(),
                ))
            }
            Err(e) => {
                eprintln!("Burst client connect failed (continuing without burst): {e}");
                None
            }
        }
    } else {
        None
    };

    eprintln!(
        "Recipe: {workers} steady workers, {conns} conns, batches create={}/spend={}/read={}/delete={}/setmined={}, {} mode, {}s warmup + {}s measure; burst peak={} rec/s every {}s over {}s\n",
        batches.create,
        batches.spend,
        batches.read,
        batches.delete,
        batches.setmined,
        if saturate {
            "SATURATE".to_string()
        } else {
            format!("{} rec/s target", args.rate)
        },
        args.warmup_secs,
        args.duration,
        args.burst_peak_rec_per_s,
        args.burst_interval_secs,
        args.burst_width_secs,
    );

    // Steady workers.
    let interval_us = if saturate {
        0
    } else {
        // `rate` is records/sec across all workers; spread over workers, paced
        // per RPC by the average steady batch size.
        let avg_batch =
            ((batches.create + batches.spend + batches.read + batches.delete) / 4).max(1) as u64;
        (1_000_000u64 * workers as u64 * avg_batch)
            .checked_div(args.rate.max(1))
            .unwrap_or(0)
    };

    let mut handles = Vec::new();
    for wid in 0..workers {
        let client = client.clone();
        let pool = pool.clone();
        let shutdown = shutdown.clone();
        let measuring = measuring.clone();
        let inflight = inflight.clone();
        let inflight_hwm = inflight_hwm.clone();
        let rec_counters = rec_counters.clone();
        let errors = errors.clone();
        let outputs = args.outputs_per_create.max(1);
        handles.push(tokio::spawn(steady_worker(
            wid,
            workers,
            client,
            pool,
            batches,
            outputs,
            interval_us,
            shutdown,
            measuring,
            inflight,
            inflight_hwm,
            rec_counters,
            errors,
        )));
    }

    let mut worker_results = Vec::with_capacity(handles.len());
    for h in handles {
        if let Ok(r) = h.await {
            worker_results.push(r);
        }
    }
    shutdown.store(true, Ordering::Relaxed);
    let measured_elapsed = window.await.unwrap_or(measure);
    if let Some(bh) = burst_handle {
        let _ = bh.await;
    }

    report(
        &worker_results,
        measured_elapsed,
        &batches,
        errors.load(Ordering::Relaxed),
        inflight_hwm.load(Ordering::Relaxed),
        workers,
        args.json,
        Some(BurstReport {
            rpc_lat: burst_rpc_lat.lock().map(|d| d.clone()).unwrap_or_default(),
            total_records: burst_records.load(Ordering::Relaxed),
            peak_rec_per_s: burst_peak_achieved.load(Ordering::Relaxed),
        }),
    );
}

/// Pre-load the working set with `target` records, batched at `batch`. Returns
/// the number of records actually created. Spreads keys across all shards.
async fn preload(
    client: &Arc<Client>,
    pool: &Arc<ShardedPool>,
    target: usize,
    batch: usize,
    outputs: usize,
) -> usize {
    let mut rng: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut created = 0usize;
    let mut shard = 0usize;
    while created < target {
        let n = batch.min(target - created);
        let (items, firsts) = make_creates(n, outputs, &mut rng);
        match client.create_batch(&items).await {
            Ok(_) => {
                if let Ok(mut sp) = pool.shard(shard).lock() {
                    for f in firsts {
                        sp.add_live(f);
                    }
                }
                created += n;
                shard += 1;
            }
            Err(e) => {
                eprintln!("  preload create error: {e}");
                break;
            }
        }
    }
    created
}

/// Build `n` fresh CreateItems and the `(txid, first_utxo_hash)` of each.
fn make_creates(n: usize, outputs: usize, rng: &mut u64) -> (Vec<CreateItem>, Vec<Key>) {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let mut items = Vec::with_capacity(n);
    let mut firsts = Vec::with_capacity(n);
    for _ in 0..n {
        let mut txid = [0u8; 32];
        fill_random(&mut txid, rng);
        let o = outputs.max(1);
        let mut hashes = Vec::with_capacity(o);
        for _ in 0..o {
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
            created_at: now_ms,
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
}

/// Steady worker: drives the four continuous streams round-robin at an equal
/// per-RECORD rate (create/spend/read/delete). Because each op uses its own
/// batch size, equal per-record budgets mean different RPC counts per op — the
/// recipe-correct behaviour. Each worker owns its own shard for create/spend so
/// the hot path never serializes on a shared lock.
#[allow(clippy::too_many_arguments)]
async fn steady_worker(
    wid: usize,
    workers: usize,
    client: Arc<Client>,
    pool: Arc<ShardedPool>,
    batches: BatchSizes,
    outputs: usize,
    interval_us: u64,
    shutdown: Arc<AtomicBool>,
    measuring: Arc<AtomicBool>,
    inflight: Arc<AtomicU64>,
    inflight_hwm: Arc<AtomicU64>,
    rec_counters: Arc<[AtomicU64; NUM_OPS]>,
    errors: Arc<AtomicU64>,
) -> WorkerResult {
    let mut rng: u64 = (wid as u64).wrapping_mul(0x2545_F491_4F6C_DD1D) ^ 0xDEAD_BEEF_CAFE_1234;
    let mut block_height: u32 = 800_000 + wid as u32 * 100_000;
    let mut samples: WorkerSamples = Default::default();
    let mut ok = [0u64; NUM_OPS];
    let mut failed = [0u64; NUM_OPS];
    let mut records = [0u64; NUM_OPS];
    let my_shard = wid;

    // Per-record budgets: equal across the four streams. We advance whichever
    // stream is furthest behind in records issued, so the realized rate is
    // ~1:1:1:1 per record regardless of batch size.
    let mut issued = [0u64; NUM_OPS]; // records issued per op (this worker)

    let mut record_lat = |op: OpKind, lat: u64, n: usize, ok_rpc: bool| {
        let i = op.index();
        rec_counters[i].fetch_add(n as u64, Ordering::Relaxed);
        if measuring.load(Ordering::Relaxed) {
            if ok_rpc {
                ok[i] += 1;
                records[i] += n as u64;
                samples[i].push(lat);
            } else {
                failed[i] += 1;
            }
        }
    };

    while !shutdown.load(Ordering::Relaxed) {
        if interval_us > 0 {
            tokio::time::sleep(Duration::from_micros(interval_us)).await;
        }
        block_height = block_height.wrapping_add(1);

        // Pick the stream furthest behind (lowest records issued among the four
        // steady streams).
        let candidates = [OpKind::Create, OpKind::Spend, OpKind::Get, OpKind::Delete];
        let chosen = *candidates
            .iter()
            .min_by_key(|op| issued[op.index()])
            .unwrap_or(&OpKind::Create);

        match chosen {
            OpKind::Create => {
                let (items, firsts) = make_creates(batches.create, outputs, &mut rng);
                let t0 = Instant::now();
                let res = {
                    let _g = InflightGuard::new(&inflight, &inflight_hwm);
                    client.create_batch(&items).await
                };
                let lat = t0.elapsed().as_micros() as u64;
                match res {
                    Ok(_) => {
                        if let Ok(mut sp) = pool.shard(my_shard).lock() {
                            for f in firsts {
                                sp.add_live(f);
                            }
                        }
                        issued[OpKind::Create.index()] += items.len() as u64;
                        record_lat(OpKind::Create, lat, items.len(), true);
                    }
                    Err(ref e) => {
                        log_err(&errors, "create", e);
                        record_lat(OpKind::Create, lat, 0, false);
                    }
                }
            }
            OpKind::Spend => {
                let entries = {
                    match pool.shard(my_shard).lock() {
                        Ok(mut sp) => sp.take_to_spend(batches.spend),
                        Err(_) => Vec::new(),
                    }
                };
                if entries.is_empty() {
                    // Nothing to spend; seed creates so the stream can progress.
                    issued[OpKind::Spend.index()] += 1;
                    continue;
                }
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
                match res {
                    Ok(_) => {
                        issued[OpKind::Spend.index()] += items.len() as u64;
                        record_lat(OpKind::Spend, lat, items.len(), true);
                    }
                    Err(ref e) => {
                        log_err(&errors, "spend", e);
                        if let Ok(mut sp) = pool.shard(my_shard).lock() {
                            sp.return_to_live(entries);
                        }
                        record_lat(OpKind::Spend, lat, 0, false);
                    }
                }
            }
            OpKind::Get => {
                let entries = {
                    match pool.shard(my_shard).lock() {
                        Ok(sp) => sp.peek_live(batches.read),
                        Err(_) => Vec::new(),
                    }
                };
                if entries.is_empty() {
                    issued[OpKind::Get.index()] += 1;
                    continue;
                }
                let txids: Vec<[u8; 32]> = entries.iter().map(|(t, _)| *t).collect();
                let mask = teraslab::protocol::codec::FieldMask::ALL_METADATA;
                let t0 = Instant::now();
                let res = {
                    let _g = InflightGuard::new(&inflight, &inflight_hwm);
                    client.get_batch(mask, &txids).await
                };
                let lat = t0.elapsed().as_micros() as u64;
                match res {
                    Ok(_) => {
                        issued[OpKind::Get.index()] += txids.len() as u64;
                        record_lat(OpKind::Get, lat, txids.len(), true);
                    }
                    Err(ref e) => {
                        log_err(&errors, "get", e);
                        record_lat(OpKind::Get, lat, 0, false);
                    }
                }
            }
            OpKind::Delete => {
                // Delete prunes spent+mined keys. They may live on any shard
                // (the burst marks across shards), so sweep shards starting at
                // this worker's own until we find deletable keys.
                let mut entries = Vec::new();
                let nshards = pool.len();
                for off in 0..nshards {
                    let idx = (my_shard + off) % nshards;
                    if let Ok(mut sp) = pool.shard(idx).lock() {
                        entries = sp.take_to_delete(batches.delete);
                        if !entries.is_empty() {
                            break;
                        }
                    }
                }
                if entries.is_empty() {
                    // Nothing prunable yet (needs spent+mined). Progress the
                    // budget so we don't busy-spin only on delete.
                    issued[OpKind::Delete.index()] += 1;
                    continue;
                }
                let txids: Vec<[u8; 32]> = entries.iter().map(|(t, _)| *t).collect();
                let t0 = Instant::now();
                let res = {
                    let _g = InflightGuard::new(&inflight, &inflight_hwm);
                    client.delete_batch(&txids).await
                };
                let lat = t0.elapsed().as_micros() as u64;
                match res {
                    Ok(_) => {
                        issued[OpKind::Delete.index()] += txids.len() as u64;
                        record_lat(OpKind::Delete, lat, txids.len(), true);
                    }
                    Err(ref e) => {
                        log_err(&errors, "delete", e);
                        record_lat(OpKind::Delete, lat, 0, false);
                    }
                }
            }
            _ => {}
        }
    }
    let _ = workers; // reserved for future per-worker rate splits.

    WorkerResult {
        samples,
        ok,
        failed,
        records,
    }
}

/// Burst report passed to `report` for the SetMined overlay section.
struct BurstReport {
    rpc_lat: Vec<u64>,
    total_records: u64,
    peak_rec_per_s: u64,
}

/// Spawn the ramped SetMined burst overlay on its dedicated client.
#[allow(clippy::too_many_arguments)]
fn spawn_burst(
    client: Arc<Client>,
    pool: Arc<ShardedPool>,
    sched: BurstSchedule,
    setmined_batch: usize,
    shutdown: Arc<AtomicBool>,
    measuring: Arc<AtomicBool>,
    rpc_lat: Arc<std::sync::Mutex<Vec<u64>>>,
    total_records: Arc<AtomicU64>,
    peak_achieved: Arc<AtomicU64>,
    rec_counters: Arc<[AtomicU64; NUM_OPS]>,
    errors: Arc<AtomicU64>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let run_start = Instant::now();
        // Tick at a fine granularity; each tick, compute the schedule's target
        // rate and issue enough setMined records (in `setmined_batch` chunks) to
        // hit that rate over the tick.
        let tick = Duration::from_millis(100);
        let mut block_height: u32 = 900_000;
        let mut was_in_burst = false;
        let mut burst_start = Instant::now();
        let mut burst_records_this = 0u64;
        loop {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            tokio::time::sleep(tick).await;
            let t = run_start.elapsed();
            let in_burst = sched.in_burst(t);
            if in_burst && !was_in_burst {
                block_height = block_height.wrapping_add(1);
                burst_start = Instant::now();
                burst_records_this = 0;
            }
            if !in_burst {
                if was_in_burst && burst_records_this > 0 {
                    // Burst just ended: record its achieved peak records/s.
                    let dur = burst_start.elapsed().as_secs_f64().max(1e-9);
                    let achieved = (burst_records_this as f64 / dur) as u64;
                    peak_achieved.fetch_max(achieved, Ordering::Relaxed);
                }
                was_in_burst = false;
                continue;
            }
            was_in_burst = true;

            let rate = sched.rate_at(t); // records/sec target right now
            let want = (rate * tick.as_secs_f64()) as usize;
            if want == 0 {
                continue;
            }
            // Gather up to `want` spent keys across shards and mark them mined.
            // Keep the full (txid, hash) tuple so a failed setMined chunk can be
            // rolled back to the Spent state rather than leaving the key stuck
            // as deletable-but-never-confirmed.
            let mut keys: Vec<Key> = Vec::with_capacity(want);
            let nshards = pool.len();
            for idx in 0..nshards {
                if keys.len() >= want {
                    break;
                }
                if let Ok(mut sp) = pool.shard(idx).lock() {
                    let need = want - keys.len();
                    let marked = sp.mark_mined(need);
                    keys.extend(marked);
                }
            }
            if keys.is_empty() {
                continue;
            }
            let params = SetMinedBatchParams {
                block_id: block_height,
                block_height,
                subtree_idx: 0,
                on_longest_chain: true,
                unset_mined: false,
                current_block_height: block_height,
                block_height_retention: 288,
            };
            // Fire chunks concurrently from this single burst client. Each
            // future returns its full key chunk so a failed chunk can be rolled
            // back to Spent.
            let mut futs = Vec::new();
            for chunk in keys.chunks(setmined_batch.clamp(1, BURST_MAX_BATCH)) {
                let client = client.clone();
                let params = params.clone();
                let chunk = chunk.to_vec();
                futs.push(tokio::spawn(async move {
                    let txids: Vec<[u8; 32]> = chunk.iter().map(|(t, _)| *t).collect();
                    let t0 = Instant::now();
                    let r = client.set_mined_batch(&params, &txids).await;
                    (r, chunk, t0.elapsed().as_micros() as u64)
                }));
            }
            let measuring_now = measuring.load(Ordering::Relaxed);
            for f in futs {
                if let Ok((res, chunk, lat)) = f.await {
                    let n = chunk.len();
                    match res {
                        Ok(_) => {
                            rec_counters[OpKind::SetMined.index()]
                                .fetch_add(n as u64, Ordering::Relaxed);
                            burst_records_this += n as u64;
                            if measuring_now {
                                total_records.fetch_add(n as u64, Ordering::Relaxed);
                                if let Ok(mut l) = rpc_lat.lock() {
                                    l.push(lat);
                                }
                            }
                        }
                        Err(ref e) => {
                            log_err(&errors, "set_mined(burst)", e);
                            // Roll the failed keys back to Spent (shard 0) so the
                            // lifecycle stays correct: they are not yet confirmed
                            // mined, so they must not be eligible for delete.
                            if let Ok(mut sp) = pool.shard(0).lock() {
                                sp.return_to_spent(chunk);
                            }
                        }
                    }
                }
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Mix workload (legacy simple mode)
// ---------------------------------------------------------------------------

/// Run the simple weighted-mix workload (legacy default).
async fn run_mix(args: Args) {
    let mix = match Mix::parse(&args.mix) {
        Ok(m) => Arc::new(m),
        Err(e) => {
            eprintln!("Invalid --mix: {e}");
            std::process::exit(1);
        }
    };
    let outputs_per_create = args.outputs_per_create.max(1);
    let saturate = args.saturate || args.rate == 0;
    let conns = args.conns.unwrap_or(args.workers).max(4);
    let cfg = build_config(&args, conns);

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
    if conns > 1 {
        eprintln!("Warming up {conns} connections...");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    let shutdown = Arc::new(AtomicBool::new(false));
    let measuring = Arc::new(AtomicBool::new(false));
    let inflight = Arc::new(AtomicU64::new(0));
    let inflight_hwm = Arc::new(AtomicU64::new(0));
    let rec_counters: Arc<[AtomicU64; NUM_OPS]> = Arc::new(Default::default());
    let errors = Arc::new(AtomicU64::new(0));

    let interval_us = if saturate {
        0
    } else {
        (1_000_000u64 * args.workers as u64)
            .checked_div(args.rate)
            .unwrap_or(0)
    };
    let batch = args.batch.max(1);

    eprintln!(
        "Mix: {} workers, {} conns, batch={}, mix=\"{}\", outputs/create={}, {} mode, {}s warmup + {}s measure{}\n",
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

    let burst_pool: Arc<std::sync::Mutex<Vec<[u8; 32]>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let burst_drains: Arc<std::sync::Mutex<Vec<u64>>> = Arc::new(std::sync::Mutex::new(Vec::new()));

    // Stats printer.
    {
        let shutdown = shutdown.clone();
        let inflight = inflight.clone();
        let inflight_hwm = inflight_hwm.clone();
        let rec_counters = rec_counters.clone();
        let errors = errors.clone();
        tokio::spawn(async move {
            let mut last = [0u64; NUM_OPS];
            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                let mut now = [0u64; NUM_OPS];
                let mut total_delta = 0u64;
                for i in 0..NUM_OPS {
                    now[i] = rec_counters[i].load(Ordering::Relaxed);
                    total_delta += now[i] - last[i];
                }
                eprintln!(
                    "  {} ops/s | inflight={}/hwm={} | create={} spend={} get={} setmined={} unlock={} delete={} errors={}",
                    total_delta / 2,
                    inflight.load(Ordering::Relaxed),
                    inflight_hwm.load(Ordering::Relaxed),
                    (now[0] - last[0]) / 2,
                    (now[1] - last[1]) / 2,
                    (now[2] - last[2]) / 2,
                    (now[3] - last[3]) / 2,
                    (now[4] - last[4]) / 2,
                    (now[5] - last[5]) / 2,
                    errors.load(Ordering::Relaxed),
                );
                last = now;
            }
        });
    }

    let window = {
        let measuring = measuring.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(warmup).await;
            measuring.store(true, Ordering::Relaxed);
            let start = Instant::now();
            tokio::time::sleep(measure).await;
            measuring.store(false, Ordering::Relaxed);
            shutdown.store(true, Ordering::Relaxed);
            start.elapsed()
        })
    };

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
                let txids: Vec<[u8; 32]> = {
                    let p = match burst_pool.lock() {
                        Ok(p) => p,
                        Err(_) => continue,
                    };
                    let take = burst_size.min(p.len());
                    if take == 0 {
                        continue;
                    }
                    p[p.len() - take..].to_vec()
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
        let rec_counters = rec_counters.clone();
        let errors = errors.clone();
        let inflight = inflight.clone();
        let inflight_hwm = inflight_hwm.clone();
        let burst_pool = burst_pool.clone();
        let track_burst = args.burst_size > 0;

        handles.push(tokio::spawn(async move {
            let mut local: std::collections::VecDeque<Key> =
                std::collections::VecDeque::with_capacity(8192);
            let mut block_height: u32 = 800_000 + wid as u32 * 100_000;
            let mut rng: u64 = wid as u64 ^ 0xDEAD_BEEF_CAFE_1234;

            let mut samples: WorkerSamples = Default::default();
            let mut ok_counts = [0u64; NUM_OPS];
            let mut failed_counts = [0u64; NUM_OPS];
            let mut records = [0u64; NUM_OPS];

            macro_rules! account {
                ($op:expr, $lat:expr, $n:expr, $ok:expr) => {{
                    let i = $op.index();
                    rec_counters[i].fetch_add($n as u64, Ordering::Relaxed);
                    if measuring.load(Ordering::Relaxed) {
                        if $ok {
                            ok_counts[i] += 1;
                            records[i] += $n as u64;
                            samples[i].push($lat);
                        } else {
                            failed_counts[i] += 1;
                        }
                    }
                }};
            }

            while !shutdown.load(Ordering::Relaxed) {
                if interval_us > 0 {
                    tokio::time::sleep(Duration::from_micros(interval_us)).await;
                }
                block_height = block_height.wrapping_add(1);
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;

                let pop_batch = |q: &mut std::collections::VecDeque<Key>| {
                    let mut v = Vec::with_capacity(batch);
                    for _ in 0..batch {
                        match q.pop_front() {
                            Some(e) => v.push(e),
                            None => break,
                        }
                    }
                    v
                };

                macro_rules! do_create {
                    () => {{
                        let (items, firsts) = make_creates(batch, outputs_per_create, &mut rng);
                        let t0 = Instant::now();
                        let res = {
                            let _g = InflightGuard::new(&inflight, &inflight_hwm);
                            client.create_batch(&items).await
                        };
                        let lat = t0.elapsed().as_micros() as u64;
                        match res {
                            Ok(_) => {
                                if track_burst {
                                    if let Ok(mut p) = burst_pool.lock() {
                                        for (txid, _) in &firsts {
                                            p.push(*txid);
                                        }
                                    }
                                }
                                local.extend(firsts);
                                account!(OpKind::Create, lat, items.len(), true);
                            }
                            Err(ref e) => {
                                log_err(&errors, "create", e);
                                account!(OpKind::Create, lat, 0, false);
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
                            match res {
                                Ok(_) => {
                                    account!(OpKind::Spend, lat, items.len(), true);
                                }
                                Err(ref e) => {
                                    log_err(&errors, "spend", e);
                                    account!(OpKind::Spend, lat, 0, false);
                                    for e in entries {
                                        local.push_back(e);
                                    }
                                }
                            }
                        }
                    }
                    OpKind::Unlock => {
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
                            match res {
                                Ok(_) => account!(OpKind::Unlock, lat, txids.len(), true),
                                Err(ref e) => {
                                    log_err(&errors, "unlock", e);
                                    account!(OpKind::Unlock, lat, 0, false);
                                }
                            }
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
                            match res {
                                Ok(_) => account!(OpKind::Get, lat, txids.len(), true),
                                Err(ref e) => {
                                    log_err(&errors, "get", e);
                                    account!(OpKind::Get, lat, 0, false);
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
                            match res {
                                Ok(_) => account!(OpKind::SetMined, lat, txids.len(), true),
                                Err(ref e) => {
                                    log_err(&errors, "set_mined", e);
                                    account!(OpKind::SetMined, lat, 0, false);
                                }
                            }
                            for e in entries {
                                local.push_back(e);
                            }
                        }
                    }
                    OpKind::Delete => {
                        let entries = pop_batch(&mut local);
                        if !entries.is_empty() {
                            let txids: Vec<[u8; 32]> = entries.iter().map(|(t, _)| *t).collect();
                            let t0 = Instant::now();
                            let res = {
                                let _g = InflightGuard::new(&inflight, &inflight_hwm);
                                client.delete_batch(&txids).await
                            };
                            let lat = t0.elapsed().as_micros() as u64;
                            match res {
                                Ok(_) => account!(OpKind::Delete, lat, txids.len(), true),
                                Err(ref e) => {
                                    log_err(&errors, "delete", e);
                                    account!(OpKind::Delete, lat, 0, false);
                                    for e in entries {
                                        local.push_back(e);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            WorkerResult {
                samples,
                ok: ok_counts,
                failed: failed_counts,
                records,
            }
        }));
    }

    let mut worker_results = Vec::with_capacity(handles.len());
    for h in handles {
        if let Ok(r) = h.await {
            worker_results.push(r);
        }
    }
    shutdown.store(true, Ordering::Relaxed);
    let measured_elapsed = window.await.unwrap_or(measure);
    if let Some(bh) = burst_handle {
        let _ = bh.await;
    }

    // Legacy burst drain stats.
    let mut burst_vec = burst_drains.lock().map(|d| d.clone()).unwrap_or_default();
    burst_vec.sort_unstable();
    let legacy_burst = if !burst_vec.is_empty() {
        Some(BurstReport {
            rpc_lat: burst_vec,
            total_records: 0,
            peak_rec_per_s: 0,
        })
    } else {
        None
    };

    // Batch sizes for record-rate reporting in mix mode: uniform `batch`.
    let mix_batches = BatchSizes {
        create: batch,
        spend: batch,
        read: batch,
        delete: batch,
        setmined: batch,
    };
    report(
        &worker_results,
        measured_elapsed,
        &mix_batches,
        errors.load(Ordering::Relaxed),
        inflight_hwm.load(Ordering::Relaxed),
        args.workers,
        args.json,
        legacy_burst,
    );
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Per-op statistics derived from merged worker results.
struct OpStat {
    op: OpKind,
    ok_rpcs: u64,
    failed_rpcs: u64,
    records: u64,
    /// Records per second (records / measurement window seconds).
    rec_per_s: f64,
    /// RPCs per second.
    rpc_per_s: f64,
    p50: f64,
    p99: f64,
    p999: f64,
}

/// Merge worker results, compute per-op stats, print a human summary, and emit
/// the `LOADGEN_RESULT {json}` line.
#[allow(clippy::too_many_arguments)]
fn report(
    worker_results: &[WorkerResult],
    measured_elapsed: Duration,
    _batches: &BatchSizes,
    errors: u64,
    inflight_hwm: u64,
    workers: usize,
    json: bool,
    burst: Option<BurstReport>,
) {
    let mut merged: [Vec<u64>; NUM_OPS] = Default::default();
    let mut ok = [0u64; NUM_OPS];
    let mut failed = [0u64; NUM_OPS];
    let mut records = [0u64; NUM_OPS];
    for wr in worker_results {
        for op in OpKind::ALL {
            let i = op.index();
            merged[i].extend_from_slice(&wr.samples[i]);
            ok[i] += wr.ok[i];
            failed[i] += wr.failed[i];
            records[i] += wr.records[i];
        }
    }
    for v in merged.iter_mut() {
        v.sort_unstable();
    }
    let secs = measured_elapsed.as_secs_f64().max(1e-9);

    let mut op_stats = Vec::new();
    for op in OpKind::ALL {
        let i = op.index();
        op_stats.push(OpStat {
            op,
            ok_rpcs: ok[i],
            failed_rpcs: failed[i],
            records: records[i],
            rec_per_s: records[i] as f64 / secs,
            rpc_per_s: ok[i] as f64 / secs,
            p50: percentile(&merged[i], 50.0),
            p99: percentile(&merged[i], 99.0),
            p999: percentile(&merged[i], 99.9),
        });
    }

    // SetMined records measured by the steady path (mix mode) plus the burst
    // overlay (recipe). For recipe, the SetMined op_stat above is empty because
    // the burst path records separately; surface the burst totals explicitly.
    let total_records: u64 = records.iter().sum();
    eprintln!(
        "\nDone. Measurement window {secs:.1}s, {total_records} records over window | peak in-flight RPCs={inflight_hwm} (of {workers} workers)",
    );
    eprintln!(
        "  {:<9} {:>10} {:>8} {:>12} {:>12} {:>12} {:>9} {:>9} {:>9}",
        "op", "ok_rpcs", "fail", "rpc/s", "rec/s", "records", "p50_us", "p99_us", "p999_us"
    );
    for st in &op_stats {
        eprintln!(
            "  {:<9} {:>10} {:>8} {:>12.0} {:>12.0} {:>12} {:>9.0} {:>9.0} {:>9.0}",
            st.op.name(),
            st.ok_rpcs,
            st.failed_rpcs,
            st.rpc_per_s,
            st.rec_per_s,
            st.records,
            st.p50,
            st.p99,
            st.p999,
        );
    }

    // Burst section.
    let mut burst_lat_sorted;
    let (burst_count, b_p50, b_p99, b_max, b_records, b_peak) = match &burst {
        Some(b) => {
            burst_lat_sorted = b.rpc_lat.clone();
            burst_lat_sorted.sort_unstable();
            (
                burst_lat_sorted.len(),
                percentile(&burst_lat_sorted, 50.0),
                percentile(&burst_lat_sorted, 99.0),
                burst_lat_sorted.last().copied().unwrap_or(0) as f64,
                b.total_records,
                b.peak_rec_per_s,
            )
        }
        None => (0, 0.0, 0.0, 0.0, 0, 0),
    };
    if burst_count > 0 || b_records > 0 {
        eprintln!(
            "  burst: rpcs={burst_count} records={b_records} peak_rec_per_s={b_peak} drain_p50={:.1}ms drain_p99={:.1}ms drain_max={:.1}ms",
            b_p50 / 1000.0,
            b_p99 / 1000.0,
            b_max / 1000.0,
        );
    }
    if errors > 0 {
        eprintln!("  errors={errors}");
    }

    if json {
        let mut results = String::new();
        for (idx, st) in op_stats.iter().enumerate() {
            if idx > 0 {
                results.push(',');
            }
            results.push_str(&format!(
                "{{\"op\":\"{}\",\"ok_rpcs\":{},\"failed_rpcs\":{},\"records\":{},\"rec_per_s\":{:.3},\"rpc_per_s\":{:.3},\"p50_us\":{:.1},\"p99_us\":{:.1},\"p999_us\":{:.1}}}",
                st.op.name(),
                st.ok_rpcs,
                st.failed_rpcs,
                st.records,
                st.rec_per_s,
                st.rpc_per_s,
                st.p50,
                st.p99,
                st.p999,
            ));
        }
        let json = format!(
            "{{\"duration_s\":{secs:.3},\"workers\":{workers},\"results\":[{results}],\"burst\":{{\"rpcs\":{burst_count},\"records\":{b_records},\"peak_rec_per_s\":{b_peak},\"drain_p50_us\":{b_p50:.1},\"drain_p99_us\":{b_p99:.1},\"drain_max_us\":{b_max:.1}}}}}",
        );
        println!("LOADGEN_RESULT {json}");
    }
}

/// Categorize and count an op error (and bump the global error counter).
fn log_err(errors: &AtomicU64, op: &str, e: &ClientError) {
    let n = errors.fetch_add(1, Ordering::Relaxed);
    if n < 20 {
        match e {
            ClientError::Partial(pe) => {
                let codes: Vec<String> = pe
                    .errors
                    .iter()
                    .take(4)
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
            _ => eprintln!("  ERR [{op}]: {e}"),
        }
    }
}

/// RAII gauge for one in-flight round-trip.
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
        let data: Vec<u64> = (1..=100).collect();
        assert_eq!(percentile(&data, 50.0), 50.0);
        assert_eq!(percentile(&data, 99.0), 99.0);
        assert_eq!(percentile(&data, 99.9), 100.0);
        assert_eq!(percentile(&data, 100.0), 100.0);
        assert_eq!(percentile(&data, 0.0), 1.0);
    }

    #[test]
    fn percentile_small_and_empty() {
        assert_eq!(percentile(&[], 50.0), 0.0);
        assert_eq!(percentile(&[42], 50.0), 42.0);
        assert_eq!(percentile(&[42], 99.9), 42.0);
        let data: Vec<u64> = (1..=10).map(|x| x * 10).collect();
        assert_eq!(percentile(&data, 50.0), 50.0);
        assert_eq!(percentile(&data, 99.0), 100.0);
    }

    #[test]
    fn mix_parse_default() {
        let m = Mix::parse("create=1,spend=1,unlock=1").unwrap();
        assert_eq!(m.total, 3);
        assert_eq!(m.cumulative[OpKind::Create.index()], 1);
        assert_eq!(m.cumulative[OpKind::Spend.index()], 2);
        assert_eq!(m.cumulative[OpKind::Get.index()], 2);
        assert_eq!(m.cumulative[OpKind::SetMined.index()], 2);
        assert_eq!(m.cumulative[OpKind::Unlock.index()], 3);
        assert_eq!(m.cumulative[OpKind::Delete.index()], 3);
    }

    #[test]
    fn mix_select_cumulative_thresholds() {
        let m = Mix::parse("create=1,spend=1,unlock=1").unwrap();
        assert!(m.select(0) == OpKind::Create);
        assert!(m.select(3) == OpKind::Create);
        assert!(m.select(1) == OpKind::Spend);
        assert!(m.select(4) == OpKind::Spend);
        assert!(m.select(2) == OpKind::Unlock);
        assert!(m.select(5) == OpKind::Unlock);
    }

    #[test]
    fn mix_select_distribution() {
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
        assert!(Mix::parse("frobnicate=1").is_err());
        assert!(Mix::parse("create").is_err());
        assert!(Mix::parse("create=abc").is_err());
        assert!(Mix::parse("create=0,spend=0").is_err());
    }

    #[test]
    fn mix_parse_includes_get_setmined_delete() {
        let m = Mix::parse("get=2,setmined=5,delete=3").unwrap();
        assert_eq!(m.total, 10);
        assert_eq!(m.cumulative[OpKind::Get.index()], 2);
        assert_eq!(m.cumulative[OpKind::SetMined.index()], 7);
        assert_eq!(m.cumulative[OpKind::Delete.index()], 10);
        assert!(m.select(0) == OpKind::Get);
        assert!(m.select(2) == OpKind::SetMined);
        assert!(m.select(7) == OpKind::Delete);
    }

    // --- Per-op batch-size arg parsing / defaults ---

    /// Parse Args from an argv, defaulting addr so parsing succeeds.
    fn parse_args(extra: &[&str]) -> Args {
        let mut argv = vec!["teraslab-loadgen", "--addr", "localhost:3300"];
        argv.extend_from_slice(extra);
        Args::parse_from(argv)
    }

    #[test]
    fn batch_sizes_recipe_defaults() {
        let a = parse_args(&["--recipe"]);
        let b = BatchSizes::from_args(&a);
        assert_eq!(b.create, 488);
        assert_eq!(b.spend, 329);
        assert_eq!(b.read, 291);
        assert_eq!(b.delete, 488);
        assert_eq!(b.setmined, 1024);
    }

    #[test]
    fn batch_sizes_overrides() {
        let a = parse_args(&[
            "--recipe",
            "--create-batch",
            "100",
            "--spend-batch",
            "50",
            "--read-batch",
            "60",
            "--delete-batch",
            "70",
            "--setmined-batch",
            "2048",
        ]);
        let b = BatchSizes::from_args(&a);
        assert_eq!(b.create, 100);
        assert_eq!(b.spend, 50);
        assert_eq!(b.read, 60);
        assert_eq!(b.delete, 70);
        assert_eq!(b.setmined, 2048);
    }

    #[test]
    fn batch_sizes_zero_is_clamped_to_one() {
        let a = parse_args(&["--recipe", "--create-batch", "0"]);
        let b = BatchSizes::from_args(&a);
        assert_eq!(b.create, 1);
    }

    #[test]
    fn recipe_burst_arg_defaults() {
        let a = parse_args(&["--recipe"]);
        assert!(a.recipe);
        assert_eq!(a.burst_interval_secs, 360);
        assert_eq!(a.burst_width_secs, 40);
        assert_eq!(a.burst_peak_rec_per_s, 8_000_000);
        assert_eq!(a.preload, 200_000);
        assert_eq!(a.steady_clients, 12);
    }

    // --- Burst ramp schedule shape ---

    fn test_sched() -> BurstSchedule {
        BurstSchedule {
            interval: Duration::from_secs(100),
            width: Duration::from_secs(40),
            peak_rec_per_s: 8_000_000.0,
        }
    }

    #[test]
    fn burst_zero_between_blocks() {
        let s = test_sched();
        // After the 40s window and before the next interval boundary: 0.
        assert_eq!(s.rate_at(Duration::from_secs(41)), 0.0);
        assert_eq!(s.rate_at(Duration::from_secs(60)), 0.0);
        assert_eq!(s.rate_at(Duration::from_secs(99)), 0.0);
        assert!(!s.in_burst(Duration::from_secs(60)));
    }

    #[test]
    fn burst_ramps_up_then_down() {
        let s = test_sched();
        // Edges of the window are ~0.
        assert_eq!(s.rate_at(Duration::from_secs(0)), 0.0);
        // Quarter way (10s of 40s, half=20) → 50% of peak.
        let q = s.rate_at(Duration::from_secs(10));
        assert!((q - 4_000_000.0).abs() < 1.0, "quarter rate was {q}");
        // Apex at the midpoint (20s) → peak.
        let apex = s.rate_at(Duration::from_secs(20));
        assert!((apex - 8_000_000.0).abs() < 1.0, "apex was {apex}");
        // Three-quarters (30s) → back down to 50% of peak.
        let tq = s.rate_at(Duration::from_secs(30));
        assert!(
            (tq - 4_000_000.0).abs() < 1.0,
            "three-quarter rate was {tq}"
        );
        // Strictly increasing up to apex, strictly decreasing after.
        assert!(s.rate_at(Duration::from_secs(5)) < apex);
        assert!(s.rate_at(Duration::from_secs(35)) < apex);
        assert!(s.rate_at(Duration::from_secs(5)) < s.rate_at(Duration::from_secs(15)));
        assert!(s.rate_at(Duration::from_secs(35)) < s.rate_at(Duration::from_secs(25)));
        assert!(s.in_burst(Duration::from_secs(20)));
    }

    #[test]
    fn burst_recurs_on_interval() {
        let s = test_sched();
        // Same phase one interval later → same rate (periodic).
        let a = s.rate_at(Duration::from_secs(20));
        let b = s.rate_at(Duration::from_secs(120));
        assert!((a - b).abs() < 1.0, "apex not periodic: {a} vs {b}");
        // And zero between the second block's burst too.
        assert_eq!(s.rate_at(Duration::from_secs(160)), 0.0);
    }

    #[test]
    fn burst_triangle_area_matches_block_total() {
        // Integrate the triangle numerically; it should equal peak*width/2
        // (the per-block record total), within a small tolerance.
        let s = test_sched();
        let dt = 0.01;
        let mut area = 0.0;
        let mut t = 0.0;
        while t < 100.0 {
            area += s.rate_at(Duration::from_secs_f64(t)) * dt;
            t += dt;
        }
        let expected = s.peak_rec_per_s * s.width.as_secs_f64() / 2.0;
        assert!(
            (area - expected).abs() / expected < 0.01,
            "area {area} vs expected {expected}"
        );
    }

    // --- Lifecycle pool transitions ---

    fn key(n: u8) -> Key {
        ([n; 32], [n.wrapping_add(100); 32])
    }

    #[test]
    fn pool_create_to_live_to_spent_to_mined_to_delete() {
        let mut p = ShardPool::new();
        p.add_live(key(1));
        p.add_live(key(2));
        assert_eq!(p.live.len(), 2);

        // spend takes Live → Spent.
        let spent = p.take_to_spend(1);
        assert_eq!(spent.len(), 1);
        assert_eq!(p.live.len(), 1);
        assert_eq!(p.spent.len(), 1);

        // setMined marks Spent → deletable.
        let mined = p.mark_mined(5); // ask for more than available
        assert_eq!(mined.len(), 1);
        assert_eq!(p.spent.len(), 0);
        assert_eq!(p.deletable.len(), 1);

        // delete takes spent+mined and removes them entirely.
        let deleted = p.take_to_delete(5);
        assert_eq!(deleted.len(), 1);
        assert_eq!(p.deletable.len(), 0);
    }

    #[test]
    fn pool_delete_only_takes_spent_and_mined() {
        let mut p = ShardPool::new();
        p.add_live(key(1));
        // Live key is never deletable.
        assert!(p.take_to_delete(10).is_empty());
        // Spent-but-not-mined is never deletable.
        let _ = p.take_to_spend(1);
        assert!(p.take_to_delete(10).is_empty());
        // Only after mark_mined.
        let _ = p.mark_mined(1);
        assert_eq!(p.take_to_delete(10).len(), 1);
    }

    #[test]
    fn pool_spend_only_takes_unspent() {
        let mut p = ShardPool::new();
        p.add_live(key(1));
        // First spend consumes the only live key.
        assert_eq!(p.take_to_spend(10).len(), 1);
        // No more live keys → spend takes nothing (won't re-spend the spent one).
        assert!(p.take_to_spend(10).is_empty());
        assert_eq!(p.spent.len(), 1);
    }

    #[test]
    fn pool_setmined_only_marks_spent() {
        let mut p = ShardPool::new();
        p.add_live(key(1));
        // Live (unspent) key cannot be marked mined.
        assert!(p.mark_mined(10).is_empty());
        // Only spent keys are markable.
        let _ = p.take_to_spend(1);
        assert_eq!(p.mark_mined(10).len(), 1);
    }

    #[test]
    fn pool_spend_failure_returns_to_live() {
        let mut p = ShardPool::new();
        p.add_live(key(1));
        let taken = p.take_to_spend(1);
        assert_eq!(p.spent.len(), 1);
        assert_eq!(p.live.len(), 0);
        // Failed spend → return to live, remove from spent.
        p.return_to_live(taken);
        assert_eq!(p.live.len(), 1);
        assert_eq!(p.spent.len(), 0);
    }

    #[test]
    fn pool_mined_failure_returns_to_spent() {
        let mut p = ShardPool::new();
        p.add_live(key(1));
        let _ = p.take_to_spend(1);
        let mined = p.mark_mined(1);
        assert_eq!(p.deletable.len(), 1);
        p.return_to_spent(mined);
        assert_eq!(p.spent.len(), 1);
        assert_eq!(p.deletable.len(), 0);
    }

    #[test]
    fn pool_read_does_not_change_state() {
        let mut p = ShardPool::new();
        p.add_live(key(1));
        p.add_live(key(2));
        let read = p.peek_live(5);
        assert_eq!(read.len(), 2);
        // peek_live leaves everything Live.
        assert_eq!(p.live.len(), 2);
        assert_eq!(p.spent.len(), 0);
    }

    #[test]
    fn sharded_pool_distributes() {
        let sp = ShardedPool::new(4);
        assert_eq!(sp.len(), 4);
        // shard() wraps modulo the shard count.
        for i in 0..8usize {
            let target = i % 4;
            assert!(std::ptr::eq(sp.shard(i), &sp.shards[target]));
        }
        // Add a live key to one shard; the others stay empty.
        sp.shard(0).lock().unwrap().add_live(key(1));
        assert_eq!(sp.shard(0).lock().unwrap().live.len(), 1);
        assert_eq!(sp.shard(1).lock().unwrap().live.len(), 0);
    }
}
