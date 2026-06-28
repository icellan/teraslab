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

/// A working-set key: `(txid, first_utxo_hash)`. The hash is needed to build
/// spend items; the txid drives every other op.
type Key = ([u8; 32], [u8; 32]);

/// Per-key lifecycle flags. A key is created Live (unspent, unmined). The
/// SPEND stream sets `spent`; the burst sets `mined`. The two flags are
/// independent — a key can be mined while still live (the burst marks the whole
/// "block" = keys created since the last burst, regardless of whether each has
/// been spent yet). A key becomes deletable only once BOTH flags are set.
#[derive(Clone, Copy)]
struct KeyEntry {
    key: Key,
    spent: bool,
    mined: bool,
}

impl KeyEntry {
    fn new(key: Key) -> KeyEntry {
        KeyEntry {
            key,
            spent: false,
            mined: false,
        }
    }
}

/// One shard of the working-set key pool. Keys are partitioned across many
/// shards (one or more per stream task) so the four independent streams never
/// serialize on a single global lock — the recipe stresses high concurrency
/// with no hot-key contention, so the pool must not introduce one itself.
///
/// Every method takes/drops a small *owned* snapshot under the lock and returns
/// it; NO method returns a lock guard, so a caller can never accidentally hold
/// the shard lock across an `.await` / RPC. This is the central correctness rule
/// (snapshot under brief lock → release → network call) that fixes the
/// multi-second stalls in the old single-loop scheduler.
struct ShardPool {
    /// Freshly created keys, still LOCKED on the server. They are NOT spendable
    /// until the UNLOCK stream clears the lock (set_locked_batch(false)). The
    /// UNLOCK stream drains this queue; on success the keys move to `unspent`.
    locked: Vec<Key>,
    /// Keys unlocked and not yet spent (spendable + readable). `mined` may be
    /// true or false.
    unspent: Vec<KeyEntry>,
    /// Spent but not yet mined (waiting for the burst).
    spent: Vec<KeyEntry>,
    /// Spent AND mined → eligible for delete (prune).
    deletable: Vec<KeyEntry>,
    /// Keys created since the last burst snapshot ("the block"). The burst
    /// drains this and marks each as mined. Holds the txid only; the burst
    /// looks the key up in `spent`/`unspent` to flip its `mined` flag.
    created_since_burst: Vec<[u8; 32]>,
}

impl ShardPool {
    fn new() -> ShardPool {
        ShardPool {
            locked: Vec::new(),
            unspent: Vec::new(),
            spent: Vec::new(),
            deletable: Vec::new(),
            created_since_burst: Vec::new(),
        }
    }

    /// Record a freshly created LOCKED key. It joins (a) the unlock queue (NOT
    /// spendable until unlocked) and (b) the since-last-burst block so the next
    /// burst can mark it mined. This is the create→unlock-queue+block-set edge of
    /// the causal model.
    fn add_locked(&mut self, key: Key) {
        self.locked.push(key);
        self.created_since_burst.push(key.0);
    }

    /// Record a key that is already unlocked + spendable (used by pre-load, which
    /// creates then unlocks before timing). It is unspent+unmined and joins the
    /// since-last-burst block so the next burst can mark it mined.
    fn add_live(&mut self, key: Key) {
        self.unspent.push(KeyEntry::new(key));
        self.created_since_burst.push(key.0);
    }

    /// Drain up to `n` locked keys for the UNLOCK stream. Returns the keys to
    /// unlock (may be fewer than `n`, or empty → caller yields). The caller
    /// issues set_locked_batch(false) for them, then calls [`Self::mark_unlocked`]
    /// on success or [`Self::return_to_locked`] on failure.
    fn take_to_unlock(&mut self, n: usize) -> Vec<Key> {
        let take = n.min(self.locked.len());
        self.locked.drain(self.locked.len() - take..).collect()
    }

    /// Mark keys as unlocked (after a successful set_locked_batch(false)): they
    /// become spendable/readable (`unspent`). Each was already counted in the
    /// since-last-burst block at create time, so no block bookkeeping here.
    fn mark_unlocked(&mut self, keys: &[Key]) {
        for k in keys {
            self.unspent.push(KeyEntry::new(*k));
        }
    }

    /// On a failed unlock, return the keys to the locked queue (still locked on
    /// the server) so a later unlock attempt retries them.
    fn return_to_locked(&mut self, keys: Vec<Key>) {
        self.locked.extend(keys);
    }

    /// Take up to `n` unspent keys to spend. A spent-but-not-mined key moves to
    /// `spent`; a key that was already mined (by an earlier burst while still
    /// live) goes straight to `deletable`. Returns the keys taken (may be fewer
    /// than `n`, or empty). Spend only ever takes UNSPENT keys.
    fn take_to_spend(&mut self, n: usize) -> Vec<Key> {
        let take = n.min(self.unspent.len());
        let taken: Vec<KeyEntry> = self.unspent.drain(self.unspent.len() - take..).collect();
        let mut out = Vec::with_capacity(taken.len());
        for mut e in taken {
            e.spent = true;
            out.push(e.key);
            if e.mined {
                self.deletable.push(e);
            } else {
                self.spent.push(e);
            }
        }
        out
    }

    /// Sample up to `n` unspent keys to read (GetMeta/decorate). Reads don't
    /// change state, so the keys stay where they are. Returns the keys read.
    fn peek_live(&self, n: usize) -> Vec<Key> {
        let take = n.min(self.unspent.len());
        self.unspent[self.unspent.len() - take..]
            .iter()
            .map(|e| e.key)
            .collect()
    }

    /// Snapshot and clear the "created since last burst" block, returning the
    /// txids to mark mined. The caller issues setMined for these, then calls
    /// [`Self::apply_mined`] to flip the flags. Returns an empty Vec when no
    /// keys were created since the last snapshot (caller yields).
    fn drain_block(&mut self) -> Vec<[u8; 32]> {
        std::mem::take(&mut self.created_since_burst)
    }

    /// Mark the given txids as mined (after a successful setMined). A spent key
    /// becomes deletable; an unspent key stays unspent but flagged mined (it
    /// becomes deletable when later spent). Unknown/already-deleted txids are
    /// ignored. Returns the number of keys that newly became deletable.
    fn apply_mined(&mut self, txids: &[[u8; 32]]) -> usize {
        let want: std::collections::HashSet<[u8; 32]> = txids.iter().copied().collect();
        // Spent → deletable.
        let mut newly = 0usize;
        let mut i = 0;
        while i < self.spent.len() {
            if want.contains(&self.spent[i].key.0) {
                let mut e = self.spent.swap_remove(i);
                e.mined = true;
                self.deletable.push(e);
                newly += 1;
            } else {
                i += 1;
            }
        }
        // Unspent → flag mined in place (stays spendable).
        for e in self.unspent.iter_mut() {
            if want.contains(&e.key.0) {
                e.mined = true;
            }
        }
        newly
    }

    /// Take up to `n` spent+mined keys to delete (prune), removing them from the
    /// pool entirely. Returns the keys deleted. Delete only ever takes keys that
    /// are both spent and mined.
    fn take_to_delete(&mut self, n: usize) -> Vec<Key> {
        let take = n.min(self.deletable.len());
        self.deletable
            .drain(self.deletable.len() - take..)
            .map(|e| e.key)
            .collect()
    }

    /// On a failed spend, return keys to the unspent list (they were not
    /// actually spent on the server). Removes them from `spent`/`deletable`
    /// first, restoring their pre-spend flags.
    fn return_to_unspent(&mut self, keys: Vec<Key>) {
        for k in &keys {
            let mined = if let Some(pos) = self.spent.iter().rposition(|e| e.key.0 == k.0) {
                self.spent.swap_remove(pos).mined
            } else if let Some(pos) = self.deletable.iter().rposition(|e| e.key.0 == k.0) {
                self.deletable.swap_remove(pos).mined
            } else {
                false
            };
            self.unspent.push(KeyEntry {
                key: *k,
                spent: false,
                mined,
            });
        }
    }

    /// Number of keys currently tracked in this shard (any state). Used by the
    /// live-pool bound check / stats.
    fn total(&self) -> usize {
        self.locked.len() + self.unspent.len() + self.spent.len() + self.deletable.len()
    }
}

/// A sharded pool of working-set keys. Each stream task owns shard
/// `task % shards`, so each task touches its own lock on the hot path. The
/// SetMined burst and the delete progression sweep across shards.
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

    /// Total keys tracked across all shards (any state). Brief per-shard locks;
    /// never held across an await.
    fn total(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.lock().map(|sp| sp.total()).unwrap_or(0))
            .sum()
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

    /// Number of steady-stream clients to model (recipe: 12). Used as the total
    /// steady worker budget when --recipe is set and --workers is left at default.
    #[arg(long, default_value = "12")]
    steady_clients: usize,

    /// Concurrent CREATE-stream tasks. 0 = auto (derive from the steady worker
    /// budget). Each stream runs its OWN independent flat-out tasks.
    #[arg(long, default_value = "0")]
    create_workers: usize,
    /// Concurrent UNLOCK-stream tasks. 0 = auto (defaults to the create task
    /// count so unlock keeps pace with create). NOT carved from the budget split.
    #[arg(long, default_value = "0")]
    unlock_workers: usize,
    /// Concurrent SPEND-stream tasks. 0 = auto.
    #[arg(long, default_value = "0")]
    spend_workers: usize,
    /// Concurrent READ-stream tasks. 0 = auto.
    #[arg(long, default_value = "0")]
    read_workers: usize,
    /// Concurrent DELETE-stream tasks. 0 = auto.
    #[arg(long, default_value = "0")]
    delete_workers: usize,

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

    /// Max concurrent SetMined RPCs in-flight during a burst window. The burst
    /// also paces issuance to spread its units across `--burst-width-secs`, so it
    /// does not monopolize a fsync-bound server and starve the steady streams
    /// (which keeps steady p50 in milliseconds). Lower = gentler on the server.
    #[arg(long, default_value = "4")]
    burst_concurrency: usize,

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

/// Resolved per-stream task counts for the recipe. Each stream runs this many
/// independent flat-out tasks. The total steady budget is `--workers` (or
/// `--steady_clients` when `--workers` is left at its default), split across the
/// four streams; explicit `--<stream>-workers` overrides win.
struct StreamWorkers {
    create: usize,
    /// UNLOCK tasks. Unlock must keep pace with create (it clears the lock on
    /// every freshly created tx), so it defaults to the create task count and is
    /// NOT carved out of the four-way budget split.
    unlock: usize,
    spend: usize,
    read: usize,
    delete: usize,
}

/// Compute per-stream task counts from the steady budget. The budget is split
/// evenly across the four streams (remainder favouring create, then spend),
/// honouring any explicit per-stream override (>0). Every stream gets at least
/// one task so none is ever starved out of existence.
fn resolve_stream_workers(budget: usize, a: &Args) -> StreamWorkers {
    let budget = budget.max(4);
    let base = budget / 4;
    let rem = budget % 4;
    // Distribute the remainder: create first, then spend, then read.
    let auto = [
        base + usize::from(rem > 0),
        base + usize::from(rem > 1),
        base + usize::from(rem > 2),
        base,
    ];
    let pick = |explicit: usize, auto: usize| if explicit > 0 { explicit } else { auto.max(1) };
    let create = pick(a.create_workers, auto[0]);
    StreamWorkers {
        create,
        // Unlock defaults to the create count so it keeps pace; explicit wins.
        unlock: pick(a.unlock_workers, create),
        spend: pick(a.spend_workers, auto[1]),
        read: pick(a.read_workers, auto[2]),
        delete: pick(a.delete_workers, auto[3]),
    }
}

/// Run the realistic UTXO-DB benchmark recipe workload.
///
/// Four CONTINUOUS, INDEPENDENT streams (create / spend / read / delete), each
/// its own set of flat-out async tasks with its own shard of the working set,
/// plus one SetMined burst task on a dedicated client. There is NO cross-stream
/// scheduler: each stream loops doing only its own op, and yields briefly when
/// its input pool is empty. No shard lock is ever held across an `.await`.
async fn run_recipe(args: Args) {
    let batches = BatchSizes::from_args(&args);
    // Steady budget: prefer explicit --workers; else the recipe's 12 clients.
    let budget = if args.workers == 4 {
        args.steady_clients.max(1)
    } else {
        args.workers.max(1)
    };
    let sw = resolve_stream_workers(budget, &args);
    let total_steady = sw.create + sw.unlock + sw.spend + sw.read + sw.delete;
    let saturate = args.saturate || args.rate == 0;
    // One connection per steady task plus headroom for the burst client.
    let conns = args.conns.unwrap_or(total_steady).max(4);

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

    // Sharded working set: a handful of shards per CREATE task keeps the create
    // and spend hot paths on their own locks while leaving the delete sweeper
    // and the burst plenty of shards to find keys across.
    let nshards = (total_steady * 4).max(8);
    let pool = Arc::new(ShardedPool::new(nshards));

    // Pre-load: create `preload` records (batched) into the working set so
    // reads/spends/deletes hit existing keys. Not sampled. Spread across shards.
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

    let shutdown = Arc::new(AtomicBool::new(false));
    let measuring = Arc::new(AtomicBool::new(false));
    let inflight = Arc::new(AtomicU64::new(0));
    let inflight_hwm = Arc::new(AtomicU64::new(0));
    let rec_counters: Arc<[AtomicU64; NUM_OPS]> = Arc::new(Default::default());
    let errors = Arc::new(AtomicU64::new(0));

    let warmup = Duration::from_secs(args.warmup_secs);
    let measure = Duration::from_secs(args.duration);

    // Burst stats.
    let burst_rpc_lat: Arc<std::sync::Mutex<Vec<u64>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let burst_records = Arc::new(AtomicU64::new(0));
    let burst_peak_achieved = Arc::new(AtomicU64::new(0));
    let burst_count = Arc::new(AtomicU64::new(0));

    // Stats printer.
    {
        let shutdown = shutdown.clone();
        let inflight = inflight.clone();
        let inflight_hwm = inflight_hwm.clone();
        let rec_counters = rec_counters.clone();
        let errors = errors.clone();
        let pool = pool.clone();
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
                    "  rec/s create={} unlock={} spend={} read={} delete={} setmined={} | inflight={}/{} live_pool={} errors={}",
                    per(OpKind::Create.index()),
                    per(OpKind::Unlock.index()),
                    per(OpKind::Spend.index()),
                    per(OpKind::Get.index()),
                    per(OpKind::Delete.index()),
                    per(OpKind::SetMined.index()),
                    inflight.load(Ordering::Relaxed),
                    inflight_hwm.load(Ordering::Relaxed),
                    pool.total(),
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

    // Burst overlay on a SEPARATE dedicated client (recipe: 1 burst client vs
    // the steady fleet). Fires every `burst_interval_secs`.
    let burst_handle = if args.burst_peak_rec_per_s > 0 {
        let burst_cfg = build_config(&args, 8);
        match Client::new(burst_cfg).await {
            Ok(bc) => {
                let bc = Arc::new(bc);
                Some(spawn_burst(
                    bc,
                    pool.clone(),
                    Duration::from_secs(args.burst_interval_secs.max(1)),
                    Duration::from_secs(args.burst_width_secs.max(1)),
                    batches.setmined,
                    args.burst_concurrency,
                    shutdown.clone(),
                    measuring.clone(),
                    inflight.clone(),
                    inflight_hwm.clone(),
                    burst_rpc_lat.clone(),
                    burst_records.clone(),
                    burst_peak_achieved.clone(),
                    burst_count.clone(),
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
        "Recipe: streams create={}/unlock={}/spend={}/read={}/delete={} tasks ({total_steady} steady), {conns} conns, batches create={}/spend={}/read={}/delete={}/setmined={}, {} mode, {}s warmup + {}s measure; burst peak={} rec/s every {}s over {}s\n",
        sw.create,
        sw.unlock,
        sw.spend,
        sw.read,
        sw.delete,
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

    // Optional per-task pacing (records/sec target spread across all steady
    // tasks, paced per RPC by the stream's batch size). 0 = saturate.
    let pace_us = |batch: usize, ntasks: usize| -> u64 {
        if saturate {
            0
        } else {
            (1_000_000u64 * ntasks as u64 * batch as u64)
                .checked_div(args.rate.max(1))
                .unwrap_or(0)
        }
    };

    let outputs = args.outputs_per_create.max(1);
    let mut handles = Vec::new();
    // Assign each task a distinct shard offset so different streams of the same
    // index don't all pile onto shard 0.
    let mut tid = 0usize;
    let mut spawn_stream = |stream: OpKind, ntasks: usize, batch: usize| {
        let pace = pace_us(batch, ntasks);
        for _ in 0..ntasks {
            let ctx = StreamCtx {
                stream,
                shard0: tid,
                batch,
                outputs,
                pace_us: pace,
                client: client.clone(),
                pool: pool.clone(),
                shutdown: shutdown.clone(),
                measuring: measuring.clone(),
                inflight: inflight.clone(),
                inflight_hwm: inflight_hwm.clone(),
                rec_counters: rec_counters.clone(),
                errors: errors.clone(),
                seed: (tid as u64).wrapping_mul(0x2545_F491_4F6C_DD1D) ^ 0xDEAD_BEEF,
            };
            handles.push(tokio::spawn(stream_task(ctx)));
            tid += 1;
        }
    };
    spawn_stream(OpKind::Create, sw.create, batches.create);
    spawn_stream(OpKind::Unlock, sw.unlock, batches.create);
    spawn_stream(OpKind::Spend, sw.spend, batches.spend);
    spawn_stream(OpKind::Get, sw.read, batches.read);
    spawn_stream(OpKind::Delete, sw.delete, batches.delete);

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
        total_steady,
        args.json,
        Some(BurstReport {
            rpc_lat: burst_rpc_lat.lock().map(|d| d.clone()).unwrap_or_default(),
            total_records: burst_records.load(Ordering::Relaxed),
            peak_rec_per_s: burst_peak_achieved.load(Ordering::Relaxed),
            bursts: burst_count.load(Ordering::Relaxed),
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
        // Pre-load creates LOCKED txs (as the recipe requires) and immediately
        // unlocks them so the timed window starts with a spendable working set.
        let (items, firsts) = make_creates(n, outputs, true, &mut rng);
        match client.create_batch(&items).await {
            Ok(_) => {
                let txids: Vec<[u8; 32]> = firsts.iter().map(|(t, _)| *t).collect();
                if let Err(e) = client.set_locked_batch(false, &txids).await {
                    eprintln!("  preload unlock error: {e}");
                    break;
                }
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
    // The pre-loaded keys form the genesis "block": the first burst will mark
    // exactly these as mined. They are already queued in `created_since_burst`
    // by add_live above, so no extra bookkeeping is needed here.
    created
}

/// CREATE-wire flags byte that marks a transaction LOCKED on the server.
///
/// NOTE: the create-wire flags byte is a SEPARATE namespace from the persisted
/// `TxFlags`. The server's create dispatch and the replication receiver both
/// decode the create-wire byte as `locked=0x01, conflicting=0x02, frozen=0x04`
/// — NOT the persisted-TxFlags layout (coinbase=0x01, conflicting=0x02,
/// locked=0x04). So a LOCKED create is `flags = 0x01` on the wire. (Sending
/// 0x04 would create a FROZEN UTXO, which `set_locked(false)` cannot clear —
/// spends would fail FROZEN forever.) The recipe creates every tx LOCKED; a
/// spend of a locked UTXO is rejected until the UNLOCK stream clears it.
const FLAG_LOCKED: u8 = 0b0000_0001;

/// Build `n` fresh CreateItems and the `(txid, first_utxo_hash)` of each. When
/// `locked` is true each tx carries the LOCKED flag, so its UTXO is not spendable
/// until an explicit `set_locked_batch(false)` (the UNLOCK stream).
fn make_creates(
    n: usize,
    outputs: usize,
    locked: bool,
    rng: &mut u64,
) -> (Vec<CreateItem>, Vec<Key>) {
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
            flags: if locked { FLAG_LOCKED } else { 0 },
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

/// Everything one steady stream task needs. Each stream (create/spend/read/
/// delete) is a set of these tasks running flat-out; the `stream` field selects
/// which op the task performs. No field is ever a lock guard — the pool hands
/// back owned snapshots so nothing is held across the RPC await.
struct StreamCtx {
    stream: OpKind,
    /// Base shard for this task (it primarily touches `shard0`; delete sweeps).
    shard0: usize,
    batch: usize,
    outputs: usize,
    /// Optional inter-RPC pacing in micros (0 = saturate / flat-out).
    pace_us: u64,
    client: Arc<Client>,
    pool: Arc<ShardedPool>,
    shutdown: Arc<AtomicBool>,
    measuring: Arc<AtomicBool>,
    inflight: Arc<AtomicU64>,
    inflight_hwm: Arc<AtomicU64>,
    rec_counters: Arc<[AtomicU64; NUM_OPS]>,
    errors: Arc<AtomicU64>,
    seed: u64,
}

/// One independent steady-stream task. Loops flat-out performing ONLY its own
/// op. When its input pool is empty it yields briefly (so create can refill)
/// rather than blocking a lock or busy-spinning — the key fix for the old
/// single-loop self-deadlock. It NEVER holds a shard lock across the RPC: it
/// drains a small owned batch under a brief lock, releases, then awaits.
async fn stream_task(ctx: StreamCtx) -> WorkerResult {
    let StreamCtx {
        stream,
        shard0,
        batch,
        outputs,
        pace_us,
        client,
        pool,
        shutdown,
        measuring,
        inflight,
        inflight_hwm,
        rec_counters,
        errors,
        seed,
    } = ctx;

    let mut rng: u64 = seed | 1; // non-zero for xorshift
    let mut block_height: u32 = 800_000 + (shard0 as u32 % 64) * 100_000;
    let mut samples: WorkerSamples = Default::default();
    let mut ok = [0u64; NUM_OPS];
    let mut failed = [0u64; NUM_OPS];
    let mut records = [0u64; NUM_OPS];

    // Short idle when the input pool is empty: yield to the scheduler, and after
    // a few consecutive misses sleep a touch so we don't spin hot on an empty
    // pool while create catches up. Never blocks a lock.
    let mut idle_streak: u32 = 0;
    macro_rules! idle {
        () => {{
            idle_streak += 1;
            if idle_streak > 8 {
                tokio::time::sleep(Duration::from_micros(200)).await;
            } else {
                tokio::task::yield_now().await;
            }
            continue;
        }};
    }

    macro_rules! account {
        ($op:expr, $lat:expr, $n:expr, $ok:expr) => {{
            let i = $op.index();
            rec_counters[i].fetch_add($n as u64, Ordering::Relaxed);
            if measuring.load(Ordering::Relaxed) {
                if $ok {
                    ok[i] += 1;
                    records[i] += $n as u64;
                    samples[i].push($lat);
                } else {
                    failed[i] += 1;
                }
            }
        }};
    }

    let nshards = pool.len();
    while !shutdown.load(Ordering::Relaxed) {
        if pace_us > 0 {
            tokio::time::sleep(Duration::from_micros(pace_us)).await;
        }
        block_height = block_height.wrapping_add(1);

        match stream {
            OpKind::Create => {
                idle_streak = 0;
                // Create LOCKED txs: their UTXOs are NOT spendable until the
                // UNLOCK stream clears the lock. Each created key joins the shard's
                // unlock queue + the since-last-burst block.
                let (items, firsts) = make_creates(batch, outputs, true, &mut rng);
                let t0 = Instant::now();
                let res = {
                    let _g = InflightGuard::new(&inflight, &inflight_hwm);
                    client.create_batch(&items).await
                };
                let lat = t0.elapsed().as_micros() as u64;
                match res {
                    Ok(_) => {
                        // Push created keys into the LOCKED queue under a brief
                        // lock (no await held). The UNLOCK stream drains it.
                        if let Ok(mut sp) = pool.shard(shard0).lock() {
                            for f in firsts {
                                sp.add_locked(f);
                            }
                        }
                        account!(OpKind::Create, lat, items.len(), true);
                    }
                    Err(ref e) => {
                        log_err(&errors, "create", e);
                        account!(OpKind::Create, lat, 0, false);
                    }
                }
            }
            OpKind::Unlock => {
                // Drain a batch of locked keys (the just-created txs) the CREATE
                // stream queued. Create writes to its own shards, so unlock SWEEPS
                // shards (brief per-shard locks, none held across the await)
                // starting at this task's base shard until it finds locked keys.
                // The source shard is remembered so the keys become spendable in
                // the same shard the spend/read tasks read from. Empty everywhere
                // → yield (never block).
                let mut src = shard0;
                let mut entries = Vec::new();
                for off in 0..nshards {
                    let idx = (shard0 + off) % nshards;
                    if let Ok(mut sp) = pool.shard(idx).lock() {
                        entries = sp.take_to_unlock(batch);
                        if !entries.is_empty() {
                            src = idx;
                            break;
                        }
                    }
                }
                if entries.is_empty() {
                    idle!();
                }
                idle_streak = 0;
                let txids: Vec<[u8; 32]> = entries.iter().map(|(t, _)| *t).collect();
                let t0 = Instant::now();
                let res = {
                    let _g = InflightGuard::new(&inflight, &inflight_hwm);
                    client.set_locked_batch(false, &txids).await
                };
                let lat = t0.elapsed().as_micros() as u64;
                match res {
                    Ok(_) => {
                        if let Ok(mut sp) = pool.shard(src).lock() {
                            sp.mark_unlocked(&entries);
                        }
                        account!(OpKind::Unlock, lat, txids.len(), true);
                    }
                    Err(ref e) => {
                        log_err(&errors, "unlock", e);
                        if let Ok(mut sp) = pool.shard(src).lock() {
                            sp.return_to_locked(entries);
                        }
                        account!(OpKind::Unlock, lat, 0, false);
                    }
                }
            }
            OpKind::Spend => {
                // Sweep shards for unspent (unlocked) keys under brief per-shard
                // locks, release BEFORE the RPC. The source shard is remembered so
                // a failed spend returns the keys to the same shard. Empty
                // everywhere → yield (never block).
                let mut src = shard0;
                let mut entries = Vec::new();
                for off in 0..nshards {
                    let idx = (shard0 + off) % nshards;
                    if let Ok(mut sp) = pool.shard(idx).lock() {
                        entries = sp.take_to_spend(batch);
                        if !entries.is_empty() {
                            src = idx;
                            break;
                        }
                    }
                }
                if entries.is_empty() {
                    idle!();
                }
                idle_streak = 0;
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
                    Ok(_) => account!(OpKind::Spend, lat, items.len(), true),
                    Err(ref e) => {
                        log_err(&errors, "spend", e);
                        if let Ok(mut sp) = pool.shard(src).lock() {
                            sp.return_to_unspent(entries);
                        }
                        account!(OpKind::Spend, lat, 0, false);
                    }
                }
            }
            OpKind::Get => {
                // Sweep shards sampling read keys (no state change) under brief
                // per-shard locks, release, then RPC. Empty everywhere → yield.
                let mut entries = Vec::new();
                for off in 0..nshards {
                    let idx = (shard0 + off) % nshards;
                    if let Ok(sp) = pool.shard(idx).lock() {
                        entries = sp.peek_live(batch);
                        if !entries.is_empty() {
                            break;
                        }
                    }
                }
                if entries.is_empty() {
                    idle!();
                }
                idle_streak = 0;
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
            }
            OpKind::Delete => {
                // Delete prunes spent+mined keys, which the burst marks across
                // shards. Sweep shards (brief per-shard locks, none held across
                // an await) starting at this task's base shard until we find a
                // deletable batch. Empty everywhere → yield.
                let mut entries = Vec::new();
                for off in 0..nshards {
                    let idx = (shard0 + off) % nshards;
                    if let Ok(mut sp) = pool.shard(idx).lock() {
                        entries = sp.take_to_delete(batch);
                        if !entries.is_empty() {
                            break;
                        }
                    }
                }
                if entries.is_empty() {
                    idle!();
                }
                idle_streak = 0;
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
                        // Delete failed: the keys are still spent+mined on the
                        // server, so they remain deletable — but we already
                        // removed them from the pool. We simply drop them (they
                        // leak from our view, harmless for the bench). Count the
                        // failure.
                        log_err(&errors, "delete", e);
                        account!(OpKind::Delete, lat, 0, false);
                    }
                }
            }
            _ => {}
        }
    }

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
    bursts: u64,
}

/// Spawn the periodic SetMined burst on its dedicated client.
///
/// Every `interval`, snapshot the txids CREATED SINCE THE LAST BURST (the
/// "block" = ingest since the last block), and mark them mined via
/// set_mined_batch in `setmined_batch` chunks issued with bounded concurrency
/// over the brief `width` window; then idle until the next interval. Marking
/// mined flips each key's `mined` flag — a key that is also spent becomes
/// deletable, feeding the DELETE stream. Snapshots are drained under brief
/// per-shard locks (never held across the RPC).
#[allow(clippy::too_many_arguments)]
fn spawn_burst(
    client: Arc<Client>,
    pool: Arc<ShardedPool>,
    interval: Duration,
    width: Duration,
    setmined_batch: usize,
    max_concurrency: usize,
    shutdown: Arc<AtomicBool>,
    measuring: Arc<AtomicBool>,
    inflight: Arc<AtomicU64>,
    inflight_hwm: Arc<AtomicU64>,
    rpc_lat: Arc<std::sync::Mutex<Vec<u64>>>,
    total_records: Arc<AtomicU64>,
    peak_achieved: Arc<AtomicU64>,
    burst_count: Arc<AtomicU64>,
    rec_counters: Arc<[AtomicU64; NUM_OPS]>,
    errors: Arc<AtomicU64>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let run_start = Instant::now();
        let mut block_height: u32 = 900_000;
        let mut next_burst = run_start + interval;
        let nshards = pool.len();
        // Max concurrent setMined RPCs in-flight during a burst window
        // (caller-configured via --burst-concurrency; clamped to >=1).
        let max_concurrency = max_concurrency.max(1);
        loop {
            // Idle until the next burst boundary (checking shutdown).
            loop {
                if shutdown.load(Ordering::Relaxed) {
                    return;
                }
                if Instant::now() >= next_burst {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            next_burst += interval;
            block_height = block_height.wrapping_add(1);

            // Snapshot the whole "block" = keys created since the last burst,
            // across all shards. Brief per-shard locks; nothing held across an
            // await. Remember which shard each txid came from so we can flip its
            // mined flag in the right shard after the RPC succeeds.
            let mut chunks: Vec<(usize, Vec<[u8; 32]>)> = Vec::new();
            for idx in 0..nshards {
                if let Ok(mut sp) = pool.shard(idx).lock() {
                    let block = sp.drain_block();
                    if !block.is_empty() {
                        chunks.push((idx, block));
                    }
                }
            }
            let total: usize = chunks.iter().map(|(_, b)| b.len()).sum();
            if total == 0 {
                continue;
            }
            burst_count.fetch_add(1, Ordering::Relaxed);

            let params = SetMinedBatchParams {
                block_id: block_height,
                block_height,
                subtree_idx: 0,
                on_longest_chain: true,
                unset_mined: false,
                current_block_height: block_height,
                block_height_retention: 288,
            };

            // Flatten into setmined_batch-sized RPC units, each tagged with its
            // source shard for the post-RPC flag flip.
            let chunk_sz = setmined_batch.clamp(1, BURST_MAX_BATCH);
            let mut units: Vec<(usize, Vec<[u8; 32]>)> = Vec::new();
            for (idx, block) in chunks {
                for c in block.chunks(chunk_sz) {
                    units.push((idx, c.to_vec()));
                }
            }

            let burst_start = Instant::now();
            let mut burst_records_this = 0u64;
            let deadline = burst_start + width;
            let measuring_now = measuring.load(Ordering::Relaxed);

            // Pace issuance to spread the units across the burst width so the
            // burst does not flood the fsync-bound server in one instant and
            // starve the steady streams. Each unit is released at most every
            // `unit_pace` apart; bounded concurrency still caps in-flight RPCs.
            let unit_count = units.len().max(1);
            let unit_pace = width
                .checked_div(unit_count as u32)
                .unwrap_or(Duration::ZERO);
            let mut next_unit_at = burst_start;

            // Issue units with bounded concurrency over the burst window.
            let mut iter = units.into_iter();
            let mut inflight_futs = Vec::new();
            loop {
                while inflight_futs.len() < max_concurrency {
                    match iter.next() {
                        Some((idx, txids)) => {
                            // Width pacing: wait until this unit's release slot
                            // (skips ahead if we're already behind). Bounded so it
                            // never blocks past the burst deadline.
                            if unit_pace > Duration::ZERO {
                                let now = Instant::now();
                                if next_unit_at > now && next_unit_at < deadline {
                                    tokio::time::sleep(next_unit_at - now).await;
                                }
                                next_unit_at += unit_pace;
                            }
                            let client = client.clone();
                            let params = params.clone();
                            let inflight = inflight.clone();
                            let inflight_hwm = inflight_hwm.clone();
                            inflight_futs.push(tokio::spawn(async move {
                                let t0 = Instant::now();
                                let r = {
                                    let _g = InflightGuard::new(&inflight, &inflight_hwm);
                                    client.set_mined_batch(&params, &txids).await
                                };
                                (idx, txids, r, t0.elapsed().as_micros() as u64)
                            }));
                        }
                        None => break,
                    }
                }
                let Some(f) = inflight_futs.pop() else {
                    break;
                };
                if let Ok((idx, txids, res, lat)) = f.await {
                    let n = txids.len();
                    match res {
                        Ok(_) => {
                            // Flip the mined flag in the source shard: spent keys
                            // become deletable for the DELETE stream.
                            if let Ok(mut sp) = pool.shard(idx).lock() {
                                sp.apply_mined(&txids);
                            }
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
                            // Failed: leave the keys' mined flag unset (they stay
                            // spent-unmined / unspent), so they are not wrongly
                            // eligible for delete. They re-enter the next block
                            // only if re-created; for the bench we simply don't
                            // confirm them. Count the failure.
                            log_err(&errors, "set_mined(burst)", e);
                        }
                    }
                }
                // Spread the issuance across the window: if we're ahead of the
                // ramp, pace slightly. Keeps the burst "bursty" not instant.
                if Instant::now() < deadline && !shutdown.load(Ordering::Relaxed) {
                    tokio::task::yield_now().await;
                }
            }

            let dur = burst_start.elapsed().as_secs_f64().max(1e-9);
            if burst_records_this > 0 {
                let achieved = (burst_records_this as f64 / dur) as u64;
                peak_achieved.fetch_max(achieved, Ordering::Relaxed);
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
                        // Mix mode creates UNLOCKED (legacy behavior); its UNLOCK
                        // op exercises set_locked(false) independently.
                        let (items, firsts) =
                            make_creates(batch, outputs_per_create, false, &mut rng);
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
            bursts: 0,
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
    let (burst_count, b_p50, b_p99, b_max, b_records, b_peak, b_bursts) = match &burst {
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
                b.bursts,
            )
        }
        None => (0, 0.0, 0.0, 0.0, 0, 0, 0),
    };
    if burst_count > 0 || b_records > 0 || b_bursts > 0 {
        eprintln!(
            "  burst: fired={b_bursts} rpcs={burst_count} records={b_records} peak_rec_per_s={b_peak} drain_p50={:.1}ms drain_p99={:.1}ms drain_max={:.1}ms",
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
            "{{\"duration_s\":{secs:.3},\"workers\":{workers},\"results\":[{results}],\"burst\":{{\"fired\":{b_bursts},\"rpcs\":{burst_count},\"records\":{b_records},\"peak_rec_per_s\":{b_peak},\"drain_p50_us\":{b_p50:.1},\"drain_p99_us\":{b_p99:.1},\"drain_max_us\":{b_max:.1}}}}}",
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

    // --- Per-stream task count resolution ---

    #[test]
    fn stream_workers_split_even_budget() {
        let a = parse_args(&["--recipe"]);
        let sw = resolve_stream_workers(12, &a);
        // The four-way budget split is unchanged by the unlock stream.
        assert_eq!((sw.create, sw.spend, sw.read, sw.delete), (3, 3, 3, 3));
        let total = sw.create + sw.spend + sw.read + sw.delete;
        assert_eq!(total, 12);
    }

    #[test]
    fn stream_workers_unlock_defaults_to_create_count() {
        let a = parse_args(&["--recipe"]);
        // Unlock is NOT carved from the budget; it mirrors create so it keeps
        // pace with newly created (locked) txs.
        let sw = resolve_stream_workers(12, &a);
        assert_eq!(sw.unlock, sw.create);
        let sw = resolve_stream_workers(9, &a); // create gets the remainder → 3
        assert_eq!(sw.unlock, sw.create);
        assert_eq!(sw.unlock, 3);
    }

    #[test]
    fn stream_workers_unlock_explicit_override_wins() {
        let a = parse_args(&["--recipe", "--unlock-workers", "7"]);
        let sw = resolve_stream_workers(12, &a);
        assert_eq!(sw.unlock, 7);
        assert_eq!(sw.create, 3); // create still auto
    }

    #[test]
    fn stream_workers_remainder_favours_create_then_spend() {
        let a = parse_args(&["--recipe"]);
        // budget 9 → base 2, rem 1 → create gets the extra.
        let sw = resolve_stream_workers(9, &a);
        assert_eq!((sw.create, sw.spend, sw.read, sw.delete), (3, 2, 2, 2));
        // budget 10 → base 2, rem 2 → create + spend get the extras.
        let sw = resolve_stream_workers(10, &a);
        assert_eq!((sw.create, sw.spend, sw.read, sw.delete), (3, 3, 2, 2));
    }

    #[test]
    fn stream_workers_explicit_overrides_win() {
        let a = parse_args(&["--recipe", "--create-workers", "5", "--delete-workers", "2"]);
        let sw = resolve_stream_workers(8, &a);
        assert_eq!(sw.create, 5); // explicit
        assert_eq!(sw.delete, 2); // explicit
        assert_eq!(sw.spend, 2); // auto (8/4)
        assert_eq!(sw.read, 2); // auto
    }

    #[test]
    fn stream_workers_each_stream_at_least_one() {
        let a = parse_args(&["--recipe"]);
        // Even a tiny budget gives every stream a task (budget floored at 4).
        let sw = resolve_stream_workers(1, &a);
        assert!(sw.create >= 1 && sw.spend >= 1 && sw.read >= 1 && sw.delete >= 1);
    }

    // --- Lifecycle pool transitions ---

    fn key(n: u8) -> Key {
        ([n; 32], [n.wrapping_add(100); 32])
    }

    /// Mine by snapshotting the since-last-burst block and applying it, the same
    /// two-step the burst task does (drain under lock → RPC → apply under lock).
    fn burst_mine(p: &mut ShardPool) -> usize {
        let block = p.drain_block();
        p.apply_mined(&block)
    }

    #[test]
    fn pool_create_to_live_to_spent_to_mined_to_delete() {
        let mut p = ShardPool::new();
        p.add_live(key(1));
        p.add_live(key(2));
        assert_eq!(p.unspent.len(), 2);

        // spend takes unspent → spent.
        let spent = p.take_to_spend(1);
        assert_eq!(spent.len(), 1);
        assert_eq!(p.unspent.len(), 1);
        assert_eq!(p.spent.len(), 1);

        // burst marks the created-since-last-burst block as mined: the spent
        // one becomes deletable, the still-unspent one is flagged mined in place.
        let newly = burst_mine(&mut p);
        assert_eq!(newly, 1);
        assert_eq!(p.spent.len(), 0);
        assert_eq!(p.deletable.len(), 1);
        assert_eq!(p.unspent.len(), 1);
        assert!(p.unspent[0].mined, "unspent key should be flagged mined");

        // delete takes spent+mined and removes them entirely.
        let deleted = p.take_to_delete(5);
        assert_eq!(deleted.len(), 1);
        assert_eq!(p.deletable.len(), 0);
    }

    #[test]
    fn pool_delete_only_takes_spent_and_mined() {
        let mut p = ShardPool::new();
        p.add_live(key(1));
        // Unspent key is never deletable.
        assert!(p.take_to_delete(10).is_empty());
        // Spend it, but don't mine yet → still not deletable.
        let _ = p.take_to_spend(1);
        assert!(p.take_to_delete(10).is_empty());
        // Only after the burst marks it mined.
        assert_eq!(burst_mine(&mut p), 1);
        assert_eq!(p.take_to_delete(10).len(), 1);
    }

    #[test]
    fn pool_delete_does_not_take_mined_but_unspent() {
        // A key mined while still live (burst marked the whole block) is NOT
        // deletable until it is also spent.
        let mut p = ShardPool::new();
        p.add_live(key(1));
        assert_eq!(burst_mine(&mut p), 0); // nothing spent → nothing newly deletable
        assert!(p.unspent[0].mined);
        // Not deletable yet.
        assert!(p.take_to_delete(10).is_empty());
        // Spend it → now spent+mined → deletable immediately.
        let taken = p.take_to_spend(1);
        assert_eq!(taken.len(), 1);
        assert_eq!(p.deletable.len(), 1);
        assert_eq!(p.take_to_delete(10).len(), 1);
    }

    #[test]
    fn pool_spend_only_takes_unspent() {
        let mut p = ShardPool::new();
        p.add_live(key(1));
        // First spend consumes the only unspent key.
        assert_eq!(p.take_to_spend(10).len(), 1);
        // No more unspent keys → spend takes nothing (won't re-spend a spent one).
        assert!(p.take_to_spend(10).is_empty());
        assert_eq!(p.spent.len(), 1);
    }

    #[test]
    fn pool_burst_marks_created_since_last_burst() {
        let mut p = ShardPool::new();
        // Block 1: two keys created and spent.
        p.add_live(key(1));
        p.add_live(key(2));
        let _ = p.take_to_spend(2);
        // First burst snapshot = exactly keys 1 and 2.
        let block1 = p.drain_block();
        assert_eq!(block1.len(), 2);
        assert_eq!(p.apply_mined(&block1), 2);
        assert_eq!(p.deletable.len(), 2);
        // Block 2: a new key created AFTER the first burst.
        p.add_live(key(3));
        let _ = p.take_to_spend(1);
        let block2 = p.drain_block();
        // Only key 3 is in the second block; keys 1,2 are NOT re-marked.
        assert_eq!(block2.len(), 1);
        assert_eq!(block2[0], key(3).0);
        assert_eq!(p.apply_mined(&block2), 1);
        assert_eq!(p.deletable.len(), 3);
    }

    #[test]
    fn pool_apply_mined_ignores_unknown_txids() {
        let mut p = ShardPool::new();
        p.add_live(key(1));
        let _ = p.take_to_spend(1);
        // Apply a txid that isn't in the pool (e.g. already deleted) → no-op.
        assert_eq!(p.apply_mined(&[key(9).0]), 0);
        assert_eq!(p.spent.len(), 1);
        assert_eq!(p.deletable.len(), 0);
    }

    #[test]
    fn pool_spend_failure_returns_to_unspent() {
        let mut p = ShardPool::new();
        p.add_live(key(1));
        let taken = p.take_to_spend(1);
        assert_eq!(p.spent.len(), 1);
        assert_eq!(p.unspent.len(), 0);
        // Failed spend → return to unspent, remove from spent.
        p.return_to_unspent(taken);
        assert_eq!(p.unspent.len(), 1);
        assert!(!p.unspent[0].spent);
        assert_eq!(p.spent.len(), 0);
    }

    #[test]
    fn pool_spend_failure_preserves_mined_flag() {
        // A key that was mined-while-live then spent (→ deletable) and whose
        // spend then fails must return to unspent with its mined flag intact.
        let mut p = ShardPool::new();
        p.add_live(key(1));
        let _ = burst_mine(&mut p); // flags unspent key mined
        let taken = p.take_to_spend(1); // spent+mined → deletable
        assert_eq!(p.deletable.len(), 1);
        p.return_to_unspent(taken);
        assert_eq!(p.deletable.len(), 0);
        assert_eq!(p.unspent.len(), 1);
        assert!(
            p.unspent[0].mined,
            "mined flag lost on spend-failure rollback"
        );
        assert!(!p.unspent[0].spent);
    }

    // --- LOCKED → UNLOCK causal stage ---

    #[test]
    fn pool_create_locked_joins_unlock_queue_and_block_not_spendable() {
        // A created (locked) key joins the unlock queue AND the since-last-burst
        // block, but is NOT yet spendable/readable.
        let mut p = ShardPool::new();
        p.add_locked(key(1));
        assert_eq!(p.locked.len(), 1);
        // Block set saw it (so the next burst will mine it).
        assert_eq!(p.created_since_burst.len(), 1);
        assert_eq!(p.created_since_burst[0], key(1).0);
        // Not spendable, not readable until unlocked.
        assert!(p.unspent.is_empty());
        assert!(p.take_to_spend(10).is_empty());
        assert!(p.peek_live(10).is_empty());
    }

    #[test]
    fn pool_unlock_makes_key_spendable() {
        let mut p = ShardPool::new();
        p.add_locked(key(1));
        // Unlock drains the locked queue.
        let to_unlock = p.take_to_unlock(10);
        assert_eq!(to_unlock.len(), 1);
        assert!(p.locked.is_empty());
        // Still not spendable until mark_unlocked confirms the RPC succeeded.
        assert!(p.take_to_spend(10).is_empty());
        // After the unlock RPC succeeds, the key becomes spendable + readable.
        p.mark_unlocked(&to_unlock);
        assert_eq!(p.unspent.len(), 1);
        assert_eq!(p.peek_live(10).len(), 1);
        assert_eq!(p.take_to_spend(10).len(), 1);
    }

    #[test]
    fn pool_unlock_failure_returns_to_locked_queue() {
        let mut p = ShardPool::new();
        p.add_locked(key(1));
        let taken = p.take_to_unlock(10);
        assert!(p.locked.is_empty());
        // Failed unlock → keys go back to the locked queue, NOT spendable.
        p.return_to_locked(taken);
        assert_eq!(p.locked.len(), 1);
        assert!(p.take_to_spend(10).is_empty());
        // A later unlock attempt finds them again.
        let retry = p.take_to_unlock(10);
        assert_eq!(retry.len(), 1);
    }

    #[test]
    fn pool_burst_mines_block_even_while_locked() {
        // The block set tracks creation, independent of the lock state: a key
        // created (locked) and not yet unlocked is still in the block and gets
        // mined by the burst (matching the recipe: the block = ingest, not
        // unlock state).
        let mut p = ShardPool::new();
        p.add_locked(key(1));
        let block = p.drain_block();
        assert_eq!(block.len(), 1);
        // apply_mined finds it nowhere in unspent/spent (it's still locked), so
        // nothing newly becomes deletable, but the block was correctly drained.
        assert_eq!(p.apply_mined(&block), 0);
        // Created-since-burst is now empty for the next block.
        assert!(p.drain_block().is_empty());
    }

    #[test]
    fn pool_cold_start_only_create_proceeds() {
        // COLD START: an empty pool — every consumer (unlock/spend/read/delete)
        // pulls nothing (so its task yields), while create alone can proceed by
        // adding to the locked queue. This is the structural anti-deadlock
        // guarantee: no consumer blocks waiting for keys.
        let mut p = ShardPool::new();
        assert!(p.take_to_unlock(10).is_empty(), "unlock yields cold");
        assert!(p.take_to_spend(10).is_empty(), "spend yields cold");
        assert!(p.peek_live(10).is_empty(), "read yields cold");
        assert!(p.take_to_delete(10).is_empty(), "delete yields cold");
        // Create proceeds unconditionally.
        p.add_locked(key(1));
        assert_eq!(p.locked.len(), 1);
        // Now the causal chain can flow: unlock → spend become possible.
        let u = p.take_to_unlock(10);
        p.mark_unlocked(&u);
        assert_eq!(p.take_to_spend(10).len(), 1);
    }

    #[test]
    fn pool_full_causal_chain_create_unlock_spend_mine_delete() {
        // End-to-end pool lifecycle for the causal recipe:
        // create(locked) → unlock → spend → mine(burst) → delete.
        let mut p = ShardPool::new();
        p.add_locked(key(1));
        // unlock
        let u = p.take_to_unlock(10);
        p.mark_unlocked(&u);
        // spend
        let s = p.take_to_spend(10);
        assert_eq!(s.len(), 1);
        assert_eq!(p.spent.len(), 1);
        // mine the block (drained at create time → contains key 1)
        let newly = burst_mine(&mut p);
        assert_eq!(newly, 1);
        assert_eq!(p.deletable.len(), 1);
        // delete
        assert_eq!(p.take_to_delete(10).len(), 1);
        assert_eq!(p.total(), 0);
    }

    #[test]
    fn pool_total_counts_locked_keys() {
        let mut p = ShardPool::new();
        p.add_locked(key(1));
        p.add_locked(key(2));
        assert_eq!(p.total(), 2, "locked keys must count toward total");
        let u = p.take_to_unlock(1);
        p.mark_unlocked(&u);
        assert_eq!(p.total(), 2, "total unchanged across locked→unspent move");
    }

    #[test]
    fn pool_read_does_not_change_state() {
        let mut p = ShardPool::new();
        p.add_live(key(1));
        p.add_live(key(2));
        let read = p.peek_live(5);
        assert_eq!(read.len(), 2);
        // peek_live leaves everything unspent.
        assert_eq!(p.unspent.len(), 2);
        assert_eq!(p.spent.len(), 0);
    }

    #[test]
    fn pool_empty_inputs_yield_none_not_block() {
        // Pulling from empty input pools returns an empty Vec (the caller then
        // yields) rather than panicking or blocking. This is the structural
        // guarantee that lets each stream yield-and-retry on an empty pool.
        let mut p = ShardPool::new();
        assert!(p.take_to_unlock(10).is_empty());
        assert!(p.take_to_spend(10).is_empty());
        assert!(p.peek_live(10).is_empty());
        assert!(p.take_to_delete(10).is_empty());
        assert!(p.drain_block().is_empty());
        assert_eq!(p.apply_mined(&[]), 0);
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
        // Add a key to one shard; the others stay empty.
        sp.shard(0).lock().unwrap().add_live(key(1));
        assert_eq!(sp.shard(0).lock().unwrap().unspent.len(), 1);
        assert_eq!(sp.shard(1).lock().unwrap().unspent.len(), 0);
        assert_eq!(sp.total(), 1);
    }

    /// Structural guarantee that no lock guard is held across an await: the pool
    /// API hands back *owned* snapshots (`Vec<Key>` / `Vec<[u8;32]>`), never a
    /// `MutexGuard`. This test pins the return types so a future refactor that
    /// tried to return a guard (and thus invite holding it across an RPC) fails
    /// to compile here.
    #[test]
    fn pool_methods_return_owned_snapshots_not_guards() {
        let mut p = ShardPool::new();
        p.add_locked(key(2));
        p.add_live(key(1));
        // Each binding is an owned Vec — it outlives any borrow of `p`, proving
        // the lock would already be released before a caller awaits on it.
        let unlocked: Vec<Key> = p.take_to_unlock(1);
        let spent: Vec<Key> = p.take_to_spend(1);
        let read: Vec<Key> = p.peek_live(1);
        let block: Vec<[u8; 32]> = p.drain_block();
        let del: Vec<Key> = p.take_to_delete(1);
        // Use them after `p` is no longer borrowed (compiles only if owned).
        drop(p);
        let _ = (
            unlocked.len(),
            spent.len(),
            read.len(),
            block.len(),
            del.len(),
        );
    }
}
