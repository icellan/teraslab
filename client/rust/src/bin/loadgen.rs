//! Load generator for TeraSlab.
//!
//! Generates a mixed workload of creates, spends, reads, and setMined operations
//! against a running TeraSlab server or cluster using the Rust client library.
//!
//! Usage:
//!   teraslab-loadgen --addr localhost:3300 --rate 500 --duration 300
//!   teraslab-loadgen --seeds localhost:3300,localhost:3310 --workers 8 --rate 2000

// CLI binary: stderr/stdout output is the user-facing reporting channel, so
// the workspace-level `disallowed_macros` ban on eprintln!/println! does not
// apply here.
#![allow(clippy::disallowed_macros)]

use clap::Parser;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use teraslab_client::*;

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

    /// Target operations per second (total across all workers).
    #[arg(long, default_value = "500")]
    rate: u64,

    /// Duration in seconds.
    #[arg(long, default_value = "300")]
    duration: u64,

    /// Number of concurrent worker tasks.
    #[arg(long, default_value = "4")]
    workers: usize,

    /// Items per batched RPC. >1 amortizes the per-batch redo fsync across many
    /// items (one group-commit per RPC), which is how production pushes past the
    /// single-item fsync floor. Counters still tally individual items.
    #[arg(long, default_value = "1")]
    batch: usize,

    /// Connection pool size. Defaults to the worker count so each worker holds
    /// its own connection and all `workers` RPCs are truly in flight at once
    /// (the single-node pool serves one round-trip per connection). Too few
    /// connections re-serializes the workers on the pool regardless of how many
    /// worker tasks there are.
    #[arg(long)]
    conns: Option<usize>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    if args.addr.is_none() && args.seeds.is_none() {
        eprintln!("Must specify --addr or --seeds");
        std::process::exit(1);
    }

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
    let creates = Arc::new(AtomicU64::new(0));
    let spends = Arc::new(AtomicU64::new(0));
    let reads = Arc::new(AtomicU64::new(0));
    let mined_count = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));

    // Error categorization for debugging.
    let err_partial = Arc::new(AtomicU64::new(0));
    let err_redirect = Arc::new(AtomicU64::new(0));
    let err_connection = Arc::new(AtomicU64::new(0));
    let err_server = Arc::new(AtomicU64::new(0));
    let err_other = Arc::new(AtomicU64::new(0));
    let err_logged = Arc::new(AtomicU64::new(0)); // cap detail logging

    let interval_us = (1_000_000u64 * args.workers as u64)
        .checked_div(args.rate)
        .unwrap_or(0);
    let batch = args.batch.max(1);

    eprintln!(
        "Running: {} ops/s target, {} workers, {} conns, batch={}, {}s duration\n",
        args.rate, args.workers, conns, batch, args.duration
    );

    let start = Instant::now();
    let duration = Duration::from_secs(args.duration);

    // Stats printer.
    let shutdown_s = shutdown.clone();
    let (c_s, s_s, r_s, m_s, e_s) = (
        creates.clone(),
        spends.clone(),
        reads.clone(),
        mined_count.clone(),
        errors.clone(),
    );
    let stats = tokio::spawn(async move {
        let mut last = (0u64, 0u64, 0u64, 0u64);
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            if shutdown_s.load(Ordering::Relaxed) {
                break;
            }
            let c = c_s.load(Ordering::Relaxed);
            let s = s_s.load(Ordering::Relaxed);
            let r = r_s.load(Ordering::Relaxed);
            let m = m_s.load(Ordering::Relaxed);
            let e = e_s.load(Ordering::Relaxed);
            let rate = ((c - last.0) + (s - last.1) + (r - last.2) + (m - last.3)) / 2;
            eprintln!(
                "  {rate} ops/s | creates={} spends={} reads={} mined={} errors={e} (totals: {c}/{s}/{r}/{m})",
                (c - last.0) / 2,
                (s - last.1) / 2,
                (r - last.2) / 2,
                (m - last.3) / 2,
            );
            last = (c, s, r, m);
        }
    });

    // Worker tasks.
    let mut handles = Vec::new();
    for wid in 0..args.workers {
        let client = client.clone();
        let shutdown = shutdown.clone();
        let creates = creates.clone();
        let spends = spends.clone();
        let reads = reads.clone();
        let mined_count = mined_count.clone();
        let errors = errors.clone();
        let err_partial = err_partial.clone();
        let err_redirect = err_redirect.clone();
        let err_connection = err_connection.clone();
        let err_server = err_server.clone();
        let err_other = err_other.clone();
        let err_logged = err_logged.clone();

        handles.push(tokio::spawn(async move {
            // Per-worker LOCAL queue of created (txid, first-utxo-hash) — no
            // shared mutex, so workers never serialize on each other. Each
            // worker spends/reads/mines what it created; random 256-bit txids
            // keep workers' key spaces effectively disjoint, so the server's
            // per-key visibility barrier lets their mutations run concurrently.
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
                if start.elapsed() >= duration {
                    shutdown.store(true, Ordering::Relaxed);
                    break;
                }

                if interval_us > 0 {
                    tokio::time::sleep(Duration::from_micros(interval_us)).await;
                }

                block_height = block_height.wrapping_add(1);
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;

                // Build `batch` fresh CreateItems and remember each one's first
                // utxo hash for later spends. Used by the create arm and by the
                // spend arm's seed-the-queue fallback.
                let make_creates = |rng: &mut u64| {
                    let mut items = Vec::with_capacity(batch);
                    let mut firsts = Vec::with_capacity(batch);
                    for _ in 0..batch {
                        let mut txid = [0u8; 32];
                        fill_random(&mut txid, rng);
                        let n = 2 + (*rng % 4) as usize;
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

                let op = (rng % 10) as u8;
                match op {
                    0..4 => {
                        let (items, firsts) = make_creates(&mut rng);
                        match client.create_batch(&items).await {
                            Ok(_) => {
                                creates.fetch_add(items.len() as u64, Ordering::Relaxed);
                                local.extend(firsts);
                            }
                            Err(ref e) => log_err("create", e),
                        }
                    }
                    4..7 => {
                        let entries = pop_batch(&mut local);
                        if entries.is_empty() {
                            // Nothing to spend yet — seed with a batch of creates.
                            let (items, firsts) = make_creates(&mut rng);
                            match client.create_batch(&items).await {
                                Ok(_) => {
                                    creates.fetch_add(items.len() as u64, Ordering::Relaxed);
                                    local.extend(firsts);
                                }
                                Err(ref e) => log_err("create", e),
                            }
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
                            match client.spend_batch(&params, &items).await {
                                Ok(_) => {
                                    spends.fetch_add(items.len() as u64, Ordering::Relaxed);
                                }
                                Err(ref e) => {
                                    log_err("spend", e);
                                    for e in entries {
                                        local.push_back(e);
                                    }
                                }
                            }
                        }
                    }
                    7..9 => {
                        let entries = pop_batch(&mut local);
                        if !entries.is_empty() {
                            let txids: Vec<[u8; 32]> = entries.iter().map(|(t, _)| *t).collect();
                            let mask = teraslab::protocol::codec::FieldMask::ALL_METADATA;
                            match client.get_batch(mask, &txids).await {
                                Ok(_) => {
                                    reads.fetch_add(txids.len() as u64, Ordering::Relaxed);
                                }
                                Err(ref e) => log_err("get", e),
                            }
                            for e in entries {
                                local.push_back(e);
                            }
                        }
                    }
                    _ => {
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
                            match client.set_mined_batch(&params, &txids).await {
                                Ok(_) => {
                                    mined_count.fetch_add(txids.len() as u64, Ordering::Relaxed);
                                }
                                Err(ref e) => log_err("set_mined", e),
                            }
                            for e in entries {
                                local.push_back(e);
                            }
                        }
                    }
                }
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }
    shutdown.store(true, Ordering::Relaxed);
    let _ = stats.await;

    let elapsed = start.elapsed();
    let c = creates.load(Ordering::Relaxed);
    let s = spends.load(Ordering::Relaxed);
    let r = reads.load(Ordering::Relaxed);
    let m = mined_count.load(Ordering::Relaxed);
    let e = errors.load(Ordering::Relaxed);
    let total = c + s + r + m;
    let ops = if elapsed.as_secs_f64() > 0.0 {
        total as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    eprintln!(
        "\nDone in {:.1}s: {total} total ops ({ops:.0} ops/s)",
        elapsed.as_secs_f64()
    );
    eprintln!("  creates={c} spends={s} reads={r} mined={m} errors={e}");
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
