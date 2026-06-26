//! Real-disk redo fsync scaling probe.
//!
//! Measures the write-path durability cost on REAL temp files via `DirectDevice`
//! (F_NOCACHE on macOS, O_DIRECT on Linux) so fsync is real, not a DRAM no-op.
//!
//! The production write path serializes whole BATCHES on the node-global
//! visibility barrier, and round-robin placement spreads each batch across all K
//! stores → K fsyncs per batch. So the realistic model is: ONE stream of
//! serialized batches, each issuing K fsyncs. The question is whether those K
//! fsyncs run in series (pre-#2) or in parallel (#2).
//!
//! Columns, at store-count K ∈ {1,2,4,8}:
//!   A raw-par   — K threads, each write(4 KiB)+fsync its OWN file in a loop.
//!                 Hardware ceiling for K independent durable streams.
//!   D old-batch — serialized batches; each batch flushes its K store logs
//!                 SEQUENTIALLY (the pre-#2 `append_redo_ops_routed`).
//!   E new-batch — serialized batches; each batch flushes its K store logs in
//!                 PARALLEL via std::thread::scope (#2).
//!   E/D         — the #2 speedup on the realistic batched path.
//!   E/A         — how close the parallel batched path gets to raw disk.
//!
//! Run: cargo run --release --example redo_fsync_scaling

use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Barrier};
use std::time::Instant;

use parking_lot::Mutex;
use teraslab::device::{BlockDevice, DirectDevice};
use teraslab::redo::{RedoLog, RedoOp};

const BATCHES: u64 = 3000;
const LOG_SIZE: u64 = 256 * 1024 * 1024;
const ALIGN: usize = 4096;

fn tmp(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("teraslab_fsync_probe_{name}"))
}

fn op(i: u64, store: u8) -> RedoOp {
    let mut txid = [0u8; 32];
    txid[0..8].copy_from_slice(&i.to_le_bytes());
    RedoOp::AllocateRegion {
        offset: i * 4096,
        size: 4096,
        device_id: store,
    }
}

/// A: K threads each write+fsync their own raw file. Total ops/sec (the ceiling
/// for K fully-independent durable streams).
fn raw_par(k: u64) -> f64 {
    use std::io::Write;
    let barrier = Arc::new(Barrier::new(k as usize));
    let t0 = Instant::now();
    std::thread::scope(|s| {
        for tid in 0..k {
            let barrier = barrier.clone();
            s.spawn(move || {
                let path = tmp(&format!("raw_{k}_{tid}"));
                let mut f = std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(&path)
                    .unwrap();
                let buf = vec![0xABu8; ALIGN];
                barrier.wait();
                for _ in 0..BATCHES {
                    f.write_all(&buf).unwrap();
                    f.sync_all().unwrap();
                }
                let _ = std::fs::remove_file(&path);
            });
        }
    });
    (k * BATCHES) as f64 / t0.elapsed().as_secs_f64()
}

fn open_logs(k: u64, tag: &str) -> (Vec<Arc<Mutex<RedoLog>>>, Vec<PathBuf>) {
    let mut logs = Vec::new();
    let mut paths = Vec::new();
    let shared = Arc::new(AtomicU64::new(1));
    for store in 0..k {
        let path = tmp(&format!("{tag}_{k}_{store}"));
        let dev: Arc<dyn BlockDevice> =
            Arc::new(DirectDevice::open(&path, LOG_SIZE, ALIGN).unwrap());
        let mut log = RedoLog::open(dev, 0, LOG_SIZE).unwrap();
        log.attach_shared_sequence(shared.clone());
        logs.push(Arc::new(Mutex::new(log)));
        paths.push(path);
    }
    (logs, paths)
}

/// D: serialized batches; each batch flushes its K store logs SEQUENTIALLY.
fn old_batched(k: u64) -> f64 {
    let (logs, paths) = open_logs(k, "old");
    let t0 = Instant::now();
    for b in 0..BATCHES {
        for (store, log) in logs.iter().enumerate() {
            let mut g = log.lock();
            g.append(op(b * k + store as u64, store as u8)).unwrap();
            g.flush().unwrap();
        }
    }
    let secs = t0.elapsed().as_secs_f64();
    for p in paths {
        let _ = std::fs::remove_file(p);
    }
    (k * BATCHES) as f64 / secs
}

/// E: serialized batches; each batch flushes its K store logs in PARALLEL,
/// exactly as `Engine::append_redo_ops_routed` now does (#2).
fn new_batched(k: u64) -> f64 {
    let (logs, paths) = open_logs(k, "new");
    let t0 = Instant::now();
    for b in 0..BATCHES {
        std::thread::scope(|scope| {
            let handles: Vec<_> = logs
                .iter()
                .enumerate()
                .map(|(store, log)| {
                    scope.spawn(move || {
                        let mut g = log.lock();
                        g.append(op(b * k + store as u64, store as u8)).unwrap();
                        g.flush().unwrap();
                    })
                })
                .collect();
            for h in handles {
                h.join().unwrap();
            }
        });
    }
    let secs = t0.elapsed().as_secs_f64();
    for p in paths {
        let _ = std::fs::remove_file(p);
    }
    (k * BATCHES) as f64 / secs
}

#[allow(clippy::disallowed_macros)] // diagnostic example: user-facing stdout
fn main() {
    println!(
        "redo fsync scaling — {BATCHES} serialized batches, DirectDevice on {}",
        std::env::temp_dir().display()
    );
    println!();
    println!(
        "{:>2}  {:>14}  {:>16}  {:>16}  {:>7}  {:>7}",
        "K", "A raw-par", "D old-batch", "E new-batch", "E/D", "E/A"
    );
    println!("{}", "-".repeat(74));
    for k in [1u64, 2, 4, 8] {
        let a = raw_par(k);
        let d = old_batched(k);
        let e = new_batched(k);
        println!(
            "{k:>2}  {a:>14.0}  {d:>16.0}  {e:>16.0}  {:>6.2}x  {:>6.2}x",
            e / d,
            e / a
        );
    }
    println!();
    println!("A = raw disk ceiling (K independent write+fsync streams)");
    println!("D = OLD: serialized batches, K SEQUENTIAL fsyncs/batch");
    println!("E = NEW (#2): serialized batches, K PARALLEL fsyncs/batch");
    println!("E/D = #2 speedup on the realistic batched path; E/A = closeness to raw disk");
}
