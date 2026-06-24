//! Fixed-workload write-path throughput bench: 100K creates and 100K spends
//! at apply-concurrency K in {1,2,4,8}. Engine-level (no TCP). Provides the
//! comparable before/after numbers recorded in WRITE_PATH_BASELINE.md.
//!
//! Also benchmarks K=8 at shard_count ∈ {1, 16} to isolate the index-lock
//! contention reduction from sharding (groups `creates_100k_shards` and
//! `spends_100k_shards`).
//!
//! Group `mixed_read_under_write_storm`: measures reader throughput while 4
//! writer threads hammer the index continuously. The sharding win is a READ
//! win: with shard_count=1 a writer holds the single RwLock exclusively,
//! starving all concurrent readers; with shard_count=16 a reader blocked by a
//! write on shard S can still proceed on the other 15 shards.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Instant;

use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, ShardedIndex, TxKey, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::create::CreateRequest;
use teraslab::ops::engine::Engine;
use teraslab::ops::spend::SpendRequest;

const N: u32 = 100_000;

fn make_tx_id(n: u32) -> [u8; 32] {
    let mut t = [0u8; 32];
    t[0..4].copy_from_slice(&n.to_le_bytes());
    // Vary bytes 16..24 so StripedLocks (keyed off those bytes) spreads.
    t[16..20].copy_from_slice(&n.wrapping_mul(0x9E37_79B9).to_le_bytes());
    t
}

fn make_utxo_hash(tx_n: u32, vout: u32) -> [u8; 32] {
    let mut h = [0u8; 32];
    h[0..4].copy_from_slice(&vout.to_le_bytes());
    h[4..8].copy_from_slice(&tx_n.to_le_bytes());
    h
}

fn create_engine() -> Arc<Engine> {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(1024 * 1024 * 1024, 4096).unwrap());
    let alloc = SlotAllocator::new(dev.clone()).unwrap();
    let index = Index::new(2 * N as usize).unwrap();
    Arc::new(Engine::new(
        dev,
        index,
        alloc,
        StripedLocks::new(65536),
        DahIndex::new(),
        UnminedIndex::new(),
    ))
}

/// Build an engine backed by a `ShardedIndex` with `shard_count` index shards.
///
/// Used to measure the index-lock contention reduction at K=8 concurrency.
/// `shard_count = 1` is the degenerate (single-shard) baseline; `shard_count = 16`
/// is the default sharded configuration.
fn create_engine_sharded(shard_count: usize) -> Arc<Engine> {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(1024 * 1024 * 1024, 4096).unwrap());
    let alloc = SlotAllocator::new(dev.clone()).unwrap();
    let index = ShardedIndex::new_in_memory(2 * N as usize, shard_count).unwrap();
    Arc::new(Engine::new_with_sharded_index(
        dev,
        index,
        alloc,
        StripedLocks::new(65536),
        DahIndex::new(),
        UnminedIndex::new(),
    ))
}

fn create_one(engine: &Engine, tx_idx: u32) {
    let utxo_hashes = [make_utxo_hash(tx_idx, 0)];
    let req = CreateRequest {
        tx_id: make_tx_id(tx_idx),
        tx_version: 1,
        locktime: 0,
        fee: 500,
        size_in_bytes: 250,
        extended_size: 0,
        is_coinbase: false,
        spending_height: 0,
        utxo_hashes: &utxo_hashes,
        inputs: None,
        outputs: None,
        inpoints: None,
        is_external: false,
        created_at: 1_710_000_000_000,
        block_height: 1000,
        mined_block_infos: &[],
        frozen: false,
        conflicting: false,
        locked: false,
        external_ref: None,
        parent_txids: &[],
    };
    let _ = engine.create(&req);
}

fn spend_one(engine: &Engine, tx_idx: u32) {
    let mut sd = [0u8; 36];
    sd[0..4].copy_from_slice(&tx_idx.wrapping_add(10_000).to_le_bytes());
    let _ = engine.spend(&SpendRequest {
        tx_key: TxKey {
            txid: make_tx_id(tx_idx),
        },
        offset: 0,
        utxo_hash: make_utxo_hash(tx_idx, 0),
        spending_data: sd,
        ignore_conflicting: false,
        ignore_locked: false,
        current_block_height: 2000,
        block_height_retention: 288,
    });
}

/// Run `work` over the half-open range [0, N) split across `threads` workers,
/// each pulling disjoint indices via an atomic cursor. Returns when all N done.
fn run_concurrent(threads: u32, work: impl Fn(u32) + Sync) {
    let cursor = AtomicU32::new(0);
    std::thread::scope(|s| {
        for _ in 0..threads {
            let cursor = &cursor;
            let work = &work;
            s.spawn(move || {
                loop {
                    let i = cursor.fetch_add(1, Ordering::Relaxed);
                    if i >= N {
                        break;
                    }
                    work(i);
                }
            });
        }
    });
}

fn bench_creates(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_path/creates_100k");
    group.sample_size(10);
    group.throughput(Throughput::Elements(N as u64));
    for k in [1u32, 2, 4, 8] {
        group.bench_with_input(BenchmarkId::new("clients", k), &k, |b, &k| {
            b.iter_batched(
                create_engine,
                |engine| {
                    teraslab::metrics::reset_writers_max();
                    run_concurrent(k, |i| create_one(&engine, i));
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

fn bench_spends(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_path/spends_100k");
    group.sample_size(10);
    group.throughput(Throughput::Elements(N as u64));
    for k in [1u32, 2, 4, 8] {
        group.bench_with_input(BenchmarkId::new("clients", k), &k, |b, &k| {
            b.iter_batched(
                || {
                    let engine = create_engine();
                    // Pre-create every tx so the spend has a target slot.
                    run_concurrent(8, |i| create_one(&engine, i));
                    engine
                },
                |engine| {
                    teraslab::metrics::reset_writers_max();
                    run_concurrent(k, |i| spend_one(&engine, i));
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

/// Creates at K=8 for shard_count ∈ {1, 16}.
///
/// Isolates the index-lock contention reduction: with shard_count=1 all K=8
/// threads serialise on one RwLock; with shard_count=16 they contend on 16
/// independent shards. The allocator and striped per-key locks are shared in
/// both cases, so the measured delta reflects the primary-index lock specifically.
fn bench_creates_shards(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_path/creates_100k_shards");
    group.sample_size(10);
    group.throughput(Throughput::Elements(N as u64));
    for shards in [1usize, 16] {
        group.bench_with_input(BenchmarkId::new("shards", shards), &shards, |b, &shards| {
            b.iter_batched(
                || create_engine_sharded(shards),
                |engine| {
                    teraslab::metrics::reset_writers_max();
                    run_concurrent(8, |i| create_one(&engine, i));
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

/// Spends at K=8 for shard_count ∈ {1, 16}.
fn bench_spends_shards(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_path/spends_100k_shards");
    group.sample_size(10);
    group.throughput(Throughput::Elements(N as u64));
    for shards in [1usize, 16] {
        group.bench_with_input(BenchmarkId::new("shards", shards), &shards, |b, &shards| {
            b.iter_batched(
                || {
                    let engine = create_engine_sharded(shards);
                    run_concurrent(8, |i| create_one(&engine, i));
                    engine
                },
                |engine| {
                    teraslab::metrics::reset_writers_max();
                    run_concurrent(8, |i| spend_one(&engine, i));
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

/// Mixed read-under-write-storm benchmark.
///
/// Measures **reader throughput** (lookups/sec) while 4 writer threads hammer
/// the index with continuous creates. The whole point of sharding is that a
/// `parking_lot::RwLock` WRITER excludes ALL readers on the same lock — so
/// under a write storm shard_count=1 starves readers, while shard_count=16
/// means a reader only has to wait for writes to its own shard (~1/16 of
/// writes hit any given shard).
///
/// Setup: 50_000 pre-existing keys are created before measurement starts.
/// Readers: R=4 threads, each doing READS_PER_READER lookups on those keys.
/// Writers: W=4 threads creating NEW keys continuously until readers finish.
/// Metric: total reader lookups / elapsed reader wall-clock time.
fn bench_mixed_read_under_write_storm(c: &mut Criterion) {
    const PRE_POPULATE: u32 = 50_000;
    const READS_PER_READER: u32 = 20_000;
    const READERS: u32 = 4;
    const WRITERS: u32 = 4;
    // Writer keys start well above the pre-populated range so they never
    // collide with reader keys (creates on an existing key are no-ops/errors
    // and don't produce meaningful write pressure; we want genuine new inserts).
    const WRITER_KEY_BASE: u32 = 1_000_000;

    let mut group = c.benchmark_group("mixed_read_under_write_storm");
    group.sample_size(10);
    // Throughput unit = total reader lookups per iteration.
    group.throughput(Throughput::Elements((READERS * READS_PER_READER) as u64));

    for shards in [1usize, 16] {
        group.bench_with_input(BenchmarkId::new("shards", shards), &shards, |b, &shards| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;

                for iter in 0..iters {
                    // --- Setup: build engine and pre-populate reader keys ---
                    let engine = create_engine_sharded(shards);
                    for i in 0..PRE_POPULATE {
                        create_one(&engine, i);
                    }

                    // Writer key cursor: per-iteration offset avoids
                    // hitting keys from prior iters (create is idempotent
                    // on existing keys — won't panic — but we want real
                    // write pressure, i.e. new inserts).
                    let writer_cursor = Arc::new(AtomicU32::new(
                        WRITER_KEY_BASE + iter as u32 * WRITERS * 50_000,
                    ));
                    let stop_writers = Arc::new(AtomicBool::new(false));

                    // --- Spawn writers (they run until readers finish) ---
                    let mut writer_handles = Vec::with_capacity(WRITERS as usize);
                    for _ in 0..WRITERS {
                        let eng = engine.clone();
                        let cursor = writer_cursor.clone();
                        let stop = stop_writers.clone();
                        writer_handles.push(std::thread::spawn(move || {
                            while !stop.load(Ordering::Relaxed) {
                                let k = cursor.fetch_add(1, Ordering::Relaxed);
                                create_one(&eng, k);
                            }
                        }));
                    }

                    // --- Measure reader completion time ---
                    let t0 = Instant::now();

                    // Readers: each does READS_PER_READER lookups spread
                    // across the pre-populated key set so they hit all shards.
                    let reader_cursor = Arc::new(AtomicU32::new(0));
                    let total_lookups = READERS * READS_PER_READER;
                    let mut reader_handles = Vec::with_capacity(READERS as usize);
                    for _ in 0..READERS {
                        let eng = engine.clone();
                        let cursor = reader_cursor.clone();
                        reader_handles.push(std::thread::spawn(move || {
                            let mut done = 0u32;
                            while done < READS_PER_READER {
                                let i = cursor.fetch_add(1, Ordering::Relaxed) % PRE_POPULATE;
                                let key = TxKey {
                                    txid: make_tx_id(i),
                                };
                                assert!(
                                    eng.lookup_checked(&key).is_ok(),
                                    "benchmark lookup failed"
                                );
                                done += 1;
                            }
                        }));
                    }

                    for h in reader_handles {
                        h.join().unwrap();
                    }
                    total += t0.elapsed();

                    // Signal writers to stop and join them.
                    stop_writers.store(true, Ordering::Relaxed);
                    for h in writer_handles {
                        h.join().unwrap();
                    }

                    let _ = total_lookups; // used above for Throughput::Elements
                }

                total
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_creates,
    bench_spends,
    bench_creates_shards,
    bench_spends_shards,
    bench_mixed_read_under_write_storm
);
criterion_main!(benches);
