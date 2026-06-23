//! Fixed-workload write-path throughput bench: 100K creates and 100K spends
//! at apply-concurrency K in {1,2,4,8}. Engine-level (no TCP). Provides the
//! comparable before/after numbers recorded in WRITE_PATH_BASELINE.md.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, TxKey, UnminedIndex};
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

criterion_group!(benches, bench_creates, bench_spends);
criterion_main!(benches);
