//! Criterion benchmarks for spend throughput under various conditions.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::sync::Arc;

use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, TxKey, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::create::*;
use teraslab::ops::engine::Engine;
use teraslab::ops::set_mined::*;
use teraslab::ops::spend::*;

fn make_tx_id(n: u32) -> [u8; 32] {
    let mut txid = [0u8; 32];
    txid[0..4].copy_from_slice(&n.to_le_bytes());
    txid[8..12].copy_from_slice(&(n.wrapping_mul(0x9E37)).to_le_bytes());
    txid[16..18].copy_from_slice(&(n as u16).to_le_bytes());
    txid
}

fn make_utxo_hash(tx_n: u32, vout: u32) -> [u8; 32] {
    let mut h = [0u8; 32];
    h[0] = (vout & 0xFF) as u8;
    h[1] = ((vout >> 8) & 0xFF) as u8;
    h[4..8].copy_from_slice(&tx_n.to_le_bytes());
    h
}

fn create_engine_sized(device_mb: u64, index_capacity: usize) -> Arc<Engine> {
    let dev: Arc<dyn BlockDevice> =
        Arc::new(MemoryDevice::new(device_mb * 1024 * 1024, 4096).unwrap());
    let alloc = SlotAllocator::new(dev.clone()).unwrap();
    let index = Index::new(index_capacity).unwrap();
    Arc::new(Engine::new(
        dev,
        index,
        alloc,
        StripedLocks::new(65536),
        DahIndex::new(),
        UnminedIndex::new(),
    ))
}

fn create_engine() -> Arc<Engine> {
    create_engine_sized(512, 200_000)
}

fn setup_engine_with_txs(count: u32, utxos_per_tx: u32) -> Arc<Engine> {
    let engine = create_engine();
    for i in 0..count {
        let tx_id = make_tx_id(i);
        let utxo_hashes: Vec<[u8; 32]> = (0..utxos_per_tx).map(|v| make_utxo_hash(i, v)).collect();
        let req = CreateRequest {
            tx_id,
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
            created_at: 1710000000000,
            block_height: 1000,
            mined_block_infos: &[],
            frozen: false,
            conflicting: false,
            locked: false,
            external_ref: None,
            parent_txids: &[],
        };
        engine.create(&req).unwrap();
    }
    engine
}

fn bench_single_spend(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_spend");
    group.throughput(Throughput::Elements(1));

    let engine = setup_engine_with_txs(50_000, 10);

    let mut tx_idx = 0u32;
    let mut slot_idx = 0u32;

    group.bench_function("spend_one", |b| {
        b.iter(|| {
            let key = TxKey {
                txid: make_tx_id(tx_idx),
            };
            let mut sd = [0u8; 36];
            sd[0..4].copy_from_slice(&(tx_idx + 10000).to_le_bytes());
            sd[32..36].copy_from_slice(&slot_idx.to_le_bytes());

            let _ = engine.spend(&SpendRequest {
                tx_key: key,
                offset: slot_idx,
                utxo_hash: make_utxo_hash(tx_idx, slot_idx),
                spending_data: sd,
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 2000,
                block_height_retention: 288,
            });

            slot_idx += 1;
            if slot_idx >= 10 {
                slot_idx = 0;
                tx_idx += 1;
                if tx_idx >= 50_000 {
                    tx_idx = 0;
                }
            }
        })
    });

    group.finish();
}

fn bench_spend_multi(c: &mut Criterion) {
    let mut group = c.benchmark_group("spend_multi");

    for batch_size in [1, 5, 10] {
        let engine = setup_engine_with_txs(20_000, batch_size as u32);

        let mut tx_idx = 0u32;

        group.throughput(Throughput::Elements(batch_size));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &size| {
                b.iter(|| {
                    let key = TxKey {
                        txid: make_tx_id(tx_idx),
                    };
                    let spends: Vec<SpendItem> = (0..size as u32)
                        .map(|v| {
                            let mut sd = [0u8; 36];
                            sd[0..4].copy_from_slice(&(tx_idx + 10000).to_le_bytes());
                            sd[32..36].copy_from_slice(&v.to_le_bytes());
                            SpendItem {
                                offset: v,
                                utxo_hash: make_utxo_hash(tx_idx, v),
                                spending_data: sd,
                                idx: v,
                            }
                        })
                        .collect();

                    let _ = engine.spend_multi(&SpendMultiRequest {
                        tx_key: key,
                        spends,
                        ignore_conflicting: false,
                        ignore_locked: false,
                        current_block_height: 2000,
                        block_height_retention: 288,
                    });

                    tx_idx += 1;
                    if tx_idx >= 20_000 {
                        tx_idx = 0;
                    }
                })
            },
        );
    }

    group.finish();
}

fn bench_set_mined(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_mined");
    group.throughput(Throughput::Elements(1));

    // Use the standard engine. setMined wrapping is okay — it exercises the
    // realistic path where txs accumulate 1-3 inline block entries. Overflow
    // (>3 entries) adds device I/O cost but is rare in production.
    let engine = setup_engine_with_txs(50_000, 5);
    let mut tx_idx = 0u32;
    let mut block_id = 1u32;

    group.bench_function("set_mined_one", |b| {
        b.iter(|| {
            let key = TxKey {
                txid: make_tx_id(tx_idx),
            };
            let _ = engine.set_mined(&SetMinedRequest {
                tx_key: key,
                block_id,
                block_height: 2000,
                subtree_idx: 0,
                current_block_height: 2000,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: false,
            });

            block_id += 1;
            tx_idx += 1;
            if tx_idx >= 50_000 {
                tx_idx = 0;
            }
        })
    });

    // Separate benchmark: setMined on fresh txs only (no wrapping, no overflow).
    // Each iteration creates a fresh tx, then mines it once.
    group.bench_function("set_mined_first_pass", |b| {
        let fresh_engine = create_engine();
        let mut fresh_idx = 0u32;
        b.iter(|| {
            // Create a tx then immediately setMined it — pure first-pass.
            let tx_id = make_tx_id(fresh_idx + 1_000_000);
            let utxo_hashes = [make_utxo_hash(fresh_idx, 0)];
            let req = CreateRequest {
                tx_id,
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
                created_at: 1710000000000,
                block_height: 1000,
                mined_block_infos: &[],
                frozen: false,
                conflicting: false,
                locked: false,
                external_ref: None,
                parent_txids: &[],
            };
            let _ = fresh_engine.create(&req);

            let key = TxKey { txid: tx_id };
            let _ = fresh_engine.set_mined(&SetMinedRequest {
                tx_key: key,
                block_id: fresh_idx + 1,
                block_height: 2000,
                subtree_idx: 0,
                current_block_height: 2000,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: false,
            });
            fresh_idx += 1;
        })
    });

    group.finish();
}

fn bench_spend_threaded(c: &mut Criterion) {
    use std::sync::atomic::{AtomicU32, Ordering};

    let mut group = c.benchmark_group("spend_threaded");

    for num_threads in [2, 4, 8] {
        let txs_per_thread = 10_000u32;
        let total = txs_per_thread * num_threads as u32;
        let engine = setup_engine_with_txs(total, 10);

        group.throughput(Throughput::Elements(num_threads as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_threads),
            &num_threads,
            |b, &threads| {
                let counters: Vec<AtomicU32> = (0..threads).map(|_| AtomicU32::new(0)).collect();
                b.iter(|| {
                    std::thread::scope(|s| {
                        for (t, ctr) in counters.iter().enumerate().take(threads) {
                            let eng = &engine;
                            let offset = t as u32 * txs_per_thread;
                            s.spawn(move || {
                                let i = ctr.fetch_add(1, Ordering::Relaxed);
                                let tx_idx = offset + (i % txs_per_thread);
                                let key = TxKey {
                                    txid: make_tx_id(tx_idx),
                                };
                                let slot = i % 10;
                                let mut sd = [0u8; 36];
                                sd[0..4].copy_from_slice(&(tx_idx + 10000).to_le_bytes());
                                sd[32..36].copy_from_slice(&slot.to_le_bytes());
                                let _ = eng.spend(&SpendRequest {
                                    tx_key: key,
                                    offset: slot,
                                    utxo_hash: make_utxo_hash(tx_idx, slot),
                                    spending_data: sd,
                                    ignore_conflicting: false,
                                    ignore_locked: false,
                                    current_block_height: 2000,
                                    block_height_retention: 288,
                                });
                            });
                        }
                    });
                })
            },
        );
    }

    group.finish();
}

fn bench_create(c: &mut Criterion) {
    let mut group = c.benchmark_group("create");

    for utxo_count in [1u32, 10, 100] {
        let engine = create_engine();
        let mut tx_idx = 0u32;

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("utxos", utxo_count),
            &utxo_count,
            |b, &count| {
                b.iter(|| {
                    let tx_id = make_tx_id(tx_idx);
                    let utxo_hashes: Vec<[u8; 32]> =
                        (0..count).map(|v| make_utxo_hash(tx_idx, v)).collect();
                    let req = CreateRequest {
                        tx_id,
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
                        created_at: 1710000000000,
                        block_height: 1000,
                        mined_block_infos: &[],
                        frozen: false,
                        conflicting: false,
                        locked: false,
                        external_ref: None,
                        parent_txids: &[],
                    };
                    let _ = engine.create(&req);
                    tx_idx += 1;
                })
            },
        );
    }

    group.finish();
}

fn bench_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("read");
    group.throughput(Throughput::Elements(1));

    let engine = setup_engine_with_txs(50_000, 5);
    let mut tx_idx = 0u32;

    group.bench_function("read_metadata", |b| {
        b.iter(|| {
            let key = TxKey {
                txid: make_tx_id(tx_idx),
            };
            let _ = engine.read_metadata(&key);
            tx_idx += 1;
            if tx_idx >= 50_000 {
                tx_idx = 0;
            }
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_single_spend,
    bench_spend_multi,
    bench_set_mined,
    bench_create,
    bench_read,
    bench_spend_threaded,
);
criterion_main!(benches);
