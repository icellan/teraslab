//! Criterion benchmarks for engine operations not covered by spend_throughput.
//!
//! Covers: unspend, delete, freeze, unfreeze, reassign, set_locked,
//! preserve_until, mark_on_longest_chain, and get_spend.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::sync::Arc;

use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, TxKey, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::create::*;
use teraslab::ops::engine::Engine;
use teraslab::ops::mark_longest_chain::*;
use teraslab::ops::remaining::*;
use teraslab::ops::spend::*;
use teraslab::ops::unspend::*;

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

fn create_engine() -> Arc<Engine> {
    let dev: Arc<dyn BlockDevice> =
        Arc::new(MemoryDevice::new(512 * 1024 * 1024, 4096).unwrap());
    let alloc = SlotAllocator::new(dev.clone()).unwrap();
    let index = Index::new(200_000).unwrap();
    Arc::new(Engine::new(
        dev,
        index,
        alloc,
        StripedLocks::new(65536),
        DahIndex::new(),
        UnminedIndex::new(),
    ))
}

fn create_tx(engine: &Engine, tx_idx: u32, utxo_count: u32) {
    let tx_id = make_tx_id(tx_idx);
    let utxo_hashes: Vec<[u8; 32]> =
        (0..utxo_count).map(|v| make_utxo_hash(tx_idx, v)).collect();
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
        parent_txids: &[],
    };
    engine.create(&req).unwrap();
}

/// Pre-populate engine with `count` transactions, each having `utxos_per_tx` UTXOs.
fn setup_engine(count: u32, utxos_per_tx: u32) -> Arc<Engine> {
    let engine = create_engine();
    for i in 0..count {
        create_tx(&engine, i, utxos_per_tx);
    }
    engine
}

/// Pre-populate engine and spend vout=0 on every transaction.
fn setup_engine_with_spent(count: u32, utxos_per_tx: u32) -> Arc<Engine> {
    let engine = setup_engine(count, utxos_per_tx);
    for i in 0..count {
        let key = TxKey { txid: make_tx_id(i) };
        let mut sd = [0u8; 36];
        sd[0..4].copy_from_slice(&(i + 10000).to_le_bytes());
        let _ = engine.spend(&SpendRequest {
            tx_key: key,
            offset: 0,
            utxo_hash: make_utxo_hash(i, 0),
            spending_data: sd,
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 2000,
            block_height_retention: 288,
        });
    }
    engine
}

// ---------------------------------------------------------------------------
// Unspend
// ---------------------------------------------------------------------------

fn bench_unspend(c: &mut Criterion) {
    let mut group = c.benchmark_group("unspend");
    group.throughput(Throughput::Elements(1));

    // Setup: create txs, spend vout=0, then benchmark unspending them.
    // After each full pass, re-spend so we can unspend again.
    let engine = setup_engine_with_spent(20_000, 5);
    let mut tx_idx = 0u32;

    group.bench_function("unspend_one", |b| {
        b.iter(|| {
            let key = TxKey { txid: make_tx_id(tx_idx) };
            let _ = engine.unspend(&UnspendRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: make_utxo_hash(tx_idx, 0),
                current_block_height: 2000,
                block_height_retention: 288,
            });

            tx_idx += 1;
            if tx_idx >= 20_000 {
                // Re-spend all so the next pass has something to unspend.
                for i in 0..20_000u32 {
                    let k = TxKey { txid: make_tx_id(i) };
                    let mut sd = [0u8; 36];
                    sd[0..4].copy_from_slice(&(i + 10000).to_le_bytes());
                    let _ = engine.spend(&SpendRequest {
                        tx_key: k,
                        offset: 0,
                        utxo_hash: make_utxo_hash(i, 0),
                        spending_data: sd,
                        ignore_conflicting: false,
                        ignore_locked: false,
                        current_block_height: 2000,
                        block_height_retention: 288,
                    });
                }
                tx_idx = 0;
            }
        })
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Freeze / Unfreeze
// ---------------------------------------------------------------------------

fn bench_freeze(c: &mut Criterion) {
    let mut group = c.benchmark_group("freeze");
    group.throughput(Throughput::Elements(1));

    let engine = setup_engine(50_000, 5);
    let mut tx_idx = 0u32;

    group.bench_function("freeze_one", |b| {
        b.iter(|| {
            let key = TxKey { txid: make_tx_id(tx_idx) };
            let _ = engine.freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: make_utxo_hash(tx_idx, 0),
            });
            tx_idx += 1;
            if tx_idx >= 50_000 {
                tx_idx = 0;
            }
        })
    });

    group.finish();
}

fn bench_unfreeze(c: &mut Criterion) {
    let mut group = c.benchmark_group("unfreeze");
    group.throughput(Throughput::Elements(1));

    // Pre-freeze all vout=0 UTXOs, then benchmark unfreezing.
    let engine = setup_engine(20_000, 5);
    for i in 0..20_000u32 {
        let key = TxKey { txid: make_tx_id(i) };
        let _ = engine.freeze(&FreezeRequest {
            tx_key: key,
            offset: 0,
            utxo_hash: make_utxo_hash(i, 0),
        });
    }

    let mut tx_idx = 0u32;

    group.bench_function("unfreeze_one", |b| {
        b.iter(|| {
            let key = TxKey { txid: make_tx_id(tx_idx) };
            let _ = engine.unfreeze(&UnfreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: make_utxo_hash(tx_idx, 0),
            });
            tx_idx += 1;
            if tx_idx >= 20_000 {
                // Re-freeze for next pass.
                for i in 0..20_000u32 {
                    let k = TxKey { txid: make_tx_id(i) };
                    let _ = engine.freeze(&FreezeRequest {
                        tx_key: k,
                        offset: 0,
                        utxo_hash: make_utxo_hash(i, 0),
                    });
                }
                tx_idx = 0;
            }
        })
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Reassign
// ---------------------------------------------------------------------------

fn bench_reassign(c: &mut Criterion) {
    let mut group = c.benchmark_group("reassign");
    group.throughput(Throughput::Elements(1));

    // Reassign requires the UTXO to be frozen first.
    let engine = setup_engine(20_000, 5);
    for i in 0..20_000u32 {
        let key = TxKey { txid: make_tx_id(i) };
        let _ = engine.freeze(&FreezeRequest {
            tx_key: key,
            offset: 0,
            utxo_hash: make_utxo_hash(i, 0),
        });
    }

    let mut tx_idx = 0u32;
    let mut pass = 0u32;

    group.bench_function("reassign_one", |b| {
        b.iter(|| {
            let key = TxKey { txid: make_tx_id(tx_idx) };
            // Alternate between two hashes to keep the benchmark going.
            let old_hash = if pass % 2 == 0 {
                make_utxo_hash(tx_idx, 0)
            } else {
                make_utxo_hash(tx_idx + 1_000_000, 0)
            };
            let new_hash = if pass % 2 == 0 {
                make_utxo_hash(tx_idx + 1_000_000, 0)
            } else {
                make_utxo_hash(tx_idx, 0)
            };

            let _ = engine.reassign(&ReassignRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: old_hash,
                new_utxo_hash: new_hash,
                block_height: 2000,
                spendable_after: 10,
            });

            pass += 1;
            tx_idx += 1;
            if tx_idx >= 20_000 {
                tx_idx = 0;
            }
        })
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// SetLocked
// ---------------------------------------------------------------------------

fn bench_set_locked(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_locked");
    group.throughput(Throughput::Elements(1));

    let engine = setup_engine(50_000, 5);
    let mut tx_idx = 0u32;
    let mut toggle = true;

    group.bench_function("toggle", |b| {
        b.iter(|| {
            let key = TxKey { txid: make_tx_id(tx_idx) };
            let _ = engine.set_locked(&SetLockedRequest {
                tx_key: key,
                value: toggle,
            });
            toggle = !toggle;
            tx_idx += 1;
            if tx_idx >= 50_000 {
                tx_idx = 0;
            }
        })
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// PreserveUntil
// ---------------------------------------------------------------------------

fn bench_preserve_until(c: &mut Criterion) {
    let mut group = c.benchmark_group("preserve_until");
    group.throughput(Throughput::Elements(1));

    let engine = setup_engine(50_000, 5);
    let mut tx_idx = 0u32;

    group.bench_function("set_preserve", |b| {
        b.iter(|| {
            let key = TxKey { txid: make_tx_id(tx_idx) };
            let _ = engine.preserve_until(&PreserveUntilRequest {
                tx_key: key,
                block_height: 5000,
            });
            tx_idx += 1;
            if tx_idx >= 50_000 {
                tx_idx = 0;
            }
        })
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// MarkOnLongestChain
// ---------------------------------------------------------------------------

fn bench_mark_on_longest_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("mark_longest_chain");
    group.throughput(Throughput::Elements(1));

    let engine = setup_engine(50_000, 5);
    let mut tx_idx = 0u32;
    let mut toggle = true;

    group.bench_function("toggle", |b| {
        b.iter(|| {
            let key = TxKey { txid: make_tx_id(tx_idx) };
            let _ = engine.mark_on_longest_chain(&MarkOnLongestChainRequest {
                tx_key: key,
                on_longest_chain: toggle,
                current_block_height: 2000,
                block_height_retention: 288,
            });
            toggle = !toggle;
            tx_idx += 1;
            if tx_idx >= 50_000 {
                tx_idx = 0;
            }
        })
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete");
    group.throughput(Throughput::Elements(1));

    // Delete is destructive, so we create fresh txs in batches.
    let engine = create_engine();
    let batch = 10_000u32;

    // Pre-seed a batch.
    for i in 0..batch {
        create_tx(&engine, i, 5);
    }
    let mut next_create = batch;
    let mut del_idx = 0u32;

    group.bench_function("delete_one", |b| {
        b.iter(|| {
            let key = TxKey { txid: make_tx_id(del_idx) };
            let _ = engine.delete(&DeleteRequest { tx_key: key });
            del_idx += 1;
            if del_idx >= next_create {
                // Create a new batch.
                for i in next_create..next_create + batch {
                    create_tx(&engine, i, 5);
                }
                next_create += batch;
            }
        })
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// GetSpend
// ---------------------------------------------------------------------------

fn bench_get_spend(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_spend");
    group.throughput(Throughput::Elements(1));

    // Read spending data — mix of unspent and spent UTXOs.
    let engine = setup_engine(50_000, 5);
    // Spend vout=0 on first 25k txs so we have both states.
    for i in 0..25_000u32 {
        let key = TxKey { txid: make_tx_id(i) };
        let mut sd = [0u8; 36];
        sd[0..4].copy_from_slice(&(i + 10000).to_le_bytes());
        let _ = engine.spend(&SpendRequest {
            tx_key: key,
            offset: 0,
            utxo_hash: make_utxo_hash(i, 0),
            spending_data: sd,
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 2000,
            block_height_retention: 288,
        });
    }

    let mut tx_idx = 0u32;

    group.bench_function("get_spend_one", |b| {
        b.iter(|| {
            let key = TxKey { txid: make_tx_id(tx_idx) };
            let _ = engine.get_spend(&GetSpendRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: make_utxo_hash(tx_idx, 0),
            });
            tx_idx += 1;
            if tx_idx >= 50_000 {
                tx_idx = 0;
            }
        })
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// SetConflicting (dedicated — the mixed_workload bench only does it indirectly)
// ---------------------------------------------------------------------------

fn bench_set_conflicting(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_conflicting");
    group.throughput(Throughput::Elements(1));

    let engine = setup_engine(50_000, 5);
    let mut tx_idx = 0u32;
    let mut toggle = true;

    group.bench_function("toggle", |b| {
        b.iter(|| {
            let key = TxKey { txid: make_tx_id(tx_idx) };
            let _ = engine.set_conflicting(&SetConflictingRequest {
                tx_key: key,
                value: toggle,
                current_block_height: 2000,
                block_height_retention: 288,
            });
            toggle = !toggle;
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
    bench_unspend,
    bench_freeze,
    bench_unfreeze,
    bench_reassign,
    bench_set_locked,
    bench_preserve_until,
    bench_mark_on_longest_chain,
    bench_delete,
    bench_get_spend,
    bench_set_conflicting,
);
criterion_main!(benches);
