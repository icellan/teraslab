//! Criterion benchmark for mixed realistic workload throughput.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::sync::Arc;

use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, TxKey, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::create::*;
use teraslab::ops::engine::Engine;
use teraslab::ops::remaining::*;
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

fn create_engine() -> Arc<Engine> {
    let dev: Arc<dyn BlockDevice> =
        Arc::new(MemoryDevice::new(512 * 1024 * 1024, 4096).unwrap());
    let alloc = SlotAllocator::new(dev.clone());
    let index = Index::new(200_000).unwrap();
    Arc::new(Engine::new(
        dev,
        index,
        alloc,
        StripedLocks::new(1024),
        DahIndex::new(),
        UnminedIndex::new(),
    ))
}

/// Simulates a realistic mixed workload: ~15% create, ~50% spend,
/// ~20% setMined, ~10% read, ~5% other.
fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");
    // Each iteration does 100 operations (10 create + 50 spend + 20 setMined + 10 read + 10 other)
    group.throughput(Throughput::Elements(100));

    group.bench_function("realistic_ratio", |b| {
        let engine = create_engine();
        let mut next_tx = 0u32;
        let mut created_txs: Vec<(TxKey, u32)> = Vec::new(); // (key, tx_n)
        let mut spend_idx = 0usize;
        let mut mine_idx = 0usize;

        // Pre-seed
        for i in 0..200u32 {
            let tx_id = make_tx_id(i);
            let utxo_hashes: Vec<[u8; 32]> = (0..10u32).map(|v| make_utxo_hash(i, v)).collect();
            let req = CreateRequest {
                tx_id,
                tx_version: 1,
                locktime: 0,
                fee: 500,
                size_in_bytes: 250,
                extended_size: 0,
                is_coinbase: false,
                spending_height: 0,
                utxo_hashes,
                inputs: None,
                outputs: None,
                inpoints: None,
                is_external: false,
                created_at: 1710000000000,
                block_height: 1000,
                mined_block_infos: vec![],
                frozen: false,
                conflicting: false,
                locked: false,
                parent_txids: vec![],
            };
            engine.create(&req).unwrap();
            created_txs.push((TxKey { txid: tx_id }, i));
            next_tx = i + 1;
        }

        b.iter(|| {
            // 10 creates
            for _ in 0..10 {
                let tx_id = make_tx_id(next_tx);
                let utxo_hashes: Vec<[u8; 32]> =
                    (0..10u32).map(|v| make_utxo_hash(next_tx, v)).collect();
                let req = CreateRequest {
                    tx_id,
                    tx_version: 1,
                    locktime: 0,
                    fee: 500,
                    size_in_bytes: 250,
                    extended_size: 0,
                    is_coinbase: false,
                    spending_height: 0,
                    utxo_hashes,
                    inputs: None,
                    outputs: None,
                    inpoints: None,
                    is_external: false,
                    created_at: 1710000000000,
                    block_height: 1000,
                    mined_block_infos: vec![],
                    frozen: false,
                    conflicting: false,
                    locked: false,
                    parent_txids: vec![],
                };
                let _ = engine.create(&req);
                created_txs.push((TxKey { txid: tx_id }, next_tx));
                next_tx += 1;
            }

            // 50 spends
            for _ in 0..50 {
                if spend_idx < created_txs.len() {
                    let (key, tx_n) = created_txs[spend_idx];
                    let v = 0u32;
                    let mut sd = [0u8; 36];
                    sd[0..4].copy_from_slice(&(tx_n + 10000).to_le_bytes());
                    let _ = engine.spend(&SpendRequest {
                        tx_key: key,
                        offset: v,
                        utxo_hash: make_utxo_hash(tx_n, v),
                        spending_data: sd,
                        ignore_conflicting: false,
                        ignore_locked: false,
                        current_block_height: 2000,
                        block_height_retention: 288,
                    });
                    spend_idx += 1;
                }
            }

            // 20 setMined
            for _ in 0..20 {
                if mine_idx < created_txs.len() {
                    let (key, _) = created_txs[mine_idx];
                    let _ = engine.set_mined(&SetMinedRequest {
                        tx_key: key,
                        block_id: mine_idx as u32 + 100,
                        block_height: 2000,
                        subtree_idx: 0,
                        current_block_height: 2000,
                        block_height_retention: 288,
                        on_longest_chain: true,
                        unset_mined: false,
                    });
                    mine_idx += 1;
                }
            }

            // 10 reads
            for i in 0..10 {
                if i < created_txs.len() {
                    let (key, _) = created_txs[i];
                    let _ = engine.read_metadata(&key);
                }
            }

            // 10 other (setConflicting toggle)
            for i in 0..10 {
                if i < created_txs.len() {
                    let (key, _) = created_txs[i];
                    let _ = engine.set_conflicting(&SetConflictingRequest {
                        tx_key: key,
                        value: true,
                        current_block_height: 2000,
                        block_height_retention: 288,
                    });
                    let _ = engine.set_conflicting(&SetConflictingRequest {
                        tx_key: key,
                        value: false,
                        current_block_height: 2000,
                        block_height_retention: 288,
                    });
                }
            }
        })
    });

    group.finish();
}

criterion_group!(benches, bench_mixed_workload);
criterion_main!(benches);
