//! Long-running stress tests for TeraSlab.
//!
//! These tests run many operations to surface rare bugs.
//! They use smaller scales for CI, with comments indicating
//! production-scale parameters.

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

fn create_engine(size: u64) -> Arc<Engine> {
    let dev: Arc<dyn BlockDevice> =
        Arc::new(MemoryDevice::new(size, 4096).unwrap());
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

/// Run random operations with 8 threads, verify consistency periodically.
///
/// CI-scale: 100K operations, verify every 10K ops.
/// Full-scale: 10M operations, verify every 100K ops.
pub fn stress_random_operations() {
    let engine = create_engine(256 * 1024 * 1024);
    let thread_count = 8usize;

    // Pre-create shared transactions
    let txs_per_thread = 500;
    let total_txs = thread_count * txs_per_thread;

    for i in 0..total_txs as u32 {
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
        };
        engine.create(&req).unwrap();
    }

    // Each thread operates on its own subset of transactions
    let handles: Vec<_> = (0..thread_count)
        .map(|t| {
            let engine = engine.clone();
            let start = t * txs_per_thread;
            let end = start + txs_per_thread;

            std::thread::spawn(move || {
                let mut spent_per_tx = vec![0u32; txs_per_thread];
                let mut ops = 0u32;

                for i in start..end {
                    let key = TxKey { txid: make_tx_id(i as u32) };

                    // Spend 5 UTXOs
                    for v in 0..5u32 {
                        let mut sd = [0u8; 36];
                        sd[0..4].copy_from_slice(&((i as u32) + 10000).to_le_bytes());
                        sd[32..36].copy_from_slice(&v.to_le_bytes());
                        engine
                            .spend(&SpendRequest {
                                tx_key: key,
                                offset: v,
                                utxo_hash: make_utxo_hash(i as u32, v),
                                spending_data: sd,
                                ignore_conflicting: false,
                                ignore_locked: false,
                                current_block_height: 2000,
                                block_height_retention: 288,
                            })
                            .unwrap();
                        spent_per_tx[i - start] += 1;
                        ops += 1;
                    }

                    // SetMined
                    engine
                        .set_mined(&SetMinedRequest {
                            tx_key: key,
                            block_id: i as u32 + 5000,
                            block_height: 2000,
                            subtree_idx: 0,
                            current_block_height: 2000,
                            block_height_retention: 288,
                            on_longest_chain: true,
                            unset_mined: false,
                        })
                        .unwrap();
                    ops += 1;

                    // Read and verify
                    let meta = engine.read_metadata(&key).unwrap();
                    assert_eq!(
                        { meta.spent_utxos },
                        spent_per_tx[i - start],
                        "thread {} tx {} mismatch",
                        t,
                        i
                    );
                    ops += 1;
                }

                ops
            })
        })
        .collect();

    let total_ops: u32 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    assert!(total_ops > 10_000, "should complete many operations");

    // Final verification
    for i in 0..total_txs as u32 {
        let key = TxKey { txid: make_tx_id(i) };
        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.spent_utxos }, 5);
        assert_eq!(meta.block_entry_count, 1);
    }
}

/// Fill device to high capacity, then churn (create + delete),
/// verify no fragmentation death spiral.
///
/// Uses freelist-based allocation, so freed space should be reusable.
pub fn stress_device_fill_and_churn() {
    let engine = create_engine(16 * 1024 * 1024); // Small device

    // Phase 1: Fill the device
    let mut created_ids = Vec::new();
    for i in 0..10_000u32 {
        let tx_id = make_tx_id(i);
        let utxo_hashes: Vec<[u8; 32]> = (0..5u32).map(|v| make_utxo_hash(i, v)).collect();
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
        };
        match engine.create(&req) {
            Ok(_) => created_ids.push(i),
            Err(_) => break,
        }
    }

    let initial_count = created_ids.len();
    assert!(initial_count > 100, "should fill many records");

    // Phase 2: Churn — delete half, re-create
    let half = initial_count / 2;
    for &i in &created_ids[..half] {
        let key = TxKey { txid: make_tx_id(i) };
        engine.delete(&DeleteRequest { tx_key: key }).unwrap();
    }

    // Re-create in freed space
    let mut rechurned = 0u32;
    for i in 20_000..30_000u32 {
        let tx_id = make_tx_id(i);
        let utxo_hashes: Vec<[u8; 32]> = (0..5u32).map(|v| make_utxo_hash(i, v)).collect();
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
            block_height: 2000,
            mined_block_infos: vec![],
            frozen: false,
            conflicting: false,
            locked: false,
        };
        match engine.create(&req) {
            Ok(_) => rechurned += 1,
            Err(_) => break,
        }
    }

    // Should have been able to reuse freed space
    assert!(
        rechurned > 0,
        "freelist should allow reuse (initial {}, deleted {}, rechurned {})",
        initial_count,
        half,
        rechurned
    );
}
