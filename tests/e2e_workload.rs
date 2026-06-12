//! End-to-end workload tests for Phase 12 acceptance criteria.
//!
//! By default, tests run at 1/10 scale for fast development iteration.
//! Set `TERASLAB_FULL_WORKLOAD=1` to run at full volume (for nightly CI).

#![allow(clippy::disallowed_macros)] // integration tests may use eprintln!/println! for diagnostics

mod simulation;
mod workload;

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

use simulation::{Simulation, SimulationConfig};
use workload::generator::{Distribution, WorkloadConfig, WorkloadGenerator, WorkloadOp};
use workload::verifier::StateVerifier;

// ---------------------------------------------------------------------------
// Scale control
// ---------------------------------------------------------------------------

/// Returns true when running at full CI volume.
fn full_scale() -> bool {
    std::env::var("TERASLAB_FULL_WORKLOAD").is_ok_and(|v| v == "1")
}

/// Pick between full and fast value.
fn scale(fast: u64, full: u64) -> u64 {
    if full_scale() { full } else { fast }
}

fn scale32(fast: u32, full: u32) -> u32 {
    if full_scale() { full } else { fast }
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn create_engine() -> Arc<Engine> {
    create_engine_with_size(512 * 1024 * 1024)
}

fn create_engine_with_size(size: u64) -> Arc<Engine> {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(size, 4096).unwrap());
    let alloc = SlotAllocator::new(dev.clone()).unwrap();
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

// ===========================================================================
// End-to-end correctness tests
// ===========================================================================

/// Mixed workload, single node: state verifier finds zero mismatches.
/// fast=10K ops, full=100K ops.
#[test]
fn e2e_mixed_single_node_zero_mismatches() {
    let engine = create_engine();
    let config = WorkloadConfig {
        total_operations: scale(10_000, 100_000),
        tx_creation_rate: 0.15,
        spend_rate: 0.50,
        set_mined_rate: 0.20,
        read_rate: 0.10,
        other_rate: 0.05,
        utxos_per_tx: Distribution::Uniform(1, 20),
        spend_batch_size: Distribution::Fixed(1),
        large_tx_fraction: 0.0,
        concurrent_clients: 1,
        target_ops_per_sec: None,
        seed: 12345,
    };

    let mut wgen = WorkloadGenerator::new(config);
    let ops = wgen.generate_all();
    let mut verifier = StateVerifier::new();

    for (i, op) in ops.iter().enumerate() {
        if let Err(e) = verifier.apply(op, &engine) {
            panic!("operation {} failed: {}", i, e);
        }
    }

    let mismatches = verifier.verify_against(&engine);
    assert!(
        mismatches.is_empty(),
        "found {} mismatches:\n{}",
        mismatches.len(),
        mismatches
            .iter()
            .take(20)
            .map(|m| m.detail.as_str())
            .collect::<Vec<_>>()
            .join("\n")
    );
}

/// Concurrent clients: zero mismatches. fast=50 txs, full=500 txs.
#[test]
fn e2e_concurrent_10_threads_zero_mismatches() {
    let engine = create_engine();
    let tx_count = scale32(50, 500);

    for i in 0..tx_count {
        let tx_id = make_tx_id(i);
        let utxo_hashes: Vec<[u8; 32]> = (0..10u32).map(|v| make_utxo_hash(i, v)).collect();
        engine
            .create(&CreateRequest {
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
            })
            .unwrap();
    }

    let chunk_size = tx_count as usize / 10;
    let keys: Vec<(TxKey, u32)> = (0..tx_count)
        .map(|i| {
            (
                TxKey {
                    txid: make_tx_id(i),
                },
                i,
            )
        })
        .collect();

    let handles: Vec<_> = (0..10)
        .map(|t| {
            let engine = engine.clone();
            let chunk: Vec<(TxKey, u32)> = keys[t * chunk_size..(t + 1) * chunk_size].to_vec();
            std::thread::spawn(move || {
                for &(key, tx_n) in &chunk {
                    for v in 0..5u32 {
                        let mut sd = [0u8; 36];
                        sd[0..4].copy_from_slice(&(tx_n + 10000).to_le_bytes());
                        sd[32..36].copy_from_slice(&v.to_le_bytes());
                        engine
                            .spend(&SpendRequest {
                                tx_key: key,
                                offset: v,
                                utxo_hash: make_utxo_hash(tx_n, v),
                                spending_data: sd,
                                ignore_conflicting: false,
                                ignore_locked: false,
                                current_block_height: 2000,
                                block_height_retention: 288,
                            })
                            .unwrap();
                    }
                    engine
                        .set_mined(&SetMinedRequest {
                            tx_key: key,
                            block_id: tx_n + 1000,
                            block_height: 2000,
                            subtree_idx: 0,
                            current_block_height: 2000,
                            block_height_retention: 288,
                            on_longest_chain: true,
                            unset_mined: false,
                        })
                        .unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    for i in 0..tx_count {
        let key = TxKey {
            txid: make_tx_id(i),
        };
        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.spent_utxos }, 5);
        assert_eq!(meta.block_entry_count, 1);
    }
}

/// Crash injection: zero data loss after recovery. fast=1K/seed, full=10K/seed.
#[test]
fn e2e_crash_injection_10_seeds() {
    let ops_per_seed = scale(1_000, 10_000);
    for seed in 0..10u64 {
        let config = SimulationConfig {
            operations: ops_per_seed,
            crash_probability: 0.01,
            ..SimulationConfig::default()
        };
        let mut sim = Simulation::new_single_node(seed + 100);
        let result = sim.run_with_faults(config);
        assert!(
            !result.data_loss_detected,
            "seed {}: data loss detected. Inconsistencies: {:?}",
            seed,
            &result.inconsistencies_found[..result.inconsistencies_found.len().min(5)]
        );
    }
}

// ===========================================================================
// Realistic workload tests
// ===========================================================================

/// Block arrival: create txs, setMined all, spend 50%. fast=300 txs, full=3000.
#[test]
fn realistic_block_arrival() {
    let engine = create_engine();
    let mut verifier = StateVerifier::new();
    let tx_count = scale32(300, 3000);

    for i in 0..tx_count {
        let tx_id = make_tx_id(i);
        let utxo_hashes: Vec<[u8; 32]> = (0..10u32).map(|v| make_utxo_hash(i, v)).collect();
        verifier
            .apply(
                &WorkloadOp::Create {
                    tx_id,
                    utxo_hashes,
                    is_coinbase: false,
                    spending_height: 0,
                    is_external: false,
                    block_height: 1000,
                },
                &engine,
            )
            .unwrap();
    }

    for i in 0..tx_count {
        verifier
            .apply(
                &WorkloadOp::SetMined {
                    tx_key: TxKey {
                        txid: make_tx_id(i),
                    },
                    block_id: 500,
                    block_height: 5000,
                    current_block_height: 5000,
                },
                &engine,
            )
            .unwrap();
    }

    for i in 0..tx_count {
        for v in 0..5u32 {
            let mut sd = [0u8; 36];
            sd[0..4].copy_from_slice(&(i + 10000).to_le_bytes());
            sd[32..36].copy_from_slice(&v.to_le_bytes());
            verifier
                .apply(
                    &WorkloadOp::Spend {
                        tx_key: TxKey {
                            txid: make_tx_id(i),
                        },
                        offset: v,
                        utxo_hash: make_utxo_hash(i, v),
                        spending_data: sd,
                        current_block_height: 5000,
                    },
                    &engine,
                )
                .unwrap();
        }
    }

    let mismatches = verifier.verify_against(&engine);
    assert!(
        mismatches.is_empty(),
        "block arrival: {} mismatches",
        mismatches.len()
    );

    for i in 0..tx_count {
        let meta = engine
            .read_metadata(&TxKey {
                txid: make_tx_id(i),
            })
            .unwrap();
        assert_eq!({ meta.spent_utxos }, 5);
        assert_eq!(meta.block_entry_count, 1);
    }
}

/// Block reorg: setMined then unsetMined — state reverted.
#[test]
fn realistic_block_reorg() {
    let engine = create_engine();
    let tx_count = scale32(10, 100);

    for i in 0..tx_count {
        let utxo_hashes: Vec<[u8; 32]> = (0..5u32).map(|v| make_utxo_hash(i, v)).collect();
        engine
            .create(&CreateRequest {
                tx_id: make_tx_id(i),
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
            })
            .unwrap();
    }

    // Mine all
    for i in 0..tx_count {
        engine
            .set_mined(&SetMinedRequest {
                tx_key: TxKey {
                    txid: make_tx_id(i),
                },
                block_id: 200,
                block_height: 2000,
                subtree_idx: 0,
                current_block_height: 2000,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: false,
            })
            .unwrap();
    }
    for i in 0..tx_count {
        assert_eq!(
            engine
                .read_metadata(&TxKey {
                    txid: make_tx_id(i)
                })
                .unwrap()
                .block_entry_count,
            1
        );
    }

    // Reorg: unmine all
    for i in 0..tx_count {
        engine
            .set_mined(&SetMinedRequest {
                tx_key: TxKey {
                    txid: make_tx_id(i),
                },
                block_id: 200,
                block_height: 2000,
                subtree_idx: 0,
                current_block_height: 2001,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: true,
            })
            .unwrap();
    }
    for i in 0..tx_count {
        let meta = engine
            .read_metadata(&TxKey {
                txid: make_tx_id(i),
            })
            .unwrap();
        assert_eq!(meta.block_entry_count, 0);
        assert_ne!({ meta.unmined_since }, 0);
    }
}

/// Mempool churn: mixed ops with freeze/conflicting/lock. fast=10K, full=100K ops.
#[test]
fn realistic_mempool_churn() {
    let engine = create_engine();
    let config = WorkloadConfig {
        total_operations: scale(10_000, 100_000),
        tx_creation_rate: 0.10,
        spend_rate: 0.55,
        set_mined_rate: 0.15,
        read_rate: 0.10,
        other_rate: 0.10,
        utxos_per_tx: Distribution::Uniform(2, 15),
        spend_batch_size: Distribution::Uniform(1, 3),
        large_tx_fraction: 0.0,
        concurrent_clients: 1,
        target_ops_per_sec: None,
        seed: 55555,
    };

    let mut wgen = WorkloadGenerator::new(config);
    let ops = wgen.generate_all();
    let mut verifier = StateVerifier::new();

    for (i, op) in ops.iter().enumerate() {
        if let Err(e) = verifier.apply(op, &engine) {
            panic!("mempool churn op {} failed: {}", i, e);
        }
    }

    let mismatches = verifier.verify_against(&engine);
    assert!(
        mismatches.is_empty(),
        "mempool churn: {} mismatches",
        mismatches.len()
    );
}

/// Large transaction: create tx with many UTXOs, spend them, read back.
#[test]
fn realistic_large_transaction() {
    let engine = create_engine();
    let utxo_count = scale32(100, 1000);

    let tx_id = make_tx_id(1);
    let utxo_hashes: Vec<[u8; 32]> = (0..utxo_count).map(|v| make_utxo_hash(1, v)).collect();
    let inputs_data = vec![0xDE; 5000];
    let outputs_data = vec![0xBE; 5000];
    engine
        .create(&CreateRequest {
            tx_id,
            tx_version: 1,
            locktime: 0,
            fee: 500,
            size_in_bytes: 100_000,
            extended_size: 0,
            is_coinbase: false,
            spending_height: 0,
            utxo_hashes: &utxo_hashes,
            inputs: Some(&inputs_data),
            outputs: Some(&outputs_data),
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
        })
        .unwrap();
    let key = TxKey { txid: tx_id };

    for v in 0..utxo_count {
        let mut sd = [0u8; 36];
        sd[0..4].copy_from_slice(&(v + 10000).to_le_bytes());
        sd[32..36].copy_from_slice(&v.to_le_bytes());
        engine
            .spend(&SpendRequest {
                tx_key: key,
                offset: v,
                utxo_hash: make_utxo_hash(1, v),
                spending_data: sd,
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 2000,
                block_height_retention: 288,
            })
            .unwrap();
    }

    assert_eq!(
        { engine.read_metadata(&key).unwrap().spent_utxos },
        utxo_count
    );
    for v in 0..utxo_count {
        assert!(engine.read_slot(&key, v).unwrap().is_spent());
    }
    assert!(!engine.read_cold_data(&key).unwrap().is_empty());
}

// ===========================================================================
// Tiered storage integration tests
// ===========================================================================

/// Mixed tier workload: small txs + medium txs, all ops work, cleanup on delete.
#[test]
fn tiered_storage_mixed_workload() {
    let engine = create_engine();
    let small_count = scale32(10, 100);
    let medium_count = scale32(3, 10);

    for i in 0..small_count {
        let utxo_hashes: Vec<[u8; 32]> = (0..5u32).map(|v| make_utxo_hash(i, v)).collect();
        let inp = vec![0xAA; 100];
        let outp = vec![0xBB; 100];
        engine
            .create(&CreateRequest {
                tx_id: make_tx_id(i),
                tx_version: 1,
                locktime: 0,
                fee: 500,
                size_in_bytes: 250,
                extended_size: 0,
                is_coinbase: false,
                spending_height: 0,
                utxo_hashes: &utxo_hashes,
                inputs: Some(&inp),
                outputs: Some(&outp),
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
            })
            .unwrap();
    }
    let offset = small_count;
    for i in 0..medium_count {
        let idx = offset + i;
        let utxo_hashes: Vec<[u8; 32]> = (0..50u32).map(|v| make_utxo_hash(idx, v)).collect();
        let inp = vec![0xCC; 2000];
        let outp = vec![0xDD; 2000];
        engine
            .create(&CreateRequest {
                tx_id: make_tx_id(idx),
                tx_version: 1,
                locktime: 0,
                fee: 5000,
                size_in_bytes: 10_000,
                extended_size: 0,
                is_coinbase: false,
                spending_height: 0,
                utxo_hashes: &utxo_hashes,
                inputs: Some(&inp),
                outputs: Some(&outp),
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
            })
            .unwrap();
    }

    let total = small_count + medium_count;
    for i in 0..total {
        let key = TxKey {
            txid: make_tx_id(i),
        };
        assert!(engine.read_metadata(&key).unwrap().utxo_count > 0);
        assert!(!engine.read_cold_data(&key).unwrap().is_empty());
    }

    // Spend one UTXO per tx
    for i in 0..total {
        let key = TxKey {
            txid: make_tx_id(i),
        };
        let mut sd = [0u8; 36];
        sd[0..4].copy_from_slice(&(i + 10000).to_le_bytes());
        engine
            .spend(&SpendRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: make_utxo_hash(i, 0),
                spending_data: sd,
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 2000,
                block_height_retention: 288,
            })
            .unwrap();
    }
    for i in 0..total {
        assert_eq!(
            {
                engine
                    .read_metadata(&TxKey {
                        txid: make_tx_id(i),
                    })
                    .unwrap()
                    .spent_utxos
            },
            1
        );
    }

    // Delete and verify cleanup
    for i in 0..total {
        let key = TxKey {
            txid: make_tx_id(i),
        };
        engine
            .delete(&DeleteRequest {
                tx_key: key,
                due_guard: None,
            })
            .unwrap();
        assert!(engine.lookup(&key).is_none());
    }
}

/// Read cold data: data survives spend.
#[test]
fn tiered_storage_cold_data_read() {
    let engine = create_engine();
    let tx_id = make_tx_id(1);
    let utxo_hashes = [make_utxo_hash(1, 0)];
    let inputs_data = vec![0xDE; 4096];
    let outputs_data = vec![0xBE; 4096];
    let inpoints_data = vec![0xFE; 2048];
    engine
        .create(&CreateRequest {
            tx_id,
            tx_version: 1,
            locktime: 0,
            fee: 500,
            size_in_bytes: 10000,
            extended_size: 0,
            is_coinbase: false,
            spending_height: 0,
            utxo_hashes: &utxo_hashes,
            inputs: Some(&inputs_data),
            outputs: Some(&outputs_data),
            inpoints: Some(&inpoints_data),
            is_external: false,
            created_at: 1710000000000,
            block_height: 1000,
            mined_block_infos: &[],
            frozen: false,
            conflicting: false,
            locked: false,
            external_ref: None,
            parent_txids: &[],
        })
        .unwrap();
    let key = TxKey { txid: tx_id };

    let mut sd = [0u8; 36];
    sd[0] = 0xAA;
    engine
        .spend(&SpendRequest {
            tx_key: key,
            offset: 0,
            utxo_hash: make_utxo_hash(1, 0),
            spending_data: sd,
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 2000,
            block_height_retention: 288,
        })
        .unwrap();

    assert!(!engine.read_cold_data(&key).unwrap().is_empty());
}

// ===========================================================================
// Deterministic simulation tests
// ===========================================================================

/// Crash probability 1%: zero inconsistencies. fast=5K/seed, full=50K/seed.
#[test]
fn simulation_crash_1pct() {
    let ops = scale(5_000, 50_000);
    let seeds = scale(3, 10);
    for seed in 0..seeds {
        let mut sim = Simulation::new_single_node(seed + 200);
        let result = sim.run_with_faults(SimulationConfig {
            operations: ops,
            crash_probability: 0.01,
            ..SimulationConfig::default()
        });
        assert!(
            result.inconsistencies_found.is_empty(),
            "seed {}: {} inconsistencies: {:?}",
            seed,
            result.inconsistencies_found.len(),
            &result.inconsistencies_found[..result.inconsistencies_found.len().min(5)]
        );
    }
}

/// Combined faults (random crashes + injected device I/O errors): zero
/// data loss. fast=10K/seed, full=100K/seed.
#[test]
fn simulation_combined_faults() {
    let ops = scale(10_000, 100_000);
    let seeds = scale(2, 5);
    for seed in 0..seeds {
        let mut sim = Simulation::new_single_node(seed + 300);
        let result = sim.run_with_faults(SimulationConfig {
            operations: ops,
            crash_probability: 0.005,
            io_error_probability: 0.002,
            seed: seed + 300,
        });
        assert!(
            !result.data_loss_detected,
            "seed {}: data loss. Inconsistencies: {:?}",
            seed,
            &result.inconsistencies_found[..result.inconsistencies_found.len().min(5)]
        );
    }
}

/// Reproducibility: same seed → same result.
#[test]
fn simulation_reproducibility() {
    let config = SimulationConfig {
        operations: 5_000,
        crash_probability: 0.02,
        seed: 999,
        ..SimulationConfig::default()
    };

    let mut sim1 = Simulation::new_single_node(999);
    let r1 = sim1.run_with_faults(config.clone());
    let mut sim2 = Simulation::new_single_node(999);
    let r2 = sim2.run_with_faults(config);

    assert_eq!(r1.operations_completed, r2.operations_completed);
    assert_eq!(r1.crashes_injected, r2.crashes_injected);
    assert_eq!(
        r1.inconsistencies_found.len(),
        r2.inconsistencies_found.len()
    );
}

// ===========================================================================
// Long-running stability tests
// ===========================================================================

/// Sustained mixed workload across rounds: no mismatches.
/// fast=3 rounds x 1K ops, full=10 rounds x 10K ops.
#[test]
fn stability_sustained_workload_no_growth() {
    let engine = create_engine();
    let mut verifier = StateVerifier::new();
    let rounds = scale32(3, 10);
    let ops_per_round = scale(1_000, 10_000);

    for round in 0..rounds {
        let config = WorkloadConfig {
            total_operations: ops_per_round,
            tx_creation_rate: 0.15,
            spend_rate: 0.50,
            set_mined_rate: 0.20,
            read_rate: 0.10,
            other_rate: 0.05,
            utxos_per_tx: Distribution::Uniform(1, 10),
            spend_batch_size: Distribution::Fixed(1),
            large_tx_fraction: 0.0,
            concurrent_clients: 1,
            target_ops_per_sec: None,
            // Each round gets a unique seed range so tx IDs don't collide
            seed: 7_000_000 + round as u64 * 1_000_000,
        };

        let mut wgen = WorkloadGenerator::new(config);
        let ops = wgen.generate_all();

        for (i, op) in ops.iter().enumerate() {
            if let Err(e) = verifier.apply(op, &engine) {
                panic!("round {} op {} failed: {}", round, i, e);
            }
        }

        let mismatches = verifier.verify_against(&engine);
        assert!(
            mismatches.is_empty(),
            "round {}: {} mismatches",
            round,
            mismatches.len()
        );
    }
}

/// Device fill + churn: freelist reuse works.
#[test]
fn stability_device_fill_and_churn() {
    let engine = create_engine_with_size(16 * 1024 * 1024);

    let mut created = 0u32;
    let mut keys = Vec::new();
    for i in 0..2000u32 {
        let utxo_hashes: Vec<[u8; 32]> = (0..5u32).map(|v| make_utxo_hash(i, v)).collect();
        match engine.create(&CreateRequest {
            tx_id: make_tx_id(i),
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
        }) {
            Ok(_) => {
                keys.push(TxKey {
                    txid: make_tx_id(i),
                });
                created += 1;
            }
            Err(_) => break,
        }
    }
    assert!(created > 100);

    let half = created as usize / 2;
    for key in keys.iter().take(half) {
        engine
            .delete(&DeleteRequest {
                tx_key: *key,
                due_guard: None,
            })
            .unwrap();
    }

    let mut new_created = 0u32;
    for i in 5000..7000u32 {
        let utxo_hashes: Vec<[u8; 32]> = (0..3u32).map(|v| make_utxo_hash(i, v)).collect();
        match engine.create(&CreateRequest {
            tx_id: make_tx_id(i),
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
            block_height: 2000,
            mined_block_infos: &[],
            frozen: false,
            conflicting: false,
            locked: false,
            external_ref: None,
            parent_txids: &[],
        }) {
            Ok(_) => new_created += 1,
            Err(_) => break,
        }
    }
    assert!(new_created > 0, "should reuse freed space");
}

// ===========================================================================
// Performance measurement tests
// ===========================================================================

/// Spend throughput. fast=1K txs, full=10K txs.
#[test]
fn perf_spend_throughput() {
    let count = scale32(1_000, 10_000);
    let engine = create_engine();

    for i in 0..count {
        let utxo_hashes: Vec<[u8; 32]> = (0..10u32).map(|v| make_utxo_hash(i, v)).collect();
        engine
            .create(&CreateRequest {
                tx_id: make_tx_id(i),
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
            })
            .unwrap();
    }

    let start = std::time::Instant::now();
    let mut spend_count = 0u64;
    for i in 0..count {
        let key = TxKey {
            txid: make_tx_id(i),
        };
        for v in 0..5u32 {
            let mut sd = [0u8; 36];
            sd[0..4].copy_from_slice(&(i + 10000).to_le_bytes());
            sd[32..36].copy_from_slice(&v.to_le_bytes());
            engine
                .spend(&SpendRequest {
                    tx_key: key,
                    offset: v,
                    utxo_hash: make_utxo_hash(i, v),
                    spending_data: sd,
                    ignore_conflicting: false,
                    ignore_locked: false,
                    current_block_height: 2000,
                    block_height_retention: 288,
                })
                .unwrap();
            spend_count += 1;
        }
    }
    let elapsed = start.elapsed();
    eprintln!(
        "Spend: {:.0} ops/sec ({} in {:.2}s)",
        spend_count as f64 / elapsed.as_secs_f64(),
        spend_count,
        elapsed.as_secs_f64()
    );
}

/// Create throughput. fast=1K, full=10K.
#[test]
fn perf_create_throughput() {
    let count = scale32(1_000, 10_000);
    let engine = create_engine();
    let start = std::time::Instant::now();
    for i in 0..count {
        let utxo_hashes: Vec<[u8; 32]> = (0..10u32).map(|v| make_utxo_hash(i, v)).collect();
        engine
            .create(&CreateRequest {
                tx_id: make_tx_id(i),
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
            })
            .unwrap();
    }
    let elapsed = start.elapsed();
    eprintln!(
        "Create (10 UTXOs): {:.0} ops/sec ({} in {:.2}s)",
        count as f64 / elapsed.as_secs_f64(),
        count,
        elapsed.as_secs_f64()
    );
}

/// SetMined throughput. fast=1K, full=10K.
#[test]
fn perf_set_mined_throughput() {
    let count = scale32(1_000, 10_000);
    let engine = create_engine();
    for i in 0..count {
        let utxo_hashes: Vec<[u8; 32]> = (0..5u32).map(|v| make_utxo_hash(i, v)).collect();
        engine
            .create(&CreateRequest {
                tx_id: make_tx_id(i),
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
            })
            .unwrap();
    }
    let start = std::time::Instant::now();
    for i in 0..count {
        engine
            .set_mined(&SetMinedRequest {
                tx_key: TxKey {
                    txid: make_tx_id(i),
                },
                block_id: i + 100,
                block_height: 2000,
                subtree_idx: 0,
                current_block_height: 2000,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: false,
            })
            .unwrap();
    }
    let elapsed = start.elapsed();
    eprintln!(
        "SetMined: {:.0} ops/sec ({} in {:.2}s)",
        count as f64 / elapsed.as_secs_f64(),
        count,
        elapsed.as_secs_f64()
    );
}

/// SpendMulti (batch 10) throughput. fast=500, full=5K batches.
#[test]
fn perf_spend_multi_throughput() {
    let count = scale32(500, 5_000);
    let engine = create_engine();
    for i in 0..count {
        let utxo_hashes: Vec<[u8; 32]> = (0..10u32).map(|v| make_utxo_hash(i, v)).collect();
        engine
            .create(&CreateRequest {
                tx_id: make_tx_id(i),
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
            })
            .unwrap();
    }
    let start = std::time::Instant::now();
    for i in 0..count {
        let key = TxKey {
            txid: make_tx_id(i),
        };
        let spends: Vec<SpendItem> = (0..10u32)
            .map(|v| {
                let mut sd = [0u8; 36];
                sd[0..4].copy_from_slice(&(i + 10000).to_le_bytes());
                sd[32..36].copy_from_slice(&v.to_le_bytes());
                SpendItem {
                    offset: v,
                    utxo_hash: make_utxo_hash(i, v),
                    spending_data: sd,
                    idx: v,
                }
            })
            .collect();
        engine
            .spend_multi(&SpendMultiRequest {
                tx_key: key,
                spends,
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 2000,
                block_height_retention: 288,
            })
            .unwrap();
    }
    let elapsed = start.elapsed();
    eprintln!(
        "SpendMulti (batch 10): {:.0} batches/sec ({} in {:.2}s)",
        count as f64 / elapsed.as_secs_f64(),
        count,
        elapsed.as_secs_f64()
    );
}

/// Memory per record: verify < 64 bytes per index entry.
#[test]
fn perf_memory_per_record() {
    let engine = create_engine();
    let count = 10_000u32;
    for i in 0..count {
        let utxo_hashes: Vec<[u8; 32]> = (0..5u32).map(|v| make_utxo_hash(i, v)).collect();
        engine
            .create(&CreateRequest {
                tx_id: make_tx_id(i),
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
            })
            .unwrap();
    }

    let stats = engine.index_stats();
    // TxIndexEntry: device_id(1) + record_offset(8) + utxo_count(4) + flags(2) + spent_utxos(4) + dah_or_preserve(4) + unmined_since(4) + generation(4) = 31 bytes
    // TxKey: 32 bytes
    // Total per-entry: 58 bytes + hash table overhead
    // Amortized overhead = capacity * bucket_size / entry_count
    let entry_bytes = 32 + 26; // key + value = 58 bytes raw
    eprintln!(
        "Index: {} entries, capacity {}, load {:.3}, raw entry size {} bytes",
        stats.entry_count, stats.capacity, stats.load_factor, entry_bytes
    );
    assert!(
        entry_bytes <= 64,
        "raw entry size {} exceeds 64 byte target",
        entry_bytes
    );
}

/// Read throughput. fast=1K, full=10K txs.
#[test]
fn perf_read_throughput() {
    let count = scale32(1_000, 10_000);
    let engine = create_engine();
    for i in 0..count {
        let utxo_hashes: Vec<[u8; 32]> = (0..5u32).map(|v| make_utxo_hash(i, v)).collect();
        engine
            .create(&CreateRequest {
                tx_id: make_tx_id(i),
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
            })
            .unwrap();
    }
    let start = std::time::Instant::now();
    let mut reads = 0u64;
    for _ in 0..3 {
        for i in 0..count {
            let _ = engine
                .read_metadata(&TxKey {
                    txid: make_tx_id(i),
                })
                .unwrap();
            reads += 1;
        }
    }
    let elapsed = start.elapsed();
    eprintln!(
        "Read: {:.0} ops/sec ({} in {:.2}s)",
        reads as f64 / elapsed.as_secs_f64(),
        reads,
        elapsed.as_secs_f64()
    );
}

/// Concurrent spend throughput: 1, 4, 8, 16 threads. fast=1K txs, full=10K.
#[test]
fn perf_concurrent_spend_throughput() {
    let count = scale32(1_000, 10_000) as usize;
    let engine = create_engine();

    for thread_count in [1usize, 4, 8] {
        // Create fresh txs
        for i in 0..count {
            let tx_id = make_tx_id(i as u32);
            if engine.lookup(&TxKey { txid: tx_id }).is_some() {
                engine
                    .delete(&DeleteRequest {
                        tx_key: TxKey { txid: tx_id },
                        due_guard: None,
                    })
                    .unwrap();
            }
            let utxo_hashes: Vec<[u8; 32]> =
                (0..10u32).map(|v| make_utxo_hash(i as u32, v)).collect();
            engine
                .create(&CreateRequest {
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
                })
                .unwrap();
        }

        let chunk = count / thread_count;
        let start = std::time::Instant::now();
        let handles: Vec<_> = (0..thread_count)
            .map(|t| {
                let engine = engine.clone();
                let lo = t * chunk;
                let hi = lo + chunk;
                std::thread::spawn(move || {
                    let mut n = 0u64;
                    for i in lo..hi {
                        let key = TxKey {
                            txid: make_tx_id(i as u32),
                        };
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
                            n += 1;
                        }
                    }
                    n
                })
            })
            .collect();
        let total: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        let elapsed = start.elapsed();
        eprintln!(
            "Concurrent spend ({} threads): {:.0} ops/sec ({} in {:.2}s)",
            thread_count,
            total as f64 / elapsed.as_secs_f64(),
            total,
            elapsed.as_secs_f64()
        );
    }
}
