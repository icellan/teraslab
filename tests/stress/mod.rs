//! Long-running stress tests for TeraSlab.
//!
//! These tests run many operations to surface rare bugs.
//! They use smaller scales for CI, with comments indicating
//! production-scale parameters.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, TxKey, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::create::*;
use teraslab::ops::engine::Engine;
use teraslab::ops::error::SpendError;
use teraslab::ops::mark_longest_chain::*;
use teraslab::ops::remaining::*;
use teraslab::ops::set_mined::*;
use teraslab::ops::spend::*;

fn create_engine(size: u64) -> Arc<Engine> {
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

fn create_stress_tx(engine: &Engine, n: u32, utxo_count: u32) -> TxKey {
    let tx_id = make_tx_id(n);
    let utxo_hashes: Vec<[u8; 32]> = (0..utxo_count).map(|v| make_utxo_hash(n, v)).collect();
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
    TxKey { txid: tx_id }
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
                    let key = TxKey {
                        txid: make_tx_id(i as u32),
                    };

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
        let key = TxKey {
            txid: make_tx_id(i),
        };
        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.spent_utxos }, 5);
        assert_eq!(meta.block_entry_count, 1);
    }

    // ----- Contended phase (REL-108) -----
    //
    // The phase above partitions txids into disjoint per-thread ranges, so no
    // two threads ever touch the same key or contend on the same engine stripe.
    // That never exercises the per-stripe mutual-exclusion contract. Here every
    // thread races to spend the SAME (txid, offset) pairs with DISTINCT
    // spending_data. The striped lock must serialize them so that, per
    // (txid, offset): exactly ONE thread wins (UNSPENT -> SPENT once) and all
    // others observe an AlreadySpent error — never a torn slot, lost increment,
    // or double-spend.
    stress_random_operations_contended();
}

/// Shared-key contention variant of [`stress_random_operations`]: many threads
/// hammer the same stripes/keys concurrently and we assert the engine stays
/// internally consistent (exactly-once spend per UTXO under the stripe lock).
fn stress_random_operations_contended() {
    let engine = create_engine(64 * 1024 * 1024);
    let thread_count = 8usize;
    // Use a txid range disjoint from the partitioned phase's keys.
    let shared_tx_base = 1_000_000u32;
    let shared_tx_count = 64u32;
    let utxos_per_tx = 5u32;

    for i in 0..shared_tx_count {
        create_stress_tx(&engine, shared_tx_base + i, utxos_per_tx);
    }

    // One success counter per (tx, offset). After the race, each must be
    // exactly 1: the stripe lock guarantees a single UNSPENT -> SPENT
    // transition even though `thread_count` threads attempted it.
    let success_counts: Arc<Vec<AtomicU32>> = Arc::new(
        (0..(shared_tx_count * utxos_per_tx))
            .map(|_| AtomicU32::new(0))
            .collect(),
    );

    let handles: Vec<_> = (0..thread_count)
        .map(|t| {
            let engine = engine.clone();
            let success_counts = success_counts.clone();
            std::thread::spawn(move || {
                let mut already_spent = 0u32;
                for i in 0..shared_tx_count {
                    let key = TxKey {
                        txid: make_tx_id(shared_tx_base + i),
                    };
                    for v in 0..utxos_per_tx {
                        // Each thread uses DISTINCT spending_data for the same
                        // UTXO, so a second spend cannot be idempotent — it must
                        // be rejected as AlreadySpent once the slot is SPENT.
                        let mut sd = [0u8; 36];
                        sd[0..4].copy_from_slice(&(t as u32 + 1).to_le_bytes());
                        sd[4..8].copy_from_slice(&i.to_le_bytes());
                        sd[32..36].copy_from_slice(&v.to_le_bytes());

                        match engine.spend(&SpendRequest {
                            tx_key: key,
                            offset: v,
                            utxo_hash: make_utxo_hash(shared_tx_base + i, v),
                            spending_data: sd,
                            ignore_conflicting: false,
                            ignore_locked: false,
                            current_block_height: 2000,
                            block_height_retention: 288,
                        }) {
                            Ok(_) => {
                                success_counts[(i * utxos_per_tx + v) as usize]
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                            Err(SpendError::AlreadySpent { offset, .. }) => {
                                assert_eq!(offset, v, "AlreadySpent reported wrong offset");
                                already_spent += 1;
                            }
                            Err(other) => panic!(
                                "contended spend on tx {} offset {} from thread {} \
                                 must succeed or be AlreadySpent, got {other:?}",
                                shared_tx_base + i,
                                v,
                                t
                            ),
                        }
                    }
                }
                already_spent
            })
        })
        .collect();

    let total_already_spent: u32 = handles.into_iter().map(|h| h.join().unwrap()).sum();

    // Each UTXO is spent exactly once across all threads (stripe mutual
    // exclusion); every other attempt is a clean AlreadySpent.
    let total_utxos = shared_tx_count * utxos_per_tx;
    for (idx, c) in success_counts.iter().enumerate() {
        assert_eq!(
            c.load(Ordering::Relaxed),
            1,
            "UTXO #{idx} must transition UNSPENT->SPENT exactly once under contention"
        );
    }
    let expected_already_spent = total_utxos * (thread_count as u32 - 1);
    assert_eq!(
        total_already_spent, expected_already_spent,
        "every losing thread must observe exactly one AlreadySpent per UTXO"
    );

    // Final state per record must be internally consistent: all UTXOs spent
    // exactly once, and the persisted slot reflects a single winner with a
    // valid SPENT status (no torn slot).
    for i in 0..shared_tx_count {
        let key = TxKey {
            txid: make_tx_id(shared_tx_base + i),
        };
        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!(
            { meta.spent_utxos },
            utxos_per_tx,
            "tx {} spent_utxos must equal the number of contended UTXOs",
            shared_tx_base + i
        );
        for v in 0..utxos_per_tx {
            let slot = engine.read_slot(&key, v).unwrap();
            assert!(
                slot.is_spent(),
                "tx {} offset {v} must be SPENT after contention",
                shared_tx_base + i
            );
            // Exactly one thread's spending_data must have landed: the winner's
            // thread id (1..=thread_count) is encoded in the first 4 bytes.
            let winner = u32::from_le_bytes(slot.spending_data[0..4].try_into().unwrap());
            assert!(
                winner >= 1 && winner <= thread_count as u32,
                "tx {} offset {v} slot holds spending_data from an unknown writer \
                 (torn slot?): winner tag {winner}",
                shared_tx_base + i
            );
        }
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
        let key = TxKey {
            txid: make_tx_id(i),
        };
        engine
            .delete(&DeleteRequest {
                tx_key: key,
                due_guard: None,
            })
            .unwrap();
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

/// Repeatedly set and unset mined block entries across many records.
pub fn stress_set_mined_reorg_churn() {
    let engine = create_engine(64 * 1024 * 1024);
    let tx_count = 512u32;

    for i in 0..tx_count {
        create_stress_tx(&engine, i, 2);
    }

    for round in 0..4u32 {
        for i in 0..tx_count {
            let key = TxKey {
                txid: make_tx_id(i),
            };
            let block_id = 10_000 + round * tx_count + i;
            let block_height = 2_000 + round;
            engine
                .set_mined(&SetMinedRequest {
                    tx_key: key,
                    block_id,
                    block_height,
                    subtree_idx: round,
                    current_block_height: block_height,
                    block_height_retention: 288,
                    on_longest_chain: true,
                    unset_mined: false,
                })
                .unwrap();

            let meta = engine.read_metadata(&key).unwrap();
            assert_eq!(meta.block_entry_count, 1);

            engine
                .set_mined(&SetMinedRequest {
                    tx_key: key,
                    block_id,
                    block_height,
                    subtree_idx: round,
                    current_block_height: block_height + 1,
                    block_height_retention: 288,
                    on_longest_chain: true,
                    unset_mined: true,
                })
                .unwrap();

            let meta = engine.read_metadata(&key).unwrap();
            assert_eq!(meta.block_entry_count, 0);
            assert_eq!({ meta.unmined_since }, block_height + 1);
        }
    }
}

/// Flip longest-chain membership after mining without changing block entries.
pub fn stress_mark_longest_chain_reorg_churn() {
    let engine = create_engine(64 * 1024 * 1024);
    let tx_count = 512u32;

    for i in 0..tx_count {
        let key = create_stress_tx(&engine, i, 2);
        engine
            .set_mined(&SetMinedRequest {
                tx_key: key,
                block_id: 20_000 + i,
                block_height: 2_000,
                subtree_idx: 0,
                current_block_height: 2_000,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: false,
            })
            .unwrap();
    }

    for round in 0..8u32 {
        let on_longest_chain = round % 2 == 1;
        let current_block_height = 2_100 + round;
        for i in 0..tx_count {
            let key = TxKey {
                txid: make_tx_id(i),
            };
            engine
                .mark_on_longest_chain(&MarkOnLongestChainRequest {
                    tx_key: key,
                    on_longest_chain,
                    current_block_height,
                    block_height_retention: 288,
                })
                .unwrap();

            let meta = engine.read_metadata(&key).unwrap();
            assert_eq!(meta.block_entry_count, 1);
            let expected_unmined = if on_longest_chain {
                0
            } else {
                current_block_height
            };
            assert_eq!({ meta.unmined_since }, expected_unmined);
        }
    }
}

/// Freeze and reassign many UTXOs, then spend the reassigned hashes.
pub fn stress_reassign_churn() {
    let engine = create_engine(64 * 1024 * 1024);
    let tx_count = 512u32;

    for i in 0..tx_count {
        let key = create_stress_tx(&engine, i, 2);
        let old_hash = make_utxo_hash(i, 0);
        let new_hash = make_utxo_hash(i + 100_000, 0);

        engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: old_hash,
            })
            .unwrap();
        engine
            .reassign(&ReassignRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: old_hash,
                new_utxo_hash: new_hash,
                block_height: 2_000,
                spendable_after: 3,
            })
            .unwrap();

        let slot = engine.read_slot(&key, 0).unwrap();
        assert!(slot.is_unspent());
        assert_eq!(slot.hash, new_hash);

        let mut spending_data = [0u8; 36];
        spending_data[0..4].copy_from_slice(&(i + 50_000).to_le_bytes());
        spending_data[32..36].copy_from_slice(&0u32.to_le_bytes());
        engine
            .spend(&SpendRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: new_hash,
                spending_data,
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 2_004,
                block_height_retention: 288,
            })
            .unwrap();

        assert_eq!({ engine.read_metadata(&key).unwrap().spent_utxos }, 1);
    }
}

/// Toggle conflicting flags repeatedly and verify spend gating recovers.
pub fn stress_set_conflicting_churn() {
    let engine = create_engine(64 * 1024 * 1024);
    let tx_count = 512u32;

    for i in 0..tx_count {
        create_stress_tx(&engine, i, 2);
    }

    for round in 0..8u32 {
        let value = round % 2 == 0;
        for i in 0..tx_count {
            let key = TxKey {
                txid: make_tx_id(i),
            };
            engine
                .set_conflicting(&SetConflictingRequest {
                    tx_key: key,
                    value,
                    current_block_height: 2_000 + round,
                    block_height_retention: 288,
                })
                .unwrap();

            let meta = engine.read_metadata(&key).unwrap();
            assert_eq!(
                meta.flags.contains(teraslab::record::TxFlags::CONFLICTING),
                value
            );
        }
    }

    for i in 0..tx_count {
        let key = TxKey {
            txid: make_tx_id(i),
        };
        let mut spending_data = [0u8; 36];
        spending_data[0..4].copy_from_slice(&(i + 75_000).to_le_bytes());
        engine
            .spend(&SpendRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: make_utxo_hash(i, 0),
                spending_data,
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 2_100,
                block_height_retention: 288,
            })
            .unwrap();
    }
}

/// Apply preserve_until repeatedly across records that also become prune candidates.
pub fn stress_preserve_until_churn() {
    let engine = create_engine(64 * 1024 * 1024);
    let tx_count = 512u32;

    for i in 0..tx_count {
        create_stress_tx(&engine, i, 2);
    }

    for round in 0..4u32 {
        for i in 0..tx_count {
            let key = TxKey {
                txid: make_tx_id(i),
            };
            let preserve_height = 5_000 + round * 100 + i % 100;
            engine
                .preserve_until(&PreserveUntilRequest {
                    tx_key: key,
                    block_height: preserve_height,
                })
                .unwrap();

            let meta = engine.read_metadata(&key).unwrap();
            assert_eq!({ meta.preserve_until }, preserve_height);
            assert_eq!({ meta.delete_at_height }, 0);
        }
    }

    for i in 0..tx_count {
        let key = TxKey {
            txid: make_tx_id(i),
        };
        for offset in 0..2u32 {
            let mut spending_data = [0u8; 36];
            spending_data[0..4].copy_from_slice(&(i + 90_000).to_le_bytes());
            spending_data[32..36].copy_from_slice(&offset.to_le_bytes());
            engine
                .spend(&SpendRequest {
                    tx_key: key,
                    offset,
                    utxo_hash: make_utxo_hash(i, offset),
                    spending_data,
                    ignore_conflicting: false,
                    ignore_locked: false,
                    current_block_height: 6_000,
                    block_height_retention: 288,
                })
                .unwrap();
        }

        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.spent_utxos }, 2);
        assert_eq!({ meta.delete_at_height }, 0);
    }
}
