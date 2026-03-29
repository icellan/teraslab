//! End-to-end integration tests exercising all phases together.
//!
//! These tests create an Engine, run realistic workloads, and verify
//! that every component (storage, index, operations, redo log, tiered
//! storage) works correctly in concert.

use std::collections::HashMap;
use std::sync::Arc;

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
use teraslab::ops::unspend::*;
use teraslab::record::*;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn create_engine() -> Arc<Engine> {
    let dev: Arc<dyn BlockDevice> =
        Arc::new(MemoryDevice::new(256 * 1024 * 1024, 4096).unwrap());
    let alloc = SlotAllocator::new(dev.clone()).unwrap();
    let index = Index::new(10_000).unwrap();
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

fn create_tx(engine: &Engine, n: u32, utxo_count: usize) -> TxKey {
    let tx_id = make_tx_id(n);
    let utxo_hashes: Vec<[u8; 32]> = (0..utxo_count as u32)
        .map(|v| make_utxo_hash(n, v))
        .collect();
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
    TxKey { txid: tx_id }
}

fn spend_utxo(engine: &Engine, key: TxKey, tx_n: u32, vout: u32) {
    let mut sd = [0u8; 36];
    sd[0..4].copy_from_slice(&(tx_n + 10000).to_le_bytes());
    sd[32..36].copy_from_slice(&vout.to_le_bytes());
    engine
        .spend(&SpendRequest {
            tx_key: key,
            offset: vout,
            utxo_hash: make_utxo_hash(tx_n, vout),
            spending_data: sd,
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 2000,
            block_height_retention: 288,
        })
        .unwrap();
}

// ---------------------------------------------------------------------------
// In-memory state verifier
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct ExpectedSlot {
    hash: [u8; 32],
    status: u8,
    spending_data: [u8; 36],
}

#[allow(dead_code)]
struct ExpectedRecord {
    utxo_count: u32,
    spent_utxos: u32,
    slots: Vec<ExpectedSlot>,
    block_ids: Vec<u32>,
    conflicting: bool,
    locked: bool,
    delete_at_height: u32,
    preserve_until: u32,
}

struct StateVerifier {
    records: HashMap<TxKey, ExpectedRecord>,
}

#[allow(dead_code)]
impl StateVerifier {
    fn new() -> Self {
        Self { records: HashMap::new() }
    }

    fn create(&mut self, key: TxKey, utxo_count: u32, hashes: &[[u8; 32]]) {
        let slots = hashes.iter().map(|h| ExpectedSlot {
            hash: *h,
            status: UTXO_UNSPENT,
            spending_data: [0u8; 36],
        }).collect();
        self.records.insert(key, ExpectedRecord {
            utxo_count,
            spent_utxos: 0,
            slots,
            block_ids: vec![],
            conflicting: false,
            locked: false,
            delete_at_height: 0,
            preserve_until: 0,
        });
    }

    fn spend(&mut self, key: &TxKey, offset: u32, spending_data: [u8; 36]) {
        let rec = self.records.get_mut(key).expect("record should exist");
        let slot = &mut rec.slots[offset as usize];
        if slot.status == UTXO_UNSPENT {
            slot.status = UTXO_SPENT;
            slot.spending_data = spending_data;
            rec.spent_utxos += 1;
        }
    }

    fn unspend(&mut self, key: &TxKey, offset: u32) {
        let rec = self.records.get_mut(key).expect("record should exist");
        let slot = &mut rec.slots[offset as usize];
        if slot.status == UTXO_SPENT {
            slot.status = UTXO_UNSPENT;
            slot.spending_data = [0u8; 36];
            rec.spent_utxos -= 1;
        }
    }

    fn set_mined(&mut self, key: &TxKey, block_id: u32) {
        let rec = self.records.get_mut(key).expect("record should exist");
        if !rec.block_ids.contains(&block_id) {
            rec.block_ids.push(block_id);
        }
    }

    fn unset_mined(&mut self, key: &TxKey, block_id: u32) {
        let rec = self.records.get_mut(key).expect("record should exist");
        rec.block_ids.retain(|&id| id != block_id);
    }

    fn delete(&mut self, key: &TxKey) {
        self.records.remove(key);
    }

    fn verify(&self, engine: &Engine) -> Vec<String> {
        let mut mismatches = Vec::new();

        for (key, expected) in &self.records {
            match engine.read_metadata(key) {
                Ok(meta) => {
                    let actual_spent = { meta.spent_utxos };
                    if actual_spent != expected.spent_utxos {
                        mismatches.push(format!(
                            "tx {:?}: spent_utxos expected {}, got {}",
                            key, expected.spent_utxos, actual_spent
                        ));
                    }

                    // Verify each slot
                    for (i, exp_slot) in expected.slots.iter().enumerate() {
                        match engine.read_slot(key, i as u32) {
                            Ok(actual) => {
                                if actual.status != exp_slot.status {
                                    mismatches.push(format!(
                                        "tx {:?} slot {}: status expected {:#x}, got {:#x}",
                                        key, i, exp_slot.status, actual.status
                                    ));
                                }
                                if actual.hash != exp_slot.hash {
                                    mismatches.push(format!(
                                        "tx {:?} slot {}: hash mismatch", key, i
                                    ));
                                }
                            }
                            Err(e) => {
                                mismatches.push(format!(
                                    "tx {:?} slot {}: read error: {}", key, i, e
                                ));
                            }
                        }
                    }
                }
                Err(SpendError::TxNotFound) => {
                    mismatches.push(format!("tx {:?}: expected to exist but not found", key));
                }
                Err(e) => {
                    mismatches.push(format!("tx {:?}: read error: {}", key, e));
                }
            }
        }

        mismatches
    }
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

/// Full lifecycle: create → spend → setMined → delete
#[test]
fn full_lifecycle_single_tx() {
    let engine = create_engine();
    let key = create_tx(&engine, 1, 5);

    // Spend 3 of 5 UTXOs
    for v in 0..3 {
        spend_utxo(&engine, key, 1, v);
    }
    let meta = engine.read_metadata(&key).unwrap();
    assert_eq!({ meta.spent_utxos }, 3);

    // Set mined
    engine.set_mined(&SetMinedRequest {
        tx_key: key, block_id: 42, block_height: 2000, subtree_idx: 0,
        current_block_height: 2000, block_height_retention: 288,
        on_longest_chain: true, unset_mined: false,
    }).unwrap();
    let meta = engine.read_metadata(&key).unwrap();
    assert_eq!(meta.block_entry_count, 1);

    // Delete
    engine.delete(&DeleteRequest { tx_key: key }).unwrap();
    assert!(engine.lookup(&key).is_none());
}

/// Block arrival: create many txs, mine them all, spend some UTXOs.
#[test]
fn simulate_block_arrival() {
    let engine = create_engine();
    let mut verifier = StateVerifier::new();

    // Create 100 transactions with 10 UTXOs each
    let mut keys = Vec::new();
    for i in 0..100u32 {
        let key = create_tx(&engine, i, 10);
        let hashes: Vec<[u8; 32]> = (0..10u32).map(|v| make_utxo_hash(i, v)).collect();
        verifier.create(key, 10, &hashes);
        keys.push((key, i));
    }

    // Mine all in block 500
    for &(key, _) in &keys {
        engine.set_mined(&SetMinedRequest {
            tx_key: key, block_id: 500, block_height: 5000, subtree_idx: 0,
            current_block_height: 5000, block_height_retention: 288,
            on_longest_chain: true, unset_mined: false,
        }).unwrap();
        verifier.set_mined(&key, 500);
    }

    // Spend 50% of UTXOs (first 5 of each tx)
    for &(key, tx_n) in &keys {
        for v in 0..5u32 {
            let mut sd = [0u8; 36];
            sd[0..4].copy_from_slice(&(tx_n + 10000).to_le_bytes());
            sd[32..36].copy_from_slice(&v.to_le_bytes());
            engine.spend(&SpendRequest {
                tx_key: key, offset: v,
                utxo_hash: make_utxo_hash(tx_n, v),
                spending_data: sd,
                ignore_conflicting: false, ignore_locked: false,
                current_block_height: 5000, block_height_retention: 288,
            }).unwrap();
            verifier.spend(&key, v, sd);
        }
    }

    // Verify state
    let mismatches = verifier.verify(&engine);
    assert!(mismatches.is_empty(), "mismatches: {mismatches:#?}");
}

/// Block reorg: mine, then unmine, verify state reverted.
#[test]
fn simulate_block_reorg() {
    let engine = create_engine();

    let key = create_tx(&engine, 1, 5);

    // Mine in block 100
    engine.set_mined(&SetMinedRequest {
        tx_key: key, block_id: 100, block_height: 1000, subtree_idx: 0,
        current_block_height: 1000, block_height_retention: 288,
        on_longest_chain: true, unset_mined: false,
    }).unwrap();

    let meta = engine.read_metadata(&key).unwrap();
    assert_eq!(meta.block_entry_count, 1);
    assert_eq!({ meta.unmined_since }, 0); // On chain

    // Reorg: unmine block 100
    engine.set_mined(&SetMinedRequest {
        tx_key: key, block_id: 100, block_height: 1000, subtree_idx: 0,
        current_block_height: 1001, block_height_retention: 288,
        on_longest_chain: true, unset_mined: true,
    }).unwrap();

    let meta = engine.read_metadata(&key).unwrap();
    assert_eq!(meta.block_entry_count, 0);
    assert_eq!({ meta.unmined_since }, 1001); // Off chain

    // Mark off longest chain
    engine.mark_on_longest_chain(&MarkOnLongestChainRequest {
        tx_key: key, on_longest_chain: false,
        current_block_height: 1001, block_height_retention: 288,
    }).unwrap();

    let meta = engine.read_metadata(&key).unwrap();
    assert_eq!({ meta.unmined_since }, 1001);
}

/// Freeze → reassign → spend with new hash.
#[test]
fn freeze_reassign_spend_lifecycle() {
    let engine = create_engine();
    let key = create_tx(&engine, 1, 3);
    let original_hash = make_utxo_hash(1, 0);

    // Freeze
    engine.freeze(&FreezeRequest {
        tx_key: key, offset: 0, utxo_hash: original_hash,
    }).unwrap();
    assert!(engine.read_slot(&key, 0).unwrap().is_frozen());

    // Reassign
    let new_hash = [0xBBu8; 32];
    engine.reassign(&ReassignRequest {
        tx_key: key, offset: 0, utxo_hash: original_hash,
        new_utxo_hash: new_hash, block_height: 1000, spendable_after: 100,
    }).unwrap();
    let slot = engine.read_slot(&key, 0).unwrap();
    assert!(slot.is_unspent());
    assert_eq!(slot.hash, new_hash);

    // Can't spend with old hash
    let mut sd = [0u8; 36]; sd[0] = 0xDD;
    assert!(matches!(
        engine.spend(&SpendRequest {
            tx_key: key, offset: 0, utxo_hash: original_hash, spending_data: sd,
            ignore_conflicting: false, ignore_locked: false,
            current_block_height: 1200, block_height_retention: 288,
        }),
        Err(SpendError::UtxoHashMismatch { .. })
    ));

    // Can spend with new hash after cooldown
    engine.spend(&SpendRequest {
        tx_key: key, offset: 0, utxo_hash: new_hash, spending_data: sd,
        ignore_conflicting: false, ignore_locked: false,
        current_block_height: 1101, block_height_retention: 288,
    }).unwrap();
    assert!(engine.read_slot(&key, 0).unwrap().is_spent());
}

/// Conflicting → spend blocked → clear → spend succeeds.
#[test]
fn conflicting_lifecycle() {
    let engine = create_engine();
    let key = create_tx(&engine, 1, 3);

    engine.set_conflicting(&SetConflictingRequest {
        tx_key: key, value: true,
        current_block_height: 1000, block_height_retention: 288,
    }).unwrap();

    let mut sd = [0u8; 36]; sd[0] = 0xAA;
    assert!(matches!(
        engine.spend(&SpendRequest {
            tx_key: key, offset: 0, utxo_hash: make_utxo_hash(1, 0), spending_data: sd,
            ignore_conflicting: false, ignore_locked: false,
            current_block_height: 1000, block_height_retention: 288,
        }),
        Err(SpendError::Conflicting)
    ));

    engine.set_conflicting(&SetConflictingRequest {
        tx_key: key, value: false,
        current_block_height: 1000, block_height_retention: 288,
    }).unwrap();

    engine.spend(&SpendRequest {
        tx_key: key, offset: 0, utxo_hash: make_utxo_hash(1, 0), spending_data: sd,
        ignore_conflicting: false, ignore_locked: false,
        current_block_height: 1000, block_height_retention: 288,
    }).unwrap();
}

/// Locked → spend blocked → setMined clears lock → spend succeeds.
#[test]
fn locked_cleared_by_set_mined() {
    let engine = create_engine();
    let key = create_tx(&engine, 1, 3);

    engine.set_locked(&SetLockedRequest { tx_key: key, value: true }).unwrap();
    let meta = engine.read_metadata(&key).unwrap();
    assert!(meta.flags.contains(TxFlags::LOCKED));

    // Mine → clears lock
    engine.set_mined(&SetMinedRequest {
        tx_key: key, block_id: 1, block_height: 1000, subtree_idx: 0,
        current_block_height: 1000, block_height_retention: 288,
        on_longest_chain: true, unset_mined: false,
    }).unwrap();
    let meta = engine.read_metadata(&key).unwrap();
    assert!(!meta.flags.contains(TxFlags::LOCKED));

    // Can spend now
    let mut sd = [0u8; 36]; sd[0] = 0xBB;
    engine.spend(&SpendRequest {
        tx_key: key, offset: 0, utxo_hash: make_utxo_hash(1, 0), spending_data: sd,
        ignore_conflicting: false, ignore_locked: false,
        current_block_height: 1000, block_height_retention: 288,
    }).unwrap();
}

/// PreserveUntil blocks DAH, then create → spend all → setMined.
#[test]
fn preserve_until_blocks_pruning() {
    let engine = create_engine();
    let req = CreateRequest {
        tx_id: make_tx_id(1),
        tx_version: 1, locktime: 0, fee: 500, size_in_bytes: 250,
        extended_size: 0, is_coinbase: false, spending_height: 0,
        utxo_hashes: vec![make_utxo_hash(1, 0), make_utxo_hash(1, 1)],
        inputs: None, outputs: None, inpoints: None, is_external: false,
        created_at: 1710000000000, block_height: 1000,
        mined_block_infos: vec![MinedBlockInfo { block_id: 1, block_height: 900, subtree_idx: 0 }],
        frozen: false, conflicting: false, locked: false, parent_txids: vec![],
    };
    let key = TxKey { txid: req.tx_id };
    engine.create(&req).unwrap();

    engine.preserve_until(&PreserveUntilRequest {
        tx_key: key, block_height: 5000,
    }).unwrap();

    // Spend all UTXOs
    for v in 0..2u32 {
        let mut sd = [0u8; 36]; sd[0] = v as u8;
        engine.spend(&SpendRequest {
            tx_key: key, offset: v, utxo_hash: make_utxo_hash(1, v), spending_data: sd,
            ignore_conflicting: false, ignore_locked: false,
            current_block_height: 2000, block_height_retention: 288,
        }).unwrap();
    }

    // DAH should NOT be set (preserve_until blocks it)
    let meta = engine.read_metadata(&key).unwrap();
    assert_eq!({ meta.delete_at_height }, 0);
    assert_eq!({ meta.preserve_until }, 5000);
}

/// GetSpend: read spending data without modifying state.
#[test]
fn get_spend_lifecycle() {
    let engine = create_engine();
    let key = create_tx(&engine, 1, 3);

    // Unspent
    let resp = engine.get_spend(&GetSpendRequest {
        tx_key: key, offset: 0, utxo_hash: make_utxo_hash(1, 0),
    }).unwrap();
    assert_eq!(resp.status, UTXO_UNSPENT);
    assert!(resp.spending_data.is_none());

    // Spend it
    let mut sd = [0u8; 36]; sd[0] = 0xAA;
    engine.spend(&SpendRequest {
        tx_key: key, offset: 0, utxo_hash: make_utxo_hash(1, 0), spending_data: sd,
        ignore_conflicting: false, ignore_locked: false,
        current_block_height: 1000, block_height_retention: 288,
    }).unwrap();

    // Now shows spent
    let resp = engine.get_spend(&GetSpendRequest {
        tx_key: key, offset: 0, utxo_hash: make_utxo_hash(1, 0),
    }).unwrap();
    assert_eq!(resp.status, UTXO_SPENT);
    assert_eq!(resp.spending_data, Some(sd));
}

/// Concurrent mixed workload: multiple threads creating and spending.
#[test]
fn concurrent_mixed_workload() {
    let engine = create_engine();

    // Pre-create 50 transactions
    let keys: Vec<(TxKey, u32)> = (0..50u32)
        .map(|i| (create_tx(&engine, i, 10), i))
        .collect();

    let engine_ref = engine.clone();
    let handles: Vec<_> = keys
        .iter()
        .map(|&(key, tx_n)| {
            let engine = engine_ref.clone();
            std::thread::spawn(move || {
                // Spend 5 UTXOs
                for v in 0..5u32 {
                    let mut sd = [0u8; 36];
                    sd[0..4].copy_from_slice(&(tx_n + 10000).to_le_bytes());
                    sd[32..36].copy_from_slice(&v.to_le_bytes());
                    engine.spend(&SpendRequest {
                        tx_key: key, offset: v,
                        utxo_hash: make_utxo_hash(tx_n, v), spending_data: sd,
                        ignore_conflicting: false, ignore_locked: false,
                        current_block_height: 2000, block_height_retention: 288,
                    }).unwrap();
                }

                // SetMined
                engine.set_mined(&SetMinedRequest {
                    tx_key: key, block_id: 1, block_height: 2000, subtree_idx: 0,
                    current_block_height: 2000, block_height_retention: 288,
                    on_longest_chain: true, unset_mined: false,
                }).unwrap();
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Verify all 50 transactions
    for &(key, _) in &keys {
        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.spent_utxos }, 5);
        assert_eq!(meta.block_entry_count, 1);
    }
}

/// SpendMulti batch with partial errors.
#[test]
fn spend_multi_partial_errors() {
    let engine = create_engine();
    let key = create_tx(&engine, 1, 10);

    // Spend some UTXOs first to create mixed state
    spend_utxo(&engine, key, 1, 3); // Already spent

    // Freeze one
    engine.freeze(&FreezeRequest {
        tx_key: key, offset: 7, utxo_hash: make_utxo_hash(1, 7),
    }).unwrap();

    // SpendMulti with mixed results
    let req = SpendMultiRequest {
        tx_key: key,
        spends: vec![
            SpendItem { offset: 0, utxo_hash: make_utxo_hash(1, 0), spending_data: [0x01; 36], idx: 0 }, // OK
            SpendItem { offset: 3, utxo_hash: make_utxo_hash(1, 3), spending_data: [0x02; 36], idx: 1 }, // AlreadySpent
            SpendItem { offset: 7, utxo_hash: make_utxo_hash(1, 7), spending_data: [0x03; 36], idx: 2 }, // Frozen
            SpendItem { offset: 5, utxo_hash: make_utxo_hash(1, 5), spending_data: [0x04; 36], idx: 3 }, // OK
            SpendItem { offset: 99, utxo_hash: [0; 32], spending_data: [0x05; 36], idx: 4 },             // OutOfRange
        ],
        ignore_conflicting: false, ignore_locked: false,
        current_block_height: 2000, block_height_retention: 288,
    };

    let resp = engine.spend_multi(&req).unwrap();
    assert_eq!(resp.spent_count, 2); // Only items 0 and 3 succeeded
    assert_eq!(resp.errors.len(), 3);
    assert!(matches!(resp.errors[&1], SpendError::AlreadySpent { .. }));
    assert!(matches!(resp.errors[&2], SpendError::Frozen { .. }));
    assert!(matches!(resp.errors[&4], SpendError::UtxoNotFound { .. }));
}

/// DAH lifecycle: spend all → DAH set → unspend → DAH cleared.
#[test]
fn dah_set_and_cleared() {
    let engine = create_engine();

    let req = CreateRequest {
        tx_id: make_tx_id(1),
        tx_version: 1, locktime: 0, fee: 500, size_in_bytes: 250,
        extended_size: 0, is_coinbase: false, spending_height: 0,
        utxo_hashes: vec![make_utxo_hash(1, 0), make_utxo_hash(1, 1)],
        inputs: None, outputs: None, inpoints: None, is_external: false,
        created_at: 1710000000000, block_height: 1000,
        mined_block_infos: vec![MinedBlockInfo { block_id: 1, block_height: 900, subtree_idx: 0 }],
        frozen: false, conflicting: false, locked: false, parent_txids: vec![],
    };
    let key = TxKey { txid: req.tx_id };
    engine.create(&req).unwrap();

    // Spend all → DAH should be set
    for v in 0..2u32 {
        spend_utxo(&engine, key, 1, v);
    }
    let meta = engine.read_metadata(&key).unwrap();
    assert_ne!({ meta.delete_at_height }, 0);
    assert!(!engine.dah_index().range_query(u32::MAX).is_empty());

    // Unspend one → DAH should be cleared
    engine.unspend(&UnspendRequest {
        tx_key: key, offset: 0, utxo_hash: make_utxo_hash(1, 0),
        current_block_height: 2000, block_height_retention: 288,
    }).unwrap();
    let meta = engine.read_metadata(&key).unwrap();
    assert_eq!({ meta.delete_at_height }, 0);
    assert!(engine.dah_index().range_query(u32::MAX).is_empty());
}

/// Large batch: create 1000 transactions, verify all accessible.
#[test]
fn create_1000_transactions() {
    let engine = create_engine();
    let mut keys = Vec::new();

    for i in 0..1000u32 {
        let key = create_tx(&engine, i, 5);
        keys.push((key, i));
    }

    // Verify all exist
    for &(key, tx_n) in &keys {
        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.utxo_count }, 5);
        for v in 0..5u32 {
            let slot = engine.read_slot(&key, v).unwrap();
            assert!(slot.is_unspent());
            assert_eq!(slot.hash, make_utxo_hash(tx_n, v));
        }
    }
}

/// Coinbase maturity lifecycle.
#[test]
fn coinbase_maturity() {
    let engine = create_engine();

    let tx_id = make_tx_id(1);
    let req = CreateRequest {
        tx_id,
        tx_version: 1, locktime: 0, fee: 0, size_in_bytes: 100,
        extended_size: 0, is_coinbase: true, spending_height: 1100,
        utxo_hashes: vec![make_utxo_hash(1, 0)],
        inputs: None, outputs: None, inpoints: None, is_external: false,
        created_at: 1710000000000, block_height: 1000,
        mined_block_infos: vec![MinedBlockInfo { block_id: 1, block_height: 1000, subtree_idx: 0 }],
        frozen: false, conflicting: false, locked: false, parent_txids: vec![],
    };
    let key = TxKey { txid: tx_id };
    engine.create(&req).unwrap();

    // Can't spend before maturity
    let mut sd = [0u8; 36]; sd[0] = 0xAA;
    assert!(matches!(
        engine.spend(&SpendRequest {
            tx_key: key, offset: 0, utxo_hash: make_utxo_hash(1, 0), spending_data: sd,
            ignore_conflicting: false, ignore_locked: false,
            current_block_height: 1050, block_height_retention: 288,
        }),
        Err(SpendError::CoinbaseImmature { .. })
    ));

    // Can spend at maturity
    engine.spend(&SpendRequest {
        tx_key: key, offset: 0, utxo_hash: make_utxo_hash(1, 0), spending_data: sd,
        ignore_conflicting: false, ignore_locked: false,
        current_block_height: 1100, block_height_retention: 288,
    }).unwrap();
}

/// Delete cleans up DAH and unmined indexes.
#[test]
fn delete_cleans_secondary_indexes() {
    let engine = create_engine();
    let key = create_tx(&engine, 1, 2);

    // Should be in unmined index (created without block info)
    assert!(!engine.unmined_index().range_query(u32::MAX).is_empty());

    engine.delete(&DeleteRequest { tx_key: key }).unwrap();

    // Secondary indexes should be clean
    assert!(engine.unmined_index().range_query(u32::MAX).is_empty());
    assert!(engine.dah_index().range_query(u32::MAX).is_empty());
}

/// Cold data survives operations.
#[test]
fn cold_data_survives_mutations() {
    let engine = create_engine();

    let tx_id = make_tx_id(1);
    let req = CreateRequest {
        tx_id,
        tx_version: 1, locktime: 0, fee: 500, size_in_bytes: 250,
        extended_size: 0, is_coinbase: false, spending_height: 0,
        utxo_hashes: vec![make_utxo_hash(1, 0), make_utxo_hash(1, 1)],
        inputs: Some(vec![0xDE, 0xAD]),
        outputs: Some(vec![0xBE, 0xEF]),
        inpoints: None, is_external: false,
        created_at: 1710000000000, block_height: 1000,
        mined_block_infos: vec![], frozen: false, conflicting: false, locked: false, parent_txids: vec![],
    };
    let key = TxKey { txid: tx_id };
    engine.create(&req).unwrap();

    let cold_before = engine.read_cold_data(&key).unwrap();

    // Spend, setMined, setConflicting — none should corrupt cold data
    spend_utxo(&engine, key, 1, 0);
    engine.set_mined(&SetMinedRequest {
        tx_key: key, block_id: 1, block_height: 1000, subtree_idx: 0,
        current_block_height: 1000, block_height_retention: 288,
        on_longest_chain: true, unset_mined: false,
    }).unwrap();
    engine.set_conflicting(&SetConflictingRequest {
        tx_key: key, value: true, current_block_height: 1000, block_height_retention: 288,
    }).unwrap();

    let cold_after = engine.read_cold_data(&key).unwrap();
    assert_eq!(cold_before, cold_after);
}
