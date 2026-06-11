//! Property-based UTXO-conservation tests (N-03 / LMNH-16).
//!
//! Drives a real [`Engine`] over a [`MemoryDevice`] with proptest-generated
//! operation sequences over a deliberately tiny keyspace (4 txids x <=4
//! slots x 3 spender identities) so operations collide constantly:
//! double-spends, idempotent re-spends, unspends with the wrong
//! spending_data, freeze/unfreeze races, deletes followed by re-creates.
//!
//! Oracle: a pure in-test model (`Model`) that mirrors the engine's
//! documented validation order. After EVERY operation the engine result is
//! compared against the model-predicted [`Outcome`] (success/error variant
//! equality, including error payloads such as the recorded spending_data in
//! `AlreadySpent`). After the full sequence, complete state equivalence is
//! asserted: per-record spent counts, per-slot status + spending_data,
//! conflicting/locked flags, mined block-id sets, and existence.
//!
//! Invariants encoded:
//! - a UTXO is accepted as spent exactly once (second spend with different
//!   data -> `AlreadySpent` carrying the FIRST spender's data);
//! - identical-data re-spend is idempotent (no `spent_utxos` bump);
//! - unspend with wrong spending_data never mutates (silent no-op success,
//!   matching the Lua `callerOwnsSpend` contract);
//! - `spent_utxos` always equals the number of spent slots;
//! - deleted records stay deleted (until an explicit re-create).
//!
//! Case count: 64 by default (CI-cheap). Crank it with the standard
//! proptest env var, e.g. `PROPTEST_CASES=4096 cargo test --test
//! property_utxo`. The env var takes precedence over the built-in default
//! because the config below only sets `cases` when the var is absent.

use std::collections::BTreeMap;
use std::sync::Arc;

use proptest::prelude::*;
use proptest::test_runner::Config as ProptestConfig;

use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, TxKey, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::create::{CreateError, CreateRequest};
use teraslab::ops::engine::Engine;
use teraslab::ops::error::SpendError;
use teraslab::ops::remaining::{
    DeleteRequest, FreezeRequest, SetConflictingRequest, SetLockedRequest, UnfreezeRequest,
};
use teraslab::ops::set_mined::SetMinedRequest;
use teraslab::ops::spend::SpendRequest;
use teraslab::ops::unspend::UnspendRequest;
use teraslab::record::{FROZEN_BYTE, TxFlags, UTXO_FROZEN, UTXO_SPENT, UTXO_UNSPENT};

// ---------------------------------------------------------------------------
// Fixed workload parameters
// ---------------------------------------------------------------------------

/// Number of distinct transactions in the keyspace.
const TX_SPACE: u8 = 4;
/// Maximum UTXO slots per transaction.
const MAX_SLOTS: u8 = 4;
/// Number of distinct spender identities (small so identical-data
/// re-spends and wrong-data unspends both occur frequently).
const SPENDER_SPACE: u8 = 3;
/// Number of distinct block ids per tx for set_mined (<= 3 keeps every
/// block entry inline so the final-state check can read them directly).
const BLOCK_SPACE: u8 = 3;

const CREATE_BLOCK_HEIGHT: u32 = 1000;
const CURRENT_BLOCK_HEIGHT: u32 = 2000;
const RETENTION: u32 = 288;

fn make_engine() -> Arc<Engine> {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(8 * 1024 * 1024, 4096).unwrap());
    let alloc = SlotAllocator::new(dev.clone()).unwrap();
    let index = Index::new(4096).unwrap();
    Arc::new(Engine::new(
        dev,
        index,
        alloc,
        StripedLocks::new(64),
        DahIndex::new(),
        UnminedIndex::new(),
    ))
}

fn txid(tx: u8) -> [u8; 32] {
    let mut id = [0u8; 32];
    id[0] = tx;
    id[1] = 0xA5;
    id[16] = tx.wrapping_mul(37);
    id
}

fn tx_key(tx: u8) -> TxKey {
    TxKey { txid: txid(tx) }
}

fn utxo_hash(tx: u8, vout: u8) -> [u8; 32] {
    let mut h = [0u8; 32];
    h[0] = vout;
    h[1] = 0x5A;
    h[4] = tx;
    h
}

/// Deterministic spending_data for (spender, vout). Never all-0xFF (the
/// reserved frozen sentinel) because byte 0 is `spender + 1` <= 3.
fn spending_data(spender: u8, vout: u8) -> [u8; 36] {
    let mut sd = [0u8; 36];
    sd[0] = spender + 1;
    sd[1] = 0xC3;
    sd[31] = spender.wrapping_mul(0x11);
    sd[32] = vout;
    sd
}

fn block_id(block: u8) -> u32 {
    100 + block as u32
}

// ---------------------------------------------------------------------------
// Operations and strategy
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum Op {
    Create { tx: u8, utxo_count: u8 },
    Spend { tx: u8, vout: u8, spender: u8 },
    Unspend { tx: u8, vout: u8, spender: u8 },
    Freeze { tx: u8, vout: u8 },
    Unfreeze { tx: u8, vout: u8 },
    SetMined { tx: u8, block: u8 },
    SetConflicting { tx: u8, value: bool },
    SetLocked { tx: u8, value: bool },
    Delete { tx: u8 },
}

fn op_strategy() -> impl Strategy<Value = Op> {
    // vout ranges over 0..=MAX_SLOTS so offsets at/above utxo_count are
    // generated, exercising the UtxoNotFound path.
    prop_oneof![
        3 => (0..TX_SPACE, 1..=MAX_SLOTS)
            .prop_map(|(tx, utxo_count)| Op::Create { tx, utxo_count }),
        6 => (0..TX_SPACE, 0..=MAX_SLOTS, 0..SPENDER_SPACE)
            .prop_map(|(tx, vout, spender)| Op::Spend { tx, vout, spender }),
        3 => (0..TX_SPACE, 0..=MAX_SLOTS, 0..SPENDER_SPACE)
            .prop_map(|(tx, vout, spender)| Op::Unspend { tx, vout, spender }),
        2 => (0..TX_SPACE, 0..=MAX_SLOTS).prop_map(|(tx, vout)| Op::Freeze { tx, vout }),
        2 => (0..TX_SPACE, 0..=MAX_SLOTS).prop_map(|(tx, vout)| Op::Unfreeze { tx, vout }),
        2 => (0..TX_SPACE, 0..BLOCK_SPACE).prop_map(|(tx, block)| Op::SetMined { tx, block }),
        1 => (0..TX_SPACE, any::<bool>())
            .prop_map(|(tx, value)| Op::SetConflicting { tx, value }),
        1 => (0..TX_SPACE, any::<bool>()).prop_map(|(tx, value)| Op::SetLocked { tx, value }),
        1 => (0..TX_SPACE).prop_map(|tx| Op::Delete { tx }),
    ]
}

// ---------------------------------------------------------------------------
// Oracle model
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum SlotState {
    Unspent,
    Spent([u8; 36]),
    Frozen,
}

#[derive(Debug, Clone)]
struct ModelTx {
    utxo_count: u8,
    slots: Vec<SlotState>,
    spent: u32,
    mined: Vec<u32>, // distinct block ids, insertion order
    conflicting: bool,
    locked: bool,
}

#[derive(Debug, Default)]
struct Model {
    txs: BTreeMap<u8, ModelTx>,
}

/// Unified outcome of any engine operation, for exact expected-vs-actual
/// comparison. Error variants carry the same payload fields the engine
/// errors carry so payload equality is asserted, not just variant equality.
#[derive(Debug, Clone, PartialEq)]
enum Outcome {
    Ok,
    /// Success carrying the record's block-id set (sorted) — used for
    /// spend and set_mined, whose responses return block ids.
    OkBlockIds(Vec<u32>),
    TxNotFound,
    DuplicateTxId,
    Conflicting,
    Locked,
    UtxoNotFound(u32),
    Frozen(u32),
    AlreadySpent(u32, [u8; 36]),
    InvalidSpend(u32, [u8; 36]),
    AlreadyFrozen(u32),
    NotFrozen(u32),
    /// Any engine result the model never predicts — always a test failure.
    Unexpected(String),
}

fn sorted(mut v: Vec<u32>) -> Vec<u32> {
    v.sort_unstable();
    v
}

fn spend_error_outcome(e: SpendError) -> Outcome {
    match e {
        SpendError::TxNotFound => Outcome::TxNotFound,
        SpendError::Conflicting => Outcome::Conflicting,
        SpendError::Locked => Outcome::Locked,
        SpendError::UtxoNotFound { offset } => Outcome::UtxoNotFound(offset),
        SpendError::Frozen { offset } => Outcome::Frozen(offset),
        SpendError::AlreadySpent {
            offset,
            spending_data,
        } => Outcome::AlreadySpent(offset, spending_data),
        SpendError::InvalidSpend {
            offset,
            spending_data,
        } => Outcome::InvalidSpend(offset, spending_data),
        SpendError::AlreadyFrozen { offset } => Outcome::AlreadyFrozen(offset),
        SpendError::NotFrozen { offset } => Outcome::NotFrozen(offset),
        other => Outcome::Unexpected(format!("{other:?}")),
    }
}

impl Model {
    /// Predict the outcome of `op` AND apply its effect when successful.
    /// The prediction mirrors the engine's validation order exactly.
    fn apply(&mut self, op: &Op) -> Outcome {
        match *op {
            Op::Create { tx, utxo_count } => {
                if self.txs.contains_key(&tx) {
                    return Outcome::DuplicateTxId;
                }
                self.txs.insert(
                    tx,
                    ModelTx {
                        utxo_count,
                        slots: vec![SlotState::Unspent; utxo_count as usize],
                        spent: 0,
                        mined: Vec::new(),
                        conflicting: false,
                        locked: false,
                    },
                );
                Outcome::Ok
            }

            Op::Spend { tx, vout, spender } => {
                let Some(rec) = self.txs.get_mut(&tx) else {
                    return Outcome::TxNotFound;
                };
                // Engine order: conflicting, locked, offset bounds, slot.
                if rec.conflicting {
                    return Outcome::Conflicting;
                }
                if rec.locked {
                    return Outcome::Locked;
                }
                if vout >= rec.utxo_count {
                    return Outcome::UtxoNotFound(vout as u32);
                }
                let sd = spending_data(spender, vout);
                match rec.slots[vout as usize].clone() {
                    SlotState::Unspent => {
                        rec.slots[vout as usize] = SlotState::Spent(sd);
                        rec.spent += 1;
                        Outcome::OkBlockIds(sorted(rec.mined.clone()))
                    }
                    SlotState::Spent(cur) if cur == sd => {
                        // Idempotent re-spend: true no-op, no counter bump.
                        Outcome::OkBlockIds(sorted(rec.mined.clone()))
                    }
                    SlotState::Spent(cur) => Outcome::AlreadySpent(vout as u32, cur),
                    SlotState::Frozen => Outcome::Frozen(vout as u32),
                }
            }

            Op::Unspend { tx, vout, spender } => {
                let Some(rec) = self.txs.get_mut(&tx) else {
                    return Outcome::TxNotFound;
                };
                if vout >= rec.utxo_count {
                    return Outcome::UtxoNotFound(vout as u32);
                }
                let sd = spending_data(spender, vout);
                match rec.slots[vout as usize].clone() {
                    // Owned spend (stored == expected, not frozen): clear the
                    // slot and decrement the counter.
                    SlotState::Spent(cur) if cur == sd => {
                        rec.slots[vout as usize] = SlotState::Unspent;
                        rec.spent -= 1;
                        Outcome::Ok
                    }
                    // LP-1 / teranode.lua: every non-ownership case is a silent
                    // no-op success — already unspent, wrong spending_data
                    // (caller doesn't own the spend), or frozen (the all-0xFF
                    // marker is never owned). Nothing mutates.
                    SlotState::Unspent | SlotState::Spent(_) | SlotState::Frozen => Outcome::Ok,
                }
            }

            Op::Freeze { tx, vout } => {
                let Some(rec) = self.txs.get_mut(&tx) else {
                    return Outcome::TxNotFound;
                };
                if vout >= rec.utxo_count {
                    return Outcome::UtxoNotFound(vout as u32);
                }
                match rec.slots[vout as usize].clone() {
                    SlotState::Frozen => Outcome::AlreadyFrozen(vout as u32),
                    SlotState::Spent(cur) => Outcome::AlreadySpent(vout as u32, cur),
                    SlotState::Unspent => {
                        rec.slots[vout as usize] = SlotState::Frozen;
                        Outcome::Ok
                    }
                }
            }

            Op::Unfreeze { tx, vout } => {
                let Some(rec) = self.txs.get_mut(&tx) else {
                    return Outcome::TxNotFound;
                };
                if vout >= rec.utxo_count {
                    return Outcome::UtxoNotFound(vout as u32);
                }
                if rec.slots[vout as usize] != SlotState::Frozen {
                    return Outcome::NotFrozen(vout as u32);
                }
                rec.slots[vout as usize] = SlotState::Unspent;
                Outcome::Ok
            }

            Op::SetMined { tx, block } => {
                let Some(rec) = self.txs.get_mut(&tx) else {
                    return Outcome::TxNotFound;
                };
                let id = block_id(block);
                if !rec.mined.contains(&id) {
                    rec.mined.push(id);
                }
                rec.locked = false; // set_mined clears LOCKED
                Outcome::OkBlockIds(sorted(rec.mined.clone()))
            }

            Op::SetConflicting { tx, value } => {
                let Some(rec) = self.txs.get_mut(&tx) else {
                    return Outcome::TxNotFound;
                };
                rec.conflicting = value;
                Outcome::Ok
            }

            Op::SetLocked { tx, value } => {
                let Some(rec) = self.txs.get_mut(&tx) else {
                    return Outcome::TxNotFound;
                };
                rec.locked = value;
                Outcome::Ok
            }

            Op::Delete { tx } => {
                if self.txs.remove(&tx).is_none() {
                    return Outcome::TxNotFound;
                }
                Outcome::Ok
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Engine driver
// ---------------------------------------------------------------------------

/// Run `op` against the real engine and map the result to an [`Outcome`].
fn run_engine(engine: &Engine, op: &Op) -> Outcome {
    match *op {
        Op::Create { tx, utxo_count } => {
            let hashes: Vec<[u8; 32]> = (0..utxo_count).map(|v| utxo_hash(tx, v)).collect();
            let req = CreateRequest {
                tx_id: txid(tx),
                tx_version: 1,
                locktime: 0,
                fee: 500,
                size_in_bytes: 250,
                extended_size: 0,
                is_coinbase: false,
                spending_height: 0,
                utxo_hashes: &hashes,
                inputs: None,
                outputs: None,
                inpoints: None,
                is_external: false,
                created_at: 1710000000000,
                block_height: CREATE_BLOCK_HEIGHT,
                mined_block_infos: &[],
                frozen: false,
                conflicting: false,
                locked: false,
                external_ref: None,
                parent_txids: &[],
            };
            match engine.create(&req) {
                Ok(resp) => {
                    // Sanity: the engine must report the requested slot count.
                    if resp.utxo_count != utxo_count as u32 {
                        return Outcome::Unexpected(format!(
                            "create returned utxo_count {} for request of {}",
                            resp.utxo_count, utxo_count
                        ));
                    }
                    Outcome::Ok
                }
                Err(CreateError::DuplicateTxId) => Outcome::DuplicateTxId,
                Err(e) => Outcome::Unexpected(format!("{e:?}")),
            }
        }

        Op::Spend { tx, vout, spender } => {
            let req = SpendRequest {
                tx_key: tx_key(tx),
                offset: vout as u32,
                utxo_hash: utxo_hash(tx, vout),
                spending_data: spending_data(spender, vout),
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: CURRENT_BLOCK_HEIGHT,
                block_height_retention: RETENTION,
            };
            match engine.spend(&req) {
                Ok(resp) => Outcome::OkBlockIds(sorted(resp.block_ids)),
                Err(e) => spend_error_outcome(e),
            }
        }

        Op::Unspend { tx, vout, spender } => {
            let req = UnspendRequest {
                tx_key: tx_key(tx),
                offset: vout as u32,
                utxo_hash: utxo_hash(tx, vout),
                spending_data: spending_data(spender, vout),
                current_block_height: CURRENT_BLOCK_HEIGHT,
                block_height_retention: RETENTION,
            };
            match engine.unspend(&req) {
                Ok(_) => Outcome::Ok,
                Err(e) => spend_error_outcome(e),
            }
        }

        Op::Freeze { tx, vout } => {
            let req = FreezeRequest {
                tx_key: tx_key(tx),
                offset: vout as u32,
                utxo_hash: utxo_hash(tx, vout),
            };
            match engine.freeze(&req) {
                Ok(_) => Outcome::Ok,
                Err(e) => spend_error_outcome(e),
            }
        }

        Op::Unfreeze { tx, vout } => {
            let req = UnfreezeRequest {
                tx_key: tx_key(tx),
                offset: vout as u32,
                utxo_hash: utxo_hash(tx, vout),
            };
            match engine.unfreeze(&req) {
                Ok(_) => Outcome::Ok,
                Err(e) => spend_error_outcome(e),
            }
        }

        Op::SetMined { tx, block } => {
            let req = SetMinedRequest {
                tx_key: tx_key(tx),
                block_id: block_id(block),
                block_height: CURRENT_BLOCK_HEIGHT,
                subtree_idx: 0,
                current_block_height: CURRENT_BLOCK_HEIGHT,
                block_height_retention: RETENTION,
                on_longest_chain: true,
                unset_mined: false,
            };
            match engine.set_mined(&req) {
                Ok(resp) => Outcome::OkBlockIds(sorted(resp.block_ids)),
                Err(e) => spend_error_outcome(e),
            }
        }

        Op::SetConflicting { tx, value } => {
            let req = SetConflictingRequest {
                tx_key: tx_key(tx),
                value,
                current_block_height: CURRENT_BLOCK_HEIGHT,
                block_height_retention: RETENTION,
            };
            match engine.set_conflicting(&req) {
                Ok(_) => Outcome::Ok,
                Err(e) => spend_error_outcome(e),
            }
        }

        Op::SetLocked { tx, value } => {
            let req = SetLockedRequest {
                tx_key: tx_key(tx),
                value,
            };
            match engine.set_locked_idempotent(&req) {
                Ok(_) => Outcome::Ok,
                Err(e) => spend_error_outcome(e),
            }
        }

        Op::Delete { tx } => {
            let req = DeleteRequest { tx_key: tx_key(tx) };
            match engine.delete(&req) {
                Ok(()) => Outcome::Ok,
                Err(e) => spend_error_outcome(e),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Full-state equivalence check
// ---------------------------------------------------------------------------

fn verify_full_state(engine: &Engine, model: &Model) -> Result<(), TestCaseError> {
    // Records the model says exist must match field-for-field.
    for (&tx, rec) in &model.txs {
        let key = tx_key(tx);
        let meta = match engine.read_metadata(&key) {
            Ok(m) => m,
            Err(e) => {
                return Err(TestCaseError::fail(format!(
                    "tx {tx}: expected to exist, read_metadata failed: {e:?}"
                )));
            }
        };

        // spent_utxos == model spent count == number of spent slots.
        let spent_slots = rec
            .slots
            .iter()
            .filter(|s| matches!(s, SlotState::Spent(_)))
            .count() as u32;
        prop_assert_eq!(
            rec.spent,
            spent_slots,
            "model self-inconsistency on tx {}",
            tx
        );
        prop_assert_eq!(
            { meta.spent_utxos },
            rec.spent,
            "tx {}: spent_utxos mismatch",
            tx
        );
        prop_assert_eq!(
            { meta.utxo_count },
            rec.utxo_count as u32,
            "tx {}: utxo_count mismatch",
            tx
        );
        prop_assert_eq!(
            meta.flags.contains(TxFlags::CONFLICTING),
            rec.conflicting,
            "tx {}: CONFLICTING flag mismatch",
            tx
        );
        prop_assert_eq!(
            meta.flags.contains(TxFlags::LOCKED),
            rec.locked,
            "tx {}: LOCKED flag mismatch",
            tx
        );

        // Mined block ids (always inline: BLOCK_SPACE <= 3).
        prop_assert_eq!(
            meta.block_entry_count as usize,
            rec.mined.len(),
            "tx {}: block_entry_count mismatch",
            tx
        );
        let inline = { meta.block_entries_inline };
        let engine_blocks: Vec<u32> = (0..meta.block_entry_count as usize)
            .map(|i| inline[i].block_id)
            .collect();
        prop_assert_eq!(
            sorted(engine_blocks),
            sorted(rec.mined.clone()),
            "tx {}: mined block-id set mismatch",
            tx
        );

        // Per-slot status, hash, and spending_data.
        for (vout, slot_state) in rec.slots.iter().enumerate() {
            let slot = engine
                .read_slot(&key, vout as u32)
                .map_err(|e| TestCaseError::fail(format!("tx {tx} slot {vout}: {e:?}")))?;
            prop_assert_eq!(
                slot.hash,
                utxo_hash(tx, vout as u8),
                "tx {} slot {}: hash mismatch",
                tx,
                vout
            );
            let (exp_status, exp_sd) = match slot_state {
                SlotState::Unspent => (UTXO_UNSPENT, [0u8; 36]),
                SlotState::Spent(sd) => (UTXO_SPENT, *sd),
                SlotState::Frozen => (UTXO_FROZEN, [FROZEN_BYTE; 36]),
            };
            prop_assert_eq!(
                slot.status,
                exp_status,
                "tx {} slot {}: status mismatch",
                tx,
                vout
            );
            prop_assert_eq!(
                slot.spending_data,
                exp_sd,
                "tx {} slot {}: spending_data mismatch",
                tx,
                vout
            );
        }
    }

    // Records the model says are deleted (or never created) must not exist.
    for tx in 0..TX_SPACE {
        if model.txs.contains_key(&tx) {
            continue;
        }
        let key = tx_key(tx);
        prop_assert!(
            engine.lookup(&key).is_none(),
            "tx {}: deleted record still present in index",
            tx
        );
        match engine.read_metadata(&key) {
            Err(SpendError::TxNotFound) => {}
            other => {
                return Err(TestCaseError::fail(format!(
                    "tx {tx}: deleted record read_metadata returned {other:?}, \
                     expected Err(TxNotFound)"
                )));
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// The property
// ---------------------------------------------------------------------------

fn run_sequence(ops: &[Op]) -> Result<(), TestCaseError> {
    let engine = make_engine();
    let mut model = Model::default();

    for (i, op) in ops.iter().enumerate() {
        let expected = model.apply(op);
        let actual = run_engine(&engine, op);
        prop_assert_eq!(
            &actual,
            &expected,
            "op {} {:?}: engine outcome diverged from model",
            i,
            op
        );
    }

    verify_full_state(&engine, &model)
}

/// Default case count, overridable with the standard `PROPTEST_CASES` env
/// var (e.g. `PROPTEST_CASES=4096 cargo test --test property_utxo`).
fn default_cases() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64)
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: default_cases(),
        ..ProptestConfig::default()
    })]

    /// UTXO conservation: random op sequences over a colliding keyspace
    /// keep engine and oracle in exact agreement, op-by-op and in final
    /// full-state comparison.
    #[test]
    fn utxo_conservation_random_op_sequences(
        ops in prop::collection::vec(op_strategy(), 30..=60)
    ) {
        run_sequence(&ops)?;
    }
}

// ---------------------------------------------------------------------------
// Deterministic regression scenarios (always run, independent of proptest
// case sampling). These pin the four headline invariants with hand-built
// sequences so a casual `cargo test` failure points at the invariant name.
// ---------------------------------------------------------------------------

#[test]
fn deterministic_spent_exactly_once_and_idempotent_respend() {
    let ops = [
        Op::Create {
            tx: 0,
            utxo_count: 2,
        },
        Op::Spend {
            tx: 0,
            vout: 0,
            spender: 0,
        },
        // Identical-data re-spend: idempotent, no counter bump.
        Op::Spend {
            tx: 0,
            vout: 0,
            spender: 0,
        },
        // Different-data re-spend: rejected with the FIRST spender's data.
        Op::Spend {
            tx: 0,
            vout: 0,
            spender: 1,
        },
    ];
    run_sequence(&ops).unwrap();

    // Direct assertion of the counter, outside the model comparison.
    let engine = make_engine();
    let mut model = Model::default();
    for op in &ops {
        model.apply(op);
        run_engine(&engine, op);
    }
    let meta = engine.read_metadata(&tx_key(0)).unwrap();
    assert_eq!({ meta.spent_utxos }, 1, "spent exactly once");
}

#[test]
fn deterministic_unspend_wrong_data_never_mutates() {
    let ops = [
        Op::Create {
            tx: 1,
            utxo_count: 1,
        },
        Op::Spend {
            tx: 1,
            vout: 0,
            spender: 2,
        },
        // Wrong spending_data: silent no-op (caller doesn't own the spend),
        // slot stays spent by spender 2.
        Op::Unspend {
            tx: 1,
            vout: 0,
            spender: 0,
        },
        // Right spending_data: slot returns to unspent.
        Op::Unspend {
            tx: 1,
            vout: 0,
            spender: 2,
        },
    ];
    run_sequence(&ops).unwrap();
}

#[test]
fn deterministic_delete_then_recreate() {
    let ops = [
        Op::Create {
            tx: 2,
            utxo_count: 3,
        },
        Op::Spend {
            tx: 2,
            vout: 1,
            spender: 0,
        },
        Op::Delete { tx: 2 },
        // Ops against the deleted record must all be TxNotFound...
        Op::Spend {
            tx: 2,
            vout: 0,
            spender: 0,
        },
        Op::SetMined { tx: 2, block: 0 },
        // ...until an explicit re-create resurrects it with fresh slots.
        Op::Create {
            tx: 2,
            utxo_count: 1,
        },
        Op::Spend {
            tx: 2,
            vout: 0,
            spender: 1,
        },
    ];
    run_sequence(&ops).unwrap();
}

#[test]
fn deterministic_freeze_blocks_spend_until_unfreeze() {
    let ops = [
        Op::Create {
            tx: 3,
            utxo_count: 1,
        },
        Op::Freeze { tx: 3, vout: 0 },
        Op::Spend {
            tx: 3,
            vout: 0,
            spender: 0,
        },
        Op::Unfreeze { tx: 3, vout: 0 },
        Op::Spend {
            tx: 3,
            vout: 0,
            spender: 0,
        },
    ];
    run_sequence(&ops).unwrap();
}
