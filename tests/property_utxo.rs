//! Property-based UTXO-conservation tests (N-03 / LMNH-16 / N-4).
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
//! Money-critical hostile-input dimensions (N-4): the generators now also
//! exercise the rejections that guard real value at the trust boundary, and
//! the model predicts each EXACTLY (error precedence matters):
//! - a deliberately WRONG `utxo_hash` on spend/unspend/freeze/unfreeze ->
//!   `UTXO_HASH_MISMATCH`, never a mutation (checked AFTER offset bounds but
//!   BEFORE any slot-state transition);
//! - COINBASE records with a maturity height -> spending before maturity is
//!   `COINBASE_IMMATURE` (record-level, after conflicting/locked, before the
//!   per-slot checks);
//! - the reassign cooldown -> after a `Reassign` stamps `spendableAfter`,
//!   spending before that height is `FROZEN_UNTIL`;
//! - the reserved all-`0xFF` sentinel as spend `spending_data` ->
//!   `ReservedSpendingData` (F-G2-002), the highest-precedence check, never a
//!   mutation.
//!
//! Crash-replay property: a random op sequence is applied, then a durability
//! checkpoint is taken (allocator `persist()` + device `sync()`), and a
//! simulated power loss (`MemoryDevice::simulate_power_loss`) reverts the
//! device to that last sync. The engine state is reconstructed through the
//! SAME cold-start recovery path production uses for an in-memory primary
//! index after an unclean shutdown (`src/server/startup.rs`):
//! `recover_or_create_allocator` -> `load_primary_index_in_memory` /
//! `rebuild_in_memory_secondaries` -> a fresh `Engine`. The recovered state
//! must equal the pre-crash model — UTXO conservation survives crash+replay.
//!
//! The checkpoint is taken at the end of the sequence (not at a mid-stream
//! prefix) on purpose: several mutating ops fsync their own device writes
//! (e.g. `delete` tombstones the metadata and syncs before freeing the
//! region), so a mid-stream checkpoint followed by more ops would leave
//! durable on-device writes whose matching allocator/index state was never
//! re-persisted. In production the redo log reconciles that window on
//! restart; this harness drives the engine directly (no dispatch-layer WAL),
//! so it pins the cleaner, sufficient invariant: a fully-checkpointed store
//! reconstructs bit-for-bit from a device scan after a power loss.
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
    DeleteRequest, FreezeRequest, ReassignRequest, SetConflictingRequest, SetLockedRequest,
    UnfreezeRequest,
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

/// Coinbase maturity height for an IMMATURE coinbase: strictly greater than
/// [`CURRENT_BLOCK_HEIGHT`], so every spend before maturity is rejected.
const COINBASE_IMMATURE_HEIGHT: u32 = CURRENT_BLOCK_HEIGHT + 100;
/// Coinbase maturity height for a MATURE coinbase: non-zero (so the engine's
/// `spending_height > 0` guard is exercised) but `<= CURRENT_BLOCK_HEIGHT`,
/// so spends are permitted.
const COINBASE_MATURE_HEIGHT: u32 = CURRENT_BLOCK_HEIGHT - 100;

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

/// A deliberately WRONG utxo hash for `(tx, vout)`: distinct from the real
/// [`utxo_hash`] in a byte the real hash never sets, so it can never collide
/// with any legitimate slot hash in the keyspace.
fn wrong_utxo_hash(tx: u8, vout: u8) -> [u8; 32] {
    let mut h = utxo_hash(tx, vout);
    h[31] = 0xEE; // real hashes leave byte 31 zero
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
    /// Create a regular (non-coinbase) record.
    Create { tx: u8, utxo_count: u8 },
    /// Create a coinbase record with a maturity height. When `immature` is
    /// true the maturity height is above [`CURRENT_BLOCK_HEIGHT`], so every
    /// spend before maturity must yield `CoinbaseImmature`.
    CreateCoinbase {
        tx: u8,
        utxo_count: u8,
        immature: bool,
    },
    /// Spend a UTXO. `wrong_hash` supplies a deliberately-wrong utxo_hash
    /// (must trigger `UtxoHashMismatch`); `sentinel` supplies the reserved
    /// all-`0xFF` spending_data (must trigger `ReservedSpendingData`).
    Spend {
        tx: u8,
        vout: u8,
        spender: u8,
        wrong_hash: bool,
        sentinel: bool,
    },
    Unspend {
        tx: u8,
        vout: u8,
        spender: u8,
        wrong_hash: bool,
    },
    Freeze {
        tx: u8,
        vout: u8,
        wrong_hash: bool,
    },
    Unfreeze {
        tx: u8,
        vout: u8,
        wrong_hash: bool,
    },
    /// Reassign a frozen UTXO, stamping a `spendable_after` cooldown. The
    /// replacement hash equals the original [`utxo_hash`] so the per-slot
    /// hash invariant is preserved; only the cooldown (encoded in the
    /// slot's leading spending_data bytes) changes, which is what gates the
    /// subsequent `FrozenUntil` rejection.
    Reassign {
        tx: u8,
        vout: u8,
        spendable_after: u8,
    },
    SetMined { tx: u8, block: u8 },
    SetConflicting { tx: u8, value: bool },
    SetLocked { tx: u8, value: bool },
    Delete { tx: u8 },
}

fn op_strategy() -> impl Strategy<Value = Op> {
    // vout ranges over 0..=MAX_SLOTS so offsets at/above utxo_count are
    // generated, exercising the UtxoNotFound path.
    //
    // The hostile dimensions (wrong_hash 1/4, sentinel 1/5, coinbase, and
    // the Reassign cooldown op) are weighted so each money-critical
    // rejection is reached frequently under the default 64 cases — verified
    // by the `*_reachable` tests below, which assert the engine actually
    // emits each outcome.
    prop_oneof![
        3 => (0..TX_SPACE, 1..=MAX_SLOTS)
            .prop_map(|(tx, utxo_count)| Op::Create { tx, utxo_count }),
        2 => (0..TX_SPACE, 1..=MAX_SLOTS, any::<bool>())
            .prop_map(|(tx, utxo_count, immature)| Op::CreateCoinbase {
                tx,
                utxo_count,
                immature,
            }),
        6 => (
            0..TX_SPACE,
            0..=MAX_SLOTS,
            0..SPENDER_SPACE,
            prop::bool::weighted(0.25),
            prop::bool::weighted(0.2),
        )
            .prop_map(|(tx, vout, spender, wrong_hash, sentinel)| Op::Spend {
                tx,
                vout,
                spender,
                wrong_hash,
                sentinel,
            }),
        3 => (
            0..TX_SPACE,
            0..=MAX_SLOTS,
            0..SPENDER_SPACE,
            prop::bool::weighted(0.25),
        )
            .prop_map(|(tx, vout, spender, wrong_hash)| Op::Unspend {
                tx,
                vout,
                spender,
                wrong_hash,
            }),
        4 => (0..TX_SPACE, 0..=MAX_SLOTS, prop::bool::weighted(0.25))
            .prop_map(|(tx, vout, wrong_hash)| Op::Freeze { tx, vout, wrong_hash }),
        2 => (0..TX_SPACE, 0..=MAX_SLOTS, prop::bool::weighted(0.25))
            .prop_map(|(tx, vout, wrong_hash)| Op::Unfreeze { tx, vout, wrong_hash }),
        // Reassign with a positive `spendable_after` (1..=2) so a successful
        // reassign always stamps a live cooldown — the only way a later spend
        // of that slot reaches the FrozenUntil rejection. Weighted high (and
        // freeze likewise) because the FrozenUntil path needs the rare
        // Create -> Freeze -> Reassign -> Spend conjunction on one slot.
        4 => (0..TX_SPACE, 0..=MAX_SLOTS, 1..=2u8)
            .prop_map(|(tx, vout, spendable_after)| Op::Reassign {
                tx,
                vout,
                spendable_after,
            }),
        2 => (0..TX_SPACE, 0..BLOCK_SPACE).prop_map(|(tx, block)| Op::SetMined { tx, block }),
        1 => (0..TX_SPACE, any::<bool>())
            .prop_map(|(tx, value)| Op::SetConflicting { tx, value }),
        1 => (0..TX_SPACE, any::<bool>()).prop_map(|(tx, value)| Op::SetLocked { tx, value }),
        1 => (0..TX_SPACE).prop_map(|tx| Op::Delete { tx }),
    ]
}

/// A single generation step expands to one or more ops. Most steps are a
/// single op from [`op_strategy`]; a small fraction emit a deterministic
/// "cooldown probe" — `Delete -> Create -> Freeze -> Reassign -> Spend` on one
/// slot — which reliably manufactures the rare `Create -> Freeze -> Reassign
/// -> Spend` conjunction so the `FrozenUntil` rejection is exercised every
/// run, not just when random chaining happens to line it up. The leading
/// `Delete` guarantees a fresh record regardless of prior state, so the
/// `Freeze` lands on an unspent slot and the `Reassign` on a frozen one.
fn step_strategy() -> impl Strategy<Value = Vec<Op>> {
    prop_oneof![
        9 => op_strategy().prop_map(|op| vec![op]),
        1 => (0..TX_SPACE, 0..MAX_SLOTS, 1..=2u8).prop_map(|(tx, vout, after)| {
            vec![
                Op::Delete { tx },
                Op::Create {
                    tx,
                    utxo_count: MAX_SLOTS,
                },
                Op::Freeze {
                    tx,
                    vout,
                    wrong_hash: false,
                },
                Op::Reassign {
                    tx,
                    vout,
                    spendable_after: after,
                },
                Op::Spend {
                    tx,
                    vout,
                    spender: 0,
                    wrong_hash: false,
                    sentinel: false,
                },
            ]
        }),
    ]
}

/// Build a flattened op sequence of roughly `min..=max` steps. Because some
/// steps expand to a 5-op probe, the realized op count can exceed `max`; that
/// is fine — longer sequences only deepen the collision coverage.
fn seq_strategy(min: usize, max: usize) -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(step_strategy(), min..=max)
        .prop_map(|steps| steps.into_iter().flatten().collect())
}

// ---------------------------------------------------------------------------
// Oracle model
// ---------------------------------------------------------------------------

/// Per-slot state in the oracle. `Unspent` carries the reassign cooldown
/// (`spendable_height`, 0 = no cooldown) so the model can both predict
/// `FrozenUntil` and reproduce the on-device `spending_data` bytes (the
/// little-endian cooldown height) during final-state comparison.
#[derive(Debug, Clone, PartialEq)]
enum SlotState {
    Unspent { spendable_height: u32 },
    Spent([u8; 36]),
    /// LP-4: a frozen slot preserves any reassign cooldown (the
    /// `spendable_height` it had while unspent) so a freeze/unfreeze cycle
    /// cannot wipe it. `cooldown == 0` is an ordinary frozen slot whose
    /// on-device representation is the all-`0xFF` marker.
    Frozen { cooldown: u32 },
}

impl SlotState {
    fn unspent() -> Self {
        SlotState::Unspent {
            spendable_height: 0,
        }
    }
}

#[derive(Debug, Clone)]
struct ModelTx {
    utxo_count: u8,
    slots: Vec<SlotState>,
    spent: u32,
    mined: Vec<u32>, // distinct block ids, insertion order
    conflicting: bool,
    locked: bool,
    /// Coinbase maturity height, or 0 for a non-coinbase record. Spending is
    /// rejected with `CoinbaseImmature` while `coinbase_height > current`.
    coinbase_height: u32,
}

#[derive(Debug, Default, Clone)]
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
    UtxoHashMismatch(u32),
    CoinbaseImmature {
        spending_height: u32,
        current_height: u32,
    },
    FrozenUntil {
        offset: u32,
        spendable_at_height: u32,
    },
    ReservedSpendingData(u32),
    Frozen(u32),
    AlreadySpent(u32, [u8; 36]),
    NotFrozen(u32),
    AlreadyFrozen(u32),
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
        SpendError::UtxoHashMismatch { offset } => Outcome::UtxoHashMismatch(offset),
        SpendError::CoinbaseImmature {
            spending_height,
            current_height,
        } => Outcome::CoinbaseImmature {
            spending_height,
            current_height,
        },
        SpendError::FrozenUntil {
            offset,
            spendable_at_height,
        } => Outcome::FrozenUntil {
            offset,
            spendable_at_height,
        },
        SpendError::ReservedSpendingData { offset } => Outcome::ReservedSpendingData(offset),
        SpendError::Frozen { offset } => Outcome::Frozen(offset),
        SpendError::AlreadySpent {
            offset,
            spending_data,
        } => Outcome::AlreadySpent(offset, spending_data),
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
            Op::Create { tx, utxo_count } => self.create(tx, utxo_count, 0),

            Op::CreateCoinbase {
                tx,
                utxo_count,
                immature,
            } => {
                let height = if immature {
                    COINBASE_IMMATURE_HEIGHT
                } else {
                    COINBASE_MATURE_HEIGHT
                };
                self.create(tx, utxo_count, height)
            }

            Op::Spend {
                tx,
                vout,
                spender,
                wrong_hash,
                sentinel,
            } => {
                // Precedence (single-spend fast path, src/ops/engine.rs
                // `Engine::spend`): the reserved-sentinel guard runs FIRST,
                // before the index lookup — so it fires even for a
                // non-existent tx.
                if sentinel {
                    return Outcome::ReservedSpendingData(vout as u32);
                }
                let Some(rec) = self.txs.get_mut(&tx) else {
                    return Outcome::TxNotFound;
                };
                // Record-level: conflicting, locked, coinbase maturity.
                if rec.conflicting {
                    return Outcome::Conflicting;
                }
                if rec.locked {
                    return Outcome::Locked;
                }
                // Engine guard: `spending_height > 0 && spending_height >
                // current`. `coinbase_height == 0` marks a non-coinbase
                // record here, and the const current height (2000) is
                // positive, so `> CURRENT_BLOCK_HEIGHT` subsumes the `> 0`
                // term (clippy flags the redundant pair).
                if rec.coinbase_height > CURRENT_BLOCK_HEIGHT {
                    return Outcome::CoinbaseImmature {
                        spending_height: rec.coinbase_height,
                        current_height: CURRENT_BLOCK_HEIGHT,
                    };
                }
                // Offset bounds before hash before slot-state.
                if vout >= rec.utxo_count {
                    return Outcome::UtxoNotFound(vout as u32);
                }
                if wrong_hash {
                    return Outcome::UtxoHashMismatch(vout as u32);
                }
                let sd = spending_data(spender, vout);
                match rec.slots[vout as usize].clone() {
                    SlotState::Unspent { spendable_height } => {
                        // Cooldown (reassign `spendableAfter`): half-open
                        // `[0, spendable_height)`; spendable exactly AT the
                        // unlock height.
                        if spendable_height != 0 && spendable_height > CURRENT_BLOCK_HEIGHT {
                            return Outcome::FrozenUntil {
                                offset: vout as u32,
                                spendable_at_height: spendable_height,
                            };
                        }
                        rec.slots[vout as usize] = SlotState::Spent(sd);
                        rec.spent += 1;
                        Outcome::OkBlockIds(sorted(rec.mined.clone()))
                    }
                    SlotState::Spent(cur) if cur == sd => {
                        // Idempotent re-spend: true no-op, no counter bump.
                        Outcome::OkBlockIds(sorted(rec.mined.clone()))
                    }
                    SlotState::Spent(cur) => Outcome::AlreadySpent(vout as u32, cur),
                    SlotState::Frozen { .. } => Outcome::Frozen(vout as u32),
                }
            }

            Op::Unspend {
                tx,
                vout,
                spender,
                wrong_hash,
            } => {
                let Some(rec) = self.txs.get_mut(&tx) else {
                    return Outcome::TxNotFound;
                };
                // Engine order (src/ops/engine.rs `Engine::unspend`): offset
                // bounds, THEN hash mismatch (a wrong hash is a hard error,
                // NOT the silent no-op), THEN the ownership decision.
                if vout >= rec.utxo_count {
                    return Outcome::UtxoNotFound(vout as u32);
                }
                if wrong_hash {
                    return Outcome::UtxoHashMismatch(vout as u32);
                }
                let sd = spending_data(spender, vout);
                match rec.slots[vout as usize].clone() {
                    // Owned spend (stored == expected, not frozen): clear the
                    // slot and decrement the counter.
                    SlotState::Spent(cur) if cur == sd => {
                        rec.slots[vout as usize] = SlotState::unspent();
                        rec.spent -= 1;
                        Outcome::Ok
                    }
                    // LP-1 / teranode.lua: every non-ownership case is a silent
                    // no-op success — already unspent, wrong spending_data
                    // (caller doesn't own the spend), or frozen (the all-0xFF
                    // marker is never owned). Nothing mutates.
                    SlotState::Unspent { .. } | SlotState::Spent(_) | SlotState::Frozen { .. } => {
                        Outcome::Ok
                    }
                }
            }

            Op::Freeze {
                tx,
                vout,
                wrong_hash,
            } => {
                let Some(rec) = self.txs.get_mut(&tx) else {
                    return Outcome::TxNotFound;
                };
                if vout >= rec.utxo_count {
                    return Outcome::UtxoNotFound(vout as u32);
                }
                if wrong_hash {
                    return Outcome::UtxoHashMismatch(vout as u32);
                }
                match rec.slots[vout as usize].clone() {
                    SlotState::Frozen { .. } => Outcome::AlreadyFrozen(vout as u32),
                    SlotState::Spent(cur) => Outcome::AlreadySpent(vout as u32, cur),
                    SlotState::Unspent { spendable_height } => {
                        // LP-4: freeze PRESERVES any reassign cooldown instead
                        // of discarding it (it survives in spending_data[0..4]
                        // through the frozen marker).
                        rec.slots[vout as usize] = SlotState::Frozen {
                            cooldown: spendable_height,
                        };
                        Outcome::Ok
                    }
                }
            }

            Op::Unfreeze {
                tx,
                vout,
                wrong_hash,
            } => {
                let Some(rec) = self.txs.get_mut(&tx) else {
                    return Outcome::TxNotFound;
                };
                if vout >= rec.utxo_count {
                    return Outcome::UtxoNotFound(vout as u32);
                }
                if wrong_hash {
                    return Outcome::UtxoHashMismatch(vout as u32);
                }
                let cooldown = match rec.slots[vout as usize] {
                    SlotState::Frozen { cooldown } => cooldown,
                    _ => return Outcome::NotFrozen(vout as u32),
                };
                // LP-4: unfreeze RESTORES the preserved cooldown rather than
                // zeroing it. A cooldown of 0 restores to immediately
                // spendable (matching a plain all-0xFF frozen slot).
                rec.slots[vout as usize] = SlotState::Unspent {
                    spendable_height: cooldown,
                };
                Outcome::Ok
            }

            Op::Reassign {
                tx,
                vout,
                spendable_after,
            } => {
                let Some(rec) = self.txs.get_mut(&tx) else {
                    return Outcome::TxNotFound;
                };
                // Engine order (src/ops/engine.rs `Engine::reassign`): offset
                // bounds, conflicting, locked, coinbase maturity (using the
                // request block_height as "current"), hash mismatch, then
                // NotFrozen.
                if vout >= rec.utxo_count {
                    return Outcome::UtxoNotFound(vout as u32);
                }
                if rec.conflicting {
                    return Outcome::Conflicting;
                }
                if rec.locked {
                    return Outcome::Locked;
                }
                // Engine guard: `spending_height > 0 && spending_height >
                // current`. `coinbase_height == 0` marks a non-coinbase
                // record here, and the const current height (2000) is
                // positive, so `> CURRENT_BLOCK_HEIGHT` subsumes the `> 0`
                // term (clippy flags the redundant pair).
                if rec.coinbase_height > CURRENT_BLOCK_HEIGHT {
                    return Outcome::CoinbaseImmature {
                        spending_height: rec.coinbase_height,
                        current_height: CURRENT_BLOCK_HEIGHT,
                    };
                }
                // The driver always supplies the correct (current) hash for
                // reassign, so no hash-mismatch case here.
                if !matches!(rec.slots[vout as usize], SlotState::Frozen { .. }) {
                    return Outcome::NotFrozen(vout as u32);
                }
                // spendable_height = block_height + spendable_after; the
                // driver uses block_height == CURRENT_BLOCK_HEIGHT, and the
                // small `spendable_after` range cannot overflow u32.
                let spendable_height = CURRENT_BLOCK_HEIGHT + spendable_after as u32;
                rec.slots[vout as usize] = SlotState::Unspent { spendable_height };
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

    /// Shared create logic for both regular and coinbase records.
    /// `coinbase_height` is 0 for a non-coinbase record.
    fn create(&mut self, tx: u8, utxo_count: u8, coinbase_height: u32) -> Outcome {
        if self.txs.contains_key(&tx) {
            return Outcome::DuplicateTxId;
        }
        self.txs.insert(
            tx,
            ModelTx {
                utxo_count,
                slots: vec![SlotState::unspent(); utxo_count as usize],
                spent: 0,
                mined: Vec::new(),
                conflicting: false,
                locked: false,
                coinbase_height,
            },
        );
        Outcome::Ok
    }
}

// ---------------------------------------------------------------------------
// Engine driver
// ---------------------------------------------------------------------------

/// Run `op` against the real engine and map the result to an [`Outcome`].
fn run_engine(engine: &Engine, op: &Op) -> Outcome {
    match *op {
        Op::Create { tx, utxo_count } => run_create(engine, tx, utxo_count, false, 0),

        Op::CreateCoinbase {
            tx,
            utxo_count,
            immature,
        } => {
            let height = if immature {
                COINBASE_IMMATURE_HEIGHT
            } else {
                COINBASE_MATURE_HEIGHT
            };
            run_create(engine, tx, utxo_count, true, height)
        }

        Op::Spend {
            tx,
            vout,
            spender,
            wrong_hash,
            sentinel,
        } => {
            let hash = if wrong_hash {
                wrong_utxo_hash(tx, vout)
            } else {
                utxo_hash(tx, vout)
            };
            let sd = if sentinel {
                [FROZEN_BYTE; 36]
            } else {
                spending_data(spender, vout)
            };
            let req = SpendRequest {
                tx_key: tx_key(tx),
                offset: vout as u32,
                utxo_hash: hash,
                spending_data: sd,
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

        Op::Unspend {
            tx,
            vout,
            spender,
            wrong_hash,
        } => {
            let hash = if wrong_hash {
                wrong_utxo_hash(tx, vout)
            } else {
                utxo_hash(tx, vout)
            };
            let req = UnspendRequest {
                tx_key: tx_key(tx),
                offset: vout as u32,
                utxo_hash: hash,
                spending_data: spending_data(spender, vout),
                current_block_height: CURRENT_BLOCK_HEIGHT,
                block_height_retention: RETENTION,
            };
            match engine.unspend(&req) {
                Ok(_) => Outcome::Ok,
                Err(e) => spend_error_outcome(e),
            }
        }

        Op::Freeze {
            tx,
            vout,
            wrong_hash,
        } => {
            let hash = if wrong_hash {
                wrong_utxo_hash(tx, vout)
            } else {
                utxo_hash(tx, vout)
            };
            let req = FreezeRequest {
                tx_key: tx_key(tx),
                offset: vout as u32,
                utxo_hash: hash,
            };
            match engine.freeze(&req) {
                Ok(_) => Outcome::Ok,
                Err(e) => spend_error_outcome(e),
            }
        }

        Op::Unfreeze {
            tx,
            vout,
            wrong_hash,
        } => {
            let hash = if wrong_hash {
                wrong_utxo_hash(tx, vout)
            } else {
                utxo_hash(tx, vout)
            };
            let req = UnfreezeRequest {
                tx_key: tx_key(tx),
                offset: vout as u32,
                utxo_hash: hash,
            };
            match engine.unfreeze(&req) {
                Ok(_) => Outcome::Ok,
                Err(e) => spend_error_outcome(e),
            }
        }

        Op::Reassign {
            tx,
            vout,
            spendable_after,
        } => {
            let req = ReassignRequest {
                tx_key: tx_key(tx),
                offset: vout as u32,
                // The driver always supplies the correct current hash, and
                // reassigns to the SAME hash so the per-slot hash invariant
                // holds — only the cooldown changes.
                utxo_hash: utxo_hash(tx, vout),
                new_utxo_hash: utxo_hash(tx, vout),
                block_height: CURRENT_BLOCK_HEIGHT,
                spendable_after: spendable_after as u32,
            };
            match engine.reassign(&req) {
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
            let req = DeleteRequest { tx_key: tx_key(tx), due_guard: None };
            match engine.delete(&req) {
                Ok(()) => Outcome::Ok,
                Err(e) => spend_error_outcome(e),
            }
        }
    }
}

/// Build and submit a create request (regular or coinbase).
fn run_create(engine: &Engine, tx: u8, utxo_count: u8, is_coinbase: bool, spending_height: u32) -> Outcome {
    let hashes: Vec<[u8; 32]> = (0..utxo_count).map(|v| utxo_hash(tx, v)).collect();
    let req = CreateRequest {
        tx_id: txid(tx),
        tx_version: 1,
        locktime: 0,
        fee: 500,
        size_in_bytes: 250,
        extended_size: 0,
        is_coinbase,
        spending_height,
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

// ---------------------------------------------------------------------------
// Full-state equivalence check
// ---------------------------------------------------------------------------

/// Compute the expected on-device `(status, spending_data)` for a slot state.
fn expected_slot_repr(slot_state: &SlotState) -> (u8, [u8; 36]) {
    match slot_state {
        SlotState::Unspent { spendable_height } => {
            let mut sd = [0u8; 36];
            sd[0..4].copy_from_slice(&spendable_height.to_le_bytes());
            (UTXO_UNSPENT, sd)
        }
        SlotState::Spent(sd) => (UTXO_SPENT, *sd),
        // LP-4: a frozen slot carries the preserved cooldown in the first 4
        // bytes; a zero cooldown is the plain all-`0xFF` frozen marker.
        SlotState::Frozen { cooldown } => {
            let mut sd = [FROZEN_BYTE; 36];
            if *cooldown != 0 {
                sd[0..4].copy_from_slice(&cooldown.to_le_bytes());
            }
            (UTXO_FROZEN, sd)
        }
    }
}

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
        prop_assert_eq!(
            meta.flags.contains(TxFlags::IS_COINBASE),
            rec.coinbase_height > 0,
            "tx {}: IS_COINBASE flag mismatch",
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
            let (exp_status, exp_sd) = expected_slot_repr(slot_state);
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
        ops in seq_strategy(20, 45)
    ) {
        run_sequence(&ops)?;
    }
}

/// Self-verifying generator-reachability guard: confirms the sequence
/// strategy actually produces every money-critical hostile outcome under
/// realistic sampling, so none of the new dimensions can silently rot into
/// dead generators. Runs the model (not the engine) over a deterministic
/// batch of generated sequences and fails if any target outcome is absent.
#[test]
fn hostile_outcomes_are_reachable_by_the_generator() {
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;
    let mut runner = TestRunner::deterministic();
    let strat = seq_strategy(20, 45);
    let mut counts = std::collections::BTreeMap::<&str, u32>::new();
    for _ in 0..200 {
        let tree = strat.new_tree(&mut runner).unwrap();
        let ops = tree.current();
        let mut model = Model::default();
        for op in &ops {
            let label = match model.apply(op) {
                Outcome::UtxoHashMismatch(_) => Some("UtxoHashMismatch"),
                Outcome::ReservedSpendingData(_) => Some("ReservedSpendingData"),
                Outcome::CoinbaseImmature { .. } => Some("CoinbaseImmature"),
                Outcome::FrozenUntil { .. } => Some("FrozenUntil"),
                _ => None,
            };
            if let Some(l) = label {
                *counts.entry(l).or_default() += 1;
            }
        }
    }
    for key in [
        "UtxoHashMismatch",
        "ReservedSpendingData",
        "CoinbaseImmature",
        "FrozenUntil",
    ] {
        assert!(
            counts.get(key).copied().unwrap_or(0) > 0,
            "hostile outcome {key} was never generated — generator coverage regressed: {counts:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Crash-replay property
// ---------------------------------------------------------------------------

/// A volatile-device engine plus the live device handle, so the test can
/// `sync()` / `simulate_power_loss()` outside the engine and reconstruct a
/// fresh engine from the reverted bytes.
struct VolatileHarness {
    engine: Arc<Engine>,
    device: Arc<MemoryDevice>,
}

const CRASH_DEVICE_SIZE: u64 = 8 * 1024 * 1024;
const CRASH_DEVICE_ALIGN: usize = 4096;

fn make_volatile_harness() -> VolatileHarness {
    let device = Arc::new(MemoryDevice::new_volatile(CRASH_DEVICE_SIZE, CRASH_DEVICE_ALIGN).unwrap());
    let dev_dyn: Arc<dyn BlockDevice> = device.clone();
    let alloc = SlotAllocator::new(dev_dyn.clone()).unwrap();
    let index = Index::new(4096).unwrap();
    let engine = Arc::new(Engine::new(
        dev_dyn,
        index,
        alloc,
        StripedLocks::new(64),
        DahIndex::new(),
        UnminedIndex::new(),
    ));
    VolatileHarness { engine, device }
}

/// Reconstruct an engine from the (post-power-loss) device contents using the
/// SAME cold-start path production uses for an in-memory primary index after
/// an unclean shutdown (`src/server/startup.rs`): recover the allocator from
/// its persisted header (falling back to a fresh allocator on a genuinely
/// blank device, exactly as `recover_or_create_allocator` does), then rebuild
/// the primary and secondary indexes by scanning the device.
fn rebuild_engine_from_device(device: Arc<MemoryDevice>) -> Result<Arc<Engine>, TestCaseError> {
    use teraslab::server::startup::{
        load_primary_index_in_memory, rebuild_in_memory_secondaries, recover_or_create_allocator,
    };
    let dev_dyn: Arc<dyn BlockDevice> = device.clone();
    let (alloc, _origin) = recover_or_create_allocator(dev_dyn.clone())
        .map_err(|e| TestCaseError::fail(format!("allocator recover: {e:?}")))?;
    let index = load_primary_index_in_memory(dev_dyn.as_ref(), &alloc)
        .map_err(|e| TestCaseError::fail(format!("primary rebuild: {e:?}")))?;
    let secondaries = rebuild_in_memory_secondaries(dev_dyn.as_ref(), &alloc);
    prop_assert!(
        secondaries.status.dah_ok && secondaries.status.unmined_ok,
        "secondary rebuild degraded after crash: {:?}",
        secondaries.status
    );
    Ok(Arc::new(Engine::new(
        dev_dyn,
        index,
        alloc,
        StripedLocks::new(64),
        secondaries.dah,
        secondaries.unmined,
    )))
}

/// Apply `ops`, take an end-of-sequence durability checkpoint, simulate a
/// power loss, and rebuild via the production cold-start path. The recovered
/// state must equal the pre-crash model.
fn run_crash_replay(ops: &[Op]) -> Result<(), TestCaseError> {
    let harness = make_volatile_harness();
    let mut model = Model::default();

    for (i, op) in ops.iter().enumerate() {
        // Apply op to both engine and model, asserting outcome equality so a
        // divergence is caught before the crash boundary too.
        let expected = model.apply(op);
        let actual = run_engine(&harness.engine, op);
        prop_assert_eq!(
            &actual,
            &expected,
            "crash-replay op {} {:?}: engine outcome diverged from model",
            i,
            op
        );
    }

    // Durability checkpoint: persist the allocator header (which itself
    // fsyncs), then flush any remaining device write cache. This is the
    // production checkpoint barrier — after it returns, every committed write
    // is durable in the volatile device's shadow buffer.
    harness
        .engine
        .persist_allocator()
        .map_err(|e| TestCaseError::fail(format!("persist_allocator: {e:?}")))?;
    prop_assert!(
        harness.device.sync().is_ok(),
        "device sync failed at checkpoint"
    );

    // Simulate power loss: revert the device to its last synced state. With a
    // clean end-of-sequence checkpoint this restores exactly the committed
    // image, but it still drives the real revert path (and would expose any
    // write the engine left in the volatile cache without a barrier).
    let reverted = harness.device.simulate_power_loss();
    prop_assert!(
        reverted,
        "device was not volatile — power-loss revert is a no-op"
    );

    // Drop the pre-crash engine so the rebuilt engine is the sole owner of
    // the (raw-pointer) device access path.
    drop(harness.engine);

    let recovered = rebuild_engine_from_device(harness.device.clone())?;
    verify_full_state(&recovered, &model)
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: default_cases(),
        ..ProptestConfig::default()
    })]

    /// Crash-replay conservation: a random op sequence is fully checkpointed,
    /// the device suffers a simulated power loss, and the engine is rebuilt
    /// via the production cold-start recovery path. The recovered state must
    /// equal the pre-crash model — committed UTXO state survives the crash.
    #[test]
    fn utxo_conservation_survives_crash_and_replay(
        ops in seq_strategy(8, 25)
    ) {
        run_crash_replay(&ops)?;
    }
}

// ---------------------------------------------------------------------------
// Generator-reachability guards
//
// These tests fail loudly if a money-critical hostile outcome stops being
// generated under the default case count — i.e. they make the new generator
// dimensions self-verifying rather than silently dead. Each drives the REAL
// engine and asserts the exact outcome is produced.
// ---------------------------------------------------------------------------

/// Drive a single op straight to the engine and return its outcome (no model).
fn engine_outcome(engine: &Engine, op: &Op) -> Outcome {
    run_engine(engine, op)
}

#[test]
fn wrong_hash_spend_triggers_hash_mismatch() {
    let engine = make_engine();
    engine_outcome(
        &engine,
        &Op::Create {
            tx: 0,
            utxo_count: 2,
        },
    );
    let out = engine_outcome(
        &engine,
        &Op::Spend {
            tx: 0,
            vout: 0,
            spender: 0,
            wrong_hash: true,
            sentinel: false,
        },
    );
    assert_eq!(out, Outcome::UtxoHashMismatch(0));
    // And the slot must NOT have mutated.
    let meta = engine.read_metadata(&tx_key(0)).unwrap();
    assert_eq!({ meta.spent_utxos }, 0, "wrong-hash spend must not mutate");
}

#[test]
fn sentinel_spend_triggers_reserved_spending_data() {
    let engine = make_engine();
    engine_outcome(
        &engine,
        &Op::Create {
            tx: 0,
            utxo_count: 1,
        },
    );
    let out = engine_outcome(
        &engine,
        &Op::Spend {
            tx: 0,
            vout: 0,
            spender: 0,
            wrong_hash: false,
            sentinel: true,
        },
    );
    assert_eq!(out, Outcome::ReservedSpendingData(0));
    let meta = engine.read_metadata(&tx_key(0)).unwrap();
    assert_eq!({ meta.spent_utxos }, 0, "sentinel spend must not mutate");
}

#[test]
fn immature_coinbase_spend_triggers_coinbase_immature() {
    let engine = make_engine();
    engine_outcome(
        &engine,
        &Op::CreateCoinbase {
            tx: 0,
            utxo_count: 1,
            immature: true,
        },
    );
    let out = engine_outcome(
        &engine,
        &Op::Spend {
            tx: 0,
            vout: 0,
            spender: 0,
            wrong_hash: false,
            sentinel: false,
        },
    );
    assert_eq!(
        out,
        Outcome::CoinbaseImmature {
            spending_height: COINBASE_IMMATURE_HEIGHT,
            current_height: CURRENT_BLOCK_HEIGHT,
        }
    );
    let meta = engine.read_metadata(&tx_key(0)).unwrap();
    assert_eq!({ meta.spent_utxos }, 0, "immature coinbase spend must not mutate");
}

#[test]
fn mature_coinbase_spend_succeeds() {
    let engine = make_engine();
    engine_outcome(
        &engine,
        &Op::CreateCoinbase {
            tx: 0,
            utxo_count: 1,
            immature: false,
        },
    );
    let out = engine_outcome(
        &engine,
        &Op::Spend {
            tx: 0,
            vout: 0,
            spender: 0,
            wrong_hash: false,
            sentinel: false,
        },
    );
    assert_eq!(out, Outcome::OkBlockIds(vec![]));
    let meta = engine.read_metadata(&tx_key(0)).unwrap();
    assert_eq!({ meta.spent_utxos }, 1, "mature coinbase must be spendable");
}

#[test]
fn reassign_cooldown_triggers_frozen_until() {
    let engine = make_engine();
    engine_outcome(
        &engine,
        &Op::Create {
            tx: 0,
            utxo_count: 1,
        },
    );
    // Must be frozen before reassign.
    engine_outcome(
        &engine,
        &Op::Freeze {
            tx: 0,
            vout: 0,
            wrong_hash: false,
        },
    );
    // Reassign with a positive cooldown -> spendable_height > current.
    let reassign = engine_outcome(
        &engine,
        &Op::Reassign {
            tx: 0,
            vout: 0,
            spendable_after: 2,
        },
    );
    assert_eq!(reassign, Outcome::Ok);
    // Spending before the cooldown height must be FrozenUntil.
    let out = engine_outcome(
        &engine,
        &Op::Spend {
            tx: 0,
            vout: 0,
            spender: 0,
            wrong_hash: false,
            sentinel: false,
        },
    );
    assert_eq!(
        out,
        Outcome::FrozenUntil {
            offset: 0,
            spendable_at_height: CURRENT_BLOCK_HEIGHT + 2,
        }
    );
    let meta = engine.read_metadata(&tx_key(0)).unwrap();
    assert_eq!({ meta.spent_utxos }, 0, "cooldown spend must not mutate");
}

// ---------------------------------------------------------------------------
// Deterministic regression scenarios (always run, independent of proptest
// case sampling). These pin the headline invariants with hand-built
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
            wrong_hash: false,
            sentinel: false,
        },
        // Identical-data re-spend: idempotent, no counter bump.
        Op::Spend {
            tx: 0,
            vout: 0,
            spender: 0,
            wrong_hash: false,
            sentinel: false,
        },
        // Different-data re-spend: rejected with the FIRST spender's data.
        Op::Spend {
            tx: 0,
            vout: 0,
            spender: 1,
            wrong_hash: false,
            sentinel: false,
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
            wrong_hash: false,
            sentinel: false,
        },
        // Wrong spending_data: silent no-op (caller doesn't own the spend),
        // slot stays spent by spender 2.
        Op::Unspend {
            tx: 1,
            vout: 0,
            spender: 0,
            wrong_hash: false,
        },
        // Right spending_data: slot returns to unspent.
        Op::Unspend {
            tx: 1,
            vout: 0,
            spender: 2,
            wrong_hash: false,
        },
    ];
    run_sequence(&ops).unwrap();
}

#[test]
fn deterministic_unspend_wrong_hash_is_hard_error() {
    let ops = [
        Op::Create {
            tx: 1,
            utxo_count: 1,
        },
        Op::Spend {
            tx: 1,
            vout: 0,
            spender: 2,
            wrong_hash: false,
            sentinel: false,
        },
        // Wrong HASH (not wrong data): a hard UtxoHashMismatch, NOT the
        // silent ownership no-op. Slot stays spent.
        Op::Unspend {
            tx: 1,
            vout: 0,
            spender: 2,
            wrong_hash: true,
        },
    ];
    run_sequence(&ops).unwrap();
    // Independently confirm the slot is still spent.
    let engine = make_engine();
    let mut model = Model::default();
    for op in &ops {
        model.apply(op);
        run_engine(&engine, op);
    }
    let meta = engine.read_metadata(&tx_key(1)).unwrap();
    assert_eq!({ meta.spent_utxos }, 1, "wrong-hash unspend must not unspend");
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
            wrong_hash: false,
            sentinel: false,
        },
        Op::Delete { tx: 2 },
        // Ops against the deleted record must all be TxNotFound...
        Op::Spend {
            tx: 2,
            vout: 0,
            spender: 0,
            wrong_hash: false,
            sentinel: false,
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
            wrong_hash: false,
            sentinel: false,
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
        Op::Freeze {
            tx: 3,
            vout: 0,
            wrong_hash: false,
        },
        Op::Spend {
            tx: 3,
            vout: 0,
            spender: 0,
            wrong_hash: false,
            sentinel: false,
        },
        Op::Unfreeze {
            tx: 3,
            vout: 0,
            wrong_hash: false,
        },
        Op::Spend {
            tx: 3,
            vout: 0,
            spender: 0,
            wrong_hash: false,
            sentinel: false,
        },
    ];
    run_sequence(&ops).unwrap();
}

#[test]
fn deterministic_crash_replay_round_trips_committed_state() {
    let ops = [
        Op::Create {
            tx: 0,
            utxo_count: 3,
        },
        Op::Spend {
            tx: 0,
            vout: 0,
            spender: 0,
            wrong_hash: false,
            sentinel: false,
        },
        Op::Spend {
            tx: 0,
            vout: 1,
            spender: 1,
            wrong_hash: false,
            sentinel: false,
        },
        Op::SetMined { tx: 0, block: 0 },
        Op::Spend {
            tx: 0,
            vout: 2,
            spender: 2,
            wrong_hash: false,
            sentinel: false,
        },
        // A delete that fsyncs its own tombstone, then a re-create, to make
        // sure the device-scan rebuild handles a freed-then-reused offset.
        Op::Create {
            tx: 1,
            utxo_count: 2,
        },
        Op::Delete { tx: 1 },
        Op::Create {
            tx: 2,
            utxo_count: 1,
        },
    ];
    run_crash_replay(&ops).unwrap();
}
