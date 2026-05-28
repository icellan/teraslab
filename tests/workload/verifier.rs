//! Independent in-memory state verifier for TeraSlab.
//!
//! Maintains a reference model of expected state. Every operation is applied
//! to both TeraSlab and the verifier. After the workload completes, verify
//! that they match exactly.

use std::collections::HashMap;

use teraslab::index::TxKey;
use teraslab::ops::create::*;
use teraslab::ops::engine::Engine;
use teraslab::ops::error::SpendError;
use teraslab::ops::remaining::*;
use teraslab::ops::set_mined::*;
use teraslab::ops::spend::*;
use teraslab::record::*;

use super::generator::WorkloadOp;

/// A mismatch between expected and actual state.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Mismatch {
    /// Description of the mismatch.
    pub detail: String,
}

/// In-memory expected state of a UTXO slot.
#[derive(Debug, Clone)]
struct ExpectedSlot {
    hash: [u8; 32],
    status: u8,
    spending_data: [u8; 36],
}

/// In-memory expected state of a transaction record.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct ExpectedRecord {
    utxo_count: u32,
    utxo_hashes: Vec<[u8; 32]>,
    slots: Vec<ExpectedSlot>,
    spent_utxos: u32,
    mined_block_ids: Vec<u32>,
    conflicting: bool,
    locked: bool,
    is_coinbase: bool,
    spending_height: u32,
    frozen_count: u32,
    preserve_until: u32,
}

/// Independent in-memory model of expected UTXO state.
pub struct StateVerifier {
    records: HashMap<TxKey, ExpectedRecord>,
}

impl StateVerifier {
    /// Create a new empty verifier.
    pub fn new() -> Self {
        Self {
            records: HashMap::new(),
        }
    }

    /// Apply a workload operation to both the engine and this verifier.
    ///
    /// Returns Ok(()) if the operation succeeded on the engine as expected,
    /// or an error description if something unexpected happened.
    pub fn apply(&mut self, op: &WorkloadOp, engine: &Engine) -> Result<(), String> {
        match op {
            WorkloadOp::Create {
                tx_id,
                utxo_hashes,
                is_coinbase,
                spending_height,
                is_external,
                block_height,
            } => {
                let req = CreateRequest {
                    tx_id: *tx_id,
                    tx_version: 1,
                    locktime: 0,
                    fee: 500,
                    size_in_bytes: 250,
                    extended_size: 0,
                    is_coinbase: *is_coinbase,
                    spending_height: *spending_height,
                    utxo_hashes,
                    inputs: None,
                    outputs: None,
                    inpoints: None,
                    is_external: *is_external,
                    created_at: 1710000000000,
                    block_height: *block_height,
                    mined_block_infos: &[],
                    frozen: false,
                    conflicting: false,
                    locked: false,
                    external_ref: (*is_external).then_some(ExternalRef {
                        store_type: 1,
                        content_hash: *tx_id,
                        total_size: 250,
                        input_count: 0,
                        output_count: 0,
                        inputs_offset: 0,
                        outputs_offset: 0,
                    }),
                    parent_txids: &[],
                };

                engine
                    .create(&req)
                    .map_err(|e| format!("create failed: {e}"))?;

                let key = TxKey { txid: *tx_id };
                let utxo_count = utxo_hashes.len() as u32;
                let slots = utxo_hashes
                    .iter()
                    .map(|h| ExpectedSlot {
                        hash: *h,
                        status: UTXO_UNSPENT,
                        spending_data: [0u8; 36],
                    })
                    .collect();

                self.records.insert(
                    key,
                    ExpectedRecord {
                        utxo_count,
                        utxo_hashes: utxo_hashes.clone(),
                        slots,
                        spent_utxos: 0,
                        mined_block_ids: Vec::new(),
                        conflicting: false,
                        locked: false,
                        is_coinbase: *is_coinbase,
                        spending_height: *spending_height,
                        frozen_count: 0,
                        preserve_until: 0,
                    },
                );
                Ok(())
            }

            WorkloadOp::Spend {
                tx_key,
                offset,
                utxo_hash,
                spending_data,
                current_block_height,
            } => {
                let req = SpendRequest {
                    tx_key: *tx_key,
                    offset: *offset,
                    utxo_hash: *utxo_hash,
                    spending_data: *spending_data,
                    ignore_conflicting: false,
                    ignore_locked: false,
                    current_block_height: *current_block_height,
                    block_height_retention: 288,
                };

                match engine.spend(&req) {
                    Ok(_) => {
                        if let Some(rec) = self.records.get_mut(tx_key) {
                            let slot = &mut rec.slots[*offset as usize];
                            if slot.status == UTXO_UNSPENT {
                                slot.status = UTXO_SPENT;
                                slot.spending_data = *spending_data;
                                rec.spent_utxos += 1;
                            }
                        }
                        Ok(())
                    }
                    Err(e) => {
                        // Certain errors are expected and acceptable
                        match &e {
                            SpendError::Conflicting
                            | SpendError::Locked
                            | SpendError::CoinbaseImmature { .. }
                            | SpendError::AlreadySpent { .. }
                            | SpendError::Frozen { .. }
                            | SpendError::FrozenUntil { .. } => Ok(()),
                            _ => Err(format!("unexpected spend error: {e}")),
                        }
                    }
                }
            }

            WorkloadOp::SpendMulti {
                tx_key,
                items,
                current_block_height,
            } => {
                let spends: Vec<SpendItem> = items
                    .iter()
                    .enumerate()
                    .map(|(i, (offset, hash, sd))| SpendItem {
                        offset: *offset,
                        utxo_hash: *hash,
                        spending_data: *sd,
                        idx: i as u32,
                    })
                    .collect();

                let req = SpendMultiRequest {
                    tx_key: *tx_key,
                    spends,
                    ignore_conflicting: false,
                    ignore_locked: false,
                    current_block_height: *current_block_height,
                    block_height_retention: 288,
                };

                match engine.spend_multi(&req) {
                    Ok(resp) => {
                        if let Some(rec) = self.records.get_mut(tx_key) {
                            for (i, (offset, _, sd)) in items.iter().enumerate() {
                                if !resp.errors.contains_key(&(i as u32)) {
                                    let slot = &mut rec.slots[*offset as usize];
                                    if slot.status == UTXO_UNSPENT {
                                        slot.status = UTXO_SPENT;
                                        slot.spending_data = *sd;
                                        rec.spent_utxos += 1;
                                    }
                                }
                            }
                        }
                        Ok(())
                    }
                    Err(e) => match &e {
                        SpendError::Conflicting
                        | SpendError::Locked
                        | SpendError::CoinbaseImmature { .. } => Ok(()),
                        _ => Err(format!("unexpected spend_multi error: {e}")),
                    },
                }
            }

            WorkloadOp::SetMined {
                tx_key,
                block_id,
                block_height,
                current_block_height,
            } => {
                let req = SetMinedRequest {
                    tx_key: *tx_key,
                    block_id: *block_id,
                    block_height: *block_height,
                    subtree_idx: 0,
                    current_block_height: *current_block_height,
                    block_height_retention: 288,
                    on_longest_chain: true,
                    unset_mined: false,
                };

                engine
                    .set_mined(&req)
                    .map_err(|e| format!("set_mined failed: {e}"))?;

                if let Some(rec) = self.records.get_mut(tx_key) {
                    if !rec.mined_block_ids.contains(block_id) {
                        rec.mined_block_ids.push(*block_id);
                    }
                    rec.locked = false; // setMined clears lock
                }
                Ok(())
            }

            WorkloadOp::UnsetMined {
                tx_key,
                block_id,
                block_height,
                current_block_height,
            } => {
                let req = SetMinedRequest {
                    tx_key: *tx_key,
                    block_id: *block_id,
                    block_height: *block_height,
                    subtree_idx: 0,
                    current_block_height: *current_block_height,
                    block_height_retention: 288,
                    on_longest_chain: true,
                    unset_mined: true,
                };

                engine
                    .set_mined(&req)
                    .map_err(|e| format!("unset_mined failed: {e}"))?;

                if let Some(rec) = self.records.get_mut(tx_key) {
                    rec.mined_block_ids.retain(|&id| id != *block_id);
                }
                Ok(())
            }

            WorkloadOp::ReadMetadata { tx_key } => {
                // Reads don't modify state, just verify the tx exists
                match engine.read_metadata(tx_key) {
                    Ok(_) => Ok(()),
                    Err(SpendError::TxNotFound) => {
                        if self.records.contains_key(tx_key) {
                            Err(format!("tx {:?} expected to exist but not found", tx_key))
                        } else {
                            Ok(())
                        }
                    }
                    Err(e) => Err(format!("read_metadata error: {e}")),
                }
            }

            WorkloadOp::ReadSlot { tx_key, offset } => match engine.read_slot(tx_key, *offset) {
                Ok(_) => Ok(()),
                Err(SpendError::TxNotFound) => {
                    if self.records.contains_key(tx_key) {
                        Err(format!("tx {:?} expected to exist but not found", tx_key))
                    } else {
                        Ok(())
                    }
                }
                Err(e) => Err(format!("read_slot error: {e}")),
            },

            WorkloadOp::Freeze {
                tx_key,
                offset,
                utxo_hash,
            } => {
                let req = FreezeRequest {
                    tx_key: *tx_key,
                    offset: *offset,
                    utxo_hash: *utxo_hash,
                };

                match engine.freeze(&req) {
                    Ok(_) => {
                        if let Some(rec) = self.records.get_mut(tx_key) {
                            let slot = &mut rec.slots[*offset as usize];
                            slot.status = UTXO_FROZEN;
                            slot.spending_data = [FROZEN_BYTE; 36];
                            rec.frozen_count += 1;
                        }
                        Ok(())
                    }
                    Err(SpendError::AlreadyFrozen { .. }) => Ok(()),
                    Err(e) => Err(format!("freeze error: {e}")),
                }
            }

            WorkloadOp::Unfreeze {
                tx_key,
                offset,
                utxo_hash,
            } => {
                let req = UnfreezeRequest {
                    tx_key: *tx_key,
                    offset: *offset,
                    utxo_hash: *utxo_hash,
                };

                match engine.unfreeze(&req) {
                    Ok(_) => {
                        if let Some(rec) = self.records.get_mut(tx_key) {
                            let slot = &mut rec.slots[*offset as usize];
                            slot.status = UTXO_UNSPENT;
                            slot.spending_data = [0u8; 36];
                            if rec.frozen_count > 0 {
                                rec.frozen_count -= 1;
                            }
                        }
                        Ok(())
                    }
                    Err(SpendError::NotFrozen { .. }) => Ok(()),
                    Err(e) => Err(format!("unfreeze error: {e}")),
                }
            }

            WorkloadOp::SetConflicting {
                tx_key,
                value,
                current_block_height,
            } => {
                let req = SetConflictingRequest {
                    tx_key: *tx_key,
                    value: *value,
                    current_block_height: *current_block_height,
                    block_height_retention: 288,
                };

                engine
                    .set_conflicting(&req)
                    .map_err(|e| format!("set_conflicting failed: {e}"))?;

                if let Some(rec) = self.records.get_mut(tx_key) {
                    rec.conflicting = *value;
                }
                Ok(())
            }

            WorkloadOp::SetLocked { tx_key, value } => {
                let req = SetLockedRequest {
                    tx_key: *tx_key,
                    value: *value,
                };

                engine
                    .set_locked_idempotent(&req)
                    .map_err(|e| format!("set_locked failed: {e}"))?;

                if let Some(rec) = self.records.get_mut(tx_key) {
                    rec.locked = *value;
                }
                Ok(())
            }

            WorkloadOp::Delete { tx_key } => {
                let req = DeleteRequest { tx_key: *tx_key };

                engine
                    .delete(&req)
                    .map_err(|e| format!("delete failed: {e}"))?;

                self.records.remove(tx_key);
                Ok(())
            }

            WorkloadOp::PreserveUntil {
                tx_key,
                block_height,
            } => {
                let req = PreserveUntilRequest {
                    tx_key: *tx_key,
                    block_height: *block_height,
                };

                engine
                    .preserve_until(&req)
                    .map_err(|e| format!("preserve_until failed: {e}"))?;

                if let Some(rec) = self.records.get_mut(tx_key) {
                    rec.preserve_until = *block_height;
                }
                Ok(())
            }
        }
    }

    /// Verify that the engine's state matches the expected state.
    ///
    /// Returns a list of all mismatches found. An empty list means
    /// the state is consistent.
    pub fn verify_against(&self, engine: &Engine) -> Vec<Mismatch> {
        let mut mismatches = Vec::new();

        for (key, expected) in &self.records {
            match engine.read_metadata(key) {
                Ok(meta) => {
                    // Check spent_utxos count
                    let actual_spent = { meta.spent_utxos };
                    if actual_spent != expected.spent_utxos {
                        mismatches.push(Mismatch {
                            detail: format!(
                                "tx {:?}: spent_utxos expected {}, got {}",
                                key, expected.spent_utxos, actual_spent
                            ),
                        });
                    }

                    // Check utxo_count
                    let actual_count = { meta.utxo_count };
                    if actual_count != expected.utxo_count {
                        mismatches.push(Mismatch {
                            detail: format!(
                                "tx {:?}: utxo_count expected {}, got {}",
                                key, expected.utxo_count, actual_count
                            ),
                        });
                    }

                    // Check each slot
                    for (i, exp_slot) in expected.slots.iter().enumerate() {
                        match engine.read_slot(key, i as u32) {
                            Ok(actual) => {
                                if actual.status != exp_slot.status {
                                    mismatches.push(Mismatch {
                                        detail: format!(
                                            "tx {:?} slot {}: status expected {:#x}, got {:#x}",
                                            key, i, exp_slot.status, actual.status
                                        ),
                                    });
                                }
                                if actual.hash != exp_slot.hash {
                                    mismatches.push(Mismatch {
                                        detail: format!("tx {:?} slot {}: hash mismatch", key, i),
                                    });
                                }
                                if actual.spending_data != exp_slot.spending_data {
                                    mismatches.push(Mismatch {
                                        detail: format!(
                                            "tx {:?} slot {}: spending_data mismatch",
                                            key, i
                                        ),
                                    });
                                }
                            }
                            Err(e) => {
                                mismatches.push(Mismatch {
                                    detail: format!("tx {:?} slot {}: read error: {}", key, i, e),
                                });
                            }
                        }
                    }
                }
                Err(SpendError::TxNotFound) => {
                    mismatches.push(Mismatch {
                        detail: format!("tx {:?}: expected to exist but not found", key),
                    });
                }
                Err(e) => {
                    mismatches.push(Mismatch {
                        detail: format!("tx {:?}: read error: {}", key, e),
                    });
                }
            }
        }

        mismatches
    }

    /// Number of records tracked by the verifier.
    #[allow(dead_code)]
    pub fn record_count(&self) -> usize {
        self.records.len()
    }
}
