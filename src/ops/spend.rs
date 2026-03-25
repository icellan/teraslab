//! Spend and spendMulti operations.
//!
//! Implements all validation logic from `teranode.lua` lines 284–466.

use crate::index::TxKey;
use crate::ops::error::SpendError;
use crate::ops::signal::Signal;
use crate::record::{TxMetadata, UtxoSlot};
use std::collections::HashMap;

/// A single spend item within a spendMulti batch.
#[derive(Debug, Clone)]
pub struct SpendItem {
    /// UTXO slot offset (vout).
    pub offset: u32,
    /// Expected UTXO hash (32 bytes).
    pub utxo_hash: [u8; 32],
    /// Spending data: txid(32) + vin(4 LE).
    pub spending_data: [u8; 36],
    /// Caller's identifier for this item (for error mapping).
    pub idx: u32,
}

/// Request for a batch of spends on a single transaction.
#[derive(Debug, Clone)]
pub struct SpendMultiRequest {
    /// Transaction key.
    pub tx_key: crate::index::TxKey,
    /// Individual spend items.
    pub spends: Vec<SpendItem>,
    /// Whether to ignore the CONFLICTING flag.
    pub ignore_conflicting: bool,
    /// Whether to ignore the LOCKED flag.
    pub ignore_locked: bool,
    /// Current block height (for coinbase maturity and DAH).
    pub current_block_height: u32,
    /// Block height retention period (for DAH calculation).
    pub block_height_retention: u32,
}

/// Response from a spendMulti operation.
#[derive(Debug, Clone)]
pub struct SpendMultiResponse {
    /// Signal from deleteAtHeight evaluation.
    pub signal: Signal,
    /// Current block IDs on the record.
    pub block_ids: Vec<u32>,
    /// Per-item errors (idx → error). Missing idx = success.
    pub errors: HashMap<u32, SpendError>,
    /// Number of UTXOs actually spent in this batch (not counting idempotent re-spends).
    pub spent_count: u32,
    /// Record generation after this mutation (for replication).
    pub generation: u32,
}

/// Request for a single spend (convenience wrapper around spendMulti).
#[derive(Debug, Clone)]
pub struct SpendRequest {
    /// Transaction key.
    pub tx_key: crate::index::TxKey,
    /// UTXO slot offset (vout).
    pub offset: u32,
    /// Expected UTXO hash.
    pub utxo_hash: [u8; 32],
    /// Spending data: txid(32) + vin(4 LE).
    pub spending_data: [u8; 36],
    /// Whether to ignore the CONFLICTING flag.
    pub ignore_conflicting: bool,
    /// Whether to ignore the LOCKED flag.
    pub ignore_locked: bool,
    /// Current block height.
    pub current_block_height: u32,
    /// Block height retention period.
    pub block_height_retention: u32,
}

/// Response from a single spend.
#[derive(Debug, Clone)]
pub struct SpendResponse {
    /// Signal from deleteAtHeight evaluation.
    pub signal: Signal,
    /// Current block IDs on the record.
    pub block_ids: Vec<u32>,
}

impl SpendRequest {
    /// Convert to a SpendMultiRequest with a single item.
    pub fn into_multi(self) -> SpendMultiRequest {
        SpendMultiRequest {
            tx_key: self.tx_key,
            spends: vec![SpendItem {
                offset: self.offset,
                utxo_hash: self.utxo_hash,
                spending_data: self.spending_data,
                idx: 0,
            }],
            ignore_conflicting: self.ignore_conflicting,
            ignore_locked: self.ignore_locked,
            current_block_height: self.current_block_height,
            block_height_retention: self.block_height_retention,
        }
    }
}

/// Result of spend validation, holding the per-record lock.
///
/// Produced by `Engine::validate_spend_multi()`. The lock guard prevents
/// other mutations on this record until `Engine::apply_spend_multi()`
/// consumes this struct and releases the lock. This enables the caller
/// to write redo log entries (WAL) between validation and application.
pub struct ValidatedSpend<'a> {
    /// RAII lock guard — holds the per-transaction stripe lock.
    /// Dropped when `apply_spend_multi()` consumes this struct.
    pub(crate) _guard: parking_lot::MutexGuard<'a, ()>,
    /// Transaction key being spent.
    pub tx_key: TxKey,
    /// Validated spend operations: (slot_offset, new_slot_state).
    /// Only items that passed all validation checks.
    pub(crate) valid_spends: Vec<(u32, UtxoSlot)>,
    /// Per-item errors from validation (idx → error).
    pub errors: HashMap<u32, SpendError>,
    /// Number of UTXOs that will actually change state (not counting idempotent re-spends).
    pub spent_count: u32,
    /// Record generation BEFORE this mutation. The post-mutation generation
    /// will be `pre_generation.wrapping_add(1)`.
    pub pre_generation: u32,
    /// Block IDs currently on the record.
    pub block_ids: Vec<u32>,
    /// Record offset on the block device (needed for apply).
    pub(crate) record_offset: u64,
    /// Metadata read during validation (needed for apply).
    pub(crate) metadata: TxMetadata,
    /// Request params needed during apply (DAH evaluation).
    pub(crate) current_block_height: u32,
    /// Block height retention for DAH.
    pub(crate) block_height_retention: u32,
}
