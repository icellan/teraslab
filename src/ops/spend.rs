//! Spend and spendMulti operations.
//!
//! Implements all validation logic from `teranode.lua` lines 284–466.

use crate::ops::error::SpendError;
use crate::ops::signal::Signal;
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
