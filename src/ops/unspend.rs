//! Unspend operation.
//!
//! Reverses a spend by clearing the spending data and decrementing the counter.
//! Implements validation from `teranode.lua` lines 478–540.

use crate::index::TxKey;
use crate::ops::signal::Signal;

/// Request to unspend a UTXO.
#[derive(Debug, Clone)]
pub struct UnspendRequest {
    /// Transaction key.
    pub tx_key: TxKey,
    /// UTXO slot offset (vout).
    pub offset: u32,
    /// Expected UTXO hash.
    pub utxo_hash: [u8; 32],
    /// Current block height (for DAH evaluation).
    pub current_block_height: u32,
    /// Block height retention (for DAH evaluation).
    pub block_height_retention: u32,
}

/// Response from an unspend operation.
#[derive(Debug, Clone)]
pub struct UnspendResponse {
    /// Signal from deleteAtHeight evaluation.
    pub signal: Signal,
}
