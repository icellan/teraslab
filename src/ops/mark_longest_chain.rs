//! MarkOnLongestChain operation types.
//!
//! Modifies only `unmined_since` without touching block entries.
//! Called during chain reorganizations for bulk longest-chain updates.

use crate::index::TxKey;
use crate::ops::signal::Signal;

/// Request to mark a transaction as on or off the longest chain.
#[derive(Debug, Clone)]
pub struct MarkOnLongestChainRequest {
    /// Transaction key.
    pub tx_key: TxKey,
    /// Whether the transaction is on the longest chain.
    pub on_longest_chain: bool,
    /// Current block height (for unmined_since and DAH evaluation).
    pub current_block_height: u32,
    /// Block height retention (for DAH evaluation).
    pub block_height_retention: u32,
}

/// Response from a markOnLongestChain operation.
#[derive(Debug, Clone)]
pub struct MarkOnLongestChainResponse {
    /// Signal from deleteAtHeight evaluation.
    pub signal: Signal,
}
