//! SetMined / UnsetMined operation types.
//!
//! Adds or removes block entries in transaction metadata. Called for every
//! transaction in every new block. Only modifies the metadata region.

use crate::index::TxKey;
use crate::ops::signal::Signal;

/// Request to set or unset the mined state of a transaction.
#[derive(Debug, Clone)]
pub struct SetMinedRequest {
    /// Transaction key.
    pub tx_key: TxKey,
    /// Block ID to add or remove.
    pub block_id: u32,
    /// Block height.
    pub block_height: u32,
    /// Subtree index within the block.
    pub subtree_idx: u32,
    /// Current block height (for DAH evaluation).
    pub current_block_height: u32,
    /// Block height retention (for DAH evaluation).
    pub block_height_retention: u32,
    /// Whether this block is on the longest chain.
    pub on_longest_chain: bool,
    /// If true, remove this block entry instead of adding it.
    pub unset_mined: bool,
}

/// Response from a setMined/unsetMined operation.
#[derive(Debug, Clone)]
pub struct SetMinedResponse {
    /// Signal from deleteAtHeight evaluation.
    pub signal: Signal,
    /// Current block IDs after this operation.
    pub block_ids: Vec<u32>,
    /// Record generation after mutation.
    pub generation: u32,
}
