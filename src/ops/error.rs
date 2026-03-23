//! Error types for UTXO store operations.
//!
//! Each variant maps to an error code from the original Lua UDF implementation.
//! Spending data is returned as raw bytes — hex encoding is a client concern.

use thiserror::Error;

/// Errors from spend/unspend operations, matching the Lua error codes.
#[derive(Error, Debug, Clone, PartialEq)]
pub enum SpendError {
    /// Record does not exist in the index.
    #[error("TX_NOT_FOUND")]
    TxNotFound,

    /// Transaction is marked as conflicting.
    #[error("CONFLICTING")]
    Conflicting,

    /// Transaction is locked and cannot be spent.
    #[error("LOCKED")]
    Locked,

    /// Coinbase UTXO has not reached maturity.
    #[error("COINBASE_IMMATURE: spendable at block {spending_height}, current {current_height}")]
    CoinbaseImmature {
        /// Block height at which the coinbase becomes spendable.
        spending_height: u32,
        /// Current block height.
        current_height: u32,
    },

    /// UTXO at the given offset does not exist.
    #[error("UTXO_NOT_FOUND at offset {offset}")]
    UtxoNotFound {
        /// The requested slot offset.
        offset: u32,
    },

    /// Expected UTXO hash does not match the stored hash.
    #[error("UTXO_HASH_MISMATCH at offset {offset}")]
    UtxoHashMismatch {
        /// The requested slot offset.
        offset: u32,
    },

    /// UTXO is already spent by a different transaction.
    #[error("SPENT at offset {offset}")]
    AlreadySpent {
        /// The slot offset.
        offset: u32,
        /// Raw spending data (36 bytes: txid + vin).
        spending_data: [u8; 36],
    },

    /// UTXO is frozen and cannot be spent.
    #[error("FROZEN at offset {offset}")]
    Frozen {
        /// The slot offset.
        offset: u32,
    },

    /// UTXO is not yet spendable (reassignment cooldown).
    #[error("FROZEN_UNTIL at offset {offset}: spendable at block {spendable_at_height}")]
    FrozenUntil {
        /// The slot offset.
        offset: u32,
        /// Block height at which the UTXO becomes spendable.
        spendable_at_height: u32,
    },

    /// Spending data targets a deleted/pruned child transaction.
    #[error("INVALID_SPEND at offset {offset}")]
    InvalidSpend {
        /// The slot offset.
        offset: u32,
        /// Raw spending data of the pruned entry (36 bytes).
        spending_data: [u8; 36],
    },

    /// UTXO is in the PRUNED terminal state (child tx deleted).
    #[error("PRUNED at offset {offset}")]
    Pruned {
        /// The slot offset.
        offset: u32,
    },

    /// UTXO is already frozen (for freeze operation).
    #[error("ALREADY_FROZEN at offset {offset}")]
    AlreadyFrozen {
        /// The slot offset.
        offset: u32,
    },

    /// UTXO is not in frozen state (for unfreeze/reassign).
    #[error("UTXO_NOT_FROZEN at offset {offset}")]
    NotFrozen {
        /// The slot offset.
        offset: u32,
    },

    /// Device I/O error during operation.
    #[error("STORAGE_ERROR: {detail}")]
    StorageError {
        /// Description of the I/O failure.
        detail: String,
    },
}
