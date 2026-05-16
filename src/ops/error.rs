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
        /// Raw spending data that caused the prune (36 bytes).
        spending_data: [u8; 36],
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

    /// `current_block_height + block_height_retention` would overflow a `u32`.
    ///
    /// This indicates a misconfiguration: either `block_height_retention` is
    /// absurdly large, or `current_block_height` is near `u32::MAX`. The
    /// server returns this instead of silently clamping (which would pin
    /// UTXOs as unprunable forever).
    #[error(
        "DAH_OVERFLOW: current_height={current_height} + retention={retention} exceeds u32::MAX"
    )]
    DahOverflow {
        /// Current block height at the time of evaluation.
        current_height: u32,
        /// Configured block-height retention window.
        retention: u32,
    },

    /// R-063 (A-13): `block_height + spendable_after` for a `reassign`
    /// operation would overflow a `u32`. Pre-fix the engine used
    /// `saturating_add`, which silently clamped to `u32::MAX` and pinned
    /// the UTXO unspendable forever. Now surfaces as an explicit error
    /// the dispatcher maps to `ERR_INTERNAL` so the operator catches the
    /// pathological input instead of silently corrupting state.
    #[error(
        "REASSIGN_OVERFLOW: block_height={block_height} + spendable_after={spendable_after} exceeds u32::MAX"
    )]
    ReassignOverflow {
        /// Current block height supplied by the reassign request.
        block_height: u32,
        /// Configured spendable-after delay (in blocks) the request asked for.
        spendable_after: u32,
    },

    /// F-G2-002: A client attempted to stamp a UTXO using the reserved
    /// all-`0xFF` sentinel as `spending_data`. That byte pattern is the
    /// on-disk marker for a frozen slot — accepting it under `status=SPENT`
    /// would let a malicious spender brick the UTXO against any future
    /// `unspend` (the frozen-marker check fires before the data-match
    /// check) and `unfreeze` (rejects non-`UTXO_FROZEN` status), leaving
    /// the slot permanently unrecoverable. The 36 bytes are also invalid
    /// BSV `txid(32) + vin(4)` (an all-`0xFF` txid does not exist on the
    /// network), so rejecting at the request boundary loses no legitimate
    /// traffic.
    #[error("INVALID_SPENDING_DATA at offset {offset}: reserved frozen sentinel")]
    ReservedSpendingData {
        /// The slot offset the request targeted.
        offset: u32,
    },
}
