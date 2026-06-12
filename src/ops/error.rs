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

    /// F-IJ-001: an `EXTERNAL`-flagged record exists in the index but its
    /// cold-data blob is absent (lost / GC'd / bit-rotted / never uploaded)
    /// or no blob store is configured to resolve it.
    ///
    /// This is a data-integrity violation, NOT a missing transaction: the
    /// record and its UTXOs are present and spendable, only the external cold
    /// data is unreachable. It MUST surface distinctly from
    /// [`SpendError::TxNotFound`] (which the dispatcher maps to
    /// `ERR_TX_NOT_FOUND`) — pre-fix a missing blob was reported as
    /// `TX_NOT_FOUND`, telling callers the tx never existed and masking the
    /// loss. The dispatcher maps this to `ERR_BLOB_NOT_FOUND` (17).
    #[error("BLOB_NOT_FOUND")]
    BlobNotFound {
        /// The txid whose external cold-data blob is missing.
        txid: [u8; 32],
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
    /// the dispatcher maps to `ERR_STORAGE_IO` (P3.10 / F-G5-017; it was
    /// `ERR_INTERNAL` before the typed wire codes) so the operator
    /// catches the pathological input instead of silently corrupting
    /// state.
    #[error(
        "REASSIGN_OVERFLOW: block_height={block_height} + spendable_after={spendable_after} exceeds u32::MAX"
    )]
    ReassignOverflow {
        /// Current block height supplied by the reassign request.
        block_height: u32,
        /// Configured spendable-after delay (in blocks) the request asked for.
        spendable_after: u32,
    },

    /// F-X-022: defense-in-depth at the idempotent-respend short-circuit.
    /// The spending child txid is present in the parent record's
    /// `deleted_children` list, meaning the chain history has been
    /// altered (the child was pruned after originally consuming this
    /// output — "resurrected-then-pruned"). The primary defense remains
    /// the slot's `UTXO_PRUNED` status; this variant fires only on the
    /// idempotent-respend short-circuit where the slot LOOKS `SPENT` by
    /// the requested child but the deleted-children list contradicts
    /// it. Callers MUST treat this as a hard rejection and re-validate
    /// chain state before retrying.
    #[error(
        "DELETED_CHILDREN at offset {offset}: child txid present in deleted_children list ({child_count} total)"
    )]
    DeletedChildren {
        /// The slot offset the request targeted.
        offset: u32,
        /// Total number of children currently in the deleted-children
        /// list (for diagnostics — the matching child txid itself is in
        /// the request).
        child_count: u8,
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

    /// KO-3: a guarded sweep delete ([`crate::ops::engine::Engine::delete`]
    /// with `DeleteRequest::due_guard == Some(height)`) re-validated the
    /// record under the per-tx stripe lock and found it no longer due:
    /// either a `preserve_until` was set or pushed past the height between
    /// the sweep's lock-free re-validation and the actual delete, or the
    /// `delete_at_height` is now unset / in the future, or the record's
    /// spent/longest-chain state regressed. This is NOT an error toward the
    /// pruner — it means a concurrent preservation (or state change) won the
    /// race and the record must be kept. The DAH sweep treats it as a
    /// skipped (not-deleted) candidate. Never returned for an unguarded
    /// client `OP_DELETE_BATCH` (`due_guard == None`), which stays
    /// unconditional per spec §3.18.
    #[error("NOT_DUE: record no longer due for sweep deletion (preserved or state changed)")]
    NotDue,

    /// KO-5: a parent record's conflicting-children list is already at the
    /// on-disk capacity (`u8::MAX` = 255 txids) and cannot accept another
    /// child. The on-device metadata stores the child count in a single
    /// `u8` ([`crate::record::TxMetadata::conflicting_children_count`]), so
    /// 255 is a hard structural limit, not a tunable.
    ///
    /// Pre-fix this surfaced as a generic [`SpendError::StorageError`] that
    /// the best-effort propagation wrapper swallowed into a `tracing::warn!`,
    /// so the 256th conflicting child of a parent was dropped while the
    /// triggering `set_conflicting` / create still returned OK — a silent
    /// truncation of the Go client's counter-conflicting cascade.
    ///
    /// It is now a distinct variant so (a) direct callers of
    /// [`crate::ops::engine::Engine::append_conflicting_child`] see the
    /// overflow rather than an opaque I/O error, and (b) the best-effort
    /// wrapper escalates it to a `tracing::error!` and bumps an engine
    /// counter ([`crate::ops::engine::Engine::conflicting_children_dropped`])
    /// so the loss is observable instead of invisible.
    #[error("CONFLICTING_CHILDREN_FULL: parent list at capacity ({cap}), child dropped")]
    ConflictingChildrenFull {
        /// The capacity that was hit (the on-disk `u8` count maximum, 255).
        cap: usize,
    },
}
