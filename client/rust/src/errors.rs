//! Error types for the TeraSlab client.
//!
//! Mirrors the error hierarchy from the Go client: global server errors,
//! per-item partial errors, connection errors, and protocol decode errors.

use crate::types::{BatchItemError, BatchItemSuccess};
use thiserror::Error;

/// Top-level error type returned by all client operations.
#[derive(Error, Debug)]
pub enum ClientError {
    /// TCP connection or I/O error.
    #[error("connection error: {0}")]
    Connection(String),

    /// Request timed out waiting for a response.
    #[error("timeout")]
    Timeout,

    /// The server returned a global error (all items in the batch failed).
    #[error("server error {code}: {message}")]
    Server {
        /// Error code from the server.
        code: u16,
        /// Human-readable error message.
        message: String,
    },

    /// Some items in the batch succeeded and some failed.
    /// The caller should inspect the contained [`PartialError`] for details.
    #[error("partial error: {0}")]
    Partial(PartialError),

    /// The server redirected to a different node. In cluster mode this is
    /// handled automatically; in single-node mode it is returned to the caller.
    #[error("redirect to {0}")]
    Redirect(String),

    /// The requested record was not found (response status 2).
    #[error("not found")]
    NotFound,

    /// No partition map is available for cluster routing.
    #[error("no partition map")]
    NoPartitionMap,

    /// Wire protocol decoding error (malformed frame or payload).
    #[error("protocol error: {0}")]
    Protocol(String),

    /// The connection pool has been closed.
    #[error("pool closed")]
    PoolClosed,
}

/// Partial error containing per-item successes and failures from a batch operation.
///
/// For spend/set-mined operations, `successes` contains signal data.
/// For other mutations, `successes` is empty and only `errors` is populated.
#[derive(Debug)]
pub struct PartialError {
    /// Per-item success results with signals and block IDs.
    /// Non-empty only for Spend/SetMined operations.
    pub successes: Vec<BatchItemSuccess>,
    /// Per-item failures. Item indices refer to the original request batch
    /// (already remapped from sub-batch indices in cluster mode).
    pub errors: Vec<BatchItemError>,
}

impl std::fmt::Display for PartialError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "partial error: {} of {} items failed",
            self.errors.len(),
            self.successes.len() + self.errors.len()
        )
    }
}

/// Returns a human-readable name for a server error code.
pub fn error_code_string(code: u16) -> &'static str {
    use teraslab::protocol::opcodes::*;
    match code {
        ERR_OK => "OK",
        ERR_TX_NOT_FOUND => "TX_NOT_FOUND",
        ERR_UTXO_HASH_MISMATCH => "UTXO_HASH_MISMATCH",
        ERR_ALREADY_SPENT => "ALREADY_SPENT",
        ERR_ALREADY_FROZEN => "ALREADY_FROZEN",
        ERR_UTXO_NOT_FROZEN => "UTXO_NOT_FROZEN",
        ERR_INVALID_SPEND => "INVALID_SPEND",
        ERR_FROZEN => "FROZEN",
        ERR_CONFLICTING => "CONFLICTING",
        ERR_LOCKED => "LOCKED",
        ERR_COINBASE_IMMATURE => "COINBASE_IMMATURE",
        ERR_VOUT_OUT_OF_RANGE => "VOUT_OUT_OF_RANGE",
        ERR_ALREADY_EXISTS => "ALREADY_EXISTS",
        ERR_FROZEN_UNTIL => "FROZEN_UNTIL",
        ERR_REDIRECT => "REDIRECT",
        ERR_STREAM_NOT_FOUND => "STREAM_NOT_FOUND",
        ERR_BLOB_NOT_FOUND => "BLOB_NOT_FOUND",
        ERR_STREAM_OFFSET_MISMATCH => "STREAM_OFFSET_MISMATCH",
        ERR_INTERNAL => "INTERNAL",
        ERR_MIGRATION_IN_PROGRESS => "MIGRATION_IN_PROGRESS",
        ERR_REPLICATION_FAILED => "REPLICATION_FAILED",
        _ => "UNKNOWN",
    }
}
