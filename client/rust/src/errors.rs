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

    /// FU#5 — a query response was flagged truncated but the negotiated server
    /// protocol version is below 3, so the server has no resume cursor and the
    /// client cannot page to completion. `partial` holds the valid-but-PARTIAL
    /// first page: callers must not treat it as the complete set. Against a
    /// version-3+ server the client pages internally and this is never returned.
    #[error("query result truncated: server (protocol < 3) has no resume cursor; {} txids returned are partial", .partial.len())]
    QueryTruncated {
        /// The partial first page of txids returned before truncation.
        partial: Vec<crate::types::TxID>,
    },
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
    /// Whether the items that DID apply were only replicated under degraded
    /// (below-quorum, best-effort) durability. Carries the same meaning as
    /// receiving `STATUS_DEGRADED_DURABILITY` on a fully-successful batch: the
    /// applied writes are single-node durable and may be lost if the master
    /// crashes before catch-up streaming. Callers that require quorum durability
    /// must treat a `true` here as a durability failure for the applied items.
    pub degraded: bool,
}

impl std::fmt::Display for PartialError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "partial error: {} of {} items failed",
            self.errors.len(),
            self.successes.len() + self.errors.len()
        )?;
        // Name WHY. Without this the message carries only counts the caller
        // already knows, and a scenario logging `{e}` reports nothing
        // actionable. Codes are summarised (distinct code + occurrence count,
        // first-seen order) so a 1024-item batch stays one readable line.
        if !self.errors.is_empty() {
            let mut counts: Vec<(u16, usize)> = Vec::new();
            for e in &self.errors {
                match counts.iter_mut().find(|(code, _)| *code == e.code) {
                    Some((_, n)) => *n += 1,
                    None => counts.push((e.code, 1)),
                }
            }
            f.write_str(" [")?;
            for (i, (code, n)) in counts.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                write!(f, "{}({code})", error_code_string(*code))?;
                if *n > 1 {
                    write!(f, " x{n}")?;
                }
            }
            f.write_str("]")?;
        }
        if self.degraded {
            write!(f, " (applied items degraded-durability)")?;
        }
        Ok(())
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
        ERR_NO_QUORUM => "NO_QUORUM",
        ERR_STREAM_NOT_FOUND => "STREAM_NOT_FOUND",
        ERR_BLOB_NOT_FOUND => "BLOB_NOT_FOUND",
        ERR_STREAM_OFFSET_MISMATCH => "STREAM_OFFSET_MISMATCH",
        ERR_INTERNAL => "INTERNAL",
        ERR_MIGRATION_IN_PROGRESS => "MIGRATION_IN_PROGRESS",
        ERR_REPLICATION_FAILED => "REPLICATION_FAILED",
        ERR_STALE_EPOCH => "STALE_EPOCH",
        // P3.10 / F-G5-017 — typed wire error codes (PROTOCOL_VERSION=2).
        ERR_PAYLOAD_MALFORMED => "PAYLOAD_MALFORMED",
        ERR_OPCODE_UNSUPPORTED => "OPCODE_UNSUPPORTED",
        ERR_STORAGE_IO => "STORAGE_IO",
        ERR_RATE_LIMITED => "RATE_LIMITED",
        ERR_NOT_CLUSTERED => "NOT_CLUSTERED",
        ERR_INVARIANT_VIOLATION => "INVARIANT_VIOLATION",
        ERR_STREAM_INVARIANT => "STREAM_INVARIANT",
        ERR_DELETED_CHILDREN => "DELETED_CHILDREN",
        ERR_NOT_DUE => "NOT_DUE",
        ERR_MIGRATION_TARGET_NOT_READY => "MIGRATION_TARGET_NOT_READY",
        ERR_RESPONSE_TOO_LARGE => "RESPONSE_TOO_LARGE",
        _ => "UNKNOWN",
    }
}

#[cfg(test)]
mod tests {
    use super::{PartialError, error_code_string};
    use teraslab::protocol::opcodes::*;

    /// Every typed server error code must map to its name, not the `UNKNOWN`
    /// fallback. This guards the tail of the code space (35..=38) that the map
    /// previously dropped — most importantly `ERR_RESPONSE_TOO_LARGE` (38),
    /// which the GET frame-budget path now emits.
    #[test]
    fn maps_full_error_code_tail_including_response_too_large() {
        assert_eq!(error_code_string(ERR_DELETED_CHILDREN), "DELETED_CHILDREN");
        assert_eq!(error_code_string(ERR_NOT_DUE), "NOT_DUE");
        assert_eq!(
            error_code_string(ERR_MIGRATION_TARGET_NOT_READY),
            "MIGRATION_TARGET_NOT_READY"
        );
        assert_eq!(
            error_code_string(ERR_RESPONSE_TOO_LARGE),
            "RESPONSE_TOO_LARGE"
        );
        // ERR_NO_QUORUM (15) was missing, so a scale-up that transiently lost
        // quorum reported "UNKNOWN(15)" in every batch error.
        assert_eq!(error_code_string(ERR_NO_QUORUM), "NO_QUORUM");
        // A genuinely unknown code still falls back.
        assert_eq!(error_code_string(9999), "UNKNOWN");
    }

    /// `PartialError`'s Display must name WHY the items failed.
    ///
    /// It used to print only "N of M items failed", which is exactly the
    /// information a caller already has. Test scenarios logging `{e}` on a
    /// failed batch produced "partial error: partial error: 1 of 1 items
    /// failed" — a line that cost a full nightly cycle to re-diagnose because
    /// the per-item code, the one thing that identifies the fault, was
    /// dropped on the floor.
    #[test]
    fn partial_error_display_names_the_failing_codes() {
        use crate::types::{BatchItemError, BatchItemSuccess};

        let err = PartialError {
            successes: vec![BatchItemSuccess {
                item_index: 1,
                signal: 0,
                block_ids: vec![],
            }],
            errors: vec![
                BatchItemError {
                    item_index: 0,
                    code: ERR_LOCKED,
                    data: vec![],
                },
                BatchItemError {
                    item_index: 2,
                    code: ERR_LOCKED,
                    data: vec![],
                },
                BatchItemError {
                    item_index: 3,
                    code: ERR_MIGRATION_TARGET_NOT_READY,
                    data: vec![],
                },
            ],
            degraded: false,
        };

        let rendered = err.to_string();
        assert!(
            rendered.starts_with("partial error: 3 of 4 items failed"),
            "counts must be preserved, got: {rendered}"
        );
        assert!(
            rendered.contains(&format!("LOCKED({ERR_LOCKED}) x2")),
            "repeated codes must be named, numbered and counted, got: {rendered}"
        );
        assert!(
            rendered.contains("MIGRATION_TARGET_NOT_READY"),
            "every distinct code must be named, got: {rendered}"
        );
    }

    /// The degraded-durability suffix must survive the code summary.
    #[test]
    fn partial_error_display_keeps_degraded_suffix() {
        use crate::types::BatchItemError;

        let err = PartialError {
            successes: vec![],
            errors: vec![BatchItemError {
                item_index: 0,
                code: ERR_REPLICATION_FAILED,
                data: vec![],
            }],
            degraded: true,
        };
        let rendered = err.to_string();
        assert!(
            rendered.contains("REPLICATION_FAILED"),
            "code must be named, got: {rendered}"
        );
        assert!(
            rendered.contains("degraded-durability"),
            "degraded suffix must be kept, got: {rendered}"
        );
    }
}
