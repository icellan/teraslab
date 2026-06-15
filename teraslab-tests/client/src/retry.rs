//! Unified bounded transient-retry policy for mutation helpers.
//!
//! All test-harness mutation helpers (seed / spend / set_mined / …) share
//! ONE retry policy for *transient* server conditions rather than each
//! re-implementing its own loop. The transient set deliberately includes
//! `ERR_REPLICATION_FAILED` (code 20): per the spec (§8.7
//! "ERR_REPLICATION_FAILED — ambiguous outcome, idempotent-retry-safe"), a
//! code-20 outcome is ambiguous — the write may be durable on master,
//! replicas, both, or neither — and the prescribed recovery for an
//! idempotent op is to re-issue it while the server's compensation
//! machinery converges the divergent state. The harness therefore treats
//! code 20 exactly like the other same-target transient codes
//! (`ERR_MIGRATION_IN_PROGRESS`, `ERR_STALE_EPOCH`) and the cluster-level
//! `ERR_NO_QUORUM`, which clears once a master/quorum re-forms.

use std::time::Duration;

use teraslab::protocol::opcodes::{
    ERR_MIGRATION_IN_PROGRESS, ERR_NO_QUORUM, ERR_REPLICATION_FAILED, ERR_STALE_EPOCH,
};
use teraslab_client::{ClientError, PartialError};

/// Maximum number of attempts a transient mutation is retried (the first
/// try plus retries). Sized to ride out a post-topology-change settle
/// window: the per-attempt backoff caps at [`MAX_BACKOFF`], so the total
/// retry budget is on the order of tens of seconds.
pub const MAX_TRANSIENT_ATTEMPTS: u32 = 16;

/// Backoff cap. Backoff grows as `BASE_BACKOFF * 2^min(attempt, 3)` and is
/// then clamped to this value.
pub const MAX_BACKOFF: Duration = Duration::from_secs(4);

/// Base backoff for the first retry.
pub const BASE_BACKOFF: Duration = Duration::from_millis(500);

/// Returns the backoff to sleep before the retry following `attempt`
/// (0-based). Matches the policy historically used by `seed_records`:
/// `500ms * 2^min(attempt, 3)` capped at [`MAX_BACKOFF`].
pub fn backoff_for_attempt(attempt: u32) -> Duration {
    let scaled = BASE_BACKOFF * (1u32 << attempt.min(3));
    scaled.min(MAX_BACKOFF)
}

/// Returns `true` for server error codes that indicate a *transient*
/// condition that a bounded retry of the identical (idempotent) op can
/// recover from.
///
/// - `ERR_MIGRATION_IN_PROGRESS` (19): shard handoff fence; clears when
///   migration completes.
/// - `ERR_REPLICATION_FAILED` (20): ambiguous, idempotent-retry-safe
///   durability-confirmation failure (spec §8.7). Compensation converges
///   the state; the retry is the prescribed recovery.
/// - `ERR_STALE_EPOCH` (24): the target's epoch lags the requester's view;
///   clears once both observe the new committed term.
/// - `ERR_NO_QUORUM` (15): no master/quorum currently; clears when a
///   quorum re-forms.
pub fn is_transient_code(code: u16) -> bool {
    matches!(
        code,
        ERR_MIGRATION_IN_PROGRESS | ERR_REPLICATION_FAILED | ERR_STALE_EPOCH | ERR_NO_QUORUM
    )
}

/// Classifies a [`ClientError`] as transient (retry the whole op) or
/// terminal (return to caller).
///
/// Retryable:
/// - `Connection` / `Timeout`: I/O blip during a topology change.
/// - `Server { code }` where `code` is [`is_transient_code`].
/// - `Partial` where **every** failed item carries a transient code. A
///   `Partial` with a mix of successes and non-transient errors is
///   *terminal* — retrying the whole op would re-apply items that already
///   succeeded, and the non-transient failures are real.
///
/// Note: this classifies the op as a *whole-op* retry decision. Helpers
/// that retry only the failed sub-set of a partial batch (e.g.
/// `seed_records`) make their own per-item decision and use this only for
/// the global/all-transient cases.
pub fn is_transient_error(err: &ClientError) -> bool {
    match err {
        ClientError::Connection(_) | ClientError::Timeout => true,
        ClientError::Server { code, .. } => is_transient_code(*code),
        ClientError::Partial(PartialError { errors, .. }) => {
            !errors.is_empty() && errors.iter().all(|e| is_transient_code(e.code))
        }
        _ => false,
    }
}

/// Runs `op` with the unified bounded transient-retry policy.
///
/// `op` is invoked up to [`MAX_TRANSIENT_ATTEMPTS`] times. After each
/// transient failure (per [`is_transient_error`]) the helper sleeps
/// [`backoff_for_attempt`] and invokes `op` again; a terminal error or a
/// success returns immediately. The final attempt's result is returned
/// even if it is transient (the caller decides what to do with a
/// still-failing op after the budget is exhausted).
///
/// `op` must be idempotent — all TeraSlab mutations are idempotent by
/// txid/op semantics, so re-issuing the identical op is safe regardless of
/// the ambiguous durability outcome behind a transient error.
pub async fn retry_transient_mutation<T, F, Fut>(mut op: F) -> Result<T, ClientError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, ClientError>>,
{
    let mut last: Result<T, ClientError> = op().await;
    let mut attempt = 0u32;
    while attempt + 1 < MAX_TRANSIENT_ATTEMPTS {
        match &last {
            Ok(_) => return last,
            Err(e) if is_transient_error(e) => {
                tokio::time::sleep(backoff_for_attempt(attempt)).await;
                last = op().await;
                attempt += 1;
            }
            Err(_) => return last,
        }
    }
    last
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use teraslab_client::types::BatchItemError;

    fn server_err(code: u16) -> ClientError {
        ClientError::Server {
            code,
            message: "x".into(),
        }
    }

    fn partial(codes: &[u16]) -> ClientError {
        ClientError::Partial(PartialError {
            successes: vec![],
            errors: codes
                .iter()
                .enumerate()
                .map(|(i, &code)| BatchItemError {
                    item_index: i as u32,
                    code,
                    data: vec![],
                })
                .collect(),
        })
    }

    #[test]
    fn replication_failed_is_transient() {
        assert!(
            is_transient_code(ERR_REPLICATION_FAILED),
            "code 20 is the ambiguous idempotent-retry-safe outcome and must be retried"
        );
        assert_eq!(ERR_REPLICATION_FAILED, 20);
    }

    #[test]
    fn migration_stale_quorum_are_transient() {
        assert!(is_transient_code(ERR_MIGRATION_IN_PROGRESS));
        assert!(is_transient_code(ERR_STALE_EPOCH));
        assert!(is_transient_code(ERR_NO_QUORUM));
    }

    #[test]
    fn terminal_codes_are_not_transient() {
        // ALREADY_SPENT (8) and TX_NOT_FOUND (1) are real, terminal outcomes.
        assert!(!is_transient_code(8));
        assert!(!is_transient_code(1));
    }

    #[test]
    fn global_replication_failed_error_is_transient() {
        assert!(is_transient_error(&server_err(ERR_REPLICATION_FAILED)));
    }

    #[test]
    fn all_transient_partial_is_transient() {
        assert!(is_transient_error(&partial(&[
            ERR_REPLICATION_FAILED,
            ERR_MIGRATION_IN_PROGRESS,
        ])));
    }

    #[test]
    fn mixed_partial_with_terminal_code_is_not_transient() {
        // One transient + one terminal → retrying the whole op would
        // re-apply the already-decided item; treat as terminal.
        assert!(!is_transient_error(&partial(&[ERR_REPLICATION_FAILED, 8])));
    }

    #[test]
    fn empty_partial_is_not_transient() {
        assert!(!is_transient_error(&partial(&[])));
    }

    #[test]
    fn backoff_grows_and_caps() {
        assert_eq!(backoff_for_attempt(0), Duration::from_millis(500));
        assert_eq!(backoff_for_attempt(1), Duration::from_millis(1000));
        assert_eq!(backoff_for_attempt(2), Duration::from_millis(2000));
        assert_eq!(backoff_for_attempt(3), MAX_BACKOFF);
        // Clamped beyond attempt 3.
        assert_eq!(backoff_for_attempt(10), MAX_BACKOFF);
    }

    #[tokio::test(start_paused = true)]
    async fn retries_transient_then_succeeds() {
        let calls = Cell::new(0u32);
        let result: Result<u32, ClientError> = retry_transient_mutation(|| {
            let n = calls.get();
            calls.set(n + 1);
            async move {
                if n == 0 {
                    Err(server_err(ERR_REPLICATION_FAILED))
                } else {
                    Ok(n)
                }
            }
        })
        .await;
        assert_eq!(result.unwrap(), 1, "second attempt should succeed");
        assert_eq!(calls.get(), 2, "op must be invoked exactly twice");
    }

    #[tokio::test(start_paused = true)]
    async fn returns_terminal_error_without_retrying() {
        let calls = Cell::new(0u32);
        let result: Result<u32, ClientError> = retry_transient_mutation(|| {
            calls.set(calls.get() + 1);
            async move { Err::<u32, _>(server_err(8)) }
        })
        .await;
        match result {
            Err(ClientError::Server { code, .. }) => assert_eq!(code, 8),
            other => panic!("expected terminal Server(8), got {other:?}"),
        }
        assert_eq!(calls.get(), 1, "terminal error must not be retried");
    }

    #[tokio::test(start_paused = true)]
    async fn exhausts_budget_and_returns_last_transient() {
        let calls = Cell::new(0u32);
        let result: Result<u32, ClientError> = retry_transient_mutation(|| {
            calls.set(calls.get() + 1);
            async move { Err::<u32, _>(server_err(ERR_REPLICATION_FAILED)) }
        })
        .await;
        assert!(matches!(
            result,
            Err(ClientError::Server {
                code: ERR_REPLICATION_FAILED,
                ..
            })
        ));
        assert_eq!(
            calls.get(),
            MAX_TRANSIENT_ATTEMPTS,
            "op must be invoked exactly MAX_TRANSIENT_ATTEMPTS times"
        );
    }
}
