package teraslab

import (
	"context"
	"time"
)

// transientRetryDelaysMs is the backoff schedule (milliseconds) applied to
// same-target transient errors. Mirrors the Rust reference client's
// TRANSIENT_MUTATION_RETRY_DELAYS_MS: 14 attempts, ~38.5s total, with the tail
// capped at 5s to ride out a live shard migration.
var transientRetryDelaysMs = []int{10, 25, 50, 100, 200, 400, 800, 1600, 3200, 5000, 5000, 5000, 5000, 5000}

// maxRefreshRetries bounds how many times a single operation will refresh the
// partition map and retry in response to a no-quorum or stale-redirect signal.
const maxRefreshRetries = 2

type retryAction int

const (
	// retryNone: the error is terminal; surface it.
	retryNone retryAction = iota
	// retryBackoff: a same-target transient; retry after a backoff delay.
	retryBackoff
	// retryRefresh: routing is stale; refresh the partition map and retry
	// immediately (no backoff), bounded by maxRefreshRetries.
	retryRefresh
)

// classifyRetry decides how (if at all) an error from a cluster operation
// should be retried.
func classifyRetry(err error) retryAction {
	switch e := err.(type) {
	case *ServerError:
		if e.Code == ErrCodeNoQuorum {
			return retryRefresh
		}
		if isRetryableErrorCode(e.Code) {
			return retryBackoff
		}
		return retryNone
	case *StaleRedirectError:
		return retryRefresh
	case *PartialError:
		// Only retry when the entire sub-batch was transiently rejected: every
		// item failed with a retryable code and none succeeded. Re-sending then
		// cannot duplicate already-applied mutations.
		if len(e.Errors) == 0 || len(e.Successes) != 0 {
			return retryNone
		}
		for i := range e.Errors {
			if !isRetryableErrorCode(e.Errors[i].Code) {
				return retryNone
			}
		}
		return retryBackoff
	default:
		return retryNone
	}
}

// withTransientRetry runs a cluster operation, retrying transient failures with
// bounded backoff (same-target transients) or a partition-map refresh (no-quorum
// / stale-redirect). It is a no-op wrapper in single-node mode. On the final
// failure it returns the last result alongside the error so callers still see
// any partial response.
func withTransientRetry[R any](ctx context.Context, c *Client, op func() (R, error)) (R, error) {
	res, err := op()
	if err == nil || c.cluster == nil {
		return res, err
	}

	attempt := 0
	refreshRetries := 0
	for {
		switch classifyRetry(err) {
		case retryRefresh:
			if refreshRetries >= maxRefreshRetries {
				return res, err
			}
			refreshRetries++
			c.cluster.tryRefresh()
		case retryBackoff:
			if attempt >= len(transientRetryDelaysMs) {
				return res, err
			}
			delay := time.Duration(transientRetryDelaysMs[attempt]) * time.Millisecond
			attempt++
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				var zero R
				return zero, ctx.Err()
			}
		default: // retryNone
			return res, err
		}

		res, err = op()
		if err == nil {
			return res, nil
		}
	}
}
