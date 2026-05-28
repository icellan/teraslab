package teraslab

import "fmt"

// BatchItemError represents a per-item failure in a batch response.
type BatchItemError struct {
	// ItemIndex is the 0-based index into the original request batch.
	ItemIndex uint32
	// Code is the error code from the server.
	Code uint16
	// Data contains additional error data (e.g., 36 bytes of spending_data
	// for ErrCodeAlreadySpent, or 4 bytes of required height for ErrCodeCoinbaseImmature).
	Data []byte
}

func (e *BatchItemError) Error() string {
	return fmt.Sprintf("item %d: %s", e.ItemIndex, ErrorCodeString(e.Code))
}

// PartialError is returned when a batch operation has mixed success/failure.
// Callers should use errors.As to extract this from the returned error.
type PartialError struct {
	// Successes contains per-item success results with signals and block IDs.
	// Non-nil only for Spend/SetMined operations that return signal data.
	Successes []BatchItemSuccess
	// Errors contains per-item failures. Item indices refer to the original
	// request batch (not sub-batch indices in cluster mode).
	Errors []BatchItemError
}

func (e *PartialError) Error() string {
	return fmt.Sprintf("partial error: %d of %d items failed",
		len(e.Errors), len(e.Successes)+len(e.Errors))
}

// ServerError is a global server error (response status = 1).
// This indicates all items in the batch failed.
type ServerError struct {
	Code    uint16
	Message string
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("server error %s: %s", ErrorCodeString(e.Code), e.Message)
}

// RedirectError indicates the request should be sent to a different node.
// In cluster mode, the client handles this automatically. In single-node mode,
// this error is returned to the caller.
type RedirectError struct {
	Addr string
}

func (e *RedirectError) Error() string {
	return fmt.Sprintf("redirect to %s", e.Addr)
}

// TooManyRedirectsError is returned when the cluster-mode redirect retry
// loop exceeds ClusterConfig.MaxRedirects. The most likely causes are:
// (1) a stale partition map combined with a slow refresh, (2) a routing
// loop on the server side, or (3) MaxRedirects set too low for a churning
// cluster. LastAddr is the final redirect target the client was pointed at.
type TooManyRedirectsError struct {
	Hops     int
	LastAddr string
}

func (e *TooManyRedirectsError) Error() string {
	return fmt.Sprintf("too many redirects: %d hops, last addr=%s", e.Hops, e.LastAddr)
}

// NotFoundError indicates the requested record was not found (response status = 2).
type NotFoundError struct{}

func (e *NotFoundError) Error() string {
	return "not found"
}

// ErrorCodeString returns a human-readable name for an error code.
func ErrorCodeString(code uint16) string {
	switch code {
	case ErrCodeOK:
		return "OK"
	case ErrCodeTxNotFound:
		return "TX_NOT_FOUND"
	case ErrCodeUtxoHashMismatch:
		return "UTXO_HASH_MISMATCH"
	case ErrCodeAlreadySpent:
		return "ALREADY_SPENT"
	case ErrCodeAlreadyFrozen:
		return "ALREADY_FROZEN"
	case ErrCodeUtxoNotFrozen:
		return "UTXO_NOT_FROZEN"
	case ErrCodeInvalidSpend:
		return "INVALID_SPEND"
	case ErrCodeFrozen:
		return "FROZEN"
	case ErrCodeConflicting:
		return "CONFLICTING"
	case ErrCodeLocked:
		return "LOCKED"
	case ErrCodeCoinbaseImmature:
		return "COINBASE_IMMATURE"
	case ErrCodeVoutOutOfRange:
		return "VOUT_OUT_OF_RANGE"
	case ErrCodeAlreadyExists:
		return "ALREADY_EXISTS"
	case ErrCodeFrozenUntil:
		return "FROZEN_UNTIL"
	case ErrCodeRedirect:
		return "REDIRECT"
	case ErrCodeInternal:
		return "INTERNAL"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", code)
	}
}
