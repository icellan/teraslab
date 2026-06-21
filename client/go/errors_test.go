package teraslab

import (
	"errors"
	"testing"
)

func TestBatchItemErrorFormat(t *testing.T) {
	e := &BatchItemError{ItemIndex: 3, Code: ErrCodeTxNotFound}
	if e.Error() != "item 3: TX_NOT_FOUND" {
		t.Errorf("unexpected: %s", e.Error())
	}
}

func TestPartialErrorFormat(t *testing.T) {
	e := &PartialError{
		Successes: make([]BatchItemSuccess, 8),
		Errors:    make([]BatchItemError, 2),
	}
	if e.Error() != "partial error: 2 of 10 items failed" {
		t.Errorf("unexpected: %s", e.Error())
	}
}

func TestServerErrorFormat(t *testing.T) {
	e := &ServerError{Code: ErrCodeInternal, Message: "disk full"}
	if e.Error() != "server error INTERNAL: disk full" {
		t.Errorf("unexpected: %s", e.Error())
	}
}

func TestRedirectErrorFormat(t *testing.T) {
	e := &RedirectError{Addr: "192.168.1.10:3300"}
	if e.Error() != "redirect to 192.168.1.10:3300" {
		t.Errorf("unexpected: %s", e.Error())
	}
}

func TestNotFoundErrorFormat(t *testing.T) {
	e := &NotFoundError{}
	if e.Error() != "not found" {
		t.Errorf("unexpected: %s", e.Error())
	}
}

func TestErrorsAs(t *testing.T) {
	var err error = &PartialError{
		Errors: []BatchItemError{{ItemIndex: 0, Code: ErrCodeAlreadySpent}},
	}

	var pe *PartialError
	if !errors.As(err, &pe) {
		t.Fatal("errors.As should match *PartialError")
	}
	if len(pe.Errors) != 1 {
		t.Fatalf("expected 1 error, got %d", len(pe.Errors))
	}
	if pe.Errors[0].Code != ErrCodeAlreadySpent {
		t.Errorf("expected ALREADY_SPENT, got %s", ErrorCodeString(pe.Errors[0].Code))
	}
}

func TestErrorCodeStringAll(t *testing.T) {
	codes := []struct {
		code uint16
		name string
	}{
		{ErrCodeOK, "OK"},
		{ErrCodeTxNotFound, "TX_NOT_FOUND"},
		{ErrCodeUtxoHashMismatch, "UTXO_HASH_MISMATCH"},
		{ErrCodeAlreadySpent, "ALREADY_SPENT"},
		{ErrCodeAlreadyFrozen, "ALREADY_FROZEN"},
		{ErrCodeUtxoNotFrozen, "UTXO_NOT_FROZEN"},
		{ErrCodeInvalidSpend, "INVALID_SPEND"},
		{ErrCodeFrozen, "FROZEN"},
		{ErrCodeConflicting, "CONFLICTING"},
		{ErrCodeLocked, "LOCKED"},
		{ErrCodeCoinbaseImmature, "COINBASE_IMMATURE"},
		{ErrCodeVoutOutOfRange, "VOUT_OUT_OF_RANGE"},
		{ErrCodeAlreadyExists, "ALREADY_EXISTS"},
		{ErrCodeFrozenUntil, "FROZEN_UNTIL"},
		{ErrCodeRedirect, "REDIRECT"},
		{ErrCodeNoQuorum, "NO_QUORUM"},
		{ErrCodeStreamNotFound, "STREAM_NOT_FOUND"},
		{ErrCodeBlobNotFound, "BLOB_NOT_FOUND"},
		{ErrCodeStreamOffsetMismatch, "STREAM_OFFSET_MISMATCH"},
		{ErrCodeMigrationInProgress, "MIGRATION_IN_PROGRESS"},
		{ErrCodeReplicationFailed, "REPLICATION_FAILED"},
		{ErrCodeMigrationManifest, "MIGRATION_MANIFEST"},
		{ErrCodeMigrationManifestStale, "MIGRATION_MANIFEST_STALE"},
		{ErrCodeTopologyPersistFailed, "TOPOLOGY_PERSIST_FAILED"},
		{ErrCodeStaleEpoch, "STALE_EPOCH"},
		{ErrCodeClusterNotReady, "CLUSTER_NOT_READY"},
		{ErrCodeIndexDegraded, "INDEX_DEGRADED"},
		{ErrCodeClusterAuthFailed, "CLUSTER_AUTH_FAILED"},
		{ErrCodePayloadMalformed, "PAYLOAD_MALFORMED"},
		{ErrCodeOpcodeUnsupported, "OPCODE_UNSUPPORTED"},
		{ErrCodeStorageIO, "STORAGE_IO"},
		{ErrCodeRateLimited, "RATE_LIMITED"},
		{ErrCodeNotClustered, "NOT_CLUSTERED"},
		{ErrCodeInvariantViolation, "INVARIANT_VIOLATION"},
		{ErrCodeStreamInvariant, "STREAM_INVARIANT"},
		{ErrCodeDeletedChildren, "DELETED_CHILDREN"},
		{ErrCodeNotDue, "NOT_DUE"},
		{ErrCodeMigrationTargetNotReady, "MIGRATION_TARGET_NOT_READY"},
		{ErrCodeInternal, "INTERNAL"},
		{99, "UNKNOWN(99)"},
	}
	for _, tc := range codes {
		got := ErrorCodeString(tc.code)
		if got != tc.name {
			t.Errorf("ErrorCodeString(%d) = %q, want %q", tc.code, got, tc.name)
		}
	}
}

func TestIsRetryableErrorCode(t *testing.T) {
	retryable := []uint16{ErrCodeMigrationInProgress, ErrCodeStaleEpoch, ErrCodeReplicationFailed}
	for _, code := range retryable {
		if !isRetryableErrorCode(code) {
			t.Errorf("isRetryableErrorCode(%s) = false, want true", ErrorCodeString(code))
		}
	}
	notRetryable := []uint16{
		ErrCodeOK, ErrCodeTxNotFound, ErrCodeRedirect, ErrCodeNoQuorum,
		ErrCodeConflicting, ErrCodeClusterNotReady, ErrCodeInternal,
	}
	for _, code := range notRetryable {
		if isRetryableErrorCode(code) {
			t.Errorf("isRetryableErrorCode(%s) = true, want false", ErrorCodeString(code))
		}
	}
}
