package teraslab

// Operation codes for the TeraSlab binary wire protocol.
// Every operation has a batch variant. Single-item ops are batches of size 1.

// Mutation opcodes.
const (
	OpSpendBatch            uint16 = 1
	OpUnspendBatch          uint16 = 2
	OpSetMinedBatch         uint16 = 3
	OpCreateBatch           uint16 = 4
	OpFreezeBatch           uint16 = 5
	OpUnfreezeBatch         uint16 = 6
	OpReassignBatch         uint16 = 7
	OpSetConflictingBatch   uint16 = 8
	OpSetLockedBatch        uint16 = 9
	OpPreserveUntilBatch    uint16 = 10
	OpDeleteBatch           uint16 = 11
	OpMarkLongestChainBatch uint16 = 12

	// OpRemoveConflictingChildBatch removes (parent, child) links from
	// parents' conflicting-children lists. Routed by the PARENT txid.
	OpRemoveConflictingChildBatch uint16 = 13
)

// Read opcodes.
const (
	OpGetBatch      uint16 = 20
	OpGetSpendBatch uint16 = 21
)

// Pruner opcodes.
const (
	OpQueryOldUnmined             uint16 = 30
	OpPreserveTransactions        uint16 = 31
	OpProcessExpiredPreservations uint16 = 32

	// OpQueryConflicting returns all txids currently flagged CONFLICTING.
	// Request payload is empty; response mirrors QueryOldUnmined.
	OpQueryConflicting uint16 = 33
)

// Cluster / admin opcodes.
const (
	OpGetPartitionMap uint16 = 100
	OpHealth          uint16 = 101
	OpPing            uint16 = 102

	// OpHello negotiates the wire protocol version. Request payload is empty;
	// the response is StatusOK + a 2-byte LE protocol version. Servers that
	// predate the handshake reply with ErrCodeOpcodeUnsupported.
	OpHello uint16 = 107
)

// ProtocolVersion is the wire protocol version this client implements.
// Matches src/protocol/opcodes.rs PROTOCOL_VERSION.
const ProtocolVersion uint16 = 2

// Streaming blob upload opcodes.
const (
	OpStreamChunk uint16 = 200
	OpStreamEnd   uint16 = 201
)

// Blob upload thresholds.
const (
	// BlobUploadThreshold is the cold_data size (in bytes) above which
	// CreateBatch will pre-upload the data via chunked streaming instead
	// of inlining it in the batch payload.
	BlobUploadThreshold = 1024 * 1024 // 1 MiB

	// BlobChunkSize is the maximum payload size per OP_STREAM_CHUNK request.
	BlobChunkSize = 4 * 1024 * 1024 // 4 MiB
)

// Response status codes.
const (
	StatusOK           uint8 = 0
	StatusError        uint8 = 1
	StatusNotFound     uint8 = 2
	StatusRedirect     uint8 = 3
	StatusPartialError uint8 = 4
	// StatusDegradedDurability indicates the mutation was applied but
	// replicated under a weakened acknowledgement policy (e.g. quorum not
	// fully met). The data is committed locally; callers should treat it as a
	// successful-but-weak ack, not an error.
	StatusDegradedDurability uint8 = 5
)

// Error codes shared across all batch operations. Mirrors
// src/protocol/opcodes.rs (ERR_* constants).
const (
	ErrCodeOK               uint16 = 0
	ErrCodeTxNotFound       uint16 = 1
	ErrCodeUtxoHashMismatch uint16 = 2
	ErrCodeAlreadySpent     uint16 = 3
	ErrCodeAlreadyFrozen    uint16 = 4
	ErrCodeUtxoNotFrozen    uint16 = 5
	ErrCodeInvalidSpend     uint16 = 6
	ErrCodeFrozen           uint16 = 7
	ErrCodeConflicting      uint16 = 8
	ErrCodeLocked           uint16 = 9
	ErrCodeCoinbaseImmature uint16 = 10
	ErrCodeVoutOutOfRange   uint16 = 11
	ErrCodeAlreadyExists    uint16 = 12
	ErrCodeFrozenUntil      uint16 = 13
	ErrCodeRedirect         uint16 = 14

	// Cluster / streaming / operational error codes (15-37).
	ErrCodeNoQuorum               uint16 = 15
	ErrCodeStreamNotFound         uint16 = 16
	ErrCodeBlobNotFound           uint16 = 17
	ErrCodeStreamOffsetMismatch   uint16 = 18
	ErrCodeMigrationInProgress    uint16 = 19
	ErrCodeReplicationFailed      uint16 = 20
	ErrCodeMigrationManifest      uint16 = 21
	ErrCodeMigrationManifestStale uint16 = 22
	ErrCodeTopologyPersistFailed  uint16 = 23
	ErrCodeStaleEpoch             uint16 = 24
	ErrCodeClusterNotReady        uint16 = 25
	ErrCodeIndexDegraded          uint16 = 26
	ErrCodeClusterAuthFailed      uint16 = 27
	ErrCodePayloadMalformed       uint16 = 28
	ErrCodeOpcodeUnsupported      uint16 = 29
	ErrCodeStorageIO              uint16 = 30
	ErrCodeRateLimited            uint16 = 31
	ErrCodeNotClustered           uint16 = 32
	ErrCodeInvariantViolation     uint16 = 33
	ErrCodeStreamInvariant        uint16 = 34
	ErrCodeDeletedChildren        uint16 = 35
	ErrCodeNotDue                 uint16 = 36
	ErrCodeMigrationTargetNotReady uint16 = 37

	ErrCodeInternal uint16 = 255
)

// isRetryableErrorCode reports whether a server error code denotes a transient
// condition that is safe to retry against the SAME target node after a backoff.
// Mirrors client/rust is_retryable_error_code: migration-in-progress and
// stale-epoch are same-target transients; replication-failed is an ambiguous
// outcome that is only safe to retry for idempotent operations.
//
// ErrCodeNoQuorum is intentionally NOT included here — it is handled by a
// partition-map refresh + single retry, not same-target backoff.
func isRetryableErrorCode(code uint16) bool {
	switch code {
	case ErrCodeMigrationInProgress, ErrCodeStaleEpoch, ErrCodeReplicationFailed:
		return true
	default:
		return false
	}
}

// Field mask bits for GetBatch requests. Each bit selects a single field.
const (
	FieldTxVersion           uint32 = 1 << 0
	FieldLocktime            uint32 = 1 << 1
	FieldFee                 uint32 = 1 << 2
	FieldSizeInBytes         uint32 = 1 << 3
	FieldExtendedSize        uint32 = 1 << 4
	FieldFlags               uint32 = 1 << 5
	FieldSpendingHeight      uint32 = 1 << 6
	FieldCreatedAt           uint32 = 1 << 7
	FieldSpentUtxos          uint32 = 1 << 8
	FieldPrunedUtxos         uint32 = 1 << 9
	FieldUtxoCount           uint32 = 1 << 10
	FieldGeneration          uint32 = 1 << 11
	FieldUpdatedAt           uint32 = 1 << 12
	FieldUnminedSince        uint32 = 1 << 13
	FieldDeleteAtHeight      uint32 = 1 << 14
	FieldPreserveUntil       uint32 = 1 << 15
	FieldExternalRef         uint32 = 1 << 16
	FieldReassignCount       uint32 = 1 << 17
	FieldBlockEntryCount     uint32 = 1 << 18
	FieldUtxoSlots           uint32 = 1 << 19
	FieldColdData            uint32 = 1 << 20
	FieldBlockEntries        uint32 = 1 << 21
	FieldConflictingChildren uint32 = 1 << 22
	// FieldRawMetadata returns the full 256-byte on-disk metadata struct
	// as-is, including internal fields (magic, schema_version, device
	// offsets, padding). For debugging only. Takes precedence over
	// individual metadata field bits if set.
	FieldRawMetadata uint32 = 1 << 23

	// FieldAllMetadata selects all metadata fields (bits 0-18).
	FieldAllMetadata uint32 = 0x0007_FFFF
	// FieldAll includes all client-facing fields (bits 0-22, excludes FieldRawMetadata).
	FieldAll uint32 = 0x007F_FFFF
)

// UTXO slot status values.
const (
	SlotUnspent uint8 = 0x00
	SlotSpent   uint8 = 0x01
	SlotPruned  uint8 = 0x02
	SlotFrozen  uint8 = 0xFF
)

// Record flag bits.
const (
	// FlagExternalBlob indicates that cold_data was pre-uploaded to the
	// blobstore via OP_STREAM_CHUNK / OP_STREAM_END and should not be
	// inlined in the CreateBatch payload.
	FlagExternalBlob uint8 = 0x08
)

// Signal values returned by spend/setMined operations.
const (
	SignalNone                uint8 = 0
	SignalAllSpent            uint8 = 1
	SignalNotAllSpent         uint8 = 2
	SignalDeleteAtHeightSet   uint8 = 3
	SignalDeleteAtHeightUnset uint8 = 4
	SignalPreserve            uint8 = 5
)

// MaxFrameSize is the maximum frame payload size (16 MiB).
const MaxFrameSize = 16 * 1024 * 1024

// NumShards is the number of shards in the cluster hash table.
const NumShards = 4096
