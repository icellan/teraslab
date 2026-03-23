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
)

// Read opcodes.
const (
	OpGetBatch      uint16 = 20
	OpGetSpendBatch uint16 = 21
)

// Pruner opcodes.
const (
	OpQueryOldUnmined            uint16 = 30
	OpPreserveTransactions       uint16 = 31
	OpProcessExpiredPreservations uint16 = 32
)

// Cluster / admin opcodes.
const (
	OpGetPartitionMap uint16 = 100
	OpHealth          uint16 = 101
	OpPing            uint16 = 102
)

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
)

// Error codes shared across all batch operations.
const (
	ErrCodeOK              uint16 = 0
	ErrCodeTxNotFound      uint16 = 1
	ErrCodeUtxoHashMismatch uint16 = 2
	ErrCodeAlreadySpent    uint16 = 3
	ErrCodeAlreadyFrozen   uint16 = 4
	ErrCodeUtxoNotFrozen   uint16 = 5
	ErrCodeInvalidSpend    uint16 = 6
	ErrCodeFrozen          uint16 = 7
	ErrCodeConflicting     uint16 = 8
	ErrCodeLocked          uint16 = 9
	ErrCodeCoinbaseImmature uint16 = 10
	ErrCodeVoutOutOfRange  uint16 = 11
	ErrCodeAlreadyExists   uint16 = 12
	ErrCodeFrozenUntil     uint16 = 13
	ErrCodeRedirect        uint16 = 14
	ErrCodeInternal        uint16 = 255
)

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
	SignalNone               uint8 = 0
	SignalAllSpent           uint8 = 1
	SignalNotAllSpent        uint8 = 2
	SignalDeleteAtHeightSet  uint8 = 3
	SignalDeleteAtHeightUnset uint8 = 4
	SignalPreserve           uint8 = 5
)

// MaxFrameSize is the maximum frame payload size (16 MiB).
const MaxFrameSize = 16 * 1024 * 1024

// NumShards is the number of shards in the cluster hash table.
const NumShards = 4096
