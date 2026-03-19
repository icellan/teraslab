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

// Field mask bits for GetBatch requests.
const (
	FieldMetadata     uint16 = 0x0001
	FieldUtxoSlots    uint16 = 0x0002
	FieldColdData     uint16 = 0x0004
	FieldBlockEntries uint16 = 0x0008
	FieldAll          uint16 = 0x000F
)

// UTXO slot status values.
const (
	SlotUnspent uint8 = 0x00
	SlotSpent   uint8 = 0x01
	SlotPruned  uint8 = 0x02
	SlotFrozen  uint8 = 0xFF
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
