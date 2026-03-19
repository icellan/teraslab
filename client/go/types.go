package teraslab

// TxID is a 32-byte transaction identifier (double-SHA256 hash).
type TxID [32]byte

// UtxoHash is a 32-byte UTXO hash (SHA256).
type UtxoHash [32]byte

// SpendingData is 36 bytes: spending txid (32) + vin index (4 LE).
type SpendingData [36]byte

// SpendItem is a single item in a SpendBatch request.
type SpendItem struct {
	TxID         TxID
	Vout         uint32
	UtxoHash     UtxoHash
	SpendingData SpendingData
}

// SpendBatchParams are shared parameters for a SpendBatch request.
type SpendBatchParams struct {
	IgnoreConflicting    bool
	IgnoreLocked         bool
	CurrentBlockHeight   uint32
	BlockHeightRetention uint32
}

// UnspendItem is a single item in an UnspendBatch request.
type UnspendItem struct {
	TxID     TxID
	Vout     uint32
	UtxoHash UtxoHash
}

// UnspendBatchParams are shared parameters for an UnspendBatch request.
type UnspendBatchParams struct {
	CurrentBlockHeight   uint32
	BlockHeightRetention uint32
}

// SetMinedBatchParams are shared parameters for a SetMinedBatch request.
type SetMinedBatchParams struct {
	BlockID              uint32
	BlockHeight          uint32
	SubtreeIdx           uint32
	OnLongestChain       bool
	UnsetMined           bool
	CurrentBlockHeight   uint32
	BlockHeightRetention uint32
}

// TxData holds the three components of transaction data stored by TeraSlab.
// Each field contains the raw serialized bytes as provided during creation.
type TxData struct {
	Inputs   []byte // Serialized transaction inputs
	Outputs  []byte // Serialized transaction outputs
	Inpoints []byte // Serialized transaction inpoints (parent txid + vout references)
}

// CreateItem is a single item in a CreateBatch request.
type CreateItem struct {
	TxID             TxID
	TxVersion        uint32
	Locktime         uint32
	Fee              uint64
	SizeInBytes      uint64
	ExtendedSize     uint64
	IsCoinbase       bool
	SpendingHeight   uint32
	CreatedAt        uint64
	Flags            uint8
	UtxoHashes       []UtxoHash
	TxData           TxData
	MinedBlockID     *uint32
	MinedBlockHeight *uint32
	MinedSubtreeIdx  *uint32
	// ParentTxIDs lists parent txids from the transaction's inputs.
	// The server uses these to update each parent's conflicting-children
	// list when creating a conflicting tx.
	ParentTxIDs []TxID
}

// FreezeItem is a single item in a Freeze/Unfreeze batch request.
type FreezeItem struct {
	TxID     TxID
	Vout     uint32
	UtxoHash UtxoHash
}

// ReassignItem is a single item in a ReassignBatch request.
type ReassignItem struct {
	TxID        TxID
	Vout        uint32
	UtxoHash    UtxoHash
	NewUtxoHash UtxoHash
}

// ReassignBatchParams are shared parameters for a ReassignBatch request.
type ReassignBatchParams struct {
	BlockHeight    uint32
	SpendableAfter uint32
}

// SetConflictingParams are shared parameters for a SetConflictingBatch request.
type SetConflictingParams struct {
	Value                bool
	CurrentBlockHeight   uint32
	BlockHeightRetention uint32
}

// MarkLongestChainParams are shared parameters for a MarkLongestChainBatch request.
type MarkLongestChainParams struct {
	OnLongestChain       bool
	CurrentBlockHeight   uint32
	BlockHeightRetention uint32
}

// GetSpendItem is a single item in a GetSpendBatch request.
type GetSpendItem struct {
	TxID TxID
	Vout uint32
}

// BatchItemSuccess is a per-item success result with signal and block IDs,
// returned by Spend/SetMined operations.
type BatchItemSuccess struct {
	ItemIndex uint32
	Signal    uint8
	BlockIDs  []uint32
}

// GetResult is a single item in a GetBatch response.
type GetResult struct {
	// Status is 0 for success, 1 for error (e.g. not found).
	Status uint8
	// Data contains the field-mask-selected serialized record data.
	Data []byte
}

// GetSpendResult is a single item in a GetSpendBatch response.
type GetSpendResult struct {
	Status       uint8
	ErrorCode    uint16
	SlotStatus   uint8
	SpendingData SpendingData
}

// SpendBatchResponse captures both success signals and per-item errors
// from a SpendBatch or SetMinedBatch operation.
type SpendBatchResponse struct {
	Successes []BatchItemSuccess
	Errors    []BatchItemError
}

// BatchResult is the generic response for mutation batch operations.
// Errors is nil when all items succeed.
type BatchResult struct {
	Errors []BatchItemError
}

// ProcessExpiredResult is the response from ProcessExpiredPreservations.
type ProcessExpiredResult struct {
	Deleted uint32
	Failed  uint32
}

// PartitionMap describes the cluster topology for client-side routing.
type PartitionMap struct {
	Version     uint64
	Nodes       []NodeInfo
	Assignments [NumShards]uint64 // shard index -> master node ID
}

// NodeInfo describes a single node in the cluster.
type NodeInfo struct {
	ID   uint64
	Addr string
}

// TxMetadata contains parsed transaction metadata from a GetBatch response.
type TxMetadata struct {
	TxVersion      uint32
	Locktime       uint32
	Fee            uint64
	SizeInBytes    uint64
	ExtendedSize   uint64
	Flags          uint8
	SpendingHeight uint32
	CreatedAt      uint64
	SpentUtxos     uint32
	PrunedUtxos    uint32
	UtxoCount      uint32
	Generation     uint32
	UpdatedAt      uint64
	UnminedSince   uint32
	DeleteAtHeight uint32
	PreserveUntil  uint32
}

// UtxoSlot represents a single UTXO slot from a GetBatch response.
type UtxoSlot struct {
	Hash         UtxoHash
	Status       uint8
	SpendingData SpendingData
}

// BlockEntry represents a single block entry from a GetBatch response.
type BlockEntry struct {
	BlockID     uint32
	BlockHeight uint32
	SubtreeIdx  uint32
}
