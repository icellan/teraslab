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
//
// SpendingData must match the spending_data recorded when the UTXO was spent
// (or be all-zero if no enforcement is required). The server uses this field
// to authorize the unspend: an unspend request with a mismatching
// spending_data is rejected. See audit fix A-04 ("unauthorized erasure").
type UnspendItem struct {
	TxID         TxID
	Vout         uint32
	UtxoHash     UtxoHash
	SpendingData SpendingData
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
	BlockHeight      uint32 // Current block height at creation time (sets unmined_since for non-mined txs)
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
//
// UtxoHash identifies the specific UTXO slot to look up; the server uses
// it (alongside txid+vout) to disambiguate slots and detect stale lookups.
// Wire layout: txid(32) + vout(4 LE) + utxo_hash(32) = 68 bytes per item.
type GetSpendItem struct {
	TxID     TxID
	Vout     uint32
	UtxoHash UtxoHash
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

// fieldSizes maps per-field metadata bits (0-18) to their byte sizes.
var fieldSizes = [19]int{
	4,  // 0: tx_version
	4,  // 1: locktime
	8,  // 2: fee
	8,  // 3: size_in_bytes
	8,  // 4: extended_size
	1,  // 5: flags
	4,  // 6: spending_height
	8,  // 7: created_at
	4,  // 8: spent_utxos
	4,  // 9: pruned_utxos
	4,  // 10: utxo_count
	4,  // 11: generation
	8,  // 12: updated_at
	4,  // 13: unmined_since
	4,  // 14: delete_at_height
	4,  // 15: preserve_until
	65, // 16: external_ref
	1,  // 17: reassignment_count
	1,  // 18: block_entry_count
}

// fieldOffset computes the byte offset of targetBit within response data
// encoded with fieldMask. Returns -1 if the bit is not set or >= 19.
func fieldOffset(fieldMask uint32, targetBit int) int {
	if targetBit >= 19 || fieldMask&(1<<targetBit) == 0 {
		return -1
	}
	offset := 0
	for bit := 0; bit < targetBit; bit++ {
		if fieldMask&(1<<bit) != 0 {
			offset += fieldSizes[bit]
		}
	}
	return offset
}

// GetBatchResult bundles the field mask with the per-item results from a
// GetBatch call. This allows zero-alloc field accessors that read directly
// from the wire bytes without allocating a TxMetadata struct.
//
// Zero-alloc access:
//
//	batch, _ := client.GetBatch(ctx, FieldSpentUtxos|FieldFlags, txids)
//	for i := range batch.Len() {
//	    spent, ok := batch.SpentUtxos(i)
//	    locked, ok := batch.IsLocked(i)
//	}
//
// Full decode when needed:
//
//	batch, _ := client.GetBatch(ctx, FieldAllMetadata, txids)
//	meta, _, _ := batch.DecodeMetadata(i)
type GetBatchResult struct {
	// FieldMask is the field mask used in the request.
	FieldMask uint32
	// Items contains per-item results, positionally aligned with the request txids.
	Items []GetResult
}

// Len returns the number of items in the batch.
func (b *GetBatchResult) Len() int { return len(b.Items) }

// Found returns true if item i was found (status == 0).
func (b *GetBatchResult) Found(i int) bool { return b.Items[i].Status == 0 }

// Item returns the raw GetResult for item i.
func (b *GetBatchResult) Item(i int) *GetResult { return &b.Items[i] }

// DecodeMetadata decodes the full TxMetadata for item i. Allocates.
// Returns (nil, 0, nil) if the record was not found.
func (b *GetBatchResult) DecodeMetadata(i int) (*TxMetadata, int, error) {
	if b.Items[i].Status != 0 {
		return nil, 0, nil
	}
	return DecodeTxMetadata(b.FieldMask, b.Items[i].Data)
}

// -- Zero-alloc scalar reads --

func (b *GetBatchResult) readU32(i int, fieldBit int) (uint32, bool) {
	item := &b.Items[i]
	if item.Status != 0 {
		return 0, false
	}
	off := fieldOffset(b.FieldMask, fieldBit)
	if off < 0 || off+4 > len(item.Data) {
		return 0, false
	}
	return getU32(item.Data[off : off+4]), true
}

func (b *GetBatchResult) readU64(i int, fieldBit int) (uint64, bool) {
	item := &b.Items[i]
	if item.Status != 0 {
		return 0, false
	}
	off := fieldOffset(b.FieldMask, fieldBit)
	if off < 0 || off+8 > len(item.Data) {
		return 0, false
	}
	return getU64(item.Data[off : off+8]), true
}

func (b *GetBatchResult) readU8(i int, fieldBit int) (uint8, bool) {
	item := &b.Items[i]
	if item.Status != 0 {
		return 0, false
	}
	off := fieldOffset(b.FieldMask, fieldBit)
	if off < 0 || off >= len(item.Data) {
		return 0, false
	}
	return item.Data[off], true
}

// -- Typed field accessors (zero-alloc) --

// TxVersion reads tx_version for item i (bit 0, u32).
func (b *GetBatchResult) TxVersion(i int) (uint32, bool) { return b.readU32(i, 0) }

// Locktime reads locktime for item i (bit 1, u32).
func (b *GetBatchResult) Locktime(i int) (uint32, bool) { return b.readU32(i, 1) }

// Fee reads fee for item i (bit 2, u64).
func (b *GetBatchResult) Fee(i int) (uint64, bool) { return b.readU64(i, 2) }

// SizeInBytes reads size_in_bytes for item i (bit 3, u64).
func (b *GetBatchResult) SizeInBytes(i int) (uint64, bool) { return b.readU64(i, 3) }

// ExtendedSize reads extended_size for item i (bit 4, u64).
func (b *GetBatchResult) ExtendedSize(i int) (uint64, bool) { return b.readU64(i, 4) }

// Flags reads the flags byte for item i (bit 5, u8).
func (b *GetBatchResult) Flags(i int) (uint8, bool) { return b.readU8(i, 5) }

// SpendingHeight reads spending_height for item i (bit 6, u32).
func (b *GetBatchResult) SpendingHeight(i int) (uint32, bool) { return b.readU32(i, 6) }

// CreatedAt reads created_at for item i (bit 7, u64).
func (b *GetBatchResult) CreatedAt(i int) (uint64, bool) { return b.readU64(i, 7) }

// SpentUtxos reads spent_utxos for item i (bit 8, u32).
func (b *GetBatchResult) SpentUtxos(i int) (uint32, bool) { return b.readU32(i, 8) }

// PrunedUtxos reads pruned_utxos for item i (bit 9, u32).
func (b *GetBatchResult) PrunedUtxos(i int) (uint32, bool) { return b.readU32(i, 9) }

// UtxoCount reads utxo_count for item i (bit 10, u32).
func (b *GetBatchResult) UtxoCount(i int) (uint32, bool) { return b.readU32(i, 10) }

// Generation reads generation for item i (bit 11, u32).
func (b *GetBatchResult) Generation(i int) (uint32, bool) { return b.readU32(i, 11) }

// UpdatedAt reads updated_at for item i (bit 12, u64).
func (b *GetBatchResult) UpdatedAt(i int) (uint64, bool) { return b.readU64(i, 12) }

// UnminedSince reads unmined_since for item i (bit 13, u32).
func (b *GetBatchResult) UnminedSince(i int) (uint32, bool) { return b.readU32(i, 13) }

// DeleteAtHeight reads delete_at_height for item i (bit 14, u32).
func (b *GetBatchResult) DeleteAtHeight(i int) (uint32, bool) { return b.readU32(i, 14) }

// PreserveUntil reads preserve_until for item i (bit 15, u32).
func (b *GetBatchResult) PreserveUntil(i int) (uint32, bool) { return b.readU32(i, 15) }

// ReassignCount reads reassignment_count for item i (bit 17, u8).
func (b *GetBatchResult) ReassignCount(i int) (uint8, bool) { return b.readU8(i, 17) }

// BlockEntryCount reads block_entry_count for item i (bit 18, u8).
func (b *GetBatchResult) BlockEntryCount(i int) (uint8, bool) { return b.readU8(i, 18) }

// -- Derived convenience accessors --

// IsConflicting checks the conflicting flag (flags bit 1).
func (b *GetBatchResult) IsConflicting(i int) (bool, bool) {
	f, ok := b.Flags(i)
	return f&0b0000_0010 != 0, ok
}

// IsLocked checks the locked flag (flags bit 2).
func (b *GetBatchResult) IsLocked(i int) (bool, bool) {
	f, ok := b.Flags(i)
	return f&0b0000_0100 != 0, ok
}

// IsCoinbase checks the coinbase flag (flags bit 0).
func (b *GetBatchResult) IsCoinbase(i int) (bool, bool) {
	f, ok := b.Flags(i)
	return f&0b0000_0001 != 0, ok
}

// IsMined checks if block_entry_count > 0.
func (b *GetBatchResult) IsMined(i int) (bool, bool) {
	c, ok := b.BlockEntryCount(i)
	return c > 0, ok
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

// ExternalRef is a reference to externally stored transaction data
// (large transactions stored in the blob store).
type ExternalRef struct {
	StoreType     uint8    // 0=inline, 1=local_file, 2=object_store
	ContentHash   [32]byte // Content hash (txID used as blob key)
	TotalSize     uint64   // Original blob size in bytes
	InputCount    uint32   // Number of inputs in the blob
	OutputCount   uint32   // Number of outputs in the blob
	InputsOffset  uint64   // Byte offset within blob for inputs section
	OutputsOffset uint64   // Byte offset within blob for outputs section
}

// TxMetadata contains parsed transaction metadata from a GetBatch response.
// Fields are populated according to which per-field bits (0-18) were set
// in the GetBatch field mask. Unrequested fields remain at their zero value.
type TxMetadata struct {
	TxVersion        uint32
	Locktime         uint32
	Fee              uint64
	SizeInBytes      uint64
	ExtendedSize     uint64
	Flags            uint8
	SpendingHeight   uint32
	CreatedAt        uint64
	SpentUtxos       uint32
	PrunedUtxos      uint32
	UtxoCount        uint32
	Generation       uint32
	UpdatedAt        uint64
	UnminedSince     uint32
	DeleteAtHeight   uint32
	PreserveUntil    uint32
	ExternalRef      ExternalRef
	ReassignCount    uint8
	BlockEntryCount  uint8
}

// TxMetadataRaw contains the full 256-byte on-disk metadata struct returned
// by FIELD_RAW_METADATA. Includes internal storage details for debugging.
//
// Decode with DecodeTxMetadataRaw.
type TxMetadataRaw struct {
	// Bytes is the raw 256-byte on-disk representation.
	Bytes [256]byte

	// Parsed convenience accessors for commonly inspected fields:
	Magic                    uint32
	SchemaVersion            uint32
	RecordSize               uint32
	UtxoCount                uint32
	TxID                     TxID
	Flags                    uint8
	SpentUtxos               uint32
	BlockEntryCount          uint8
	BlockOverflowOffset      uint64
	ReassignmentOffset       uint64
	ReassignmentCount        uint8
	ConflictingChildrenCount uint8
	ConflictingChildrenOffset uint64
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
