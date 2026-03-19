package teraslab

import (
	"context"
	"fmt"
)

// TxRecord is the fully-decoded result of a GetBatch call for a single
// transaction.  It contains all the sections that the server may return
// depending on the field mask, already parsed into Go structs.
type TxRecord struct {
	// Found is false when the server returned status != 0 (e.g. not found).
	Found bool

	// Metadata is non-nil when FieldMetadata was requested and the record exists.
	Metadata *TxMetadata

	// Slots contains one entry per UTXO output.  Non-nil when FieldUtxoSlots
	// was requested and the record exists.
	Slots []UtxoSlot

	// TxData holds the stored transaction inputs, outputs, and inpoints.
	// Non-nil when FieldColdData was requested and data was stored.
	TxData *TxData

	// BlockEntries lists the blocks this transaction has been mined into.
	// Non-nil when FieldBlockEntries was requested and the record exists.
	BlockEntries []BlockEntry

	// ConflictingChildren contains txids of transactions that were created
	// as conflicting and reference this transaction's UTXOs as inputs.
	// Non-nil when FieldConflictingChildren was requested.
	ConflictingChildren []TxID
}

// GetRecordBatch is a high-level wrapper around GetBatch that decodes the raw
// wire response into structured TxRecord values.  The returned slice is
// positionally aligned with the input txids slice.
func (c *Client) GetRecordBatch(ctx context.Context, fieldMask uint16, txids []TxID) ([]TxRecord, error) {
	raw, err := c.GetBatch(ctx, fieldMask, txids)
	if err != nil {
		return nil, err
	}

	records := make([]TxRecord, len(raw))
	for i, r := range raw {
		if r.Status != 0 {
			continue
		}
		rec, err := decodeRecord(fieldMask, r.Data)
		if err != nil {
			return nil, fmt.Errorf("item %d: %w", i, err)
		}
		records[i] = rec
	}
	return records, nil
}

// decodeRecord parses the concatenated field sections in a single GetResult.Data
// blob according to the field mask that was used in the request.  The sections
// appear in a fixed order: metadata, utxo_slots, cold_data, block_entries —
// but only sections whose bit is set in fieldMask are present.
func decodeRecord(fieldMask uint16, data []byte) (TxRecord, error) {
	rec := TxRecord{Found: true}
	pos := 0

	if fieldMask&FieldMetadata != 0 {
		if pos+MetadataSize > len(data) {
			return rec, fmt.Errorf("metadata section truncated: need %d, have %d", MetadataSize, len(data)-pos)
		}
		md, err := DecodeTxMetadata(data[pos:])
		if err != nil {
			return rec, err
		}
		rec.Metadata = md
		pos += MetadataSize
	}

	if fieldMask&FieldUtxoSlots != 0 {
		if pos+4 > len(data) {
			return rec, fmt.Errorf("utxo slots section truncated")
		}
		slots, err := DecodeUtxoSlots(data[pos:])
		if err != nil {
			return rec, err
		}
		count := int(getU32(data[pos : pos+4]))
		pos += 4 + count*69
		rec.Slots = slots
	}

	if fieldMask&FieldColdData != 0 {
		if pos+4 > len(data) {
			return rec, fmt.Errorf("cold data section truncated")
		}
		coldLen := int(getU32(data[pos : pos+4]))
		pos += 4
		if coldLen > 0 {
			if pos+coldLen > len(data) {
				return rec, fmt.Errorf("cold data truncated: need %d, have %d", coldLen, len(data)-pos)
			}
			txData, err := decodeTxData(data[pos : pos+coldLen])
			if err != nil {
				return rec, fmt.Errorf("tx data: %w", err)
			}
			rec.TxData = txData
			pos += coldLen
		}
	}

	if fieldMask&FieldBlockEntries != 0 {
		if pos < len(data) {
			entries, err := DecodeBlockEntries(data[pos:])
			if err != nil {
				return rec, err
			}
			rec.BlockEntries = entries
			// Advance past: 1-byte count + min(count, 3) * 12 bytes
			count := int(data[pos])
			inlineCount := count
			if inlineCount > 3 {
				inlineCount = 3
			}
			pos += 1 + inlineCount*12
		}
	}

	if fieldMask&FieldConflictingChildren != 0 {
		if pos < len(data) {
			count := int(data[pos])
			pos++
			if count > 0 && pos+count*32 <= len(data) {
				rec.ConflictingChildren = make([]TxID, count)
				for i := 0; i < count; i++ {
					copy(rec.ConflictingChildren[i][:], data[pos:pos+32])
					pos += 32
				}
			}
		}
	}

	return rec, nil
}

// decodeTxData parses the server's cold data format into a TxData struct.
// Format: [inputs_len:4 LE][inputs][outputs_len:4 LE][outputs][inpoints_len:4 LE][inpoints]
func decodeTxData(data []byte) (*TxData, error) {
	if len(data) < 12 {
		return nil, fmt.Errorf("tx data too short: need 12, have %d", len(data))
	}
	pos := 0

	inputsLen := int(getU32(data[pos : pos+4]))
	pos += 4
	if pos+inputsLen > len(data) {
		return nil, fmt.Errorf("inputs truncated")
	}
	inputs := make([]byte, inputsLen)
	copy(inputs, data[pos:pos+inputsLen])
	pos += inputsLen

	if pos+4 > len(data) {
		return nil, fmt.Errorf("outputs length truncated")
	}
	outputsLen := int(getU32(data[pos : pos+4]))
	pos += 4
	if pos+outputsLen > len(data) {
		return nil, fmt.Errorf("outputs truncated")
	}
	outputs := make([]byte, outputsLen)
	copy(outputs, data[pos:pos+outputsLen])
	pos += outputsLen

	if pos+4 > len(data) {
		return nil, fmt.Errorf("inpoints length truncated")
	}
	inpointsLen := int(getU32(data[pos : pos+4]))
	pos += 4
	if pos+inpointsLen > len(data) {
		return nil, fmt.Errorf("inpoints truncated")
	}
	inpoints := make([]byte, inpointsLen)
	copy(inpoints, data[pos:pos+inpointsLen])

	return &TxData{
		Inputs:   inputs,
		Outputs:  outputs,
		Inpoints: inpoints,
	}, nil
}
