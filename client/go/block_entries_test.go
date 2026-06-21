package teraslab

import "testing"

func encodeBlockEntriesPayload(total int, inline int) []byte {
	buf := []byte{byte(total)}
	for i := 0; i < inline; i++ {
		buf = appendU32(buf, uint32(100+i)) // block id
		buf = appendU32(buf, uint32(200+i)) // block height
		buf = appendU32(buf, uint32(i))     // subtree idx
	}
	return buf
}

func TestDecodeBlockEntriesWithCount(t *testing.T) {
	t.Run("within inline limit", func(t *testing.T) {
		payload := encodeBlockEntriesPayload(2, 2)
		entries, total, err := DecodeBlockEntriesWithCount(payload)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if total != 2 || len(entries) != 2 {
			t.Fatalf("total=%d entries=%d, want 2/2", total, len(entries))
		}
		if entries[0].BlockID != 100 || entries[1].BlockHeight != 201 {
			t.Fatalf("decoded entries wrong: %+v", entries)
		}
	})

	t.Run("truncated beyond inline limit", func(t *testing.T) {
		// Declares 5 entries; only MaxInlineBlockEntries are on the wire.
		payload := encodeBlockEntriesPayload(5, MaxInlineBlockEntries)
		entries, total, err := DecodeBlockEntriesWithCount(payload)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if total != 5 {
			t.Fatalf("total = %d, want 5", total)
		}
		if len(entries) != MaxInlineBlockEntries {
			t.Fatalf("entries = %d, want %d", len(entries), MaxInlineBlockEntries)
		}
		if total <= len(entries) {
			t.Fatalf("expected truncation (total %d > entries %d)", total, len(entries))
		}
	})
}

func TestDecodeRecordBlockEntriesTruncatedFlag(t *testing.T) {
	// Build a minimal record payload with only FieldBlockEntries set, declaring
	// more entries than fit inline.
	payload := encodeBlockEntriesPayload(7, MaxInlineBlockEntries)
	rec, err := decodeRecord(FieldBlockEntries, payload)
	if err != nil {
		t.Fatalf("decodeRecord: %v", err)
	}
	if len(rec.BlockEntries) != MaxInlineBlockEntries {
		t.Fatalf("BlockEntries = %d, want %d", len(rec.BlockEntries), MaxInlineBlockEntries)
	}
	if !rec.BlockEntriesTruncated {
		t.Fatal("expected BlockEntriesTruncated = true")
	}
}
