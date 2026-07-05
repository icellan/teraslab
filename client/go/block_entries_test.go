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

func TestDecodeAllBlockEntries(t *testing.T) {
	// 5 entries fully present on the wire (count == entries).
	payload := encodeBlockEntriesPayload(5, 5)
	entries, err := DecodeAllBlockEntries(payload)
	if err != nil {
		t.Fatalf("DecodeAllBlockEntries: %v", err)
	}
	if len(entries) != 5 {
		t.Fatalf("entries = %d, want 5", len(entries))
	}
	for i, e := range entries {
		wantID := uint32(100 + i)
		wantHeight := uint32(200 + i)
		wantSubtree := uint32(i)
		if e.BlockID != wantID || e.BlockHeight != wantHeight || e.SubtreeIdx != wantSubtree {
			t.Fatalf("entry %d = %+v, want {ID:%d Height:%d Subtree:%d}", i, e, wantID, wantHeight, wantSubtree)
		}
	}

	// Truncated payload: declares 5, only 3 present.
	short := encodeBlockEntriesPayload(5, 3)
	if _, err := DecodeAllBlockEntries(short); err == nil {
		t.Fatal("expected truncation error for count=5 with only 3 entries")
	}
}

func TestDecodeRecordBlockEntriesAllFullSet(t *testing.T) {
	// FieldBlockEntriesAll with 5 entries all present → full slice, not truncated.
	payload := encodeBlockEntriesPayload(5, 5)
	rec, err := decodeRecord(FieldBlockEntriesAll, payload)
	if err != nil {
		t.Fatalf("decodeRecord: %v", err)
	}
	if len(rec.BlockEntries) != 5 {
		t.Fatalf("BlockEntries = %d, want 5", len(rec.BlockEntries))
	}
	if rec.BlockEntriesTruncated {
		t.Fatal("expected BlockEntriesTruncated = false for FieldBlockEntriesAll")
	}
	// Table assert on decoded values.
	for i, e := range rec.BlockEntries {
		if e.BlockID != uint32(100+i) || e.BlockHeight != uint32(200+i) || e.SubtreeIdx != uint32(i) {
			t.Fatalf("entry %d = %+v, unexpected", i, e)
		}
	}

	// The old FieldBlockEntries path is unchanged: still caps + flags.
	rec2, err := decodeRecord(FieldBlockEntries, encodeBlockEntriesPayload(5, MaxInlineBlockEntries))
	if err != nil {
		t.Fatalf("decodeRecord (inline): %v", err)
	}
	if len(rec2.BlockEntries) != MaxInlineBlockEntries || !rec2.BlockEntriesTruncated {
		t.Fatalf("FieldBlockEntries path changed: entries=%d truncated=%v", len(rec2.BlockEntries), rec2.BlockEntriesTruncated)
	}
}
