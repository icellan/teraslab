package teraslab

import (
	"encoding/binary"
	"runtime"
	"testing"
)

// buildSparseErrorPayload builds a STATUS_PARTIAL_ERROR payload in the sparse
// format the server actually sends for SpendBatch double-spends:
//
//	[count:4][ per item: index:4, code:2, dataLen:2, data:dataLen ]
//
// dataHead seeds the first bytes of the (single) error's data section. Those
// bytes are exactly what the signal-format decoder misreads as an element count
// once its success loop has walked off the rails, so they let us reproduce the
// pathological allocation deterministically.
func buildSparseErrorPayload(code uint16, data []byte) []byte {
	buf := make([]byte, 0, 8+len(data))
	var u32 [4]byte
	binary.LittleEndian.PutUint32(u32[:], 1) // one error
	buf = append(buf, u32[:]...)
	binary.LittleEndian.PutUint32(u32[:], 0) // item index 0
	buf = append(buf, u32[:]...)
	var u16 [2]byte
	binary.LittleEndian.PutUint16(u16[:], code)
	buf = append(buf, u16[:]...)
	binary.LittleEndian.PutUint16(u16[:], uint16(len(data)))
	buf = append(buf, u16[:]...)
	buf = append(buf, data...)
	return buf
}

// TestDecodePartialWithSignalsRejectsSparsePayloadCheaply is the regression test
// for the unbounded-allocation bug: handleSignalResponse speculatively decodes a
// PartialError payload with decodePartialWithSignals before falling back to
// decodeSparseErrors. For a real double-spend the payload is sparse, the signal
// decoder misreads it, and (before the fix) the misread element count drove a
// multi-gigabyte make(). The decoder must now reject it with an error and a
// negligible allocation.
func TestDecodePartialWithSignalsRejectsSparsePayloadCheaply(t *testing.T) {
	// 36 bytes of "spending data" whose leading bytes are 0xFF, so the
	// signal decoder's misread errorCount becomes enormous.
	data := make([]byte, 36)
	for i := range data {
		data[i] = 0xFF
	}
	payload := buildSparseErrorPayload(ErrCodeAlreadySpent, data)

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, _, err := decodePartialWithSignals(payload)
	runtime.ReadMemStats(&after)

	if err == nil {
		t.Fatal("expected decodePartialWithSignals to reject the sparse payload, got nil error")
	}

	const maxAllocBytes = 16 << 20 // 16 MiB — generous; pre-fix this was gigabytes
	if grew := after.TotalAlloc - before.TotalAlloc; grew > maxAllocBytes {
		t.Fatalf("decodePartialWithSignals allocated %d bytes decoding a %d-byte payload; unbounded allocation not guarded", grew, len(payload))
	}
}

// TestDecodeSparseErrorsStillDecodesValidPayload guards the success path: the
// same payload the signal decoder must reject is decoded correctly by the sparse
// decoder, so handleSignalResponse's fallback continues to work.
func TestDecodeSparseErrorsStillDecodesValidPayload(t *testing.T) {
	data := []byte{0x01, 0x02, 0x03, 0x04}
	payload := buildSparseErrorPayload(ErrCodeAlreadySpent, data)

	errs, err := decodeSparseErrors(payload)
	if err != nil {
		t.Fatalf("decodeSparseErrors failed on a valid payload: %v", err)
	}
	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}
	if errs[0].Code != ErrCodeAlreadySpent {
		t.Fatalf("expected code %d, got %d", ErrCodeAlreadySpent, errs[0].Code)
	}
	if len(errs[0].Data) != len(data) {
		t.Fatalf("expected %d data bytes, got %d", len(data), len(errs[0].Data))
	}
}

// TestDecodeSparseErrorsRejectsHugeCount ensures the sparse decoder itself is
// guarded against a hostile/garbage element count.
func TestDecodeSparseErrorsRejectsHugeCount(t *testing.T) {
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint32(payload[0:4], 0xFFFFFFF0) // ~4.29 billion errors
	// only 4 bytes of body follow — nowhere near enough

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := decodeSparseErrors(payload)
	runtime.ReadMemStats(&after)

	if err == nil {
		t.Fatal("expected decodeSparseErrors to reject an impossible count, got nil error")
	}
	const maxAllocBytes = 16 << 20
	if grew := after.TotalAlloc - before.TotalAlloc; grew > maxAllocBytes {
		t.Fatalf("decodeSparseErrors allocated %d bytes; unbounded allocation not guarded", grew)
	}
}

func TestCheckElemCount(t *testing.T) {
	if err := checkElemCount("t", 3, 8, 24); err != nil {
		t.Fatalf("3 elements of 8 bytes fit in 24 bytes, got error: %v", err)
	}
	if err := checkElemCount("t", 4, 8, 24); err == nil {
		t.Fatal("4 elements of 8 bytes do not fit in 24 bytes, expected error")
	}
	if err := checkElemCount("t", -1, 8, 24); err == nil {
		t.Fatal("negative count must error")
	}
	if err := checkElemCount("t", 0, 8, 0); err != nil {
		t.Fatalf("zero count is always valid, got error: %v", err)
	}
}
