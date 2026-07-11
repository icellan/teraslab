package teraslab

import (
	"encoding/binary"
	"testing"
)

// buildPartialWithSignalsPayload builds a STATUS_PARTIAL_ERROR payload in the
// two-section signal layout the server sends for SetMinedBatch:
//
//	[success_count:4][ per success: index:4, signal:1, bidCount:1, bids:4*n ]
//	[error_count:4]  [ per error:   index:4, code:2,  dataLen:2, data ]
func buildPartialWithSignalsPayload(successes []BatchItemSuccess, errors []BatchItemError) []byte {
	var u32 [4]byte
	var u16 [2]byte
	buf := make([]byte, 0, 8)

	binary.LittleEndian.PutUint32(u32[:], uint32(len(successes)))
	buf = append(buf, u32[:]...)
	for _, s := range successes {
		binary.LittleEndian.PutUint32(u32[:], s.ItemIndex)
		buf = append(buf, u32[:]...)
		buf = append(buf, s.Signal, byte(len(s.BlockIDs)))
		for _, bid := range s.BlockIDs {
			binary.LittleEndian.PutUint32(u32[:], bid)
			buf = append(buf, u32[:]...)
		}
	}

	binary.LittleEndian.PutUint32(u32[:], uint32(len(errors)))
	buf = append(buf, u32[:]...)
	for _, e := range errors {
		binary.LittleEndian.PutUint32(u32[:], e.ItemIndex)
		buf = append(buf, u32[:]...)
		binary.LittleEndian.PutUint16(u16[:], e.Code)
		buf = append(buf, u16[:]...)
		binary.LittleEndian.PutUint16(u16[:], uint16(len(e.Data)))
		buf = append(buf, u16[:]...)
		buf = append(buf, e.Data...)
	}
	return buf
}

// TestMutationPartialErrorSurfacesDegradedTrailer is the P1-8 regression for the
// plain sparse mutation path: a partial response whose applied items were only
// replicated below quorum carries the reserved trailer byte, which the client
// must surface as PartialError.Degraded. Pre-fix the server dropped the degraded
// signal entirely on the partial path.
func TestMutationPartialErrorSurfacesDegradedTrailer(t *testing.T) {
	base := buildSparseErrorPayload(ErrCodeAlreadyExists, nil)

	// Without the trailer: partial error, but not degraded.
	resp := responseFrame{Status: StatusPartialError, Payload: append([]byte(nil), base...)}
	_, err := handleMutationResponse(resp)
	pe, ok := err.(*PartialError)
	if !ok {
		t.Fatalf("expected *PartialError, got %T", err)
	}
	if pe.Degraded {
		t.Fatal("no trailer must decode as not degraded")
	}
	if len(pe.Errors) != 1 || pe.Errors[0].Code != ErrCodeAlreadyExists {
		t.Fatalf("per-item errors lost: %+v", pe.Errors)
	}

	// With the trailer: partial error AND degraded.
	degradedPayload := append(append([]byte(nil), base...), PartialDurabilityDegraded)
	resp = responseFrame{Status: StatusPartialError, Payload: degradedPayload}
	_, err = handleMutationResponse(resp)
	pe, ok = err.(*PartialError)
	if !ok {
		t.Fatalf("expected *PartialError, got %T", err)
	}
	if !pe.Degraded {
		t.Fatal("trailer byte must surface degraded durability on the partial path")
	}
	if len(pe.Errors) != 1 || pe.Errors[0].Code != ErrCodeAlreadyExists {
		t.Fatalf("per-item errors lost alongside the trailer: %+v", pe.Errors)
	}
}

// TestSignalSpendPartialSurfacesDegradedTrailer covers the spend-batch path:
// spend PARTIAL_ERROR uses the sparse layout, decoded by the signal handler's
// sparse fallback. A degraded spend batch must surface both errors and degraded.
func TestSignalSpendPartialSurfacesDegradedTrailer(t *testing.T) {
	base := buildSparseErrorPayload(ErrCodeAlreadySpent, make([]byte, 36))
	degradedPayload := append(append([]byte(nil), base...), PartialDurabilityDegraded)

	resp := responseFrame{Status: StatusPartialError, Payload: degradedPayload}
	_, err := handleSignalResponse(resp)
	pe, ok := err.(*PartialError)
	if !ok {
		t.Fatalf("expected *PartialError, got %T", err)
	}
	if !pe.Degraded {
		t.Fatal("spend partial must surface degraded durability via the sparse fallback")
	}
	if len(pe.Errors) != 1 || pe.Errors[0].Code != ErrCodeAlreadySpent {
		t.Fatalf("spend per-item errors lost: %+v", pe.Errors)
	}
}

// TestSignalSetMinedPartialSurfacesDegradedTrailer covers the set-mined path:
// the two-section signal layout must round-trip the degraded trailer alongside
// the per-item successes and errors.
func TestSignalSetMinedPartialSurfacesDegradedTrailer(t *testing.T) {
	successes := []BatchItemSuccess{{ItemIndex: 0, Signal: 2, BlockIDs: []uint32{900}}}
	errors := []BatchItemError{{ItemIndex: 1, Code: ErrCodeConflicting}}
	base := buildPartialWithSignalsPayload(successes, errors)

	// With trailer: partial + degraded.
	degradedPayload := append(append([]byte(nil), base...), PartialDurabilityDegraded)
	resp := responseFrame{Status: StatusPartialError, Payload: degradedPayload}
	_, err := handleSignalResponse(resp)
	pe, ok := err.(*PartialError)
	if !ok {
		t.Fatalf("expected *PartialError, got %T", err)
	}
	if !pe.Degraded {
		t.Fatal("set-mined partial must surface degraded durability")
	}
	if len(pe.Errors) != 1 || pe.Errors[0].Code != ErrCodeConflicting {
		t.Fatalf("set-mined per-item errors lost: %+v", pe.Errors)
	}
	if len(pe.Successes) != 1 || len(pe.Successes[0].BlockIDs) != 1 || pe.Successes[0].BlockIDs[0] != 900 {
		t.Fatalf("set-mined applied-item signals lost: %+v", pe.Successes)
	}

	// Without trailer: partial but not degraded (control).
	resp = responseFrame{Status: StatusPartialError, Payload: append([]byte(nil), base...)}
	_, err = handleSignalResponse(resp)
	pe, ok = err.(*PartialError)
	if !ok {
		t.Fatalf("expected *PartialError, got %T", err)
	}
	if pe.Degraded {
		t.Fatal("no trailer must decode as not degraded")
	}
}
