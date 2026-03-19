package teraslab

import (
	"bytes"
	"testing"
)

func TestRequestFrameRoundTrip(t *testing.T) {
	f := &requestFrame{
		RequestID: 42,
		OpCode:    OpSpendBatch,
		Flags:     0,
		Payload:   []byte{1, 2, 3, 4, 5},
	}
	encoded := encodeRequest(nil, f)

	// Verify total_length
	totalLen := getU32(encoded[0:4])
	if totalLen != 12+5 { // request_id(8) + op_code(2) + flags(2) + payload(5)
		t.Fatalf("total_length = %d, want %d", totalLen, 17)
	}

	// Verify frame size
	if len(encoded) != requestHeaderSize+5 {
		t.Fatalf("encoded length = %d, want %d", len(encoded), requestHeaderSize+5)
	}

	// Decode and compare
	reqID := getU64(encoded[4:12])
	opCode := getU16(encoded[12:14])
	flags := getU16(encoded[14:16])
	payload := encoded[16:]

	if reqID != 42 {
		t.Errorf("request_id = %d, want 42", reqID)
	}
	if opCode != OpSpendBatch {
		t.Errorf("op_code = %d, want %d", opCode, OpSpendBatch)
	}
	if flags != 0 {
		t.Errorf("flags = %d, want 0", flags)
	}
	if !bytes.Equal(payload, []byte{1, 2, 3, 4, 5}) {
		t.Errorf("payload mismatch")
	}
}

func TestResponseFrameRoundTrip(t *testing.T) {
	// Build a response frame manually (as the server would).
	payload := []byte{0xAA, 0xBB}
	innerLen := 8 + 1 + len(payload) // request_id + status + payload
	buf := make([]byte, 4+innerLen)
	putU32(buf[0:4], uint32(innerLen))
	putU64(buf[4:12], 99)
	buf[12] = StatusOK
	copy(buf[13:], payload)

	// Decode it.
	f, consumed, err := decodeResponse(buf)
	if err != nil {
		t.Fatal(err)
	}
	if consumed != len(buf) {
		t.Errorf("consumed = %d, want %d", consumed, len(buf))
	}
	if f.RequestID != 99 {
		t.Errorf("request_id = %d, want 99", f.RequestID)
	}
	if f.Status != StatusOK {
		t.Errorf("status = %d, want %d", f.Status, StatusOK)
	}
	if !bytes.Equal(f.Payload, payload) {
		t.Errorf("payload mismatch")
	}
}

func TestResponseFrameViaReader(t *testing.T) {
	// Build response bytes.
	payload := []byte{0xDE, 0xAD}
	innerLen := 8 + 1 + len(payload)
	raw := make([]byte, 4+innerLen)
	putU32(raw[0:4], uint32(innerLen))
	putU64(raw[4:12], 7)
	raw[12] = StatusPartialError
	copy(raw[13:], payload)

	r := bytes.NewReader(raw)
	f, _, err := readResponse(r, nil)
	if err != nil {
		t.Fatal(err)
	}
	if f.RequestID != 7 {
		t.Errorf("request_id = %d, want 7", f.RequestID)
	}
	if f.Status != StatusPartialError {
		t.Errorf("status = %d, want %d", f.Status, StatusPartialError)
	}
	if !bytes.Equal(f.Payload, payload) {
		t.Errorf("payload mismatch")
	}
}

func TestMaxPayloadFrame(t *testing.T) {
	f := &requestFrame{
		RequestID: 1,
		OpCode:    OpGetBatch,
		Flags:     0,
		Payload:   make([]byte, 1024*1024), // 1 MB
	}
	encoded := encodeRequest(nil, f)
	totalLen := getU32(encoded[0:4])
	if totalLen != uint32(12+1024*1024) {
		t.Errorf("total_length = %d, want %d", totalLen, 12+1024*1024)
	}
}

func TestTooLargeFrameRejected(t *testing.T) {
	// Craft a response with total_length > MaxFrameSize.
	buf := make([]byte, 8)
	putU32(buf[0:4], MaxFrameSize+1)

	_, _, err := decodeResponse(buf)
	if err == nil {
		t.Fatal("expected error for oversized frame")
	}
}

func TestTruncatedFrameError(t *testing.T) {
	payload := make([]byte, 100)
	innerLen := 8 + 1 + len(payload)
	raw := make([]byte, 4+innerLen)
	putU32(raw[0:4], uint32(innerLen))

	// Only provide half the data.
	_, _, err := decodeResponse(raw[:len(raw)/2])
	if err == nil {
		t.Fatal("expected error for truncated frame")
	}
}

func TestRequestHeaderSize(t *testing.T) {
	f := &requestFrame{RequestID: 0, OpCode: 0, Flags: 0, Payload: nil}
	encoded := encodeRequest(nil, f)
	if len(encoded) != requestHeaderSize {
		t.Errorf("empty request = %d bytes, want %d", len(encoded), requestHeaderSize)
	}
}

func TestResponseHeaderSize(t *testing.T) {
	buf := make([]byte, responseHeaderSize)
	putU32(buf[0:4], 9) // request_id(8) + status(1)
	putU64(buf[4:12], 0)
	buf[12] = 0

	f, consumed, err := decodeResponse(buf)
	if err != nil {
		t.Fatal(err)
	}
	if consumed != responseHeaderSize {
		t.Errorf("consumed = %d, want %d", consumed, responseHeaderSize)
	}
	if len(f.Payload) != 0 {
		t.Errorf("payload length = %d, want 0", len(f.Payload))
	}
}

func TestTotalLengthComputedCorrectly(t *testing.T) {
	f := &requestFrame{
		RequestID: 42,
		OpCode:    OpSpendBatch,
		Flags:     0,
		Payload:   make([]byte, 100),
	}
	encoded := encodeRequest(nil, f)
	totalLength := getU32(encoded[0:4])
	// total_length = request_id(8) + op_code(2) + flags(2) + payload(100) = 112
	if totalLength != 112 {
		t.Errorf("total_length = %d, want 112", totalLength)
	}
	if len(encoded) != 4+112 {
		t.Errorf("encoded length = %d, want %d", len(encoded), 4+112)
	}
}
