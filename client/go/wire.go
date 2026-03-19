package teraslab

import (
	"fmt"
	"io"
	"sync"
)

const (
	requestHeaderSize  = 16 // total_length(4) + request_id(8) + op_code(2) + flags(2)
	responseHeaderSize = 13 // total_length(4) + request_id(8) + status(1)
)

// requestFrame is a decoded request frame (used for encoding outgoing requests).
type requestFrame struct {
	RequestID uint64
	OpCode    uint16
	Flags     uint16
	Payload   []byte
}

// responseFrame is a decoded response frame (used for incoming responses).
type responseFrame struct {
	RequestID uint64
	Status    uint8
	Payload   []byte
}

// encodeRequest appends the encoded request frame to buf and returns it.
// The caller controls the buffer allocation.
func encodeRequest(buf []byte, f *requestFrame) []byte {
	innerLen := uint32(8 + 2 + 2 + len(f.Payload))
	// Grow buf for the 16-byte header + payload.
	need := 4 + int(innerLen)
	if cap(buf)-len(buf) < need {
		newBuf := make([]byte, len(buf), len(buf)+need)
		copy(newBuf, buf)
		buf = newBuf
	}
	start := len(buf)
	buf = buf[:start+need]
	putU32(buf[start:], innerLen)
	putU64(buf[start+4:], f.RequestID)
	putU16(buf[start+12:], f.OpCode)
	putU16(buf[start+14:], f.Flags)
	copy(buf[start+16:], f.Payload)
	return buf
}

// writeRequest encodes and writes a request frame to w, using writeBuf
// as a reusable buffer to avoid allocations. Returns the (possibly grown) buffer.
func writeRequest(w io.Writer, f *requestFrame, writeBuf []byte) ([]byte, error) {
	writeBuf = writeBuf[:0]
	writeBuf = encodeRequest(writeBuf, f)
	_, err := w.Write(writeBuf)
	return writeBuf, err
}

// payloadPool pools byte slices used for response payload copies.
var payloadPool = sync.Pool{
	New: func() any { return make([]byte, 0, 4096) },
}

// readResponse reads a single response frame from r.
// buf is a reusable read buffer that may be grown; the returned buf should be
// passed to subsequent calls. The responseFrame.Payload is a pooled copy that
// remains valid until the caller is done processing the response.
func readResponse(r io.Reader, buf []byte) (responseFrame, []byte, error) {
	// Read the 4-byte length prefix.
	if len(buf) < 4 {
		buf = make([]byte, 4096)
	}
	if _, err := io.ReadFull(r, buf[:4]); err != nil {
		return responseFrame{}, buf, fmt.Errorf("read length: %w", err)
	}
	totalLength := getU32(buf[:4])
	if totalLength > MaxFrameSize {
		return responseFrame{}, buf, fmt.Errorf("frame too large: %d exceeds max %d", totalLength, MaxFrameSize)
	}
	if totalLength < 9 { // request_id(8) + status(1)
		return responseFrame{}, buf, fmt.Errorf("frame too short: %d < 9", totalLength)
	}

	// Grow buf if needed.
	bodyLen := int(totalLength)
	if len(buf) < bodyLen {
		buf = make([]byte, bodyLen)
	}

	// Read the body.
	if _, err := io.ReadFull(r, buf[:bodyLen]); err != nil {
		return responseFrame{}, buf, fmt.Errorf("read body: %w", err)
	}

	reqID := getU64(buf[0:8])
	status := buf[8]

	// Get a pooled slice for the payload copy.
	payloadLen := bodyLen - 9
	payload := payloadPool.Get().([]byte)
	if cap(payload) < payloadLen {
		payload = make([]byte, payloadLen)
	} else {
		payload = payload[:payloadLen]
	}
	copy(payload, buf[9:9+payloadLen])

	return responseFrame{
		RequestID: reqID,
		Status:    status,
		Payload:   payload,
	}, buf, nil
}

// recyclePayload returns a response payload to the pool. Call this after
// you are done processing the response frame.
func recyclePayload(payload []byte) {
	if payload != nil {
		payloadPool.Put(payload[:0])
	}
}

// decodeResponse decodes a response frame from a complete byte slice.
// Returns the decoded frame and the number of bytes consumed.
func decodeResponse(data []byte) (responseFrame, int, error) {
	if len(data) < 4 {
		return responseFrame{}, 0, fmt.Errorf("need at least 4 bytes, have %d", len(data))
	}
	totalLength := getU32(data[0:4])
	if totalLength > MaxFrameSize {
		return responseFrame{}, 0, fmt.Errorf("frame too large: %d exceeds max %d", totalLength, MaxFrameSize)
	}
	frameSize := 4 + int(totalLength)
	if len(data) < frameSize {
		return responseFrame{}, 0, fmt.Errorf("truncated: declared %d, have %d", frameSize, len(data))
	}
	if totalLength < 9 {
		return responseFrame{}, 0, fmt.Errorf("frame too short: %d < 9", totalLength)
	}

	reqID := getU64(data[4:12])
	status := data[12]
	// Sub-slice for decodeResponse since it operates on owned data.
	payload := data[13:frameSize]

	return responseFrame{
		RequestID: reqID,
		Status:    status,
		Payload:   payload,
	}, frameSize, nil
}
