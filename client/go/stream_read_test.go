package teraslab

import (
	"context"
	"crypto/sha256"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

// streamReadServer is a mock server for the FU#4 OpStreamRead path. It answers
// pool prewarm/health traffic (OP_HELLO / OP_PING) with StatusOK and, for an
// OpStreamRead request, emits `blob` as `chunkSize`-byte StatusStreamChunk
// frames followed by a terminal StatusStreamEnd whose trailer digest is
// `endHash`. If `forceError` is non-zero it replies with a single StatusError
// frame carrying that code instead of streaming.
type streamReadServer struct {
	blob       []byte
	chunkSize  int
	endHash    [32]byte
	forceError uint16
}

func (s *streamReadServer) serveLoop(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go s.serveConn(conn)
	}
}

func (s *streamReadServer) serveConn(conn net.Conn) {
	defer conn.Close()
	var buf []byte
	for {
		lenBuf := make([]byte, 4)
		if _, err := readFull(conn, lenBuf); err != nil {
			return
		}
		totalLen := int(getU32(lenBuf))
		if totalLen < 12 {
			return
		}
		if cap(buf) < totalLen {
			buf = make([]byte, totalLen)
		}
		buf = buf[:totalLen]
		if _, err := readFull(conn, buf); err != nil {
			return
		}
		req := requestFrame{
			RequestID: getU64(buf[0:8]),
			OpCode:    getU16(buf[8:10]),
			Flags:     getU16(buf[10:12]),
			Payload:   buf[12:],
		}
		switch req.OpCode {
		case OpStreamRead:
			if err := s.writeBurst(conn, req.RequestID); err != nil {
				return
			}
		case OpHello:
			var p []byte
			p = appendU16(p, ProtocolVersion)
			if _, err := conn.Write(encodeResponseFrame(responseFrame{RequestID: req.RequestID, Status: StatusOK, Payload: p})); err != nil {
				return
			}
		default:
			if _, err := conn.Write(encodeResponseFrame(responseFrame{RequestID: req.RequestID, Status: StatusOK})); err != nil {
				return
			}
		}
	}
}

func (s *streamReadServer) writeBurst(conn net.Conn, reqID uint64) error {
	if s.forceError != 0 {
		// Error payload wire format: [code:u16 LE][msg_len:u16 LE][msg].
		const msg = "external blob not found"
		payload := appendU16(nil, s.forceError)
		payload = appendU16(payload, uint16(len(msg)))
		payload = append(payload, msg...)
		_, err := conn.Write(encodeResponseFrame(responseFrame{RequestID: reqID, Status: StatusError, Payload: payload}))
		return err
	}
	offset := uint64(0)
	cs := s.chunkSize
	if cs < 1 {
		cs = 1
	}
	for start := 0; start < len(s.blob); start += cs {
		end := start + cs
		if end > len(s.blob) {
			end = len(s.blob)
		}
		payload := encodeStreamReadChunk(nil, offset, s.blob[start:end])
		if _, err := conn.Write(encodeResponseFrame(responseFrame{RequestID: reqID, Status: StatusStreamChunk, Payload: payload})); err != nil {
			return err
		}
		offset += uint64(end - start)
	}
	payload := encodeStreamReadEnd(nil, uint64(len(s.blob)), s.endHash)
	_, err := conn.Write(encodeResponseFrame(responseFrame{RequestID: reqID, Status: StatusStreamEnd, Payload: payload}))
	return err
}

func startStreamReadServer(t *testing.T, s *streamReadServer) *Client {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go s.serveLoop(ln)
	t.Cleanup(func() { ln.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := New(ctx, ClientConfig{
		Addr:                   ln.Addr().String(),
		Pool:                   PoolConfig{MinConns: 1, MaxConns: 2, DialTimeout: 2 * time.Second},
		ClusterRefreshInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { cli.Close() })
	return cli
}

// A > 16 MiB blob streamed as many chunk frames must reassemble byte-identically
// on the client, with the digest verified against the END trailer.
func TestStreamReadColdDataReassemblesOver16MiB(t *testing.T) {
	blob := make([]byte, 17*1024*1024+321)
	for i := range blob {
		blob[i] = byte(i % 251)
	}
	hash := sha256.Sum256(blob)
	cli := startStreamReadServer(t, &streamReadServer{blob: blob, chunkSize: 200 * 1024, endHash: hash})

	var txid TxID
	txid[0] = 0x11
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	got, err := cli.StreamReadColdData(ctx, txid)
	if err != nil {
		t.Fatalf("StreamReadColdData: %v", err)
	}
	if len(got) != len(blob) {
		t.Fatalf("reassembled %d bytes, want %d", len(got), len(blob))
	}
	for i := range got {
		if got[i] != blob[i] {
			t.Fatalf("byte %d differs: got %d want %d", i, got[i], blob[i])
		}
	}
}

// A mismatched END-trailer digest must be rejected by the client's integrity
// check rather than silently returned.
func TestStreamReadColdDataRejectsDigestMismatch(t *testing.T) {
	blob := make([]byte, 70_000)
	for i := range blob {
		blob[i] = byte(i % 256)
	}
	var badHash [32]byte
	for i := range badHash {
		badHash[i] = 0xFF
	}
	cli := startStreamReadServer(t, &streamReadServer{blob: blob, chunkSize: 16 * 1024, endHash: badHash})

	var txid TxID
	txid[0] = 0x22
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := cli.StreamReadColdData(ctx, txid)
	if err == nil {
		t.Fatal("expected a digest-mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "content-hash mismatch") {
		t.Fatalf("want content-hash mismatch error, got: %v", err)
	}
}

// A mid-stream error frame terminates the stream as a *ServerError carrying the
// server's code.
func TestStreamReadColdDataSurfacesErrorFrame(t *testing.T) {
	cli := startStreamReadServer(t, &streamReadServer{forceError: ErrCodeBlobNotFound})

	var txid TxID
	txid[0] = 0x33
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := cli.StreamReadColdData(ctx, txid)
	var se *ServerError
	if !errors.As(err, &se) {
		t.Fatalf("want *ServerError, got %v", err)
	}
	if se.Code != ErrCodeBlobNotFound {
		t.Fatalf("want ErrCodeBlobNotFound, got %d", se.Code)
	}
}
