package teraslab

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

// mockServer reads request frames from a connection and sends back response
// frames with the matching request_id and the given status/payload.
func mockServer(t *testing.T, ln net.Listener, handler func(req requestFrame) responseFrame) {
	t.Helper()
	conn, err := ln.Accept()
	if err != nil {
		return
	}
	defer conn.Close()

	var buf []byte
	for {
		// Read 4-byte length prefix.
		lenBuf := make([]byte, 4)
		if _, err := readFull(conn, lenBuf); err != nil {
			return
		}
		totalLen := int(getU32(lenBuf))
		if totalLen < 12 {
			return
		}
		// Read the rest of the frame.
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
		resp := handler(req)
		respBytes := encodeResponseFrame(resp)
		if _, err := conn.Write(respBytes); err != nil {
			return
		}
	}
}

func readFull(conn net.Conn, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		nn, err := conn.Read(buf[n:])
		n += nn
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func encodeResponseFrame(f responseFrame) []byte {
	innerLen := 8 + 1 + len(f.Payload)
	buf := make([]byte, 4+innerLen)
	putU32(buf[0:4], uint32(innerLen))
	putU64(buf[4:12], f.RequestID)
	buf[12] = f.Status
	copy(buf[13:], f.Payload)
	return buf
}

func TestPipeConnRoundTrip(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go mockServer(t, ln, func(req requestFrame) responseFrame {
		return responseFrame{
			RequestID: req.RequestID,
			Status:    StatusOK,
			Payload:   []byte("pong"),
		}
	})

	ctx := context.Background()
	pc, err := dial(ctx, ln.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.close()

	resp, err := pc.roundTrip(ctx, OpPing, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != StatusOK {
		t.Errorf("status = %d, want %d", resp.Status, StatusOK)
	}
	if string(resp.Payload) != "pong" {
		t.Errorf("payload = %q, want %q", resp.Payload, "pong")
	}
}

func TestPipeConnConcurrentPipelining(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go mockServer(t, ln, func(req requestFrame) responseFrame {
		// Echo the request_id back in the payload for verification.
		payload := make([]byte, 8)
		putU64(payload, req.RequestID)
		return responseFrame{
			RequestID: req.RequestID,
			Status:    StatusOK,
			Payload:   payload,
		}
	})

	ctx := context.Background()
	pc, err := dial(ctx, ln.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.close()

	// Send 10 concurrent requests.
	const n = 10
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			resp, err := pc.roundTrip(ctx, OpPing, 0, nil)
			if err != nil {
				errs[idx] = err
				return
			}
			if resp.Status != StatusOK {
				errs[idx] = &ServerError{Code: ErrCodeInternal, Message: "bad status"}
			}
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: %v", i, err)
		}
	}
}

func TestPipeConnContextCancellation(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	// Server that never responds.
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		// Read requests but never respond.
		buf := make([]byte, 4096)
		for {
			if _, err := conn.Read(buf); err != nil {
				return
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	pc, err := dial(context.Background(), ln.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.close()

	_, err = pc.roundTrip(ctx, OpPing, 0, nil)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
	if ctx.Err() == nil {
		t.Error("context should be done")
	}
}

func TestPipeConnCloseWakesPending(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	// Server that never responds.
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 4096)
		for {
			if _, err := conn.Read(buf); err != nil {
				return
			}
		}
	}()

	ctx := context.Background()
	pc, err := dial(ctx, ln.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Start a request, then close the connection.
	done := make(chan error, 1)
	go func() {
		_, err := pc.roundTrip(ctx, OpPing, 0, nil)
		done <- err
	}()

	time.Sleep(50 * time.Millisecond)
	pc.close()

	select {
	case err := <-done:
		if err == nil {
			t.Error("expected error after close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending request not woken after close")
	}
}

func TestPipeConnAlive(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, _ := ln.Accept()
		if conn != nil {
			conn.Close()
		}
	}()

	pc, err := dial(context.Background(), ln.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !pc.alive() {
		t.Error("should be alive immediately after dial")
	}
	pc.close()
	if pc.alive() {
		t.Error("should not be alive after close")
	}
}
