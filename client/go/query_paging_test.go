package teraslab

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sort"
	"testing"
	"time"
)

// pagingServer is an in-process mock that implements FU#5 cursor pagination for
// OP_QUERY_OLD_UNMINED / OP_QUERY_CONFLICTING so the client's paging loop and
// capability gate can be exercised without a live Rust server (which would need
// >524k seeded records to trigger a real truncation).
type pagingServer struct {
	version     uint16 // reported via OP_HELLO
	pageCap     int    // max txids per response frame
	full        []TxID // the complete candidate set (sorted on construction)
	honorCursor bool   // false simulates a pre-v3 server that ignores the cursor
}

func newPagingServer(version uint16, pageCap int, honorCursor bool, txids []TxID) *pagingServer {
	sorted := make([]TxID, len(txids))
	copy(sorted, txids)
	sort.Slice(sorted, func(i, j int) bool { return bytes.Compare(sorted[i][:], sorted[j][:]) < 0 })
	return &pagingServer{version: version, pageCap: pageCap, full: sorted, honorCursor: honorCursor}
}

func (s *pagingServer) handle(req requestFrame) responseFrame {
	switch req.OpCode {
	case OpHello:
		var p []byte
		p = appendU16(p, s.version)
		return responseFrame{RequestID: req.RequestID, Status: StatusOK, Payload: p}
	case OpPing:
		return responseFrame{RequestID: req.RequestID, Status: StatusOK}
	case OpQueryOldUnmined:
		var cursor *TxID
		if len(req.Payload) == 36 { // [cutoff:4][cursor:32]
			var c TxID
			copy(c[:], req.Payload[4:36])
			cursor = &c
		}
		return s.page(req.RequestID, cursor)
	case OpQueryConflicting:
		var cursor *TxID
		if len(req.Payload) == 32 { // [cursor:32]
			var c TxID
			copy(c[:], req.Payload[0:32])
			cursor = &c
		}
		return s.page(req.RequestID, cursor)
	default:
		return responseFrame{RequestID: req.RequestID, Status: StatusOK}
	}
}

// page returns the sorted qualifying txids strictly greater than cursor, capped
// at pageCap with the truncated trailer set when more remain. When honorCursor
// is false the cursor is ignored (always page 1) — the pathological old-server
// behaviour a naive client would loop against forever.
func (s *pagingServer) page(reqID uint64, cursor *TxID) responseFrame {
	var qualifying []TxID
	for _, t := range s.full {
		if s.honorCursor && cursor != nil && bytes.Compare(t[:], cursor[:]) <= 0 {
			continue
		}
		qualifying = append(qualifying, t)
	}
	truncated := byte(0)
	if len(qualifying) > s.pageCap {
		qualifying = qualifying[:s.pageCap]
		truncated = 1
	}
	var p []byte
	p = appendU32(p, uint32(len(qualifying)))
	for _, t := range qualifying {
		p = append(p, t[:]...)
	}
	p = append(p, truncated)
	return responseFrame{RequestID: reqID, Status: StatusOK, Payload: p}
}

func startPagingServer(t *testing.T, s *pagingServer) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
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
					if _, err := conn.Write(encodeResponseFrame(s.handle(req))); err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	return ln
}

func pagingTestClient(t *testing.T, ln net.Listener) *Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := New(ctx, ClientConfig{
		Addr: ln.Addr().String(),
		Pool: PoolConfig{MinConns: 1, MaxConns: 2, DialTimeout: 2 * time.Second},
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { cli.Close() })
	return cli
}

// Against a v3 server the client pages a truncated result to completion and
// returns the full deduplicated set. Seed count (7) is > 2× the page cap (3).
func TestQueryOldUnminedPagesToCompletion(t *testing.T) {
	full := make([]TxID, 7)
	for i := range full {
		full[i] = testTxID(byte(i + 1))
	}
	srv := newPagingServer(3, 3, true, full)
	ln := startPagingServer(t, srv)
	defer ln.Close()
	cli := pagingTestClient(t, ln)

	if got := cli.NegotiatedVersion(); got != 3 {
		t.Fatalf("NegotiatedVersion = %d, want 3", got)
	}

	ctx := context.Background()
	got, err := cli.QueryOldUnmined(ctx, 1000)
	if err != nil {
		t.Fatalf("QueryOldUnmined: %v", err)
	}
	assertSameSet(t, got, full)
}

// Same for the conflicting query.
func TestQueryConflictingPagesToCompletion(t *testing.T) {
	full := make([]TxID, 5)
	for i := range full {
		full[i] = testTxID(byte(i + 1))
	}
	srv := newPagingServer(3, 2, true, full)
	ln := startPagingServer(t, srv)
	defer ln.Close()
	cli := pagingTestClient(t, ln)

	ctx := context.Background()
	got, err := cli.QueryConflicting(ctx)
	if err != nil {
		t.Fatalf("QueryConflicting: %v", err)
	}
	assertSameSet(t, got, full)
}

// Capability gate: against a server advertising protocol version 2 (which
// ignores the cursor and returns page 1 forever), the client must NOT loop. It
// makes a single bounded call and surfaces the truncation via ErrQueryTruncated
// with the partial page — never a silent drop, never a hang.
func TestQueryOldUnminedCapabilityGateNoInfiniteLoop(t *testing.T) {
	full := make([]TxID, 7)
	for i := range full {
		full[i] = testTxID(byte(i + 1))
	}
	// version 2, honorCursor=false: a faithful pre-FU#5 server.
	srv := newPagingServer(2, 3, false, full)
	ln := startPagingServer(t, srv)
	defer ln.Close()
	cli := pagingTestClient(t, ln)

	if got := cli.NegotiatedVersion(); got != 2 {
		t.Fatalf("NegotiatedVersion = %d, want 2", got)
	}

	ctx := context.Background()
	// A generous timeout that an infinite loop would blow through; the single
	// call returns near-instantly.
	done := make(chan struct{})
	var got []TxID
	var qErr error
	go func() {
		got, qErr = cli.QueryOldUnmined(ctx, 1000)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("QueryOldUnmined looped against a pre-v3 server (never returned)")
	}

	if !errors.Is(qErr, ErrQueryTruncated) {
		t.Fatalf("want ErrQueryTruncated, got %v", qErr)
	}
	if len(got) != 3 {
		t.Fatalf("partial page = %d txids, want 3 (one capped page)", len(got))
	}
}

func assertSameSet(t *testing.T, got, want []TxID) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d txids, want %d", len(got), len(want))
	}
	seen := make(map[TxID]int)
	for _, x := range got {
		seen[x]++
	}
	for _, w := range want {
		if seen[w] == 0 {
			t.Fatalf("missing txid %x from paged result", w[:4])
		}
	}
	for x, n := range seen {
		if n > 1 {
			t.Fatalf("duplicate txid %x appeared %d times across pages", x[:4], n)
		}
	}
}
