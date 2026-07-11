package teraslab

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sort"
	"sync/atomic"
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

	// Cluster fields (unused by single-node tests, left at zero value):
	partMap       []byte       // served for OP_GET_PARTITION_MAP when non-nil
	queryCalls    atomic.Int32 // number of query round-trips observed
	maxQueryCalls int          // >0 caps query calls; excess ones return an error
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
	case OpGetPartitionMap:
		return responseFrame{RequestID: req.RequestID, Status: StatusOK, Payload: s.partMap}
	case OpQueryOldUnmined:
		if resp, capped := s.capReached(req.RequestID); capped {
			return resp
		}
		var cursor *TxID
		if len(req.Payload) == 36 { // [cutoff:4][cursor:32]
			var c TxID
			copy(c[:], req.Payload[4:36])
			cursor = &c
		}
		return s.page(req.RequestID, cursor)
	case OpQueryConflicting:
		if resp, capped := s.capReached(req.RequestID); capped {
			return resp
		}
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

// capReached counts a query round-trip and, once the count exceeds maxQueryCalls
// (when set), returns a synthetic StatusError. It is the regression backstop: a
// client that loops against a cursor-ignoring node is converted into a bounded,
// observable server error instead of a hung goroutine.
func (s *pagingServer) capReached(reqID uint64) (responseFrame, bool) {
	n := s.queryCalls.Add(1)
	if s.maxQueryCalls > 0 && int(n) > s.maxQueryCalls {
		var p []byte
		p = appendU16(p, ErrCodeInternal)
		p = appendU16(p, 0) // zero-length message
		return responseFrame{RequestID: reqID, Status: StatusError, Payload: p}, true
	}
	return responseFrame{}, false
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
	go s.serveLoop(ln)
	return ln
}

func (s *pagingServer) serveLoop(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go s.serveConn(conn)
	}
}

func (s *pagingServer) serveConn(conn net.Conn) {
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
}

// startClusterPagingServers stands up a two-node cluster: seed also serves the
// partition map (which lists both nodes), and other serves its own shard. Both
// answer OP_HELLO with their configured version, so the client negotiates each
// node's protocol version independently. Returns (seedAddr, otherAddr).
func startClusterPagingServers(t *testing.T, seed, other *pagingServer) (string, string) {
	t.Helper()
	lnSeed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	lnOther, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	nodes := []NodeInfo{
		{ID: 1, Addr: lnSeed.Addr().String()},
		{ID: 2, Addr: lnOther.Addr().String()},
	}
	pm := encodePartitionMapAssign(1, nodes, func(shard int) uint64 {
		if shard%2 == 0 {
			return 1
		}
		return 2
	})
	seed.partMap = pm
	other.partMap = pm
	go seed.serveLoop(lnSeed)
	go other.serveLoop(lnOther)
	t.Cleanup(func() { lnSeed.Close(); lnOther.Close() })
	return nodes[0].Addr, nodes[1].Addr
}

// clusterPagingClient builds a cluster-mode client seeded at seedAddr.
func clusterPagingClient(t *testing.T, seedAddr string) *Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := New(ctx, ClientConfig{
		Seeds:                  []string{seedAddr},
		Pool:                   PoolConfig{MinConns: 1, MaxConns: 2, DialTimeout: 2 * time.Second},
		ClusterRefreshInterval: time.Hour,
		MaxRedirects:           3,
	})
	if err != nil {
		t.Fatalf("new cluster client: %v", err)
	}
	t.Cleanup(func() { cli.Close() })
	return cli
}

// runQueryBounded runs a query in a goroutine and fails the test if it does not
// return within a hard deadline — so a paging regression surfaces as a clear
// failure rather than a hung test binary.
func runQueryBounded(t *testing.T, fn func() ([]TxID, error)) ([]TxID, error) {
	t.Helper()
	type result struct {
		txids []TxID
		err   error
	}
	ch := make(chan result, 1)
	go func() {
		txids, err := fn()
		ch <- result{txids, err}
	}()
	select {
	case r := <-ch:
		return r.txids, r.err
	case <-time.After(3 * time.Second):
		t.Fatal("query did not terminate within 3s (infinite paging loop against a cursor-ignoring node)")
		return nil, nil
	}
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

// TestQueryNodesUnionTerminatesAgainstV2NodeInMixedCluster is the FU#5 rolling-
// upgrade regression. The cluster fan-out must decide paging PER NODE, never from
// one client-global version applied to every node. Here one node is v3 (honours
// the resume cursor and pages to completion) and the other is still v2 (advertises
// version 2, ignores the cursor, and always returns the same truncated page). A
// client that applied a single global verdict to both nodes would loop forever
// against the v2 node. The fan-out must instead terminate, return the v3 node's
// COMPLETE set plus the v2 node's partial page, and signal incompleteness via
// ErrQueryTruncated (never a silent drop, never a hang).
func TestQueryNodesUnionTerminatesAgainstV2NodeInMixedCluster(t *testing.T) {
	v3full := make([]TxID, 7)
	for i := range v3full {
		v3full[i] = testTxID(byte(i + 1)) // 1..7
	}
	v2full := make([]TxID, 4)
	for i := range v2full {
		v2full[i] = testTxID(byte(i + 20)) // 20..23, disjoint from the v3 set
	}
	v3 := newPagingServer(3, 3, true, v3full)  // pages to completion
	v2 := newPagingServer(2, 2, false, v2full) // ignores the cursor, truncates
	v2.maxQueryCalls = 5                       // loop backstop
	seedAddr, _ := startClusterPagingServers(t, v3, v2)
	cli := clusterPagingClient(t, seedAddr)

	ctx := context.Background()
	got, err := runQueryBounded(t, func() ([]TxID, error) {
		return cli.QueryOldUnmined(ctx, 1000)
	})

	if !errors.Is(err, ErrQueryTruncated) {
		t.Fatalf("want ErrQueryTruncated (partial union), got %v", err)
	}
	// The per-node gate stops the v2 node after a single call; the guard would
	// stop it after two. Anything above two means the loop was not broken.
	if n := v2.queryCalls.Load(); n > 2 {
		t.Fatalf("v2 node queried %d times, want <=2 (no infinite loop)", n)
	}
	assertContainsAll(t, got, v3full)      // v3 node paged to completion
	assertContainsAll(t, got, v2full[:2])  // v2 node's first page is present
	assertContainsNone(t, got, v2full[2:]) // v2 node's unpageable tail is dropped
	if len(got) != len(v3full)+2 {
		t.Fatalf("union size = %d, want %d (7 v3 + 2 v2)", len(got), len(v3full)+2)
	}
}

// TestQueryNodesUnionGuardStopsCursorIgnoringNode isolates the non-advancing-
// cursor guard (the load-bearing protection). Here BOTH nodes advertise version 3
// so the per-node capability gate says "page it", yet one node ignores the cursor
// and re-serves the same page. The guard must detect that the page's last txid did
// not advance past the cursor just sent, stop after exactly two round-trips, keep
// that node's partial page, and mark the union incomplete.
func TestQueryNodesUnionGuardStopsCursorIgnoringNode(t *testing.T) {
	goodFull := make([]TxID, 5)
	for i := range goodFull {
		goodFull[i] = testTxID(byte(i + 1)) // 1..5
	}
	badFull := make([]TxID, 4)
	for i := range badFull {
		badFull[i] = testTxID(byte(i + 30)) // 30..33
	}
	good := newPagingServer(3, 2, true, goodFull) // honours the cursor
	// Advertises version 3 (gate says page it) but ignores the cursor: only the
	// guard can stop this one.
	bad := newPagingServer(3, 2, false, badFull)
	bad.maxQueryCalls = 5 // loop backstop
	seedAddr, _ := startClusterPagingServers(t, good, bad)
	cli := clusterPagingClient(t, seedAddr)

	ctx := context.Background()
	got, err := runQueryBounded(t, func() ([]TxID, error) {
		return cli.QueryConflicting(ctx)
	})

	if !errors.Is(err, ErrQueryTruncated) {
		t.Fatalf("want ErrQueryTruncated (partial union), got %v", err)
	}
	// The guard fires on the second round-trip (the page did not advance), so the
	// cursor-ignoring node is queried exactly twice.
	if n := bad.queryCalls.Load(); n != 2 {
		t.Fatalf("cursor-ignoring node queried %d times, want exactly 2 (guard on 2nd)", n)
	}
	assertContainsAll(t, got, goodFull)     // good node paged to completion
	assertContainsAll(t, got, badFull[:2])  // bad node's first page is present
	assertContainsNone(t, got, badFull[2:]) // bad node's tail is dropped
	if len(got) != len(goodFull)+2 {
		t.Fatalf("union size = %d, want %d (5 good + 2 bad)", len(got), len(goodFull)+2)
	}
}

// assertContainsAll fails if any want txid is missing from got.
func assertContainsAll(t *testing.T, got, want []TxID) {
	t.Helper()
	seen := make(map[TxID]struct{}, len(got))
	for _, x := range got {
		seen[x] = struct{}{}
	}
	for _, w := range want {
		if _, ok := seen[w]; !ok {
			t.Fatalf("expected txid %x in union, missing", w[:4])
		}
	}
}

// assertContainsNone fails if any forbidden txid is present in got.
func assertContainsNone(t *testing.T, got, forbidden []TxID) {
	t.Helper()
	seen := make(map[TxID]struct{}, len(got))
	for _, x := range got {
		seen[x] = struct{}{}
	}
	for _, f := range forbidden {
		if _, ok := seen[f]; ok {
			t.Fatalf("txid %x should not be in the partial union", f[:4])
		}
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
