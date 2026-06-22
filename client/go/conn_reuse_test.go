package teraslab

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

// TestPipeConnPooledChannelReuseAfterCancel reproduces the pooled
// response-channel reuse race.
//
// roundTrip gets its response channel from respChanPool and registers it in
// c.pending. readLoop claims the channel via LoadAndDelete and then sends the
// response on it. If a caller's context cancels in the window AFTER readLoop has
// claimed the channel but BEFORE it sends, the cancel path must not return the
// channel to the pool — readLoop still owns it and will buffer a (now stale)
// response into it. Returning it to the pool lets the next caller reuse a
// channel carrying the previous request's response, which manifests in
// production as "get response: need 4 bytes, have 0" / cross-delivered frames.
//
// The readLoop test hook forces exactly that interleaving so the bug is
// deterministic rather than timing-dependent.
func TestPipeConnPooledChannelReuseAfterCancel(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	// Server echoes the request payload back under the matching request id.
	go mockServer(t, ln, func(req requestFrame) responseFrame {
		return responseFrame{
			RequestID: req.RequestID,
			Status:    StatusOK,
			Payload:   append([]byte(nil), req.Payload...),
		}
	})

	pc, err := dial(context.Background(), ln.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.close()

	aReturned := make(chan struct{})
	aCtx, aCancel := context.WithCancel(context.Background())

	var once sync.Once
	prev := testHookReadLoopDeliver
	testHookReadLoopDeliver = func(uint64) {
		// Act only on the first delivered response (request A): cancel A, then
		// wait until A's roundTrip has fully returned (and, on the buggy code,
		// returned its channel to the pool) before letting readLoop send. This
		// forces readLoop to send into a channel the pool may now hand out to B.
		once.Do(func() {
			aCancel()
			<-aReturned
		})
	}
	defer func() { testHookReadLoopDeliver = prev }()

	// Request A — readLoop holds its delivery in the hook until we cancel + return.
	go func() {
		_, _ = pc.roundTrip(aCtx, OpPing, 0, []byte("AAAA"))
		close(aReturned)
	}()

	// Let A get in flight and become hook-held first.
	time.Sleep(100 * time.Millisecond)

	// Request B — a normal request that must receive ITS OWN echo, never A's
	// stale "AAAA" leaked through a reused pooled channel.
	want := []byte("BBBB")
	resp, err := pc.roundTrip(context.Background(), OpPing, 0, want)
	if err != nil {
		t.Fatalf("request B roundTrip: %v", err)
	}
	if !bytes.Equal(resp.Payload, want) {
		t.Fatalf("request B received a stale/leaked response: got payload %q (reqID %d), want %q "+
			"— a pooled response channel was returned to the pool while readLoop still owned it",
			resp.Payload, resp.RequestID, want)
	}
}
