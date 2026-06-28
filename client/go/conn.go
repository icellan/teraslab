package teraslab

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// respChanPool pools the per-request response channels to avoid
// allocating a new channel on every roundTrip call.
var respChanPool = sync.Pool{
	New: func() any { return make(chan responseFrame, 1) },
}

// testHookReadLoopDeliver, when non-nil, is invoked by readLoop after it has
// claimed a pending channel (LoadAndDelete) but before it sends the response on
// that channel. It is nil in production (a single nil check per response) and
// exists only so tests can deterministically interleave a caller's
// cancellation with response delivery.
var testHookReadLoopDeliver func(reqID uint64)

// pipeConn is a pipelined TCP connection that supports multiple in-flight
// requests matched by request_id. It is safe for concurrent use.
type pipeConn struct {
	conn      net.Conn
	mu        sync.Mutex // serializes writes
	writeBuf  []byte     // reusable write buffer, protected by mu
	pending   sync.Map   // map[uint64]chan responseFrame
	nextID    atomic.Uint64
	inflight  atomic.Int64 // count of in-flight roundTrips (incl. health pings)
	readDone  chan struct{}
	closeOnce sync.Once
	closed    atomic.Bool
	connErr   atomic.Pointer[error]
}

// hasInflight reports whether the connection currently has in-flight requests.
// The pool uses this to avoid health-probing or reaping a connection that is in
// active use: a pipelined conn carrying live requests does not need a liveness
// ping (a genuine failure surfaces via the read/write path), and closing it
// would abort those legitimate in-flight requests with "connection closed".
func (c *pipeConn) hasInflight() bool {
	return c.inflight.Load() > 0
}

// inflightCount returns the number of requests currently in flight on this
// connection (including health pings). The pool uses this to pick the
// least-loaded connection and to decide whether a connection still has headroom
// below the pipeline-depth target before growing the pool.
func (c *pipeConn) inflightCount() int64 {
	return c.inflight.Load()
}

// dial creates a new pipelined connection to the given address.
func dial(ctx context.Context, addr string, timeout time.Duration) (*pipeConn, error) {
	d := net.Dialer{Timeout: timeout}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	// Disable Nagle's algorithm. Requests and responses are small framed
	// messages; with Nagle on, the client's small request writes are held in
	// the kernel TCP send buffer waiting for a peer ACK, interacting with the
	// peer's delayed-ACK to add 40 ms–3 s of latency per RPC. The server sets
	// TCP_NODELAY on its accept path (src/server/mod.rs); without the matching
	// client-side setting, every round-trip still pays the Nagle/delayed-ACK
	// tax. Best-effort: only *net.TCPConn supports it.
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	pc := &pipeConn{
		conn:     conn,
		writeBuf: make([]byte, 0, 4096),
		readDone: make(chan struct{}),
	}
	go pc.readLoop()
	return pc, nil
}

// releaseChan returns ch to respChanPool, but only if this goroutine still owns
// the pending entry for reqID. Winning the LoadAndDelete guarantees that neither
// readLoop nor closeInternal will send on ch (they remove the entry before
// sending), so ch is empty and safe to reuse. If we lose the race, the winner
// will send a frame on ch; returning it to the pool would leak that buffered
// frame into the next request that reuses the channel, so we drop it instead and
// let the GC reclaim it.
func (c *pipeConn) releaseChan(reqID uint64, ch chan responseFrame) {
	if _, ok := c.pending.LoadAndDelete(reqID); ok {
		respChanPool.Put(ch)
	}
}

// roundTrip sends a request and waits for its response.
// The request_id is assigned automatically.
func (c *pipeConn) roundTrip(ctx context.Context, opCode uint16, flags uint16, payload []byte) (responseFrame, error) {
	if c.closed.Load() {
		return responseFrame{}, c.getErr()
	}

	reqID := c.nextID.Add(1)
	ch := respChanPool.Get().(chan responseFrame)
	c.pending.Store(reqID, ch)
	c.inflight.Add(1)
	defer c.inflight.Add(-1)

	// Write the request using the connection's reusable write buffer.
	f := &requestFrame{
		RequestID: reqID,
		OpCode:    opCode,
		Flags:     flags,
		Payload:   payload,
	}

	c.mu.Lock()
	var err error
	c.writeBuf, err = writeRequest(c.conn, f, c.writeBuf)
	c.mu.Unlock()
	if err != nil {
		c.releaseChan(reqID, ch)
		return responseFrame{}, fmt.Errorf("write: %w", err)
	}

	// Wait for response or context cancellation.
	select {
	case resp := <-ch:
		respChanPool.Put(ch)
		// Zero RequestID with closed connection means we were woken by closeInternal.
		if resp.RequestID == 0 && c.closed.Load() {
			return responseFrame{}, c.getErr()
		}
		return resp, nil
	case <-ctx.Done():
		c.releaseChan(reqID, ch)
		return responseFrame{}, ctx.Err()
	case <-c.readDone:
		c.releaseChan(reqID, ch)
		return responseFrame{}, c.getErr()
	}
}

// roundTripSigned is like roundTrip but HMAC-signs the WHOLE frame body
// (request_id || op_code || flags || payload) with the cluster secret, as
// required by the server's strict_auth gate for inter-node opcodes (e.g.
// OP_GET_PARTITION_MAP). The frame is signed AFTER request_id is assigned via
// signFrame (mirroring src/cluster/auth.rs::sign_frame) so the signature covers
// the exact bytes on the wire.
func (c *pipeConn) roundTripSigned(ctx context.Context, opCode uint16, flags uint16, payload, secret []byte) (responseFrame, error) {
	if c.closed.Load() {
		return responseFrame{}, c.getErr()
	}

	reqID := c.nextID.Add(1)
	ch := respChanPool.Get().(chan responseFrame)
	c.pending.Store(reqID, ch)
	c.inflight.Add(1)
	defer c.inflight.Add(-1)

	f := &requestFrame{
		RequestID: reqID,
		OpCode:    opCode,
		Flags:     flags,
		Payload:   payload,
	}
	encoded := encodeRequest(nil, f)
	signed := signFrame(secret, encoded, uint64(time.Now().UnixMilli()))

	c.mu.Lock()
	_, err := c.conn.Write(signed)
	c.mu.Unlock()
	if err != nil {
		c.releaseChan(reqID, ch)
		return responseFrame{}, fmt.Errorf("write: %w", err)
	}

	select {
	case resp := <-ch:
		respChanPool.Put(ch)
		if resp.RequestID == 0 && c.closed.Load() {
			return responseFrame{}, c.getErr()
		}
		return resp, nil
	case <-ctx.Done():
		c.releaseChan(reqID, ch)
		return responseFrame{}, ctx.Err()
	case <-c.readDone:
		c.releaseChan(reqID, ch)
		return responseFrame{}, c.getErr()
	}
}

// readLoop runs in a goroutine, reading response frames and dispatching
// them to waiting callers.
func (c *pipeConn) readLoop() {
	defer close(c.readDone)
	var buf []byte
	for {
		resp, newBuf, err := readResponse(c.conn, buf)
		buf = newBuf
		if err != nil {
			c.setErr(fmt.Errorf("read: %w", err))
			c.closeInternal()
			return
		}
		if ch, ok := c.pending.LoadAndDelete(resp.RequestID); ok {
			if testHookReadLoopDeliver != nil {
				testHookReadLoopDeliver(resp.RequestID)
			}
			ch.(chan responseFrame) <- resp
		} else {
			// No pending entry — response is for a cancelled request.
			// Recycle the payload since nobody will consume it.
			recyclePayload(resp.Payload)
		}
	}
}

// close closes the connection and wakes all pending callers.
func (c *pipeConn) close() error {
	c.setErr(fmt.Errorf("connection closed"))
	return c.closeInternal()
}

func (c *pipeConn) closeInternal() error {
	var closeErr error
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		closeErr = c.conn.Close()
		// Wake all pending callers by sending a zero-value response on their
		// channels, then return channels to the pool. We don't close pooled
		// channels because they may be reused.
		c.pending.Range(func(key, value any) bool {
			ch := value.(chan responseFrame)
			select {
			case ch <- responseFrame{}:
			default:
			}
			c.pending.Delete(key)
			return true
		})
	})
	return closeErr
}

// alive returns true if the connection is healthy.
func (c *pipeConn) alive() bool {
	return !c.closed.Load()
}

func (c *pipeConn) setErr(err error) {
	c.connErr.CompareAndSwap(nil, &err)
}

func (c *pipeConn) getErr() error {
	if p := c.connErr.Load(); p != nil {
		return *p
	}
	return fmt.Errorf("connection closed")
}
