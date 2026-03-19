package teraslab

import (
	"context"
	"net"
	"testing"
	"time"
)

func startEchoServer(t *testing.T) net.Listener {
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
			go func(c net.Conn) {
				defer c.Close()
				var buf []byte
				for {
					lenBuf := make([]byte, 4)
					if _, err := readFull(c, lenBuf); err != nil {
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
					if _, err := readFull(c, buf); err != nil {
						return
					}
					reqID := getU64(buf[0:8])
					resp := encodeResponseFrame(responseFrame{
						RequestID: reqID,
						Status:    StatusOK,
						Payload:   []byte("ok"),
					})
					if _, err := c.Write(resp); err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	return ln
}

func TestPoolGetReturnsConnection(t *testing.T) {
	ln := startEchoServer(t)
	defer ln.Close()

	p := newPool(ln.Addr().String(), PoolConfig{
		MinConns:    1,
		MaxConns:    4,
		DialTimeout: 2 * time.Second,
		HealthCheck: 1 * time.Hour, // disable for test
	})
	defer p.close()

	ctx := context.Background()
	c, err := p.get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !c.alive() {
		t.Error("connection should be alive")
	}
}

func TestPoolReusesConnections(t *testing.T) {
	ln := startEchoServer(t)
	defer ln.Close()

	p := newPool(ln.Addr().String(), PoolConfig{
		MinConns:    1,
		MaxConns:    4,
		DialTimeout: 2 * time.Second,
		HealthCheck: 1 * time.Hour,
	})
	defer p.close()

	ctx := context.Background()
	c1, err := p.get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := p.get(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Since pipelining shares connections, they should be the same.
	if c1 != c2 {
		t.Error("expected same connection (pipelining reuse)")
	}
}

func TestPoolCloseClosesAll(t *testing.T) {
	ln := startEchoServer(t)
	defer ln.Close()

	p := newPool(ln.Addr().String(), PoolConfig{
		MinConns:    1,
		MaxConns:    4,
		DialTimeout: 2 * time.Second,
		HealthCheck: 1 * time.Hour,
	})

	ctx := context.Background()
	c, err := p.get(ctx)
	if err != nil {
		t.Fatal(err)
	}

	p.close()

	if c.alive() {
		t.Error("connection should be closed after pool close")
	}

	_, err = p.get(ctx)
	if err == nil {
		t.Error("get after close should return error")
	}
}

func TestPoolRoundTripThroughPool(t *testing.T) {
	ln := startEchoServer(t)
	defer ln.Close()

	p := newPool(ln.Addr().String(), PoolConfig{
		MinConns:    1,
		MaxConns:    4,
		DialTimeout: 2 * time.Second,
		HealthCheck: 1 * time.Hour,
	})
	defer p.close()

	ctx := context.Background()
	c, err := p.get(ctx)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.roundTrip(ctx, OpPing, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != StatusOK {
		t.Errorf("status = %d, want %d", resp.Status, StatusOK)
	}
}
