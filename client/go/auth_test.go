package teraslab

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"testing"
	"time"
)

// TestSignFrameLayout verifies signFrame produces the exact on-wire layout the
// server's verify_frame expects: [signedBodyLen:4][body][ts:8][tag:32] where the
// HMAC tag covers the WHOLE frame body (request_id||op||flags||payload) || ts.
func TestSignFrameLayout(t *testing.T) {
	secret := []byte("cluster-secret")
	f := &requestFrame{
		RequestID: 7,
		OpCode:    OpGetPartitionMap,
		Flags:     0,
		Payload:   []byte{0x01, 0x02, 0x03},
	}
	encoded := encodeRequest(nil, f)
	body := encoded[4:] // request_id||op||flags||payload
	const ts uint64 = 1_700_000_000_000

	signed := signFrame(secret, encoded, ts)

	wantBodyLen := len(body) + authTimestampLen + authTagLen
	if int(getU32(signed[:4])) != wantBodyLen {
		t.Fatalf("length prefix = %d, want %d", getU32(signed[:4]), wantBodyLen)
	}
	if len(signed) != 4+wantBodyLen {
		t.Fatalf("signed len = %d, want %d", len(signed), 4+wantBodyLen)
	}
	// The body (including request_id) must be preserved verbatim.
	if !bytes.Equal(signed[4:4+len(body)], body) {
		t.Fatalf("frame body not preserved in signed frame")
	}
	if getU64(signed[4+len(body):4+len(body)+authTimestampLen]) != ts {
		t.Fatalf("timestamp mismatch")
	}
	// Tag is HMAC over body || ts (everything after the length prefix, minus tag).
	mac := hmac.New(sha256.New, secret)
	mac.Write(signed[4 : 4+len(body)+authTimestampLen])
	wantTag := mac.Sum(nil)
	gotTag := signed[4+len(body)+authTimestampLen:]
	if !hmac.Equal(gotTag, wantTag) {
		t.Fatalf("tag mismatch")
	}
}

// TestSignFrameCoversRequestID confirms the request_id is inside the HMAC input:
// changing it after signing must invalidate the tag. This is exactly why
// payload-only signing failed the server gate.
func TestSignFrameCoversRequestID(t *testing.T) {
	secret := []byte("cluster-secret")
	f := &requestFrame{RequestID: 42, OpCode: OpGetPartitionMap, Flags: 0, Payload: nil}
	signed := signFrame(secret, encodeRequest(nil, f), 1_700_000_000_000)

	// Recompute the tag as the server would, but with a different request_id.
	body := make([]byte, 0, 12)
	body = appendU64(body, 43) // tampered request_id
	body = appendU16(body, OpGetPartitionMap)
	body = appendU16(body, 0)
	ts := signed[4+12 : 4+12+authTimestampLen]
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	mac.Write(ts)
	wrongTag := mac.Sum(nil)
	gotTag := signed[4+12+authTimestampLen:]
	if hmac.Equal(gotTag, wrongTag) {
		t.Fatal("tag must change when request_id changes (request_id must be in the HMAC input)")
	}
}

// TestClusterBootstrapSignsPartitionMap verifies the bootstrap GET_PARTITION_MAP
// request carries a valid WHOLE-FRAME HMAC tag when a ClusterSecret is
// configured — verified exactly the way the server's verify_frame does.
func TestClusterBootstrapSignsPartitionMap(t *testing.T) {
	secret := []byte("bootstrap-secret")
	node := newFakeNode(t)
	nodes := []NodeInfo{{ID: 1, Addr: node.addr}}
	pm := encodePartitionMapAssign(1, nodes, func(int) uint64 { return 1 })

	verified := make(chan bool, 4)
	node.setHandler(func(req requestFrame) responseFrame {
		if req.OpCode == OpGetPartitionMap {
			ok := false
			// Whole-frame signing: the original payload was empty, so the
			// decoded frame payload is exactly [ts:8][tag:32], and the tag
			// covers request_id||op||flags||payload||ts.
			if len(req.Payload) == authTimestampLen+authTagLen {
				body := make([]byte, 0, 12)
				body = appendU64(body, req.RequestID)
				body = appendU16(body, req.OpCode)
				body = appendU16(body, req.Flags)
				ts := req.Payload[:authTimestampLen]
				mac := hmac.New(sha256.New, secret)
				mac.Write(body)
				mac.Write(ts)
				want := mac.Sum(nil)
				got := req.Payload[authTimestampLen:]
				ok = hmac.Equal(got, want)
			}
			select {
			case verified <- ok:
			default:
			}
			return responseFrame{RequestID: req.RequestID, Status: StatusOK, Payload: pm}
		}
		if req.OpCode == OpHello {
			var p []byte
			p = appendU16(p, ProtocolVersion)
			return responseFrame{RequestID: req.RequestID, Status: StatusOK, Payload: p}
		}
		return responseFrame{RequestID: req.RequestID, Status: StatusOK}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := New(ctx, ClientConfig{
		Seeds:                  []string{node.addr},
		Pool:                   PoolConfig{MinConns: 1, MaxConns: 2, DialTimeout: 2 * time.Second},
		ClusterRefreshInterval: time.Hour,
		ClusterSecret:          secret,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cli.Close()

	select {
	case ok := <-verified:
		if !ok {
			t.Fatal("bootstrap partition-map request had an invalid whole-frame HMAC tag")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("never observed a partition-map request")
	}
}

// TestHelloNegotiatesVersion checks the single-node handshake records the
// server's protocol version.
func TestHelloNegotiatesVersion(t *testing.T) {
	ln := startClientTestServer(t)
	defer ln.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := New(ctx, ClientConfig{Addr: ln.Addr().String(), Pool: PoolConfig{MinConns: 1, MaxConns: 2}})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer cli.Close()

	if got := cli.NegotiatedVersion(); got != ProtocolVersion {
		t.Fatalf("NegotiatedVersion = %d, want %d", got, ProtocolVersion)
	}
}
