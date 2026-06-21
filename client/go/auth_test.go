package teraslab

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"testing"
	"time"
)

func TestSignFramePayloadLayout(t *testing.T) {
	secret := []byte("cluster-secret")
	payload := []byte{0x01, 0x02, 0x03}
	const ts uint64 = 1_700_000_000_000

	signed := signFramePayload(secret, payload, ts)

	// Layout: [payload][timestamp:8 LE][tag:32].
	wantLen := len(payload) + authTimestampLen + authTagLen
	if len(signed) != wantLen {
		t.Fatalf("signed len = %d, want %d", len(signed), wantLen)
	}
	if !bytes.Equal(signed[:len(payload)], payload) {
		t.Fatalf("payload prefix mismatch")
	}
	if getU64(signed[len(payload):len(payload)+8]) != ts {
		t.Fatalf("timestamp mismatch")
	}

	// Recompute the expected tag over payload || timestamp.
	mac := hmac.New(sha256.New, secret)
	mac.Write(signed[:len(payload)+authTimestampLen])
	wantTag := mac.Sum(nil)
	gotTag := signed[len(payload)+authTimestampLen:]
	if !hmac.Equal(gotTag, wantTag) {
		t.Fatalf("tag mismatch")
	}
}

func TestSignPartitionMapPayloadNoSecret(t *testing.T) {
	if got := signPartitionMapPayload(nil, nil); got != nil {
		t.Fatalf("expected nil payload with no secret, got %d bytes", len(got))
	}
	raw := []byte{0xAA}
	if got := signPartitionMapPayload(nil, raw); !bytes.Equal(got, raw) {
		t.Fatalf("expected raw payload unchanged with no secret")
	}
}

func TestSignPartitionMapPayloadWithSecret(t *testing.T) {
	secret := []byte("s3cr3t")
	got := signPartitionMapPayload(secret, nil)
	// Empty payload + timestamp(8) + tag(32) = 40 bytes.
	if len(got) != authTimestampLen+authTagLen {
		t.Fatalf("signed empty payload len = %d, want %d", len(got), authTimestampLen+authTagLen)
	}
}

// TestClusterBootstrapSignsPartitionMap verifies the bootstrap GET_PARTITION_MAP
// request carries a valid HMAC tag when a ClusterSecret is configured.
func TestClusterBootstrapSignsPartitionMap(t *testing.T) {
	secret := []byte("bootstrap-secret")
	node := newFakeNode(t)
	nodes := []NodeInfo{{ID: 1, Addr: node.addr}}
	pm := encodePartitionMapAssign(1, nodes, func(int) uint64 { return 1 })

	verified := make(chan bool, 4)
	node.setHandler(func(req requestFrame) responseFrame {
		if req.OpCode == OpGetPartitionMap {
			// Validate the signed payload: [..][ts:8][tag:32] with empty body.
			ok := false
			if len(req.Payload) == authTimestampLen+authTagLen {
				// body is empty, so the signed prefix is just the timestamp.
				mac := hmac.New(sha256.New, secret)
				mac.Write(req.Payload[:authTimestampLen])
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
			t.Fatal("bootstrap partition-map request had an invalid HMAC tag")
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
