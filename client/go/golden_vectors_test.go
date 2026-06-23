package teraslab

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"testing"
)

// Cross-language golden vectors: these pin the exact wire byte layout that the
// Rust server (src/protocol/codec.rs, src/cluster/auth.rs) produces/consumes, so
// any drift in endianness or field order is caught here rather than at runtime
// against a live cluster.

// TestGoldenRedirectWithVersion pins the redirect payload layout:
// [addr_len:2 LE][addr][shard_table_version:8 LE].
func TestGoldenRedirectWithVersion(t *testing.T) {
	addr := "10.0.0.7:3300"
	// Hand-built golden bytes matching encode_redirect_with_version.
	golden := []byte{
		byte(len(addr)), 0x00, // addr_len = 13, LE u16
	}
	golden = append(golden, []byte(addr)...)
	golden = append(golden,
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // version = 0x0807060504030201, LE
	)

	gotAddr, ver, has, err := decodeRedirectWithVersion(golden)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if gotAddr != addr {
		t.Fatalf("addr = %q, want %q", gotAddr, addr)
	}
	if !has {
		t.Fatal("expected hasVersion = true")
	}
	const wantVer uint64 = 0x0807060504030201
	if ver != wantVer {
		t.Fatalf("version = %#x, want %#x", ver, wantVer)
	}

	// Round-trip: building the same payload from parts must equal the golden.
	rebuilt := encodeRedirectPayloadVersion(addr, wantVer)
	if !bytes.Equal(rebuilt, golden) {
		t.Fatalf("rebuilt payload mismatch:\n got %x\nwant %x", rebuilt, golden)
	}
}

// TestGoldenHMACFrame pins the signed inter-node FRAME layout produced by
// signFrame: given an encoded frame [length:4][body], the output is
// [signedBodyLen:4][body][timestamp_ms:8 LE][tag:32] with
// tag = HMAC-SHA256(secret, body||ts). This mirrors src/cluster/auth.rs::sign_frame.
func TestGoldenHMACFrame(t *testing.T) {
	secret := []byte("test-secret-key")
	body := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	const ts uint64 = 0x0000_0123_4567_89AB

	// signFrame takes a full encoded frame [length:4][body].
	encoded := make([]byte, 4, 4+len(body))
	putU32(encoded, uint32(len(body)))
	encoded = append(encoded, body...)

	signed := signFrame(secret, encoded, ts)

	// length prefix = len(body)+8+32.
	if int(getU32(signed[:4])) != len(body)+8+32 {
		t.Fatalf("length prefix = %d, want %d", getU32(signed[:4]), len(body)+8+32)
	}
	// body, preserved after the new length prefix.
	if !bytes.Equal(signed[4:4+len(body)], body) {
		t.Fatal("body mismatch")
	}
	// timestamp, LE, immediately after body.
	wantTS := []byte{0xAB, 0x89, 0x67, 0x45, 0x23, 0x01, 0x00, 0x00}
	if !bytes.Equal(signed[4+len(body):4+len(body)+8], wantTS) {
		t.Fatalf("timestamp bytes = %x, want %x", signed[4+len(body):4+len(body)+8], wantTS)
	}
	// tag = HMAC-SHA256 over body||ts.
	mac := hmac.New(sha256.New, secret)
	mac.Write(signed[4 : 4+len(body)+8])
	wantTag := mac.Sum(nil)
	if !bytes.Equal(signed[4+len(body)+8:], wantTag) {
		t.Fatalf("tag mismatch")
	}
	if len(signed) != 4+len(body)+8+32 {
		t.Fatalf("total len = %d, want %d", len(signed), 4+len(body)+8+32)
	}
}

// TestGoldenHelloOpcode pins the OP_HELLO opcode and protocol version constants
// against the server (src/protocol/opcodes.rs).
func TestGoldenHelloOpcode(t *testing.T) {
	if OpHello != 107 {
		t.Fatalf("OpHello = %d, want 107", OpHello)
	}
	if ProtocolVersion != 2 {
		t.Fatalf("ProtocolVersion = %d, want 2", ProtocolVersion)
	}
}

// TestGoldenShardForTxID pins the shard hash against the Rust shard_for_txid:
// u16::from_le_bytes([txid[0], txid[1]]) & 0x0FFF.
func TestGoldenShardForTxID(t *testing.T) {
	var txid TxID
	txid[0] = 0x34
	txid[1] = 0x12
	// LE u16 of {0x34,0x12} = 0x1234; & 0x0FFF = 0x0234.
	if got := ShardForTxID(txid); got != 0x0234 {
		t.Fatalf("ShardForTxID = %#x, want 0x0234", got)
	}
}
