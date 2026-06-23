package teraslab

import (
	"crypto/hmac"
	"crypto/sha256"
)

// HMAC frame authentication for inter-node opcodes (e.g. OP_GET_PARTITION_MAP).
// When a cluster runs with a shared secret, those opcodes must carry an
// HMAC-SHA256 tag over the ENTIRE frame body. The server's strict_auth gate
// (src/server/mod.rs) splices the peeked request_id+op_code back onto the
// stream and verifies via src/cluster/auth.rs::verify_frame, which HMACs
//
//	request_id || op_code || flags || payload || timestamp_ms_le
//
// — i.e. the whole frame body, NOT just the payload. Signing only the payload
// (or signing without the on-wire request_id) fails that gate. signFrame below
// mirrors src/cluster/auth.rs::sign_frame exactly so the bytes verify
// byte-for-byte.
const (
	authTimestampLen = 8
	authTagLen       = 32
)

// signFrame mirrors src/cluster/auth.rs::sign_frame. Given an encoded request
// frame `[length:4][body]` (body = request_id || op_code || flags || payload),
// it returns the on-wire signed frame
//
//	[signedBodyLen:4 LE][body][timestamp_ms:8 LE][tag:32]
//
// where tag = HMAC-SHA256(secret, body || timestamp_ms_le). tsMs is the signing
// time in Unix milliseconds.
func signFrame(secret, encoded []byte, tsMs uint64) []byte {
	body := encoded[4:] // strip the 4-byte length prefix; sign the frame body
	signedBodyLen := len(body) + authTimestampLen + authTagLen
	out := make([]byte, 4, 4+signedBodyLen)
	putU32(out, uint32(signedBodyLen))
	out = append(out, body...)
	out = appendU64(out, tsMs)
	mac := hmac.New(sha256.New, secret)
	mac.Write(out[4:]) // HMAC over body || timestamp (everything after the length prefix)
	return mac.Sum(out)
}
