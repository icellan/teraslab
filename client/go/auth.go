package teraslab

import (
	"crypto/hmac"
	"crypto/sha256"
	"time"
)

// HMAC frame authentication for inter-node opcodes (e.g. OP_GET_PARTITION_MAP).
// When a cluster runs with a shared secret, those opcodes must carry an
// HMAC-SHA256 tag. The signed payload layout mirrors src/cluster/auth.rs:
//
//	[original_payload][timestamp_ms:8 LE][tag:32]
//
// where tag = HMAC-SHA256(secret, original_payload || timestamp_ms_le). The
// server validates the tag and that the timestamp is within its clock-skew
// window.
const (
	authTimestampLen = 8
	authTagLen       = 32
)

// signFramePayload appends the timestamp and HMAC-SHA256 tag to payload,
// returning the signed payload. tsMs is the signing time in Unix milliseconds.
func signFramePayload(secret, payload []byte, tsMs uint64) []byte {
	out := make([]byte, 0, len(payload)+authTimestampLen+authTagLen)
	out = append(out, payload...)
	out = appendU64(out, tsMs)
	mac := hmac.New(sha256.New, secret)
	mac.Write(out) // HMAC over payload || timestamp
	return mac.Sum(out)
}

// signPartitionMapPayload returns the (possibly signed) request payload for
// OP_GET_PARTITION_MAP. When no secret is configured it returns the raw payload
// unchanged so behaviour is identical to an unsecured cluster.
func signPartitionMapPayload(secret, payload []byte) []byte {
	if len(secret) == 0 {
		return payload
	}
	return signFramePayload(secret, payload, uint64(time.Now().UnixMilli()))
}
