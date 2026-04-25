//! Cluster authentication via HMAC-SHA256.
//!
//! When a cluster secret is configured, all SWIM UDP messages and
//! inter-node TCP frames carry an 8-byte millisecond Unix timestamp
//! plus a 32-byte HMAC tag appended to the payload. Peers that cannot
//! produce a valid tag — or whose timestamp falls outside the
//! [`MAX_CLOCK_SKEW`] window — are rejected.
//!
//! The timestamp is covered by the HMAC (it is included in the signed
//! input as `payload || timestamp`), which means an attacker cannot
//! alter the time without invalidating the tag. The skew window
//! bounds how old a captured message can be and still be accepted,
//! limiting replay attacks to a short window even if the secret is
//! known to an on-path attacker.
//!
//! This wire extension is additive: legacy unsigned peers still pass
//! a payload through `sign`/`verify` unchanged (except for the
//! timestamp+tag suffix). Since nobody runs TeraSlab in production
//! yet, we do not maintain a bypass for tag-less peers.

use std::io;
use std::time::{SystemTime, UNIX_EPOCH};

/// Length of the HMAC-SHA256 tag in bytes.
pub const HMAC_TAG_LEN: usize = 32;

/// Length of the millisecond Unix timestamp embedded alongside the HMAC tag.
pub const TIMESTAMP_LEN: usize = 8;

/// Total overhead appended to every signed message: `[timestamp_ms:8][tag:32]`.
pub const SIGNED_SUFFIX_LEN: usize = TIMESTAMP_LEN + HMAC_TAG_LEN;

/// Maximum tolerated clock skew between peers. Messages whose timestamp
/// differs from local time by more than this are rejected as stale /
/// replayed. Five minutes matches the wording in the security task and
/// is generous enough to accommodate reasonable NTP drift.
pub const MAX_CLOCK_SKEW: std::time::Duration = std::time::Duration::from_secs(5 * 60);

/// Return the current Unix time in milliseconds, clamped to `u64`.
fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Compute HMAC-SHA256 over `data` using the given `key`.
///
/// Uses a minimal hand-rolled HMAC-SHA256 built on top of the OS
/// CommonCrypto (macOS) or a pure-Rust fallback. This avoids pulling
/// in a heavy crypto crate for a single function.
pub fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    // HMAC(K, m) = H((K' ^ opad) || H((K' ^ ipad) || m))
    // where K' = H(K) if len(K) > 64, else K padded to 64 bytes.
    let mut k_prime = [0u8; 64];
    if key.len() > 64 {
        let h = sha256(key);
        k_prime[..32].copy_from_slice(&h);
    } else {
        k_prime[..key.len()].copy_from_slice(key);
    }

    let mut ipad = [0x36u8; 64];
    let mut opad = [0x5cu8; 64];
    for i in 0..64 {
        ipad[i] ^= k_prime[i];
        opad[i] ^= k_prime[i];
    }

    // inner = H(ipad || data)
    let mut inner_input = Vec::with_capacity(64 + data.len());
    inner_input.extend_from_slice(&ipad);
    inner_input.extend_from_slice(data);
    let inner = sha256(&inner_input);

    // outer = H(opad || inner)
    let mut outer_input = Vec::with_capacity(64 + 32);
    outer_input.extend_from_slice(&opad);
    outer_input.extend_from_slice(&inner);
    sha256(&outer_input)
}

/// Sign `data` by appending an 8-byte timestamp and a 32-byte HMAC tag.
///
/// The HMAC input is `payload || timestamp_ms_le`. The returned buffer
/// has the layout `[payload][timestamp_ms:8][tag:32]`.
pub fn sign(key: &[u8], data: &[u8]) -> Vec<u8> {
    sign_with_timestamp(key, data, now_unix_ms())
}

/// Sign `data` using a caller-supplied timestamp. Exposed for tests;
/// production callers should use [`sign`].
pub fn sign_with_timestamp(key: &[u8], data: &[u8], timestamp_ms: u64) -> Vec<u8> {
    let mut to_sign = Vec::with_capacity(data.len() + TIMESTAMP_LEN);
    to_sign.extend_from_slice(data);
    to_sign.extend_from_slice(&timestamp_ms.to_le_bytes());
    let tag = hmac_sha256(key, &to_sign);
    let mut signed = Vec::with_capacity(data.len() + SIGNED_SUFFIX_LEN);
    signed.extend_from_slice(data);
    signed.extend_from_slice(&timestamp_ms.to_le_bytes());
    signed.extend_from_slice(&tag);
    signed
}

/// Verify and strip the timestamp+HMAC tag from `data`.
///
/// Returns the payload without the suffix on success. Fails when:
/// - `data` is shorter than [`SIGNED_SUFFIX_LEN`] (`InvalidData`);
/// - the HMAC tag does not match (`PermissionDenied`);
/// - the embedded timestamp differs from local wall-clock time by more
///   than [`MAX_CLOCK_SKEW`] (`InvalidData`, message "stale timestamp").
pub fn verify<'a>(key: &[u8], data: &'a [u8]) -> io::Result<&'a [u8]> {
    verify_with_now(key, data, now_unix_ms())
}

/// Verify `data` against `now_ms` as the local wall-clock reference.
/// Exposed for tests; production callers should use [`verify`].
pub fn verify_with_now<'a>(key: &[u8], data: &'a [u8], now_ms: u64) -> io::Result<&'a [u8]> {
    if data.len() < SIGNED_SUFFIX_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "message too short for HMAC",
        ));
    }
    let (head, tag) = data.split_at(data.len() - HMAC_TAG_LEN);
    // head = payload || timestamp_ms_le
    let (payload, ts_bytes) = head.split_at(head.len() - TIMESTAMP_LEN);
    let expected = hmac_sha256(key, head);
    if !constant_time_eq(tag, &expected) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "HMAC verification failed",
        ));
    }
    // Tag is valid — now enforce the freshness window. The timestamp is
    // covered by the HMAC so an attacker cannot shift it without
    // invalidating the tag above.
    let ts_arr: [u8; TIMESTAMP_LEN] = ts_bytes
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "timestamp read failed"))?;
    let ts_ms = u64::from_le_bytes(ts_arr);
    let skew_ms = now_ms.abs_diff(ts_ms);
    if skew_ms > MAX_CLOCK_SKEW.as_millis() as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "stale timestamp: outside clock skew window",
        ));
    }
    Ok(payload)
}

/// Constant-time comparison to prevent timing attacks.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Minimal SHA-256 implementation (FIPS 180-4).
pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];

    let k: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];

    // Pre-processing: pad message
    let bit_len = (data.len() as u64) * 8;
    let mut padded = data.to_vec();
    padded.push(0x80);
    while (padded.len() % 64) != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    // Process each 64-byte block
    for block in padded.chunks_exact(64) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] =
                u32::from_be_bytes(block[i * 4..(i + 1) * 4].try_into().expect(
                    "invariant: 64-byte block sliced in 4-byte windows always yields 4 bytes",
                ));
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }

        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(k[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);

            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }

        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }

    let mut out = [0u8; 32];
    for (i, val) in h.iter().enumerate() {
        out[i * 4..(i + 1) * 4].copy_from_slice(&val.to_be_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_empty() {
        let h = sha256(b"");
        assert_eq!(
            hex(&h),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn sha256_abc() {
        let h = sha256(b"abc");
        assert_eq!(
            hex(&h),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn hmac_rfc4231_test_case_2() {
        // RFC 4231 Test Case 2: key = "Jefe", data = "what do ya want for nothing?"
        let tag = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
        assert_eq!(
            hex(&tag),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn sign_verify_round_trip() {
        let key = b"test-cluster-secret";
        let data = b"hello world";
        let signed = sign(key, data);
        let payload = verify(key, &signed).unwrap();
        assert_eq!(payload, data);
    }

    #[test]
    fn verify_rejects_tampered() {
        let key = b"test-cluster-secret";
        let mut signed = sign(key, b"hello");
        signed[0] ^= 0xFF; // tamper with payload
        assert!(verify(key, &signed).is_err());
    }

    #[test]
    fn verify_rejects_wrong_key() {
        let signed = sign(b"key1", b"hello");
        assert!(verify(b"key2", &signed).is_err());
    }

    #[test]
    fn verify_rejects_truncated() {
        // Too short to even contain the timestamp+tag suffix.
        match verify(b"key", &[0u8; 10]) {
            Err(e) if e.kind() == io::ErrorKind::InvalidData => {}
            Err(e) => panic!("expected InvalidData, got {e:?}"),
            Ok(_) => panic!("expected error on truncated message"),
        }
    }

    #[test]
    fn hmac_with_valid_timestamp_accepted() {
        let key = b"cluster-secret";
        let data = b"some cluster gossip";
        let now_ms = 1_700_000_000_000u64;
        let signed = sign_with_timestamp(key, data, now_ms);
        // Verify with a local clock that differs by 10s — well under the window.
        let payload = verify_with_now(key, &signed, now_ms + 10_000)
            .expect("verify must accept timestamps within skew window");
        assert_eq!(payload, data);
    }

    #[test]
    fn hmac_with_old_timestamp_is_rejected() {
        let key = b"cluster-secret";
        let data = b"some cluster gossip";
        let now_ms = 1_700_000_000_000u64;
        let six_minutes_ago = now_ms - 6 * 60 * 1000;
        let signed = sign_with_timestamp(key, data, six_minutes_ago);
        match verify_with_now(key, &signed, now_ms) {
            Ok(_) => panic!("stale timestamp must be rejected"),
            Err(e) => {
                assert_eq!(e.kind(), io::ErrorKind::InvalidData);
                assert!(
                    e.to_string().contains("stale timestamp"),
                    "error message must identify stale timestamp, got: {e}"
                );
            }
        }
    }

    #[test]
    fn hmac_with_future_timestamp_outside_skew_rejected() {
        // Symmetric: far-future timestamps are also rejected.
        let key = b"k";
        let data = b"x";
        let now_ms = 1_700_000_000_000u64;
        let six_minutes_ahead = now_ms + 6 * 60 * 1000;
        let signed = sign_with_timestamp(key, data, six_minutes_ahead);
        match verify_with_now(key, &signed, now_ms) {
            Ok(_) => panic!("future-skew timestamp must be rejected"),
            Err(e) => assert_eq!(e.kind(), io::ErrorKind::InvalidData),
        }
    }

    #[test]
    fn hmac_timestamp_is_covered_by_tag() {
        // An attacker that flips a bit in the timestamp without recomputing
        // the tag must be rejected on tag mismatch — NOT on skew — so
        // the timestamp is cryptographically bound.
        let key = b"k";
        let data = b"payload";
        let now_ms = 1_700_000_000_000u64;
        let mut signed = sign_with_timestamp(key, data, now_ms);
        // Tamper with the embedded timestamp bytes (right before the 32-byte tag).
        let ts_start = signed.len() - HMAC_TAG_LEN - TIMESTAMP_LEN;
        signed[ts_start] ^= 0xFF;
        match verify_with_now(key, &signed, now_ms) {
            Ok(_) => panic!("tampered timestamp must be rejected"),
            Err(e) => assert_eq!(e.kind(), io::ErrorKind::PermissionDenied),
        }
    }

    fn hex(data: &[u8]) -> String {
        data.iter().map(|b| format!("{b:02x}")).collect()
    }
}
