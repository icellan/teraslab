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

use std::io::{self, Read, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

use crate::protocol::opcodes::MAX_FRAME_SIZE;

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

/// Sign an already-encoded request/response frame body.
///
/// The length prefix is not part of the signed input. Instead, the body
/// (`request_id || opcode/status || flags? || payload`) is signed and the
/// frame is re-emitted with a larger length prefix that includes the
/// timestamp+tag suffix. This lets receivers verify before decoding while
/// keeping the existing frame structs unchanged.
pub fn sign_frame(key: &[u8], encoded_frame: &[u8]) -> io::Result<Vec<u8>> {
    if encoded_frame.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame too short for length prefix",
        ));
    }
    let len = u32::from_le_bytes(encoded_frame[0..4].try_into().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "frame length prefix read failed",
        )
    })?) as usize;
    let frame_len = 4usize
        .checked_add(len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "frame length overflow"))?;
    if encoded_frame.len() != frame_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame length does not match buffer",
        ));
    }
    let signed_body = sign(key, &encoded_frame[4..]);
    if signed_body.len() > MAX_FRAME_SIZE as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "signed frame exceeds maximum frame size",
        ));
    }
    let mut out = Vec::with_capacity(4 + signed_body.len());
    out.extend_from_slice(&(signed_body.len() as u32).to_le_bytes());
    out.extend_from_slice(&signed_body);
    Ok(out)
}

/// Verify an authenticated frame and return a normal unsigned frame.
///
/// The returned bytes are suitable for [`crate::protocol::frame::RequestFrame::decode`]
/// or [`crate::protocol::frame::ResponseFrame::decode`].
///
/// This is a thin wrapper around [`verify_frame_streaming`] for the
/// `&[u8]` case (when the caller has already buffered the entire
/// signed frame). New call sites that read from a `TcpStream` should
/// prefer [`verify_frame_streaming`] directly so they never need to
/// allocate the full frame upfront — see C-7 / F-G5-016.
pub fn verify_frame(key: &[u8], encoded_frame: &[u8]) -> io::Result<Vec<u8>> {
    if encoded_frame.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame too short for length prefix",
        ));
    }
    let body_len = u32::from_le_bytes(encoded_frame[0..4].try_into().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "frame length prefix read failed",
        )
    })?) as usize;
    let frame_len = 4usize
        .checked_add(body_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "frame length overflow"))?;
    if encoded_frame.len() != frame_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame length does not match buffer",
        ));
    }

    // Defer to the streaming verifier so there is exactly one
    // authoritative implementation of the HMAC-verify state machine.
    // We seed the output Vec with a 4-byte length-prefix placeholder
    // and overwrite it once we know the payload length; this matches
    // the existing `verify_frame` contract of returning a
    // `[length:4][payload]` blob.
    let mut out = Vec::with_capacity(encoded_frame.len());
    out.extend_from_slice(&[0u8; 4]);
    let mut reader = io::Cursor::new(encoded_frame);
    let payload_len = verify_frame_streaming(key, &mut reader, &mut out)?;
    let prefix = (payload_len as u32).to_le_bytes();
    out[0..4].copy_from_slice(&prefix);
    Ok(out)
}

/// Streaming HMAC-SHA256 accumulator.
///
/// Equivalent to [`hmac_sha256`] when the input is fed in chunks via
/// [`HmacSha256::update`] and finalized with [`HmacSha256::finalize`]. The
/// streaming form lets callers verify large frames without ever
/// materialising the full payload in a single buffer.
///
/// HMAC(K, m) = H((K' ^ opad) || H((K' ^ ipad) || m)) where K' is K
/// padded/hashed to 64 bytes. The inner hash is fed `ipad || m` as
/// `m` arrives chunk-by-chunk; the outer hash is computed once at
/// finalize.
struct HmacSha256 {
    inner: Sha256,
    opad: [u8; 64],
}

impl HmacSha256 {
    /// Construct a streaming HMAC context for `key`.
    fn new(key: &[u8]) -> Self {
        let mut k_prime = [0u8; 64];
        if key.len() > 64 {
            // Long keys are hashed to 32 bytes per RFC 2104; pad the
            // remaining 32 with zeros.
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

        let mut inner = Sha256::new();
        inner.update(ipad);
        Self { inner, opad }
    }

    /// Feed `data` into the inner SHA-256.
    fn update(&mut self, data: &[u8]) {
        self.inner.update(data);
    }

    /// Finalize and return the 32-byte tag.
    fn finalize(self) -> [u8; 32] {
        let inner_hash = self.inner.finalize();
        let mut outer = Sha256::new();
        outer.update(self.opad);
        outer.update(inner_hash);
        let tag = outer.finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(&tag);
        out
    }
}

/// Streaming variant of [`verify_frame`].
///
/// Reads the 4-byte length prefix and then the body from `reader`,
/// feeding the body through an [`HmacSha256`] context as it arrives.
/// The verified payload bytes are emitted to `payload_sink` as they
/// stream — see the note about the slow-loris failure case below.
///
/// On HMAC success: `payload_sink` contains the full payload, and
/// `Ok(payload_len)` returns the payload byte count.
///
/// On HMAC failure: returns `Err(PermissionDenied)`. The bytes the
/// verifier did manage to write to `payload_sink` before discovering
/// the bad tag are the caller's problem — typically the caller passes
/// a sink it is about to drop anyway (`Vec::new()` + `drop` on error)
/// or `io::sink()` if it does not need the payload.
///
/// Memory used by the verifier itself: a chunk buffer
/// (`STREAM_CHUNK_SIZE`, 8 KiB) plus a [`SIGNED_SUFFIX_LEN`]-byte
/// rolling tail. The verifier never materialises a buffer the size
/// of the full payload — that is the slow-loris property: a 16 MiB
/// frame with a wrong tag is rejected without the verifier
/// allocating 16 MiB of working memory.
///
/// Errors mirror [`verify_frame`]:
/// - `InvalidData` for length / size violations and stale timestamps;
/// - `PermissionDenied` for HMAC tag mismatch;
/// - any I/O error from `reader` is propagated.
pub fn verify_frame_streaming<R: Read, W: Write>(
    key: &[u8],
    reader: &mut R,
    payload_sink: &mut W,
) -> io::Result<usize> {
    verify_frame_streaming_with_now(key, reader, payload_sink, now_unix_ms())
}

/// Streaming chunk size for [`verify_frame_streaming`]. Chosen large
/// enough to amortise per-chunk overhead but small enough that a
/// hostile peer cannot drive verifier memory beyond a few KiB.
pub const STREAM_CHUNK_SIZE: usize = 8 * 1024;

/// Test-visible variant of [`verify_frame_streaming`] that accepts a
/// caller-supplied `now_ms` reference. Production callers should use
/// [`verify_frame_streaming`].
pub fn verify_frame_streaming_with_now<R: Read, W: Write>(
    key: &[u8],
    reader: &mut R,
    payload_sink: &mut W,
    now_ms: u64,
) -> io::Result<usize> {
    // 1. Length prefix.
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let body_len = u32::from_le_bytes(len_buf) as usize;

    // 2. Bounds: body must hold at least the [timestamp || tag] suffix,
    //    and the unsigned payload must fit inside MAX_FRAME_SIZE.
    if body_len < SIGNED_SUFFIX_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame too short for HMAC suffix",
        ));
    }
    let payload_len = body_len - SIGNED_SUFFIX_LEN;
    if payload_len > MAX_FRAME_SIZE as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsigned frame exceeds maximum frame size",
        ));
    }

    // 3. Stream the body. The HMAC input is `payload || timestamp`
    //    (i.e. the first `body_len - HMAC_TAG_LEN` bytes); the trailing
    //    32 bytes are the tag to compare. We keep a rolling tail of
    //    `SIGNED_SUFFIX_LEN` bytes so we can isolate the timestamp +
    //    tag at the end while feeding everything before that into
    //    the HMAC.
    //
    //    Memory used during the loop is bounded: chunk buffer
    //    (STREAM_CHUNK_SIZE) + tail window (SIGNED_SUFFIX_LEN). We
    //    write payload bytes to `payload_sink` as they pass through
    //    so the verifier itself never accumulates a payload-sized
    //    buffer — slow-loris HMAC-mismatch frames reject without
    //    O(payload_len) verifier-side memory.
    let mut hmac = HmacSha256::new(key);
    let mut tail = [0u8; SIGNED_SUFFIX_LEN];
    let mut tail_filled = 0usize;
    // Track HMAC-fed bytes so we know which portion of evicted bytes
    // is payload (< payload_len) vs timestamp (>= payload_len).
    let mut hmac_fed = 0usize;
    let mut chunk = [0u8; STREAM_CHUNK_SIZE];
    let mut remaining = body_len;

    while remaining > 0 {
        let want = remaining.min(STREAM_CHUNK_SIZE);
        let got = read_some(reader, &mut chunk[..want])?;
        if got == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF reading signed frame body",
            ));
        }
        let mut new_bytes = &chunk[..got];

        // Slide the tail window. Bytes that fall out the front of the
        // [tail ++ new_bytes] window are part of the HMAC input.
        if tail_filled + new_bytes.len() > SIGNED_SUFFIX_LEN {
            let evict = tail_filled + new_bytes.len() - SIGNED_SUFFIX_LEN;
            let from_tail = evict.min(tail_filled);
            // Feed the evicted prefix of `tail` first.
            feed_evicted(
                &mut hmac,
                payload_sink,
                &mut hmac_fed,
                payload_len,
                &tail[..from_tail],
            )?;
            // Shift the remaining tail down.
            tail.copy_within(from_tail..tail_filled, 0);
            tail_filled -= from_tail;
            let from_new = evict - from_tail;
            if from_new > 0 {
                feed_evicted(
                    &mut hmac,
                    payload_sink,
                    &mut hmac_fed,
                    payload_len,
                    &new_bytes[..from_new],
                )?;
                new_bytes = &new_bytes[from_new..];
            }
        }
        // Whatever's left of `new_bytes` fits in the tail window.
        tail[tail_filled..tail_filled + new_bytes.len()].copy_from_slice(new_bytes);
        tail_filled += new_bytes.len();

        remaining -= got;
    }
    debug_assert_eq!(tail_filled, SIGNED_SUFFIX_LEN);
    debug_assert_eq!(hmac_fed, payload_len);

    // 4. Tail now holds `[timestamp:8][tag:32]`. Feed timestamp into
    //    HMAC (it is covered by the signature) and compare the tag.
    let ts_bytes = &tail[..TIMESTAMP_LEN];
    let tag = &tail[TIMESTAMP_LEN..];
    hmac.update(ts_bytes);
    let expected = hmac.finalize();
    if !constant_time_eq(tag, &expected) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "HMAC verification failed",
        ));
    }

    // 5. Tag is valid — enforce the freshness window.
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

    Ok(payload_len)
}

/// Feed `bytes` (which are guaranteed to fall outside the rolling
/// tail window) into the HMAC, and write any portion that belongs
/// to the payload (not the trailing timestamp) into `payload_sink`.
///
/// `hmac_fed` is the running counter of bytes already fed; bytes
/// whose absolute offset is `< payload_len` are payload, bytes
/// `>= payload_len` are the timestamp (which is covered by HMAC
/// but is not part of the unsigned payload).
fn feed_evicted<W: Write>(
    hmac: &mut HmacSha256,
    payload_sink: &mut W,
    hmac_fed: &mut usize,
    payload_len: usize,
    bytes: &[u8],
) -> io::Result<()> {
    if *hmac_fed < payload_len {
        let payload_take = (payload_len - *hmac_fed).min(bytes.len());
        payload_sink.write_all(&bytes[..payload_take])?;
    }
    hmac.update(bytes);
    *hmac_fed += bytes.len();
    Ok(())
}

/// `read` a chunk with retry on `Interrupted`; returns 0 on EOF.
fn read_some<R: Read>(reader: &mut R, buf: &mut [u8]) -> io::Result<usize> {
    loop {
        match reader.read(buf) {
            Ok(n) => return Ok(n),
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
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

    #[test]
    fn signed_frame_round_trip_strips_suffix() {
        let frame = crate::protocol::frame::RequestFrame {
            request_id: 9,
            op_code: crate::protocol::opcodes::OP_REPLICA_BATCH,
            flags: 2,
            payload: b"batch".to_vec().into(),
        };
        let encoded = frame.encode();
        let signed = sign_frame(b"cluster-secret", &encoded).unwrap();
        assert_eq!(
            u32::from_le_bytes(signed[0..4].try_into().unwrap()) as usize,
            encoded.len() - 4 + SIGNED_SUFFIX_LEN,
            "signed frame length must include the auth suffix"
        );

        let verified = verify_frame(b"cluster-secret", &signed).unwrap();
        assert_eq!(verified, encoded);
        let (decoded, consumed) = crate::protocol::frame::RequestFrame::decode(&verified).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, frame);
    }

    #[test]
    fn signed_frame_rejects_unsigned_body() {
        let frame = crate::protocol::frame::ResponseFrame {
            request_id: 1,
            status: crate::protocol::opcodes::STATUS_OK,
            payload: b"ack".to_vec(),
        };
        let encoded = frame.encode();
        match verify_frame(b"cluster-secret", &encoded) {
            Ok(_) => panic!("unsigned frame must not verify"),
            Err(e) => assert_eq!(e.kind(), io::ErrorKind::InvalidData),
        }
    }

    // -- Streaming HMAC tests (C-7 / F-G5-016) --

    /// Counting sink that tracks how many bytes have been "written"
    /// without actually storing them. Used by the slow-loris test to
    /// prove that the streaming verifier doesn't itself allocate a
    /// payload-sized buffer in the bad-HMAC failure path.
    struct CountingSink {
        bytes_written: usize,
    }

    impl io::Write for CountingSink {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.bytes_written += buf.len();
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn streaming_verify_round_trip_matches_verify_frame() {
        // The streaming verifier on a `Cursor<&[u8]>` over a signed
        // frame produces exactly the same unsigned body that
        // `verify_frame` produces on the same input.
        let frame = crate::protocol::frame::RequestFrame {
            request_id: 42,
            op_code: crate::protocol::opcodes::OP_REPLICA_BATCH,
            flags: 1,
            payload: bytes::Bytes::from_static(b"hello-stream"),
        };
        let encoded = frame.encode();
        let signed = sign_frame(b"k", &encoded).unwrap();
        let baseline = verify_frame(b"k", &signed).expect("buffered path verifies");

        let mut reader = io::Cursor::new(&signed[..]);
        let mut sink = Vec::<u8>::new();
        let payload_len =
            verify_frame_streaming(b"k", &mut reader, &mut sink).expect("streaming verifies");
        // The streaming verifier emits the unsigned body (no length
        // prefix). The buffered `verify_frame` emits
        // `[length:4][unsigned_body]`. They should agree on the body.
        assert_eq!(payload_len, encoded.len() - 4);
        assert_eq!(payload_len, sink.len());
        assert_eq!(&baseline[4..], &sink[..]);
    }

    #[test]
    fn streaming_verify_rejects_wrong_tag() {
        let frame = crate::protocol::frame::RequestFrame {
            request_id: 1,
            op_code: crate::protocol::opcodes::OP_REPLICA_BATCH,
            flags: 0,
            payload: bytes::Bytes::from_static(b"x"),
        };
        let encoded = frame.encode();
        let mut signed = sign_frame(b"k", &encoded).unwrap();
        // Flip the last byte of the tag.
        let last = signed.len() - 1;
        signed[last] ^= 0xFF;

        let mut reader = io::Cursor::new(&signed[..]);
        let mut sink = Vec::<u8>::new();
        match verify_frame_streaming(b"k", &mut reader, &mut sink) {
            Err(e) => assert_eq!(e.kind(), io::ErrorKind::PermissionDenied),
            Ok(_) => panic!("streaming verifier must reject tampered tag"),
        }
    }

    #[test]
    fn streaming_verify_rejects_short_frame() {
        // body_len advertised < SIGNED_SUFFIX_LEN.
        let mut buf = Vec::new();
        buf.extend_from_slice(&(5u32).to_le_bytes());
        buf.extend_from_slice(&[0u8; 5]);
        let mut reader = io::Cursor::new(&buf[..]);
        let mut sink = io::sink();
        match verify_frame_streaming(b"k", &mut reader, &mut sink) {
            Err(e) => assert_eq!(e.kind(), io::ErrorKind::InvalidData),
            Ok(_) => panic!("streaming verifier must reject short frames"),
        }
    }

    #[test]
    fn streaming_verify_rejects_oversized_unsigned_body() {
        // body_len advertised so large that unsigned payload exceeds MAX_FRAME_SIZE.
        let oversized = MAX_FRAME_SIZE as u32 + SIGNED_SUFFIX_LEN as u32 + 1;
        let mut buf = Vec::new();
        buf.extend_from_slice(&oversized.to_le_bytes());
        // We don't bother filling the body — the bounds check should
        // reject before any read happens.
        let mut reader = io::Cursor::new(&buf[..]);
        let mut sink = io::sink();
        match verify_frame_streaming(b"k", &mut reader, &mut sink) {
            Err(e) => assert_eq!(e.kind(), io::ErrorKind::InvalidData),
            Ok(_) => panic!("streaming verifier must reject oversized frames"),
        }
    }

    #[test]
    fn streaming_verify_rejects_stale_timestamp() {
        // Forge a frame whose timestamp is far in the past. The HMAC
        // is recomputed correctly so the verifier should pass tag
        // check but fail the freshness window.
        let key = b"k";
        let now_ms = 1_700_000_000_000u64;
        let six_minutes_ago = now_ms - 6 * 60 * 1000;
        let body = sign_with_timestamp(key, b"payload", six_minutes_ago);
        let mut framed = Vec::with_capacity(4 + body.len());
        framed.extend_from_slice(&(body.len() as u32).to_le_bytes());
        framed.extend_from_slice(&body);

        let mut reader = io::Cursor::new(&framed[..]);
        let mut sink = Vec::<u8>::new();
        match verify_frame_streaming_with_now(key, &mut reader, &mut sink, now_ms) {
            Err(e) => {
                assert_eq!(e.kind(), io::ErrorKind::InvalidData);
                assert!(
                    e.to_string().contains("stale timestamp"),
                    "expected stale-timestamp error, got: {e}"
                );
            }
            Ok(_) => panic!("stale timestamp must be rejected"),
        }
    }

    #[test]
    fn streaming_verify_chunks_correctly_on_unaligned_reader() {
        // A Reader that returns at most 1 byte per `read()` call. The
        // streaming verifier must reassemble the tail window correctly
        // even when chunks are tiny.
        struct OneByteReader<'a> {
            data: &'a [u8],
            pos: usize,
        }
        impl<'a> io::Read for OneByteReader<'a> {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                if self.pos >= self.data.len() {
                    return Ok(0);
                }
                buf[0] = self.data[self.pos];
                self.pos += 1;
                Ok(1)
            }
        }

        let frame = crate::protocol::frame::RequestFrame {
            request_id: 7,
            op_code: crate::protocol::opcodes::OP_REPLICA_BATCH,
            flags: 0,
            payload: bytes::Bytes::from(vec![0xABu8; 1024]), // larger than the tail window
        };
        let encoded = frame.encode();
        let signed = sign_frame(b"key", &encoded).unwrap();

        let mut reader = OneByteReader {
            data: &signed,
            pos: 0,
        };
        let mut sink = Vec::<u8>::new();
        let payload_len = verify_frame_streaming(b"key", &mut reader, &mut sink).expect("verify");
        // `encoded` is `[length:4][body]`; the streaming verifier
        // emits just the body in `sink`.
        assert_eq!(payload_len, encoded.len() - 4);
        assert_eq!(&sink[..], &encoded[4..]);
    }

    #[test]
    fn slow_loris_16mib_wrong_hmac_rejects_without_buffering_payload() {
        // SLOW-LORIS REGRESSION (C-7 / F-G5-016).
        //
        // Build a 16 MiB signed frame body (the maximum the production
        // server will accept) where the HMAC tag is intentionally
        // wrong. The streaming verifier must reject this with
        // `PermissionDenied` without internally accumulating the
        // payload in a verifier-side buffer — a `CountingSink`
        // discards the bytes as they pass through.
        //
        // The acceptance property: the verifier hands the bytes off
        // to the sink as they stream, so the verifier itself never
        // allocates a 16 MiB Vec. The sink the test passes here
        // tracks count only; in production the caller similarly
        // passes either a Vec it is about to drop (success path
        // reuses it; failure path drops it) or `io::sink()`.
        let payload_len = MAX_FRAME_SIZE as usize;
        let body_len = payload_len + SIGNED_SUFFIX_LEN;
        let mut signed = Vec::with_capacity(4 + body_len);
        signed.extend_from_slice(&(body_len as u32).to_le_bytes());
        signed.extend(std::iter::repeat(0xAAu8).take(payload_len));
        // Timestamp + tag: 8 bytes of timestamp, then 32 bytes of
        // zeroes for the tag. The HMAC will (overwhelmingly) not
        // produce all-zeroes for this input, so verification fails
        // on tag mismatch.
        let timestamp = now_unix_ms().to_le_bytes();
        signed.extend_from_slice(&timestamp);
        signed.extend_from_slice(&[0u8; HMAC_TAG_LEN]);
        assert_eq!(signed.len(), 4 + body_len);

        let mut reader = io::Cursor::new(signed);
        let mut sink = CountingSink { bytes_written: 0 };
        match verify_frame_streaming(b"slow-loris-key", &mut reader, &mut sink) {
            Err(e) => assert_eq!(e.kind(), io::ErrorKind::PermissionDenied),
            Ok(_) => panic!("slow-loris wrong-HMAC frame must reject"),
        }
        // Sanity: the verifier did stream the full payload past the
        // sink before discovering the bad tag (the tag is the LAST
        // 32 bytes, so all earlier bytes flow through HMAC first).
        // The point of this test is that those bytes were never
        // accumulated inside the verifier — `CountingSink` proves
        // the verifier did not need its own copy of the payload to
        // reject.
        assert_eq!(sink.bytes_written, payload_len);
    }

    #[test]
    fn streaming_verify_propagates_io_error() {
        // A Reader that returns an io::Error on the first read.
        struct BrokenReader;
        impl io::Read for BrokenReader {
            fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::new(io::ErrorKind::ConnectionReset, "boom"))
            }
        }
        let mut reader = BrokenReader;
        let mut sink = io::sink();
        match verify_frame_streaming(b"k", &mut reader, &mut sink) {
            Err(e) => assert_eq!(e.kind(), io::ErrorKind::ConnectionReset),
            Ok(_) => panic!("must propagate underlying I/O error"),
        }
    }

    fn hex(data: &[u8]) -> String {
        data.iter().map(|b| format!("{b:02x}")).collect()
    }
}
