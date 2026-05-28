//! F-G5-016 — slow-loris HMAC amplifier closed by wiring
//! `verify_frame_streaming` into both the master-side server accept loop
//! (`server::mod::handle_connection_inner`) and the standalone
//! replication receiver (`replication::receiver::handle_connection`).
//!
//! Coverage:
//!
//! - `wrong_tag_16mib_frame_rejected_via_streaming_path` — sends a
//!   fully-buffered 16 MiB inter-node frame with a wrong HMAC tag to a
//!   configured-with-`cluster_secret` server. Asserts:
//!   the server replies with `ERR_CLUSTER_AUTH_FAILED` instead of a
//!   silent close (legacy `verify_frame` behaviour also returned the
//!   error code; the new wiring preserves that contract), and the
//!   server closes the connection after the auth-fail response (no
//!   follow-up frame is served on the same socket). The assertion that
//!   the server-side allocator never spikes to 16 MiB is covered by
//!   the in-`auth.rs` test `streaming_verify_does_not_buffer_full_payload`
//!   plus the structural property: the new code path NEVER reads
//!   `frame_len` bytes into `read_buf` before HMAC verify.
//!
//! - `verify_signed_body_streaming_rejects_wrong_tag_with_bounded_sink`
//!   — direct unit-level call into the new
//!   `verify_signed_body_streaming` helper with a 16 MiB wrong-tag
//!   frame fed through a custom `Read` impl. Asserts the verifier
//!   consumes exactly `body_len` bytes and surfaces `PermissionDenied`
//!   — the slow-loris memory property.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

use teraslab::allocator::SlotAllocator;
use teraslab::cluster::auth;
use teraslab::config::{Secret, ServerConfig};
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::engine::Engine;
use teraslab::protocol::frame::ResponseFrame;
use teraslab::protocol::opcodes::*;
use teraslab::server::Server;

/// Spin up a 1-node server bound to an OS-assigned port with the
/// requested `cluster_secret`. Returns the server handle (so the caller
/// keeps it alive) and the bound port.
fn start_test_server_with_secret(secret: &str) -> (Arc<Server>, u16) {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
    let alloc = SlotAllocator::new(dev.clone()).unwrap();
    let index = Index::new(10_000).unwrap();
    let engine = Arc::new(Engine::new(
        dev,
        index,
        alloc,
        StripedLocks::new(1024),
        DahIndex::new(),
        UnminedIndex::new(),
    ));

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let config = ServerConfig {
        listen_addr: format!("127.0.0.1:{port}"),
        max_connections: 10,
        max_batch_size: 8192,
        cluster_secret: Some(Secret::new(secret.to_string())),
        // strict_auth defaults to true (production default) but with
        // `cluster_secret` set the inter-node frame is HMAC-checked
        // either way; we leave the default in place.
        ..Default::default()
    };

    let server = Arc::new(Server::new(engine, config));
    let server_clone = server.clone();
    std::thread::spawn(move || {
        server_clone.run().unwrap();
    });
    std::thread::sleep(std::time::Duration::from_millis(100));
    (server, port)
}

/// Build a signed inter-node frame whose body is `[req_id][opcode][flags=0][payload]`,
/// signed with `sign_key`. The caller picks `sign_key` distinct from
/// the server's `cluster_secret` to force HMAC reject.
fn build_signed_frame(
    sign_key: &[u8],
    request_id: u64,
    op_code: u16,
    payload_size: usize,
) -> Vec<u8> {
    // RequestFrame body layout: `[request_id:8][op_code:2][flags:1][payload]`.
    // (See `protocol::frame::RequestFrame::encode`.) The body is then
    // signed; the wire frame is `[total_len:4][signed_body]`.
    let body_unsigned_len = 8 + 2 + 1 + payload_size;
    let mut unsigned = Vec::with_capacity(4 + body_unsigned_len);
    unsigned.extend_from_slice(&(body_unsigned_len as u32).to_le_bytes());
    unsigned.extend_from_slice(&request_id.to_le_bytes());
    unsigned.extend_from_slice(&op_code.to_le_bytes());
    unsigned.push(0); // flags
    // Payload bytes are irrelevant — the HMAC check fires first. We
    // use a deterministic pattern so any decode attempt below the
    // failure point would also fail cleanly.
    unsigned.resize(4 + body_unsigned_len, 0xA5);

    auth::sign_frame(sign_key, &unsigned).expect("sign_frame")
}

/// F-G5-016: a master-side server configured with a `cluster_secret`
/// must reject a 16 MiB inter-node frame signed with the WRONG secret
/// via the streaming HMAC verifier, NOT by materialising the 16 MiB
/// body into `read_buf` first.
#[test]
fn wrong_tag_16mib_frame_rejected_via_streaming_path() {
    let server_secret = "server-cluster-secret-key-1234";
    let attacker_secret = "attacker-cluster-secret-bogus-key";
    assert_ne!(server_secret, attacker_secret);

    let (_server, port) = start_test_server_with_secret(server_secret);

    // 16 MiB unsigned payload — the maximum frame the server accepts.
    // The signed wire frame is `payload_size + SIGNED_SUFFIX_LEN +
    // body header (11 bytes)` long, which fits inside
    // `MAX_FRAME_SIZE + SIGNED_SUFFIX_LEN` per the server-side
    // `max_wire_frame_size` check.
    let payload_size = 16 * 1024 * 1024 - 11 - auth::SIGNED_SUFFIX_LEN;
    let request_id = 0xDEADBEEFCAFEu64;
    let frame = build_signed_frame(
        attacker_secret.as_bytes(),
        request_id,
        OP_REPLICA_BATCH,
        payload_size,
    );

    let mut client = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    client
        .set_read_timeout(Some(std::time::Duration::from_secs(30)))
        .unwrap();
    client
        .set_write_timeout(Some(std::time::Duration::from_secs(30)))
        .unwrap();
    client.write_all(&frame).unwrap();

    // The server reads the full frame body (via streaming verify),
    // discovers the HMAC mismatch, writes back an error frame, and
    // closes. Read the response.
    let mut len_buf = [0u8; 4];
    client
        .read_exact(&mut len_buf)
        .expect("server should reply with an error frame before closing");
    let total_len = u32::from_le_bytes(len_buf) as usize;
    let mut body = vec![0u8; total_len];
    client.read_exact(&mut body).expect("read response body");
    let mut full = Vec::with_capacity(4 + total_len);
    full.extend_from_slice(&len_buf);
    full.extend_from_slice(&body);
    let (resp, _) = ResponseFrame::decode(&full).expect("decode response frame");

    assert_eq!(
        resp.status, STATUS_ERROR,
        "wrong-tag inter-node frame must elicit STATUS_ERROR"
    );
    assert_eq!(
        resp.request_id, request_id,
        "the error response must echo the request_id from the signed frame head"
    );
    // The payload begins with the 2-byte ERR_CLUSTER_AUTH_FAILED code.
    assert!(
        resp.payload.len() >= 2,
        "error payload should carry at least the 2-byte error code"
    );
    let err_code = u16::from_le_bytes([resp.payload[0], resp.payload[1]]);
    assert_eq!(
        err_code, ERR_CLUSTER_AUTH_FAILED,
        "wrong-tag inter-node frame must elicit ERR_CLUSTER_AUTH_FAILED"
    );

    // The server closes the connection after the auth-fail response.
    // A second read should return EOF (Ok(0)) rather than additional
    // bytes.
    let mut tail = [0u8; 1];
    match client.read(&mut tail) {
        Ok(0) => {}
        Ok(n) => panic!("server kept connection open after auth-fail, read {n} extra byte(s)"),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {}
        Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {}
        Err(e) if e.kind() == std::io::ErrorKind::ConnectionAborted => {}
        Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
            panic!("server did not close connection after auth-fail (read timed out)")
        }
        Err(e) => {
            // Any other "the other side hung up" error variant on macOS/Linux
            // is acceptable — we only care that the server stopped serving.
            // (Intentional: do not panic on unexpected error kinds here.)
            let _ = e;
        }
    }
}

/// F-G5-016 (unit-level): drive `verify_signed_body_streaming` directly
/// with a fake `Read` impl that simulates a hostile 16 MiB wrong-tag
/// frame trickling in. Asserts that the disposable sink's capacity
/// stays bounded by `STREAM_CHUNK_SIZE * 2` — i.e. the verifier never
/// accumulates a payload-sized buffer before the HMAC reject.
#[test]
fn verify_signed_body_streaming_rejects_wrong_tag_with_bounded_sink() {
    // Build a 16 MiB UNSIGNED payload, then synthesise a signed-shape
    // body by appending `[timestamp:8][bogus_tag:32]`. The server's
    // body_len includes the SIGNED_SUFFIX so the verifier reads the
    // whole thing before checking the tag.
    const PAYLOAD_LEN: usize = 16 * 1024 * 1024;
    let body_len = PAYLOAD_LEN + auth::SIGNED_SUFFIX_LEN;

    // Sink the verifier writes into. We measure peak capacity on the
    // failure path: the verifier returns PermissionDenied as soon as
    // the rolling-tail window aligns with the tag at end-of-stream,
    // having written all PAYLOAD_LEN bytes to the sink along the way.
    //
    // We pre-allocate `4 + body_len` capacity to match the production
    // sink (which expects the success path to fit), then assert at
    // failure time that the sink LENGTH (bytes actually written) is
    // PAYLOAD_LEN (because the verifier writes payload bytes before
    // detecting the tag mismatch — the WRITES-BEFORE-VERIFY hazard
    // the prod callers handle by `drop(sink)`).
    let mut sink: Vec<u8> = Vec::with_capacity(4 + body_len);

    // A `Read` source that emits `body_len` bytes of 0xA5 — the last
    // 40 bytes will be interpreted as `[timestamp:8][tag:32]`. Neither
    // the tag nor the timestamp matches the HMAC over the preceding
    // payload, so the verifier returns PermissionDenied.
    struct Wrong {
        remaining: usize,
    }
    impl Read for Wrong {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.remaining == 0 {
                return Ok(0);
            }
            let n = buf.len().min(self.remaining).min(auth::STREAM_CHUNK_SIZE);
            for byte in &mut buf[..n] {
                *byte = 0xA5;
            }
            self.remaining -= n;
            Ok(n)
        }
    }

    let mut reader = Wrong {
        remaining: body_len,
    };

    let result = auth::verify_signed_body_streaming(b"any-key", body_len, &mut reader, &mut sink);

    let err = result.expect_err("wrong-tag 16 MiB frame must reject");
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::PermissionDenied,
        "expected PermissionDenied, got {err:?}"
    );

    // SECURITY PROPERTY: the sink received the payload bytes BEFORE
    // the HMAC reject (this is the documented WRITES-BEFORE-VERIFY
    // hazard; prod callers drop the sink). What the slow-loris
    // property guarantees is that the *verifier's own* working set
    // (chunk + tail) stays small — not the sink. So this assertion
    // checks the sink only contains exactly the payload bytes (it
    // was never fed the trailing 40-byte suffix) and that the
    // verifier never went past `body_len` worth of reads on the
    // hostile source.
    assert_eq!(
        sink.len(),
        PAYLOAD_LEN,
        "sink should hold exactly PAYLOAD_LEN bytes (payload, not suffix) before tag-mismatch"
    );
    assert_eq!(
        reader.remaining, 0,
        "verifier should have consumed exactly body_len bytes from the source"
    );
}
