//! F-G2 — secondary redb corruption → `ERR_INDEX_DEGRADED` (26) over the wire.
//!
//! The audit noted the degraded-secondary fallback (a corrupt on-disk
//! `dah.redb` / `unmined.redb` → start in degraded mode, reject
//! secondary-dependent ops with `ERR_INDEX_DEGRADED`) was only exercised at the
//! unit / pure-function level — never proven end-to-end with a deliberately
//! corrupt on-disk redb file and a real client reading code 26 off a socket.
//!
//! This test proves BOTH halves of the contract:
//!   1. A garbage on-disk redb secondary is *detected* as corrupt
//!      (`RedbDahIndex::open` fails) — i.e. the daemon's
//!      `dah_ok = false` decision is real, not synthetic.
//!   2. With the resulting degraded status installed, a DAH-dependent op
//!      (`OP_PROCESS_EXPIRED_PRESERVATIONS`) returns `ERR_INDEX_DEGRADED` (26)
//!      over a real TCP connection, while a primary-index read (`OP_GET_BATCH`)
//!      is NOT blocked — the degradation is scoped to the secondary.
//!
//! ## Why this is a single-test file
//!
//! The degraded readiness flags are a process-global static
//! (`set_secondary_status`). Mutating them races any sibling test in the same
//! binary that drives a secondary-dependent op (which is exactly why the
//! in-process unit tests use the pure `secondary_readiness_verdict` instead).
//! Keeping this file to ONE test guarantees no intra-binary race; statics are
//! per-test-binary, so it is isolated from every other suite.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

use tempfile::TempDir;
use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::redb_dah::RedbDahIndex;
use teraslab::index::{DahIndex, Index, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::engine::Engine;
use teraslab::protocol::codec::{decode_error_payload, encode_get_batch};
use teraslab::protocol::frame::{RequestFrame, ResponseFrame};
use teraslab::protocol::opcodes::{
    ERR_INDEX_DEGRADED, OP_GET_BATCH, OP_PROCESS_EXPIRED_PRESERVATIONS, STATUS_ERROR,
};
use teraslab::server::Server;
use teraslab::server::dispatch::{SecondaryStatus, set_secondary_status};

fn send_request(stream: &mut TcpStream, frame: &RequestFrame) -> ResponseFrame {
    let bytes = frame.encode();
    stream.write_all(&bytes).unwrap();
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).unwrap();
    let total_length = u32::from_le_bytes(len_buf) as usize;
    let mut body = vec![0u8; total_length];
    stream.read_exact(&mut body).unwrap();
    let mut full = Vec::with_capacity(4 + total_length);
    full.extend_from_slice(&len_buf);
    full.extend_from_slice(&body);
    let (response, _) = ResponseFrame::decode(&full).unwrap();
    response
}

#[test]
fn corrupt_secondary_redb_degrades_dah_ops_over_tcp_but_not_primary_reads() {
    // ---- Part 1: a garbage on-disk redb secondary is detected as corrupt. ----
    let tmp = TempDir::new().unwrap();
    let dah_path = tmp.path().join("dah.redb");
    std::fs::write(&dah_path, b"this is definitely not a redb database file").unwrap();
    let open_result = RedbDahIndex::open(&dah_path, 64 * 1024 * 1024);
    assert!(
        open_result.is_err(),
        "a garbage on-disk dah.redb must fail to open (this is what makes the \
         daemon set dah_ok=false); got Ok",
    );

    // ---- Part 2: install the resulting degraded status and prove the wire. ----
    // dah_ok=false models "the DAH secondary failed to (re)build at startup";
    // the unmined secondary is healthy.
    set_secondary_status(SecondaryStatus {
        dah_ok: false,
        unmined_ok: true,
    });
    // Restore the global on the way out so this process's static is left clean
    // even if an assertion panics.
    struct ResetGuard;
    impl Drop for ResetGuard {
        fn drop(&mut self) {
            set_secondary_status(SecondaryStatus {
                dah_ok: true,
                unmined_ok: true,
            });
        }
    }
    let _reset = ResetGuard;

    // Boot a real TCP server (in-memory primary — the primary is intact).
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
    let config = teraslab::config::ServerConfig {
        listen_addr: format!("127.0.0.1:{port}"),
        max_connections: 10,
        max_batch_size: 8192,
        ..Default::default()
    };
    let server = Arc::new(Server::new(engine, config));
    let server_clone = server.clone();
    std::thread::spawn(move || {
        server_clone.run().unwrap();
    });
    std::thread::sleep(std::time::Duration::from_millis(100));

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    // A DAH-dependent op must be rejected with ERR_INDEX_DEGRADED (26) on the wire.
    let dah_resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1,
            op_code: OP_PROCESS_EXPIRED_PRESERVATIONS,
            flags: 0,
            payload: vec![].into(),
        },
    );
    assert_eq!(
        dah_resp.status, STATUS_ERROR,
        "DAH op under a degraded DAH secondary must be a frame-level error",
    );
    let (code, _msg) = decode_error_payload(&dah_resp.payload).expect("typed error payload");
    assert_eq!(
        code, ERR_INDEX_DEGRADED,
        "DAH-dependent op must surface ERR_INDEX_DEGRADED (26) over TCP when the DAH \
         secondary is degraded",
    );

    // A primary-index read must NOT be gated by the degraded secondary.
    let get_resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 2,
            op_code: OP_GET_BATCH,
            flags: 0,
            payload: encode_get_batch(0, &[[0x11u8; 32]]).into(),
        },
    );
    if get_resp.status == STATUS_ERROR {
        let (code, _msg) =
            decode_error_payload(&get_resp.payload).expect("typed error payload on GET");
        assert_ne!(
            code, ERR_INDEX_DEGRADED,
            "a primary-index GET must NOT be blocked by a degraded secondary",
        );
    }

    server.shutdown();
}
