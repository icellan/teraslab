//! Audit Milestone 4 — H-1 / LM-1 (concurrent-stream cap) and H-2
//! (idle-stream reaper) end-to-end over a real TCP server backed by a
//! `FileBlobStore`.
//!
//! H-1/LM-1: `ConnectionState.streams` previously had no cap on the NUMBER
//! of concurrent in-progress streams, so one connection could open millions
//! of half-open `OP_STREAM_CHUNK` sessions — each holding an fd + tmp file +
//! hasher — and exhaust the process fd table. These tests pin the
//! `max_active_streams_per_connection` cap: opening past it is rejected with
//! `ERR_RATE_LIMITED`, existing streams are unaffected, and the rejected
//! open allocates no resources.
//!
//! H-2: abandoned streams were reaped ONLY on connection close (no idle
//! timer). A client that pinged but never finished a stream pinned its
//! resources indefinitely. These tests pin the per-stream idle reaper: a
//! stream that receives no chunk within `stream_idle_timeout_secs` is
//! aborted (its `.tmp` removed) on the next request, even though the
//! connection stays open, and a subsequent op on the reaped stream id
//! returns a clean `ERR_STREAM_NOT_FOUND`.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use teraslab::allocator::SlotAllocator;
use teraslab::config::ServerConfig;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::engine::Engine;
use teraslab::protocol::codec::*;
use teraslab::protocol::frame::*;
use teraslab::protocol::opcodes::*;
use teraslab::server::Server;
use teraslab::storage::blobstore::FileBlobStore;

/// Start a 1-node server backed by a `FileBlobStore` rooted at `blob_dir`,
/// with the requested stream caps. Returns the live server handle and port.
fn start_server(
    blob_dir: &Path,
    max_active_streams: usize,
    stream_idle_timeout_secs: u64,
) -> (Arc<Server>, u16) {
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
        max_active_streams_per_connection: max_active_streams,
        stream_idle_timeout_secs,
        ..Default::default()
    };

    let blob_store = Arc::new(FileBlobStore::new(blob_dir, 2));
    let server = Arc::new(Server::new(engine, config).with_blob_store(blob_store));
    let server_clone = server.clone();
    std::thread::spawn(move || {
        server_clone.run().unwrap();
    });
    std::thread::sleep(Duration::from_millis(100));
    (server, port)
}

fn connect(port: u16) -> TcpStream {
    let stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
}

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

fn txid(n: u32) -> [u8; 32] {
    let mut t = [0u8; 32];
    t[0..4].copy_from_slice(&n.to_le_bytes());
    t[28..32].copy_from_slice(&n.wrapping_mul(0x9E37).to_le_bytes());
    t
}

fn stream_chunk(stream: &mut TcpStream, req_id: u64, t: &[u8; 32], offset: u64, data: &[u8]) -> ResponseFrame {
    send_request(
        stream,
        &RequestFrame {
            request_id: req_id,
            op_code: OP_STREAM_CHUNK,
            flags: 0,
            payload: encode_stream_chunk(t, offset, data).into(),
        },
    )
}

fn stream_end(stream: &mut TcpStream, req_id: u64, t: &[u8; 32], total_size: u64) -> ResponseFrame {
    send_request(
        stream,
        &RequestFrame {
            request_id: req_id,
            op_code: OP_STREAM_END,
            flags: 0,
            payload: encode_stream_end(t, total_size).into(),
        },
    )
}

fn ping(stream: &mut TcpStream, req_id: u64) -> ResponseFrame {
    send_request(
        stream,
        &RequestFrame {
            request_id: req_id,
            op_code: OP_PING,
            flags: 0,
            payload: Vec::new().into(),
        },
    )
}

/// Recursively collect every `.tmp` file under `root`.
fn tmp_files(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(d) = stack.pop() {
        let entries = match std::fs::read_dir(&d) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            match entry.file_type() {
                Ok(ft) if ft.is_dir() => stack.push(path),
                Ok(_) => {
                    if let Some(name) = path.file_name().and_then(|n| n.to_str())
                        && name.contains(".tmp")
                    {
                        found.push(path);
                    }
                }
                Err(_) => {}
            }
        }
    }
    found
}

fn err_code(resp: &ResponseFrame) -> u16 {
    let (code, _msg) = decode_error_payload(&resp.payload).expect("typed error payload");
    code
}

/// H-1 / LM-1: a connection may hold at most
/// `max_active_streams_per_connection` concurrent in-progress streams. The
/// (cap+1)-th distinct stream is rejected with `ERR_RATE_LIMITED`; existing
/// streams keep working; the rejected open leaks no tmp file.
#[test]
fn concurrent_stream_cap_rejects_extra_open_over_tcp() {
    const CAP: usize = 3;
    let blob_dir = tempfile::tempdir().unwrap();
    // Disable the idle reaper here so it never confounds the cap accounting.
    let (server, port) = start_server(blob_dir.path(), CAP, 0);
    let mut client = connect(port);

    // Open CAP distinct streams (one chunk each, never ended).
    for n in 0..CAP as u32 {
        let resp = stream_chunk(&mut client, n as u64, &txid(n), 0, &[0xAB; 16]);
        assert_eq!(
            resp.status,
            STATUS_OK,
            "stream {n} under the cap must be accepted: {:?}",
            String::from_utf8_lossy(&resp.payload),
        );
    }

    // The (cap+1)-th distinct stream must be rejected with ERR_RATE_LIMITED.
    let over = txid(CAP as u32);
    let resp = stream_chunk(&mut client, 99, &over, 0, &[0xAB; 16]);
    assert_eq!(
        resp.status, STATUS_ERROR,
        "opening past the concurrent-stream cap must be rejected",
    );
    assert_eq!(
        err_code(&resp),
        ERR_RATE_LIMITED,
        "over-cap stream open must surface ERR_RATE_LIMITED",
    );

    // The rejected open must not have created a tmp file (begin_stream is
    // never reached). Only the CAP accepted streams have tmp files.
    let tmps = tmp_files(blob_dir.path());
    assert_eq!(
        tmps.len(),
        CAP,
        "only the {CAP} accepted streams should hold tmp files; found {tmps:?}",
    );

    // An existing stream can still receive further chunks.
    let resp = stream_chunk(&mut client, 200, &txid(0), 16, &[0xCD; 8]);
    assert_eq!(
        resp.status, STATUS_OK,
        "existing stream must keep accepting chunks after an over-cap reject",
    );

    server.shutdown();
}

/// H-2: a stream that receives no chunk within `stream_idle_timeout_secs`
/// is reaped — its `.tmp` removed and map entry freed — on the next request
/// on the SAME (still-open) connection, NOT only on connection close. A
/// subsequent `OP_STREAM_END` on the reaped id returns `ERR_STREAM_NOT_FOUND`.
#[test]
fn idle_stream_reaped_on_live_connection_and_tmp_removed() {
    let blob_dir = tempfile::tempdir().unwrap();
    // 1-second idle timeout, generous concurrent cap.
    let (server, port) = start_server(blob_dir.path(), 64, 1);
    let mut client = connect(port);

    let t = txid(0xBEEF);
    let resp = stream_chunk(&mut client, 1, &t, 0, &[0x55; 32]);
    assert_eq!(resp.status, STATUS_OK, "initial chunk must be accepted");

    // The in-progress stream holds exactly one tmp file.
    assert_eq!(
        tmp_files(blob_dir.path()).len(),
        1,
        "in-progress stream must hold one tmp file",
    );

    // Idle past the 1 s timeout WITHOUT touching the stream, then keep the
    // connection alive with a ping. The reaper runs before the ping is
    // dispatched and must abort the idle stream (removing its tmp file).
    std::thread::sleep(Duration::from_millis(1500));
    let resp = ping(&mut client, 2);
    assert_eq!(resp.status, STATUS_OK, "ping must succeed on the live connection");

    // Give the post-ping abort a beat to flush the unlink, then assert the
    // tmp file is gone even though the connection never closed.
    let mut remaining_tmp = tmp_files(blob_dir.path());
    for _ in 0..20 {
        if remaining_tmp.is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
        remaining_tmp = tmp_files(blob_dir.path());
    }
    assert!(
        remaining_tmp.is_empty(),
        "idle stream's tmp file must be removed by the reaper on the live \
         connection; still present: {remaining_tmp:?}",
    );

    // The reaped stream id is now unknown — ending it is a clean error.
    let resp = stream_end(&mut client, 3, &t, 32);
    assert_eq!(resp.status, STATUS_ERROR);
    assert_eq!(
        err_code(&resp),
        ERR_STREAM_NOT_FOUND,
        "ending a reaped stream must surface ERR_STREAM_NOT_FOUND",
    );

    server.shutdown();
}

/// Regression: a normal stream (open → chunks → end) still completes and the
/// blob is retrievable, with the idle reaper enabled and a normal cap — the
/// hardening must not break the happy path.
#[test]
fn normal_stream_completes_with_caps_enabled() {
    let blob_dir = tempfile::tempdir().unwrap();
    // Short idle timeout, but we send chunks quickly so it never trips.
    let (server, port) = start_server(blob_dir.path(), 64, 2);
    let mut client = connect(port);

    let t = txid(0xF00D);
    let part1 = vec![0x11u8; 1024];
    let part2 = vec![0x22u8; 2048];

    let resp = stream_chunk(&mut client, 1, &t, 0, &part1);
    assert_eq!(resp.status, STATUS_OK, "first chunk must succeed");
    let resp = stream_chunk(&mut client, 2, &t, part1.len() as u64, &part2);
    assert_eq!(resp.status, STATUS_OK, "second chunk must succeed");

    let total = (part1.len() + part2.len()) as u64;
    let resp = stream_end(&mut client, 3, &t, total);
    assert_eq!(
        resp.status, STATUS_OK,
        "stream end must commit the blob: {:?}",
        String::from_utf8_lossy(&resp.payload),
    );

    // No tmp files remain after a clean finish.
    assert!(
        tmp_files(blob_dir.path()).is_empty(),
        "a finished stream must leave no tmp files behind",
    );

    // The committed blob is readable directly from the store.
    let store = FileBlobStore::new(blob_dir.path(), 2);
    let bytes = teraslab::storage::blobstore::BlobStore::get(&store, &t)
        .unwrap()
        .expect("committed blob must be present after stream end");
    let mut expected = part1.clone();
    expected.extend_from_slice(&part2);
    assert_eq!(bytes, expected, "blob payload must match the streamed chunks");

    server.shutdown();
}
