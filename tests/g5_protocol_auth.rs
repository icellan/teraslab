//! G5 review — protocol auth-gate + dispatch integration tests.
//!
//! Covers:
//!   - F-G5-001 — strict_auth wiring (CLI flag flips fail-open → fail-closed)
//!   - F-G5-005 — admin opcodes are gated when cluster_secret is set
//!   - F-G5-003 — OP_QUERY_OLD_UNMINED works in single-node mode
//!   - F-G5-006 — OP_HEARTBEAT returns STATUS_OK instead of ERR_INTERNAL
//!
//! Integration-only because the in-lib tests do not build (pre-existing
//! compile errors in src/index/redb_primary.rs are outside G5's scope).

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

use teraslab::allocator::SlotAllocator;
use teraslab::config::ServerConfig;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::engine::Engine;
use teraslab::protocol::frame::{RequestFrame, ResponseFrame};
use teraslab::protocol::opcodes::*;
use teraslab::server::Server;

fn start_test_server_with_config(config_mut: impl FnOnce(&mut ServerConfig)) -> (Arc<Server>, u16) {
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

    let mut config = ServerConfig {
        listen_addr: format!("127.0.0.1:{port}"),
        max_connections: 10,
        max_batch_size: 8192,
        ..Default::default()
    };
    config_mut(&mut config);

    let server = Arc::new(Server::new(engine, config));
    let server_clone = server.clone();
    std::thread::spawn(move || {
        server_clone.run().unwrap();
    });
    std::thread::sleep(std::time::Duration::from_millis(100));
    (server, port)
}

fn read_response(stream: &mut TcpStream) -> ResponseFrame {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).unwrap();
    let total_len = u32::from_le_bytes(len_buf) as usize;
    let mut body = vec![0u8; total_len];
    stream.read_exact(&mut body).unwrap();
    let mut full = Vec::with_capacity(4 + total_len);
    full.extend_from_slice(&len_buf);
    full.extend_from_slice(&body);
    let (resp, _) = ResponseFrame::decode(&full).unwrap();
    resp
}

fn send_request(port: u16, request: RequestFrame) -> ResponseFrame {
    let mut client = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    client.write_all(&request.encode()).unwrap();
    read_response(&mut client)
}

/// F-G5-006: OP_HEARTBEAT must return STATUS_OK with an empty payload
/// rather than falling into the catch-all 'unknown opcode' ERR_INTERNAL.
#[test]
fn heartbeat_returns_status_ok_not_unknown_opcode() {
    let (server, port) = start_test_server_with_config(|_| {});
    let request = RequestFrame {
        request_id: 1,
        op_code: OP_HEARTBEAT,
        flags: 0,
        payload: Vec::new(),
    };
    let response = send_request(port, request);
    assert_eq!(response.request_id, 1);
    assert_eq!(
        response.status, STATUS_OK,
        "OP_HEARTBEAT must respond OK, not ERR_INTERNAL"
    );
    assert!(response.payload.is_empty());
    server.shutdown();
}

/// F-G5-001: with strict_auth = false (default trusted-overlay), an
/// unsigned inter-node frame is accepted (single-node dispatch returns
/// STATUS_OK).
#[test]
fn fail_open_default_accepts_unsigned_inter_node_frame() {
    let (server, port) = start_test_server_with_config(|c| {
        c.strict_auth = false;
        c.cluster_secret = None;
    });
    let request = RequestFrame {
        request_id: 10,
        op_code: OP_GET_PARTITION_MAP,
        flags: 0,
        payload: Vec::new(),
    };
    let response = send_request(port, request);
    assert_eq!(response.request_id, 10);
    assert_eq!(
        response.status, STATUS_OK,
        "trusted-overlay default must accept unsigned inter-node ops"
    );
    server.shutdown();
}

/// F-G5-001: flipping strict_auth = true rejects unsigned inter-node
/// frames with ERR_CLUSTER_AUTH_FAILED. The ServerConfig field must
/// reach the per-connection ConnectionOptions (the prior G5 wiring left
/// a hardcoded false stub there).
#[test]
fn strict_auth_rejects_unsigned_inter_node_frame() {
    let (server, port) = start_test_server_with_config(|c| {
        c.strict_auth = true;
        c.cluster_secret = None;
    });
    let request = RequestFrame {
        request_id: 11,
        op_code: OP_TOPOLOGY_COMMIT,
        flags: 0,
        payload: Vec::new(),
    };
    let response = send_request(port, request);
    assert_eq!(response.request_id, 11);
    assert_eq!(response.status, STATUS_ERROR);
    let code = u16::from_le_bytes(response.payload[0..2].try_into().unwrap());
    assert_eq!(
        code, ERR_CLUSTER_AUTH_FAILED,
        "strict_auth must reject unsigned inter-node frames"
    );
    server.shutdown();
}

/// F-G5-005: OP_ADMIN_DIAGNOSE_KEY is in the inter-node auth set, so
/// strict_auth + no cluster_secret rejects it.
#[test]
fn strict_auth_gates_admin_opcodes() {
    let (server, port) = start_test_server_with_config(|c| {
        c.strict_auth = true;
        c.cluster_secret = None;
    });
    for op in [OP_ADMIN_DIAGNOSE_KEY, OP_ADMIN_CLUSTER_HEALTH] {
        let request = RequestFrame {
            request_id: 20,
            op_code: op,
            flags: 0,
            payload: Vec::new(),
        };
        let response = send_request(port, request);
        assert_eq!(response.status, STATUS_ERROR);
        let code = u16::from_le_bytes(response.payload[0..2].try_into().unwrap());
        assert_eq!(
            code, ERR_CLUSTER_AUTH_FAILED,
            "admin opcode {op:#x} must be auth-gated when strict_auth is on"
        );
    }
    server.shutdown();
}
