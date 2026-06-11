//! Wire-level error-code conformance tests — error-code triggerability
//! gaps T-1..T-6.
//!
//! Each test starts a real TCP server, sends framed requests as a client,
//! and asserts the EXACT top-level status, sparse item index, error code,
//! and error payload bytes the client receives. Engine-only or
//! dispatch-unit coverage deliberately does not appear here — these tests
//! pin the full client-observable wire contract.
//!
//! Mapping pinned (current `spend_error_to_batch_error`, post P3.10 /
//! F-G5-017 typed wire error codes):
//! - `DahOverflow` / `ReassignOverflow` / `StorageError` → `ERR_STORAGE_IO` (30)
//! - `Pruned` → `ERR_INVALID_SPEND` (6) + 36-byte preserved spending_data
//! - `ReservedSpendingData` → `ERR_INVALID_SPEND` (6) + EMPTY payload
//! - `DeletedChildren` → `ERR_DELETED_CHILDREN` (35) + 1-byte child_count
//! - dispatch internal-invariant paths → `ERR_INTERNAL` (255)

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

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

/// Start a server on a random port and return (server_handle, port).
///
/// Intentionally has NO blob store: T-1 relies on the
/// blobstore-not-configured dispatch invariant path.
fn start_test_server() -> (Arc<Server>, u16) {
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

fn connect(port: u16) -> TcpStream {
    let stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();
    stream
}

/// Send a request frame and receive a response frame over TCP.
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

fn test_txid(n: u32) -> [u8; 32] {
    let mut t = [0u8; 32];
    t[0..4].copy_from_slice(&n.to_le_bytes());
    t[8..12].copy_from_slice(&(n.wrapping_mul(0x9E37)).to_le_bytes());
    t[16..18].copy_from_slice(&(n as u16).to_le_bytes());
    t
}

fn test_utxo_hash(tx_n: u32, vout: u32) -> [u8; 32] {
    let mut h = [0u8; 32];
    h[0] = (vout & 0xFF) as u8;
    h[1] = ((vout >> 8) & 0xFF) as u8;
    h[4..8].copy_from_slice(&tx_n.to_le_bytes());
    h
}

/// Helper: create a simple record with N UTXOs.
fn make_create_item(txid: [u8; 32], utxo_count: u32, tx_n: u32) -> WireCreateItem {
    WireCreateItem {
        txid,
        tx_version: 2,
        locktime: 0,
        fee: 1000,
        size_in_bytes: 250,
        extended_size: 0,
        is_coinbase: false,
        spending_height: 0,
        created_at: 1700000000000,
        flags: 0,
        utxo_hashes: (0..utxo_count).map(|v| test_utxo_hash(tx_n, v)).collect(),
        cold_data: vec![],
        block_height: 0,
        mined_block_id: None,
        mined_block_height: None,
        mined_subtree_idx: None,
        parent_txids: vec![],
    }
}

fn create_records(stream: &mut TcpStream, items: &[WireCreateItem], req_id: u64) -> ResponseFrame {
    send_request(
        stream,
        &RequestFrame {
            request_id: req_id,
            op_code: OP_CREATE_BATCH,
            flags: 0,
            payload: encode_create_batch(items).into(),
        },
    )
}

fn spend(
    stream: &mut TcpStream,
    req_id: u64,
    params: &SpendBatchParams,
    items: &[WireSpendItem],
) -> ResponseFrame {
    send_request(
        stream,
        &RequestFrame {
            request_id: req_id,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload: encode_spend_batch(params, items).into(),
        },
    )
}

fn default_spend_params() -> SpendBatchParams {
    SpendBatchParams {
        ignore_conflicting: false,
        ignore_locked: false,
        current_block_height: 1000,
        block_height_retention: 288,
    }
}

/// 36-byte BSV spending data: child txid + LE vin.
fn spending_data(child_txid: [u8; 32], vin: u32) -> [u8; 36] {
    let mut sd = [0u8; 36];
    sd[..32].copy_from_slice(&child_txid);
    sd[32..36].copy_from_slice(&vin.to_le_bytes());
    sd
}

/// Decode the response as a sparse-error batch and assert exactly one
/// item error at `expected_index` with `expected_code`, returning it for
/// payload assertions.
fn assert_single_sparse_error(
    resp: &ResponseFrame,
    expected_index: u32,
    expected_code: u16,
) -> BatchItemError {
    assert_eq!(
        resp.status,
        STATUS_PARTIAL_ERROR,
        "expected STATUS_PARTIAL_ERROR carrying sparse code {expected_code}, got status={} payload_len={}",
        resp.status,
        resp.payload.len()
    );
    let errors = decode_sparse_errors(&resp.payload).unwrap();
    assert_eq!(errors.len(), 1, "expected exactly one sparse item error");
    assert_eq!(errors[0].item_index, expected_index);
    assert_eq!(errors[0].error_code, expected_code);
    errors[0].clone()
}

/// Build the inline cold-data blob for a child transaction whose single
/// extended input references `parent_txid` at `parent_vout`. Format
/// matches `extract_parent_txids_from_cold_data`:
/// outer `[inputs_len:4][inputs]`, inner `[count:4][len:4][txid:32 + vin:4]`.
fn child_cold_data(parent_txid: [u8; 32], parent_vout: u32) -> Vec<u8> {
    let mut extended_input = vec![0u8; 36];
    extended_input[..32].copy_from_slice(&parent_txid);
    extended_input[32..36].copy_from_slice(&parent_vout.to_le_bytes());
    let mut inputs_blob = Vec::new();
    inputs_blob.extend_from_slice(&1u32.to_le_bytes());
    inputs_blob.extend_from_slice(&(extended_input.len() as u32).to_le_bytes());
    inputs_blob.extend_from_slice(&extended_input);
    teraslab::ops::engine::build_cold_data(Some(&inputs_blob), None, None)
}

/// Create a child record (1 output) whose cold data references
/// `parent_txid`:`parent_vout`, then spend that parent slot with the
/// child's spending data. Returns the child's 36-byte spending data.
fn create_child_and_spend_parent(
    stream: &mut TcpStream,
    parent_txid: [u8; 32],
    parent_tx_n: u32,
    parent_vout: u32,
    child_txid: [u8; 32],
) -> [u8; 36] {
    let child = WireCreateItem {
        txid: child_txid,
        tx_version: 1,
        locktime: 0,
        fee: 500,
        size_in_bytes: 250,
        extended_size: 250,
        is_coinbase: false,
        spending_height: 0,
        created_at: 1700000000000,
        flags: 0,
        utxo_hashes: vec![[0xCC; 32]],
        cold_data: child_cold_data(parent_txid, parent_vout),
        block_height: 0,
        mined_block_id: None,
        mined_block_height: None,
        mined_subtree_idx: None,
        parent_txids: vec![],
    };
    let resp = create_records(stream, &[child], 2);
    assert_eq!(resp.status, STATUS_OK, "child create must succeed");

    let sd = spending_data(child_txid, parent_vout);
    let resp = spend(
        stream,
        3,
        &default_spend_params(),
        &[WireSpendItem {
            txid: parent_txid,
            vout: parent_vout,
            utxo_hash: test_utxo_hash(parent_tx_n, parent_vout),
            spending_data: sd,
        }],
    );
    assert_eq!(resp.status, STATUS_OK, "parent spend by child must succeed");
    sd
}

// ---------------------------------------------------------------------------
// T-1 — ERR_INTERNAL (255) reaches a real TCP client
// ---------------------------------------------------------------------------

/// T-1: a structurally valid `OP_STREAM_CHUNK` against a server with no
/// blob store configured hits the dispatch invariant-violation path and
/// must surface `ERR_INTERNAL` (255) in a `STATUS_ERROR` error payload —
/// deterministically, with no fault injection.
#[test]
fn t1_stream_chunk_without_blobstore_returns_err_internal() {
    let (server, port) = start_test_server();
    let mut stream = connect(port);

    let txid = test_txid(9101);
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 9101,
            op_code: OP_STREAM_CHUNK,
            flags: 0,
            payload: encode_stream_chunk(&txid, 0, b"chunk-data").into(),
        },
    );

    assert_eq!(resp.request_id, 9101);
    assert_eq!(resp.status, STATUS_ERROR);
    let (code, msg) = decode_error_payload(&resp.payload).unwrap();
    assert_eq!(code, ERR_INTERNAL, "expected wire code 255 (ERR_INTERNAL)");
    assert_eq!(msg, "blobstore not configured");

    server.shutdown();
}

// ---------------------------------------------------------------------------
// T-2 — ERR_STORAGE_IO (30), batch-wide apply-failure path
// ---------------------------------------------------------------------------

/// T-2: a storage-failing condition reachable from a plain client frame —
/// the spend apply path's DAH evaluation overflowing u32 (`u32::MAX`
/// current height + retention 1) — must abort the batch with top-level
/// `STATUS_ERROR` and wire code 30 (`ERR_STORAGE_IO`) in the error
/// payload. This pins the batch-wide `error_response(.., ERR_STORAGE_IO, ..)`
/// dispatch-site family that previously had zero client-observable tests.
#[test]
fn t2_spend_apply_dah_overflow_returns_batch_wide_err_storage_io() {
    let (server, port) = start_test_server();
    let mut stream = connect(port);

    let txid = test_txid(9201);
    let resp = create_records(&mut stream, &[make_create_item(txid, 1, 9201)], 1);
    assert_eq!(resp.status, STATUS_OK);

    let params = SpendBatchParams {
        ignore_conflicting: false,
        ignore_locked: false,
        current_block_height: u32::MAX,
        block_height_retention: 1,
    };
    let resp = spend(
        &mut stream,
        9202,
        &params,
        &[WireSpendItem {
            txid,
            vout: 0,
            utxo_hash: test_utxo_hash(9201, 0),
            spending_data: spending_data(test_txid(9202), 0),
        }],
    );

    assert_eq!(resp.request_id, 9202);
    assert_eq!(resp.status, STATUS_ERROR);
    let (code, msg) = decode_error_payload(&resp.payload).unwrap();
    assert_eq!(code, ERR_STORAGE_IO, "expected wire code 30 (ERR_STORAGE_IO)");
    assert!(
        msg.contains("DAH_OVERFLOW"),
        "error message must carry the DAH_OVERFLOW detail, got: {msg}"
    );

    server.shutdown();
}

// ---------------------------------------------------------------------------
// T-3 — DahOverflow / ReassignOverflow per-item mapping (→ 30)
// ---------------------------------------------------------------------------

/// T-3a: `SpendError::DahOverflow` surfaced per-item through
/// `spend_error_to_batch_error` — a set_mined at `current_block_height ==
/// u32::MAX` with retention 1 overflows the DAH computation. The client
/// must receive `STATUS_PARTIAL_ERROR` with sparse item 0 carrying wire
/// code 30 (`ERR_STORAGE_IO`, the current typed mapping — NOT a silent
/// saturating clamp, NOT the pre-P3.10 `ERR_INTERNAL`) and an empty
/// error payload.
#[test]
fn t3_set_mined_dah_overflow_returns_sparse_err_storage_io() {
    let (server, port) = start_test_server();
    let mut stream = connect(port);

    let txid = test_txid(9301);
    let resp = create_records(&mut stream, &[make_create_item(txid, 1, 9301)], 1);
    assert_eq!(resp.status, STATUS_OK);

    let params = SetMinedBatchParams {
        block_id: 7,
        block_height: 100,
        subtree_idx: 0,
        on_longest_chain: true,
        unset_mined: false,
        current_block_height: u32::MAX,
        block_height_retention: 1,
    };
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 9302,
            op_code: OP_SET_MINED_BATCH,
            flags: 0,
            payload: encode_set_mined_batch(&params, &[txid]).into(),
        },
    );

    assert_eq!(resp.request_id, 9302);
    let err = assert_single_sparse_error(&resp, 0, ERR_STORAGE_IO);
    assert!(
        err.error_data.is_empty(),
        "DahOverflow carries no error payload, got {:?}",
        err.error_data
    );

    server.shutdown();
}

/// T-3b: `SpendError::ReassignOverflow` — reassigning a frozen UTXO with
/// `block_height + spendable_after` overflowing u32 (the historic
/// saturating-add pin-forever bug, R-063/A-13). The client must receive
/// `STATUS_PARTIAL_ERROR` with sparse item 0 carrying wire code 30
/// (`ERR_STORAGE_IO`) and an empty error payload.
#[test]
fn t3_reassign_overflow_returns_sparse_err_storage_io() {
    let (server, port) = start_test_server();
    let mut stream = connect(port);

    let txid = test_txid(9311);
    let resp = create_records(&mut stream, &[make_create_item(txid, 1, 9311)], 1);
    assert_eq!(resp.status, STATUS_OK);

    // Reassign requires a FROZEN slot.
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 9312,
            op_code: OP_FREEZE_BATCH,
            flags: 0,
            payload: encode_slot_item_batch(&[WireSlotItem {
                txid,
                vout: 0,
                utxo_hash: test_utxo_hash(9311, 0),
            }])
            .into(),
        },
    );
    assert_eq!(resp.status, STATUS_OK, "freeze must succeed");

    let params = ReassignBatchParams {
        block_height: u32::MAX,
        spendable_after: 1,
    };
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 9313,
            op_code: OP_REASSIGN_BATCH,
            flags: 0,
            payload: encode_reassign_batch(
                &params,
                &[WireReassignItem {
                    txid,
                    vout: 0,
                    utxo_hash: test_utxo_hash(9311, 0),
                    new_utxo_hash: [0xAB; 32],
                }],
            )
            .into(),
        },
    );

    assert_eq!(resp.request_id, 9313);
    let err = assert_single_sparse_error(&resp, 0, ERR_STORAGE_IO);
    assert!(
        err.error_data.is_empty(),
        "ReassignOverflow carries no error payload, got {:?}",
        err.error_data
    );

    server.shutdown();
}

// ---------------------------------------------------------------------------
// T-4 — SpendError::Pruned → ERR_INVALID_SPEND (6) + preserved spending_data
// ---------------------------------------------------------------------------

/// T-4: spending a PRUNED slot. Setup: parent created, child spends parent
/// vout 0, child is deleted (R-119 prunes the parent slot it spent). A new
/// spend attempt on the pruned slot by a different transaction must
/// receive sparse wire code 6 (`ERR_INVALID_SPEND`) whose 36-byte error
/// payload is the slot's PRESERVED original spending data (R-015/A-07
/// forensic contract) — not the new attacker data, not an empty payload.
#[test]
fn t4_spend_pruned_slot_returns_invalid_spend_with_preserved_spending_data() {
    let (server, port) = start_test_server();
    let mut stream = connect(port);

    let parent_txid = test_txid(9401);
    let child_txid = test_txid(9402);
    let resp = create_records(&mut stream, &[make_create_item(parent_txid, 2, 9401)], 1);
    assert_eq!(resp.status, STATUS_OK);

    let child_sd = create_child_and_spend_parent(&mut stream, parent_txid, 9401, 0, child_txid);

    // Delete the child: R-119 flips parent slot 0 to UTXO_PRUNED.
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 9403,
            op_code: OP_DELETE_BATCH,
            flags: 0,
            payload: encode_txid_batch(&[child_txid], &[]).into(),
        },
    );
    assert_eq!(resp.status, STATUS_OK, "child delete must succeed");

    // Re-spend the pruned slot with a DIFFERENT transaction.
    let resp = spend(
        &mut stream,
        9404,
        &default_spend_params(),
        &[WireSpendItem {
            txid: parent_txid,
            vout: 0,
            utxo_hash: test_utxo_hash(9401, 0),
            spending_data: spending_data(test_txid(9405), 0),
        }],
    );

    assert_eq!(resp.request_id, 9404);
    let err = assert_single_sparse_error(&resp, 0, ERR_INVALID_SPEND);
    assert_eq!(
        err.error_data,
        child_sd.to_vec(),
        "Pruned rejection must carry the slot's preserved 36-byte spending data"
    );

    server.shutdown();
}

// ---------------------------------------------------------------------------
// T-5 — SpendError::DeletedChildren → ERR_DELETED_CHILDREN (35) + child_count
// ---------------------------------------------------------------------------

/// T-5: the F-X-022 `addDeletedChildren` anti-double-spend guard. Setup:
/// child spends parent vout 0, child is deleted (vout 0 pruned AND the
/// child txid is appended to the parent's deleted-children list). The
/// resurrected child then spends parent vout 1 (fresh slot — succeeds,
/// flipping it to SPENT with the child's spending data). The idempotent
/// re-spend of vout 1 hits the deleted-children defense-in-depth check
/// and must receive sparse wire code 35 (`ERR_DELETED_CHILDREN`) with the
/// 1-byte child_count payload — NOT a silent idempotent OK and NOT a
/// collapse into code 6.
#[test]
fn t5_deleted_children_respend_returns_code_35_with_child_count() {
    let (server, port) = start_test_server();
    let mut stream = connect(port);

    let parent_txid = test_txid(9501);
    let child_txid = test_txid(9502);
    let resp = create_records(&mut stream, &[make_create_item(parent_txid, 2, 9501)], 1);
    assert_eq!(resp.status, STATUS_OK);

    create_child_and_spend_parent(&mut stream, parent_txid, 9501, 0, child_txid);

    // Delete the child: prunes parent vout 0 and appends child_txid to the
    // parent's deleted-children list (count = 1).
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 9503,
            op_code: OP_DELETE_BATCH,
            flags: 0,
            payload: encode_txid_batch(&[child_txid], &[]).into(),
        },
    );
    assert_eq!(resp.status, STATUS_OK, "child delete must succeed");

    // The "resurrected" child spends parent vout 1 — a fresh UNSPENT slot,
    // so this first spend succeeds and stamps the child's spending data.
    let sd_vout1 = spending_data(child_txid, 1);
    let resp = spend(
        &mut stream,
        9504,
        &default_spend_params(),
        &[WireSpendItem {
            txid: parent_txid,
            vout: 1,
            utxo_hash: test_utxo_hash(9501, 1),
            spending_data: sd_vout1,
        }],
    );
    assert_eq!(resp.status, STATUS_OK, "fresh-slot spend must succeed");

    // Idempotent re-spend: slot reads SPENT by this exact child, but the
    // deleted-children list contradicts it — hard rejection, code 35.
    let resp = spend(
        &mut stream,
        9505,
        &default_spend_params(),
        &[WireSpendItem {
            txid: parent_txid,
            vout: 1,
            utxo_hash: test_utxo_hash(9501, 1),
            spending_data: sd_vout1,
        }],
    );

    assert_eq!(resp.request_id, 9505);
    let err = assert_single_sparse_error(&resp, 0, ERR_DELETED_CHILDREN);
    assert_eq!(
        err.error_data,
        vec![1u8],
        "DeletedChildren must carry the 1-byte child_count payload"
    );

    server.shutdown();
}

// ---------------------------------------------------------------------------
// T-6 — ReservedSpendingData → ERR_INVALID_SPEND (6) + EMPTY payload
// ---------------------------------------------------------------------------

/// T-6: the F-G2-002 slot-bricking guard. A spend whose spending_data is
/// the reserved all-0xFF frozen sentinel must be rejected with sparse
/// wire code 6 (`ERR_INVALID_SPEND`) and an EMPTY error payload — the
/// documented discriminator from real `InvalidSpend`/`Pruned` rejections,
/// which carry 36 bytes. The slot must remain spendable afterwards (the
/// guard exists precisely so this request cannot brick it).
#[test]
fn t6_reserved_spending_data_returns_invalid_spend_with_empty_payload() {
    let (server, port) = start_test_server();
    let mut stream = connect(port);

    let txid = test_txid(9601);
    let resp = create_records(&mut stream, &[make_create_item(txid, 1, 9601)], 1);
    assert_eq!(resp.status, STATUS_OK);

    let resp = spend(
        &mut stream,
        9602,
        &default_spend_params(),
        &[WireSpendItem {
            txid,
            vout: 0,
            utxo_hash: test_utxo_hash(9601, 0),
            spending_data: [0xFF; 36],
        }],
    );

    assert_eq!(resp.request_id, 9602);
    let err = assert_single_sparse_error(&resp, 0, ERR_INVALID_SPEND);
    assert!(
        err.error_data.is_empty(),
        "ReservedSpendingData must carry an EMPTY payload (the discriminator \
         from real InvalidSpend/Pruned, which carry 36 bytes), got {:?}",
        err.error_data
    );

    // The guard's whole point: the slot is NOT bricked — a legitimate
    // spend still succeeds.
    let resp = spend(
        &mut stream,
        9603,
        &default_spend_params(),
        &[WireSpendItem {
            txid,
            vout: 0,
            utxo_hash: test_utxo_hash(9601, 0),
            spending_data: spending_data(test_txid(9604), 0),
        }],
    );
    assert_eq!(resp.status, STATUS_OK, "slot must remain spendable");

    server.shutdown();
}
