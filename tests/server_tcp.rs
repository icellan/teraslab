//! TCP server integration tests.
//!
//! Starts an actual server on a random port, connects as a client,
//! sends wire protocol frames, and verifies responses.

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
use teraslab::storage::blobstore::{BlobStore, MemoryBlobStore};

/// Start a server on a random port and return (server_handle, port).
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

    // Bind to port 0 to get a random available port
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

    // Wait for server to start
    std::thread::sleep(std::time::Duration::from_millis(100));

    (server, port)
}

fn start_test_server_with_max_connections(max_connections: usize) -> (Arc<Server>, u16) {
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
        max_connections,
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

fn start_test_server_with_blob_store() -> (Arc<Server>, u16) {
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

    let blob_store: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
    let server = Arc::new(Server::new(engine, config).with_blob_store(blob_store));
    let server_clone = server.clone();

    std::thread::spawn(move || {
        server_clone.run().unwrap();
    });

    std::thread::sleep(std::time::Duration::from_millis(100));

    (server, port)
}

/// Send a request frame and receive a response frame over TCP.
fn send_request(stream: &mut TcpStream, frame: &RequestFrame) -> ResponseFrame {
    let bytes = frame.encode();
    stream.write_all(&bytes).unwrap();

    // Read response: 4-byte length, then payload
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

/// Helper: create a batch of records via the wire protocol.
fn create_records(stream: &mut TcpStream, items: &[WireCreateItem], req_id: u64) -> ResponseFrame {
    let payload = encode_create_batch(items);
    send_request(
        stream,
        &RequestFrame {
            request_id: req_id,
            op_code: OP_CREATE_BATCH,
            flags: 0,
            payload: payload.into(),
        },
    )
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

fn assert_single_sparse_error(resp: &ResponseFrame, expected_code: u16) -> BatchItemError {
    assert_eq!(
        resp.status,
        STATUS_PARTIAL_ERROR,
        "expected sparse error code {expected_code}, got status={} payload_len={}",
        resp.status,
        resp.payload.len()
    );
    let errors = decode_sparse_errors(&resp.payload).unwrap();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].item_index, 0);
    assert_eq!(errors[0].error_code, expected_code);
    errors[0].clone()
}

// ---------------------------------------------------------------------------
// Framing / basic tests
// ---------------------------------------------------------------------------

#[test]
fn ping_pong() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1,
            op_code: OP_PING,
            flags: 0,
            payload: vec![].into(),
        },
    );

    assert_eq!(resp.request_id, 1);
    assert_eq!(resp.status, STATUS_OK);

    server.shutdown();
}

// ---------------------------------------------------------------------------
// Create + Get tests
// ---------------------------------------------------------------------------

#[test]
fn create_10_then_get_batch_all() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    // Create 10 records
    let items: Vec<WireCreateItem> = (0..10u32)
        .map(|i| make_create_item(test_txid(200 + i), 3, 200 + i))
        .collect();
    let resp = create_records(&mut stream, &items, 100);
    assert_eq!(resp.status, STATUS_OK);

    // GetBatch all 10 with METADATA
    let txids: Vec<[u8; 32]> = (0..10u32).map(|i| test_txid(200 + i)).collect();
    let get_payload = encode_get_batch(FieldMask::ALL_METADATA, &txids);
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 101,
            op_code: OP_GET_BATCH,
            flags: 0,
            payload: get_payload.into(),
        },
    );
    assert_eq!(resp.status, STATUS_OK);

    let results = decode_get_response(&resp.payload).unwrap();
    assert_eq!(results.len(), 10);
    for r in &results {
        assert_eq!(r.status, 0); // All found
        assert!(!r.data.is_empty());
    }

    server.shutdown();
}

#[test]
fn create_then_get_spend() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    let txid = test_txid(1);
    let item = make_create_item(txid, 3, 1);
    let resp = create_records(&mut stream, &[item], 10);
    assert_eq!(resp.status, STATUS_OK, "create failed: {:?}", resp.payload);

    // GetSpend to verify the UTXO exists and is unspent
    let get_payload = encode_get_spend_batch(&[WireGetSpendItem {
        txid,
        vout: 0,
        utxo_hash: test_utxo_hash(1, 0),
    }]);
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 11,
            op_code: OP_GET_SPEND_BATCH,
            flags: 0,
            payload: get_payload.into(),
        },
    );
    assert_eq!(resp.status, STATUS_OK);

    let results = decode_get_spend_response(&resp.payload).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].status, 0);
    assert_eq!(results[0].slot_status, 0x00); // Unspent

    server.shutdown();
}

#[test]
fn get_spend_wire_validates_utxo_hash() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    let txid = test_txid(130);
    let resp = create_records(&mut stream, &[make_create_item(txid, 2, 130)], 130);
    assert_eq!(resp.status, STATUS_OK);

    let mut wrong_hash = test_utxo_hash(130, 0);
    wrong_hash[0] ^= 0xFF;
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 131,
            op_code: OP_GET_SPEND_BATCH,
            flags: 0,
            payload: encode_get_spend_batch(&[WireGetSpendItem {
                txid,
                vout: 0,
                utxo_hash: wrong_hash,
            }]).into(),

        },
    );
    assert_eq!(resp.status, STATUS_OK);
    let results = decode_get_spend_response(&resp.payload).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].status, 1);
    assert_eq!(results[0].error_code, ERR_UTXO_HASH_MISMATCH);

    server.shutdown();
}

#[test]
fn tcp_error_code_triggerability_core_item_errors() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    // 12 ALREADY_EXISTS: duplicate create of the same txid.
    let duplicate_txid = test_txid(1310);
    let item = make_create_item(duplicate_txid, 1, 1310);
    let resp = create_records(&mut stream, std::slice::from_ref(&item), 1310);
    assert_eq!(resp.status, STATUS_OK);
    let resp = create_records(&mut stream, &[item], 1311);
    assert_single_sparse_error(&resp, ERR_ALREADY_EXISTS);

    // 11 VOUT_OUT_OF_RANGE: spend an output beyond the slot count.
    let range_txid = test_txid(1312);
    let resp = create_records(&mut stream, &[make_create_item(range_txid, 1, 1312)], 1312);
    assert_eq!(resp.status, STATUS_OK);
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1313,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload: encode_spend_batch(
                &SpendBatchParams {
                    ignore_conflicting: false,
                    ignore_locked: false,
                    current_block_height: 1_000,
                    block_height_retention: 288,
                },
                &[WireSpendItem {
                    txid: range_txid,
                    vout: 9,
                    utxo_hash: test_utxo_hash(1312, 9),
                    spending_data: [0x11; 36],
                }],
            ).into(),

        },
    );
    assert_single_sparse_error(&resp, ERR_VOUT_OUT_OF_RANGE);

    // 4 ALREADY_FROZEN: freeze an already-frozen UTXO.
    let frozen_txid = test_txid(1314);
    let frozen_hash = test_utxo_hash(1314, 0);
    let resp = create_records(&mut stream, &[make_create_item(frozen_txid, 1, 1314)], 1314);
    assert_eq!(resp.status, STATUS_OK);
    let freeze_payload = encode_slot_item_batch(&[WireSlotItem {
        txid: frozen_txid,
        vout: 0,
        utxo_hash: frozen_hash,
    }]);
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1315,
            op_code: OP_FREEZE_BATCH,
            flags: 0,
            payload: freeze_payload.clone().into(),
        },
    );
    assert_eq!(resp.status, STATUS_OK);
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1316,
            op_code: OP_FREEZE_BATCH,
            flags: 0,
            payload: freeze_payload.into(),
        },
    );
    assert_single_sparse_error(&resp, ERR_ALREADY_FROZEN);

    // 5 UTXO_NOT_FROZEN: reassign requires the old UTXO to be frozen.
    let reassign_txid = test_txid(1317);
    let reassign_hash = test_utxo_hash(1317, 0);
    let resp = create_records(
        &mut stream,
        &[make_create_item(reassign_txid, 1, 1317)],
        1317,
    );
    assert_eq!(resp.status, STATUS_OK);
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1318,
            op_code: OP_REASSIGN_BATCH,
            flags: 0,
            payload: encode_reassign_batch(
                &ReassignBatchParams {
                    block_height: 1_000,
                    spendable_after: 5,
                },
                &[WireReassignItem {
                    txid: reassign_txid,
                    vout: 0,
                    utxo_hash: reassign_hash,
                    new_utxo_hash: test_utxo_hash(99_999, 0),
                }],
            ).into(),

        },
    );
    assert_single_sparse_error(&resp, ERR_UTXO_NOT_FROZEN);

    // 6 INVALID_SPEND: wrong unspend marker must not clear a real spend.
    let unspend_txid = test_txid(1319);
    let unspend_hash = test_utxo_hash(1319, 0);
    let resp = create_records(
        &mut stream,
        &[make_create_item(unspend_txid, 1, 1319)],
        1319,
    );
    assert_eq!(resp.status, STATUS_OK);
    let good_spending_data = [0x22; 36];
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1320,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload: encode_spend_batch(
                &SpendBatchParams {
                    ignore_conflicting: false,
                    ignore_locked: false,
                    current_block_height: 1_000,
                    block_height_retention: 288,
                },
                &[WireSpendItem {
                    txid: unspend_txid,
                    vout: 0,
                    utxo_hash: unspend_hash,
                    spending_data: good_spending_data,
                }],
            ).into(),

        },
    );
    assert_eq!(resp.status, STATUS_OK);
    let err = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1321,
            op_code: OP_UNSPEND_BATCH,
            flags: 0,
            payload: encode_unspend_batch(
                &UnspendBatchParams {
                    current_block_height: 1_000,
                    block_height_retention: 288,
                },
                &[WireUnspendItem {
                    txid: unspend_txid,
                    vout: 0,
                    utxo_hash: unspend_hash,
                    spending_data: [0x33; 36],
                }],
            ).into(),

        },
    );
    let err = assert_single_sparse_error(&err, ERR_INVALID_SPEND);
    assert_eq!(err.error_data, good_spending_data);

    // 13 FROZEN_UNTIL: reassign cooldown blocks spend until the target height.
    let cooldown_txid = test_txid(1322);
    let cooldown_hash = test_utxo_hash(1322, 0);
    let cooldown_new_hash = test_utxo_hash(1323, 0);
    let resp = create_records(
        &mut stream,
        &[make_create_item(cooldown_txid, 1, 1322)],
        1322,
    );
    assert_eq!(resp.status, STATUS_OK);
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1323,
            op_code: OP_FREEZE_BATCH,
            flags: 0,
            payload: encode_slot_item_batch(&[WireSlotItem {
                txid: cooldown_txid,
                vout: 0,
                utxo_hash: cooldown_hash,
            }]).into(),

        },
    );
    assert_eq!(resp.status, STATUS_OK);
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1324,
            op_code: OP_REASSIGN_BATCH,
            flags: 0,
            payload: encode_reassign_batch(
                &ReassignBatchParams {
                    block_height: 1_000,
                    spendable_after: 10,
                },
                &[WireReassignItem {
                    txid: cooldown_txid,
                    vout: 0,
                    utxo_hash: cooldown_hash,
                    new_utxo_hash: cooldown_new_hash,
                }],
            ).into(),

        },
    );
    assert_eq!(resp.status, STATUS_OK);
    let err = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1325,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload: encode_spend_batch(
                &SpendBatchParams {
                    ignore_conflicting: false,
                    ignore_locked: false,
                    current_block_height: 1_005,
                    block_height_retention: 288,
                },
                &[WireSpendItem {
                    txid: cooldown_txid,
                    vout: 0,
                    utxo_hash: cooldown_new_hash,
                    spending_data: [0x44; 36],
                }],
            ).into(),

        },
    );
    let err = assert_single_sparse_error(&err, ERR_FROZEN_UNTIL);
    assert_eq!(err.error_data, 1_010u32.to_le_bytes());

    // 3 ALREADY_SPENT: second spend with different spending_data returns the
    // original 36-byte winner payload.
    let spent_txid = test_txid(1326);
    let spent_hash = test_utxo_hash(1326, 0);
    let resp = create_records(&mut stream, &[make_create_item(spent_txid, 1, 1326)], 1326);
    assert_eq!(resp.status, STATUS_OK);
    let winner_spending_data = [0x55; 36];
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1326,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload: encode_spend_batch(
                &SpendBatchParams {
                    ignore_conflicting: false,
                    ignore_locked: false,
                    current_block_height: 1_000,
                    block_height_retention: 288,
                },
                &[WireSpendItem {
                    txid: spent_txid,
                    vout: 0,
                    utxo_hash: spent_hash,
                    spending_data: winner_spending_data,
                }],
            ).into(),

        },
    );
    assert_eq!(resp.status, STATUS_OK);
    let err = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1327,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload: encode_spend_batch(
                &SpendBatchParams {
                    ignore_conflicting: false,
                    ignore_locked: false,
                    current_block_height: 1_000,
                    block_height_retention: 288,
                },
                &[WireSpendItem {
                    txid: spent_txid,
                    vout: 0,
                    utxo_hash: spent_hash,
                    spending_data: [0x66; 36],
                }],
            ).into(),

        },
    );
    let err = assert_single_sparse_error(&err, ERR_ALREADY_SPENT);
    assert_eq!(err.error_data, winner_spending_data);

    // 7 FROZEN: frozen UTXO cannot be spent.
    let frozen_spend_txid = test_txid(1328);
    let frozen_spend_hash = test_utxo_hash(1328, 0);
    let mut frozen_item = make_create_item(frozen_spend_txid, 1, 1328);
    frozen_item.flags = 0x04;
    let resp = create_records(&mut stream, &[frozen_item], 1328);
    assert_eq!(resp.status, STATUS_OK);
    let err = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1328,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload: encode_spend_batch(
                &SpendBatchParams {
                    ignore_conflicting: false,
                    ignore_locked: false,
                    current_block_height: 1_000,
                    block_height_retention: 288,
                },
                &[WireSpendItem {
                    txid: frozen_spend_txid,
                    vout: 0,
                    utxo_hash: frozen_spend_hash,
                    spending_data: [0x77; 36],
                }],
            ).into(),

        },
    );
    assert_single_sparse_error(&err, ERR_FROZEN);

    // 8 CONFLICTING: conflicting UTXO cannot be spent unless explicitly ignored.
    let conflicting_txid = test_txid(1329);
    let conflicting_hash = test_utxo_hash(1329, 0);
    let mut conflicting_item = make_create_item(conflicting_txid, 1, 1329);
    conflicting_item.flags = 0x02;
    let resp = create_records(&mut stream, &[conflicting_item], 1329);
    assert_eq!(resp.status, STATUS_OK);
    let err = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1329,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload: encode_spend_batch(
                &SpendBatchParams {
                    ignore_conflicting: false,
                    ignore_locked: false,
                    current_block_height: 1_000,
                    block_height_retention: 288,
                },
                &[WireSpendItem {
                    txid: conflicting_txid,
                    vout: 0,
                    utxo_hash: conflicting_hash,
                    spending_data: [0x88; 36],
                }],
            ).into(),

        },
    );
    assert_single_sparse_error(&err, ERR_CONFLICTING);

    // 9 LOCKED: locked UTXO cannot be spent unless explicitly ignored.
    let locked_txid = test_txid(1330);
    let locked_hash = test_utxo_hash(1330, 0);
    let mut locked_item = make_create_item(locked_txid, 1, 1330);
    locked_item.flags = 0x01;
    let resp = create_records(&mut stream, &[locked_item], 1330);
    assert_eq!(resp.status, STATUS_OK);
    let err = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1330,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload: encode_spend_batch(
                &SpendBatchParams {
                    ignore_conflicting: false,
                    ignore_locked: false,
                    current_block_height: 1_000,
                    block_height_retention: 288,
                },
                &[WireSpendItem {
                    txid: locked_txid,
                    vout: 0,
                    utxo_hash: locked_hash,
                    spending_data: [0x99; 36],
                }],
            ).into(),

        },
    );
    assert_single_sparse_error(&err, ERR_LOCKED);

    // 10 COINBASE_IMMATURE: immature coinbase carries the maturity height.
    let coinbase_txid = test_txid(1331);
    let coinbase_hash = test_utxo_hash(1331, 0);
    let mut coinbase_item = make_create_item(coinbase_txid, 1, 1331);
    coinbase_item.is_coinbase = true;
    coinbase_item.spending_height = 1_100;
    let resp = create_records(&mut stream, &[coinbase_item], 1331);
    assert_eq!(resp.status, STATUS_OK);
    let err = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1331,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload: encode_spend_batch(
                &SpendBatchParams {
                    ignore_conflicting: false,
                    ignore_locked: false,
                    current_block_height: 1_050,
                    block_height_retention: 288,
                },
                &[WireSpendItem {
                    txid: coinbase_txid,
                    vout: 0,
                    utxo_hash: coinbase_hash,
                    spending_data: [0xAA; 36],
                }],
            ).into(),

        },
    );
    let err = assert_single_sparse_error(&err, ERR_COINBASE_IMMATURE);
    assert_eq!(err.error_data, 1_100u32.to_le_bytes());

    server.shutdown();
}

#[test]
fn create_spend_get_spend() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    let txid = test_txid(2);
    let hash0 = test_utxo_hash(2, 0);

    let resp = create_records(&mut stream, &[make_create_item(txid, 5, 2)], 20);
    assert_eq!(resp.status, STATUS_OK);

    // Spend UTXO 0
    let mut sd = [0u8; 36];
    sd[0] = 0xAB;
    let spend_payload = encode_spend_batch(
        &SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        },
        &[WireSpendItem {
            txid,
            vout: 0,
            utxo_hash: hash0,
            spending_data: sd,
        }],
    );
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 21,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload: spend_payload.into(),
        },
    );
    assert_eq!(resp.status, STATUS_OK);

    // GetSpend — should show spent
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 22,
            op_code: OP_GET_SPEND_BATCH,
            flags: 0,
            payload: encode_get_spend_batch(&[WireGetSpendItem {
                txid,
                vout: 0,
                utxo_hash: test_utxo_hash(2, 0),
            }]).into(),

        },
    );
    let results = decode_get_spend_response(&resp.payload).unwrap();
    assert_eq!(results[0].slot_status, 0x01); // Spent
    assert_eq!(results[0].spending_data[0], 0xAB);

    server.shutdown();
}

#[test]
fn create_spend_across_multiple_txids_then_get() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    // Create 3 records
    let items: Vec<WireCreateItem> = (0..3u32)
        .map(|i| make_create_item(test_txid(300 + i), 2, 300 + i))
        .collect();
    let resp = create_records(&mut stream, &items, 300);
    assert_eq!(resp.status, STATUS_OK);

    // Spend across all 3 txids in a single batch
    let spend_items: Vec<WireSpendItem> = (0..3u32)
        .map(|i| {
            let mut sd = [0u8; 36];
            sd[0] = (i + 1) as u8;
            WireSpendItem {
                txid: test_txid(300 + i),
                vout: 0,
                utxo_hash: test_utxo_hash(300 + i, 0),
                spending_data: sd,
            }
        })
        .collect();
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 301,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload: encode_spend_batch(
                &SpendBatchParams {
                    ignore_conflicting: false,
                    ignore_locked: false,
                    current_block_height: 1000,
                    block_height_retention: 288,
                },
                &spend_items,
            ).into(),

        },
    );
    assert_eq!(resp.status, STATUS_OK);

    // Verify all 3 via GetBatch
    let txids: Vec<[u8; 32]> = (0..3u32).map(|i| test_txid(300 + i)).collect();
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 302,
            op_code: OP_GET_BATCH,
            flags: 0,
            payload: encode_get_batch(FieldMask::ALL_METADATA | FieldMask::UTXO_SLOTS, &txids).into(),
        },
    );
    assert_eq!(resp.status, STATUS_OK);
    let results = decode_get_response(&resp.payload).unwrap();
    assert_eq!(results.len(), 3);
    for r in &results {
        assert_eq!(r.status, 0);
    }

    server.shutdown();
}

// ---------------------------------------------------------------------------
// SetMined + MarkLongestChain
// ---------------------------------------------------------------------------

#[test]
fn create_set_mined_delete() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    let txid = test_txid(3);
    let resp = create_records(&mut stream, &[make_create_item(txid, 2, 3)], 30);
    assert_eq!(resp.status, STATUS_OK);

    // SetMined
    let mined_payload = encode_set_mined_batch(
        &SetMinedBatchParams {
            block_id: 42,
            block_height: 1000,
            subtree_idx: 0,
            on_longest_chain: true,
            unset_mined: false,
            current_block_height: 1000,
            block_height_retention: 288,
        },
        &[txid],
    );
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 31,
            op_code: OP_SET_MINED_BATCH,
            flags: 0,
            payload: mined_payload.into(),
        },
    );
    assert_eq!(resp.status, STATUS_OK);

    // Delete
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 32,
            op_code: OP_DELETE_BATCH,
            flags: 0,
            payload: encode_txid_batch(&[txid], &[]).into(),
        },
    );
    assert_eq!(resp.status, STATUS_OK);

    // GetSpend after delete — should be not found
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 33,
            op_code: OP_GET_SPEND_BATCH,
            flags: 0,
            payload: encode_get_spend_batch(&[WireGetSpendItem {
                txid,
                vout: 0,
                utxo_hash: test_utxo_hash(3, 0),
            }]).into(),

        },
    );
    let results = decode_get_spend_response(&resp.payload).unwrap();
    assert_eq!(results[0].status, 1);
    assert_eq!(results[0].error_code, ERR_TX_NOT_FOUND);

    server.shutdown();
}

#[test]
fn create_set_mined_mark_longest_chain() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    let txid = test_txid(400);
    let resp = create_records(&mut stream, &[make_create_item(txid, 2, 400)], 400);
    assert_eq!(resp.status, STATUS_OK);

    // SetMined
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 401,
            op_code: OP_SET_MINED_BATCH,
            flags: 0,
            payload: encode_set_mined_batch(
                &SetMinedBatchParams {
                    block_id: 100,
                    block_height: 5000,
                    subtree_idx: 0,
                    on_longest_chain: true,
                    unset_mined: false,
                    current_block_height: 5000,
                    block_height_retention: 288,
                },
                &[txid],
            ).into(),

        },
    );
    assert_eq!(resp.status, STATUS_OK);

    // MarkLongestChain (off)
    let mut shared = Vec::new();
    shared.push(0); // not on longest chain
    shared.extend_from_slice(&5001u32.to_le_bytes());
    shared.extend_from_slice(&288u32.to_le_bytes());
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 402,
            op_code: OP_MARK_LONGEST_CHAIN_BATCH,
            flags: 0,
            payload: encode_txid_batch(&[txid], &shared).into(),
        },
    );
    assert_eq!(resp.status, STATUS_OK);

    // GetBatch to verify unmined_since was updated
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 403,
            op_code: OP_GET_BATCH,
            flags: 0,
            payload: encode_get_batch(FieldMask::ALL_METADATA, &[txid]).into(),
        },
    );
    assert_eq!(resp.status, STATUS_OK);
    let results = decode_get_response(&resp.payload).unwrap();
    assert_eq!(results[0].status, 0);

    server.shutdown();
}

// ---------------------------------------------------------------------------
// Freeze / Unfreeze / Reassign
// ---------------------------------------------------------------------------

#[test]
fn freeze_unfreeze_over_tcp() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    let txid = test_txid(4);
    let hash0 = test_utxo_hash(4, 0);

    let resp = create_records(&mut stream, &[make_create_item(txid, 3, 4)], 40);
    assert_eq!(resp.status, STATUS_OK);

    // Freeze
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 41,
            op_code: OP_FREEZE_BATCH,
            flags: 0,
            payload: encode_slot_item_batch(&[WireSlotItem {
                txid,
                vout: 0,
                utxo_hash: hash0,
            }]).into(),

        },
    );
    assert_eq!(resp.status, STATUS_OK);

    // Verify frozen via GetSpend
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 42,
            op_code: OP_GET_SPEND_BATCH,
            flags: 0,
            payload: encode_get_spend_batch(&[WireGetSpendItem {
                txid,
                vout: 0,
                utxo_hash: test_utxo_hash(4, 0),
            }]).into(),

        },
    );
    let results = decode_get_spend_response(&resp.payload).unwrap();
    assert_eq!(results[0].slot_status, 0xFF); // Frozen

    // Unfreeze
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 43,
            op_code: OP_UNFREEZE_BATCH,
            flags: 0,
            payload: encode_slot_item_batch(&[WireSlotItem {
                txid,
                vout: 0,
                utxo_hash: hash0,
            }]).into(),

        },
    );
    assert_eq!(resp.status, STATUS_OK);

    // Verify unspent again
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 44,
            op_code: OP_GET_SPEND_BATCH,
            flags: 0,
            payload: encode_get_spend_batch(&[WireGetSpendItem {
                txid,
                vout: 0,
                utxo_hash: test_utxo_hash(4, 0),
            }]).into(),

        },
    );
    let results = decode_get_spend_response(&resp.payload).unwrap();
    assert_eq!(results[0].slot_status, 0x00); // Unspent

    server.shutdown();
}

#[test]
fn freeze_reassign_get_spend() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    let txid = test_txid(500);
    let hash0 = test_utxo_hash(500, 0);
    let new_hash = {
        let mut h = [0u8; 32];
        h[0] = 0xDE;
        h[1] = 0xAD;
        h
    };

    let resp = create_records(&mut stream, &[make_create_item(txid, 2, 500)], 500);
    assert_eq!(resp.status, STATUS_OK);

    // Freeze
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 501,
            op_code: OP_FREEZE_BATCH,
            flags: 0,
            payload: encode_slot_item_batch(&[WireSlotItem {
                txid,
                vout: 0,
                utxo_hash: hash0,
            }]).into(),

        },
    );
    assert_eq!(resp.status, STATUS_OK);

    // Reassign
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 502,
            op_code: OP_REASSIGN_BATCH,
            flags: 0,
            payload: encode_reassign_batch(
                &ReassignBatchParams {
                    block_height: 1000,
                    spendable_after: 100,
                },
                &[WireReassignItem {
                    txid,
                    vout: 0,
                    utxo_hash: hash0,
                    new_utxo_hash: new_hash,
                }],
            ).into(),

        },
    );
    assert_eq!(resp.status, STATUS_OK);

    // GetSpend should show unspent (reassign unfreezes)
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 503,
            op_code: OP_GET_SPEND_BATCH,
            flags: 0,
            payload: encode_get_spend_batch(&[WireGetSpendItem {
                txid,
                vout: 0,
                utxo_hash: new_hash,
            }]).into(),

        },
    );
    let results = decode_get_spend_response(&resp.payload).unwrap();
    assert_eq!(results[0].slot_status, 0x00); // Unspent after reassign

    server.shutdown();
}

// ---------------------------------------------------------------------------
// SetConflicting / SetLocked / PreserveUntil
// ---------------------------------------------------------------------------

#[test]
fn create_set_conflicting() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    let txid = test_txid(600);
    let resp = create_records(&mut stream, &[make_create_item(txid, 2, 600)], 600);
    assert_eq!(resp.status, STATUS_OK);

    // SetConflicting
    let mut shared = Vec::new();
    shared.push(1); // value=true
    shared.extend_from_slice(&1000u32.to_le_bytes());
    shared.extend_from_slice(&288u32.to_le_bytes());
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 601,
            op_code: OP_SET_CONFLICTING_BATCH,
            flags: 0,
            payload: encode_txid_batch(&[txid], &shared).into(),
        },
    );
    assert_eq!(resp.status, STATUS_OK);

    // GetBatch to verify flag
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 602,
            op_code: OP_GET_BATCH,
            flags: 0,
            payload: encode_get_batch(FieldMask::ALL_METADATA, &[txid]).into(),
        },
    );
    assert_eq!(resp.status, STATUS_OK);
    let results = decode_get_response(&resp.payload).unwrap();
    assert_eq!(results[0].status, 0);
    // flags is at offset: tx_version(4)+locktime(4)+fee(8)+size(8)+ext(8) = 32
    let flags = results[0].data[32];
    assert!(flags & 0x02 != 0, "CONFLICTING flag should be set");

    server.shutdown();
}

#[test]
fn create_set_locked() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    let txid = test_txid(700);
    let resp = create_records(&mut stream, &[make_create_item(txid, 2, 700)], 700);
    assert_eq!(resp.status, STATUS_OK);

    // SetLocked
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 701,
            op_code: OP_SET_LOCKED_BATCH,
            flags: 0,
            payload: encode_txid_batch(&[txid], &[1u8]).into(), // value=true
        },

    );
    assert_eq!(resp.status, STATUS_OK);

    // GetBatch to verify locked flag
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 702,
            op_code: OP_GET_BATCH,
            flags: 0,
            payload: encode_get_batch(FieldMask::ALL_METADATA, &[txid]).into(),
        },
    );
    let results = decode_get_response(&resp.payload).unwrap();
    let flags = results[0].data[32];
    assert!(flags & 0x04 != 0, "LOCKED flag should be set");

    server.shutdown();
}

#[test]
fn create_preserve_until_get() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    let txid = test_txid(800);
    let resp = create_records(&mut stream, &[make_create_item(txid, 2, 800)], 800);
    assert_eq!(resp.status, STATUS_OK);

    // PreserveUntil
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 801,
            op_code: OP_PRESERVE_UNTIL_BATCH,
            flags: 0,
            payload: encode_txid_batch(&[txid], &5000u32.to_le_bytes()).into(),
        },
    );
    assert_eq!(resp.status, STATUS_OK);

    // GetBatch to verify preserve_until field
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 802,
            op_code: OP_GET_BATCH,
            flags: 0,
            payload: encode_get_batch(FieldMask::ALL_METADATA, &[txid]).into(),
        },
    );
    let results = decode_get_response(&resp.payload).unwrap();
    assert_eq!(results[0].status, 0);
    // preserve_until is in the metadata response
    // offset: tx_version(4)+locktime(4)+fee(8)+size(8)+ext(8)+flags(1)+sh(4)+created(8)+spent(4)+pruned(4)+utxo_count(4)+gen(4)+updated(8)+unmined_since(4)+dah(4) = 77
    let preserve_until = u32::from_le_bytes(results[0].data[77..81].try_into().unwrap());
    assert_eq!(preserve_until, 5000);

    server.shutdown();
}

// ---------------------------------------------------------------------------
// Batch tests
// ---------------------------------------------------------------------------

#[test]
fn batch_spend_1024_items() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .unwrap();

    let txid = test_txid(5);
    let resp = create_records(&mut stream, &[make_create_item(txid, 1024, 5)], 50);
    assert_eq!(resp.status, STATUS_OK);

    // Spend all 1024 in one batch
    let items: Vec<WireSpendItem> = (0..1024u32)
        .map(|v| {
            let mut sd = [0u8; 36];
            sd[0..4].copy_from_slice(&v.to_le_bytes());
            WireSpendItem {
                txid,
                vout: v,
                utxo_hash: test_utxo_hash(5, v),
                spending_data: sd,
            }
        })
        .collect();

    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 51,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload: encode_spend_batch(
                &SpendBatchParams {
                    ignore_conflicting: false,
                    ignore_locked: false,
                    current_block_height: 2000,
                    block_height_retention: 288,
                },
                &items,
            ).into(),

        },
    );
    assert_eq!(resp.status, STATUS_OK);

    // Verify a few are spent
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 52,
            op_code: OP_GET_SPEND_BATCH,
            flags: 0,
            payload: encode_get_spend_batch(&[
                WireGetSpendItem {
                    txid,
                    vout: 0,
                    utxo_hash: test_utxo_hash(5, 0),
                },
                WireGetSpendItem {
                    txid,
                    vout: 512,
                    utxo_hash: test_utxo_hash(5, 512),
                },
                WireGetSpendItem {
                    txid,
                    vout: 1023,
                    utxo_hash: test_utxo_hash(5, 1023),
                },
            ]).into(),

        },
    );
    let results = decode_get_spend_response(&resp.payload).unwrap();
    assert_eq!(results.len(), 3);
    for r in &results {
        assert_eq!(r.slot_status, 0x01);
    }

    server.shutdown();
}

#[test]
fn batch_spend_100_same_txid() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    let txid = test_txid(900);
    let resp = create_records(&mut stream, &[make_create_item(txid, 100, 900)], 900);
    assert_eq!(resp.status, STATUS_OK);

    // All 100 spends on the same txid (single lock hold)
    let items: Vec<WireSpendItem> = (0..100u32)
        .map(|v| {
            let mut sd = [0u8; 36];
            sd[0] = v as u8;
            WireSpendItem {
                txid,
                vout: v,
                utxo_hash: test_utxo_hash(900, v),
                spending_data: sd,
            }
        })
        .collect();

    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 901,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload: encode_spend_batch(
                &SpendBatchParams {
                    ignore_conflicting: false,
                    ignore_locked: false,
                    current_block_height: 1000,
                    block_height_retention: 288,
                },
                &items,
            ).into(),

        },
    );
    assert_eq!(resp.status, STATUS_OK);

    server.shutdown();
}

#[test]
fn batch_exceeding_max_batch_size_rejected() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    // max_batch_size is 8192 — try sending 8193 items
    let txids: Vec<[u8; 32]> = (0..8193u16)
        .map(|i| {
            let mut t = [0u8; 32];
            t[0..2].copy_from_slice(&i.to_le_bytes());
            t
        })
        .collect();
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1000,
            op_code: OP_DELETE_BATCH,
            flags: 0,
            payload: encode_txid_batch(&txids, &[]).into(),
        },
    );
    assert_eq!(resp.status, STATUS_ERROR);

    server.shutdown();
}

// ---------------------------------------------------------------------------
// Concurrent connections
// ---------------------------------------------------------------------------

#[test]
fn multiple_concurrent_connections() {
    let (server, port) = start_test_server();

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    // Create 5 txs with 10 UTXOs each
    for i in 0..5u32 {
        let item = make_create_item(test_txid(100 + i), 10, 100 + i);
        create_records(&mut stream, &[item], 60 + i as u64);
    }
    drop(stream);

    // 5 concurrent clients, each spending from a different tx
    let handles: Vec<_> = (0..5u32)
        .map(|i| {
            std::thread::spawn(move || {
                let mut s = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
                s.set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();

                let txid = test_txid(100 + i);
                for v in 0..10u32 {
                    let mut sd = [0u8; 36];
                    sd[0] = v as u8;
                    let resp = send_request(
                        &mut s,
                        &RequestFrame {
                            request_id: 1000 + v as u64,
                            op_code: OP_SPEND_BATCH,
                            flags: 0,
                            payload: encode_spend_batch(
                                &SpendBatchParams {
                                    ignore_conflicting: false,
                                    ignore_locked: false,
                                    current_block_height: 2000,
                                    block_height_retention: 288,
                                },
                                &[WireSpendItem {
                                    txid,
                                    vout: v,
                                    utxo_hash: test_utxo_hash(100 + i, v),
                                    spending_data: sd,
                                }],
                            ).into(),

                        },
                    );
                    assert_eq!(resp.status, STATUS_OK, "spend failed: client {i} vout {v}");
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    server.shutdown();
}

// ---------------------------------------------------------------------------
// Error handling tests
// ---------------------------------------------------------------------------

#[test]
fn invalid_opcode_returns_error() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1,
            op_code: 999,
            flags: 0,
            payload: vec![].into(),
        },
    );
    assert_eq!(resp.status, STATUS_ERROR);

    // Should still be connected — send a ping to verify
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 2,
            op_code: OP_PING,
            flags: 0,
            payload: vec![].into(),
        },
    );
    assert_eq!(resp.status, STATUS_OK);

    server.shutdown();
}

#[test]
fn malformed_payload_returns_error() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    // Send a SpendBatch with truncated payload
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload: vec![0x01, 0x02].into(), // Way too short
        },

    );
    assert_eq!(resp.status, STATUS_ERROR);

    server.shutdown();
}

#[test]
fn request_for_nonexistent_tx_partial_error() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    let txid = test_txid(1100);
    let resp = create_records(&mut stream, &[make_create_item(txid, 2, 1100)], 1100);
    assert_eq!(resp.status, STATUS_OK);

    // Spend batch: item 0 exists, item 1 doesn't
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1101,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload: encode_spend_batch(
                &SpendBatchParams {
                    ignore_conflicting: false,
                    ignore_locked: false,
                    current_block_height: 1000,
                    block_height_retention: 288,
                },
                &[
                    WireSpendItem {
                        txid,
                        vout: 0,
                        utxo_hash: test_utxo_hash(1100, 0),
                        spending_data: [0xAA; 36],
                    },
                    WireSpendItem {
                        txid: test_txid(9999), // doesn't exist
                        vout: 0,
                        utxo_hash: [0; 32],
                        spending_data: [0xBB; 36],
                    },
                ],
            ).into(),

        },
    );
    assert_eq!(resp.status, STATUS_PARTIAL_ERROR);

    let errors = decode_sparse_errors(&resp.payload).unwrap();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].item_index, 1);
    assert_eq!(errors[0].error_code, ERR_TX_NOT_FOUND);

    server.shutdown();
}

#[test]
fn oversized_frame_rejected() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    // Send a frame with total_length > 16 MiB
    let too_big: u32 = MAX_FRAME_SIZE + 1;
    stream.write_all(&too_big.to_le_bytes()).unwrap();

    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .expect("server must send an explicit error frame");
    let total_length = u32::from_le_bytes(len_buf) as usize;
    let mut body = vec![0u8; total_length];
    stream
        .read_exact(&mut body)
        .expect("server must send the complete error frame body");

    let mut full = Vec::with_capacity(4 + total_length);
    full.extend_from_slice(&len_buf);
    full.extend_from_slice(&body);
    let (resp, consumed) = ResponseFrame::decode(&full).unwrap();
    assert_eq!(consumed, full.len());
    assert_eq!(resp.request_id, 0);
    assert_eq!(resp.status, STATUS_ERROR);
    assert_eq!(resp.payload, b"frame too large");

    server.shutdown();
}

#[test]
fn max_connection_rejection_sends_error_frame() {
    let (server, port) = start_test_server_with_max_connections(1);
    let _held = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();

    // Give the accept thread time to count the held connection before
    // opening the over-limit connection.
    std::thread::sleep(std::time::Duration::from_millis(100));

    let mut rejected = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    rejected
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    let mut len_buf = [0u8; 4];
    rejected
        .read_exact(&mut len_buf)
        .expect("over-limit connection must receive an error frame");
    let total_length = u32::from_le_bytes(len_buf) as usize;
    let mut body = vec![0u8; total_length];
    rejected
        .read_exact(&mut body)
        .expect("over-limit connection must receive a complete error body");

    let mut full = Vec::with_capacity(4 + total_length);
    full.extend_from_slice(&len_buf);
    full.extend_from_slice(&body);
    let (resp, consumed) = ResponseFrame::decode(&full).unwrap();
    assert_eq!(consumed, full.len());
    assert_eq!(resp.request_id, 0);
    assert_eq!(resp.status, STATUS_ERROR);
    let (code, msg) = decode_error_payload(&resp.payload).unwrap();
    assert_eq!(code, ERR_INTERNAL);
    assert!(msg.contains("max connections"));

    server.shutdown();
}

#[test]
fn stream_isolation_per_connection() {
    let (server, port) = start_test_server_with_blob_store();
    let mut stream_a = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    let mut stream_b = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream_a
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();
    stream_b
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    let txid = test_txid(1300);
    let resp_a = send_request(
        &mut stream_a,
        &RequestFrame {
            request_id: 1300,
            op_code: OP_STREAM_CHUNK,
            flags: 0,
            payload: encode_stream_chunk(&txid, 0, b"hello").into(),
        },
    );
    assert_eq!(resp_a.status, STATUS_OK);

    // If stream state leaked across connections, B would inherit A's
    // 5-byte offset and this chunk would be accepted. Per-connection
    // isolation means B has no prior bytes and must reject offset 5.
    let resp_b = send_request(
        &mut stream_b,
        &RequestFrame {
            request_id: 1301,
            op_code: OP_STREAM_CHUNK,
            flags: 0,
            payload: encode_stream_chunk(&txid, 5, b"world").into(),
        },
    );
    assert_eq!(resp_b.status, STATUS_ERROR);
    let (code, msg) = decode_error_payload(&resp_b.payload).unwrap();
    assert_eq!(code, ERR_STREAM_OFFSET_MISMATCH);
    assert!(msg.contains("expected offset 0"));

    server.shutdown();
}

#[test]
fn stream_end_without_active_stream_returns_stream_not_found() {
    let (server, port) = start_test_server_with_blob_store();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    let txid = test_txid(1302);
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1302,
            op_code: OP_STREAM_END,
            flags: 0,
            payload: encode_stream_end(&txid, 0).into(),
        },
    );
    assert_eq!(resp.status, STATUS_ERROR);
    let (code, msg) = decode_error_payload(&resp.payload).unwrap();
    assert_eq!(code, ERR_STREAM_NOT_FOUND);
    assert!(msg.contains("no active stream"));

    server.shutdown();
}

#[test]
fn external_blob_create_without_uploaded_blob_returns_blob_not_found() {
    let (server, port) = start_test_server_with_blob_store();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    let mut item = make_create_item(test_txid(1303), 1, 1303);
    item.flags = FLAG_EXTERNAL_BLOB;

    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1303,
            op_code: OP_CREATE_BATCH,
            flags: 0,
            payload: encode_create_batch(&[item]).into(),
        },
    );
    assert_eq!(resp.status, STATUS_PARTIAL_ERROR);
    let errors = decode_sparse_errors(&resp.payload).unwrap();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].item_index, 0);
    assert_eq!(errors[0].error_code, ERR_BLOB_NOT_FOUND);
    assert!(errors[0].error_data.is_empty());

    server.shutdown();
}

#[test]
fn pipelined_requests_5_batches() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    // Create 5 records
    let items: Vec<WireCreateItem> = (0..5u32)
        .map(|i| make_create_item(test_txid(1200 + i), 2, 1200 + i))
        .collect();
    let resp = create_records(&mut stream, &items, 1200);
    assert_eq!(resp.status, STATUS_OK);

    // Send 5 requests without waiting for responses (pipelining)
    // Note: current server is sequential per connection, but this tests that
    // responses are matched correctly by request_id.
    for i in 0..5u32 {
        let frame = RequestFrame {
            request_id: 1300 + i as u64,
            op_code: OP_GET_SPEND_BATCH,
            flags: 0,
            payload: encode_get_spend_batch(&[WireGetSpendItem {
                txid: test_txid(1200 + i),
                vout: 0,
                utxo_hash: test_utxo_hash(1200 + i, 0),
            }]).into(),

        };
        let bytes = frame.encode();
        stream.write_all(&bytes).unwrap();
    }

    // Read 5 responses
    let mut responses = Vec::new();
    for _ in 0..5 {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).unwrap();
        let total_length = u32::from_le_bytes(len_buf) as usize;
        let mut body = vec![0u8; total_length];
        stream.read_exact(&mut body).unwrap();
        let mut full = Vec::with_capacity(4 + total_length);
        full.extend_from_slice(&len_buf);
        full.extend_from_slice(&body);
        let (response, _) = ResponseFrame::decode(&full).unwrap();
        responses.push(response);
    }

    assert_eq!(responses.len(), 5);
    // Verify all responses have matching request IDs (in order for sequential server)
    for (i, resp) in responses.iter().enumerate() {
        assert_eq!(resp.request_id, 1300 + i as u64);
        assert_eq!(resp.status, STATUS_OK);
    }

    server.shutdown();
}

#[test]
fn client_disconnect_mid_session_server_survives() {
    let (server, port) = start_test_server();

    // Connect and send a request
    {
        let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let resp = send_request(
            &mut stream,
            &RequestFrame {
                request_id: 1,
                op_code: OP_PING,
                flags: 0,
                payload: vec![].into(),
            },
        );
        assert_eq!(resp.status, STATUS_OK);
        // Drop stream — client disconnects
    }

    // Server should survive and accept new connections
    std::thread::sleep(std::time::Duration::from_millis(100));

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 2,
            op_code: OP_PING,
            flags: 0,
            payload: vec![].into(),
        },
    );
    assert_eq!(resp.status, STATUS_OK);

    server.shutdown();
}

#[test]
fn all_operations_from_phases_3_through_6_over_tcp() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .unwrap();

    let txid = test_txid(1500);
    let resp = create_records(&mut stream, &[make_create_item(txid, 4, 1500)], 1500);
    assert_eq!(resp.status, STATUS_OK);

    // Spend (Phase 3)
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1501,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload: encode_spend_batch(
                &SpendBatchParams {
                    ignore_conflicting: false,
                    ignore_locked: false,
                    current_block_height: 1000,
                    block_height_retention: 288,
                },
                &[WireSpendItem {
                    txid,
                    vout: 0,
                    utxo_hash: test_utxo_hash(1500, 0),
                    spending_data: [0xAA; 36],
                }],
            ).into(),

        },
    );
    assert_eq!(resp.status, STATUS_OK);

    // SetMined (Phase 4)
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1502,
            op_code: OP_SET_MINED_BATCH,
            flags: 0,
            payload: encode_set_mined_batch(
                &SetMinedBatchParams {
                    block_id: 50,
                    block_height: 2000,
                    subtree_idx: 0,
                    on_longest_chain: true,
                    unset_mined: false,
                    current_block_height: 2000,
                    block_height_retention: 288,
                },
                &[txid],
            ).into(),

        },
    );
    assert_eq!(resp.status, STATUS_OK);

    // Freeze (Phase 6)
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1503,
            op_code: OP_FREEZE_BATCH,
            flags: 0,
            payload: encode_slot_item_batch(&[WireSlotItem {
                txid,
                vout: 1,
                utxo_hash: test_utxo_hash(1500, 1),
            }]).into(),

        },
    );
    assert_eq!(resp.status, STATUS_OK);

    // Unfreeze (Phase 6)
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1504,
            op_code: OP_UNFREEZE_BATCH,
            flags: 0,
            payload: encode_slot_item_batch(&[WireSlotItem {
                txid,
                vout: 1,
                utxo_hash: test_utxo_hash(1500, 1),
            }]).into(),

        },
    );
    assert_eq!(resp.status, STATUS_OK);

    // SetConflicting (Phase 6)
    let mut shared = Vec::new();
    shared.push(1);
    shared.extend_from_slice(&2000u32.to_le_bytes());
    shared.extend_from_slice(&288u32.to_le_bytes());
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1505,
            op_code: OP_SET_CONFLICTING_BATCH,
            flags: 0,
            payload: encode_txid_batch(&[txid], &shared).into(),
        },
    );
    assert_eq!(resp.status, STATUS_OK);

    // SetLocked (Phase 6)
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1506,
            op_code: OP_SET_LOCKED_BATCH,
            flags: 0,
            payload: encode_txid_batch(&[txid], &[1u8]).into(),
        },
    );
    assert_eq!(resp.status, STATUS_OK);

    // PreserveUntil (Phase 6)
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1507,
            op_code: OP_PRESERVE_UNTIL_BATCH,
            flags: 0,
            payload: encode_txid_batch(&[txid], &3000u32.to_le_bytes()).into(),
        },
    );
    assert_eq!(resp.status, STATUS_OK);

    // MarkLongestChain (Phase 6)
    let mut shared2 = Vec::new();
    shared2.push(0); // off longest chain
    shared2.extend_from_slice(&2001u32.to_le_bytes());
    shared2.extend_from_slice(&288u32.to_le_bytes());
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1508,
            op_code: OP_MARK_LONGEST_CHAIN_BATCH,
            flags: 0,
            payload: encode_txid_batch(&[txid], &shared2).into(),
        },
    );
    assert_eq!(resp.status, STATUS_OK);

    // GetSpend (read)
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1509,
            op_code: OP_GET_SPEND_BATCH,
            flags: 0,
            payload: encode_get_spend_batch(&[WireGetSpendItem {
                txid,
                vout: 0,
                utxo_hash: test_utxo_hash(1500, 0),
            }]).into(),

        },
    );
    assert_eq!(resp.status, STATUS_OK);
    let results = decode_get_spend_response(&resp.payload).unwrap();
    assert_eq!(results[0].slot_status, 0x01); // Spent

    // Delete (Phase 6)
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 1510,
            op_code: OP_DELETE_BATCH,
            flags: 0,
            payload: encode_txid_batch(&[txid], &[]).into(),
        },
    );
    assert_eq!(resp.status, STATUS_OK);

    server.shutdown();
}
