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

/// Start a server on a random port and return (server_handle, port).
fn start_test_server() -> (Arc<Server>, u16) {
    let dev: Arc<dyn BlockDevice> =
        Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
    let alloc = SlotAllocator::new(dev.clone());
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn ping_pong() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();

    let resp = send_request(&mut stream, &RequestFrame {
        request_id: 1,
        op_code: OP_PING,
        flags: 0,
        payload: vec![],
    });

    assert_eq!(resp.request_id, 1);
    assert_eq!(resp.status, STATUS_OK);

    server.shutdown();
}

#[test]
fn create_then_get_spend() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();

    let txid = test_txid(1);
    let _hash0 = test_utxo_hash(1, 0);

    // Create a record: [count:4][txid:32][utxo_count:4][hashes:32×N]
    let mut create_payload = Vec::new();
    create_payload.extend_from_slice(&1u32.to_le_bytes()); // count=1
    create_payload.extend_from_slice(&txid);
    create_payload.extend_from_slice(&3u32.to_le_bytes()); // 3 UTXOs
    for v in 0..3u32 {
        create_payload.extend_from_slice(&test_utxo_hash(1, v));
    }

    let resp = send_request(&mut stream, &RequestFrame {
        request_id: 10,
        op_code: OP_CREATE_BATCH,
        flags: 0,
        payload: create_payload,
    });
    assert_eq!(resp.status, STATUS_OK, "create failed: {:?}", resp.payload);

    // GetSpend to verify the UTXO exists and is unspent
    let get_payload = encode_get_spend_batch(&[WireGetSpendItem { txid, vout: 0 }]);
    let resp = send_request(&mut stream, &RequestFrame {
        request_id: 11,
        op_code: OP_GET_SPEND_BATCH,
        flags: 0,
        payload: get_payload,
    });
    assert_eq!(resp.status, STATUS_OK);

    let results = decode_get_spend_response(&resp.payload).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].status, 0); // OK
    assert_eq!(results[0].slot_status, 0x00); // Unspent

    server.shutdown();
}

#[test]
fn create_spend_get_spend() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();

    let txid = test_txid(2);
    let hash0 = test_utxo_hash(2, 0);

    // Create
    let mut create_payload = Vec::new();
    create_payload.extend_from_slice(&1u32.to_le_bytes());
    create_payload.extend_from_slice(&txid);
    create_payload.extend_from_slice(&5u32.to_le_bytes());
    for v in 0..5u32 {
        create_payload.extend_from_slice(&test_utxo_hash(2, v));
    }
    let resp = send_request(&mut stream, &RequestFrame {
        request_id: 20, op_code: OP_CREATE_BATCH, flags: 0, payload: create_payload,
    });
    assert_eq!(resp.status, STATUS_OK);

    // Spend UTXO 0
    let mut sd = [0u8; 36]; sd[0] = 0xAB;
    let spend_payload = encode_spend_batch(
        &SpendBatchParams {
            ignore_conflicting: false, ignore_locked: false,
            current_block_height: 1000, block_height_retention: 288,
        },
        &[WireSpendItem {
            txid, vout: 0, utxo_hash: hash0, spending_data: sd,
        }],
    );
    let resp = send_request(&mut stream, &RequestFrame {
        request_id: 21, op_code: OP_SPEND_BATCH, flags: 0, payload: spend_payload,
    });
    assert_eq!(resp.status, STATUS_OK);

    // GetSpend — should show spent
    let get_payload = encode_get_spend_batch(&[WireGetSpendItem { txid, vout: 0 }]);
    let resp = send_request(&mut stream, &RequestFrame {
        request_id: 22, op_code: OP_GET_SPEND_BATCH, flags: 0, payload: get_payload,
    });
    let results = decode_get_spend_response(&resp.payload).unwrap();
    assert_eq!(results[0].slot_status, 0x01); // Spent
    assert_eq!(results[0].spending_data[0], 0xAB);

    server.shutdown();
}

#[test]
fn create_set_mined_delete() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();

    let txid = test_txid(3);

    // Create
    let mut create_payload = Vec::new();
    create_payload.extend_from_slice(&1u32.to_le_bytes());
    create_payload.extend_from_slice(&txid);
    create_payload.extend_from_slice(&2u32.to_le_bytes());
    for v in 0..2u32 {
        create_payload.extend_from_slice(&test_utxo_hash(3, v));
    }
    send_request(&mut stream, &RequestFrame {
        request_id: 30, op_code: OP_CREATE_BATCH, flags: 0, payload: create_payload,
    });

    // SetMined
    let mined_payload = encode_set_mined_batch(
        &SetMinedBatchParams {
            block_id: 42, block_height: 1000, subtree_idx: 0,
            on_longest_chain: true, unset_mined: false,
            current_block_height: 1000, block_height_retention: 288,
        },
        &[txid],
    );
    let resp = send_request(&mut stream, &RequestFrame {
        request_id: 31, op_code: OP_SET_MINED_BATCH, flags: 0, payload: mined_payload,
    });
    assert_eq!(resp.status, STATUS_OK);

    // Delete
    let delete_payload = encode_txid_batch(&[txid], &[]);
    let resp = send_request(&mut stream, &RequestFrame {
        request_id: 32, op_code: OP_DELETE_BATCH, flags: 0, payload: delete_payload,
    });
    assert_eq!(resp.status, STATUS_OK);

    // GetSpend after delete — should be not found
    let get_payload = encode_get_spend_batch(&[WireGetSpendItem { txid, vout: 0 }]);
    let resp = send_request(&mut stream, &RequestFrame {
        request_id: 33, op_code: OP_GET_SPEND_BATCH, flags: 0, payload: get_payload,
    });
    let results = decode_get_spend_response(&resp.payload).unwrap();
    assert_eq!(results[0].status, 1); // Error
    assert_eq!(results[0].error_code, ERR_TX_NOT_FOUND);

    server.shutdown();
}

#[test]
fn freeze_unfreeze_over_tcp() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();

    let txid = test_txid(4);
    let hash0 = test_utxo_hash(4, 0);

    // Create
    let mut cp = Vec::new();
    cp.extend_from_slice(&1u32.to_le_bytes());
    cp.extend_from_slice(&txid);
    cp.extend_from_slice(&3u32.to_le_bytes());
    for v in 0..3u32 { cp.extend_from_slice(&test_utxo_hash(4, v)); }
    send_request(&mut stream, &RequestFrame {
        request_id: 40, op_code: OP_CREATE_BATCH, flags: 0, payload: cp,
    });

    // Freeze
    let freeze_payload = encode_slot_item_batch(&[WireSlotItem {
        txid, vout: 0, utxo_hash: hash0,
    }]);
    let resp = send_request(&mut stream, &RequestFrame {
        request_id: 41, op_code: OP_FREEZE_BATCH, flags: 0, payload: freeze_payload,
    });
    assert_eq!(resp.status, STATUS_OK);

    // Verify frozen
    let get_payload = encode_get_spend_batch(&[WireGetSpendItem { txid, vout: 0 }]);
    let resp = send_request(&mut stream, &RequestFrame {
        request_id: 42, op_code: OP_GET_SPEND_BATCH, flags: 0, payload: get_payload,
    });
    let results = decode_get_spend_response(&resp.payload).unwrap();
    assert_eq!(results[0].slot_status, 0xFF); // Frozen

    // Unfreeze
    let unfreeze_payload = encode_slot_item_batch(&[WireSlotItem {
        txid, vout: 0, utxo_hash: hash0,
    }]);
    let resp = send_request(&mut stream, &RequestFrame {
        request_id: 43, op_code: OP_UNFREEZE_BATCH, flags: 0, payload: unfreeze_payload,
    });
    assert_eq!(resp.status, STATUS_OK);

    // Verify unspent again
    let resp = send_request(&mut stream, &RequestFrame {
        request_id: 44, op_code: OP_GET_SPEND_BATCH, flags: 0,
        payload: encode_get_spend_batch(&[WireGetSpendItem { txid, vout: 0 }]),
    });
    let results = decode_get_spend_response(&resp.payload).unwrap();
    assert_eq!(results[0].slot_status, 0x00); // Unspent

    server.shutdown();
}

#[test]
fn batch_spend_1024_items() {
    let (server, port) = start_test_server();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream.set_read_timeout(Some(std::time::Duration::from_secs(10))).unwrap();

    // Create a tx with 1024 UTXOs
    let txid = test_txid(5);
    let mut cp = Vec::new();
    cp.extend_from_slice(&1u32.to_le_bytes());
    cp.extend_from_slice(&txid);
    cp.extend_from_slice(&1024u32.to_le_bytes());
    for v in 0..1024u32 { cp.extend_from_slice(&test_utxo_hash(5, v)); }
    let resp = send_request(&mut stream, &RequestFrame {
        request_id: 50, op_code: OP_CREATE_BATCH, flags: 0, payload: cp,
    });
    assert_eq!(resp.status, STATUS_OK);

    // Spend all 1024 in one batch
    let items: Vec<WireSpendItem> = (0..1024u32).map(|v| {
        let mut sd = [0u8; 36];
        sd[0..4].copy_from_slice(&v.to_le_bytes());
        WireSpendItem {
            txid, vout: v, utxo_hash: test_utxo_hash(5, v), spending_data: sd,
        }
    }).collect();

    let spend_payload = encode_spend_batch(
        &SpendBatchParams {
            ignore_conflicting: false, ignore_locked: false,
            current_block_height: 2000, block_height_retention: 288,
        },
        &items,
    );
    let resp = send_request(&mut stream, &RequestFrame {
        request_id: 51, op_code: OP_SPEND_BATCH, flags: 0, payload: spend_payload,
    });
    assert_eq!(resp.status, STATUS_OK);

    // Verify a few are spent
    let get_payload = encode_get_spend_batch(&[
        WireGetSpendItem { txid, vout: 0 },
        WireGetSpendItem { txid, vout: 512 },
        WireGetSpendItem { txid, vout: 1023 },
    ]);
    let resp = send_request(&mut stream, &RequestFrame {
        request_id: 52, op_code: OP_GET_SPEND_BATCH, flags: 0, payload: get_payload,
    });
    let results = decode_get_spend_response(&resp.payload).unwrap();
    assert_eq!(results.len(), 3);
    for r in &results {
        assert_eq!(r.slot_status, 0x01); // All spent
    }

    server.shutdown();
}

#[test]
fn multiple_concurrent_connections() {
    let (server, port) = start_test_server();

    // Create a shared tx first
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();

    // Create 5 txs with 10 UTXOs each
    for i in 0..5u32 {
        let txid = test_txid(100 + i);
        let mut cp = Vec::new();
        cp.extend_from_slice(&1u32.to_le_bytes());
        cp.extend_from_slice(&txid);
        cp.extend_from_slice(&10u32.to_le_bytes());
        for v in 0..10u32 { cp.extend_from_slice(&test_utxo_hash(100 + i, v)); }
        send_request(&mut stream, &RequestFrame {
            request_id: 60 + i as u64, op_code: OP_CREATE_BATCH, flags: 0, payload: cp,
        });
    }
    drop(stream);

    // 5 concurrent clients, each spending from a different tx
    let handles: Vec<_> = (0..5u32).map(|i| {
        std::thread::spawn(move || {
            let mut s = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
            s.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();

            let txid = test_txid(100 + i);
            for v in 0..10u32 {
                let mut sd = [0u8; 36]; sd[0] = v as u8;
                let payload = encode_spend_batch(
                    &SpendBatchParams {
                        ignore_conflicting: false, ignore_locked: false,
                        current_block_height: 2000, block_height_retention: 288,
                    },
                    &[WireSpendItem {
                        txid, vout: v, utxo_hash: test_utxo_hash(100 + i, v),
                        spending_data: sd,
                    }],
                );
                let resp = send_request(&mut s, &RequestFrame {
                    request_id: 1000 + v as u64, op_code: OP_SPEND_BATCH, flags: 0,
                    payload,
                });
                assert_eq!(resp.status, STATUS_OK, "spend failed: client {i} vout {v}");
            }
        })
    }).collect();

    for h in handles {
        h.join().unwrap();
    }

    server.shutdown();
}
