//! TCP replication integration tests.
//!
//! Each test starts two in-process servers (master and replica) with
//! MemoryDevice backends, replicates operations from master to replica
//! via TCP, and verifies the replica state matches.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use teraslab::allocator::SlotAllocator;
use teraslab::config::ServerConfig;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, TxKey, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::create::CreateRequest;
use teraslab::ops::engine::Engine;
use teraslab::protocol::codec::*;
use teraslab::protocol::frame::*;
use teraslab::protocol::opcodes::*;
use teraslab::record::{UTXO_FROZEN, UTXO_SPENT, UTXO_UNSPENT};
use teraslab::replication::manager::*;
use teraslab::replication::protocol::*;
use teraslab::replication::tcp_transport::TcpReplicaTransport;
use teraslab::server::Server;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build an Engine from an in-memory device.
fn make_engine() -> Arc<Engine> {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
    let alloc = SlotAllocator::new(dev.clone()).unwrap();
    let index = Index::new(10_000).unwrap();
    Arc::new(Engine::new(
        dev,
        index,
        alloc,
        StripedLocks::new(1024),
        DahIndex::new(),
        UnminedIndex::new(),
    ))
}

/// Start a server on a random port, return (server, engine, port).
fn start_test_server() -> (Arc<Server>, Arc<Engine>, u16) {
    let engine = make_engine();

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let config = ServerConfig {
        listen_addr: format!("127.0.0.1:{port}"),
        max_connections: 10,
        max_batch_size: 8192,
        ..Default::default()
    };

    let server = Arc::new(Server::new(engine.clone(), config));
    let server_clone = server.clone();

    std::thread::spawn(move || {
        server_clone.run().unwrap();
    });

    // Wait for the server to start listening
    std::thread::sleep(Duration::from_millis(100));

    (server, engine, port)
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

fn key_from_txid(txid: [u8; 32]) -> TxKey {
    TxKey { txid }
}

/// Create a record directly on an engine (bypassing the server).
fn create_record_on_engine(engine: &Engine, txid: [u8; 32], utxo_count: u32) {
    let hashes: Vec<[u8; 32]> = (0..utxo_count)
        .map(|v| test_utxo_hash(u32::from_le_bytes(txid[0..4].try_into().unwrap()), v))
        .collect();
    let req = CreateRequest {
        tx_id: txid,
        tx_version: 1,
        locktime: 0,
        fee: 0,
        size_in_bytes: 0,
        extended_size: 0,
        is_coinbase: false,
        spending_height: 0,
        utxo_hashes: &hashes,
        inputs: None,
        outputs: None,
        inpoints: None,
        is_external: false,
        created_at: 0,
        block_height: 0,
        mined_block_infos: &[],
        frozen: false,
        conflicting: false,
        locked: false,
        external_ref: None,
        parent_txids: &[],
    };
    engine.create(&req).unwrap();
}

/// Send a wire-protocol request and receive a response over TCP.
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

/// Send a ReplicaBatch via TCP to a server's replication endpoint
/// (using OP_REPLICA_BATCH frames) and return the ReplicaAck.
fn send_replica_batch_tcp(port: u16, batch: &ReplicaBatch) -> ReplicaAck {
    let addr = format!("127.0.0.1:{port}");
    let mut transport = TcpReplicaTransport::connect(&addr, Duration::from_secs(5)).unwrap();
    transport.send_batch(batch).unwrap();
    transport.recv_ack(Duration::from_secs(5)).unwrap()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn tcp_replicate_spend() {
    // Start master and replica servers
    let (master_server, master_engine, master_port) = start_test_server();
    let (replica_server, replica_engine, replica_port) = start_test_server();

    let txid = test_txid(500);
    let key = key_from_txid(txid);
    let hash0 = test_utxo_hash(500, 0);

    // Create the record on both master and replica
    create_record_on_engine(&master_engine, txid, 3);
    create_record_on_engine(&replica_engine, txid, 3);

    // Spend on master
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
    let mut stream = TcpStream::connect(format!("127.0.0.1:{master_port}")).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let resp = send_request(
        &mut stream,
        &RequestFrame {
            request_id: 10,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload: spend_payload,
        },
    );
    assert_eq!(resp.status, STATUS_OK);

    // Replicate the spend to the replica
    let batch = ReplicaBatch {
        first_sequence: 1,
        ops: vec![ReplicaOp::Spend {
            tx_key: key,
            offset: 0,
            spending_data: sd,
            current_block_height: 1000,
            block_height_retention: 288,
            master_generation: 0,
        }],
        trace_ctx: None,
        source_node_id: None,
        cluster_key: 0,
    };
    let ack = send_replica_batch_tcp(replica_port, &batch);
    assert_eq!(
        ack,
        ReplicaAck::Ok {
            through_sequence: 1
        }
    );

    // Verify replica has the spent slot
    let slot = replica_engine.read_slot(&key, 0).unwrap();
    assert_eq!(slot.status, UTXO_SPENT);
    assert_eq!(slot.spending_data[0], 0xAB);

    master_server.shutdown();
    replica_server.shutdown();
}

#[test]
fn tcp_replicate_spend_uses_replicated_dah_context() {
    let (replica_server, replica_engine, replica_port) = start_test_server();

    let txid = test_txid(1500);
    let key = key_from_txid(txid);
    create_record_on_engine(&replica_engine, txid, 1);

    let mut sd = [0u8; 36];
    sd[0] = 0xD0;
    let batch = ReplicaBatch {
        first_sequence: 1,
        ops: vec![
            ReplicaOp::SetMined {
                tx_key: key,
                block_id: 1500,
                block_height: 800_000,
                subtree_idx: 0,
                on_longest_chain: true,
                current_block_height: 800_010,
                block_height_retention: 7,
                master_generation: 1,
            },
            ReplicaOp::Spend {
                tx_key: key,
                offset: 0,
                spending_data: sd,
                current_block_height: 800_050,
                block_height_retention: 13,
                master_generation: 2,
            },
        ],
        trace_ctx: None,
        source_node_id: None,
        cluster_key: 0,
    };

    let ack = send_replica_batch_tcp(replica_port, &batch);
    assert_eq!(
        ack,
        ReplicaAck::Ok {
            through_sequence: 2
        }
    );

    let meta = replica_engine.read_metadata(&key).unwrap();
    assert_eq!(
        { meta.delete_at_height },
        800_063,
        "replica must evaluate DAH with the context carried on the Spend op",
    );

    replica_server.shutdown();
}

#[test]
fn tcp_replicate_create_and_spend_lifecycle() {
    let (master_server, master_engine, _master_port) = start_test_server();
    let (_replica_server, replica_engine, replica_port) = start_test_server();

    let txid = test_txid(501);
    let key = key_from_txid(txid);

    // Create on master directly
    create_record_on_engine(&master_engine, txid, 5);

    // Replicate the create to the replica
    let hashes: Vec<[u8; 32]> = (0..5u32).map(|v| test_utxo_hash(501, v)).collect();
    let create_batch = ReplicaBatch {
        first_sequence: 1,
        ops: vec![ReplicaOp::Create {
            tx_key: key,
            metadata_bytes: vec![],
            utxo_hashes: hashes,
            cold_data: None,
            is_external: false,
        }],
        trace_ctx: None,
        source_node_id: None,
        cluster_key: 0,
    };
    let ack = send_replica_batch_tcp(replica_port, &create_batch);
    assert_eq!(
        ack,
        ReplicaAck::Ok {
            through_sequence: 1
        }
    );

    // Verify replica has the record
    let slot = replica_engine.read_slot(&key, 0).unwrap();
    assert_eq!(slot.status, UTXO_UNSPENT);

    // Now spend on master
    let sd = [0xCC; 36];
    let hash0 = test_utxo_hash(501, 0);
    let spend_req = teraslab::ops::spend::SpendRequest {
        tx_key: key,
        offset: 0,
        utxo_hash: hash0,
        spending_data: sd,
        ignore_conflicting: false,
        ignore_locked: false,
        current_block_height: 1000,
        block_height_retention: 288,
    };
    master_engine.spend(&spend_req).unwrap();

    // Replicate the spend
    let spend_batch = ReplicaBatch {
        first_sequence: 2,
        ops: vec![ReplicaOp::Spend {
            tx_key: key,
            offset: 0,
            spending_data: sd,
            current_block_height: 1000,
            block_height_retention: 288,
            master_generation: 0,
        }],
        trace_ctx: None,
        source_node_id: None,
        cluster_key: 0,
    };
    let ack = send_replica_batch_tcp(replica_port, &spend_batch);
    assert_eq!(
        ack,
        ReplicaAck::Ok {
            through_sequence: 2
        }
    );

    // Verify replica has the spent slot
    let slot = replica_engine.read_slot(&key, 0).unwrap();
    assert_eq!(slot.status, UTXO_SPENT);
    assert_eq!(slot.spending_data, sd);

    master_server.shutdown();
    _replica_server.shutdown();
}

#[test]
fn tcp_replicate_batch_50_ops() {
    let (_master_server, master_engine, _master_port) = start_test_server();
    let (_replica_server, replica_engine, replica_port) = start_test_server();

    // Create 50 records on both sides
    let mut ops = Vec::with_capacity(50);
    for i in 0..50u32 {
        let txid = test_txid(600 + i);
        create_record_on_engine(&master_engine, txid, 3);
        create_record_on_engine(&replica_engine, txid, 3);

        let hash0 = test_utxo_hash(600 + i, 0);
        let mut sd = [0u8; 36];
        sd[0..4].copy_from_slice(&i.to_le_bytes());

        // Spend on master
        let key = key_from_txid(txid);
        let spend_req = teraslab::ops::spend::SpendRequest {
            tx_key: key,
            offset: 0,
            utxo_hash: hash0,
            spending_data: sd,
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        master_engine.spend(&spend_req).unwrap();

        ops.push(ReplicaOp::Spend {
            tx_key: key,
            offset: 0,
            spending_data: sd,
            current_block_height: 1000,
            block_height_retention: 288,
            master_generation: 0,
        });
    }

    // Replicate all 50 in a single batch
    let batch = ReplicaBatch {
        first_sequence: 1,
        ops,
        trace_ctx: None,
        source_node_id: None,
        cluster_key: 0,
    };
    let ack = send_replica_batch_tcp(replica_port, &batch);
    assert_eq!(
        ack,
        ReplicaAck::Ok {
            through_sequence: 50
        }
    );

    // Verify all 50 are spent on the replica
    for i in 0..50u32 {
        let txid = test_txid(600 + i);
        let key = key_from_txid(txid);
        let slot = replica_engine.read_slot(&key, 0).unwrap();
        assert_eq!(
            slot.status, UTXO_SPENT,
            "slot {i} should be spent on replica"
        );
        assert_eq!(
            u32::from_le_bytes(slot.spending_data[0..4].try_into().unwrap()),
            i,
            "spending_data mismatch for slot {i}"
        );
    }

    _master_server.shutdown();
    _replica_server.shutdown();
}

#[test]
fn tcp_replicate_mixed_ops() {
    let (_master_server, master_engine, _master_port) = start_test_server();
    let (_replica_server, replica_engine, replica_port) = start_test_server();

    let txid_spend = test_txid(700);
    let txid_freeze = test_txid(701);
    let txid_mined = test_txid(702);

    // Create records on both sides
    for txid in [txid_spend, txid_freeze, txid_mined] {
        create_record_on_engine(&master_engine, txid, 3);
        create_record_on_engine(&replica_engine, txid, 3);
    }

    // Spend on master
    let hash0 = test_utxo_hash(700, 0);
    let sd = [0xDD; 36];
    master_engine
        .spend(&teraslab::ops::spend::SpendRequest {
            tx_key: key_from_txid(txid_spend),
            offset: 0,
            utxo_hash: hash0,
            spending_data: sd,
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        })
        .unwrap();

    // Freeze on master
    let hash_freeze = test_utxo_hash(701, 1);
    master_engine
        .freeze(&teraslab::ops::remaining::FreezeRequest {
            tx_key: key_from_txid(txid_freeze),
            offset: 1,
            utxo_hash: hash_freeze,
        })
        .unwrap();

    // SetMined on master
    master_engine
        .set_mined(&teraslab::ops::set_mined::SetMinedRequest {
            tx_key: key_from_txid(txid_mined),
            block_id: 42,
            block_height: 1000,
            subtree_idx: 0,
            current_block_height: 1000,
            block_height_retention: 288,
            on_longest_chain: true,
            unset_mined: false,
        })
        .unwrap();

    // Replicate all three ops in one batch
    let batch = ReplicaBatch {
        first_sequence: 1,
        ops: vec![
            ReplicaOp::Spend {
                tx_key: key_from_txid(txid_spend),
                offset: 0,
                spending_data: sd,
                current_block_height: 1000,
                block_height_retention: 288,
                master_generation: 0,
            },
            ReplicaOp::Freeze {
                tx_key: key_from_txid(txid_freeze),
                offset: 1,
                master_generation: 0,
            },
            ReplicaOp::SetMined {
                tx_key: key_from_txid(txid_mined),
                block_id: 42,
                block_height: 1000,
                subtree_idx: 0,
                on_longest_chain: true,
                current_block_height: 1000,
                block_height_retention: 288,
                master_generation: 0,
            },
        ],
        trace_ctx: None,
        source_node_id: None,
        cluster_key: 0,
    };

    let ack = send_replica_batch_tcp(replica_port, &batch);
    assert_eq!(
        ack,
        ReplicaAck::Ok {
            through_sequence: 3
        }
    );

    // Verify spend
    let slot = replica_engine
        .read_slot(&key_from_txid(txid_spend), 0)
        .unwrap();
    assert_eq!(slot.status, UTXO_SPENT);

    // Verify freeze
    let slot = replica_engine
        .read_slot(&key_from_txid(txid_freeze), 1)
        .unwrap();
    assert_eq!(slot.status, UTXO_FROZEN);

    // Verify set_mined
    let meta = replica_engine
        .read_metadata(&key_from_txid(txid_mined))
        .unwrap();
    assert_eq!(meta.block_entry_count, 1);

    _master_server.shutdown();
    _replica_server.shutdown();
}

#[test]
fn tcp_catchup_missed_ops() {
    let (_master_server, master_engine, _master_port) = start_test_server();
    let (_replica_server, replica_engine, replica_port) = start_test_server();

    // Create 10 records on both sides
    for i in 0..10u32 {
        let txid = test_txid(800 + i);
        create_record_on_engine(&master_engine, txid, 3);
        create_record_on_engine(&replica_engine, txid, 3);
    }

    // "Miss" the first 5 ops (don't send them yet)
    // Spend all 10 on the master
    let mut all_ops = Vec::new();
    for i in 0..10u32 {
        let txid = test_txid(800 + i);
        let key = key_from_txid(txid);
        let hash0 = test_utxo_hash(800 + i, 0);
        let mut sd = [0u8; 36];
        sd[0] = i as u8;
        master_engine
            .spend(&teraslab::ops::spend::SpendRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: hash0,
                spending_data: sd,
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 1000,
                block_height_retention: 288,
            })
            .unwrap();
        all_ops.push(ReplicaOp::Spend {
            tx_key: key,
            offset: 0,
            spending_data: sd,
            current_block_height: 1000,
            block_height_retention: 288,
            master_generation: 0,
        });
    }

    // Simulate the replica only having received through sequence 5
    // by sending the first 5 ops
    let batch1 = ReplicaBatch {
        first_sequence: 1,
        ops: all_ops[0..5].to_vec(),
        trace_ctx: None,
        source_node_id: None,
        cluster_key: 0,
    };
    let ack = send_replica_batch_tcp(replica_port, &batch1);
    assert_eq!(
        ack,
        ReplicaAck::Ok {
            through_sequence: 5
        }
    );

    // Simulate catchup: send the remaining 5 ops (6..10)
    let catchup_batch = ReplicaBatch {
        first_sequence: 6,
        ops: all_ops[5..10].to_vec(),
        trace_ctx: None,
        source_node_id: None,
        cluster_key: 0,
    };
    let ack = send_replica_batch_tcp(replica_port, &catchup_batch);
    assert_eq!(
        ack,
        ReplicaAck::Ok {
            through_sequence: 10
        }
    );

    // Verify all 10 are spent on the replica
    for i in 0..10u32 {
        let txid = test_txid(800 + i);
        let key = key_from_txid(txid);
        let slot = replica_engine.read_slot(&key, 0).unwrap();
        assert_eq!(
            slot.status, UTXO_SPENT,
            "slot {i} should be spent after catchup"
        );
        assert_eq!(slot.spending_data[0], i as u8);
    }

    _master_server.shutdown();
    _replica_server.shutdown();
}

#[test]
fn tcp_concurrent_replicate_and_client() {
    let (_master_server, master_engine, _master_port) = start_test_server();
    let (replica_server, replica_engine, replica_port) = start_test_server();

    // Create 20 records on replica: 10 for client writes, 10 for replication
    for i in 0..20u32 {
        let txid = test_txid(900 + i);
        create_record_on_engine(&replica_engine, txid, 3);
    }

    // Also create on master for the replication set
    for i in 10..20u32 {
        let txid = test_txid(900 + i);
        create_record_on_engine(&master_engine, txid, 3);
    }

    // Client thread: spend records 0..10 via regular TCP
    let client_handle = std::thread::spawn(move || {
        let mut stream = TcpStream::connect(format!("127.0.0.1:{replica_port}")).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        for i in 0..10u32 {
            let txid = test_txid(900 + i);
            let hash0 = test_utxo_hash(900 + i, 0);
            let mut sd = [0u8; 36];
            sd[0] = 0xAA;
            sd[1] = i as u8;
            let payload = encode_spend_batch(
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
                    request_id: 100 + i as u64,
                    op_code: OP_SPEND_BATCH,
                    flags: 0,
                    payload,
                },
            );
            assert_eq!(resp.status, STATUS_OK, "client spend {i} failed");
        }
    });

    // Replication thread: replicate spend ops for records 10..20
    let replication_handle = std::thread::spawn(move || {
        let addr = format!("127.0.0.1:{replica_port}");
        let mut transport = TcpReplicaTransport::connect(&addr, Duration::from_secs(5)).unwrap();

        let mut ops = Vec::new();
        for i in 10..20u32 {
            let txid = test_txid(900 + i);
            let key = key_from_txid(txid);
            let mut sd = [0u8; 36];
            sd[0] = 0xBB;
            sd[1] = i as u8;
            ops.push(ReplicaOp::Spend {
                tx_key: key,
                offset: 0,
                spending_data: sd,
                current_block_height: 1000,
                block_height_retention: 288,
                master_generation: 0,
            });
        }

        let batch = ReplicaBatch {
            first_sequence: 1,
            ops,
            trace_ctx: None,
            source_node_id: None,
            cluster_key: 0,
        };
        transport.send_batch(&batch).unwrap();
        let ack = transport.recv_ack(Duration::from_secs(5)).unwrap();
        assert_eq!(
            ack,
            ReplicaAck::Ok {
                through_sequence: 10
            }
        );
    });

    client_handle.join().unwrap();
    replication_handle.join().unwrap();

    // Verify all 20 are spent on the replica
    for i in 0..20u32 {
        let txid = test_txid(900 + i);
        let key = key_from_txid(txid);
        let slot = replica_engine.read_slot(&key, 0).unwrap();
        assert_eq!(
            slot.status, UTXO_SPENT,
            "record {i} should be spent on replica"
        );
    }

    replica_server.shutdown();
    _master_server.shutdown();
}

#[test]
fn tcp_replica_timeout() {
    // Start a server that we'll connect to, then never respond
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    // Accept and hold the connection open without sending a response
    let handle = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        // Read the request but don't respond
        let mut buf = [0u8; 4];
        let _ = stream.read_exact(&mut buf);
        let total = u32::from_le_bytes(buf) as usize;
        let mut body = vec![0u8; total];
        let _ = stream.read_exact(&mut body);
        // Hold connection open
        std::thread::sleep(Duration::from_secs(3));
        drop(stream);
    });

    let mut transport =
        TcpReplicaTransport::connect(&addr.to_string(), Duration::from_secs(5)).unwrap();

    let batch = ReplicaBatch {
        first_sequence: 1,
        ops: vec![ReplicaOp::Freeze {
            tx_key: key_from_txid(test_txid(999)),
            offset: 0,
            master_generation: 0,
        }],
        trace_ctx: None,
        source_node_id: None,
        cluster_key: 0,
    };
    transport.send_batch(&batch).unwrap();

    // recv_ack should timeout
    let result = transport.recv_ack(Duration::from_millis(200));
    assert!(
        matches!(result, Err(ReplicationError::Timeout(_))),
        "expected timeout, got {result:?}"
    );

    handle.join().unwrap();
}

#[test]
fn tcp_consistency_verification() {
    let (_master_server, master_engine, _master_port) = start_test_server();
    let (_replica_server, replica_engine, replica_port) = start_test_server();

    let num_records = 100u32;

    // Create 100 records on both
    for i in 0..num_records {
        let txid = test_txid(1000 + i);
        create_record_on_engine(&master_engine, txid, 3);
        create_record_on_engine(&replica_engine, txid, 3);
    }

    // Apply a diverse set of operations on the master, collect ReplicaOps
    let mut ops = Vec::new();

    // Spend the first 50
    for i in 0..50u32 {
        let txid = test_txid(1000 + i);
        let key = key_from_txid(txid);
        let hash0 = test_utxo_hash(1000 + i, 0);
        let mut sd = [0u8; 36];
        sd[0..4].copy_from_slice(&i.to_le_bytes());
        master_engine
            .spend(&teraslab::ops::spend::SpendRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: hash0,
                spending_data: sd,
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 1000,
                block_height_retention: 288,
            })
            .unwrap();
        ops.push(ReplicaOp::Spend {
            tx_key: key,
            offset: 0,
            spending_data: sd,
            current_block_height: 1000,
            block_height_retention: 288,
            master_generation: 0,
        });
    }

    // Freeze slots on records 50..70
    for i in 50..70u32 {
        let txid = test_txid(1000 + i);
        let key = key_from_txid(txid);
        let hash1 = test_utxo_hash(1000 + i, 1);
        master_engine
            .freeze(&teraslab::ops::remaining::FreezeRequest {
                tx_key: key,
                offset: 1,
                utxo_hash: hash1,
            })
            .unwrap();
        ops.push(ReplicaOp::Freeze {
            tx_key: key,
            offset: 1,
            master_generation: 0,
        });
    }

    // SetMined on records 70..90
    for i in 70..90u32 {
        let txid = test_txid(1000 + i);
        let key = key_from_txid(txid);
        master_engine
            .set_mined(&teraslab::ops::set_mined::SetMinedRequest {
                tx_key: key,
                block_id: i,
                block_height: 1000 + i,
                subtree_idx: 0,
                current_block_height: 1000 + i,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: false,
            })
            .unwrap();
        ops.push(ReplicaOp::SetMined {
            tx_key: key,
            block_id: i,
            block_height: 1000 + i,
            subtree_idx: 0,
            on_longest_chain: true,
            current_block_height: 1000 + i,
            block_height_retention: 288,
            master_generation: 0,
        });
    }

    // SetLocked on records 90..100
    for i in 90..num_records {
        let txid = test_txid(1000 + i);
        let key = key_from_txid(txid);
        master_engine
            .set_locked(&teraslab::ops::remaining::SetLockedRequest {
                tx_key: key,
                value: true,
            })
            .unwrap();
        ops.push(ReplicaOp::SetLocked {
            tx_key: key,
            value: true,
            master_generation: 0,
        });
    }

    // Send all ops as one large batch
    let batch = ReplicaBatch {
        first_sequence: 1,
        ops: ops.clone(),
        trace_ctx: None,
        source_node_id: None,
        cluster_key: 0,
    };
    let ack = send_replica_batch_tcp(replica_port, &batch);
    let expected_through = ops.len() as u64;
    assert_eq!(
        ack,
        ReplicaAck::Ok {
            through_sequence: expected_through
        }
    );

    // Verify consistency: every record should match master state on replica
    // Check spent records
    for i in 0..50u32 {
        let txid = test_txid(1000 + i);
        let key = key_from_txid(txid);
        let master_slot = master_engine.read_slot(&key, 0).unwrap();
        let replica_slot = replica_engine.read_slot(&key, 0).unwrap();
        assert_eq!(
            master_slot.status, replica_slot.status,
            "status mismatch for record {i}, slot 0"
        );
        assert_eq!(
            master_slot.spending_data, replica_slot.spending_data,
            "spending_data mismatch for record {i}, slot 0"
        );
    }

    // Check frozen records
    for i in 50..70u32 {
        let txid = test_txid(1000 + i);
        let key = key_from_txid(txid);
        let master_slot = master_engine.read_slot(&key, 1).unwrap();
        let replica_slot = replica_engine.read_slot(&key, 1).unwrap();
        assert_eq!(
            master_slot.status, replica_slot.status,
            "status mismatch for frozen record {i}, slot 1"
        );
    }

    // Check mined metadata
    for i in 70..90u32 {
        let txid = test_txid(1000 + i);
        let key = key_from_txid(txid);
        let master_meta = master_engine.read_metadata(&key).unwrap();
        let replica_meta = replica_engine.read_metadata(&key).unwrap();
        assert_eq!(
            master_meta.block_entry_count, replica_meta.block_entry_count,
            "block_entry_count mismatch for record {i}"
        );
    }

    // Check locked metadata
    for i in 90..num_records {
        let txid = test_txid(1000 + i);
        let key = key_from_txid(txid);
        let master_meta = master_engine.read_metadata(&key).unwrap();
        let replica_meta = replica_engine.read_metadata(&key).unwrap();
        assert_eq!(
            master_meta
                .flags
                .contains(teraslab::record::TxFlags::LOCKED),
            replica_meta
                .flags
                .contains(teraslab::record::TxFlags::LOCKED),
            "locked flag mismatch for record {i}"
        );
    }

    _master_server.shutdown();
    _replica_server.shutdown();
}

// ---------------------------------------------------------------------------
// R-052 / R-053: MarkLongestChain replication
// ---------------------------------------------------------------------------

/// R-052: A `MarkLongestChain` `ReplicaOp` MUST replicate the master's
/// `unmined_since`, `delete_at_height`, and `generation` mutations to
/// the replica. Pre-fix the dispatch handler emitted no `ReplicaOp` for
/// `OP_MARK_LONGEST_CHAIN_BATCH`, so any reorg silently desynced master
/// and replica DAH/unmined state.
///
/// This test exercises the receiver path directly: the master mutates
/// its own engine; the replica receives the matching `ReplicaOp` over
/// TCP. If R-052 is reverted (variant removed or the encode arm
/// dropped) this test will fail to compile or the receiver will reject
/// opcode 14 — both are loud failure modes.
#[test]
fn cluster_mark_longest_chain_replicates_dah_unmined() {
    let (master_server, master_engine, _master_port) = start_test_server();
    let (replica_server, replica_engine, replica_port) = start_test_server();

    // Create the same record on both sides so they share a starting
    // generation of 1 (Engine::create initialises generation = 1).
    let txid = test_txid(7000);
    let key = key_from_txid(txid);
    create_record_on_engine(&master_engine, txid, 2);
    create_record_on_engine(&replica_engine, txid, 2);

    // Sanity: both records start with unmined_since = 0 and DAH = 0.
    let master_pre = master_engine.read_metadata(&key).unwrap();
    let replica_pre = replica_engine.read_metadata(&key).unwrap();
    assert_eq!({ master_pre.unmined_since }, 0);
    assert_eq!({ replica_pre.unmined_since }, 0);
    assert_eq!({ master_pre.delete_at_height }, 0);
    assert_eq!({ replica_pre.delete_at_height }, 0);
    let master_pre_gen = { master_pre.generation };
    assert_eq!(master_pre_gen, { replica_pre.generation });

    // Apply mark_on_longest_chain(false) on master — simulates the tx
    // dropping off the longest chain in a reorg. unmined_since is set
    // to current_block_height; DAH is evaluated and may be set; the
    // record's generation increments.
    let req = teraslab::ops::mark_longest_chain::MarkOnLongestChainRequest {
        tx_key: key,
        on_longest_chain: false,
        current_block_height: 800_500,
        block_height_retention: 288,
    };
    let resp = master_engine.mark_on_longest_chain(&req).unwrap();
    let master_post = master_engine.read_metadata(&key).unwrap();
    let master_unmined_after = { master_post.unmined_since };
    let master_dah_after = { master_post.delete_at_height };
    let master_gen_after = { master_post.generation };
    assert_eq!(
        master_unmined_after, 800_500,
        "master's unmined_since must update on mark(off, h)",
    );
    assert_eq!(master_gen_after, master_pre_gen + 1);
    assert_eq!(resp.generation, master_gen_after);

    // Build and send the matching ReplicaOp::MarkLongestChain to the
    // replica. Pre-fix this variant did not exist; the bug was that the
    // dispatch handler had nothing to send, so the replica observed no
    // mutation at all.
    let batch = ReplicaBatch {
        first_sequence: 1,
        ops: vec![ReplicaOp::MarkLongestChain {
            tx_key: key,
            on_longest_chain: false,
            current_block_height: 800_500,
            block_height_retention: 288,
            master_generation: master_gen_after,
        }],
        trace_ctx: None,
        source_node_id: None,
        cluster_key: 0,
    };
    let ack = send_replica_batch_tcp(replica_port, &batch);
    assert_eq!(
        ack,
        ReplicaAck::Ok {
            through_sequence: 1
        }
    );

    // Replica state MUST converge with master.
    let replica_post = replica_engine.read_metadata(&key).unwrap();
    assert_eq!(
        { replica_post.unmined_since },
        master_unmined_after,
        "replica unmined_since diverged from master",
    );
    assert_eq!(
        { replica_post.delete_at_height },
        master_dah_after,
        "replica delete_at_height diverged from master — DAH index would drift",
    );
    assert_eq!(
        { replica_post.generation },
        master_gen_after,
        "replica generation must match master after applying MarkLongestChain",
    );

    master_server.shutdown();
    replica_server.shutdown();
}

/// R-053: applying the same `MarkLongestChain` `ReplicaOp` twice on a
/// replica MUST be a no-op the second time. Without per-generation
/// idempotency, recovery replay of the redo log (or duplicate receive
/// of the same wire batch after a transient ack drop) would cause:
///   - generation to bump twice (de-syncing future stale-op checks),
///   - DAH/unmined secondary indexes to be re-written for no reason,
///   - the master's `master_generation` value to be overwritten, then
///     immediately overwritten back via the post-apply generation
///     sync — visible as redb churn under load.
#[test]
fn mark_longest_chain_replay_idempotent() {
    let (replica_server, replica_engine, replica_port) = start_test_server();

    // Replica starts with the record so the op has something to apply.
    let txid = test_txid(7100);
    let key = key_from_txid(txid);
    create_record_on_engine(&replica_engine, txid, 1);

    let pre = replica_engine.read_metadata(&key).unwrap();
    let pre_gen = { pre.generation };

    // Master generation is one ahead of the replica's starting generation
    // — the canonical "first apply on the replica" case.
    let master_generation = pre_gen + 1;

    let op = ReplicaOp::MarkLongestChain {
        tx_key: key,
        on_longest_chain: false,
        current_block_height: 900_000,
        block_height_retention: 288,
        master_generation,
    };

    // First apply: must mutate the replica's metadata.
    let ack1 = send_replica_batch_tcp(
        replica_port,
        &ReplicaBatch {
            first_sequence: 1,
            ops: vec![op.clone()],
            trace_ctx: None,
            source_node_id: None,
            cluster_key: 0,
        },
    );
    assert_eq!(
        ack1,
        ReplicaAck::Ok {
            through_sequence: 1
        }
    );
    let post1 = replica_engine.read_metadata(&key).unwrap();
    let post1_gen = { post1.generation };
    let post1_unmined = { post1.unmined_since };
    let post1_dah = { post1.delete_at_height };
    assert_eq!(
        post1_gen, master_generation,
        "first apply must sync replica generation to master",
    );
    assert_eq!(
        post1_unmined, 900_000,
        "first apply must set unmined_since to current_block_height",
    );

    // Second apply of the SAME op (same master_generation): must be a
    // no-op. The pre-apply guard inside `apply_op` for MarkLongestChain
    // (R-053) skips when `local_gen >= master_generation` — here they
    // are EQUAL, so the engine call is bypassed entirely. Without the
    // R-053 fix, the equal-generation case would fall through and
    // bump the engine generation again before the trailing
    // generation-sync rewrites it back to master_generation.
    let ack2 = send_replica_batch_tcp(
        replica_port,
        &ReplicaBatch {
            first_sequence: 2,
            ops: vec![op.clone()],
            trace_ctx: None,
            source_node_id: None,
            cluster_key: 0,
        },
    );
    assert_eq!(
        ack2,
        ReplicaAck::Ok {
            through_sequence: 2
        }
    );
    let post2 = replica_engine.read_metadata(&key).unwrap();
    assert_eq!(
        { post2.generation },
        post1_gen,
        "replay of same MarkLongestChain must NOT bump generation (R-053)",
    );
    assert_eq!(
        { post2.unmined_since },
        post1_unmined,
        "replay must NOT change unmined_since",
    );
    assert_eq!(
        { post2.delete_at_height },
        post1_dah,
        "replay must NOT churn the DAH index",
    );

    // A strictly-stale op (master_generation < local_gen) must also be
    // rejected by the existing pre-apply guard at the top of apply_op.
    // We send an op with an older master_generation and assert the
    // replica state is unchanged.
    let stale_op = ReplicaOp::MarkLongestChain {
        tx_key: key,
        on_longest_chain: true, // would normally clear unmined_since
        current_block_height: 950_000,
        block_height_retention: 288,
        master_generation: master_generation.saturating_sub(1),
    };
    let ack3 = send_replica_batch_tcp(
        replica_port,
        &ReplicaBatch {
            first_sequence: 3,
            ops: vec![stale_op],
            trace_ctx: None,
            source_node_id: None,
            cluster_key: 0,
        },
    );
    assert_eq!(
        ack3,
        ReplicaAck::Ok {
            through_sequence: 3
        }
    );
    let post3 = replica_engine.read_metadata(&key).unwrap();
    assert_eq!(
        { post3.generation },
        post1_gen,
        "stale op must not bump generation",
    );
    assert_eq!(
        { post3.unmined_since },
        post1_unmined,
        "stale op must not revert unmined_since to 0",
    );

    replica_server.shutdown();
}

// ---------------------------------------------------------------------------
// C-10 / F-G7-018 — WriteMajority early-return on majority ACK.
//
// 3-replica fan-out (so RF=4 — master + 3 replicas) where one replica
// sleeps 500ms before ACKing. With `WriteMajority`, the master needs
// 2 replica ACKs (master counts as 1; majority of 4 = 3 copies). The
// two fast replicas ACK in ~5ms, so the master returns in under
// ~100ms — without the C-10 fix the previous "join all" path waited
// 500ms.
//
// Uses `InMemoryTransport` for determinism — the slow ACK is a literal
// `thread::sleep` rather than a TCP RTT, which removes scheduler /
// kernel jitter from the latency assertion.
// ---------------------------------------------------------------------------

#[test]
fn write_majority_early_return_does_not_wait_for_slow_replica() {
    use teraslab::index::TxKey;

    // Helper: replica that sleeps `delay` before ACKing each batch.
    fn spawn_delayed_ack(
        rt: InMemoryTransport,
        delay: Duration,
    ) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            while let Ok(batch) = rt.recv_batch(Duration::from_secs(5)) {
                std::thread::sleep(delay);
                let ack = ReplicaAck::Ok {
                    through_sequence: batch.last_sequence(),
                };
                if rt.send_ack(&ack).is_err() {
                    return;
                }
            }
        })
    }

    let (mt1, rt1) = InMemoryTransport::pair();
    let (mt2, rt2) = InMemoryTransport::pair();
    let (mt3, rt3) = InMemoryTransport::pair();

    let slow_delay = Duration::from_millis(500);
    let fast_delay = Duration::from_millis(5);
    let h_slow = spawn_delayed_ack(rt1, slow_delay);
    let h_fast_a = spawn_delayed_ack(rt2, fast_delay);
    let h_fast_b = spawn_delayed_ack(rt3, fast_delay);

    let mut mgr = ReplicationManager::new(
        ReplicationConfig {
            ack_policy: AckPolicy::WriteMajority,
            // Generous per-replica timeout — the slow replica's 500ms
            // is well under this so the master would otherwise wait
            // the full 500ms on the "join all" path.
            replication_timeout: Duration::from_secs(2),
            ..Default::default()
        },
        vec![Box::new(mt1), Box::new(mt2), Box::new(mt3)],
    );

    // 3 replicas + 1 master = RF=4. Majority of 4 = 3 copies. Master
    // counts as 1, so we need 2 replica ACKs. The two fast replicas
    // satisfy this; the slow replica becomes a background straggler.
    assert_eq!(
        mgr.required_ack_count(),
        2,
        "3 replicas + master = RF=4; majority needs 2 replica acks",
    );

    let ops = vec![ReplicaOp::Freeze {
        tx_key: TxKey { txid: [1u8; 32] },
        offset: 0,
        master_generation: 0,
    }];

    let start = std::time::Instant::now();
    mgr.replicate_batch(&ops).expect("majority ACK should succeed");
    let elapsed = start.elapsed();

    // The two fast replicas ack at ~5ms; the slow one at ~500ms. With
    // C-10 the master returns after the first fast ack, well under 100ms.
    // Without C-10 the master would wait 500ms (the slow replica's RTT).
    assert!(
        elapsed < Duration::from_millis(100),
        "C-10 early-return must return < 100ms with one 500ms-slow replica; \
         observed {elapsed:?} (without the fix this would be ~500ms)",
    );

    // The next_sequence advances past the batch regardless of which
    // replicas have acked (durable-log invariant — every assigned
    // sequence is journalled).
    assert_eq!(
        mgr.current_sequence(),
        2,
        "next_sequence must advance past the 1-op batch",
    );

    // Drop the manager — its destructor closes the InMemoryTransport
    // channels and the replica loops exit cleanly. The slow replica
    // worker is still running in the background; it ack's into a
    // (possibly dropped) receiver and exits.
    drop(mgr);
    let _ = h_slow.join();
    let _ = h_fast_a.join();
    let _ = h_fast_b.join();
}

/// Sanity: even when the slow replica is index-0 (first in dispatch
/// order) the master still early-returns on the fast acks. Prevents
/// regressions where the master accidentally waits for `senders[0]`
/// first.
#[test]
fn write_majority_early_return_with_slow_replica_first() {
    use teraslab::index::TxKey;

    fn spawn_delayed_ack(
        rt: InMemoryTransport,
        delay: Duration,
    ) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            while let Ok(batch) = rt.recv_batch(Duration::from_secs(5)) {
                std::thread::sleep(delay);
                let ack = ReplicaAck::Ok {
                    through_sequence: batch.last_sequence(),
                };
                if rt.send_ack(&ack).is_err() {
                    return;
                }
            }
        })
    }

    let (mt0, rt0) = InMemoryTransport::pair();
    let (mt1, rt1) = InMemoryTransport::pair();
    let (mt2, rt2) = InMemoryTransport::pair();

    // Slow replica is at index 0 — the previous "join all" path joined
    // in sender order so this is the worst-case latency for the old
    // implementation.
    let h0 = spawn_delayed_ack(rt0, Duration::from_millis(500));
    let h1 = spawn_delayed_ack(rt1, Duration::from_millis(5));
    let h2 = spawn_delayed_ack(rt2, Duration::from_millis(5));

    let mut mgr = ReplicationManager::new(
        ReplicationConfig {
            ack_policy: AckPolicy::WriteMajority,
            replication_timeout: Duration::from_secs(2),
            ..Default::default()
        },
        vec![Box::new(mt0), Box::new(mt1), Box::new(mt2)],
    );

    let ops = vec![ReplicaOp::Freeze {
        tx_key: TxKey { txid: [2u8; 32] },
        offset: 0,
        master_generation: 0,
    }];

    let start = std::time::Instant::now();
    mgr.replicate_batch(&ops).expect("majority ACK should succeed");
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_millis(100),
        "early-return must not depend on dispatch order; got {elapsed:?}",
    );

    drop(mgr);
    let _ = h0.join();
    let _ = h1.join();
    let _ = h2.join();
}
