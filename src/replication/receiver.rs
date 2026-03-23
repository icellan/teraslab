//! Replica-side replication receiver.
//!
//! Listens for `OP_REPLICA_BATCH` frames from the master and applies
//! operations to the local engine using idempotent mutation methods.
//! Each incoming batch is acknowledged with a `ReplicaAck` response frame.

use crate::io;
use crate::ops::create::*;
use crate::ops::engine::Engine;
use crate::ops::remaining::*;
use crate::ops::set_mined::*;
use crate::ops::spend::*;
use crate::ops::unspend::*;
use crate::protocol::frame::{RequestFrame, ResponseFrame};
use crate::protocol::opcodes::*;
use crate::record::*;
use crate::replication::protocol::{ReplicaAck, ReplicaBatch, ReplicaOp};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// Replica-side replication receiver.
///
/// Accepts TCP connections from the master, reads `OP_REPLICA_BATCH`
/// request frames, applies each operation to the local `Engine`, and
/// sends back `ReplicaAck` response frames.
///
/// Multiple master connections can be handled concurrently; each gets
/// its own handler thread.
pub struct ReplicationReceiver {
    engine: Arc<Engine>,
    last_applied_sequence: Arc<AtomicU64>,
    running: Arc<AtomicBool>,
}

impl ReplicationReceiver {
    /// Create a new receiver backed by the given engine.
    pub fn new(engine: Arc<Engine>) -> Self {
        Self {
            engine,
            last_applied_sequence: Arc::new(AtomicU64::new(0)),
            running: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Start listening on the given address for replication connections.
    ///
    /// Spawns a background thread that accepts connections and a handler
    /// thread for each accepted connection. Returns after the listener
    /// thread is spawned. Use [`stop`](Self::stop) to shut down.
    pub fn start(&self, addr: &str) -> Result<(), String> {
        let listener = TcpListener::bind(addr)
            .map_err(|e| format!("failed to bind replication receiver on {addr}: {e}"))?;
        listener
            .set_nonblocking(true)
            .map_err(|e| format!("set non-blocking: {e}"))?;

        let engine = self.engine.clone();
        let running = self.running.clone();
        let last_applied = self.last_applied_sequence.clone();

        std::thread::spawn(move || {
            while running.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _addr)) => {
                        let eng = engine.clone();
                        let run = running.clone();
                        let la = last_applied.clone();
                        std::thread::spawn(move || {
                            handle_connection(&eng, stream, &run, &la);
                        });
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(std::time::Duration::from_millis(10));
                    }
                    Err(_e) => {
                        // Transient accept error; keep looping
                        std::thread::sleep(std::time::Duration::from_millis(50));
                    }
                }
            }
        });

        Ok(())
    }

    /// Highest sequence number that has been durably applied.
    pub fn last_applied_sequence(&self) -> u64 {
        self.last_applied_sequence.load(Ordering::Relaxed)
    }

    /// Signal the receiver to stop accepting new connections.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

/// Handle a single connection from the master.
///
/// Reads request frames in a loop. For each `OP_REPLICA_BATCH`,
/// deserializes the batch, applies every op to the engine, and sends
/// back a `ReplicaAck` response.
fn handle_connection(
    engine: &Engine,
    mut stream: TcpStream,
    running: &AtomicBool,
    last_applied: &AtomicU64,
) {
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(30)));

    loop {
        if !running.load(Ordering::Relaxed) {
            return;
        }

        // Read 4-byte length prefix
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => continue,
            Err(_) => return,
        }

        let total_length = u32::from_le_bytes(len_buf);
        if total_length > MAX_FRAME_SIZE {
            // Frame too large, close connection
            return;
        }

        // Read the frame body
        let frame_len = total_length as usize;
        let mut body = vec![0u8; frame_len];
        if stream.read_exact(&mut body).is_err() {
            return;
        }

        // Reconstruct and decode full frame
        let mut frame_bytes = Vec::with_capacity(4 + frame_len);
        frame_bytes.extend_from_slice(&len_buf);
        frame_bytes.extend_from_slice(&body);

        let (request, _) = match RequestFrame::decode(&frame_bytes) {
            Ok(r) => r,
            Err(_) => return,
        };

        let response = if request.op_code == OP_REPLICA_BATCH {
            handle_replica_batch(&request, engine, last_applied)
        } else {
            // Unknown opcode for replication receiver
            ResponseFrame {
                request_id: request.request_id,
                status: STATUS_ERROR,
                payload: b"unsupported opcode".to_vec(),
            }
        };

        let response_bytes = response.encode();
        if stream.write_all(&response_bytes).is_err() {
            return;
        }
    }
}

/// Process an `OP_REPLICA_BATCH` request frame.
///
/// Deserializes the `ReplicaBatch` from the payload, applies each op
/// to the engine, and returns a `ResponseFrame` containing a serialized
/// `ReplicaAck`.
pub fn handle_replica_batch(
    request: &RequestFrame,
    engine: &Engine,
    last_applied: &AtomicU64,
) -> ResponseFrame {
    let batch = match ReplicaBatch::deserialize(&request.payload) {
        Ok(b) => b,
        Err(e) => {
            let ack = ReplicaAck::Error {
                failed_sequence: 0,
                message: format!("deserialize batch: {e}"),
            };
            return ResponseFrame {
                request_id: request.request_id,
                status: STATUS_OK,
                payload: ack.serialize(),
            };
        }
    };

    let mut seq = batch.first_sequence;
    for op in &batch.ops {
        if let Err(msg) = apply_op(engine, op) {
            let ack = ReplicaAck::Error {
                failed_sequence: seq,
                message: msg,
            };
            return ResponseFrame {
                request_id: request.request_id,
                status: STATUS_OK,
                payload: ack.serialize(),
            };
        }
        seq += 1;
    }

    let through = batch.last_sequence();
    last_applied.store(through, Ordering::Relaxed);

    let ack = ReplicaAck::Ok {
        through_sequence: through,
    };
    ResponseFrame {
        request_id: request.request_id,
        status: STATUS_OK,
        payload: ack.serialize(),
    }
}

/// Apply a single `ReplicaOp` to the engine.
///
/// For Spend, Freeze, Unfreeze, and Reassign operations the replica
/// does not have the UTXO hash in the op payload, so it reads the
/// current slot from the device to obtain the hash. The replica uses
/// `ignore_conflicting = true` and `ignore_locked = true` because the
/// master already validated those constraints.
///
/// Returns `Ok(())` on success (including graceful skip for
/// not-found records), or `Err(message)` if the operation fails in a
/// way that should abort the batch.
pub fn apply_op(engine: &Engine, op: &ReplicaOp) -> std::result::Result<(), String> {
    match op {
        ReplicaOp::Spend {
            tx_key,
            offset,
            spending_data,
        } => {
            // Read the slot to get the UTXO hash
            let hash = match engine.read_slot(tx_key, *offset) {
                Ok(slot) => slot.hash,
                Err(_) => {
                    // TX or slot not found — skip gracefully
                    return Ok(());
                }
            };
            let req = SpendRequest {
                tx_key: *tx_key,
                offset: *offset,
                utxo_hash: hash,
                spending_data: *spending_data,
                ignore_conflicting: true,
                ignore_locked: true,
                current_block_height: 0,
                block_height_retention: 0,
            };
            match engine.spend(&req) {
                Ok(_) => Ok(()),
                // Already spent with same data is idempotent
                Err(crate::ops::error::SpendError::AlreadySpent { .. }) => Ok(()),
                // Frozen is expected if the slot was frozen and we're replaying
                Err(crate::ops::error::SpendError::Frozen { .. }) => Ok(()),
                // Pruned slots cannot be spent
                Err(crate::ops::error::SpendError::Pruned { .. }) => Ok(()),
                Err(e) => Err(format!("spend: {e}")),
            }
        }
        ReplicaOp::Unspend { tx_key, offset } => {
            let hash = match engine.read_slot(tx_key, *offset) {
                Ok(slot) => slot.hash,
                Err(_) => return Ok(()),
            };
            let req = UnspendRequest {
                tx_key: *tx_key,
                offset: *offset,
                utxo_hash: hash,
                current_block_height: 0,
                block_height_retention: 0,
            };
            match engine.unspend(&req) {
                Ok(_) => Ok(()),
                // Already unspent is fine (idempotent)
                Err(crate::ops::error::SpendError::InvalidSpend { .. }) => Ok(()),
                Err(e) => Err(format!("unspend: {e}")),
            }
        }
        ReplicaOp::SetMined {
            tx_key,
            block_id,
            block_height,
            subtree_idx,
            on_longest_chain,
        } => {
            let req = SetMinedRequest {
                tx_key: *tx_key,
                block_id: *block_id,
                block_height: *block_height,
                subtree_idx: *subtree_idx,
                current_block_height: *block_height,
                block_height_retention: 288,
                on_longest_chain: *on_longest_chain,
                unset_mined: false,
            };
            match engine.set_mined(&req) {
                Ok(_) => Ok(()),
                Err(crate::ops::error::SpendError::TxNotFound) => Ok(()),
                Err(e) => Err(format!("set_mined: {e}")),
            }
        }
        ReplicaOp::UnsetMined { tx_key, block_id } => {
            let req = SetMinedRequest {
                tx_key: *tx_key,
                block_id: *block_id,
                block_height: 0,
                subtree_idx: 0,
                current_block_height: 0,
                block_height_retention: 288,
                on_longest_chain: false,
                unset_mined: true,
            };
            match engine.set_mined(&req) {
                Ok(_) => Ok(()),
                Err(crate::ops::error::SpendError::TxNotFound) => Ok(()),
                Err(e) => Err(format!("unset_mined: {e}")),
            }
        }
        ReplicaOp::Freeze { tx_key, offset } => {
            let hash = match engine.read_slot(tx_key, *offset) {
                Ok(slot) => slot.hash,
                Err(_) => return Ok(()),
            };
            let req = FreezeRequest {
                tx_key: *tx_key,
                offset: *offset,
                utxo_hash: hash,
            };
            match engine.freeze(&req) {
                Ok(()) => Ok(()),
                Err(crate::ops::error::SpendError::AlreadyFrozen { .. }) => Ok(()),
                Err(crate::ops::error::SpendError::AlreadySpent { .. }) => Ok(()),
                Err(crate::ops::error::SpendError::TxNotFound) => Ok(()),
                Err(e) => Err(format!("freeze: {e}")),
            }
        }
        ReplicaOp::Unfreeze { tx_key, offset } => {
            let hash = match engine.read_slot(tx_key, *offset) {
                Ok(slot) => slot.hash,
                Err(_) => return Ok(()),
            };
            let req = UnfreezeRequest {
                tx_key: *tx_key,
                offset: *offset,
                utxo_hash: hash,
            };
            match engine.unfreeze(&req) {
                Ok(()) => Ok(()),
                Err(crate::ops::error::SpendError::NotFrozen { .. }) => Ok(()),
                Err(crate::ops::error::SpendError::TxNotFound) => Ok(()),
                Err(e) => Err(format!("unfreeze: {e}")),
            }
        }
        ReplicaOp::Reassign {
            tx_key,
            offset,
            new_hash,
            block_height,
            spendable_after,
        } => {
            let old_hash = match engine.read_slot(tx_key, *offset) {
                Ok(slot) => slot.hash,
                Err(_) => return Ok(()),
            };
            let req = ReassignRequest {
                tx_key: *tx_key,
                offset: *offset,
                utxo_hash: old_hash,
                new_utxo_hash: *new_hash,
                block_height: *block_height,
                spendable_after: *spendable_after,
            };
            match engine.reassign(&req) {
                Ok(()) => Ok(()),
                Err(crate::ops::error::SpendError::NotFrozen { .. }) => Ok(()),
                Err(crate::ops::error::SpendError::TxNotFound) => Ok(()),
                Err(e) => Err(format!("reassign: {e}")),
            }
        }
        ReplicaOp::SetConflicting {
            tx_key,
            value,
            current_block_height,
            retention,
        } => {
            let req = SetConflictingRequest {
                tx_key: *tx_key,
                value: *value,
                current_block_height: *current_block_height,
                block_height_retention: *retention,
            };
            match engine.set_conflicting(&req) {
                Ok(_) => Ok(()),
                Err(crate::ops::error::SpendError::TxNotFound) => Ok(()),
                Err(e) => Err(format!("set_conflicting: {e}")),
            }
        }
        ReplicaOp::SetLocked { tx_key, value } => {
            let req = SetLockedRequest {
                tx_key: *tx_key,
                value: *value,
            };
            match engine.set_locked(&req) {
                Ok(()) => Ok(()),
                Err(crate::ops::error::SpendError::TxNotFound) => Ok(()),
                Err(e) => Err(format!("set_locked: {e}")),
            }
        }
        ReplicaOp::PreserveUntil {
            tx_key,
            block_height,
        } => {
            let req = PreserveUntilRequest {
                tx_key: *tx_key,
                block_height: *block_height,
            };
            match engine.preserve_until(&req) {
                Ok(_) => Ok(()),
                Err(crate::ops::error::SpendError::TxNotFound) => Ok(()),
                Err(e) => Err(format!("preserve_until: {e}")),
            }
        }
        ReplicaOp::Create {
            tx_key,
            metadata_bytes,
            utxo_hashes,
            cold_data,
            is_external,
        } => {
            // Build a CreateRequest using metadata from the master.
            // The metadata_bytes contains: tx_version(4) + locktime(4) + fee(8) +
            // size_in_bytes(8) + extended_size(8) + is_coinbase(1) + spending_height(4) +
            // created_at(8) + flags(1) = 46 bytes.
            let (tx_version, locktime, fee, size_in_bytes, extended_size,
                 is_coinbase, spending_height, created_at) =
                if metadata_bytes.len() >= 46 {
                    let m = metadata_bytes.as_slice();
                    (
                        u32::from_le_bytes(m[0..4].try_into().unwrap()),
                        u32::from_le_bytes(m[4..8].try_into().unwrap()),
                        u64::from_le_bytes(m[8..16].try_into().unwrap()),
                        u64::from_le_bytes(m[16..24].try_into().unwrap()),
                        u64::from_le_bytes(m[24..32].try_into().unwrap()),
                        m[32] != 0,
                        u32::from_le_bytes(m[33..37].try_into().unwrap()),
                        u64::from_le_bytes(m[37..45].try_into().unwrap()),
                    )
                } else {
                    (1, 0, 0, 0, 0, false, 0,
                     std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64)
                };

            let create_req = CreateRequest {
                tx_id: tx_key.txid,
                tx_version,
                locktime,
                fee,
                size_in_bytes,
                extended_size,
                is_coinbase,
                spending_height,
                utxo_hashes: utxo_hashes.clone(),
                inputs: None,
                outputs: None,
                inpoints: None,
                is_external: *is_external,
                created_at,
                block_height: 0,
                mined_block_infos: vec![],
                frozen: false,
                conflicting: false,
                locked: false,
                parent_txids: vec![],
            };
            match engine.create(&create_req) {
                Ok(_) => {
                    // Store cold data in the blobstore if provided.
                    if let Some(data) = cold_data
                        && !data.is_empty()
                        && let Some(bs) = engine.blob_store()
                        && let Err(e) = bs.put(&tx_key.txid, data)
                    {
                        eprintln!("replication: failed to store cold data for {:?}: {e}", tx_key);
                    }
                    Ok(())
                }
                Err(CreateError::DuplicateTxId) => Ok(()), // idempotent
                Err(e) => Err(format!("create: {e}")),
            }
        }
        ReplicaOp::Delete { tx_key } => {
            let req = DeleteRequest { tx_key: *tx_key };
            match engine.delete(&req) {
                Ok(()) => Ok(()),
                Err(crate::ops::error::SpendError::TxNotFound) => Ok(()),
                Err(e) => Err(format!("delete: {e}")),
            }
        }
        ReplicaOp::PruneSlot { tx_key, offset } => {
            // PruneSlot sets the UTXO status to PRUNED. Since the engine
            // doesn't have a dedicated prune_slot method, we write the
            // slot directly via io, similar to how recovery handles it.
            let entry = match engine.lookup(tx_key) {
                Some(e) => e,
                None => return Ok(()), // TX not found — skip
            };
            let slot = match io::read_utxo_slot(engine.device(), entry.record_offset, *offset) {
                Ok(s) => s,
                Err(_) => return Ok(()),
            };
            if slot.status == UTXO_PRUNED {
                return Ok(()); // already pruned
            }
            let mut pruned = slot;
            pruned.status = UTXO_PRUNED;
            io::write_utxo_slot(engine.device(), entry.record_offset, *offset, &pruned)
                .map_err(|e| format!("prune_slot: {e}"))?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::SlotAllocator;
    use crate::device::{BlockDevice, MemoryDevice};
    use crate::index::{DahIndex, Index, TxKey, UnminedIndex};
    use crate::locks::StripedLocks;

    fn make_engine() -> Arc<Engine> {
        let dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone());
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

    fn key(n: u8) -> TxKey {
        let mut txid = [0u8; 32];
        txid[0] = n;
        TxKey { txid }
    }

    fn create_record(engine: &Engine, k: TxKey, utxo_count: u32) {
        let hashes: Vec<[u8; 32]> = (0..utxo_count)
            .map(|i| {
                let mut h = [0u8; 32];
                h[0] = i as u8;
                h[4..8].copy_from_slice(&k.txid[0..4]);
                h
            })
            .collect();
        let req = CreateRequest {
            tx_id: k.txid,
            tx_version: 1,
            locktime: 0,
            fee: 0,
            size_in_bytes: 0,
            extended_size: 0,
            is_coinbase: false,
            spending_height: 0,
            utxo_hashes: hashes,
            inputs: None,
            outputs: None,
            inpoints: None,
            is_external: false,
            created_at: 0,
            block_height: 0,
            mined_block_infos: vec![],
            frozen: false,
            conflicting: false,
            locked: false,
            parent_txids: vec![],
        };
        engine.create(&req).unwrap();
    }

    #[test]
    fn apply_spend_op() {
        let engine = make_engine();
        let k = key(1);
        create_record(&engine, k, 3);

        let slot = engine.read_slot(&k, 0).unwrap();
        assert_eq!(slot.status, UTXO_UNSPENT);

        let op = ReplicaOp::Spend {
            tx_key: k,
            offset: 0,
            spending_data: [0xAB; 36],
        };
        apply_op(&engine, &op).unwrap();

        let slot = engine.read_slot(&k, 0).unwrap();
        assert_eq!(slot.status, UTXO_SPENT);
        assert_eq!(slot.spending_data[0], 0xAB);
    }

    #[test]
    fn apply_spend_idempotent() {
        let engine = make_engine();
        let k = key(2);
        create_record(&engine, k, 3);

        let op = ReplicaOp::Spend {
            tx_key: k,
            offset: 0,
            spending_data: [0xAB; 36],
        };
        apply_op(&engine, &op).unwrap();
        // Apply again — should not error
        apply_op(&engine, &op).unwrap();

        let slot = engine.read_slot(&k, 0).unwrap();
        assert_eq!(slot.status, UTXO_SPENT);
    }

    #[test]
    fn apply_create_op() {
        let engine = make_engine();
        let k = key(10);
        let hashes = vec![[0xAA; 32]; 5];

        let op = ReplicaOp::Create {
            tx_key: k,
            metadata_bytes: vec![0; 64],
            utxo_hashes: hashes,
            cold_data: None,
            is_external: false,
        };
        apply_op(&engine, &op).unwrap();

        let slot = engine.read_slot(&k, 0).unwrap();
        assert_eq!(slot.status, UTXO_UNSPENT);
        assert_eq!(slot.hash, [0xAA; 32]);
    }

    #[test]
    fn apply_create_idempotent() {
        let engine = make_engine();
        let k = key(11);
        let hashes = vec![[0xBB; 32]; 2];

        let op = ReplicaOp::Create {
            tx_key: k,
            metadata_bytes: vec![],
            utxo_hashes: hashes.clone(),
            cold_data: None,
            is_external: false,
        };
        apply_op(&engine, &op).unwrap();
        apply_op(&engine, &op).unwrap(); // duplicate — should be ok
    }

    #[test]
    fn apply_freeze_unfreeze() {
        let engine = make_engine();
        let k = key(20);
        create_record(&engine, k, 3);

        apply_op(
            &engine,
            &ReplicaOp::Freeze {
                tx_key: k,
                offset: 1,
            },
        )
        .unwrap();
        let slot = engine.read_slot(&k, 1).unwrap();
        assert_eq!(slot.status, UTXO_FROZEN);

        apply_op(
            &engine,
            &ReplicaOp::Unfreeze {
                tx_key: k,
                offset: 1,
            },
        )
        .unwrap();
        let slot = engine.read_slot(&k, 1).unwrap();
        assert_eq!(slot.status, UTXO_UNSPENT);
    }

    #[test]
    fn apply_delete_op() {
        let engine = make_engine();
        let k = key(30);
        create_record(&engine, k, 2);

        apply_op(&engine, &ReplicaOp::Delete { tx_key: k }).unwrap();
        assert!(engine.lookup(&k).is_none());
    }

    #[test]
    fn apply_set_mined() {
        let engine = make_engine();
        let k = key(40);
        create_record(&engine, k, 2);

        apply_op(
            &engine,
            &ReplicaOp::SetMined {
                tx_key: k,
                block_id: 42,
                block_height: 1000,
                subtree_idx: 0,
                on_longest_chain: true,
            },
        )
        .unwrap();

        let meta = engine.read_metadata(&k).unwrap();
        assert_eq!(meta.block_entry_count, 1);
    }

    #[test]
    fn apply_missing_tx_gracefully_skipped() {
        let engine = make_engine();
        let k = key(99);
        // No record created — ops should succeed (skip)
        apply_op(
            &engine,
            &ReplicaOp::Spend {
                tx_key: k,
                offset: 0,
                spending_data: [0; 36],
            },
        )
        .unwrap();
        apply_op(&engine, &ReplicaOp::Delete { tx_key: k }).unwrap();
        apply_op(
            &engine,
            &ReplicaOp::Freeze {
                tx_key: k,
                offset: 0,
            },
        )
        .unwrap();
    }
}
