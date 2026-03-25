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
///
/// When an `ack_state_path` is configured, the highest applied sequence
/// is persisted to disk periodically so the master can resume streaming
/// from the correct position after a replica restart.
pub struct ReplicationReceiver {
    engine: Arc<Engine>,
    last_applied_sequence: Arc<AtomicU64>,
    running: Arc<AtomicBool>,
    /// Path for persisting the last-applied sequence. None in test setups.
    ack_state_path: Option<std::path::PathBuf>,
    /// Counter for amortized persistence (flush every N batches).
    batches_since_flush: Arc<std::sync::atomic::AtomicU32>,
}

/// Number of batches between forced persistence of last_applied_sequence.
const PERSIST_EVERY_N_BATCHES: u32 = 100;

impl ReplicationReceiver {
    /// Create a new receiver backed by the given engine.
    pub fn new(engine: Arc<Engine>) -> Self {
        Self {
            engine,
            last_applied_sequence: Arc::new(AtomicU64::new(0)),
            running: Arc::new(AtomicBool::new(true)),
            ack_state_path: None,
            batches_since_flush: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        }
    }

    /// Create a receiver with persistent ACK state.
    ///
    /// Loads the last-applied sequence from disk if the file exists,
    /// allowing the master to resume streaming from where the replica
    /// left off after a restart.
    pub fn with_ack_state(engine: Arc<Engine>, path: std::path::PathBuf) -> Self {
        let initial_seq = Self::load_sequence(&path).unwrap_or(0);
        Self {
            engine,
            last_applied_sequence: Arc::new(AtomicU64::new(initial_seq)),
            running: Arc::new(AtomicBool::new(true)),
            ack_state_path: Some(path),
            batches_since_flush: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        }
    }

    /// Load the persisted sequence from disk. Format: `[sequence:8 LE]`.
    fn load_sequence(path: &std::path::Path) -> Option<u64> {
        let data = std::fs::read(path).ok()?;
        if data.len() >= 8 {
            Some(u64::from_le_bytes(data[0..8].try_into().unwrap()))
        } else {
            None
        }
    }

    /// Persist the current last-applied sequence to disk.
    fn persist_sequence(path: &std::path::Path, seq: u64) {
        let tmp = path.with_extension("tmp");
        if std::fs::write(&tmp, seq.to_le_bytes()).is_ok() {
            let _ = std::fs::rename(&tmp, path);
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
        let ack_state_path = self.ack_state_path.clone();
        let batches_counter = self.batches_since_flush.clone();

        std::thread::spawn(move || {
            while running.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _addr)) => {
                        let eng = engine.clone();
                        let run = running.clone();
                        let la = last_applied.clone();
                        let asp = ack_state_path.clone();
                        let bc = batches_counter.clone();
                        std::thread::spawn(move || {
                            handle_connection(&eng, stream, &run, &la, asp.as_deref(), &bc);
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
    ///
    /// Persists the final last-applied sequence to disk before returning.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
        // Final flush to ensure we don't lose the latest sequence.
        if let Some(ref path) = self.ack_state_path {
            let seq = self.last_applied_sequence.load(Ordering::Relaxed);
            if seq > 0 {
                Self::persist_sequence(path, seq);
            }
        }
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
    ack_state_path: Option<&std::path::Path>,
    batches_counter: &std::sync::atomic::AtomicU32,
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
            let resp = handle_replica_batch(&request, engine, last_applied);
            // Periodically persist the last-applied sequence to disk.
            if let Some(path) = ack_state_path {
                let count = batches_counter.fetch_add(1, Ordering::Relaxed) + 1;
                if count >= PERSIST_EVERY_N_BATCHES {
                    batches_counter.store(0, Ordering::Relaxed);
                    let seq = last_applied.load(Ordering::Relaxed);
                    if seq > 0 {
                        ReplicationReceiver::persist_sequence(path, seq);
                    }
                }
            }
            resp
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
    // Pre-apply generation guard: reject stale ops BEFORE mutating state.
    // An op is stale if its master_generation is strictly less than the
    // record's current generation — a newer mutation has already been applied.
    // Equal-generation replays are allowed through since all mutation ops
    // are idempotent and the generation sync at the end is a no-op.
    // Ops without master_generation (Create, Delete, PruneSlot) skip this
    // check; they rely on idempotency in their match arms instead.
    if let Some(master_gen) = op.master_generation() {
        let tx_key = op.tx_key();
        if let Ok(meta) = engine.read_metadata(&tx_key) {
            let local_gen = { meta.generation };
            if master_gen < local_gen {
                return Ok(()); // Stale op — already superseded by a newer mutation
            }
        }
        // If read_metadata fails (TxNotFound), the record may not exist yet
        // or was deleted. Let the match arm handle it gracefully.
    }

    match op {
        ReplicaOp::Spend {
            tx_key,
            offset,
            spending_data,
            ..
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
        ReplicaOp::Unspend { tx_key, offset, .. } => {
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
            ..
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
        ReplicaOp::UnsetMined { tx_key, block_id, .. } => {
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
        ReplicaOp::Freeze { tx_key, offset, .. } => {
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
                Ok(_) => Ok(()),
                Err(crate::ops::error::SpendError::AlreadyFrozen { .. }) => Ok(()),
                Err(crate::ops::error::SpendError::AlreadySpent { .. }) => Ok(()),
                Err(crate::ops::error::SpendError::TxNotFound) => Ok(()),
                Err(e) => Err(format!("freeze: {e}")),
            }
        }
        ReplicaOp::Unfreeze { tx_key, offset, .. } => {
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
                Ok(_) => Ok(()),
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
            ..
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
                Ok(_) => Ok(()),
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
            ..
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
        ReplicaOp::SetLocked { tx_key, value, .. } => {
            let req = SetLockedRequest {
                tx_key: *tx_key,
                value: *value,
            };
            match engine.set_locked(&req) {
                Ok(_) => Ok(()),
                Err(crate::ops::error::SpendError::TxNotFound) => Ok(()),
                Err(e) => Err(format!("set_locked: {e}")),
            }
        }
        ReplicaOp::PreserveUntil {
            tx_key,
            block_height,
            ..
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

            // Extract frozen/conflicting/locked from the wire flags byte
            // at offset 45 (locked=0x01, conflicting=0x02, frozen=0x04).
            let (frozen, conflicting, locked) = if metadata_bytes.len() >= 46 {
                let wire_flags = metadata_bytes[45];
                (wire_flags & 0x04 != 0, wire_flags & 0x02 != 0, wire_flags & 0x01 != 0)
            } else {
                (false, false, false)
            };

            // Parse extended fields at offset 70+: block_height(4) +
            // block_count(1) + [block_id(4)+block_height(4)+subtree_idx(4)]*N +
            // parent_txid_count(2) + parent_txids(32*N).
            let mut block_height = 0u32;
            let mut mined_block_infos = Vec::new();
            let mut parent_txids: Vec<[u8; 32]> = Vec::new();
            if metadata_bytes.len() >= 75 {
                let m = metadata_bytes.as_slice();
                block_height = u32::from_le_bytes(m[70..74].try_into().unwrap());
                let block_count = m[74] as usize;
                let mut pos = 75;
                for _ in 0..block_count {
                    if pos + 12 > m.len() { break; }
                    mined_block_infos.push(crate::ops::create::MinedBlockInfo {
                        block_id: u32::from_le_bytes(m[pos..pos + 4].try_into().unwrap()),
                        block_height: u32::from_le_bytes(m[pos + 4..pos + 8].try_into().unwrap()),
                        subtree_idx: u32::from_le_bytes(m[pos + 8..pos + 12].try_into().unwrap()),
                    });
                    pos += 12;
                }
                if pos + 2 <= m.len() {
                    let ptx_count = u16::from_le_bytes(m[pos..pos + 2].try_into().unwrap()) as usize;
                    pos += 2;
                    for _ in 0..ptx_count {
                        if pos + 32 > m.len() { break; }
                        let mut ptx = [0u8; 32];
                        ptx.copy_from_slice(&m[pos..pos + 32]);
                        parent_txids.push(ptx);
                        pos += 32;
                    }
                }
            }

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
                block_height,
                mined_block_infos,
                frozen,
                conflicting,
                locked,
                parent_txids,
            };
            match engine.create(&create_req) {
                Ok(_) | Err(CreateError::DuplicateTxId) => {
                    // Apply extended lifecycle metadata if present.
                    // Layout after the core 46 bytes: generation(4) +
                    // updated_at(8) + unmined_since(4) + delete_at_height(4) +
                    // preserve_until(4) = 24 bytes (total 70).
                    if metadata_bytes.len() >= 70
                        && let Ok(mut meta) = engine.read_metadata(tx_key)
                    {
                        let m = metadata_bytes.as_slice();
                        meta.generation = u32::from_le_bytes(m[46..50].try_into().unwrap());
                        meta.updated_at = u64::from_le_bytes(m[50..58].try_into().unwrap());
                        meta.unmined_since = u32::from_le_bytes(m[58..62].try_into().unwrap());
                        meta.delete_at_height = u32::from_le_bytes(m[62..66].try_into().unwrap());
                        meta.preserve_until = u32::from_le_bytes(m[66..70].try_into().unwrap());
                        if let Some(entry) = engine.lookup(tx_key) {
                            let _ = crate::io::write_metadata(
                                engine.device(),
                                entry.record_offset,
                                &meta,
                            );
                        }
                    }

                    // Store cold data in the blobstore if provided.
                    // Blob persistence is part of the durability contract:
                    // failing to store cold data must fail the ACK so the
                    // master knows this replica is not a complete copy.
                    if let Some(data) = cold_data
                        && !data.is_empty()
                        && let Some(bs) = engine.blob_store()
                        && let Err(e) = bs.put(&tx_key.txid, data)
                    {
                        return Err(format!("cold data write failed for {:?}: {e}", tx_key));
                    }
                    Ok(())
                }
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
    }?;

    // After applying the mutation, sync the record's generation counter
    // to the master's value. The engine auto-increments generation on
    // every mutation, but the replica must use the master's generation
    // so both sides agree. The pre-apply guard above already rejected
    // stale ops (master_gen <= local_gen), so here we unconditionally
    // set the generation to the master's value.
    if let Some(master_gen) = op.master_generation() {
        let tx_key = op.tx_key();
        if let Ok(mut meta) = engine.read_metadata(&tx_key)
            && let Some(entry) = engine.lookup(&tx_key)
        {
            meta.generation = master_gen;
            let _ = crate::io::write_metadata(engine.device(), entry.record_offset, &meta);
        }
    }

    Ok(())
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
            master_generation: 0,
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
            master_generation: 0,
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
                master_generation: 0,
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
                master_generation: 0,
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
                master_generation: 0,
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
                master_generation: 0,
            },
        )
        .unwrap();
        apply_op(&engine, &ReplicaOp::Delete { tx_key: k }).unwrap();
        apply_op(
            &engine,
            &ReplicaOp::Freeze {
                tx_key: k,
                offset: 0,
                master_generation: 0,
            },
        )
        .unwrap();
    }

    #[test]
    fn apply_stale_spend_skipped() {
        let engine = make_engine();
        let k = key(100);
        create_record(&engine, k, 3);

        // Apply a spend with master_generation=2 to advance the record's gen.
        let op1 = ReplicaOp::Spend {
            tx_key: k,
            offset: 0,
            spending_data: [0xAA; 36],
            master_generation: 2,
        };
        apply_op(&engine, &op1).unwrap();
        let cur_gen = { engine.read_metadata(&k).unwrap().generation };
        assert_eq!(cur_gen, 2);

        // Now send a stale spend (master_gen=1 <= local_gen=2) on slot 1.
        // The pre-apply guard should skip it entirely.
        let op2 = ReplicaOp::Spend {
            tx_key: k,
            offset: 1,
            spending_data: [0xBB; 36],
            master_generation: 1,
        };
        apply_op(&engine, &op2).unwrap();
        // Slot 1 should still be UNSPENT because the stale op was rejected.
        let slot = engine.read_slot(&k, 1).unwrap();
        assert_eq!(slot.status, UTXO_UNSPENT);
    }

    #[test]
    fn apply_fresh_spend_applies() {
        let engine = make_engine();
        let k = key(101);
        create_record(&engine, k, 3);

        // Fresh op: master_gen=1 > local_gen=0.
        let op = ReplicaOp::Spend {
            tx_key: k,
            offset: 0,
            spending_data: [0xCC; 36],
            master_generation: 1,
        };
        apply_op(&engine, &op).unwrap();

        let slot = engine.read_slot(&k, 0).unwrap();
        assert_eq!(slot.status, UTXO_SPENT);
        let cur_gen = { engine.read_metadata(&k).unwrap().generation };
        assert_eq!(cur_gen, 1);
    }

    #[test]
    fn apply_equal_generation_idempotent() {
        // Equal-generation replays are allowed through because all ops are
        // idempotent. The guard only rejects strictly-lower generations.
        let engine = make_engine();
        let k = key(102);
        create_record(&engine, k, 3);

        // Advance to gen=2 with a freeze.
        let op1 = ReplicaOp::Freeze {
            tx_key: k,
            offset: 0,
            master_generation: 2,
        };
        apply_op(&engine, &op1).unwrap();
        let cur_gen = { engine.read_metadata(&k).unwrap().generation };
        assert_eq!(cur_gen, 2);

        // Replay the same freeze (master_gen=2 == local_gen=2) — allowed,
        // handled idempotently by the engine (AlreadyFrozen → Ok(())).
        apply_op(&engine, &op1).unwrap();
        let slot = engine.read_slot(&k, 0).unwrap();
        assert_eq!(slot.status, UTXO_FROZEN);
    }

    #[test]
    fn apply_stale_freeze_skipped() {
        let engine = make_engine();
        let k = key(103);
        create_record(&engine, k, 3);

        // Advance to gen=5 via a spend.
        let op1 = ReplicaOp::Spend {
            tx_key: k,
            offset: 0,
            spending_data: [0xEE; 36],
            master_generation: 5,
        };
        apply_op(&engine, &op1).unwrap();

        // Stale freeze (gen=3 <= 5) on slot 1 should be rejected.
        let op2 = ReplicaOp::Freeze {
            tx_key: k,
            offset: 1,
            master_generation: 3,
        };
        apply_op(&engine, &op2).unwrap();
        let slot = engine.read_slot(&k, 1).unwrap();
        assert_eq!(slot.status, UTXO_UNSPENT);
    }

    /// Build a full metadata buffer matching the extended wire format used by
    /// the live create path: core(46) + lifecycle(24) + extended fields.
    fn build_full_metadata(
        tx_version: u32,
        is_coinbase: bool,
        wire_flags: u8,
        generation: u32,
        block_height: u32,
        block_infos: &[(u32, u32, u32)],   // (block_id, block_height, subtree_idx)
        parent_txids: &[[u8; 32]],
    ) -> Vec<u8> {
        let mut buf = Vec::with_capacity(128);
        // Core 46 bytes
        buf.extend_from_slice(&tx_version.to_le_bytes()); // tx_version
        buf.extend_from_slice(&0u32.to_le_bytes());       // locktime
        buf.extend_from_slice(&0u64.to_le_bytes());       // fee
        buf.extend_from_slice(&0u64.to_le_bytes());       // size_in_bytes
        buf.extend_from_slice(&0u64.to_le_bytes());       // extended_size
        buf.push(if is_coinbase { 1 } else { 0 });        // is_coinbase
        buf.extend_from_slice(&0u32.to_le_bytes());       // spending_height
        buf.extend_from_slice(&0u64.to_le_bytes());       // created_at
        buf.push(wire_flags);                              // flags
        // Lifecycle 24 bytes
        buf.extend_from_slice(&generation.to_le_bytes());  // generation
        buf.extend_from_slice(&0u64.to_le_bytes());        // updated_at
        buf.extend_from_slice(&0u32.to_le_bytes());        // unmined_since
        buf.extend_from_slice(&0u32.to_le_bytes());        // delete_at_height
        buf.extend_from_slice(&0u32.to_le_bytes());        // preserve_until
        // Extended: block_height + block_infos + parent_txids
        buf.extend_from_slice(&block_height.to_le_bytes());
        buf.push(block_infos.len() as u8);
        for (bid, bht, bsi) in block_infos {
            buf.extend_from_slice(&bid.to_le_bytes());
            buf.extend_from_slice(&bht.to_le_bytes());
            buf.extend_from_slice(&bsi.to_le_bytes());
        }
        buf.extend_from_slice(&(parent_txids.len() as u16).to_le_bytes());
        for ptx in parent_txids {
            buf.extend_from_slice(ptx);
        }
        buf
    }

    #[test]
    fn create_replication_full_state() {
        let engine = make_engine();
        let k = key(110);
        let hashes = vec![[0xAA; 32]; 3];

        // Build metadata with mined_block_info, frozen flag, and parent_txids.
        let parent = [0xBBu8; 32];
        let meta_bytes = build_full_metadata(
            2,           // tx_version
            false,       // is_coinbase
            0x04,        // frozen=0x04
            5,           // generation
            1000,        // block_height
            &[(42, 1000, 7)],  // one block entry
            &[parent],         // one parent_txid
        );

        let op = ReplicaOp::Create {
            tx_key: k,
            metadata_bytes: meta_bytes,
            utxo_hashes: hashes,
            cold_data: None,
            is_external: false,
        };
        apply_op(&engine, &op).unwrap();

        // Verify the record was created.
        let slot = engine.read_slot(&k, 0).unwrap();
        // Frozen flag should have been applied via CreateRequest.frozen = true
        assert_eq!(slot.status, UTXO_FROZEN);

        // Verify lifecycle metadata was applied.
        let meta = engine.read_metadata(&k).unwrap();
        let restored_gen = { meta.generation };
        assert_eq!(restored_gen, 5);

        // Verify block entry was applied.
        let block_count = { meta.block_entry_count };
        assert_eq!(block_count, 1);
        let be_id = { meta.block_entries_inline[0].block_id };
        assert_eq!(be_id, 42);
    }

    #[test]
    fn create_replication_46byte_compat() {
        // Old-format 46-byte payload should still work with defaults.
        let engine = make_engine();
        let k = key(111);
        let hashes = vec![[0xCC; 32]; 2];

        let mut meta_bytes = Vec::with_capacity(46);
        meta_bytes.extend_from_slice(&1u32.to_le_bytes()); // tx_version
        meta_bytes.extend_from_slice(&0u32.to_le_bytes()); // locktime
        meta_bytes.extend_from_slice(&100u64.to_le_bytes()); // fee
        meta_bytes.extend_from_slice(&200u64.to_le_bytes()); // size_in_bytes
        meta_bytes.extend_from_slice(&0u64.to_le_bytes()); // extended_size
        meta_bytes.push(0); // is_coinbase
        meta_bytes.extend_from_slice(&0u32.to_le_bytes()); // spending_height
        meta_bytes.extend_from_slice(&0u64.to_le_bytes()); // created_at
        meta_bytes.push(0); // flags

        let op = ReplicaOp::Create {
            tx_key: k,
            metadata_bytes: meta_bytes,
            utxo_hashes: hashes,
            cold_data: None,
            is_external: false,
        };
        apply_op(&engine, &op).unwrap();

        let slot = engine.read_slot(&k, 0).unwrap();
        assert_eq!(slot.status, UTXO_UNSPENT);
        let meta = engine.read_metadata(&k).unwrap();
        let block_count = { meta.block_entry_count };
        assert_eq!(block_count, 0); // No block entries in 46-byte format
    }

    #[test]
    fn create_replication_lifecycle_fields() {
        let engine = make_engine();
        let k = key(112);
        let hashes = vec![[0xDD; 32]; 1];

        let mut meta_bytes = build_full_metadata(
            1, false, 0, 10, 0, &[], &[],
        );
        // Patch lifecycle fields: set delete_at_height=500 and preserve_until=700
        // Offsets: generation(46-49), updated_at(50-57), unmined_since(58-61),
        //          delete_at_height(62-65), preserve_until(66-69)
        meta_bytes[62..66].copy_from_slice(&500u32.to_le_bytes());
        meta_bytes[66..70].copy_from_slice(&700u32.to_le_bytes());

        let op = ReplicaOp::Create {
            tx_key: k,
            metadata_bytes: meta_bytes,
            utxo_hashes: hashes,
            cold_data: None,
            is_external: false,
        };
        apply_op(&engine, &op).unwrap();

        let meta = engine.read_metadata(&k).unwrap();
        let restored_gen = { meta.generation };
        let restored_dah = { meta.delete_at_height };
        let restored_pu = { meta.preserve_until };
        assert_eq!(restored_gen, 10);
        assert_eq!(restored_dah, 500);
        assert_eq!(restored_pu, 700);
    }
}
