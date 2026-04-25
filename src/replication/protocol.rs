//! Wire-format types for operation-based replication.
//!
//! Compact binary serialization — a Spend op is under 80 bytes on the wire.
//!
//! # Protocol versioning
//!
//! The `ReplicaBatch` frame on the wire starts with a 1-byte version tag
//! so receivers can tolerate older senders during a rolling upgrade.
//!
//! * [`BATCH_PROTOCOL_V1`] — legacy layout (no trace context).
//! * [`BATCH_PROTOCOL_V2`] — legacy layout with a fixed 24-byte trace context
//!   trailer immediately after `[first_sequence:8][count:4]`. See
//!   [`crate::observability::WireTraceContext`] for the byte layout.
//!   When the caller has no active/sampled span, the 24 bytes are zero
//!   and the receiver treats the context as absent.
//! * [`BATCH_PROTOCOL_V3`] — **current**. Adds `[source_node_id:8]` after
//!   the trace context so receiver-side high-water marks can survive TCP
//!   reconnects and process restarts by source node identity.
//!
//! The version byte is new in V2. V1 frames never included one, so
//! receivers only interpret the leading byte as a version tag when its
//! value equals a known version; any other value is treated as a V1
//! frame whose first byte is the low byte of `first_sequence`.

use crate::index::TxKey;
use crate::observability::WireTraceContext;
use thiserror::Error;

/// Legacy batch wire layout (no trace context). Deprecated; kept for
/// backward-compatible receiver reads only — this crate never produces V1.
pub const BATCH_PROTOCOL_V1: u8 = 1;

/// Legacy batch wire layout with a 24-byte W3C trace context trailer.
pub const BATCH_PROTOCOL_V2: u8 = 2;

/// Current batch wire layout with trace context and stable source node id.
pub const BATCH_PROTOCOL_V3: u8 = 3;

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("buffer too short: need {need}, have {have}")]
    BufferTooShort { need: usize, have: usize },
    #[error("unknown op type: {0}")]
    UnknownOp(u8),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, ProtocolError>;

/// Convert persisted transaction flags into the create-replication metadata
/// layout consumed by the replica receiver.
///
/// Offset 32 in create metadata is the standalone `is_coinbase` boolean.
/// Offset 45 is the client create flags byte (locked=0x01,
/// conflicting=0x02, frozen=0x04, external=0x08). Frozen is a per-slot state
/// during migration replay, so this helper never sets it.
pub fn create_metadata_flag_bytes(flags: crate::record::TxFlags) -> (u8, u8) {
    let is_coinbase = u8::from(flags.contains(crate::record::TxFlags::IS_COINBASE));
    let mut wire_flags = 0u8;
    if flags.contains(crate::record::TxFlags::LOCKED) {
        wire_flags |= 0x01;
    }
    if flags.contains(crate::record::TxFlags::CONFLICTING) {
        wire_flags |= 0x02;
    }
    if flags.contains(crate::record::TxFlags::EXTERNAL) {
        wire_flags |= crate::protocol::opcodes::FLAG_EXTERNAL_BLOB;
    }
    (is_coinbase, wire_flags)
}

// -- Op type tags --
const OP_SPEND: u8 = 1;
const OP_UNSPEND: u8 = 2;
const OP_SET_MINED: u8 = 3;
const OP_UNSET_MINED: u8 = 4;
const OP_FREEZE: u8 = 5;
const OP_UNFREEZE: u8 = 6;
const OP_REASSIGN: u8 = 7;
const OP_SET_CONFLICTING: u8 = 8;
const OP_SET_LOCKED: u8 = 9;
const OP_PRESERVE_UNTIL: u8 = 10;
const OP_CREATE: u8 = 11;
const OP_DELETE: u8 = 12;
const OP_PRUNE_SLOT: u8 = 13;
const _OP_MARK_LONGEST_CHAIN: u8 = 14;

/// A single replication operation sent from master to replica.
/// A mutation operation to be replicated from master to replica.
///
/// Every mutation variant carries `master_generation` — the record's
/// generation counter on the master AFTER the mutation was applied.
/// The replica uses this to:
/// - Set the generation to the master's value instead of auto-incrementing
/// - Detect stale/out-of-order ops (master_generation <= local generation)
#[derive(Debug, Clone, PartialEq)]
pub enum ReplicaOp {
    Spend {
        tx_key: TxKey,
        offset: u32,
        spending_data: [u8; 36],
        master_generation: u32,
    },
    Unspend {
        tx_key: TxKey,
        offset: u32,
        master_generation: u32,
    },
    SetMined {
        tx_key: TxKey,
        block_id: u32,
        block_height: u32,
        subtree_idx: u32,
        on_longest_chain: bool,
        master_generation: u32,
    },
    UnsetMined {
        tx_key: TxKey,
        block_id: u32,
        master_generation: u32,
    },
    Freeze {
        tx_key: TxKey,
        offset: u32,
        master_generation: u32,
    },
    Unfreeze {
        tx_key: TxKey,
        offset: u32,
        master_generation: u32,
    },
    Reassign {
        tx_key: TxKey,
        offset: u32,
        new_hash: [u8; 32],
        block_height: u32,
        spendable_after: u32,
        master_generation: u32,
    },
    SetConflicting {
        tx_key: TxKey,
        value: bool,
        current_block_height: u32,
        retention: u32,
        master_generation: u32,
    },
    SetLocked {
        tx_key: TxKey,
        value: bool,
        master_generation: u32,
    },
    PreserveUntil {
        tx_key: TxKey,
        block_height: u32,
        master_generation: u32,
    },
    Create {
        tx_key: TxKey,
        metadata_bytes: Vec<u8>,
        utxo_hashes: Vec<[u8; 32]>,
        cold_data: Option<Vec<u8>>,
        is_external: bool,
    },
    Delete {
        tx_key: TxKey,
    },
    PruneSlot {
        tx_key: TxKey,
        offset: u32,
    },
}

impl ReplicaOp {
    /// Extract the transaction key from any op variant.
    pub fn tx_key(&self) -> TxKey {
        match self {
            Self::Spend { tx_key, .. }
            | Self::Unspend { tx_key, .. }
            | Self::SetMined { tx_key, .. }
            | Self::UnsetMined { tx_key, .. }
            | Self::Freeze { tx_key, .. }
            | Self::Unfreeze { tx_key, .. }
            | Self::Reassign { tx_key, .. }
            | Self::SetConflicting { tx_key, .. }
            | Self::SetLocked { tx_key, .. }
            | Self::PreserveUntil { tx_key, .. }
            | Self::Create { tx_key, .. }
            | Self::Delete { tx_key, .. }
            | Self::PruneSlot { tx_key, .. } => *tx_key,
        }
    }

    /// Extract the master generation from a mutation op, if present.
    ///
    /// Create, Delete, and PruneSlot don't carry generation because
    /// Create sets it via metadata_bytes and Delete/PruneSlot remove data.
    pub fn master_generation(&self) -> Option<u32> {
        match self {
            Self::Spend {
                master_generation, ..
            }
            | Self::Unspend {
                master_generation, ..
            }
            | Self::SetMined {
                master_generation, ..
            }
            | Self::UnsetMined {
                master_generation, ..
            }
            | Self::Freeze {
                master_generation, ..
            }
            | Self::Unfreeze {
                master_generation, ..
            }
            | Self::Reassign {
                master_generation, ..
            }
            | Self::SetConflicting {
                master_generation, ..
            }
            | Self::SetLocked {
                master_generation, ..
            }
            | Self::PreserveUntil {
                master_generation, ..
            } => Some(*master_generation),
            Self::Create { .. } | Self::Delete { .. } | Self::PruneSlot { .. } => None,
        }
    }
}

impl ReplicaOp {
    /// Serialize this op to bytes. Returns the serialized form.
    ///
    /// Mutation ops append `master_generation` (4 bytes LE) after all
    /// other fields. Create/Delete/PruneSlot do not carry generation.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(80);
        match self {
            ReplicaOp::Spend {
                tx_key,
                offset,
                spending_data,
                master_generation,
            } => {
                buf.push(OP_SPEND);
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&offset.to_le_bytes());
                buf.extend_from_slice(spending_data);
                buf.extend_from_slice(&master_generation.to_le_bytes());
            }
            ReplicaOp::Unspend {
                tx_key,
                offset,
                master_generation,
            } => {
                buf.push(OP_UNSPEND);
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&offset.to_le_bytes());
                buf.extend_from_slice(&master_generation.to_le_bytes());
            }
            ReplicaOp::SetMined {
                tx_key,
                block_id,
                block_height,
                subtree_idx,
                on_longest_chain,
                master_generation,
            } => {
                buf.push(OP_SET_MINED);
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&block_id.to_le_bytes());
                buf.extend_from_slice(&block_height.to_le_bytes());
                buf.extend_from_slice(&subtree_idx.to_le_bytes());
                buf.push(u8::from(*on_longest_chain));
                buf.extend_from_slice(&master_generation.to_le_bytes());
            }
            ReplicaOp::UnsetMined {
                tx_key,
                block_id,
                master_generation,
            } => {
                buf.push(OP_UNSET_MINED);
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&block_id.to_le_bytes());
                buf.extend_from_slice(&master_generation.to_le_bytes());
            }
            ReplicaOp::Freeze {
                tx_key,
                offset,
                master_generation,
            } => {
                buf.push(OP_FREEZE);
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&offset.to_le_bytes());
                buf.extend_from_slice(&master_generation.to_le_bytes());
            }
            ReplicaOp::Unfreeze {
                tx_key,
                offset,
                master_generation,
            } => {
                buf.push(OP_UNFREEZE);
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&offset.to_le_bytes());
                buf.extend_from_slice(&master_generation.to_le_bytes());
            }
            ReplicaOp::Reassign {
                tx_key,
                offset,
                new_hash,
                block_height,
                spendable_after,
                master_generation,
            } => {
                buf.push(OP_REASSIGN);
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&offset.to_le_bytes());
                buf.extend_from_slice(new_hash);
                buf.extend_from_slice(&block_height.to_le_bytes());
                buf.extend_from_slice(&spendable_after.to_le_bytes());
                buf.extend_from_slice(&master_generation.to_le_bytes());
            }
            ReplicaOp::SetConflicting {
                tx_key,
                value,
                current_block_height,
                retention,
                master_generation,
            } => {
                buf.push(OP_SET_CONFLICTING);
                buf.extend_from_slice(&tx_key.txid);
                buf.push(u8::from(*value));
                buf.extend_from_slice(&current_block_height.to_le_bytes());
                buf.extend_from_slice(&retention.to_le_bytes());
                buf.extend_from_slice(&master_generation.to_le_bytes());
            }
            ReplicaOp::SetLocked {
                tx_key,
                value,
                master_generation,
            } => {
                buf.push(OP_SET_LOCKED);
                buf.extend_from_slice(&tx_key.txid);
                buf.push(u8::from(*value));
                buf.extend_from_slice(&master_generation.to_le_bytes());
            }
            ReplicaOp::PreserveUntil {
                tx_key,
                block_height,
                master_generation,
            } => {
                buf.push(OP_PRESERVE_UNTIL);
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&block_height.to_le_bytes());
                buf.extend_from_slice(&master_generation.to_le_bytes());
            }
            ReplicaOp::Create {
                tx_key,
                metadata_bytes,
                utxo_hashes,
                cold_data,
                is_external,
            } => {
                buf.push(OP_CREATE);
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&(metadata_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(metadata_bytes);
                buf.extend_from_slice(&(utxo_hashes.len() as u32).to_le_bytes());
                for h in utxo_hashes {
                    buf.extend_from_slice(h);
                }
                match cold_data {
                    Some(cd) => {
                        buf.extend_from_slice(&(cd.len() as u32).to_le_bytes());
                        buf.extend_from_slice(cd);
                    }
                    None => buf.extend_from_slice(&0u32.to_le_bytes()),
                }
                buf.push(if *is_external { 1 } else { 0 });
            }
            ReplicaOp::Delete { tx_key } => {
                buf.push(OP_DELETE);
                buf.extend_from_slice(&tx_key.txid);
            }
            ReplicaOp::PruneSlot { tx_key, offset } => {
                buf.push(OP_PRUNE_SLOT);
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&offset.to_le_bytes());
            }
        }
        buf
    }

    /// Deserialize from bytes. Returns (op, bytes_consumed).
    pub fn deserialize(data: &[u8]) -> Result<(Self, usize)> {
        if data.is_empty() {
            return Err(ProtocolError::BufferTooShort { need: 1, have: 0 });
        }
        let op_type = data[0];
        let rest = &data[1..];

        match op_type {
            OP_SPEND => {
                need(rest, 76)?; // 32 + 4 + 36 + 4(gen)
                let key = read_key(rest);
                let offset = u32::from_le_bytes(rest[32..36].try_into().unwrap());
                let mut sd = [0u8; 36];
                sd.copy_from_slice(&rest[36..72]);
                let master_generation = r_u32(rest, 72);
                Ok((
                    ReplicaOp::Spend {
                        tx_key: key,
                        offset,
                        spending_data: sd,
                        master_generation,
                    },
                    77,
                ))
            }
            OP_UNSPEND => {
                need(rest, 40)?; // 32 + 4 + 4(gen)
                Ok((
                    ReplicaOp::Unspend {
                        tx_key: read_key(rest),
                        offset: r_u32(rest, 32),
                        master_generation: r_u32(rest, 36),
                    },
                    41,
                ))
            }
            OP_SET_MINED => {
                need(rest, 49)?; // 32 + 4 + 4 + 4 + 1 + 4(gen)
                Ok((
                    ReplicaOp::SetMined {
                        tx_key: read_key(rest),
                        block_id: r_u32(rest, 32),
                        block_height: r_u32(rest, 36),
                        subtree_idx: r_u32(rest, 40),
                        on_longest_chain: rest[44] != 0,
                        master_generation: r_u32(rest, 45),
                    },
                    50,
                ))
            }
            OP_UNSET_MINED => {
                need(rest, 40)?; // 32 + 4 + 4(gen)
                Ok((
                    ReplicaOp::UnsetMined {
                        tx_key: read_key(rest),
                        block_id: r_u32(rest, 32),
                        master_generation: r_u32(rest, 36),
                    },
                    41,
                ))
            }
            OP_FREEZE => {
                need(rest, 40)?; // 32 + 4 + 4(gen)
                Ok((
                    ReplicaOp::Freeze {
                        tx_key: read_key(rest),
                        offset: r_u32(rest, 32),
                        master_generation: r_u32(rest, 36),
                    },
                    41,
                ))
            }
            OP_UNFREEZE => {
                need(rest, 40)?; // 32 + 4 + 4(gen)
                Ok((
                    ReplicaOp::Unfreeze {
                        tx_key: read_key(rest),
                        offset: r_u32(rest, 32),
                        master_generation: r_u32(rest, 36),
                    },
                    41,
                ))
            }
            OP_REASSIGN => {
                need(rest, 80)?; // 32 + 4 + 32 + 4 + 4 + 4(gen)
                let mut nh = [0u8; 32];
                nh.copy_from_slice(&rest[36..68]);
                Ok((
                    ReplicaOp::Reassign {
                        tx_key: read_key(rest),
                        offset: r_u32(rest, 32),
                        new_hash: nh,
                        block_height: r_u32(rest, 68),
                        spendable_after: r_u32(rest, 72),
                        master_generation: r_u32(rest, 76),
                    },
                    81,
                ))
            }
            OP_SET_CONFLICTING => {
                need(rest, 45)?; // 32 + 1 + 4 + 4 + 4(gen)
                Ok((
                    ReplicaOp::SetConflicting {
                        tx_key: read_key(rest),
                        value: rest[32] != 0,
                        current_block_height: r_u32(rest, 33),
                        retention: r_u32(rest, 37),
                        master_generation: r_u32(rest, 41),
                    },
                    46,
                ))
            }
            OP_SET_LOCKED => {
                need(rest, 37)?; // 32 + 1 + 4(gen)
                Ok((
                    ReplicaOp::SetLocked {
                        tx_key: read_key(rest),
                        value: rest[32] != 0,
                        master_generation: r_u32(rest, 33),
                    },
                    38,
                ))
            }
            OP_PRESERVE_UNTIL => {
                need(rest, 40)?; // 32 + 4 + 4(gen)
                Ok((
                    ReplicaOp::PreserveUntil {
                        tx_key: read_key(rest),
                        block_height: r_u32(rest, 32),
                        master_generation: r_u32(rest, 36),
                    },
                    41,
                ))
            }
            OP_CREATE => {
                need(rest, 36)?; // key + meta_len
                let key = read_key(rest);
                let meta_len = r_u32(rest, 32) as usize;
                let mut pos = 36;
                need(rest, pos + meta_len)?;
                let metadata_bytes = rest[pos..pos + meta_len].to_vec();
                pos += meta_len;
                need(rest, pos + 4)?;
                let hash_count = r_u32(rest, pos) as usize;
                pos += 4;
                need(rest, pos + hash_count * 32)?;
                let mut utxo_hashes = Vec::with_capacity(hash_count);
                for _ in 0..hash_count {
                    let mut h = [0u8; 32];
                    h.copy_from_slice(&rest[pos..pos + 32]);
                    utxo_hashes.push(h);
                    pos += 32;
                }
                need(rest, pos + 4)?;
                let cold_len = r_u32(rest, pos) as usize;
                pos += 4;
                let cold_data = if cold_len > 0 {
                    need(rest, pos + cold_len)?;
                    let cd = rest[pos..pos + cold_len].to_vec();
                    pos += cold_len;
                    Some(cd)
                } else {
                    None
                };
                // Backward-compatible: if there is a byte remaining, read
                // is_external; otherwise default to false so old replication
                // streams still work.
                let is_external = if pos < rest.len() {
                    let v = rest[pos] != 0;
                    pos += 1;
                    v
                } else {
                    false
                };
                Ok((
                    ReplicaOp::Create {
                        tx_key: key,
                        metadata_bytes,
                        utxo_hashes,
                        cold_data,
                        is_external,
                    },
                    1 + pos,
                ))
            }
            OP_DELETE => {
                need(rest, 32)?;
                Ok((
                    ReplicaOp::Delete {
                        tx_key: read_key(rest),
                    },
                    33,
                ))
            }
            OP_PRUNE_SLOT => {
                need(rest, 36)?;
                Ok((
                    ReplicaOp::PruneSlot {
                        tx_key: read_key(rest),
                        offset: r_u32(rest, 32),
                    },
                    37,
                ))
            }
            _ => Err(ProtocolError::UnknownOp(op_type)),
        }
    }
}

fn need(data: &[u8], n: usize) -> Result<()> {
    if data.len() < n {
        Err(ProtocolError::BufferTooShort {
            need: n,
            have: data.len(),
        })
    } else {
        Ok(())
    }
}

fn read_key(data: &[u8]) -> TxKey {
    let mut txid = [0u8; 32];
    txid.copy_from_slice(&data[..32]);
    TxKey { txid }
}

fn r_u32(data: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap())
}

// ---------------------------------------------------------------------------
// Batch / Ack types
// ---------------------------------------------------------------------------

/// A batch of operations with contiguous sequence numbers.
///
/// Since protocol V2 (see [`BATCH_PROTOCOL_V2`]) each batch carries an
/// optional W3C trace context (`trace_id`, `span_id`) so replicas can
/// stitch their `handle_replica_batch` span into the sender's trace.
/// When `trace_ctx` is `None` the on-wire bytes are zero and receivers
/// treat the absence as "start a new root span."
#[derive(Debug, Clone, PartialEq)]
pub struct ReplicaBatch {
    /// Sequence number of the first op.
    pub first_sequence: u64,
    /// Operations in order.
    pub ops: Vec<ReplicaOp>,
    /// Optional W3C trace context propagated from the sender's current
    /// span. `None` when the sender had no active/sampled span.
    pub trace_ctx: Option<WireTraceContext>,
    /// Stable sender node id used to key receiver-side idempotency state.
    ///
    /// `None` is kept for legacy frames and non-clustered tests.
    pub source_node_id: Option<u64>,
}

/// Acknowledgment from a replica.
#[derive(Debug, Clone, PartialEq)]
pub enum ReplicaAck {
    /// All ops through this sequence have been applied.
    Ok { through_sequence: u64 },
    /// An error occurred at the given sequence.
    Error {
        failed_sequence: u64,
        message: String,
    },
}

/// Sent by a replica to request catchup from the master.
#[derive(Debug, Clone, PartialEq)]
pub struct CatchupRequest {
    /// Highest sequence the replica has durably applied.
    pub last_ack_sequence: u64,
}

impl CatchupRequest {
    /// Serialize to bytes: `[last_ack_sequence:8]`.
    pub fn serialize(&self) -> Vec<u8> {
        self.last_ack_sequence.to_le_bytes().to_vec()
    }

    /// Deserialize from bytes.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        need(data, 8)?;
        Ok(Self {
            last_ack_sequence: u64::from_le_bytes(data[0..8].try_into().unwrap()),
        })
    }
}

impl ReplicaBatch {
    /// Serialize to bytes.
    ///
    /// Layout (protocol V3):
    /// `[version:1][first_seq:8][count:4][trace_id:16][span_id:8][source_node_id:8][op0][op1]...`
    ///
    /// When `trace_ctx` is `None`, the 24 trace-context bytes are zero.
    /// When `source_node_id` is `None`, the source field is zero.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::HEADER_SIZE + self.ops.len() * 64);
        buf.push(BATCH_PROTOCOL_V3);
        buf.extend_from_slice(&self.first_sequence.to_le_bytes());
        buf.extend_from_slice(&(self.ops.len() as u32).to_le_bytes());
        let mut tc = [0u8; WireTraceContext::SIZE];
        if let Some(ctx) = self.trace_ctx {
            ctx.write_to(&mut tc);
        }
        buf.extend_from_slice(&tc);
        buf.extend_from_slice(&self.source_node_id.unwrap_or(0).to_le_bytes());
        for op in &self.ops {
            let ob = op.serialize();
            buf.extend_from_slice(&(ob.len() as u32).to_le_bytes());
            buf.extend_from_slice(&ob);
        }
        buf
    }

    /// Deserialize from bytes.
    ///
    /// Tolerates legacy V1 frames (`[first_seq:8][count:4][op...]`) and
    /// V2 frames (trace context but no source node id). V1/V2 frames decode
    /// with `source_node_id = None`; new senders always produce V3.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        need(data, 1)?;
        if data[0] == BATCH_PROTOCOL_V3 {
            // V3: [version(1) + first_seq(8) + count(4) + trace_ctx(24) + source_node_id(8)]
            need(data, Self::HEADER_SIZE)?;
            let first_sequence = u64::from_le_bytes(data[1..9].try_into().unwrap());
            let count = u32::from_le_bytes(data[9..13].try_into().unwrap()) as usize;
            let trace_ctx = WireTraceContext::read_from(&data[13..13 + WireTraceContext::SIZE]);
            let source_off = 13 + WireTraceContext::SIZE;
            let raw_source =
                u64::from_le_bytes(data[source_off..source_off + 8].try_into().unwrap());
            let source_node_id = if raw_source == 0 {
                None
            } else {
                Some(raw_source)
            };
            let mut pos = Self::HEADER_SIZE;
            let mut ops = Vec::with_capacity(count);
            for _ in 0..count {
                need(&data[pos..], 4)?;
                let op_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                need(&data[pos..], op_len)?;
                let (op, _) = ReplicaOp::deserialize(&data[pos..pos + op_len])?;
                ops.push(op);
                pos += op_len;
            }
            Ok(ReplicaBatch {
                first_sequence,
                ops,
                trace_ctx,
                source_node_id,
            })
        } else if data[0] == BATCH_PROTOCOL_V2 {
            // V2: [version(1) + first_seq(8) + count(4) + trace_ctx(24)] = 37 bytes
            let header_size = 1 + 8 + 4 + WireTraceContext::SIZE;
            need(data, header_size)?;
            let first_sequence = u64::from_le_bytes(data[1..9].try_into().unwrap());
            let count = u32::from_le_bytes(data[9..13].try_into().unwrap()) as usize;
            let trace_ctx = WireTraceContext::read_from(&data[13..13 + WireTraceContext::SIZE]);
            let mut pos = header_size;
            let mut ops = Vec::with_capacity(count);
            for _ in 0..count {
                need(&data[pos..], 4)?;
                let op_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                need(&data[pos..], op_len)?;
                let (op, _) = ReplicaOp::deserialize(&data[pos..pos + op_len])?;
                ops.push(op);
                pos += op_len;
            }
            Ok(ReplicaBatch {
                first_sequence,
                ops,
                trace_ctx,
                source_node_id: None,
            })
        } else {
            // Legacy V1: no version byte. `data[0]` is the low byte of
            // first_sequence.
            need(data, 12)?;
            let first_sequence = u64::from_le_bytes(data[..8].try_into().unwrap());
            let count = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;
            let mut ops = Vec::with_capacity(count);
            let mut pos = 12;
            for _ in 0..count {
                need(&data[pos..], 4)?;
                let op_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                need(&data[pos..], op_len)?;
                let (op, _) = ReplicaOp::deserialize(&data[pos..pos + op_len])?;
                ops.push(op);
                pos += op_len;
            }
            Ok(ReplicaBatch {
                first_sequence,
                ops,
                trace_ctx: None,
                source_node_id: None,
            })
        }
    }

    /// The last sequence number in this batch.
    pub fn last_sequence(&self) -> u64 {
        self.first_sequence + self.ops.len().saturating_sub(1) as u64
    }

    /// Batch header overhead in bytes.
    ///
    /// Protocol V3 layout: `version(1) + first_sequence(8) + count(4) + trace_ctx(24) + source_node_id(8) = 45`.
    pub const HEADER_SIZE: usize = 1 + 8 + 4 + WireTraceContext::SIZE + 8;

    /// Byte offset of the trace_id field in the V2 serialized frame.
    /// Exposed for the test suite that inspects exact byte layout.
    pub const TRACE_ID_OFFSET: usize = 1 + 8 + 4;

    /// Byte offset of the span_id field in the V2 serialized frame.
    pub const SPAN_ID_OFFSET: usize = Self::TRACE_ID_OFFSET + 16;
}

impl ReplicaAck {
    /// Serialize to bytes.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            ReplicaAck::Ok { through_sequence } => {
                buf.push(0);
                buf.extend_from_slice(&through_sequence.to_le_bytes());
            }
            ReplicaAck::Error {
                failed_sequence,
                message,
            } => {
                buf.push(1);
                buf.extend_from_slice(&failed_sequence.to_le_bytes());
                buf.extend_from_slice(&(message.len() as u32).to_le_bytes());
                buf.extend_from_slice(message.as_bytes());
            }
        }
        buf
    }

    /// Deserialize from bytes.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        need(data, 1)?;
        match data[0] {
            0 => {
                need(data, 9)?;
                Ok(ReplicaAck::Ok {
                    through_sequence: u64::from_le_bytes(data[1..9].try_into().unwrap()),
                })
            }
            1 => {
                need(data, 13)?;
                let seq = u64::from_le_bytes(data[1..9].try_into().unwrap());
                let len = u32::from_le_bytes(data[9..13].try_into().unwrap()) as usize;
                need(data, 13 + len)?;
                let msg = String::from_utf8_lossy(&data[13..13 + len]).into_owned();
                Ok(ReplicaAck::Error {
                    failed_sequence: seq,
                    message: msg,
                })
            }
            _ => Err(ProtocolError::UnknownOp(data[0])),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::TxFlags;

    fn key(n: u8) -> TxKey {
        let mut txid = [0u8; 32];
        txid[0] = n;
        TxKey { txid }
    }

    #[test]
    fn create_metadata_flag_bytes_use_replica_create_layout() {
        let flags =
            TxFlags::IS_COINBASE | TxFlags::LOCKED | TxFlags::CONFLICTING | TxFlags::EXTERNAL;

        let (is_coinbase, wire_flags) = create_metadata_flag_bytes(flags);

        assert_eq!(is_coinbase, 1);
        assert_eq!(wire_flags & 0x01, 0x01, "locked is wire bit 0");
        assert_eq!(wire_flags & 0x02, 0x02, "conflicting is wire bit 1");
        assert_eq!(
            wire_flags & crate::protocol::opcodes::FLAG_EXTERNAL_BLOB,
            crate::protocol::opcodes::FLAG_EXTERNAL_BLOB,
        );
        assert_eq!(wire_flags & 0x04, 0, "frozen is not a persisted tx flag");
    }

    #[test]
    fn spend_round_trip() {
        let op = ReplicaOp::Spend {
            tx_key: key(1),
            offset: 5,
            spending_data: [0xAB; 36],
            master_generation: 0,
        };
        let bytes = op.serialize();
        assert!(
            bytes.len() < 80,
            "spend serialized to {} bytes",
            bytes.len()
        );
        let (decoded, consumed) = ReplicaOp::deserialize(&bytes).unwrap();
        assert_eq!(decoded, op);
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn prune_slot_round_trip() {
        let op = ReplicaOp::PruneSlot {
            tx_key: key(2),
            offset: 42,
        };
        let bytes = op.serialize();
        assert!(
            bytes.len() < 44,
            "prune serialized to {} bytes",
            bytes.len()
        );
        let (decoded, _) = ReplicaOp::deserialize(&bytes).unwrap();
        assert_eq!(decoded, op);
    }

    #[test]
    fn all_variants_round_trip() {
        let ops = vec![
            ReplicaOp::Spend {
                tx_key: key(1),
                offset: 0,
                spending_data: [0x11; 36],
                master_generation: 0,
            },
            ReplicaOp::Unspend {
                tx_key: key(2),
                offset: 1,
                master_generation: 0,
            },
            ReplicaOp::SetMined {
                tx_key: key(3),
                block_id: 100,
                block_height: 800000,
                subtree_idx: 7,
                on_longest_chain: true,
                master_generation: 0,
            },
            ReplicaOp::UnsetMined {
                tx_key: key(4),
                block_id: 200,
                master_generation: 0,
            },
            ReplicaOp::Freeze {
                tx_key: key(5),
                offset: 3,
                master_generation: 0,
            },
            ReplicaOp::Unfreeze {
                tx_key: key(6),
                offset: 4,
                master_generation: 0,
            },
            ReplicaOp::Reassign {
                tx_key: key(7),
                offset: 5,
                new_hash: [0xCC; 32],
                block_height: 1000,
                spendable_after: 100,
                master_generation: 0,
            },
            ReplicaOp::SetConflicting {
                tx_key: key(8),
                value: true,
                current_block_height: 500,
                retention: 288,
                master_generation: 0,
            },
            ReplicaOp::SetLocked {
                tx_key: key(9),
                value: false,
                master_generation: 0,
            },
            ReplicaOp::PreserveUntil {
                tx_key: key(10),
                block_height: 5000,
                master_generation: 0,
            },
            ReplicaOp::Create {
                tx_key: key(11),
                metadata_bytes: vec![0x42; 100],
                utxo_hashes: vec![[0xAA; 32], [0xBB; 32]],
                cold_data: Some(vec![0xDD; 50]),
                is_external: false,
            },
            ReplicaOp::Delete { tx_key: key(12) },
            ReplicaOp::PruneSlot {
                tx_key: key(13),
                offset: 99,
            },
        ];

        for op in &ops {
            let bytes = op.serialize();
            let (decoded, consumed) = ReplicaOp::deserialize(&bytes).unwrap();
            assert_eq!(&decoded, op, "round-trip failed for {op:?}");
            assert_eq!(consumed, bytes.len());
        }
    }

    #[test]
    fn create_with_100_utxos_round_trip() {
        let hashes: Vec<[u8; 32]> = (0..100)
            .map(|i| {
                let mut h = [0u8; 32];
                h[0] = i;
                h
            })
            .collect();
        let op = ReplicaOp::Create {
            tx_key: key(1),
            metadata_bytes: vec![0; 256],
            utxo_hashes: hashes.clone(),
            cold_data: None,
            is_external: false,
        };
        let bytes = op.serialize();
        let (decoded, _) = ReplicaOp::deserialize(&bytes).unwrap();
        assert_eq!(decoded, op);
    }

    #[test]
    fn batch_round_trip() {
        let batch = ReplicaBatch {
            first_sequence: 100,
            ops: vec![
                ReplicaOp::Spend {
                    tx_key: key(1),
                    offset: 0,
                    spending_data: [0x11; 36],
                    master_generation: 0,
                },
                ReplicaOp::Freeze {
                    tx_key: key(2),
                    offset: 1,
                    master_generation: 0,
                },
                ReplicaOp::PruneSlot {
                    tx_key: key(3),
                    offset: 2,
                },
            ],
            trace_ctx: None,
            source_node_id: None,
        };
        let bytes = batch.serialize();
        let decoded = ReplicaBatch::deserialize(&bytes).unwrap();
        assert_eq!(decoded, batch);
        assert_eq!(decoded.last_sequence(), 102);
    }

    #[test]
    fn batch_100_ops_round_trip() {
        let ops: Vec<ReplicaOp> = (0..100u8)
            .map(|i| ReplicaOp::Spend {
                tx_key: key(i),
                offset: i as u32,
                spending_data: [i; 36],
                master_generation: 0,
            })
            .collect();
        let batch = ReplicaBatch {
            first_sequence: 1000,
            ops,
            trace_ctx: None,
            source_node_id: None,
        };
        let bytes = batch.serialize();
        let decoded = ReplicaBatch::deserialize(&bytes).unwrap();
        assert_eq!(decoded.ops.len(), 100);
        assert_eq!(decoded.first_sequence, 1000);
        assert_eq!(decoded.last_sequence(), 1099);
    }

    #[test]
    fn batch_header_overhead() {
        // V3: version(1) + first_sequence(8) + count(4) + trace_ctx(24) + source_node_id(8) = 45 bytes.
        assert_eq!(ReplicaBatch::HEADER_SIZE, 45);
        assert_eq!(ReplicaBatch::TRACE_ID_OFFSET, 13);
        assert_eq!(ReplicaBatch::SPAN_ID_OFFSET, 29);
    }

    #[test]
    fn replication_batch_header_roundtrips_trace_context() {
        let ctx = WireTraceContext {
            trace_id: [
                0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE,
                0xAF, 0xB0,
            ],
            span_id: [0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8],
        };
        let batch = ReplicaBatch {
            first_sequence: 42,
            ops: vec![ReplicaOp::Freeze {
                tx_key: key(7),
                offset: 3,
                master_generation: 1,
            }],
            trace_ctx: Some(ctx),
            source_node_id: Some(9),
        };
        let bytes = batch.serialize();
        // Version byte.
        assert_eq!(bytes[0], BATCH_PROTOCOL_V3);
        // Exact trace_id bytes at declared offset.
        assert_eq!(
            &bytes[ReplicaBatch::TRACE_ID_OFFSET..ReplicaBatch::TRACE_ID_OFFSET + 16],
            &ctx.trace_id,
        );
        // Exact span_id bytes at declared offset.
        assert_eq!(
            &bytes[ReplicaBatch::SPAN_ID_OFFSET..ReplicaBatch::SPAN_ID_OFFSET + 8],
            &ctx.span_id,
        );
        let decoded = ReplicaBatch::deserialize(&bytes).unwrap();
        assert_eq!(decoded, batch);
        assert_eq!(decoded.trace_ctx, Some(ctx));
    }

    #[test]
    fn replication_batch_source_node_id_roundtrips() {
        let batch = ReplicaBatch {
            first_sequence: 88,
            ops: vec![ReplicaOp::Delete { tx_key: key(8) }],
            trace_ctx: None,
            source_node_id: Some(42),
        };
        let bytes = batch.serialize();
        let source_offset = ReplicaBatch::SPAN_ID_OFFSET + 8;
        assert_eq!(
            u64::from_le_bytes(bytes[source_offset..source_offset + 8].try_into().unwrap()),
            42,
        );

        let decoded = ReplicaBatch::deserialize(&bytes).unwrap();
        assert_eq!(decoded.source_node_id, Some(42));
        assert_eq!(decoded, batch);
    }

    #[test]
    fn replication_batch_without_trace_context_roundtrips_zero_bytes() {
        let batch = ReplicaBatch {
            first_sequence: 7,
            ops: vec![ReplicaOp::Delete { tx_key: key(1) }],
            trace_ctx: None,
            source_node_id: None,
        };
        let bytes = batch.serialize();
        // 24 bytes at the trace_ctx offset must be all zero.
        let tc = &bytes[ReplicaBatch::TRACE_ID_OFFSET..ReplicaBatch::SPAN_ID_OFFSET + 8];
        assert!(
            tc.iter().all(|b| *b == 0),
            "trace context region must be zero when None: {tc:?}",
        );
        let decoded = ReplicaBatch::deserialize(&bytes).unwrap();
        assert_eq!(decoded.trace_ctx, None);
        assert_eq!(decoded, batch);
    }

    #[test]
    fn replication_batch_v1_legacy_frame_decodes_without_trace_context() {
        // Manually craft a V1 frame: [first_seq:8][count:4][op_len:4][op_bytes]
        // The first byte is the low byte of first_sequence, which we pick
        // to NOT collide with BATCH_PROTOCOL_V2.
        let op = ReplicaOp::Delete { tx_key: key(2) };
        let ob = op.serialize();
        let first_seq: u64 = 0x1000; // low byte = 0x00, not the V2 tag
        let mut v1 = Vec::new();
        v1.extend_from_slice(&first_seq.to_le_bytes());
        v1.extend_from_slice(&1u32.to_le_bytes());
        v1.extend_from_slice(&(ob.len() as u32).to_le_bytes());
        v1.extend_from_slice(&ob);
        let decoded = ReplicaBatch::deserialize(&v1).expect("v1 frame decodes");
        assert_eq!(decoded.first_sequence, first_seq);
        assert_eq!(decoded.ops.len(), 1);
        assert_eq!(decoded.ops[0], op);
        assert!(decoded.trace_ctx.is_none());
    }

    #[test]
    fn replication_batch_v2_legacy_frame_decodes_without_source_node_id() {
        let ctx = WireTraceContext {
            trace_id: [0x44; 16],
            span_id: [0x55; 8],
        };
        let op = ReplicaOp::Freeze {
            tx_key: key(3),
            offset: 1,
            master_generation: 7,
        };
        let ob = op.serialize();
        let first_seq = 123u64;
        let mut v2 = Vec::new();
        v2.push(BATCH_PROTOCOL_V2);
        v2.extend_from_slice(&first_seq.to_le_bytes());
        v2.extend_from_slice(&1u32.to_le_bytes());
        let mut tc = [0u8; WireTraceContext::SIZE];
        ctx.write_to(&mut tc);
        v2.extend_from_slice(&tc);
        v2.extend_from_slice(&(ob.len() as u32).to_le_bytes());
        v2.extend_from_slice(&ob);

        let decoded = ReplicaBatch::deserialize(&v2).expect("v2 frame decodes");
        assert_eq!(decoded.first_sequence, first_seq);
        assert_eq!(decoded.ops, vec![op]);
        assert_eq!(decoded.trace_ctx, Some(ctx));
        assert_eq!(decoded.source_node_id, None);
    }

    #[test]
    fn ack_ok_round_trip() {
        let ack = ReplicaAck::Ok {
            through_sequence: 42,
        };
        let bytes = ack.serialize();
        let decoded = ReplicaAck::deserialize(&bytes).unwrap();
        assert_eq!(decoded, ack);
    }

    #[test]
    fn ack_error_round_trip() {
        let ack = ReplicaAck::Error {
            failed_sequence: 99,
            message: "test error".into(),
        };
        let bytes = ack.serialize();
        let decoded = ReplicaAck::deserialize(&bytes).unwrap();
        assert_eq!(decoded, ack);
    }

    #[test]
    fn catchup_request_round_trip() {
        let req = CatchupRequest {
            last_ack_sequence: 12345,
        };
        let bytes = req.serialize();
        assert_eq!(bytes.len(), 8);
        let decoded = CatchupRequest::deserialize(&bytes).unwrap();
        assert_eq!(decoded.last_ack_sequence, 12345);
    }
}
